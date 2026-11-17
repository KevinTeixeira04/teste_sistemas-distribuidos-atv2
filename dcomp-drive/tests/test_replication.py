import dataclasses
import json
import threading

import pytest
import zmq

from src.controller import metadata as md
from src.controller.config import settings
from src.controller.controller_server import ControllerServer
from src.controller.handlers import register_handlers
from src.controller.replication import Replicator

from tests.test_end_to_end import cluster, make_file  
from tests.test_handlers import FakeRegistry, call, db, ok  


class FakeSender:

    def __init__(self):
        self.calls = []          
        self.fail = False
        self.size = 1024         
        self.hook = None         
    def __call__(self, addr, request):
        self.calls.append((addr, request))
        if self.hook:
            self.hook()
        if self.fail:
            raise TimeoutError("sem resposta")
        return {"req_id": request["req_id"], "status": "ACK", "code": "OK", "size_bytes": self.size}


@pytest.fixture
def env(db, monkeypatch):
    made = []

    def make(ids, rf=2, max_concurrent=2):
        monkeypatch.setenv("REPLICATION_MAX_CONCURRENT", str(max_concurrent))
        cfg = dataclasses.replace(settings, replication_factor=rf)
        registry = FakeRegistry(ids)
        server = ControllerServer(cfg, db, registry, bind_addr="tcp://127.0.0.1:0")
        register_handlers(server, db, registry, cfg)
        sender = FakeSender()
        replicator = Replicator(db, registry, cfg, send_copy=sender)
        made.append((server, replicator))
        return server, registry, replicator, sender

    yield make
    for server, replicator in made:
        server._pool.shutdown(wait=False)
        replicator.stop()


def put_block(server, path, keep=None):
    file_id = ok(call(server, "INIT_WRITE", remote_path=path, size_bytes=1024, file_sha256="h"))["file_id"]
    block_id = f"blk-{path.strip('/')}"
    alloc = ok(call(server, "ALLOCATE_BLOCK", file_id=file_id, block_id=block_id, block_index=0,
                    size_bytes=1024, sha256="b" * 64))
    nodes = [n["node_id"] for n in alloc["nodes"]][:keep]
    ok(call(server, "COMMIT_BLOCK", file_id=file_id, block_id=block_id, node_ids=nodes))
    ok(call(server, "FINISH_WRITE", file_id=file_id, file_sha256="h"))
    return block_id, nodes


def replica_rows(db, block_id):
    with db.tx() as cur:
        cur.execute("SELECT maquina, state FROM replicas WHERE block_id = %s", (block_id,))
        return {(r["maquina"], r["state"]) for r in cur.fetchall()}


def live_replicas(server, path):
    meta = ok(call(server, "GET_FILE_METADATA", remote_path=path))
    return {r["node_id"] for r in meta["blocks"][0]["replicas"]}


def test_node_death_triggers_copy_to_a_new_node(env, db):
    server, registry, rep, sender = env(["n1", "n2", "n3"])
    block, holders = put_block(server, "/a.txt")
    spare = ({"n1", "n2", "n3"} - set(holders)).pop()
    dead, survivor = holders
    registry.nodes[dead]["state"] = "DEAD"

    assert rep.run_once() == 1

    (addr, request), = sender.calls
    assert addr == f"tcp://{spare}:5555"                        
    assert request["source_addr"] == f"tcp://{survivor}:5555"    
    assert request["action"] == "COPY_BLOCK" and request["block_id"] == block
    assert live_replicas(server, "/a.txt") == {survivor, spare}
    assert rep.run_once() == 0                                   


def test_blocks_with_fewer_live_replicas_go_first(env):
    server, _, rep, sender = env(["n1", "n2", "n3", "n4"], rf=3, max_concurrent=1)
    block_a, _ = put_block(server, "/a", keep=1)   
    block_b, _ = put_block(server, "/b", keep=2)    

    assert rep.run_once() == 2                      
    assert [req["block_id"] for _, req in sender.calls] == [block_a, block_a]
    assert rep.run_once() == 1                      
    assert sender.calls[-1][1]["block_id"] == block_b
    assert rep.run_once() == 0


def test_block_without_any_live_replica_is_left_alone(env, db):
    server, registry, rep, sender = env(["n1", "n2", "n3"])
    block, holders = put_block(server, "/a.txt")
    for node in holders:
        registry.nodes[node]["state"] = "DEAD"

    assert rep.run_once() == 0
    assert sender.calls == []                       
    assert not any(state == "PENDENTE" for _, state in replica_rows(db, block))


def test_failed_copy_is_cleaned_and_retried_after_cooldown(env, db):
    server, registry, rep, sender = env(["n1", "n2", "n3"])
    block, holders = put_block(server, "/a.txt")
    registry.nodes[holders[0]]["state"] = "DEAD"

    sender.fail = True
    assert rep.run_once() == 0
    assert not any(state == "PENDENTE" for _, state in replica_rows(db, block))   
    assert rep.status()["failed"] == 1

    sender.fail = False
    assert rep.run_once() == 0 and len(sender.calls) == 1                         
    rep._cooldown.clear()
    assert rep.run_once() == 1


def test_corrupted_replica_is_replaced_by_a_healthy_copy(env, db):
    server, _, rep, sender = env(["n1", "n2", "n3"])
    block, holders = put_block(server, "/a.txt")
    bad, good = holders
    ok(call(server, "REPORT_CORRUPTION", block_id=block, node_id=bad))
    assert live_replicas(server, "/a.txt") == {good}

    assert rep.run_once() == 1

    assert sender.calls[0][1]["source_addr"] == f"tcp://{good}:5555"
    assert len(live_replicas(server, "/a.txt")) == 2


def test_no_eligible_node_means_no_copy(env, db):
    server, registry, rep, sender = env(["n1", "n2"])
    block, _ = put_block(server, "/a.txt")
    registry.nodes["n1"]["state"] = "DEAD"          
    assert rep.run_once() == 0
    assert sender.calls == []
    assert rep.status()["under_replicated_blocks"] == 1
    assert not any(state == "PENDENTE" for _, state in replica_rows(db, block))


def test_copy_with_wrong_size_is_rejected_and_queued_for_gc(env, db):
    server, registry, rep, sender = env(["n1", "n2", "n3"])
    block, holders = put_block(server, "/a.txt")
    registry.nodes[holders[0]]["state"] = "DEAD"
    sender.size = 999

    assert rep.run_once() == 0
    target = sender.calls[0][0].split("//")[1].split(":")[0]
    assert (target, "PENDENTE") not in replica_rows(db, block)
    with db.tx() as cur:
        cur.execute("SELECT maquina FROM gc_blocks WHERE block_id = %s", (block,))
        assert [r["maquina"] for r in cur.fetchall()] == [target]    

def test_file_deleted_during_copy_does_not_leave_a_replica(env, db):
    server, registry, rep, sender = env(["n1", "n2", "n3"])
    block, holders = put_block(server, "/a.txt")
    registry.nodes[holders[0]]["state"] = "DEAD"
    sender.hook = lambda: ok(call(server, "DELETE_FILE", remote_path="/a.txt"))

    assert rep.run_once() == 0
    assert replica_rows(db, block) == set()
    with db.tx() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM gc_blocks WHERE block_id = %s", (block,))
        assert cur.fetchone()["n"] >= 1


def test_stale_pending_replicas_of_complete_files_are_cleared(env, db):
    server, _, rep, _ = env(["n1", "n2", "n3"])
    block, holders = put_block(server, "/a.txt")
    spare = ({"n1", "n2", "n3"} - set(holders)).pop()
    with db.tx() as cur:
        md.add_pending_replicas(cur, block, [(spare, "disk1")])   
        assert md.clear_stale_pending(cur) == 1
    assert (spare, "PENDENTE") not in replica_rows(db, block)


def test_pending_replicas_of_files_being_written_are_kept(env, db):
    server, _, _, _ = env(["n1", "n2", "n3"])
    file_id = ok(call(server, "INIT_WRITE", remote_path="/w", size_bytes=1, file_sha256="h"))["file_id"]
    ok(call(server, "ALLOCATE_BLOCK", file_id=file_id, block_id="w1", block_index=0, size_bytes=10, sha256="x"))
    with db.tx() as cur:
        assert md.clear_stale_pending(cur) == 0                   

def test_zmq_copy_speaks_the_node_protocol(db):
    ctx = zmq.Context()
    router = ctx.socket(zmq.ROUTER)
    port = router.bind_to_random_port("tcp://127.0.0.1")
    seen = {}

    def fake_node():
        identity, empty, body = router.recv_multipart()
        request = json.loads(body)
        seen.update(request)
        reply = {"req_id": request["req_id"], "status": "ACK", "code": "OK", "size_bytes": 1024}
        router.send_multipart([identity, b"", json.dumps(reply).encode()])

    thread = threading.Thread(target=fake_node, daemon=True)
    thread.start()
    rep = Replicator(db, FakeRegistry(), settings)
    reply = rep._zmq_copy(f"tcp://127.0.0.1:{port}",
                          {"req_id": "r1", "action": "COPY_BLOCK", "block_id": "b", "source_addr": "tcp://x:1"})
    thread.join(timeout=3)
    router.close(0)
    ctx.term()
    rep.stop()
    assert reply["status"] == "ACK" and seen["action"] == "COPY_BLOCK" and seen["source_addr"] == "tcp://x:1"



def drain(rep, rounds=10):
    for _ in range(rounds):
        if rep.run_once() == 0:
            return


def test_dead_node_is_replaced_and_every_block_is_readable(cluster, db, tmp_path):  
    client, nodes, registry = cluster
    src = make_file(tmp_path)
    client.put_file(str(src), "/f.bin")
    registry.nodes["n1"]["state"] = "DEAD"

    rep = Replicator(db, registry, settings)
    drain(rep)
    rep.stop()

    meta = client._rpc_controller("GET_FILE_METADATA", {"remote_path": "/f.bin"})
    for block in meta["blocks"]:
        assert {r["node_id"] for r in block["replicas"]} == {"n2", "n3"}      
        for name in ("n2", "n3"):
            assert (nodes[name].storage_path / block["block_id"]).exists()
    dst = tmp_path / "out.bin"
    client.get_file("/f.bin", str(dst))
    assert dst.read_bytes() == src.read_bytes()


def test_corrupted_block_is_healed_end_to_end(cluster, db, tmp_path):  
    client, nodes, registry = cluster
    src = make_file(tmp_path, 500)
    client.put_file(str(src), "/f.bin")
    block = client._rpc_controller("GET_FILE_METADATA", {"remote_path": "/f.bin"})["blocks"][0]
    bad = block["replicas"][0]["node_id"]
    (nodes[bad].storage_path / block["block_id"]).write_bytes(b"adulterado")

    client.get_file("/f.bin", str(tmp_path / "out.bin"))     
    rep = Replicator(db, registry, settings)
    drain(rep)
    rep.stop()

    healed = client._rpc_controller("GET_FILE_METADATA", {"remote_path": "/f.bin"})["blocks"][0]["replicas"]
    assert len(healed) == 2
    dst = tmp_path / "out2.bin"
    client.get_file("/f.bin", str(dst))
    assert dst.read_bytes() == src.read_bytes()
