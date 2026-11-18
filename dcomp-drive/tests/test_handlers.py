import os
import threading
import uuid

import psycopg2
import pytest

from src.controller.config import settings
from src.controller.controller_server import ControllerServer
from src.controller.db import Database
from src.controller.handlers import register_handlers

GB = 1024 ** 3


def conn_params():
    return {
        "host": os.getenv("TEST_DB_HOST", "localhost"),
        "port": int(os.getenv("TEST_DB_PORT", "5433")),
        "dbname": os.getenv("TEST_DB_NAME", "dfs_metadata"),
        "user": os.getenv("TEST_DB_USER", "postgre"),
        "password": os.getenv("TEST_DB_PASSWORD", "postgre"),
    }


@pytest.fixture
def db():
    schema = "t_" + uuid.uuid4().hex[:10]
    try:
        admin = psycopg2.connect(connect_timeout=3, **conn_params())
    except psycopg2.OperationalError:
        pytest.skip("PostgreSQL de teste indisponível (suba com: docker compose up -d postgres)")
    admin.autocommit = True
    admin.cursor().execute(f"CREATE SCHEMA {schema}")

    database = Database(settings)
    database.params = {**conn_params(), "options": f"-c search_path={schema}"}
    database.init_schema()
    yield database
    database.close()
    admin.cursor().execute(f"DROP SCHEMA {schema} CASCADE")
    admin.close()


def disks(used_gb=10):
    return [{"nome": "disk1", "capacity": 100 * GB, "used": used_gb * GB}]


class FakeRegistry:
    def __init__(self, ids=("n1", "n2", "n3")):
        self.nodes = {i: {"node_id": i, "state": "ALIVE", "draining": False, "host": None,
                          "addr": f"tcp://{i}:5555", "disks": disks()} for i in ids}

    def get_nodes(self):
        return self.nodes


@pytest.fixture
def registry():
    return FakeRegistry()


@pytest.fixture
def server(db, registry):
    s = ControllerServer(settings, db, registry, bind_addr="tcp://127.0.0.1:0")
    register_handlers(s, db, registry, settings)
    yield s
    s._pool.shutdown(wait=False)


def call(server, action, **fields):
    return server.handle_request({"req_id": "t", "action": action, **fields})


def ok(reply):
    assert reply["status"] == "ACK", reply
    return reply


def err(reply, code):
    assert reply["status"] == "ERR" and reply["code"] == code, reply
    return reply


def put(server, path, blocks=1, sha="h" * 64, nodes=None, finish=True):
    """Executa o fluxo de escrita do cliente. Devolve (file_id, [block_ids])."""
    file_id = ok(call(server, "INIT_WRITE", remote_path=path, size_bytes=10, encrypted=False, file_sha256=sha))["file_id"]
    ids = []
    for i in range(blocks):
        block_id = str(uuid.uuid4())
        alloc = ok(call(server, "ALLOCATE_BLOCK", file_id=file_id, block_id=block_id, block_index=i,
                        size_bytes=1024, sha256="b" * 64))
        chosen = nodes or [n["node_id"] for n in alloc["nodes"]]
        ok(call(server, "COMMIT_BLOCK", file_id=file_id, block_id=block_id, node_ids=chosen))
        ids.append(block_id)
    if finish:
        ok(call(server, "FINISH_WRITE", file_id=file_id, file_sha256=sha))
    return file_id, ids


def test_put_then_get_metadata_roundtrip(server):
    file_id, (b0, b1) = put(server, "/docs/a.txt", blocks=2)
    meta = ok(call(server, "GET_FILE_METADATA", remote_path="/docs/a.txt"))
    assert meta["file_id"] == file_id and meta["file_sha256"] == "h" * 64
    assert [b["block_id"] for b in meta["blocks"]] == [b0, b1]
    assert all(len(b["replicas"]) == 2 for b in meta["blocks"])      # fator de replicação 2
    assert all(r["addr"].startswith("tcp://") for b in meta["blocks"] for r in b["replicas"])


def test_allocate_returns_distinct_nodes(server):
    file_id = ok(call(server, "INIT_WRITE", remote_path="/f", size_bytes=1, file_sha256="h"))["file_id"]
    alloc = ok(call(server, "ALLOCATE_BLOCK", file_id=file_id, block_id="b1", block_index=0, size_bytes=10, sha256="x"))
    assert len({n["node_id"] for n in alloc["nodes"]}) == 2 and alloc["under_replicated"] is False


def test_file_is_invisible_until_finish(server):
    put(server, "/docs/a.txt", finish=False)
    err(call(server, "GET_FILE_METADATA", remote_path="/docs/a.txt"), "NOT_FOUND")
    assert ok(call(server, "LIST_DIR", remote_path="/docs"))["entries"] == []


def test_list_dir_shows_files_and_dirs(server):
    put(server, "/docs/a.txt")
    ok(call(server, "MKDIR", remote_path="/docs/sub"))
    entries = ok(call(server, "LIST_DIR", remote_path="/docs"))["entries"]
    assert [(e["name"], e["is_directory"]) for e in entries] == [("sub", True), ("a.txt", False)]
    assert [e["name"] for e in ok(call(server, "LIST_DIR", remote_path="/"))["entries"]] == ["docs"]


def test_put_over_existing_file_is_refused(server):
    put(server, "/a.txt")
    err(call(server, "INIT_WRITE", remote_path="/a.txt", size_bytes=1, file_sha256="h"), "EXISTS")


def test_second_writer_gets_busy_until_lease_expires(server, db):
    first = ok(call(server, "INIT_WRITE", remote_path="/a.txt", size_bytes=1, file_sha256="h"))["file_id"]
    err(call(server, "INIT_WRITE", remote_path="/a.txt", size_bytes=1, file_sha256="h"), "BUSY")
    with db.tx() as cur:
        cur.execute("UPDATE write_leases SET expires_at = NOW() - interval '1 second'")
    second = ok(call(server, "INIT_WRITE", remote_path="/a.txt", size_bytes=1, file_sha256="h"))["file_id"]
    assert second != first
    err(call(server, "ALLOCATE_BLOCK", file_id=first, block_id="b", block_index=0, size_bytes=1, sha256="x"), "NOT_FOUND")


def test_expired_lease_blocks_further_writes(server, db):
    file_id = ok(call(server, "INIT_WRITE", remote_path="/a.txt", size_bytes=1, file_sha256="h"))["file_id"]
    with db.tx() as cur:
        cur.execute("UPDATE write_leases SET expires_at = NOW() - interval '1 second'")
    err(call(server, "ALLOCATE_BLOCK", file_id=file_id, block_id="b", block_index=0, size_bytes=1, sha256="x"), "LEASE_EXPIRED")


def test_concurrent_writers_only_one_wins(server):
    results = []

    def writer():
        results.append(call(server, "INIT_WRITE", remote_path="/race.txt", size_bytes=1, file_sha256="h")["status"])

    threads = [threading.Thread(target=writer) for _ in range(6)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert results.count("ACK") == 1 and results.count("ERR") == 5


def test_under_replicated_when_only_one_node(server, registry):
    registry.nodes = {"n1": registry.nodes["n1"]}
    file_id = ok(call(server, "INIT_WRITE", remote_path="/a", size_bytes=1, file_sha256="h"))["file_id"]
    alloc = ok(call(server, "ALLOCATE_BLOCK", file_id=file_id, block_id="b", block_index=0, size_bytes=10, sha256="x"))
    assert len(alloc["nodes"]) == 1 and alloc["under_replicated"] is True
    assert ok(call(server, "COMMIT_BLOCK", file_id=file_id, block_id="b", node_ids=["n1"]))["under_replicated"] is True


def test_strict_policy_rejects_when_not_enough_nodes(server, registry, db):
    registry.nodes = {"n1": registry.nodes["n1"]}
    from src.controller.handlers import Handlers
    handlers = Handlers(db, registry, settings)
    handlers.allow_partial = False
    server.register("ALLOCATE_BLOCK", handlers.allocate_block)
    file_id = ok(call(server, "INIT_WRITE", remote_path="/a", size_bytes=1, file_sha256="h"))["file_id"]
    err(call(server, "ALLOCATE_BLOCK", file_id=file_id, block_id="b", block_index=0, size_bytes=10, sha256="x"), "NOT_ENOUGH_NODES")


def test_no_nodes_available(server, registry):
    registry.nodes = {}
    file_id = ok(call(server, "INIT_WRITE", remote_path="/a", size_bytes=1, file_sha256="h"))["file_id"]
    err(call(server, "ALLOCATE_BLOCK", file_id=file_id, block_id="b", block_index=0, size_bytes=10, sha256="x"), "NO_NODES")


def test_commit_with_unallocated_node_is_refused(server):
    file_id = ok(call(server, "INIT_WRITE", remote_path="/a", size_bytes=1, file_sha256="h"))["file_id"]
    ok(call(server, "ALLOCATE_BLOCK", file_id=file_id, block_id="b", block_index=0, size_bytes=10, sha256="x"))
    err(call(server, "COMMIT_BLOCK", file_id=file_id, block_id="b", node_ids=["fantasma"]), "NO_VALID_REPLICA")



def test_finish_fails_with_missing_block_index(server):
    file_id = ok(call(server, "INIT_WRITE", remote_path="/a", size_bytes=1, file_sha256="h"))["file_id"]
    alloc = ok(call(server, "ALLOCATE_BLOCK", file_id=file_id, block_id="b", block_index=1, size_bytes=10, sha256="x"))
    ok(call(server, "COMMIT_BLOCK", file_id=file_id, block_id="b", node_ids=[alloc["nodes"][0]["node_id"]]))
    err(call(server, "FINISH_WRITE", file_id=file_id, file_sha256="h"), "INCOMPLETE_FILE")


def test_finish_fails_with_uncommitted_block_and_wrong_hash(server):
    file_id = ok(call(server, "INIT_WRITE", remote_path="/a", size_bytes=1, file_sha256="h"))["file_id"]
    ok(call(server, "ALLOCATE_BLOCK", file_id=file_id, block_id="b", block_index=0, size_bytes=10, sha256="x"))
    err(call(server, "FINISH_WRITE", file_id=file_id, file_sha256="h"), "INCOMPLETE_FILE")
    err(call(server, "FINISH_WRITE", file_id=file_id, file_sha256="outro"), "HASH_MISMATCH")


def test_empty_file_can_be_written(server):
    put(server, "/vazio", blocks=0)
    assert ok(call(server, "GET_FILE_METADATA", remote_path="/vazio"))["blocks"] == []



def test_get_metadata_skips_dead_nodes(server, registry):
    put(server, "/a.txt")
    registry.nodes["n1"]["state"] = "DEAD"
    meta = ok(call(server, "GET_FILE_METADATA", remote_path="/a.txt"))
    assert all(r["node_id"] != "n1" for b in meta["blocks"] for r in b["replicas"])


def test_stat_reports_replication_state(server, registry):
    put(server, "/a.txt", nodes=None)
    stat = ok(call(server, "STAT", remote_path="/a.txt"))
    assert stat["replication_state"] == "INTEGRO" and stat["blocks"] == 1
    holders = stat["replicas"][0]["nodes"]
    registry.nodes[holders[0]]["state"] = "DEAD"
    assert ok(call(server, "STAT", remote_path="/a.txt"))["replication_state"] == "SUBREPLICADO"
    registry.nodes[holders[1]]["state"] = "DEAD"
    assert ok(call(server, "STAT", remote_path="/a.txt"))["replication_state"] == "SEM_REPLICA"


def test_report_corruption_drops_the_replica(server):
    _, (block_id,) = put(server, "/a.txt")
    holder = ok(call(server, "GET_FILE_METADATA", remote_path="/a.txt"))["blocks"][0]["replicas"][0]["node_id"]
    assert ok(call(server, "REPORT_CORRUPTION", block_id=block_id, node_id=holder))["remaining_replicas"] == 1
    meta = ok(call(server, "GET_FILE_METADATA", remote_path="/a.txt"))
    assert holder not in [r["node_id"] for r in meta["blocks"][0]["replicas"]]
    err(call(server, "REPORT_CORRUPTION", block_id="nao-existe", node_id=holder), "NOT_FOUND")


def test_delete_file_queues_replicas_for_gc(server, db):
    put(server, "/a.txt", blocks=2)
    ok(call(server, "DELETE_FILE", remote_path="/a.txt"))
    err(call(server, "GET_FILE_METADATA", remote_path="/a.txt"), "NOT_FOUND")
    with db.tx() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM gc_blocks")
        assert cur.fetchone()["n"] == 4                       # 2 blocos x 2 réplicas
        cur.execute("SELECT COUNT(*) AS n FROM blocks")
        assert cur.fetchone()["n"] == 0
    err(call(server, "DELETE_FILE", remote_path="/a.txt"), "NOT_FOUND")


def test_mkdir_rmdir(server):
    assert ok(call(server, "MKDIR", remote_path="/a/b/c"))["created"] is True
    assert ok(call(server, "MKDIR", remote_path="/a/b/c"))["created"] is False
    err(call(server, "RMDIR", remote_path="/a"), "NOT_EMPTY")
    ok(call(server, "RMDIR", remote_path="/a/b/c"))
    err(call(server, "RMDIR", remote_path="/"), "BAD_PATH")
    put(server, "/f.txt")
    err(call(server, "RMDIR", remote_path="/f.txt"), "NOT_A_DIRECTORY")
    err(call(server, "MKDIR", remote_path="/f.txt/x"), "NOT_A_DIRECTORY")


def test_move_file_and_directory(server):
    put(server, "/a.txt")
    ok(call(server, "MKDIR", remote_path="/dest"))
    ok(call(server, "MOVE", src_path="/a.txt", dst_path="/dest/b.txt"))
    err(call(server, "GET_FILE_METADATA", remote_path="/a.txt"), "NOT_FOUND")
    assert ok(call(server, "GET_FILE_METADATA", remote_path="/dest/b.txt"))["blocks"]
    ok(call(server, "MOVE", src_path="/dest", dst_path="/renamed"))
    assert ok(call(server, "GET_FILE_METADATA", remote_path="/renamed/b.txt"))["blocks"]


def test_move_rejects_existing_destination_and_cycles(server):
    put(server, "/a.txt")
    put(server, "/b.txt")
    err(call(server, "MOVE", src_path="/a.txt", dst_path="/b.txt"), "EXISTS")
    ok(call(server, "MKDIR", remote_path="/d/sub"))
    err(call(server, "MOVE", src_path="/d", dst_path="/d/sub/x"), "BAD_PATH")
    err(call(server, "MOVE", src_path="/nada", dst_path="/x"), "NOT_FOUND")
    err(call(server, "MOVE", src_path="/a.txt", dst_path="/sem/pai/x"), "NOT_FOUND")


@pytest.mark.parametrize("path", ["sem-barra", "/a/../b", "/a/./b", "", None, 5])
def test_bad_paths_are_rejected(server, path):
    reply = call(server, "INIT_WRITE", remote_path=path, size_bytes=1, file_sha256="h")
    assert reply["status"] == "ERR" and reply["code"] in ("BAD_PATH", "BAD_REQUEST")


def test_missing_fields_are_rejected(server):
    err(call(server, "INIT_WRITE", remote_path="/a"), "BAD_REQUEST")
    err(call(server, "ALLOCATE_BLOCK", file_id="1"), "BAD_REQUEST")
    err(call(server, "COMMIT_BLOCK", file_id="1", block_id="b", node_ids=[]), "BAD_REQUEST")
    err(call(server, "ALLOCATE_BLOCK", file_id="abc", block_id="b", block_index=0, size_bytes=1, sha256="x"), "BAD_REQUEST")


def test_abort_write_frees_the_path_and_queues_gc(server, db):
    file_id = ok(call(server, "INIT_WRITE", remote_path="/a.txt", size_bytes=1, file_sha256="h"))["file_id"]
    alloc = ok(call(server, "ALLOCATE_BLOCK", file_id=file_id, block_id="b1", block_index=0, size_bytes=10, sha256="x"))
    err(call(server, "INIT_WRITE", remote_path="/a.txt", size_bytes=1, file_sha256="h"), "BUSY")
    ok(call(server, "ABORT_WRITE", file_id=file_id))
    ok(call(server, "INIT_WRITE", remote_path="/a.txt", size_bytes=1, file_sha256="h"))   # pode gravar de novo
    with db.tx() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM gc_blocks")
        assert cur.fetchone()["n"] == len(alloc["nodes"])      # réplicas alocadas vão para a limpeza
    err(call(server, "ABORT_WRITE", file_id=file_id), "NOT_FOUND")


def test_abort_is_refused_for_a_completed_file(server):
    file_id, _ = put(server, "/a.txt")
    err(call(server, "ABORT_WRITE", file_id=file_id), "NOT_WRITING")


def test_no_nodes_message_explains_disk_usage(server, registry):
    for node in registry.nodes.values():
        node["disks"] = [{"nome": "disk1", "capacity": 100 * GB, "used": 95 * GB}]   # discos 95% cheios
    file_id = ok(call(server, "INIT_WRITE", remote_path="/a", size_bytes=1, file_sha256="h"))["file_id"]
    reply = err(call(server, "ALLOCATE_BLOCK", file_id=file_id, block_id="b", block_index=0, size_bytes=10, sha256="x"),
                "NO_NODES")
    assert "3 vivo(s)" in reply["msg"] and "95%" in reply["msg"] and "limite 90%" in reply["msg"]


def test_usage_limit_can_be_changed_by_environment(db, registry, monkeypatch):
    from src.controller.handlers import Handlers
    monkeypatch.setenv("MAX_DISK_USAGE", "0.99")
    assert Handlers(db, registry, settings).max_usage == 0.99


class FakeReplicator:
    def status(self):
        return {"in_progress": [{"block_id": "b", "source": "n1", "target": "n3"}],
                "copied": 2, "failed": 1, "under_replicated_blocks": 3}


def test_status_reports_nodes_files_and_replication(server, registry):
    put(server, "/a.bin", blocks=2)

    reply = ok(call(server, "STATUS"))
    assert reply["replication_factor"] == 2
    assert set(reply["nodes"]) == {"n1", "n2", "n3"}
    assert sum(n["blocks"] for n in reply["nodes"].values()) == 4        
    disk = reply["nodes"]["n1"]["disks"][0]
    assert disk["usado"] == 10 * GB and disk["total"] == 100 * GB
    assert reply["files"] == {"INTEGRO": 1, "SUBREPLICADO": 0, "SEM_REPLICA": 0}
    assert reply["replication"] is None                                  


def test_status_counts_files_by_replication_state(server, registry):
    put(server, "/ok.bin", nodes=["n1", "n2"])
    put(server, "/pouco.bin", nodes=["n1", "n3"])
    put(server, "/nenhum.bin", nodes=["n2", "n3"])
    registry.nodes["n3"]["state"] = "DEAD"
    registry.nodes["n2"]["state"] = "DEAD"

    files = ok(call(server, "STATUS"))["files"]
    assert files == {"INTEGRO": 0, "SUBREPLICADO": 2, "SEM_REPLICA": 1}


def test_status_shows_draining_and_replicator(server, registry):
    registry.nodes["n2"]["draining"] = True
    server._handlers["STATUS"].__self__.replicator = FakeReplicator()  

    reply = ok(call(server, "STATUS"))
    assert reply["nodes"]["n2"]["draining"] is True
    assert reply["replication"]["copied"] == 2 and len(reply["replication"]["in_progress"]) == 1
