import os
import socket
import threading
import uuid

import pytest

from src.client.crypto import CryptoManager
from src.client.pipeline import DFSClient
from src.controller.config import settings
from src.controller.controller_server import ControllerServer
from src.controller.handlers import register_handlers
from src.storage import node as node_module
from src.storage.integrity_node import IntegrityNode

from tests.test_handlers import FakeRegistry, db  


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def cluster(db, tmp_path, monkeypatch):  
    monkeypatch.setattr(node_module.wrapperKazoo, "init", lambda: None)
    monkeypatch.setattr(node_module.wrapperKazoo, "register_storage", lambda *a, **k: None)

    nodes, threads = {}, []
    for name in ("n1", "n2", "n3"):
        n = IntegrityNode(name, "127.0.0.1", free_port(), str(tmp_path / name), scrub_interval=0)
        n.start()
        t = threading.Thread(target=n.serve_forever, daemon=True)
        t.start()
        nodes[name] = n
        threads.append(t)

    registry = FakeRegistry(ids=tuple(nodes))
    for name, n in nodes.items():
        registry.nodes[name]["addr"] = n.addr

    server = ControllerServer(settings, db, registry, bind_addr="tcp://127.0.0.1:0")
    register_handlers(server, db, registry, settings)
    server.start()
    client = DFSClient(f"tcp://127.0.0.1:{server.bound_port}", CryptoManager(None), chunk_size=1024)

    yield client, nodes, registry

    server.stop()
    for n in nodes.values():
        n.request_stop()
    for t in threads:
        t.join(timeout=3)
    for n in nodes.values():
        n.stop()


def make_file(tmp_path, size=3500):
    path = tmp_path / "entrada.bin"
    path.write_bytes(os.urandom(size))
    return path


def test_put_get_roundtrip_with_replication(cluster, tmp_path):
    client, nodes, _ = cluster
    src = make_file(tmp_path)
    result = client.put_file(str(src), "/docs/arquivo.bin")
    assert result["blocks_count"] == 4                       

    dst = tmp_path / "saida.bin"
    client.get_file("/docs/arquivo.bin", str(dst))
    assert dst.read_bytes() == src.read_bytes()

    # cada bloco existe em exatamente 2 dos 3 nós
    for block in client._rpc_controller("GET_FILE_METADATA", {"remote_path": "/docs/arquivo.bin"})["blocks"]:
        holders = [n for n in nodes.values() if (n.storage_path / block["block_id"]).exists()]
        assert len(holders) == 2


def test_ls_and_rm(cluster, tmp_path):
    client, _, _ = cluster
    client.put_file(str(make_file(tmp_path, 10)), "/a.txt")
    assert [e["name"] for e in client.list_dir("/")] == ["a.txt"]
    client.delete_file("/a.txt")
    assert client.list_dir("/") == []


def test_get_survives_a_dead_node(cluster, tmp_path):
    client, nodes, registry = cluster
    src = make_file(tmp_path)
    client.put_file(str(src), "/f.bin")
    registry.nodes["n1"]["state"] = "DEAD"
    dst = tmp_path / "out.bin"
    client.get_file("/f.bin", str(dst))
    assert dst.read_bytes() == src.read_bytes()


def test_corrupted_replica_is_skipped_and_reported(cluster, tmp_path):
    client, nodes, _ = cluster
    src = make_file(tmp_path, 500)
    client.put_file(str(src), "/f.bin")
    meta = client._rpc_controller("GET_FILE_METADATA", {"remote_path": "/f.bin"})
    block = meta["blocks"][0]
    first = block["replicas"][0]["node_id"]
    (nodes[first].storage_path / block["block_id"]).write_bytes(b"adulterado")   # corrompe a 1ª réplica

    dst = tmp_path / "out.bin"
    client.get_file("/f.bin", str(dst))                      # lê da outra réplica
    assert dst.read_bytes() == src.read_bytes()
    after = client._rpc_controller("GET_FILE_METADATA", {"remote_path": "/f.bin"})["blocks"][0]["replicas"]
    assert first not in [r["node_id"] for r in after]       # o controller descartou a réplica corrompida


def test_failed_put_is_aborted_and_can_be_retried(cluster, tmp_path):
    client, nodes, registry = cluster
    saved = dict(registry.nodes)
    registry.nodes.clear()                                    # sem nós: o ALLOCATE_BLOCK falha
    src = make_file(tmp_path, 500)
    with pytest.raises(RuntimeError):
        client.put_file(str(src), "/f.bin")
    registry.nodes.update(saved)                              # nós de volta
    client.put_file(str(src), "/f.bin")                       # sem BUSY: a escrita falha foi desfeita
    dst = tmp_path / "out.bin"
    client.get_file("/f.bin", str(dst))
    assert dst.read_bytes() == src.read_bytes()