import hashlib
import json
import socket
import threading
import uuid

import pytest
import zmq

from src.client.crypto import CryptoManager
from src.client.pipeline import DFSClient
from src.storage import node as node_module
from src.storage.integrity_node import IntegrityNode


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def node(tmp_path, monkeypatch):
    monkeypatch.setattr(node_module.wrapperKazoo, "init", lambda: None)
    monkeypatch.setattr(node_module.wrapperKazoo, "register_storage", lambda *a, **k: None)

    n = IntegrityNode("t1", "127.0.0.1", free_port(), str(tmp_path / "blocks"), scrub_interval=0)
    n.start()
    t = threading.Thread(target=n.serve_forever, daemon=True)
    t.start()
    yield n
    n.request_stop()
    t.join(timeout=3)
    n.stop()


@pytest.fixture
def client():
    return DFSClient("tcp://127.0.0.1:1", CryptoManager(None))


def test_roundtrip_writes_meta_next_to_block(node, client):
    bid, data = str(uuid.uuid4()), b"conteudo" * 100
    client._send_block_to_storage(node.addr, bid, sha(data), data)
    assert client._fetch_block_from_storage(node.addr, bid) == data
    assert (node.storage_path / f"{bid}.meta").exists()


def test_read_of_corrupted_block_is_refused_and_quarantined(node, client):
    bid, data = str(uuid.uuid4()), b"A" * 500
    client._send_block_to_storage(node.addr, bid, sha(data), data)
    (node.storage_path / bid).write_bytes(b"bit rot")  
    
    with pytest.raises(RuntimeError):
        client._fetch_block_from_storage(node.addr, bid)
    assert not (node.storage_path / bid).exists()
    assert any(p.name.startswith(bid) for p in node.corrupt_path.iterdir())


def test_scrub_finds_only_the_corrupted_block(node, client):
    good, bad = str(uuid.uuid4()), str(uuid.uuid4())
    for bid, data in ((good, b"bom"), (bad, b"ruim")):
        client._send_block_to_storage(node.addr, bid, sha(data), data)
    (node.storage_path / bad).write_bytes(b"adulterado")

    checked, corrupt = node.scrub_once()
    assert checked == 2 and corrupt == [bad]
    assert client._fetch_block_from_storage(node.addr, good) == b"bom"


def test_wrong_hash_leaves_no_meta_behind(node, client):
    bid = str(uuid.uuid4())
    with pytest.raises(RuntimeError):
        client._send_block_to_storage(node.addr, bid, sha(b"a"), b"b")
    assert not (node.storage_path / f"{bid}.meta").exists()


def test_cleanup_removes_interrupted_writes(node):
    leftover = node.storage_path / ".abc.xyz.tmp"
    leftover.write_bytes(b"pela metade")
    node._cleanup_tmp()
    assert not leftover.exists()


@pytest.fixture
def two_nodes(tmp_path, monkeypatch):
    monkeypatch.setattr(node_module.wrapperKazoo, "init", lambda: None)
    monkeypatch.setattr(node_module.wrapperKazoo, "register_storage", lambda *a, **k: None)

    nodes = []
    threads = []

    for node_id, directory in (("source", "source"), ("destination", "destination")):
        storage = IntegrityNode(
            node_id,
            "127.0.0.1",
            free_port(),
            str(tmp_path / directory),
            scrub_interval=0,
        )
        storage.start()

        thread = threading.Thread(target=storage.serve_forever, daemon=True)
        thread.start()

        nodes.append(storage)
        threads.append(thread)

    yield nodes[0], nodes[1]

    for storage in nodes:
        storage.request_stop()

    for thread in threads:
        thread.join(timeout=3)

    for storage in nodes:
        storage.stop()


def request_copy_block(destination, source, block_id):
    context = zmq.Context()
    sock = context.socket(zmq.DEALER)

    try:
        sock.setsockopt(zmq.IDENTITY, str(uuid.uuid4()).encode("utf-8"))
        sock.setsockopt(zmq.RCVTIMEO, 5000)
        sock.setsockopt(zmq.SNDTIMEO, 5000)
        sock.setsockopt(zmq.LINGER, 0)

        request = {
            "req_id": str(uuid.uuid4()),
            "action": "COPY_BLOCK",
            "block_id": block_id,
            "source_addr": source.addr,
        }

        sock.connect(destination.addr)
        sock.send_multipart([
            b"",
            json.dumps(request).encode("utf-8"),
        ])

        frames = sock.recv_multipart()
        assert len(frames) == 2

        return json.loads(frames[1].decode("utf-8"))

    finally:
        sock.close()
        context.term()


def test_copy_block_between_nodes(two_nodes, client):
    source, destination = two_nodes
    block_id = str(uuid.uuid4())
    data = b"conteudo transferido entre dois storages"

    client._send_block_to_storage(source.addr, block_id, sha(data), data)

    response = request_copy_block(destination, source, block_id)

    assert response["status"] == "ACK"
    assert response["size_bytes"] == len(data)
    assert destination._get_block({"block_id": block_id}) == data
    assert (destination.storage_path / f"{block_id}.meta").exists()


def test_copy_block_fails_when_source_block_does_not_exist(two_nodes):
    source, destination = two_nodes
    block_id = str(uuid.uuid4())

    response = request_copy_block(destination, source, block_id)

    assert response["status"] != "ACK"
    assert not (destination.storage_path / block_id).exists()
    assert not (destination.storage_path / f"{block_id}.meta").exists()


def test_copy_block_rejects_corrupted_source(two_nodes, client):
    source, destination = two_nodes
    block_id = str(uuid.uuid4())
    data = b"conteudo original do bloco"

    client._send_block_to_storage(source.addr, block_id, sha(data), data)

    (source.storage_path / block_id).write_bytes(b"conteudo adulterado")

    response = request_copy_block(destination, source, block_id)

    assert response["status"] != "ACK"
    assert not (destination.storage_path / block_id).exists()
    assert not (destination.storage_path / f"{block_id}.meta").exists()
