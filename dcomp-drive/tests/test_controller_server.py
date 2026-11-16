import json
import uuid

import pytest
import zmq

from src.controller.config import settings
from src.controller.controller_server import ControllerServer, RequestError


class FakeRegistry:
    def __init__(self):
        self.beats = []

    def heartbeat(self, node_id, addr=None, host=None, disks=None):
        self.beats.append(node_id)
        return node_id == "node1"

    def get_nodes(self):
        return {"node1": {"state": "ALIVE"}}


@pytest.fixture
def server():
    s = ControllerServer(settings, db=None, registry=FakeRegistry(),
                         bind_addr="tcp://127.0.0.1:0")
    s.start()
    yield s
    s.stop()


def rpc(server, payload, raw=None):
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.DEALER)
    sock.setsockopt(zmq.LINGER, 0)
    sock.setsockopt(zmq.RCVTIMEO, 3000)
    sock.connect(f"tcp://127.0.0.1:{server.bound_port}")
    try:
        body = raw if raw is not None else json.dumps(payload).encode()
        sock.send_multipart([b"", body])
        frames = sock.recv_multipart()
        assert len(frames) == 2 and frames[0] == b""
        return json.loads(frames[1])
    finally:
        sock.close()


def req(action, **kw):
    return {"req_id": str(uuid.uuid4()), "action": action, **kw}


def test_ping(server):
    r = rpc(server, req("PING"))
    assert r["status"] == "ACK" and r["pong"] is True


def test_reply_keeps_req_id(server):
    q = req("PING")
    assert rpc(server, q)["req_id"] == q["req_id"]


def test_unknown_action(server):
    r = rpc(server, req("XYZ"))
    assert r["status"] == "ERR" and r["code"] == "UNKNOWN_ACTION"


def test_client_actions_are_not_implemented_stubs(server):
    r = rpc(server, req("INIT_WRITE"))
    assert r["status"] == "ERR" and r["code"] == "NOT_IMPLEMENTED"


def test_invalid_json(server):
    r = rpc(server, None, raw=b"nao-e-json")
    assert r["code"] == "BAD_REQUEST"


def test_heartbeat_known_and_unknown_node(server):
    assert rpc(server, req("HEARTBEAT", node_id="node1"))["status"] == "ACK"
    r = rpc(server, req("HEARTBEAT", node_id="fantasma"))
    assert r["code"] == "UNKNOWN_NODE"
    assert rpc(server, req("HEARTBEAT"))["code"] == "BAD_REQUEST"


def test_status_lists_nodes(server):
    assert "node1" in rpc(server, req("STATUS"))["nodes"]


def test_register_custom_handler_and_request_error(server):
    server.register("ECHO", lambda r: {"eco": r["x"]})
    assert rpc(server, req("ECHO", x=7))["eco"] == 7

    def boom(r):
        raise RequestError("NOT_FOUND", "sumiu")
    server.register("BOOM", boom)
    r = rpc(server, req("BOOM"))
    assert r["code"] == "NOT_FOUND" and r["msg"] == "sumiu"


def test_handler_bug_does_not_kill_server(server):
    server.register("BUG", lambda r: 1 / 0)
    assert rpc(server, req("BUG"))["code"] == "INTERNAL"
    assert rpc(server, req("PING"))["status"] == "ACK"
