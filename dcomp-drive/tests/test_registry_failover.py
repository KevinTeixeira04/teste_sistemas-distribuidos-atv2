"""
Troca de líder: um registry novo (memória vazia) precisa enxergar os nós que já existiam.

Usa um ZooKeeper simulado que imita o kazoo: ao armar o ChildrenWatch, a função é
chamada uma vez com os filhos que já existem. O registry só usa o que o wrapper
já oferece: init, get_node e set_storage_watch(remove_func, add_func).
"""
import json
import time
import types

import pytest

from src.controller import registry as reg_mod
from src.controller.registry import NodeRegistry


class FakeZK:
    def __init__(self):
        self.nodes = {}
        self.watchers = []

    def register(self, name, data=None):
        # formato que o wrapper grava hoje: só o endereço
        self.nodes[name] = json.dumps(data if data is not None else {"addr": f"tcp://{name}:1"}).encode()
        self._fire()

    def unregister(self, name):
        self.nodes.pop(name, None)
        self._fire()

    def _fire(self):
        for w in list(self.watchers):
            w(list(self.nodes))

    # --- API do wrapper usada pelo registry ---
    def init(self):
        pass

    def get_node(self, path):
        name = path.rsplit("/", 1)[1]
        return (self.nodes[name], None) if name in self.nodes else None

    def set_storage_watch(self, remove_func=None, add_func=None):
        previous = set()

        def w(children):
            nonlocal previous
            cur = set(children)
            for n in previous - cur:
                remove_func(n)
            for n in cur - previous:
                add_func(n)
            previous = cur

        self.watchers.append(w)
        w(list(self.nodes))  # o kazoo chama uma vez ao armar o watch
        return w


@pytest.fixture
def zk(monkeypatch):
    z = FakeZK()
    for fn in ("init", "get_node", "set_storage_watch"):
        monkeypatch.setattr(reg_mod.wrapperKazoo, fn, getattr(z, fn))
    monkeypatch.setattr(NodeRegistry, "is_draining", lambda self, node_id: False)
    return z


def cfg():
    return types.SimpleNamespace(membership_interval_s=60, suspect_after_s=10, dead_after_s=30)


def test_new_leader_sees_existing_nodes(zk):
    zk.register("node1")
    zk.register("node2")

    leader2 = NodeRegistry(cfg(), db=None)  # "novo líder": memória vazia
    leader2.start()
    try:
        nodes = leader2.get_nodes()
        assert set(nodes) == {"node1", "node2"}
        assert nodes["node1"]["state"] == "ALIVE"
        assert nodes["node1"]["addr"] == "tcp://node1:1"
    finally:
        leader2.stop()


def test_znode_without_disks_is_accepted(zk):
    """O wrapper atual só grava o addr; antes isso rejeitava o nó como inválido."""
    zk.register("node1", {"addr": "tcp://node1:1"})
    r = NodeRegistry(cfg(), db=None)
    r.start()
    try:
        assert r.get_nodes()["node1"]["disks"] == []
        # os discos chegam pelo heartbeat
        r.heartbeat("node1", disks=[{"nome": "node1-disk0", "capacity": 10, "used": 1}])
        assert r.get_nodes()["node1"]["disks"][0]["nome"] == "node1-disk0"
    finally:
        r.stop()


def test_existing_nodes_notified_once(zk):
    zk.register("node1")
    events = []
    r = NodeRegistry(cfg(), db=None, on_change=lambda n, s: events.append((n, s)))
    r.start()
    try:
        assert events == [("node1", "ALIVE")]
    finally:
        r.stop()


def test_changes_after_start_still_tracked(zk):
    zk.register("node1")
    r = NodeRegistry(cfg(), db=None)
    r.start()
    try:
        zk.register("node3")
        assert "node3" in r.get_nodes()
        zk.unregister("node1")
        assert r.get_nodes()["node1"]["state"] == "DEAD"
    finally:
        r.stop()


def test_heartbeat_from_node_missed_by_leader(zk):
    r = NodeRegistry(cfg(), db=None)  # nunca iniciado: não conhece ninguém
    zk.register("node1")
    assert r.heartbeat("node1") is True
    assert "node1" in r.get_nodes()
    assert r.heartbeat("fantasma") is False


def test_db_failure_does_not_block_discovery(zk, monkeypatch):
    def boom(self, node_id):
        raise RuntimeError("banco fora do ar")

    monkeypatch.setattr(NodeRegistry, "is_draining", boom)
    zk.register("node1")
    r = NodeRegistry(cfg(), db=None)
    r.start()
    try:
        assert "node1" in r.get_nodes()
    finally:
        r.stop()


def test_invalid_znode_does_not_break_other_nodes(zk):
    zk.nodes["lixo"] = b"nao e json"
    zk.register("node1")
    r = NodeRegistry(cfg(), db=None)
    r.start()
    try:
        assert set(r.get_nodes()) == {"node1"}
    finally:
        r.stop()


def test_stopped_registry_ignores_events(zk):
    zk.register("node1")
    old = NodeRegistry(cfg(), db=None)
    old.start()
    old.stop()  # perdeu a liderança
    zk.register("node2")
    assert "node2" not in old.get_nodes()


def test_node_removed_from_zk_stays_dead_even_without_heartbeats(zk):
    """Com os limites de tempo altos (docker-compose) o monitor não pode ressuscitar um nó que saiu do ZooKeeper."""
    zk.register("node1")
    config = types.SimpleNamespace(membership_interval_s=0.05, suspect_after_s=86400, dead_after_s=172800)
    r = NodeRegistry(config, db=None)
    r.start()
    try:
        zk.unregister("node1")
        time.sleep(0.4)                                   # vários ciclos do monitor
        assert r.get_nodes()["node1"]["state"] == "DEAD"
        assert r.heartbeat("node1") is False              # sem znode, um heartbeat atrasado também não o reanima
        zk.register("node1")                              # o nó voltou: aí sim fica vivo de novo
        assert r.get_nodes()["node1"]["state"] == "ALIVE"
    finally:
        r.stop()
