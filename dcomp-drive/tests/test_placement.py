import pytest

from src.controller.placement import NotEnoughReplicasError, choose_replicas
from src.controller.registry import StorageNode

GB = 1024 ** 3
BLOCK = 4 * 1024 * 1024  


def make_node(node_id, host=None, available_gb=50, total_gb=100, state="ALIVE", draining=False, disks=None):
    if disks is None:
        disks = [{"nome": "disk1", "capacity": total_gb * GB, "used": (total_gb - available_gb) * GB}]
    return StorageNode(node_id=node_id, disks=disks, last_seen=0.0, state=state,
                       addr=f"tcp://{node_id}:5555", host=host or node_id, draining=draining)


def ids(placements):
    return [p.node_id for p in placements]


def test_chooses_distinct_machines_with_most_free_space():
    nodes = [make_node("a", available_gb=20), make_node("b", available_gb=80), make_node("c", available_gb=40)]
    assert ids(choose_replicas(nodes, 2, BLOCK)) == ["b", "c"]


def test_two_nodes_on_same_machine_count_as_one():
    nodes = [make_node("a1", host="pc1", available_gb=90), make_node("a2", host="pc1", available_gb=80),
             make_node("b", host="pc2", available_gb=20)]
    chosen = choose_replicas(nodes, 2, BLOCK)
    assert sorted(p.host for p in chosen) == ["pc1", "pc2"]   
    assert "a1" in ids(chosen)                                  


@pytest.mark.parametrize("kwargs", [{"state": "DEAD"}, {"state": "SUSPECT"}, {"draining": True}])
def test_ignores_unavailable_nodes(kwargs):
    nodes = [make_node("bad", available_gb=99, **kwargs), make_node("ok", available_gb=20)]
    assert ids(choose_replicas(nodes, 2, BLOCK)) == ["ok"]


def test_disk_above_usage_limit_is_skipped():
    nodes = [make_node("full", available_gb=5), make_node("free", available_gb=20)]
    assert ids(choose_replicas(nodes, 2, BLOCK)) == ["free"]


def test_usage_limit_considers_the_block_itself():
    node = make_node("n", disks=[{"nome": "d1", "capacity": 100, "used": 85}])
    assert choose_replicas([node], 1, 10) == []
    assert ids(choose_replicas([node], 1, 5)) == ["n"]


def test_exclude_nodes_that_already_hold_the_block():
    nodes = [make_node("a", available_gb=90), make_node("b", available_gb=50), make_node("c", available_gb=20)]
    assert ids(choose_replicas(nodes, 2, BLOCK, exclude_nodes={"a"})) == ["b", "c"]


def test_falls_back_to_distinct_disks_when_machines_are_missing():
    two_disks = [{"nome": "d1", "capacity": 100 * GB, "used": 40 * GB},
                 {"nome": "d2", "capacity": 100 * GB, "used": 50 * GB}]
    chosen = choose_replicas([make_node("solo", disks=two_disks)], 2, BLOCK)
    assert [(p.node_id, p.disk) for p in chosen] == [("solo", "d1"), ("solo", "d2")]
    assert len({(p.node_id, p.disk) for p in chosen}) == 2   


def test_fallback_prefers_a_node_not_used_yet():
    nodes = [make_node("a", host="pc1", available_gb=90), make_node("b", host="pc1", available_gb=20)]
    assert ids(choose_replicas(nodes, 2, BLOCK)) == ["a", "b"]  


def test_not_enough_destinations_partial_or_error():
    nodes = [make_node("a")]
    assert ids(choose_replicas(nodes, 3, BLOCK)) == ["a"]        
    with pytest.raises(NotEnoughReplicasError) as err:
        choose_replicas(nodes, 3, BLOCK, allow_partial=False)    
    assert (err.value.found, err.value.wanted) == (1, 3)


def test_no_nodes_returns_empty_list():
    assert choose_replicas([], 2, BLOCK) == []


def test_result_is_deterministic_on_ties():
    nodes = [make_node("c"), make_node("a"), make_node("b")]
    assert ids(choose_replicas(nodes, 2, BLOCK)) == ["a", "b"]


def test_accepts_the_format_returned_by_registry_get_nodes():
    from dataclasses import asdict
    registry_view = {n.node_id: asdict(n) for n in (make_node("a", available_gb=80),
                                                    make_node("b", available_gb=40),
                                                    make_node("dead", available_gb=99, state="DEAD"))}
    assert ids(choose_replicas(registry_view, 2, BLOCK)) == ["a", "b"]
    assert ids(choose_replicas(list(registry_view.values()), 2, BLOCK)) == ["a", "b"]  

def test_plain_numbers_as_disks_are_ignored():
    node = make_node("n", disks=[100 * GB])
    assert choose_replicas([node], 1, BLOCK) == []


def test_node_without_disks_is_ignored():
    assert choose_replicas([make_node("n", disks=[])], 1, BLOCK) == []


@pytest.mark.parametrize("bad", [{"n": 0}, {"block_size": -1}, {"max_usage": 0}, {"max_usage": 1.5}])
def test_invalid_arguments(bad):
    args = {"n": 1, "block_size": BLOCK, "max_usage": 0.9, **bad}
    with pytest.raises(ValueError):
        choose_replicas([make_node("a")], **args)


def wrapper_disk(name, used_gb, capacity_gb=100):
    return {"nome": name, "capacity": capacity_gb * GB, "used": used_gb * GB}


def test_accepts_wrapper_disk_format():
    nodes = [make_node("a", disks=[wrapper_disk("disk1", used_gb=20)]),
             make_node("b", disks=[wrapper_disk("disk1", used_gb=80)])]
    chosen = choose_replicas(nodes, 2, BLOCK)
    assert ids(chosen) == ["a", "b"]
    assert chosen[0].disk == "disk1" and chosen[0].available == 80 * GB


def test_wrapper_format_respects_usage_limit():
    nodes = [make_node("full", disks=[wrapper_disk("disk1", used_gb=95)]),
             make_node("ok", disks=[wrapper_disk("disk1", used_gb=10)])]
    assert ids(choose_replicas(nodes, 2, BLOCK)) == ["ok"]


def test_disk_names_can_be_text_and_two_disks_are_never_repeated():
    node = make_node("n", disks=[wrapper_disk("ssd", 10), wrapper_disk("hdd", 30)])
    chosen = choose_replicas([node], 3, BLOCK)
    assert sorted(p.disk for p in chosen) == ["hdd", "ssd"]


def test_exclude_hosts_blocks_other_nodes_of_the_same_machine():
    nodes = [make_node("a1", host="pc1", available_gb=90), make_node("a2", host="pc1", available_gb=80),
             make_node("b", host="pc2", available_gb=20)]
    chosen = choose_replicas(nodes, 2, BLOCK, exclude_hosts={"pc1"})
    assert ids(chosen) == ["b"]


def test_accepts_the_wrapper_disco_dataclass_and_its_asdict():
    from dataclasses import asdict, dataclass

    @dataclass
    class Disco:  
        nome: str
        capacidade: int
        atual: int

    objetos = [Disco("d1", 100 * GB, 20 * GB)]
    como_dict = [asdict(d) for d in objetos]   
    for disks in (objetos, como_dict):
        chosen = choose_replicas([make_node("n", disks=disks)], 1, BLOCK)
        assert chosen[0].disk == "d1" and chosen[0].available == 80 * GB
