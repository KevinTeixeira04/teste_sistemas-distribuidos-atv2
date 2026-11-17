"""
Escolha de onde gravar as réplicas de um bloco 

Dentre as regras temos:
  1. só entram nós ALIVE e fora de drain 
  2. só entram discos que continuam abaixo de max_usage depois de receber o bloco (cumpre o R7)
  3. nível 1: no máximo uma réplica por máquina, preferindo a com mais espaço livre (cumpre R3)
  4. nível 2, se faltarem máquinas: completa com discos diferentes 
  5. se ainda faltar, devolve o que foi encontrado ou levanta erro 
"""
from dataclasses import dataclass


class NotEnoughReplicasError(Exception):
    def __init__(self, found: int, wanted: int):
        super().__init__(f"Só há {found} destino(s) disponível(is) para {wanted} réplica(s).")
        self.found = found
        self.wanted = wanted


@dataclass(frozen=True)
class Placement:
    node_id: str
    disk: int | str  
    host: str        
    available: int   


def _rank(p: Placement) -> tuple:
    return (-p.available, p.node_id, p.disk)


def _field(node, name: str):
    return node.get(name) if isinstance(node, dict) else getattr(node, name, None)


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _normalize_disks(disks) -> list[tuple]:
    result = []
    for disk in disks or []:
        get = disk.get if isinstance(disk, dict) else (lambda key, d=disk: getattr(d, key, None))
        name = get("nome")
        total = get("capacity") if get("capacity") is not None else get("capacidade")
        used = get("used") if get("used") is not None else get("atual")
        if name is not None and _is_number(total) and _is_number(used):
            result.append((name, total, total - used))
    return result


def _candidates(nodes, block_size: int, max_usage: float, exclude: set, exclude_hosts: set) -> list[Placement]:
    #Todos os pares (nó, disco) que podem receber o bloco
    found = []
    for node in nodes:
        node_id = _field(node, "node_id")
        if _field(node, "state") != "ALIVE" or _field(node, "draining") or node_id in exclude:
            continue
        host = _field(node, "host") or node_id  # sem host informado, cada nó conta como uma máquina
        if host in exclude_hosts:
            continue

        for index, total, available in _normalize_disks(_field(node, "disks")):
            if total <= 0 or available < block_size:
                continue
            if (total - available + block_size) / total > max_usage:
                continue
            found.append(Placement(node_id, index, host, int(available)))
    return found


def choose_replicas(
    nodes,
    n: int, #quantas réplicas queremos
    block_size: int,
    max_usage: float = 0.90, #fração máxima de uso do disco depois da gravação (0.90 = 90%)
    exclude_nodes=(), #id de nós que não podem ser escolhidos 
    allow_partial: bool = True,
    exclude_hosts=(), #máquinas que não podem ser escolhidas
) -> list[Placement]:
  
    if n < 1:
        raise ValueError("n deve ser pelo menos 1")
    if block_size < 0:
        raise ValueError("block_size não pode ser negativo")
    if not 0 < max_usage <= 1:
        raise ValueError("max_usage deve estar em (0, 1]")

    if isinstance(nodes, dict):  
        nodes = list(nodes.values())

    candidates = _candidates(nodes, block_size, max_usage, set(exclude_nodes), set(exclude_hosts))

    # nível 1
    best_per_host: dict[str, Placement] = {}
    for c in candidates:
        current = best_per_host.get(c.host)
        if current is None or _rank(c) < _rank(current):
            best_per_host[c.host] = c
    chosen = sorted(best_per_host.values(), key=_rank)[:n]

    # nível 2
    used_pairs = {(c.node_id, c.disk) for c in chosen}
    used_nodes = {c.node_id for c in chosen}
    while len(chosen) < n:
        rest = [c for c in candidates if (c.node_id, c.disk) not in used_pairs]
        if not rest:
            break
        pick = min(rest, key=lambda c: (c.node_id in used_nodes,) + _rank(c))
        chosen.append(pick)
        used_pairs.add((pick.node_id, pick.disk))
        used_nodes.add(pick.node_id)

    if len(chosen) < n and not allow_partial:
        raise NotEnoughReplicasError(len(chosen), n)
    return chosen