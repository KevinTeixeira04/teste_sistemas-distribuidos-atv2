""" Descoberta e monitoramento dos nós de armazenamento """

import json
import logging
import threading
import time
from dataclasses import asdict, dataclass
from typing import Callable

from src.wrapper import wrapperKazoo


@dataclass
class StorageNode:
    node_id: str
    disks: list
    last_seen: float
    state: str = "ALIVE"
    addr: str | None = None
    host: str | None = None
    draining: bool = False
    in_zk: bool = True   # False quando o znode efêmero sumiu

class NodeRegistry:
    def __init__(
        self,
        cfg,
        db,
        on_change: Callable | None = None,
    ):
        self.cfg = cfg
        self.db = db
        self.on_change = on_change

        self._nodes: dict[str, StorageNode] = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._watcher = None
        self._monitor_thread = None
        self._closed = False

    def start(self):
        """Inicia a descoberta e o monitoramento dos nós."""

        if self._closed:
            raise RuntimeError("Um registro encerrado não pode ser reiniciado.")


        if self._monitor_thread and self._monitor_thread.is_alive():
            return

        wrapperKazoo.init()
        self._stop_event.clear()

        if self._watcher is None:
            self._watcher = wrapperKazoo.set_storage_watch(
                remove_func=self._remove_node,
                add_func=self._add_node,
            )

        self._monitor_thread = threading.Thread(
            target=self._monitor,
            name="storage-node-monitor",
            daemon=True,
        )
        self._monitor_thread.start()

        logging.info("Monitoramento dos nós iniciado")

    def stop(self):
        """Encerra definitivamente o monitoramento."""
        if self._closed:
            return

        self._closed = True
        self._stop_event.set()

        if self._monitor_thread:
            self._monitor_thread.join(timeout=2)
            self._monitor_thread = None

        logging.info("Monitoramento dos nós encerrado")

    def _load_from_zk(self, node_id: str) -> bool:
        """
        Carrega um nó do ZooKeeper para a memória. Idempotente: pode ser chamado
        várias vezes para o mesmo nó. Retorna True se o nó existe e foi carregado.
        """
        if self._stop_event.is_set():
            return False

        result = wrapperKazoo.get_node(f"/storage/nodes/{node_id}")
        if result is None:
            return False

        data, _ = result

        try:
            registration = json.loads(data.decode("utf-8"))

            if isinstance(registration, dict):
                disks = registration.get("disks")
                addr = registration.get("addr")
                host = registration.get("host")
            elif isinstance(registration, list):
                # Mantém compatibilidade com registros antigos.
                disks = registration
                addr = None
                host = None
            else:
                raise ValueError("Formato de registro inválido")

            # O znode pode trazer só o endereço; os discos chegam depois pelo heartbeat.
            if disks is None:
                disks = []
            if not isinstance(disks, list):
                raise ValueError("A lista de discos é inválida")

        except (AttributeError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
            logging.exception("Dados inválidos para o nó %s", node_id)
            return False

        try:
            draining = self.is_draining(node_id)
        except Exception:
            # falha no banco não pode impedir a descoberta do nó
            logging.exception("Não foi possível ler o estado de drain de %s", node_id)
            draining = False

        with self._lock:
            previous = self._nodes.get(node_id)
            was_alive = previous is not None and previous.state == "ALIVE"

            self._nodes[node_id] = StorageNode(
                node_id=node_id,
                disks=disks or (previous.disks if previous else []),
                last_seen=time.monotonic(),  # período de graça antes de suspeitar
                state="ALIVE",
                addr=addr or (previous.addr if previous else None),
                host=host or (previous.host if previous else None),
                draining=draining,
            )

        # só notifica se for novo ou estava DEAD/SUSPECT
        if not was_alive:
            self._notify(node_id, "ALIVE")
            logging.info("Nó %s descoberto", node_id)
        return True

    def _add_node(self, node_id: str):
        # Chamado pelo watch do kazoo. Nunca pode lançar: uma exceção aqui
        # pode derrubar o watch inteiro e o controlador deixa de ver nós.
        try:
            self._load_from_zk(node_id)
        except Exception:
            logging.exception("Erro ao adicionar o nó %s", node_id)

    def _remove_node(self, node_id: str):
        if self._stop_event.is_set():
            return

        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                return

            node.state = "DEAD"
            node.in_zk = False

        self._notify(node_id, "DEAD")
        logging.warning("Nó %s removido do ZooKeeper", node_id)

    def heartbeat(
        self,
        node_id: str,
        addr: str | None = None,
        host: str | None = None,
        disks: list | None = None,
    ):
        """Atualiza a última comunicação recebida de um nó."""
        with self._lock:
            known = node_id in self._nodes

        if not known and not self._load_from_zk(node_id):
            return False

        with self._lock:
            node = self._nodes.get(node_id)

            if node is None or not node.in_zk:
                return False

            previous_state = node.state
            node.last_seen = time.monotonic()
            node.state = "ALIVE"

            if addr is not None:
                node.addr = addr

            if host is not None:
                node.host = host

            if disks is not None:
                node.disks = disks

        if previous_state != "ALIVE":
            self._notify(node_id, "ALIVE")

        return True

    def _monitor(self):
        while not self._stop_event.wait(self.cfg.membership_interval_s):
            now = time.monotonic()

            with self._lock:
                node_ids = list(self._nodes)

            for node_id in node_ids:
                with self._lock:
                    node = self._nodes.get(node_id)
                    if node is None:
                        continue

                    elapsed = now - node.last_seen

                    if not node.in_zk:
                        new_state = "DEAD"   
                    elif elapsed >= self.cfg.dead_after_s:
                        new_state = "DEAD"
                    elif elapsed >= self.cfg.suspect_after_s:
                        new_state = "SUSPECT"
                    else:
                        new_state = "ALIVE"

                    if node.state == new_state:
                        continue

                    node.state = new_state

                self._notify(node_id, new_state)
                logging.warning(
                    "Estado do nó %s alterado para %s",
                    node_id,
                    new_state,
                )

    def get_node(self, node_id: str):
        with self._lock:
            node = self._nodes.get(node_id)
            return asdict(node) if node else None

    def get_nodes(self):
        with self._lock:
            return {
                node_id: asdict(node)
                for node_id, node in self._nodes.items()
            }

    def is_draining(self, node_id: str) -> bool:
        with self.db.tx() as cur:
            cur.execute(
                "SELECT draining FROM maquina WHERE nome = %s",
                (node_id,),
            )
            result = cur.fetchone()

        return bool(result["draining"]) if result else False

    def _field(node, name: str):
        return node.get(name) if isinstance(node, dict) else getattr(node, name, None)

    def set_draining(self, nome_maquina: str):
        """
        Começa o drain de uma maquina
        """
        with wrapperKazoo.lock_drain(): #Só teremos o drain de uma máquina por vez
            logging.info(f"lock_drain adquirido por {nome_maquina}, iniciando o drain...")
            with self.db.tx() as cur:
                cur.execute(
                    """
                    INSERT INTO maquina (nome, draining)
                    VALUES (%s, %s)
                    ON CONFLICT (nome)
                    DO UPDATE SET draining = EXCLUDED.draining
                    """,
                    (nome_maquina, True),
                )
            alive = {
                        node_id: node for node_id, node in self.registry.get_nodes().items()
                        if node.get("state") == "ALIVE" and node.get("addr")
                    }
            # Até o drain acabar ...
            while True:
                #if time.monotonic() - inicio > timeout:
                #    raise TimeoutError(f"drain de {nome_maquina} excedeu {timeout}s")

                with self.db.tx() as cur:
                    cur.execute("""
                        SELECT COUNT(*) FROM (
                            SELECT b.block_id
                            FROM blocks b
                            JOIN inodes i ON i.id = b.inode_id AND i.state = 'COMPLETO'
                            JOIN replicas r_d ON r_d.block_id = b.block_id
                                            AND r_d.maquina = %(m)s
                            LEFT JOIN replicas r2 ON
                                r2.block_id = b.block_id
                                AND r2.state = 'OK'
                                AND r2.maquina != %(m)s
                                AND r2.maquina = ANY(%(alive)s::text[])
                                AND r2.maquina IN (
                                    SELECT nome FROM maquina WHERE draining = FALSE
                                )
                            GROUP BY b.block_id
                            HAVING COUNT(r2.block_id) < %(target)s
                        ) sub
                    """, {
                        "m": nome_maquina,
                        "alive": alive,
                        "target": self.cfg.replication_factor,
                    })
                    pendentes = cur.fetchone()[0]

                if pendentes == 0:
                    logging.info(f"drain de {nome_maquina}: todos os blocos re-replicados")
                    break

            logging.info(f"drain de {nome_maquina}: {pendentes} blocos pendentes, aguardando...")
            #time.sleep(poll_interval)
            logging.info(f"drain em {nome_maquina} finalizado.")
            
            

            


    def _notify(self, node_id: str, state: str):
        if self.on_change:
            try:
                self.on_change(node_id, state)
            except Exception:
                logging.exception(
                    "Erro ao notificar mudança de estado do nó %s",
                    node_id,
                )
