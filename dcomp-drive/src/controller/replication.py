"""Re-replicação: responsável por restaurar o fator de replicação quando um nó cai ou uma réplica corrompe.

Ideia aplicada: Roda só no controlador líder. A cada rodada acha os blocos com menos réplicas OK em nós vivos
que o fator, começando pelos mais urgentes, e manda COPY_BLOCK ao nó destino, que busca o bloco
direto na origem
"""

import json
import logging
import os
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import zmq

from src.controller import metadata as md
from src.controller import placement

DEFAULT_INTERVAL_S = 5.0
DEFAULT_MAX_CONCURRENT = 2       # blocos copiados ao mesmo tempo
DEFAULT_COPY_TIMEOUT_MS = 60000  
RETRY_COOLDOWN_S = 30.0          # espera antes de tentar de novo um bloco que falhou


class Replicator:
    def __init__(self, db, registry, cfg, send_copy=None):
        self.db = db
        self.registry = registry
        self.target = cfg.replication_factor
        self.max_usage = getattr(cfg, "max_disk_usage", float(os.getenv("MAX_DISK_USAGE", 0.90)))
        self.interval = float(os.getenv("REPLICATION_INTERVAL_S", DEFAULT_INTERVAL_S))
        self.max_concurrent = int(os.getenv("REPLICATION_MAX_CONCURRENT", DEFAULT_MAX_CONCURRENT))
        self.copy_timeout_ms = int(os.getenv("COPY_TIMEOUT_MS", DEFAULT_COPY_TIMEOUT_MS))

        self._ctx = zmq.Context()
        self._send_copy = send_copy or self._zmq_copy  
        self._pool = ThreadPoolExecutor(max_workers=self.max_concurrent, thread_name_prefix="repl")
        self._stop_event = threading.Event()
        self._thread = None

        self._lock = threading.Lock()
        self._active: dict[str, dict] = {}
        self._cooldown: dict[str, float] = {}
        self._warned: set = set()
        self.copied = 0
        self.failed = 0
        self.pending_blocks = 0

    def start(self):
        with self.db.tx() as cur:
            stale = md.clear_stale_pending(cur)   # cópias interrompidas por uma queda do líder anterior
        if stale:
            logging.warning("evento=PENDENTES_ANTIGAS_REMOVIDAS quantidade=%d", stale)
        self._thread = threading.Thread(target=self._loop, name="replicator", daemon=True)
        self._thread.start()
        logging.info("evento=REPLICADOR_INICIADO fator=%d concorrencia=%d", self.target, self.max_concurrent)

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=3)
            self._thread = None
        self._pool.shutdown(wait=False, cancel_futures=True)

    def status(self) -> dict:
        with self._lock:
            return {
                "in_progress": list(self._active.values()),
                "copied": self.copied,
                "failed": self.failed,
                "under_replicated_blocks": self.pending_blocks,
            }

    def _loop(self):
        while not self._stop_event.is_set():
            copied = 0
            try:
                copied = self.run_once()
            except Exception:
                logging.exception("evento=REPLICADOR_ERRO")
            if not copied:   # se copiou algo, olha de novo já, pq pode haver mais blocos esperando
                self._stop_event.wait(self.interval)

    def run_once(self) -> int:
        """Uma rodada. Devolve quantas réplicas foram copiadas."""
        plans = self._plan()
        if not plans:
            return 0
        futures = [self._pool.submit(self._repair, plan) for plan in plans]
        return sum(f.result() for f in futures)

    def _plan(self) -> list[dict]:
        alive = {
            node_id: node for node_id, node in self.registry.get_nodes().items()
            if node.get("state") == "ALIVE" and node.get("addr")
        }
        with self.db.tx() as cur:
            rows = md.under_replicated_blocks(cur, list(alive), self.target)
        with self._lock:
            self.pending_blocks = len(rows)

        now = time.monotonic()
        plans = []
        for row in rows:
            if len(plans) >= self.max_concurrent:
                break
            block_id = row["block_id"]
            if self._cooldown.get(block_id, 0) > now:
                continue
            if row["ok_alive"] == 0:
                self._warn_once(("sem_origem", block_id),
                                "evento=BLOCO_SEM_REPLICA_VIVA bloco=%s", block_id)
                continue

            missing = self.target - row["ok_alive"]
            targets = self._choose_targets(alive, row, missing)
            if not targets:
                self._warn_once(("sem_destino", block_id),
                                "evento=SEM_DESTINO bloco=%s faltam=%d", block_id, missing)
                continue
            self._warned.discard(("sem_destino", block_id))
            plans.append({
                "block_id": block_id, "size": row["size_bytes"],
                "source": alive[random.choice(row["sources"])],
                "targets": [(t.node_id, t.disk, alive[t.node_id]["addr"]) for t in targets],
                "ok_alive": row["ok_alive"],
            })
        return plans

    def _choose_targets(self, alive: dict, row: dict, missing: int):
        """Usa o placement da escrita, sem repetir nó que já tem (ou está recebendo) o bloco."""
        holders = set(row["holders"])
        source_hosts = {alive[n].get("host") for n in row["sources"] if alive[n].get("host")}
        for exclude_hosts in (source_hosts, ()):   # prefere outra máquina; sem opção, aceita qualquer nó
            chosen = placement.choose_replicas(
                alive, missing, row["size_bytes"], max_usage=self.max_usage,
                exclude_nodes=holders, allow_partial=True, exclude_hosts=exclude_hosts,
            )
            if chosen:
                return chosen
        return []

    def _repair(self, plan: dict) -> int:
        block_id = plan["block_id"]
        copied = 0
        for node_id, disk, addr in plan["targets"]:
            started = time.monotonic()
            with self._lock:
                self._active[block_id] = {"block_id": block_id, "source": plan["source"]["node_id"],
                                          "target": node_id, "started": time.time()}
            try:
                if not self._copy_one(plan, node_id, disk, addr):
                    self._cooldown[block_id] = time.monotonic() + RETRY_COOLDOWN_S
                    break
                copied += 1
                logging.info("evento=REREPLICACAO_OK bloco=%s origem=%s destino=%s bytes=%d ms=%.0f",
                             block_id, plan["source"]["node_id"], node_id, plan["size"],
                             (time.monotonic() - started) * 1000)
            finally:
                with self._lock:
                    self._active.pop(block_id, None)
        if copied and copied >= self.target - plan["ok_alive"]:
            logging.info("evento=NIVEL_RESTAURADO bloco=%s replicas=%d", block_id, self.target)
        return copied

    def _copy_one(self, plan: dict, node_id: str, disk, addr: str) -> bool:
        block_id = plan["block_id"]
        try:
            with self.db.tx() as cur:
                md.start_repair(cur, block_id, node_id, disk)
        except Exception:
            logging.warning("evento=REREPLICACAO_CANCELADA bloco=%s (apagado durante o planejamento?)", block_id)
            return False

        try:
            reply = self._send_copy(addr, {
                "req_id": str(uuid.uuid4()), "action": "COPY_BLOCK",
                "block_id": block_id, "source_addr": plan["source"]["addr"],
            })
        except Exception as error:
            return self._fail(block_id, node_id, disk, f"sem resposta de {node_id}: {error}")

        if reply.get("status") != "ACK":
            return self._fail(block_id, node_id, disk, f"{node_id} recusou a cópia: {reply.get('code')} {reply.get('msg', '')}")
        if reply.get("size_bytes") != plan["size"]:
            # o nó pode ter gravado algo, então a cópia vai para a coleta de lixo
            return self._fail(block_id, node_id, disk,
                              f"tamanho divergente em {node_id}: {reply.get('size_bytes')} != {plan['size']}",
                              leftover=True)

        with self.db.tx() as cur:
            registered = md.finish_repair(cur, block_id, node_id, disk)
        with self._lock:
            self.copied += 1
        if not registered:
            logging.warning("evento=REREPLICACAO_DESCARTADA bloco=%s (arquivo apagado durante a cópia)", block_id)
        return registered

    def _fail(self, block_id, node_id, disk, reason: str, leftover: bool = False) -> bool:
        with self.db.tx() as cur:
            md.cancel_repair(cur, block_id, node_id, disk, leftover=leftover)
        with self._lock:
            self.failed += 1
        logging.error("evento=REREPLICACAO_FALHA bloco=%s destino=%s motivo=%s", block_id, node_id, reason)
        return False

    def _zmq_copy(self, addr: str, request: dict) -> dict:
        sock = self._ctx.socket(zmq.DEALER)
        try:
            sock.setsockopt(zmq.IDENTITY, str(uuid.uuid4()).encode())
            sock.setsockopt(zmq.LINGER, 0)
            sock.setsockopt(zmq.RCVTIMEO, self.copy_timeout_ms)
            sock.setsockopt(zmq.SNDTIMEO, self.copy_timeout_ms)
            sock.connect(addr)
            sock.send_multipart([b"", json.dumps(request).encode()])
            frames = sock.recv_multipart()
            reply = json.loads(frames[1].decode())
            if reply.get("req_id") != request["req_id"]:
                raise RuntimeError("resposta de outra requisição")
            return reply
        finally:
            sock.close()

    def _warn_once(self, key, message, *args):
        if key not in self._warned:
            self._warned.add(key)
            logging.warning(message, *args)