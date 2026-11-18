"""Handlers das operações do cliente (RF1, RF3, RF4)

Cada handler recebe o pedido (dict) e devolve os campos extras da resposta
Para erros esperados, levanta RequestError 

"""

import functools
import logging
import os

from src.controller import metadata as md
from src.controller import placement
from src.controller.errors import RequestError

DEFAULT_LEASE_S = 300        # prazo da lease de escrita; renovado a cada ALLOCATE/COMMIT
DEFAULT_MAX_USAGE = 0.90     # disco acima desse valor não recebe réplicas


def register_handlers(server, db, registry, cfg):
    h = Handlers(db, registry, cfg)
    for action, handler in {
        "INIT_WRITE": h.init_write,
        "ALLOCATE_BLOCK": h.allocate_block,
        "COMMIT_BLOCK": h.commit_block,
        "FINISH_WRITE": h.finish_write,
        "GET_FILE_METADATA": h.get_file_metadata,
        "LIST_DIR": h.list_dir,
        "DELETE_FILE": h.delete_file,
        "REPORT_CORRUPTION": h.report_corruption,
        "ABORT_WRITE": h.abort_write,
        # operações de RF1 que o cliente ainda não expõe na CLI
        "MKDIR": h.mkdir,
        "RMDIR": h.rmdir,
        "MOVE": h.move,
        "STAT": h.stat,
        "STATUS": h.status,
    }.items():
        server.register(action, handler)
    return h


def _field(req: dict, name: str, kind, required: bool = True, default=None):
    value = req.get(name, default)
    if value is None:
        if required:
            raise RequestError("BAD_REQUEST", f"campo '{name}' obrigatório")
        return default
    if kind is int and isinstance(value, bool):
        raise RequestError("BAD_REQUEST", f"campo '{name}' inválido")
    if not isinstance(value, kind):
        raise RequestError("BAD_REQUEST", f"campo '{name}' inválido")
    return value


def _translate_errors(func):
    """MetadataError (camada SQL) vira RequestError (camada RPC)."""
    @functools.wraps(func)
    def wrapper(self, req):
        try:
            return func(self, req)
        except md.MetadataError as error:
            raise RequestError(error.code, error.msg) from error
    return wrapper


class Handlers:
    def __init__(self, db, registry, cfg):
        self.db = db
        self.registry = registry
        self.cfg = cfg
        self.lease_s = getattr(cfg, "write_lease_s", DEFAULT_LEASE_S)
        self.max_usage = getattr(cfg, "max_disk_usage", float(os.getenv("MAX_DISK_USAGE", DEFAULT_MAX_USAGE)))
        # RF3: sem nós suficientes, aceitar como sub-replicado (True) ou rejeitar (False)
        self.allow_partial = getattr(cfg, "allow_partial_writes", True)
        self.replicator = None  # o leader() preenche depois de criar o replicator

    @_translate_errors
    def init_write(self, req):
        path = _field(req, "remote_path", str)
        size = _field(req, "size_bytes", int, default=0)
        sha = _field(req, "file_sha256", str)
        encrypted = bool(req.get("encrypted", False))
        with self.db.tx() as cur:
            file_id = md.begin_write(cur, path, size, encrypted, sha, self.lease_s)
        logging.info("evento=INIT_WRITE arquivo=%s file_id=%s", path, file_id)
        return {"file_id": str(file_id)}

    @_translate_errors
    def allocate_block(self, req):
        file_id = _field(req, "file_id", (str, int))
        block_id = _field(req, "block_id", str)
        idx = _field(req, "block_index", int)
        size = _field(req, "size_bytes", int)
        sha = _field(req, "sha256", str)

        usable = {nid: n for nid, n in self.registry.get_nodes().items() if n.get("addr")}
        wanted = self.cfg.replication_factor
        with self.db.tx() as cur:
            inode = md.get_writing_file(cur, file_id, self.lease_s)
            chosen = placement.choose_replicas(
                usable, wanted, size, max_usage=self.max_usage, allow_partial=True
            )
            if not chosen:
                raise RequestError("NO_NODES", self._no_nodes_message(usable))
            if len(chosen) < wanted and not self.allow_partial:
                raise RequestError(
                    "NOT_ENOUGH_NODES", f"só há {len(chosen)} destino(s) para {wanted} réplica(s)"
                )
            md.save_block(cur, inode["id"], block_id, idx, size, sha)
            md.add_pending_replicas(cur, block_id, [(c.node_id, c.disk) for c in chosen])

        under = len(chosen) < wanted
        if under:
            logging.warning("evento=SUBREPLICADO bloco=%s alocadas=%d desejadas=%d", block_id, len(chosen), wanted)
        logging.info("evento=ALLOCATE_BLOCK bloco=%s nos=%s", block_id, [c.node_id for c in chosen])
        return {
            "nodes": [{"node_id": c.node_id, "addr": usable[c.node_id]["addr"], "disk": c.disk} for c in chosen],
            "under_replicated": under,
        }

    @_translate_errors
    def abort_write(self, req):
        """O cliente falhou no meio do put: descarta o arquivo parcial para o caminho poder ser gravado de novo."""
        file_id = _field(req, "file_id", (str, int))
        with self.db.tx() as cur:
            md.abort_write(cur, md._as_int(file_id, "file_id"))
        logging.warning("evento=ABORT_WRITE file_id=%s", file_id)
        return {}

    @_translate_errors
    def commit_block(self, req):
        file_id = _field(req, "file_id", (str, int))
        block_id = _field(req, "block_id", str)
        node_ids = _field(req, "node_ids", list)
        if not node_ids or not all(isinstance(n, str) for n in node_ids):
            raise RequestError("BAD_REQUEST", "campo 'node_ids' inválido")
        with self.db.tx() as cur:
            inode = md.get_writing_file(cur, file_id, self.lease_s)
            replicas = md.commit_block(cur, inode["id"], block_id, node_ids)
        under = replicas < self.cfg.replication_factor
        logging.info("evento=COMMIT_BLOCK bloco=%s replicas=%d", block_id, replicas)
        return {"replicas": replicas, "under_replicated": under}

    @_translate_errors
    def finish_write(self, req):
        file_id = _field(req, "file_id", (str, int))
        sha = _field(req, "file_sha256", str)
        with self.db.tx() as cur:
            inode = md.get_writing_file(cur, file_id, self.lease_s)
            blocks = md.finish_write(cur, inode["id"], sha)
        logging.info("evento=FINISH_WRITE file_id=%s blocos=%d", file_id, blocks)
        return {"blocks": blocks}

    @_translate_errors
    def get_file_metadata(self, req):
        path = _field(req, "remote_path", str)
        with self.db.tx() as cur:
            inode = md.get_complete_file(cur, path)
            blocks = md.file_blocks(cur, inode["id"])
        nodes = self.registry.get_nodes()
        out = []
        for b in blocks:
            replicas = self._alive_replicas(b["replicas"], nodes)
            out.append({
                "block_id": b["block_id"], "block_index": b["idx"],
                "size_bytes": b["size_bytes"], "sha256": b["sha256"], "replicas": replicas,
            })
        return {
            "file_id": str(inode["id"]),
            "size_bytes": inode["size_bytes"],
            "file_sha256": inode["file_sha256"],
            "encrypted": inode["encrypted"],
            "blocks": out,
        }

    @_translate_errors
    def stat(self, req):
        """Metadados completos: tamanho, data, hash, réplicas e estado de replicação (RF1)."""
        path = _field(req, "remote_path", str)
        with self.db.tx() as cur:
            inode = md.resolve(cur, path)
            if inode is None or inode["state"] != md.COMPLETE:
                raise RequestError("NOT_FOUND", f"'{path}' não existe")
            blocks = [] if inode["is_dir"] else md.file_blocks(cur, inode["id"])
        info = {
            "name": inode["name"], "is_directory": inode["is_dir"], "size_bytes": inode["size_bytes"],
            "created_at": inode["created_at"].isoformat(),
        }
        if inode["is_dir"]:
            return info
        nodes = self.registry.get_nodes()
        info.update({
            "file_sha256": inode["file_sha256"], "encrypted": inode["encrypted"],
            "blocks": len(blocks),
            "replicas": [{"block_id": b["block_id"], "nodes": [r["node_id"] for r in b["replicas"]]} for b in blocks],
            "replication_state": self._replication_state(blocks, nodes),
        })
        return info

    @_translate_errors
    def status(self, req):
        """RF11: nós, espaço por disco, arquivos por estado de replicação e cópias em andamento."""
        nodes = self.registry.get_nodes()
        alive = [nid for nid, n in nodes.items() if n.get("state") == "ALIVE" and n.get("addr")]
        wanted = self.cfg.replication_factor
        with self.db.tx() as cur:
            files = md.replication_summary(cur, alive, wanted)
            blocks = md.blocks_per_node(cur)

        report = {}
        for node_id, node in nodes.items():
            disks = [
                {"nome": name, "total": total, "usado": total - available}
                for name, total, available in placement._normalize_disks(node.get("disks"))
            ]
            report[node_id] = {
                "state": node.get("state"),
                "draining": bool(node.get("draining")),
                "addr": node.get("addr"),
                "blocks": blocks.get(node_id, 0),
                "disks": disks,
            }
        return {
            "replication_factor": wanted,
            "nodes": report,
            "files": files,
            "replication": self.replicator.status() if self.replicator else None,
        }

    @_translate_errors
    def list_dir(self, req):
        path = _field(req, "remote_path", str, default="/")
        with self.db.tx() as cur:
            rows = md.list_dir(cur, path)
        return {"entries": [
            {"name": r["name"], "is_directory": r["is_dir"], "size_bytes": r["size_bytes"],
             "created_at": r["created_at"].isoformat()}
            for r in rows
        ]}

    @_translate_errors
    def delete_file(self, req):
        path = _field(req, "remote_path", str)
        with self.db.tx() as cur:
            md.delete_file(cur, path)
        logging.info("evento=DELETE_FILE arquivo=%s", path)
        return {}

    @_translate_errors
    def mkdir(self, req):
        path = _field(req, "remote_path", str)
        with self.db.tx() as cur:
            created = md.make_dir(cur, path)
        return {"created": created}

    @_translate_errors
    def rmdir(self, req):
        path = _field(req, "remote_path", str)
        with self.db.tx() as cur:
            md.remove_dir(cur, path)
        return {}

    @_translate_errors
    def move(self, req):
        src = _field(req, "src_path", str)
        dst = _field(req, "dst_path", str)
        with self.db.tx() as cur:
            md.move(cur, src, dst)
        logging.info("evento=MOVE origem=%s destino=%s", src, dst)
        return {}

    @_translate_errors
    def report_corruption(self, req):
        block_id = _field(req, "block_id", str)
        node_id = _field(req, "node_id", str)
        with self.db.tx() as cur:
            remaining = md.report_corruption(cur, block_id, node_id)
        logging.warning("evento=CORRUPCAO bloco=%s no=%s replicas_ok=%d", block_id, node_id, remaining)
        # a re-replicação (RF5) deve achar esse bloco: ele agora tem menos réplicas OK que o fator
        return {"remaining_replicas": remaining}

    def _no_nodes_message(self, usable: dict) -> str:
        """Explica por que não há destino: quantos nós estão vivos e quão cheios estão os discos."""
        alive = [n for n in usable.values() if n.get("state") == "ALIVE" and not n.get("draining")]
        if not alive:
            return f"nenhum nó disponível (vivos e fora de drain: 0 de {len(usable)} registrados)"
        usages = [1 - avail / total for n in alive for _, total, avail in placement._normalize_disks(n.get("disks"))
                  if total > 0]
        if not usages:
            return f"nenhum nó disponível ({len(alive)} vivo(s), mas sem informação válida de disco)"
        return (f"nenhum nó disponível com espaço para o bloco ({len(alive)} vivo(s); menor uso de disco "
                f"{min(usages):.0%}, limite {self.max_usage:.0%})")

    @staticmethod
    def _alive_replicas(replicas: list, nodes: dict) -> list[dict]:
        """Réplicas em nós ALIVE e com endereço conhecido, para o cliente não perder tempo com nó morto."""
        alive = []
        for r in replicas:
            node = nodes.get(r["node_id"])
            if node and node.get("state") == "ALIVE" and node.get("addr"):
                alive.append({"node_id": r["node_id"], "addr": node["addr"], "disk": r["disk"]})
        return alive

    def _replication_state(self, blocks: list, nodes: dict) -> str:
        """INTEGRO: todo bloco tem N réplicas vivas. SUBREPLICADO: algum tem menos. SEM_REPLICA: algum não tem nenhuma."""
        wanted = self.cfg.replication_factor
        counts = [len(self._alive_replicas(b["replicas"], nodes)) for b in blocks]
        if not counts or min(counts) >= wanted:
            return "INTEGRO"
        return "SEM_REPLICA" if min(counts) == 0 else "SUBREPLICADO"