"""Camada de metadados

Semântica de concorrência (RF8): lease de escrita.
  - INIT_WRITE cria o inode em estado ESCREVENDO e uma lease com prazo.
  - Enquanto a lease não expira, outro INIT_WRITE no mesmo caminho recebe BUSY.
  - Se expira (cliente caiu), o próximo INIT_WRITE descarta a escrita abandonada.
  - Só arquivos COMPLETO aparecem em leitura e listagem: nunca se lê arquivo parcial.
"""

import re

from psycopg2 import errors

ROOT_ID = 1
WRITING = "ESCREVENDO"
COMPLETE = "COMPLETO"

MAX_PATH_LEN = 4096
MAX_NAME_LEN = 255


class MetadataError(Exception):
    def __init__(self, code: str, msg: str):
        super().__init__(msg)
        self.code = code
        self.msg = msg


def split_path(path) -> list[str]:
    """'/a//b/c.txt' -> ['a', 'b', 'c.txt']; '/' -> []. Rejeita caminhos inválidos."""
    if not isinstance(path, str) or not path.startswith("/"):
        raise MetadataError("BAD_PATH", "o caminho deve ser uma string começando com '/'")
    if len(path) > MAX_PATH_LEN or "\x00" in path:
        raise MetadataError("BAD_PATH", "caminho inválido")
    parts = [p for p in re.split(r"/+", path) if p]
    for part in parts:
        if part in (".", "..") or len(part) > MAX_NAME_LEN:
            raise MetadataError("BAD_PATH", f"nome de caminho inválido: {part!r}")
    return parts


def _child(cur, parent_id: int, name: str):
    cur.execute("SELECT * FROM inodes WHERE parent_id = %s AND name = %s", (parent_id, name))
    return cur.fetchone()


def resolve(cur, path: str):
    """Devolve a linha do inode do caminho, ou None se não existir."""
    node = get_inode(cur, ROOT_ID)
    for part in split_path(path):
        if not node["is_dir"]:
            return None
        node = _child(cur, node["id"], part)
        if node is None:
            return None
    return node


def get_inode(cur, inode_id):
    cur.execute("SELECT * FROM inodes WHERE id = %s", (inode_id,))
    return cur.fetchone()


def ensure_dirs(cur, parts: list[str]) -> int:
    """Cria os diretórios que faltam (como mkdir -p) e devolve o id do último."""
    parent_id = ROOT_ID
    for name in parts:
        cur.execute(
            """INSERT INTO inodes (parent_id, name, is_dir, state)
               VALUES (%s, %s, TRUE, %s)
               ON CONFLICT (parent_id, name) DO NOTHING""",
            (parent_id, name, COMPLETE),
        )
        row = _child(cur, parent_id, name)
        if not row["is_dir"]:
            raise MetadataError("NOT_A_DIRECTORY", f"'{name}' é um arquivo, não um diretório")
        parent_id = row["id"]
    return parent_id


def _lock_dir(cur, dir_id: int):
    """Serializa alterações concorrentes dentro do mesmo diretório."""
    cur.execute("SELECT id FROM inodes WHERE id = %s FOR UPDATE", (dir_id,))


def begin_write(cur, path: str, size_bytes: int, encrypted: bool, file_sha256: str, lease_s: int) -> int:
    """INIT_WRITE: cria o arquivo em estado ESCREVENDO com uma lease. Devolve o id do arquivo."""
    parts = split_path(path)
    if not parts:
        raise MetadataError("BAD_PATH", "o caminho do arquivo não pode ser '/'")
    *dirs, name = parts
    parent_id = ensure_dirs(cur, dirs)
    _lock_dir(cur, parent_id)

    existing = _child(cur, parent_id, name)
    if existing is not None:
        if existing["is_dir"]:
            raise MetadataError("IS_A_DIRECTORY", f"'{path}' é um diretório")
        if existing["state"] == COMPLETE:
            raise MetadataError("EXISTS", f"'{path}' já existe")
        cur.execute("SELECT 1 FROM write_leases WHERE inode_id = %s AND expires_at > NOW()", (existing["id"],))
        if cur.fetchone():
            raise MetadataError("BUSY", f"'{path}' está sendo gravado por outro cliente")
        _discard_inode(cur, existing["id"])  # escrita abandonada: a lease expirou

    cur.execute(
        """INSERT INTO inodes (parent_id, name, is_dir, size_bytes, file_sha256, encrypted, state)
           VALUES (%s, %s, FALSE, %s, %s, %s, %s) RETURNING id""",
        (parent_id, name, size_bytes, file_sha256, encrypted, WRITING),
    )
    file_id = cur.fetchone()["id"]
    cur.execute(
        "INSERT INTO write_leases (inode_id, expires_at) VALUES (%s, NOW() + make_interval(secs => %s))",
        (file_id, lease_s),
    )
    return file_id


def abort_write(cur, inode_id: int):
    """ABORT_WRITE: o cliente desistiu (ou falhou) no meio da gravação; desfaz o arquivo parcial."""
    inode = get_inode(cur, inode_id)
    if inode is None or inode["is_dir"]:
        raise MetadataError("NOT_FOUND", "arquivo desconhecido")
    if inode["state"] != WRITING:
        raise MetadataError("NOT_WRITING", "o arquivo não está em escrita")
    _discard_inode(cur, inode_id)


def get_writing_file(cur, file_id, lease_s: int):
    """Confere que o arquivo está em escrita e renova a lease. Devolve a linha do inode."""
    inode = get_inode(cur, _as_int(file_id, "file_id"))
    if inode is None or inode["is_dir"]:
        raise MetadataError("NOT_FOUND", "arquivo desconhecido")
    if inode["state"] != WRITING:
        raise MetadataError("NOT_WRITING", "o arquivo não está em escrita")
    cur.execute(
        """UPDATE write_leases SET expires_at = NOW() + make_interval(secs => %s)
           WHERE inode_id = %s AND expires_at > NOW() RETURNING 1""",
        (lease_s, inode["id"]),
    )
    if cur.fetchone() is None:
        raise MetadataError("LEASE_EXPIRED", "a lease de escrita expirou")
    return inode


def save_block(cur, inode_id: int, block_id: str, idx: int, size_bytes: int, sha256: str):
    """Registra um bloco. Repetir o mesmo ALLOCATE_BLOCK (retry) recomeça a alocação desse bloco."""
    cur.execute("SELECT inode_id FROM blocks WHERE block_id = %s", (block_id,))
    row = cur.fetchone()
    if row is not None and row["inode_id"] != inode_id:
        raise MetadataError("BAD_REQUEST", "block_id já pertence a outro arquivo")
    cur.execute("DELETE FROM blocks WHERE inode_id = %s AND idx = %s AND block_id <> %s", (inode_id, idx, block_id))
    cur.execute(
        """INSERT INTO blocks (block_id, inode_id, idx, size_bytes, sha256)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (block_id) DO UPDATE
           SET idx = EXCLUDED.idx, size_bytes = EXCLUDED.size_bytes, sha256 = EXCLUDED.sha256""",
        (block_id, inode_id, idx, size_bytes, sha256),
    )
    cur.execute("DELETE FROM replicas WHERE block_id = %s", (block_id,))  # retry: a nova alocação substitui a anterior


def add_pending_replicas(cur, block_id: str, targets: list[tuple[str, str]]):
    """targets = [(node_id, disco)]. Registra máquina e disco se ainda não existirem."""
    for node_id, disk in targets:
        cur.execute("INSERT INTO maquina (nome) VALUES (%s) ON CONFLICT DO NOTHING", (node_id,))
        cur.execute("INSERT INTO disco (maquina, nome) VALUES (%s, %s) ON CONFLICT DO NOTHING", (node_id, str(disk)))
        cur.execute(
            "INSERT INTO replicas (block_id, maquina, disco, state) VALUES (%s, %s, %s, 'PENDENTE')",
            (block_id, node_id, str(disk)),
        )


def commit_block(cur, inode_id: int, block_id: str, node_ids: list[str]) -> int:
    """COMMIT_BLOCK: confirma as réplicas que o cliente conseguiu gravar. Devolve quantas ficaram OK."""
    cur.execute("SELECT 1 FROM blocks WHERE block_id = %s AND inode_id = %s", (block_id, inode_id))
    if cur.fetchone() is None:
        raise MetadataError("NOT_FOUND", "bloco desconhecido neste arquivo")
    cur.execute(
        "UPDATE replicas SET state = 'OK' WHERE block_id = %s AND state = 'PENDENTE' AND maquina = ANY(%s)",
        (block_id, node_ids),
    )
    cur.execute("DELETE FROM replicas WHERE block_id = %s AND state = 'PENDENTE'", (block_id,))  # alocadas, mas o cliente não gravou
    cur.execute("SELECT COUNT(*) AS n FROM replicas WHERE block_id = %s AND state = 'OK'", (block_id,))
    total_ok = cur.fetchone()["n"]
    if total_ok == 0:
        raise MetadataError("NO_VALID_REPLICA", "nenhum dos nós informados foi alocado para este bloco")
    return total_ok


def finish_write(cur, inode_id: int, file_sha256: str):
    """FINISH_WRITE: valida o arquivo e o torna visível (COMPLETO)."""
    inode = get_inode(cur, inode_id)
    if inode["file_sha256"] and inode["file_sha256"] != file_sha256:
        raise MetadataError("HASH_MISMATCH", "o hash do arquivo difere do informado no INIT_WRITE")

    cur.execute(
        """SELECT b.idx, COUNT(r.block_id) FILTER (WHERE r.state = 'OK') AS ok
           FROM blocks b LEFT JOIN replicas r USING (block_id)
           WHERE b.inode_id = %s GROUP BY b.idx ORDER BY b.idx""",
        (inode_id,),
    )
    blocks = cur.fetchall()
    if [b["idx"] for b in blocks] != list(range(len(blocks))):
        raise MetadataError("INCOMPLETE_FILE", "faltam blocos (índices não são contíguos)")
    if any(b["ok"] == 0 for b in blocks):
        raise MetadataError("INCOMPLETE_FILE", "há bloco sem nenhuma réplica confirmada")

    cur.execute("UPDATE inodes SET state = %s, updated_at = NOW() WHERE id = %s", (COMPLETE, inode_id))
    cur.execute("DELETE FROM write_leases WHERE inode_id = %s", (inode_id,))
    return len(blocks)


def file_blocks(cur, inode_id: int) -> list[dict]:
    """Blocos do arquivo em ordem, cada um com as réplicas OK: [{maquina, disco}]."""
    cur.execute(
        """SELECT b.block_id, b.idx, b.size_bytes, b.sha256,
                  COALESCE(json_agg(json_build_object('node_id', r.maquina, 'disk', r.disco)
                                    ORDER BY r.maquina) FILTER (WHERE r.state = 'OK'), '[]') AS replicas
           FROM blocks b LEFT JOIN replicas r USING (block_id)
           WHERE b.inode_id = %s GROUP BY b.block_id ORDER BY b.idx""",
        (inode_id,),
    )
    return cur.fetchall()


def get_complete_file(cur, path: str):
    inode = resolve(cur, path)
    if inode is None or inode["state"] != COMPLETE:
        raise MetadataError("NOT_FOUND", f"'{path}' não existe")
    if inode["is_dir"]:
        raise MetadataError("IS_A_DIRECTORY", f"'{path}' é um diretório")
    return inode


def list_dir(cur, path: str) -> list[dict]:
    inode = resolve(cur, path)
    if inode is None or inode["state"] != COMPLETE:
        raise MetadataError("NOT_FOUND", f"'{path}' não existe")
    if not inode["is_dir"]:
        raise MetadataError("NOT_A_DIRECTORY", f"'{path}' não é um diretório")
    cur.execute(
        """SELECT name, is_dir, size_bytes, created_at FROM inodes
           WHERE parent_id = %s AND state = %s ORDER BY is_dir DESC, name""",
        (inode["id"], COMPLETE),
    )
    return cur.fetchall()


def _discard_inode(cur, inode_id: int):
    """Apaga o inode e deixa as réplicas registradas em gc_blocks para a coleta de lixo."""
    cur.execute(
        """INSERT INTO gc_blocks (block_id, maquina, disco)
           SELECT r.block_id, r.maquina, r.disco FROM replicas r JOIN blocks b USING (block_id)
           WHERE b.inode_id = %s ON CONFLICT DO NOTHING""",
        (inode_id,),
    )
    cur.execute("DELETE FROM inodes WHERE id = %s", (inode_id,))


def delete_file(cur, path: str):
    inode = get_complete_file(cur, path)
    _lock_dir(cur, inode["parent_id"])
    _discard_inode(cur, inode["id"])


def make_dir(cur, path: str) -> bool:
    """Cria o diretório (e os pais). Devolve False se já existia."""
    parts = split_path(path)
    if not parts:
        return False
    existing = resolve(cur, path)
    if existing is not None:
        if not existing["is_dir"]:
            raise MetadataError("EXISTS", f"'{path}' já existe e é um arquivo")
        return False
    ensure_dirs(cur, parts)
    return True


def remove_dir(cur, path: str):
    inode = resolve(cur, path)
    if inode is None:
        raise MetadataError("NOT_FOUND", f"'{path}' não existe")
    if not inode["is_dir"]:
        raise MetadataError("NOT_A_DIRECTORY", f"'{path}' não é um diretório")
    if inode["id"] == ROOT_ID:
        raise MetadataError("BAD_PATH", "a raiz não pode ser removida")
    cur.execute("SELECT 1 FROM inodes WHERE parent_id = %s LIMIT 1", (inode["id"],))
    if cur.fetchone():
        raise MetadataError("NOT_EMPTY", f"'{path}' não está vazio")
    cur.execute("DELETE FROM inodes WHERE id = %s", (inode["id"],))


def move(cur, src: str, dst: str):
    """Renomeia/move arquivo ou diretório. O destino não pode existir."""
    node = resolve(cur, src)
    if node is None or node["state"] != COMPLETE:
        raise MetadataError("NOT_FOUND", f"'{src}' não existe")
    if node["id"] == ROOT_ID:
        raise MetadataError("BAD_PATH", "a raiz não pode ser movida")
    dst_parts = split_path(dst)
    if not dst_parts:
        raise MetadataError("BAD_PATH", "destino inválido")
    *dst_dirs, new_name = dst_parts
    parent = resolve(cur, "/" + "/".join(dst_dirs))
    if parent is None or not parent["is_dir"]:
        raise MetadataError("NOT_FOUND", "o diretório de destino não existe")
    # sobe a árvore a partir do destino: se passar por node, seria mover um diretório para dentro dele mesmo
    cursor_id = parent["id"]
    while cursor_id is not None:
        if cursor_id == node["id"]:
            raise MetadataError("BAD_PATH", "não é possível mover um diretório para dentro dele mesmo")
        cursor_id = get_inode(cur, cursor_id)["parent_id"]
    _lock_dir(cur, parent["id"])
    if _child(cur, parent["id"], new_name) is not None:
        raise MetadataError("EXISTS", f"'{dst}' já existe")
    try:
        cur.execute(
            "UPDATE inodes SET parent_id = %s, name = %s, updated_at = NOW() WHERE id = %s",
            (parent["id"], new_name, node["id"]),
        )
    except errors.UniqueViolation as error:
        raise MetadataError("EXISTS", f"'{dst}' já existe") from error


def report_corruption(cur, block_id: str, node_id: str) -> int:
    """REPORT_CORRUPTION: marca a réplica como CORROMPIDA. Devolve quantas réplicas OK restam."""
    cur.execute("SELECT 1 FROM blocks WHERE block_id = %s", (block_id,))
    if cur.fetchone() is None:
        raise MetadataError("NOT_FOUND", "bloco desconhecido")
    cur.execute(
        "UPDATE replicas SET state = 'CORROMPIDA' WHERE block_id = %s AND maquina = %s AND state = 'OK'",
        (block_id, node_id),
    )
    cur.execute("SELECT COUNT(*) AS n FROM replicas WHERE block_id = %s AND state = 'OK'", (block_id,))
    return cur.fetchone()["n"]



def under_replicated_blocks(cur, alive_nodes: list[str], target: int) -> list[dict]:
    #Os blocos mais urgentes (menos réplicas vivas) vêm primeiro.
    #holders = nós que já têm ou estão recebendo o bloco/ sources = nós vivos com cópia OK
    cur.execute(
        """SELECT b.block_id, b.size_bytes, b.sha256,
                  COUNT(r.block_id) FILTER (WHERE r.state = 'OK' AND r.maquina = ANY(%(alive)s::text[]) AND m.draining = FALSE) AS ok_alive,
                  COALESCE(array_agg(r.maquina) FILTER (WHERE r.state IN ('OK', 'PENDENTE')), '{}') AS holders,
                  COALESCE(array_agg(r.maquina) FILTER (WHERE r.state = 'OK' AND r.maquina = ANY(%(alive)s::text[])), '{}') AS sources
           FROM blocks b
           JOIN inodes i ON i.id = b.inode_id AND i.state = %(complete)s
           LEFT JOIN replicas r ON r.block_id = b.block_id
           LEFT JOIN maquina m ON m.nome = r.maquina
           GROUP BY b.block_id
           HAVING COUNT(r.block_id) FILTER (WHERE r.state = 'OK' AND r.maquina = ANY(%(alive)s::text[]) AND m.draining = FALSE) < %(target)s
           ORDER BY ok_alive, b.block_id""",
        {"alive": list(alive_nodes), "target": target, "complete": COMPLETE},
    )
    return cur.fetchall()


def start_repair(cur, block_id: str, node_id: str, disk: str):
    #Reserva o destino da cópia: uma réplica pendente. Uma corrompida antiga no mesmo disco é descartada
    cur.execute(
        "DELETE FROM replicas WHERE block_id = %s AND maquina = %s AND disco = %s AND state = 'CORROMPIDA'",
        (block_id, node_id, str(disk)),
    )
    add_pending_replicas(cur, block_id, [(node_id, disk)])


def finish_repair(cur, block_id: str, node_id: str, disk: str) -> bool:
    #Se o arquivo foi apagado durante a cópia, a cópia vira lixo
    cur.execute(
        "UPDATE replicas SET state = 'OK' WHERE block_id = %s AND maquina = %s AND disco = %s AND state = 'PENDENTE'",
        (block_id, node_id, str(disk)),
    )
    if cur.rowcount == 0:
        queue_gc(cur, block_id, node_id, disk)
        return False
    return True


def cancel_repair(cur, block_id: str, node_id: str, disk: str, leftover: bool = False):
    #Cópia falhou: some a PENDENTE. leftover=True quando o nó pode ter ficado com o bloco gravado
    cur.execute(
        "DELETE FROM replicas WHERE block_id = %s AND maquina = %s AND disco = %s AND state = 'PENDENTE'",
        (block_id, node_id, str(disk)),
    )
    if leftover:
        queue_gc(cur, block_id, node_id, disk)


def queue_gc(cur, block_id: str, node_id: str, disk: str):
    cur.execute(
        "INSERT INTO gc_blocks (block_id, maquina, disco) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
        (block_id, node_id, str(disk)),
    )


def clear_stale_pending(cur) -> int:
    #Ao assumir a liderança: pendente de arquivo já completo é cópia interrompida por uma queda
    cur.execute(
        """DELETE FROM replicas r USING blocks b, inodes i
           WHERE r.state = 'PENDENTE' AND b.block_id = r.block_id AND i.id = b.inode_id AND i.state = %s""",
        (COMPLETE,),
    )
    return cur.rowcount


def _as_int(value, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        raise MetadataError("BAD_REQUEST", f"campo '{field}' inválido") from None