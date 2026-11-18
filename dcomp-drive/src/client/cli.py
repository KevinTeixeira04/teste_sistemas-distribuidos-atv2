import argparse
import logging
import os
import sys
from src.client.crypto import CryptoManager
from src.client.pipeline import DFSClient, DEFAULT_CHUNK_SIZE


class RF12Formatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'evento'):
            record.evento = 'SISTEMA'
        if not hasattr(record, 'identificador'):
            record.identificador = 'N/A'
        return super().format(record)

def setup_logging(debug: bool, log_file: str):
    level = logging.DEBUG if debug else logging.INFO
    rf12_format = "%(asctime)s | COMP:%(name)s | EVT:%(evento)s | ID:%(identificador)s | MSG:%(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    formatter = RF12Formatter(fmt=rf12_format, datefmt=date_format)

    handlers = []
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    logging.basicConfig(level=level, handlers=handlers)


def print_stat(path: str, info: dict):
    print(path)
    print(f"  tipo: {'diretório' if info.get('is_directory') else 'arquivo'}")
    print(f"  tamanho: {info.get('size_bytes', 0)} bytes")
    print(f"  criado em: {info.get('created_at')}")
    if info.get("is_directory"):
        return
    print(f"  sha256: {info.get('file_sha256')}")
    print(f"  criptografado: {'sim' if info.get('encrypted') else 'não'}")
    print(f"  blocos: {info.get('blocks')}")
    print(f"  estado de replicação: {info.get('replication_state')}")
    for item in info.get("replicas", []):
        print(f"    {item['block_id']}: {', '.join(item['nodes'])}")


NODE_STATES = {"ALIVE": "VIVO", "SUSPECT": "SUSPEITO", "DEAD": "MORTO"}


def format_bytes(value) -> str:
    value = float(value)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024:
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


def print_status(info: dict):
    print(f"Nós (fator de replicação {info.get('replication_factor')}):")
    for node_id, node in sorted(info.get("nodes", {}).items()):
        estado = NODE_STATES.get(node.get("state"), node.get("state"))
        if node.get("draining"):
            estado += " (em drain)"
        print(f"  {node_id}: {estado}, blocos: {node.get('blocks', 0)}")
        for disk in node.get("disks", []):
            percent = 100 * disk["usado"] / disk["total"] if disk["total"] else 0
            print(f"    {disk['nome']}: {format_bytes(disk['usado'])} de {format_bytes(disk['total'])} ({percent:.0f}%)")

    files = info.get("files", {})
    print("Arquivos por estado de replicação:")
    for estado in ("INTEGRO", "SUBREPLICADO", "SEM_REPLICA"):
        print(f"  {estado}: {files.get(estado, 0)}")

    rep = info.get("replication")
    if rep is None:
        print("Re-replicação: indisponível")
        return
    print(f"Re-replicação: {len(rep.get('in_progress', []))} em andamento, "
          f"{rep.get('under_replicated_blocks', 0)} bloco(s) pendente(s), "
          f"{rep.get('copied', 0)} concluída(s), {rep.get('failed', 0)} falha(s)")
    for item in rep.get("in_progress", []):
        print(f"    {item.get('block_id')}: {item.get('source')} -> {item.get('target')}")


def main():
    parser = argparse.ArgumentParser(description="CLI do Cliente - DFS")
    parser.add_argument("--controller", default=os.getenv("DFS_CONTROLLER", "tcp://localhost:5000"))
    parser.add_argument("--key", default=os.getenv("DFS_AES_KEY"), help="Chave AES-256 em hex (opcional)")
    parser.add_argument("--chunk-mb", type=int, default=DEFAULT_CHUNK_SIZE // (1024 * 1024))
    parser.add_argument("--debug", action="store_true", help="Ativa logs detalhados (DEBUG)")
    parser.add_argument("--log-file", default=os.getenv("DFS_CLIENT_LOG", "dfs_client.log"))

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("keygen", help="Gera chave AES-256")
    
    put_p = subparsers.add_parser("put")
    put_p.add_argument("local_path")
    put_p.add_argument("remote_path")

    get_p = subparsers.add_parser("get")
    get_p.add_argument("remote_path")
    get_p.add_argument("dest_path")

    ls_p = subparsers.add_parser("ls")
    ls_p.add_argument("remote_path", nargs="?", default="/")

    rm_p = subparsers.add_parser("rm")
    rm_p.add_argument("remote_path")

    mkdir_p = subparsers.add_parser("mkdir", help="Cria um diretório (e os pais)")
    mkdir_p.add_argument("remote_path")

    rmdir_p = subparsers.add_parser("rmdir", help="Remove um diretório vazio")
    rmdir_p.add_argument("remote_path")

    mv_p = subparsers.add_parser("mv", help="Move ou renomeia um arquivo ou diretório")
    mv_p.add_argument("src_path")
    mv_p.add_argument("dst_path")

    stat_p = subparsers.add_parser("stat", help="Mostra os metadados e o estado de replicação")
    stat_p.add_argument("remote_path")

    subparsers.add_parser("status", help="Mostra nós, espaço, arquivos por estado e re-replicações em andamento")

    args = parser.parse_args()

    if args.command == "keygen":
        print(os.urandom(32).hex())
        sys.exit(0)

    setup_logging(debug=args.debug, log_file=args.log_file)

    crypto = CryptoManager(key_hex=args.key)
    chunk_bytes = args.chunk_mb * 1024 * 1024
    client = DFSClient(controller_addr=args.controller, crypto=crypto, chunk_size=chunk_bytes)

    try:
        if args.command == "put":
            client.put_file(args.local_path, args.remote_path)
        elif args.command == "get":
            client.get_file(args.remote_path, args.dest_path)
        elif args.command == "ls":
            for item in client.list_dir(args.remote_path):
                tipo = "DIR " if item.get("is_directory") else "FILE"
                print(f"[{tipo}] {item.get('name')} ({item.get('size_bytes', 0)} bytes)")
        elif args.command == "rm":
            client.delete_file(args.remote_path)
        elif args.command == "mkdir":
            created = client.make_dir(args.remote_path)
            print(f"Diretório criado: {args.remote_path}" if created else f"Já existe: {args.remote_path}")
        elif args.command == "rmdir":
            client.remove_dir(args.remote_path)
        elif args.command == "mv":
            client.move(args.src_path, args.dst_path)
        elif args.command == "stat":
            print_stat(args.remote_path, client.stat(args.remote_path))
        elif args.command == "status":
            print_status(client.status())
    except Exception as e:
        logging.getLogger("ClienteDFS").error(f"Operação abortada: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
