import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # servidor do controlador
    controller_host: str
    controller_port: int

    # zooKeeper
    zk_hosts: str
    zk_nodes_path: str

    # postgre
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    # comunicação com os nós
    node_rpc_timeout: int

    # configuração dos blocos
    replication_factor: int
    block_size: int

    # monitoramento dos nós
    heartbeat_interval: int
    heartbeat_timeout: int
    membership_interval_s: float   
    suspect_after_s: float         
    dead_after_s: float            

    # servidor RPC
    rpc_workers: int               

    # logs
    log_level: str


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)

    if value is None:
        return default

    try:
        return int(value)
    except ValueError:
        raise ValueError(
            f"A variável de ambiente {name} deve ser um número inteiro."
        )


def load_settings() -> Settings:
    settings = Settings(
        controller_host=os.getenv("CONTROLLER_HOST", "0.0.0.0"),
        controller_port=_get_int("CONTROLLER_PORT", 5000),

        zk_hosts=os.getenv("ZK_HOSTS", "localhost:2181"),
        zk_nodes_path=os.getenv("ZK_NODES_PATH", "/dfs/nodes"),

        db_host=os.getenv("DB_HOST", "localhost"),
        db_port=_get_int("DB_PORT", 5432),
        db_name=os.getenv("DB_NAME", "dfs_metadata"),
        db_user=os.getenv("DB_USER", "postgre"),
        db_password=os.getenv("DB_PASSWORD", "postgre"),

        node_rpc_timeout=_get_int("NODE_RPC_TIMEOUT", 5000),

        replication_factor=_get_int("REPLICATION_FACTOR", 2),
        block_size=_get_int("BLOCK_SIZE", 64 * 1024 * 1024),

        heartbeat_interval=_get_int("HEARTBEAT_INTERVAL", 5),
        heartbeat_timeout=_get_int("HEARTBEAT_TIMEOUT", 15),

        membership_interval_s=float(os.getenv("MEMBERSHIP_INTERVAL_S", "2")),
        suspect_after_s=float(os.getenv("SUSPECT_AFTER_S", "10")),
        dead_after_s=float(os.getenv("DEAD_AFTER_S", "30")),

        rpc_workers=_get_int("RPC_WORKERS", 8),

        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
    )

    # validações das configurações
    if not 1 <= settings.controller_port <= 65535:
        raise ValueError("CONTROLLER_PORT deve estar entre 1 e 65535.")

    if not 1 <= settings.db_port <= 65535:
        raise ValueError("DB_PORT deve estar entre 1 e 65535.")

    if settings.replication_factor < 1:
        raise ValueError("REPLICATION_FACTOR deve ser maior que zero.")

    if settings.block_size < 1:
        raise ValueError("BLOCK_SIZE deve ser maior que zero.")

    if settings.node_rpc_timeout < 1:
        raise ValueError("NODE_RPC_TIMEOUT deve ser maior que zero.")

    if settings.heartbeat_interval < 1:
        raise ValueError("HEARTBEAT_INTERVAL deve ser maior que zero.")

    if settings.heartbeat_timeout <= settings.heartbeat_interval:
        raise ValueError(
            "HEARTBEAT_TIMEOUT deve ser maior que HEARTBEAT_INTERVAL."
        )

    if settings.rpc_workers < 1:
        raise ValueError("RPC_WORKERS deve ser maior que zero.")

    if settings.suspect_after_s >= settings.dead_after_s:
        raise ValueError("SUSPECT_AFTER_S deve ser menor que DEAD_AFTER_S.")

    return settings


# configurações carregadas na inicialização do modulo
settings = load_settings()
