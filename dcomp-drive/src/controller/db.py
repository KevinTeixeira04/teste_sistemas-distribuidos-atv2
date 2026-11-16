import logging
import time
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor


logger = logging.getLogger(__name__)


""" Scheema de metadados do DFS """

SCHEMA = """
CREATE TABLE IF NOT EXISTS inodes ( --essa tabela forma a "arvore" de arquivos montada a partir de todas as maquinas, tabela "abstrata"
    id BIGSERIAL NOT NULL,
    parent_id BIGINT REFERENCES inodes(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    is_dir BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    state TEXT NOT NULL, -- em replicação, sub-replicado ou ok
    UNIQUE (parent_id, name),
    UNIQUE (id, maquina) -- dois inodes podem ter o mesmo id mas maquinas diferentes (obviamente o contrario é verdade)
);

CREATE TABLE IF NOT EXISTS maquina (
    nome PRIMARY KEY, --nome da maquina (de preferencia definido la no docker)
    draining BOOLEAN
    --falta algo?
);

INSERT INTO inodes (id, parent_id, name, is_dir)
VALUES (1, NULL, NULL, 'root/', TRUE)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS blocks ( --representa fisicamente onde tao as coisa, tabela menos abstrata
    block_id TEXT PRIMARY KEY,
    inode_id REFERENCES inodes(id),
    maquina REFERENCES maquina(nome), --quais maquinas estou salvo?
    idx INTEGER NOT NULL,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    sha256 TEXT NOT NULL,
    state TEXT NOT NULL, -- ?
    UNIQUE (version_id, idx)
);

CREATE TABLE IF NOT EXISTS write_leases (
    path TEXT PRIMARY KEY,
    version_id TEXT NOT NULL
        REFERENCES file_versions(version_id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_leases_version
    ON write_leases(version_id);
"""


class Database:
    def __init__(self, cfg):
        self.cfg = cfg
        self.params = {
            "host": cfg.db_host,
            "port": cfg.db_port,
            "dbname": cfg.db_name,
            "user": cfg.db_user,
            "password": cfg.db_password,
        }
        self.maxconn = cfg.rpc_workers + 16
        self.pool = None

    def connect(self, retries=30, delay=2.0):
        if self.pool is not None:
            return

        """ Aguarda o postgre ficar disponível durante a inicialização """
        for attempt in range(1, retries + 1):
            try:
                self.pool = pool.ThreadedConnectionPool(
                    1,
                    self.maxconn,
                    **self.params,
                )
                logger.info("Conexão com PostgreSQL estabelecida.")
                return
            except psycopg2.OperationalError as error:
                logger.warning(
                    "Tentativa %s/%s de conexão com PostgreSQL falhou: %s",
                    attempt,
                    retries,
                    error,
                )

                if attempt == retries:
                    raise RuntimeError(
                        "Não foi possível conectar ao PostgreSQL."
                    ) from error

                time.sleep(delay)

    def init_schema(self):
        self.connect()

        with self.tx() as cursor:
            
            """ Evita inicializações simultâneas do esquema """
            cursor.execute("SELECT pg_advisory_xact_lock(727274)")
            cursor.execute(SCHEMA)

        logger.info("Esquema do PostgreSQL inicializado.")

    @contextmanager
    def tx(self):
        if self.pool is None:
            self.connect()

        connection = self.pool.getconn()

        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                yield cursor

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            self.pool.putconn(
                connection,
                close=bool(connection.closed),
            )

    def close(self):
        if self.pool is not None:
            self.pool.closeall()
            self.pool = None
            logger.info("Conexões com PostgreSQL encerradas.")
