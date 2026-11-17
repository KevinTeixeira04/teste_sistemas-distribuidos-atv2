import logging
import time
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor


logger = logging.getLogger(__name__)


""" Schema de metadados do DFS """

SCHEMA = """
CREATE TABLE IF NOT EXISTS inodes ( --essa tabela forma a "arvore" de arquivos montada a partir de todas as maquinas, tabela "abstrata"
    id BIGSERIAL PRIMARY KEY,
    parent_id BIGINT REFERENCES inodes(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    is_dir BOOLEAN NOT NULL DEFAULT FALSE,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    file_sha256 TEXT,
    encrypted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    state TEXT NOT NULL DEFAULT 'COMPLETO', -- ciclo da escrita: ESCREVENDO ou COMPLETO (sub-replicado ou ok é calculado a partir das replicas)
    UNIQUE (parent_id, name)
);

INSERT INTO inodes (id, parent_id, name, is_dir)
VALUES (1, NULL, '', TRUE)
ON CONFLICT (id) DO NOTHING;

SELECT setval(pg_get_serial_sequence('inodes', 'id'), (SELECT MAX(id) FROM inodes)); -- sem isso o primeiro arquivo novo tentaria usar o id 1 da raiz

CREATE TABLE IF NOT EXISTS maquina (
    nome TEXT PRIMARY KEY, --nome da maquina (de preferencia definido la no docker)
    draining BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS disco (
    maquina TEXT NOT NULL REFERENCES maquina(nome) ON DELETE CASCADE, --a qual maquina eu pertenço?
    nome TEXT NOT NULL, -- o nome do disco só é único dentro da maquina (todas têm "disk1")
    PRIMARY KEY (maquina, nome)
);

CREATE TABLE IF NOT EXISTS blocks ( --representa fisicamente onde tao as coisa, tabela menos abstrata
    block_id TEXT PRIMARY KEY,
    inode_id BIGINT NOT NULL REFERENCES inodes(id) ON DELETE CASCADE,
    idx INTEGER NOT NULL,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    sha256 TEXT NOT NULL,
    UNIQUE (inode_id, idx)
);

CREATE TABLE IF NOT EXISTS replicas ( -- um bloco tem varias copias, entao deixei aqui ao invés de blocks
    block_id TEXT NOT NULL REFERENCES blocks(block_id) ON DELETE CASCADE,
    maquina TEXT NOT NULL,
    disco TEXT NOT NULL, --quais discos estou salvo? pode deduzir as maquinas facilmente
    state TEXT NOT NULL DEFAULT 'PENDENTE', -- PENDENTE (alocada, nao confirmada), OK ou CORROMPIDA
    PRIMARY KEY (block_id, maquina, disco),
    FOREIGN KEY (maquina, disco) REFERENCES disco(maquina, nome) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS write_leases (
    inode_id BIGINT PRIMARY KEY REFERENCES inodes(id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS gc_blocks ( -- copias de arquivos apagados que os nos ainda precisam remover
    block_id TEXT NOT NULL,
    maquina TEXT NOT NULL,
    disco TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (block_id, maquina, disco)
);

CREATE INDEX IF NOT EXISTS idx_replicas_state ON replicas(state);
CREATE INDEX IF NOT EXISTS idx_blocks_inode ON blocks(inode_id);
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