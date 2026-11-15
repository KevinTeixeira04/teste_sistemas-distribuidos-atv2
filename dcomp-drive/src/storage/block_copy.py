"""Responsável pela transferência de blocos entre nós"""

import hashlib
import json
import uuid

import zmq


class BlockTransferError(Exception):
    """Falha durante a transferência de um bloco """

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def fetch_block(context, source_addr: str, block_id: str):
    """Solicita um bloco à origem e valida seu SHA-256."""

    sock = context.socket(zmq.DEALER)

    try:
        sock.setsockopt(zmq.IDENTITY, str(uuid.uuid4()).encode("utf-8"))
        sock.setsockopt(zmq.RCVTIMEO, 5000)
        sock.setsockopt(zmq.SNDTIMEO, 5000)
        sock.setsockopt(zmq.LINGER, 0)

        req_id = str(uuid.uuid4())
        request = {
            "req_id": req_id,
            "action": "GET_BLOCK",
            "block_id": block_id,
        }

        sock.connect(source_addr)
        sock.send_multipart([b"", json.dumps(request).encode("utf-8")])

        try:
            frames = sock.recv_multipart()
        except zmq.Again as error:
            raise BlockTransferError(
                "SOURCE_UNAVAILABLE",
                "Tempo esgotado ao solicitar o bloco ao nó de origem.",
            ) from error

        if len(frames) != 3:
            raise BlockTransferError(
                "COPY_FAILED",
                "Resposta inválida recebida do nó de origem.",
            )

        response = json.loads(frames[1].decode("utf-8"))
        if not isinstance(response, dict):
            raise BlockTransferError(
                "COPY_FAILED",
                "Metadados inválidos recebidos do nó de origem.",
            )

        if response.get("req_id") != req_id or response.get("status") != "ACK":
            raise BlockTransferError(
                "COPY_FAILED",
                f"Falha ao obter bloco da origem: "
                f"{response.get('msg', response.get('code'))}.",
            )

        data = frames[2]
        expected_hash = response.get("sha256")

        if expected_hash != hashlib.sha256(data).hexdigest():
            raise BlockTransferError(
                "HASH_MISMATCH",
                "O hash do bloco recebido da origem não corresponde.",
            )

        return data, expected_hash

    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BlockTransferError(
            "COPY_FAILED",
            "Resposta inválida do nó de origem.",
        ) from error
    except zmq.ZMQError as error:
        raise BlockTransferError(
            "SOURCE_UNAVAILABLE",
            f"Falha de comunicação com o nó de origem: {error}.",
        ) from error
    finally:
        sock.close()
