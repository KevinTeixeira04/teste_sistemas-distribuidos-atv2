"""Servidor RPC do controlador."""

import json
import logging
import queue
import signal
import threading
import time
from src.controller.config import settings
from src.controller.db import Database
from src.controller.registry import NodeRegistry
from src.wrapper import wrapperKazoo as wzk
from concurrent.futures import ThreadPoolExecutor

import zmq


class RequestError(Exception):

    def __init__(self, code: str, msg: str):
        super().__init__(msg)
        self.code = code
        self.msg = msg


CLIENT_ACTIONS = (
    "INIT_WRITE",          
    "ALLOCATE_BLOCK",      
    "COMMIT_BLOCK",       
    "FINISH_WRITE",        
    "GET_FILE_METADATA",  
    "LIST_DIR",            
    "DELETE_FILE",         
    "REPORT_CORRUPTION",   
)


class ControllerServer:
    def __init__(self, cfg, db=None, registry=None, bind_addr=None):
        self.cfg = cfg
        self.db = db
        self.registry = registry
        self.bind_addr = bind_addr or f"tcp://{cfg.controller_host}:{cfg.controller_port}"

        self._handlers = {}
        self._pool = ThreadPoolExecutor(
            max_workers=cfg.rpc_workers, thread_name_prefix="rpc"
        )
        self._replies: queue.Queue = queue.Queue()
        self._stop_event = threading.Event()
        self._ctx = None
        self._sock = None
        self._thread = None
        self.bound_port = None

        # ações que já foram implementadas
        self.register("PING", self._ping)
        self.register("HEARTBEAT", self._heartbeat)
        self.register("STATUS", self._status)
        # ações dos clientes que não foram implementadas ainda 
        for action in CLIENT_ACTIONS:
            self.register(action, self._not_implemented(action))

    def register(self, action: str, handler):
        self._handlers[action] = handler

    def start(self):
        self._ctx = zmq.Context()
        self._sock = self._ctx.socket(zmq.ROUTER)
        self._sock.setsockopt(zmq.LINGER, 0)
        if self.bind_addr.endswith(":0"):
            self.bound_port = self._sock.bind_to_random_port(
                self.bind_addr[:-2]
            )
        else:
            self._sock.bind(self.bind_addr)
            self.bound_port = int(self.bind_addr.rsplit(":", 1)[1])
        self._thread = threading.Thread(
            target=self._loop, name="controller-loop", daemon=True
        )
        self._thread.start()
        logging.info("Controlador ouvindo em %s", self.bind_addr)

    def serve_forever(self):
        self.start()
        while not self._stop_event.wait(0.5):
            pass
        self.stop()

    def request_stop(self):
        self._stop_event.set()

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=3)
            self._thread = None
        self._pool.shutdown(wait=False, cancel_futures=True)
        if self._sock:
            self._sock.close(0)
            self._sock = None
        if self._ctx:
            self._ctx.term()
            self._ctx = None

    def handle_request(self, req: dict) -> dict:
        req_id = req.get("req_id")
        action = req.get("action")
        started = time.monotonic()
        try:
            handler = self._handlers.get(action)
            if handler is None:
                raise RequestError("UNKNOWN_ACTION", f"ação desconhecida: {action}")
            extra = handler(req) or {}
            reply = {"req_id": req_id, "status": "ACK", **extra}
        except RequestError as e:
            reply = {"req_id": req_id, "status": "ERR", "code": e.code, "msg": e.msg}
        except Exception as e:  
            logging.exception("Erro interno em %s", action)
            reply = {"req_id": req_id, "status": "ERR", "code": "INTERNAL", "msg": str(e)}
        logging.info(
            "action=%s req_id=%s status=%s ms=%.1f",
            action, req_id, reply["status"], (time.monotonic() - started) * 1000,
        )
        return reply

    def _loop(self): # única thread que mexe no socket, responsável por mandar respostas prontas que os handlers já deixaram na fila 
        poller = zmq.Poller()
        poller.register(self._sock, zmq.POLLIN)
        while not self._stop_event.is_set():
            while True:
                try:
                    identity, payload = self._replies.get_nowait()
                except queue.Empty:
                    break
                self._sock.send_multipart([identity, b"", payload])
            if not poller.poll(10):
                continue
            frames = self._sock.recv_multipart()
            identity, body = frames[0], frames[-1]
            try:
                req = json.loads(body)
                if not isinstance(req, dict):
                    raise ValueError("JSON não é objeto")
            except ValueError:
                err = {"req_id": None, "status": "ERR",
                       "code": "BAD_REQUEST", "msg": "JSON inválido"}
                self._replies.put((identity, json.dumps(err).encode()))
                continue
            self._pool.submit(self._run, identity, req)

    def _run(self, identity, req):
        reply = self.handle_request(req)
        self._replies.put((identity, json.dumps(reply).encode()))

    def _ping(self, req):
        return {"pong": True}

    def _heartbeat(self, req):
        node_id = req.get("node_id")
        if not node_id:
            raise RequestError("BAD_REQUEST", "node_id obrigatório")
        if self.registry is None:
            raise RequestError("UNAVAILABLE", "registry indisponível")
        known = self.registry.heartbeat(
            node_id, addr=req.get("addr"), host=req.get("host"), disks=req.get("disks")
        )
        if not known:
            raise RequestError("UNKNOWN_NODE", f"nó {node_id} não registrado no ZooKeeper")
        return {}

    def _status(self, req):
        nodes = self.registry.get_nodes() if self.registry else {}
        return {"nodes": nodes, "replication_factor": self.cfg.replication_factor}

    @staticmethod
    def _not_implemented(action):
        def handler(req):
            raise RequestError("NOT_IMPLEMENTED", f"{action} ainda não implementada")
        return handler


def leader():
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
    )
    db = Database(settings)
    db.connect()
    db.init_schema()
    registry = NodeRegistry(settings, db)
    registry.start()
    server = ControllerServer(settings, db, registry)

    signal.signal(signal.SIGINT, lambda *_: server.request_stop())
    signal.signal(signal.SIGTERM, lambda *_: server.request_stop())
    try:
        server.serve_forever()
    finally:
        registry.stop()
        db.close()
#revisar pq deve ta super errado
def main():
    wzk.init()
    while(True)
        do:
            election = wzk.ElectionController(os.getenv("container_name")) #n lembro
        while election is None
        election.run(leader)
    
    

if __name__ == "__main__":
    main()
