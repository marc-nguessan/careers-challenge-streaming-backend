"""Front worker (DESIGN §10): accept POSTs locally, proxy reads to the owner.

Several of these run as separate processes that **share one listening socket**
(pre-fork): the launcher binds the public port once and forks the workers, which
all `accept()` on the inherited socket. The kernel then hands each new connection
to one waiting worker, distributing load across cores/GILs. (We measured that the
alternative, SO_REUSEPORT, does *not* load-balance on macOS — all connections
went to a single worker — so pre-fork is what actually scales here.)

Each worker appends accepted events to its OWN log file, `events.w{id}.ndjson`;
the owner tails all of them (owner.py). Reads (GET) are proxied to the owner,
which holds the derived state.
"""

import json
import os
import time
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse

from .http_api import QuietThreadingHTTPServer
from .log import EventLog
from .metrics import METRICS
from .validate import validate, ValidationError


def make_front_handler(event_log, owner_host, owner_port, alarms_path):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_POST(self):
            if urlparse(self.path).path != "/events":
                self._text(404, "unknown path")
                return
            t0 = time.perf_counter()
            length = int(self.headers.get("Content-Length", "0") or "0")
            body = self.rfile.read(length) if length else b""
            try:
                event = validate(body, time.time())
            except ValidationError as e:
                METRICS.inc("events_rejected_total", (e.reason,))
                METRICS.observe("ingest_request_seconds", time.perf_counter() - t0)
                self._json(400, {"error": e.reason})
                return
            # Append to this worker's own log, then ack. The owner applies it
            # later by tailing the file (§10) — the front door holds no state.
            event_log.append(event)
            METRICS.inc("events_accepted_total", (event["type"],))
            METRICS.observe("ingest_request_seconds", time.perf_counter() - t0)
            self._json(200, {"status": "accepted"})

        def do_GET(self):
            # SSE is served locally by following the alarm log the owner writes
            # (§8/§10) — never proxied, so a long-lived stream doesn't tie up the
            # owner. Everything else proxies to the owner (the only process with
            # state). GETs are low-volume, so a fresh localhost connection is fine.
            if urlparse(self.path).path == "/alarms/stream":
                from .sse import serve_alarm_stream
                serve_alarm_stream(self, alarms_path)
                return
            try:
                conn = HTTPConnection(owner_host, owner_port, timeout=5)
                conn.request("GET", self.path)
                resp = conn.getresponse()
                payload = resp.read()
                self.send_response(resp.status)
                self.send_header("Content-Type",
                                 resp.getheader("Content-Type", "application/json"))
                self.send_header("Content-Length", str(len(payload)))
                retry_after = resp.getheader("Retry-After")
                if retry_after:
                    self.send_header("Retry-After", retry_after)
                self.end_headers()
                self.wfile.write(payload)
                conn.close()
            except OSError:
                self._text(503, "owner unavailable", {"Retry-After": "1"})

        def _json(self, status, obj):
            payload = json.dumps(obj).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _text(self, status, text, extra=None):
            payload = text.encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(payload)

    return Handler


def run_front_on_socket(sock, worker_id: int, owner_port: int, data_dir: str):
    """Serve the front-door HTTP on an already-bound, already-listening socket
    inherited from the launcher (pre-fork). Blocks until the process is killed."""
    os.makedirs(data_dir, exist_ok=True)
    log = EventLog(os.path.join(data_dir, f"events.w{worker_id}.ndjson"))
    alarms_path = os.path.join(data_dir, "alarms.ndjson")
    handler = make_front_handler(log, "127.0.0.1", owner_port, alarms_path)
    # Build a server without letting it create/bind its own socket, then attach
    # the shared one so every worker accepts on the same listen queue.
    server = QuietThreadingHTTPServer(("0.0.0.0", 0), handler, bind_and_activate=False)
    server.socket.close()
    server.socket = sock
    try:
        server.serve_forever()
    finally:
        log.close()
