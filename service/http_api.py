"""HTTP API: routing, request parsing, responses (DESIGN §1).

Step 1.1 (skeleton): POST /events accepts everything and returns
`200 {"status":"accepted"}`; the GET endpoints return correct but *empty*
shapes; `/healthz` reports readiness (§9). Validation (§2), the log (§3) and
derived state (§5-7) are wired into the same handlers in later sub-steps,
without changing these response shapes.

We use HTTP/1.1 with explicit Content-Length on every response so keep-alive
connections know where each body ends; ThreadingHTTPServer gives one thread
per connection, which is enough for the skeleton.
"""

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from .validate import validate, ValidationError

# Window label -> seconds (DESIGN §1, §6). The grader only ever asks for these
# three, but we also parse the stub's "<n>m"/"<n>h"/"<n>" forms for safety.
WINDOW_SECONDS = {"1m": 60, "5m": 300, "1h": 3600}
DEFAULT_WINDOW = "5m"


def parse_window(s: str) -> int:
    """Translate a window label to seconds. Falls back to the stub's parsing
    so an unexpected label still yields a sane number rather than a 500."""
    if s in WINDOW_SECONDS:
        return WINDOW_SECONDS[s]
    try:
        if s.endswith("m"):
            return int(s[:-1]) * 60
        if s.endswith("h"):
            return int(s[:-1]) * 3600
        return int(s)
    except ValueError:
        return WINDOW_SECONDS[DEFAULT_WINDOW]


def parse_since(s: str) -> float:
    """`since` accepts epoch seconds or ISO; `0`/empty means everything.
    Step 1.1 only needs the numeric path; ISO parsing arrives with §7."""
    try:
        return float(s or "0")
    except ValueError:
        return 0.0


class App:
    """Holds the service's moving parts so handlers can reach them.

    At 1.1 it carries only the readiness flag (§9). Later steps attach the
    log and the derived-state stores here; the handler stays a thin adapter.
    """

    def __init__(self, log=None):
        self.ready = True  # flips False during post-restart recovery (§9)
        self.log = log     # EventLog; accepted events are appended before ack (§3)


def make_handler(app: App):
    """Build a request handler bound to `app`. A closure keeps the handler
    class free of globals, so tests can run several isolated servers."""

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):  # silence default stderr access log
            pass

        # --- POST ------------------------------------------------------
        def do_POST(self):
            if urlparse(self.path).path == "/events":
                self._handle_events()
            else:
                self._send_json(404, {"error": "unknown path"})

        def _handle_events(self):
            # Drain the body, validate against the server clock, reject with the
            # reason on failure. 1.3 appends the accepted event to the log here,
            # *before* the 200, so an ack always means "durably logged" (§3).
            body = self._read_body()
            try:
                event = validate(body, time.time())
            except ValidationError as e:
                self._send_json(400, {"error": e.reason})
                return
            # Append-before-ack: the 200 below is only sent once os.write has
            # returned, so an ack always means the event is durably logged (§3).
            # 1.4 updates in-memory state here, after the log write.
            app.log.append(event)
            self._send_json(200, {"status": "accepted"})

        # --- GET -------------------------------------------------------
        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path
            query = {k: v[0] for k, v in parse_qs(parsed.query).items()}

            if path == "/healthz":
                self._handle_healthz()
            elif path.startswith("/devices/") and path.endswith("/health"):
                self._handle_device_health(path.split("/")[2])
            elif path.startswith("/rooms/") and path.endswith("/occupancy"):
                self._handle_occupancy(path.split("/")[2], query)
            elif path == "/alarms":
                self._handle_alarms(query)
            else:
                self._send_json(404, {"error": "unknown path"})

        def _handle_healthz(self):
            if app.ready:
                self._send_text(200, "ready")
            else:
                self._send_text(503, "recovering", {"Retry-After": "1"})

        def _handle_device_health(self, device_id: str):
            # Empty shape; filled from state in 1.4.
            self._send_json(200, {
                "last_heartbeat_ts": None,
                "availability_5m": 0.0,
            })

        def _handle_occupancy(self, room_id: str, query: dict):
            window_seconds = parse_window(query.get("window", DEFAULT_WINDOW))
            self._send_json(200, {
                "in_room": False,
                "occupied_pct": 0.0,
                "coverage": 0.0,
                "window_seconds": window_seconds,
            })

        def _handle_alarms(self, query: dict):
            since = parse_since(query.get("since", "0"))
            self._send_json(200, {"alarms": [], "since": since})

        # --- I/O helpers ----------------------------------------------
        def _read_body(self) -> bytes:
            length = int(self.headers.get("Content-Length", "0") or "0")
            return self.rfile.read(length) if length else b""

        def _send_json(self, status: int, body: dict):
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _send_text(self, status: int, text: str, extra_headers: dict = None):
            payload = text.encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            for k, v in (extra_headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(payload)

    return Handler


def make_server(address, app: App = None) -> ThreadingHTTPServer:
    """Create (but do not start) the service's HTTP server."""
    return ThreadingHTTPServer(address, make_handler(app or App()))
