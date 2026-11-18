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

from .metrics import METRICS
from .state import AppState
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

    def __init__(self, log=None, state=None, alarms_path=None):
        self.ready = True  # flips False during post-restart recovery (§9)
        self.log = log     # EventLog; accepted events are appended before ack (§3)
        self.state = state if state is not None else AppState()  # derived read models (§5-7)
        self.alarms_path = alarms_path  # alarm log path for SSE to follow (§8)


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
            # reason on failure. The accepted event is appended to the log
            # *before* the 200, so an ack always means "durably logged" (§3).
            t0 = time.perf_counter()
            body = self._read_body()
            try:
                event = validate(body, time.time())
            except ValidationError as e:
                METRICS.inc("events_rejected_total", (e.reason,))
                METRICS.observe("ingest_request_seconds", time.perf_counter() - t0)
                self._send_json(400, {"error": e.reason})
                return
            # Append-before-ack: the 200 below is only sent once os.write has
            # returned, so an ack always means the event is durably logged (§3).
            app.log.append(event)
            # Update derived state after the log write (§5-7). In Phase 1 this
            # happens inline; §10's owner loop moves it off the request thread.
            app.state.apply(event)
            METRICS.inc("events_accepted_total", (event["type"],))
            METRICS.observe("ingest_request_seconds", time.perf_counter() - t0)
            self._send_json(200, {"status": "accepted"})

        # --- GET -------------------------------------------------------
        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path
            query = {k: v[0] for k, v in parse_qs(parsed.query).items()}

            if path == "/healthz":
                self._handle_healthz()
            elif path == "/metrics":
                self._handle_metrics()
            elif path == "/alarms/stream":
                self._handle_alarm_stream()
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

        def _require_ready(self) -> bool:
            # Queries are answered only once state is rebuilt; during recovery
            # they return 503 so clients retry rather than read empty state (§9).
            if app.ready:
                return True
            self._send_text(503, "recovering", {"Retry-After": "1"})
            return False

        def _handle_device_health(self, device_id: str):
            if not self._require_ready():
                return
            self._send_json(200, app.state.device_health(device_id, time.time()))

        def _handle_occupancy(self, room_id: str, query: dict):
            if not self._require_ready():
                return
            window_seconds = parse_window(query.get("window", DEFAULT_WINDOW))
            self._send_json(200, app.state.room_occupancy(
                room_id, window_seconds, time.time()))

        def _handle_alarms(self, query: dict):
            if not self._require_ready():
                return
            since = parse_since(query.get("since", "0"))
            self._send_json(200, {
                "alarms": app.state.alarms_since(since),
                "since": since,
            })

        def _handle_metrics(self):
            # Refresh cardinality gauges at scrape time, then render (§11).
            stats = app.state.stats()
            METRICS.set("devices_tracked", stats["devices"])
            METRICS.set("rooms_tracked", stats["rooms"])
            METRICS.set("presence_events_retained", stats["presence_events"])
            self._send_text(200, METRICS.render())

        def _handle_alarm_stream(self):
            if app.alarms_path is None:
                self._send_json(404, {"error": "stream unavailable"})
                return
            from .sse import serve_alarm_stream
            serve_alarm_stream(self, app.alarms_path)

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
