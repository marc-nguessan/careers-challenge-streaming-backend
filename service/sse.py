"""Live alarm feed over SSE (DESIGN §8).

Served by **following the alarm log file**: on connect we replay alarms with
`seq > Last-Event-ID`, then tail the file for new ones. Because every subscriber
reads the file independently at its own pace, a slow consumer can never block the
ingest/apply pipeline — the alarm log is the buffer. If a consumer is so slow its
socket backs up, the write raises and we drop that connection; the client resumes
from its last id. This file-follow model is identical in the single process and
in the fleet, where front workers tail the owner's alarm log (§10).

Wire format (§8): `retry: 1000` once, then `id: <seq>\ndata: <json>\n\n` per
alarm, and `: ping\n\n` every 15 s. HTTP framing is chunked so the connection
stays valid while streaming indefinitely.
"""

import json
import time
from urllib.parse import urlparse, parse_qs

from .owner import LogTailer


def _format_event(record: dict) -> bytes:
    seq = record.get("seq", 0)
    public = {k: v for k, v in record.items() if k not in ("ts_epoch", "rt")}
    return f"id: {seq}\ndata: {json.dumps(public)}\n\n".encode()


def run_alarm_stream(send, alarms_path: str, last_event_id: int, stop=None,
                     ping_interval: float = 15.0, poll: float = 0.1):
    """Drive one SSE subscriber. `send(bytes)` must raise on disconnect. Replays
    alarms with seq > last_event_id from the log, then tails it live. `stop()`
    (tests) ends the loop; in production the loop ends when `send` raises."""
    send(b"retry: 1000\n\n")
    tailer = LogTailer(alarms_path)
    last_ping = time.monotonic()
    while stop is None or not stop():
        wrote = False
        for record in tailer.poll():
            if record.get("seq", 0) <= last_event_id:
                continue  # already delivered before the client's last id
            send(_format_event(record))
            wrote = True
        now = time.monotonic()
        if now - last_ping >= ping_interval:
            send(b": ping\n\n")  # keep-alive comment
            last_ping = now
        if not wrote:
            if stop is not None and stop():
                break
            time.sleep(poll)


def parse_last_event_id(handler) -> int:
    """SSE reconnect id from the Last-Event-ID header, else a ?last_event_id
    query param, else 0 (replay everything)."""
    raw = handler.headers.get("Last-Event-ID")
    if raw is None:
        q = parse_qs(urlparse(handler.path).query)
        raw = (q.get("last_event_id") or ["0"])[0]
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def serve_alarm_stream(handler, alarms_path: str):
    """Serve GET /alarms/stream on a BaseHTTPRequestHandler. Sends chunked SSE
    headers, then streams until the client disconnects."""
    last_event_id = parse_last_event_id(handler)
    # SSE is a terminal response: when it ends (client disconnects) we close the
    # connection rather than loop back to read another request on a socket the
    # client has already dropped (which would raise mid-handler).
    handler.close_connection = True
    handler.send_response(200)
    handler.send_header("Content-Type", "text/event-stream")
    handler.send_header("Cache-Control", "no-cache")
    handler.send_header("Transfer-Encoding", "chunked")
    handler.end_headers()

    def send(data: bytes):
        # One HTTP chunk per SSE block: <hex len>CRLF <data> CRLF.
        handler.wfile.write(b"%X\r\n" % len(data) + data + b"\r\n")
        handler.wfile.flush()

    try:
        run_alarm_stream(send, alarms_path, last_event_id)
    except (BrokenPipeError, ConnectionResetError, OSError, ValueError):
        pass  # client went away; nothing to clean up (the log is the buffer)
