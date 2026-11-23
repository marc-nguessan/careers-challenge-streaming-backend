"""Step 5.1 tests: SSE live alarm feed (DESIGN §8).

Unit-level tests drive run_alarm_stream against a temp alarm log (format,
replay, Last-Event-ID resume). The integration test proves the HTTP wiring:
connect to /alarms/stream over a real socket and receive an alarm.
"""

import io
import json
import os
import shutil
import socket
import struct
import sys
import tempfile
import threading
import time
import unittest
from urllib.request import Request, urlopen

from service.log import EventLog
from service.http_api import App, make_server, make_handler, QuietThreadingHTTPServer
from service.state import AppState
from service.sse import run_alarm_stream, _format_event


def alarm_record(seq, device_id="dev_0001", ts="iso", ts_epoch=1.0):
    return {"event_id": f"{device_id}:{seq}", "device_id": device_id,
            "room_id": "room_000", "type": "fall_warn", "ts": ts,
            "ts_epoch": ts_epoch, "confidence": 0.9, "seq": seq}


class RunStreamTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "alarms.ndjson")

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def collect(self, last_event_id):
        log = EventLog(self.path, fsync_interval=0)
        log.append(alarm_record(1))
        log.append(alarm_record(2))
        log.close()
        out = []
        # Stop once we've drained the replay (two id: lines at most).
        def send(data):
            out.append(data)
        def stop():
            return sum(b.startswith(b"id:") for b in out) >= (2 - last_event_id)
        run_alarm_stream(send, self.path, last_event_id, stop=stop, poll=0.0)
        return out

    def test_format_event_strips_internal_fields(self):
        b = _format_event(alarm_record(5))
        self.assertTrue(b.startswith(b"id: 5\n"))
        self.assertIn(b"data: ", b)
        self.assertTrue(b.endswith(b"\n\n"))
        payload = json.loads(b.split(b"data: ", 1)[1].split(b"\n\n")[0])
        self.assertEqual(payload["event_id"], "dev_0001:5")
        self.assertNotIn("ts_epoch", payload)  # internal fields not exposed
        self.assertNotIn("rt", payload)

    def test_replays_all_from_zero(self):
        out = self.collect(last_event_id=0)
        self.assertEqual(out[0], b"retry: 1000\n\n")      # retry sent once, first
        ids = [b for b in out if b.startswith(b"id:")]
        self.assertEqual([b.split(b"\n")[0] for b in ids], [b"id: 1", b"id: 2"])

    def test_resume_skips_already_seen(self):
        out = self.collect(last_event_id=1)
        ids = [b.split(b"\n")[0] for b in out if b.startswith(b"id:")]
        self.assertEqual(ids, [b"id: 2"])  # only seq > 1


class SseHttpIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.events_path = os.path.join(self.dir, "events.ndjson")
        self.alarms_path = os.path.join(self.dir, "alarms.ndjson")
        self.event_log = EventLog(self.events_path, fsync_interval=0)
        self.alarm_log = EventLog(self.alarms_path, fsync_interval=0)
        state = AppState(alarm_log=self.alarm_log)
        self.app = App(log=self.event_log, state=state, alarms_path=self.alarms_path)
        self.server = make_server(("127.0.0.1", 0), self.app)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self):
        self.server.shutdown()
        self.event_log.close()
        self.alarm_log.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def _post_fall(self):
        ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + ".000Z"
        body = json.dumps({"device_id": "dev_0002", "room_id": "room_001",
                           "type": "fall_warn", "ts": ts, "confidence": 0.9,
                           "seq": 1}).encode()
        req = Request(f"http://127.0.0.1:{self.port}/events", data=body,
                      method="POST", headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=5) as resp:
            self.assertEqual(resp.status, 200)

    def test_stream_delivers_alarm_over_http(self):
        self._post_fall()  # alarm now in the alarm log
        s = socket.create_connection(("127.0.0.1", self.port), timeout=5)
        s.sendall(b"GET /alarms/stream HTTP/1.1\r\nHost: t\r\n\r\n")
        s.settimeout(5)
        buf = b""
        deadline = time.time() + 5
        while time.time() < deadline and b'"event_id"' not in buf:
            try:
                chunk = s.recv(4096)
                if not chunk:
                    break
                buf += chunk
            except socket.timeout:
                break
        s.close()
        self.assertIn(b"text/event-stream", buf)       # correct content type
        self.assertIn(b"id: 1", buf)                    # SSE id = alarm seq
        self.assertIn(b'"event_id": "dev_0002:1"', buf)

    def test_abrupt_disconnect_keeps_server_responsive(self):
        self._post_fall()
        s = socket.create_connection(("127.0.0.1", self.port), timeout=5)
        s.sendall(b"GET /alarms/stream HTTP/1.1\r\nHost: t\r\n\r\n")
        s.settimeout(5)
        s.recv(256)  # read a little of the stream
        # SO_LINGER (0) makes close() send a TCP RST — a rude disconnect, like
        # Ctrl-C on `curl -N`.
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        s.close()
        # The server must still answer normally afterwards.
        with urlopen(f"http://127.0.0.1:{self.port}/healthz", timeout=5) as resp:
            self.assertEqual(resp.status, 200)


class QuietServerTest(unittest.TestCase):
    """A client vanishing mid-request must not spam a stack trace, but genuine
    errors must still be reported (DESIGN §8/§11)."""

    def _server(self):
        return QuietThreadingHTTPServer(("127.0.0.1", 0), make_handler(App()),
                                        bind_and_activate=False)

    def _handle_with_captured_stderr(self, srv, exc):
        buf = io.StringIO()
        old = sys.stderr
        sys.stderr = buf
        try:
            try:
                raise exc
            except type(exc):
                srv.handle_error(None, ("127.0.0.1", 1))
        finally:
            sys.stderr = old
        return buf.getvalue()

    def test_disconnect_errors_are_silent(self):
        srv = self._server()
        try:
            for exc in (ConnectionResetError(54, "reset"), BrokenPipeError()):
                self.assertEqual(self._handle_with_captured_stderr(srv, exc), "")
        finally:
            srv.server_close()

    def test_real_errors_still_reported(self):
        srv = self._server()
        try:
            out = self._handle_with_captured_stderr(srv, ValueError("boom"))
            self.assertIn("ValueError", out)
        finally:
            srv.server_close()


if __name__ == "__main__":
    unittest.main()
