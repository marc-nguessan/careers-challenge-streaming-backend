"""Step 5.2 tests: metrics registry + /metrics endpoint (DESIGN §11)."""

import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from service.metrics import Metrics, METRICS
from service.log import EventLog
from service.http_api import App, make_server
from service.state import AppState


class MetricsRegistryTest(unittest.TestCase):
    def test_counter_render(self):
        m = Metrics()
        m.declare("events_accepted_total", "counter", "accepted", ("type",))
        m.inc("events_accepted_total", ("heartbeat",))
        m.inc("events_accepted_total", ("heartbeat",))
        m.inc("events_accepted_total", ("fall_warn",))
        out = m.render()
        self.assertIn("# TYPE events_accepted_total counter", out)
        self.assertIn('events_accepted_total{type="heartbeat"} 2', out)
        self.assertIn('events_accepted_total{type="fall_warn"} 1', out)

    def test_gauge_render(self):
        m = Metrics()
        m.declare("alarm_last_seq", "gauge", "last seq")
        m.set("alarm_last_seq", 7)
        self.assertIn("alarm_last_seq 7", m.render())

    def test_histogram_render(self):
        m = Metrics()
        m.declare("ingest_request_seconds", "histogram", "ingest")
        for v in (0.0005, 0.02, 0.3):
            m.observe("ingest_request_seconds", v)
        out = m.render()
        self.assertIn("# TYPE ingest_request_seconds histogram", out)
        self.assertIn('ingest_request_seconds_bucket{le="+Inf"} 3', out)
        self.assertIn("ingest_request_seconds_count 3", out)
        # cumulative: le=0.001 catches only the 0.0005 sample
        self.assertIn('ingest_request_seconds_bucket{le="0.001"} 1', out)


def iso(epoch):
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(epoch * 1000) % 1000:03d}Z"


class MetricsEndpointTest(unittest.TestCase):
    def setUp(self):
        METRICS.reset()  # isolate from other in-process tests' counters
        self.dir = tempfile.mkdtemp()
        self.event_log = EventLog(os.path.join(self.dir, "events.ndjson"), fsync_interval=0)
        self.alarm_log = EventLog(os.path.join(self.dir, "alarms.ndjson"), fsync_interval=0)
        state = AppState(alarm_log=self.alarm_log)
        self.app = App(log=self.event_log, state=state,
                       alarms_path=os.path.join(self.dir, "alarms.ndjson"))
        self.server = make_server(("127.0.0.1", 0), self.app)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self):
        self.server.shutdown()
        self.event_log.close()
        self.alarm_log.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def post(self, obj):
        body = json.dumps(obj).encode()
        req = Request(f"http://127.0.0.1:{self.port}/events", data=body,
                      method="POST", headers={"Content-Type": "application/json"})
        try:
            with urlopen(req, timeout=5) as resp:
                return resp.status
        except HTTPError as e:
            return e.code  # e.g. 400 for the deliberately malformed event

    def get_text(self, path):
        with urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=5) as resp:
            return resp.status, resp.read().decode()

    def test_metrics_endpoint_reflects_activity(self):
        now = time.time()
        self.post({"device_id": "dev_0001", "room_id": "room_000",
                   "type": "heartbeat", "ts": iso(now), "seq": 1})
        self.post({"device_id": "dev_0002", "room_id": "room_001",
                   "type": "fall_warn", "ts": iso(now), "confidence": 0.9, "seq": 1})
        self.post({"bad": "event"})  # -> rejected malformed

        status, text = self.get_text("/metrics")
        self.assertEqual(status, 200)
        self.assertIn('events_accepted_total{type="heartbeat"} 1', text)
        self.assertIn('events_accepted_total{type="fall_warn"} 1', text)
        self.assertIn('events_rejected_total{reason="malformed"} 1', text)
        self.assertIn("alarms_emitted_total 1", text)
        self.assertIn("alarm_last_seq 1", text)
        self.assertIn("devices_tracked 1", text)
        self.assertIn("ingest_request_seconds_count", text)
        # Never per-device/room labels (§11).
        self.assertNotIn("dev_0001", text)
        self.assertNotIn("room_000", text)


if __name__ == "__main__":
    unittest.main()
