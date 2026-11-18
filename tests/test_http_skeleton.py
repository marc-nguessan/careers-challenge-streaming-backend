"""Step 1.1 tests: HTTP routing + response shapes (DESIGN §1, §9).

These assert the *contract surface* only. At 1.1 the service accepts every
POST without validation or logging and returns empty-but-correctly-shaped
GET responses; later sub-steps fill in behaviour without changing shapes.
"""

import json
import threading
import unittest
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from service.http_api import make_server


def start_server():
    """Start the real service on an ephemeral port in a background thread.

    Returns (base_url, server). Port 0 lets the OS pick a free port so tests
    never collide with a running service or each other.
    """
    server = make_server(("127.0.0.1", 0))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return f"http://127.0.0.1:{port}", server


def get(url):
    """GET returning (status, parsed-json-or-text)."""
    try:
        with urlopen(url, timeout=5) as resp:
            body = resp.read()
            return resp.status, _parse(resp, body)
    except HTTPError as e:
        return e.code, _parse(e, e.read())


def post(url, obj):
    data = json.dumps(obj).encode()
    req = Request(url, data=data, method="POST",
                  headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=5) as resp:
            return resp.status, _parse(resp, resp.read())
    except HTTPError as e:
        return e.code, _parse(e, e.read())


def _parse(resp, body):
    ctype = resp.headers.get("Content-Type", "")
    if "application/json" in ctype:
        return json.loads(body)
    return body.decode()


class HttpSkeletonTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base, cls.server = start_server()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    # POST /events -------------------------------------------------------
    def test_post_events_accepts_and_returns_200(self):
        status, body = post(self.base + "/events", {"anything": "goes"})
        self.assertEqual(status, 200)
        self.assertEqual(body, {"status": "accepted"})

    def test_post_unknown_path_404(self):
        status, _ = post(self.base + "/nope", {})
        self.assertEqual(status, 404)

    # /healthz -----------------------------------------------------------
    def test_healthz_ready(self):
        status, body = get(self.base + "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(body.strip(), "ready")

    # GET shapes ---------------------------------------------------------
    def test_device_health_shape(self):
        status, body = get(self.base + "/devices/dev_0000/health")
        self.assertEqual(status, 200)
        self.assertIsNone(body["last_heartbeat_ts"])
        self.assertEqual(body["availability_5m"], 0.0)

    def test_room_occupancy_shape(self):
        status, body = get(self.base + "/rooms/room_000/occupancy?window=1m")
        self.assertEqual(status, 200)
        self.assertEqual(body["in_room"], False)
        self.assertEqual(body["occupied_pct"], 0.0)
        self.assertEqual(body["coverage"], 0.0)
        self.assertEqual(body["window_seconds"], 60)  # 1m -> 60s

    def test_room_occupancy_default_window_5m(self):
        _, body = get(self.base + "/rooms/room_000/occupancy")
        self.assertEqual(body["window_seconds"], 300)

    def test_alarms_shape(self):
        status, body = get(self.base + "/alarms?since=0")
        self.assertEqual(status, 200)
        self.assertEqual(body["alarms"], [])
        self.assertEqual(body["since"], 0.0)

    def test_get_unknown_path_404(self):
        status, _ = get(self.base + "/nope")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
