"""Step 3.2 integration test: hard kill + restart recovery (DESIGN §9, §13).

Runs the real service as a subprocess, drives it over HTTP, SIGKILLs it (no
clean shutdown, no fsync-on-close), restarts it on the same data dir, and
asserts state and the alarm seq were recovered from the logs.
"""

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def iso(epoch: float) -> str:
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(epoch * 1000) % 1000:03d}Z"


def post(base, obj):
    data = json.dumps(obj).encode()
    req = Request(base + "/events", data=data, method="POST",
                  headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=5) as resp:
        return resp.status


def get(base, path):
    with urlopen(base + path, timeout=5) as resp:
        return json.loads(resp.read())


class RestartRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.port = free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        self.procs = []

    def tearDown(self):
        for p in self.procs:
            if p.poll() is None:
                p.kill()
                p.wait(timeout=5)
        shutil.rmtree(self.dir, ignore_errors=True)

    def start_service(self):
        env = dict(os.environ, PORT=str(self.port), DATA_DIR=self.dir)
        p = subprocess.Popen([sys.executable, "-m", "service"], cwd=ROOT, env=env,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.procs.append(p)
        self.wait_ready()
        return p

    def wait_ready(self, timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with urlopen(self.base + "/healthz", timeout=1) as resp:
                    if resp.status == 200 and resp.read().strip() == b"ready":
                        return
            except (URLError, HTTPError, OSError):
                time.sleep(0.1)
        self.fail("service did not become ready in time")

    def test_hard_kill_and_restart_recovers_state_and_seq(self):
        p1 = self.start_service()
        now = time.time()

        # Heartbeats for a device (health), presence for a room (occupancy),
        # and three distinct falls for another device (3 alarms, seq 1..3).
        for i in range(5):
            self.assertEqual(post(self.base, {
                "device_id": "dev_0001", "room_id": "room_000",
                "type": "heartbeat", "ts": iso(now - i), "seq": i + 1}), 200)
        self.assertEqual(post(self.base, {
            "device_id": "dev_0001", "room_id": "room_000", "type": "presence",
            "ts": iso(now - 30), "in_room": True, "seq": 10}), 200)
        for k in range(3):  # distinct ts -> distinct alarms under exact dedup
            self.assertEqual(post(self.base, {
                "device_id": "dev_0002", "room_id": "room_001", "type": "fall_warn",
                "ts": iso(now - 2 + k), "confidence": 0.9, "seq": 100 + k}), 200)

        alarms = get(self.base, "/alarms?since=0")["alarms"]
        self.assertEqual([a["seq"] for a in alarms], [1, 2, 3])
        health_before = get(self.base, "/devices/dev_0001/health")
        self.assertIsNotNone(health_before["last_heartbeat_ts"])

        # Hard kill: SIGKILL, no clean shutdown, no fsync-on-close.
        p1.send_signal(signal.SIGKILL)
        p1.wait(timeout=5)

        # Restart on the same data dir.
        self.start_service()

        # Alarms survived with their seqs; health recovered from replay.
        alarms2 = get(self.base, "/alarms?since=0")["alarms"]
        self.assertEqual([a["seq"] for a in alarms2], [1, 2, 3])
        health_after = get(self.base, "/devices/dev_0001/health")
        self.assertEqual(health_after["last_heartbeat_ts"],
                         health_before["last_heartbeat_ts"])
        occ = get(self.base, "/rooms/room_000/occupancy?window=1h")
        self.assertTrue(occ["in_room"])  # replayed presence

        # A new distinct fall continues the seq at 4 (never reused).
        self.assertEqual(post(self.base, {
            "device_id": "dev_0002", "room_id": "room_001", "type": "fall_warn",
            "ts": iso(now + 5), "confidence": 0.9, "seq": 200}), 200)
        alarms3 = get(self.base, "/alarms?since=0")["alarms"]
        self.assertEqual([a["seq"] for a in alarms3], [1, 2, 3, 4])


if __name__ == "__main__":
    unittest.main()
