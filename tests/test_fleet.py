"""Step 4.4 integration test: the pre-fork fleet (DESIGN §10).

Starts the real fleet (launcher -> owner + 2 front workers) in its own process
session, drives it over HTTP (POSTs hit front workers; reads proxy to the
owner), hard-kills the whole process group, restarts on the same data dir, and
asserts recovery. Because the owner applies asynchronously (tailing the front
logs), queries are eventually-consistent, so reads poll until settled.
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


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def iso(epoch):
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


class FleetRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.port = free_port()
        self.owner_port = free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        self.procs = []

    def tearDown(self):
        for p in self.procs:
            self._killgroup(p)
        shutil.rmtree(self.dir, ignore_errors=True)

    def _killgroup(self, p, sig=signal.SIGKILL):
        if p.poll() is None:
            try:
                os.killpg(os.getpgid(p.pid), sig)
            except (ProcessLookupError, OSError):
                pass
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

    def start_fleet(self, workers=2):
        env = dict(os.environ, PORT=str(self.port), OWNER_PORT=str(self.owner_port),
                   DATA_DIR=self.dir)
        p = subprocess.Popen(
            [sys.executable, "-m", "service.fleet", "--workers", str(workers)],
            cwd=ROOT, env=env, start_new_session=True,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.procs.append(p)
        self.wait_ready()
        return p

    def wait_ready(self, timeout=20):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with urlopen(self.base + "/healthz", timeout=1) as resp:
                    if resp.status == 200 and resp.read().strip() == b"ready":
                        return
            except (URLError, HTTPError, OSError):
                time.sleep(0.1)
        self.fail("fleet did not become ready in time")

    def wait_for(self, fn, timeout=10):
        """Poll fn() until it returns truthy (eventual consistency)."""
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                last = fn()
                if last:
                    return last
            except (URLError, HTTPError, OSError):
                pass
            time.sleep(0.05)
        self.fail(f"condition not met in time (last={last})")

    def test_fleet_hard_kill_and_restart(self):
        p1 = self.start_fleet(workers=2)
        now = time.time()

        for i in range(5):
            self.assertEqual(post(self.base, {
                "device_id": "dev_0001", "room_id": "room_000", "type": "heartbeat",
                "ts": iso(now - i), "seq": i + 1}), 200)
        for k in range(3):  # distinct ts -> 3 alarms under exact dedup
            self.assertEqual(post(self.base, {
                "device_id": "dev_0002", "room_id": "room_001", "type": "fall_warn",
                "ts": iso(now - 2 + k), "confidence": 0.9, "seq": 100 + k}), 200)

        # Owner applies asynchronously; wait until it reflects 3 alarms + health.
        self.wait_for(lambda: len(get(self.base, "/alarms?since=0")["alarms"]) == 3)
        self.wait_for(lambda: get(self.base, "/devices/dev_0001/health")["last_heartbeat_ts"])
        hb_before = get(self.base, "/devices/dev_0001/health")["last_heartbeat_ts"]

        # Hard kill the entire fleet process group.
        self._killgroup(p1)

        # Restart on the same data dir.
        self.start_fleet(workers=2)
        alarms = self.wait_for(
            lambda: get(self.base, "/alarms?since=0")["alarms"]
            if len(get(self.base, "/alarms?since=0")["alarms"]) == 3 else None)
        self.assertEqual([a["seq"] for a in alarms], [1, 2, 3])
        self.assertEqual(
            get(self.base, "/devices/dev_0001/health")["last_heartbeat_ts"], hb_before)

        # New distinct fall continues the seq at 4.
        self.assertEqual(post(self.base, {
            "device_id": "dev_0002", "room_id": "room_001", "type": "fall_warn",
            "ts": iso(now + 5), "confidence": 0.9, "seq": 200}), 200)
        final = self.wait_for(
            lambda: get(self.base, "/alarms?since=0")["alarms"]
            if len(get(self.base, "/alarms?since=0")["alarms"]) == 4 else None)
        self.assertEqual([a["seq"] for a in final], [1, 2, 3, 4])


if __name__ == "__main__":
    unittest.main()
