"""Step 4.3 tests: owner apply loop (DESIGN §10).

The owner tails several front-worker log files with per-file byte offsets and
applies new events into shared state, fall_warn first (lowest alarm latency).
These test the tailer's incremental/partial-line behaviour and the pump's
cross-file apply + priority ordering.
"""

import json
import os
import shutil
import tempfile
import unittest

from service.log import EventLog
from service.owner import LogTailer, order_for_apply, pump
from service.state import AppState


def hb(device_id, ts_epoch, seq=1):
    return {"type": "heartbeat", "device_id": device_id, "room_id": "room_000",
            "ts": f"iso-{ts_epoch}", "ts_epoch": ts_epoch, "seq": seq}


def fall(device_id, ts_epoch, seq=1):
    return {"type": "fall_warn", "device_id": device_id, "room_id": "room_000",
            "ts": f"iso-{ts_epoch}", "ts_epoch": ts_epoch, "confidence": 0.9, "seq": seq}


class LogTailerTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "events.ndjson")

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_missing_file_is_empty(self):
        t = LogTailer(self.path)
        self.assertEqual(t.poll(), [])

    def test_incremental_reads_only_new(self):
        log = EventLog(self.path, fsync_interval=0)
        log.append(hb("dev_0001", 1.0))
        log.append(hb("dev_0001", 2.0))
        t = LogTailer(self.path)
        first = t.poll()
        self.assertEqual(len(first), 2)
        log.append(hb("dev_0001", 3.0))
        second = t.poll()
        self.assertEqual([r["ts_epoch"] for r in second], [3.0])  # only the new one
        log.close()

    def test_partial_final_line_held_until_complete(self):
        # Write a complete record and a half-written one (crash/in-flight write).
        with open(self.path, "wb") as f:
            f.write(b'{"a":1}\n{"a":2')
        t = LogTailer(self.path)
        self.assertEqual(t.poll(), [{"a": 1}])     # partial line not yet yielded
        with open(self.path, "ab") as f:
            f.write(b'}\n')                         # complete the line
        self.assertEqual(t.poll(), [{"a": 2}])     # now yielded, no dup of a:1


class OrderTest(unittest.TestCase):
    def test_falls_first_preserving_arrival_order(self):
        batch = [hb("d", 1.0, 1), fall("d", 2.0, 2), hb("d", 3.0, 3),
                 fall("d", 4.0, 4)]
        ordered = order_for_apply(batch)
        self.assertEqual([r["type"] for r in ordered],
                         ["fall_warn", "fall_warn", "heartbeat", "heartbeat"])
        self.assertEqual([r["ts_epoch"] for r in ordered], [2.0, 4.0, 1.0, 3.0])


class PumpTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_pump_applies_across_files_and_emits_alarm(self):
        p1 = os.path.join(self.dir, "events.w0.ndjson")
        p2 = os.path.join(self.dir, "events.w1.ndjson")
        log1 = EventLog(p1, fsync_interval=0)
        log2 = EventLog(p2, fsync_interval=0)
        log1.append(hb("dev_0001", 100.0))
        log2.append(fall("dev_0002", 100.0))
        state = AppState()
        tailers = [LogTailer(p1), LogTailer(p2)]

        n = pump(tailers, state, now=100.0)
        self.assertEqual(n, 2)
        self.assertEqual(len(state.alarms_since(0)), 1)                 # fall applied
        self.assertIsNotNone(
            state.device_health("dev_0001", now=100.0)["last_heartbeat_ts"])

        # Second pump with nothing new -> no work, no duplicate alarm.
        self.assertEqual(pump(tailers, state, now=100.0), 0)
        self.assertEqual(len(state.alarms_since(0)), 1)
        log1.close()
        log2.close()


if __name__ == "__main__":
    unittest.main()
