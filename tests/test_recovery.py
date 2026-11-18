"""Step 3 tests: alarm-log persistence and recovery (DESIGN §7, §9).

3.1 covers the alarm log: emitted alarms are durable, and on restart the alarm
seq resumes from the highest persisted seq (never recomputed). The kill/restart
integration test is added in 3.2.
"""

import os
import shutil
import tempfile
import unittest

from service.log import EventLog, iter_records
from service.state.falls import FallStore


def fall(device_id, ts_epoch, seq, confidence=0.9):
    return {"type": "fall_warn", "device_id": device_id, "room_id": "room_000",
            "ts": f"iso-{ts_epoch}", "ts_epoch": ts_epoch,
            "confidence": confidence, "seq": seq}


class AlarmLogTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "alarms.ndjson")

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_alarm_seq_resumes_across_restart(self):  # §13 alarm seq across restart
        # Emit 3 alarms, persist, "restart" by loading a fresh store from the
        # same log; the next alarm must be seq 4.
        log1 = EventLog(self.path, fsync_interval=0)
        s1 = FallStore(alarm_log=log1)
        for i, t in enumerate((100.0, 200.0, 300.0), start=1):
            self.assertIsNotNone(s1.apply_fall(fall("dev_0001", t, i)))
        self.assertEqual(len(s1.query(0)), 3)
        log1.close()

        # Restart: new store, load persisted alarms, resume.
        log2 = EventLog(self.path, fsync_interval=0)
        s2 = FallStore(alarm_log=log2)
        s2.load(iter_records(self.path))
        self.assertEqual(len(s2.query(0)), 3)           # alarms survived
        self.assertEqual(s2.last_seq(), 3)              # seq resumed
        new = s2.apply_fall(fall("dev_0001", 400.0, 9))  # distinct fall
        self.assertEqual(new["seq"], 4)                 # next seq is 4
        log2.close()

    def test_loaded_anchors_absorb_replayed_duplicates(self):
        # After loading persisted alarms, replaying the same fall (same ts) must
        # not emit a second alarm — the anchor is already there (idempotent §7).
        log1 = EventLog(self.path, fsync_interval=0)
        s1 = FallStore(alarm_log=log1)
        s1.apply_fall(fall("dev_0001", 500.0, 1))
        log1.close()

        log2 = EventLog(self.path, fsync_interval=0)
        s2 = FallStore(alarm_log=log2)
        s2.load(iter_records(self.path))
        self.assertIsNone(s2.apply_fall(fall("dev_0001", 500.0, 2)))  # same ts
        self.assertEqual(len(s2.query(0)), 1)
        log2.close()

    def test_iter_records_skips_torn_final_line(self):
        # A crash can leave a half-written final line. iter_records must yield
        # the whole records and drop the torn tail.
        with open(self.path, "w") as f:
            f.write('{"seq":1}\n{"seq":2}\n{"seq":3')  # last line has no newline
        records = list(iter_records(self.path))
        self.assertEqual([r["seq"] for r in records], [1, 2])


if __name__ == "__main__":
    unittest.main()
