"""Step 1.4 tests: naive in-memory derived state (DESIGN §5-7, naive).

These assert the *naive* behaviour that step 1 promises: shapes are correct,
updates land, exact duplicates are absorbed, and the cheap §13 wins hold (fall
jitter -> one alarm). The full §13 suite — occupancy carry-in / unknown-start,
the W=5s fall debounce, alarm-seq-across-restart — arrives with the correct
algorithms in step 2.
"""

import unittest

from service.state import AppState


def heartbeat(device_id, ts_epoch, ts_iso=None, seq=1):
    return {"type": "heartbeat", "device_id": device_id, "room_id": "room_000",
            "ts": ts_iso or "2026-01-01T00:00:00.000Z", "ts_epoch": ts_epoch, "seq": seq}


def presence(room_id, ts_epoch, in_room, seq=1):
    return {"type": "presence", "device_id": "dev_0001", "room_id": room_id,
            "ts": "2026-01-01T00:00:00.000Z", "ts_epoch": ts_epoch,
            "in_room": in_room, "seq": seq}


def fall(device_id, ts_epoch, ts_iso, seq, confidence=0.9):
    return {"type": "fall_warn", "device_id": device_id, "room_id": "room_000",
            "ts": ts_iso, "ts_epoch": ts_epoch, "confidence": confidence, "seq": seq}


class HealthStateTest(unittest.TestCase):
    def test_last_heartbeat_is_the_newest_iso(self):
        s = AppState()
        s.apply(heartbeat("dev_0001", 100.0, "2026-01-01T00:01:40.000Z", seq=1), now=200.0)
        s.apply(heartbeat("dev_0001", 50.0, "2026-01-01T00:00:50.000Z", seq=2), now=200.0)  # older
        h = s.device_health("dev_0001", now=200.0)
        self.assertEqual(h["last_heartbeat_ts"], "2026-01-01T00:01:40.000Z")  # max ts wins

    def test_availability_ring_distinct_seconds_with_gap(self):  # §13 health
        # 300s window [701,1000]. 208 distinct seconds (160 in 701..860, 48 in
        # 901..948, a 40s gap 861..900), plus two extra heartbeats at second 701
        # (three sharing one second). 210 heartbeats total -> availability
        # == 208/300 (and <= 208/300 per the spec wording).
        s = AppState()
        now = 1000.0
        seconds = list(range(701, 861)) + list(range(901, 949))  # 160 + 48 = 208
        self.assertEqual(len(seconds), 208)
        for sec in seconds:
            s.apply(heartbeat("dev_0001", float(sec)), now=now)
        for _ in range(2):  # two extra at second 701 -> three total there
            s.apply(heartbeat("dev_0001", 701.0), now=now)
        h = s.device_health("dev_0001", now=now)
        self.assertLessEqual(h["availability_5m"], 208 / 300)
        self.assertAlmostEqual(h["availability_5m"], 208 / 300, places=9)

    def test_old_heartbeat_updates_last_hb_not_ring(self):
        # A heartbeat older than now-300 sets last_hb but must not mark the ring
        # (can't affect any window; avoids clobbering a fresher slot).
        s = AppState()
        s.apply(heartbeat("dev_0001", 100.0, "2026-01-01T00:01:40.000Z"), now=1000.0)
        h = s.device_health("dev_0001", now=1000.0)
        self.assertEqual(h["last_heartbeat_ts"], "2026-01-01T00:01:40.000Z")
        self.assertEqual(h["availability_5m"], 0.0)  # ring untouched

    def test_duplicate_heartbeat_idempotent(self):  # §13 duplicate
        s = AppState()
        now = 1000.0
        s.apply(heartbeat("dev_0001", 950.0), now=now)
        before = s.device_health("dev_0001", now=now)["availability_5m"]
        s.apply(heartbeat("dev_0001", 950.0), now=now)  # same second again
        after = s.device_health("dev_0001", now=now)["availability_5m"]
        self.assertEqual(before, after)
        self.assertAlmostEqual(after, 1 / 300)

    def test_unknown_device_is_empty_shape(self):
        s = AppState()
        h = s.device_health("ghost", now=1000.0)
        self.assertIsNone(h["last_heartbeat_ts"])
        self.assertEqual(h["availability_5m"], 0.0)


class OccupancyStateTest(unittest.TestCase):
    def test_latest_presence_wins_for_in_room(self):
        s = AppState()
        s.apply(presence("room_000", 10.0, True))
        s.apply(presence("room_000", 20.0, False))
        occ = s.room_occupancy("room_000", window_s=60, now=30.0)
        self.assertEqual(occ["in_room"], False)  # newest ts decides

    def test_occupied_pct_three_options(self):  # §13 (occupied_pct only; naive)
        # True at -25m, False at -10m, window=1h -> 15m True / 60m = 0.25.
        s = AppState()
        now = 3600.0
        s.apply(presence("room_000", now - 25 * 60, True))
        s.apply(presence("room_000", now - 10 * 60, False))
        occ = s.room_occupancy("room_000", window_s=3600, now=now)
        self.assertAlmostEqual(occ["occupied_pct"], 15 / 60, places=6)

    def test_duplicate_presence_absorbed(self):
        s = AppState()
        now = 100.0
        s.apply(presence("room_000", 10.0, True))
        s.apply(presence("room_000", 10.0, True))  # identical -> no effect (§7)
        occ = s.room_occupancy("room_000", window_s=200, now=now)
        self.assertEqual(occ["in_room"], True)

    def test_unknown_room_is_empty_shape(self):
        s = AppState()
        occ = s.room_occupancy("ghost", window_s=60, now=1.0)
        self.assertEqual(occ["in_room"], False)
        self.assertEqual(occ["occupied_pct"], 0.0)
        self.assertEqual(occ["coverage"], 0.0)
        self.assertEqual(occ["window_seconds"], 60)


class FallStateTest(unittest.TestCase):
    def test_jitter_copies_one_alarm(self):  # §13 fall jitter
        s = AppState()
        alarms = []
        for seq in (1, 2, 3):  # identical ts, new seq each (sensor jitter)
            a = s.apply(fall("dev_0001", 500.0, "2026-01-01T00:08:20.000Z", seq))
            if a:
                alarms.append(a)
        self.assertEqual(len(alarms), 1)
        self.assertEqual(len(s.alarms_since(0)), 1)

    def test_distinct_ts_two_alarms(self):
        s = AppState()
        s.apply(fall("dev_0001", 500.0, "2026-01-01T00:08:20.000Z", 1))
        s.apply(fall("dev_0001", 530.0, "2026-01-01T00:08:50.000Z", 2))
        self.assertEqual(len(s.alarms_since(0)), 2)

    def test_alarm_shape_and_seq(self):
        s = AppState()
        a = s.apply(fall("dev_0007", 500.0, "2026-01-01T00:08:20.000Z", 42, confidence=0.88))
        self.assertEqual(a["event_id"], "dev_0007:42")   # device_id:anchor_seq
        self.assertEqual(a["device_id"], "dev_0007")
        self.assertEqual(a["room_id"], "room_000")
        self.assertEqual(a["type"], "fall_warn")
        self.assertEqual(a["ts"], "2026-01-01T00:08:20.000Z")
        self.assertEqual(a["confidence"], 0.88)
        self.assertEqual(a["seq"], 1)
        self.assertNotIn("ts_epoch", a)  # internal field not exposed

    def test_since_filters_by_ts(self):
        s = AppState()
        s.apply(fall("dev_0001", 100.0, "2026-01-01T00:01:40.000Z", 1))
        s.apply(fall("dev_0001", 200.0, "2026-01-01T00:03:20.000Z", 2))
        self.assertEqual(len(s.alarms_since(0)), 2)      # since=0 -> all
        self.assertEqual(len(s.alarms_since(150.0)), 1)  # only ts>150


if __name__ == "__main__":
    unittest.main()
