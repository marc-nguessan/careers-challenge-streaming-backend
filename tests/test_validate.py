"""Step 1.2 tests: stateless validation + ts parsing (DESIGN §2, §13).

These drive `service.validate.validate`, which takes a raw request body and
the server clock and either returns a normalized event (with `ts_epoch`) or
raises ValidationError(reason). Pure function, no server needed.
"""

import json
import unittest
from datetime import datetime, timezone

from service.validate import (
    validate, parse_ts, ValidationError,
    MALFORMED, UNKNOWN_TYPE, FUTURE_TS, PAST_TS, FUTURE_LIMIT, PAST_LIMIT,
)

NOW = 1_700_000_000.0  # fixed server clock for deterministic bound tests


def ts_at(epoch: float) -> str:
    """Epoch seconds -> ISO 8601 with ms and a trailing Z, like the generator."""
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(round(epoch * 1000)) % 1000:03d}Z"


def body(now=NOW, **overrides) -> bytes:
    """A valid heartbeat body, with field overrides. Pass a key set to the
    sentinel `_OMIT` to drop it entirely."""
    e = {
        "device_id": "dev_0001",
        "room_id": "room_000",
        "type": "heartbeat",
        "ts": ts_at(now),
        "seq": 1,
    }
    e.update(overrides)
    e = {k: v for k, v in e.items() if v is not _OMIT}
    return json.dumps(e).encode()


_OMIT = object()


class ValidateTest(unittest.TestCase):
    def assertReason(self, expected, body_bytes, now=NOW):
        with self.assertRaises(ValidationError) as cm:
            validate(body_bytes, now)
        self.assertEqual(cm.exception.reason, expected)

    # --- happy paths ---------------------------------------------------
    def test_valid_heartbeat(self):
        e = validate(body(), NOW)
        self.assertEqual(e["type"], "heartbeat")
        self.assertAlmostEqual(e["ts_epoch"], NOW, places=3)

    def test_trailing_z_parses(self):  # §13
        # The point is that the trailing Z parses; keep `now` near the ts so the
        # time-bound checks don't fire. 2026-05-23T18:53:49.123Z == 1779562429.123.
        e = validate(body(ts="2026-05-23T18:53:49.123Z"), now=1_779_562_429.0)
        self.assertAlmostEqual(e["ts_epoch"], 1_779_562_429.123, places=3)

    def test_presence_valid(self):
        validate(body(type="presence", in_room=True), NOW)

    def test_motion_valid(self):
        validate(body(type="motion", magnitude=0.81), NOW)

    def test_fall_warn_valid(self):
        validate(body(type="fall_warn", confidence=0.92), NOW)

    def test_net_status_valid(self):
        validate(body(type="net_status", rssi=-68), NOW)

    def test_sleep_state_valid(self):
        validate(body(type="sleep_state", state="asleep"), NOW)

    # --- the §13 validation cases -------------------------------------
    def test_missing_type_malformed(self):  # §13
        self.assertReason(MALFORMED, body(type=_OMIT))

    def test_future_ts_rejected(self):  # §13
        self.assertReason(FUTURE_TS, body(ts=ts_at(NOW + 2 * 3600)))

    # --- time bounds (§2) ---------------------------------------------
    def test_past_ts_rejected(self):
        self.assertReason(PAST_TS, body(ts=ts_at(NOW - 2 * 3600)))

    def test_future_boundary_ok_then_reject(self):
        validate(body(ts=ts_at(NOW + FUTURE_LIMIT)), NOW)          # exactly 1h: ok
        self.assertReason(FUTURE_TS, body(ts=ts_at(NOW + FUTURE_LIMIT + 2)))

    def test_past_boundary_ok_then_reject(self):
        validate(body(ts=ts_at(NOW - PAST_LIMIT)), NOW)            # exactly 1h: ok
        self.assertReason(PAST_TS, body(ts=ts_at(NOW - PAST_LIMIT - 2)))

    # --- structure / types --------------------------------------------
    def test_unknown_type(self):
        self.assertReason(UNKNOWN_TYPE, body(type="frobnicate"))

    def test_malformed_json(self):
        self.assertReason(MALFORMED, b"{not json")

    def test_non_object_json(self):
        self.assertReason(MALFORMED, b"[1, 2, 3]")
        self.assertReason(MALFORMED, b"5")

    def test_missing_device_id(self):
        self.assertReason(MALFORMED, body(device_id=_OMIT))

    def test_missing_ts(self):
        self.assertReason(MALFORMED, body(ts=_OMIT))

    def test_seq_must_be_int(self):
        self.assertReason(MALFORMED, body(seq="1"))
        self.assertReason(MALFORMED, body(seq=1.5))
        self.assertReason(MALFORMED, body(seq=True))  # bool is not a valid seq

    def test_presence_requires_bool_in_room(self):
        self.assertReason(MALFORMED, body(type="presence", in_room=_OMIT))
        self.assertReason(MALFORMED, body(type="presence", in_room="yes"))

    def test_motion_magnitude_must_be_number(self):
        self.assertReason(MALFORMED, body(type="motion", magnitude="x"))
        self.assertReason(MALFORMED, body(type="motion", magnitude=True))  # bool != number

    # --- ts parsing edge cases ----------------------------------------
    def test_naive_ts_treated_as_utc(self):
        # No timezone designator -> treated as UTC (device wall-clock spec).
        got = parse_ts("2026-05-23T18:53:49.123")
        want = parse_ts("2026-05-23T18:53:49.123Z")
        self.assertEqual(got, want)

    def test_unparseable_ts_malformed(self):
        self.assertReason(MALFORMED, body(ts="not-a-timestamp"))


if __name__ == "__main__":
    unittest.main()
