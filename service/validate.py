"""Stateless validation + timestamp parsing (DESIGN §2).

This is the front door. It uses **only the event and the server clock** — no
state lookups — so the service can accept events before post-restart recovery
has finished (§9). Acceptance checks run here, once, at ingest; they are
**never** re-run during log replay, where a replayed event's old `ts` would
wrongly look "too far in the past" (§2).

`ts` is parsed to float epoch seconds here so every downstream structure
(health, occupancy, falls) works in one numeric time base.
"""

import json
from datetime import datetime, timezone

# Reject reasons. These double as `/metrics` labels (§11), so they must be a
# small, bounded set — never anything derived from device_id/room_id.
MALFORMED = "malformed"          # bad JSON, non-object, missing/wrong-typed field, unparseable ts
UNKNOWN_TYPE = "unknown_type"    # `type` is a string but not one we handle
FUTURE_TS = "future_ts"          # ts more than 1h ahead of the server clock
PAST_TS = "past_ts"              # ts more than 1h behind the server clock

# Lateness bounds from the spec: reject >1h future (clearly broken clock),
# reject >1h past. The future bound is explicit in the schema; the past bound
# is our reading of "accept up to 1h in the past" as a hard edge (DESIGN §2, [I]).
FUTURE_LIMIT = 3600.0
PAST_LIMIT = 3600.0

# Required per-type payload fields and their accepted Python types. Envelope
# fields are checked separately. `bool` is handled specially everywhere because
# in Python `bool` is a subclass of `int`.
_PAYLOAD = {
    "heartbeat":   {},
    "presence":    {"in_room": bool},
    "motion":      {"magnitude": (int, float)},
    "sleep_state": {"state": str},
    "fall_warn":   {"confidence": (int, float)},
    "net_status":  {"rssi": (int, float)},
}


class ValidationError(Exception):
    """Raised on any reject. `reason` is one of the constants above."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def parse_ts(ts: str) -> float:
    """ISO 8601 (optionally with a trailing `Z`) -> float epoch seconds.

    Python 3.10's `datetime.fromisoformat` rejects a trailing `Z`; 3.11+
    accepts it. We replace `Z` with `+00:00` first so the code is correct on
    3.10 too (measured: on this box's 3.13 the raw `Z` already parses, so the
    replacement is a no-op here but keeps us portable). A value with no
    timezone is treated as UTC, matching the device wall-clock spec.
    """
    if not isinstance(ts, str):
        raise ValidationError(MALFORMED)
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        raise ValidationError(MALFORMED)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def validate(body: bytes, now: float) -> dict:
    """Validate a raw request body against the server clock `now` (epoch s).

    On success returns the parsed event dict augmented with `ts_epoch` (float).
    On any failure raises ValidationError(reason). The checks are ordered so a
    structurally broken event is reported as such before we look at its clock.
    """
    try:
        event = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        raise ValidationError(MALFORMED)
    if not isinstance(event, dict):
        raise ValidationError(MALFORMED)

    # Envelope: every event must carry these, correctly typed.
    etype = event.get("type")
    if not isinstance(etype, str):
        raise ValidationError(MALFORMED)
    if not isinstance(event.get("device_id"), str):
        raise ValidationError(MALFORMED)
    if not isinstance(event.get("room_id"), str):
        raise ValidationError(MALFORMED)
    # seq is an int; reject bool explicitly (True/False are ints in Python).
    seq = event.get("seq")
    if not isinstance(seq, int) or isinstance(seq, bool):
        raise ValidationError(MALFORMED)
    if not isinstance(event.get("ts"), str):
        raise ValidationError(MALFORMED)

    # Unknown type is its own reason, distinct from a structurally bad field.
    if etype not in _PAYLOAD:
        raise ValidationError(UNKNOWN_TYPE)

    # Per-type payload fields.
    for field, expected in _PAYLOAD[etype].items():
        if field not in event:
            raise ValidationError(MALFORMED)
        value = event[field]
        if expected is bool:
            # in_room must be a real bool, not 0/1.
            if not isinstance(value, bool):
                raise ValidationError(MALFORMED)
        else:
            # Numeric fields: reject bool (it would sneak through isinstance int).
            if isinstance(value, bool) or not isinstance(value, expected):
                raise ValidationError(MALFORMED)

    # ts: parse first (malformed if unparseable), then bound against the clock.
    ts_epoch = parse_ts(event["ts"])
    if ts_epoch > now + FUTURE_LIMIT:
        raise ValidationError(FUTURE_TS)
    if ts_epoch < now - PAST_LIMIT:
        raise ValidationError(PAST_TS)

    event["ts_epoch"] = ts_epoch
    return event
