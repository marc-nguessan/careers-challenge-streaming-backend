"""Falls / alarms (DESIGN §7).

Fall debounce per device. An alarm is absorbed if it lands within a threshold
of its nearest already-emitted anchor:

    anchors = sorted ts of falls already emitted for this device
    on fall_warn at ts t:
        a = anchor nearest to t
        if a exists and |t - a| <= threshold:  absorb (emit nothing)
        else:                                  new fall -> emit alarm, insert t

Two strategies set the threshold:
  - "exact" (default): threshold 0 — absorb only identical-ts copies. The
    provided generator models jitter as identical-ts copies and counts every
    other fall as distinct, so exact matches its ground truth exactly (measured:
    alarms == distinct_falls). Chosen as the default on that measurement.
  - "window": threshold W (default 5 s) — absorb copies that arrive within a few
    seconds with drifting timestamps (a real sensor). More robust in the field,
    but on this generator it wrongly merges two distinct same-device falls that
    land within W (measured: alarms < distinct_falls by the number of such pairs).

Both share this machinery and its guarantees: idempotent (an exact duplicate is
distance 0 from its own anchor); a late fall far from every anchor always raises
an alarm, however old its ts; bridging (a late event within the threshold of two
emitted alarms) attaches to the nearest anchor (tie -> earlier) and emits
nothing; emitted alarms are never merged.

Alarm record (additive to the stub's shape):
  event_id = "<device_id>:<anchor_seq>" (the device's seq of the event that
  created the alarm — stable and unique), device_id, room_id, type, ts (the
  anchor's genuine device timestamp), confidence, seq (a strictly increasing
  alarm counter). `ts_epoch` is kept internally for `since` filtering/ordering
  and stripped from query output.

Deferred to step 3 (§9): persisting alarms to an append-only alarm log and
resuming `seq` across restart. In memory only here, so `seq` resets on restart.
"""

import bisect


class FallStore:
    def __init__(self, strategy: str = "exact", window: float = 5.0):
        # "exact" absorbs only identical-ts copies (threshold 0); "window"
        # absorbs anything within W. See module docstring for why exact is the
        # default on this generator.
        self._strategy = strategy
        self._threshold = 0.0 if strategy == "exact" else window
        self._anchors = {}  # device_id -> sorted list of anchor ts_epoch
        self._alarms = []   # alarm dicts in emission (seq) order
        self._seq = 0

    def apply_fall(self, event: dict):
        device_id = event["device_id"]
        t = event["ts_epoch"]
        anchors = self._anchors.setdefault(device_id, [])

        nearest = self._nearest(anchors, t)
        if nearest is not None and abs(t - nearest) <= self._threshold:
            return None  # jitter copy / bridged / exact duplicate -> absorbed

        bisect.insort(anchors, t)  # new distinct fall becomes an anchor
        self._seq += 1
        alarm = {
            "event_id": f"{device_id}:{event['seq']}",  # device's seq of anchor
            "device_id": device_id,
            "room_id": event["room_id"],
            "type": "fall_warn",
            "ts": event["ts"],      # the anchor's genuine device timestamp
            "ts_epoch": t,          # internal: since-filter + ordering
            "confidence": event.get("confidence"),
            "seq": self._seq,
        }
        self._alarms.append(alarm)
        return _public(alarm)

    @staticmethod
    def _nearest(anchors: list, t: float):
        """The anchor closest to t; on a tie, the earlier (smaller ts)."""
        if not anchors:
            return None
        i = bisect.bisect_left(anchors, t)
        candidates = []
        if i < len(anchors):
            candidates.append(anchors[i])
        if i > 0:
            candidates.append(anchors[i - 1])
        return min(candidates, key=lambda a: (abs(a - t), a))

    def query(self, since_epoch: float) -> list:
        # Literal contract: alarms with ts > since; since=0 returns all (§1).
        return [_public(a) for a in self._alarms if a["ts_epoch"] > since_epoch]


def _public(alarm: dict) -> dict:
    """Alarm as exposed to clients — without the internal ts_epoch."""
    return {k: v for k, v in alarm.items() if k != "ts_epoch"}
