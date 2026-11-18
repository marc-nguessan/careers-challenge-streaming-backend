"""Falls / alarms (DESIGN §7), naive Phase-1.

Dedup here is **exact-ts per device**: the first fall at a given `ts` for a
device emits an alarm; later falls at that same `ts` (the sensor's jitter
copies, which share an identical `ts` — [V]) are absorbed. This yields one
alarm per distinct fall locally.

Alarm record (additive to the stub's shape):
  event_id = "<device_id>:<anchor_seq>", device_id, room_id, type, ts
  (the device's ISO timestamp), confidence, seq (a strictly increasing alarm
  counter). `ts_epoch` is kept internally for `since` filtering/ordering and
  stripped from query output.

Naive vs step 2: step 2 replaces exact-ts with the W=5s debounce (§7),
persists alarms to an append-only alarm log, and resumes `seq` across restart
(§9). In memory only for now, so `seq` resets on restart.
"""


class FallStore:
    def __init__(self):
        self._seen = {}     # device_id -> set of ts_epoch already alarmed
        self._alarms = []   # alarm dicts in emission (seq) order
        self._seq = 0

    def apply_fall(self, event: dict):
        device_id = event["device_id"]
        ts_epoch = event["ts_epoch"]
        seen = self._seen.setdefault(device_id, set())
        if ts_epoch in seen:
            return None  # jitter copy / exact duplicate -> absorbed (§7)
        seen.add(ts_epoch)

        self._seq += 1
        alarm = {
            "event_id": f"{device_id}:{event['seq']}",  # device's seq, stable+unique
            "device_id": device_id,
            "room_id": event["room_id"],
            "type": "fall_warn",
            "ts": event["ts"],          # the anchor's genuine device timestamp
            "ts_epoch": ts_epoch,       # internal: for since-filter + ordering
            "confidence": event.get("confidence"),
            "seq": self._seq,
        }
        self._alarms.append(alarm)
        return _public(alarm)

    def query(self, since_epoch: float) -> list:
        # Literal contract: alarms with ts > since; since=0 returns all (§1).
        return [_public(a) for a in self._alarms if a["ts_epoch"] > since_epoch]


def _public(alarm: dict) -> dict:
    """Alarm as exposed to clients — without the internal ts_epoch."""
    return {k: v for k, v in alarm.items() if k != "ts_epoch"}
