"""Per-device health (DESIGN §5).

- `last_hb`: the newest heartbeat `ts` seen (`max`; O(1); order-independent).
- `ring`: 300 slots indexed by `int(ts) % 300`. Each slot stores the integer
  second it currently holds (a *tag*), or -1. Marking a heartbeat is
  `ring[s % 300] = s` — idempotent, and self-expiring: a slot is only "live"
  if its tag falls inside the query window, so a stale tag from 300 s ago is
  simply not counted.
- `availability_5m` = number of slots whose tag is in `[now_s - 299, now_s]`,
  divided by 300, computed **at query time** (a silent device sends nothing, so
  an outage can only be noticed on read).

Why the ring is bounded and correct: within any 300-wide window `[now-299, now]`
no two in-window seconds map to the same slot (they'd have to be exactly 300 s
apart, which can't both sit in a 300-integer span). So each slot contributes at
most one valid tag, and 300 slots exactly cover the window.

Ring guard: a heartbeat whose second is older than `now - 299` at apply time can
never land in a future window, and writing it could clobber a fresher tag in the
same slot (e.g. second 700 and second 1000 share slot 100). Such heartbeats
update `last_hb` but skip the ring. This also makes replay safe: old replayed
events don't disturb the ring (§9).
"""


class DeviceHealthStore:
    RING = 300  # one slot per second of the 5-minute window

    def __init__(self):
        self._last_hb = {}  # device_id -> (ts_epoch, ts_iso) of newest heartbeat
        self._ring = {}     # device_id -> list[RING] of int second-tags (-1 = empty)

    def apply_heartbeat(self, device_id: str, ts_epoch: float, ts_iso: str, now: float):
        prev = self._last_hb.get(device_id)
        if prev is None or ts_epoch > prev[0]:
            # Keep the device's own ISO string so the query echoes it exactly.
            self._last_hb[device_id] = (ts_epoch, ts_iso)

        s = int(ts_epoch)
        if s < int(now) - (self.RING - 1):
            return  # too old to matter; last_hb updated above, ring untouched
        ring = self._ring.get(device_id)
        if ring is None:
            ring = [-1] * self.RING
            self._ring[device_id] = ring
        ring[s % self.RING] = s  # idempotent; a duplicate re-writes the same slot

    def get(self, device_id: str, now: float) -> dict:
        last = self._last_hb.get(device_id)
        ring = self._ring.get(device_id)
        count = 0
        if ring is not None:
            lo, hi = int(now) - (self.RING - 1), int(now)
            count = sum(1 for tag in ring if tag >= 0 and lo <= tag <= hi)
        return {
            "last_heartbeat_ts": last[1] if last is not None else None,
            "availability_5m": count / self.RING,
        }
