"""Per-device health (DESIGN §5), naive Phase-1.

- last heartbeat = the newest heartbeat `ts` seen (order-independent max);
- availability_5m = distinct whole-seconds carrying a heartbeat in the last
  300 s, divided by 300, computed at query time (a silent device sends nothing,
  so it can only be caught on read).

Naive vs step 2: step 2 replaces the per-device second-set with the fixed
300-slot ring from §5 (bounded memory, O(1) update) and prunes. The query
value is the same; only the storage changes.
"""


class DeviceHealthStore:
    def __init__(self):
        # device_id -> (ts_epoch, ts_iso) of the newest heartbeat
        self._last_hb = {}
        # device_id -> set of int seconds that carry a heartbeat
        self._seconds = {}

    def apply_heartbeat(self, device_id: str, ts_epoch: float, ts_iso: str):
        prev = self._last_hb.get(device_id)
        if prev is None or ts_epoch > prev[0]:
            # Keep the device's own ISO string so the query echoes it exactly,
            # rather than reformatting the epoch and risking ms drift.
            self._last_hb[device_id] = (ts_epoch, ts_iso)
        self._seconds.setdefault(device_id, set()).add(int(ts_epoch))

    def get(self, device_id: str, now: float) -> dict:
        last = self._last_hb.get(device_id)
        seconds = self._seconds.get(device_id, ())
        lo, hi = int(now) - 299, int(now)  # 300 whole seconds, inclusive
        count = sum(1 for s in seconds if lo <= s <= hi)
        return {
            "last_heartbeat_ts": last[1] if last is not None else None,
            "availability_5m": min(count / 300.0, 1.0),
        }
