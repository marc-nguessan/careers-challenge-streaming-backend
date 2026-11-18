"""Derived state (DESIGN §5-7), naive Phase-1 implementation.

`AppState` owns the three read models and a single lock. Every accepted event
is `apply`-ed after the log write; queries read under the same lock. Step 1
uses one coarse lock (simple and correct under ThreadingHTTPServer); the
single-threaded owner/apply loop of §10 replaces it when we measure the need.

What's naive here (made correct in step 2):
  - occupancy ignores carry-in, so a room whose only presence event predates the
    window reads 0% instead of carrying that state forward (§6, §13 carry-in);
  - fall dedup is exact-ts only, not the W=5s debounce (§7). Locally the
    generator's jitter copies share an identical ts, so exact dedup already
    yields one alarm per distinct fall;
  - alarms live in memory only (no alarm log / seq-across-restart yet, §7/§9).
"""

import threading
import time

from .health import DeviceHealthStore
from .occupancy import RoomOccupancyStore
from .falls import FallStore


class AppState:
    def __init__(self, fall_strategy="exact", fall_window=5.0, alarm_log=None):
        self._lock = threading.Lock()
        self.health = DeviceHealthStore()
        self.occupancy = RoomOccupancyStore()
        self.falls = FallStore(strategy=fall_strategy, window=fall_window,
                               alarm_log=alarm_log)

    def load_alarms(self, records) -> None:
        """Restore persisted alarms before replaying the event log (§9)."""
        with self._lock:
            self.falls.load(records)

    def alarm_last_seq(self) -> int:
        with self._lock:
            return self.falls.last_seq()

    def apply(self, event: dict, now: float = None):
        """Update derived state from one accepted event (called after the log
        write). `now` is the apply-time server clock (defaults to wall clock);
        health uses it for its ring guard (§5). Returns a new alarm dict for a
        fresh fall, else None. motion / sleep_state / net_status are logged but
        build no derived state (§3)."""
        if now is None:
            now = time.time()
        etype = event["type"]
        ts = event["ts_epoch"]
        with self._lock:
            if etype == "heartbeat":
                self.health.apply_heartbeat(event["device_id"], ts, event["ts"], now)
            elif etype == "presence":
                self.occupancy.apply_presence(event["room_id"], ts, event["in_room"])
            elif etype == "fall_warn":
                return self.falls.apply_fall(event)
            return None

    def device_health(self, device_id: str, now: float) -> dict:
        with self._lock:
            return self.health.get(device_id, now)

    def room_occupancy(self, room_id: str, window_s: int, now: float) -> dict:
        with self._lock:
            return self.occupancy.get(room_id, window_s, now)

    def alarms_since(self, since_epoch: float) -> list:
        with self._lock:
            return self.falls.query(since_epoch)
