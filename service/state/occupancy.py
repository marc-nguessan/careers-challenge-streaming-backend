"""Per-room occupancy (DESIGN §6), naive Phase-1.

Per room we keep presence events `(ts_epoch, in_room)` sorted by ts, merged
across the room's devices. `in_room` is the value of the newest event (latest
presence wins, across devices). `occupied_pct(window)` integrates time spent
True inside `[now - window, now]`; `coverage` is the fraction of the window for
which we have known state.

NAIVE / known bug (fixed in step 2, §13 carry-in): we do **not** carry the
state active at the window's start. A room whose only presence event is older
than the window therefore reads 0% here, even though that state should carry
forward. `in_room` is still correct (newest event, any age). Step 2 adds the
carry-in and bisect-from-window-start logic.
"""

import bisect


class RoomOccupancyStore:
    def __init__(self):
        self._events = {}  # room_id -> sorted list of (ts_epoch, in_room)

    def apply_presence(self, room_id: str, ts_epoch: float, in_room: bool):
        evs = self._events.setdefault(room_id, [])
        entry = (ts_epoch, in_room)
        # Idempotent: an identical (ts, in_room) already present is a duplicate
        # event and must not change anything (§7). Rooms are small, so the O(n)
        # membership test is fine for Phase 1.
        if entry in evs:
            return
        bisect.insort(evs, entry)  # keep sorted by ts (then by in_room)

    def get(self, room_id: str, window_s: int, now: float) -> dict:
        evs = self._events.get(room_id, [])
        empty = {"in_room": False, "occupied_pct": 0.0, "coverage": 0.0,
                 "window_seconds": window_s}
        if not evs:
            return empty

        in_room = evs[-1][1]  # newest event wins, regardless of age
        start = now - window_s
        in_window = [e for e in evs if e[0] >= start]
        if not in_window:
            # All events precede the window: naive returns the latest in_room but
            # 0 pct/coverage. This is the carry-in bug called out above.
            return {"in_room": in_room, "occupied_pct": 0.0, "coverage": 0.0,
                    "window_seconds": window_s}

        occupied = 0.0  # seconds known-True inside the window
        known = 0.0     # seconds with any known state inside the window
        prev_ts, prev_val = in_window[0]
        for ts, val in in_window[1:]:
            dur = ts - prev_ts
            known += dur
            if prev_val:
                occupied += dur
            prev_ts, prev_val = ts, val
        # Tail: from the last event up to `now`.
        dur = now - prev_ts
        known += dur
        if prev_val:
            occupied += dur

        return {
            "in_room": in_room,
            "occupied_pct": min(occupied / window_s, 1.0),
            "coverage": min(known / window_s, 1.0),
            "window_seconds": window_s,
        }
