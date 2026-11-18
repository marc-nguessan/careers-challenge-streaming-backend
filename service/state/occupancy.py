"""Per-room occupancy (DESIGN §6).

Per room we keep presence events `(ts_epoch, in_room)` sorted by ts, merged
across the room's devices.

- `in_room` = the value of the newest event (latest presence wins, across
  devices and regardless of age).
- `occupied_pct(window)` integrates time spent True over `[now - window, now]`,
  **starting from the state that was active at the window's start** — found by
  bisect as the last event with `ts <= start` (the "carry-in"). Without this, a
  room whose only event predates the window reads 0% instead of carrying that
  state forward.
- `coverage` = fraction of the window for which the state is *known*. Before the
  first event ever seen for the room the state is unknown (counts as neither
  occupied nor covered); unknown time is excluded from both numerator (occupied)
  and the coverage count, but the denominator is always the full window.
- Late events are inserted in ts order (`bisect.insort`), and because everything
  is computed on read, history corrects itself when an older event arrives.

Retention note: Phase 1 keeps every presence event. Bounded retention
(`[now-1h, now]` plus a single carry-in, §6) is a memory optimisation to add
under load testing (step 4); it does not change any value computed here.
"""

import bisect


class RoomOccupancyStore:
    def __init__(self):
        self._events = {}  # room_id -> sorted list of (ts_epoch, in_room)

    def apply_presence(self, room_id: str, ts_epoch: float, in_room: bool):
        evs = self._events.setdefault(room_id, [])
        entry = (ts_epoch, in_room)
        # Idempotent: an identical (ts, in_room) is a duplicate event and must
        # not change anything (§7). Rooms are small, so O(n) membership is fine.
        if entry in evs:
            return
        bisect.insort(evs, entry)  # keep sorted by ts (then by in_room)

    def get(self, room_id: str, window_s: int, now: float) -> dict:
        evs = self._events.get(room_id, [])
        if not evs:
            return {"in_room": False, "occupied_pct": 0.0, "coverage": 0.0,
                    "window_seconds": window_s}

        in_room = evs[-1][1]  # newest event wins, any age
        start = now - window_s

        # Carry-in: the state active at the window's start is the last event with
        # ts <= start. (start, True) is the largest key for second `start`, so
        # bisect_right counts every event with ts <= start.
        idx = bisect.bisect_right(evs, (start, True))
        state = evs[idx - 1][1] if idx > 0 else None  # None = unknown

        occupied = 0.0  # seconds known-True in the window
        known = 0.0     # seconds with known state in the window
        cursor = start
        for ts, val in evs[idx:]:
            if ts >= now:
                break  # events at/after now don't contribute to [start, now]
            dur = ts - cursor
            if state is not None:
                known += dur
                if state:
                    occupied += dur
            cursor = ts
            state = val
        # Tail: from the last transition up to now.
        dur = now - cursor
        if state is not None:
            known += dur
            if state:
                occupied += dur

        return {
            "in_room": in_room,
            "occupied_pct": min(occupied / window_s, 1.0),
            "coverage": min(known / window_s, 1.0),
            "window_seconds": window_s,
        }
