"""Restart recovery (DESIGN §9).

On startup we rebuild in-memory state from the durable logs before serving:

  1) load the alarm log -> alarm list, per-device anchors, seq counter;
  2) replay the event log -> health / occupancy, and re-emit any fall whose
     alarm was not yet persisted (idempotent apply absorbs the overlap).

Validation is NOT re-run on replay: the records were accepted once at ingest and
already carry `ts_epoch`; re-checking their age would wrongly reject old events
(§2). The health ring guard uses the recovery wall clock `now`, so stale
heartbeats update last_hb but don't mark the ring (§5).

Phase 1 replays the whole event log. Step 3.3 adds a snapshot so recovery only
replays the tail past the snapshot's offset.
"""

import time

from .log import iter_records


def recover(app, events_path: str, alarms_path: str, now: float = None) -> int:
    """Rebuild `app.state` from the logs. Returns the number of events replayed."""
    if now is None:
        now = time.time()
    # Alarms first, so replayed falls see their anchors and absorb instead of
    # re-emitting (unless the alarm was never persisted).
    app.state.load_alarms(iter_records(alarms_path))
    replayed = 0
    for record in iter_records(events_path):
        app.state.apply(record, now=now)
        replayed += 1
    return replayed
