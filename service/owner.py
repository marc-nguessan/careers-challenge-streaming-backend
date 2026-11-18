"""Owner apply loop: tail many front-worker logs into shared state (DESIGN §10).

In the fleet topology, N front-worker processes each append accepted events to
their **own** log file; one owner process tails all of them and maintains the
derived state. The log is the delivery channel — there is no in-memory queue
between the tiers, so a front worker that is killed loses nothing already
written, and the owner resumes from its per-file offset.

`LogTailer` reads one file incrementally by byte offset and only advances past
**complete** newline-terminated lines, so a half-written final line (an in-flight
or crash-torn write) is held back until it is finished. `pump` reads every
tailer, orders the batch fall_warn-first (alarms get the least latency, §10
priority), and applies it.
"""

import json
import time


class LogTailer:
    def __init__(self, path: str):
        self.path = path
        self.offset = 0  # bytes consumed (end of the last complete line)

    def poll(self) -> list:
        """Return records written since the last poll; advance the offset only
        past complete lines."""
        try:
            with open(self.path, "rb") as f:
                f.seek(self.offset)
                data = f.read()
        except FileNotFoundError:
            return []
        if not data:
            return []

        records = []
        start = 0
        consumed = 0
        while True:
            nl = data.find(b"\n", start)
            if nl == -1:
                break  # no newline past here -> partial tail, leave unconsumed
            line = data[start:nl].strip()
            start = nl + 1
            consumed = start
            if line:
                try:
                    records.append(json.loads(line))
                except ValueError:
                    pass  # skip a corrupt line, keep going
        self.offset += consumed
        return records


def order_for_apply(batch: list) -> list:
    """fall_warn first (lowest alarm latency), then everything else (§10).
    Stable within each group, so arrival order is preserved."""
    falls = [r for r in batch if r.get("type") == "fall_warn"]
    rest = [r for r in batch if r.get("type") != "fall_warn"]
    return falls + rest


def pump(tailers: list, state, now: float = None) -> int:
    """Read all tailers once and apply the combined batch. Returns the number of
    events applied."""
    if now is None:
        now = time.time()
    batch = []
    for tailer in tailers:
        batch.extend(tailer.poll())
    for record in order_for_apply(batch):
        state.apply(record, now=now)
    return len(batch)
