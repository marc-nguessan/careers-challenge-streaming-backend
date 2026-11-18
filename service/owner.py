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
import os
import threading
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


def run_owner(owner_port: int, data_dir: str, n_workers: int,
              poll_interval: float = 0.0005):
    """The owner process (DESIGN §10): recover from the alarm log + front logs,
    then run an apply loop tailing all front logs while serving reads from live
    state on an internal port. Front workers proxy GETs here.
    """
    # Imported here so unit tests can use LogTailer/pump without the HTTP stack.
    from .http_api import App, make_server
    from .log import EventLog, iter_records
    from .state import AppState

    alarms_path = os.path.join(data_dir, "alarms.ndjson")
    alarm_log = EventLog(alarms_path)
    state = AppState(alarm_log=alarm_log)
    state.load_alarms(iter_records(alarms_path))  # recover alarms first (§7/§9)

    app = App(log=None, state=state)
    app.ready = False

    # One tailer per front log. Starting each at offset 0 means the first drain
    # *is* recovery (replay existing events), after which the same tailers follow
    # live. Idempotent apply absorbs any overlap.
    tailers = [LogTailer(os.path.join(data_dir, f"events.w{i}.ndjson"))
               for i in range(n_workers)]
    while pump(tailers, state) > 0:  # catch up to the end of every front log
        pass
    app.ready = True

    stop = threading.Event()

    def apply_loop():
        while not stop.is_set():
            if pump(tailers, state) == 0:
                time.sleep(poll_interval)  # idle: nothing new to apply

    thread = threading.Thread(target=apply_loop, name="owner-apply", daemon=True)
    thread.start()

    server = make_server(("127.0.0.1", owner_port), app)
    print(f"owner on :{owner_port} tailing {n_workers} front logs "
          f"(alarms resume at seq {state.alarm_last_seq()})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.shutdown()
        alarm_log.close()


def main():
    run_owner(
        owner_port=int(os.environ.get("OWNER_PORT", "8081")),
        data_dir=os.environ.get("DATA_DIR", "data"),
        n_workers=int(os.environ.get("N_WORKERS", "1")),
    )


if __name__ == "__main__":
    main()
