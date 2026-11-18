"""Service entrypoint: `python -m service`.

Startup order (§9): open the event log and the alarm log, recover state from
them, then serve. Recovery is synchronous here — the socket isn't bound until
state is rebuilt, so queries never see a half-recovered service. (Accepting
POSTs *during* recovery, §9, is an availability refinement for later.)
"""

import os
import time

from .http_api import App, make_server
from .log import EventLog
from .recovery import recover
from .state import AppState

PORT = int(os.environ.get("PORT", "8080"))
DATA_DIR = os.environ.get("DATA_DIR", "data")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    events_path = os.path.join(DATA_DIR, "events.ndjson")
    alarms_path = os.path.join(DATA_DIR, "alarms.ndjson")

    alarm_log = EventLog(alarms_path)
    state = AppState(alarm_log=alarm_log)
    event_log = EventLog(events_path)
    app = App(log=event_log, state=state, alarms_path=alarms_path)

    app.ready = False
    t0 = time.time()
    replayed = recover(app, events_path, alarms_path)
    app.ready = True
    print(f"Recovered {replayed} events in {time.time() - t0:.3f}s; "
          f"alarms resume at seq {state.alarm_last_seq()}")

    server = make_server(("0.0.0.0", PORT), app)
    print(f"Teton streaming backend listening on :{PORT} "
          f"(data: {DATA_DIR})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down")
    finally:
        server.shutdown()
        event_log.close()
        alarm_log.close()


if __name__ == "__main__":
    main()
