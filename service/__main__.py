"""Service entrypoint: `python -m service`.

Step 1.3: open the append-only log, then construct and serve the HTTP API.
Later steps insert recovery (§9) *before* serving and start the snapshot timer.
"""

import os

from .http_api import App, make_server
from .log import EventLog

PORT = int(os.environ.get("PORT", "8080"))
DATA_DIR = os.environ.get("DATA_DIR", "data")


def main():
    log = EventLog(os.path.join(DATA_DIR, "events.ndjson"))
    app = App(log=log)
    server = make_server(("0.0.0.0", PORT), app)
    print(f"Teton streaming backend listening on :{PORT} (log: {DATA_DIR}/events.ndjson)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down")
    finally:
        server.shutdown()
        log.close()  # fsync + close so a clean stop loses nothing


if __name__ == "__main__":
    main()
