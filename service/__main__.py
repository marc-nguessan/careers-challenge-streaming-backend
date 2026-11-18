"""Service entrypoint: `python -m service`.

Step 1.1: construct the HTTP server (§1) and serve. Later steps insert
recovery (§9) *before* serving and start background timers (fsync, snapshot).
"""

import os

from .http_api import App, make_server

PORT = int(os.environ.get("PORT", "8080"))


def main():
    app = App()
    server = make_server(("0.0.0.0", PORT), app)
    print(f"Teton streaming backend listening on :{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down")
        server.shutdown()


if __name__ == "__main__":
    main()
