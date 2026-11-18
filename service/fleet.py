"""Fleet launcher (DESIGN §10): one owner + N front workers, pre-fork.

    python -m service.fleet --workers 5

The launcher binds the public port **once**, then forks the workers so they all
accept() on that shared socket (the kernel distributes connections — this is
what actually scales on macOS, where SO_REUSEPORT does not load-balance). The
owner is forked too; it serves reads on an internal port and front workers proxy
GETs to it. `python -m service` remains the single-process service used for dev
and tests.

Stop with SIGINT/SIGTERM to the launcher (it SIGTERMs the children). For a hard
kill, SIGKILL the whole process group — the children are separate processes
(the restart test starts the launcher in its own session and kills the group).
"""

import argparse
import os
import signal
import socket
import sys
import time

from .front import run_front_on_socket
from .owner import run_owner


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int,
                    default=int(os.environ.get("WORKERS", "4")))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8080")))
    ap.add_argument("--owner-port", type=int,
                    default=int(os.environ.get("OWNER_PORT", "8081")))
    ap.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "data"))
    args = ap.parse_args()
    os.makedirs(args.data_dir, exist_ok=True)

    # Bind the shared public socket once, before forking.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("0.0.0.0", args.port))
    sock.listen(2048)

    children = []

    # Owner first (binds its internal port; front GET-proxies target it).
    pid = os.fork()
    if pid == 0:
        sock.close()  # owner doesn't accept on the public socket
        run_owner(args.owner_port, args.data_dir, args.workers)
        os._exit(0)
    children.append(pid)

    # Front workers share the inherited listening socket.
    for i in range(args.workers):
        pid = os.fork()
        if pid == 0:
            run_front_on_socket(sock, i, args.owner_port, args.data_dir)
            os._exit(0)
        children.append(pid)

    sock.close()  # parent only supervises
    print(f"fleet: owner(:{args.owner_port}) + {args.workers} front workers "
          f"on :{args.port} (data: {args.data_dir}, pid {os.getpid()})")

    def shutdown(*_):
        for pid in children:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        for pid in children:
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # If any child dies, bring the whole fleet down.
    while True:
        for pid in list(children):
            try:
                reaped, _ = os.waitpid(pid, os.WNOHANG)
                if reaped:
                    shutdown()
            except ChildProcessError:
                shutdown()
        time.sleep(0.5)


if __name__ == "__main__":
    main()
