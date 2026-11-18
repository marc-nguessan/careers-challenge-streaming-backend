"""Concurrent load generator (DESIGN §10).

The provided `event_generator/` sends events **sequentially** (one blocking POST
at a time), so it tops out around a couple thousand events/s and cannot produce
the grader's 10x burst. This tool drives many **keep-alive** connections in
parallel to measure the service's real accept throughput and POST latency under
load — including whether a single process sustains 50,000 events/s.

Standard library only: `http.client` for persistent connections, threads for
concurrency. Each worker pins one connection and reuses it (reconnecting on
error), which is what lets us push far past the stub's per-request-connection
ceiling.

    python tools/loadgen.py --rate 50000 --duration 30
    python tools/loadgen.py --concurrency 64 --duration 10      # unbounded (find ceiling)
    python tools/loadgen.py --rate 50000 --duration 30 --fall-every 200
"""

import argparse
import json
import threading
import time
from datetime import datetime, timezone
from http.client import HTTPConnection
from urllib.parse import urlparse

HEADERS = {"Content-Type": "application/json"}


def iso(epoch: float) -> str:
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(epoch * 1000) % 1000:03d}Z"


def make_body(worker_id: int, seq: int, devices: int, fall_every: int) -> bytes:
    # Spread load across `devices` device ids; emit a fall occasionally so the
    # alarm path is exercised too (distinct ts -> distinct alarm under exact).
    dev = (worker_id * 100003 + seq) % devices
    now = time.time()
    e = {
        "device_id": f"dev_{dev:05d}",
        "room_id": f"room_{dev // 2:05d}",
        "type": "heartbeat",
        "ts": iso(now),
        "seq": seq,
    }
    if fall_every and seq % fall_every == 0:
        e["type"] = "fall_warn"
        e["confidence"] = 0.9
    return json.dumps(e).encode()


def worker(wid, host, port, stop_at, interval, devices, fall_every, out):
    conn = HTTPConnection(host, port, timeout=5)
    sent = failed = 0
    latencies = []
    seq = 0
    # perf_counter schedule for paced mode; absolute so we don't drift.
    next_send = time.perf_counter()
    while time.time() < stop_at:
        if interval:
            next_send += interval
            delay = next_send - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
        seq += 1
        body = make_body(wid, seq, devices, fall_every)
        t0 = time.perf_counter()
        try:
            conn.request("POST", "/events", body, HEADERS)
            resp = conn.getresponse()
            resp.read()  # must drain to reuse the keep-alive connection
            if resp.status == 200:
                sent += 1
            else:
                failed += 1
        except OSError:
            failed += 1
            try:
                conn.close()
            except OSError:
                pass
            conn = HTTPConnection(host, port, timeout=5)
        latencies.append(time.perf_counter() - t0)
    conn.close()
    out[wid] = (sent, failed, latencies)


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    k = min(len(sorted_vals) - 1, int(round((p / 100.0) * (len(sorted_vals) - 1))))
    return sorted_vals[k]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="http://localhost:8080")
    ap.add_argument("--duration", type=float, default=30.0)
    ap.add_argument("--rate", type=float, default=0.0,
                    help="target events/s across all workers; 0 = unbounded (ceiling)")
    ap.add_argument("--concurrency", type=int, default=64,
                    help="number of concurrent connections/threads")
    ap.add_argument("--devices", type=int, default=5000)
    ap.add_argument("--fall-every", type=int, default=0,
                    help="every Nth event per worker is a fall_warn (0 = none)")
    args = ap.parse_args()

    u = urlparse(args.target)
    host, port = u.hostname, u.port or 80
    # Per-worker pacing interval (seconds between sends). Unbounded if rate=0.
    interval = (args.concurrency / args.rate) if args.rate else 0.0

    out = {}
    threads = []
    stop_at = time.time() + args.duration
    t_start = time.time()
    for wid in range(args.concurrency):
        t = threading.Thread(target=worker, args=(
            wid, host, port, stop_at, interval, args.devices, args.fall_every, out))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    elapsed = time.time() - t_start

    sent = sum(o[0] for o in out.values())
    failed = sum(o[1] for o in out.values())
    latencies = sorted(l for o in out.values() for l in o[2])

    print(f"target        {args.target}")
    print(f"concurrency   {args.concurrency}   rate target "
          f"{'unbounded' if not args.rate else int(args.rate)}/s")
    print(f"duration      {elapsed:.1f}s")
    print(f"accepted      {sent}   failed {failed}")
    print(f"achieved      {sent / elapsed:,.0f} events/s")
    if latencies:
        print(f"POST latency  p50 {percentile(latencies,50)*1e3:.2f} ms   "
              f"p95 {percentile(latencies,95)*1e3:.2f} ms   "
              f"p99 {percentile(latencies,99)*1e3:.2f} ms   "
              f"max {latencies[-1]*1e3:.2f} ms")


if __name__ == "__main__":
    main()
