# Submission, Real-time Streaming Backend

**Your name:** Marc N'Guessan
**Email:** arthur.nguessan@gmail.com
**Link to your fork or solution:** https://github.com/marc-nguessan/careers-challenge-streaming-backend

---

## Stack and storage

Python 3.10+, standard library only — no broker, no database. The source of truth is an **append-only NDJSON log** (event sourcing): every accepted event is `os.write`-n to disk *before* the 200, so an ack means durable — it survives `kill -9`, and only power loss needs the background group `fsync`. Derived state (health, occupancy, alarms) is in-memory, rebuilt by replaying the log. The log is also the delivery channel between tiers and the recovery mechanism; the same partitioned-log + offsets model maps straight onto Kafka when one machine isn't enough.

## Ordering and late events

Anything about the world is ordered by event `ts`, never arrival, and each read model self-corrects when history arrives late. Health is a 300-slot per-second ring (idempotent `max`). Occupancy stores presence sorted by `ts` and integrates **on read** from a carry-in at the window's edge, so a 20-minute offline replay retroactively fixes the 1-hour window. Fall dedup keeps per-device anchors and absorbs jitter copies. Clock skew (±30 s) is trusted per the spec; events over 1 h in the future or past are rejected at a stateless front door, so ingest works before recovery finishes.

## Backpressure

Accept must keep up with the burst; apply may lag. One process tops out ~11k ev/s (HTTP + GIL bound — the logic alone does 436k/s), so the scale mode is **pre-fork**: N front workers share one listening socket (the kernel distributes connections; SO_REUSEPORT does *not* load-balance on macOS), each appending to its own log. One owner tails every log and applies, `fall_warn` first. Measured 11k → ~25k ev/s, scaling with cores. There is no in-memory queue between tiers — the log absorbs the burst and SSE subscribers tail it, so a slow consumer never blocks ingest. Nothing is dropped; applying simply trails.

## Restart correctness

Kill `-9` and restart: the alarm log makes alarms durable and `seq` resumes from its max; replaying the event log rebuilds health and occupancy (idempotent apply absorbs overlap). An automated kill-and-restart test asserts alarms, seq, health and occupancy all recover and the next alarm continues in sequence.

## How to run it locally

Python 3.10+, no dependencies.

```bash
make fleet WORKERS=5   # DEFAULT: multi-process, scales to burst, on :8080
# or
make run               # single process: simplest to read, same behaviour, one-core throughput

make smoke             # 30s generator run + scorecard
make test              # unit + integration suite (incl. kill/restart)
make loadgen RATE=50000 DURATION=30   # concurrent burst load
```

Both modes serve the same contract: `POST /events`; `GET /devices/{id}/health`,
`/rooms/{id}/occupancy?window=1m|5m|1h`, `/alarms?since=`, `/alarms/stream` (SSE),
`/metrics`, `/healthz`. `make fleet` is the default for burst scale; `make run` is
the single-process build — identical behaviour, just capped at one core — if you
prefer the simplest thing to read first.

## Reported metrics

- **Sustained ingest rate:** ~11k ev/s single process; ~25k ev/s on a 5-worker fleet (measured with the load generator colocated on a 10-core laptop; scales with cores / client off-box).
- **Alarm feed latency p50 / p95:** ingest → alarm emission p95 ≤ 1 ms (1,914 falls under ~9.6k ev/s); SSE delivery adds ≤ 100 ms (subscriber poll), so the live feed stays well under the 1 s bar. Ingest POST p95 ~5 ms.
- **Behavior under hard kill + restart:** recovers by full log replay (~2 s for 900k events, 436k/s); automated test green.
- **Aggregation correctness on replayed events:** scorecard `alarms == distinct_falls`; occupancy carry-in and late-event cases pass; 77 tests green.

## With another week

Add the periodic state snapshot so recovery loads a checkpoint and replays only the tail (today it replays the whole log, ~2 s), plus accept-during-recovery so the front door never refuses during the restart window. Estimate each device's clock offset from `max(ts − arrival)` and correct windows instead of trusting skew. Partition owner state by `zlib.crc32(room_id)` to apply across cores, and bound occupancy retention to the 1-hour window with a carry-in so memory stays flat under long runs.
