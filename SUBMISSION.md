# Submission, Real-time Streaming Backend

**Your name:** Marc N'Guessan
**Email:** arthur.nguessan@gmail.com
**Link to your fork or solution:** https://github.com/marc-nguessan/careers-challenge-streaming-backend

---

## Stack and storage

Python 3.10+, standard library only: no broker, no database. The source of truth is an **append-only NDJSON log** (event sourcing). Every accepted event is `os.write`-n before the 200, so an ack survives `kill -9`; only power loss needs the background group `fsync`. Derived state (health, occupancy, alarms) lives in memory and is rebuilt by replaying the log. The same partitioned-log + offsets model maps onto Kafka when one machine isn't enough.

## Ordering and late events

Everything about the world is ordered by event `ts`, never arrival, and each read model self-corrects when history arrives late. Health is a 300-slot per-second ring (idempotent `max`). Occupancy keeps presence sorted by `ts` and integrates **on read** from a carry-in at the window edge, so a 20-minute offline replay retroactively fixes the 1-hour window. Fall dedup keeps per-device anchors and absorbs jitter copies. Clock skew (±30 s) is trusted per the spec; events over 1 h in the future or past are rejected at a stateless front door.

## Backpressure

Accept must keep up with the burst; apply may lag. One process tops out near 11k ev/s (HTTP and GIL bound; the logic alone does 436k/s), so the scale mode is **pre-fork**: N workers share one listening socket, each appending to its own log. One owner tails every log and applies, `fall_warn` first. Measured: 11k to ~25k ev/s, scaling with cores. No in-memory queue sits between tiers, so a slow SSE consumer never blocks ingest. Nothing is dropped; applying just trails.

## Restart correctness

After `kill -9`, alarms are durable and `seq` resumes from the alarm log's max; replaying the event log rebuilds health and occupancy (idempotent apply absorbs overlap). An automated kill-and-restart test asserts this, including that the next alarm continues the sequence.

## With another week

Periodic state snapshots so recovery replays only the tail (today the full log, ~2 s), and accepting events during recovery. Estimate each device's clock offset from `max(ts − arrival)` instead of trusting skew. Partition owner state by `zlib.crc32(room_id)` to apply across cores, and bound occupancy retention to one hour so memory stays flat.


## How to run it locally

Python 3.10+, no dependencies.

```bash
make run               # DEFAULT: single process on :8080 — simplest, cleanest restart
# or, for burst scale:
make fleet WORKERS=5   # multi-process pre-fork: N front workers share the port + one owner

make smoke             # 30s generator run + scorecard
make test              # unit + integration suite (incl. kill/restart)
make loadgen RATE=50000 DURATION=30   # concurrent burst load
```

Both modes serve the same contract: `POST /events`; `GET /devices/{id}/health`,
`/rooms/{id}/occupancy?window=1m|5m|1h`, `/alarms?since=`, `/alarms/stream` (SSE),
`/metrics`, `/healthz`. `make run` is the default: one process, so a hard kill and
restart is unambiguous — it rebinds cleanly and recovers from the log. `make fleet`
adds pre-fork workers for burst throughput (~25k ev/s); stop it by killing the
process group (e.g. Ctrl-C the launcher) so no workers orphan.

## Reported metrics

- **Sustained ingest rate:** ~11k ev/s single process; ~25k ev/s on a 5-worker fleet (measured with the load generator colocated on a 10-core laptop; scales with cores / client off-box).
- **Alarm feed latency p50 / p95:** ingest → alarm emission p95 ≤ 1 ms (1,914 falls under ~9.6k ev/s); SSE delivery adds ≤ 100 ms (subscriber poll), so the live feed stays well under the 1 s bar. Ingest POST p95 ~5 ms.
- **Behavior under hard kill + restart:** recovers by full log replay (~2 s for 900k events, 436k/s); automated test green.
- **Aggregation correctness on replayed events:** scorecard `alarms == distinct_falls`; occupancy carry-in and late-event cases pass; 77 tests green.
