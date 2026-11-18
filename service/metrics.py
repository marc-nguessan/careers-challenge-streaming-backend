"""Metrics registry + Prometheus text exposition (DESIGN §11).

Standard library only. Supports counters, gauges, and histograms with the fixed
bucket set from §11. **Never label by device_id or room_id** — labels are
bounded dimensions only (event type, reject reason), so cardinality stays small.

A process-wide singleton `METRICS` is instrumented on the hot paths; `/metrics`
renders it. In the fleet each process has its own registry (owner metrics are
the ones scraped; front-worker ingest counters are per-process — a documented
limitation of not aggregating across workers).
"""

import threading


def _num(v) -> str:
    return "%d" % v if isinstance(v, int) or v == int(v) else repr(v)


def _labels(names, values) -> str:
    if not names:
        return ""
    return "{" + ",".join(f'{n}="{v}"' for n, v in zip(names, values)) + "}"


class Metrics:
    BUCKETS = (0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)

    def __init__(self):
        self._lock = threading.Lock()
        self._counters = {}  # name -> {labelkey: value}
        self._gauges = {}    # name -> {labelkey: value}
        self._hists = {}     # name -> {labelkey: [bucket_counts, sum, count]}
        self._meta = {}      # name -> (kind, help, labelnames)

    def declare(self, name, kind, help="", labelnames=()):
        with self._lock:
            self._meta[name] = (kind, help, tuple(labelnames))

    def reset(self):
        """Clear all recorded values (keeps declarations). For test isolation —
        the process-wide singleton otherwise accumulates across in-process tests."""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._hists.clear()

    def inc(self, name, labels=(), amount=1.0):
        key = tuple(labels)
        with self._lock:
            d = self._counters.setdefault(name, {})
            d[key] = d.get(key, 0.0) + amount

    def set(self, name, value, labels=()):
        with self._lock:
            self._gauges.setdefault(name, {})[tuple(labels)] = value

    def observe(self, name, value, labels=()):
        key = tuple(labels)
        with self._lock:
            d = self._hists.setdefault(name, {})
            entry = d.get(key)
            if entry is None:
                entry = [[0] * len(self.BUCKETS), 0.0, 0]
                d[key] = entry
            for i, b in enumerate(self.BUCKETS):
                if value <= b:
                    entry[0][i] += 1
            entry[1] += value
            entry[2] += 1

    def render(self) -> str:
        lines = []
        with self._lock:
            names = sorted(set(self._counters) | set(self._gauges) | set(self._hists))
            for name in names:
                kind, help, labelnames = self._meta.get(name, ("untyped", "", ()))
                if help:
                    lines.append(f"# HELP {name} {help}")
                lines.append(f"# TYPE {name} {kind}")
                if name in self._counters:
                    for key, v in sorted(self._counters[name].items()):
                        lines.append(f"{name}{_labels(labelnames, key)} {_num(v)}")
                elif name in self._gauges:
                    for key, v in sorted(self._gauges[name].items()):
                        lines.append(f"{name}{_labels(labelnames, key)} {_num(v)}")
                else:
                    for key, (buckets, total, count) in sorted(self._hists[name].items()):
                        le_names = labelnames + ("le",)
                        for i, b in enumerate(self.BUCKETS):
                            lines.append(f"{name}_bucket"
                                         f"{_labels(le_names, key + ('%g' % b,))} {buckets[i]}")
                        lines.append(f"{name}_bucket{_labels(le_names, key + ('+Inf',))} {count}")
                        lines.append(f"{name}_sum{_labels(labelnames, key)} {_num(total)}")
                        lines.append(f"{name}_count{_labels(labelnames, key)} {count}")
        return "\n".join(lines) + "\n"


# Process-wide registry, declared up front so types/help appear even at zero.
METRICS = Metrics()
METRICS.declare("events_accepted_total", "counter", "events accepted at ingest", ("type",))
METRICS.declare("events_rejected_total", "counter", "events rejected at ingest", ("reason",))
METRICS.declare("ingest_request_seconds", "histogram", "POST /events wall time")
METRICS.declare("events_applied_total", "counter", "events applied to state", ("type",))
METRICS.declare("alarms_emitted_total", "counter", "distinct fall alarms emitted")
METRICS.declare("alarm_latency_seconds", "histogram", "ingest receive -> alarm emission")
METRICS.declare("alarm_last_seq", "gauge", "highest alarm seq emitted")
METRICS.declare("devices_tracked", "gauge", "devices with health state")
METRICS.declare("rooms_tracked", "gauge", "rooms with occupancy state")
METRICS.declare("presence_events_retained", "gauge", "presence events held across rooms")
