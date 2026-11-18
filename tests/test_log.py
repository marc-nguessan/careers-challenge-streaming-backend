"""Step 1.3 tests: append-only NDJSON log (DESIGN §3).

Covers the ack-durability guarantee (os.write before return, visible without
fsync), monotonic offsets, and corruption-free concurrent appends.
"""

import json
import os
import shutil
import tempfile
import threading
import unittest

from service.log import EventLog


class LogTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "events.ndjson")

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def read_lines(self):
        with open(self.path) as f:
            return [json.loads(line) for line in f if line.strip()]

    def test_append_writes_line_with_rt(self):
        log = EventLog(self.path, fsync_interval=0)
        log.append({"device_id": "dev_0001", "type": "heartbeat",
                    "ts_epoch": 123.0}, rt=111.0)
        log.close()
        lines = self.read_lines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0]["device_id"], "dev_0001")
        self.assertEqual(lines[0]["rt"], 111.0)        # server receive time added
        self.assertEqual(lines[0]["ts_epoch"], 123.0)  # event fields preserved

    def test_offsets_are_monotonic_and_equal_filesize(self):
        log = EventLog(self.path, fsync_interval=0)
        o1 = log.append({"a": 1}, rt=1.0)
        o2 = log.append({"b": 2}, rt=2.0)
        log.close()
        self.assertLess(o1, o2)
        self.assertEqual(o2, os.path.getsize(self.path))  # offset == bytes on disk

    def test_written_bytes_visible_without_fsync(self):
        # os.write pushes bytes into the OS page cache immediately; a separate
        # reader (and a *restarted* process reading the same file) sees them
        # with no fsync. fsync only narrows the power-loss window. This is the
        # ack-durability guarantee of §3: 200 after os.write survives a kill.
        log = EventLog(self.path, fsync_interval=0)
        log.append({"x": 1}, rt=1.0)
        self.assertEqual(len(self.read_lines()), 1)
        log.close()

    def test_concurrent_appends_no_corruption(self):
        # ThreadingHTTPServer calls append() from many request threads at once.
        # A lock + O_APPEND must yield exactly N whole, parseable lines.
        log = EventLog(self.path, fsync_interval=0)
        per_thread, n_threads = 200, 8

        def worker(tid):
            for i in range(per_thread):
                log.append({"t": tid, "i": i}, rt=float(i))

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        log.close()
        self.assertEqual(len(self.read_lines()), per_thread * n_threads)


if __name__ == "__main__":
    unittest.main()
