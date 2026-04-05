#!/usr/bin/env python3
"""Aggregate per-client throughput logs into system-wide throughput over time."""

import re
import sys
import glob
from collections import defaultdict

LOG_PATTERN = re.compile(
    r"(\d+\.\d+)s \| txns: (\d+) \| ops/sec: (\d+) \| avg_latency: ([\d.]+)ms"
)

def parse_logs(log_dir):
    buckets = defaultdict(lambda: {"ops": 0, "latencies": [], "clients": 0})

    for path in sorted(glob.glob(f"{log_dir}/*-client.log")):
        client = path.rsplit("/", 1)[-1].replace("-client.log", "")
        with open(path) as f:
            for line in f:
                m = LOG_PATTERN.search(line)
                if m:
                    t = round(float(m.group(1)))
                    ops = int(m.group(2))
                    latency = float(m.group(4))
                    buckets[t]["ops"] += ops
                    buckets[t]["latencies"].append(latency)
                    buckets[t]["clients"] += 1

    print("time_s,total_ops_sec,avg_latency_ms,num_clients")
    for t in sorted(buckets):
        if t <= 1:
            continue
        b = buckets[t]
        avg_lat = sum(b["latencies"]) / len(b["latencies"])
        print(f"{t},{b['ops']},{avg_lat:.3f},{b['clients']}")

if __name__ == "__main__":
    log_dir = sys.argv[1] if len(sys.argv) > 1 else "logs"
    parse_logs(log_dir)
