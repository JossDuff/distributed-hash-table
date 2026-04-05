#!/usr/bin/env python3
"""Aggregate per-client throughput logs into system-wide throughput over time."""

import argparse
import re
import glob
from collections import defaultdict

LOG_PATTERN = re.compile(
    r"(\d+\.\d+)s \| txns: (\d+) \| ops/sec: (\d+) \| avg_latency: ([\d.]+)ms"
)

def parse_logs(log_dir, per_client=False):
    if per_client:
        # {client: {time: ops}}
        client_data = defaultdict(dict)
        all_times = set()

        for path in sorted(glob.glob(f"{log_dir}/*-client.log")):
            client = path.rsplit("/", 1)[-1].replace("-client.log", "")
            with open(path) as f:
                for line in f:
                    m = LOG_PATTERN.search(line)
                    if m:
                        t = round(float(m.group(1)))
                        if t <= 1:
                            continue
                        ops = int(m.group(3))
                        client_data[client][t] = ops
                        all_times.add(t)

        clients = sorted(client_data.keys())
        print("time_s," + ",".join(clients))
        for t in sorted(all_times):
            vals = [str(client_data[c].get(t, 0)) for c in clients]
            print(f"{t}," + ",".join(vals))
    else:
        buckets = defaultdict(lambda: {"ops": 0, "latencies": [], "clients": 0})

        for path in sorted(glob.glob(f"{log_dir}/*-client.log")):
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
    parser = argparse.ArgumentParser()
    parser.add_argument("log_dir", nargs="?", default="logs")
    parser.add_argument("--per-client", action="store_true",
                        help="Output per-client ops/sec in wide format")
    args = parser.parse_args()
    parse_logs(args.log_dir, per_client=args.per_client)
