# SPDX-License-Identifier: Apache-2.0
"""Recalculate the published CPU, rate, latency and remote-read totals."""
import csv
import gzip
import math
from pathlib import Path

root = Path(__file__).resolve().parent


def records(name):
    with (root / name).open() as file:
        return list(csv.DictReader(file))


def group(rows, key):
    return [r for r in rows if (r["regime"], r["fused"]) == key]


def check(actual, expected):
    assert math.isclose(actual, float(expected), rel_tol=1e-8, abs_tol=1e-8), (actual, expected)


windows = records("windows.csv")
meters = records("server-counters.csv")
with gzip.open(root / "requests.csv.gz", "rt") as file:
    queries = list(csv.DictReader(file))

for summary in records("summary.csv"):
    key = summary["regime"], summary["fused"]
    arms, counters, responses = group(windows, key), group(meters, key), group(queries, key)
    assert len(arms) == 4 and len(counters) == 8
    assert all(int(row["server_accepted_groups"]) == 128 for row in responses)
    n = len(responses)
    assert sum(int(row["correct"]) for row in arms) == n == int(summary["correct"])
    cpu = sum(float(r["server_user_end_s"]) - float(r["server_user_start_s"])
              + float(r["server_system_end_s"]) - float(r["server_system_start_s"]) for r in counters)
    storage = sum(float(r["storage_end_s"]) - float(r["storage_start_s"]) for r in counters)
    rate = n / sum(float(r["duration_with_drain_s"]) for r in arms)
    latency = sorted(float(r["latency_ms"]) for r in responses)[math.ceil(.9 * n) - 1]
    check(cpu * 1000 / n, summary["server_cpu_s_per_1000"])
    check(storage * 1000 / n, summary["storage_cpu_s_per_1000"])
    check(rate, summary["achieved_qps"])
    check(latency, summary["p90_ms"])
    assert latency <= 1000
    for units in ("gets", "bytes"):
        total = sum(int(r[f"s3_{units}_end"]) - int(r[f"s3_{units}_start"]) for r in counters)
        assert total == int(summary[f"s3_{units}"])
        if key[0] == "forced-cold":
            assert sum(int(r[f"s3_{units}"]) for r in responses) == total
    if key[0] == "forced-cold":
        assert all(int(r["s3_gets"]) == 34 and int(r["s3_bytes"]) == 2300120
                   and int(r["resident_pages_after_reset_fact"]) == 0
                   and int(r["resident_pages_after_reset_dim"]) == 0 for r in responses)
    print(key, n, f"{rate:.2f} QPS", f"P90 {latency:.2f} ms", f"{1000 * cpu / n:.2f} server CPU-s/1k")
