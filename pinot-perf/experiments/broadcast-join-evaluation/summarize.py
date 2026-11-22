# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Generate per-seed and pooled tables from the checked CSV samples beside this file."""
import csv
from collections import defaultdict
import math
from pathlib import Path
from statistics import median

HERE = Path(__file__).resolve().parent


def measured(name):
    with (HERE / name).open(newline="") as stream:
        return [row for row in csv.DictReader(stream) if row["phase"] == "measured"]


def group(rows, dimensions):
    groups = defaultdict(list)
    for row in rows:
        key = tuple(row[d] for d in dimensions)
        for scope in (row["seed"], "pooled"):
            groups[(scope, *key)].append(row)
    return groups


def write(name, rows):
    with (HERE / name).open("w", newline="") as stream:
        output = csv.DictWriter(stream, list(rows[0]), lineterminator="\n")
        output.writeheader()
        output.writerows(rows)


def latency(samples):
    wall = sorted(float(row["wall_ms"]) for row in samples)
    return dict(requests=len(wall), median_ms=round(median(wall), 2),
                p90_ms=round(wall[math.ceil(0.90 * len(wall)) - 1], 2))


def report_single():
    keys = ("experiment", "build", "server_endpoints", "shape", "readers", "fusion")
    rows = []
    groups = group(measured("single_queries.csv"), keys)
    for (seed, *values), arm in sorted(groups.items()):
        cpu = [float(item["process_cpu_ms"]) for item in arm]
        controls = groups[(seed, *values[:-2], "1", "false")]
        timestamp = {(item["seed"], item["block"]): float(item["wall_ms"]) for item in controls}
        paired = [1 - float(item["wall_ms"]) / timestamp[item["seed"], item["block"]] for item in arm]
        rows.append(dict(seed=seed, **dict(zip(keys, values)), **latency(arm), median_cpu_ms=round(median(cpu), 2),
                         median_paired_wall_reduction_pct=round(100 * median(paired), 2)))
    write("single_summary.csv", rows)


def report_concurrent():
    keys = ("shape", "readers", "fusion")
    batches = group(measured("concurrent_batches.csv"), keys)
    rows = []
    for key, arm in sorted(group(measured("concurrent_requests.csv"), keys).items()):
        totals = batches[key]
        requests = sum(int(item["queries"]) for item in totals)
        assert requests == len(arm)
        wall = sum(float(item["wall_ms"]) for item in totals)
        cpu = sum(float(item["process_cpu_ms"]) for item in totals)
        rows.append(dict(seed=key[0], **dict(zip(keys, key[1:])), **latency(arm), batches=len(totals),
                         achieved_qps=round(1000 * requests / wall, 2), mean_cpu_ms_per_query=round(cpu / requests, 2)))
    write("concurrent_summary.csv", rows)


if __name__ == "__main__":
    report_single()
    report_concurrent()
