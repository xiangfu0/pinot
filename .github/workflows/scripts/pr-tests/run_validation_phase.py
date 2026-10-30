#!/usr/bin/env python3
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

"""Stream one original CI script and preserve its bytes, status, and timing.

This validation-only runner never selects tests, changes Maven arguments,
retries, merges reports, or launches an analyzer. The tested script owns its
normal test and aggregate lifecycles. Only an allowlist of public environment
settings enters metadata; credentials are never dumped.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--root", required=True, type=Path)
parser.add_argument("--output", required=True, type=Path)
parser.add_argument("--phase", choices=("build", "unit", "integration"), required=True)
parser.add_argument("--script", required=True)
parser.add_argument("--expected-head", required=True)
args = parser.parse_args()
root = args.root.resolve(strict=True)
output = args.output.resolve()
output.mkdir(parents=True, exist_ok=True)
metadata_path = output / "run-metadata.json"
actual_head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
if actual_head != args.expected_head:
    raise SystemExit("Source checkout is not the pinned expected commit")
script = root / args.script
if not script.is_file() or not os.access(script, os.X_OK):
    raise SystemExit("Original CI script is missing or not executable")
script_hash = hashlib.sha256(script.read_bytes()).hexdigest()
metadata = json.loads(metadata_path.read_text()) if metadata_path.exists() else {
    "schema_version": 1,
    "source_commit": actual_head,
    "control_commit": os.environ.get("PINOT_CONTROL_SHA"),
    "github_run_id": os.environ.get("GITHUB_RUN_ID"),
    "github_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
    "github_job": os.environ.get("GITHUB_JOB"),
    "github_event_name": os.environ.get("GITHUB_EVENT_NAME"),
    "scope_id": os.environ.get("PINOT_CAPTURE_SCOPE"),
    "variant": os.environ.get("PINOT_VARIANT"),
    "expected_core_forks": os.environ.get("PINOT_EXPECTED_CORE_FORKS"),
    "expected_unit_forks": 3,
    "retry_count": 0,
    "phases": [],
    "public_environment": {
        key: os.environ.get(key)
        for key in ("RUN_TEST_SET", "RUN_INTEGRATION_TESTS", "RUN_CODECOVERAGE", "MAVEN_OPTS")
    },
}
if metadata["source_commit"] != actual_head:
    raise SystemExit("Run metadata belongs to another source commit")
if any(phase["name"] == args.phase for phase in metadata["phases"]):
    raise SystemExit("Refusing a phase rerun in the same output directory")
log_path = output / (args.phase + ".log")
started_at = time.time()
started_monotonic = time.monotonic()
phase = {
    "name": args.phase,
    "argv": [args.script],
    "cwd": str(root),
    "script_sha256": script_hash,
    "started_at": started_at,
    "log": str(log_path),
    "maven_invocations": [],
    "tested_github_workspace": str(root),
}
samples_path = output / (args.phase + "-process-samples.jsonl")
monitor_stop = threading.Event()


def monitor_resources():
    # Process command names contain no arguments or environment values. CPU is
    # ps's lifetime average, and summed RSS can double-count shared mappings.
    with samples_path.open("x", encoding="utf-8") as sample_file:
        while not monitor_stop.is_set():
            snapshot = subprocess.run(
                ["ps", "-eo", "pid,ppid,pcpu,rss,comm"],
                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            )
            processes = []
            for row in snapshot.stdout.splitlines()[1:]:
                parts = row.split(maxsplit=4)
                if len(parts) == 5 and Path(parts[4]).name in {"java", "javac"}:
                    processes.append({"pid": int(parts[0]), "ppid": int(parts[1]),
                                      "lifetime_cpu_percent": float(parts[2]),
                                      "rss_kib": int(parts[3]), "command_name": parts[4]})
            sample_file.write(json.dumps({"sampled_at": time.time(), "ps_exit_code": snapshot.returncode,
                                          "java_processes": processes,
                                          "sum_reported_java_rss_kib": sum(p["rss_kib"] for p in processes)}) + "\n")
            sample_file.flush()
            monitor_stop.wait(15)


monitor_thread = threading.Thread(target=monitor_resources, daemon=True)
monitor_thread.start()
metadata["phases"].append(phase)
metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
if args.phase == "build":
    github_env = os.environ.get("GITHUB_ENV")
    if github_env:
        with open(github_env, "a", encoding="utf-8") as env_file:
            env_file.write(f"PINOT_BUILD_START_EPOCH={started_at:.9f}\n")
print(json.dumps({"event": "phase_started", "phase": args.phase, "source": actual_head}), flush=True)
with log_path.open("xb") as log_file:
    # Two checkouts keep the harness immutable. The original integration wrapper
    # expects GITHUB_WORKSPACE to name its actual source root, as it does in the
    # original CI workflow, rather than the parent containing both checkouts.
    child_environment = dict(os.environ)
    child_environment["GITHUB_WORKSPACE"] = str(root)
    process = subprocess.Popen([args.script], cwd=root, env=child_environment,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in iter(process.stdout.readline, b""):
        arrival = time.time()
        log_file.write(line)
        log_file.flush()
        sys.stdout.buffer.write(line)
        sys.stdout.buffer.flush()
        text_line = line.decode("utf-8", errors="replace").rstrip("\r\n")
        if re.match(r"^\+ mvn (?:clean install|test(?: |$)|jacoco:report-aggregate@report(?: |$))", text_line):
            if phase["maven_invocations"]:
                phase["maven_invocations"][-1]["ended_at"] = arrival
            goal = "aggregate" if "jacoco:report-aggregate@report" in text_line else (
                "build" if "clean install" in text_line else "test"
            )
            phase["maven_invocations"].append({
                "kind": goal,
                "shell_trace": text_line,
                "started_at": arrival,
                "reported_maven_result": None,
            })
        if "[INFO] BUILD SUCCESS" in text_line or "[INFO] BUILD FAILURE" in text_line:
            if phase["maven_invocations"]:
                phase["maven_invocations"][-1]["reported_maven_result"] = (
                    "success" if "BUILD SUCCESS" in text_line else "failure"
                )
    exit_code = process.wait()
monitor_stop.set()
monitor_thread.join(timeout=5)
ended_at = time.time()
if phase["maven_invocations"]:
    phase["maven_invocations"][-1]["ended_at"] = ended_at
for invocation in phase["maven_invocations"]:
    invocation["elapsed_seconds"] = invocation["ended_at"] - invocation["started_at"]
    invocation["timing_basis"] = "Observed original bash trace boundaries; raw output is unchanged"
phase.update({
    "ended_at": ended_at,
    "elapsed_seconds": time.monotonic() - started_monotonic,
    "exit_code": exit_code,
    "log_sha256": hashlib.sha256(log_path.read_bytes()).hexdigest(),
    "resource_samples": str(samples_path),
    "resource_samples_sha256": hashlib.sha256(samples_path.read_bytes()).hexdigest(),
})
metadata["exit_code"] = next(
    (p["exit_code"] for p in metadata["phases"] if p.get("exit_code", 0) != 0), 0
)
metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
print(json.dumps({"event": "phase_completed", "phase": args.phase, "exit_code": exit_code}), flush=True)
raise SystemExit(exit_code if exit_code >= 0 else 128 - exit_code)
