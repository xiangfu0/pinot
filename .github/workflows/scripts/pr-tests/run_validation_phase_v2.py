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
import stat
import sys
import threading
import time


def pin_regular_file(path):
    before = path.stat()
    sha = hashlib.sha256()
    blob = hashlib.sha1(b"blob " + str(before.st_size).encode() + b"\0")
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            sha.update(chunk)
            blob.update(chunk)
    after = path.stat()
    stable = (before.st_size, before.st_mtime_ns, before.st_ino) == (
        after.st_size, after.st_mtime_ns, after.st_ino)
    return {"sha256": sha.hexdigest(), "git_blob_sha1": blob.hexdigest(),
            "size": before.st_size, "filesystem_mode": stat.S_IMODE(before.st_mode),
            "stable_during_read": stable}


def is_generated_output(relative):
    parts = Path(relative).parts
    if "target" in parts:
        return True
    return len(parts) > 1 and parts[0] == "pinot-integration-tests" and parts[1] in {
        "target-lane-a", "target-lane-b", "target-concurrent-logs"}


def source_like(relative):
    p = Path(relative)
    return (p.suffix.lower() in {".java", ".scala", ".xml", ".sh", ".py", ".yml", ".yaml", ".properties"}
            or ("src" in p.parts and "resources" in p.parts) or p.parts[0] in {"config", ".mvn"})


def snapshot_inputs(root):
    index_raw = subprocess.check_output(["git", "ls-files", "--stage", "-z"], cwd=root)
    tracked = {}
    index = {}
    issues = []
    for entry in index_raw.split(b"\0"):
        if not entry:
            continue
        header, name = entry.split(b"\t", 1)
        mode, oid, stage_number = header.decode().split()
        relative = os.fsdecode(name)
        index[relative] = {"mode": mode, "oid": oid, "stage": stage_number}
        path = root / relative
        try:
            if mode == "120000":
                data = os.fsencode(os.readlink(path))
                tracked[relative] = {"symlink_target_sha256": hashlib.sha256(data).hexdigest(),
                                     "git_blob_sha1": hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()}
            elif mode == "160000":
                raise ValueError("Submodule source guard needs an explicit recursive contract")
            else:
                tracked[relative] = pin_regular_file(path)
            tracked[relative]["working_blob_matches_index"] = tracked[relative]["git_blob_sha1"] == oid
            # Git clean filters/LFS or line-ending attributes can legitimately make
            # working bytes differ from the index blob. Pin both; clean status and
            # the before/after byte comparison remain mandatory.
            if stage_number != "0":
                issues.append("Tracked input has an unresolved index stage: " + relative)
            if not tracked[relative].get("stable_during_read", True):
                issues.append("Tracked input changed during hashing: " + relative)
        except (OSError, ValueError) as error:
            tracked[relative] = {"error": str(error)}
            issues.append("Cannot pin tracked input: " + relative)
    untracked = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=root)
    untracked_sources = {}
    generated = {}
    for raw in untracked.split(b"\0"):
        if not raw:
            continue
        relative = os.fsdecode(raw)
        if is_generated_output(relative):
            prefix = relative.split("/target", 1)[0]
            generated[prefix] = generated.get(prefix, 0) + 1
        elif source_like(relative):
            try:
                untracked_sources[relative] = pin_regular_file(root / relative)
            except OSError as error:
                untracked_sources[relative] = {"error": str(error)}
    java_home = Path(os.environ["JAVA_HOME"]).resolve(strict=True)
    jdk = {relative: pin_regular_file(java_home / relative)
           for relative in ["bin/java", "bin/javac", "release", "lib/modules"]}
    for relative, pin in jdk.items():
        if not pin["stable_during_read"]:
            issues.append("JDK input changed during hashing: " + relative)
    return {
        "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "tracked_status": subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=root, text=True),
        "index": index, "tracked_files": tracked, "untracked_source_inputs": untracked_sources,
        "generated_untracked_output_counts": generated, "jdk": jdk, "issues": issues,
        "guard_contract": "All tracked source/resource/config/index/mode bytes; source-like untracked inputs. "
                          "Generated target outputs may regenerate and are independently bound to runtime ClassIDs/classes by the collector.",
    }


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
before_guard_path = output / "inputs-before-run.json"
after_guard_path = output / "inputs-after-run.json"
if args.phase == "build":
    if before_guard_path.exists():
        raise SystemExit("Refusing to replace the original input guard")
    before_guard = snapshot_inputs(root)
    before_guard_path.write_text(json.dumps(before_guard, indent=2) + "\n")
    if before_guard["head"] != actual_head or before_guard["tracked_status"] or before_guard["issues"]:
        raise SystemExit("Source inputs are not clean and pinned before the build")
metadata = json.loads(metadata_path.read_text()) if metadata_path.exists() else {
    "schema_version": 2,
    "capture_kind": "incomplete",
    "build_exit_code": None,
    "test_exit_code": None,
    "inputs_unchanged": False,
    "jdk_unchanged": False,
    "source_commit": actual_head,
    "control_commit": os.environ.get("PINOT_CONTROL_SHA"),
    "github_run_id": os.environ.get("GITHUB_RUN_ID"),
    "github_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
    "github_job": os.environ.get("GITHUB_JOB"),
    "github_event_name": os.environ.get("GITHUB_EVENT_NAME"),
    "scope_id": os.environ.get("PINOT_CAPTURE_SCOPE"),
    "variant": os.environ.get("PINOT_VARIANT"),
    "expected_core_forks": os.environ.get("PINOT_EXPECTED_CORE_FORKS"),
    "expected_unit_forks": 3 if os.environ.get("RUN_INTEGRATION_TESTS") == "false" else None,
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
samples_path = output / ("process-samples-01.jsonl" if args.phase == "build" else "process-samples-02.jsonl")
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
if args.phase == "build":
    metadata["build_exit_code"] = exit_code
else:
    metadata["test_exit_code"] = exit_code
    metadata["test_start_epoch"] = phase["started_at"]
    metadata["test_end_epoch"] = phase["ended_at"]
    metadata["test_command"] = phase["argv"]
    metadata["test_environment"] = {
        key: child_environment.get(key)
        for key in ["RUN_TEST_SET", "RUN_INTEGRATION_TESTS", "RUN_CODECOVERAGE", "MAVEN_OPTS",
                    "UNIT_TEST_FORK_COUNT", "UNIT_TEST_CORE_FORK_COUNT", "UNIT_TEST_FORK_HEAP",
                    "UNIT_TEST_RERUN_COUNT", "JAVA_HOME", "JAVA_TOOL_OPTIONS", "GITHUB_WORKSPACE"]
    }
    before_guard = json.loads(before_guard_path.read_text())
    after_guard = snapshot_inputs(root)
    after_guard_path.write_text(json.dumps(after_guard, indent=2) + "\n")
    source_keys = ["head", "tracked_status", "index", "tracked_files", "untracked_source_inputs"]
    metadata["inputs_unchanged"] = (not before_guard["issues"] and not after_guard["issues"]
                                    and all(before_guard[key] == after_guard[key] for key in source_keys))
    metadata["jdk_unchanged"] = before_guard["jdk"] == after_guard["jdk"]
    metadata["source_input_guard"] = {
        "before_manifest": before_guard_path.name, "after_manifest": after_guard_path.name,
        "before_sha256": hashlib.sha256(before_guard_path.read_bytes()).hexdigest(),
        "after_sha256": hashlib.sha256(after_guard_path.read_bytes()).hexdigest(),
        "tracked_file_count": len(before_guard["tracked_files"]),
        "source_inputs_unchanged": metadata["inputs_unchanged"],
        "jdk_unchanged": metadata["jdk_unchanged"],
        "generated_outputs": "Permitted target regeneration; actual runtime ClassIDs and compiled test/production bytes bound by collector",
    }
    metadata["capture_kind"] = "full" if (metadata["build_exit_code"] == 0 and exit_code == 0
                                               and metadata["inputs_unchanged"] and metadata["jdk_unchanged"]) else "failed"
metadata["exit_code"] = next(
    (p["exit_code"] for p in metadata["phases"] if p.get("exit_code", 0) != 0), 0
)
if args.phase != "build" and (not metadata["inputs_unchanged"] or not metadata["jdk_unchanged"]):
    metadata["input_guard_exit_code"] = 2
    if exit_code == 0:
        exit_code = 2
        metadata["exit_code"] = 2
metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
print(json.dumps({"event": "phase_completed", "phase": args.phase, "exit_code": exit_code}), flush=True)
raise SystemExit(exit_code if exit_code >= 0 else 128 - exit_code)
