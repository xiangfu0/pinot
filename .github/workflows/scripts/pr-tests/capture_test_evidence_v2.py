#!/usr/bin/env python3
#
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
#
"""Capture immutable Maven test evidence. This program never builds or runs tests.

Archive layout (schema_version 1): manifest.json; original reports/exec/stdout
under raw/; content-addressed classobjects/<sha256>.class and resources/. Every
module/classpath occurrence is retained in the manifest even when bytes match.
Missing runtime classpath entries and divergent Pinot definitions fail capture.
An unsuccessful capture is retained for diagnosis and exits 2, never as proof.
"""

import argparse
import datetime
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import struct
import subprocess
import sys
import tarfile
import time
import xml.etree.ElementTree as ET
import zipfile


CI_KEYS = (
    "GITHUB_ACTIONS", "GITHUB_JOB", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA",
    "GITHUB_REF", "GITHUB_EVENT_NAME", "GITHUB_REPOSITORY", "GITHUB_WORKFLOW", "RUNNER_OS", "RUNNER_ARCH",
)
JAVA_KEYS = (
    "java.home", "java.version", "java.runtime.version", "java.vendor", "java.vm.name", "java.vm.version",
    "java.specification.version", "os.name", "os.arch", "os.version", "file.encoding", "user.dir",
    "surefire.forkNumber", "unit.test.fork.count", "unit.test.core.fork.count", "unit.test.fork.heap",
    "unit.test.rerun.count", "jacoco.exec.suffix",
)
SENSITIVE_KEY = re.compile(
    r"password|passwd|secret|credential|authorization|private.?key|access.?key|api.?key|auth.?token|github.?token|(^|[._-])token$",
    re.I,
)
SOURCE_SUFFIXES = {".java", ".scala", ".xml", ".sh", ".py", ".yml", ".yaml", ".properties"}
REVIEWED_GENERATED_PATHS = {
    "pinot-integration-tests/target-lane-a": "directory",
    "pinot-integration-tests/target-lane-b": "directory",
    "pinot-integration-tests/examples/batch/baseballStats/segments": "directory",
    "pinot-plugins/pinot-batch-ingestion/pinot-batch-ingestion-hadoop/pinot-plugins.tar.gz": "file",
}
PACKAGE_INFO_NAME = "org/apache/pinot/client/package-info"
PACKAGE_INFO_HASHES = {
    "35e3190904855bd4e488f09861cb0afc87c86ecc55b1a71c65c5dfb9f293b2af": "pinot-java-client",
    "75da4a23e87d3230be42b2819b8c9f46eca71570dc9c6da652655f84e0d2f265": "pinot-jdbc-client",
}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def json_bytes(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args])


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def class_name(data):
    """Read the JVM internal name without loading any class or invoking Java."""
    if data[:4] != b"\xca\xfe\xba\xbe":
        raise ValueError("Invalid class magic")
    count = struct.unpack_from(">H", data, 8)[0]
    pool = [None] * count
    pos = 10
    index = 1
    while index < count:
        tag = data[pos]
        pos += 1
        if tag == 1:
            size = struct.unpack_from(">H", data, pos)[0]
            pos += 2
            pool[index] = data[pos:pos + size].decode("utf-8", errors="replace")
            pos += size
        elif tag in (7, 8, 16, 19, 20):
            pool[index] = struct.unpack_from(">H", data, pos)[0]
            pos += 2
        elif tag in (3, 4, 9, 10, 11, 12, 17, 18):
            pos += 4
        elif tag in (5, 6):
            pos += 8
            index += 1
        elif tag == 15:
            pos += 3
        else:
            raise ValueError("Unknown constant-pool tag")
        index += 1
    this_class = struct.unpack_from(">H", data, pos + 2)[0]
    name = pool[pool[this_class]]
    if not isinstance(name, str) or not name:
        raise ValueError("Missing JVM class name")
    return name


def nonexecutable_package_info(data):
    """Decode only the exact reviewed metadata bytes; reject every other class."""
    sha = digest(data)
    if sha not in PACKAGE_INFO_HASHES or class_name(data) != PACKAGE_INFO_NAME:
        raise ValueError("Not the exact reviewed Pinot client package metadata")
    pos = 8
    def read_u2():
        nonlocal pos
        value = struct.unpack_from(">H", data, pos)[0]
        pos += 2
        return value
    pool = [None] * read_u2()
    index = 1
    while index < len(pool):
        tag = data[pos]
        pos += 1
        if tag == 1:
            size = read_u2()
            pool[index] = data[pos:pos + size].decode("utf-8")
            pos += size
        elif tag in (7, 8, 16, 19, 20):
            pool[index] = read_u2()
        elif tag in (3, 4, 9, 10, 11, 12, 17, 18):
            pos += 4
        elif tag in (5, 6):
            pos += 8
            index += 1
        elif tag == 15:
            pos += 3
        else:
            raise ValueError("Unknown classfile constant tag")
        index += 1
    flags, _this, _super = read_u2(), read_u2(), read_u2()
    interfaces, fields, methods = read_u2(), read_u2(), read_u2()
    if interfaces or fields or methods or flags != 0x1600:
        raise ValueError("Reviewed package metadata must have no executable members")
    annotations = []
    attributes = []
    for _ in range(read_u2()):
        name = pool[read_u2()]
        size = struct.unpack_from(">I", data, pos)[0]
        pos += 4
        end = pos + size
        if name == "SourceFile":
            if size != 2 or pool[read_u2()] != "package-info.java":
                raise ValueError("Unexpected metadata SourceFile")
        elif name == "RuntimeVisibleAnnotations":
            for _ in range(read_u2()):
                annotations.append(pool[read_u2()])
                if read_u2() != 0:
                    raise ValueError("Unexpected annotation element values")
        else:
            raise ValueError("Unreviewed package metadata attribute")
        if pos != end:
            raise ValueError("Malformed package metadata attribute")
        attributes.append(name)
    if pos != len(data) or struct.unpack_from(">HH", data, 4) != (0, 55):
        raise ValueError("Malformed or unreviewed classfile version")
    expected = [] if PACKAGE_INFO_HASHES[sha] == "pinot-java-client" else [
        "Ljavax/annotation/ParametersAreNonnullByDefault;", "Lorg/jspecify/annotations/NullMarked;"]
    if annotations != expected:
        raise ValueError("Unreviewed runtime annotations")
    return {"sha256": sha, "binary_name": PACKAGE_INFO_NAME, "major": 55,
            "fields": fields, "methods": methods, "has_code": False,
            "annotations": annotations, "attributes": attributes}


def reviewed_metadata_origin(origin, sha):
    module = PACKAGE_INFO_HASHES.get(sha)
    if not module:
        return False
    suffix = PACKAGE_INFO_NAME + ".class"
    if "!/" in origin:
        jar_path, member = origin.split("!/", 1)
        return member == suffix and jar_path.endswith(
            "/org/apache/pinot/" + module + "/1.7.0-SNAPSHOT/" + module + "-1.7.0-SNAPSHOT.jar")
    return origin.endswith("/pinot-clients/" + module + "/target/classes/" + suffix)


class Capture:
    """Own one append-only archive; original input files are only read."""

    def __init__(self, args):
        self.args = args
        self.root = args.root.resolve()
        self.output = args.output.absolute()
        self.errors = []
        self.file_pins = {}
        self.written = set()
        self.jar_cache = {}
        self.dir_cache = {}
        self.classpath_ids = {}
        self.formal_jdk_guard = None
        self.started = time.time()
        self.manifest = {
            "schema_version": 1,
            "collector_version": 2,
            "identity": {"label": args.label, "variant": args.variant, "workspace": str(self.root)},
            "created_at_utc": utc_now(),
            "ci": {key: os.environ[key] for key in CI_KEYS if key in os.environ},
            "host": {"system": platform.system(), "release": platform.release(), "machine": platform.machine(),
                     "python": platform.python_version(), "cpu_count": os.cpu_count()},
            "execution": {}, "files": {}, "sources": {}, "git_index": {}, "modules": {},
            "reports": [], "cases": [], "logs": [], "classpaths": {}, "jdk": [],
            "coverage": {"production_classes": [], "test_classes": [], "runtime_production_classes": [],
                         "exec_files": []},
            "duplicates": {"divergent_pinot_classes": [], "divergent_other_classes": [],
                           "reviewed_nonexecutable_metadata": []},
            "generated_outputs": {"approved_paths": sorted(args.allow_generated_output_path),
                                  "inventory": []},
            "freshness": {"started_at_epoch": args.started_at, "checked_raw_reports_and_exec": args.started_at is not None},
        }

    def error(self, message):
        if message not in self.errors:
            self.errors.append(message)

    def pin(self, path, data=None):
        path = Path(path)
        before = path.stat()
        if not path.is_file():
            raise ValueError("Expected regular file: " + str(path))
        if data is None:
            hasher = hashlib.sha256()
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    hasher.update(chunk)
            sha = hasher.hexdigest()
        else:
            sha = digest(data)
        after = path.stat()
        stamp = (before.st_size, before.st_mtime_ns, before.st_ino)
        if stamp != (after.st_size, after.st_mtime_ns, after.st_ino):
            self.error("Input changed while hashing: " + str(path))
        old = self.file_pins.setdefault(str(path), (stamp, sha))
        if old != (stamp, sha):
            self.error("Input changed between capture reads: " + str(path))
        return {"sha256": sha, "size": before.st_size, "mtime_ns": before.st_mtime_ns, "origin": str(path)}

    def add_bytes(self, archive_path, data, kind, origin=None):
        if archive_path.startswith("/") or ".." in Path(archive_path).parts:
            raise ValueError("Unsafe archive member")
        record = {"sha256": digest(data), "size": len(data), "kind": kind, "origin": origin}
        if archive_path in self.written:
            if self.manifest["files"][archive_path]["sha256"] != record["sha256"]:
                raise ValueError("Divergent archive member: " + archive_path)
            return record
        info = tarfile.TarInfo(archive_path)
        info.size = len(data)
        info.mtime = 0
        info.mode = 0o644
        self.tar.addfile(info, io.BytesIO(data))
        self.written.add(archive_path)
        self.manifest["files"][archive_path] = record
        return record

    def add_file(self, path, archive_path, kind, freshness=False):
        data = path.read_bytes()
        pin = self.pin(path, data)
        if freshness and self.args.started_at is not None and path.stat().st_mtime < self.args.started_at:
            self.error("Stale raw output predates run start: " + str(path))
        record = self.add_bytes(archive_path, data, kind, str(path))
        return {**record, "archive_path": archive_path, "mtime_ns": pin["mtime_ns"]}

    def class_record(self, data, module, origin, kind, **extra):
        name = class_name(data)
        sha = digest(data)
        archive_path = "classobjects/" + sha + ".class"
        self.add_bytes(archive_path, data, "class", origin)
        return {"binary_name": name, "sha256": sha, "archive_path": archive_path,
                "module": module, "origin": origin, "class_kind": kind, **extra}

    def source_snapshot(self):
        index = {}
        sources = {}
        for item in git(self.root, "ls-files", "-s", "-z").split(b"\0"):
            if not item:
                continue
            header, encoded_path = item.split(b"\t", 1)
            mode, blob, stage = header.decode().split()
            path = os.fsdecode(encoded_path)
            index[path] = {"mode": mode, "git_blob": blob, "stage": stage}
            if stage != "0":
                self.error("Unmerged Git index entry: " + path)
            p = self.root / path
            if p.is_file() and (p.suffix in SOURCE_SUFFIXES or path in (".codecov.yml", "HEADER")):
                sources[path] = self.pin(p)["sha256"]
        status = git(self.root, "status", "--porcelain=v1", "-z").decode(errors="replace")
        return {
            "head": git(self.root, "rev-parse", "HEAD").decode().strip(),
            "tree": git(self.root, "rev-parse", "HEAD^{tree}").decode().strip(),
            "status": status, "index": index, "sources": sources,
        }

    def read_metadata(self):
        if not self.args.metadata:
            return
        metadata = json.loads(self.args.metadata.read_text())
        def check(value, path="metadata"):
            if isinstance(value, dict):
                for key, item in value.items():
                    if SENSITIVE_KEY.search(key) and item not in (None, "", False):
                        raise ValueError("Sensitive-looking metadata key; original will not be copied: " + path + "." + key)
                    check(item, path + "." + key)
            elif isinstance(value, list):
                for index, item in enumerate(value):
                    check(item, path + "." + str(index))
        check(metadata)
        self.manifest["execution"] = metadata.get("execution", metadata)
        self.add_file(self.args.metadata, "raw/run-metadata.json", "execution-metadata")

    def require_formal_input_guard(self):
        execution = self.manifest["execution"]
        guard = execution.get("source_input_guard", {})
        if execution.get("schema_version", 0) < 2 or any(execution.get(key) is not True for key in
                ("inputs_unchanged", "jdk_unchanged")) or any(guard.get(key) is not True for key in
                ("source_inputs_unchanged", "jdk_unchanged")):
            raise ValueError("Generated outputs require the successful formal V2 source/JDK input guard")
        snapshots = {}
        for label in ("before", "after"):
            basename = guard.get(label + "_manifest")
            paths = [path for path in self.args.log if path.name == basename]
            if len(paths) != 1 or digest(paths[0].read_bytes()) != guard.get(label + "_sha256"):
                raise ValueError("Missing or unbound formal input guard: " + label)
            snapshots[label] = json.loads(paths[0].read_text())
        before, after = snapshots["before"], snapshots["after"]
        for key in ("head", "tracked_status", "index", "tracked_files", "untracked_source_inputs", "jdk", "issues"):
            if key not in before or before[key] != after.get(key):
                raise ValueError("Formal input guard differs or is missing: " + key)
        if before["head"] != self.manifest["identity"]["git_head"] or before["tracked_status"] or before["issues"]:
            raise ValueError("Formal input guard does not describe clean pinned tracked inputs")
        if not before["tracked_files"] or not before["index"] or set(before["jdk"]) != {
                "bin/java", "bin/javac", "release", "lib/modules"}:
            raise ValueError("Formal source/JDK input guard is incomplete")
        actual_index = self.manifest["git_index"]
        if set(before["index"]) != set(actual_index) or set(before["tracked_files"]) != set(actual_index):
            raise ValueError("Formal guard tracked inventory differs from actual Git index")
        for relative, indexed in actual_index.items():
            guarded_index = before["index"][relative]
            if (guarded_index.get("mode"), guarded_index.get("oid"), guarded_index.get("stage")) != (
                    indexed["mode"], indexed["git_blob"], indexed["stage"]):
                raise ValueError("Formal guard index differs from actual index: " + relative)
            path = self.root / relative
            guarded = before["tracked_files"][relative]
            if indexed["mode"] == "120000":
                if not path.is_symlink() or digest(os.fsencode(os.readlink(path))) != guarded.get("symlink_target_sha256"):
                    raise ValueError("Formal guard symlink differs from actual input: " + relative)
            else:
                if path.is_symlink():
                    raise ValueError("Tracked regular input became a symlink: " + relative)
                actual = self.pin(path)
                if any(guarded.get(key) != actual[key] for key in ("sha256", "size")) or \
                        guarded.get("filesystem_mode") != path.stat().st_mode & 0o7777:
                    raise ValueError("Formal guard tracked bytes/mode differ from actual input: " + relative)
        self.formal_jdk_guard = before["jdk"]

    def generated_inventory(self, archive):
        inventory = []
        for relative in sorted(self.args.allow_generated_output_path):
            expected_kind = REVIEWED_GENERATED_PATHS[relative]
            root = self.root / relative
            if not root.exists() and not root.is_symlink():
                continue
            for parent in (root, *root.parents):
                if parent == self.root:
                    break
                if parent.is_symlink():
                    raise ValueError("Symlink in generated output path: " + str(parent))
            if (expected_kind == "directory") != root.is_dir():
                raise ValueError("Wrong generated output type: " + relative)
            paths = [root]
            if root.is_dir():
                for parent, directories, filenames in os.walk(root, followlinks=False):
                    paths.extend(Path(parent) / name for name in sorted(directories + filenames))
            for path in sorted(paths):
                if path.is_symlink():
                    raise ValueError("Generated output symlink is not permitted: " + str(path))
                if self.root not in path.resolve().parents:
                    raise ValueError("Generated output escapes workspace")
                rel = path.relative_to(self.root).as_posix()
                record = {"path": rel, "mode": path.stat().st_mode & 0o7777}
                if path.is_dir():
                    record["kind"] = "directory"
                elif path.is_file():
                    pin = self.pin(path)
                    record.update(kind="file", sha256=pin["sha256"], size=pin["size"],
                                  archive_path="generated-outputs/" + rel)
                    if archive:
                        self.add_file(path, record["archive_path"], "reviewed-generated-output")
                else:
                    raise ValueError("Nonregular generated output: " + rel)
                inventory.append(record)
        return inventory

    def capture_generated_outputs(self, snapshot):
        if any(path not in REVIEWED_GENERATED_PATHS for path in self.args.allow_generated_output_path):
            raise ValueError("Unreviewed generated output allowance")
        self.manifest["generated_outputs"]["original_git_status"] = snapshot["status"]
        records = [record for record in snapshot["status"].split("\0") if record]
        if any(not record.startswith("?? ") for record in records):
            raise ValueError("Tracked workspace changes are never permitted")
        untracked = [os.fsdecode(path) for path in git(self.root, "ls-files", "--others", "--exclude-standard", "-z").split(b"\0") if path]
        for path in untracked:
            if not any(path == allowed or path.startswith(allowed + "/")
                       for allowed in self.args.allow_generated_output_path):
                raise ValueError("Unmatched untracked workspace path: " + path)
        self.require_formal_input_guard()
        if records and not self.args.allow_generated_output_path:
            raise ValueError("Dirty workspace has no reviewed generated output allowances")
        inventory = self.generated_inventory(archive=True)
        self.manifest["generated_outputs"].update(inventory=inventory, inventory_sha256=digest(json_bytes(inventory)))

    def module_outputs(self):
        supported = {"target", "target-lane-a", "target-lane-b"}
        for parent, dirs, _files in os.walk(self.root):
            dirs[:] = sorted(d for d in dirs if d not in (".git", "node_modules", ".idea", ".gradle"))
            found = sorted(supported.intersection(dirs))
            for name in found:
                dirs.remove(name)
            module_root = Path(parent)
            if not (module_root / "pom.xml").is_file():
                continue
            for name in found:
                self.output_root(module_root, module_root / name)

    def output_root(self, module_root, target):
        module = module_root.relative_to(self.root).as_posix() or "."
        info = {"path": module, "production_classes": [], "test_classes": [], "resources": [],
                "report_files": [], "exec_files": []}
        info["build_root"] = target.name
        occurrence = module if target.name == "target" else module + "::" + target.name
        self.manifest["modules"][occurrence] = info
        for folder, kind in (("classes", "production"), ("test-classes", "test")):
            base = target / folder
            if not base.is_dir():
                continue
            for path in sorted(base.rglob("*")):
                if not path.is_file():
                    continue
                data = path.read_bytes()
                self.pin(path, data)
                if path.suffix == ".class":
                    try:
                        record = self.class_record(data, module, str(path), "workspace-" + kind, build_root=target.name)
                    except (ValueError, IndexError, struct.error):
                        if kind == "production":
                            self.error("Invalid compiled production class: " + str(path))
                        record = None
                    if record:
                        key = "production_classes" if kind == "production" else "test_classes"
                        self.manifest["coverage"][key].append(record)
                        info[key].append(record)
                        continue
                sha = digest(data)
                member = "resources/" + sha + ".bin"
                self.add_bytes(member, data, "workspace-" + kind + "-resource", str(path))
                info["resources"].append({"path": str(path.relative_to(base)), "root_kind": kind,
                                          "sha256": sha, "archive_path": member})
                if kind == "test" and path.suffix == ".jar":
                    fixture = self.jar_index(path, module, support=True)
                    self.manifest["coverage"]["test_classes"].extend(
                        {**record, "build_root": target.name} for record in fixture["pinot_classes"])
        reports = target / "surefire-reports"
        if reports.is_dir():
            for path in sorted(reports.rglob("*")):
                if path.is_file():
                    member = "raw/" + path.relative_to(self.root).as_posix()
                    if path.name.startswith("TEST-") and path.suffix == ".xml":
                        self.report(path, module, member, target.name)
                    else:
                        self.add_file(path, member, "test-stdout", freshness=True)
                    info["report_files"].append(member)
        for path in sorted(target.glob("jacoco*.exec")):
            if path.is_file():
                record = self.add_file(path, "raw/" + path.relative_to(self.root).as_posix(),
                                       "jacoco-exec", freshness=True)
                record["module"] = module
                record["build_root"] = target.name
                self.manifest["coverage"]["exec_files"].append(record)
                info["exec_files"].append(record["archive_path"])

    def report(self, path, module, member, build_root):
        doc = ET.fromstring(path.read_bytes())
        props = {p.get("name", ""): p.get("value", p.text or "")
                 for p in doc.findall(".//properties/property")}
        sensitive = [key for key, value in props.items() if value and SENSITIVE_KEY.search(key)]
        if sensitive:
            raise ValueError("Sensitive-looking XML property keys; raw report not copied: " + ", ".join(sensitive))
        record = self.add_file(path, member, "test-xml", freshness=True)
        cp = props.get("java.class.path") or props.get("surefire.test.class.path")
        cp_id = None
        if cp:
            cp_id = digest((str(path.parent.parent.parent) + "\0" + cp).encode())
            if cp_id not in self.classpath_ids:
                self.classpath_ids[cp_id] = {"module": module, "build_root": build_root, "raw": cp, "properties": props, "reports": []}
            self.classpath_ids[cp_id]["reports"].append(member)
        else:
            self.error("Missing actual classpath property in report: " + member)
        self.manifest["reports"].append({**record, "module": module, "suite_name": doc.get("name"),
                                          "attributes": dict(doc.attrib), "classpath_id": cp_id, "build_root": build_root})
        for ordinal, case in enumerate(doc.findall(".//testcase")):
            if case.find("failure") is not None:
                status = "failed"
            elif case.find("error") is not None:
                status = "error"
            elif case.find("skipped") is not None:
                status = "skipped"
            else:
                status = "passed"
            self.manifest["cases"].append({
                "module": module, "build_root": build_root, "report": member, "classname": case.get("classname"), "name": case.get("name"),
                "status": status, "ordinal": ordinal, "attributes": dict(case.attrib),
                "skip_details": [{"attributes": dict(x.attrib), "text": x.text or ""} for x in case.findall("skipped")],
                "retry_elements": [x.tag for x in case if x.tag in
                                   ("flakyFailure", "flakyError", "rerunFailure", "rerunError")],
            })

    def jar_index(self, path, module, support=False, java_feature=25):
        key = (str(path.resolve()), java_feature)
        if key in self.jar_cache:
            return self.jar_cache[key]
        pin = self.pin(path)
        entries = {}
        pinot = []
        resources = []
        with zipfile.ZipFile(path) as jar:
            multi = False
            if "META-INF/MANIFEST.MF" in jar.namelist():
                multi = bool(re.search(rb"(?im)^Multi-Release:\s*true\s*$", jar.read("META-INF/MANIFEST.MF")))
            for entry in jar.infolist():
                name = entry.filename
                version = 0
                if name.startswith("META-INF/versions/"):
                    match = re.match(r"META-INF/versions/(\d+)/(.*)", name)
                    if not multi or not match or int(match[1]) > java_feature:
                        continue
                    version, name = int(match[1]), match[2]
                if not name.endswith(".class") or name in ("module-info.class",) or name.startswith("META-INF/"):
                    continue
                previous = entries.get(name)
                if previous and previous["version"] == version and name.startswith("org/apache/pinot/") \
                        and (previous["crc32"], previous["size"]) != (entry.CRC, entry.file_size):
                    self.error("Divergent duplicate Pinot ZIP member: " + str(path) + "!/" + name)
                if previous and previous["version"] > version:
                    continue
                entries[name] = {"entry": entry.filename, "crc32": entry.CRC, "size": entry.file_size,
                                 "version": version, "origin": str(path) + "!/" + entry.filename}
            for name, value in entries.items():
                if name.startswith("org/apache/pinot/"):
                    data = jar.read(value["entry"])
                    record = self.class_record(data, module, value["origin"],
                                               "runtime-test-jar" if support else "runtime-production-jar")
                    value["sha256"] = record["sha256"]
                    pinot.append(record)
            if pinot:
                for entry in jar.infolist():
                    if not entry.is_dir() and not entry.filename.endswith(".class"):
                        data = jar.read(entry.filename)
                        sha = digest(data)
                        member = "resources/" + sha + ".bin"
                        self.add_bytes(member, data, "runtime-pinot-jar-resource", str(path) + "!/" + entry.filename)
                        resources.append({"path": entry.filename, "sha256": sha, "archive_path": member})
        poms = []
        for pom in sorted(path.parent.glob("*.pom")):
            poms.append(self.pin(pom))
        record = {**pin, "kind": "jar", "entries": entries, "pinot_classes": pinot, "pom_pins": poms,
                  "resources": resources,
                  "support": support}
        self.jar_cache[key] = record
        return record

    def directory_index(self, path, module):
        key = str(path.resolve())
        if key in self.dir_cache:
            return self.dir_cache[key]
        entries = {}
        pinot = []
        pins = []
        for file in sorted(path.rglob("*")):
            if not file.is_file():
                continue
            relative = file.relative_to(path).as_posix()
            record = self.pin(file)
            pins.append([relative, record["sha256"]])
            if relative.endswith(".class") and relative != "module-info.class" and not relative.startswith("META-INF/"):
                entries[relative] = {"sha256": record["sha256"], "size": record["size"], "origin": str(file)}
                if relative.startswith("org/apache/pinot/"):
                    data = file.read_bytes()
                    pinot.append(self.class_record(data, module, str(file), "runtime-directory"))
        result = {"kind": "directory", "tree_sha256": digest(json_bytes(pins)), "entries": entries,
                  "pinot_classes": pinot}
        self.dir_cache[key] = result
        return result

    def logical_path(self, path):
        try:
            return "workspace:" + path.relative_to(self.root).as_posix()
        except ValueError:
            for token in ("/maven-repository/", "/.m2/repository/"):
                if token in str(path):
                    return "maven:" + str(path).split(token, 1)[1]
            return "external:" + str(path)

    @staticmethod
    def definition_bytes(value):
        origin = value["origin"]
        if "!/" in origin:
            jar_path, member = origin.split("!/", 1)
            with zipfile.ZipFile(jar_path) as jar:
                return jar.read(member)
        return Path(origin).read_bytes()

    def reviewed_metadata_duplicate(self, name, previous, definition, left, right):
        if name != PACKAGE_INFO_NAME + ".class" or {left, right} != set(PACKAGE_INFO_HASHES):
            return None
        if not reviewed_metadata_origin(previous["origin"], left) or not reviewed_metadata_origin(definition["origin"], right):
            return None
        first_bytes, later_bytes = self.definition_bytes(previous), self.definition_bytes(definition)
        if digest(first_bytes) != left or digest(later_bytes) != right:
            raise ValueError("Reviewed package metadata changed between indexing and decoding")
        return {"policy": "pinot-client-package-info-v1", "annotation_equivalent": False,
                "first": nonexecutable_package_info(first_bytes), "later": nonexecutable_package_info(later_bytes)}

    def classpaths(self):
        homes = {}
        for cp_id, source in self.classpath_ids.items():
            props = source["properties"]
            feature = int(props.get("java.specification.version", "25").split(".")[-1])
            entries = []
            definitions = {}
            effective_reviewed_metadata = {}
            for ordinal, value in enumerate(source["raw"].split(os.pathsep)):
                if not value or "${" in value or "@{" in value or "*" in value:
                    self.error("Unresolved/empty runtime classpath entry in " + cp_id)
                    continue
                path = Path(value)
                if not path.is_absolute():
                    path = Path(props.get("user.dir", str(self.root))) / path
                path = path.resolve()
                if not path.exists():
                    self.error("Missing runtime classpath entry: " + str(path))
                    entries.append({"ordinal": ordinal, "path": str(path), "kind": "missing"})
                    continue
                support = path.name.endswith(("-tests.jar", "-test.jar")) or path.name == "test-classes"
                indexed = self.directory_index(path, source["module"]) if path.is_dir() else \
                    self.jar_index(path, source["module"], support, feature)
                entry = {k: v for k, v in indexed.items() if k not in ("entries", "pinot_classes", "support")}
                entry.update(ordinal=ordinal, path=str(path), logical_path=self.logical_path(path))
                entries.append(entry)
                for record in indexed.get("pinot_classes", []):
                    target = "test_classes" if support else "runtime_production_classes"
                    self.manifest["coverage"][target].append({**record, "classpath_id": cp_id,
                                                            "runtime_owner_module": source["module"]})
                for name, definition in indexed["entries"].items():
                    previous = definitions.get(name)
                    if previous:
                        left = previous.get("sha256")
                        right = definition.get("sha256")
                        if left is None or right is None:
                            # CRC/size are a quick equality filter. Hash both originals for a divergent candidate.
                            if (previous.get("crc32"), previous["size"]) == (definition.get("crc32"), definition["size"]):
                                continue
                            left = self.definition_hash(previous)
                            right = self.definition_hash(definition)
                        if left != right:
                            detail = {"classpath_id": cp_id, "binary_name": name[:-6],
                                      "first_origin": previous["origin"], "first_sha256": left,
                                      "later_origin": definition["origin"], "later_sha256": right,
                                      "first_ordinal": previous["classpath_ordinal"], "later_ordinal": ordinal}
                            pinot = name.startswith("org/apache/pinot/")
                            key = "divergent_pinot_classes" if pinot else "divergent_other_classes"
                            self.manifest["duplicates"][key].append(detail)
                            if pinot:
                                qualification = self.reviewed_metadata_duplicate(name, previous, definition, left, right)
                                if qualification:
                                    detail["reviewed_nonexecutable_metadata"] = True
                                    self.manifest["duplicates"]["reviewed_nonexecutable_metadata"].append(
                                        {**detail, "qualification": qualification})
                                    effective_reviewed_metadata[name] = {
                                        "binary_name": name[:-6], "sha256": left, "origin": previous["origin"],
                                        "ordinal": previous["classpath_ordinal"]}
                                else:
                                    self.error("Divergent Pinot runtime class: " + name + " in " + cp_id)
                    else:
                        definitions[name] = {**definition, "classpath_ordinal": ordinal}
            self.manifest["classpaths"][cp_id] = {"module": source["module"], "build_root": source["build_root"], "entries": entries,
                                                       "reports": source["reports"],
                                                       "effective_reviewed_metadata": list(effective_reviewed_metadata.values()),
                                                       "java_properties": {k: props[k] for k in JAVA_KEYS if k in props}}
            home = props.get("java.home")
            if home:
                homes.setdefault(home, {k: props[k] for k in JAVA_KEYS if k.startswith("java.") and k in props})
        for home, properties in sorted(homes.items()):
            pins = {}
            for rel in ("bin/java", "bin/javac", "release", "lib/modules"):
                path = Path(home) / rel
                if not path.is_file():
                    self.error("Missing JDK runtime/compiler input: " + str(path))
                else:
                    pins[rel] = self.pin(path)
                    expected = (self.formal_jdk_guard or {}).get(rel, {})
                    if any(expected.get(key) != pins[rel][key] for key in ("sha256", "size")):
                        self.error("Formal guard JDK differs from actual XML runtime JDK: " + str(path))
            self.manifest["jdk"].append({"home": home, "properties": properties, "files": pins})
        if not homes:
            self.error("No actual XML runtime JDK available to bind formal input guard")

    @staticmethod
    def definition_hash(value):
        if "sha256" in value:
            return value["sha256"]
        jar_path, member = value["origin"].split("!/", 1)
        with zipfile.ZipFile(jar_path) as jar:
            return digest(jar.read(member))

    def run(self):
        if not self.root.is_dir():
            raise ValueError("Workspace is not a directory")
        if self.output.exists() or Path(str(self.output) + ".partial").exists():
            raise ValueError("Refusing to replace an existing evidence archive")
        for suffix in (".manifest.json", ".sha256"):
            if Path(str(self.output) + suffix).exists():
                raise ValueError("Refusing to replace an existing evidence sidecar")
        self.output.parent.mkdir(parents=True, exist_ok=True)
        snapshot = self.source_snapshot()
        self.manifest["identity"].update(git_head=snapshot["head"], git_tree=snapshot["tree"])
        self.manifest["git_status"] = snapshot["status"]
        self.manifest["git_index"] = snapshot["index"]
        self.manifest["sources"] = snapshot["sources"]
        if self.args.expected_head and snapshot["head"] != self.args.expected_head:
            self.error("Expected Git head differs from actual source head")
        partial = Path(str(self.output) + ".partial")
        with partial.open("xb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=1, mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode="w|") as self.tar:
                    try:
                        self.read_metadata()
                        self.capture_generated_outputs(snapshot)
                        self.module_outputs()
                        self.classpaths()
                        for ordinal, path in enumerate(self.args.log):
                            purpose = "build" if "build" in path.name.lower() else \
                                "test" if any(s in path.name.lower() for s in ("unit", "test")) else "auxiliary"
                            record = self.add_file(path, "raw/logs/" + str(ordinal) + "-" + path.name, "explicit-stdout")
                            self.manifest["logs"].append({**record, "purpose": purpose})
                        if not self.manifest["reports"]:
                            self.error("No original test XML reports captured")
                        if not self.manifest["coverage"]["exec_files"]:
                            self.error("No raw JaCoCo execution files captured")
                        end = self.source_snapshot()
                        consistent = all(snapshot[key] == end[key] for key in ("head", "tree", "status", "index", "sources"))
                        self.manifest["capture_input_consistency"] = consistent
                        if not consistent:
                            self.error("Git/source inputs changed during capture")
                        outputs_consistent = self.generated_inventory(archive=False) == self.manifest["generated_outputs"]["inventory"]
                        self.manifest["generated_outputs"]["consistent_during_capture"] = outputs_consistent
                        if not outputs_consistent:
                            self.error("Generated output inventory changed during capture")
                        changed = []
                        for path, (stamp, _sha) in self.file_pins.items():
                            p = Path(path)
                            current = p.stat()
                            if stamp != (current.st_size, current.st_mtime_ns, current.st_ino):
                                changed.append(path)
                        self.manifest["capture_file_stat_consistency"] = not changed
                        for path in changed:
                            self.error("Pinned input/output changed during capture: " + path)
                    except Exception as exc:
                        self.error(type(exc).__name__ + ": " + str(exc))
                    self.manifest["capture_seconds"] = time.time() - self.started
                    self.manifest["capture_status"] = "PASS" if not self.errors else "FAIL"
                    self.manifest["errors"] = self.errors
                    self.manifest["counts"] = {"modules": len(self.manifest["modules"]),
                                                "reports": len(self.manifest["reports"]),
                                                "cases": len(self.manifest["cases"]),
                                                "classpaths": len(self.manifest["classpaths"]),
                                                "archive_objects": len(self.written)}
                    encoded = json_bytes(self.manifest)
                    info = tarfile.TarInfo("manifest.json")
                    info.size = len(encoded)
                    info.mode = 0o644
                    self.tar.addfile(info, io.BytesIO(encoded))
        os.rename(partial, self.output)
        Path(str(self.output) + ".manifest.json").write_bytes(encoded)
        archive_sha = self.pin(self.output)["sha256"]
        Path(str(self.output) + ".sha256").write_text(archive_sha + "  " + self.output.name + "\n")
        print(json.dumps({"archive": str(self.output), "sha256": archive_sha,
                          "status": self.manifest["capture_status"], "counts": self.manifest["counts"],
                          "capture_seconds": round(self.manifest["capture_seconds"], 3), "errors": self.errors}))
        return 0 if not self.errors else 2


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--variant", default="unspecified")
    parser.add_argument("--expected-head")
    parser.add_argument("--started-at", type=float)
    parser.add_argument("--metadata", type=Path)
    parser.add_argument("--log", type=Path, action="append", default=[])
    parser.add_argument("--allow-generated-output-path", "--allow-generated-output-root", action="append", default=[],
                        choices=sorted(REVIEWED_GENERATED_PATHS),
                        help="Exact reviewed generated file/directory; requires formal V2 input guard and archives every descendant")
    args = parser.parse_args(argv)
    try:
        return Capture(args).run()
    except Exception as exc:
        print(json.dumps({"status": "FAIL", "error": type(exc).__name__ + ": " + str(exc)}))
        return 2


if __name__ == "__main__":
    sys.exit(main())
