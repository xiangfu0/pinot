# SPDX-License-Identifier: Apache-2.0
"""Read Linux CPU and S3 counters on the dedicated Pinot server. Serve on a private or tunneled interface."""
import argparse
import json
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


def process(pid):
    # stat's command name is parenthesized and may contain whitespace and parentheses.
    values = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
    return int(values[19]), int(values[11]), int(values[12])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", required=True, type=int)
    parser.add_argument("--listen", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18103)
    parser.add_argument("--cgroup", help="Path of exclusive Pinot server cgroup-v2 directory on this host")
    parser.add_argument("--s3-counters", help="JSON snapshot file with monotonically increasing S3 GET/byte counters")
    parser.add_argument("--storage-pid", type=int, help="Optional separate S3 filesystem process")
    args = parser.parse_args()
    started, _, _ = process(args.pid)
    boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    ticks = os.sysconf("SC_CLK_TCK")
    storage_start = process(args.storage_pid)[0] if args.storage_pid else None
    if args.cgroup:
        member = Path(f"/proc/{args.pid}/cgroup").read_text().strip().splitlines()
        actual = next(line[3:] for line in member if line.startswith("0::"))
        if Path(args.cgroup).resolve() != (Path("/sys/fs/cgroup") / actual.lstrip("/")).resolve():
            raise ValueError("requested cgroup must be the running Pinot server PID's cgroup")
        if actual == "/":
            raise ValueError("root cgroup can include other processes")
    def snapshot():
        uptime = time.monotonic_ns()
        start, user, system = process(args.pid)
        if start != started:
            raise ValueError("Pinot server PID was reused")
        out = dict(pid=args.pid, boot=boot, start_ticks=start, monotonic_ns=uptime, tick_hz=ticks,
                   process_user_s=user/ticks, process_system_s=system/ticks, cpu_s=(user+system)/ticks,
                   cpu_basis="Pinot server process user + system")
        if args.storage_pid:
            identity, user_io, system_io = process(args.storage_pid)
            if identity != storage_start:
                raise ValueError("S3 filesystem PID was reused")
            out.update(storage_pid=args.storage_pid, storage_start_ticks=identity,
                       storage_cpu_s=(user_io+system_io)/ticks)
        if args.cgroup:
            cpu = dict(line.split() for line in (Path(args.cgroup)/"cpu.stat").read_text().splitlines())
            out.update(cpu_s=int(cpu["usage_usec"])/1e6, cpu_basis="dedicated Pinot server cgroup-v2 usage_usec",
                       cgroup_cpu_stat={k:int(v) for k,v in cpu.items()})
        if args.s3_counters:
            counts = json.loads(Path(args.s3_counters).read_text())
            if not all(isinstance(counts.get(k), int) and counts[k]>=0 for k in ("s3_gets","s3_bytes")):
                raise ValueError("S3 counters must contain nonnegative integer s3_gets and s3_bytes")
            out.update(s3_gets=counts["s3_gets"],s3_bytes=counts["s3_bytes"])
        return out

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/snapshot":
                self.send_error(404)
                return
            try:
                body = json.dumps(snapshot()).encode()
            except Exception as error:
                self.send_error(503, str(error))
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *_):
            pass
    snapshot()
    HTTPServer((args.listen,args.port),Handler).serve_forever()

if __name__ == "__main__":
    main()
