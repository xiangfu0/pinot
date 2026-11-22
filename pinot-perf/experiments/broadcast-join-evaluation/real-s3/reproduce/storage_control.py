# SPDX-License-Identifier: Apache-2.0
"""Maintain counters for the benchmark mount and evict only its segment indexes."""
import ctypes
import json
import os
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path("/mnt/pr20344-s3")
RUN = Path("/run/pr20344")
COUNTERS = RUN / "storage.json"
TRACE = Path("/var/log/pr20344-storage.log")
LOCK = threading.RLock()
LOGPOS = 0
TOTAL = dict(s3_gets=0, s3_bytes=0, other_gets=0)
libc = ctypes.CDLL(None, use_errno=True)
libc.mmap.restype = ctypes.c_void_p
PAGE = os.sysconf("SC_PAGE_SIZE")
FDS = {}
VIEWS = {}
def refresh():
    global LOGPOS
    if (RUN/"rangefs-active").exists():
        return json.loads((RUN/"range-count.json").read_text())
    if TRACE.exists():
        with TRACE.open("rb") as f:
            f.seek(LOGPOS)
            for line in f:
                if not line.endswith(b"\n"):
                    break
                LOGPOS=f.tell()
                if b"S3 request finished" not in line or b"GetObject" not in line:
                    continue
                # The CRT reports the exclusive end of its successful Content-Range.
                match = re.search(rb"range=Some\((\d+)\.\.(\d+)\)", line)
                if match:
                    lo,hi = map(int,match.groups())
                    TOTAL["s3_gets"]+=1
                    TOTAL["s3_bytes"]+=hi-lo
                else:
                    TOTAL["other_gets"]+=1
    return TOTAL.copy()
def save():
    with LOCK:
        s=refresh()
        tmp=COUNTERS.with_suffix(".tmp")
        tmp.write_text(json.dumps(s));tmp.replace(COUNTERS)
    return s
def reset():
    with LOCK:
        manifest=json.loads((RUN/"paths.json").read_text())
        if not manifest:raise ValueError("empty object manifest")
        for relative in manifest:
            if relative in FDS:continue
            path=ROOT/relative
            fd=os.open(path,os.O_RDONLY)
            size=os.fstat(fd).st_size
            addr=libc.mmap(None,ctypes.c_size_t(size),0,1,fd,0)
            if addr==ctypes.c_void_p(-1).value:raise OSError(ctypes.get_errno(),"mmap")
            FDS[relative]=fd
            VIEWS[relative]=(addr,size)
        def resident():
            count=0
            for addr,size in VIEWS.values():
                pages=(size+PAGE-1)//PAGE
                vec=(ctypes.c_ubyte*pages)()
                if libc.mincore(ctypes.c_void_p(addr),ctypes.c_size_t(size),vec)!=0:raise OSError(ctypes.get_errno(),"mincore")
                count+=sum(v&1 for v in vec)
            return count
        before=resident()
        # Opening the inode without FOPEN_KEEP_CACHE tells FUSE to invalidate the
        # page cache. No read, write, GET or server restart is requested here.
        for relative,fd in FDS.items():
            scratch=os.open(ROOT/relative,os.O_RDONLY)
            try:os.posix_fadvise(scratch,0,0,os.POSIX_FADV_DONTNEED)
            finally:os.close(scratch)
        after=resident()
        return {**save(),"objects":len(FDS),"pages_before":before,"pages_after":after}
class Handler(BaseHTTPRequestHandler):
    def reply(self,func):
        try:body=json.dumps(func()).encode();code=200
        except Exception as exc:body=json.dumps({"error":repr(exc)}).encode();code=503
        self.send_response(code);self.send_header("Content-Type","application/json");self.send_header("Content-Length",str(len(body)));self.end_headers();self.wfile.write(body)
    def do_GET(self):
        if self.path=="/snapshot":self.reply(save)
        else:self.send_error(404)
    def do_POST(self):
        if self.path=="/reset":self.reply(reset)
        else:self.send_error(404)
    def log_message(self,*args):pass
def poll():
    while True:
        try:save()
        except (ValueError,OSError):pass
        time.sleep(.025)
if __name__=="__main__":
    RUN.mkdir(exist_ok=True);threading.Thread(target=poll,daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0",18104),Handler).serve_forever()
