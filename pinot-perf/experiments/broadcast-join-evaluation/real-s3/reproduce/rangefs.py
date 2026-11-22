# SPDX-License-Identifier: Apache-2.0
"""Read-only benchmark mount: one signed real S3 Range GET for each FUSE read."""
import errno, json, os, stat, sys, threading
from pathlib import Path
import boto3
from botocore.config import Config
try:
    from fusepy import FUSE, FuseOSError, Operations
except ImportError:
    from fuse import FUSE, FuseOSError, Operations
RUN=Path("/run/pr20344")
class S3Index(Operations):
    def __init__(self,bucket,prefix):
        self.bucket,self.prefix=bucket,prefix
        self.s3=boto3.client("s3",region_name="us-east-1",config=Config(max_pool_connections=64))
        self.sizes={"/"+k:v for k,v in json.loads((RUN/"paths.json").read_text()).items()}
        self.dirs={"/"}
        for p in self.sizes:
            self.dirs.update(str(a) for a in Path(p).parents)
        self.lock=threading.Lock()
        self.counts={"s3_gets":0,"s3_bytes":0,"s3_failed_reads":0,"s3_sdk_retries":0}
        self.save()
    def save(self):
        temp=RUN/"range.new"
        temp.write_text(json.dumps(self.counts));temp.replace(RUN/"range-count.json")
    def getattr(self,path,fh=None):
        if path in self.dirs: mode,size=stat.S_IFDIR|0o555,0
        elif path in self.sizes:mode,size=stat.S_IFREG|0o444,self.sizes[path]
        else:raise FuseOSError(errno.ENOENT)
        return dict(st_mode=mode,st_nlink=2 if path in self.dirs else 1,st_size=size,st_uid=0,st_gid=0,
                    st_atime=1700000000,st_mtime=1700000000,st_ctime=1700000000)
    def readdir(self,path,fh):
        if path not in self.dirs:raise FuseOSError(errno.ENOTDIR)
        return ['.','..']+sorted(Path(x).name for x in self.dirs|self.sizes.keys() if x!='/' and str(Path(x).parent)==path)
    def open(self,path,flags):
        if flags & (os.O_WRONLY|os.O_RDWR):raise FuseOSError(errno.EROFS)
        if path not in self.sizes:raise FuseOSError(errno.ENOENT)
        return 0
    def read(self,path,size,offset,fh):
        n=min(size,self.sizes[path]-offset)
        if n<=0:return b''
        try:
            ans=self.s3.get_object(Bucket=self.bucket,Key=self.prefix+path[1:],Range=f"bytes={offset}-{offset+n-1}")
            with ans['Body'] as body:data=body.read()
            if len(data)!=n or ans["ResponseMetadata"]["HTTPStatusCode"]!=206:raise RuntimeError("invalid S3 range")
            with self.lock:
                self.counts['s3_gets']+=1; self.counts['s3_bytes']+=len(data)
                self.counts['s3_sdk_retries']+=ans['ResponseMetadata'].get('RetryAttempts',0);self.save()
            return data
        except Exception:
            with self.lock:self.counts['s3_failed_reads']+=1;self.save()
            raise FuseOSError(errno.EIO)
if __name__=="__main__":
    bucket,prefix,mountpoint=sys.argv[1:]
    FUSE(S3Index(bucket,prefix),mountpoint,foreground=True,ro=True,allow_other=True,fsname="pr20344-real-s3-range")
