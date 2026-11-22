# SPDX-License-Identifier: Apache-2.0
"""Schedule a fixed-rate, fully validated MSE workload on the support host; capture server CPU around all requests."""
import argparse
import concurrent.futures
import decimal
import hashlib
import http.client
import json
import math
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from fixture import result_hash

INCOMPLETE = ("partialResult", "numGroupsLimitReached", "groupsTrimmed", "maxRowsInDistinctReached",
              "maxRowsWithoutChangeInDistinctReached", "maxExecutionTimeInDistinctReached",
              "mseLiteLeafStageLimitReached", "approximatedResult", "maxRowsInJoinReached")

def quantile(values, q):
    return sorted(values)[max(0, math.ceil(q*len(values))-1)] if values else None

def meter(url):
    begin=time.monotonic()
    with urlopen(url,timeout=5) as response: snapshot=json.load(response)
    snapshot["rtt_ms"]=1000*(time.monotonic()-begin)
    return snapshot

def get_meters(urls):
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(urls)) as pool:
        return list(pool.map(meter,urls))

def reset_storage(url):
    request=Request(url.rstrip("/")+"/reset",data=b"{}",headers={"Content-Type":"application/json"},method="POST")
    start=time.monotonic()
    with urlopen(request,timeout=45) as response:
        data=json.load(response)
    if not isinstance(data,dict) or data.get("ok") is False:
        raise RuntimeError("S3 cache reset unsuccessful")
    return {"elapsed_ms":(time.monotonic()-start)*1000,"receipt":data}

def reset_all(urls):
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(urls)) as pool:
        return list(pool.map(reset_storage,urls))

def settled_storage(urls, pause):
    first=get_meters(urls)
    until=time.monotonic()+3
    while True:
        time.sleep(pause)
        second=get_meters(urls)
        if all((a["s3_gets"],a["s3_bytes"])==(b["s3_gets"],b["s3_bytes"]) for a,b in zip(first,second)):
            return second
        if time.monotonic()>until: raise RuntimeError("storage reads did not settle before query")
        first=second

def wait_until(deadline):
    delay=deadline-time.monotonic()
    if delay>0: time.sleep(delay)

def cpu_summary(before,after):
    cpus=[]
    for a,b in zip(before,after):
        if any(a[k]!=b[k] for k in ("boot","pid","start_ticks","cpu_basis")):
            raise RuntimeError("server process changed within the window")
        if a["cpu_s"]>b["cpu_s"]: raise RuntimeError("server CPU counter reset")
        cpus.append(b["cpu_s"]-a["cpu_s"])
    return cpus

def checked_rows(result,expected):
    if result.get("exceptions"): raise ValueError("engine exception: "+str(result["exceptions"])[:190])
    if any(result.get(key) is True for key in INCOMPLETE):
        raise ValueError("partial or approximate result")
    q=result.get("numServersQueried")
    if q is not None and q!=result.get("numServersResponded"):
        raise ValueError("incomplete server reply counts")
    rows=result["resultTable"]["rows"]
    if len(rows)!=expected["groups"] or result_hash(rows)!=expected["sha256"]:
        raise ValueError("unexpected grouping/count/sum result")
    return len(rows)

def run(args):
    sql=Path(args.sql).read_text().strip()
    if "fuse_broadcast_joins" in sql.lower() or "fusebroadcastjoins" in (sql+args.options).lower():
        raise ValueError("fusion is controlled only by --fusion")
    if len(set(args.meter))!=len(args.meter): raise ValueError("duplicate server meter URL")
    expected=json.loads(Path(args.oracle).read_text())
    opts=args.options.strip(";")+";fuseBroadcastJoins="+args.fusion
    dest=urlsplit(args.broker)
    if dest.scheme not in ("https","http") or dest.username or dest.password or not dest.hostname:
        raise ValueError("broker must be http(s) URL with no embedded credentials")
    if not args.meter: raise ValueError("at least one distinct Pinot server meter is required")
    path=dest.path or "/query/sql"
    tls=threading.local()
    tag=str(uuid.uuid4())
    request_count=round(args.rate*args.seconds)
    if args.rate<=0 or args.seconds<=0 or request_count<1 or args.connections<1:
        raise ValueError("positive rate, duration, requests and connection limit required")
    offered=request_count/args.seconds
    options_hash=hashlib.sha256(args.options.strip(";").encode()).hexdigest()
    output=Path(args.output)
    output.mkdir(parents=True,exist_ok=False)
    def submit(i,scheduled,origin):
        started=time.monotonic()
        rec={"i":i,"scheduled_s":scheduled-origin,"launch_lag_ms":1000*(started-scheduled)}
        try:
            if not hasattr(tls,"conn"):
                cls=http.client.HTTPSConnection if dest.scheme=="https" else http.client.HTTPConnection
                tls.conn=cls(dest.hostname,dest.port,timeout=args.timeout)
            opts_i=opts+";clientQueryId="+tag+"-"+str(i)
            body=json.dumps({"sql":sql,"queryOptions":opts_i})
            tls.conn.request("POST",path,body,{"Content-Type":"application/json"})
            resp=tls.conn.getresponse()
            raw=resp.read()
            finished=time.monotonic()
            if resp.status!=200: raise ValueError("HTTP "+str(resp.status))
            result=json.loads(raw,parse_float=decimal.Decimal)
            rec["groups"]=checked_rows(result,expected)
            rec.update(ok=True,broker_ms=result.get("timeUsedMs"),response_s=finished-origin,
                       bytes=len(raw),servers_queried=result.get("numServersQueried"),
                       reached_group_limit=result.get("numGroupsLimitReached",False))
        except Exception as error:
            finished=time.monotonic()
            rec.update(ok=False,error=f"{type(error).__name__}: {error}"[:500],response_s=finished-origin)
            if hasattr(tls,"conn"):
                tls.conn.close()
                del tls.conn
        rec.update(latency_ms=1000*(finished-scheduled),service_ms=1000*(finished-started),
                   checked_s=time.monotonic()-origin)
        return rec

    cold_proofs=[]
    cold_uris=[url.rstrip("/")+"/snapshot" for url in args.cold_storage]
    if cold_uris and (len(cold_uris)!=len(args.meter) or len(cold_uris)!=len(set(cold_uris))):
        raise ValueError("one distinct storage-control endpoint is required for each measured server")
    # First cold sample is prepared before starting the CPU/arrival window.
    if cold_uris:
        resets=reset_all(args.cold_storage)
        sources=settled_storage(cold_uris,args.settle_s)
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.connections) as pool:
        before=get_meters(args.meter)
        if len({(x["boot"],x["pid"],x["start_ticks"]) for x in before})!=len(before):
            raise ValueError("duplicate worker process from multiple meters")
        t0=time.monotonic()
        wall_ns=time.time_ns()
        futures=[]
        for i in range(request_count):
            deadline=t0+i/offered
            if cold_uris and i:
                resets=reset_all(args.cold_storage)
                sources=settled_storage(cold_uris,args.settle_s)
            if cold_uris:
                # Capture per-query process CPU immediately before arrival, without delaying
                # dispatch due to non-query preparation if the control hosts are responsive.
                wait_until(deadline-.02)
                cpu0=before if i==0 else get_meters(args.meter)
            wait_until(deadline)
            f=pool.submit(submit,i,deadline,t0)
            futures.append(f)
            if cold_uris:
                obs=f.result()
                cpu1=get_meters(args.meter)
                mounts=settled_storage(cold_uris,args.settle_s)
                # A trial only scores if every timed request produced S3 data on this fixture.
                requests_by_worker=[b["s3_gets"]-a["s3_gets"] for a,b in zip(sources,mounts)]
                bytes_by_worker=[b["s3_bytes"]-a["s3_bytes"] for a,b in zip(sources,mounts)]
                if min(requests_by_worker+bytes_by_worker)<0: raise ValueError("S3 counter reset")
                cold_proofs.append(dict(i=i,s3_gets=sum(requests_by_worker),s3_bytes=sum(bytes_by_worker),
                    gets_by_worker=requests_by_worker,bytes_by_worker=bytes_by_worker,
                    query_active_cpu_s=cpu_summary(cpu0,cpu1),resets=resets,
                    before_s3=sources,after_s3=mounts))
        # The CPU denominator covers the full scheduled interval and any outstanding work.
        wait_until(t0+args.seconds)
        observations=[future.result() for future in futures]
        after=get_meters(args.meter)
    with (output/"requests.jsonl").open("x") as f:
        for rec in observations: f.write(json.dumps(rec)+"\n")
    cpus=cpu_summary(before,after)
    if len({x["cpu_basis"] for x in before}) != 1:
        raise ValueError("mixed server accounting bases")
    good=[row for row in observations if row["ok"]]
    elapsed=max(args.seconds,max(r["response_s"] for r in observations))
    actual=len(good)/elapsed
    inside=sum(row["response_s"]<=args.seconds for row in good)
    p90=quantile([r["latency_ms"] for r in good],.9)
    p99lag=quantile([r["launch_lag_ms"] for r in observations],.99)
    common={"fusion":args.fusion,"broker":args.broker,"rate":offered,"offered_seconds":args.seconds,
            "queries":request_count,"correct":len(good),"errors":request_count-len(good),
            "achieved_qps":actual,"completed_in_window":inside,"in_window_qps":inside/args.seconds,
            "elapsed_including_drain_s":elapsed,"post_window_drain_s":elapsed-args.seconds,
            "p50_ms":quantile([r["latency_ms"] for r in good],.5),"p90_ms":p90,
            "p99_ms":quantile([r["latency_ms"] for r in good],.99),"p99_launch_lag_ms":p99lag,
            "server_cpu_s":cpus,"total_server_cpu_s":sum(cpus),
            "server_cpu_s_per_1000_correct":1000*sum(cpus)/len(good) if good else None,
            "eligible_latency_load":bool(len(good)==request_count and p90<=args.p90_ms and
                                      p99lag<20 and actual>=.98*offered),
            "p90_target_ms":args.p90_ms,"before":before,"after":after,"utc_start_ns":wall_ns,
            "outstanding_at_start":0,"outstanding_at_end":0,
            "outstanding_at_scheduled_end":sum(row["response_s"]>args.seconds for row in observations),
            "oracle":expected,"query_sha256":hashlib.sha256(sql.encode()).hexdigest(),
            "fixed_options_sha256":options_hash,"client_run_id":tag}
    if cold_uris:
        common["cold_read_proof"]=cold_proofs
        common["query_active_cpu_s"]=sum(sum(e["query_active_cpu_s"]) for e in cold_proofs)
        common["cold_read_verified"]=all(e["s3_bytes"]>0 and e["s3_gets"]>0 and all(
            r["receipt"].get("objects",0)>0 and r["receipt"].get("pages_after") == 0
            for r in e["resets"]) for e in cold_proofs)
    if all("storage_cpu_s" in x for x in before+after):
        storage={}
        for a,b in zip(before,after):
            if a["storage_pid"]!=b["storage_pid"] or a["storage_start_ticks"]!=b["storage_start_ticks"]:
                raise ValueError("S3 filesystem process changed")
            key=(a["boot"],a["storage_pid"],a["storage_start_ticks"])
            storage.setdefault(key,b["storage_cpu_s"]-a["storage_cpu_s"])
        common["s3_filesystem_cpu_s"]=sum(storage.values())
    if all("s3_bytes" in x and "s3_gets" in x for x in before+after):
        common["s3_gets"]=[b["s3_gets"]-a["s3_gets"] for a,b in zip(before,after)]
        common["s3_bytes"]=[b["s3_bytes"]-a["s3_bytes"] for a,b in zip(before,after)]
        if min(common["s3_gets"]+common["s3_bytes"])<0: raise ValueError("S3 counter reset")
    (output/"window.json").write_text(json.dumps(common,indent=2)+"\n")
    print(json.dumps({k:v for k,v in common.items() if k not in ("before","after","oracle","cold_read_proof")}))
    return common

def parser():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--broker",required=True)
    p.add_argument("--meter",action="append",required=True)
    p.add_argument("--cold-storage",default=[],action="append",help="Control base URL with POST /reset and GET /snapshot; one per worker")
    p.add_argument("--settle-s",type=float,default=.10,help="storage quiet interval before and after each forced-cold request")
    p.add_argument("--sql",required=True)
    p.add_argument("--oracle",required=True)
    p.add_argument("--options",default="useMultistageEngine=true;usePhysicalOptimizer=false;useLeafServerForIntermediateStage=true;stageParallelism=2;timeoutMs=30000")
    p.add_argument("--output",required=True)
    p.add_argument("--fusion",choices=("true","false"),required=True)
    p.add_argument("--seconds",type=float,required=True)
    p.add_argument("--rate",type=float,required=True)
    p.add_argument("--p90-ms",type=float,default=1000)
    p.add_argument("--timeout",type=float,default=35)
    p.add_argument("--connections",type=int,default=64)
    return p

if __name__=="__main__":
    run(parser().parse_args())
