# SPDX-License-Identifier: Apache-2.0
"""Summarize fully matched pairs from window.json records. Pairing is explicit in the manifest."""
import argparse, csv, json, math, statistics
from pathlib import Path

def read_arm(root,path):
    folder=root/path
    d=json.loads((folder/"window.json").read_text())
    events=[json.loads(x) for x in (folder/"requests.jsonl").read_text().splitlines()]
    assert len(events)==d["queries"] and sum(e["ok"] for e in events)==d["correct"]
    d["latencies"]=[e["latency_ms"] for e in events if e["ok"]]
    return d

def pct(vectors):
    vals=sorted(i for a in vectors for i in a)
    return vals[math.ceil(.9*len(vals))-1]

def summarize(root,manifest):
    pairs=[]
    selected={flag:[] for flag in ("false","true")}
    for item in manifest["pairs"]:
        a,b=[read_arm(root,item[flag]) for flag in ("false","true")]
        reasons=[]
        for k in ("query_sha256","fixed_options_sha256","oracle","p90_target_ms","rate","offered_seconds","queries"):
            if a[k]!=b[k]:reasons.append(k+" differs")
        if a["fusion"]!="false" or b["fusion"]!="true":reasons.append("fusion labels")
        if abs(a["achieved_qps"]-b["achieved_qps"])>.02*a["rate"]:reasons.append("rate difference")
        if not (a["eligible_latency_load"] and b["eligible_latency_load"]):reasons.append("load/SLO/correctness gate")
        if a["p90_target_ms"]!=manifest["p90_target_ms"]:reasons.append("SLO mismatch")
        if manifest["regime"]=="forced-cold":
            for d in (a,b):
                steps=d.get("cold_read_proof")
                if not d.get("cold_read_verified") or not steps or len(steps)!=d["queries"] or min(x.get("s3_bytes",0) for x in steps)<=0 or min(x.get("s3_gets",0) for x in steps)<=0:
                    reasons.append("incomplete per-query S3 evidence")
            if not reasons:
                traffic=[sum(s["s3_bytes"] for s in d["cold_read_proof"])/d["queries"] for d in (a,b)]
                if max(traffic)/min(traffic)>manifest.get("max_s3_bytes_ratio",1.10):reasons.append("object byte difference")
        def identity(d):
            return sorted((v["boot"],v["pid"],v["start_ticks"],v["cpu_basis"]) for v in d["before"])
        if identity(a)!=identity(b):reasons.append("server host/PID changes")
        metric=lambda d:{k:d.get(k) for k in ("correct","achieved_qps","p90_ms","total_server_cpu_s","server_cpu_s_per_1000_correct","s3_filesystem_cpu_s","s3_gets","s3_bytes")}
        pairs.append(dict(name=item["name"],eligible=not reasons,reasons=reasons,off=metric(a),on=metric(b),
                          pct_change=100*(b["server_cpu_s_per_1000_correct"]/a["server_cpu_s_per_1000_correct"]-1)
                          if a["server_cpu_s_per_1000_correct"] and b["server_cpu_s_per_1000_correct"] else None))
        if not reasons:
            selected["false"].append(a);selected["true"].append(b)
    aggregate={}
    for flag,ds in selected.items():
        if not ds:continue
        correct=sum(d["correct"] for d in ds)
        aggregate[flag]={"windows":len(ds),"correct":correct,"achieved_qps":correct/sum(d["elapsed_including_drain_s"] for d in ds),
                         "p90_ms":pct(d["latencies"] for d in ds),
                         "server_cpu_s_per_1000_correct":1000*sum(d["total_server_cpu_s"] for d in ds)/correct}
    valid=bool(pairs) and len(selected["false"])==len(pairs)
    return dict(regime=manifest["regime"],scored=valid,pairs=pairs,matched_pairs_aggregate=aggregate,
                pct_change=100*(aggregate["true"]["server_cpu_s_per_1000_correct"]/aggregate["false"]["server_cpu_s_per_1000_correct"]-1) if aggregate else None)

if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("manifest",type=Path)
    p.add_argument("output",type=Path)
    args=p.parse_args()
    data=summarize(args.manifest.parent,json.loads(args.manifest.read_text()))
    args.output.write_text(json.dumps(data,indent=2)+"\n")
    print(json.dumps({k:v for k,v in data.items() if k!="pairs"}))
