# SPDX-License-Identifier: Apache-2.0
"""Run a prespecified common-load pilot then independent four-pair comparisons on existing Pinot workers."""
import argparse
import json
import math
import time
from pathlib import Path
from types import SimpleNamespace

import run_window
import summarize

PAIRS=(("false","true"),("true","false"),("true","false"),("false","true"))

def execute(config,root,phase,name,rate,fusion,count,p90,meters,storage):
    # Duration is a multiple of the arrival period; both arms schedule exactly the same number.
    folder=root/phase/name
    ns=SimpleNamespace(broker=config["broker"],meter=meters,cold_storage=storage,settle_s=config.get("settle_s",.10),
        sql=config["sql"],oracle=config["oracle"],options=config["options"],fusion=fusion,seconds=count/rate,
        rate=rate,output=str(folder),p90_ms=p90,timeout=config.get("timeout",35),connections=config.get("connections",64))
    return run_window.run(ns)

def compare(config,root,regime):
    p90=config["p90_target_ms"]
    meters=config["meters"]
    storage=config["storage_controls"] if regime=="forced-cold" else []
    rates=config["regimes"][regime]["rates"]
    min_n=config["regimes"][regime]["min_queries"]
    min_t=config["regimes"][regime]["min_seconds"]
    times=config["regimes"][regime]
    if regime=="warm":
        # Run warmups of both modes before every pilot/confirmation phase. Queries must execute and validate.
        for flag in ("false","true"):
            d=execute(config,root,regime+"/warmup","on" if flag=="true" else "off",rates[0],flag,max(8,math.ceil(5*rates[0])),p90,meters,[])
            if d["errors"]: raise RuntimeError("warmup returned an incorrect/error reply")
    passing=[]
    for i,rate in enumerate(rates):
        valid=True
        n=max(times["pilot_min_queries"],math.ceil(rate*times["pilot_seconds"]))
        for flag in PAIRS[i%len(PAIRS)]:
            d=execute(config,root,regime+"/pilot",f"r{i}-"+flag,rate,flag,n,p90,meters,storage)
            valid &= d["eligible_latency_load"] and (not storage or d["cold_read_verified"])
        if not valid:
            break
        passing.append(rate)
    if not passing:
        print("NO_COMMON_PILOT_RATE",regime,flush=True)
        return False
    for candidate,rate in enumerate(reversed(passing)):
        phase=f"{regime}/score-{candidate}"
        manifest={"regime":regime,"rate":rate,"p90_target_ms":p90,"max_s3_bytes_ratio":1.10,"pairs":[]}
        n=max(min_n,math.ceil(rate*min_t))
        for i,order in enumerate(PAIRS):
            item={"name":"pair-"+str(i+1)}
            for flag in order:
                label=item["name"]+"-"+flag
                execute(config,root,phase,label,rate,flag,n,p90,meters,storage)
                item[flag]=label
            manifest["pairs"].append(item)
            (root/phase/"pairs.json").write_text(json.dumps(manifest,indent=2)+"\n")
            partial=summarize.summarize(root/phase,manifest)
            (root/phase/"summary.json").write_text(json.dumps(partial,indent=2)+"\n")
            if not partial["scored"]:
                print("FAILED_PAIR",regime,rate,partial["pairs"][-1],flush=True)
                break
        if partial["scored"] and len(manifest["pairs"])==len(PAIRS):
            print("SCORED",regime,rate,partial["matched_pairs_aggregate"],flush=True)
            return True
    return False

if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("config",type=Path)
    p.add_argument("output",type=Path)
    p.add_argument("--regime",choices=("forced-cold","warm"),required=True)
    args=p.parse_args()
    settings=json.loads(args.config.read_text())
    args.output.mkdir(parents=True,exist_ok=True)
    (args.output/"frozen-config.json").write_text(json.dumps(settings,indent=2)+"\n")
    ok=compare(settings,args.output,args.regime)
    raise SystemExit(0 if ok else 2)
