# SPDX-License-Identifier: Apache-2.0
"""Confirm both plans and full grouped answers through the same HTTP broker that receives timed traffic."""
import argparse
from collections import Counter
import json
from pathlib import Path
from urllib.request import Request, urlopen

from run_window import checked_rows

def query(url,sql,options,trace):
    body=json.dumps({"sql":sql,"queryOptions":options,"trace":trace}).encode()
    with urlopen(Request(url,data=body,headers={"Content-Type":"application/json"}),timeout=90) as response:
        result=json.load(response)
    if result.get("exceptions"):raise RuntimeError(result["exceptions"])
    return result

def inspect(node,stage=None):
    if isinstance(node,list):
        for elem in node:yield from inspect(elem,stage)
    elif isinstance(node,dict):
        current=node.get("stage",stage)
        if node.get("type")=="HASH_JOIN":yield current,node.get("parallelism"),node.get("emittedRows")
        for val in node.values():
            if isinstance(val,(list,dict)):yield from inspect(val,current)

if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("config",type=Path);p.add_argument("output",type=Path);a=p.parse_args()
    cfg=json.loads(a.config.read_text());sql=Path(cfg["sql"]).read_text()
    expected=json.loads(Path(cfg["oracle"]).read_text())
    a.output.mkdir(parents=True,exist_ok=False)
    for flag in ("false","true"):
        options=cfg["options"]+";fuseBroadcastJoins="+flag
        plan=query(cfg["broker"],"EXPLAIN IMPLEMENTATION PLAN FOR "+sql,options,False)
        (a.output/("physical-"+flag+".json")).write_text(json.dumps(plan,indent=2)+"\n")
        result=query(cfg["broker"],sql,options,True)
        checked_rows(result,expected)
        stats=result.get("stageStats")
        (a.output/("stats-"+flag+".json")).write_text(json.dumps(stats,indent=2)+"\n")
        joins=list(inspect(stats))
        print(flag,joins,flush=True)
        assert len(joins)==2 and all(j[0] is not None and j[1]==4 and j[2]==524288 for j in joins), joins
        assert len({j[0] for j in joins})==(1 if flag=="true" else 2), joins
