# SPDX-License-Identifier: Apache-2.0
"""Generate aggregate answers from PR 20344's deterministic input generator, without SQL or Pinot."""
import decimal
import hashlib
import json

def result_hash(rows):
    def norm(value):
        if isinstance(value, bool) or value is None: return value
        if isinstance(value, (int, float, decimal.Decimal)):
            n = decimal.Decimal(str(value))
            if not n.is_finite(): raise ValueError("non-finite number")
            return {"number":format(n.normalize(),"f") if n else "0"}
        if isinstance(value, (list, tuple)): return [norm(x) for x in value]
        if isinstance(value, dict): return {k:norm(v) for k,v in value.items()}
        return value
    serialized=sorted(json.dumps(norm(row),sort_keys=True,separators=(",",":")) for row in rows)
    return hashlib.sha256(("["+",".join(serialized)+"]").encode()).hexdigest()

def expected(count=524288):
    groups={}
    for i in range(count):
        customer=i%4096
        product=(i*17+i//4096)%1024
        key=customer%16, product%8
        totals=groups.setdefault(key,[0,0])
        totals[0]+=1
        totals[1]+=1+i%101
    rows=[[*key,*totals] for key,totals in groups.items()]
    return {"groups":len(rows),"fact_rows":count,"sha256":result_hash(rows)}

if __name__ == "__main__":
    import argparse
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--count",type=int,default=524288)
    print(json.dumps(expected(p.parse_args().count)))
