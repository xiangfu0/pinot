# SPDX-License-Identifier: Apache-2.0
import json,sys
from pathlib import Path
import boto3
bucket,prefix=sys.argv[1:3]
s3=boto3.client("s3",region_name="us-east-1")
objects={o["Key"][len(prefix):]:o["Size"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket,Prefix=prefix) for o in page.get("Contents",[]) if o['Key'].endswith('/columns.psf')}
if not objects:sys.exit("No native index objects in the experiment prefix")
out=Path("/run/pr20344");out.mkdir(exist_ok=True)
(out/"paths.json").write_text(json.dumps(objects))
print(len(objects),"objects",sum(objects.values()),"bytes")
