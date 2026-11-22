# Reproduce the real-S3 fusion comparison

Use the host layout in [hardware.json](../hardware.json). All three machines stay in place for both modes. Run ZooKeeper, the production Pinot controller, broker and load driver on the control host. Run one Pinot server per worker host. Use JDK 25 on all three, Linux with FUSE on the workers, and a new S3 Standard prefix in `us-east-1`. The supplied S3 reader selects that region explicitly. Workers need permission to list the experiment prefix and read its objects.

The examples assume Bash. Set `BROKER`, `WORKER_A` and `WORKER_B` to their private host addresses, `BUCKET` to your S3 bucket and `PREFIX` to your experiment prefix with a trailing slash. Restrict the unauthenticated application, meter and reset ports to the benchmark hosts. Keep the SQL and all controller/worker settings identical across modes.

## Build Pinot and the fixture

Use the files in this directory as the reproducibility kit. On a build host, set `KIT` to its absolute path. Build the exact PR head with the Maven wrapper and capture a classpath for the generator and services.

```bash
git clone https://github.com/apache/pinot.git pinot
cd pinot
git fetch origin refs/pull/20344/head
git checkout --detach ede33d157dbcbd675f8fc1b3aa60d1b32cc237a4
./mvnw -pl pinot-tools -am -Ppinot-fastdev \
  -Dtest=PostQueryCommandTest -Dsurefire.failIfNoSpecifiedTests=false test
export DIST="$PWD/../pinot-run"
mkdir -p "$DIST/lib"
python3 - <<'PY'
from pathlib import Path
import os, shutil, subprocess, xml.etree.ElementTree as ET
out = Path(os.environ['DIST'])
xml = next(Path('pinot-tools/target/surefire-reports').glob('TEST-*PostQueryCommandTest.xml'))
cp = ET.parse(xml).find('.//property[@name="java.class.path"]').get('value')
entries = []
for index, path in enumerate(map(Path, cp.split(os.pathsep))):
    if path.name == 'test-classes':
        continue
    name = path.parent.parent.name + '.jar' if path.is_dir() else path.name
    dest = out / 'lib' / f'{index:04d}-{name}'
    if path.is_dir():
        subprocess.run(['jar', 'cf', dest, '-C', path, '.'], check=True)
    else:
        shutil.copyfile(path, dest)
    entries.append(str(dest.relative_to(out)))
(out / 'classpath').write_text(':'.join(entries) + '\n')
PY
cd "$DIST"
javac -cp "$(cat classpath)" -d . "$KIT/Generate.java"
java --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  -cp ".:$(cat classpath)" Generate "$PWD/corpus"
(cd "$KIT" && python3 fixture.py)
```

The generator writes schema/table JSON under `corpus/tables` and 18 segment directories under `corpus/native`. Compare the independently generated result hash to [fixture-oracle.json](fixture-oracle.json). To package the segments and stage the real query data, run on the build host:

```bash
mkdir -p corpus/upload
for segment in corpus/native/*; do
  name=$(basename "$segment")
  tar -C corpus/native -czf "corpus/upload/$name.tar.gz" "$name"
done
aws s3 sync corpus/native "s3://$BUCKET/${PREFIX}native/" \
  --region us-east-1 --exclude '*' --include '*/v3/columns.psf'
```

Copy `lib/` and `classpath` to `/opt/pr20344/` on all hosts, the kit to `/opt/pr20344/kit/`, and the generated `corpus/` to the control host. The 18 `columns.psf` files contain the native dictionaries and forward indexes used by the query. File sizes and hashes for the reported run are in [service-fixture.json](../service-fixture.json).

## Start the services and assign segments

On each host, use this wrapper as `/opt/pr20344/start` and make it executable. Use a server-only systemd service named `pr20344-server.service` so the meter can find its JVM PID.

```bash
#!/usr/bin/env bash
set -eu
cd /opt/pr20344
heap=$1
shift
exec java "-Xms$heap" "-Xmx$heap" -XX:+UseG1GC -XX:MaxDirectMemorySize=6g \
  -Djava.net.preferIPv4Stack=true -Dio.netty.tryReflectionSetAccessible=true \
  -Dio.grpc.netty.shaded.io.netty.tryReflectionSetAccessible=true --enable-native-access=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-exports=java.base/jdk.internal.util.random=ALL-UNNAMED -cp "$(cat classpath)" "$@"
```

Use separate long-running processes for these launch commands. Set `CLI=org.apache.pinot.tools.admin.PinotAdministrator`. On the control host, start a standalone ZooKeeper listening on TCP 2181, then run:

```bash
/opt/pr20344/start 1500m "$CLI" StartController -controllerHost "$BROKER" -controllerPort 9000 \
  -dataDir /opt/pr20344/controller -zkAddress "$BROKER:2181" -clusterName pr20344 \
  -configOverride cluster.tenant.isolation.enable=true
/opt/pr20344/start 4g "$CLI" StartBroker -brokerHost "$BROKER" -brokerPort 8099 \
  -brokerMultiStageRunnerPort 8090 -zkAddress "$BROKER:2181" -clusterName pr20344 \
  -configOverrides "pinot.query.runner.hostname=$BROKER"
```

On each worker, replace `WORKER` with that worker's private address. Use the following command as the server unit's `ExecStart`, substituting literal addresses and the full CLI class name in the unit file:

```bash
/opt/pr20344/start 12g "$CLI" StartServer -serverHost "$WORKER" -serverPort 8098 \
  -serverAdminPort 8097 -serverMultiStageServerPort 8091 -serverMultiStageRunnerPort 8092 \
  -zkAddress "$BROKER:2181" -clusterName pr20344 \
  -dataDir /opt/pr20344/data -segmentDir /opt/pr20344/segments \
  -configOverride "pinot.query.runner.hostname=$WORKER"
```

From the control host, confirm all roles appear in `GET /instances`. Tag the servers before creating the three tables. Replication is one: `Fact_OFFLINE` owns the 16 fact segments, and `Dim_OFFLINE` owns one segment for each dimension. The broker uses `DefaultTenant`.

```bash
cd /opt/pr20344
CTRL="http://$BROKER:9000"
for item in "$WORKER_A:Fact" "$WORKER_B:Dim"; do
  host=${item%:*}
  tenant=${item#*:}
  curl -fsS -X PUT -H 'Content-Type: application/json' \
    "$CTRL/instances/Server_${host}_8098/updateTags?tags=${tenant}_OFFLINE"
done
for table in fact customerDim productDim; do
  curl -fsS -H 'Content-Type: application/json' --data-binary "@corpus/tables/$table-schema.json" "$CTRL/schemas"
  curl -fsS -H 'Content-Type: application/json' --data-binary "@corpus/tables/$table-table.json" "$CTRL/tables"
  for seg in corpus/upload/"$table"_*.tar.gz; do
    curl -fsS -H 'UPLOAD_TYPE: SEGMENT' -F "segment=@$seg" "$CTRL/segments?tableName=$table&tableType=OFFLINE"
  done
done
```

## Serve the index bytes from S3

On both workers, install Python 3, boto3, libfuse and fusepy (Ubuntu packages include `python3-boto3`, `libfuse2t64` and `python3-fusepy`). Allow FUSE mounts and run the following as root. The signed range reader makes one real S3 GET for each FUSE read and maintains byte/request counters. Linux can still cache clean mapped pages.

```bash
cd /opt/pr20344/kit
mkdir -p /mnt/pr20344-s3 /run/pr20344
/usr/bin/python3 make_manifest.py "$BUCKET" "${PREFIX}native/"
python3 -c 'import json; assert len(json.load(open("/run/pr20344/paths.json"))) == 18'
touch /run/pr20344/rangefs-active
systemd-run --unit=pr20344-mount --collect /usr/bin/python3 "$PWD/rangefs.py" \
  "$BUCKET" "${PREFIX}native/" /mnt/pr20344-s3
for i in {1..50}; do mountpoint -q /mnt/pr20344-s3 && break; sleep 0.2; done
mountpoint /mnt/pr20344-s3
```

Wait for all assigned segments to load locally. There must be 16 index files on worker A and two on worker B. Stop the server and replace only each active `columns.psf` with its mounted counterpart. Leave its metadata and index offset map local. Repeat on the other worker.

```bash
systemctl stop pr20344-server
python3 - <<'PY'
from pathlib import Path
files = sorted(Path('/opt/pr20344/data').rglob('columns.psf'))
assert len(files) in (2, 16)
for f in files:
    target = Path('/mnt/pr20344-s3') / f.parents[1].name / 'v3' / 'columns.psf'
    assert target.stat().st_size == f.stat().st_size
    if f.is_symlink():
        assert f.resolve() == target
        continue
    f.rename(f.with_suffix('.psf.original'))
    f.symlink_to(target)
PY
systemctl start pr20344-server
```

Wait for `/health` on port 8097. Inspect `/proc/$SERVER_PID/maps` and check that the unique mounted index paths match the assigned segments (16 or two). Start the observer services **outside** the Pinot server unit. The recorded campaign used JVM process user + system CPU from Linux, summing both servers; it measured the separate filesystem PID independently.

```bash
cd /opt/pr20344/kit
SERVER_PID=$(systemctl show pr20344-server --property MainPID --value)
FUSE_PID=$(systemctl show pr20344-mount --property MainPID --value)
grep /mnt/pr20344-s3/ "/proc/$SERVER_PID/maps" | awk '{print $NF}' | sort -u
systemd-run --unit=pr20344-storage --collect /usr/bin/python3 "$PWD/storage_control.py"
until test -s /run/pr20344/storage.json; do sleep 0.2; done
systemd-run --unit=pr20344-meter --collect /usr/bin/python3 "$PWD/server_meter.py" \
  --listen "$WORKER" --pid "$SERVER_PID" --storage-pid "$FUSE_PID" \
  --s3-counters /run/pr20344/storage.json
```

For an additional cgroup-v2 run, pass `--cgroup "/sys/fs/cgroup$(sed -n 's/^0:://p' /proc/$SERVER_PID/cgroup)"` to the meter on **both** servers, using units dedicated exclusively to Pinot. This switches the primary counter to `cpu.stat/usage_usec`. Keep the same accounting mode throughout a comparison. After restarting a JVM or mount, restart the meter with its new PID. The meter validates PID identity.

## Validate the plans, run and read the campaign

On the control host, work from the kit directory so the relative SQL and oracle paths resolve. Substitute the three private addresses in [config.example.json](config.example.json). Check the meter and storage control endpoints before timing. The reset endpoint invalidates only the listed mounted indexes and reports resident pages via `mincore`.

```bash
cd /opt/pr20344/kit
sed -e "s/BROKER/$BROKER/g" -e "s/WORKER_A/$WORKER_A/g" -e "s/WORKER_B/$WORKER_B/g" \
  config.example.json > config.json
for worker in "$WORKER_A" "$WORKER_B"; do
  curl -fsS "http://$worker:18103/snapshot"
  curl -fsS -X POST "http://$worker:18104/reset"
done
python3 plan_proof.py config.json ../preflight
python3 campaign.py config.json ../results --regime forced-cold
python3 campaign.py config.json ../results --regime warm
```

`plan_proof.py` saves physical explain output and traced operator counters for each arm, validates the SQL answer, and checks two joins with four logical workers per join in separate stages with fusion off and one shared stage with it on. Its output directory must be new. To fingerprint the explain responses for your run, use `sha256sum ../preflight/physical-{false,true}.json`. The operator checks compare plan behavior across deployments. Use a fresh results directory for a new campaign, and run the regimes sequentially. For cold requests the driver checks both workers for zero resident pages after reset, takes settled source counters, and requires positive GET and byte deltas for the actual SQL request. Hot runs record observed source deltas separately. The helper launches ordinary HTTP queries through the broker; authenticated gateways and TLS would require adapting the endpoint clients.

For an accepted comparison, find the selected `score-*/summary.json` under each regime with `scored: true` and **four** pairs. Inspect each window's `requests.jsonl` for individual responses and `window.json` for CPU and source-read deltas. Failed pilots and confirmations remain in the same directory tree. The [protocol](protocol.md) defines the selection, correctness, latency and rate gates. Exit code 2 means that the driver did not obtain a passing common rate.
