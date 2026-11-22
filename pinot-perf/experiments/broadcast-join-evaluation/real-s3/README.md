# Broadcast join fusion on real Amazon S3

[Apache Pinot #20344](https://github.com/apache/pinot/pull/20344) was tested at `ede33d157dbcbd675f8fc1b3aa60d1b32cc237a4`. The only query option varied was `fuseBroadcastJoins`. One fact leaf worker fed four intermediate join workers, two on each server. No query-result cache was deployed. Every query had to reproduce the independent count and sum for each of 128 groups.

## Measured results

With verified S3 reads on every query, fusion used **31.25% lower Pinot server CPU per 1,000 correct queries**. With warm index pages, fusion used **22.14% lower server CPU** at the higher matched rate. The P90 threshold for all four cells was 1,000 ms.

| Index cache during measurement | Fusion | Correct queries | Achieved QPS | End-to-end P90 | Pinot server CPU-s / 1,000 correct | S3 reader CPU-s / 1,000 correct |
|---|---|---:|---:|---:|---:|---:|
| S3 read on every query | Off | 128 | 1.00 | 301.0 ms | 230.00 | 88.05 |
| S3 read on every query | On | 128 | 1.00 | 291.6 ms | 158.13 | 87.34 |
| Warmed index pages | Off | 11,520 | 63.98 | 46.8 ms | 172.78 | 0.00 |
| Warmed index pages | On | 11,520 | 63.97 | 47.5 ms | 134.53 | 0.00 |

Paired changes in Pinot server CPU (fusion on relative to off) were -38.8%, -31.8%, -22.1%, -29.4% for forced-cold reads and -23.1%, -21.7%, -21.8%, -21.9% for warm indexes. [Worker process counters](server-counters.csv)

## What ran

- Two EC2 `m7i.2xlarge` instances each ran one production Pinot query server. A distinct `m7i.xlarge` ran the production broker, controller, ZooKeeper and load generator. All three instances and their disks were reused in every arm, in `us-east-1a`, using S3 Standard in `us-east-1` through a regional S3 gateway endpoint. On-demand compute was **$1.008/hour**. With 366 GiB of gp3 and three IPv4 addresses the modeled fixed rate was **$1.06311/hour**, before S3 requests/storage, discounts and taxes. [Hardware and price receipt](hardware.json)
- The native OFFLINE tables contained 524,288 fact rows in 16 segments, 4,096 customers and 1,024 products. The index files total 2,300,120 bytes. The fact table was assigned to one server and the dimensions to the other. Each join emitted 524,288 rows. With fusion off, those joins ran in stages 4 and 3. With fusion on, both ran in stage 3. In the unfused diagnostic query the intermediate mailbox sent 524,288 rows, including 3,337,620 serialized bytes. Fusion removed this intermediate send; both build sides still broadcast. [SQL](reproduce/fixture.sql) · [Executed operator counters](operator-counters.json) · [Service and fixture verification](service-fixture.json)
- The native server read memory-mapped indexes backed by a read-only FUSE adapter. On each kernel read miss the adapter issues an authenticated S3 Range GET and checks the response length. In the **forced-cold** campaign, the driver invalidated only the fixture index pages before each arrival, checked zero resident pages, took a storage-counter snapshot, and verified fresh GETs and returned bytes for every query. An initial test with AWS Mountpoint retained these small objects in its userspace cache after Linux eviction, which motivated the uncached reader. Every scored cold query made 34 successful range GETs for exactly 2,300,120 bytes. Each arm transferred 294,415,360 bytes in 4,352 GETs.
- In the **warm** campaign, the same files stayed memory-mapped after warmup, under sustained open-loop traffic. The scored windows recorded 0 GETs and 0 S3 bytes with fusion off, and 0 GETs and 0 S3 bytes with it on. The comparisons use small native indexes and one fixed SQL query. The measured storage and query path are specific to this fixture and selected loads.

## How to read the CPU numbers

The primary metric sums user and system CPU seconds for the two dedicated Pinot server JVMs over each entire window, including query drain, divides by correctly completed queries, then multiplies by 1,000. Counters come from Linux process accounting and are checked against process identity across windows. Idle and background server work at the measured offered rate is included. The adjacent column measures the two S3 reader processes separately. The broker, driver and cache-control processes are on separate accounting paths.

The shared target was **P90 ≤1,000 ms** with all answers correct, ≥98% of the offered rate achieved, and ≤2% difference in achieved QPS within each pair. Latency starts at the scheduled arrival and ends when the whole HTTP response is received. The predeclared driver-lag guard was P99 below 20 ms. Both settings ran pilots at the same loads. The reported confirmations were held out from rate selection, with four pairs run in off/on, on/off, on/off, off/on order. Pooling uses total CPU divided by total completions, total completions divided by total elapsed time, and nearest-rank P90 from individual requests. A rate rejected for client scheduling or cache-reset pacing does not establish an engine capacity ceiling. One dataset and one host set limit generalization.

Run `python3 verify.py` to independently recalculate the rates, P90s and CPU totals from the published samples and process counters.

[Machine-readable summary](summary.csv) · [Per-window measurements](windows.csv) · [Paired CPU changes](pairs.csv) · [Calibration and confirmation decisions](calibration-and-windows.csv) · [Per-request measurements](requests.csv.gz) · [Reproduction guide](reproduce/README.md)
