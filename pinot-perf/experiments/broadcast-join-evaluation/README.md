# Broadcast joins on Apache Pinot

This experiment measures [apache/pinot#20344](https://github.com/apache/pinot/pull/20344) at `ede33d157dbcbd675f8fc1b3aa60d1b32cc237a4`, based on `1b1094b34f5e9d86d5f808ad813673fafa09c9fd`. Fusion reduced the CPU cost of two consecutive broadcast joins and removed a full intermediate exchange. **The default plan had the lowest median latency on both topologies.** Four fact readers also increased median latency. These measurements favor keeping both options off for this workload.

## What prompted the change

Consider an orders query that groups sales by customer region and product category. The orders table is large enough to distribute. Both dimensions fit in each join worker's memory. Profiling that query shape raised two questions.

1. **Can one server feed several join workers faster?** `stageParallelism` can give a join several workers while one MSE leaf worker still emits all the fact rows assigned to a server. `leafStageWorkersPerServer=4` distributes eligible OFFLINE segments across four leaf workers. This is useful to test when a single sender limits the rate at which probe rows reach the join. The single-stage engine can already process segments in parallel within one leaf worker.
2. **Why redistribute the results of the first join?** With random probe distribution, each fact row goes to one worker. Both joins broadcast their entire build side to every worker. The first join can therefore pass every match, including duplicate matches, directly to the second join in the same worker. `fuseBroadcastJoins=true` removes the intervening random exchange. Separating the joins into stages can also let their work overlap, so removing traffic need not shorten elapsed time.

The leaf option accepts 1–16 and caps the actual number at the segment count. It applies to a plain, unordered OFFLINE scan, optionally with a column-only projection, with multiple segments routed to a **single server** and random output. Filters in the leaf plan, explicit leaf limits, sampling, partition-affine routing and broadcast build leaves retain their existing assignments. Enable fan-out after upgrading the participating broker and servers so concurrent local leaf requests receive distinct IDs.

Fusion applies in the default MSE planner (`usePhysicalOptimizer=false`) to an eligible pair of inner equijoins with random probe and broadcast build inputs. A projection that just passes columns through can sit between the joins. Query-level fusion can be overridden with `joinOptions(fuse_broadcast_joins='false')`. The benchmark explicitly chooses the existing random/broadcast strategy. Ordinary hash/hash equijoins do not become broadcast joins when the option is set.

## One query at a time

Each latency cell is median / P90 in milliseconds. CPU is median whole-JVM CPU milliseconds per query. Each cell pools 48 requests across two JVM launches.

| Two consecutive joins | One endpoint, latency | One endpoint, CPU | Two endpoints, latency | Two endpoints, CPU |
|---|---:|---:|---:|---:|
| Default (one reader, separate stages) | 12.07 / 17.09 | 71.24 | 15.49 / 23.68 | 109.28 |
| Four readers only | 12.73 / 17.60 | 71.85 | 15.84 / 21.49 | 109.91 |
| Fusion only | 16.16 / 21.61 | 66.00 | 18.46 / 24.15 | 98.06 |
| Four readers and fusion | 16.71 / 20.80 | 72.33 | 19.00 / 23.77 | 103.09 |

With one reader, fusion saved 7.4% CPU on one endpoint and 10.3% on two. Median latency increased by 34% and 19%, respectively, and the increase appeared in both JVMs on each topology. The unfused chain had eight stages, two random sends and three broadcast sends. Fusion removed one stage and one random send. Both joins still emitted 524,288 rows. On one endpoint, 64 in-memory messages carried these intermediate rows. With two endpoints, a separate diagnostic query using one reader sent 317,680 of those rows over loopback gRPC, accounting for 3,818,244 serialized bytes at the intermediate send. The remote fraction varies with random block routing.

| Single join control | One endpoint, latency | One endpoint, CPU | Two endpoints, latency | Two endpoints, CPU |
|---|---:|---:|---:|---:|
| Default | 8.63 / 10.42 | 35.99 | 10.10 / 12.45 | 51.11 |
| Four readers only | 9.23 / 10.86 | 41.93 | 10.94 / 17.91 | 63.79 |
| Fusion only | 8.86 / 9.82 | 38.56 | 10.55 / 12.68 | 53.22 |
| Four readers and fusion | 9.05 / 10.26 | 39.84 | 11.00 / 12.80 | 60.32 |

The single join has no join-to-join exchange to remove. Its plan kept six stages for every configuration. Differences between default and fusion-only on this control give a sense of timing variation.

## Four concurrent clients

Four clients issued the two-join query in closed loop on one endpoint. Each client sent four queries per batch. The four configurations rotated across four warmup blocks and eight measured blocks in each of two fresh JVMs, for 256 measured queries per configuration. QPS is total completions divided by total batch wall time, including result validation. Per-request latency covers planning through result delivery. CPU per query uses a single process CPU counter across the batch to avoid double counting overlapping requests.

| Two consecutive joins | Achieved QPS | Median ms | P90 ms | Mean CPU ms/query |
|---|---:|---:|---:|---:|
| Default | 122.59 | 28.87 | 41.31 | 75.99 |
| Four readers only | 96.44 | 32.47 | 59.70 | 87.28 |
| Fusion only | 111.54 | 30.38 | 43.68 | 79.85 |
| Four readers and fusion | 109.16 | 30.38 | 52.08 | 80.56 |

The fusion-only throughput difference changed direction across runs (125.46 vs 123.06 QPS in the first JVM, 100.41 vs 122.12 in the second). Four readers delivered fewer queries per second than the default in both runs. These are closed-loop completions with four in flight. An open-loop load sweep would be needed to establish sustainable throughput at a particular latency target.

## Method and controls

- **Data.** Deterministic native OFFLINE segments containing 524,288 facts (16 × 32,768), 4,096 customers and 1,024 products. The single join has 16 result groups. Adding the product join produces 128. Every timed query was freshly planned and executed, and its group keys, counts and sums were checked against totals computed directly from the input generator. Segment assignments were checked for coverage and duplicates. The PR also exercises duplicate dimension keys against H2 across two servers.
- **Resources.** One Apple M4 Pro (10 performance cores and 4 efficiency cores) with 24 GiB RAM, JDK 25.0.4.1, fixed 3 GiB heap, `-XX:ActiveProcessorCount=8`. This JVM setting sizes worker pools and does not impose a CPU quota. The host was shared. Builds and timed benchmark processes ran separately.
- **Execution.** Apache Pinot's `QueryRunnerTestBase` plans the SQL, dispatches to `QueryServerEnclosure`, runs the production leaf executor, joins, mailboxes and reducer. It bypasses broker HTTP, the result cache and dispatch RPC serialization. Same-endpoint mailboxes are in memory. Distinct endpoints use loopback gRPC. Both topologies run in one JVM. All facts reside on one endpoint. In the second topology, both dimensions reside on the other endpoint. The total is four intermediate workers (4 or 2+2).
- **Sampling.** For each topology, two fresh JVMs used seeds 1042 and 6901. Each JVM ran eight warmup queries and 24 measured queries per shape/configuration. A rotating, seeded order interleaved the eight combinations. The full sweep produced 768 measured queries. Traces were collected outside the timing window. P90 is the nearest-rank empirical quantile. Small tail differences on this host carry limited precision.
- **Unmodified master.** In four separate default-only JVM runs, the two builds ran in base / PR / PR / base order. Neither new option appeared in the SQL. The single-join and two-join plans matched and all 192 timed answers agreed. Median latency on base → PR was 11.15 → 10.90 ms for one join and 15.55 → 14.99 ms for the chain. These control runs have a different warmup mix from the full sweep. Their timing spread limits conclusions about small default-path changes.
- **Scope.** The fixture uses warm local segments with integer keys and small dimensions. Its timings quantify this Apache patch on the Java execution path. S3 cold reads, remote machines, high-latency networks and comparisons against other engines require separate measurements.

## Reproduce

Clone the evidence branch to get the exact Pinot sources and fixture used for the evaluation. Use JDK 25, Python 3 and the repository's Maven wrapper.

```sh
git clone --branch xiangfu0/codex1/broadcast-join-evaluation --single-branch https://github.com/xiangfu0/pinot.git pinot-join-eval
cd pinot-join-eval
fixture="$PWD/pinot-perf/experiments/broadcast-join-evaluation"
./mvnw -pl pinot-query-runtime -am -Dtest=MailboxSendOperatorTest -Dsurefire.failIfNoSpecifiedTests=false test
for endpoints in 1 2; do
  for seed in 1042 6901; do
    python3 "$fixture/run.py" "$PWD" -Deval.servers="$endpoints" -Deval.seed="$seed" > "single-$endpoints-$seed.log" 2>&1
  done
done
for seed in 1042 6901; do
  EVAL_MAIN=ConcurrentBroadcastJoin python3 "$fixture/run.py" "$PWD" -Deval.seed="$seed" > "concurrent-$seed.log" 2>&1
done
```

The default-only run accepts `-Deval.baseline=true` on either checkout. Build the runtime test classpath on each revision first and pass that checkout's path to the launcher. `EVALPLAN`, `EVALTRACE`, `EVAL` and `CONCURRENT` lines in the logs identify plan guards, operator traces, individual requests and concurrent batches.

Run `python3 "$fixture/summarize.py"` to regenerate the [single-client summary](single_summary.csv) and [four-client summary](concurrent_summary.csv) from the saved request and batch CSVs. The [plans](plans.csv), [mailbox counters](mailbox_sends.csv) and [separate gRPC diagnostic](grpc_debug.csv) record the physical changes behind the measurements.
