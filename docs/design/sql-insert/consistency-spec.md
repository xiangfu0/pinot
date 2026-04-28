<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# INSERT INTO Consistency Specification (v1)

## Overview

This document describes the consistency guarantees, visibility semantics, failure modes, and recovery behavior for the push-based INSERT INTO feature in Apache Pinot.

## Statement Lifecycle

```
NEW -> ACCEPTED -> PREPARED -> COMMITTED -> VISIBLE -> (GC)
           \          \           \
            +-------> ABORTED <---+
```

| State     | Meaning |
|-----------|---------|
| NEW       | Statement created but not yet acknowledged by the coordinator. |
| ACCEPTED  | Coordinator has accepted the statement and delegated to an executor. |
| PREPARED  | Data has been staged (prepared store / shard log) and is ready to commit. |
| COMMITTED | Commit decision recorded. Servers should apply the data to mutable segments. |
| VISIBLE   | All replicas have applied. Data is now queryable. |
| ABORTED   | Statement was cancelled, timed out, or failed. Cleanup may still be pending. |
| GC        | All resources (ZK manifest, prepared data, staging artifacts) have been cleaned up. |

## Consistency Guarantees

### What is guaranteed in v1

1. **Atomic visibility (file insert):** Segments from a single INSERT INTO ... FROM FILE become visible atomically via the segment replacement protocol (startReplaceSegments / endReplaceSegments). No partial set of segments is ever queryable.

2. **Idempotent replay (row insert):** The InsertRowApplier tracks applied statement+partition+sequence tuples in a persistent file. Replaying after a server restart will not produce duplicate rows.

3. **Coordinator crash recovery:** The coordinator reads all state from ZooKeeper on startup. No in-memory state is required for correctness. A new leader will resume cleanup sweeps from ZK.

4. **Server crash recovery:** The InsertRecoveryManager scans the local prepared store on startup, queries the coordinator for the authoritative state, and either replays (if COMMITTED), cleans up (if ABORTED/GC), or leaves alone (if still PREPARED).

5. **Timeout-based liveness:** Statements stuck in ACCEPTED or PREPARED beyond a configurable timeout (default 30 minutes) are automatically aborted by the coordinator. Statements stuck in COMMITTED beyond a longer timeout (default 60 minutes) are also aborted.

6. **Double-abort idempotency:** Calling abort on an already-aborted statement returns success without side effects.

### What is NOT guaranteed in v1

1. **Cross-statement ordering:** Multiple concurrent INSERT INTO statements for the same table have no guaranteed commit order. They are independent transactions.

2. **Read-your-writes from the SQL client:** After an INSERT INTO returns ACCEPTED/COMMITTED, the data may not yet be queryable. The client should poll the statement status until it reaches VISIBLE.

3. **Exactly-once for row inserts across replicas:** In v1, row inserts target a single server per partition. Multi-replica consistency (replicating prepared data to all replicas) is deferred to v2.

4. **Transactional rollback of partial applies:** If a server crashes after applying some partitions but not others for a COMMITTED statement, the recovery manager will replay the remaining partitions. However, for a brief window during recovery, queries may see partial data.

5. **Sub-second visibility latency:** The COMMITTED -> VISIBLE transition depends on server-side apply completion notification. Under normal conditions this is fast, but is not bounded by an SLA.

## Visibility Semantics

### File Inserts

- Segments are NOT queryable until `endReplaceSegments` completes, which transitions the lineage entry to COMPLETED.
- The coordinator calls `endReplaceSegments` only when the Minion task finishes and the statement is committed.
- If aborted, `revertReplaceSegments` is called to roll back the lineage entry, ensuring no segments leak.

### Row Inserts

- Rows written to the prepared store are NOT queryable.
- The InsertRowApplier only reads from the prepared store and indexes rows into the mutable segment when the statement state is COMMITTED.
- The mutable segment makes data queryable immediately upon indexing. Therefore, the COMMITTED -> VISIBLE transition represents the point at which the coordinator has confirmed all replicas have applied.

## Failure Modes and Recovery

### Server crashes during PREPARED

- Prepared data persists on disk in the prepared store.
- On restart, InsertRecoveryManager queries the coordinator.
- If the coordinator says COMMITTED: replay the data.
- If the coordinator says ABORTED: clean up the prepared data.
- If the coordinator says PREPARED: leave the data. The coordinator timeout will eventually abort it if the executor never finishes.

### Server crashes during COMMITTED (after partial apply)

- The applied-statements tracking file records which partitions have been applied.
- On restart, InsertRecoveryManager replays only the unapplied partitions (idempotent).
- After successful replay, the server notifies the coordinator via ApplyCompleteCallback.

### Coordinator unreachable during server recovery

- The InsertRecoveryManager retries with exponential backoff (1s, 2s, 4s, ..., up to 30s per retry, 5 retries max by default).
- If all retries are exhausted, the statement is left in its current state. The coordinator's cleanup sweep will handle it once the coordinator becomes reachable.
- Server startup is NOT blocked; recovery runs asynchronously.

### Controller failover

- The new leader starts the cleanup scheduler, which reads all manifests from ZK.
- Tables are tracked via the `_tablesWithStatements` set, which is populated as new statements arrive and during cleanup sweeps.
- No manual intervention is required after controller failover.

### Abort during PREPARED

- The executor's `abort()` method is called, which cleans up the prepared store data.
- For file inserts, `revertReplaceSegments` is called.
- The manifest is moved to ABORTED in ZK.

### Abort during COMMITTED

- If data has already been applied to mutable segments on some servers, it cannot be un-applied in v1.
- The abort will prevent the statement from reaching VISIBLE, but servers that have already applied may serve the data.
- This is a known limitation of v1. Full rollback of applied data is deferred to v2.

## Garbage Collection

- VISIBLE statements are retained in ZK for a configurable period (default 24 hours) for observability and debugging.
- After the retention period, the coordinator deletes the ZK manifest node.
- ABORTED statements follow the same retention and GC policy.
- On the server side, the InsertRowApplier's applied-statements tracking file can be compacted via `cleanAppliedEntries()` after a statement is GC'd.
- For file inserts, the FileInsertArtifactPublisher cleans up staging artifacts on abort or after publish.

## Operator-Visible States

Operators can query statement status via the REST API (`/insert/status`). The response includes:

- `statementId`: unique identifier
- `state`: one of the lifecycle states above
- `message`: human-readable status or error message
- `segmentNames`: list of segments produced (if any)
- `errorCode`: machine-readable error code (if aborted)

The `/insert/list` endpoint shows all statements for a table, useful for monitoring stuck or failed statements.

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `InsertStatementsSubmitted` | Counter | Total statements submitted |
| `InsertStatementsCommitted` | Counter | Statements that reached COMMITTED |
| `InsertStatementsAborted` | Counter | Statements that reached ABORTED (including timeout) |
| `InsertStatementsVisible` | Counter | Statements that reached VISIBLE |
| `InsertStatementsGarbageCollected` | Counter | Statements deleted from ZK during GC |
| `InsertStatementsRecoveryReplayed` | Counter | Statements replayed during server recovery |
| `InsertStatementsRecoveryCleaned` | Counter | Statements cleaned up during server recovery |
| `InsertStatementsRecoveryFailed` | Counter | Statements that failed recovery |
| `InsertStatementsActive` | Gauge | Currently active (non-terminal) statements |
