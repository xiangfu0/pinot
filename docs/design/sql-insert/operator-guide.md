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

# INSERT INTO Operator Guide

## Overview

This document covers storage requirements, configuration, monitoring, troubleshooting, and recovery behavior for operators managing clusters with the push-based INSERT INTO feature enabled.

## Storage Requirements

### Server-side prepared store

Row inserts stage data under the server's data directory:

```
<dataDir>/insert/prepared/<statementId>/<partitionId>/
```

Each partition directory contains serialized row batches. These files are cleaned up after a statement reaches VISIBLE or ABORTED state. Size depends on the volume of row data per statement.

**Sizing guidance:** Allow headroom equal to the maximum expected in-flight row insert data. For example, if you expect at most 10 concurrent row inserts each with 100 MB of data, provision 1 GB of additional disk space per server.

### Controller-side ZooKeeper state

Statement manifests are stored in ZooKeeper under a per-table path. Each manifest is a small JSON document (typically under 1 KB). Manifests for completed and aborted statements are retained for the configurable GC retention period (default: 24 hours) before deletion.

### File insert staging

File inserts use the existing Minion SegmentGenerationAndPushTask pipeline. Staging data is stored wherever the Minion task writes intermediate segments (typically the Minion's working directory or a configured deep store).

## Configuration Options

### Coordinator timeouts (Controller)

| Property | Default | Description |
|----------|---------|-------------|
| Statement timeout | 30 minutes | Statements stuck in ACCEPTED or PREPARED beyond this duration are automatically aborted. |
| Committed timeout | 60 minutes | Statements stuck in COMMITTED beyond this duration are aborted (servers may have failed to apply). |
| Visible retention | 24 hours | VISIBLE and ABORTED manifests are retained in ZK for this period before GC. |
| Cleanup interval | 5 minutes | How often the background cleanup sweep runs. |

These are set via the `InsertStatementCoordinator` constructor parameters. The 3-argument constructor uses defaults; the 6-argument constructor accepts custom values for `statementTimeoutMs`, `committedTimeoutMs`, and `visibleRetentionMs`.

### Recovery (Server)

| Property | Default | Description |
|----------|---------|-------------|
| Max retries | 5 | Maximum number of retry attempts when the coordinator is unreachable during recovery. |
| Initial backoff | 1 second | Initial delay before the first retry. |
| Max backoff | 30 seconds | Maximum delay between retries (exponential backoff). |

Recovery runs asynchronously on server startup and does not block server initialization.

## Metrics

### Controller metrics

| Metric | Type | Description |
|--------|------|-------------|
| `InsertStatementsSubmitted` | Meter | Total statements submitted via the coordinator. |
| `InsertStatementsCommitted` | Meter | Statements that reached COMMITTED state. |
| `InsertStatementsAborted` | Meter | Statements that reached ABORTED (including timeouts). |
| `InsertStatementsVisible` | Meter | Statements that reached VISIBLE state. |
| `InsertStatementsGC` | Meter | Statement manifests garbage collected from ZK. |
| `InsertStatementsActive` | Gauge | Currently active (non-terminal) statements across all tables. |

### Server recovery metrics

| Metric | Type | Description |
|--------|------|-------------|
| `InsertStatementsRecoveryReplayed` | Counter | Statements replayed during server recovery. |
| `InsertStatementsRecoveryCleaned` | Counter | Statements cleaned up during server recovery. |
| `InsertStatementsRecoveryFailed` | Counter | Statements that failed recovery. |

### What to alert on

- **`InsertStatementsActive` consistently increasing:** Statements may be getting stuck. Check if executors are healthy and the coordinator cleanup sweep is running.
- **`InsertStatementsAborted` spiking:** Frequent aborts may indicate executor failures, network issues, or misconfigured timeouts.
- **`InsertStatementsRecoveryFailed` > 0 after server restart:** Some statements could not be recovered. Check server logs for details.

## REST API Reference

All endpoints are on the controller unless noted otherwise.

### Submit an INSERT request

```
POST /insert/execute
Content-Type: application/json

{
  "statementId": "optional-custom-id",
  "requestId": "idempotency-key",
  "tableName": "myTable",
  "tableType": "OFFLINE",
  "insertType": "ROW",
  "rows": [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25}
  ],
  "consistencyMode": "WAIT_FOR_ACCEPT"
}
```

### Check statement status

```
GET /insert/status/{statementId}?table=myTable_OFFLINE
```

Response:

```json
{
  "statementId": "abc-123",
  "state": "VISIBLE",
  "message": null,
  "segmentNames": ["myTable_OFFLINE_0_0_20260322T0000Z"],
  "errorCode": null
}
```

### Abort a statement

```
POST /insert/abort/{statementId}?table=myTable_OFFLINE
```

Double-abort is idempotent: aborting an already-aborted statement returns success. You cannot abort a VISIBLE statement (data is already queryable).

### List statements for a table

```
GET /insert/list?table=myTable_OFFLINE
```

Returns all statements (active and terminal within the GC retention period) for the given table.

### Server-side row insert endpoint

```
POST /insert/rows  (on the server)
Content-Type: application/json

{
  "statementId": "abc-123",
  "tableName": "myTable_OFFLINE",
  "tableType": "OFFLINE",
  "rows": [
    {"name": "Alice", "age": 30}
  ]
}
```

This endpoint is used internally by the coordinator to dispatch rows to the target server. It is not typically called by users directly.

## Aborting Stuck Statements

### Automatic cleanup

The coordinator runs a background cleanup sweep every 5 minutes that:

1. Aborts statements stuck in ACCEPTED or PREPARED beyond the statement timeout (default 30 min).
2. Aborts statements stuck in COMMITTED beyond the committed timeout (default 60 min).
3. Deletes VISIBLE and ABORTED manifests beyond the GC retention period (default 24 hours).

### Manual abort

To manually abort a stuck statement:

```bash
curl -X POST "http://controller:9000/insert/abort/{statementId}?table=myTable_OFFLINE"
```

### Identifying stuck statements

List all statements for a table and look for non-terminal states (ACCEPTED, PREPARED, COMMITTED) with old timestamps:

```bash
curl "http://controller:9000/insert/list?table=myTable_OFFLINE"
```

## Recovery Behavior on Restart

### Server restart

On startup, the `InsertRecoveryManager` runs asynchronously:

1. Scans the prepared store (`<dataDir>/insert/prepared/`) for statement directories.
2. For each statement, queries the coordinator for its authoritative state.
3. Takes action based on the state:
   - **COMMITTED:** Replays the prepared data to mutable segments, then notifies the coordinator.
   - **VISIBLE:** Replays idempotently (the applier deduplicates), then cleans up prepared data.
   - **ABORTED / GC:** Cleans up the prepared data.
   - **PREPARED / ACCEPTED / NEW:** Leaves the data alone; the coordinator timeout will eventually resolve it.
4. If the coordinator is unreachable, retries with exponential backoff (1s, 2s, 4s, ..., up to 30s, max 5 retries). If all retries fail, leaves the statement for coordinator decision.

Server startup is not blocked by recovery.

### Controller failover

The new controller leader starts the cleanup scheduler, which reads all state from ZooKeeper. No in-memory state is required for correctness. The `_tablesWithStatements` set is repopulated as statements arrive and during cleanup sweeps. No manual intervention is needed.

## Troubleshooting

### Statement stuck in ACCEPTED

**Cause:** The executor may have failed to process the request, or the server hosting the executor is down.

**Action:**
1. Check controller logs for executor errors related to the statement ID.
2. Check if the target server is healthy.
3. If the server is down, wait for automatic timeout (30 min) or abort manually.

### Statement stuck in COMMITTED

**Cause:** Servers may have failed to apply the data, or the apply-complete notification was lost.

**Action:**
1. Check server logs for apply errors.
2. If the server restarted, recovery should handle it automatically.
3. The committed timeout (60 min) will eventually abort it.
4. Data that was already applied to some servers may be queryable even though the statement is not VISIBLE.

### Statement stuck in PREPARED

**Cause:** The executor prepared the data but has not yet received a commit decision, or the commit call failed.

**Action:**
1. Check controller logs for commit errors.
2. Wait for automatic timeout (30 min) or abort manually.

### File insert fails with TABLE_MODE_REJECTED

**Cause:** The table is configured with partial upsert or dedup, which are not supported for file inserts in v1.

**Action:** Use stream ingestion for these table types.

### Row insert fails with PARTITION_CONFIG_ERROR

**Cause:** The table's partition configuration is invalid or missing.

**Action:** Verify the table config has a valid partition function configured. For upsert/dedup tables, partition configuration is required.

### Recovery fails after server restart

**Cause:** The prepared store data may be corrupted, or the coordinator was unreachable during all retry attempts.

**Action:**
1. Check server logs for `InsertRecoveryManager` errors.
2. If the coordinator was unreachable, recovery will leave the statements for coordinator decision.
3. Once the coordinator is reachable again, its cleanup sweep will handle stuck statements.
4. If prepared data is corrupted, the statement will fail recovery and be logged. The coordinator timeout will abort it.

## Rolling upgrade and rollback

### Rolling upgrade order

1. Upgrade controllers first, with `controller.insert.enabled=false` (the default).
2. Upgrade brokers next; brokers need the new SQL grammar to parse `INSERT INTO ... VALUES`.
3. Once all controllers and brokers are on the new version, set `controller.insert.enabled=true` to enable the feature.

The forward-compat protocol (envelope `schemaVersion` + body `schemaVersion` + `@JsonAnyGetter`/`@JsonAnySetter` carrier) ensures a controller running an older schema version refuses to read manifests written by a newer peer rather than silently mis-parsing them. Operators see explicit `IOException` errors when this happens.

### Downgrade caveat

When `controller.insert.enabled=true` has been set on at least one controller, the cluster begins
writing to two new ZK trees:

- `/INSERT_STATEMENTS/{table}/{statementId}` — manifest envelopes
- `/INSERT_REQUEST_IDS/{table}/{requestId}` — idempotency reservations and tombstones

A downgrade to a controller version that predates this feature will leave those trees orphaned —
the older controller has no consumer that reads or GCs them, and they will continue to grow if a
fresh controller (running the new code) is later started against the same ZK quorum.

Before downgrading:

1. Drain all in-flight INSERT statements by waiting until `/insert/list` returns empty for every
   table (or set `controller.insert.enabled=false` and wait for the cleanup sweep to GC retained
   manifests after `visibleRetentionMs`, default 24h).
2. After draining, manually delete the two ZK paths (`/INSERT_STATEMENTS` and `/INSERT_REQUEST_IDS`)
   to avoid orphan accumulation if the cluster is later upgraded again.

This is a v1 limitation. Future versions may add a controller-side "drain and delete" admin
endpoint to automate step 2.

