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

# INSERT INTO User Guide

## Overview

Apache Pinot supports push-based INSERT INTO statements that allow you to insert rows directly into tables via SQL or ingest data from external files. This feature provides a familiar SQL interface for data ingestion alongside Pinot's existing stream and batch ingestion pipelines.

## Supported SQL Syntax

### Row Insert

```sql
INSERT INTO tableName VALUES (val1, val2, ...)
```

```sql
INSERT INTO tableName (col1, col2, col3) VALUES (val1, val2, val3)
```

```sql
INSERT INTO tableName VALUES (val1, val2, ...), (val1, val2, ...), (val1, val2, ...)
```

### File Insert

```sql
INSERT INTO tableName FROM FILE 'file:///path/to/data.csv' SET(key='value', ...)
```

### Options

Options are specified using the `SET` clause appended to the statement:

| Option | Description | Required |
|--------|-------------|----------|
| `tableType` | Target table type: `OFFLINE` or `REALTIME`. Required for hybrid tables. | Conditional |
| `requestId` | User-supplied idempotency key. If the same `requestId` with the same payload is sent again, the server returns the existing result instead of re-executing. | No |

## Row Insert Examples

### Single row into an OFFLINE table

```sql
INSERT INTO myTable VALUES ('Alice', 30, 'Engineering')
```

If `myTable` exists only as an OFFLINE table, the type is auto-detected.

### Single row with explicit columns

```sql
INSERT INTO myTable (name, age, department) VALUES ('Bob', 25, 'Sales')
```

Columns that are not listed receive their default values based on the schema.

### Multi-row insert

```sql
INSERT INTO myTable (name, age, department) VALUES
  ('Alice', 30, 'Engineering'),
  ('Bob', 25, 'Sales'),
  ('Carol', 28, 'Marketing')
```

All rows in a single statement are processed atomically -- they either all succeed or all fail.

### Insert into a REALTIME table

```sql
INSERT INTO myTable (name, age, department) VALUES ('Dave', 35, 'Product')
SET tableType='REALTIME'
```

When only a REALTIME table exists for the given name, `tableType` can be omitted.

### Hybrid table with explicit type

When a table exists as both OFFLINE and REALTIME (a hybrid table), you must specify the target type. Omitting it produces an error.

```sql
INSERT INTO myHybridTable (name, age) VALUES ('Eve', 40)
SET tableType='OFFLINE'
```

Error if `tableType` is omitted for a hybrid table:

```
Table 'myHybridTable' is a hybrid table. Please specify tableType (OFFLINE or REALTIME)
via SET tableType='OFFLINE' or SET tableType='REALTIME'
```

### Idempotent insert with requestId

```sql
INSERT INTO myTable (name, age) VALUES ('Frank', 45)
SET requestId='req-abc-123'
```

If you retry this exact statement with the same `requestId` and the same row data, the server returns the result of the original execution without inserting duplicate rows. If the same `requestId` is used with different row data, the server returns an `IDEMPOTENCY_CONFLICT` error.

## File Insert Examples

### Insert from a local file

```sql
INSERT INTO myTable FROM FILE 'file:///data/input/users.csv'
SET inputFormat='csv'
```

### Insert from S3

```sql
INSERT INTO myTable FROM FILE 's3://my-bucket/data/users.parquet'
SET inputFormat='parquet'
```

File inserts are executed asynchronously via a Minion `SegmentGenerationAndPushTask`. The statement returns immediately with an `ACCEPTED` state; use the status API to track progress.

## Checking Insert Status

After submitting an insert, you receive a `statementId`. Use the REST API to check its progress:

```
GET /insert/status/{statementId}?table=myTable_OFFLINE
```

Response:

```json
{
  "statementId": "abc-123",
  "state": "VISIBLE",
  "message": null,
  "segmentNames": ["myTable_OFFLINE_0_0_20260322T0000Z"]
}
```

## Statement Lifecycle

Each INSERT statement progresses through these states:

```
NEW -> ACCEPTED -> PREPARED -> COMMITTED -> VISIBLE -> (GC)
         \           \           \
          +--------> ABORTED <---+
```

| State | Meaning |
|-------|---------|
| NEW | Statement created but not yet acknowledged. |
| ACCEPTED | Coordinator has accepted the statement and delegated to an executor. |
| PREPARED | Data has been staged and is ready to commit. |
| COMMITTED | Commit decision recorded; data is being applied to segments. |
| VISIBLE | Data is queryable. This is the terminal success state. |
| ABORTED | Statement was cancelled, timed out, or failed. |
| GC | Resources have been cleaned up (internal state, not user-visible). |

## Consistency Modes

v1 ships with a single consistency mode:

| Mode | Behavior |
|------|----------|
| `WAIT_FOR_ACCEPT` | Returns after the coordinator accepts the statement. This is the default and only supported mode in v1. |

With this mode, you should poll the `/insert/status` endpoint to confirm the data becomes visible before querying it. Future versions may add `FIRE_AND_FORGET` (return immediately after enqueue) and `WAIT_FOR_VISIBLE` (return after data is queryable); those names are not yet pinned to specific wire semantics.

## Error Messages

| Error Code | Meaning |
|------------|---------|
| `TABLE_RESOLUTION_ERROR` | Table does not exist, or is a hybrid table without an explicit `tableType`. |
| `NO_EXECUTOR` | No executor is registered for the insert type (row or file). The feature may not be enabled. |
| `IDEMPOTENCY_CONFLICT` | The same `requestId` was used with different payload data. |
| `STORE_ERROR` | Failed to persist the statement manifest to ZooKeeper. |
| `EXECUTOR_ERROR` | The executor failed during execution. See the message for details. |
| `INVALID_TABLE` | Table name is missing or empty. |
| `EMPTY_ROWS` | Row insert with no rows provided. |
| `TABLE_NOT_FOUND` | The resolved table name does not exist in the cluster. |
| `PARTITION_CONFIG_ERROR` | Table partition configuration is invalid for row insert routing. |
| `WRITE_ERROR` | Failed to write rows to the shard log or prepared store. |
| `TABLE_MODE_REJECTED` | Table mode (partial upsert, dedup) is not supported for file insert in v1. |
| `INVALID_FILE_URI` | The file URI is empty or syntactically invalid. |
| `SEGMENT_REPLACE_FAILED` | Failed to start the segment replacement protocol. |
| `TASK_SCHEDULE_FAILED` | Failed to schedule the Minion task for file insert. |

## Limitations in v1

1. **No INSERT INTO ... SELECT.** Subquery-based inserts are not supported. Use `VALUES` or `FROM FILE`.

2. **No cross-statement ordering.** Multiple concurrent INSERT statements for the same table have no guaranteed commit order.

3. **No read-your-writes guarantee from the SQL client.** After an INSERT returns `ACCEPTED`, the data is not yet queryable. Poll the status endpoint until the state reaches `VISIBLE`.

4. **Single-replica row inserts.** Row inserts target a single server per partition in v1. Multi-replica consistency is deferred to v2.

5. **No rollback of applied data.** If a statement is aborted after reaching `COMMITTED` and some servers have already applied the data, the applied data cannot be un-applied. Full rollback is deferred to v2.

6. **Partial upsert and dedup tables reject file inserts.** Tables with partial upsert or dedup enabled cannot use `INSERT INTO ... FROM FILE`. Use stream ingestion instead.

7. **Full upsert tables require partition configuration for file inserts.** The table must have `segmentPartitionConfig` configured in the indexing config.

8. **No sub-second visibility SLA.** The time between `COMMITTED` and `VISIBLE` depends on server-side apply completion and is not bounded.
