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

# INSERT INTO Examples

This document provides copy-paste ready examples for common INSERT INTO operations.

## 1. Basic Row Insert into an OFFLINE Table

```sql
INSERT INTO userEvents (userId, eventType, timestamp)
VALUES ('user_001', 'click', 1711123200000)
```

If `userEvents` only exists as an OFFLINE table, the table type is auto-detected.

## 2. Multi-Row Insert into a REALTIME Table

```sql
INSERT INTO pageViews (userId, pageUrl, viewTimeMs) VALUES
  ('user_001', '/home', 1500),
  ('user_002', '/products', 2300),
  ('user_003', '/checkout', 890)
SET tableType='REALTIME'
```

All three rows are processed in a single statement. They are routed to partitions based on the table's partition function configuration.

## 3. File Insert

```sql
INSERT INTO salesData FROM FILE 's3://my-data-bucket/sales/2026-03-22/'
SET inputFormat='parquet', tableType='OFFLINE'
```

This schedules a Minion SegmentGenerationAndPushTask that reads all Parquet files in the given directory, generates segments, and publishes them atomically.

## 4. Hybrid Table with Explicit Type

When `orders` exists as both OFFLINE and REALTIME:

```sql
-- Insert into the OFFLINE side
INSERT INTO orders (orderId, customerId, amount) VALUES ('ord-100', 'cust-42', 99.99)
SET tableType='OFFLINE'

-- Insert into the REALTIME side
INSERT INTO orders (orderId, customerId, amount) VALUES ('ord-101', 'cust-43', 149.50)
SET tableType='REALTIME'
```

Omitting `tableType` for a hybrid table produces an error:

```
Table 'orders' is a hybrid table. Please specify tableType (OFFLINE or REALTIME)
via SET tableType='OFFLINE' or SET tableType='REALTIME'
```

## 5. Idempotent Retry with requestId

```sql
INSERT INTO userEvents (userId, eventType, timestamp)
VALUES ('user_001', 'click', 1711123200000)
SET requestId='ingest-batch-2026-03-22-001'
```

If this statement fails due to a transient error (e.g., network timeout) and you retry with the same `requestId` and the same row data, the server returns the result of the original execution without creating duplicates.

If you accidentally use the same `requestId` with different data, the server returns:

```json
{
  "statementId": "...",
  "state": "ABORTED",
  "errorCode": "IDEMPOTENCY_CONFLICT",
  "message": "Request id 'ingest-batch-2026-03-22-001' already used with a different payload"
}
```

## 6. Checking Insert Status

After submitting an INSERT, note the `statementId` from the response:

```json
{
  "statementId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "state": "ACCEPTED",
  "message": "Insert accepted and prepared with 3 rows across 2 partition(s)"
}
```

Poll the status endpoint using curl:

```bash
curl "http://controller:9000/insert/status/a1b2c3d4-e5f6-7890-abcd-ef1234567890?table=pageViews_REALTIME"
```

Example responses as the statement progresses:

```json
{"statementId": "a1b2c3d4-...", "state": "PREPARED", "message": null}
```

```json
{"statementId": "a1b2c3d4-...", "state": "COMMITTED", "message": null}
```

```json
{"statementId": "a1b2c3d4-...", "state": "VISIBLE", "message": null, "segmentNames": ["pageViews_REALTIME_0_0"]}
```

Once the state is `VISIBLE`, the data is queryable.

## 7. Listing All Statements for a Table

```bash
curl "http://controller:9000/insert/list?table=pageViews_REALTIME"
```

Response:

```json
[
  {
    "statementId": "a1b2c3d4-...",
    "state": "VISIBLE",
    "message": null,
    "segmentNames": ["pageViews_REALTIME_0_0"]
  },
  {
    "statementId": "x9y8z7w6-...",
    "state": "ACCEPTED",
    "message": "Insert accepted and prepared with 1 rows across 1 partition(s)"
  }
]
```

## 8. Aborting an Insert

To abort a statement that is stuck or no longer needed:

```bash
curl -X POST "http://controller:9000/insert/abort/a1b2c3d4-e5f6-7890-abcd-ef1234567890?table=pageViews_REALTIME"
```

Response:

```json
{
  "statementId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "state": "ABORTED",
  "message": "Statement aborted"
}
```

Notes:
- Aborting an already-aborted statement is idempotent and returns success.
- You cannot abort a VISIBLE statement because the data is already queryable.
- For file inserts, aborting reverts the segment replacement lineage entry so no partial segments become visible.

## 9. Insert with Database Prefix

If you use multiple databases:

```sql
INSERT INTO myDatabase.userEvents (userId, eventType) VALUES ('user_001', 'click')
```

## 10. Full Workflow Example

Here is a complete workflow from insert to query:

```bash
# 1. Submit the insert
curl -X POST "http://broker:8099/sql" \
  -H "Content-Type: application/json" \
  -d '{"sql": "INSERT INTO metrics (host, cpu, ts) VALUES ('\''server1'\'', 85.2, 1711123200000) SET tableType='\''REALTIME'\''"}'

# Response includes statementId
# {"statementId":"abc-123","state":"ACCEPTED",...}

# 2. Poll until VISIBLE
curl "http://controller:9000/insert/status/abc-123?table=metrics_REALTIME"
# Repeat until state is "VISIBLE"

# 3. Query the data
curl -X POST "http://broker:8099/sql" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT host, cpu, ts FROM metrics WHERE host = '\''server1'\''"}'
```
