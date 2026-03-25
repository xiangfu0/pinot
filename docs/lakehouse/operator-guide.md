# Lakehouse-Native Pinot Operator Guide

## Overview

Lakehouse-native mode allows Apache Pinot to query Apache Iceberg tables in place, using
Parquet as the base storage format, without importing data into Pinot-native segments. Pinot
groups Iceberg data files into **tablets** (supersegments) as the Helix-visible routing and
assignment unit, avoiding the one-file-one-segment control-plane bottleneck that would occur
with large Iceberg tables containing hundreds of thousands of files.

Key properties:

- **Query-in-place**: Pinot reads Parquet files directly from your data lake. No ETL or data
  duplication required.
- **Tablet abstraction**: Multiple Iceberg data files are grouped into a single tablet. Tablets
  are the unit of Helix partition assignment, broker routing, and server execution.
- **Snapshot pinning**: Queries are pinned to a specific Iceberg snapshot for consistency.
  Snapshots are refreshed periodically by the controller.
- **Opt-in**: Lakehouse mode is fully additive. Existing Pinot-native OFFLINE, REALTIME, and
  HYBRID tables are unaffected.
- **Sidecar indexes** (future): Optional Pinot-specific acceleration indexes layered on top of
  Iceberg/Parquet. Sidecars are never required for correctness.

## Prerequisites

1. **Iceberg catalog**: A running Iceberg catalog accessible from the Pinot controller. Supported
   catalog types: REST, Hadoop, Hive, Glue, Nessie, JDBC.
2. **Parquet data files**: The Iceberg table must use Parquet as its file format.
3. **Network access**: The Pinot controller must be able to reach the catalog URI. Pinot servers
   must be able to read data files from the warehouse location (S3, HDFS, GCS, ADLS, etc.).
4. **Credentials**: Catalog authentication credentials must be configured via the `properties`
   map in the catalog config.
5. **Pinot version**: The lakehouse-native feature requires Apache Pinot with the
   `pinot-iceberg` catalog plugin deployed. Ensure the plugin JAR is on the controller and
   server classpaths.

## Creating a Lakehouse Table

### Step 1: Create a Pinot schema

Create a Pinot schema that matches the columns you want to query from the Iceberg table.
The schema does not need to include every Iceberg column -- only the columns referenced in
queries.

```bash
curl -X POST "http://localhost:9000/schemas" \
  -H "Content-Type: application/json" \
  -d @events-schema.json
```

See [examples/events-schema.json](examples/events-schema.json) for a complete schema example.

### Step 2: Create the table config

Create a table config with `lakehouseConfig` enabled. The table type can be OFFLINE (read-only
from Iceberg) or REALTIME (with optional direct write).

```bash
curl -X POST "http://localhost:9000/tables" \
  -H "Content-Type: application/json" \
  -d @iceberg-rest-offline-table.json
```

See the [examples/](examples/) directory for complete table configs:
- [iceberg-rest-offline-table.json](examples/iceberg-rest-offline-table.json) -- OFFLINE table
  with REST catalog
- [iceberg-hadoop-offline-table.json](examples/iceberg-hadoop-offline-table.json) -- OFFLINE
  table with Hadoop catalog
- [iceberg-realtime-table.json](examples/iceberg-realtime-table.json) -- REALTIME table with
  write enabled

### Step 3: Verify the table

After creation, the controller's periodic tablet refresh task (default: every 60 seconds) will
resolve the Iceberg snapshot, list data files, group them into tablets, and register the tablets
with Helix. You can trigger a manual refresh or inspect the result:

```bash
# Trigger a manual refresh
curl -X POST "http://localhost:9000/lakehouse/tables/events_OFFLINE/refresh"

# Check the table summary
curl "http://localhost:9000/lakehouse/tables/events_OFFLINE/summary"

# List all tablets
curl "http://localhost:9000/lakehouse/tables/events_OFFLINE/tablets"
```

## Table Config Reference

The `lakehouseConfig` section is a top-level field in the Pinot TableConfig JSON. All fields
below are nested under `lakehouseConfig`.

### Top-level fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | Yes | -- | Set to `true` to enable lakehouse-native mode for this table. |
| `mode` | enum | Yes | -- | Lakehouse storage mode. Currently only `ICEBERG_NATIVE` is supported. |
| `catalog` | object | Yes | -- | Iceberg catalog connection configuration. See below. |
| `read` | object | No | See defaults | Read path configuration. See below. |
| `write` | object | No | `null` | Write path configuration (REALTIME tables only). See below. |
| `tablet` | object | No | See defaults | Tablet sizing and grouping configuration. See below. |
| `sidecars` | object | No | `null` | Sidecar index configuration (future). See below. |

### catalog

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | enum | Yes | -- | Catalog type: `REST`, `HADOOP`, `HIVE`, `GLUE`, `NESSIE`, `JDBC`. |
| `uri` | string | No | `null` | Catalog URI. For REST catalogs, the endpoint URL. For Hive, the metastore URI. Not required for Hadoop or Glue catalogs. |
| `warehouse` | string | No | `null` | Warehouse location (e.g. `s3://my-warehouse`). Required for Hadoop catalogs. |
| `tableIdentifier` | string | Yes | -- | Fully qualified Iceberg table identifier (e.g. `analytics.events`). |
| `properties` | map | No | `null` | Additional catalog-specific properties. Used for authentication, S3 configuration, custom IO implementations, etc. |

#### Catalog properties examples

**REST catalog with OAuth2:**
```json
{
  "credential": "client_credentials",
  "oauth2-server-uri": "https://auth.example.com/token"
}
```

**Hadoop catalog with S3:**
```json
{
  "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
  "s3.region": "us-east-1",
  "s3.access-key-id": "AKIA...",
  "s3.secret-access-key": "..."
}
```

**Glue catalog:**
```json
{
  "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
  "glue.region": "us-east-1"
}
```

### read

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `visibilityMode` | enum | No | `HOT_PLUS_SNAPSHOT` | Read visibility mode. `SNAPSHOT_ONLY` queries only committed Iceberg snapshots (stronger consistency, slower freshness). `HOT_PLUS_SNAPSHOT` queries the committed snapshot plus hot rows beyond the committed watermark (Pinot-style freshness, not fully transactional before commit). |
| `defaultRefType` | enum | No | `BRANCH` | Default Iceberg ref type for snapshot resolution: `BRANCH`, `TAG`, or `SNAPSHOT_ID`. |
| `defaultRefName` | string | No | `"main"` | Default Iceberg ref name. For `BRANCH`, typically `"main"`. For `TAG`, the tag name. For `SNAPSHOT_ID`, the snapshot ID as a string. |

### write

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | Yes | -- | Set to `true` to enable direct Iceberg write from the Pinot realtime writer. Only valid for REALTIME tables. |
| `mode` | enum | No | `APPEND` | Write mode: `APPEND` (append-only writes) or `CDC` (changelog for mutable workloads). |
| `fileFormat` | string | No | `"PARQUET"` | Base file format for written data files. |

### tablet

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `targetFilesPerTablet` | integer | No | `128` | Target number of Iceberg data files per tablet. The grouper splits a tablet when this limit is reached. |
| `targetBytesPerTablet` | long | No | `68719476736` (64 GiB) | Target total bytes per tablet. The grouper splits a tablet when this limit is reached. |
| `timeBucket` | string | No | `"DAY"` | Time bucket granularity for tablet grouping (e.g. `DAY`, `HOUR`). Files are grouped by partition and time bucket, then split into generations within each group. |

### sidecars (future)

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `storeURI` | string | No | `null` | URI for sidecar index storage (e.g. `s3://my-bucket/sidecars/`). |
| `indexes` | list | No | `null` | List of sidecar index types to build: `INVERTED`, `RANGE`, `JSON`, `NATIVE_TEXT`, `VECTOR`, `STAR_TREE`, `BLOOM_FILTER`, `TIMESTAMP`, `H3`. |

## Query Options

Lakehouse-native tables support per-query Iceberg snapshot resolution via query options. These
override the table's default read configuration for a single query. At most one option should
be set per query. If none are set, the table's default ref (from `read.defaultRefType` /
`read.defaultRefName`) is used.

| Query Option | Type | Description |
|-------------|------|-------------|
| `icebergSnapshotId` | long | Pin the query to a specific Iceberg snapshot ID. |
| `icebergBranch` | string | Resolve the query against a named Iceberg branch. |
| `icebergTag` | string | Resolve the query against a named Iceberg tag. |
| `icebergAsOfMs` | long | Resolve the query to the snapshot valid at the given epoch millisecond timestamp. |

### Usage examples

**SQL with query option (snapshot ID):**
```sql
SET icebergSnapshotId = 3847562981234567890;
SELECT event_type, COUNT(*) FROM events GROUP BY event_type LIMIT 10
```

**SQL with query option (branch):**
```sql
SET icebergBranch = 'staging';
SELECT * FROM events WHERE event_timestamp > 1700000000000 LIMIT 100
```

**SQL with query option (tag):**
```sql
SET icebergTag = 'release-2026-03';
SELECT COUNT(*) FROM events
```

**SQL with query option (as-of timestamp):**
```sql
SET icebergAsOfMs = 1711324800000;
SELECT user_id, event_type FROM events WHERE country = 'US' LIMIT 50
```

## Admin APIs

The controller exposes REST endpoints under `/lakehouse` for managing lakehouse-native tables.

### GET /lakehouse/tables/{tableName}/tablets

Lists all tablet metadata envelopes for the specified table.

```bash
curl "http://localhost:9000/lakehouse/tables/events_OFFLINE/tablets"
```

Response: JSON array of `TabletMetadataEnvelope` objects containing tablet ID, snapshot ID,
partition info, time range, row count, size, microsegment count, and manifest pointer URI.

### POST /lakehouse/tables/{tableName}/refresh

Forces an immediate refresh of Iceberg snapshot and tablet metadata. Normally the controller
refreshes automatically every 60 seconds (configurable).

```bash
curl -X POST "http://localhost:9000/lakehouse/tables/events_OFFLINE/refresh"
```

Response:
```json
{
  "tableName": "events_OFFLINE",
  "tabletCount": 42,
  "status": "SUCCESS",
  "snapshotId": 3847562981234567890
}
```

### GET /lakehouse/tables/{tableName}/tablets/{tabletId}

Returns the metadata envelope for a specific tablet.

```bash
curl "http://localhost:9000/lakehouse/tables/events_OFFLINE/tablets/events_spec0_p0_d20260325_g0"
```

### GET /lakehouse/tables/{tableName}/summary

Returns a summary of the lakehouse table including snapshot info and aggregate tablet
statistics.

```bash
curl "http://localhost:9000/lakehouse/tables/events_OFFLINE/summary"
```

Response:
```json
{
  "tableName": "events_OFFLINE",
  "lakehouseMode": "ICEBERG_NATIVE",
  "catalogType": "REST",
  "tableIdentifier": "analytics.events",
  "tabletCount": 42,
  "snapshotId": 3847562981234567890,
  "totalRows": 1500000000,
  "totalBytes": 245760000000,
  "totalMicrosegments": 4200
}
```

## Monitoring

### Key metrics to watch

- **Tablet count per table**: Available via the `/summary` endpoint. A large number of tablets
  (thousands) may indicate that `targetFilesPerTablet` or `targetBytesPerTablet` is too low, or
  that the Iceberg table has many partitions.
- **Refresh latency**: Time taken by `TabletRefreshTask` to resolve snapshots, list files, group
  them, and produce envelopes. High latency indicates catalog or deep-store performance issues.
- **Manifest loads**: Track how often tablet manifests are loaded from the manifest store on the
  server side. The `TabletManifestCache` provides lazy loading with thread-safe concurrent
  access.
- **Snapshot ID changes**: Monitor snapshot ID transitions in the `/summary` output or controller
  logs. Rapid snapshot changes indicate frequent upstream writes.

### Controller logs

The `LakehouseTableManager` logs key lifecycle events at INFO level:

- `Creating new catalog adapter for table: {tableName}` -- first initialization
- `Snapshot changed for table: {tableName} (old={}, new={})` -- snapshot transition detected
- `Listed {} data files for table: {tableName} at snapshot {}` -- file count per snapshot
- `Refreshed {} tablets for table: {tableName} at snapshot {}` -- refresh complete

Set log level to DEBUG for the `org.apache.pinot.controller.helix.core.lakehouse` package to
see additional detail including cache hits for unchanged snapshots.

## Troubleshooting

### Too many tablets

**Symptom**: The `/summary` endpoint reports a very high tablet count (thousands or tens of
thousands), causing broker heap pressure or slow routing.

**Cause**: The Iceberg table has many partitions or many small files, and the tablet sizing
thresholds are too conservative.

**Fix**:
- Increase `targetFilesPerTablet` (default: 128). Values of 256 or 512 are reasonable for
  tables with many small files.
- Increase `targetBytesPerTablet` (default: 64 GiB). If files are small, the file count limit
  will dominate.
- Use a coarser `timeBucket` (e.g. `DAY` instead of `HOUR`).
- Compact the upstream Iceberg table to reduce file count.

### Stale sidecars

**Symptom**: Queries return correct results but sidecar-accelerated paths are not being used.

**Cause**: Sidecars are built against a previous Iceberg snapshot and have not been rebuilt
after a snapshot change. Sidecars are optional and fail open to the base Parquet scan path.

**Fix**:
- Sidecar rebuilds are triggered by the `TabletRefreshTask` when it detects a snapshot change.
  Verify that the refresh task is running (check controller logs).
- If sidecars are persistently stale, check that the sidecar store URI is accessible and
  writable by the controller.

### Commit conflicts

**Symptom**: REALTIME table with write enabled reports commit failures in controller logs.

**Cause**: Multiple writers are appending to the same Iceberg table concurrently, and an
optimistic concurrency conflict occurred.

**Fix**:
- Iceberg uses optimistic concurrency for commits. Conflicts are retried automatically. If
  conflicts are frequent, consider partitioning the Iceberg table to reduce writer contention.
- Verify that only one Pinot cluster is writing to the Iceberg table at a time.

### Schema mismatch

**Symptom**: Queries fail with column-not-found or type-mismatch errors.

**Cause**: The Pinot schema references columns that do not exist in the Iceberg table, or
column types do not match.

**Fix**:
- Verify the Pinot schema column names match the Iceberg table column names exactly
  (case-sensitive).
- Check that Pinot data types are compatible with Iceberg/Parquet types. Common mappings:
  - Iceberg `string` -> Pinot `STRING`
  - Iceberg `long` -> Pinot `LONG`
  - Iceberg `int` -> Pinot `INT`
  - Iceberg `double` -> Pinot `DOUBLE`
  - Iceberg `float` -> Pinot `FLOAT`
  - Iceberg `boolean` -> Pinot `BOOLEAN`
  - Iceberg `timestamp` / `timestamptz` -> Pinot `TIMESTAMP`
  - Iceberg `binary` -> Pinot `BYTES`
- Update the Pinot schema and reload the table after fixing mismatches.

### Catalog connection failures

**Symptom**: The `/refresh` endpoint returns a FAILURE status, or controller logs show
connection errors to the catalog.

**Cause**: Network connectivity, authentication, or catalog service availability issues.

**Fix**:
- Verify the `catalog.uri` is reachable from the controller host.
- Check that credentials in `catalog.properties` are valid and not expired.
- For REST catalogs with OAuth2, verify the `oauth2-server-uri` is reachable and the
  `credential` value is correct.
- For Hadoop catalogs, verify that the Hadoop configuration files are on the controller
  classpath and that the warehouse path is accessible.
- Check controller logs for the specific exception message.

### Tablet refresh not running

**Symptom**: Tablet metadata is stale; the snapshot ID in the `/summary` does not match the
latest Iceberg snapshot.

**Cause**: The `TabletRefreshTask` periodic task may not be running, or the controller may not
be the leader for the table.

**Fix**:
- Verify the controller is the leader for the table's resource.
- Check controller logs for `TabletRefreshTask` execution. The default period is 60 seconds.
- Trigger a manual refresh via `POST /lakehouse/tables/{tableName}/refresh`.

## Anti-Patterns

### One file = one tablet

Do not set `targetFilesPerTablet` to 1. This defeats the purpose of the tablet abstraction and
creates a Helix partition for every Iceberg data file, causing the same control-plane
bottleneck that lakehouse-native mode is designed to avoid. Use the default (128) or higher.

### Overly fine time buckets

Setting `timeBucket` to `SECOND` or `MINUTE` on a table with continuous timestamps will
create a very large number of tablet groups. Use `HOUR` or `DAY` unless you have a specific
partition scheme that warrants finer granularity.

### Lakehouse mode with upsert or dedup

Lakehouse-native tables do not support upsert or dedup. These features rely on Pinot-native
segment internals that are incompatible with the tablet/microsegment model. If you need
mutable workloads, use the CDC write mode (future) or a standard Pinot REALTIME table.

### Pointing multiple Pinot tables at the same Iceberg table

While technically possible, this creates redundant tablet metadata and doubles catalog polling.
If you need different views of the same data, use Iceberg branches or tags with query options
instead.

### Storing secrets in table config

Do not embed plaintext secrets (access keys, passwords) in the `catalog.properties` field of
the table config. Use environment variable references (e.g. `${S3_ACCESS_KEY}`) or external
secret management systems. Table configs are stored in ZooKeeper and visible through the
controller API.
