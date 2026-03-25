# Release Notes: Lakehouse-Native Pinot (Phase 1)

## Feature: Lakehouse-Native Query-in-Place for Iceberg Tables

Apache Pinot now supports querying Apache Iceberg tables directly without importing data into
Pinot-native segments. This feature, called **lakehouse-native mode**, lets Pinot read Parquet
data files in place from your data lake while maintaining low-latency query performance through
a new tablet abstraction.

### What is supported (Phase 1)

- **Query-in-place**: Read Iceberg tables backed by Parquet files from S3, HDFS, GCS, ADLS,
  or any PinotFS-compatible storage.
- **Tablet abstraction**: Iceberg data files are automatically grouped into tablets
  (supersegments) for efficient Helix partition assignment and broker routing. Tablet sizing is
  configurable by file count, byte size, and time bucket granularity.
- **Iceberg catalog support**: REST, Hadoop, Hive, Glue, Nessie, and JDBC catalog types.
- **Snapshot pinning**: Queries are pinned to a consistent Iceberg snapshot. Per-query snapshot
  override is available via `icebergSnapshotId`, `icebergBranch`, `icebergTag`, and
  `icebergAsOfMs` query options.
- **Periodic snapshot refresh**: The controller automatically polls the Iceberg catalog for new
  snapshots (default: every 60 seconds) and re-groups tablets as needed.
- **Admin APIs**: New REST endpoints under `/lakehouse/tables/{tableName}/` for listing tablets,
  triggering manual refreshes, inspecting individual tablets, and viewing table summaries.
- **Tablet pruning**: Time-range pruning of tablets at the broker for efficient query routing.
- **OFFLINE and REALTIME table types**: Lakehouse mode is supported for both OFFLINE (read-only
  from Iceberg) and REALTIME (with optional direct write) tables.

### What is not yet supported

- **Direct write (production-ready)**: The write path (`write.enabled=true`) is available for
  REALTIME tables in append mode but should be considered experimental in Phase 1.
- **Sidecar indexes**: The `sidecars` configuration is accepted but sidecar index build and
  acceleration are not yet implemented. Queries always use the base Parquet scan path.
- **CDC write mode**: The `CDC` write mode enum is defined but not yet implemented. Use
  `APPEND` mode for Phase 1.
- **Helix IdealState integration**: Full tablet-as-partition assignment in Helix IdealState is
  in progress. Phase 1 provides the metadata and grouping infrastructure.

### How to enable

1. Deploy the `pinot-iceberg` catalog plugin JAR to the controller and server classpaths.
2. Create a Pinot schema matching the Iceberg table columns you want to query.
3. Create a table config with `lakehouseConfig.enabled: true` and
   `lakehouseConfig.mode: "ICEBERG_NATIVE"`. See the
   [operator guide](docs/lakehouse/operator-guide.md) and
   [example configs](docs/lakehouse/examples/) for complete configuration reference.

### Breaking changes

None. Lakehouse-native mode is fully opt-in. Existing Pinot-native OFFLINE, REALTIME, and
HYBRID tables are completely unaffected. The `lakehouseConfig` field is ignored if not present
or if `enabled` is `false`.

### Validation constraints (Phase 1)

- Lakehouse tables cannot use upsert or dedup.
- Only `ICEBERG_NATIVE` mode is supported.
- `write.enabled` requires `tableType: "REALTIME"`.
- `targetFilesPerTablet` and `targetBytesPerTablet` must be positive if specified.
