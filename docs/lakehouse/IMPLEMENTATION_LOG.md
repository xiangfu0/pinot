# Lakehouse-Native Pinot Implementation Log

## Phase 1 - Lakehouse-native query-in-place MVP

### Step 1: Shared Contracts and Config DTOs (Agent A)

**Status**: Complete
**Date**: 2026-03-25

#### New Files Created

**Config DTOs (pinot-spi)**
- `pinot-spi/.../config/table/LakehouseConfig.java` - Top-level lakehouse config with Mode, VisibilityMode, WriteMode, RefType enums
- `pinot-spi/.../config/table/IcebergCatalogConfig.java` - Iceberg catalog connection config (REST, Hadoop, Hive, Glue, Nessie, JDBC)
- `pinot-spi/.../config/table/LakehouseReadConfig.java` - Read path config (visibility mode, default ref)
- `pinot-spi/.../config/table/LakehouseWriteConfig.java` - Write path config (enabled, mode, file format)
- `pinot-spi/.../config/table/TabletConfig.java` - Tablet sizing/grouping config (files per tablet, bytes, time bucket)
- `pinot-spi/.../config/table/SidecarConfig.java` - Sidecar index config (store URI, index types)

**Metadata DTOs (pinot-common)**
- `pinot-common/.../metadata/lakehouse/TabletMetadataEnvelope.java` - Small ZK-safe envelope for broker routing
- `pinot-common/.../metadata/lakehouse/TabletManifest.java` - Full manifest stored outside ZK
- `pinot-common/.../metadata/lakehouse/MicrosegmentDescriptor.java` - Per-file descriptor with column stats
- `pinot-common/.../metadata/lakehouse/SidecarReference.java` - Sidecar artifact reference

**Tests**
- `pinot-spi/.../config/table/LakehouseConfigTest.java` - 11 tests covering config DTOs, serialization, defaults, TableConfig integration
- `pinot-common/.../metadata/lakehouse/TabletMetadataEnvelopeTest.java` - 4 tests covering creation, JSON round-trip, versioning, forward compat
- `pinot-common/.../metadata/lakehouse/TabletManifestTest.java` - 8 tests covering manifests, delete files, sidecars, microsegment types

#### Modified Files

- `pinot-spi/.../config/table/TableConfig.java` - Added `lakehouseConfig` field, getter/setter, `isLakehouseEnabled()` convenience
- `pinot-spi/.../utils/builder/TableConfigBuilder.java` - Added `setLakehouseConfig()` builder method
- `pinot-spi/.../utils/CommonConstants.java` - Added `ICEBERG_SNAPSHOT_ID`, `ICEBERG_BRANCH`, `ICEBERG_TAG`, `ICEBERG_AS_OF_MS` query option keys
- `pinot-common/.../utils/config/TableConfigSerDeUtils.java` - Updated constructor call for new parameter
- `pinot-segment-local/.../utils/TableConfigUtils.java` - Added `validateLakehouseConfig()` validation method
- `pinot-segment-local/.../utils/TableConfigUtilsTest.java` - Added 7 lakehouse validation tests
- `pinot-segment-local/.../segment/index/creator/CLPForwardIndexCreatorTest.java` - Updated constructor call

#### Key Design Decisions

1. **Envelope/Manifest split**: TabletMetadataEnvelope (small, ZK-safe) vs TabletManifest (large, deep store) to keep ZK metadata bounded by tablet count, not file count.
2. **Forward compatibility**: All metadata DTOs use `@JsonIgnoreProperties(ignoreUnknown = true)` and version fields.
3. **No Iceberg deps in foundational modules**: All metadata DTOs use plain types (String, long, Map) rather than Iceberg API types.
4. **Phase 1 constraints enforced**: Lakehouse tables cannot use upsert or dedup. Write config only for REALTIME tables.
5. **Composite DocRef model**: MicrosegmentDescriptor uses (microsegmentId, rowPosition) addressing, aligning with Iceberg position deletes.

### Step 2: SPI Interfaces (Agent A continued)

**Status**: Complete
**Date**: 2026-03-25

**New SPI Interfaces (pinot-spi)**
- `pinot-spi/.../lakehouse/LakehouseCatalogAdapter.java` - SPI for Iceberg catalog operations (snapshot resolution, file listing, schema)
- `pinot-spi/.../lakehouse/LakehouseFileDescriptor.java` - Plain-type file descriptor (no Iceberg deps)
- `pinot-spi/.../lakehouse/LakehouseFieldDescriptor.java` - Plain-type schema field descriptor
- `pinot-spi/.../lakehouse/TabletManifestStore.java` - SPI for manifest persistence outside ZK

### Step 3: Controller / Catalog / Assignment (Agent B)

**Status**: Complete
**Date**: 2026-03-25

#### New Files Created

**Iceberg Plugin Module**
- `pinot-plugins/pinot-catalog/pom.xml` - Catalog plugin family parent
- `pinot-plugins/pinot-catalog/pinot-iceberg/pom.xml` - Iceberg adapter with iceberg-core 1.7.1 deps
- `pinot-plugins/pinot-catalog/pinot-iceberg/.../IcebergCatalogAdapterImpl.java` - Full adapter: REST/Hadoop/Hive/Glue/Nessie/JDBC support, snapshot resolution, file listing with column bounds/null counts

**Controller Services**
- `pinot-controller/.../lakehouse/TabletGrouper.java` - Groups files into tablets by (specId, partition, timeBucket, generation), respects file/byte limits
- `pinot-controller/.../lakehouse/LakehouseTableManager.java` - Orchestrates tablet lifecycle: catalog adapter init, snapshot resolution, file grouping, manifest persistence, envelope creation
- `pinot-controller/.../lakehouse/TabletRefreshTask.java` - Periodic task (60s default) for snapshot polling, extends ControllerPeriodicTask

**Manifest Store**
- `pinot-common/.../lakehouse/FileSystemTabletManifestStore.java` - PinotFS-backed manifest persistence

**REST Endpoints**
- `pinot-controller/.../resources/PinotLakehouseRestletResource.java` - Admin APIs: GET tablets, POST refresh, GET tablet by ID, GET summary

**Tests**
- `pinot-controller/.../lakehouse/TabletGrouperTest.java` - 12 tests for grouping by file count, bytes, partitions, spec IDs, time buckets

#### Key Design Decisions

1. **Plugin isolation**: Iceberg dependencies (iceberg-core 1.7.1) only in pinot-plugins/pinot-catalog/pinot-iceberg. Controller uses SPI interfaces.
2. **Snapshot change detection**: LakehouseTableManager caches last snapshot ID per table, skips re-processing when unchanged.
3. **Deterministic tablet grouping**: Files grouped by (specId, partitionHash, timeBucket), split into generations at configurable limits.
4. **Periodic refresh**: TabletRefreshTask polls every 60s (configurable), leader-only per table.

### Step 4: Broker Routing / Snapshot Pinning (Agent C)

**Status**: Complete
**Date**: 2026-03-25

**New Files**
- `pinot-common/.../lakehouse/LakehouseQueryOptions.java` - Immutable DTO for query-level Iceberg ref resolution
- `pinot-broker/.../segmentpruner/TabletPruner.java` - Time-range pruning of tablet envelopes

**Modified Files**
- `pinot-common/.../utils/config/QueryOptionsUtils.java` - Added 4 Iceberg query option parsers

**Tests**
- `LakehouseQueryOptionsTest.java` - 9 tests for option parsing
- `TabletPrunerTest.java` - 8 tests for time-range pruning

### Step 5: Server Tablet Execution Abstractions (Agent D)

**Status**: Complete
**Date**: 2026-03-25

**New Files**
- `pinot-segment-local/.../lakehouse/TabletSegmentMetadata.java` - SegmentMetadata adapter for tablets (wraps TabletManifest + Envelope)
- `pinot-segment-local/.../data/manager/TabletDataManager.java` - Server-side tablet lifecycle (extends SegmentDataManager)
- `pinot-segment-local/.../lakehouse/TabletManifestCache.java` - Thread-safe manifest cache with lazy loading

**Tests**
- `TabletSegmentMetadataTest.java` - 22 tests for metadata interface compliance
- `TabletManifestCacheTest.java` - 7 tests (including concurrent access with 10 threads)

### Phase 1 Test Summary

| Module | Test Class | Tests |
|--------|-----------|-------|
| pinot-spi | LakehouseConfigTest | 11 |
| pinot-common | TabletMetadataEnvelopeTest | 4 |
| pinot-common | TabletManifestTest | 8 |
| pinot-common | LakehouseQueryOptionsTest | 9 |
| pinot-segment-local | TableConfigUtilsTest (lakehouse) | 7 |
| pinot-segment-local | TabletSegmentMetadataTest | 22 |
| pinot-segment-local | TabletManifestCacheTest | 7 |
| pinot-broker | TabletPrunerTest | 8 |
| pinot-controller | TabletGrouperTest | 12 |
| pinot-controller | LakehouseTableManagerTest | 8 |
| **Total** | | **96** |

### Step 6: Controller Wiring (Agent B continued)

**Status**: Complete
**Date**: 2026-03-25

**Changes to BaseControllerStarter**:
- Added `_lakehouseTableManager` field and `createLakehouseTableManager()` factory method
- Wired `TabletRefreshTask` into periodic task list
- Wired `LakehouseTableManager` into HK2 DI binder for REST resource injection
- Default catalog adapter factory uses reflective class loading (SPI pattern) to avoid hard dependency on Iceberg plugin

**Integration Test**:
- `LakehouseTableManagerTest` - 8 tests exercising full pipeline with mock catalog adapter and in-memory manifest store:
  - Refresh creates tablets with correct snapshot ID
  - Refresh skips when snapshot unchanged (change detection)
  - Refresh detects snapshot changes
  - Tablet grouping respects file count limits
  - Get/remove table lifecycle
  - Empty data files handled correctly
  - Envelope metadata accuracy (row count, size, microsegment count)

### Step 7: Helix IdealState Integration (Agent B continued)

**Status**: Complete
**Date**: 2026-03-25

- `TabletAssignmentManager.java` - Manages Helix IdealState for lakehouse tablets: adds new tablets as ONLINE partitions with round-robin server assignment, removes stale tablets, preserves existing assignments, caps replication at server count
- Updated `TabletRefreshTask.processTable()` to call `TabletAssignmentManager.updateTabletIdealState()` after refresh
- `TabletAssignmentManagerTest.java` - 14 tests for IdealState management

### Step 8: Integration & Scale Tests (Agent G)

**Status**: Complete
**Date**: 2026-03-25

- `LakehouseIntegrationTest.java` - 11 end-to-end tests: small/medium/large tables, partitioned data, time-bucketed data, snapshot change detection, manifest round-trip, envelope serialization, tablet pruning
- `LakehouseScaleTest.java` - 6 scale tests: 100k files -> 782 bounded tablets, envelope size < 2KB, total ZK metadata < 1MB, grouping < 5s, full pipeline < 30s

### Step 9: Documentation & Examples (Agent H)

**Status**: Complete
**Date**: 2026-03-25

- `docs/lakehouse/examples/iceberg-rest-offline-table.json` - REST catalog OFFLINE table example
- `docs/lakehouse/examples/iceberg-hadoop-offline-table.json` - Hadoop catalog OFFLINE table example
- `docs/lakehouse/examples/iceberg-realtime-table.json` - REALTIME table with write enabled
- `docs/lakehouse/examples/events-schema.json` - Pinot schema for events table
- `docs/lakehouse/operator-guide.md` - Comprehensive operator guide (config reference, APIs, troubleshooting)
- `docs/lakehouse/release-notes-draft.md` - Phase 1 release note

### Final Phase 1 Test Summary

| Module | Test Class | Tests |
|--------|-----------|-------|
| pinot-spi | LakehouseConfigTest | 11 |
| pinot-common | TabletMetadataEnvelopeTest | 4 |
| pinot-common | TabletManifestTest | 8 |
| pinot-common | LakehouseQueryOptionsTest | 9 |
| pinot-segment-local | TableConfigUtilsTest (lakehouse) | 7 |
| pinot-segment-local | TabletSegmentMetadataTest | 22 |
| pinot-segment-local | TabletManifestCacheTest | 7 (incl. 10-thread concurrency) |
| pinot-broker | TabletPrunerTest | 8 |
| pinot-controller | TabletGrouperTest | 12 |
| pinot-controller | LakehouseTableManagerTest | 8 |
| pinot-controller | TabletAssignmentManagerTest | 14 |
| pinot-controller | LakehouseIntegrationTest | 11 |
| pinot-controller | LakehouseScaleTest | 6 |
| **Total** | | **130** |

### Phase 1 Complete File Inventory

| Category | Files |
|----------|-------|
| Config DTOs (pinot-spi) | 6 |
| SPI interfaces (pinot-spi) | 4 |
| Metadata DTOs (pinot-common) | 4 |
| Common services (pinot-common) | 2 |
| Controller services | 4 |
| Controller REST endpoints | 1 |
| Broker services | 1 |
| Server abstractions | 3 |
| Plugin module (pinot-iceberg) | 3 (pom + impl) |
| Tests | 13 |
| Documentation | 6 |
| Modified existing files | ~12 |
| **Total new files** | **~47** |

### Risk / Follow-up List

1. **Iceberg plugin not tested with real catalog** - Phase 1 uses mock adapters; real Iceberg integration test deferred to Phase 2
2. **TabletIndexSegment not implemented** - TabletDataManager.getSegment() returns null; actual Parquet query execution deferred
3. **No MSQ support yet** - Extension points preserved but not wired; tracked for Phase 2+
4. **No sidecar support yet** - SidecarConfig/SidecarReference defined but not wired; tracked for Phase 4
5. **No write path yet** - LakehouseWriteConfig defined but not implemented; tracked for Phase 3
6. **Controller restart recovery** - Tablet envelopes are in-memory; ZK persistence of envelope pointers deferred
