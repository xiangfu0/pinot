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

# Push-Based INSERT INTO: Codebase Touchpoint Map

## 1. Current INSERT INTO ... FROM FILE Flow (End-to-End)

### 1.1 SQL Parsing

| Step | Module | File | Notes |
|------|--------|------|-------|
| Grammar definition | pinot-common | `src/main/codegen/config.fmpp` | Declares `SqlInsertFromFile()` in `statementParserMethods`, keywords `FILE`/`ARCHIVE` |
| Grammar implementation | pinot-common | `src/main/codegen/includes/parserImpls.ftl` | JavaCC production rules: `SqlInsertFromFile()`, `DataFileDefList()`, `DataFileDef()` |
| Calcite SqlNode | pinot-common | `src/main/java/org/apache/pinot/sql/parsers/parser/SqlInsertFromFile.java` | Extends `SqlCall`. Fields: `_dbName`, `_tableName`, `_fileList` |
| SQL type enum | pinot-common | `src/main/java/org/apache/pinot/sql/parsers/PinotSqlType.java` | `DQL`, `DCL`, `DML`, `DDL` |
| Parser top-level | pinot-common | `src/main/java/org/apache/pinot/sql/parsers/CalciteSqlParser.java` | Detects `SqlInsertFromFile` instance, sets `sqlType = PinotSqlType.DML` |
| DML parsed form | pinot-common | `src/main/java/org/apache/pinot/sql/parsers/dml/InsertIntoFile.java` | Parses `SqlInsertFromFile` into table + options map; generates `AdhocTaskConfig` with `taskType=SegmentGenerationAndPushTask` |
| DML interface | pinot-common | `src/main/java/org/apache/pinot/sql/parsers/dml/DataManipulationStatement.java` | Interface: `getExecutionType()`, `generateAdhocTaskConfig()`, `execute()`, `getResultSchema()` |
| DML dispatcher | pinot-common | `src/main/java/org/apache/pinot/sql/parsers/dml/DataManipulationStatementParser.java` | Routes `SqlInsertFromFile` -> `InsertIntoFile.parse()` |
| Parser output | pinot-common | `src/main/java/org/apache/pinot/sql/parsers/SqlNodeAndOptions.java` | Wraps `SqlNode` + options map + `PinotSqlType` |

### 1.2 Broker Routing

| Step | Module | File | Notes |
|------|--------|------|-------|
| REST endpoint | pinot-broker | `src/main/java/org/apache/pinot/broker/api/resources/PinotClientRequest.java` | Switches on `PinotSqlType.DML`, calls `_sqlQueryExecutor.executeDMLStatement()` |
| DML executor | pinot-core | `src/main/java/org/apache/pinot/core/query/executor/sql/SqlQueryExecutor.java` | Parses DML, switches on `ExecutionType.MINION`, creates `MinionClient`, calls `executeTask()` |
| Minion HTTP client | pinot-common | `src/main/java/org/apache/pinot/common/minion/MinionClient.java` | POSTs `AdhocTaskConfig` JSON to controller `/tasks/execute` endpoint |

### 1.3 Controller Task Dispatch

| Step | Module | File | Notes |
|------|--------|------|-------|
| REST endpoint | pinot-controller | `src/main/java/org/apache/pinot/controller/api/resources/PinotTaskRestletResource.java` | `POST /tasks/execute` -> `executeAdhocTask()`, calls `_pinotTaskManager.createTask()` |
| Task manager | pinot-controller | `src/main/java/org/apache/pinot/controller/helix/core/minion/PinotTaskManager.java` | `createTask()` method creates Helix tasks |
| Task resource manager | pinot-controller | `src/main/java/org/apache/pinot/controller/helix/core/minion/PinotHelixTaskResourceManager.java` | Submits tasks to Helix task driver |
| Task config (SPI) | pinot-spi | `src/main/java/org/apache/pinot/spi/config/task/AdhocTaskConfig.java` | Carries `taskType`, `tableName`, `taskName`, `taskConfigs` |

### 1.4 Minion Execution

| Step | Module | File | Notes |
|------|--------|------|-------|
| Executor factory | pinot-plugins/.../pinot-minion-builtin-tasks | `.../segmentgenerationandpush/SegmentGenerationAndPushTaskExecutorFactory.java` | Implements `PinotTaskExecutorFactory` |
| Executor | pinot-plugins/.../pinot-minion-builtin-tasks | `.../segmentgenerationandpush/SegmentGenerationAndPushTaskExecutor.java` | Extends `BaseTaskExecutor`, reads input files, generates segments, pushes to controller |
| Generator | pinot-plugins/.../pinot-minion-builtin-tasks | `.../segmentgenerationandpush/SegmentGenerationAndPushTaskGenerator.java` | Extends `BaseTaskGenerator`, used for scheduled (non-adhoc) generation |
| Base executor | pinot-plugins/.../pinot-minion-builtin-tasks | `.../BaseTaskExecutor.java` | Base class for all minion task executors |
| Executor interface | pinot-minion | `src/main/java/org/apache/pinot/minion/executor/PinotTaskExecutor.java` | SPI interface |
| Executor factory interface | pinot-minion | `src/main/java/org/apache/pinot/minion/executor/PinotTaskExecutorFactory.java` | SPI factory interface |
| Generator base | pinot-controller | `src/main/java/org/apache/pinot/controller/helix/core/minion/generator/BaseTaskGenerator.java` | Base class for task generators |
| Task constants | pinot-core | `src/main/java/org/apache/pinot/core/common/MinionConstants.java` | Task type string constants |

---

## 2. Modules and Classes Likely to Change for Push-Based INSERT

### 2.1 SQL Parsing / Validation

**New SQL syntax needed:** `INSERT INTO table_name VALUES (...)` or `INSERT INTO table_name SELECT ...`

| What to change | File | Action |
|----------------|------|--------|
| Grammar config | `pinot-common/src/main/codegen/config.fmpp` | Add new `statementParserMethods` entry (e.g. `SqlInsertIntoValues()`) and any new keywords |
| Grammar rules | `pinot-common/src/main/codegen/includes/parserImpls.ftl` | Add JavaCC production for `INSERT INTO ... VALUES (...)` syntax |
| New SqlNode | `pinot-common/src/main/java/org/apache/pinot/sql/parsers/parser/` | New class (e.g. `SqlInsertIntoValues.java`) extending `SqlCall` |
| DML parsed form | `pinot-common/src/main/java/org/apache/pinot/sql/parsers/dml/` | New class (e.g. `InsertIntoValues.java`) implementing `DataManipulationStatement` |
| DML dispatcher | `pinot-common/src/main/java/org/apache/pinot/sql/parsers/dml/DataManipulationStatementParser.java` | Add `instanceof` branch for new SqlNode type |
| CalciteSqlParser | `pinot-common/src/main/java/org/apache/pinot/sql/parsers/CalciteSqlParser.java` | Handle new SqlNode type in the parsing loop (line ~132) |
| Execution type enum | `pinot-common/src/main/java/org/apache/pinot/sql/parsers/dml/DataManipulationStatement.java` | May need new `ExecutionType` (e.g. `SERVER_DIRECT` or `PUSH`) beyond `HTTP` and `MINION` |
| Tests | `pinot-common/src/test/java/org/apache/pinot/sql/parsers/dml/InsertIntoFileTest.java` | Add tests for new syntax |
| Tests | `pinot-common/src/test/java/org/apache/pinot/sql/parsers/CalciteSqlCompilerTest.java` | Add parsing tests |

### 2.2 Controller-Side Coordination

| What to change | File | Action |
|----------------|------|--------|
| New REST API | `pinot-controller/src/main/java/org/apache/pinot/controller/api/resources/` | New endpoint(s) for push-based ingest coordination (transaction open/commit/abort) |
| Segment upload | `pinot-controller/src/main/java/org/apache/pinot/controller/api/resources/PinotSegmentUploadDownloadRestletResource.java` | May need transaction-aware upload variant |
| Lineage manager | `pinot-controller/src/main/java/org/apache/pinot/controller/helix/core/lineage/DefaultLineageManager.java` | May need to participate in transaction protocol |
| Lineage interface | `pinot-controller/src/main/java/org/apache/pinot/controller/helix/core/lineage/LineageManager.java` | Interface for lineage management |
| Resource manager | `pinot-controller/src/main/java/org/apache/pinot/controller/helix/core/PinotHelixResourceManager.java` | `startReplaceSegments()`, `endReplaceSegments()` are the segment replacement protocol entry points |
| Segment completion | `pinot-controller/src/main/java/org/apache/pinot/controller/helix/core/realtime/SegmentCompletionManager.java` | Existing completion protocol; may need analogous protocol for push-based ingest |
| LLC manager | `pinot-controller/src/main/java/org/apache/pinot/controller/helix/core/realtime/PinotLLCRealtimeSegmentManager.java` | Manages realtime segment lifecycle; model for push-based equivalent |
| Query resource | `pinot-controller/src/main/java/org/apache/pinot/controller/api/resources/PinotQueryResource.java` | Handles DML queries forwarded to controller |
| Task REST resource | `pinot-controller/src/main/java/org/apache/pinot/controller/api/resources/PinotTaskRestletResource.java` | Existing adhoc task endpoint; may need new endpoints |

### 2.3 SPI Interfaces

| What to change | File | Action |
|----------------|------|--------|
| AdhocTaskConfig | `pinot-spi/src/main/java/org/apache/pinot/spi/config/task/AdhocTaskConfig.java` | May need extension for push-based mode |
| StreamConsumerFactory | `pinot-spi/src/main/java/org/apache/pinot/spi/stream/StreamConsumerFactory.java` | Reference model for new insert SPI; push-based ingest may use similar factory pattern |
| StreamConfig | `pinot-spi/src/main/java/org/apache/pinot/spi/stream/StreamConfig.java` | Reference for how stream configs are structured |
| IngestionConfig | `pinot-spi/src/main/java/org/apache/pinot/spi/config/table/ingestion/IngestionConfig.java` | May need new ingest config for push-based mode |
| StreamIngestionConfig | `pinot-spi/src/main/java/org/apache/pinot/spi/config/table/ingestion/StreamIngestionConfig.java` | Reference for stream ingestion config |
| GenericRow | `pinot-spi/src/main/java/org/apache/pinot/spi/data/readers/GenericRow.java` | Row representation used in all ingest paths |
| UpsertConfig | `pinot-spi/src/main/java/org/apache/pinot/spi/config/table/UpsertConfig.java` | Upsert table config; push-based insert must respect upsert semantics |
| DedupConfig | `pinot-spi/src/main/java/org/apache/pinot/spi/config/table/DedupConfig.java` | Dedup table config; push-based insert must respect dedup |
| New SPI interface | `pinot-spi/src/main/java/org/apache/pinot/spi/` | New interface for push-based ingest backend (e.g. `InsertBackend`, `InsertTransaction`) |

### 2.4 Row Ingest Path (Server-Side)

| What to change | File | Action |
|----------------|------|--------|
| Realtime table data manager | `pinot-core/src/main/java/org/apache/pinot/core/data/manager/realtime/RealtimeTableDataManager.java` | Primary class for managing realtime segments on a server; model for direct row injection |
| Realtime segment data manager | `pinot-core/src/main/java/org/apache/pinot/core/data/manager/realtime/RealtimeSegmentDataManager.java` | Manages individual consuming segment lifecycle; model for push-based segment |
| Mutable segment | `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/indexsegment/mutable/MutableSegmentImpl.java` | In-memory segment that rows are written to; will need to accept pushed rows |
| Segment build time lease | `pinot-core/src/main/java/org/apache/pinot/core/data/manager/realtime/SegmentBuildTimeLeaseExtender.java` | Keeps segment lease alive during build; may need equivalent for push-based |
| Table data manager provider | `pinot-core/src/main/java/org/apache/pinot/core/data/manager/provider/TableDataManagerProvider.java` | Factory for table data managers |
| Upsert metadata manager | `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/upsert/PartitionUpsertMetadataManager.java` | Interface for partition-level upsert tracking |
| Table upsert metadata manager | `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/upsert/TableUpsertMetadataManager.java` | Interface for table-level upsert tracking |
| Upsert factory | `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/upsert/TableUpsertMetadataManagerFactory.java` | Creates upsert metadata managers |
| Server gRPC/REST endpoint | `pinot-server/` | New endpoint(s) for receiving pushed rows |

### 2.5 File Ingest Path

| What to change | File | Action |
|----------------|------|--------|
| SegmentGenerationAndPushTaskExecutor | `pinot-plugins/.../segmentgenerationandpush/SegmentGenerationAndPushTaskExecutor.java` | Existing file->segment pipeline; may need transaction-aware variant |
| Segment upload resource | `pinot-controller/.../PinotSegmentUploadDownloadRestletResource.java` | Receives uploaded segments; may need transaction-aware upload |
| PinotHelixResourceManager | `pinot-controller/.../PinotHelixResourceManager.java` | `startReplaceSegments()` / `endReplaceSegments()` for atomic replacement |

### 2.6 Recovery / Restart Logic

| What to change | File | Action |
|----------------|------|--------|
| Server startup | `pinot-server/src/main/java/org/apache/pinot/server/starter/helix/BaseServerStarter.java` | Server startup reconciliation; must recover in-flight push-based inserts |
| Server instance | `pinot-server/src/main/java/org/apache/pinot/server/starter/ServerInstance.java` | Server initialization |
| Helix server starter | `pinot-server/src/main/java/org/apache/pinot/server/starter/helix/HelixServerStarter.java` | Helix-based server startup |
| Segment completion manager | `pinot-controller/.../SegmentCompletionManager.java` | Protocol for segment completion; need analogous for push-based |
| LLC realtime segment manager | `pinot-controller/.../PinotLLCRealtimeSegmentManager.java` | Realtime segment lifecycle on controller side |
| Missing consuming segment finder | `pinot-controller/.../MissingConsumingSegmentFinder.java` | Detects missing consuming segments; may need equivalent for push-based |
| Retention manager | `pinot-controller/.../retention/RetentionManager.java` | Cleans up old segments; must handle push-based segment lineage |
| Segment lineage cleanup | `pinot-controller/src/test/java/org/apache/pinot/controller/helix/core/retention/SegmentLineageCleanupTest.java` | Test for lineage cleanup |

---

## 3. Key Existing Code Reference

### 3.1 Minion Segment Generation / Push

| Class | Path | Purpose |
|-------|------|---------|
| `SegmentGenerationAndPushTaskGenerator` | `pinot-plugins/.../segmentgenerationandpush/SegmentGenerationAndPushTaskGenerator.java` | Scheduled task generator, extends `BaseTaskGenerator` |
| `SegmentGenerationAndPushTaskExecutor` | `pinot-plugins/.../segmentgenerationandpush/SegmentGenerationAndPushTaskExecutor.java` | Reads files, generates segments, uploads to controller. Extends `BaseTaskExecutor` |
| `SegmentGenerationAndPushTaskExecutorFactory` | `pinot-plugins/.../segmentgenerationandpush/SegmentGenerationAndPushTaskExecutorFactory.java` | Factory, implements `PinotTaskExecutorFactory` |
| `SegmentGenerationAndPushResult` | `pinot-plugins/.../segmentgenerationandpush/SegmentGenerationAndPushResult.java` | Result wrapper |
| `BaseTaskExecutor` | `pinot-plugins/.../BaseTaskExecutor.java` | Common task executor logic |
| `PinotTaskExecutor` | `pinot-minion/src/main/java/org/apache/pinot/minion/executor/PinotTaskExecutor.java` | SPI interface for task execution |
| `PinotTaskExecutorFactory` | `pinot-minion/src/main/java/org/apache/pinot/minion/executor/PinotTaskExecutorFactory.java` | SPI factory interface |
| `BaseTaskGenerator` | `pinot-controller/.../minion/generator/BaseTaskGenerator.java` | Base class for controller-side task generators |
| `MinionConstants` | `pinot-core/src/main/java/org/apache/pinot/core/common/MinionConstants.java` | Task type string constants |

### 3.2 Stream Ingestion Plugins

| Plugin | Factory Class | Path |
|--------|--------------|------|
| Kafka 3.0 | `KafkaConsumerFactory` | `pinot-plugins/pinot-stream-ingestion/pinot-kafka-3.0/src/main/java/.../KafkaConsumerFactory.java` |
| Kafka 4.0 | `KafkaConsumerFactory` | `pinot-plugins/pinot-stream-ingestion/pinot-kafka-4.0/src/main/java/.../KafkaConsumerFactory.java` |
| Kinesis | `KinesisConsumerFactory` | `pinot-plugins/pinot-stream-ingestion/pinot-kinesis/src/main/java/.../KinesisConsumerFactory.java` |
| Pulsar | `PulsarConsumerFactory` | `pinot-plugins/pinot-stream-ingestion/pinot-pulsar/src/main/java/.../PulsarConsumerFactory.java` |

SPI interfaces for stream ingestion:
- `pinot-spi/src/main/java/org/apache/pinot/spi/stream/StreamConsumerFactory.java`
- `pinot-spi/src/main/java/org/apache/pinot/spi/stream/PartitionGroupConsumer.java`
- `pinot-spi/src/main/java/org/apache/pinot/spi/stream/PartitionLevelConsumer.java`
- `pinot-spi/src/main/java/org/apache/pinot/spi/stream/StreamMessageDecoder.java`
- `pinot-spi/src/main/java/org/apache/pinot/spi/stream/StreamMetadataProvider.java`
- `pinot-spi/src/main/java/org/apache/pinot/spi/stream/StreamConsumerFactoryProvider.java`
- `pinot-spi/src/main/java/org/apache/pinot/spi/stream/StreamDataDecoderImpl.java`

### 3.3 Upsert / Dedup Routing and Validation

| Class | Path | Purpose |
|-------|------|---------|
| `UpsertConfig` | `pinot-spi/.../config/table/UpsertConfig.java` | Table-level upsert configuration |
| `DedupConfig` | `pinot-spi/.../config/table/DedupConfig.java` | Table-level dedup configuration |
| `PartitionUpsertMetadataManager` | `pinot-segment-local/.../upsert/PartitionUpsertMetadataManager.java` | Interface for partition-level upsert metadata |
| `TableUpsertMetadataManager` | `pinot-segment-local/.../upsert/TableUpsertMetadataManager.java` | Interface for table-level upsert metadata |
| `TableUpsertMetadataManagerFactory` | `pinot-segment-local/.../upsert/TableUpsertMetadataManagerFactory.java` | Factory for upsert managers |

### 3.4 Segment Lineage / Replacement Protocol

| Class | Path | Purpose |
|-------|------|---------|
| `SegmentLineage` | `pinot-common/.../lineage/SegmentLineage.java` | Data model for segment lineage entries |
| `SegmentLineageAccessHelper` | `pinot-common/.../lineage/SegmentLineageAccessHelper.java` | ZK read/write for lineage |
| `SegmentLineageUtils` | `pinot-common/.../lineage/SegmentLineageUtils.java` | Utility methods |
| `LineageManager` | `pinot-controller/.../lineage/LineageManager.java` | Interface |
| `DefaultLineageManager` | `pinot-controller/.../lineage/DefaultLineageManager.java` | Default implementation |
| `PinotHelixResourceManager` | `pinot-controller/.../PinotHelixResourceManager.java` | `startReplaceSegments()` / `endReplaceSegments()` methods |
| `SegmentLineageBasedSegmentPreSelector` | `pinot-broker/.../segmentpreselector/SegmentLineageBasedSegmentPreSelector.java` | Broker-side segment filtering based on lineage |

### 3.5 Server Local Data Directory

| Class | Path | Purpose |
|-------|------|---------|
| `BaseServerStarter` | `pinot-server/.../starter/helix/BaseServerStarter.java` | Configures server data dir |
| `ServerInstance` | `pinot-server/.../starter/ServerInstance.java` | Server initialization |
| `HelixServerStarter` | `pinot-server/.../starter/helix/HelixServerStarter.java` | Helix-integrated server startup |
| `PredownloadSegmentInfo` | `pinot-server/.../predownload/PredownloadSegmentInfo.java` | Segment predownload data dir usage |
| `PredownloadTableInfo` | `pinot-server/.../predownload/PredownloadTableInfo.java` | Table-level predownload info |

---

## 4. Merge-Risk Hotspots

These files are heavily trafficked and likely to cause merge conflicts if multiple agents edit them concurrently:

| File | Risk Level | Reason |
|------|-----------|--------|
| `pinot-common/src/main/codegen/config.fmpp` | **HIGH** | Single file for all parser config; keyword and method additions collide easily |
| `pinot-common/src/main/codegen/includes/parserImpls.ftl` | **HIGH** | Single file for all custom grammar rules |
| `pinot-common/src/main/java/.../CalciteSqlParser.java` | **HIGH** | Central parser dispatch; `instanceof` chains grow here |
| `pinot-common/src/main/java/.../dml/DataManipulationStatementParser.java` | **MEDIUM** | DML routing; new `instanceof` branches |
| `pinot-common/src/main/java/.../dml/DataManipulationStatement.java` | **MEDIUM** | Enum additions to `ExecutionType` |
| `pinot-broker/src/main/java/.../PinotClientRequest.java` | **MEDIUM** | DML dispatch in broker; `switch` statement |
| `pinot-core/src/main/java/.../SqlQueryExecutor.java` | **MEDIUM** | DML execution; `switch` on `ExecutionType` |
| `pinot-controller/.../PinotHelixResourceManager.java` | **HIGH** | Large file (~5000+ lines); segment replacement protocol lives here |
| `pinot-controller/.../PinotTaskRestletResource.java` | **MEDIUM** | Task REST endpoints |
| `pinot-core/.../MinionConstants.java` | **LOW** | Constants file, but many PRs touch it |
| `pinot-spi/.../config/table/ingestion/IngestionConfig.java` | **MEDIUM** | If push-based config is added here |

---

## 5. Integration Tests

| Test | Path | Relevance |
|------|------|-----------|
| `SegmentGenerationMinionClusterIntegrationTest` | `pinot-integration-tests/.../SegmentGenerationMinionClusterIntegrationTest.java` | Tests INSERT INTO FROM FILE end-to-end via minion |
| `SegmentGenerationMinionRealtimeIngestionTest` | `pinot-integration-tests/.../SegmentGenerationMinionRealtimeIngestionTest.java` | Tests realtime segment generation via minion |
| `SegmentUploadIntegrationTest` | `pinot-integration-tests/.../SegmentUploadIntegrationTest.java` | Tests segment upload/lineage |
| `MergeRollupMinionClusterIntegrationTest` | `pinot-integration-tests/.../MergeRollupMinionClusterIntegrationTest.java` | Tests segment replacement via lineage |
| `PurgeMinionClusterIntegrationTest` | `pinot-integration-tests/.../PurgeMinionClusterIntegrationTest.java` | Tests minion purge task |
| `InsertIntoFileTest` | `pinot-common/src/test/java/.../dml/InsertIntoFileTest.java` | Unit test for INSERT INTO FROM FILE parsing |
| `CalciteSqlCompilerTest` | `pinot-common/src/test/java/.../CalciteSqlCompilerTest.java` | Comprehensive SQL parsing tests |

---

## 6. Extension Point Summary

For push-based INSERT INTO, the recommended extension points are:

1. **Grammar**: Add new production in `parserImpls.ftl`, register in `config.fmpp`
2. **SqlNode**: New `SqlInsertIntoValues` class in `o.a.p.sql.parsers.parser`
3. **DML Statement**: New `InsertIntoValues` class implementing `DataManipulationStatement`
4. **Execution Type**: Add new type (e.g. `PUSH`) to `DataManipulationStatement.ExecutionType`
5. **Broker Dispatch**: Extend `PinotClientRequest` and `SqlQueryExecutor` to handle new execution type
6. **SPI**: New interface in `pinot-spi` for push-based ingest backend
7. **Controller Coordinator**: New REST endpoints for transaction management (open/commit/abort)
8. **Server Ingest**: New endpoint on server for receiving rows/batches directly
9. **Recovery**: Server startup must reconcile in-flight push transactions
10. **Lineage**: Extend segment lineage for push-based segment tracking
