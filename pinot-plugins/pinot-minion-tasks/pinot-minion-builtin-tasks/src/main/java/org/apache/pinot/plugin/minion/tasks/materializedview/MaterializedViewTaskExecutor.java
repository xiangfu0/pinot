/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.minion.tasks.materializedview;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.MaterializedViewTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.materializedview.executor.MaterializedViewQueryExecutor;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadataUtils;
import org.apache.pinot.materializedview.metadata.PartitionFingerprint;
import org.apache.pinot.materializedview.metadata.PartitionInfo;
import org.apache.pinot.materializedview.metadata.PartitionState;
import org.apache.pinot.materializedview.scheduler.MaterializedViewTaskUtils;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.minion.event.MinionEventObserver;
import org.apache.pinot.minion.event.MinionEventObservers;
import org.apache.pinot.minion.executor.MinionTaskZkMetadataManager;
import org.apache.pinot.plugin.minion.tasks.BaseTaskExecutor;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.Obfuscator;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Executor for [MaterializedViewTask].
///
/// This task receives a SQL query with an appended time range (from the generator),
/// executes it via a pluggable [MaterializedViewQueryExecutor] (e.g. gRPC, Arrow Flight),
/// and builds a segment from the query results for the MV table.
///
/// Lifecycle:
///
///   - `preProcess` – validates watermark against windowStartMs
///   - `executeTask` – queries broker, builds segment, uploads
///   - `postProcess` – advances watermark to windowEndMs
///
public class MaterializedViewTaskExecutor extends BaseTaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewTaskExecutor.class);

  private final MinionTaskZkMetadataManager _minionTaskZkMetadataManager;
  private final MinionConf _minionConf;
  private final MaterializedViewQueryExecutor _queryExecutor;

  public MaterializedViewTaskExecutor(MinionTaskZkMetadataManager minionTaskZkMetadataManager,
      MinionConf minionConf, MaterializedViewQueryExecutor queryExecutor) {
    _minionTaskZkMetadataManager = minionTaskZkMetadataManager;
    _minionConf = minionConf;
    _queryExecutor = queryExecutor;
  }

  public void preProcess(PinotTaskConfig pinotTaskConfig) {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    String taskMode = configs.getOrDefault(MaterializedViewTask.TASK_MODE_KEY,
        MaterializedViewTask.TASK_MODE_APPEND);
    long windowStartMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_START_MS_KEY));

    // Fetch MaterializedViewRuntimeMetadata for watermark validation and optimistic locking
    HelixPropertyStore<ZNRecord> propertyStore = MINION_CONTEXT.getHelixPropertyStore();
    Stat stat = new Stat();
    MaterializedViewRuntimeMetadata runtime = MaterializedViewRuntimeMetadataUtils.fetchWithVersion(
        propertyStore, tableName, stat);

    if (runtime != null) {
      if (MaterializedViewTask.TASK_MODE_APPEND.equals(taskMode)) {
        Preconditions.checkState(runtime.getWatermarkMs() <= windowStartMs,
            "watermarkMs %d should not be larger than windowStartMs %d for table %s",
            runtime.getWatermarkMs(), windowStartMs, tableName);
      } else if (MaterializedViewTask.TASK_MODE_OVERWRITE.equals(taskMode)) {
        PartitionInfo partitionInfo = runtime.getPartitions().get(windowStartMs);
        Preconditions.checkState(partitionInfo != null && partitionInfo.getState() == PartitionState.STALE,
            "Overwrite target partition %d should exist and be STALE for table %s",
            windowStartMs, tableName);
      } else if (MaterializedViewTask.TASK_MODE_DELETE.equals(taskMode)) {
        // DELETE is now an executor-internal cleanup triggered when an OVERWRITE finds the
        // source data has been retention-deleted (empty result + zero source segments).  The
        // partition must exist and be STALE; the executor will remove it from the map and
        // drop the corresponding MV segments.
        PartitionInfo partitionInfo = runtime.getPartitions().get(windowStartMs);
        Preconditions.checkState(partitionInfo != null && partitionInfo.getState() == PartitionState.STALE,
            "Delete target partition %d should exist and be STALE for table %s",
            windowStartMs, tableName);
      }
    } else {
      LOGGER.warn("MaterializedViewRuntimeMetadata for table: {} not found; will be initialized in postProcess",
          tableName);
    }
  }

  @Override
  public SegmentConversionResult executeTask(PinotTaskConfig pinotTaskConfig)
      throws Exception {
    preProcess(pinotTaskConfig);

    MinionEventObserver eventObserver =
        MinionEventObservers.getInstance().getMinionEventObserver(pinotTaskConfig.getTaskId());

    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String taskType = pinotTaskConfig.getTaskType();
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Starting task: {} with configs: {}", taskType, Obfuscator.DEFAULT.toJsonString(configs));
    }

    String tableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    long windowStartMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_START_MS_KEY));
    long windowEndMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_END_MS_KEY));
    String taskMode = configs.getOrDefault(MaterializedViewTask.TASK_MODE_KEY,
        MaterializedViewTask.TASK_MODE_APPEND);

    // DELETE mode: skip query execution, only remove existing MV segments
    if (MaterializedViewTask.TASK_MODE_DELETE.equals(taskMode)) {
      return executeDeleteTask(pinotTaskConfig, eventObserver, tableName, windowStartMs, windowEndMs);
    }
    PartitionFingerprint taskFingerprint = getTaskFingerprint(configs, tableName, windowStartMs);
    validateSourceFingerprintAtCommit(configs, tableName, windowStartMs, windowEndMs, taskFingerprint);

    String definedSQL = configs.get(MaterializedViewTask.DEFINED_SQL_KEY);
    LOGGER.info("MaterializedViewTask for table: {}, window: [{}, {}), SQL: {}",
        tableName, windowStartMs, windowEndMs, definedSQL);

    TableConfig tableConfig = getTableConfig(tableName);
    Schema schema = getSchema(tableName);

    eventObserver.notifyProgress(pinotTaskConfig, "Executing query for MV table: " + tableName);
    AuthProvider authProvider = resolveAuthProvider(configs);
    String uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);
    Map<String, String> authHeaders = AuthProviderUtils.makeAuthHeadersMap(authProvider);
    MaterializedViewQueryExecutor.QueryResult queryResult = _queryExecutor.executeQuery(definedSQL, authHeaders);
    List<GenericRow> rows = convertToGenericRows(queryResult.getDataSchema(), queryResult.getRows(), schema);
    LOGGER.info("Query returned {} rows for table: {}", rows.size(), tableName);

    // Completeness gate: reject windows whose result set saturated the declared LIMIT.  We must
    // fail BEFORE building segments or calling postProcess, so neither the partition is marked
    // VALID nor the runtime watermark/partitions map is advanced.  The task will be retried by
    // Helix; a permanently over-sized window requires narrowing the time bucket / filters in
    // definedSQL, or adding an explicit LIMIT larger than the expected per-window row count.
    MaterializedViewTaskUtils.verifyResultNotTruncated(configs, tableName, windowStartMs, windowEndMs, rows.size());

    if (rows.isEmpty()) {
      LOGGER.info("No data returned for window [{}, {}) of table: {}.", windowStartMs, windowEndMs, tableName);
      if (MaterializedViewTask.TASK_MODE_OVERWRITE.equals(taskMode)) {
        validateSourceFingerprintAtCommit(configs, tableName, windowStartMs, windowEndMs, taskFingerprint);
        replaceWindowSegments(tableName, windowStartMs, windowEndMs, Collections.emptyList(), uploadURL, authProvider);
      }
      postProcess(pinotTaskConfig);
      return new SegmentConversionResult.Builder()
          .setTableNameWithType(tableName)
          .build();
    }

    String maxRecordsStr = configs.get(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY);
    int maxNumRecordsPerSegment = maxRecordsStr != null
        ? Integer.parseInt(maxRecordsStr)
        : MaterializedViewTask.DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT;

    int totalRows = rows.size();
    int numSegments = (totalRows + maxNumRecordsPerSegment - 1) / maxNumRecordsPerSegment;
    LOGGER.info("Splitting {} rows into {} segment(s) (maxNumRecordsPerSegment={})",
        totalRows, numSegments, maxNumRecordsPerSegment);
    // Generate a per-attempt UUID so segment names are unique across retries of the same window.
    // Helix reuses the same subtask id (PinotTaskConfig#getTaskId) on every retry, so we cannot
    // rely on taskId for uniqueness — a retry after a partial upload would reproduce identical
    // names and the controller would reject the new lineage entry.
    String attemptId = UUID.randomUUID().toString();

    File tempDir = new File(FileUtils.getTempDirectory(),
        "materialized_view_task_" + tableName + "_" + attemptId);
    FileUtils.forceMkdir(tempDir);

    try {
      // Phase 1: Build all segments and collect results
      List<SegmentConversionResult> conversionResults = new ArrayList<>();
      List<File> tarFiles = new ArrayList<>();

      for (int segIdx = 0; segIdx < numSegments; segIdx++) {
        int fromIndex = segIdx * maxNumRecordsPerSegment;
        int toIndex = Math.min(fromIndex + maxNumRecordsPerSegment, totalRows);
        List<GenericRow> chunk = rows.subList(fromIndex, toIndex);

        String segmentName = MaterializedViewTaskUtils.buildSegmentName(
            tableName, windowStartMs, windowEndMs, attemptId, segIdx);

        File segmentOutputDir = new File(tempDir, "segmentOutput_" + segIdx);
        FileUtils.forceMkdir(segmentOutputDir);

        SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
        segmentGeneratorConfig.setTableName(tableName);
        segmentGeneratorConfig.setOutDir(segmentOutputDir.getAbsolutePath());
        segmentGeneratorConfig.setSegmentName(segmentName);

        eventObserver.notifyProgress(pinotTaskConfig,
            String.format("Building segment %d/%d: %s (%d rows)", segIdx + 1, numSegments, segmentName, chunk.size()));

        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        driver.init(segmentGeneratorConfig, new GenericRowRecordReader(chunk));
        driver.build();

        File segmentDir = new File(segmentOutputDir, segmentName);
        Preconditions.checkState(segmentDir.exists(), "Segment generation failed for: %s", segmentName);

        File segmentTarFile = new File(tempDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
        TarCompressionUtils.createCompressedTarFile(segmentDir, segmentTarFile);

        conversionResults.add(new SegmentConversionResult.Builder()
            .setFile(segmentDir)
            .setSegmentName(segmentName)
            .setTableNameWithType(tableName)
            .build());
        tarFiles.add(segmentTarFile);
      }

      // Phase 2: Segment lineage — find old segments and start replace
      List<String> segmentsTo = new ArrayList<>();
      for (SegmentConversionResult r : conversionResults) {
        segmentsTo.add(r.getSegmentName());
      }

      validateSourceFingerprintAtCommit(configs, tableName, windowStartMs, windowEndMs, taskFingerprint);
      String lineageEntryId =
          startWindowSegmentReplace(tableName, windowStartMs, windowEndMs, segmentsTo, uploadURL, authProvider);

      try {
        // Phase 3: Upload all segments
        for (int i = 0; i < conversionResults.size(); i++) {
          SegmentConversionResult result = conversionResults.get(i);
          File tarFile = tarFiles.get(i);
          String segmentName = result.getSegmentName();

          eventObserver.notifyProgress(pinotTaskConfig,
              String.format("Uploading segment %d/%d: %s", i + 1, numSegments, segmentName));

          List<Header> httpHeaders = getSegmentPushMetadataHeaders(pinotTaskConfig, authProvider, result);
          List<NameValuePair> parameters = getSegmentPushCommonParams(tableName);
          SegmentConversionUtils.uploadSegment(configs, httpHeaders, parameters, tableName, segmentName,
              uploadURL, tarFile);

          reportSegmentUploadMetrics(result.getFile(), tableName, taskType);

          LOGGER.info("Successfully uploaded segment {}/{}: {} for table: {}",
              i + 1, numSegments, segmentName, tableName);
        }

        // Phase 4: End segment replace to atomically swap lineage
        if (lineageEntryId != null) {
          validateSourceFingerprintAtCommit(configs, tableName, windowStartMs, windowEndMs, taskFingerprint);
          SegmentConversionUtils.endSegmentReplace(
              tableName, uploadURL, lineageEntryId,
              _minionConf.getEndReplaceSegmentsTimeoutMs(), authProvider);
          LOGGER.info("Ended segment replace for table: {}, lineageEntryId: {}", tableName, lineageEntryId);
        }
      } catch (Exception e) {
        // Best-effort revert of the IN_PROGRESS lineage entry so the next attempt is not blocked.
        // If revert itself fails, the next startSegmentReplace will mark the previous entry as
        // REVERTED and clean up leftover segments — same recovery contract as ConsistentDataPushUtils.
        if (lineageEntryId != null) {
          revertWindowSegmentReplace(tableName, lineageEntryId, uploadURL, authProvider);
        }
        throw e;
      }

      postProcess(pinotTaskConfig);

      return conversionResults.get(conversionResults.size() - 1);
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  private void replaceWindowSegments(String tableName, long windowStartMs, long windowEndMs, List<String> segmentsTo,
      String uploadURL, AuthProvider authProvider)
      throws Exception {
    String lineageEntryId =
        startWindowSegmentReplace(tableName, windowStartMs, windowEndMs, segmentsTo, uploadURL, authProvider);
    if (lineageEntryId != null) {
      SegmentConversionUtils.endSegmentReplace(
          tableName, uploadURL, lineageEntryId, _minionConf.getEndReplaceSegmentsTimeoutMs(), authProvider);
      LOGGER.info("Ended segment replace for table: {}, lineageEntryId: {}", tableName, lineageEntryId);
    }
  }

  private String startWindowSegmentReplace(String tableName, long windowStartMs, long windowEndMs,
      List<String> segmentsTo, String uploadURL, AuthProvider authProvider)
      throws Exception {
    String segmentPrefix = tableName + "_" + windowStartMs + "_" + windowEndMs;
    Set<String> allExistingSegments = SegmentConversionUtils.getSegmentNamesForTable(
        tableName, new URI(uploadURL).resolve("/"), authProvider);
    List<String> segmentsFrom = new ArrayList<>();
    for (String name : allExistingSegments) {
      if (name.equals(segmentPrefix) || name.startsWith(segmentPrefix + "_")) {
        segmentsFrom.add(name);
      }
    }

    if (segmentsFrom.isEmpty() && segmentsTo.isEmpty()) {
      return null;
    }
    String lineageEntryId = SegmentConversionUtils.startSegmentReplace(
        tableName, uploadURL, new StartReplaceSegmentsRequest(segmentsFrom, segmentsTo), authProvider);
    LOGGER.info("Started segment replace for table: {}, lineageEntryId: {}, segmentsFrom: {}, segmentsTo: {}",
        tableName, lineageEntryId, segmentsFrom, segmentsTo);
    return lineageEntryId;
  }

  /// Best-effort revert of a started segment replace lineage entry.
  ///
  /// Used when the upload phase or `endSegmentReplace` throws after a successful
  /// `startSegmentReplace`, to avoid an orphaned IN_PROGRESS lineage entry that would
  /// block subsequent task retries. Always treats the table as OFFLINE — MV tables are
  /// always offline by construction.
  ///
  /// Failures are swallowed (logged only): if the controller is unreachable now, the
  /// next `startSegmentReplace` call will mark the previous entry as REVERTED and clean
  /// up any leftover segments.
  private void revertWindowSegmentReplace(String tableNameWithType, String lineageEntryId, String uploadURL,
      AuthProvider authProvider) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    try (FileUploadDownloadClient client = new FileUploadDownloadClient()) {
      URI revertUri = FileUploadDownloadClient.getRevertReplaceSegmentsURI(
          new URI(uploadURL), rawTableName, TableType.OFFLINE.name(), lineageEntryId, true);
      client.revertReplaceSegments(revertUri, authProvider);
      LOGGER.info("Reverted segment replace for table: {}, lineageEntryId: {}", tableNameWithType, lineageEntryId);
    } catch (Exception revertException) {
      LOGGER.error("Failed to revert segment replace for table: {}, lineageEntryId: {}. Next "
              + "startSegmentReplace will clean up the orphaned entry.",
          tableNameWithType, lineageEntryId, revertException);
    }
  }

  /// Handles DELETE mode: removes all existing MV segments for the given time window
  /// via segment lineage replace (segmentsFrom=[old segments], segmentsTo=[]).
  /// No query is executed and no new segments are created.
  private SegmentConversionResult executeDeleteTask(PinotTaskConfig pinotTaskConfig,
      MinionEventObserver eventObserver, String tableName, long windowStartMs, long windowEndMs)
      throws Exception {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);
    AuthProvider authProvider = resolveAuthProvider(configs);

    LOGGER.info("DELETE task for table: {}, window: [{}, {}). Removing MV segments.",
        tableName, windowStartMs, windowEndMs);
    eventObserver.notifyProgress(pinotTaskConfig,
        "Deleting MV segments for window [" + windowStartMs + ", " + windowEndMs + ")");

    String segmentPrefix = tableName + "_" + windowStartMs + "_" + windowEndMs;
    Set<String> allExistingSegments = SegmentConversionUtils.getSegmentNamesForTable(
        tableName, new URI(uploadURL).resolve("/"), authProvider);
    List<String> segmentsFrom = new ArrayList<>();
    for (String name : allExistingSegments) {
      if (name.equals(segmentPrefix) || name.startsWith(segmentPrefix + "_")) {
        segmentsFrom.add(name);
      }
    }

    if (!segmentsFrom.isEmpty()) {
      List<String> segmentsTo = Collections.emptyList();
      String lineageEntryId = SegmentConversionUtils.startSegmentReplace(
          tableName, uploadURL,
          new StartReplaceSegmentsRequest(segmentsFrom, segmentsTo),
          authProvider);
      LOGGER.info("Started segment delete-replace for table: {}, lineageEntryId: {}, segmentsFrom: {}",
          tableName, lineageEntryId, segmentsFrom);

      SegmentConversionUtils.endSegmentReplace(
          tableName, uploadURL, lineageEntryId,
          _minionConf.getEndReplaceSegmentsTimeoutMs(), authProvider);
      LOGGER.info("Ended segment delete-replace for table: {}, lineageEntryId: {}", tableName, lineageEntryId);
    } else {
      LOGGER.info("No existing segments found for prefix: {} in table: {}. Nothing to delete.",
          segmentPrefix, tableName);
    }

    postProcess(pinotTaskConfig);

    return new SegmentConversionResult.Builder()
        .setTableNameWithType(tableName)
        .build();
  }

  public void postProcess(PinotTaskConfig pinotTaskConfig) {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    String taskMode = configs.getOrDefault(MaterializedViewTask.TASK_MODE_KEY,
        MaterializedViewTask.TASK_MODE_APPEND);
    long windowStartMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_START_MS_KEY));
    long windowEndMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_END_MS_KEY));

    updateMaterializedViewRuntime(configs, tableName, taskMode, windowStartMs, windowEndMs);
  }

  /// Updates [MaterializedViewRuntimeMetadata] in a single CAS write, combining:
  ///
  ///   - partitions: set VALID with new fingerprint (APPEND/OVERWRITE) or remove (DELETE)
  ///   - watermarkMs: advance on APPEND only (drives both scheduler dispatch and the
  ///       broker's SPLIT_REWRITE boundary)
  ///
  // Bound on CAS retry attempts when racing to update MaterializedViewRuntimeMetadata. Up to
  // maxTasksPerBatch executors can contend per batch completion; each retry re-fetches the
  // latest version with jittered backoff (Thread.sleep below). 128 is well above any
  // realistic maxTasksPerBatch and stays low enough that genuinely pathological contention
  // still surfaces as a task failure (caught by Helix and retried at the task level).
  private static final int MAX_RUNTIME_UPDATE_ATTEMPTS = 128;

  private void updateMaterializedViewRuntime(Map<String, String> configs, String tableName,
      String taskMode, long windowStartMs, long windowEndMs) {
    HelixPropertyStore<ZNRecord> propertyStore = MINION_CONTEXT.getHelixPropertyStore();

    Exception lastException = null;
    for (int attempt = 0; attempt < MAX_RUNTIME_UPDATE_ATTEMPTS; attempt++) {
      if (attempt > 0) {
        // Jittered backoff to avoid thundering herd against ZK when batched APPEND tasks
        // race for the same MaterializedViewRuntimeMetadata znode (see MAX_RUNTIME_UPDATE_ATTEMPTS comment).
        try {
          Thread.sleep(50L + ThreadLocalRandom.current().nextInt(150));
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted while retrying MV runtime update for table: " + tableName, ie);
        }
        LOGGER.warn("Retrying MV runtime update for table: {} (attempt {}/{})", tableName, attempt + 1,
            MAX_RUNTIME_UPDATE_ATTEMPTS);
      }
      try {
        // Re-fetch with version on every attempt to pick up concurrent ConsistencyManager
        // updates (e.g. STALE markings) that may have arrived since the previous attempt.
        Stat freshStat = new Stat();
        MaterializedViewRuntimeMetadata existing =
            MaterializedViewRuntimeMetadataUtils.fetchWithVersion(propertyStore, tableName, freshStat);
        int writeVersion = (existing != null) ? freshStat.getVersion() : -1;

        Map<Long, PartitionInfo> mergedInfos;
        long existingWatermarkMs;

        if (existing != null) {
          mergedInfos = new HashMap<>(existing.getPartitions());
          existingWatermarkMs = existing.getWatermarkMs();
        } else {
          mergedInfos = new HashMap<>();
          existingWatermarkMs = 0L;
        }

        long newWatermarkMs;

        if (MaterializedViewTask.TASK_MODE_DELETE.equals(taskMode)) {
          mergedInfos.remove(windowStartMs);
          newWatermarkMs = existingWatermarkMs;
          LOGGER.info("DELETE mode: removed partition {} from MV runtime for table: {}", windowStartMs, tableName);
        } else {
          PartitionFingerprint newFingerprint = getTaskFingerprint(configs, tableName, windowStartMs);
          validateSourceFingerprintAtCommit(configs, tableName, windowStartMs, windowEndMs, newFingerprint);
          long nowMs = System.currentTimeMillis();
          PartitionInfo completedInfo = new PartitionInfo(PartitionState.VALID, newFingerprint, nowMs);
          mergedInfos.put(windowStartMs, completedInfo);
          LOGGER.info("Set partition {} to VALID (lastRefreshTime={}) for table: {}", windowStartMs, nowMs, tableName);

          if (MaterializedViewTask.TASK_MODE_APPEND.equals(taskMode)) {
            // Advance to the highest contiguous VALID block starting from the existing watermark.
            // Concurrent batch tasks may complete out of order; only advancing to windowEndMs would
            // leave gaps when an earlier window hasn't finished yet.  bucketMs is derived from the
            // task's window length (one bucket per APPEND task by construction).
            long bucketMs = windowEndMs - windowStartMs;
            Preconditions.checkState(bucketMs > 0,
                "Invalid window: windowEndMs (%s) <= windowStartMs (%s) for table %s",
                windowEndMs, windowStartMs, tableName);
            newWatermarkMs = MaterializedViewTaskUtils.computeContiguousUpperMs(existingWatermarkMs, mergedInfos,
                bucketMs);
            LOGGER.info("APPEND mode: advancing watermarkMs from {} to {} for table: {}",
                existingWatermarkMs, newWatermarkMs, tableName);
          } else {
            newWatermarkMs = existingWatermarkMs;
            LOGGER.info("OVERWRITE mode: keeping watermarkMs at {} for table: {}", newWatermarkMs, tableName);
          }
        }

        MaterializedViewRuntimeMetadata updated = new MaterializedViewRuntimeMetadata(
            tableName, newWatermarkMs, mergedInfos);
        MaterializedViewRuntimeMetadataUtils.persist(propertyStore, updated, writeVersion);

        LOGGER.info("Updated MV runtime for table: {} (partitions={}, watermarkMs={})",
            tableName, mergedInfos.size(), newWatermarkMs);
        return;
      } catch (Exception e) {
        lastException = e;
        LOGGER.warn("Failed to update MV runtime for table: {} on attempt {}", tableName, attempt + 1, e);
      }
    }
    throw new RuntimeException(
        "Failed to update MV runtime for table: " + tableName + " after " + MAX_RUNTIME_UPDATE_ATTEMPTS + " attempts",
        lastException);
  }

  private PartitionFingerprint getTaskFingerprint(Map<String, String> configs, String tableName, long windowStartMs) {
    String fingerprintStr = configs.get(MaterializedViewTask.PARTITION_FINGERPRINTS_KEY);
    Preconditions.checkState(fingerprintStr != null && !fingerprintStr.isEmpty(),
        "Missing source partition fingerprint for MV task table %s windowStartMs %s", tableName, windowStartMs);
    Map<Long, PartitionFingerprint> taskFingerprints = PartitionFingerprint.decodeMap(fingerprintStr);
    PartitionFingerprint fingerprint = taskFingerprints.get(windowStartMs);
    Preconditions.checkState(fingerprint != null,
        "Missing source partition fingerprint for MV task table %s windowStartMs %s", tableName, windowStartMs);
    return fingerprint;
  }

  private void validateSourceFingerprintAtCommit(Map<String, String> configs, String tableName, long windowStartMs,
      long windowEndMs, PartitionFingerprint taskFingerprint) {
    String sourceTableName = configs.get(MaterializedViewTask.SOURCE_TABLE_NAME_KEY);
    Preconditions.checkState(sourceTableName != null && !sourceTableName.isEmpty(),
        "Missing source table name for MV task table %s window [%s, %s)", tableName, windowStartMs, windowEndMs);
    String sourceTableWithType = resolveSourceTableNameWithType(sourceTableName);
    PartitionFingerprint currentFingerprint = computeWindowFingerprint(
        ZKMetadataProvider.getSegmentsZKMetadata(MINION_CONTEXT.getHelixPropertyStore(), sourceTableWithType),
        windowStartMs, windowEndMs);
    Preconditions.checkState(taskFingerprint.equals(currentFingerprint),
        "Source table %s changed while refreshing MV table %s window [%s, %s): taskFingerprint=%s, "
            + "currentFingerprint=%s. Leaving MV partition stale for retry.",
        sourceTableWithType, tableName, windowStartMs, windowEndMs, taskFingerprint, currentFingerprint);
  }

  private String resolveSourceTableNameWithType(String sourceTableName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(sourceTableName);
    if (tableType != null) {
      return sourceTableName;
    }
    String rawSourceTableName = TableNameBuilder.extractRawTableName(sourceTableName);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawSourceTableName);
    if (ZKMetadataProvider.getTableConfig(MINION_CONTEXT.getHelixPropertyStore(), offlineTableName) != null) {
      return offlineTableName;
    }
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawSourceTableName);
    Preconditions.checkState(
        ZKMetadataProvider.getTableConfig(MINION_CONTEXT.getHelixPropertyStore(), realtimeTableName) != null,
        "Source table config not found for: %s", sourceTableName);
    return realtimeTableName;
  }

  /// Computes a [PartitionFingerprint] for the segments that overlap [windowStartMs, windowEndMs).
  ///
  /// The fingerprint is `Hashing.farmHashFingerprint64` over the sorted concatenation of
  /// `<segmentName>\0<crc>\n` lines. Sorting by segment name makes the hash insensitive to
  /// listing order. FarmHash64 is non-cryptographic but collision-resistant for non-adversarial
  /// inputs; in particular it avoids the cancellation pathology of XOR-CRC (where swapping
  /// one segment for another with the same XOR contribution produces an identical fingerprint).
  @VisibleForTesting
  static PartitionFingerprint computeWindowFingerprint(List<SegmentZKMetadata> allSegments,
      long windowStartMs, long windowEndMs) {
    List<SegmentZKMetadata> overlapping = new ArrayList<>();
    for (SegmentZKMetadata seg : allSegments) {
      long segStartMs = seg.getStartTimeMs();
      long segEndMs = seg.getEndTimeMs();
      if (segStartMs < windowEndMs && segEndMs >= windowStartMs) {
        overlapping.add(seg);
      }
    }
    overlapping.sort(Comparator.comparing(SegmentZKMetadata::getSegmentName));

    Hasher hasher = Hashing.farmHashFingerprint64().newHasher();
    for (SegmentZKMetadata seg : overlapping) {
      hasher.putString(seg.getSegmentName(), StandardCharsets.UTF_8);
      hasher.putByte((byte) 0);
      hasher.putLong(seg.getCrc());
      hasher.putByte((byte) '\n');
    }
    return new PartitionFingerprint(overlapping.size(), hasher.hash().asLong());
  }

  /// Returns the highest contiguous VALID upper boundary starting from `fromMs`.
  ///
  /// When batch APPEND tasks run concurrently, windows may complete out of order.
  /// Advancing `watermarkMs` only to the just-completed `windowEndMs` would
  /// regress coverage if an earlier window hasn't finished yet. This method scans
  /// `partitions` for an unbroken chain of VALID windows beginning at `fromMs`
  /// and returns the end of the last VALID window in that chain.
  ///
  /// Bounded by `partitions.size()` iterations to defend against pathological maps.
  @VisibleForTesting
  static long computeContiguousUpperMs(long fromMs, Map<Long, PartitionInfo> partitions, long bucketMs) {
    return MaterializedViewTaskUtils.computeContiguousUpperMs(fromMs, partitions, bucketMs);
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(
      PinotTaskConfig pinotTaskConfig, SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(
        SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE, Collections.emptyMap());
  }

  /// Fails the task if the query result set saturated the declared `LIMIT`, since that
  /// strongly suggests the window was truncated and the resulting MV would be incomplete.
  ///
  /// Throwing here (before any segment build or `postProcess`) ensures:
  ///
  ///   - the partition is NOT marked [PartitionState#VALID];
  ///   - the runtime `watermarkMs` / partitions map are NOT advanced, so the broker will not
  ///       rewrite subsequent queries against the incomplete MV;
  ///   - Helix retries the task, letting transient causes self-heal.
  ///
  ///
  /// If the config is missing (older tasks in flight during rolling upgrade) or non-positive,
  /// the task fails loud — silently disabling the saturation gate is exactly the silent-truncation
  /// regression this guard protects against. Helix retries with the same task config, so a
  /// pre-upgrade task without `EFFECTIVE_LIMIT_KEY` will exhaust its retry budget and
  /// surface as a failed task; the operator must regenerate the task once the controller is
  /// upgraded. Documented upgrade order: upgrade controller before minion executors.
  @VisibleForTesting
  static void verifyResultNotTruncated(Map<String, String> configs, String tableName,
      long windowStartMs, long windowEndMs, int actualRows) {
    MaterializedViewTaskUtils.verifyResultNotTruncated(configs, tableName, windowStartMs, windowEndMs, actualRows);
  }

  /// Converts raw query result rows into [GenericRow] objects using column names
  /// from the [DataSchema].
  ///
  /// Each column name returned by the query must exist in the MV [Schema]. A
  /// mismatch indicates the `definedSQL` produced a column the MV table cannot store
  /// (e.g. an alias was renamed, the schema is out of date, or the analyzer mapping is
  /// stale); proceeding would silently drop the column from the persisted segment, so we
  /// fail loud instead. The analyzer enforces this invariant at table-create time, so
  /// hitting it at runtime points at a real correctness drift.
  private List<GenericRow> convertToGenericRows(DataSchema dataSchema, List<Object[]> rows, Schema schema) {
    String[] columnNames = dataSchema.getColumnNames();
    FieldSpec[] fieldSpecs = new FieldSpec[columnNames.length];
    for (int i = 0; i < columnNames.length; i++) {
      String columnName = columnNames[i];
      FieldSpec fieldSpec = schema.getFieldSpecFor(columnName);
      Preconditions.checkState(fieldSpec != null,
          "MV query returned column '%s' which is not declared in the MV schema. "
              + "Update the MV schema to include this column or fix the definedSQL projection.",
          columnName);
      fieldSpecs[i] = fieldSpec;
    }
    List<GenericRow> genericRows = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      GenericRow genericRow = new GenericRow();
      for (int i = 0; i < columnNames.length; i++) {
        String columnName = columnNames[i];
        Object value = row[i];
        FieldSpec fieldSpec = fieldSpecs[i];
        if (fieldSpec.getDataType().getStoredType() == FieldSpec.DataType.BYTES) {
          value = decodeBytesValue(columnName, value);
        }
        genericRow.putValue(columnName, value);
      }
      genericRows.add(genericRow);
    }
    return genericRows;
  }

  @VisibleForTesting
  static Object decodeBytesValue(String columnName, Object value) {
    if (value == null || value instanceof byte[]) {
      return value;
    }
    if (!(value instanceof String)) {
      return value;
    }
    String stringValue = (String) value;
    try {
      return BytesUtils.toBytes(stringValue);
    } catch (IllegalArgumentException hexException) {
      try {
        return Base64.getDecoder().decode(stringValue);
      } catch (IllegalArgumentException base64Exception) {
        base64Exception.addSuppressed(hexException);
        throw new IllegalArgumentException(
            "Cannot decode BYTES value for column: " + columnName + " as hex or base64", base64Exception);
      }
    }
  }

  /// Builds a segment name that is stable within a single attempt but unique across retries of the
  /// same window. The `attemptId` must be a per-invocation value (e.g., a fresh UUID) and
  /// must NOT be the Helix subtask id, which is reused across retries. Using the Helix subtask id
  /// would reproduce identical names on retry, causing the controller to reject the new lineage
  /// entry when segments from a previous partial attempt already exist.
  @VisibleForTesting
  static String buildSegmentName(String tableName, long windowStartMs, long windowEndMs,
      String attemptId, int segIdx) {
    return MaterializedViewTaskUtils.buildSegmentName(tableName, windowStartMs, windowEndMs, attemptId, segIdx);
  }
}
