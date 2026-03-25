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
package org.apache.pinot.controller.helix.core.lakehouse;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.lakehouse.MicrosegmentDescriptor;
import org.apache.pinot.common.metadata.lakehouse.TabletManifest;
import org.apache.pinot.common.metadata.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.spi.config.table.LakehouseConfig;
import org.apache.pinot.spi.config.table.LakehouseReadConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TabletConfig;
import org.apache.pinot.spi.lakehouse.LakehouseCatalogAdapter;
import org.apache.pinot.spi.lakehouse.LakehouseFileDescriptor;
import org.apache.pinot.spi.lakehouse.TabletManifestStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Core controller service for managing lakehouse tablet lifecycle.
 *
 * <p>This manager is responsible for:
 * <ul>
 *   <li>Resolving Iceberg snapshots via {@link LakehouseCatalogAdapter}</li>
 *   <li>Listing data files and grouping them into tablets using {@link TabletGrouper}</li>
 *   <li>Creating {@link TabletManifest} objects and persisting them via {@link TabletManifestStore}</li>
 *   <li>Producing {@link TabletMetadataEnvelope} objects for ZooKeeper / broker routing</li>
 * </ul>
 *
 * <p>Catalog adapters and tablet envelopes are cached per table in concurrent hash maps.
 * The manager is thread-safe and designed to be shared across periodic task invocations.</p>
 */
public class LakehouseTableManager implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(LakehouseTableManager.class);

  private final TabletManifestStore _manifestStore;

  /** Cached catalog adapters keyed by tableNameWithType. */
  private final ConcurrentHashMap<String, LakehouseCatalogAdapter> _catalogAdapters = new ConcurrentHashMap<>();

  /** Cached tablet envelopes keyed by tableNameWithType. */
  private final ConcurrentHashMap<String, List<TabletMetadataEnvelope>> _tabletEnvelopes = new ConcurrentHashMap<>();

  /** Cached snapshot IDs keyed by tableNameWithType, used for change detection. */
  private final ConcurrentHashMap<String, Long> _lastSnapshotIds = new ConcurrentHashMap<>();

  /** Factory for creating catalog adapter instances. Pluggable for testing. */
  private final CatalogAdapterFactory _catalogAdapterFactory;

  /**
   * Factory interface for creating {@link LakehouseCatalogAdapter} instances.
   * This allows test code to inject mock adapters.
   */
  public interface CatalogAdapterFactory {
    /**
     * Creates and initializes a catalog adapter for the given lakehouse configuration.
     *
     * @param lakehouseConfig lakehouse configuration containing catalog settings
     * @return initialized catalog adapter
     */
    LakehouseCatalogAdapter create(LakehouseConfig lakehouseConfig);
  }

  /**
   * Creates a new LakehouseTableManager.
   *
   * @param manifestStore store for persisting tablet manifests outside ZooKeeper
   * @param catalogAdapterFactory factory for creating catalog adapter instances
   */
  public LakehouseTableManager(TabletManifestStore manifestStore, CatalogAdapterFactory catalogAdapterFactory) {
    _manifestStore = manifestStore;
    _catalogAdapterFactory = catalogAdapterFactory;
  }

  /**
   * Refreshes the tablet metadata for a lakehouse-enabled table.
   *
   * <p>This method resolves the current Iceberg snapshot, lists data files, groups them
   * into tablets, creates manifests, and produces envelope metadata for each tablet.
   * If the snapshot has not changed since the last refresh, this method returns the
   * cached envelopes without re-processing.</p>
   *
   * @param tableNameWithType table name with type suffix
   * @param tableConfig table configuration (must have lakehouse enabled)
   * @return list of tablet metadata envelopes, or empty list if lakehouse is not configured
   */
  public List<TabletMetadataEnvelope> refreshTableTablets(String tableNameWithType, TableConfig tableConfig) {
    LakehouseConfig lakehouseConfig = tableConfig.getLakehouseConfig();
    if (lakehouseConfig == null || !lakehouseConfig.isEnabled()) {
      LOGGER.debug("Lakehouse not enabled for table: {}, skipping refresh", tableNameWithType);
      return Collections.emptyList();
    }

    try {
      // Step 1: Get or create catalog adapter
      LakehouseCatalogAdapter adapter = getOrCreateCatalogAdapter(tableNameWithType, lakehouseConfig);

      // Step 2: Resolve the current snapshot
      long snapshotId = resolveSnapshotId(adapter, lakehouseConfig);
      if (snapshotId < 0) {
        LOGGER.warn("No valid snapshot found for table: {}, skipping refresh", tableNameWithType);
        return _tabletEnvelopes.getOrDefault(tableNameWithType, Collections.emptyList());
      }

      // Step 3: Check if snapshot has changed
      Long lastSnapshotId = _lastSnapshotIds.get(tableNameWithType);
      if (lastSnapshotId != null && lastSnapshotId == snapshotId) {
        LOGGER.debug("Snapshot {} unchanged for table: {}, returning cached envelopes", snapshotId,
            tableNameWithType);
        return _tabletEnvelopes.getOrDefault(tableNameWithType, Collections.emptyList());
      }

      LOGGER.info("Snapshot changed for table: {} (old={}, new={}), refreshing tablets", tableNameWithType,
          lastSnapshotId, snapshotId);

      // Step 4: List data files from the snapshot
      List<LakehouseFileDescriptor> dataFiles = adapter.listDataFiles(snapshotId);
      LOGGER.info("Listed {} data files for table: {} at snapshot {}", dataFiles.size(), tableNameWithType,
          snapshotId);

      // Step 5: Group files into tablets
      TabletConfig tabletConfig = resolveTabletConfig(lakehouseConfig);
      String timeColumnName = extractTimeColumnName(tableConfig);
      Map<String, List<LakehouseFileDescriptor>> tabletGroups =
          TabletGrouper.groupFiles(dataFiles, tabletConfig, timeColumnName);

      // Step 6: Create manifests and envelopes for each tablet
      String tableUuid = adapter.getTableUuid();
      List<TabletMetadataEnvelope> envelopes = new ArrayList<>(tabletGroups.size());

      for (Map.Entry<String, List<LakehouseFileDescriptor>> entry : tabletGroups.entrySet()) {
        String tabletId = entry.getKey();
        List<LakehouseFileDescriptor> tabletFiles = entry.getValue();

        TabletManifest manifest = buildManifest(tabletId, tableUuid, snapshotId, tabletFiles);
        String manifestUri = _manifestStore.writeManifest(tableNameWithType, tabletId, manifest.toJsonString());

        TabletMetadataEnvelope envelope = buildEnvelope(tabletId, tableNameWithType, snapshotId, tabletFiles,
            manifestUri);
        envelopes.add(envelope);
      }

      // Step 7: Update caches
      _tabletEnvelopes.put(tableNameWithType, Collections.unmodifiableList(envelopes));
      _lastSnapshotIds.put(tableNameWithType, snapshotId);

      LOGGER.info("Refreshed {} tablets for table: {} at snapshot {}", envelopes.size(), tableNameWithType,
          snapshotId);
      return envelopes;
    } catch (Exception e) {
      LOGGER.error("Failed to refresh tablets for table: {}", tableNameWithType, e);
      return _tabletEnvelopes.getOrDefault(tableNameWithType, Collections.emptyList());
    }
  }

  /**
   * Returns the current cached tablet envelopes for a table.
   *
   * @param tableNameWithType table name with type suffix
   * @return list of cached tablet metadata envelopes, or empty list if none cached
   */
  public List<TabletMetadataEnvelope> getTableTablets(String tableNameWithType) {
    return _tabletEnvelopes.getOrDefault(tableNameWithType, Collections.emptyList());
  }

  /**
   * Removes cached state for a table (e.g. when the table is deleted or leadership is lost).
   *
   * @param tableNameWithType table name with type suffix
   */
  public void removeTable(String tableNameWithType) {
    _tabletEnvelopes.remove(tableNameWithType);
    _lastSnapshotIds.remove(tableNameWithType);
    LakehouseCatalogAdapter adapter = _catalogAdapters.remove(tableNameWithType);
    if (adapter != null) {
      try {
        adapter.close();
      } catch (IOException e) {
        LOGGER.warn("Error closing catalog adapter for table: {}", tableNameWithType, e);
      }
    }
    LOGGER.info("Removed cached lakehouse state for table: {}", tableNameWithType);
  }

  @Override
  public void close()
      throws IOException {
    LOGGER.info("Closing LakehouseTableManager, cleaning up {} catalog adapters", _catalogAdapters.size());
    for (Map.Entry<String, LakehouseCatalogAdapter> entry : _catalogAdapters.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        LOGGER.warn("Error closing catalog adapter for table: {}", entry.getKey(), e);
      }
    }
    _catalogAdapters.clear();
    _tabletEnvelopes.clear();
    _lastSnapshotIds.clear();
    _manifestStore.close();
  }

  /**
   * Returns the cached snapshot ID for a table, or -1 if not cached.
   * Exposed for testing and monitoring.
   */
  @VisibleForTesting
  long getLastSnapshotId(String tableNameWithType) {
    Long snapshotId = _lastSnapshotIds.get(tableNameWithType);
    return snapshotId != null ? snapshotId : -1;
  }

  /**
   * Gets or creates a catalog adapter for the given table. The adapter is cached
   * and reused across refresh cycles.
   */
  private LakehouseCatalogAdapter getOrCreateCatalogAdapter(String tableNameWithType,
      LakehouseConfig lakehouseConfig) {
    return _catalogAdapters.computeIfAbsent(tableNameWithType, k -> {
      LOGGER.info("Creating new catalog adapter for table: {}", tableNameWithType);
      return _catalogAdapterFactory.create(lakehouseConfig);
    });
  }

  /**
   * Resolves the snapshot ID based on the read configuration (branch, tag, or snapshot ID).
   */
  private long resolveSnapshotId(LakehouseCatalogAdapter adapter, LakehouseConfig lakehouseConfig) {
    LakehouseReadConfig readConfig = lakehouseConfig.getRead();
    if (readConfig == null) {
      return adapter.getCurrentSnapshotId();
    }

    LakehouseConfig.RefType refType = readConfig.getDefaultRefType();
    String refName = readConfig.getDefaultRefName();

    switch (refType) {
      case BRANCH:
        return adapter.resolveSnapshotForBranch(refName);
      case TAG:
        return adapter.resolveSnapshotForTag(refName);
      case SNAPSHOT_ID:
        try {
          return Long.parseLong(refName);
        } catch (NumberFormatException e) {
          LOGGER.error("Invalid snapshot ID in read config: {}", refName);
          return -1;
        }
      default:
        LOGGER.warn("Unknown ref type: {}, falling back to current snapshot", refType);
        return adapter.getCurrentSnapshotId();
    }
  }

  /**
   * Resolves the tablet configuration, using defaults if not explicitly configured.
   */
  private TabletConfig resolveTabletConfig(LakehouseConfig lakehouseConfig) {
    TabletConfig tabletConfig = lakehouseConfig.getTablet();
    if (tabletConfig != null) {
      return tabletConfig;
    }
    // Return default tablet config
    return new TabletConfig(null, null, null);
  }

  /**
   * Extracts the time column name from the table config's validation config, if available.
   */
  @Nullable
  private String extractTimeColumnName(TableConfig tableConfig) {
    if (tableConfig.getValidationConfig() != null) {
      return tableConfig.getValidationConfig().getTimeColumnName();
    }
    return null;
  }

  /**
   * Builds a {@link TabletManifest} from the grouped files.
   */
  private TabletManifest buildManifest(String tabletId, @Nullable String tableUuid, long snapshotId,
      List<LakehouseFileDescriptor> files) {
    List<MicrosegmentDescriptor> microsegments = new ArrayList<>(files.size());
    int specId = 0;

    for (int i = 0; i < files.size(); i++) {
      LakehouseFileDescriptor file = files.get(i);
      specId = file.getSpecId();

      // Compute min/max time for this file from column bounds
      long fileMinTime = extractMinTimeMs(file);
      long fileMaxTime = extractMaxTimeMs(file);

      String microsegmentId = tabletId + "_ms_" + i;
      MicrosegmentDescriptor descriptor = new MicrosegmentDescriptor(
          microsegmentId,
          file.getFilePath(),
          file.getFileFormat(),
          file.getFileSizeBytes(),
          file.getRowCount(),
          fileMinTime,
          fileMaxTime,
          file.getColumnLowerBounds(),
          file.getColumnUpperBounds(),
          file.getColumnNullCounts(),
          file.getContentType()
      );
      microsegments.add(descriptor);
    }

    return new TabletManifest(
        tabletId,
        TabletManifest.CURRENT_VERSION,
        tableUuid,
        snapshotId,
        specId,
        microsegments,
        null,   // deleteFiles
        null,   // sidecarReferences
        0L,     // committedWatermarkMs
        System.currentTimeMillis()
    );
  }

  /**
   * Builds a {@link TabletMetadataEnvelope} summarizing the tablet.
   */
  private TabletMetadataEnvelope buildEnvelope(String tabletId, String tableNameWithType, long snapshotId,
      List<LakehouseFileDescriptor> files, String manifestUri) {
    long totalRows = 0;
    long totalBytes = 0;
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    int specId = 0;
    List<String> partitionValues = null;

    for (LakehouseFileDescriptor file : files) {
      totalRows += file.getRowCount();
      totalBytes += file.getFileSizeBytes();
      specId = file.getSpecId();
      if (partitionValues == null) {
        partitionValues = file.getPartitionValues();
      }

      long fileMinTime = extractMinTimeMs(file);
      long fileMaxTime = extractMaxTimeMs(file);
      if (fileMinTime < minTime) {
        minTime = fileMinTime;
      }
      if (fileMaxTime > maxTime) {
        maxTime = fileMaxTime;
      }
    }

    // If no valid time bounds were found, use 0
    if (minTime == Long.MAX_VALUE) {
      minTime = 0;
    }
    if (maxTime == Long.MIN_VALUE) {
      maxTime = 0;
    }

    return new TabletMetadataEnvelope(
        tabletId,
        tableNameWithType,
        TabletMetadataEnvelope.CURRENT_VERSION,
        snapshotId,
        specId,
        partitionValues,
        minTime,
        maxTime,
        totalRows,
        totalBytes,
        files.size(),
        manifestUri
    );
  }

  /**
   * Extracts the minimum time in milliseconds from a file's column lower bounds.
   * Returns 0 if no time information is available.
   */
  private long extractMinTimeMs(LakehouseFileDescriptor file) {
    // Use the file's min time from column lower bounds if available
    // The MicrosegmentDescriptor stores minTimeMs/maxTimeMs; we derive them from bounds
    Map<String, String> lowerBounds = file.getColumnLowerBounds();
    if (lowerBounds != null) {
      // Try common time column names
      for (String col : lowerBounds.keySet()) {
        // Return the first parseable long value found in lower bounds
        // In practice, the caller should filter by the time column
        try {
          return Long.parseLong(lowerBounds.get(col));
        } catch (NumberFormatException ignored) {
          // Not a time column, continue
        }
      }
    }
    return 0;
  }

  /**
   * Extracts the maximum time in milliseconds from a file's column upper bounds.
   * Returns 0 if no time information is available.
   */
  private long extractMaxTimeMs(LakehouseFileDescriptor file) {
    Map<String, String> upperBounds = file.getColumnUpperBounds();
    if (upperBounds != null) {
      for (String col : upperBounds.keySet()) {
        try {
          return Long.parseLong(upperBounds.get(col));
        } catch (NumberFormatException ignored) {
          // Not a time column, continue
        }
      }
    }
    return 0;
  }
}
