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
package org.apache.pinot.plugin.catalog.iceberg;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.pinot.spi.config.table.IcebergCatalogConfig;
import org.apache.pinot.spi.lakehouse.LakehouseCatalogAdapter;
import org.apache.pinot.spi.lakehouse.LakehouseFieldDescriptor;
import org.apache.pinot.spi.lakehouse.LakehouseFileDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Iceberg implementation of {@link LakehouseCatalogAdapter}.
 *
 * <p>Bridges Pinot to Apache Iceberg catalogs (REST, Hadoop, Hive, Glue, Nessie, JDBC).
 * This class uses the Iceberg Java API to load a table, resolve snapshots by branch, tag,
 * or timestamp, and enumerate data/delete files from snapshot manifests.</p>
 *
 * <p>Thread-safe after {@link #init(IcebergCatalogConfig)} completes. The underlying
 * {@link Catalog} and {@link Table} references are immutable once set.</p>
 */
public class IcebergCatalogAdapterImpl implements LakehouseCatalogAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergCatalogAdapterImpl.class);

  private static final String CATALOG_IMPL_REST = "org.apache.iceberg.rest.RESTCatalog";
  private static final String CATALOG_IMPL_HADOOP = "org.apache.iceberg.hadoop.HadoopCatalog";
  private static final String CATALOG_IMPL_HIVE = "org.apache.iceberg.hive.HiveCatalog";

  private volatile Catalog _catalog;
  private volatile Table _table;
  private volatile IcebergCatalogConfig _config;

  @Override
  public void init(IcebergCatalogConfig config) {
    Preconditions.checkNotNull(config, "IcebergCatalogConfig must not be null");
    Preconditions.checkNotNull(config.getType(), "Catalog type must not be null");
    Preconditions.checkNotNull(config.getTableIdentifier(), "Table identifier must not be null");

    _config = config;

    Map<String, String> catalogProperties = new HashMap<>();
    if (config.getUri() != null) {
      catalogProperties.put("uri", config.getUri());
    }
    if (config.getWarehouse() != null) {
      catalogProperties.put("warehouse", config.getWarehouse());
    }
    if (config.getProperties() != null) {
      catalogProperties.putAll(config.getProperties());
    }

    String catalogImpl = resolveCatalogImpl(config.getType());
    LOGGER.info("Initializing Iceberg catalog: type={}, impl={}, table={}",
        config.getType(), catalogImpl, config.getTableIdentifier());

    _catalog = CatalogUtil.buildIcebergCatalog("pinot-iceberg", catalogProperties, null);

    TableIdentifier tableId = TableIdentifier.parse(config.getTableIdentifier());
    _table = _catalog.loadTable(tableId);

    LOGGER.info("Loaded Iceberg table: uuid={}, currentSnapshot={}",
        _table.uuid(), _table.currentSnapshot() != null ? _table.currentSnapshot().snapshotId() : "none");
  }

  @Override
  public long getCurrentSnapshotId() {
    ensureInitialized();
    // Refresh to get the latest snapshot
    _table.refresh();
    Snapshot current = _table.currentSnapshot();
    if (current == null) {
      return -1;
    }
    return current.snapshotId();
  }

  @Override
  public long resolveSnapshotForBranch(String branch) {
    ensureInitialized();
    _table.refresh();
    Map<String, SnapshotRef> refs = _table.refs();
    SnapshotRef ref = refs.get(branch);
    if (ref == null || !ref.isBranch()) {
      LOGGER.warn("Branch '{}' not found in Iceberg table refs", branch);
      return -1;
    }
    return ref.snapshotId();
  }

  @Override
  public long resolveSnapshotForTag(String tag) {
    ensureInitialized();
    _table.refresh();
    Map<String, SnapshotRef> refs = _table.refs();
    SnapshotRef ref = refs.get(tag);
    if (ref == null || !ref.isTag()) {
      LOGGER.warn("Tag '{}' not found in Iceberg table refs", tag);
      return -1;
    }
    return ref.snapshotId();
  }

  @Override
  public long resolveSnapshotAsOf(long timestampMs) {
    ensureInitialized();
    _table.refresh();

    // Walk snapshots backwards to find the latest one at or before the given timestamp.
    Snapshot match = null;
    for (Snapshot snapshot : _table.snapshots()) {
      if (snapshot.timestampMillis() <= timestampMs) {
        if (match == null || snapshot.timestampMillis() > match.timestampMillis()) {
          match = snapshot;
        }
      }
    }

    if (match == null) {
      LOGGER.warn("No Iceberg snapshot found at or before timestamp {}", timestampMs);
      return -1;
    }
    return match.snapshotId();
  }

  @Override
  public List<LakehouseFileDescriptor> listDataFiles(long snapshotId) {
    return listFiles(snapshotId, true);
  }

  @Override
  public List<LakehouseFileDescriptor> listDeleteFiles(long snapshotId) {
    return listFiles(snapshotId, false);
  }

  @Nullable
  @Override
  public String getTableUuid() {
    ensureInitialized();
    return _table.uuid();
  }

  @Override
  public List<LakehouseFieldDescriptor> getSchemaFields() {
    ensureInitialized();
    _table.refresh();
    Schema schema = _table.schema();
    List<LakehouseFieldDescriptor> fields = new ArrayList<>();
    for (Types.NestedField field : schema.columns()) {
      fields.add(new LakehouseFieldDescriptor(
          field.fieldId(),
          field.name(),
          field.type().toString(),
          field.isRequired()));
    }
    return fields;
  }

  @Override
  public void close() throws IOException {
    if (_catalog instanceof java.io.Closeable) {
      ((java.io.Closeable) _catalog).close();
    }
    LOGGER.info("Closed Iceberg catalog adapter for table: {}",
        _config != null ? _config.getTableIdentifier() : "unknown");
  }

  /**
   * Lists data files or delete files from the given snapshot.
   *
   * @param snapshotId Iceberg snapshot ID
   * @param dataFilesOnly if true, return only data files; if false, return only delete files
   * @return list of file descriptors
   */
  private List<LakehouseFileDescriptor> listFiles(long snapshotId, boolean dataFilesOnly) {
    ensureInitialized();

    Snapshot snapshot = _table.snapshot(snapshotId);
    Preconditions.checkArgument(snapshot != null, "Snapshot %s not found", snapshotId);

    List<LakehouseFileDescriptor> result = new ArrayList<>();

    // Use a table scan pinned to the specific snapshot for data files
    if (dataFilesOnly) {
      TableScan scan = _table.newScan().useSnapshot(snapshotId);
      try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
        for (FileScanTask task : tasks) {
          DataFile dataFile = task.file();
          if (dataFile.content() == FileContent.DATA) {
            result.add(toDescriptor(dataFile));
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to scan data files for snapshot " + snapshotId, e);
      }
    } else {
      // For delete files, scan tasks expose them through deletes()
      TableScan scan = _table.newScan().useSnapshot(snapshotId);
      try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
        // Track seen paths to avoid duplicates across tasks
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (FileScanTask task : tasks) {
          for (org.apache.iceberg.DeleteFile deleteFile : task.deletes()) {
            if (seen.add(deleteFile.location())) {
              result.add(toDeleteDescriptor(deleteFile));
            }
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to scan delete files for snapshot " + snapshotId, e);
      }
    }

    LOGGER.debug("Listed {} {} files for snapshot {}", result.size(),
        dataFilesOnly ? "data" : "delete", snapshotId);
    return result;
  }

  /**
   * Converts an Iceberg {@link DataFile} to a {@link LakehouseFileDescriptor}.
   */
  private LakehouseFileDescriptor toDescriptor(DataFile dataFile) {
    int contentType = mapFileContent(dataFile.content());

    List<String> partitionValues = extractPartitionValues(dataFile);
    Map<String, String> lowerBounds = convertBoundsMap(dataFile.lowerBounds());
    Map<String, String> upperBounds = convertBoundsMap(dataFile.upperBounds());
    Map<String, Long> nullCounts = dataFile.nullValueCounts();

    return new LakehouseFileDescriptor(
        dataFile.location(),
        dataFile.format().name(),
        dataFile.fileSizeInBytes(),
        dataFile.recordCount(),
        contentType,
        dataFile.specId(),
        partitionValues,
        lowerBounds,
        upperBounds,
        nullCounts);
  }

  /**
   * Converts an Iceberg {@link org.apache.iceberg.DeleteFile} to a {@link LakehouseFileDescriptor}.
   */
  private LakehouseFileDescriptor toDeleteDescriptor(org.apache.iceberg.DeleteFile deleteFile) {
    int contentType = mapFileContent(deleteFile.content());

    List<String> partitionValues = extractDeletePartitionValues(deleteFile);
    Map<String, String> lowerBounds = convertBoundsMap(deleteFile.lowerBounds());
    Map<String, String> upperBounds = convertBoundsMap(deleteFile.upperBounds());
    Map<String, Long> nullCounts = deleteFile.nullValueCounts();

    return new LakehouseFileDescriptor(
        deleteFile.location(),
        deleteFile.format().name(),
        deleteFile.fileSizeInBytes(),
        deleteFile.recordCount(),
        contentType,
        deleteFile.specId(),
        partitionValues,
        lowerBounds,
        upperBounds,
        nullCounts);
  }

  /**
   * Maps an Iceberg {@link FileContent} to a {@link LakehouseFileDescriptor} content type constant.
   */
  private static int mapFileContent(FileContent content) {
    switch (content) {
      case DATA:
        return LakehouseFileDescriptor.CONTENT_DATA;
      case POSITION_DELETES:
        return LakehouseFileDescriptor.CONTENT_POSITION_DELETES;
      case EQUALITY_DELETES:
        return LakehouseFileDescriptor.CONTENT_EQUALITY_DELETES;
      default:
        return LakehouseFileDescriptor.CONTENT_DATA;
    }
  }

  /**
   * Extracts partition values from a data file as a list of strings.
   */
  private List<String> extractPartitionValues(DataFile dataFile) {
    org.apache.iceberg.StructLike partition = dataFile.partition();
    if (partition == null || partition.size() == 0) {
      return Collections.emptyList();
    }
    List<String> values = new ArrayList<>(partition.size());
    for (int i = 0; i < partition.size(); i++) {
      Object val = partition.get(i, Object.class);
      values.add(val != null ? val.toString() : null);
    }
    return values;
  }

  /**
   * Extracts partition values from a delete file as a list of strings.
   */
  private List<String> extractDeletePartitionValues(org.apache.iceberg.DeleteFile deleteFile) {
    org.apache.iceberg.StructLike partition = deleteFile.partition();
    if (partition == null || partition.size() == 0) {
      return Collections.emptyList();
    }
    List<String> values = new ArrayList<>(partition.size());
    for (int i = 0; i < partition.size(); i++) {
      Object val = partition.get(i, Object.class);
      values.add(val != null ? val.toString() : null);
    }
    return values;
  }

  /**
   * Converts Iceberg column bounds ({@code Map<Integer, ByteBuffer>}) to a string map
   * keyed by field ID with UTF-8 string values.
   */
  @Nullable
  private Map<String, String> convertBoundsMap(@Nullable Map<Integer, ByteBuffer> bounds) {
    if (bounds == null || bounds.isEmpty()) {
      return null;
    }
    Map<String, String> result = new HashMap<>(bounds.size());
    for (Map.Entry<Integer, ByteBuffer> entry : bounds.entrySet()) {
      ByteBuffer buf = entry.getValue().duplicate();
      byte[] bytes = new byte[buf.remaining()];
      buf.get(bytes);
      result.put(String.valueOf(entry.getKey()), new String(bytes, StandardCharsets.UTF_8));
    }
    return result;
  }

  /**
   * Resolves the Iceberg catalog implementation class name from the configured catalog type.
   */
  private static String resolveCatalogImpl(IcebergCatalogConfig.CatalogType type) {
    switch (type) {
      case REST:
        return CATALOG_IMPL_REST;
      case HADOOP:
        return CATALOG_IMPL_HADOOP;
      case HIVE:
        return CATALOG_IMPL_HIVE;
      case GLUE:
        return "org.apache.iceberg.aws.glue.GlueCatalog";
      case NESSIE:
        return "org.apache.iceberg.nessie.NessieCatalog";
      case JDBC:
        return "org.apache.iceberg.jdbc.JdbcCatalog";
      default:
        throw new IllegalArgumentException("Unsupported Iceberg catalog type: " + type);
    }
  }

  /**
   * Ensures that the adapter has been properly initialized before use.
   */
  private void ensureInitialized() {
    Preconditions.checkState(_table != null, "IcebergCatalogAdapterImpl has not been initialized. Call init() first.");
  }
}
