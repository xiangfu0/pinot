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
package org.apache.pinot.spi.lakehouse;

import java.io.Closeable;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.IcebergCatalogConfig;


/**
 * SPI interface for lakehouse catalog operations.
 *
 * <p>Implementations bridge Pinot to a specific lakehouse catalog (e.g. Iceberg REST, Hadoop,
 * Hive). This keeps Iceberg-specific dependencies out of Pinot's foundational modules.</p>
 *
 * <p>Implementations must be thread-safe after {@link #init(IcebergCatalogConfig)}.</p>
 */
public interface LakehouseCatalogAdapter extends Closeable {

  /**
   * Initializes the adapter with catalog configuration.
   */
  void init(IcebergCatalogConfig config);

  /**
   * Returns the current snapshot ID for the table (head of main branch).
   */
  long getCurrentSnapshotId();

  /**
   * Resolves a named branch to a snapshot ID.
   *
   * @param branch branch name (e.g. "main")
   * @return snapshot ID, or -1 if branch does not exist
   */
  long resolveSnapshotForBranch(String branch);

  /**
   * Resolves a named tag to a snapshot ID.
   *
   * @param tag tag name
   * @return snapshot ID, or -1 if tag does not exist
   */
  long resolveSnapshotForTag(String tag);

  /**
   * Resolves a timestamp to the latest snapshot at or before that time.
   *
   * @param timestampMs epoch milliseconds
   * @return snapshot ID, or -1 if no snapshot exists at or before the given time
   */
  long resolveSnapshotAsOf(long timestampMs);

  /**
   * Lists all data files for a given snapshot.
   *
   * @param snapshotId Iceberg snapshot ID
   * @return list of file descriptors (data files only, not delete files)
   */
  List<LakehouseFileDescriptor> listDataFiles(long snapshotId);

  /**
   * Lists all delete files (position and equality) for a given snapshot.
   *
   * @param snapshotId Iceberg snapshot ID
   * @return list of delete file descriptors, or empty list if none
   */
  List<LakehouseFileDescriptor> listDeleteFiles(long snapshotId);

  /**
   * Returns the Iceberg table UUID if available.
   */
  @Nullable
  String getTableUuid();

  /**
   * Returns the current Iceberg schema field names and types.
   *
   * @return list of field descriptors
   */
  List<LakehouseFieldDescriptor> getSchemaFields();
}
