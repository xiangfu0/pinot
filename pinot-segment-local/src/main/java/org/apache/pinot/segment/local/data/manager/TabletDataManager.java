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
package org.apache.pinot.segment.local.data.manager;

import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.lakehouse.TabletManifest;
import org.apache.pinot.common.metadata.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.segment.spi.IndexSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages a single tablet's data on the server side.
 *
 * <p>This is the lakehouse-native equivalent of segment-level data managers. It handles
 * manifest loading/caching and provides access to the tablet's execution state.</p>
 *
 * <p>For Phase 1, {@link #getSegment()} returns {@code null} because the full
 * TabletIndexSegment (backed by Parquet readers) is not yet implemented. The tablet
 * metadata is still available for routing and planning purposes.</p>
 *
 * <p>Thread-safety: The manifest field is volatile to support safe publication after
 * refresh. The envelope is immutable after construction. Reference counting is inherited
 * from {@link SegmentDataManager}.</p>
 */
public class TabletDataManager extends SegmentDataManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(TabletDataManager.class);

  private final TabletMetadataEnvelope _envelope;
  private volatile TabletManifest _manifest;

  /**
   * Creates a tablet data manager.
   *
   * @param envelope the ZK-stored tablet metadata envelope (immutable)
   * @param manifest the initial tablet manifest loaded from deep store, or null if not yet loaded
   */
  public TabletDataManager(TabletMetadataEnvelope envelope, @Nullable TabletManifest manifest) {
    _envelope = envelope;
    _manifest = manifest;
  }

  @Override
  public String getSegmentName() {
    return _envelope.getTabletId();
  }

  /**
   * Returns {@code null} for Phase 1. A future phase will return a {@code TabletIndexSegment}
   * backed by Parquet file readers.
   */
  @Nullable
  @Override
  public IndexSegment getSegment() {
    // Phase 1: TabletIndexSegment not yet implemented
    return null;
  }

  /**
   * Returns the tablet metadata envelope.
   */
  public TabletMetadataEnvelope getEnvelope() {
    return _envelope;
  }

  /**
   * Returns the current tablet manifest, or null if not yet loaded.
   */
  @Nullable
  public TabletManifest getManifest() {
    return _manifest;
  }

  /**
   * Replaces the current manifest with a refreshed version.
   * This is used when the controller publishes a new manifest for the same tablet
   * (e.g. after an Iceberg snapshot advance).
   *
   * @param manifest the new tablet manifest
   */
  public void refreshManifest(TabletManifest manifest) {
    LOGGER.info("Refreshing manifest for tablet: {} (snapshot {} -> {})", _envelope.getTabletId(),
        _manifest != null ? _manifest.getSnapshotId() : "null", manifest.getSnapshotId());
    _manifest = manifest;
  }

  @Override
  public void doOffload() {
    LOGGER.info("Offloading tablet data manager for tablet: {}", _envelope.getTabletId());
    // Phase 1: No metadata to clean up. Future phases may need to unregister from
    // upsert metadata or other server-side bookkeeping.
  }

  @Override
  protected void doDestroy() {
    LOGGER.info("Destroying tablet data manager for tablet: {}", _envelope.getTabletId());
    _manifest = null;
    // Phase 1: No resources to release. Future phases will close Parquet readers,
    // release cached file handles, etc.
  }
}
