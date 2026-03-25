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
package org.apache.pinot.segment.local.indexsegment.lakehouse;

import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.lakehouse.TabletManifest;
import org.apache.pinot.spi.lakehouse.TabletManifestStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Thread-safe cache for tablet manifests.
 *
 * <p>Manifests are loaded lazily from the {@link TabletManifestStore} on first access
 * and cached by tablet ID. This avoids repeated deep store reads for frequently accessed
 * tablets during query execution.</p>
 *
 * <p>Callers should use {@link #invalidate(String)} when a tablet's manifest is known to
 * have changed (e.g. after a snapshot advance notification from the controller), and
 * {@link #invalidateAll()} during server shutdown or table reload.</p>
 *
 * <p>This class is thread-safe. Concurrent calls to {@link #getManifest(String, String)}
 * for the same tablet ID may result in duplicate loads, but the cache will converge to
 * a single entry. This trade-off avoids lock contention on the hot path.</p>
 */
public class TabletManifestCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(TabletManifestCache.class);

  private final ConcurrentHashMap<String, TabletManifest> _cache;
  private final TabletManifestStore _manifestStore;

  /**
   * Creates a new manifest cache backed by the given store.
   *
   * @param manifestStore the store used to load manifests on cache miss
   */
  public TabletManifestCache(TabletManifestStore manifestStore) {
    _cache = new ConcurrentHashMap<>();
    _manifestStore = manifestStore;
  }

  /**
   * Returns the cached manifest for the given tablet, loading it from the store on cache miss.
   *
   * @param tabletId the tablet identifier (used as cache key)
   * @param manifestUri the URI to load the manifest from on cache miss
   * @return the tablet manifest, or null if the manifest could not be loaded
   */
  @Nullable
  public TabletManifest getManifest(String tabletId, String manifestUri) {
    TabletManifest cached = _cache.get(tabletId);
    if (cached != null) {
      return cached;
    }

    // Cache miss: load from store
    LOGGER.info("Cache miss for tablet manifest: tabletId={}, loading from URI={}", tabletId, manifestUri);
    String manifestJson = _manifestStore.readManifest(manifestUri);
    if (manifestJson == null) {
      LOGGER.warn("Manifest not found at URI={} for tabletId={}", manifestUri, tabletId);
      return null;
    }

    TabletManifest manifest = TabletManifest.fromJsonString(manifestJson);
    _cache.put(tabletId, manifest);
    return manifest;
  }

  /**
   * Removes the cached manifest for the given tablet, forcing a reload on next access.
   *
   * @param tabletId the tablet identifier to invalidate
   */
  public void invalidate(String tabletId) {
    TabletManifest removed = _cache.remove(tabletId);
    if (removed != null) {
      LOGGER.info("Invalidated cached manifest for tabletId={}", tabletId);
    }
  }

  /**
   * Removes all cached manifests.
   */
  public void invalidateAll() {
    int size = _cache.size();
    _cache.clear();
    LOGGER.info("Invalidated all {} cached tablet manifests", size);
  }

  /**
   * Returns the number of manifests currently cached.
   */
  public int getSize() {
    return _cache.size();
  }
}
