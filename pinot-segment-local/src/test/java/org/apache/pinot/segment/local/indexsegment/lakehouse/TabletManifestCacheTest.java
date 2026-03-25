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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.metadata.lakehouse.MicrosegmentDescriptor;
import org.apache.pinot.common.metadata.lakehouse.TabletManifest;
import org.apache.pinot.spi.lakehouse.TabletManifestStore;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Tests for {@link TabletManifestCache}.
 */
public class TabletManifestCacheTest {

  private static final String TABLET_ID = "tablet-001";
  private static final String MANIFEST_URI = "s3://bucket/manifests/tablet-001.json";

  private TabletManifest createTestManifest(String tabletId) {
    MicrosegmentDescriptor descriptor = new MicrosegmentDescriptor(
        "ms-001", "s3://bucket/data/file1.parquet", "PARQUET",
        1024L, 100L, 1000L, 2000L,
        null, null, null, 0);
    return new TabletManifest(
        tabletId, TabletManifest.CURRENT_VERSION, "iceberg-uuid-123",
        42L, 0, Collections.singletonList(descriptor),
        null, null, 0L, System.currentTimeMillis());
  }

  private TabletManifestStore createMockStore(String manifestUri, TabletManifest manifest) {
    TabletManifestStore store = mock(TabletManifestStore.class);
    when(store.readManifest(manifestUri)).thenReturn(manifest.toJsonString());
    return store;
  }

  @Test
  public void testCacheMissTriggersLoadFromStore() {
    TabletManifest manifest = createTestManifest(TABLET_ID);
    TabletManifestStore store = createMockStore(MANIFEST_URI, manifest);
    TabletManifestCache cache = new TabletManifestCache(store);

    Assert.assertEquals(cache.getSize(), 0);

    TabletManifest result = cache.getManifest(TABLET_ID, MANIFEST_URI);

    Assert.assertNotNull(result);
    Assert.assertEquals(result.getTabletId(), TABLET_ID);
    Assert.assertEquals(result.getSnapshotId(), 42L);
    Assert.assertEquals(result.getTotalRowCount(), 100L);
    Assert.assertEquals(cache.getSize(), 1);
    verify(store, times(1)).readManifest(MANIFEST_URI);
  }

  @Test
  public void testCacheHitReturnsCachedManifest() {
    TabletManifest manifest = createTestManifest(TABLET_ID);
    TabletManifestStore store = createMockStore(MANIFEST_URI, manifest);
    TabletManifestCache cache = new TabletManifestCache(store);

    // First call - cache miss
    TabletManifest result1 = cache.getManifest(TABLET_ID, MANIFEST_URI);
    // Second call - cache hit
    TabletManifest result2 = cache.getManifest(TABLET_ID, MANIFEST_URI);

    Assert.assertNotNull(result1);
    Assert.assertNotNull(result2);
    Assert.assertSame(result1, result2);
    Assert.assertEquals(cache.getSize(), 1);
    // Store should only be called once
    verify(store, times(1)).readManifest(MANIFEST_URI);
  }

  @Test
  public void testInvalidateRemovesEntry() {
    TabletManifest manifest = createTestManifest(TABLET_ID);
    TabletManifestStore store = createMockStore(MANIFEST_URI, manifest);
    TabletManifestCache cache = new TabletManifestCache(store);

    // Load into cache
    cache.getManifest(TABLET_ID, MANIFEST_URI);
    Assert.assertEquals(cache.getSize(), 1);

    // Invalidate
    cache.invalidate(TABLET_ID);
    Assert.assertEquals(cache.getSize(), 0);

    // Next access should reload from store
    cache.getManifest(TABLET_ID, MANIFEST_URI);
    verify(store, times(2)).readManifest(MANIFEST_URI);
  }

  @Test
  public void testInvalidateAllClearsCache() {
    TabletManifestStore store = mock(TabletManifestStore.class);
    TabletManifestCache cache = new TabletManifestCache(store);

    // Load multiple tablets
    String tabletId1 = "tablet-001";
    String tabletId2 = "tablet-002";
    String uri1 = "s3://bucket/manifests/tablet-001.json";
    String uri2 = "s3://bucket/manifests/tablet-002.json";

    TabletManifest manifest1 = createTestManifest(tabletId1);
    TabletManifest manifest2 = createTestManifest(tabletId2);
    when(store.readManifest(uri1)).thenReturn(manifest1.toJsonString());
    when(store.readManifest(uri2)).thenReturn(manifest2.toJsonString());

    cache.getManifest(tabletId1, uri1);
    cache.getManifest(tabletId2, uri2);
    Assert.assertEquals(cache.getSize(), 2);

    // Invalidate all
    cache.invalidateAll();
    Assert.assertEquals(cache.getSize(), 0);
  }

  @Test
  public void testManifestNotFoundReturnsNull() {
    TabletManifestStore store = mock(TabletManifestStore.class);
    when(store.readManifest(MANIFEST_URI)).thenReturn(null);
    TabletManifestCache cache = new TabletManifestCache(store);

    TabletManifest result = cache.getManifest(TABLET_ID, MANIFEST_URI);

    Assert.assertNull(result);
    Assert.assertEquals(cache.getSize(), 0);
  }

  @Test
  public void testConcurrentAccessSafety()
      throws Exception {
    TabletManifest manifest = createTestManifest(TABLET_ID);
    AtomicInteger loadCount = new AtomicInteger(0);

    TabletManifestStore store = mock(TabletManifestStore.class);
    when(store.readManifest(MANIFEST_URI)).thenAnswer(invocation -> {
      loadCount.incrementAndGet();
      return manifest.toJsonString();
    });

    TabletManifestCache cache = new TabletManifestCache(store);
    int numThreads = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);

    List<Future<TabletManifest>> futures = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      futures.add(executor.submit(() -> {
        startLatch.await();
        return cache.getManifest(TABLET_ID, MANIFEST_URI);
      }));
    }

    // Release all threads simultaneously
    startLatch.countDown();

    // Collect results
    for (Future<TabletManifest> future : futures) {
      TabletManifest result = future.get();
      Assert.assertNotNull(result);
      Assert.assertEquals(result.getTabletId(), TABLET_ID);
    }

    // Cache should have exactly one entry
    Assert.assertEquals(cache.getSize(), 1);

    executor.shutdown();
  }

  @Test
  public void testInvalidateNonExistentKeyIsNoOp() {
    TabletManifestStore store = mock(TabletManifestStore.class);
    TabletManifestCache cache = new TabletManifestCache(store);

    // Should not throw
    cache.invalidate("non-existent-tablet");
    Assert.assertEquals(cache.getSize(), 0);
  }
}
