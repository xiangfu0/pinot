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
package org.apache.pinot.common.lakehouse;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link LakehouseQueryOptions}.
 */
public class LakehouseQueryOptionsTest {

  @Test
  public void testFromQueryOptionsWithSnapshotId() {
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put(QueryOptionKey.ICEBERG_SNAPSHOT_ID, "12345");

    LakehouseQueryOptions opts = LakehouseQueryOptions.fromQueryOptions(queryOptions);

    assertEquals(opts.getSnapshotId(), Long.valueOf(12345L));
    assertNull(opts.getBranch());
    assertNull(opts.getTag());
    assertNull(opts.getAsOfMs());
    assertTrue(opts.hasExplicitSnapshot());
  }

  @Test
  public void testFromQueryOptionsWithBranch() {
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put(QueryOptionKey.ICEBERG_BRANCH, "feature-branch");

    LakehouseQueryOptions opts = LakehouseQueryOptions.fromQueryOptions(queryOptions);

    assertNull(opts.getSnapshotId());
    assertEquals(opts.getBranch(), "feature-branch");
    assertNull(opts.getTag());
    assertNull(opts.getAsOfMs());
    assertTrue(opts.hasExplicitSnapshot());
  }

  @Test
  public void testFromQueryOptionsWithTag() {
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put(QueryOptionKey.ICEBERG_TAG, "v1.0");

    LakehouseQueryOptions opts = LakehouseQueryOptions.fromQueryOptions(queryOptions);

    assertNull(opts.getSnapshotId());
    assertNull(opts.getBranch());
    assertEquals(opts.getTag(), "v1.0");
    assertNull(opts.getAsOfMs());
    assertTrue(opts.hasExplicitSnapshot());
  }

  @Test
  public void testFromQueryOptionsWithAsOfMs() {
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put(QueryOptionKey.ICEBERG_AS_OF_MS, "1700000000000");

    LakehouseQueryOptions opts = LakehouseQueryOptions.fromQueryOptions(queryOptions);

    assertNull(opts.getSnapshotId());
    assertNull(opts.getBranch());
    assertNull(opts.getTag());
    assertEquals(opts.getAsOfMs(), Long.valueOf(1700000000000L));
    assertTrue(opts.hasExplicitSnapshot());
  }

  @Test
  public void testFromQueryOptionsWithAllOptions() {
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put(QueryOptionKey.ICEBERG_SNAPSHOT_ID, "99");
    queryOptions.put(QueryOptionKey.ICEBERG_BRANCH, "main");
    queryOptions.put(QueryOptionKey.ICEBERG_TAG, "release");
    queryOptions.put(QueryOptionKey.ICEBERG_AS_OF_MS, "1000");

    LakehouseQueryOptions opts = LakehouseQueryOptions.fromQueryOptions(queryOptions);

    assertEquals(opts.getSnapshotId(), Long.valueOf(99L));
    assertEquals(opts.getBranch(), "main");
    assertEquals(opts.getTag(), "release");
    assertEquals(opts.getAsOfMs(), Long.valueOf(1000L));
    assertTrue(opts.hasExplicitSnapshot());
  }

  @Test
  public void testDefaultsWhenNoOptionsSet() {
    Map<String, String> queryOptions = new HashMap<>();

    LakehouseQueryOptions opts = LakehouseQueryOptions.fromQueryOptions(queryOptions);

    assertNull(opts.getSnapshotId());
    assertNull(opts.getBranch());
    assertNull(opts.getTag());
    assertNull(opts.getAsOfMs());
    assertFalse(opts.hasExplicitSnapshot());
  }

  @Test
  public void testDefaultsWithUnrelatedOptions() {
    // Query options map contains other options but no lakehouse ones
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("timeoutMs", "30000");
    queryOptions.put("enableNullHandling", "true");

    LakehouseQueryOptions opts = LakehouseQueryOptions.fromQueryOptions(queryOptions);

    assertNull(opts.getSnapshotId());
    assertNull(opts.getBranch());
    assertNull(opts.getTag());
    assertNull(opts.getAsOfMs());
    assertFalse(opts.hasExplicitSnapshot());
  }

  @Test
  public void testDirectConstructor() {
    LakehouseQueryOptions opts = new LakehouseQueryOptions(42L, "dev", "v2", 5000L);

    assertEquals(opts.getSnapshotId(), Long.valueOf(42L));
    assertEquals(opts.getBranch(), "dev");
    assertEquals(opts.getTag(), "v2");
    assertEquals(opts.getAsOfMs(), Long.valueOf(5000L));
    assertTrue(opts.hasExplicitSnapshot());
  }

  @Test
  public void testDirectConstructorAllNulls() {
    LakehouseQueryOptions opts = new LakehouseQueryOptions(null, null, null, null);

    assertNull(opts.getSnapshotId());
    assertNull(opts.getBranch());
    assertNull(opts.getTag());
    assertNull(opts.getAsOfMs());
    assertFalse(opts.hasExplicitSnapshot());
  }
}
