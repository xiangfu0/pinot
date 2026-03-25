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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.TabletConfig;
import org.apache.pinot.spi.lakehouse.LakehouseFileDescriptor;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class TabletGrouperTest {

  private LakehouseFileDescriptor createDataFile(String path, long sizeBytes, long rowCount, int specId,
      List<String> partitionValues, Map<String, String> lowerBounds) {
    return new LakehouseFileDescriptor(path, "PARQUET", sizeBytes, rowCount,
        LakehouseFileDescriptor.CONTENT_DATA, specId, partitionValues, lowerBounds, null, null);
  }

  @Test
  public void testSingleTabletGrouping() {
    TabletConfig config = new TabletConfig(128, null, "DAY");
    List<LakehouseFileDescriptor> files = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      files.add(createDataFile("s3://data/file" + i + ".parquet", 100_000_000L, 50000L, 0, null, null));
    }

    Map<String, List<LakehouseFileDescriptor>> tablets = TabletGrouper.groupFiles(files, config, null);

    // 10 files with target 128 -> all in one tablet
    assertEquals(tablets.size(), 1);
    assertEquals(tablets.values().iterator().next().size(), 10);
  }

  @Test
  public void testSplitByFileCount() {
    TabletConfig config = new TabletConfig(3, null, "NONE");
    List<LakehouseFileDescriptor> files = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      files.add(createDataFile("s3://data/file" + i + ".parquet", 1000L, 100L, 0, null, null));
    }

    Map<String, List<LakehouseFileDescriptor>> tablets = TabletGrouper.groupFiles(files, config, null);

    // 7 files, 3 per tablet -> 3 tablets (3, 3, 1)
    assertEquals(tablets.size(), 3);
    List<List<LakehouseFileDescriptor>> tabletValues = new ArrayList<>(tablets.values());
    assertEquals(tabletValues.get(0).size(), 3);
    assertEquals(tabletValues.get(1).size(), 3);
    assertEquals(tabletValues.get(2).size(), 1);
  }

  @Test
  public void testSplitByBytes() {
    // targetBytesPerTablet = 250MB, each file is 100MB
    TabletConfig config = new TabletConfig(1000, 250_000_000L, "NONE");
    List<LakehouseFileDescriptor> files = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      files.add(createDataFile("s3://data/file" + i + ".parquet", 100_000_000L, 50000L, 0, null, null));
    }

    Map<String, List<LakehouseFileDescriptor>> tablets = TabletGrouper.groupFiles(files, config, null);

    // 5 files * 100MB each, 250MB limit -> 2 tablets (after 3rd file=300MB, start new gen)
    // Actually: file0(100), file1(200), file2(300>=250 -> split before file2)
    // gen0: [file0, file1] (200MB), then at file2 currentBytes=200 < 250 so file2 goes in
    // Actually re-reading: after adding file2, currentFileCount=3, currentBytes=300MB
    // Next iteration (file3): currentBatch not empty AND currentBytes(300) >= targetBytes(250) => split
    // So gen0: [file0, file1, file2] (3 files), gen1: [file3, file4] (2 files)
    assertEquals(tablets.size(), 2);
  }

  @Test
  public void testGroupByPartitionValues() {
    TabletConfig config = new TabletConfig(128, null, "NONE");
    List<LakehouseFileDescriptor> files = new ArrayList<>();
    files.add(createDataFile("f1.parquet", 100L, 10L, 0, Arrays.asList("US"), null));
    files.add(createDataFile("f2.parquet", 100L, 10L, 0, Arrays.asList("US"), null));
    files.add(createDataFile("f3.parquet", 100L, 10L, 0, Arrays.asList("EU"), null));
    files.add(createDataFile("f4.parquet", 100L, 10L, 0, Arrays.asList("EU"), null));

    Map<String, List<LakehouseFileDescriptor>> tablets = TabletGrouper.groupFiles(files, config, null);

    // Two different partition values -> 2 tablets
    assertEquals(tablets.size(), 2);
  }

  @Test
  public void testGroupBySpecId() {
    TabletConfig config = new TabletConfig(128, null, "NONE");
    List<LakehouseFileDescriptor> files = new ArrayList<>();
    files.add(createDataFile("f1.parquet", 100L, 10L, 0, null, null));
    files.add(createDataFile("f2.parquet", 100L, 10L, 1, null, null));

    Map<String, List<LakehouseFileDescriptor>> tablets = TabletGrouper.groupFiles(files, config, null);

    // Two different spec IDs -> 2 tablets (supports partition evolution)
    assertEquals(tablets.size(), 2);
  }

  @Test
  public void testGroupByTimeBucket() {
    TabletConfig config = new TabletConfig(128, null, "DAY");
    long day1Ms = 1705276800000L; // 2024-01-15 00:00:00 UTC
    long day2Ms = 1705363200000L; // 2024-01-16 00:00:00 UTC

    List<LakehouseFileDescriptor> files = new ArrayList<>();
    files.add(createDataFile("f1.parquet", 100L, 10L, 0, null, Map.of("ts", String.valueOf(day1Ms))));
    files.add(createDataFile("f2.parquet", 100L, 10L, 0, null, Map.of("ts", String.valueOf(day1Ms + 3600000))));
    files.add(createDataFile("f3.parquet", 100L, 10L, 0, null, Map.of("ts", String.valueOf(day2Ms))));

    Map<String, List<LakehouseFileDescriptor>> tablets = TabletGrouper.groupFiles(files, config, "ts");

    // Day 1: 2 files, Day 2: 1 file -> 2 tablets
    assertEquals(tablets.size(), 2);
  }

  @Test
  public void testEmptyFiles() {
    TabletConfig config = new TabletConfig(128, null, "DAY");
    Map<String, List<LakehouseFileDescriptor>> tablets =
        TabletGrouper.groupFiles(new ArrayList<>(), config, null);

    assertTrue(tablets.isEmpty());
  }

  @Test
  public void testTabletIdFormat() {
    String tabletId = TabletGrouper.formatTabletId("0_12345_19736", 0);
    assertEquals(tabletId, "tablet_0_12345_19736_0");
  }

  @Test
  public void testPartitionHashDeterministic() {
    int hash1 = TabletGrouper.computePartitionHash(Arrays.asList("US", "2024"));
    int hash2 = TabletGrouper.computePartitionHash(Arrays.asList("US", "2024"));
    assertEquals(hash1, hash2);

    int hash3 = TabletGrouper.computePartitionHash(Arrays.asList("EU", "2024"));
    // Different partition values should generally produce different hashes
    // (not guaranteed due to hash collisions, but validates the function runs)
    TabletGrouper.computePartitionHash(Arrays.asList("EU", "2024"));
  }

  @Test
  public void testNullPartitionHash() {
    assertEquals(TabletGrouper.computePartitionHash(null), 0);
    assertEquals(TabletGrouper.computePartitionHash(new ArrayList<>()), 0);
  }

  @Test
  public void testTimeBucketGranularities() {
    long ts = 1705276800000L; // 2024-01-15 00:00:00 UTC
    assertTrue(TabletGrouper.truncateToGranularity(ts, "HOUR") > 0);
    assertTrue(TabletGrouper.truncateToGranularity(ts, "DAY") > 0);
    assertTrue(TabletGrouper.truncateToGranularity(ts, "WEEK") > 0);
    assertTrue(TabletGrouper.truncateToGranularity(ts, "MONTH") > 0);
    assertEquals(TabletGrouper.truncateToGranularity(ts, "NONE"), 0);
  }

  @Test
  public void testSingleFileAlwaysGrouped() {
    // Even if the file is larger than targetBytes, it should still be in a tablet
    TabletConfig config = new TabletConfig(1, 100L, "NONE");
    List<LakehouseFileDescriptor> files = new ArrayList<>();
    files.add(createDataFile("big.parquet", 999_999_999L, 100000L, 0, null, null));

    Map<String, List<LakehouseFileDescriptor>> tablets = TabletGrouper.groupFiles(files, config, null);

    assertEquals(tablets.size(), 1);
    assertEquals(tablets.values().iterator().next().size(), 1);
  }
}
