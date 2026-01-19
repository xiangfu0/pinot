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
package org.apache.pinot.broker.routing.tablesampler;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.helix.FakePropertyStore;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.sampler.TableSamplerConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TimeBucketSegmentsTableSamplerTest {

  @Test
  public void testSelectNSegmentsPerDayDefaultBucketDaysOne() {
    String tableNameWithType = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();

    // Two days, two segments per day. We want 1 per bucket (bucketDays=1 by default).
    String segDay0A = "segment_0_A";
    String segDay0B = "segment_0_B";
    String segDay1A = "segment_1_A";
    String segDay1B = "segment_1_B";

    // 2020-01-01T00:00:00Z and 2020-01-02T00:00:00Z
    long day0Ms = 1577836800000L;
    long day1Ms = 1577923200000L;

    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segDay0A, day0Ms);
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segDay0B, day0Ms + TimeUnit.HOURS.toMillis(1));
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segDay1A, day1Ms);
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segDay1B, day1Ms + TimeUnit.HOURS.toMillis(2));

    TimeBucketSegmentsTableSampler sampler = new TimeBucketSegmentsTableSampler();
    sampler.init(tableNameWithType, new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build(),
        new TableSamplerConfig("perBucket", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1")),
        propertyStore);

    Set<String> selected = sampler.selectSegments(Set.of(segDay0A, segDay0B, segDay1A, segDay1B));
    Assert.assertEquals(selected, Set.of(segDay0A, segDay1A));
  }

  @Test
  public void testBucketDaysTwo() {
    String tableNameWithType = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();

    // 2 days fall into 1 bucket when bucketDays=2, so we should select 1 segment total.
    String segDay0A = "segment_0_A";
    String segDay1A = "segment_1_A";

    long day0Ms = 1577836800000L;
    long day1Ms = 1577923200000L;

    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segDay0A, day0Ms);
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segDay1A, day1Ms);

    TimeBucketSegmentsTableSampler sampler = new TimeBucketSegmentsTableSampler();
    sampler.init(tableNameWithType, new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build(),
        new TableSamplerConfig("perBucket", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1",
                TimeBucketSegmentsTableSampler.PROP_BUCKET_DAYS, "2")),
        propertyStore);

    Set<String> selected = sampler.selectSegments(Set.of(segDay0A, segDay1A));
    Assert.assertEquals(selected, Set.of(segDay0A));
  }

  private static void writeSegmentEndTimeMs(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType,
      String segmentName, long endTimeMs) {
    SegmentZKMetadata zkMetadata = new SegmentZKMetadata(segmentName);
    zkMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
    zkMetadata.setEndTime(endTimeMs);
    String path = ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, segmentName);
    propertyStore.set(path, zkMetadata.toZNRecord(), AccessOption.PERSISTENT);
  }
}
