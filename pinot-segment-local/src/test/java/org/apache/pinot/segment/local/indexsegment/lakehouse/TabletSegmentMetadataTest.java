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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.lakehouse.MicrosegmentDescriptor;
import org.apache.pinot.common.metadata.lakehouse.TabletManifest;
import org.apache.pinot.common.metadata.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests for {@link TabletSegmentMetadata}.
 */
public class TabletSegmentMetadataTest {

  private static final String TABLET_ID = "tablet-001";
  private static final String TABLE_NAME_WITH_TYPE = "myTable_OFFLINE";
  private static final long SNAPSHOT_ID = 42L;
  private static final long MIN_TIME_MS = 1000000L;
  private static final long MAX_TIME_MS = 2000000L;
  private static final long ROW_COUNT = 500L;
  private static final long CREATED_AT_MS = 1700000000000L;

  private TabletMetadataEnvelope _envelope;
  private TabletManifest _manifest;
  private Schema _schema;
  private TabletSegmentMetadata _metadata;

  @BeforeMethod
  public void setUp() {
    _envelope = new TabletMetadataEnvelope(
        TABLET_ID, TABLE_NAME_WITH_TYPE, 1, SNAPSHOT_ID, 0,
        Collections.singletonList("2024-01-01"),
        MIN_TIME_MS, MAX_TIME_MS, ROW_COUNT, 10240L, 3,
        "s3://bucket/manifests/tablet-001.json");

    MicrosegmentDescriptor ms1 = new MicrosegmentDescriptor(
        "ms-001", "s3://bucket/data/file1.parquet", "PARQUET",
        4096L, 200L, MIN_TIME_MS, 1500000L,
        null, null, null, 0);
    MicrosegmentDescriptor ms2 = new MicrosegmentDescriptor(
        "ms-002", "s3://bucket/data/file2.parquet", "PARQUET",
        4096L, 300L, 1500001L, MAX_TIME_MS,
        null, null, null, 0);

    _manifest = new TabletManifest(
        TABLET_ID, TabletManifest.CURRENT_VERSION, "iceberg-uuid-123",
        SNAPSHOT_ID, 0,
        java.util.Arrays.asList(ms1, ms2),
        null, null, 0L, CREATED_AT_MS);

    _schema = new Schema.SchemaBuilder()
        .setSchemaName("myTable")
        .addSingleValueDimension("city", DataType.STRING)
        .addSingleValueDimension("country", DataType.STRING)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    _metadata = new TabletSegmentMetadata(_envelope, _manifest, _schema);
  }

  @Test
  public void testGetNameReturnsTabletId() {
    Assert.assertEquals(_metadata.getName(), TABLET_ID);
  }

  @Test
  public void testGetTableNameReturnsRawTableName() {
    Assert.assertEquals(_metadata.getTableName(), "myTable");
  }

  @Test
  public void testGetTotalDocsReturnsRowCount() {
    Assert.assertEquals(_metadata.getTotalDocs(), 500);
  }

  @Test
  public void testGetTotalDocsClampedToIntMaxValue() {
    // Create a manifest with a huge row count
    MicrosegmentDescriptor bigMs = new MicrosegmentDescriptor(
        "ms-big", "s3://bucket/data/big.parquet", "PARQUET",
        1024L, (long) Integer.MAX_VALUE + 100L, MIN_TIME_MS, MAX_TIME_MS,
        null, null, null, 0);
    TabletManifest bigManifest = new TabletManifest(
        TABLET_ID, 1, null, SNAPSHOT_ID, 0,
        Collections.singletonList(bigMs),
        null, null, 0L, CREATED_AT_MS);

    TabletSegmentMetadata bigMetadata = new TabletSegmentMetadata(_envelope, bigManifest, _schema);
    Assert.assertEquals(bigMetadata.getTotalDocs(), Integer.MAX_VALUE);
  }

  @Test
  public void testTimeBoundsFromEnvelope() {
    Assert.assertEquals(_metadata.getStartTime(), MIN_TIME_MS);
    Assert.assertEquals(_metadata.getEndTime(), MAX_TIME_MS);
    Assert.assertEquals(_metadata.getTimeUnit(), TimeUnit.MILLISECONDS);
  }

  @Test
  public void testTimeInterval() {
    Interval interval = _metadata.getTimeInterval();
    Assert.assertNotNull(interval);
    Assert.assertEquals(interval.getStartMillis(), MIN_TIME_MS);
    Assert.assertEquals(interval.getEndMillis(), MAX_TIME_MS);
  }

  @Test
  public void testTimeIntervalNullWhenTimeBoundsInvalid() {
    TabletMetadataEnvelope noTimeEnvelope = new TabletMetadataEnvelope(
        TABLET_ID, TABLE_NAME_WITH_TYPE, 1, SNAPSHOT_ID, 0,
        null, 0L, 0L, ROW_COUNT, 10240L, 3,
        "s3://bucket/manifests/tablet-001.json");

    TabletSegmentMetadata noTimeMetadata = new TabletSegmentMetadata(noTimeEnvelope, _manifest, _schema);
    Assert.assertNull(noTimeMetadata.getTimeInterval());
  }

  @Test
  public void testGetCrcReturnsSnapshotId() {
    Assert.assertEquals(_metadata.getCrc(), String.valueOf(SNAPSHOT_ID));
  }

  @Test
  public void testGetVersionReturnsV3() {
    Assert.assertEquals(_metadata.getVersion(), SegmentVersion.v3);
  }

  @Test
  public void testGetSchema() {
    Schema schema = _metadata.getSchema();
    Assert.assertNotNull(schema);
    Assert.assertTrue(schema.getColumnNames().contains("city"));
    Assert.assertTrue(schema.getColumnNames().contains("country"));
    Assert.assertTrue(schema.getColumnNames().contains("ts"));
  }

  @Test
  public void testGetIndexDirReturnsNull() {
    Assert.assertNull(_metadata.getIndexDir());
  }

  @Test
  public void testGetCreatorName() {
    Assert.assertEquals(_metadata.getCreatorName(), "lakehouse-tablet");
  }

  @Test
  public void testGetIndexCreationTime() {
    Assert.assertEquals(_metadata.getIndexCreationTime(), CREATED_AT_MS);
  }

  @Test
  public void testStarTreeMetadataListIsNull() {
    Assert.assertNull(_metadata.getStarTreeV2MetadataList());
  }

  @Test
  public void testCustomMapIsEmpty() {
    Assert.assertTrue(_metadata.getCustomMap().isEmpty());
  }

  @Test
  public void testColumnMetadataMapIsEmpty() {
    Assert.assertTrue(_metadata.getColumnMetadataMap().isEmpty());
  }

  @Test
  public void testGetTimeColumn() {
    Assert.assertEquals(_metadata.getTimeColumn(), "ts");
  }

  @Test
  public void testGetTimeColumnNullWhenNoTimeField() {
    Schema noTimeSchema = new Schema.SchemaBuilder()
        .setSchemaName("noTime")
        .addSingleValueDimension("dim", DataType.STRING)
        .build();
    TabletSegmentMetadata noTimeMeta = new TabletSegmentMetadata(_envelope, _manifest, noTimeSchema);
    Assert.assertNull(noTimeMeta.getTimeColumn());
  }

  @Test
  public void testToJson() {
    JsonNode json = _metadata.toJson(null);
    Assert.assertNotNull(json);
    Assert.assertEquals(json.get("segmentName").asText(), TABLET_ID);
    Assert.assertEquals(json.get("tabletId").asText(), TABLET_ID);
    Assert.assertEquals(json.get("snapshotId").asLong(), SNAPSHOT_ID);
    Assert.assertEquals(json.get("totalDocs").asInt(), 500);
    Assert.assertEquals(json.get("startTimeMs").asLong(), MIN_TIME_MS);
    Assert.assertEquals(json.get("endTimeMs").asLong(), MAX_TIME_MS);
  }

  @Test
  public void testGetManifestAndEnvelope() {
    Assert.assertSame(_metadata.getManifest(), _manifest);
    Assert.assertSame(_metadata.getEnvelope(), _envelope);
  }

  @Test
  public void testOffsets() {
    Assert.assertNull(_metadata.getStartOffset());
    Assert.assertNull(_metadata.getEndOffset());
  }

  @Test
  public void testIngestionTimestamps() {
    Assert.assertEquals(_metadata.getLastIndexedTimestamp(), Long.MIN_VALUE);
    Assert.assertEquals(_metadata.getLatestIngestionTimestamp(), Long.MIN_VALUE);
    Assert.assertEquals(_metadata.getMinimumIngestionLagMs(), Long.MAX_VALUE);
  }

  @Test
  public void testRemoveColumnIsNoOp() {
    // Should not throw
    _metadata.removeColumn("nonExistent");
  }

  @Test
  public void testGetDataCrc() {
    Assert.assertEquals(_metadata.getDataCrc(), String.valueOf(Long.MIN_VALUE));
  }

  @Test
  public void testGetTimeGranularity() {
    Assert.assertNotNull(_metadata.getTimeGranularity());
    Assert.assertEquals(_metadata.getTimeGranularity().getMillis(), 1L);
  }
}
