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
package org.apache.pinot.integration.tests.custom;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for HLL and HLL_PLUS as first-class Pinot data types.
 *
 * <p>Each row pre-aggregates a randomly chosen integer into an HLL sketch. This mirrors
 * the typical pre-aggregated sketch pattern where the sketch column is a metric.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class HllTypeTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "HllTypeTest";
  private static final String ID = "id";
  private static final String HLL_COL = "hllCol";
  private static final String HLL_PLUS_COL = "hllPlusCol";

  private static final int NUM_DOCS = 1000;
  private static final int LOG2M = 8;

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(getTableName())
        .addSingleValueDimension(ID, FieldSpec.DataType.INT)
        .addMetric(HLL_COL, FieldSpec.DataType.HLL)
        .addMetric(HLL_PLUS_COL, FieldSpec.DataType.HLL_PLUS)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(ID,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null),
        new org.apache.avro.Schema.Field(HLL_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES), null, null),
        new org.apache.avro.Schema.Field(HLL_PLUS_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES), null, null)));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < NUM_DOCS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        int value = RANDOM.nextInt(100);
        record.put(ID, value);
        record.put(HLL_COL, ByteBuffer.wrap(buildHllBytes(value)));
        record.put(HLL_PLUS_COL, ByteBuffer.wrap(buildHllPlusBytes(value)));
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  private static byte[] buildHllBytes(int value) {
    try {
      HyperLogLog hll = new HyperLogLog(LOG2M);
      hll.offer(value);
      return ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(hll);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static byte[] buildHllPlusBytes(int value) {
    try {
      // sp=0 means no sparse representation (same default as Pinot aggregation functions)
      HyperLogLogPlus hllPlus = new HyperLogLogPlus(LOG2M, 0);
      hllPlus.offer(value);
      return ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.serialize(hllPlus);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testDistinctCountHll(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT distinctCountHLL(%s) FROM %s", HLL_COL, getTableName());
    JsonNode result = postQuery(query);
    long count = result.get("resultTable").get("rows").get(0).get(0).asLong();
    // 1000 rows each carrying one of 100 distinct values — merged HLL should report ~100
    assertTrue(count >= 50 && count <= 200,
        "distinctCountHLL estimate should be between 50 and 200 for 100 distinct values, got " + count);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testDistinctCountHllPlus(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT distinctCountHLLPlus(%s) FROM %s", HLL_PLUS_COL, getTableName());
    JsonNode result = postQuery(query);
    long count = result.get("resultTable").get("rows").get(0).get(0).asLong();
    assertTrue(count >= 50 && count <= 200,
        "distinctCountHLLPlus estimate should be between 50 and 200 for 100 distinct values, got " + count);
  }

  @Test(dataProvider = "useV1QueryEngine")
  public void testRawHll(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT distinctCountHLL(%s), distinctCountRawHLL(%s) FROM %s",
        HLL_COL, HLL_COL, getTableName());
    JsonNode result = postQuery(query);
    long distinctCount = result.get("resultTable").get("rows").get(0).get(0).asLong();
    // Raw result is a hex-encoded serialized HLL
    String rawHex = result.get("resultTable").get("rows").get(0).get(1).asText();
    byte[] rawBytes = Hex.decodeHex(rawHex);
    HyperLogLog mergedHll = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(rawBytes);
    assertEquals((long) mergedHll.cardinality(), distinctCount,
        "Raw HLL cardinality should match distinctCountHLL result");
  }

  @Test(dataProvider = "useV1QueryEngine")
  public void testRawHllPlus(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT distinctCountHLLPlus(%s), distinctCountRawHLLPlus(%s) FROM %s",
        HLL_PLUS_COL, HLL_PLUS_COL, getTableName());
    JsonNode result = postQuery(query);
    long distinctCount = result.get("resultTable").get("rows").get(0).get(0).asLong();
    // Raw result is a hex-encoded serialized HLL+
    String rawHex = result.get("resultTable").get("rows").get(0).get(1).asText();
    byte[] rawBytes = Hex.decodeHex(rawHex);
    HyperLogLogPlus mergedHllPlus = ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.deserialize(rawBytes);
    assertEquals((long) mergedHllPlus.cardinality(), distinctCount,
        "Raw HLL+ cardinality should match distinctCountHLLPlus result");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGroupByWithHll(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Group by id % 10 (10 groups) and compute HLL — each group should have a positive estimate
    String query = String.format(
        "SELECT %s %% 10, distinctCountHLL(%s) FROM %s GROUP BY %s %% 10 ORDER BY %s %% 10",
        ID, HLL_COL, getTableName(), ID, ID);
    JsonNode result = postQuery(query);
    JsonNode rows = result.get("resultTable").get("rows");
    assertEquals(rows.size(), 10, "Expected 10 groups (id %% 10)");
    for (int i = 0; i < rows.size(); i++) {
      long count = rows.get(i).get(1).asLong();
      assertTrue(count > 0, "Each group should have a positive HLL estimate");
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCountStarWithHllColumns(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT COUNT(*) FROM %s", getTableName());
    JsonNode result = postQuery(query);
    long count = result.get("resultTable").get("rows").get(0).get(0).asLong();
    assertEquals(count, NUM_DOCS, "COUNT(*) should equal number of ingested docs");
  }
}
