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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Integration test for the codec pipeline forward index (version 7).
 *
 * <p>Builds an offline table where every supported single-codec and multi-stage codec spec is
 * applied to its own INT and LONG raw column. All columns are populated with identical values
 * (intVal = i, longVal = i * 1_000_000_000L), so every codec must read back the same aggregates,
 * filter counts, and point lookups. A STRING dictionary column verifies that codec-pipeline raw
 * columns and dictionary-encoded columns coexist in the same segment.
 *
 * <p>Codec specs covered (each as its own column):
 * <ul>
 *   <li>{@code LZ4}, {@code ZSTD(3)}, {@code SNAPPY}, {@code GZIP} — single-stage compression</li>
 *   <li>{@code CODEC(DELTA,LZ4)}, {@code CODEC(DELTA,ZSTD(3))} — DELTA transform + compression</li>
 *   <li>{@code CODEC(DELTADELTA,LZ4)} — second-order DELTA transform + compression</li>
 * </ul>
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class CodecPipelineIntegrationTest extends CustomDataQueryClusterIntegrationTest {

  private static final String TABLE_NAME = "CodecPipelineIntegrationTest";
  private static final int NUM_DOCS = 1000;

  private static final String STR_COL = "strVal";
  // STRING column with RAW encoding + legacy ZSTANDARD compressionCodec (codecSpec doesn't yet
  // cover STRING in v1, so legacy must coexist with the codec-pipeline INT/LONG columns).
  private static final String STR_RAW_COL = "strRawZstd";
  private static final String TIME_COL = "ts";

  // Expected aggregates: SUM(0..999) = 499_500
  private static final long EXPECTED_INT_SUM = 499_500L;
  private static final long EXPECTED_LONG_SUM = 499_500L * 1_000_000_000L;

  /**
   * Codec spec → column-name suffix. Each codec spec gets its own INT and LONG column
   * ({@code int<suffix>} / {@code long<suffix>}). Order matters only for diagnostic output.
   * LinkedHashMap preserves declaration order so the data provider is stable.
   */
  private static final Map<String, String> CODEC_SPECS;
  static {
    Map<String, String> m = new LinkedHashMap<>();
    m.put("LZ4", "Lz4");
    m.put("ZSTD(3)", "Zstd");
    m.put("SNAPPY", "Snappy");
    m.put("GZIP", "Gzip");
    m.put("CODEC(DELTA,LZ4)", "DeltaLz4");
    m.put("CODEC(DELTA,ZSTD(3))", "DeltaZstd");
    m.put("CODEC(DELTADELTA,LZ4)", "DeltadeltaLz4");
    CODEC_SPECS = m;
  }

  private static String intColFor(String suffix) {
    return "int" + suffix;
  }

  private static String longColFor(String suffix) {
    return "long" + suffix;
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    Schema.SchemaBuilder builder = new Schema.SchemaBuilder().setSchemaName(getTableName());
    for (String suffix : CODEC_SPECS.values()) {
      builder.addMetric(intColFor(suffix), FieldSpec.DataType.INT);
      builder.addMetric(longColFor(suffix), FieldSpec.DataType.LONG);
    }
    builder.addSingleValueDimension(STR_COL, FieldSpec.DataType.STRING);
    builder.addSingleValueDimension(STR_RAW_COL, FieldSpec.DataType.STRING);
    builder.addDateTimeField(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS");
    return builder.build();
  }

  @Override
  public List<File> createAvroFiles()
      throws IOException {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("codecRecord", null, null, false);
    List<Field> fields = new ArrayList<>();
    for (String suffix : CODEC_SPECS.values()) {
      fields.add(new Field(intColFor(suffix), org.apache.avro.Schema.create(Type.INT), null, null));
      fields.add(new Field(longColFor(suffix), org.apache.avro.Schema.create(Type.LONG), null, null));
    }
    fields.add(new Field(STR_COL, org.apache.avro.Schema.create(Type.STRING), null, null));
    fields.add(new Field(STR_RAW_COL, org.apache.avro.Schema.create(Type.STRING), null, null));
    fields.add(new Field(TIME_COL, org.apache.avro.Schema.create(Type.LONG), null, null));
    avroSchema.setFields(fields);

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < NUM_DOCS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        for (String suffix : CODEC_SPECS.values()) {
          record.put(intColFor(suffix), i);
          record.put(longColFor(suffix), (long) i * 1_000_000_000L);
        }
        record.put(STR_COL, "str_" + i);
        record.put(STR_RAW_COL, "rawstr_" + i);
        record.put(TIME_COL, (long) i);
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Override
  public String getTimeColumnName() {
    return TIME_COL;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setNoDictionaryColumns(getNoDictionaryColumns())
        .setFieldConfigList(getFieldConfigs())
        .build();
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    // STR_COL uses a dictionary (default), so it is intentionally NOT in this list.
    // STR_RAW_COL uses RAW encoding with legacy ZSTANDARD compressionCodec.
    List<String> noDict = new ArrayList<>();
    for (String suffix : CODEC_SPECS.values()) {
      noDict.add(intColFor(suffix));
      noDict.add(longColFor(suffix));
    }
    noDict.add(STR_RAW_COL);
    return noDict;
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    for (Map.Entry<String, String> entry : CODEC_SPECS.entrySet()) {
      String spec = entry.getKey();
      String suffix = entry.getValue();
      fieldConfigs.add(new FieldConfig.Builder(intColFor(suffix))
          .withEncodingType(FieldConfig.EncodingType.RAW)
          .withCodecSpec(spec)
          .build());
      fieldConfigs.add(new FieldConfig.Builder(longColFor(suffix))
          .withEncodingType(FieldConfig.EncodingType.RAW)
          .withCodecSpec(spec)
          .build());
    }
    // STR_COL with dictionary encoding — verifies codec-pipeline and dict columns coexist.
    fieldConfigs.add(new FieldConfig.Builder(STR_COL)
        .withEncodingType(FieldConfig.EncodingType.DICTIONARY)
        .build());
    // STR_RAW_COL with RAW + legacy ZSTANDARD compressionCodec — verifies the legacy raw path
    // (which codecSpec doesn't yet cover for STRING) still works alongside codec-pipeline columns.
    fieldConfigs.add(new FieldConfig.Builder(STR_RAW_COL)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.ZSTANDARD)
        .build());
    return fieldConfigs;
  }

  @Nullable
  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getInvertedIndexColumns() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getRangeIndexColumns() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getBloomFilterColumns() {
    return null;
  }

  /**
   * Cartesian product of (codec spec, query engine) so every codec is exercised on both engines.
   */
  @DataProvider(name = "codecAndEngine")
  public Object[][] codecAndEngine() {
    List<Object[]> rows = new ArrayList<>(CODEC_SPECS.size() * 2);
    for (Map.Entry<String, String> entry : CODEC_SPECS.entrySet()) {
      String spec = entry.getKey();
      String suffix = entry.getValue();
      rows.add(new Object[]{spec, suffix, false});
      rows.add(new Object[]{spec, suffix, true});
    }
    return rows.toArray(new Object[0][]);
  }

  @Test(dataProvider = "codecAndEngine")
  public void testSumPerCodec(String codecSpec, String suffix, boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String intCol = intColFor(suffix);
    String longCol = longColFor(suffix);

    JsonNode intSum = postQuery("SELECT SUM(" + intCol + ") FROM " + getTableName());
    assertEquals(intSum.get("resultTable").get("rows").get(0).get(0).asLong(), EXPECTED_INT_SUM,
        "Unexpected SUM(" + intCol + ") for codec " + codecSpec);

    JsonNode longSum = postQuery("SELECT SUM(" + longCol + ") FROM " + getTableName());
    assertEquals(longSum.get("resultTable").get("rows").get(0).get(0).asLong(), EXPECTED_LONG_SUM,
        "Unexpected SUM(" + longCol + ") for codec " + codecSpec);
  }

  @Test(dataProvider = "codecAndEngine")
  public void testFilterPerCodec(String codecSpec, String suffix, boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String intCol = intColFor(suffix);
    String longCol = longColFor(suffix);

    // intVal < 100 → 100 rows (values 0..99)
    JsonNode intFilter = postQuery("SELECT COUNT(*) FROM " + getTableName() + " WHERE " + intCol + " < 100");
    assertEquals(intFilter.get("resultTable").get("rows").get(0).get(0).asLong(), 100L,
        "Unexpected count for " + intCol + " < 100, codec " + codecSpec);

    // longVal < 100_000_000_000L → 100 rows
    JsonNode longFilter =
        postQuery("SELECT COUNT(*) FROM " + getTableName() + " WHERE " + longCol + " < 100000000000");
    assertEquals(longFilter.get("resultTable").get("rows").get(0).get(0).asLong(), 100L,
        "Unexpected count for " + longCol + " < 100B, codec " + codecSpec);
  }

  /**
   * Per-codec point lookups across multiple chunk boundaries. Aggregate queries can mask per-doc
   * decoding errors that average out — point lookups force the reader to materialize specific
   * values, including chunk-boundary docs.
   */
  @Test(dataProvider = "codecAndEngine")
  public void testPointLookupsPerCodec(String codecSpec, String suffix, boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String intCol = intColFor(suffix);
    String longCol = longColFor(suffix);

    int[] spotCheckIds = {0, 1, 511, 512, 513, 999};
    for (int id : spotCheckIds) {
      JsonNode intResult =
          postQuery("SELECT " + intCol + " FROM " + getTableName() + " WHERE " + TIME_COL + " = " + id);
      assertEquals(intResult.get("resultTable").get("rows").get(0).get(0).asInt(), id,
          "Wrong " + intCol + " for ts=" + id + ", codec " + codecSpec);

      JsonNode longResult =
          postQuery("SELECT " + longCol + " FROM " + getTableName() + " WHERE " + TIME_COL + " = " + id);
      assertEquals(longResult.get("resultTable").get("rows").get(0).get(0).asLong(), (long) id * 1_000_000_000L,
          "Wrong " + longCol + " for ts=" + id + ", codec " + codecSpec);
    }
  }

  /**
   * Verifies that a single SELECT touching multiple codec-encoded columns returns consistent values
   * across codecs in the same row — catches any chunk-state cross-talk between readers.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testCrossCodecConsistency(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    List<String> suffixes = new ArrayList<>(CODEC_SPECS.values());
    String selectList = String.join(", ", Stream.concat(
            suffixes.stream().map(CodecPipelineIntegrationTest::intColFor),
            suffixes.stream().map(CodecPipelineIntegrationTest::longColFor))
        .toArray(String[]::new));

    int[] spotCheckIds = {0, 511, 512, 999};
    for (int id : spotCheckIds) {
      JsonNode result = postQuery("SELECT " + selectList + " FROM " + getTableName() + " WHERE ts = " + id);
      JsonNode row = result.get("resultTable").get("rows").get(0);
      // First N columns are int; next N are long.
      for (int i = 0; i < suffixes.size(); i++) {
        assertEquals(row.get(i).asInt(), id,
            "Cross-codec int mismatch at suffix " + suffixes.get(i) + " for ts=" + id);
      }
      for (int i = 0; i < suffixes.size(); i++) {
        assertEquals(row.get(suffixes.size() + i).asLong(), (long) id * 1_000_000_000L,
            "Cross-codec long mismatch at suffix " + suffixes.get(i) + " for ts=" + id);
      }
    }
  }

  /**
   * Verifies that a STRING column stored with dictionary encoding (not codec pipeline) reads back
   * correctly alongside codec-pipeline columns, confirming both can coexist in the same segment.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testStringColumnWithDictEncoding(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    int[] spotCheckIds = {0, 42, 500, 999};
    for (int id : spotCheckIds) {
      JsonNode result = postQuery("SELECT strVal FROM " + getTableName() + " WHERE ts = " + id);
      assertEquals(result.get("resultTable").get("rows").get(0).get(0).asText(), "str_" + id,
          "Wrong strVal for ts=" + id);
    }

    JsonNode countDistinctResult = postQuery("SELECT COUNT(DISTINCT strVal) FROM " + getTableName());
    assertEquals(countDistinctResult.get("resultTable").get("rows").get(0).get(0).asLong(), NUM_DOCS,
        "Expected all " + NUM_DOCS + " distinct string values");
  }

  /**
   * Verifies that a STRING column stored RAW with a legacy {@code compressionCodec=ZSTANDARD}
   * (which {@code codecSpec} does not yet cover for STRING) reads back correctly alongside the
   * codec-pipeline INT/LONG columns. This exercises the legacy chunk forward-index path within
   * a segment that also contains V7 codec-pipeline columns.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testStringColumnWithRawLegacyCompression(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    int[] spotCheckIds = {0, 42, 500, 999};
    for (int id : spotCheckIds) {
      JsonNode result =
          postQuery("SELECT " + STR_RAW_COL + " FROM " + getTableName() + " WHERE ts = " + id);
      assertEquals(result.get("resultTable").get("rows").get(0).get(0).asText(), "rawstr_" + id,
          "Wrong " + STR_RAW_COL + " for ts=" + id);
    }

    JsonNode countDistinctResult =
        postQuery("SELECT COUNT(DISTINCT " + STR_RAW_COL + ") FROM " + getTableName());
    assertEquals(countDistinctResult.get("resultTable").get("rows").get(0).get(0).asLong(), NUM_DOCS,
        "Expected all " + NUM_DOCS + " distinct " + STR_RAW_COL + " values");
  }

  /** Verify COUNT(*) reads through every codec column path consistently via a join-style test. */
  @Test(dataProvider = "useBothQueryEngines")
  public void testCountAcrossCodecs(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // COUNT with no WHERE: should be NUM_DOCS regardless of column choice.
    JsonNode count = postQuery("SELECT COUNT(*) FROM " + getTableName());
    assertEquals(count.get("resultTable").get("rows").get(0).get(0).asLong(), NUM_DOCS,
        "Unexpected total row count");

    // COUNT WHERE intLz4 = intZstd (every row should match: same values across codecs).
    JsonNode crossCount = postQuery(
        "SELECT COUNT(*) FROM " + getTableName() + " WHERE intLz4 = intZstd AND longSnappy = longGzip");
    assertEquals(crossCount.get("resultTable").get("rows").get(0).get(0).asLong(), NUM_DOCS,
        "Cross-codec equality must hold for every row");
  }

  /**
   * Sanity check: the codec spec list stays in sync with the rest of the test setup.
   */
  @Test
  public void testAllCodecSpecsRegisteredInTableConfig() {
    List<String> expectedColumns = new ArrayList<>();
    for (String suffix : CODEC_SPECS.values()) {
      expectedColumns.add(intColFor(suffix));
      expectedColumns.add(longColFor(suffix));
    }
    List<String> noDict = getNoDictionaryColumns();
    assertEquals(noDict.size(), expectedColumns.size(),
        "noDictionaryColumns size must match the codec-spec matrix");
    for (String col : expectedColumns) {
      if (!noDict.contains(col)) {
        throw new AssertionError("Expected " + col + " in noDictionaryColumns; got " + noDict);
      }
    }
  }
}
