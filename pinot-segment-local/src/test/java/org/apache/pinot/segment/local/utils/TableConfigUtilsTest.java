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
package org.apache.pinot.segment.local.utils;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.StarTreeAggregationConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class TableConfigUtilsTest {
  private static final String TABLE_NAME = "testTable";
  private static final String TIME_COLUMN = "timeColumn";
  private static final String PARTITION_COLUMN = "partitionColumn";

  @Test
  public void validateTimeColumnValidationConfig() {
    // REALTIME table

    // null timeColumnName and schema
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).build();
    try {
      TableConfigUtils.validate(tableConfig, null);
      fail("Should fail for null timeColumnName and null schema in REALTIME table");
    } catch (IllegalArgumentException e) {
      // expected
    }

    // null schema only
    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, null);
      fail("Should fail for null schema in REALTIME table");
    } catch (IllegalArgumentException e) {
      // expected
    }

    // null timeColumnName only
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).build();
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for null timeColumnName in REALTIME table");
    } catch (IllegalStateException e) {
      // expected
    }

    // timeColumnName not present in schema
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).build();
    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for timeColumnName not present in schema for REALTIME table");
    } catch (IllegalStateException e) {
      // expected
    }

    // timeColumnName not present as valid time spec schema
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(TIME_COLUMN, FieldSpec.DataType.LONG)
        .build();
    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid fieldSpec for timeColumnName in schema for REALTIME table");
    } catch (IllegalStateException e) {
      // expected
    }

    // valid
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setStreamConfigs(getStreamConfigs())
        .setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .build();
    TableConfigUtils.validate(tableConfig, schema);

    // OFFLINE table
    // null timeColumnName and schema - allowed in OFFLINE
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    try {
      TableConfigUtils.validate(tableConfig, null);
      fail("Should fail for null timeColumnName and null schema in OFFLINE table");
    } catch (IllegalArgumentException e) {
      // expected
    }

    // null schema only - allowed in OFFLINE
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, null);
      fail("Should fail for null schema in OFFLINE table");
    } catch (IllegalArgumentException e) {
      // expected
    }

    // null timeColumnName only - allowed in OFFLINE
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).build();
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    TableConfigUtils.validate(tableConfig, schema);

    // non-null schema and timeColumnName, but timeColumnName not present in schema
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).build();
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for timeColumnName not present in schema for OFFLINE table");
    } catch (IllegalStateException e) {
      // expected
    }

    // non-null schema and timeColumnName, but timeColumnName not present as a time spec in schema
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(TIME_COLUMN, FieldSpec.DataType.STRING)
        .build();
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for timeColumnName not present in schema for OFFLINE table");
    } catch (IllegalStateException e) {
      // expected
    }

    // valid
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    TableConfigUtils.validate(tableConfig, schema);
  }

  @Test
  public void validateDimensionTableConfig() {
    // dimension table with REALTIME type (should be OFFLINE)
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(TIME_COLUMN, FieldSpec.DataType.STRING)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setIsDimTable(true)
        .setTimeColumnName(TIME_COLUMN)
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail with a Dimension table of type REALTIME");
    } catch (IllegalStateException e) {
      // expected
    }

    // dimension table without a schema
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setIsDimTable(true)
        .setTimeColumnName(TIME_COLUMN)
        .build();
    try {
      TableConfigUtils.validate(tableConfig, null);
      fail("Should fail with a Dimension table without a schema");
    } catch (IllegalArgumentException e) {
      // expected
    }

    // dimension table without a Primary Key
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(TIME_COLUMN, FieldSpec.DataType.STRING)
        .build();
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setIsDimTable(true)
        .setTimeColumnName(TIME_COLUMN)
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail with a Dimension without a primary key");
    } catch (IllegalStateException e) {
      // expected
    }

    // valid dimension table
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .setPrimaryKeyColumns(Lists.newArrayList("myCol"))
        .build();
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIsDimTable(true).build();
    TableConfigUtils.validate(tableConfig, schema);
  }

  @Test
  public void validateIngestionConfig() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).build();
    // null ingestion config
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(null).build();
    TableConfigUtils.validate(tableConfig, schema);

    // null filter config, transform config
    IngestionConfig ingestionConfig = new IngestionConfig();
    tableConfig.setIngestionConfig(ingestionConfig);
    TableConfigUtils.validate(tableConfig, schema);

    // null filter function
    ingestionConfig.setFilterConfig(new FilterConfig(null));
    TableConfigUtils.validate(tableConfig, schema);

    // valid filterFunction
    ingestionConfig.setFilterConfig(new FilterConfig("startsWith(columnX, \"myPrefix\")"));
    TableConfigUtils.validate(tableConfig, schema);

    // valid filterFunction
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy({x == 10}, x)"));
    TableConfigUtils.validate(tableConfig, schema);

    // invalid filter function
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy(badExpr)"));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail on invalid filter function string");
    } catch (IllegalStateException e) {
      // expected
    }

    ingestionConfig.setFilterConfig(new FilterConfig("fakeFunction(xx)"));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid filter function");
    } catch (IllegalStateException e) {
      // expected
    }

    // empty transform configs
    ingestionConfig.setFilterConfig(null);
    ingestionConfig.setTransformConfigs(Collections.emptyList());
    TableConfigUtils.validate(tableConfig, schema);

    // transformed column not in schema
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig("myCol", "reverse(anotherCol)")));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for transformedColumn not present in schema");
    } catch (IllegalStateException e) {
      // expected
    }

    // using a transformation column in an aggregation
    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setNoDictionaryColumns(List.of("twiceSum"));
    tableConfig.setIndexingConfig(indexingConfig);
    schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addMetric("twiceSum", FieldSpec.DataType.DOUBLE).build();
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig("twice", "col * 2")));
    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("twiceSum", "SUM(twice)")));
    TableConfigUtils.validate(tableConfig, schema);

    // valid transform configs
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .build();
    indexingConfig.setNoDictionaryColumns(List.of("myCol"));
    ingestionConfig.setAggregationConfigs(null);
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig("myCol", "reverse(anotherCol)")));
    TableConfigUtils.validate(tableConfig, schema);

    // valid transform configs
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addMetric("transformedCol", FieldSpec.DataType.LONG)
        .build();
    ingestionConfig.setTransformConfigs(Arrays.asList(new TransformConfig("myCol", "reverse(anotherCol)"),
        new TransformConfig("transformedCol", "Groovy({x+y}, x, y)")));
    TableConfigUtils.validate(tableConfig, schema);

    // invalid transform config since Groovy is disabled
    try {
      TableConfigUtils.setDisableGroovy(true);
      TableConfigUtils.validate(tableConfig, schema);
      // Reset to false
      TableConfigUtils.setDisableGroovy(false);
      fail("Should fail when Groovy functions disabled but found in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // Using peer download scheme with replication of 1
    ingestionConfig.setTransformConfigs(null);
    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig =
        new SegmentsValidationAndRetentionConfig();
    segmentsValidationAndRetentionConfig.setReplication("1");
    segmentsValidationAndRetentionConfig.setPeerSegmentDownloadScheme(CommonConstants.HTTP_PROTOCOL);
    tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail when peer download scheme is used with replication of 1");
    } catch (IllegalStateException e) {
      // expected
      assertEquals(e.getMessage(), "peerSegmentDownloadScheme can't be used when replication is < 2");
    }

    segmentsValidationAndRetentionConfig.setReplication("2");
    tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    try {
      TableConfigUtils.validate(tableConfig, schema);
    } catch (IllegalStateException e) {
      // expected
      fail("Should not fail when peer download scheme is used with replication of > 1");
    }

    // invalid filter config since Groovy is disabled
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy({timestamp > 0}, timestamp)"));
    try {
      TableConfigUtils.setDisableGroovy(true);
      TableConfigUtils.validate(tableConfig, schema);
      // Reset to false
      TableConfigUtils.setDisableGroovy(false);
      fail("Should fail when Groovy functions disabled but found in filter config");
    } catch (IllegalStateException e) {
      // expected
    }

    // null transform column name
    ingestionConfig.setFilterConfig(null);
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig(null, "reverse(anotherCol)")));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for null column name in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // null transform function string
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig("myCol", null)));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for null transform function in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // invalid function
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig("myCol", "fakeFunction(col)")));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid transform function in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // invalid function
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig("myCol", "Groovy(badExpr)")));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid transform function in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // input field name used as destination field
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig("myCol", "reverse(myCol)")));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail due to use of myCol as arguments and columnName");
    } catch (IllegalStateException e) {
      // expected
    }

    // input field name used as destination field
    ingestionConfig.setTransformConfigs(
        Collections.singletonList(new TransformConfig("myCol", "Groovy({x + y + myCol}, x, myCol, y)")));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail due to use of myCol as arguments and columnName");
    } catch (IllegalStateException e) {
      // expected
    }

    // duplicate transform config
    ingestionConfig.setTransformConfigs(
        Arrays.asList(new TransformConfig("myCol", "reverse(x)"), new TransformConfig("myCol", "lower(y)")));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail due to duplicate transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // derived columns - should pass
    ingestionConfig.setTransformConfigs(Arrays.asList(new TransformConfig("transformedCol", "reverse(x)"),
        new TransformConfig("myCol", "lower(transformedCol)")));
    TableConfigUtils.validate(tableConfig, schema);

    // invalid field name in schema with matching prefix from complexConfigType's prefixesToRename
    ingestionConfig.setTransformConfigs(null);
    ingestionConfig.setComplexTypeConfig(
        new ComplexTypeConfig(null, ".", null, Collections.singletonMap("after.", "")));
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addMultiValueDimension("after.test", FieldSpec.DataType.STRING)
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail due to name conflict from field name in schema with a prefix in prefixesToRename");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void ingestionAggregationConfigsTest() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addDateTime("timeColumn", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("d1", "SUM(s1)")));
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName("timeColumn")
        .setIngestionConfig(ingestionConfig)
        .build();
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      fail("Should fail due to destination column not being in schema");
    } catch (IllegalStateException e) {
      // expected
    }

    schema.addField(new DimensionFieldSpec("d1", FieldSpec.DataType.DOUBLE, true));
    tableConfig.getIndexingConfig().setAggregateMetrics(true);
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      fail("Should fail due to aggregateMetrics being set");
    } catch (IllegalStateException e) {
      // expected
    }

    tableConfig.getIndexingConfig().setAggregateMetrics(false);
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      fail("Should fail due to aggregation column being a dimension");
    } catch (IllegalStateException e) {
      // expected
    }

    schema.addField(new MetricFieldSpec("m1", FieldSpec.DataType.DOUBLE));
    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig(null, null)));
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      fail("Should fail due to null columnName/aggregationFunction");
    } catch (IllegalStateException e) {
      // expected
    }

    ingestionConfig.setAggregationConfigs(
        Arrays.asList(new AggregationConfig("m1", "SUM(s1)"), new AggregationConfig("m1", "SUM(s2)")));
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      fail("Should fail due to duplicate destination column");
    } catch (IllegalStateException e) {
      // expected
    }

    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("m1", "SUM s1")));
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      fail("Should fail due to invalid aggregation function");
    } catch (IllegalStateException e) {
      // expected
    }

    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("m1", "SUM(s1 - s2)")));
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      fail("Should fail due to inner value not being a column");
    } catch (IllegalStateException e) {
      // expected
    }

    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("m1", "SUM(m1)")));
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      fail("Should fail due to noDictionaryColumns being null");
    } catch (IllegalStateException e) {
      // expected
    }

    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setNoDictionaryColumns(List.of());
    tableConfig.setIndexingConfig(indexingConfig);

    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      fail("Should fail due to noDictionaryColumns not containing m1");
    } catch (IllegalStateException e) {
      // expected
    }

    indexingConfig.setNoDictionaryColumns(List.of("m1"));

    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("m1", "SUM(m1)")));
    TableConfigUtils.validateIngestionConfig(tableConfig, schema);

    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("m1", "SUM(s1)")));
    TableConfigUtils.validateIngestionConfig(tableConfig, schema);

    schema.addField(new MetricFieldSpec("m2", FieldSpec.DataType.DOUBLE));
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      fail("Should fail due to one metric column not being aggregated");
    } catch (IllegalStateException e) {
      // expected
    }

    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addMetric("d1", FieldSpec.DataType.BYTES).build();
    // distinctcounthllmv is not supported, we expect this to not validate
    List<AggregationConfig> aggregationConfigs = Arrays.asList(new AggregationConfig("d1", "DISTINCTCOUNTHLLMV(s1)"));
    ingestionConfig.setAggregationConfigs(aggregationConfigs);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("myTable_REALTIME")
        .setTimeColumnName("timeColumn")
        .setIngestionConfig(ingestionConfig)
        .setNoDictionaryColumns(List.of("d1"))
        .build();

    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      fail("Should fail due to not supported aggregation function");
    } catch (IllegalStateException e) {
      // expected
    }

    // distinctcounthll, expect that the function name in various forms (with and without underscores) still validates
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addMetric("d1", FieldSpec.DataType.BYTES)
        .addMetric("d2", FieldSpec.DataType.BYTES)
        .addMetric("d3", FieldSpec.DataType.BYTES)
        .addMetric("d4", FieldSpec.DataType.BYTES)
        .addMetric("d5", FieldSpec.DataType.BYTES)
        .build();

    aggregationConfigs = Arrays.asList(new AggregationConfig("d1", "distinct_count_hll(s1)"),
        new AggregationConfig("d2", "DISTINCTCOUNTHLL(s1)"), new AggregationConfig("d3", "distinctcounthll(s1)"),
        new AggregationConfig("d4", "DISTINCTCOUNT_HLL(s1)"), new AggregationConfig("d5", "DISTINCT_COUNT_HLL(s1)"));

    ingestionConfig.setAggregationConfigs(aggregationConfigs);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("myTable_REALTIME")
        .setTimeColumnName("timeColumn")
        .setIngestionConfig(ingestionConfig)
        .setNoDictionaryColumns(List.of("d1", "d2", "d3", "d4", "d5"))
        .build();

    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      fail("Should not fail due to valid aggregation function", e);
    }

    // distinctcounthllplus, expect that the function name in various forms still validates
    aggregationConfigs = Arrays.asList(new AggregationConfig("d1", "distinct_count_hll_plus(s1)"),
        new AggregationConfig("d2", "DISTINCTCOUNTHLLPLUS(s1)"),
        new AggregationConfig("d3", "distinctcounthllplus(s1)"),
        new AggregationConfig("d4", "DISTINCTCOUNT_HLL_PLUS(s1)"),
        new AggregationConfig("d5", "DISTINCT_COUNT_HLL_PLUS(s1)"));

    ingestionConfig.setAggregationConfigs(aggregationConfigs);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("myTable_REALTIME")
        .setTimeColumnName("timeColumn")
        .setIngestionConfig(ingestionConfig)
        .setNoDictionaryColumns(List.of("d1", "d2", "d3", "d4", "d5"))
        .build();

    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      fail("Should not fail due to valid aggregation function", e);
    }

    // distinctcounthll, expect not specified log2m argument to default to 8
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addMetric("d1", FieldSpec.DataType.BYTES).build();

    aggregationConfigs = Arrays.asList(new AggregationConfig("d1", "DISTINCTCOUNTHLL(s1)"));
    ingestionConfig.setAggregationConfigs(aggregationConfigs);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("myTable_REALTIME")
        .setTimeColumnName("timeColumn")
        .setIngestionConfig(ingestionConfig)
        .setNoDictionaryColumns(List.of("d1"))
        .build();

    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      fail("Log2m should have defaulted to 8", e);
    }

    aggregationConfigs = Arrays.asList(new AggregationConfig("d1", "s1 + s2"));
    ingestionConfig.setAggregationConfigs(aggregationConfigs);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("myTable_REALTIME")
        .setTimeColumnName("timeColumn")
        .setIngestionConfig(ingestionConfig)
        .setNoDictionaryColumns(List.of("d1"))
        .build();

    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      fail("Should fail due to multiple arguments");
    } catch (IllegalArgumentException e) {
      // expected
    }

    // sumprecision, expect that the function name in various forms (with and without underscores) still validates
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("s1", FieldSpec.DataType.BIG_DECIMAL)
        .addMetric("d1", FieldSpec.DataType.BIG_DECIMAL)
        .addMetric("d2", FieldSpec.DataType.BIG_DECIMAL)
        .addMetric("d3", FieldSpec.DataType.BIG_DECIMAL)
        .addMetric("d4", FieldSpec.DataType.BIG_DECIMAL)
        .build();

    aggregationConfigs = Arrays.asList(new AggregationConfig("d1", "sum_precision(s1, 10, 32)"),
        new AggregationConfig("d2", "SUM_PRECISION(s1, 1)"), new AggregationConfig("d3", "sumprecision(s1, 2)"),
        new AggregationConfig("d4", "SUMPRECISION(s1, 10, 99)"));

    ingestionConfig.setAggregationConfigs(aggregationConfigs);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("myTable_REALTIME")
        .setTimeColumnName("timeColumn")
        .setIngestionConfig(ingestionConfig)
        .setNoDictionaryColumns(List.of("d1", "d2", "d3", "d4", "d5"))
        .build();
    TableConfigUtils.validateIngestionConfig(tableConfig, schema);

    // with too many arguments should fail
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("s1", FieldSpec.DataType.BIG_DECIMAL)
        .addMetric("d1", FieldSpec.DataType.BIG_DECIMAL)
        .build();

    aggregationConfigs = Arrays.asList(new AggregationConfig("d1", "sum_precision(s1, 10, 32, 99)"));

    ingestionConfig.setAggregationConfigs(aggregationConfigs);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("myTable_REALTIME")
        .setTimeColumnName("timeColumn")
        .setIngestionConfig(ingestionConfig)
        .setNoDictionaryColumns(List.of("d1"))
        .build();

    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      fail("Should have failed with too many arguments but didn't");
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void ingestionStreamConfigsTest() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addDateTime("timeColumn", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    Map<String, String> streamConfigs = getStreamConfigs();
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(Arrays.asList(streamConfigs, streamConfigs)));
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName("timeColumn")
        .setIngestionConfig(ingestionConfig)
        .build();

    // Multiple stream configs are allowed
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      fail("Multiple stream configs should be supported");
    }

    // stream config should be valid
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(Collections.singletonList(streamConfigs)));
    TableConfigUtils.validateIngestionConfig(tableConfig, schema);

    // validate the proto decoder
    streamConfigs = getKafkaStreamConfigs();
    //test config should be valid
    TableConfigUtils.validateStreamConfig(new StreamConfig("test", streamConfigs));
    streamConfigs.remove("stream.kafka.decoder.prop.descriptorFile");
    try {
      TableConfigUtils.validateStreamConfig(new StreamConfig("test", streamConfigs));
      fail("Should fail without descriptor file");
    } catch (IllegalStateException e) {
      // expected
    }
    streamConfigs = getKafkaStreamConfigs();
    streamConfigs.remove("stream.kafka.decoder.prop.protoClassName");
    try {
      TableConfigUtils.validateStreamConfig(new StreamConfig("test", streamConfigs));
      fail("Should fail without descriptor proto class name");
    } catch (IllegalStateException e) {
      // expected
    }
    //validate the protobuf pulsar config
    streamConfigs = getPulsarStreamConfigs();
    //test config should be valid
    TableConfigUtils.validateStreamConfig(new StreamConfig("test", streamConfigs));
    //remove the descriptor file, should fail
    streamConfigs.remove("stream.pulsar.decoder.prop.descriptorFile");
    try {
      TableConfigUtils.validateStreamConfig(new StreamConfig("test", streamConfigs));
      fail("Should fail without descriptor file");
    } catch (IllegalStateException e) {
      // expected
    }
    streamConfigs = getPulsarStreamConfigs();
    //remove the proto class name, should fail
    streamConfigs.remove("stream.pulsar.decoder.prop.protoClassName");
    try {
      TableConfigUtils.validateStreamConfig(new StreamConfig("test", streamConfigs));
      fail("Should fail without descriptor proto class name");
    } catch (IllegalStateException e) {
      // expected
    }

    // When size based threshold is specified, default rows should not be set
    streamConfigs = getKafkaStreamConfigs();
    streamConfigs.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_SEGMENT_SIZE);
    streamConfigs.remove(StreamConfigProperties.DEPRECATED_SEGMENT_FLUSH_DESIRED_SIZE);
    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_SEGMENT_SIZE, "100m");
    streamConfigs.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS);
    TableConfigUtils.validateStreamConfig(new StreamConfig("test", streamConfigs));

    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "1000");
    try {
      TableConfigUtils.validateStreamConfig(new StreamConfig("test", streamConfigs));
      fail("Should fail when both rows and size based threshold are specified");
    } catch (IllegalStateException e) {
      // expected
    }

    // Legacy behavior: allow size based threshold to be explicitly set to 0
    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "0");
    TableConfigUtils.validateStreamConfig(new StreamConfig("test", streamConfigs));
  }

  @Test
  public void ingestionBatchConfigsTest() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).build();

    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.INPUT_DIR_URI, "s3://foo");
    batchConfigMap.put(BatchConfigProperties.OUTPUT_DIR_URI, "gs://bar");
    batchConfigMap.put(BatchConfigProperties.INPUT_FS_CLASS, "org.foo.S3FS");
    batchConfigMap.put(BatchConfigProperties.OUTPUT_FS_CLASS, "org.foo.GcsFS");
    batchConfigMap.put(BatchConfigProperties.INPUT_FORMAT, "avro");
    batchConfigMap.put(BatchConfigProperties.RECORD_READER_CLASS, "org.foo.Reader");

    IngestionConfig ingestionConfig = new IngestionConfig();
    // TODO: Check if we should allow duplicate config maps
    ingestionConfig.setBatchIngestionConfig(
        new BatchIngestionConfig(Arrays.asList(batchConfigMap, batchConfigMap), null, null));
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(ingestionConfig).build();
    TableConfigUtils.validateIngestionConfig(tableConfig, schema);
  }

  @Test
  public void ingestionConfigForDimensionTableTest() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).setPrimaryKeyColumns(List.of("pk")).build();

    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.INPUT_DIR_URI, "s3://foo");
    batchConfigMap.put(BatchConfigProperties.OUTPUT_DIR_URI, "gs://bar");
    batchConfigMap.put(BatchConfigProperties.INPUT_FS_CLASS, "org.foo.S3FS");
    batchConfigMap.put(BatchConfigProperties.OUTPUT_FS_CLASS, "org.foo.GcsFS");
    batchConfigMap.put(BatchConfigProperties.INPUT_FORMAT, "avro");
    batchConfigMap.put(BatchConfigProperties.RECORD_READER_CLASS, "org.foo.Reader");

    // valid dimension table ingestion config
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(
        new BatchIngestionConfig(Collections.singletonList(batchConfigMap), "REFRESH", null));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setIsDimTable(true)
        .setIngestionConfig(ingestionConfig)
        .build();
    TableConfigUtils.validateIngestionConfig(tableConfig, schema);

    // dimension tables should have batch ingestion config
    ingestionConfig.setBatchIngestionConfig(null);
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      fail("Should fail for Dimension table without batch ingestion config");
    } catch (IllegalStateException e) {
      // expected
    }

    // dimension tables should have batch ingestion config of type REFRESH
    ingestionConfig.setBatchIngestionConfig(
        new BatchIngestionConfig(Collections.singletonList(batchConfigMap), "APPEND", null));
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      fail("Should fail for Dimension table with ingestion type APPEND (should be REFRESH)");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void validateTierConfigs() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    // null tier configs
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(null).build();
    TableConfigUtils.validate(tableConfig, schema);

    // empty tier configs
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTierConfigList(Collections.emptyList())
        .build();
    TableConfigUtils.validate(tableConfig, schema);

    // 1 tier configs
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTierConfigList(Lists.newArrayList(
            new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null)))
        .build();
    TableConfigUtils.validate(tableConfig, schema);

    // 2 tier configs, case insensitive check
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTierConfigList(Lists.newArrayList(
            new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE.toLowerCase(), "30d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "40d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE.toLowerCase(), "tier2_tag_OFFLINE", null, null)))
        .build();
    TableConfigUtils.validate(tableConfig, schema);

    //realtime table
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(getStreamConfigs())
        .setTierConfigList(Lists.newArrayList(
            new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE.toLowerCase(), "tier1_tag_OFFLINE", null, null),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE.toLowerCase(), "40d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE", null, null)))
        .build();
    TableConfigUtils.validate(tableConfig, schema);

    // tier name empty
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTierConfigList(Lists.newArrayList(new TierConfig("", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null)))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should have failed due to empty tier name");
    } catch (IllegalStateException e) {
      // expected
    }

    // tier name repeats
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTierConfigList(Lists.newArrayList(
            new TierConfig("sameTierName", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null),
            new TierConfig("sameTierName", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "100d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE", null, null)))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should have failed due to duplicate tier name");
    } catch (IllegalStateException e) {
      // expected
    }

    // segmentSelectorType invalid
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTierConfigList(Lists.newArrayList(
            new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null),
            new TierConfig("tier2", "unsupportedSegmentSelector", "40d", null, TierFactory.PINOT_SERVER_STORAGE_TYPE,
                "tier2_tag_OFFLINE", null, null)))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should have failed due to invalid segmentSelectorType");
    } catch (IllegalStateException e) {
      // expected
    }

    // segmentAge not provided for TIME segmentSelectorType
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTierConfigList(Lists.newArrayList(
            new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, null, null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "40d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE", null, null)))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should have failed due to missing segmentAge");
    } catch (IllegalStateException e) {
      // expected
    }

    // segmentAge invalid
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTierConfigList(Lists.newArrayList(
            new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "3600", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE", null, null)))
        .build();

    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should have failed due to invalid segment age");
    } catch (IllegalStateException e) {
      // expected
    }

    // fixedSegmentSelector
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTierConfigList(Lists.newArrayList(
            new TierConfig("tier1", TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, null, null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null)))
        .build();
    TableConfigUtils.validate(tableConfig, schema);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTierConfigList(Lists.newArrayList(
            new TierConfig("tier1", TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, "30d", Lists.newArrayList(),
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null)))
        .build();
    TableConfigUtils.validate(tableConfig, schema);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTierConfigList(Lists.newArrayList(
            new TierConfig("tier1", TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, null, Lists.newArrayList("seg0", "seg1"),
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null)))
        .build();
    TableConfigUtils.validate(tableConfig, schema);

    // storageType invalid
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTierConfigList(Lists.newArrayList(
            new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null, "unsupportedStorageType",
                "tier1_tag_OFFLINE", null, null),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "40d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE", null, null)))
        .build();

    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should have failed due to invalid storage type");
    } catch (IllegalStateException e) {
      // expected
    }

    // serverTag not provided for PINOT_SERVER storageType
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTierConfigList(Lists.newArrayList(
            new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "40d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, null, null, null)))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should have failed due to ");
    } catch (IllegalStateException e) {
      // expected
    }

    // serverTag invalid
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTierConfigList(Lists.newArrayList(
            new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag", null, null),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "40d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE", null, null)))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should have failed due to invalid server tag");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testTableName() {
    String[] malformedTableName = {"test.test.table", "test table"};
    for (int i = 0; i < 2; i++) {
      String tableName = malformedTableName[i];
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build();
      try {
        TableConfigUtils.validateTableName(tableConfig);
        fail("Should fail for malformed table name : " + tableName);
      } catch (IllegalStateException e) {
        // expected
      }
    }

    String[] allowedTableName = {"test.table", "testTable"};
    for (int i = 0; i < 2; i++) {
      String tableName = allowedTableName[i];
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build();
      TableConfigUtils.validateTableName(tableConfig);
    }
  }

  @Test
  public void testValidateFieldConfig() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS")
        .addSingleValueDimension("myCol1", FieldSpec.DataType.STRING)
        .addMultiValueDimension("myCol2", FieldSpec.DataType.INT)
        .addSingleValueDimension("intCol", FieldSpec.DataType.INT)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol1"))
        .build();

    try {
      FieldConfig fieldConfig = new FieldConfig("myCol1", FieldConfig.EncodingType.RAW, null, null, null, null, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      fail("all nullable fields set for fieldConfig should pass", e);
    }

    try {
      FieldConfig fieldConfig =
          new FieldConfig("myCol1", FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.FST, null, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for with conflicting encoding type of myCol1");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "FieldConfig encoding type is different from indexingConfig for column: myCol1");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol1"))
        .build();
    try {
      FieldConfig fieldConfig =
          new FieldConfig("myCol1", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.FST, null, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail since FST index is enabled on RAW encoding type");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Cannot create FST index on column: myCol1 without dictionary");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    try {
      FieldConfig fieldConfig =
          new FieldConfig("myCol2", FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.FST, null, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail since FST index is enabled on multi value column");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Cannot create FST index on multi-value column: myCol2");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    try {
      FieldConfig fieldConfig =
          new FieldConfig("intCol", FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.FST, null, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail since FST index is enabled on non String column");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Cannot create FST index on column: intCol of stored type other than STRING");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol2", "intCol"))
        .build();
    try {
      FieldConfig fieldConfig =
          new FieldConfig("intCol", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.TEXT, null, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail since TEXT index is enabled on non String column");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Cannot create TEXT index on column: intCol of stored type other than STRING");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol1"))
        .build();
    try {
      FieldConfig fieldConfig =
          new FieldConfig("myCol21", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.FST, null, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail since field name is not present in schema");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Failed to find column: myCol21 in schema");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    try {
      FieldConfig fieldConfig = new FieldConfig("intCol", FieldConfig.EncodingType.DICTIONARY, Collections.emptyList(),
          CompressionCodec.SNAPPY, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail since dictionary encoding does not support compression codec SNAPPY");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Compression codec: SNAPPY is not applicable to dictionary encoded column: intCol");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    try {
      FieldConfig fieldConfig = new FieldConfig("intCol", FieldConfig.EncodingType.RAW, Collections.emptyList(),
          CompressionCodec.MV_ENTRY_DICT, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail since raw encoding does not support compression codec MV_ENTRY_DICT");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Compression codec: MV_ENTRY_DICT is not applicable to raw column: intCol");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol1"))
        .build();
    try {
      // Enable forward index disabled flag for a raw column. This should succeed as though the forward index cannot
      // be rebuilt without a dictionary, the constraint to have a dictionary has been lifted.
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig =
          new FieldConfig("myCol1", FieldConfig.EncodingType.RAW, null, null, null, null, fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      fail("Validation should pass since forward index can be disabled for a column without a dictionary");
    }

    try {
      // Enable forward index disabled flag for a column without inverted index. This should succeed as though the
      // forward index cannot be rebuilt without an inverted index, the constraint to have an inverted index has been
      // lifted.
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig =
          new FieldConfig("myCol2", FieldConfig.EncodingType.DICTIONARY, null, null, null, null, fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      fail("Validation should pass since forward index can be disabled for a column without an inverted index");
    }

    try {
      // Enable forward index disabled flag for a column and verify that dictionary override options are not set.
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      tableConfig.getIndexingConfig().setOptimizeDictionaryForMetrics(true);
      tableConfig.getIndexingConfig().setOptimizeDictionaryForMetrics(true);
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Dictionary override optimization options (OptimizeDictionary, "
          + "optimizeDictionaryForMetrics) not supported with forward index disabled for column: myCol2");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol1"))
        .setInvertedIndexColumns(Arrays.asList("myCol2"))
        .build();
    try {
      // Enable forward index disabled flag for a column with inverted index
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig =
          new FieldConfig("myCol2", FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.INVERTED, null, null,
              null, fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      fail("Should not fail as myCol2 has forward index disabled but inverted index enabled");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of("myCol2"))
        .setInvertedIndexColumns(List.of("myCol1"))
        .setSortedColumn("myCol1")
        .build();
    try {
      // Enable forward index disabled flag for a column with inverted index and is sorted
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig =
          new FieldConfig("myCol1", FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.INVERTED, null, null,
              null, fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      fail("Should not fail for myCol1 with forward index disabled but is sorted, this is a no-op");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol1"))
        .setInvertedIndexColumns(Arrays.asList("myCol2"))
        .setRangeIndexColumns(Arrays.asList("myCol2"))
        .build();
    try {
      // Enable forward index disabled flag for a multi-value column with inverted index and range index
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig =
          new FieldConfig("myCol2", FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.INVERTED,
              Arrays.asList(FieldConfig.IndexType.INVERTED, FieldConfig.IndexType.RANGE), null, null,
              fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for MV myCol2 with forward index disabled but has range and inverted index");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Feature not supported for multi-value columns with range index. "
          + "Cannot disable forward index for column: myCol2. Disable range index on this column to use this feature.");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Arrays.asList("myCol1"))
        .setRangeIndexColumns(Arrays.asList("myCol1"))
        .build();
    try {
      // Enable forward index disabled flag for a singe-value column with inverted index and range index v1
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig =
          new FieldConfig("myCol1", FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.INVERTED,
              Arrays.asList(FieldConfig.IndexType.INVERTED, FieldConfig.IndexType.RANGE), null, null,
              fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      tableConfig.getIndexingConfig().setRangeIndexVersion(1);
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for SV myCol1 with forward index disabled but has range v1 and inverted index");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Feature not supported for single-value columns with range index version "
          + "< 2. Cannot disable forward index for column: myCol1. Either disable range index or create range index "
          + "with version >= 2 to use this feature.");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol2"))
        .setInvertedIndexColumns(Arrays.asList("myCol2"))
        .build();
    try {
      // Enable forward index disabled flag for a column with inverted index and disable dictionary
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig =
          new FieldConfig("myCol2", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.INVERTED, null, null, null,
              fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should not be able to disable dictionary but keep inverted index");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Cannot create inverted index on column: myCol2 without dictionary");
    }

    // Tests the case when the field-config list marks a column as raw (non-dictionary) and enables
    // inverted index on it
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    try {
      FieldConfig fieldConfig =
          new FieldConfig.Builder("myCol2").withIndexTypes(Arrays.asList(FieldConfig.IndexType.INVERTED))
              .withEncodingType(FieldConfig.EncodingType.RAW)
              .build();
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should not be able to disable dictionary but keep inverted index");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Cannot create inverted index on column: myCol2 without dictionary");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol2"))
        .build();
    try {
      // Enable forward index disabled flag for a column with FST index and disable dictionary
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig =
          new FieldConfig("myCol2", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.FST, null, null, null,
              fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should not be able to disable dictionary but keep inverted index");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Cannot create FST index on column: myCol2 without dictionary");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("intCol"))
        .setRangeIndexColumns(Arrays.asList("intCol"))
        .build();
    try {
      // Enable forward index disabled flag for a column with FST index and disable dictionary
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig =
          new FieldConfig("intCol", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.RANGE, null, null, null,
              fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      fail("Range index with forward index disabled no dictionary column is allowed");
    }

    // Disabling forward index for realtime table will make the validation failed.
    Map<String, String> streamConfigs = getStreamConfigs();
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setNoDictionaryColumns(Arrays.asList("intCol"))
        .setStreamConfigs(streamConfigs)
        .build();
    try {
      // Enable forward index disabled flag for a column with inverted index index and disable dictionary
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig =
          new FieldConfig("intCol", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.INVERTED, null, null, null,
              fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Cannot disable forward index for column: intCol, as the table type is REALTIME");
    }
  }

  @Test
  public void testValidateBFOnBoolean() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.BOOLEAN)
        .addSingleValueDimension("mycol2", FieldSpec.DataType.STRING)
        .build();

    TableConfig tableconfig1 = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setBloomFilterColumns(Arrays.asList("mycol"))
        .build();
    assertThrows(IllegalStateException.class, () -> TableConfigUtils.validate(tableconfig1, schema));

    TableConfig tableconfig2 = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).build();
    tableconfig2.getIndexingConfig()
        .setBloomFilterConfigs(Collections.singletonMap("myCol", new BloomFilterConfig(0.01, 1000, true)));
    assertThrows(IllegalStateException.class, () -> TableConfigUtils.validate(tableconfig2, schema));

    TableConfig tableconfig3 = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).build();
    ObjectNode indexesNode = JsonNodeFactory.instance.objectNode();
    indexesNode.putObject("bloom");
    FieldConfig fieldConfig =
        new FieldConfig("MyCol", FieldConfig.EncodingType.DICTIONARY, null, null, null, null, indexesNode, null, null);
    tableconfig3.setFieldConfigList(Arrays.asList(fieldConfig));
    assertThrows(IllegalStateException.class, () -> TableConfigUtils.validate(tableconfig3, schema));
  }

  @Test
  public void testValidateIndexingConfig() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addSingleValueDimension("bytesCol", FieldSpec.DataType.BYTES)
        .addSingleValueDimension("intCol", FieldSpec.DataType.INT)
        .addSingleValueDimension("bigDecimalCol", FieldSpec.DataType.BIG_DECIMAL)
        .addMultiValueDimension("multiValCol", FieldSpec.DataType.STRING)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setBloomFilterColumns(Arrays.asList("myCol2"))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid Bloom filter column name");
    } catch (Exception e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Arrays.asList(""))
        .build();
    TableConfigUtils.validate(tableConfig, schema);

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Arrays.asList("myCol2"))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid Inverted Index column name");
    } catch (Exception e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList(""))
        .build();
    TableConfigUtils.validate(tableConfig, schema);

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol2"))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid No Dictionary column name");
    } catch (Exception e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setOnHeapDictionaryColumns(Arrays.asList(""))
        .build();
    TableConfigUtils.validate(tableConfig, schema);

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setOnHeapDictionaryColumns(Arrays.asList("myCol2"))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid On Heap Dictionary column name");
    } catch (Exception e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setRangeIndexColumns(Arrays.asList(""))
        .build();
    TableConfigUtils.validate(tableConfig, schema);

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setRangeIndexColumns(Arrays.asList("myCol2"))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid Range Index column name");
    } catch (Exception e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setSortedColumn("").build();
    TableConfigUtils.validate(tableConfig, schema);

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setSortedColumn("myCol2").build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid Sorted column name");
    } catch (Exception e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setVarLengthDictionaryColumns(Arrays.asList(""))
        .build();
    TableConfigUtils.validate(tableConfig, schema);

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setVarLengthDictionaryColumns(Arrays.asList("myCol2"))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid Var Length Dictionary column name");
    } catch (Exception e) {
      // expected
    }

    ColumnPartitionConfig columnPartitionConfig = new ColumnPartitionConfig("Murmur", 4);
    Map<String, ColumnPartitionConfig> partitionConfigMap = new HashMap<>();
    partitionConfigMap.put("myCol2", columnPartitionConfig);
    SegmentPartitionConfig partitionConfig = new SegmentPartitionConfig(partitionConfigMap);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setSegmentPartitionConfig(partitionConfig)
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid Segment Partition column name");
    } catch (Exception e) {
      // expected
    }

    // Although this config makes no sense, it should pass the validation phase
    StarTreeIndexConfig starTreeIndexConfig =
        new StarTreeIndexConfig(List.of("myCol"), List.of("myCol"), List.of("SUM__myCol"), null, 1);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(List.of(starTreeIndexConfig))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      fail("Should not fail for valid StarTreeIndex config column name");
    }

    starTreeIndexConfig = new StarTreeIndexConfig(List.of("myCol2"), List.of("myCol"), List.of("SUM__myCol"), null, 1);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(List.of(starTreeIndexConfig))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid StarTreeIndex config column name in dimension split order");
    } catch (Exception e) {
      // expected
    }

    starTreeIndexConfig = new StarTreeIndexConfig(List.of("myCol"), List.of("myCol2"), List.of("SUM__myCol"), null, 1);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(List.of(starTreeIndexConfig))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid StarTreeIndex config column name in skip star node for dimension");
    } catch (Exception e) {
      // expected
    }

    starTreeIndexConfig = new StarTreeIndexConfig(List.of("myCol"), List.of("myCol"), List.of("SUM__myCol2"), null, 1);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(List.of(starTreeIndexConfig))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid StarTreeIndex config column name in function column pair");
    } catch (Exception e) {
      // expected
    }

    starTreeIndexConfig =
        new StarTreeIndexConfig(List.of("myCol"), null, null, List.of(new StarTreeAggregationConfig("myCol2", "SUM")),
            1);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(List.of(starTreeIndexConfig))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid StarTreeIndex config column name in aggregation config");
    } catch (Exception e) {
      // expected
    }

    starTreeIndexConfig = new StarTreeIndexConfig(List.of("myCol"), null, List.of("SUM__myCol"),
        List.of(new StarTreeAggregationConfig("myCol", "SUM")), 1);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(List.of(starTreeIndexConfig))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid StarTreeIndex config with both function column pair and aggregation config");
    } catch (Exception e) {
      // expected
    }

    starTreeIndexConfig =
        new StarTreeIndexConfig(List.of("multiValCol"), List.of("multiValCol"), List.of("SUM__multiValCol"), null, 1);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(List.of(starTreeIndexConfig))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for multi-value column name in StarTreeIndex config");
    } catch (Exception e) {
      // expected
    }

    FieldConfig fieldConfig = new FieldConfig("myCol2", null, Collections.emptyList(), null, null);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(Arrays.asList(fieldConfig))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid column name in Field Config List");
    } catch (Exception e) {
      // expected
    }

    List<String> columnList = Arrays.asList("myCol");
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(columnList)
        .setInvertedIndexColumns(columnList)
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for valid column name in both no dictionary and inverted index column config");
    } catch (Exception e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setJsonIndexColumns(Arrays.asList("non-existent-column"))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for non existent column in Json index config");
    } catch (Exception e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setJsonIndexColumns(Arrays.asList("intCol"))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for Json index defined on non string column");
    } catch (Exception e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setJsonIndexColumns(Arrays.asList("multiValCol"))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for Json index defined on multi-value column");
    } catch (Exception e) {
      // expected
    }

    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setRangeIndexColumns(columnList).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      fail("Should work for range index defined on dictionary encoded string column");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setRangeIndexColumns(columnList)
        .setNoDictionaryColumns(columnList)
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for range index defined on non numeric/no-dictionary column");
    } catch (Exception e) {
      // Expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setVarLengthDictionaryColumns(Arrays.asList("myCol", "bytesCol", "bigDecimalCol", "multiValCol"))
        .build();
    TableConfigUtils.validate(tableConfig, schema);

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setVarLengthDictionaryColumns(Arrays.asList("intCol"))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for Var length dictionary defined for fixed-width column");
    } catch (Exception e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setJsonIndexColumns(Arrays.asList("multiValCol"))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for Json Index defined on a multi value column");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testValidateStarTreeIndexDuplicateFunctionColumnPair() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addSingleValueDimension("bytesCol", FieldSpec.DataType.BYTES)
        .addSingleValueDimension("intCol", FieldSpec.DataType.INT)
        .addMultiValueDimension("multiValCol", FieldSpec.DataType.STRING)
        .build();

    StarTreeIndexConfig starTreeIndexConfig =
        new StarTreeIndexConfig(List.of("myCol"), List.of("myCol"), List.of("SUM__myCol", "SUM__myCol"), null, 1);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(List.of(starTreeIndexConfig))
        .build();
    IllegalStateException e =
        expectThrows(IllegalStateException.class, () -> TableConfigUtils.validate(tableConfig, schema));
    assertTrue(e.getMessage().contains("Duplicate function column pair"));

    starTreeIndexConfig =
        new StarTreeIndexConfig(List.of("myCol"), List.of("myCol"), List.of("DISTINCTCOUNTHLL__myCol"), List.of(
            new StarTreeAggregationConfig("myCol", "DISTINCTCOUNTHLL", Map.of(Constants.HLL_LOG2M_KEY, 16), null, null,
                null, null, null)), 1);
    TableConfig tableConfig2 = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(List.of(starTreeIndexConfig))
        .build();
    e = expectThrows(IllegalStateException.class, () -> TableConfigUtils.validate(tableConfig2, schema));
    assertEquals(e.getMessage(),
        "Either 'functionColumnPairs' or 'aggregationConfigs' must be specified, but not both");

    starTreeIndexConfig = new StarTreeIndexConfig(List.of("myCol"), List.of("myCol"), null, List.of(
        new StarTreeAggregationConfig("myCol", "DISTINCTCOUNTHLL", Map.of(Constants.HLL_LOG2M_KEY, 16), null, null,
            null, null, null),
        new StarTreeAggregationConfig("myCol", "DISTINCTCOUNTHLL", Map.of(Constants.HLL_LOG2M_KEY, 8), null, null, null,
            null, null)), 1);
    TableConfig tableConfig3 = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(List.of(starTreeIndexConfig))
        .build();
    e = expectThrows(IllegalStateException.class, () -> TableConfigUtils.validate(tableConfig3, schema));
    assertTrue(e.getMessage().contains("Duplicate function column pair"));
  }

  @Test
  public void testValidateRetentionConfig() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setRetentionTimeUnit("hours")
        .setRetentionTimeValue("24")
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      fail("Should not fail for valid retention time unit value");
    }

    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setRetentionTimeUnit("abc").build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Should fail for invalid retention time unit value");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testValidateDedupConfig() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setDedupConfig(new DedupConfig())
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Upsert/Dedup table is for realtime table only.");
    }

    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setDedupConfig(new DedupConfig()).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Upsert/Dedup table must have primary key columns in the schema");
    }

    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addMultiValueDimension("myCol", FieldSpec.DataType.STRING)
        .setPrimaryKeyColumns(Lists.newArrayList("myCol"))
        .build();
    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setDedupConfig(new DedupConfig()).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Upsert/Dedup primary key column: myCol cannot be of multi-value type");
    }

    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .setPrimaryKeyColumns(Lists.newArrayList("myCol"))
        .build();
    Map<String, String> streamConfigs = getStreamConfigs();
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setDedupConfig(new DedupConfig())
        .setStreamConfigs(streamConfigs)
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "Upsert/Dedup table must use strict replica-group (i.e. strictReplicaGroup) based routing");
    }
    StarTreeIndexConfig starTreeIndexConfig = new StarTreeIndexConfig(Lists.newArrayList("myCol"), null,
        Collections.singletonList(
            new AggregationFunctionColumnPair(AggregationFunctionType.COUNT, "myCol").toColumnName()), null, 10);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setDedupConfig(new DedupConfig())
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setStarTreeIndexConfigs(Lists.newArrayList(starTreeIndexConfig))
        .setStreamConfigs(streamConfigs)
        .build();
    TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);

    // Dedup and upsert can't be enabled simultaneously
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setDedupConfig(new DedupConfig())
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setStreamConfigs(streamConfigs)
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "A table can have either Upsert or Dedup enabled, but not both");
    }
  }

  @Test
  public void testValidateInvalidDedupConfigs() {
    // Invalid STRING time column
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.STRING, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(Lists.newArrayList("myCol"))
        .build();

    Map<String, String> streamConfigs = getStreamConfigs();
    DedupConfig dedupConfig = new DedupConfig();
    dedupConfig.setMetadataTTL(10);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setDedupConfig(dedupConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs)
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "MetadataTTL must have time column: timeColumn in numeric type, found: STRING");
    }

    // Invalid TIMESTAMP dedupTimeColumn
    dedupConfig.setDedupTimeColumn(TIME_COLUMN);
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(Lists.newArrayList("myCol"))
        .build();
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setDedupConfig(dedupConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setStreamConfigs(streamConfigs)
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "MetadataTTL must have dedupTimeColumn: timeColumn in numeric type, found: TIMESTAMP");
    }
  }

  @Test
  public void testValidateUpsertConfig() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setUpsertConfig(upsertConfig)
        .setTimeColumnName(TIME_COLUMN)
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Upsert/Dedup table is for realtime table only.");
    }

    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setUpsertConfig(upsertConfig).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Upsert/Dedup table must have primary key columns in the schema");
    }

    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .setPrimaryKeyColumns(Lists.newArrayList("myCol"))
        .build();
    Map<String, String> streamConfigs = getStreamConfigs();
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setUpsertConfig(upsertConfig)
        .setStreamConfigs(streamConfigs)
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "Upsert/Dedup table must use strict replica-group (i.e. strictReplicaGroup) based routing");
    }

    // invalid tag override with upsert
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setStreamConfigs(getStreamConfigs())
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setTagOverrideConfig(new TagOverrideConfig("T1_REALTIME", "T2_REALTIME"))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail("Tag override must not be allowed with upsert");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Invalid tenant tag override used for Upsert/Dedup table");
    }

    // tag override even with same tag for CONSUMING and COMPLETED with upsert should fail
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setStreamConfigs(getStreamConfigs())
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setTagOverrideConfig(new TagOverrideConfig("T1_REALTIME", "T1_REALTIME"))
        .build();

    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail("Tag override must not be allowed with upsert");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Invalid tenant tag override used for Upsert/Dedup table");
    }

    // empty tag override with upsert should pass
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setStreamConfigs(getStreamConfigs())
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setTagOverrideConfig(new TagOverrideConfig(null, null))
        .build();
    TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);

    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setStreamConfigs(streamConfigs)
        .build();
    TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);

    StarTreeIndexConfig starTreeIndexConfig = new StarTreeIndexConfig(Lists.newArrayList("myCol"), null,
        Collections.singletonList(
            new AggregationFunctionColumnPair(AggregationFunctionType.COUNT, "myCol").toColumnName()), null, 10);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setStarTreeIndexConfigs(Lists.newArrayList(starTreeIndexConfig))
        .setStreamConfigs(streamConfigs)
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "The upsert table cannot have star-tree index.");
    }

    //With Aggregate Metrics
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setStreamConfigs(streamConfigs)
        .setAggregateMetrics(true)
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Metrics aggregation and upsert cannot be enabled together");
    }

    //With aggregation Configs in Ingestion Config
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("twiceSum", "SUM(twice)")));
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setStreamConfigs(streamConfigs)
        .setIngestionConfig(ingestionConfig)
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Metrics aggregation and upsert cannot be enabled together");
    }

    //With aggregation Configs in Ingestion Config and IndexingConfig at the same time
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setStreamConfigs(streamConfigs)
        .setAggregateMetrics(true)
        .setIngestionConfig(ingestionConfig)
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "Metrics aggregation cannot be enabled in the Indexing Config and Ingestion Config at the same time");
    }

    // Table upsert with delete column
    String stringTypeDelCol = "stringTypeDelCol";
    String delCol = "myDelCol";
    String mvCol = "mvCol";
    String timestampCol = "timestampCol";
    String invalidCol = "invalidCol";
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .setPrimaryKeyColumns(Lists.newArrayList("myPkCol"))
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addSingleValueDimension("myPkCol", FieldSpec.DataType.STRING)
        .addSingleValueDimension(stringTypeDelCol, FieldSpec.DataType.STRING)
        .addSingleValueDimension(delCol, FieldSpec.DataType.BOOLEAN)
        .addSingleValueDimension(timestampCol, FieldSpec.DataType.TIMESTAMP)
        .addMultiValueDimension(mvCol, FieldSpec.DataType.STRING)
        .build();
    streamConfigs = getStreamConfigs();

    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(stringTypeDelCol);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      fail("Shouldn't fail table creation when delete column type is single-valued.");
    }

    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(delCol);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      fail("Shouldn't fail table creation when delete column type is single-valued.");
    }

    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(timestampCol);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail("Should have failed table creation when delete column type is timestamp.");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "The deleteRecordColumn - timestampCol must be of type: String / Boolean / Numeric");
    }

    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(invalidCol);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail("Should have failed table creation when invalid delete column entered.");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Column invalidCol specified in deleteRecordColumn does not exist");
    }

    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(mvCol);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail("Should have failed table creation when delete column type is multi-valued.");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "The deleteRecordColumn - mvCol must be a single-valued column");
    }

    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .setPrimaryKeyColumns(Lists.newArrayList("myPkCol"))
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addMultiValueDimension("myPkCol", FieldSpec.DataType.STRING)
        .addSingleValueDimension(stringTypeDelCol, FieldSpec.DataType.STRING)
        .addSingleValueDimension(delCol, FieldSpec.DataType.BOOLEAN)
        .addSingleValueDimension(timestampCol, FieldSpec.DataType.TIMESTAMP)
        .addMultiValueDimension(mvCol, FieldSpec.DataType.STRING)
        .build();
    streamConfigs = getStreamConfigs();

    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(stringTypeDelCol);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      fail("Should fail table creation when PK column type is multi-valued.");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Upsert/Dedup primary key column: myPkCol cannot be of multi-value type");
    }

    // upsert deleted-keys-ttl configs with no deleted column
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .setPrimaryKeyColumns(Lists.newArrayList("myPkCol"))
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addSingleValueDimension("myPkCol", FieldSpec.DataType.STRING)
        .addSingleValueDimension(delCol, FieldSpec.DataType.BOOLEAN)
        .build();
    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeletedKeysTTL(3600);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setTimeColumnName(TIME_COLUMN)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Deleted Keys TTL can only be enabled with deleteRecordColumn set.");
    }

    upsertConfig.setDeleteRecordColumn(delCol);
    // multiple comparison columns set for deleted-keys-ttl
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .setPrimaryKeyColumns(Lists.newArrayList("myPkCol"))
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addSingleValueDimension("myPkCol", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .addSingleValueDimension(delCol, FieldSpec.DataType.BOOLEAN)
        .build();
    upsertConfig.setComparisonColumns(Lists.newArrayList(TIME_COLUMN, "myCol"));
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "MetadataTTL / DeletedKeysTTL does not work with multiple comparison columns");
    }

    // comparison column with non-numeric type
    upsertConfig.setComparisonColumns(Lists.newArrayList("myCol"));
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "MetadataTTL / DeletedKeysTTL must have comparison column: myCol in numeric type, found: STRING");
    }

    // time column as comparison column
    upsertConfig.setComparisonColumns(Lists.newArrayList(TIME_COLUMN));
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);

    // upsert out-of-order configs
    String outOfOrderRecordColumn = "outOfOrderRecordColumn";
    boolean dropOutOfOrderRecord = true;
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .setPrimaryKeyColumns(Lists.newArrayList("myPkCol"))
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addSingleValueDimension("myPkCol", FieldSpec.DataType.STRING)
        .addSingleValueDimension(outOfOrderRecordColumn, FieldSpec.DataType.BOOLEAN)
        .build();
    streamConfigs = getStreamConfigs();

    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDropOutOfOrderRecord(dropOutOfOrderRecord);
    upsertConfig.setOutOfOrderRecordColumn(outOfOrderRecordColumn);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "outOfOrderRecordColumn and dropOutOfOrderRecord shouldn't exist together for upsert table");
    }

    // outOfOrderRecordColumn not of type BOOLEAN
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .setPrimaryKeyColumns(Lists.newArrayList("myPkCol"))
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addSingleValueDimension("myPkCol", FieldSpec.DataType.STRING)
        .addSingleValueDimension(outOfOrderRecordColumn, FieldSpec.DataType.STRING)
        .build();
    streamConfigs = getStreamConfigs();

    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setOutOfOrderRecordColumn(outOfOrderRecordColumn);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "The outOfOrderRecordColumn must be a single-valued BOOLEAN column");
    }

    // test enableDeletedKeysCompactionConsistency shouldn't exist with metadataTTL
    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setEnableDeletedKeysCompactionConsistency(true);
    upsertConfig.setMetadataTTL(1.0);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "enableDeletedKeysCompactionConsistency and metadataTTL shouldn't exist together for upsert table");
    }

    // test enableDeletedKeysCompactionConsistency shouldn't exist with enablePreload
    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setEnableDeletedKeysCompactionConsistency(true);
    upsertConfig.setPreload(Enablement.ENABLE);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "enableDeletedKeysCompactionConsistency and preload shouldn't exist together for upsert table");
    }

    // test enableDeletedKeysCompactionConsistency should exist with deletedKeysTTL
    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setEnableDeletedKeysCompactionConsistency(true);
    upsertConfig.setDeletedKeysTTL(0);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "enableDeletedKeysCompactionConsistency should exist with deletedKeysTTL for upsert table");
    }

    // test enableDeletedKeysCompactionConsistency should exist with enableSnapshot
    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setEnableDeletedKeysCompactionConsistency(true);
    upsertConfig.setDeletedKeysTTL(100);
    upsertConfig.setSnapshot(Enablement.DISABLE);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "enableDeletedKeysCompactionConsistency should exist with snapshot for upsert table");
    }

    // test enableDeletedKeysCompactionConsistency should exist with UpsertCompactionTask / UpsertCompactMerge task
    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setEnableDeletedKeysCompactionConsistency(true);
    upsertConfig.setDeletedKeysTTL(100);
    upsertConfig.setSnapshot(Enablement.ENABLE);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "enableDeletedKeysCompactionConsistency should exist with UpsertCompactionTask "
          + "/ UpsertCompactMergeTask for upsert table");
    }
  }

  @Test
  public void testValidatePartialUpsertConfig() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol1", FieldSpec.DataType.LONG)
        .addSingleValueDimension("myCol2", FieldSpec.DataType.STRING)
        .addDateTime("myTimeCol", FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .setPrimaryKeyColumns(Lists.newArrayList("myCol1"))
        .build();

    Map<String, String> streamConfigs = getStreamConfigs();
    Map<String, UpsertConfig.Strategy> partialUpsertStratgies = new HashMap<>();
    partialUpsertStratgies.put("myCol2", UpsertConfig.Strategy.IGNORE);
    UpsertConfig partialUpsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    partialUpsertConfig.setPartialUpsertStrategies(partialUpsertStratgies);
    partialUpsertConfig.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.OVERWRITE);
    partialUpsertConfig.setComparisonColumn("myCol2");
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setUpsertConfig(partialUpsertConfig)
        .setNullHandlingEnabled(true)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setStreamConfigs(streamConfigs)
        .build();
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Merger cannot be applied to comparison column");
    }

    partialUpsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    partialUpsertConfig.setPartialUpsertStrategies(partialUpsertStratgies);
    partialUpsertConfig.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.OVERWRITE);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName("myCol2")
        .setUpsertConfig(partialUpsertConfig)
        .setNullHandlingEnabled(true)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setStreamConfigs(streamConfigs)
        .build();
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Merger cannot be applied to time column");
    }

    partialUpsertStratgies.put("myCol1", UpsertConfig.Strategy.INCREMENT);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName("timeCol")
        .setUpsertConfig(partialUpsertConfig)
        .setNullHandlingEnabled(true)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setStreamConfigs(streamConfigs)
        .build();
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Merger cannot be applied to primary key columns");
    }

    partialUpsertStratgies.clear();
    partialUpsertStratgies.put("randomCol", UpsertConfig.Strategy.OVERWRITE);
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Merger cannot be applied to non-existing column: randomCol");
    }

    partialUpsertStratgies.clear();
    partialUpsertStratgies.put("myCol2", UpsertConfig.Strategy.INCREMENT);
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "INCREMENT merger cannot be applied to non-numeric column: myCol2");
    }

    partialUpsertStratgies.clear();
    partialUpsertStratgies.put("myCol2", UpsertConfig.Strategy.APPEND);
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "APPEND merger cannot be applied to single-value column: myCol2");
    }

    partialUpsertStratgies.clear();
    partialUpsertStratgies.put("myTimeCol", UpsertConfig.Strategy.INCREMENT);
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, schema);
      fail();
    } catch (Exception e) {
      assertEquals(e.getMessage(), "INCREMENT merger cannot be applied to date time column: myTimeCol");
    }
  }

  /**
   * Utility function that can be used to write simple tests that modify the table config and schema.
   *
   * This function has been designed to test the nullability of the partial upsert config, but feel free to use it to
   * other tests as well (probably changing the method name).
   *
   * @param configureFun A BiConsumer that takes a TableConfigBuilder and a Schema.SchemaBuilder and modifies them
   *                   accordingly to what the test needs.
   */
  private void testPartialUpsertConfigNullability(BiConsumer<TableConfigBuilder, Schema.SchemaBuilder> configureFun) {
    Map<String, String> streamConfigs = getStreamConfigs();

    Map<String, UpsertConfig.Strategy> partialUpsertStratgies = new HashMap<>();
    partialUpsertStratgies.put("myTimeCol", UpsertConfig.Strategy.IGNORE);
    UpsertConfig partialUpsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    partialUpsertConfig.setPartialUpsertStrategies(partialUpsertStratgies);
    partialUpsertConfig.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.OVERWRITE);
    partialUpsertConfig.setComparisonColumn("myCol2");

    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol1", FieldSpec.DataType.LONG)
        .addSingleValueDimension("myCol2", FieldSpec.DataType.STRING)
        .addDateTime("myTimeCol", FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .setPrimaryKeyColumns(Lists.newArrayList("myCol1"));

    TableConfigBuilder tableConfigBuilder = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName("timeCol")
        .setUpsertConfig(partialUpsertConfig)
        .setNullHandlingEnabled(true)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setStreamConfigs(streamConfigs);

    configureFun.accept(tableConfigBuilder, schemaBuilder);

    TableConfig tableConfig = tableConfigBuilder.build();
    Schema schema = schemaBuilder.build();

    TableConfigUtils.validatePartialUpsertStrategies(tableConfig, schema);
  }

  /**
   * Tests that the partial upsert config fails when the null handling is disabled.
   *
   * This means that both table config and schema nullability is turned off.
   */
  @Test
  public void partialUpsertConfigFailWhenNotNullableColumns() {
    try {
      testPartialUpsertConfigNullability((tableConfigBuilder, schemaBuilder) -> {
        tableConfigBuilder.setNullHandlingEnabled(false);
        schemaBuilder.setEnableColumnBasedNullHandling(false);
      });
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Null handling must be enabled for partial upsert tables");
    }
  }

  /**
   * Tests that the partial upsert config succeeds when table null handling is used.
   *
   * This means that schema nullability is turned off, but table null handling is turned on.
   */
  @Test
  public void partialUpsertConfigSuccessWhenUsingTableLevelNullability() {
    testPartialUpsertConfigNullability((tableConfigBuilder, schemaBuilder) -> {
      tableConfigBuilder.setNullHandlingEnabled(true);
      schemaBuilder.setEnableColumnBasedNullHandling(false);
    });
  }

  /**
   * Tests that the partial upsert config succeeds when column null handling is used.
   *
   * This means that schema nullability is turned on, but table null handling can be either true or false.
   */
  @Test
  public void partialUpsertConfigSuccessWhenUsingColumnLevelNullability() {
    testPartialUpsertConfigNullability((tableConfigBuilder, schemaBuilder) -> {
      tableConfigBuilder.setNullHandlingEnabled(false);
      schemaBuilder.setEnableColumnBasedNullHandling(true);
    });

    testPartialUpsertConfigNullability((tableConfigBuilder, schemaBuilder) -> {
      tableConfigBuilder.setNullHandlingEnabled(true);
      schemaBuilder.setEnableColumnBasedNullHandling(true);
    });
  }

  @Test
  public void testValidateInstancePartitionsMap() {
    InstanceAssignmentConfig instanceAssignmentConfig = Mockito.mock(InstanceAssignmentConfig.class);

    TableConfig tableConfigWithoutInstancePartitionsMap =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();

    // Call validate with a table-config without any instance partitions or instance assignment config
    TableConfigUtils.validateInstancePartitionsTypeMapConfig(tableConfigWithoutInstancePartitionsMap);

    TableConfig tableConfigWithInstancePartitionsMap =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
            .setInstancePartitionsMap(ImmutableMap.of(InstancePartitionsType.OFFLINE, "test_OFFLINE"))
            .build();

    // Call validate with a table-config with instance partitions set but not instance assignment config
    TableConfigUtils.validateInstancePartitionsTypeMapConfig(tableConfigWithInstancePartitionsMap);

    TableConfig invalidTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInstancePartitionsMap(ImmutableMap.of(InstancePartitionsType.OFFLINE, "test_OFFLINE"))
        .setInstanceAssignmentConfigMap(
            ImmutableMap.of(InstancePartitionsType.OFFLINE.toString(), instanceAssignmentConfig))
        .build();
    try {
      // Call validate with instance partitions and config set for the same type
      TableConfigUtils.validateInstancePartitionsTypeMapConfig(invalidTableConfig);
      fail("Validation should have failed since both instancePartitionsMap and config are set");
    } catch (IllegalStateException ignored) {
    }
  }

  @Test
  public void testValidateHybridTableConfig() {
    TableConfig realtimeTableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).build();
    TableConfig offlineTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    try {
      // Call validate hybrid table which realtime/offline tables are missing timeColumn.
      TableConfigUtils.verifyHybridTableConfigs(TABLE_NAME, offlineTableConfig, realtimeTableConfig);
      fail();
    } catch (IllegalStateException ignored) {
      // Expected
    }

    realtimeTableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName("secondsSinceEpoch")
        .build();
    offlineTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTimeColumnName("secondssinceepoch")
        .build();
    try {
      // Call validate hybrid table which realtime table and offline table have different time columns.
      TableConfigUtils.verifyHybridTableConfigs(TABLE_NAME, offlineTableConfig, realtimeTableConfig);
      fail();
    } catch (IllegalStateException ignored) {
      // Expected
    }

    realtimeTableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName("secondsSinceEpoch")
        .setBrokerTenant("broker1")
        .build();
    offlineTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTimeColumnName("secondsSinceEpoch")
        .setBrokerTenant("broker2")
        .build();
    try {
      // Call validate hybrid table which realtime and offline table have different brokers.
      TableConfigUtils.verifyHybridTableConfigs(TABLE_NAME, offlineTableConfig, realtimeTableConfig);
      fail();
    } catch (IllegalArgumentException ignored) {
      // Expected
    }
  }

  @Test
  public void testValidateTTLConfigForUpsertConfig() {
    // Default comparison column (timestamp)
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(Lists.newArrayList("myCol"))
        .build();
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setMetadataTTL(3600);
    upsertConfig.setSnapshot(Enablement.ENABLE);
    TableConfig tableConfigWithoutComparisonColumn = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setUpsertConfig(upsertConfig)
        .build();
    TableConfigUtils.validateTTLForUpsertConfig(tableConfigWithoutComparisonColumn, schema);

    // Invalid comparison columns: "myCol"
    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setComparisonColumns(Collections.singletonList("myCol"));
    upsertConfig.setSnapshot(Enablement.ENABLE);
    upsertConfig.setMetadataTTL(3600);
    TableConfig tableConfigWithInvalidComparisonColumn =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
            .setTimeColumnName(TIME_COLUMN)
            .setUpsertConfig(upsertConfig)
            .build();
    try {
      TableConfigUtils.validateTTLForUpsertConfig(tableConfigWithInvalidComparisonColumn, schema);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Invalid comparison columns: multiple comparison columns are not supported for TTL-enabled upsert table.
    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setComparisonColumns(Lists.newArrayList(TIME_COLUMN, "myCol"));
    upsertConfig.setSnapshot(Enablement.ENABLE);
    upsertConfig.setMetadataTTL(3600);
    TableConfig tableConfigWithInvalidComparisonColumn2 =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
            .setTimeColumnName(TIME_COLUMN)
            .setUpsertConfig(upsertConfig)
            .build();
    try {
      TableConfigUtils.validateTTLForUpsertConfig(tableConfigWithInvalidComparisonColumn2, schema);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Invalid config with TTLConfig but Snapshot is not enabled
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(Lists.newArrayList("myCol"))
        .build();
    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setMetadataTTL(3600);
    upsertConfig.setSnapshot(Enablement.DISABLE);
    TableConfig tableConfigWithInvalidTTLConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setUpsertConfig(upsertConfig)
        .build();
    try {
      TableConfigUtils.validateTTLForUpsertConfig(tableConfigWithInvalidTTLConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Invalid STRING time column
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.STRING, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(Lists.newArrayList("myCol"))
        .build();
    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setMetadataTTL(3600);
    upsertConfig.setSnapshot(Enablement.ENABLE);
    tableConfigWithInvalidTTLConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setUpsertConfig(upsertConfig)
        .build();
    try {
      TableConfigUtils.validateTTLForUpsertConfig(tableConfigWithInvalidTTLConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Invalid TIMESTAMP time column
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(Lists.newArrayList("myCol"))
        .build();
    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setMetadataTTL(3600);
    upsertConfig.setSnapshot(Enablement.ENABLE);
    tableConfigWithInvalidTTLConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setUpsertConfig(upsertConfig)
        .build();
    try {
      TableConfigUtils.validateTTLForUpsertConfig(tableConfigWithInvalidTTLConfig, schema);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testValidatePartitionedReplicaGroupInstance() {
    String partitionColumn = "testPartitionCol";
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = new ReplicaGroupStrategyConfig(partitionColumn, 2);

    TableConfig tableConfigWithoutReplicaGroupStrategyConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    // Call validate with a table-config without replicaGroupStrategyConfig or replicaGroupPartitionConfig.
    TableConfigUtils.validatePartitionedReplicaGroupInstance(tableConfigWithoutReplicaGroupStrategyConfig);

    TableConfig tableConfigWithReplicaGroupStrategyConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    tableConfigWithReplicaGroupStrategyConfig.getValidationConfig()
        .setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);

    // Call validate with a table-config with replicaGroupStrategyConfig and without replicaGroupPartitionConfig.
    TableConfigUtils.validatePartitionedReplicaGroupInstance(tableConfigWithReplicaGroupStrategyConfig);

    InstanceAssignmentConfig instanceAssignmentConfig = Mockito.mock(InstanceAssignmentConfig.class);
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, 0, 0, 2, 0, false, partitionColumn);
    Mockito.doReturn(instanceReplicaGroupPartitionConfig)
        .when(instanceAssignmentConfig)
        .getReplicaGroupPartitionConfig();

    TableConfig invalidTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInstanceAssignmentConfigMap(ImmutableMap.of(TableType.OFFLINE.toString(), instanceAssignmentConfig))
        .build();
    invalidTableConfig.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);

    try {
      // Call validate with a table-config with replicaGroupStrategyConfig and replicaGroupPartitionConfig.
      TableConfigUtils.validatePartitionedReplicaGroupInstance(invalidTableConfig);
      fail("Validation should have failed since both replicaGroupStrategyConfig "
          + "and replicaGroupPartitionConfig are set");
    } catch (IllegalStateException ignored) {
    }
  }

  @Test
  public void testValidateImplicitRealtimeTablePartitionSelectorConfigs() {
    InstanceAssignmentConfig instanceAssignmentConfig = Mockito.mock(InstanceAssignmentConfig.class);
    when(instanceAssignmentConfig.getPartitionSelector()).thenReturn(
        InstanceAssignmentConfig.PartitionSelector.IMPLICIT_REALTIME_TABLE_PARTITION_SELECTOR);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInstanceAssignmentConfigMap(Map.of(InstancePartitionsType.CONSUMING.name(), instanceAssignmentConfig))
        .build();
    IllegalStateException e = expectThrows(IllegalStateException.class,
        () -> TableConfigUtils.validateInstanceAssignmentConfigs(tableConfig));
    assertTrue(
        e.getMessage().contains("IMPLICIT_REALTIME_TABLE_PARTITION_SELECTOR can only be used for REALTIME tables"));

    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        Mockito.mock(InstanceReplicaGroupPartitionConfig.class);
    when(instanceReplicaGroupPartitionConfig.isReplicaGroupBased()).thenReturn(true);
    when(instanceAssignmentConfig.getReplicaGroupPartitionConfig()).thenReturn(instanceReplicaGroupPartitionConfig);
    TableConfig tableConfig2 = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setInstanceAssignmentConfigMap(Map.of(InstancePartitionsType.COMPLETED.name(), instanceAssignmentConfig))
        .build();
    e = expectThrows(IllegalStateException.class,
        () -> TableConfigUtils.validateInstanceAssignmentConfigs(tableConfig2));
    assertTrue(e.getMessage()
        .contains(
            "IMPLICIT_REALTIME_TABLE_PARTITION_SELECTOR can only be used for CONSUMING instance partitions type"));

    when(instanceReplicaGroupPartitionConfig.isReplicaGroupBased()).thenReturn(false);
    when(instanceAssignmentConfig.getReplicaGroupPartitionConfig()).thenReturn(instanceReplicaGroupPartitionConfig);
    TableConfig tableConfig3 = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setInstanceAssignmentConfigMap(Map.of(InstancePartitionsType.CONSUMING.name(), instanceAssignmentConfig))
        .build();
    e = expectThrows(IllegalStateException.class,
        () -> TableConfigUtils.validateInstanceAssignmentConfigs(tableConfig3));
    assertTrue(e.getMessage()
        .contains("IMPLICIT_REALTIME_TABLE_PARTITION_SELECTOR can only be used with replica group based instance "
            + "assignment"));

    when(instanceReplicaGroupPartitionConfig.isReplicaGroupBased()).thenReturn(true);
    when(instanceReplicaGroupPartitionConfig.getNumPartitions()).thenReturn(1);
    TableConfig tableConfig4 = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setInstanceAssignmentConfigMap(Map.of(InstancePartitionsType.CONSUMING.name(), instanceAssignmentConfig))
        .build();
    e = expectThrows(IllegalStateException.class,
        () -> TableConfigUtils.validateInstanceAssignmentConfigs(tableConfig4));
    assertTrue(e.getMessage()
        .contains("numPartitions should not be explicitly set when using IMPLICIT_REALTIME_TABLE_PARTITION_SELECTOR"));

    when(instanceReplicaGroupPartitionConfig.isReplicaGroupBased()).thenReturn(true);
    when(instanceReplicaGroupPartitionConfig.getNumPartitions()).thenReturn(0);
    when(instanceReplicaGroupPartitionConfig.getNumInstancesPerPartition()).thenReturn(2);
    TableConfig tableConfig5 = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setInstanceAssignmentConfigMap(Map.of(InstancePartitionsType.CONSUMING.name(), instanceAssignmentConfig))
        .build();
    e = expectThrows(IllegalStateException.class,
        () -> TableConfigUtils.validateInstanceAssignmentConfigs(tableConfig5));
    assertTrue(e.getMessage()
        .contains("numInstancesPerPartition must be 1 when using IMPLICIT_REALTIME_TABLE_PARTITION_SELECTOR"));

    when(instanceReplicaGroupPartitionConfig.getNumPartitions()).thenReturn(0);
    when(instanceReplicaGroupPartitionConfig.getNumInstancesPerPartition()).thenReturn(0);
    TableConfig tableConfig6 = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setInstanceAssignmentConfigMap(Map.of(InstancePartitionsType.CONSUMING.name(), instanceAssignmentConfig))
        .build();
    TableConfigUtils.validateInstanceAssignmentConfigs(tableConfig6);
  }

  private Map<String, String> getStreamConfigs() {
    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put("streamType", "kafka");
    streamConfigs.put("stream.kafka.topic.name", "test");
    streamConfigs.put("stream.kafka.decoder.class.name",
        "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder");
    return streamConfigs;
  }

  private Map<String, String> getKafkaStreamConfigs() {
    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put("streamType", "kafka");
    streamConfigs.put("stream.kafka.topic.name", "test");
    streamConfigs.put("stream.kafka.decoder.class.name",
        "org.apache.pinot.plugin.inputformat.protobuf.ProtoBufMessageDecoder");
    streamConfigs.put("stream.kafka.decoder.prop.descriptorFile", "file://test");
    streamConfigs.put("stream.kafka.decoder.prop.protoClassName", "test");
    return streamConfigs;
  }

  private Map<String, String> getPulsarStreamConfigs() {
    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put("streamType", "pulsar");
    streamConfigs.put("stream.pulsar.topic.name", "test");
    streamConfigs.put("stream.pulsar.decoder.prop.descriptorFile", "file://test");
    streamConfigs.put("stream.pulsar.decoder.prop.protoClassName", "test");
    streamConfigs.put("stream.pulsar.decoder.class.name",
        "org.apache.pinot.plugin.inputformat.protobuf.ProtoBufMessageDecoder");
    return streamConfigs;
  }

  @Test
  public void testValidIGnRGOfflineTable() {
    InstanceAssignmentConfig config =
        new InstanceAssignmentConfig(new InstanceTagPoolConfig("DefaultTenant", true, 0, null), null,
            new InstanceReplicaGroupPartitionConfig(true, 0, 0, 0, 0, 0, false, null), null, false);

    TableConfig tableConfig =
        new TableConfig("table", TableType.OFFLINE.name(), new SegmentsValidationAndRetentionConfig(),
            new TenantConfig("DefaultTenant", "DefaultTenant", null), new IndexingConfig(), new TableCustomConfig(null),
            null, null, null, null, Map.of("OFFLINE", config), null, null, null, null, null, null, false, null, null,
            null);

    assertTrue(TableConfigUtils.isTableUsingInstancePoolAndReplicaGroup(tableConfig));
  }

  @Test
  public void testValidIGnRGRealtimeTable() {
    InstanceAssignmentConfig config =
        new InstanceAssignmentConfig(new InstanceTagPoolConfig("DefaultTenant", true, 0, null), null,
            new InstanceReplicaGroupPartitionConfig(true, 0, 0, 0, 0, 0, false, null), null, false);

    TableConfig tableConfig =
        new TableConfig("table", TableType.REALTIME.name(), new SegmentsValidationAndRetentionConfig(),
            new TenantConfig("DefaultTenant", "DefaultTenant", null), new IndexingConfig(), new TableCustomConfig(null),
            null, null, null, null, Map.of("CONSUMING", config), null, null, null, null, null, null, false, null, null,
            null);

    assertTrue(TableConfigUtils.isTableUsingInstancePoolAndReplicaGroup(tableConfig));
  }

  @Test
  public void testNoIACOfflineTable() {
    TableConfig tableConfig =
        new TableConfig("table", TableType.OFFLINE.name(), new SegmentsValidationAndRetentionConfig(),
            new TenantConfig("DefaultTenant", "DefaultTenant", null), new IndexingConfig(), new TableCustomConfig(null),
            null, null, null, null, null, null, null, null, null, null, null, false, null, null, null);

    assertFalse(TableConfigUtils.isTableUsingInstancePoolAndReplicaGroup(tableConfig));
  }

  @Test
  public void testNoIACRealtimeTable() {
    TableConfig tableConfig =
        new TableConfig("table", TableType.REALTIME.name(), new SegmentsValidationAndRetentionConfig(),
            new TenantConfig("DefaultTenant", "DefaultTenant", null), new IndexingConfig(), new TableCustomConfig(null),
            null, null, null, null, null, null, null, null, null, null, null, false, null, null, null);

    assertFalse(TableConfigUtils.isTableUsingInstancePoolAndReplicaGroup(tableConfig));
  }

  @Test
  public void testNoPoolsOfflineTable() {
    InstanceAssignmentConfig config =
        new InstanceAssignmentConfig(new InstanceTagPoolConfig("DefaultTenant", false, 0, null), null,
            new InstanceReplicaGroupPartitionConfig(true, 0, 0, 0, 0, 0, false, null), null, false);

    TableConfig tableConfig =
        new TableConfig("table", TableType.OFFLINE.name(), new SegmentsValidationAndRetentionConfig(),
            new TenantConfig("DefaultTenant", "DefaultTenant", null), new IndexingConfig(), new TableCustomConfig(null),
            null, null, null, null, Map.of("OFFLINE", config), null, null, null, null, null, null, false, null, null,
            null);

    assertFalse(TableConfigUtils.isTableUsingInstancePoolAndReplicaGroup(tableConfig));
  }

  @Test
  public void testNoPoolsRealtimeTable() {
    InstanceAssignmentConfig config =
        new InstanceAssignmentConfig(new InstanceTagPoolConfig("DefaultTenant", false, 0, null), null,
            new InstanceReplicaGroupPartitionConfig(true, 0, 0, 0, 0, 0, false, null), null, false);

    TableConfig tableConfig =
        new TableConfig("table", TableType.REALTIME.name(), new SegmentsValidationAndRetentionConfig(),
            new TenantConfig("DefaultTenant", "DefaultTenant", null), new IndexingConfig(), new TableCustomConfig(null),
            null, null, null, null, Map.of("CONSUMING", config), null, null, null, null, null, null, false, null, null,
            null);

    assertFalse(TableConfigUtils.isTableUsingInstancePoolAndReplicaGroup(tableConfig));
  }

  @Test
  public void testNoRgOfflineTable() {
    InstanceAssignmentConfig config =
        new InstanceAssignmentConfig(new InstanceTagPoolConfig("DefaultTenant", true, 0, null), null,
            new InstanceReplicaGroupPartitionConfig(false, 0, 0, 0, 0, 0, false, null), null, false);

    TableConfig tableConfig =
        new TableConfig("table", TableType.OFFLINE.name(), new SegmentsValidationAndRetentionConfig(),
            new TenantConfig("DefaultTenant", "DefaultTenant", null), new IndexingConfig(), new TableCustomConfig(null),
            null, null, null, null, Map.of("OFFLINE", config), null, null, null, null, null, null, false, null, null,
            null);

    assertFalse(TableConfigUtils.isTableUsingInstancePoolAndReplicaGroup(tableConfig));
  }

  @Test
  public void testNoRGRealtimeTable() {
    InstanceAssignmentConfig config =
        new InstanceAssignmentConfig(new InstanceTagPoolConfig("DefaultTenant", true, 0, null), null,
            new InstanceReplicaGroupPartitionConfig(false, 0, 0, 0, 0, 0, false, null), null, false);

    TableConfig tableConfig =
        new TableConfig("table", TableType.REALTIME.name(), new SegmentsValidationAndRetentionConfig(),
            new TenantConfig("DefaultTenant", "DefaultTenant", null), new IndexingConfig(), new TableCustomConfig(null),
            null, null, null, null, Map.of("CONSUMING", config), null, null, null, null, null, null, false, null, null,
            null);

    assertFalse(TableConfigUtils.isTableUsingInstancePoolAndReplicaGroup(tableConfig));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testConvertFromLegacyTableConfig() {
    String expectedPushFrequency = "HOURLY";
    String expectedPushType = "APPEND";

    Map<String, String> expectedStreamConfigsMap = getTestStreamConfigs();
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setSegmentPushFrequency(expectedPushFrequency)
        .setSegmentPushType(expectedPushType)
        .setStreamConfigs(expectedStreamConfigsMap)
        .build();

    // Before conversion, the ingestion config should be null.
    assertNull(tableConfig.getIngestionConfig());

    // Perform conversion.
    TableConfigUtils.convertFromLegacyTableConfig(tableConfig);

    // After conversion, assert that the configs are transferred ingestionConfig.
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    assertNotNull(ingestionConfig);
    BatchIngestionConfig batchIngestionConfig = ingestionConfig.getBatchIngestionConfig();
    assertNotNull(batchIngestionConfig);
    assertEquals(batchIngestionConfig.getSegmentIngestionFrequency(), expectedPushFrequency);
    assertEquals(batchIngestionConfig.getSegmentIngestionType(), expectedPushType);

    StreamIngestionConfig streamIngestionConfig = ingestionConfig.getStreamIngestionConfig();
    assertNotNull(streamIngestionConfig);
    Map<String, String> actualStreamConfigsMap = streamIngestionConfig.getStreamConfigMaps().get(0);
    assertEquals(actualStreamConfigsMap, expectedStreamConfigsMap);

    // Assert that the deprecated fields are cleared.
    assertNull(tableConfig.getIndexingConfig().getStreamConfigs());

    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    assertNull(validationConfig.getSegmentPushFrequency());
    assertNull(validationConfig.getSegmentPushType());
  }

  private Map<String, String> getTestStreamConfigs() {
    String streamType = "testStream";
    String topic = "testTopic";
    String consumerFactoryClass = TestStreamConsumerFactory.class.getName();
    String decoderClass = TestStreamMessageDecoder.class.getName();

    // All mandatory properties set
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, "streamType");
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), consumerFactoryClass);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        decoderClass);

    return streamConfigMap;
  }

  private static class TestStreamMessageDecoder implements StreamMessageDecoder<byte[]> {

    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName) {
    }

    @Nullable
    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      return null;
    }

    @Nullable
    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      return null;
    }
  }

  private static class TestStreamConsumerFactory extends StreamConsumerFactory {

    @Override
    public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
      return null;
    }

    @Override
    public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
      return null;
    }

    @Override
    public PartitionGroupConsumer createPartitionGroupConsumer(String clientId,
        PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
      return null;
    }
  }

  @Test
  public void testOverwriteTableConfigForTier()
      throws Exception {
    String col1CfgStr = "{"
        + "  \"name\": \"col1\","
        + "  \"encodingType\": \"DICTIONARY\","
        + "  \"indexes\": {"
        + "    \"bloom\": {\"enabled\": \"true\"}"
        + "  },"
        + "  \"tierOverwrites\": {"
        + "    \"coldTier\": {"
        + "      \"encodingType\": \"RAW\","
        + "      \"indexes\": {}"
        + "    }"
        + "  }"
        + "}";
    FieldConfig col2Cfg = JsonUtils.stringToObject(col1CfgStr, FieldConfig.class);
    String stIdxCfgStr = "{"
        + "  \"dimensionsSplitOrder\": [\"col1\"],"
        + "  \"functionColumnPairs\": [\"MAX__col1\"],"
        + "  \"maxLeafRecords\": 10"
        + "}";
    StarTreeIndexConfig stIdxCfg = JsonUtils.stringToObject(stIdxCfgStr, StarTreeIndexConfig.class);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(Collections.singletonList(stIdxCfg))
        .setTierOverwrites(JsonUtils.stringToJsonNode("{\"coldTier\": {\"starTreeIndexConfigs\": []}}"))
        .setFieldConfigList(Collections.singletonList(col2Cfg))
        .build();

    TableConfig tierTblCfg = TableConfigUtils.overwriteTableConfigForTier(tableConfig, "unknownTier");
    assertEquals(tierTblCfg, tableConfig);
    tierTblCfg = TableConfigUtils.overwriteTableConfigForTier(tableConfig, null);
    assertEquals(tierTblCfg, tableConfig);
    // Check original TableConfig and tier specific TableConfig
    assertEquals(tierTblCfg.getFieldConfigList().get(0).getEncodingType(), FieldConfig.EncodingType.DICTIONARY);
    assertEquals(tierTblCfg.getFieldConfigList().get(0).getIndexes().size(), 1);
    assertEquals(tierTblCfg.getIndexingConfig().getStarTreeIndexConfigs().size(), 1);
    tierTblCfg = TableConfigUtils.overwriteTableConfigForTier(tableConfig, "coldTier");
    assertEquals(tierTblCfg.getFieldConfigList().get(0).getEncodingType(), FieldConfig.EncodingType.RAW);
    assertEquals(tierTblCfg.getFieldConfigList().get(0).getIndexes().size(), 0);
    assertEquals(tierTblCfg.getIndexingConfig().getStarTreeIndexConfigs().size(), 0);
  }

  @Test
  public void testOverwriteTableConfigForTierWithError()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTierOverwrites(JsonUtils.stringToJsonNode("{\"coldTier\": {\"starTreeIndexConfigs\": {}}}"))
        .build();
    TableConfig tierTblCfg = TableConfigUtils.overwriteTableConfigForTier(tableConfig, "coldTier");
    assertEquals(tierTblCfg, tableConfig);
  }

  @Test
  public void testGetPartitionColumnWithoutAnyConfig() {
    // without instanceAssignmentConfigMap
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).build();
    assertNull(TableConfigUtils.getPartitionColumn(tableConfig));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testGetPartitionColumnWithReplicaGroupConfig() {
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = new ReplicaGroupStrategyConfig(PARTITION_COLUMN, 1);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).build();

    // setting up ReplicaGroupStrategyConfig for backward compatibility test.
    SegmentsValidationAndRetentionConfig validationConfig = new SegmentsValidationAndRetentionConfig();
    validationConfig.setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);
    tableConfig.setValidationConfig(validationConfig);

    assertEquals(TableConfigUtils.getPartitionColumn(tableConfig), PARTITION_COLUMN);
  }
}
