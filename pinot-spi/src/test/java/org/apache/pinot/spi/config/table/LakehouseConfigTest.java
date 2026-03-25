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
package org.apache.pinot.spi.config.table;

import java.util.Arrays;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class LakehouseConfigTest {

  @Test
  public void testBasicLakehouseConfig()
      throws Exception {
    IcebergCatalogConfig catalogConfig =
        new IcebergCatalogConfig(IcebergCatalogConfig.CatalogType.REST, "https://catalog.example.com",
            "s3://warehouse", "analytics.events", null);

    LakehouseConfig config =
        new LakehouseConfig(true, LakehouseConfig.Mode.ICEBERG_NATIVE, catalogConfig, null, null, null, null);

    assertTrue(config.isEnabled());
    assertEquals(config.getMode(), LakehouseConfig.Mode.ICEBERG_NATIVE);
    assertNotNull(config.getCatalog());
    assertNull(config.getRead());
    assertNull(config.getWrite());
    assertNull(config.getTablet());
    assertNull(config.getSidecars());
  }

  @Test
  public void testFullLakehouseConfig()
      throws Exception {
    IcebergCatalogConfig catalogConfig =
        new IcebergCatalogConfig(IcebergCatalogConfig.CatalogType.REST, "https://catalog.example.com",
            "s3://warehouse", "analytics.events", Map.of("token", "secret"));

    LakehouseReadConfig readConfig =
        new LakehouseReadConfig(LakehouseConfig.VisibilityMode.HOT_PLUS_SNAPSHOT, LakehouseConfig.RefType.BRANCH,
            "main");

    LakehouseWriteConfig writeConfig =
        new LakehouseWriteConfig(true, LakehouseConfig.WriteMode.APPEND, "PARQUET");

    TabletConfig tabletConfig = new TabletConfig(128, 68719476736L, "DAY");

    SidecarConfig sidecarConfig = new SidecarConfig("s3://pinot-sidecars",
        Arrays.asList(SidecarConfig.SidecarIndexType.INVERTED, SidecarConfig.SidecarIndexType.RANGE));

    LakehouseConfig config =
        new LakehouseConfig(true, LakehouseConfig.Mode.ICEBERG_NATIVE, catalogConfig, readConfig, writeConfig,
            tabletConfig, sidecarConfig);

    assertTrue(config.isEnabled());
    assertEquals(config.getMode(), LakehouseConfig.Mode.ICEBERG_NATIVE);
    assertEquals(config.getCatalog().getType(), IcebergCatalogConfig.CatalogType.REST);
    assertEquals(config.getCatalog().getUri(), "https://catalog.example.com");
    assertEquals(config.getCatalog().getWarehouse(), "s3://warehouse");
    assertEquals(config.getCatalog().getTableIdentifier(), "analytics.events");
    assertEquals(config.getCatalog().getProperties().get("token"), "secret");

    assertEquals(config.getRead().getVisibilityMode(), LakehouseConfig.VisibilityMode.HOT_PLUS_SNAPSHOT);
    assertEquals(config.getRead().getDefaultRefType(), LakehouseConfig.RefType.BRANCH);
    assertEquals(config.getRead().getDefaultRefName(), "main");

    assertTrue(config.getWrite().isEnabled());
    assertEquals(config.getWrite().getMode(), LakehouseConfig.WriteMode.APPEND);
    assertEquals(config.getWrite().getFileFormat(), "PARQUET");

    assertEquals(config.getTablet().getTargetFilesPerTablet(), 128);
    assertEquals(config.getTablet().getTargetBytesPerTablet(), 68719476736L);
    assertEquals(config.getTablet().getTimeBucket(), "DAY");

    assertEquals(config.getSidecars().getStoreURI(), "s3://pinot-sidecars");
    assertEquals(config.getSidecars().getIndexes().size(), 2);
  }

  @Test
  public void testJsonSerializationRoundTrip()
      throws Exception {
    IcebergCatalogConfig catalogConfig =
        new IcebergCatalogConfig(IcebergCatalogConfig.CatalogType.REST, "https://catalog.example.com",
            "s3://warehouse", "analytics.events", null);
    TabletConfig tabletConfig = new TabletConfig(64, null, "HOUR");
    LakehouseConfig config =
        new LakehouseConfig(true, LakehouseConfig.Mode.ICEBERG_NATIVE, catalogConfig, null, null, tabletConfig, null);

    String json = JsonUtils.objectToString(config);
    LakehouseConfig deserialized = JsonUtils.stringToObject(json, LakehouseConfig.class);

    assertTrue(deserialized.isEnabled());
    assertEquals(deserialized.getMode(), LakehouseConfig.Mode.ICEBERG_NATIVE);
    assertEquals(deserialized.getCatalog().getTableIdentifier(), "analytics.events");
    assertEquals(deserialized.getTablet().getTargetFilesPerTablet(), 64);
    assertEquals(deserialized.getTablet().getTimeBucket(), "HOUR");
  }

  @Test
  public void testReadConfigDefaults() {
    LakehouseReadConfig readConfig = new LakehouseReadConfig(null, null, null);
    assertEquals(readConfig.getVisibilityMode(), LakehouseConfig.VisibilityMode.HOT_PLUS_SNAPSHOT);
    assertEquals(readConfig.getDefaultRefType(), LakehouseConfig.RefType.BRANCH);
    assertEquals(readConfig.getDefaultRefName(), "main");
  }

  @Test
  public void testWriteConfigDefaults() {
    LakehouseWriteConfig writeConfig = new LakehouseWriteConfig(false, null, null);
    assertFalse(writeConfig.isEnabled());
    assertEquals(writeConfig.getMode(), LakehouseConfig.WriteMode.APPEND);
    assertEquals(writeConfig.getFileFormat(), "PARQUET");
  }

  @Test
  public void testTabletConfigDefaults() {
    TabletConfig tabletConfig = new TabletConfig(null, null, null);
    assertEquals(tabletConfig.getTargetFilesPerTablet(), TabletConfig.DEFAULT_TARGET_FILES_PER_TABLET);
    assertEquals(tabletConfig.getTargetBytesPerTablet(), TabletConfig.DEFAULT_TARGET_BYTES_PER_TABLET);
    assertEquals(tabletConfig.getTimeBucket(), TabletConfig.DEFAULT_TIME_BUCKET);
  }

  @Test
  public void testDisabledConfig() {
    IcebergCatalogConfig catalogConfig =
        new IcebergCatalogConfig(IcebergCatalogConfig.CatalogType.REST, null, null, "db.table", null);
    LakehouseConfig config =
        new LakehouseConfig(false, LakehouseConfig.Mode.ICEBERG_NATIVE, catalogConfig, null, null, null, null);
    assertFalse(config.isEnabled());
  }

  @Test
  public void testCatalogTypes() {
    for (IcebergCatalogConfig.CatalogType type : IcebergCatalogConfig.CatalogType.values()) {
      IcebergCatalogConfig config = new IcebergCatalogConfig(type, null, null, "db.table", null);
      assertEquals(config.getType(), type);
    }
  }

  @Test
  public void testIgnoreUnknownFieldsOnDeserialization()
      throws Exception {
    // Verify forward compatibility: unknown fields are ignored
    String json = "{\"enabled\":true,\"mode\":\"ICEBERG_NATIVE\","
        + "\"catalog\":{\"type\":\"REST\",\"tableIdentifier\":\"db.t\",\"futureField\":\"value\"},"
        + "\"futureTopLevel\":123}";
    LakehouseConfig config = JsonUtils.stringToObject(json, LakehouseConfig.class);
    assertTrue(config.isEnabled());
    assertEquals(config.getCatalog().getTableIdentifier(), "db.t");
  }

  @Test
  public void testTableConfigWithLakehouse()
      throws Exception {
    IcebergCatalogConfig catalogConfig =
        new IcebergCatalogConfig(IcebergCatalogConfig.CatalogType.REST, "https://catalog.example.com",
            "s3://warehouse", "analytics.events", null);
    LakehouseConfig lakehouseConfig =
        new LakehouseConfig(true, LakehouseConfig.Mode.ICEBERG_NATIVE, catalogConfig, null, null, null, null);

    TableConfig tableConfig =
        new org.apache.pinot.spi.utils.builder.TableConfigBuilder(TableType.OFFLINE).setTableName("events")
            .setLakehouseConfig(lakehouseConfig).build();

    assertTrue(tableConfig.isLakehouseEnabled());
    assertNotNull(tableConfig.getLakehouseConfig());
    assertEquals(tableConfig.getLakehouseConfig().getCatalog().getTableIdentifier(), "analytics.events");

    // Round-trip through JSON
    String json = tableConfig.toJsonString();
    TableConfig deserialized = JsonUtils.stringToObject(json, TableConfig.class);
    assertTrue(deserialized.isLakehouseEnabled());
    assertEquals(deserialized.getLakehouseConfig().getMode(), LakehouseConfig.Mode.ICEBERG_NATIVE);
  }

  @Test
  public void testTableConfigWithoutLakehouse()
      throws Exception {
    TableConfig tableConfig =
        new org.apache.pinot.spi.utils.builder.TableConfigBuilder(TableType.OFFLINE).setTableName("regular").build();

    assertFalse(tableConfig.isLakehouseEnabled());
    assertNull(tableConfig.getLakehouseConfig());
  }
}
