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
package org.apache.pinot.controller.api.resources;

import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.lakehouse.IcebergCatalogConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseMode;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseTabletConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseWriteMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests the controller-only Phase 1 lakehouse validation rules.
 */
public class TableConfigValidationUtilsTest {

  @Test
  public void shouldIgnoreDisabledLakehouseConfig() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("table").build();

    TableConfigValidationUtils.validateLakehouseNativeConfig(tableConfig);
  }

  @Test
  public void shouldRejectLakehouseRealtimeTableConfig() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("table")
        .setLakehouseConfig(enabledLakehouseConfig())
        .build();

    IllegalStateException exception =
        Assert.expectThrows(IllegalStateException.class, () -> TableConfigValidationUtils.validateLakehouseNativeConfig(
            tableConfig));
    Assert.assertTrue(exception.getMessage().contains("Phase 1 lakehouse-native validation failed"));
    Assert.assertTrue(exception.getMessage().contains("only OFFLINE tables are supported"));
  }

  @Test
  public void shouldRejectUnsupportedLakehouseMode() {
    LakehouseConfig lakehouseConfig = enabledLakehouseConfig();
    lakehouseConfig.setMode(null);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("table")
        .setLakehouseConfig(lakehouseConfig)
        .build();

    IllegalStateException exception =
        Assert.expectThrows(IllegalStateException.class, () -> TableConfigValidationUtils.validateLakehouseNativeConfig(
            tableConfig));
    Assert.assertTrue(exception.getMessage().contains("lakehouseConfig.mode must be ICEBERG_NATIVE"));
  }

  @Test
  public void shouldRejectMissingLakehouseCatalogConfig() {
    LakehouseConfig lakehouseConfig = enabledLakehouseConfig();
    lakehouseConfig.setCatalogConfig(null);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("table")
        .setLakehouseConfig(lakehouseConfig)
        .build();

    IllegalStateException exception =
        Assert.expectThrows(IllegalStateException.class, () -> TableConfigValidationUtils.validateLakehouseNativeConfig(
            tableConfig));
    Assert.assertTrue(exception.getMessage().contains("lakehouseConfig.catalogConfig is required"));
  }

  @Test
  public void shouldRejectMissingLakehouseTableIdentifier() {
    LakehouseConfig lakehouseConfig = enabledLakehouseConfig();
    lakehouseConfig.getCatalogConfig().setTableIdentifier("   ");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("table")
        .setLakehouseConfig(lakehouseConfig)
        .build();

    IllegalStateException exception =
        Assert.expectThrows(IllegalStateException.class, () -> TableConfigValidationUtils.validateLakehouseNativeConfig(
            tableConfig));
    Assert.assertTrue(exception.getMessage().contains("lakehouseConfig.catalogConfig.tableIdentifier is required"));
  }

  @Test
  public void shouldRejectNonDisabledWriteMode() {
    LakehouseConfig lakehouseConfig = enabledLakehouseConfig();
    lakehouseConfig.setWriteMode(LakehouseWriteMode.DIRECT_APPEND);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("table")
        .setLakehouseConfig(lakehouseConfig)
        .build();

    IllegalStateException exception =
        Assert.expectThrows(IllegalStateException.class, () -> TableConfigValidationUtils.validateLakehouseNativeConfig(
            tableConfig));
    Assert.assertTrue(exception.getMessage().contains("lakehouseConfig.writeMode must remain DISABLED"));
  }

  @Test
  public void shouldRejectInvalidTabletSizing() {
    LakehouseConfig lakehouseConfig = enabledLakehouseConfig();
    LakehouseTabletConfig tabletConfig = new LakehouseTabletConfig();
    tabletConfig.setTargetFilesPerTablet(0);
    lakehouseConfig.setTabletConfig(tabletConfig);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("table")
        .setLakehouseConfig(lakehouseConfig)
        .build();

    IllegalStateException exception =
        Assert.expectThrows(IllegalStateException.class, () -> TableConfigValidationUtils.validateLakehouseNativeConfig(
            tableConfig));
    Assert.assertTrue(exception.getMessage().contains("targetFilesPerTablet must be positive"));
  }

  private static LakehouseConfig enabledLakehouseConfig() {
    LakehouseConfig lakehouseConfig = new LakehouseConfig();
    lakehouseConfig.setEnabled(true);
    lakehouseConfig.setMode(LakehouseMode.ICEBERG_NATIVE);
    IcebergCatalogConfig catalogConfig = new IcebergCatalogConfig();
    catalogConfig.setTableIdentifier("db.table");
    lakehouseConfig.setCatalogConfig(catalogConfig);
    lakehouseConfig.setWriteMode(LakehouseWriteMode.DISABLED);
    return lakehouseConfig;
  }
}
