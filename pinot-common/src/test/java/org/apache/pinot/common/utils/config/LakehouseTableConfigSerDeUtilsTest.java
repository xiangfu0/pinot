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
package org.apache.pinot.common.utils.config;

import java.io.IOException;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.lakehouse.IcebergCatalogConfig;
import org.apache.pinot.spi.config.table.lakehouse.IcebergCatalogType;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseMode;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseReadVisibilityMode;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseTabletConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseWriteMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class LakehouseTableConfigSerDeUtilsTest {
  @Test
  public void testLakehouseConfigZnRecordRoundTrip()
      throws IOException {
    IcebergCatalogConfig catalogConfig = new IcebergCatalogConfig();
    catalogConfig.setCatalogType(IcebergCatalogType.FILESYSTEM);
    catalogConfig.setWarehouseUri("file:///tmp/warehouse");
    catalogConfig.setTableIdentifier("db.table");
    catalogConfig.setProperties(Map.of("root", "/tmp/warehouse"));

    LakehouseTabletConfig tabletConfig = new LakehouseTabletConfig();
    tabletConfig.setTargetFilesPerTablet(64);
    tabletConfig.setTargetBytesPerTablet(4_294_967_296L);

    LakehouseConfig lakehouseConfig = new LakehouseConfig();
    lakehouseConfig.setEnabled(true);
    lakehouseConfig.setMode(LakehouseMode.ICEBERG_NATIVE);
    lakehouseConfig.setCatalogConfig(catalogConfig);
    lakehouseConfig.setReadVisibilityMode(LakehouseReadVisibilityMode.SNAPSHOT_ONLY);
    lakehouseConfig.setWriteMode(LakehouseWriteMode.DISABLED);
    lakehouseConfig.setTabletConfig(tabletConfig);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("lakehouseSerde")
        .setLakehouseConfig(lakehouseConfig)
        .build();

    TableConfig roundTrip = TableConfigSerDeUtils.fromZNRecord(TableConfigSerDeUtils.toZNRecord(tableConfig));
    assertEquals(roundTrip, tableConfig);
    assertNotNull(roundTrip.getLakehouseConfig());
    assertEquals(roundTrip.getLakehouseConfig().getCatalogConfig().getCatalogType(), IcebergCatalogType.FILESYSTEM);
    assertEquals(roundTrip.getLakehouseConfig().getTabletConfig().getTargetFilesPerTablet(), 64);
  }

  @Test
  public void testLakehouseConfigAbsentFromZnRecordForLegacyTable()
      throws IOException {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("legacySerde")
        .build();

    TableConfig roundTrip = TableConfigSerDeUtils.fromZNRecord(TableConfigSerDeUtils.toZNRecord(tableConfig));
    assertNull(roundTrip.getLakehouseConfig());
  }
}
