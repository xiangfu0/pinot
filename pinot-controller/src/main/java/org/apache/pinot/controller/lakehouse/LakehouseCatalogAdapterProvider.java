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
package org.apache.pinot.controller.lakehouse;

import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.lakehouse.IcebergCatalogConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.lakehouse.LakehouseCatalogAdapter;
import org.apache.pinot.spi.plugin.PluginManager;


/**
 * Resolves the controller-side {@link LakehouseCatalogAdapter} implementation for a lakehouse table.
 *
 * <p>Phase 1 keeps the resolution rule intentionally small: the controller accepts either a fully qualified adapter
 * class name in {@code lakehouseConfig.catalogConfig.pluginName} or in
 * {@code lakehouseConfig.catalogConfig.properties.controller.adapter.class}. This keeps the controller adapter-based
 * without forcing Iceberg dependencies into the controller module.</p>
 */
public class LakehouseCatalogAdapterProvider {
  public static final String ADAPTER_CLASS_PROPERTY_KEY = "controller.adapter.class";

  public LakehouseCatalogAdapter getAdapter(LakehouseConfig lakehouseConfig) {
    String adapterClassName = resolveAdapterClassName(lakehouseConfig);
    if (adapterClassName == null) {
      throw new IllegalStateException("No controller lakehouse catalog adapter class is configured. Set "
          + "lakehouseConfig.catalogConfig.pluginName to a fully qualified class name or set "
          + "lakehouseConfig.catalogConfig.properties." + ADAPTER_CLASS_PROPERTY_KEY);
    }

    try {
      return PluginManager.get().createInstance(adapterClassName);
    } catch (Exception e) {
      try {
        return (LakehouseCatalogAdapter) Class.forName(adapterClassName).getDeclaredConstructor().newInstance();
      } catch (Exception fallbackException) {
        fallbackException.addSuppressed(e);
        throw new IllegalStateException("Failed to instantiate lakehouse catalog adapter: " + adapterClassName,
            fallbackException);
      }
    }
  }

  private String resolveAdapterClassName(LakehouseConfig lakehouseConfig) {
    IcebergCatalogConfig catalogConfig = lakehouseConfig.getCatalogConfig();
    if (catalogConfig == null) {
      return null;
    }

    if (catalogConfig.getProperties() != null) {
      String adapterClassName = StringUtils.trimToNull(catalogConfig.getProperties().get(ADAPTER_CLASS_PROPERTY_KEY));
      if (adapterClassName != null) {
        return adapterClassName;
      }
    }

    String pluginName = StringUtils.trimToNull(catalogConfig.getPluginName());
    if (pluginName != null && pluginName.contains(".")) {
      return pluginName;
    }
    return null;
  }
}
