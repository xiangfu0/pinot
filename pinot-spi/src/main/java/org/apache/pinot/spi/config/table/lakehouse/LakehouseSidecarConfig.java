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
package org.apache.pinot.spi.config.table.lakehouse;

import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Declares optional Pinot-managed sidecar acceleration settings for lakehouse-native tables.
 */
public class LakehouseSidecarConfig extends BaseJsonConfig {
  private boolean _enabled;
  private boolean _failOpen = true;
  private String _provider;
  private String _storageUri;

  public boolean isEnabled() {
    return _enabled;
  }

  public void setEnabled(boolean enabled) {
    _enabled = enabled;
  }

  public boolean isFailOpen() {
    return _failOpen;
  }

  public void setFailOpen(boolean failOpen) {
    _failOpen = failOpen;
  }

  @Nullable
  public String getProvider() {
    return _provider;
  }

  public void setProvider(@Nullable String provider) {
    _provider = provider;
  }

  @Nullable
  public String getStorageUri() {
    return _storageUri;
  }

  public void setStorageUri(@Nullable String storageUri) {
    _storageUri = storageUri;
  }
}
