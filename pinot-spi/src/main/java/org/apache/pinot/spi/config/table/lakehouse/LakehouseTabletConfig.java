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
 * Configures how Iceberg files are grouped into Pinot tablets for routing and assignment.
 */
public class LakehouseTabletConfig extends BaseJsonConfig {
  private int _targetFilesPerTablet = 128;
  private long _targetBytesPerTablet = 32L * 1024 * 1024 * 1024;
  private String _timePartitionColumn;
  private String _timePartitionBucket;
  private int _maxEnvelopeBytes = 16 * 1024;

  public int getTargetFilesPerTablet() {
    return _targetFilesPerTablet;
  }

  public void setTargetFilesPerTablet(int targetFilesPerTablet) {
    _targetFilesPerTablet = targetFilesPerTablet;
  }

  public long getTargetBytesPerTablet() {
    return _targetBytesPerTablet;
  }

  public void setTargetBytesPerTablet(long targetBytesPerTablet) {
    _targetBytesPerTablet = targetBytesPerTablet;
  }

  @Nullable
  public String getTimePartitionColumn() {
    return _timePartitionColumn;
  }

  public void setTimePartitionColumn(@Nullable String timePartitionColumn) {
    _timePartitionColumn = timePartitionColumn;
  }

  @Nullable
  public String getTimePartitionBucket() {
    return _timePartitionBucket;
  }

  public void setTimePartitionBucket(@Nullable String timePartitionBucket) {
    _timePartitionBucket = timePartitionBucket;
  }

  public int getMaxEnvelopeBytes() {
    return _maxEnvelopeBytes;
  }

  public void setMaxEnvelopeBytes(int maxEnvelopeBytes) {
    _maxEnvelopeBytes = maxEnvelopeBytes;
  }
}
