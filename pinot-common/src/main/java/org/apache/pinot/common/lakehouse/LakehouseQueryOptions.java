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
package org.apache.pinot.common.lakehouse;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;


/**
 * Extracted lakehouse-specific query options for a single query.
 *
 * <p>This is an immutable DTO that captures Iceberg snapshot/ref resolution options
 * from the broker query options map. It is used by the broker routing layer to
 * determine which Iceberg snapshot to pin a query to.</p>
 *
 * <p>At most one of {@link #getSnapshotId()}, {@link #getBranch()}, {@link #getTag()},
 * or {@link #getAsOfMs()} should be set. If none are set, the table's default ref
 * (from {@code LakehouseReadConfig}) is used.</p>
 *
 * <p>This class is thread-safe and immutable.</p>
 */
public class LakehouseQueryOptions {

  @Nullable
  private final Long _snapshotId;

  @Nullable
  private final String _branch;

  @Nullable
  private final String _tag;

  @Nullable
  private final Long _asOfMs;

  public LakehouseQueryOptions(@Nullable Long snapshotId, @Nullable String branch,
      @Nullable String tag, @Nullable Long asOfMs) {
    _snapshotId = snapshotId;
    _branch = branch;
    _tag = tag;
    _asOfMs = asOfMs;
  }

  /**
   * Extracts lakehouse query options from the broker query options map.
   *
   * @param queryOptions the raw query options map from the broker request
   * @return a new {@link LakehouseQueryOptions} instance
   */
  public static LakehouseQueryOptions fromQueryOptions(Map<String, String> queryOptions) {
    return new LakehouseQueryOptions(
        QueryOptionsUtils.getIcebergSnapshotId(queryOptions),
        QueryOptionsUtils.getIcebergBranch(queryOptions),
        QueryOptionsUtils.getIcebergTag(queryOptions),
        QueryOptionsUtils.getIcebergAsOfMs(queryOptions)
    );
  }

  /**
   * Returns the explicit Iceberg snapshot ID, or {@code null} if not specified.
   */
  @Nullable
  public Long getSnapshotId() {
    return _snapshotId;
  }

  /**
   * Returns the Iceberg branch name, or {@code null} if not specified.
   */
  @Nullable
  public String getBranch() {
    return _branch;
  }

  /**
   * Returns the Iceberg tag name, or {@code null} if not specified.
   */
  @Nullable
  public String getTag() {
    return _tag;
  }

  /**
   * Returns the Iceberg as-of timestamp in epoch milliseconds, or {@code null} if not specified.
   */
  @Nullable
  public Long getAsOfMs() {
    return _asOfMs;
  }

  /**
   * Returns {@code true} if any explicit snapshot resolution option is set.
   * When this returns {@code false}, the table's default ref should be used.
   */
  public boolean hasExplicitSnapshot() {
    return _snapshotId != null || _branch != null || _tag != null || _asOfMs != null;
  }
}
