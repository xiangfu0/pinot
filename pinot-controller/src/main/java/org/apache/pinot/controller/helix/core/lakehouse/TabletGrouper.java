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
package org.apache.pinot.controller.helix.core.lakehouse;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TabletConfig;
import org.apache.pinot.spi.lakehouse.LakehouseFileDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Groups Iceberg data files into tablets based on {@link TabletConfig} settings.
 *
 * <p>Files are grouped by the composite key {@code (specId, partitionValues, timeBucket)}.
 * Within each group, files are packed into generations: a new generation is started when
 * either {@code targetFilesPerTablet} or {@code targetBytesPerTablet} is exceeded.</p>
 *
 * <p>The resulting tablet ID has the format:
 * {@code tablet_<specId>_<partitionHash>_<timeBucket>_<generation>}</p>
 *
 * <p>This class is stateless and thread-safe.</p>
 */
public final class TabletGrouper {
  private static final Logger LOGGER = LoggerFactory.getLogger(TabletGrouper.class);

  private static final long MILLIS_PER_HOUR = TimeUnit.HOURS.toMillis(1);
  private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1);

  private TabletGrouper() {
    // Utility class
  }

  /**
   * Groups the given data files into tablets according to the tablet configuration.
   *
   * @param files list of data files from an Iceberg snapshot
   * @param tabletConfig tablet sizing and grouping configuration
   * @param timeColumnName time column name for extracting time bounds; may be null if
   *                       the table has no time column
   * @return map from tablet ID to the list of files belonging to that tablet
   */
  public static Map<String, List<LakehouseFileDescriptor>> groupFiles(List<LakehouseFileDescriptor> files,
      TabletConfig tabletConfig, @Nullable String timeColumnName) {
    int targetFiles = tabletConfig.getTargetFilesPerTablet();
    long targetBytes = tabletConfig.getTargetBytesPerTablet();
    String timeBucketGranularity = tabletConfig.getTimeBucket();

    // First pass: bucket files by (specId, partitionHash, timeBucket)
    Map<String, List<LakehouseFileDescriptor>> buckets = new LinkedHashMap<>();
    for (LakehouseFileDescriptor file : files) {
      String bucketKey = computeBucketKey(file, timeBucketGranularity, timeColumnName);
      buckets.computeIfAbsent(bucketKey, k -> new ArrayList<>()).add(file);
    }

    // Second pass: split buckets into generations when limits are exceeded
    Map<String, List<LakehouseFileDescriptor>> tablets = new LinkedHashMap<>();
    for (Map.Entry<String, List<LakehouseFileDescriptor>> entry : buckets.entrySet()) {
      String bucketKey = entry.getKey();
      List<LakehouseFileDescriptor> bucketFiles = entry.getValue();

      int generation = 0;
      int currentFileCount = 0;
      long currentBytes = 0;
      List<LakehouseFileDescriptor> currentBatch = new ArrayList<>();

      for (LakehouseFileDescriptor file : bucketFiles) {
        // Check if adding this file would exceed limits (allow at least one file per generation)
        if (!currentBatch.isEmpty() && (currentFileCount >= targetFiles || currentBytes >= targetBytes)) {
          String tabletId = formatTabletId(bucketKey, generation);
          tablets.put(tabletId, currentBatch);
          generation++;
          currentFileCount = 0;
          currentBytes = 0;
          currentBatch = new ArrayList<>();
        }

        currentBatch.add(file);
        currentFileCount++;
        currentBytes += file.getFileSizeBytes();
      }

      // Flush remaining files
      if (!currentBatch.isEmpty()) {
        String tabletId = formatTabletId(bucketKey, generation);
        tablets.put(tabletId, currentBatch);
      }
    }

    LOGGER.debug("Grouped {} files into {} tablets", files.size(), tablets.size());
    return tablets;
  }

  /**
   * Computes the bucket key for a file based on spec ID, partition values, and time bucket.
   */
  @VisibleForTesting
  static String computeBucketKey(LakehouseFileDescriptor file, String timeBucketGranularity,
      @Nullable String timeColumnName) {
    int specId = file.getSpecId();
    int partitionHash = computePartitionHash(file.getPartitionValues());
    long timeBucket = computeTimeBucket(file, timeBucketGranularity, timeColumnName);
    return specId + "_" + partitionHash + "_" + timeBucket;
  }

  /**
   * Computes a stable hash for partition values.
   */
  @VisibleForTesting
  static int computePartitionHash(@Nullable List<String> partitionValues) {
    if (partitionValues == null || partitionValues.isEmpty()) {
      return 0;
    }
    // Use a deterministic hash combining all partition values
    int hash = 1;
    for (String value : partitionValues) {
      hash = 31 * hash + (value != null ? value.hashCode() : 0);
    }
    // Make the hash non-negative for cleaner tablet IDs
    return hash & 0x7FFFFFFF;
  }

  /**
   * Computes the time bucket for a file based on its column bounds.
   *
   * <p>If the time column is present in the file's lower bounds, the minimum time value
   * is truncated to the bucket granularity. Otherwise returns 0 (all files in the same
   * time bucket).</p>
   */
  @VisibleForTesting
  static long computeTimeBucket(LakehouseFileDescriptor file, String timeBucketGranularity,
      @Nullable String timeColumnName) {
    if (timeColumnName == null || timeColumnName.isEmpty()) {
      return 0;
    }

    Map<String, String> lowerBounds = file.getColumnLowerBounds();
    if (lowerBounds == null || !lowerBounds.containsKey(timeColumnName)) {
      return 0;
    }

    try {
      long minTimeMs = Long.parseLong(lowerBounds.get(timeColumnName));
      return truncateToGranularity(minTimeMs, timeBucketGranularity);
    } catch (NumberFormatException e) {
      LOGGER.debug("Could not parse time column '{}' lower bound as long, using bucket 0", timeColumnName);
      return 0;
    }
  }

  /**
   * Truncates a timestamp to the given granularity bucket.
   */
  @VisibleForTesting
  static long truncateToGranularity(long timestampMs, String granularity) {
    switch (granularity.toUpperCase()) {
      case "HOUR":
        return timestampMs / MILLIS_PER_HOUR;
      case "DAY":
        return timestampMs / MILLIS_PER_DAY;
      case "WEEK":
        // Truncate to week boundary (7 days)
        return timestampMs / (7 * MILLIS_PER_DAY);
      case "MONTH":
        // Approximate month as 30 days for bucketing
        return timestampMs / (30 * MILLIS_PER_DAY);
      case "NONE":
        return 0;
      default:
        LOGGER.warn("Unknown time bucket granularity '{}', falling back to DAY", granularity);
        return timestampMs / MILLIS_PER_DAY;
    }
  }

  /**
   * Formats the tablet ID string.
   */
  @VisibleForTesting
  static String formatTabletId(String bucketKey, int generation) {
    return "tablet_" + bucketKey + "_" + generation;
  }
}
