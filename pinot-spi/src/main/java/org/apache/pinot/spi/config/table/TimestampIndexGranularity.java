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

import org.apache.commons.lang3.StringUtils;


/**
 * TimestampIndexGranularity is the enum of different time granularities from MILLIS to YEAR.
 */
public enum TimestampIndexGranularity {
  MILLISECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR;

  public static final String TIMESTAMP_COLUMN_WITH_GRANULARITY_PATTERN = "$%s$%s";

  public static String getColumnNameWithGranularity(String column, TimestampIndexGranularity granularity) {
    return String.format(TIMESTAMP_COLUMN_WITH_GRANULARITY_PATTERN, column, granularity.toString());
  }

  public static String extractColumnNameFromColumnWithGranularity(String columnWithGranularity) {
    return StringUtils.split(columnWithGranularity, "$")[0];
  }

  public static TimestampIndexGranularity extractGranularityFromColumnWithGranularity(String columnWithGranularity) {
    return TimestampIndexGranularity.valueOf(StringUtils.split(columnWithGranularity, "$")[1]);
  }

  public static boolean isValidTimeColumnWithGranularityName(String columnName) {
    if (columnName.charAt(0) != '$') {
      return false;
    }
    String[] splits = StringUtils.split(columnName, "$", 2);
    if (splits.length != 2) {
      return false;
    }
    try {
      TimestampIndexGranularity.valueOf(splits[1]);
    } catch (Exception e) {
      return false;
    }
    return true;
  }
}
