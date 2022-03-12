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

import org.testng.Assert;
import org.testng.annotations.Test;


public class TimestampIndexGranularityTest {

  @Test
  public void testTimestampIndexGranularity() {
    String timeColumn = "testTs";
    TimestampIndexGranularity granularity = TimestampIndexGranularity.DAY;
    String timeColumnWithGranularity = "$testTs$DAY";
    Assert.assertEquals(TimestampIndexGranularity.getColumnNameWithGranularity(timeColumn, granularity),
        timeColumnWithGranularity);
    Assert.assertEquals(TimestampIndexGranularity.extractColumnNameFromColumnWithGranularity(timeColumnWithGranularity),
        timeColumn);
    Assert.assertEquals(
        TimestampIndexGranularity.extractGranularityFromColumnWithGranularity(timeColumnWithGranularity), granularity);
    Assert.assertTrue(TimestampIndexGranularity.isValidTimeColumnWithGranularityName(timeColumnWithGranularity));
    Assert.assertFalse(TimestampIndexGranularity.isValidTimeColumnWithGranularityName(timeColumn));
    Assert.assertFalse(TimestampIndexGranularity.isValidTimeColumnWithGranularityName("$docId"));
  }
}
