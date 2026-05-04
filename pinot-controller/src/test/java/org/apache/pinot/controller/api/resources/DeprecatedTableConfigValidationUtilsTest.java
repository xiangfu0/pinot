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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class DeprecatedTableConfigValidationUtilsTest {

  @Test
  public void testRejectsDeprecatedConfigsWithArrayAndMapPaths()
      throws Exception {
    JsonNode tableConfigJson = JsonUtils.stringToJsonNode("{"
        + "\"segmentsConfig\":{\"segmentPushType\":\"APPEND\",\"minimizeDataMovement\":false},"
        + "\"tableIndexConfig\":{\"createInvertedIndexDuringSegmentGeneration\":false},"
        + "\"fieldConfigList\":[{\"name\":\"c1\",\"indexType\":\"INVERTED\"}],"
        + "\"instanceAssignmentConfigMap\":{\"CONSUMING\":{\"replicaGroupPartitionConfig\":"
        + "{\"minimizeDataMovement\":false}}}}");

    IllegalStateException e = expectThrows(IllegalStateException.class,
        () -> DeprecatedTableConfigValidationUtils.validateNoDeprecatedConfigs(tableConfigJson, "realtime"));
    assertTrue(e.getMessage().contains("realtime.segmentsConfig.segmentPushType"));
    assertTrue(e.getMessage().contains("realtime.segmentsConfig.minimizeDataMovement"));
    assertTrue(e.getMessage().contains("realtime.tableIndexConfig.createInvertedIndexDuringSegmentGeneration"));
    assertTrue(e.getMessage().contains("realtime.fieldConfigList[0].indexType"));
    assertTrue(
        e.getMessage().contains("realtime.instanceAssignmentConfigMap.CONSUMING.replicaGroupPartitionConfig"
            + ".minimizeDataMovement"));
  }

  @Test
  public void testAllowsModernConfigs()
      throws Exception {
    JsonNode tableConfigJson = JsonUtils.stringToJsonNode("{"
        + "\"segmentsConfig\":{\"replication\":\"1\"},"
        + "\"fieldConfigList\":[{\"name\":\"c1\",\"indexTypes\":[\"INVERTED\"]}],"
        + "\"ingestionConfig\":{\"batchIngestionConfig\":{\"segmentIngestionType\":\"APPEND\"},"
        + "\"streamIngestionConfig\":{\"streamConfigMaps\":[{\"streamType\":\"kafka\"}]}}}");

    DeprecatedTableConfigValidationUtils.validateNoDeprecatedConfigs(tableConfigJson);
  }
}
