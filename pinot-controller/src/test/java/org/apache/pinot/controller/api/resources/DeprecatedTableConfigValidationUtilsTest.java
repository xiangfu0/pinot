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
import org.apache.pinot.controller.api.resources.DeprecatedTableConfigValidationUtils.Result;
import org.apache.pinot.controller.api.resources.DeprecatedTableConfigValidationUtils.Severity;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class DeprecatedTableConfigValidationUtilsTest {

  @Test
  public void testRejectsDeprecatedConfigsOnCreate()
      throws Exception {
    // Each of these deprecations is older than the current major.minor, so they should all be reported as errors.
    JsonNode tableConfigJson = JsonUtils.stringToJsonNode("{"
        + "\"segmentsConfig\":{\"segmentPushType\":\"APPEND\",\"minimizeDataMovement\":false},"
        + "\"fieldConfigList\":[{\"name\":\"c1\",\"indexType\":\"INVERTED\"}],"
        + "\"instanceAssignmentConfigMap\":{\"CONSUMING\":{\"replicaGroupPartitionConfig\":"
        + "{\"minimizeDataMovement\":false}}}}");

    IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
        () -> DeprecatedTableConfigValidationUtils.validateOnCreate(tableConfigJson, "realtime"));
    assertTrue(e.getMessage().contains("realtime.segmentsConfig.segmentPushType"), e.getMessage());
    assertTrue(e.getMessage().contains("realtime.segmentsConfig.minimizeDataMovement"), e.getMessage());
    assertTrue(e.getMessage().contains("realtime.fieldConfigList[0].indexType"), e.getMessage());
    assertTrue(e.getMessage().contains(
            "realtime.instanceAssignmentConfigMap.CONSUMING.replicaGroupPartitionConfig.minimizeDataMovement"),
        e.getMessage());
  }

  @Test
  public void testCurrentVersionDeprecationIsWarningNotError()
      throws Exception {
    // tableIndexConfig.createInvertedIndexDuringSegmentGeneration is annotated with since=1.6.0, matching the
    // current Pinot release line. It should be reported as a warning (one-release grace period) rather than as an
    // error that blocks creation.
    JsonNode tableConfigJson = JsonUtils.stringToJsonNode(
        "{\"tableIndexConfig\":{\"createInvertedIndexDuringSegmentGeneration\":false}}");

    java.util.List<String> warnings = DeprecatedTableConfigValidationUtils.validateOnCreate(tableConfigJson, null);
    assertTrue(warnings.stream().anyMatch(
            w -> w.contains("tableIndexConfig.createInvertedIndexDuringSegmentGeneration")),
        "expected warning for current-version deprecation, got: " + warnings);
  }

  @Test
  public void testAllowsModernConfigsOnCreate()
      throws Exception {
    JsonNode tableConfigJson = JsonUtils.stringToJsonNode("{"
        + "\"segmentsConfig\":{\"replication\":\"1\"},"
        + "\"fieldConfigList\":[{\"name\":\"c1\",\"indexTypes\":[\"INVERTED\"]}],"
        + "\"ingestionConfig\":{\"batchIngestionConfig\":{\"segmentIngestionType\":\"APPEND\"},"
        + "\"streamIngestionConfig\":{\"streamConfigMaps\":[{\"streamType\":\"kafka\"}]}}}");

    DeprecatedTableConfigValidationUtils.validateOnCreate(tableConfigJson, null);
  }

  @Test
  public void testUpdateAllowsUnchangedLegacyDeprecatedValue()
      throws Exception {
    // Legacy config is already stored with segmentPushType=APPEND. Re-submitting the same value must NOT trigger
    // an error: the diff sees the value as unchanged.
    JsonNode oldJson = JsonUtils.stringToJsonNode("{\"segmentsConfig\":{\"segmentPushType\":\"APPEND\"}}");
    JsonNode newJson = JsonUtils.stringToJsonNode("{\"segmentsConfig\":{\"segmentPushType\":\"APPEND\"}}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertFalse(result.hasErrors(), "errors=" + result.getErrors());
    assertFalse(result.hasWarnings(), "warnings=" + result.getWarnings());
  }

  @Test
  public void testUpdateRejectsValueChangeOnDeprecatedField()
      throws Exception {
    // Legacy config had segmentPushType=APPEND. The update changes it to REFRESH — the diff treats this as a new
    // write to a deprecated key and reports an error (because the annotation's since=1.5.0 is older than the
    // running release line).
    JsonNode oldJson = JsonUtils.stringToJsonNode("{\"segmentsConfig\":{\"segmentPushType\":\"APPEND\"}}");
    JsonNode newJson = JsonUtils.stringToJsonNode("{\"segmentsConfig\":{\"segmentPushType\":\"REFRESH\"}}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertTrue(result.hasErrors(), "expected error on changed deprecated value");
    assertTrue(result.getErrors().get(0).contains("segmentsConfig.segmentPushType"), result.getErrors().toString());
  }

  @Test
  public void testUpdateRejectsNewlyIntroducedDeprecatedField()
      throws Exception {
    // Legacy config did not contain the deprecated field. Adding it on update fires the same error as on create.
    JsonNode oldJson = JsonUtils.stringToJsonNode("{\"segmentsConfig\":{\"replication\":\"1\"}}");
    JsonNode newJson = JsonUtils.stringToJsonNode(
        "{\"segmentsConfig\":{\"replication\":\"1\",\"segmentPushType\":\"APPEND\"}}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertTrue(result.hasErrors());
    assertTrue(result.getErrors().get(0).contains("segmentsConfig.segmentPushType"));
  }

  @Test
  public void testMajorMinorParsing() {
    assertEquals(DeprecatedTableConfigValidationUtils.majorMinor("1.6.0"), "1.6");
    assertEquals(DeprecatedTableConfigValidationUtils.majorMinor("1.6.0-SNAPSHOT"), "1.6");
    assertEquals(DeprecatedTableConfigValidationUtils.majorMinor("1.6"), "1.6");
    assertEquals(DeprecatedTableConfigValidationUtils.majorMinor("12.345.6"), "12.345");
    assertNull(DeprecatedTableConfigValidationUtils.majorMinor(null));
    assertNull(DeprecatedTableConfigValidationUtils.majorMinor("garbage"));
    assertNull(DeprecatedTableConfigValidationUtils.majorMinor("1"));
  }

  @Test
  public void testSeverityClassification() {
    // When current version cannot be determined, default to ERROR (safe).
    Severity s = DeprecatedTableConfigValidationUtils.classifySeverity("garbage");
    assertEquals(s, Severity.ERROR);
  }

  @Test
  public void testRulesDiscoveredFromAnnotations() {
    // Sanity check that the annotation walk picks up the expected paths from TableConfig.
    boolean foundIndexType = DeprecatedTableConfigValidationUtils.rulesForTesting().stream()
        .anyMatch(rule -> rule.pathSegments().equals(java.util.List.of("fieldConfigList", "*", "indexType")));
    assertTrue(foundIndexType, "expected fieldConfigList[*].indexType rule");

    boolean foundNestedMinimize = DeprecatedTableConfigValidationUtils.rulesForTesting().stream()
        .anyMatch(rule -> rule.pathSegments().equals(java.util.List.of(
            "instanceAssignmentConfigMap", "*", "replicaGroupPartitionConfig", "minimizeDataMovement")));
    assertTrue(foundNestedMinimize, "expected nested map-wildcard rule");
  }
}
