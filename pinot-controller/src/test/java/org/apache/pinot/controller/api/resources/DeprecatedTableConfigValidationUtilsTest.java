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
import java.util.List;
import org.apache.pinot.controller.api.resources.DeprecatedTableConfigValidationUtils.DeprecatedConfigRule;
import org.apache.pinot.controller.api.resources.DeprecatedTableConfigValidationUtils.Result;
import org.apache.pinot.controller.api.resources.DeprecatedTableConfigValidationUtils.Severity;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.DataProvider;
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
    // write to a deprecated key and reports an error (because the annotation's since is older than the
    // running release line).
    JsonNode oldJson = JsonUtils.stringToJsonNode("{\"segmentsConfig\":{\"segmentPushType\":\"APPEND\"}}");
    JsonNode newJson = JsonUtils.stringToJsonNode("{\"segmentsConfig\":{\"segmentPushType\":\"REFRESH\"}}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertTrue(result.hasErrors(), "expected error on changed deprecated value");
    assertTrue(result.getErrors().get(0).contains("segmentsConfig.segmentPushType"), result.getErrors().toString());
  }

  @Test
  public void testUpdateAllowsReSubmittedDefaultValueWhenAbsentFromStored()
      throws Exception {
    // Many deprecated booleans carry @JsonInclude(NON_DEFAULT). A previous create with `enableSnapshot: false`
    // (the type default) is stripped at ZK write time, so the stored config has no `enableSnapshot` key. On PUT,
    // the diff sees the path as missing in the stored config but present in the new submission with the type
    // default — the validator must treat this as a no-op so users can re-submit cached configs unchanged.
    JsonNode oldJson = JsonUtils.stringToJsonNode("{\"upsertConfig\":{}}");
    JsonNode newJson = JsonUtils.stringToJsonNode("{\"upsertConfig\":{\"enableSnapshot\":false}}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertFalse(result.hasErrors(), "errors=" + result.getErrors());
    assertFalse(result.hasWarnings(), "warnings=" + result.getWarnings());
  }

  @Test
  public void testUpdateTreatsExplicitJsonNullStoredValueAsPresent()
      throws Exception {
    // The stored JSON has an explicit `null` value for a deprecated path. The new submission flips it to a
    // non-default `true`. Both differ, so the diff must report it (the default-skip applies only when the path
    // was *absent* in the stored config, not when it was present-but-null).
    JsonNode oldJson = JsonUtils.stringToJsonNode("{\"upsertConfig\":{\"enableSnapshot\":null}}");
    JsonNode newJson = JsonUtils.stringToJsonNode("{\"upsertConfig\":{\"enableSnapshot\":true}}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertTrue(result.hasErrors(), "expected error on flip from null to true");
    assertTrue(result.getErrors().get(0).contains("upsertConfig.enableSnapshot"), result.getErrors().toString());
  }

  @Test
  public void testUpdateRejectsDeliberateFlipFromNonDefaultToDefault()
      throws Exception {
    // The default-skip optimisation must not silently swallow a deliberate value flip on an existing field.
    // Stored config has `enableSnapshot: true`; user submits `enableSnapshot: false` — this is a value change on
    // a deprecated path and must be reported (per validateOnUpdate's contract: value-changed deprecated paths are
    // flagged, regardless of whether the new value happens to be the type default).
    JsonNode oldJson = JsonUtils.stringToJsonNode("{\"upsertConfig\":{\"enableSnapshot\":true}}");
    JsonNode newJson = JsonUtils.stringToJsonNode("{\"upsertConfig\":{\"enableSnapshot\":false}}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertTrue(result.hasErrors(), "expected error on deliberate flip true → false");
    assertTrue(result.getErrors().get(0).contains("upsertConfig.enableSnapshot"), result.getErrors().toString());
  }

  @Test
  public void testUpdateRejectsEmptyStringValueForNullDefaultField()
      throws Exception {
    // String-returning deprecated getters initialise to null (the Java default), not "". A user submitting
    // "segmentPushType":"" on update — when the stored config lacks the key — is supplying a real value that
    // Jackson would NOT elide under NON_DEFAULT, and must be flagged. Locks the textual branch of
    // isJacksonDefault returning false (rather than treating empty string as default).
    JsonNode oldJson = JsonUtils.stringToJsonNode("{\"segmentsConfig\":{\"replication\":\"1\"}}");
    JsonNode newJson = JsonUtils.stringToJsonNode(
        "{\"segmentsConfig\":{\"replication\":\"1\",\"segmentPushType\":\"\"}}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertTrue(result.hasErrors(), "expected error on empty-string value for deprecated path");
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
    // An unparseable annotation `since` reflects a code-side bug and classifies as ERROR.
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("garbage"), Severity.ERROR);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity(""), Severity.ERROR);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("1"), Severity.ERROR);
  }

  @Test
  public void testSeverityFallsBackToWarningWhenCurrentVersionUnknown() {
    // When the running Pinot version cannot be determined (e.g. shaded jar / IDE without filtered resources), a
    // misconfigured deployment must NOT silently start rejecting every previously-valid table config. The
    // fallback is WARNING for parseable `since` values, ERROR only when `since` itself is unparseable.
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("1.6.0", null), Severity.WARNING);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("1.5.0", null), Severity.WARNING);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("0.3.0", null), Severity.WARNING);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("garbage", null), Severity.ERROR);
  }

  @Test
  public void testSeverityWithExplicitCurrentVersion() {
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("1.6.0", "1.6"), Severity.WARNING);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("1.6.0-SNAPSHOT", "1.6"), Severity.WARNING);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("1.5.0", "1.6"), Severity.ERROR);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("0.3.0", "1.6"), Severity.ERROR);
    // Unparseable since wins over a known current version.
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("garbage", "1.6"), Severity.ERROR);
  }

  @Test
  public void testRulesDiscoveredFromAnnotations() {
    // Sanity check that the annotation walk picks up the expected paths from TableConfig.
    boolean foundIndexType = DeprecatedTableConfigValidationUtils.rulesForTesting().stream()
        .anyMatch(rule -> rule.pathSegments().equals(List.of("fieldConfigList", "*", "indexType")));
    assertTrue(foundIndexType, "expected fieldConfigList[*].indexType rule");

    boolean foundNestedMinimize = DeprecatedTableConfigValidationUtils.rulesForTesting().stream()
        .anyMatch(rule -> rule.pathSegments().equals(List.of(
            "instanceAssignmentConfigMap", "*", "replicaGroupPartitionConfig", "minimizeDataMovement")));
    assertTrue(foundNestedMinimize, "expected nested map-wildcard rule");
  }

  /// Provides every rule discovered by the annotation walk so the parameterized test below covers the full set
  /// 1:1. If a new {@link org.apache.pinot.spi.config.DeprecatedConfig @DeprecatedConfig} is added on a getter, this
  /// test automatically exercises it without needing a new test case.
  @DataProvider(name = "allRules")
  public Object[][] allRules() {
    List<DeprecatedConfigRule> rules = DeprecatedTableConfigValidationUtils.rulesForTesting();
    Object[][] data = new Object[rules.size()][];
    for (int i = 0; i < rules.size(); i++) {
      data[i] = new Object[] {rules.get(i)};
    }
    return data;
  }

  @Test(dataProvider = "allRules")
  public void testEveryRuleFiresOnSyntheticInputAsArrayWildcard(DeprecatedConfigRule rule)
      throws Exception {
    runEveryRuleCase(rule, /* arrayWildcard */ true);
  }

  @Test(dataProvider = "allRules")
  public void testEveryRuleFiresOnSyntheticInputAsMapWildcard(DeprecatedConfigRule rule)
      throws Exception {
    runEveryRuleCase(rule, /* arrayWildcard */ false);
  }

  /// Builds a JSON tree containing the deprecated path and asserts the rule fires. The {@code arrayWildcard} flag
  /// controls how `*` segments are realised: as `[ {...} ]` (array branch) or `{"x":...}` (object branch). Running
  /// both shapes ensures `collectMatches` is exercised on both wildcard branches for every rule.
  private static void runEveryRuleCase(DeprecatedConfigRule rule, boolean arrayWildcard)
      throws Exception {
    String synthetic = synthesizeJsonForPath(rule.pathSegments(), arrayWildcard);
    String expectedPath = expectedPathInMessage(rule.pathSegments(), arrayWildcard);

    Result result = DeprecatedTableConfigValidationUtils.validate(JsonUtils.stringToJsonNode(synthetic), null, null);
    if (rule.severity() == Severity.ERROR) {
      assertTrue(result.getErrors().stream().anyMatch(m -> m.contains(expectedPath)),
          "expected error containing '" + expectedPath + "', got errors=" + result.getErrors() + ", warnings="
              + result.getWarnings());
    } else {
      assertTrue(result.getWarnings().stream().anyMatch(m -> m.contains(expectedPath)),
          "expected warning containing '" + expectedPath + "', got warnings=" + result.getWarnings() + ", errors="
              + result.getErrors());
    }
  }

  private static String synthesizeJsonForPath(List<String> path, boolean arrayWildcard) {
    StringBuilder open = new StringBuilder();
    StringBuilder close = new StringBuilder();
    for (String segment : path.subList(0, path.size() - 1)) {
      if ("*".equals(segment)) {
        if (arrayWildcard) {
          open.append("[");
          close.insert(0, "]");
        } else {
          open.append("{\"x\":");
          close.insert(0, "}");
        }
      } else {
        open.append("{\"").append(segment).append("\":");
        close.insert(0, "}");
      }
    }
    String leaf = path.get(path.size() - 1);
    if ("*".equals(leaf)) {
      open.append(arrayWildcard ? "[\"v\"]" : "{\"x\":\"v\"}");
    } else {
      open.append("{\"").append(leaf).append("\":\"v\"}");
    }
    return open.append(close).toString();
  }

  private static String expectedPathInMessage(List<String> path, boolean arrayWildcard) {
    StringBuilder sb = new StringBuilder();
    for (String segment : path) {
      if ("*".equals(segment) && arrayWildcard) {
        // The walker emits `[<index>]` (no preceding `.`) for array entries.
        sb.append("[0]");
      } else {
        String key = "*".equals(segment) ? "x" : segment;
        if (sb.length() > 0) {
          sb.append('.');
        }
        sb.append(key);
      }
    }
    return sb.toString();
  }
}
