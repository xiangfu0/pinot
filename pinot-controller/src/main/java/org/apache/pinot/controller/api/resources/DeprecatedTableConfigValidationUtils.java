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
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;


/**
 * Validates raw table-config JSON for deprecated properties that must not be accepted on table creation paths.
 *
 * <p>This utility operates on the input JSON instead of the deserialized {@code TableConfig} so it can reject
 * explicitly provided deprecated keys even when they carry default or false-y values.</p>
 */
public final class DeprecatedTableConfigValidationUtils {
  private DeprecatedTableConfigValidationUtils() {
  }

  private static final String WILDCARD = "*";

  private static final List<DeprecatedConfigRule> DEPRECATED_CONFIG_RULES = List.of(
      new DeprecatedConfigRule("Use 'ingestionConfig.batchIngestionConfig.segmentIngestionFrequency' instead.",
          "segmentsConfig", "segmentPushFrequency"),
      new DeprecatedConfigRule("Use 'ingestionConfig.batchIngestionConfig.segmentIngestionType' instead.",
          "segmentsConfig", "segmentPushType"),
      new DeprecatedConfigRule("Use 'segmentsConfig.replication' instead.", "segmentsConfig", "replicasPerPartition"),
      new DeprecatedConfigRule("Use 'segmentAssignmentConfigMap' instead.", "segmentsConfig",
          "segmentAssignmentStrategy"),
      new DeprecatedConfigRule("Use 'segmentAssignmentConfigMap' instead.", "segmentsConfig",
          "replicaGroupStrategyConfig"),
      new DeprecatedConfigRule("Use 'instanceAssignmentConfigMap' instead.", "segmentsConfig", "minimizeDataMovement"),
      new DeprecatedConfigRule("Use 'ingestionConfig.streamIngestionConfig.streamConfigMaps' instead.",
          "tableIndexConfig", "streamConfigs"),
      new DeprecatedConfigRule("Use 'tableIndexConfig.jsonIndexConfigs' instead.", "tableIndexConfig",
          "jsonIndexColumns"),
      new DeprecatedConfigRule("Remove this field; it is ignored.", "tableIndexConfig",
          "createInvertedIndexDuringSegmentGeneration"),
      new DeprecatedConfigRule("Use 'routing.segmentPrunerTypes' and 'routing.instanceSelectorType' instead.",
          "routing", "routingTableBuilderName"),
      new DeprecatedConfigRule("Use 'fieldConfigList[].indexTypes' instead.", "fieldConfigList", WILDCARD,
          "indexType"),
      new DeprecatedConfigRule("Use 'upsertConfig.snapshot' instead.", "upsertConfig", "enableSnapshot"),
      new DeprecatedConfigRule("Use 'upsertConfig.preload' instead.", "upsertConfig", "enablePreload"),
      new DeprecatedConfigRule(
          "Use 'ingestionConfig.streamIngestionConfig.parallelSegmentConsumptionPolicy' instead.",
          "upsertConfig", "allowPartialUpsertConsumptionDuringCommit"),
      new DeprecatedConfigRule("Use 'dedupConfig.preload' instead.", "dedupConfig", "enablePreload"),
      new DeprecatedConfigRule(
          "Use 'ingestionConfig.streamIngestionConfig.parallelSegmentConsumptionPolicy' instead.",
          "dedupConfig", "allowDedupConsumptionDuringCommit"),
      new DeprecatedConfigRule("Remove this field; it will be removed in a future release.",
          "instanceAssignmentConfigMap", WILDCARD, "replicaGroupPartitionConfig", "minimizeDataMovement"));

  public static void validateNoDeprecatedConfigs(JsonNode tableConfigJson) {
    validateNoDeprecatedConfigs(tableConfigJson, null);
  }

  public static void validateNoDeprecatedConfigs(JsonNode tableConfigJson, @Nullable String rootPathPrefix) {
    List<String> violations = new ArrayList<>();
    for (DeprecatedConfigRule deprecatedConfigRule : DEPRECATED_CONFIG_RULES) {
      deprecatedConfigRule.collectMatches(tableConfigJson, rootPathPrefix, violations);
    }
    if (!violations.isEmpty()) {
      throw new IllegalStateException(
          "Deprecated table config properties are not allowed on table creation: " + String.join("; ", violations));
    }
  }

  private static final class DeprecatedConfigRule {
    private final String _replacementMessage;
    private final List<String> _pathSegments;

    private DeprecatedConfigRule(String replacementMessage, String... pathSegments) {
      _replacementMessage = replacementMessage;
      _pathSegments = List.of(pathSegments);
    }

    private void collectMatches(JsonNode tableConfigJson, @Nullable String rootPathPrefix, List<String> violations) {
      List<String> matchingPaths = new ArrayList<>();
      collectMatches(tableConfigJson, 0, rootPathPrefix == null ? "" : rootPathPrefix, matchingPaths);
      for (String matchingPath : matchingPaths) {
        violations.add("'" + matchingPath + "' is deprecated. " + _replacementMessage);
      }
    }

    private void collectMatches(@Nullable JsonNode currentNode, int pathSegmentIndex, String currentPath,
        List<String> matchingPaths) {
      if (currentNode == null || currentNode.isMissingNode()) {
        return;
      }
      if (pathSegmentIndex == _pathSegments.size()) {
        matchingPaths.add(currentPath);
        return;
      }

      String pathSegment = _pathSegments.get(pathSegmentIndex);
      if (WILDCARD.equals(pathSegment)) {
        if (currentNode.isArray()) {
          for (int i = 0; i < currentNode.size(); i++) {
            collectMatches(currentNode.get(i), pathSegmentIndex + 1, currentPath + "[" + i + "]", matchingPaths);
          }
        } else if (currentNode.isObject()) {
          currentNode.fields().forEachRemaining(entry -> collectMatches(entry.getValue(), pathSegmentIndex + 1,
              appendObjectSegment(currentPath, entry.getKey()), matchingPaths));
        }
        return;
      }

      if (currentNode.has(pathSegment)) {
        collectMatches(currentNode.get(pathSegment), pathSegmentIndex + 1,
            appendObjectSegment(currentPath, pathSegment), matchingPaths);
      }
    }

    private static String appendObjectSegment(String currentPath, String pathSegment) {
      return currentPath.isEmpty() ? pathSegment : currentPath + "." + pathSegment;
    }
  }
}
