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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.config.DeprecatedConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Validates raw table-config JSON for deprecated properties on create and update paths.
///
/// Rules are derived at class-load time from [DeprecatedConfig] annotations on getters reachable from [TableConfig].
/// A violation's severity ([Severity#WARNING] vs [Severity#ERROR]) is decided by comparing the annotation's `since`
/// version against the running Pinot version (see [#classifySeverity]): properties deprecated in the current
/// major.minor release are warnings; properties deprecated in any earlier release are errors. When the running
/// version cannot be determined, all violations are reported as errors so the safe default is to reject.
///
/// The validator operates on raw JSON rather than the deserialized [TableConfig] so it can detect explicitly
/// provided deprecated keys even when they carry default or false-y values that Jackson would otherwise elide.
public final class DeprecatedTableConfigValidationUtils {
  private DeprecatedTableConfigValidationUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DeprecatedTableConfigValidationUtils.class);
  private static final String WILDCARD = "*";
  // CURRENT_MAJOR_MINOR must be initialized before RULES because discoverRules() -> classifySeverity() reads it.
  private static final String CURRENT_MAJOR_MINOR = currentMajorMinor();
  private static final List<DeprecatedConfigRule> RULES = discoverRules();

  public enum Severity {
    WARNING, ERROR
  }

  /// Result of validating a table-config JSON, exposing errors and warnings separately so callers can decide
  /// whether to reject the request or surface non-fatal warnings to the user.
  public static final class Result {
    private final List<String> _errors;
    private final List<String> _warnings;

    private Result(List<String> errors, List<String> warnings) {
      _errors = List.copyOf(errors);
      _warnings = List.copyOf(warnings);
    }

    public List<String> getErrors() {
      return _errors;
    }

    public List<String> getWarnings() {
      return _warnings;
    }

    public boolean hasErrors() {
      return !_errors.isEmpty();
    }

    public boolean hasWarnings() {
      return !_warnings.isEmpty();
    }
  }

  /// Validates a table-config JSON document. When `oldTableConfigJson` is non-null, only paths whose value newly
  /// appears in `newTableConfigJson` or whose value differs from the corresponding path in `oldTableConfigJson` are
  /// reported, so re-submitting an unchanged legacy field on an update is a no-op. When `oldTableConfigJson` is null
  /// (creation path), every present deprecated path is reported.
  ///
  /// @param newTableConfigJson the incoming table config to validate; must not be `null`
  /// @param oldTableConfigJson the currently-stored table config to diff against, or `null` for create paths
  /// @param rootPathPrefix optional prefix for emitted paths (e.g. `"realtime"` when validating a sub-section)
  public static Result validate(JsonNode newTableConfigJson, @Nullable JsonNode oldTableConfigJson,
      @Nullable String rootPathPrefix) {
    Objects.requireNonNull(newTableConfigJson, "newTableConfigJson");
    List<String> errors = new ArrayList<>();
    List<String> warnings = new ArrayList<>();
    String prefix = rootPathPrefix == null ? "" : rootPathPrefix;
    for (DeprecatedConfigRule rule : RULES) {
      rule.collect(newTableConfigJson, oldTableConfigJson, prefix, errors, warnings);
    }
    return new Result(errors, warnings);
  }

  /// Validates a freshly-submitted table config (no prior stored value). On any error the method throws
  /// [IllegalArgumentException]; warnings are returned so the caller can surface them in the response.
  public static List<String> validateOnCreate(JsonNode newTableConfigJson, @Nullable String rootPathPrefix) {
    Result result = validate(newTableConfigJson, null, rootPathPrefix);
    if (result.hasErrors()) {
      throw new IllegalArgumentException("Deprecated table config properties are not allowed on table creation: "
          + String.join("; ", result.getErrors()));
    }
    if (result.hasWarnings()) {
      LOGGER.warn("Deprecated table config properties on creation: {}", String.join("; ", result.getWarnings()));
    }
    return result.getWarnings();
  }

  /// Validates an updated table config against its currently-stored counterpart. Only newly-introduced or
  /// value-changed deprecated paths are reported, so legacy values that were already present do not block updates.
  /// On any error the method throws [IllegalArgumentException]; warnings are returned for the caller to surface.
  public static List<String> validateOnUpdate(JsonNode newTableConfigJson, @Nullable JsonNode oldTableConfigJson,
      @Nullable String rootPathPrefix) {
    Result result = validate(newTableConfigJson, oldTableConfigJson, rootPathPrefix);
    if (result.hasErrors()) {
      throw new IllegalArgumentException("Newly introduced deprecated table config properties are not allowed: "
          + String.join("; ", result.getErrors()));
    }
    if (result.hasWarnings()) {
      LOGGER.warn("Newly introduced deprecated table config properties on update: {}",
          String.join("; ", result.getWarnings()));
    }
    return result.getWarnings();
  }

  // Backward-compatible API kept for existing call sites.
  public static void validateNoDeprecatedConfigs(JsonNode tableConfigJson) {
    validateOnCreate(tableConfigJson, null);
  }

  public static void validateNoDeprecatedConfigs(JsonNode tableConfigJson, @Nullable String rootPathPrefix) {
    validateOnCreate(tableConfigJson, rootPathPrefix);
  }

  static List<DeprecatedConfigRule> rulesForTesting() {
    return Collections.unmodifiableList(RULES);
  }

  private static List<DeprecatedConfigRule> discoverRules() {
    List<DeprecatedConfigRule> rules = new ArrayList<>();
    walk(TableConfig.class, new ArrayList<>(), new HashSet<>(), rules);
    return Collections.unmodifiableList(rules);
  }

  private static void walk(Type type, List<String> currentPath, Set<Class<?>> visiting,
      List<DeprecatedConfigRule> rules) {
    Class<?> rawClass = rawClass(type);
    if (rawClass == null) {
      return;
    }
    if (Map.class.isAssignableFrom(rawClass)) {
      Type valueType = typeArg(type, 1);
      if (valueType != null) {
        currentPath.add(WILDCARD);
        walk(valueType, currentPath, visiting, rules);
        currentPath.remove(currentPath.size() - 1);
      }
      return;
    }
    if (Collection.class.isAssignableFrom(rawClass)) {
      Type elemType = typeArg(type, 0);
      if (elemType != null) {
        currentPath.add(WILDCARD);
        walk(elemType, currentPath, visiting, rules);
        currentPath.remove(currentPath.size() - 1);
      }
      return;
    }
    if (rawClass.isArray()) {
      currentPath.add(WILDCARD);
      walk(rawClass.getComponentType(), currentPath, visiting, rules);
      currentPath.remove(currentPath.size() - 1);
      return;
    }
    if (!isConfigBean(rawClass) || !visiting.add(rawClass)) {
      return;
    }
    try {
      for (Method method : rawClass.getMethods()) {
        if (!isJsonAccessor(method)) {
          continue;
        }
        String propertyName = jsonPropertyName(method);
        if (propertyName == null) {
          continue;
        }
        currentPath.add(propertyName);
        DeprecatedConfig anno = method.getAnnotation(DeprecatedConfig.class);
        if (anno != null) {
          rules.add(new DeprecatedConfigRule(List.copyOf(currentPath), anno.replacement(), anno.since(),
              classifySeverity(anno.since())));
        }
        walk(method.getGenericReturnType(), currentPath, visiting, rules);
        currentPath.remove(currentPath.size() - 1);
      }
    } finally {
      visiting.remove(rawClass);
    }
  }

  private static boolean isConfigBean(Class<?> rawClass) {
    return BaseJsonConfig.class.isAssignableFrom(rawClass) && rawClass != BaseJsonConfig.class;
  }

  private static boolean isJsonAccessor(Method method) {
    if (method.getParameterCount() != 0 || method.getDeclaringClass() == Object.class
        || Modifier.isStatic(method.getModifiers())) {
      return false;
    }
    Class<?> returnType = method.getReturnType();
    if (returnType == void.class) {
      return false;
    }
    String name = method.getName();
    if (name.startsWith("get") && name.length() > 3) {
      return true;
    }
    return name.startsWith("is") && name.length() > 2 && (returnType == boolean.class || returnType == Boolean.class);
  }

  @Nullable
  private static String jsonPropertyName(Method method) {
    JsonProperty jsonProperty = method.getAnnotation(JsonProperty.class);
    if (jsonProperty != null && !jsonProperty.value().isEmpty()) {
      return jsonProperty.value();
    }
    String name = method.getName();
    String stripped;
    if (name.startsWith("get")) {
      stripped = name.substring(3);
    } else if (name.startsWith("is")) {
      stripped = name.substring(2);
    } else {
      return null;
    }
    if (stripped.isEmpty()) {
      return null;
    }
    return Character.toLowerCase(stripped.charAt(0)) + stripped.substring(1);
  }

  @Nullable
  private static Class<?> rawClass(Type type) {
    if (type instanceof Class<?>) {
      return (Class<?>) type;
    }
    if (type instanceof ParameterizedType) {
      Type rawType = ((ParameterizedType) type).getRawType();
      if (rawType instanceof Class<?>) {
        return (Class<?>) rawType;
      }
    }
    return null;
  }

  @Nullable
  private static Type typeArg(Type type, int index) {
    if (type instanceof ParameterizedType) {
      Type[] args = ((ParameterizedType) type).getActualTypeArguments();
      if (index < args.length) {
        return args[index];
      }
    }
    return null;
  }

  @Nullable
  private static String currentMajorMinor() {
    String version = PinotVersion.VERSION;
    if (version == null || PinotVersion.UNKNOWN.equals(version)) {
      return null;
    }
    return majorMinor(version);
  }

  /// Returns the `major.minor` prefix of a version string, stripping any pre-release qualifier (e.g. `-SNAPSHOT`)
  /// and ignoring the patch component. Returns `null` if the input does not parse.
  @Nullable
  static String majorMinor(@Nullable String version) {
    if (version == null) {
      return null;
    }
    String trimmed = version.trim();
    int qualifierIdx = trimmed.indexOf('-');
    if (qualifierIdx >= 0) {
      trimmed = trimmed.substring(0, qualifierIdx);
    }
    String[] parts = trimmed.split("\\.");
    if (parts.length < 2) {
      return null;
    }
    if (!isNumeric(parts[0]) || !isNumeric(parts[1])) {
      return null;
    }
    return parts[0] + "." + parts[1];
  }

  private static boolean isNumeric(String s) {
    if (s.isEmpty()) {
      return false;
    }
    for (int i = 0; i < s.length(); i++) {
      if (!Character.isDigit(s.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  /// Decides the severity for a violation based on the annotation's `since` value. The current Pinot release's
  /// deprecations are warnings (one-release grace period); older deprecations escalate to errors. If either version
  /// is unparseable, defaults to [Severity#ERROR] so unintended-warning regressions are avoided.
  static Severity classifySeverity(String since) {
    String sinceMajorMinor = majorMinor(since);
    if (CURRENT_MAJOR_MINOR == null || sinceMajorMinor == null) {
      return Severity.ERROR;
    }
    return CURRENT_MAJOR_MINOR.equals(sinceMajorMinor) ? Severity.WARNING : Severity.ERROR;
  }

  static final class DeprecatedConfigRule {
    private final List<String> _pathSegments;
    private final String _replacement;
    private final String _since;
    private final Severity _severity;

    DeprecatedConfigRule(List<String> pathSegments, String replacement, String since, Severity severity) {
      _pathSegments = pathSegments;
      _replacement = replacement;
      _since = since;
      _severity = severity;
    }

    List<String> pathSegments() {
      return _pathSegments;
    }

    Severity severity() {
      return _severity;
    }

    void collect(JsonNode newRoot, @Nullable JsonNode oldRoot, String pathPrefix, List<String> errors,
        List<String> warnings) {
      List<MatchedPath> matches = new ArrayList<>();
      collectMatches(newRoot, oldRoot, 0, pathPrefix, matches);
      for (MatchedPath match : matches) {
        // On create paths (oldRoot == null) every match is reported. On update paths a path is only reported if it
        // is newly introduced or its value differs from the previously-stored value.
        if (oldRoot != null && match._oldValue != null && Objects.equals(match._newValue, match._oldValue)) {
          continue;
        }
        String message = "'" + match._path + "' is deprecated since " + _since + ". " + _replacement;
        if (_severity == Severity.ERROR) {
          errors.add(message);
        } else {
          warnings.add(message);
        }
      }
    }

    private void collectMatches(@Nullable JsonNode newNode, @Nullable JsonNode oldNode, int idx, String currentPath,
        List<MatchedPath> matches) {
      if (newNode == null || newNode.isMissingNode()) {
        return;
      }
      if (idx == _pathSegments.size()) {
        matches.add(new MatchedPath(currentPath, newNode, oldNode));
        return;
      }
      String segment = _pathSegments.get(idx);
      if (WILDCARD.equals(segment)) {
        if (newNode.isArray()) {
          for (int i = 0; i < newNode.size(); i++) {
            JsonNode oldElem = (oldNode != null && oldNode.isArray() && i < oldNode.size()) ? oldNode.get(i) : null;
            collectMatches(newNode.get(i), oldElem, idx + 1, currentPath + "[" + i + "]", matches);
          }
        } else if (newNode.isObject()) {
          newNode.fieldNames().forEachRemaining(field -> {
            JsonNode oldChild = (oldNode != null && oldNode.isObject()) ? oldNode.get(field) : null;
            collectMatches(newNode.get(field), oldChild, idx + 1, append(currentPath, field), matches);
          });
        }
        return;
      }
      if (newNode.has(segment)) {
        JsonNode oldChild = (oldNode != null && oldNode.isObject()) ? oldNode.get(segment) : null;
        collectMatches(newNode.get(segment), oldChild, idx + 1, append(currentPath, segment), matches);
      }
    }

    private static String append(String currentPath, String segment) {
      return currentPath.isEmpty() ? segment : currentPath + "." + segment;
    }
  }

  private static final class MatchedPath {
    final String _path;
    final JsonNode _newValue;
    @Nullable
    final JsonNode _oldValue;

    MatchedPath(String path, JsonNode newValue, @Nullable JsonNode oldValue) {
      _path = path;
      _newValue = newValue;
      _oldValue = oldValue;
    }
  }
}
