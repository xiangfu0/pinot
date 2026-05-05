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
import java.math.BigDecimal;
import java.math.BigInteger;
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
/// Pinot version itself cannot be determined (e.g. running from an IDE or a shaded jar where
/// `pinot-version.properties` was not maven-filtered), violations fall back to **warnings** so a misconfigured
/// deployment does not silently start rejecting every previously-valid table config; only an unparseable annotation
/// `since` value still classifies as an error, since that case is a code-side bug rather than an environment one.
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
  ///
  /// Callers must ensure the table exists before invoking this method; pass the stored config JSON (never `null`)
  /// for `oldTableConfigJson`. For paths where no stored counterpart exists, use [#validateOnCreate] instead.
  public static List<String> validateOnUpdate(JsonNode newTableConfigJson, JsonNode oldTableConfigJson,
      @Nullable String rootPathPrefix) {
    Objects.requireNonNull(oldTableConfigJson, "oldTableConfigJson; use validateOnCreate for create paths");
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
  /// deprecations are warnings (one-release grace period); older deprecations escalate to errors.
  ///
  /// When the current Pinot version cannot be determined (e.g. running from an IDE / shaded jar where
  /// `pinot-version.properties` was not maven-filtered), severity falls back to [Severity#WARNING] so a
  /// misconfigured deployment does not silently start rejecting every previously-valid table config. An
  /// unparseable `since` value on the annotation itself still classifies as [Severity#ERROR] because that case
  /// reflects a code-side bug rather than an environment-side one.
  static Severity classifySeverity(String since) {
    return classifySeverity(since, CURRENT_MAJOR_MINOR);
  }

  /// Test seam for [#classifySeverity(String)] that takes the current major.minor version explicitly so the
  /// version-unknown fallback branch can be unit-tested without manipulating the classloader-scoped
  /// `pinot-version.properties` resource.
  static Severity classifySeverity(String since, @Nullable String currentMajorMinor) {
    String sinceMajorMinor = majorMinor(since);
    if (sinceMajorMinor == null) {
      return Severity.ERROR;
    }
    if (currentMajorMinor == null) {
      return Severity.WARNING;
    }
    return currentMajorMinor.equals(sinceMajorMinor) ? Severity.WARNING : Severity.ERROR;
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
        if (oldRoot != null) {
          // Update path. Silently drop matches that are unchanged from the stored config, OR that are absent in
          // the stored config but whose new value is the type's Java default. The default-skip handles the case
          // where many deprecated getters carry @JsonInclude(NON_DEFAULT): a previous create with
          // `enableSnapshot: false` is stripped at ZK write time, so on PUT the diff would otherwise see `false`
          // as "newly introduced" and reject an unchanged config. Crucially, the default-skip applies ONLY when
          // the stored config did not contain the path — a deliberate flip of an existing non-default value back
          // to the default (e.g. `true` → `false`) is still reported, matching validateOnUpdate's contract that
          // value-changed deprecated paths must be flagged.
          if (match._oldValue != null) {
            if (Objects.equals(match._newValue, match._oldValue)) {
              continue;
            }
          } else if (isJacksonDefault(match._newValue)) {
            continue;
          }
        }
        String message = "'" + match._path + "' is deprecated since " + _since + ". " + _replacement;
        if (_severity == Severity.ERROR) {
          errors.add(message);
        } else {
          warnings.add(message);
        }
      }
    }

    /// Returns true when the JSON value matches the Java zero-value for its type (`false` for booleans, `0` for
    /// numerics, empty for strings/arrays/objects, and explicit JSON `null`). This is an approximation of
    /// Jackson's `@JsonInclude(NON_DEFAULT)` semantics: the real Jackson behaviour consults the bean's actual
    /// initialised default by instantiating it, but every deprecated getter currently annotated with
    /// `@DeprecatedConfig` uses a type-zero default, so the approximation is exact for today's rules. If a future
    /// `@DeprecatedConfig` annotation is added on a field whose bean default is non-zero (e.g. an enum that
    /// defaults to a non-null value), update this helper to consult the bean default via reflection.
    ///
    /// Numeric types are compared via integer paths where possible to avoid the `double`-coercion edge cases
    /// (lossy conversion for `BigDecimal`/`BigInteger`, IEEE-754 `-0.0 == 0.0d`, etc.). Today no annotated getter
    /// returns a floating-point or arbitrary-precision type, but the dispatch is robust enough to extend to those
    /// cases in the future.
    private static boolean isJacksonDefault(@Nullable JsonNode node) {
      if (node == null || node.isMissingNode() || node.isNull()) {
        return true;
      }
      if (node.isBoolean()) {
        return !node.booleanValue();
      }
      if (node.isShort() || node.isInt() || node.isLong()) {
        return node.longValue() == 0L;
      }
      if (node.isBigInteger()) {
        return BigInteger.ZERO.equals(node.bigIntegerValue());
      }
      if (node.isBigDecimal()) {
        return BigDecimal.ZERO.compareTo(node.decimalValue()) == 0;
      }
      if (node.isFloatingPointNumber()) {
        // Use Double.compare so -0.0 is treated as non-default (it differs from the Java zero-value of 0.0d).
        return Double.compare(node.doubleValue(), 0.0d) == 0;
      }
      if (node.isTextual()) {
        // String-returning deprecated getters today (e.g. segmentPushType) all initialise to null, not "". Match
        // Jackson's actual NON_DEFAULT contract: only the explicit-null case (handled above) is "default";
        // empty-string is a real user-supplied value and must be flagged on update.
        return false;
      }
      if (node.isArray() || node.isObject()) {
        return node.isEmpty();
      }
      // BinaryNode / POJONode never arise from text JSON parsed by Jackson; treat as non-default conservatively.
      return false;
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
