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
package org.apache.pinot.segment.local.utils;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Migration helper that converts legacy {@link FieldConfig.CompressionCodec} settings to their
 * recommended codec pipeline {@code codecSpec} strings for the forward-index codec framework.
 *
 * <h3>Mapping table</h3>
 * <pre>
 *   PASS_THROUGH  → not migrated (raw no-op, no codec pipeline needed)
 *   LZ4           → "LZ4"
 *   ZSTANDARD     → "ZSTD(3)"
 *   SNAPPY        → "SNAPPY"
 *   GZIP          → "GZIP"
 *   DELTA         → "CODEC(DELTA,LZ4)"      (NOTE: adds LZ4 byte compression; prior DELTA stored
 *                                             delta-encoded values without any byte-level compression)
 *   DELTADELTA    → "CODEC(DELTADELTA,LZ4)" (same NOTE as DELTA: adds LZ4 byte compression)
 *   MV_ENTRY_DICT → not migrated (dict-encoded MV index, not a raw codec)
 *   CLP / CLPV2 / CLPV2_ZSTD / CLPV2_LZ4 → not migrated (CLP has its own pipeline)
 * </pre>
 *
 * <p>The codec pipeline ({@code codecSpec}) currently writes V7 segments for single-value INT/LONG
 * RAW columns only. Use {@link #migrate(FieldConfig, Schema)} (the schema-aware overload) so
 * non-SV-INT/LONG columns retain their legacy {@code compressionCodec} until the codec pipeline
 * supports those types.
 *
 * <h3>Usage</h3>
 * <p>Call {@link #migrateTableConfig(TableConfig)} to produce a new {@link TableConfig} where
 * every migratable {@link FieldConfig} has its {@code compressionCodec} replaced by the
 * recommended {@code codecSpec}.  The original {@link TableConfig} is not modified.
 *
 * <p>This helper is intended for use in tooling and admin APIs (e.g. a minion task or a
 * controller endpoint) that performs transparent migration after upgrading to 1.6.0.
 * In 1.5.0 both pipelines remain supported so that segments produced with either can be read
 * by mixed-version clusters during rolling upgrade.
 *
 * <p><b>Concurrency:</b> the methods on this helper are pure functions over the input config —
 * they do not touch ZooKeeper. Callers that persist the migrated config <em>must</em> use a
 * version-checked write (Helix CAS / ZK optimistic locking) to avoid clobbering concurrent admin
 * updates that may have landed between the read-and-migrate and the write.
 *
 * <p>TODO: Remove this class in 2.0 once all deployments have completed migration from
 * {@code compressionCodec} to {@code codecSpec} and the legacy {@code compressionCodec}
 * field is removed from {@link FieldConfig}.
 */
// TODO: Remove in 2.0 (see class Javadoc)
public final class CompressionCodecMigrator {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompressionCodecMigrator.class);

  private CompressionCodecMigrator() {
  }

  /**
   * Returns the codec pipeline {@code codecSpec} string equivalent to {@code codec}, or
   * {@code null} if the codec cannot be expressed in the pipeline framework (e.g. CLP,
   * MV_ENTRY_DICT).
   *
   * @param codec the legacy compression codec; {@code null} is treated as "no codec" and returns
   *              {@code null}
   */
  @Nullable
  public static String toCodecSpec(@Nullable FieldConfig.CompressionCodec codec) {
    if (codec == null) {
      return null;
    }
    switch (codec) {
      case LZ4:
        return "LZ4";
      case ZSTANDARD:
        return "ZSTD(3)";
      case SNAPPY:
        return "SNAPPY";
      case GZIP:
        return "GZIP";
      case DELTA:
        return "CODEC(DELTA,LZ4)";
      case DELTADELTA:
        return "CODEC(DELTADELTA,LZ4)";
      default:
        // PASS_THROUGH: no-op codec, no migration target
        // MV_ENTRY_DICT, CLP, CLPV2, CLPV2_ZSTD, CLPV2_LZ4: not supported in codec pipeline
        return null;
    }
  }

  /**
   * Returns {@code true} if {@code codec} can be transparently migrated to a codec pipeline spec.
   */
  public static boolean isMigratable(@Nullable FieldConfig.CompressionCodec codec) {
    return toCodecSpec(codec) != null;
  }

  /**
   * Returns {@code true} if the given column can be safely migrated to a codec pipeline spec.
   * Unlike {@link #isMigratable(FieldConfig.CompressionCodec)}, this overload also verifies that
   * the column's stored type is single-value INT or LONG — the only types the V7 codec pipeline
   * currently supports for LZ4 / ZSTANDARD.
   *
   * @param fieldConfig the field configuration to evaluate
   * @param schema      the table schema; if {@code null} the column-type check is skipped (legacy
   *                    callers that have no schema available)
   */
  public static boolean isMigratableWithSchema(FieldConfig fieldConfig, @Nullable Schema schema) {
    FieldConfig.CompressionCodec codec = fieldConfig.getCompressionCodec();
    if (!isMigratable(codec)) {
      return false;
    }
    if (schema == null) {
      return true;
    }
    FieldSpec fieldSpec = schema.getFieldSpecFor(fieldConfig.getName());
    if (fieldSpec == null) {
      return false;
    }
    if (!fieldSpec.isSingleValueField()) {
      return false;
    }
    FieldSpec.DataType stored = fieldSpec.getDataType().getStoredType();
    return stored == FieldSpec.DataType.INT || stored == FieldSpec.DataType.LONG;
  }

  /**
   * Returns a new {@link FieldConfig} where the {@code compressionCodec} has been replaced by its
   * equivalent {@code codecSpec}, or the original instance if the codec is not migratable.
   *
   * <p>This overload is column-type-agnostic. Use {@link #migrate(FieldConfig, Schema)} when a
   * schema is available to avoid migrating columns whose stored type is not yet supported by the
   * codec pipeline (e.g. FLOAT, DOUBLE, STRING, BYTES, or multi-value columns).
   */
  public static FieldConfig migrate(FieldConfig fieldConfig) {
    FieldConfig.CompressionCodec codec = fieldConfig.getCompressionCodec();
    String codecSpec = toCodecSpec(codec);
    if (codecSpec == null) {
      return fieldConfig;
    }
    if (codec == FieldConfig.CompressionCodec.DELTA || codec == FieldConfig.CompressionCodec.DELTADELTA) {
      // Migrating DELTA/DELTADELTA to CODEC(...,LZ4) adds byte-level compression that the legacy
      // path did not have. Existing segments will be rewritten to V7 on next reload — log so
      // operators can plan for the rewrite cost.
      LOGGER.warn("Migrating column '{}' from {} to '{}' adds LZ4 byte compression. Existing segments will be"
              + " rewritten to V7 on next reload.", fieldConfig.getName(), codec, codecSpec);
    }
    return new FieldConfig.Builder(fieldConfig)
        .withCodecSpec(codecSpec)
        .build();
  }

  /**
   * Returns a new {@link FieldConfig} where the {@code compressionCodec} has been replaced by its
   * equivalent {@code codecSpec}, or the original instance if the codec is not migratable for the
   * given column's stored type.
   *
   * <p>When {@code schema} is provided the migration is skipped for columns whose stored type is
   * not INT or LONG (SV), because the V7 codec pipeline does not yet support those types.
   *
   * @param fieldConfig the field configuration to migrate
   * @param schema      the table schema; if {@code null} falls back to type-agnostic migration
   */
  public static FieldConfig migrate(FieldConfig fieldConfig, @Nullable Schema schema) {
    if (!isMigratableWithSchema(fieldConfig, schema)) {
      return fieldConfig;
    }
    return migrate(fieldConfig);
  }

  /**
   * Returns a new {@link TableConfig} where every migratable {@link FieldConfig} in the field
   * config list has its {@code compressionCodec} replaced by the equivalent {@code codecSpec}.
   * The original {@link TableConfig} is not modified.
   *
   * <p>Uses schema-aware migration: columns whose stored type is not SV INT or LONG are left
   * on the legacy {@code compressionCodec} path until the codec pipeline supports them.
   *
   * @param tableConfig the table configuration to migrate
   * @param schema      the table schema used for column-type checks; {@code null} falls back to
   *                    type-agnostic migration (legacy behaviour)
   */
  public static TableConfig migrateTableConfig(TableConfig tableConfig, @Nullable Schema schema) {
    List<FieldConfig> original = tableConfig.getFieldConfigList();
    if (original == null || original.isEmpty()) {
      return tableConfig;
    }

    boolean anyMigrated = false;
    List<FieldConfig> migrated = new ArrayList<>(original.size());
    for (FieldConfig fc : original) {
      FieldConfig updated = migrate(fc, schema);
      migrated.add(updated);
      if (updated != fc) {
        anyMigrated = true;
      }
    }

    if (!anyMigrated) {
      return tableConfig;
    }

    TableConfig copy = new TableConfig(tableConfig);
    copy.setFieldConfigList(migrated);
    return copy;
  }

  /**
   * Returns a new {@link TableConfig} where every migratable {@link FieldConfig} in the field
   * config list has its {@code compressionCodec} replaced by the equivalent {@code codecSpec}.
   * The original {@link TableConfig} is not modified.
   *
   * <p>This overload is column-type-agnostic. Prefer {@link #migrateTableConfig(TableConfig, Schema)}
   * when a schema is available.
   *
   * <p>If no field configs are migratable, the original {@link TableConfig} is returned unchanged.
   */
  public static TableConfig migrateTableConfig(TableConfig tableConfig) {
    List<FieldConfig> original = tableConfig.getFieldConfigList();
    if (original == null || original.isEmpty()) {
      return tableConfig;
    }

    boolean anyMigrated = false;
    List<FieldConfig> migrated = new ArrayList<>(original.size());
    for (FieldConfig fc : original) {
      FieldConfig updated = migrate(fc);
      migrated.add(updated);
      if (updated != fc) {
        anyMigrated = true;
      }
    }

    if (!anyMigrated) {
      return tableConfig;
    }

    TableConfig copy = new TableConfig(tableConfig);
    copy.setFieldConfigList(migrated);
    return copy;
  }
}
