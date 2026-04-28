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
package org.apache.pinot.spi.ingest;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Locale;
import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.InterfaceStability;

/**
 * Lifecycle states for an INSERT INTO statement.
 *
 * <p>The v1 state machine is deliberately minimal: ACCEPTED → VISIBLE on success and
 * ACCEPTED → ABORTED on any failure. No intermediate states are exposed to the wire format,
 * since v2's two-phase commit (NEW/PREPARED/COMMITTED) is not yet designed and shipping
 * placeholder names now would freeze them before the v2 protocol is locked.
 *
 * <pre>
 *   ACCEPTED -> VISIBLE      (ROW insert: synchronous segment build+upload; FILE insert: auto-
 *                             complete via cleanup sweep when Minion task succeeds)
 *   ACCEPTED -> ABORTED      (executor exception, user abort, cleanup-sweep timeout, task failure)
 * </pre>
 *
 * <p>Forward-compat: a v2 manifest carrying a state name not in this enum will be rejected at
 * deserialization by {@code InsertStatementManifest.MAX_SUPPORTED_VERSION}, so v1 readers do not
 * need to know about future states.
 *
 * <p>Garbage collection is a store-level operation (the manifest ZNode is deleted). There is no
 * {@code GC} state — a GC'd statement is simply absent from the store, indistinguishable from one
 * that never existed.
 *
 * <ul>
 *   <li>{@link #ACCEPTED} — coordinator has accepted the statement for processing.</li>
 *   <li>{@link #VISIBLE} — segments are live and queryable.</li>
 *   <li>{@link #ABORTED} — accepted statement was cancelled or failed; resources may still need cleanup.</li>
 *   <li>{@link #REJECTED} — request was rejected before acceptance; <strong>no manifest is created in
 *       ZK</strong>. Distinct from ABORTED: callers cannot {@code getStatus(statementId)} a REJECTED
 *       result. Used for validation rejections (table not found, no executor, unsupported consistency
 *       mode, etc.).</li>
 * </ul>
 *
 * <p><strong>Wire compatibility: these enum values are PERMANENT.</strong> Names are serialized
 * into the {@code InsertStatementManifest} JSON written to ZooKeeper and into the REST response
 * JSON. Renaming or removing a value would break rolling upgrades and client integrations.
 *
 * <p>This enum is thread-safe (immutable).
 */
@InterfaceStability.Evolving
public enum InsertStatementState {
  ACCEPTED,
  VISIBLE,
  ABORTED,
  REJECTED;

  /**
   * Strict JSON deserializer that fails loudly on unknown values. A future controller version that
   * introduces a new state name and writes it to ZK will not have its manifests silently mis-parsed
   * by an older reader — instead the reader gets a clear error pointing at the unknown name.
   * Pairs with {@link InsertStatementManifest#MAX_SUPPORTED_VERSION} for forward-compat safety.
   */
  @JsonCreator
  @Nullable
  public static InsertStatementState fromJson(@Nullable String value) {
    if (value == null) {
      return null;  // null/absent → caller defaults; consistent with InsertConsistencyMode.fromJson
    }
    try {
      return InsertStatementState.valueOf(value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unknown InsertStatementState: '" + value + "'. This controller version does not "
              + "recognize that state. Supported: ACCEPTED, VISIBLE, ABORTED, REJECTED.");
    }
  }
}
