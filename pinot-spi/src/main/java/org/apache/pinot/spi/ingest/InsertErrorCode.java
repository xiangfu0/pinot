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

import org.apache.pinot.spi.annotations.InterfaceStability;

/**
 * Stable string codes returned in {@link InsertResult#getErrorCode()}.
 *
 * <p>These string values are surfaced to dashboards, alerts, and client-side retry logic. Keep them
 * frozen — once shipped, renaming a code requires coordinated client migration.
 *
 * <p>Codes are organized by lifecycle phase:
 * <ul>
 *   <li><strong>Pre-acceptance</strong> ({@code REJECTED} state): coordinator rejected the request
 *       before persisting a manifest. No statementId is queryable via {@code getStatus} afterward.</li>
 *   <li><strong>Post-acceptance</strong> ({@code ABORTED}/{@code VISIBLE} state): manifest exists in
 *       ZK; the code distinguishes failure modes during execution or persistence.</li>
 *   <li><strong>Query-side</strong>: rejection of a {@code getStatus}/{@code abort}/{@code complete}
 *       call — typically because the statement does not exist or is in an incompatible state.</li>
 * </ul>
 *
 * <p>This is a constants holder, not an enum, because executor plugins may emit additional codes
 * the coordinator does not know about. Treat the field as opaque on the wire.
 */
@InterfaceStability.Evolving
public final class InsertErrorCode {
  private InsertErrorCode() {
  }

  // ---- Pre-acceptance (state=REJECTED, no manifest) -----------------------------------------

  /** Coordinator is not started — feature flag off or controller starting/stopping. */
  public static final String COORDINATOR_NOT_READY = "COORDINATOR_NOT_READY";
  /** Could not resolve the table name (does not exist, ambiguous hybrid type, type mismatch). */
  public static final String TABLE_RESOLUTION_ERROR = "TABLE_RESOLUTION_ERROR";
  /** Idempotency reservation could not be created (typically a ZK-availability issue). */
  public static final String IDEMPOTENCY_ERROR = "IDEMPOTENCY_ERROR";
  /** Same requestId was previously used with a different payloadHash — rejecting to avoid silent overwrite. */
  public static final String IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT";
  /** Requested {@link InsertConsistencyMode} not supported by this controller version. */
  public static final String UNSUPPORTED_CONSISTENCY_MODE = "UNSUPPORTED_CONSISTENCY_MODE";
  /** No {@link InsertExecutor} registered for the requested {@link InsertType}. */
  public static final String NO_EXECUTOR = "NO_EXECUTOR";
  /** Stale-rebind race lost; client should retry. */
  public static final String REBIND_RACE_LOST = "REBIND_RACE_LOST";
  /** Failed to persist the manifest in ZK during initial create. */
  public static final String STORE_ERROR = "STORE_ERROR";
  /** Segment names provided to /insert/complete failed Pinot's segment-name pattern. */
  public static final String INVALID_SEGMENT_NAME = "INVALID_SEGMENT_NAME";
  /** ROW insert request had null/empty rows; coordinator rejects pre-acceptance. */
  public static final String EMPTY_ROWS = "EMPTY_ROWS";

  // ---- Post-acceptance (state=ABORTED, manifest exists) -------------------------------------

  /** Executor threw during execute(); manifest is now ABORTED. */
  public static final String EXECUTOR_ERROR = "EXECUTOR_ERROR";
  /** Executor threw but the manifest reached a terminal state durably; data may already be queryable. */
  public static final String EXECUTOR_ERROR_BUT_DURABLE = "EXECUTOR_ERROR_BUT_DURABLE";
  /** Failed to persist a state transition after CAS retries; cleanup sweep will reconcile. */
  public static final String STATE_PERSIST_ERROR = "STATE_PERSIST_ERROR";
  /** Concurrent writer changed the manifest's state during this operation. */
  public static final String CONCURRENT_STATE_CHANGE = "CONCURRENT_STATE_CHANGE";
  /** Tried a state transition that is not legal from the current state (e.g., abort a VISIBLE). */
  public static final String INVALID_STATE = "INVALID_STATE";
  /** /insert/complete invoked with a wrong-typed FILE executor wrapper. */
  public static final String WRONG_EXECUTOR = "WRONG_EXECUTOR";
  /** Failed to persist Minion task name to the manifest after task creation. */
  public static final String TASK_NAME_PERSIST_ERROR = "TASK_NAME_PERSIST_ERROR";

  // ---- Query-side ---------------------------------------------------------------------------

  /** No manifest exists for the requested statementId. */
  public static final String NOT_FOUND = "NOT_FOUND";
}
