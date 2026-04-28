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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.InterfaceStability;


/**
 * Immutable data transfer object representing the result of an INSERT INTO operation.
 *
 * <p>Contains the current state of the statement, any produced segment names, and optional
 * human-readable messages or error codes.
 *
 * <p>Instances are immutable and therefore thread-safe.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@InterfaceStability.Evolving
public class InsertResult {
  private final String _statementId;
  private final InsertStatementState _state;
  private final String _message;
  private final List<String> _segmentNames;
  private final String _errorCode;

  @JsonCreator
  public InsertResult(
      @JsonProperty("statementId") String statementId,
      @JsonProperty("state") InsertStatementState state,
      @JsonProperty("message") String message,
      @JsonProperty("segmentNames") List<String> segmentNames,
      @JsonProperty("errorCode") String errorCode) {
    // Validate non-null state and statementId on the wire path (matches Builder.build's check).
    // A wire-deserialized result with null state would silently propagate through downstream
    // consumers that read result.getState().name() or compare against canonical state values.
    if (state == null) {
      throw new IllegalArgumentException("InsertResult.state is required (statementId=" + statementId + ")");
    }
    if (statementId == null) {
      throw new IllegalArgumentException("InsertResult.statementId is required (state=" + state + ")");
    }
    _statementId = statementId;
    _state = state;
    _message = message;
    _segmentNames = segmentNames != null
        ? Collections.unmodifiableList(new ArrayList<>(segmentNames)) : Collections.emptyList();
    _errorCode = errorCode;
  }

  private InsertResult(Builder builder) {
    _statementId = builder._statementId;
    _state = builder._state;
    _message = builder._message;
    _segmentNames = builder._segmentNames != null
        ? Collections.unmodifiableList(new ArrayList<>(builder._segmentNames)) : Collections.emptyList();
    _errorCode = builder._errorCode;
  }

  /**
   * Returns the statement ID this result is for. Non-null for results constructed via
   * {@link Builder#build()}; can only be null when the JSON deserializer encounters a malformed
   * payload (in which case the caller should reject the result).
   */
  @JsonProperty("statementId")
  public String getStatementId() {
    return _statementId;
  }

  /**
   * Returns the manifest state this result represents. Non-null for results constructed via
   * {@link Builder#build()}; can only be null when the JSON deserializer encounters a malformed
   * payload.
   */
  @JsonProperty("state")
  public InsertStatementState getState() {
    return _state;
  }

  @JsonProperty("message")
  @Nullable
  public String getMessage() {
    return _message;
  }

  @JsonProperty("segmentNames")
  public List<String> getSegmentNames() {
    return _segmentNames;
  }

  @JsonProperty("errorCode")
  @Nullable
  public String getErrorCode() {
    return _errorCode;
  }

  /**
   * Builder for constructing {@link InsertResult} instances.
   */
  public static class Builder {
    private String _statementId;
    private InsertStatementState _state;
    private String _message;
    private List<String> _segmentNames;
    private String _errorCode;

    public Builder setStatementId(String statementId) {
      _statementId = statementId;
      return this;
    }

    public Builder setState(InsertStatementState state) {
      _state = state;
      return this;
    }

    public Builder setMessage(String message) {
      _message = message;
      return this;
    }

    public Builder setSegmentNames(List<String> segmentNames) {
      _segmentNames = segmentNames;
      return this;
    }

    public Builder setErrorCode(String errorCode) {
      _errorCode = errorCode;
      return this;
    }

    /**
     * Builds the {@link InsertResult}. Validates that {@code state} is non-null — a result with no
     * state is meaningless to clients and indicates a programming error in the executor or
     * coordinator that constructed it. {@code statementId} is similarly required since callers
     * use it to correlate status/abort/list operations.
     */
    public InsertResult build() {
      if (_state == null) {
        throw new IllegalStateException("InsertResult.state is required; set it via setState() before build(). "
            + "A null state cannot be surfaced to clients (statementId=" + _statementId + ")");
      }
      if (_statementId == null) {
        throw new IllegalStateException("InsertResult.statementId is required; set it via setStatementId() before "
            + "build(). Clients cannot correlate without a statementId (state=" + _state + ")");
      }
      return new InsertResult(this);
    }
  }
}
