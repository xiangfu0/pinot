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
 * The type of data provided in an INSERT INTO request.
 *
 * <ul>
 *   <li>{@link #ROW} — inline row values (e.g., {@code INSERT INTO t VALUES (...)}).</li>
 *   <li>{@link #FILE} — reference to an external file (e.g., {@code INSERT INTO t FROM FILE '...'}).</li>
 * </ul>
 *
 * <p><strong>Wire compatibility: these enum values are PERMANENT.</strong> They are serialized into
 * the {@code InsertStatementManifest} JSON written to ZooKeeper. Renaming or removing a value
 * would orphan every manifest persisted by an older controller; a rolling upgrade that reads such
 * a blob would fail Jackson deserialization. To add a new insert type, append a new value; never
 * reuse or rename existing ones.
 *
 * <p>This enum is thread-safe (immutable).
 */
@InterfaceStability.Evolving
public enum InsertType {
  ROW,
  FILE;

  /**
   * Strict deserializer for the {@code insertType} JSON field. Uses {@link Locale#ROOT} to match
   * sibling SPI enums ({@code InsertConsistencyMode}, {@code InsertStatementState}). Throws
   * {@link IllegalArgumentException} on unknown values rather than silently defaulting — wire
   * mismatches across controller versions must surface as deserialization failures.
   */
  @JsonCreator
  @Nullable
  public static InsertType fromJson(@Nullable String value) {
    if (value == null) {
      return null;
    }
    return InsertType.valueOf(value.toUpperCase(Locale.ROOT));
  }
}
