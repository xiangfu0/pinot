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
package org.apache.pinot.common.evaluator;

/// Thrown when a Groovy transform expression is constructed while the ingestion-time Groovy policy is disabled.
///
/// This is a distinct exception type (rather than a generic {@link IllegalStateException}) so that callers can tell
/// a deliberate policy rejection apart from a malformed/invalid expression, and surface a clear configuration error
/// instead of silently ignoring or executing the transform.
public class GroovyDisabledException extends IllegalStateException {
  public GroovyDisabledException(String message) {
    super(message);
  }
}
