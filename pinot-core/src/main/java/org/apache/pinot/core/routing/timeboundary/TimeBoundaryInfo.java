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
package org.apache.pinot.core.routing.timeboundary;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class TimeBoundaryInfo {
  private final String _timeColumn;
  private final String _timeValue;

  @JsonCreator
  public TimeBoundaryInfo(@JsonProperty("timeColumn") String timeColumn, @JsonProperty("timeValue") String timeValue) {
    _timeColumn = timeColumn;
    _timeValue = timeValue;
  }

  public String getTimeColumn() {
    return _timeColumn;
  }

  public String getTimeValue() {
    return _timeValue;
  }
}
