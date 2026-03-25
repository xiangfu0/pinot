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
package org.apache.pinot.spi.lakehouse;


/**
 * Describes a single field from the lakehouse table schema.
 *
 * <p>Uses plain types to avoid leaking Iceberg API classes into foundational modules.</p>
 */
public class LakehouseFieldDescriptor {

  private final int _fieldId;
  private final String _name;
  private final String _type;
  private final boolean _required;

  public LakehouseFieldDescriptor(int fieldId, String name, String type, boolean required) {
    _fieldId = fieldId;
    _name = name;
    _type = type;
    _required = required;
  }

  public int getFieldId() {
    return _fieldId;
  }

  public String getName() {
    return _name;
  }

  /** Iceberg type string (e.g. "long", "string", "timestamp", "decimal(10,2)"). */
  public String getType() {
    return _type;
  }

  public boolean isRequired() {
    return _required;
  }
}
