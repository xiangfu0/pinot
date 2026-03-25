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

import java.io.Closeable;
import javax.annotation.Nullable;


/**
 * SPI interface for storing and retrieving tablet manifests.
 *
 * <p>Manifests are stored outside ZooKeeper to keep ZK metadata bounded by tablet count.
 * Implementations may use deep store (S3/GCS/HDFS), a controller metadata store, or
 * other durable storage.</p>
 *
 * <p>Implementations must be thread-safe after initialization.</p>
 */
public interface TabletManifestStore extends Closeable {

  /**
   * Initializes the store with a base URI for manifest storage.
   *
   * @param baseUri base URI (e.g. "s3://bucket/pinot-manifests")
   */
  void init(String baseUri);

  /**
   * Writes a tablet manifest and returns its storage URI.
   *
   * @param tableNameWithType table name with type suffix
   * @param tabletId tablet identifier
   * @param manifestJson serialized TabletManifest JSON
   * @return URI where the manifest was stored
   */
  String writeManifest(String tableNameWithType, String tabletId, String manifestJson);

  /**
   * Reads a tablet manifest by its URI.
   *
   * @param manifestUri URI returned by {@link #writeManifest}
   * @return serialized TabletManifest JSON, or null if not found
   */
  @Nullable
  String readManifest(String manifestUri);

  /**
   * Deletes a tablet manifest by its URI.
   *
   * @param manifestUri URI to delete
   */
  void deleteManifest(String manifestUri);
}
