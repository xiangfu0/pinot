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
package org.apache.pinot.common.lakehouse;

import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import javax.annotation.Nullable;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.lakehouse.TabletManifestStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link TabletManifestStore} implementation that reads and writes tablet manifest JSON files
 * to a deep store (S3, GCS, HDFS, local filesystem, etc.) via {@link PinotFS}.
 *
 * <p>Manifests are stored at: {@code {baseUri}/{tableNameWithType}/{tabletId}/manifest.json}.
 * This keeps ZooKeeper metadata bounded by tablet count, as the actual manifest content lives
 * in the deep store rather than in ZK znodes.</p>
 *
 * <p>Thread-safe after {@link #init(String)} completes. All operations delegate to the
 * {@link PinotFS} instance resolved from the base URI scheme.</p>
 */
public class FileSystemTabletManifestStore implements TabletManifestStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemTabletManifestStore.class);
  private static final String MANIFEST_FILENAME = "manifest.json";

  private volatile URI _baseUri;
  private volatile PinotFS _pinotFS;

  @Override
  public void init(String baseUri) {
    Preconditions.checkNotNull(baseUri, "Base URI must not be null");
    _baseUri = URI.create(baseUri);
    String scheme = _baseUri.getScheme();
    if (scheme == null) {
      scheme = "file";
    }
    _pinotFS = PinotFSFactory.create(scheme);
    LOGGER.info("Initialized FileSystemTabletManifestStore with base URI: {}", _baseUri);
  }

  @Override
  public String writeManifest(String tableNameWithType, String tabletId, String manifestJson) {
    ensureInitialized();
    Preconditions.checkNotNull(tableNameWithType, "tableNameWithType must not be null");
    Preconditions.checkNotNull(tabletId, "tabletId must not be null");
    Preconditions.checkNotNull(manifestJson, "manifestJson must not be null");

    URI manifestUri = buildManifestUri(tableNameWithType, tabletId);

    try {
      // Write JSON to a temporary local file, then copy to the deep store
      File tempFile = File.createTempFile("tablet-manifest-", ".json");
      try {
        Files.write(tempFile.toPath(), manifestJson.getBytes(StandardCharsets.UTF_8));

        // Ensure the parent directory exists
        URI parentDir = buildParentUri(tableNameWithType, tabletId);
        _pinotFS.mkdir(parentDir);

        _pinotFS.copyFromLocalFile(tempFile, manifestUri);
        LOGGER.debug("Wrote tablet manifest: {}", manifestUri);
      } finally {
        if (!tempFile.delete()) {
          LOGGER.warn("Failed to delete temp file: {}", tempFile);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to write tablet manifest to " + manifestUri, e);
    }

    return manifestUri.toString();
  }

  @Nullable
  @Override
  public String readManifest(String manifestUri) {
    ensureInitialized();
    Preconditions.checkNotNull(manifestUri, "manifestUri must not be null");

    URI uri = URI.create(manifestUri);
    try {
      if (!_pinotFS.exists(uri)) {
        LOGGER.debug("Manifest not found at: {}", uri);
        return null;
      }

      try (InputStream is = _pinotFS.open(uri);
           BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
          sb.append(line);
        }
        String json = sb.toString();
        LOGGER.debug("Read tablet manifest from: {} ({} bytes)", uri, json.length());
        return json;
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read tablet manifest from " + uri, e);
    }
  }

  @Override
  public void deleteManifest(String manifestUri) {
    ensureInitialized();
    Preconditions.checkNotNull(manifestUri, "manifestUri must not be null");

    URI uri = URI.create(manifestUri);
    try {
      if (_pinotFS.exists(uri)) {
        _pinotFS.delete(uri, false);
        LOGGER.debug("Deleted tablet manifest: {}", uri);
      } else {
        LOGGER.debug("Manifest already absent, nothing to delete: {}", uri);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to delete tablet manifest at " + uri, e);
    }
  }

  @Override
  public void close() throws IOException {
    // PinotFS lifecycle is managed by PinotFSFactory; nothing to close here.
    LOGGER.info("Closed FileSystemTabletManifestStore for base URI: {}", _baseUri);
  }

  /**
   * Builds the full manifest URI for a given table and tablet.
   *
   * @param tableNameWithType table name with type suffix
   * @param tabletId tablet identifier
   * @return URI pointing to the manifest file
   */
  private URI buildManifestUri(String tableNameWithType, String tabletId) {
    String basePath = _baseUri.toString();
    if (!basePath.endsWith("/")) {
      basePath += "/";
    }
    return URI.create(basePath + tableNameWithType + "/" + tabletId + "/" + MANIFEST_FILENAME);
  }

  /**
   * Builds the parent directory URI for a given table and tablet.
   *
   * @param tableNameWithType table name with type suffix
   * @param tabletId tablet identifier
   * @return URI pointing to the parent directory
   */
  private URI buildParentUri(String tableNameWithType, String tabletId) {
    String basePath = _baseUri.toString();
    if (!basePath.endsWith("/")) {
      basePath += "/";
    }
    return URI.create(basePath + tableNameWithType + "/" + tabletId);
  }

  /**
   * Ensures that the store has been properly initialized before use.
   */
  private void ensureInitialized() {
    Preconditions.checkState(_pinotFS != null,
        "FileSystemTabletManifestStore has not been initialized. Call init() first.");
  }
}
