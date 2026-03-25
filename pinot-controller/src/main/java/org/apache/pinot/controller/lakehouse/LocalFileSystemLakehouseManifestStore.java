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
package org.apache.pinot.controller.lakehouse;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.apache.pinot.spi.lakehouse.LakehouseManifestStore;
import org.apache.pinot.spi.lakehouse.TabletManifest;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Stores lakehouse tablet manifests as JSON files under the controller data directory.
 *
 * <p>Thread-safety is delegated to the underlying filesystem. Each refresh writes deterministic per-table, per-snapshot
 * paths, so concurrent refreshes for different tables do not share files.</p>
 */
public class LocalFileSystemLakehouseManifestStore
    implements LakehouseManifestStore {
  private final Path _rootDirectory;

  public LocalFileSystemLakehouseManifestStore(Path rootDirectory) {
    _rootDirectory = rootDirectory;
  }

  @Override
  public String persistTabletManifest(TabletManifest manifest)
      throws IOException {
    Path manifestPath = buildManifestPath(manifest);
    Files.createDirectories(manifestPath.getParent());
    Path temporaryManifestPath = Files.createTempFile(manifestPath.getParent(), manifestPath.getFileName().toString(),
        ".tmp");
    try (OutputStream outputStream = Files.newOutputStream(temporaryManifestPath)) {
      JsonUtils.objectToOutputStream(manifest, outputStream);
    }
    try {
      Files.move(temporaryManifestPath, manifestPath, StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException e) {
      Files.move(temporaryManifestPath, manifestPath, StandardCopyOption.REPLACE_EXISTING);
    } finally {
      Files.deleteIfExists(temporaryManifestPath);
    }
    return manifestPath.toUri().toString();
  }

  @Override
  public TabletManifest fetchTabletManifest(String manifestUri)
      throws IOException {
    return JsonUtils.fileToObject(Path.of(URI.create(manifestUri)).toFile(), TabletManifest.class);
  }

  public Path getRootDirectory() {
    return _rootDirectory;
  }

  private Path buildManifestPath(TabletManifest manifest) {
    return _rootDirectory.resolve(manifest.getTableNameWithType())
        .resolve(Long.toString(manifest.getSnapshotId()))
        .resolve(manifest.getTabletId() + ".json");
  }
}
