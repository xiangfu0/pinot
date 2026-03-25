<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Phase 1 Config And APIs

This document describes the repo-local Phase 1 surface for Pinot lakehouse-native mode.
Phase 1 is query-in-place only. It is opt-in, disabled by default, and does not change the behavior of existing
Pinot-native OFFLINE, REALTIME, or HYBRID tables.

## Supported Config Names

The landed shared contract uses these names:

- `lakehouseConfig`
- `LakehouseMode.ICEBERG_NATIVE`
- `LakehouseReadVisibilityMode.SNAPSHOT_ONLY`
- `LakehouseReadVisibilityMode.HOT_PLUS_SNAPSHOT`
- `LakehouseWriteMode.DISABLED`
- `LakehouseWriteMode.DIRECT_APPEND`
- `LakehouseWriteMode.CDC`
- `IcebergCatalogType.REST`
- `IcebergCatalogType.FILESYSTEM`
- `IcebergCatalogType.HADOOP`
- `IcebergCatalogType.CUSTOM`

Phase 1 uses the config as a declaration of intent and validation input. It does not enable direct writes yet.

## Table Config Shape

The new top-level section is `lakehouseConfig`. The important fields are:

- `enabled`
- `mode`
- `catalogConfig`
- `readVisibilityMode`
- `writeMode`
- `tabletConfig`
- `sidecarConfig`

The supporting nested configs are:

- `IcebergCatalogConfig`
- `LakehouseTabletConfig`
- `LakehouseSidecarConfig`

## Phase 1 Contract Notes

- `mode` must be `ICEBERG_NATIVE` for lakehouse-native tables.
- `readVisibilityMode` may be declared as `SNAPSHOT_ONLY` or `HOT_PLUS_SNAPSHOT`, but Phase 1 only executes
  query-in-place against pinned snapshots.
- `writeMode` should remain `DISABLED` for Phase 1 query-in-place tables.
- `lakehouseConfig` is optional and disabled by default, so older Pinot-native table configs remain valid.
- `catalogConfig.pluginName` must resolve to a controller adapter class, or the same class must be provided in
  `catalogConfig.properties.controller.adapter.class`.
- `TabletMetadataEnvelope` is the small Helix-visible object.
- `TabletManifest` is the full manifest that lives outside ZooKeeper.
- `MicrosegmentDescriptor` describes file-level execution units inside a tablet.
- `SidecarManifest` is reserved for later phase acceleration overlays.
- `sidecarConfig` is scaffold-only in Phase 1 and does not participate in correctness.

## Query And Admin APIs

The Phase 1 controller and broker surface is intentionally narrow, and the branch already implements the
controller validation and broker guardrails listed below:

- Query options:
  - `snapshotId`
  - `branch`
  - `tag`
  - `asOfTimestampMs`
- Controller APIs:
  - `GET /tables/{tableNameWithType}/lakehouse/refs`
  - `POST /tables/{tableNameWithType}/lakehouse/refresh`
  - `GET /tables/{tableNameWithType}/lakehouse/tablets`
  - `GET /tables/{tableNameWithType}/lakehouse/tablets/{tabletId}`
- Controller validation behavior:
  - enabled lakehouse configs are validated on the table create/update path before persistence
  - validation rejects non-`OFFLINE` tables, missing catalog wiring, unsupported `mode` values, and non-disabled
    `writeMode`
- Controller request validation:
  - refresh and refs reject requests that provide more than one of `snapshotId`, `branch`, `tag`, or
    `asOfTimestampMs`
- Broker validation behavior:
  - reject multi-stage execution for enabled `ICEBERG_NATIVE` tables during Phase 1

There is no separate `/tables/{tableName}/lakehouse/validate` endpoint in this branch yet. Validation is wired into
the normal table create/update flow, and the dedicated lakehouse controller APIs cover refs, refresh, and tablet
inspection.

## Important Limits

- Do not model one Iceberg file as one Helix partition.
- Do not store full file-level Iceberg metadata in ZooKeeper.
- Do not require sidecars for correctness.
- Do not treat `HOT_PLUS_SNAPSHOT` as a transactional hot-store guarantee in Phase 1.
- Do not claim MSQ support for lakehouse-native tables in Phase 1.
- Do not assume Parquet footers will carry Pinot-specific index blobs.

## What To Look For In Later Phases

Later phases will add the write coordinator, hot visibility tracking, and sidecar acceleration, but those are
deliberately outside the Phase 1 documentation and example configs in this folder.
