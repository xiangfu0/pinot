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

# Phase 1 Operator Runbook

This runbook covers the repo-local Phase 1 operating model for Pinot lakehouse-native mode.
It is scoped to query-in-place over Parquet-backed Iceberg tables.

## Before You Enable It

1. Confirm the feature flag is enabled in the controller and broker deployment.
2. Confirm the lakehouse adapter wiring required by the controller is present in either
   `lakehouseConfig.catalogConfig.pluginName` or `lakehouseConfig.catalogConfig.properties.controller.adapter.class`.
3. Confirm the table config uses `lakehouseConfig.enabled=true` and `LakehouseMode.ICEBERG_NATIVE`.
4. Confirm the table is intended for query-in-place only in Phase 1.
5. Confirm the table is not relying on MSQ for correctness or availability.
6. Run the normal table create/update path first so controller validation can reject unsupported lakehouse config
   shapes before the table is persisted.

## Expected Table Behavior

- Existing Pinot-native tables should behave exactly as before.
- Lakehouse-native tables should only be enabled for the explicit opt-in tables.
- Routing should be tablet-based, not file-based.
- Snapshot pinning should happen before query routing.
- The controller should keep the large manifest outside ZooKeeper.
- Server-side tablet stubs enter through a dedicated load hook, but the default load path is still unsupported
  until the real manifest/Parquet loader lands.

## Operational Guardrails

- A single Iceberg file must not become a single Helix partition.
- If you see tablet counts approaching file counts, stop and revisit the grouping policy.
- Keep the ZK envelope small and stable.
- Keep sidecars disabled unless you are working on a later phase.
- Treat `HOT_PLUS_SNAPSHOT` as a future visibility mode, not as a Phase 1 promise of transactional hot-state
  semantics.

## Refresh And Inspect

Phase 1 already exposes controller-facing refresh, refs, and inspect operations for snapshot/tablet management.
Operators should use them to:

- resolve the current snapshot or ref before a refresh
- refresh the current snapshot and tablet mapping
- inspect the current tablet envelopes
- inspect the resolved Iceberg refs for a table

Validation happens on the normal controller table create/update path, so use that path to catch unsupported Phase 1
configs early.

## What To Watch

- `LAKEHOUSE_TABLET_COUNT`
- `LAKEHOUSE_STALE_MANIFEST_COUNT`
- `LAKEHOUSE_SNAPSHOT_REFRESH_SUCCESS`
- `LAKEHOUSE_SNAPSHOT_REFRESH_FAILURE`
- `LAKEHOUSE_MANIFEST_PUBLISH_SUCCESS`
- `LAKEHOUSE_MANIFEST_PUBLISH_FAILURE`
- broker routed tablet count
- broker snapshot pinning latency
- broker validation failures for conflicting snapshot selectors
- broker rejections for lakehouse tables sent down the multi-stage path

## Rollback Guidance

If a lakehouse-native table is causing operational issues:

1. Disable the table-level `lakehouseConfig`.
2. Return the table to standard Pinot-native behavior.
3. Investigate the manifest pointer, snapshot resolution, and tablet grouping policy.

Do not attempt to make the table depend on sidecars or MSQ for recovery in Phase 1.
