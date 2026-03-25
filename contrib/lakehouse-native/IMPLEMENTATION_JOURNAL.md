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

# Lakehouse-Native Pinot Implementation Journal

This journal tracks the phased implementation of Pinot lakehouse-native mode for Iceberg + Parquet tables.
It is intentionally operational instead of aspirational: it records the active phase, ownership lanes, shared
contracts, integration points, and intentionally deferred items.

## Scope Anchor

- PEP source: `/Users/xiangfu/Downloads/pinot_iceberg_lakehouse_pep.md`
- Multi-agent prompt source: `/Users/xiangfu/Downloads/pinot_iceberg_codex_multi_agent_prompt.md`
- Related Pinot issues:
  - `#17694` `[PEP] Apache Iceberg Ingestion Plugin`
  - `#17331` `[PEP] stream plugin that supports object-storage micro-batches with optional inline batch support`

## Current Phase

- Active phase: `Phase 1 - Lakehouse-native query-in-place MVP`
- Goal:
  - Query existing Parquet-backed Iceberg tables in place
  - Introduce tablets as the Helix-visible routing unit
  - Keep the feature opt-in and disabled by default
  - Preserve existing Pinot-native OFFLINE / REALTIME / HYBRID behavior
- Explicit non-goals for this phase:
  - No direct Pinot realtime write path into Iceberg yet
  - No delete file execution yet
  - No sidecar correctness dependency
  - No one-file-per-Helix-partition mapping

## Locked Design Inputs

- Iceberg is the source of truth for snapshots, manifests, and files.
- Parquet is the base storage format for lakehouse-native mode.
- Pinot retains Helix and ZooKeeper for cluster coordination and coarse routing.
- Brokers route on `TabletMetadataEnvelope`, not individual data files.
- Servers execute at tablet, file, and row-group granularity.
- Query planning pins each query to a single Iceberg snapshot or reference.
- Phase 1 can be single-stage first, but extension points for multi-stage execution must remain explicit.

## Shared Identifiers

The implementation uses the following stable identifiers across subsystems:

- `tabletId`
- `snapshotId`
- `specId`
- `microsegmentId`
- `sidecarVersion`

Any component that needs a derived encoding must document the mapping back to these canonical identifiers.

## Agent Lanes

- Agent A: shared contracts and SPI surfaces
- Agent B: controller, catalog adapter, tablet assignment, admin APIs
- Agent C: broker routing, snapshot pinning, query options, explain/debug output
- Agent D: server tablet execution, manifest caching, Parquet read path
- Agent E: realtime direct writer and hot visibility
- Agent F: sidecar indexes and minion tasks
- Agent G: integration, scale, compatibility, and benchmark coverage
- Agent H: docs, examples, operator UX, release-note material

## Phase 1 Critical Path

1. Lock config and manifest schemas in shared modules
2. Add config parsing and validation with feature gating
3. Add controller metadata plumbing and tablet publication
4. Add broker routing on tablet envelopes
5. Add server-side tablet execution over Parquet
6. Add admin APIs, metrics, and integration tests
7. Add docs, example configs, and follow-up risk list

## Shared Contract Freeze Checklist

- `lakehouseConfig` table-config schema
- `LakehouseMode` enum and feature flag behavior
- `TabletMetadataEnvelope` schema, versioning, and compatibility rules
- `TabletManifest` schema, versioning, and compatibility rules
- `MicrosegmentDescriptor` schema
- `SidecarManifest` placeholder schema for later phases
- Minimal SPI surfaces for:
  - catalog adapter
  - manifest store
  - sidecar provider
  - write coordinator

Status: `PHASE 1 BASELINE LOCKED`

## Known Repo Integration Points

- Table config model lives in `pinot-spi`
- Table config ZK serde lives in `pinot-common`
- Table validation path is centered in `pinot-controller`
- Broker routing and query-option handling live in `pinot-broker` and `pinot-common`
- Existing Parquet reader support lives in `pinot-plugins/pinot-input-format/pinot-parquet`

## Dependency Guardrails

- Keep Iceberg-specific dependencies in plugin or adapter modules wherever practical.
- Avoid introducing heavy Iceberg transitive dependencies into foundational modules unless a stable SPI cannot
  reasonably express the boundary.
- If a foundational dependency is unavoidable, document the reason in this journal before merging the change.

## Initial Risks / Deferred Follow-Ups

- Multi-stage query engine support needs explicit staging even if Phase 1 lands through the single-stage path first.
- Delete-file execution, hot visibility, and sidecar acceleration remain follow-up phases and must not leak partial
  semantics into Phase 1.
- Upgrade and rollback behavior must be validated before the feature is enabled outside targeted testing.

## Wave 1 Outputs

### Agent A

- Landed the Phase 1 shared contract baseline in `pinot-spi` and `pinot-common`.
- `TableConfig` now supports an optional top-level `lakehouseConfig` section without changing the existing
  constructor signature.
- Added shared DTOs and SPI surfaces under `org.apache.pinot.spi.config.table.lakehouse` and
  `org.apache.pinot.spi.lakehouse`.
- Added targeted tests covering builder/json round-trip, ZK serde round-trip, and manifest JSON round-trip.

### Agent B

- Controller should persist `TabletMetadataEnvelope` objects in a dedicated ZK subtree rather than overloading
  segment metadata paths.
- Full manifests should live outside ZK and be referenced by URI + version from the envelope.
- Phase 1 controller slice should center on:
  - `PinotHelixResourceManager`
  - `ZKMetadataProvider`
  - lakehouse-specific controller services/resources
  - reuse of existing ideal-state assignment machinery with `tabletId` as the Helix partition name
- Implemented in this branch:
  - lakehouse config validation is wired into the controller create/update path
  - dedicated tablet inspect resources are available for list/get workflows
  - controller-side refresh and refs APIs are present
  - controller validation now rejects invalid Phase 1 lakehouse configs before they are persisted
  - refresh publishes Helix-visible OFFLINE tablet partitions with `tabletId` as the partition name
  - refresh persists manifests and tablet envelopes, and emits tablet / stale-manifest metrics

### Agent C

- Broker query options should use:
  - `snapshotId`
  - `branch`
  - `tag`
  - `asOfTimestampMs`
- Single-stage broker routing can keep the existing wire format by routing `tabletId` strings through the current
  segment-list path.
- Multi-stage execution should be explicitly rejected for lakehouse-native tables in Phase 1 rather than partially
  supported.
- Implemented in this branch:
  - broker query validation now rejects mutually exclusive snapshot selectors
  - broker query handling now rejects multi-stage execution for enabled `ICEBERG_NATIVE` tables in Phase 1
  - refresh and refs APIs share the same single-selector contract

### Agent D

- The server seam for Phase 1 should live at `SegmentDataManager`, not in a new public `IndexSegment` SPI.
- Tablets should expand into Parquet-backed microsegments, each served as an `IndexSegment`.
- Existing query operators can remain mostly unchanged if they continue to consume `IndexSegment` / `DataSource`
  abstractions instead of Pinot-native tarball assumptions.
- `BaseTableDataManager` now has a dedicated `addNewLakehouseTabletSegment` hook for tablet stubs, but the default
  implementation is intentionally unsupported until the real manifest/parquet loader lands.

### Agent G

- Additive unit/integration coverage should focus on:
  - config/serde compatibility
  - controller refresh and bounded-tablet assignment
  - broker routing by tablets instead of files
  - server correctness over Parquet-backed microsegments
  - mixed-version rejection / guardrails before full upgrade

### Agent H

- Repo-local Phase 1 docs and runbooks should live under `contrib/lakehouse-native`.
- Example configs should live under `pinot-tools/src/main/resources/examples/lakehouse/icebergNative/`.
- Controller API docs should stay code-first through Swagger annotations on dedicated controller resources.
- Example configs should call out the controller adapter resolution rule explicitly and keep sidecars disabled by
  default.

## Selected Next Slice

The next implementation slice after the shared contract baseline is controller metadata plumbing. The selected order is:

1. Add the dedicated tablet ZK metadata path and CRUD support.
2. Add lakehouse config validation into the existing table create/update flow.
3. Add a controller lakehouse service for snapshot resolution, file grouping, manifest publication, and tablet diffing.
4. Add dedicated controller admin endpoints for validate, refresh, inspect-tablets, and inspect-refs.
5. Only then wire broker routing on tablet envelopes and server-side tablet execution.

## Branch Status Update

The branch has already crossed the first controller/broker guardrail milestone:

- controller-side Phase 1 validation is in place for enabled lakehouse configs
- tablet list/get inspect APIs are present in the controller
- refresh and refs controller APIs are present in the controller
- controller now publishes tablet Helix partitions during refresh
- broker-side snapshot selector validation is in place
- broker trace info now includes bounded routed `tabletId` samples in addition to routed/optional/pruned tablet counts
- lakehouse tables are blocked from multi-stage query execution in Phase 1
- controller metrics now track tablet count, stale manifest count, refresh success/failure, and manifest
  publish success/failure
- server-side tablet stubs route through a dedicated load hook, but the default path remains unsupported until the
  real manifest/parquet loader is wired in

Feasible Phase 1 end-to-end proof now exists in tests:

- `LakehousePhase1FeasibleE2ETest` stitches controller refresh, manifest publication, server-side tablet loading via
  `LakehouseOfflineTableDataManager`, query-time microsegment flattening through `SingleTableExecutionInfo`, and
  broker trace visibility for routed tablets.
- This is still placeholder-execution only. It proves the Phase 1 control-plane and query-seam contract without
  claiming a finished Parquet-native execution path.

Still scaffold-only in Phase 1:

- direct Pinot writes into Iceberg
- MSQ support for lakehouse-native tables
- sidecar correctness dependency
- transactional hot-state semantics for `HOT_PLUS_SNAPSHOT`
