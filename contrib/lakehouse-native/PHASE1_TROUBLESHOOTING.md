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

# Phase 1 Troubleshooting

This page captures the failure modes that matter for Phase 1 query-in-place.
It does not cover direct writes, delete-file execution, or sidecar acceleration.

## `lakehouseConfig` Rejected

If a table config is rejected:

- verify `lakehouseConfig.enabled=true`
- verify `lakehouseConfig.mode=ICEBERG_NATIVE`
- verify `lakehouseConfig.catalogConfig.tableIdentifier` is set
- verify the controller adapter wiring is present in either `lakehouseConfig.catalogConfig.pluginName` or
  `lakehouseConfig.catalogConfig.properties.controller.adapter.class`
- verify the table is using a supported Iceberg catalog type
- verify `lakehouseConfig.writeMode=DISABLED`
- verify you are not trying to enable lakehouse-native mode before the cluster is ready for it

## Too Many Tablets

Symptoms:

- tablet count is close to file count
- routing metadata is growing faster than expected
- controller refreshes are slow

What to check:

- `LakehouseTabletConfig.targetFilesPerTablet`
- `LakehouseTabletConfig.targetBytesPerTablet`
- the partition grouping strategy
- whether the dataset has been split too finely upstream

The anti-pattern is one Iceberg file per tablet or one Iceberg file per Helix partition.

## Stale Manifest Pointer

Symptoms:

- queries pin to an old snapshot
- tablets inspect correctly but query results do not move forward
- controller refresh succeeds but servers keep loading old data

What to check:

- `TabletMetadataEnvelope.manifestUri`
- `TabletMetadataEnvelope.manifestVersion`
- controller refresh logs
- whether the manifest store path changed without a corresponding pointer update

## Snapshot Resolution Problems

Symptoms:

- queries fail when `snapshotId`, `branch`, `tag`, or `asOfTimestampMs` is supplied
- the broker cannot pin a single snapshot

What to check:

- only one snapshot selector should be supplied for a query
- the Iceberg catalog config should be complete
- the table should be using `ICEBERG_NATIVE`

If the broker rejects the query up front, that is the expected Phase 1 behavior rather than a routing failure.

If refresh or refs calls are rejected with a bad-request error, the first thing to check is that only one selector
was supplied.

## MSQ Rejection

Phase 1 does not support multi-stage query execution for lakehouse-native tables.
If a query is rejected by the broker or planner for this reason, the response is expected.
Do not treat it as a corruption or routing bug.

If the table is lakehouse-enabled and the query is sent through the multi-stage path, the rejection is also
expected in Phase 1.

## Missing Or Corrupt Sidecars

Sidecars are not required for correctness in Phase 1.
If sidecars are missing or stale, the table should continue to be queryable via the base Parquet path.

## What Not To Do

- Do not store large file metadata blobs in ZooKeeper.
- Do not treat read visibility modes as write semantics.
- Do not assume direct append is live in Phase 1.
- Do not try to debug lakehouse tables using Pinot-native segment assumptions.
- Do not forget that controller refresh writes metrics even when the response is successful; inspect the metrics if
  tablet counts or stale manifests look wrong.
