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

# INSERT INTO Feature - Test Report

## Test Coverage Map

This document tracks the test classes that ship with the push-based INSERT INTO feature. The list
below is the source of truth for v1; CI is the authoritative pass/fail signal — run
`./mvnw -pl pinot-controller -am test` for the controller subsystem, plus the integration test.

### pinot-common (SQL Parsing)

| Test Class | Coverage |
|-----------|----------|
| `InsertIntoValuesTest` | Parser DML round-trip, literal extraction, table-type resolution, hybrid validation rejection |

### pinot-spi (SPI types)

The seven SPI types in `pinot-spi/src/main/java/org/apache/pinot/spi/ingest/` are covered indirectly
by `InsertStatementManifestTest` (JSON round-trip + schema-version forward-compat) and through
the controller-side coordinator and store tests.

### pinot-controller (Coordinator, Store, Executors, REST)

| Test Class | Coverage |
|-----------|----------|
| `InsertStatementCoordinatorTest` | State-machine transitions (ACCEPTED→VISIBLE/ABORTED/REJECTED), idempotency precondition, hybrid table validation, executor lookup fail-fast, stuck-statement timeout abort, ROW vs FILE differentiation in the cleanup sweep |
| `InsertStatementCoordinatorZkIntegrationTest` | Real (in-memory) ZK property-store integration — concurrent submission with stale reservation, schema-version envelope check, rebind race exact-once-winner |
| `InsertStatementStoreTest` | ZK persistence: createStatement / getStatement / updateStatement (CAS), reservation reserve/rebind/release/tombstone-prune protocol, manifest envelope schema-version rejection |
| `InsertStatementManifestTest` | JSON round-trip, missing-field defaults, schemaVersion forward-incompat rejection, unknown-property tolerance via `@JsonAnyGetter`/`@JsonAnySetter` |
| `ControllerRowInsertExecutorTest` | Per-partition segment build + upload happy path, multi-segment failure rollback, primary-key validation, table-mode safety (full/partial upsert + dedup), partition-value coercion |
| `FileInsertExecutorTest` | Minion task scheduling, completion validation, table-mode safety, abort hook idempotency, schema-driven URI defaulting |

### pinot-integration-tests (End-to-End)

| Test Class | Coverage |
|-----------|----------|
| `InsertIntoValuesClusterIntegrationTest` | Full SQL → broker → controller → executor pipeline with a real cluster: submit/status/abort/list lifecycle, idempotency, hybrid-table rejection, parser-end-to-end query verifying rows are queryable after VISIBLE |

## Pre-commit Check Results

The following checks must pass before pushing on any module touched by this PR. Run them per
module via `./mvnw <goal> -pl <module>`:

| Check | Required |
|-------|----------|
| `spotless:apply` | yes |
| `checkstyle:check` | yes |
| `license:format` | yes |
| `license:check` | yes |
| `test` | yes (unit + integration on touched modules) |

## Known Gaps (deferred to v2 follow-ups)

1. **Mixed-version controller cluster.** No integration test exercises a real rolling upgrade
   where one controller is on a newer manifest schemaVersion than another. The
   `InsertStatementManifestTest.testDeserializeRejectsForwardIncompatibleSchemaVersion` covers the
   refuse-forward-version path at unit level only.
2. **Auth-forwarding HTTP path.** `SqlQueryExecutor.executePushInsert` falls back to broker→controller
   HTTP routing when no local executor is available. The integration test runs broker and controller
   in the same JVM with a local executor, so the HTTP-fallback header-forwarding path is unit-tested
   only via mocks (TODO).
3. **Concurrent INSERT stress test.** No test fires N concurrent submits with the same `requestId`
   to validate the rebind protocol under contention. The
   `testConcurrentSubmissionsWithStaleReservationExactlyOneWins` ZK integration test covers the
   2-thread case.
4. **FILE-insert end-to-end.** No integration test schedules an actual Minion task and verifies
   `/insert/complete` callback flow against a running Minion. v1 ROW path is fully exercised; FILE
   path is unit-tested only.
5. **Long-retention GC for stuck-ACCEPTED ROW manifests.** The coordinator's 7-day retention path
   is unit-tested via clock injection but not under a production-scale long-running cluster.

## Recommended Pre-Merge Verification

Before flipping `controller.insert.enabled=true` in production:

1. Run the full `pinot-controller` test suite (`./mvnw -pl pinot-controller test`).
2. Run the integration test (`./mvnw -pl pinot-integration-tests -Dtest=InsertIntoValuesClusterIntegrationTest test`).
3. Verify the feature flag is OFF by default in the deployed `controller.conf`.
4. Document the order of operations in the rolling-upgrade section of operator-guide.md (controllers
   first, then brokers — brokers must not enable INSERT parsing while any controller still rejects
   the new ZK manifest schemaVersion).
