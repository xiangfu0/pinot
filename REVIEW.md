# Code Review Guide — Apache Pinot

This is a short digest. The authoritative source is [`kb/code-review-principles.md`](kb/code-review-principles.md) (8 domains, ~100 principles, with BAD/GOOD examples).
Also read: [`CLAUDE.md`](CLAUDE.md), [`CONTRIBUTING.md`](CONTRIBUTING.md), [`.github/copilot-instructions.md`](.github/copilot-instructions.md).

## Severity tiers

- **CRITICAL** — must fix before merge. Data loss, data corruption, silent wrong results, backward incompatibility, security, race conditions.
- **MAJOR** — should fix; strong justification required to skip. Performance regressions, design violations, missing tests, wrong abstractions.
- **MINOR** — improves quality; acceptable to defer. Naming, style, idioms, process suggestions.

Priority order when principles collide: Production Safety > Backward Compatibility > Correctness > State Management > Performance > Architecture > Testing > Naming > Process.

## Top critical rules (cite principle ID in review comments)

| ID       | Rule                                                                                                                |
| -------- | ------------------------------------------------------------------------------------------------------------------- |
| C1.1     | Config key renames must keep the old key working (deprecate, don't remove).                                          |
| C1.2     | Schema type names and public enum values are permanent.                                                              |
| C1.3     | Don't widen SPI method signatures (`List` → `Collection` breaks plugins).                                            |
| C1.5     | Validate *effective* config after override chain, not raw input.                                                     |
| C2.1     | State transitions must be atomic — no eager wipes before the new state is durable.                                   |
| C2.2–2.6 | Default to explicit synchronization; prove visibility on shared observers; version-checked writes for IdealState.    |
| C4.x     | No allocations / autoboxing in per-row query operators and segment scans.                                            |
| C5.x     | Null handling: dispatch on `getStoredType()`, preserve precision for INT/LONG/BIG_DECIMAL, never silently coerce to double. |
| C6.x     | Tests cover positive + negative cases, use real dictionaries/segments (not mocks where semantics matter), include the mixed-version / rolling-upgrade path for wire changes. |
| C7.x     | Public SPI / REST JSON field names / Protobuf field numbers are frozen.                                              |

## What blocks merge (critical) vs. what's a nit

**Blocks merge** — evidence from recent PR history:
- Null dereference without a null-safety story (crashes visible from empty-segment path).
- Missing case in a type/index-type `switch` (silent broken segments).
- Memory leak on segment destroy / realtime persist.
- Backward-incompat config change without a rolling-upgrade note.
- New REST / Thrift / Protobuf field added in a way that breaks mixed-version clusters.
- Lock/volatile change without a race-condition analysis walkthrough.

**Nit / defer** — routinely approved with suggestions:
- Comment typos, stale TODO cleanup, log-level tuning.
- Package / class renames that don't cross the SPI boundary.
- Additional test cases beyond the required positive/negative pair.
- Javadoc polish.

## Project-specific conventions the reviewer enforces

- **Imports**: never use fully qualified class names inline; always `import` first.
- **Logging**: SLF4J via `private static final Logger LOGGER = LoggerFactory.getLogger(ClassName.class);`. No `printStackTrace`, no `System.out`. Do not log user query text (PII).
- **Test framework**: TestNG by default; `@Test` refers to `org.testng.annotations.Test` unless the file already imports JUnit. Prefer real dictionary / real segment over mocks when the semantics under test are encoding-sensitive.
- **License headers**: all new source files must carry the ASF 2.0 header. Run `./mvnw license:format` before commit.
- **Formatting**: always `./mvnw spotless:apply` + `./mvnw checkstyle:check` on affected modules before push.
- **Java version**: target Java 11. No Java 17+ syntax (`record`, pattern-matching `switch`).
- **Feature rollout**: new behavior ships OFF by default. Name flags as `enableXxx` (default `false`) or `disableXxx` (default `false` = enabled), never ambiguous.
- **Wire formats**: DataTable version, segment format version, Protobuf field numbers, REST JSON field names are all external contracts — changes require the backward-compat checklist in domain 1.

## Testing core functionality

Every non-trivial change must exercise its core functionality end-to-end, not just at unit-test level. Unit tests prove a class behaves correctly in isolation; they do not prove the feature works through planner → broker → server → segment.

**Integration-test base-class rule:** when the change can be validated with ordinary table data and the default cluster topology, the integration test MUST extend [`CustomDataQueryClusterIntegrationTest`](pinot-integration-tests/src/test/java/org/apache/pinot/integration/tests/custom/CustomDataQueryClusterIntegrationTest.java) and live under `pinot-integration-tests/src/test/java/org/apache/pinot/integration/tests/custom/`. That base shares one controller / broker / server / ZK across the whole suite (`@BeforeSuite`), so each test only pays for its own schema + data + SQL, not another ~30–60 s of cluster bring-up.

Spinning up a fresh cluster (extending `BaseClusterIntegrationTest` directly, or a specialized base) is justified **only** when the change requires:

- a non-default cluster topology (multi-tenant, multi-broker, multi-server, dedicated minion), or
- non-default component configuration (custom broker/server/controller properties, auth, TLS, access control), or
- a different Helix / ZK layout or tenant isolation that table-level config can't simulate, or
- realtime/streaming wiring not provided by the custom base, or lifecycle transitions that require cluster restart.

If none of those apply and the PR adds a standalone `*IntegrationTest` with its own cluster, the reviewer will ask to re-parent it to `CustomDataQueryClusterIntegrationTest`. Look at neighbors in the `custom/` package (window functions, sketches, vector indexes, geo, JSON, timestamp, distinct, star-tree, unnest, SSB) for the shape a new test should follow.

## When to defer to the developer

Skills flag items as "defer" (not a blocker) when:
- The concern is stylistic and the existing file uses a different convention consistently.
- The reviewer can see an explicit design rationale in the PR description that addresses the concern.
- The finding is MINOR and the change is urgent (hotfix, revert).

Explicit deferrals should be surfaced in the review so the author can accept or push back.

## How reviews are run

Invoke the `code-reviewer` agent with scope + one-line change description. The agent fans out 8 parallel domain-scoped sub-reviewers (config-backcompat, concurrency-state, architecture, performance, correctness-nulls, testing, naming-api, process-scope), each backed by a `SKILL.md` under `.claude/skills/review-*/`, then aggregates and de-duplicates findings into one consolidated severity-sorted report. Each sub-reviewer reads `kb/code-review-principles.md` itself — never pass your own analysis or opinions to the reviewer.
