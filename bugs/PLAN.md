# Remediation Plan — Close all 19 skipped EXPLAIN tests

**Context.** `src/cli/templates/__tests__/template-explain.test.ts` scaffolds
every template pipeline, runs `runSynth`, and submits the generated SQL to
Flink's SQL Gateway for `EXPLAIN` validation. As of today, **19 of 21
template pipelines are in the `SKIP` set**. This plan groups the 19 into
10 PR-sized fixes and sequences them for delivery.

**Exit criterion.** `SKIP` set is empty (or contains only items proven
to be Flink-side limitations with a documented comment), and the CI
`explain` job validates all 21 template pipelines on every PR.

---

## Inventory — 10 distinct fixes unblock 19 pipelines

Root-cause grouping collapses the 19 SKIP entries into 10 PR-sized
increments. Existing bug records are linked; new ones are listed with
their target filename.

| # | Scope | Pipelines unblocked | Bug record | Est. |
|---|---|---|---|---|
| 1 | BUG-015 window column case | 2 — `page-view-analytics`, `grocery-store-rankings` (also unmasks BUG-025 on `ecom-revenue-analytics`) | [`015-window-column-case-mismatch.md`](./015-window-column-case-mismatch.md) [FIXED] | few hours |
| 2 | Iceberg REST catalog wiring | 4 — `cdc-to-lakehouse`, `medallion-{bronze,silver,gold}` (also unmasks BUG-018 on `lakehouse-ingest`) | [`016-iceberg-rest-catalog-wiring.md`](./016-iceberg-rest-catalog-wiring.md) [FIXED] | 0.5–1 day |
| 3 | Temporal-join column ambiguity | 0 directly — codegen fix shipped, both pipelines blocked on deeper template/codegen issues (BUG-027, BUG-028) | [`017-temporal-join-column-ambiguity.md`](./017-temporal-join-column-ambiguity.md) [FIXED] | 1 day |
| 4 | Multi-pair StatementSet type mismatch | 4 — `pump-ecom`, `pump-iot`, `pump-lakehouse`, `lakehouse-ingest` (added via A.2) | [`018-statement-set-type-mismatch.md`](./018-statement-set-type-mismatch.md) [FIXED] | 1–2 days |
| 5 | Interval-join table refs | 0 directly — codegen + template fixes shipped, pipeline blocked on BUG-029 (rowtime attrs in sink) | [`019-interval-join-table-refs.md`](./019-interval-join-table-refs.md) [FIXED] | 1 day |
| 6 | LookupJoin external table | 1 — `ecom-customer-360` | [`020-lookupjoin-external-table.md`](./020-lookupjoin-external-table.md) [FIXED] | 1 day |
| 7 | BroadcastJoin intermediate table | 1 — `rides-surge-pricing` (references `demand`) | [`021-broadcastjoin-intermediate.md`](./021-broadcastjoin-intermediate.md) [FIXED] | 1–2 days |
| 8 | Route-after-window type mismatch | 1 — `bank-compliance-agg` | [`022-route-after-window.md`](./022-route-after-window.md) [FIXED] | 1 day |
| 9 | STDDEV_POP in windowed context | 1 — `iot-predictive-maintenance` | [`023-stddev-pop-windowed.md`](./023-stddev-pop-windowed.md) [FIXED] (uncovered BUG-030, pipeline still skipped) | 0.5–1 day |
| 10 | MATCH_RECOGNIZE EXPLAIN | 1 — `rides-trip-tracking` | create `024-match-recognize-explain.md` after investigation | TBD |
| 11 | Template schema missing `category` field | 1 — `ecom-revenue-analytics` | [`025-ecom-revenue-schema-missing-category.md`](./025-ecom-revenue-schema-missing-category.md) | few hours |
| 12 | MatchRecognize MEASURES type inference | 1 — `bank-fraud-detection` (uncovered by A.3) | [`027-match-recognize-measures-type-inference.md`](./027-match-recognize-measures-type-inference.md) | 0.5–1 day |
| 13 | Temporal join partial PK coverage (template) | 1 — `grocery-order-fulfillment` (uncovered by A.3) | [`028-temporal-join-partial-pk-coverage.md`](./028-temporal-join-partial-pk-coverage.md) | few hours (template) |

Note: BUG-001 through BUG-014 are all marked `[FIXED]`. None of the 19
skipped pipelines correspond to those closed bugs — the remaining issues
are genuinely new regressions or freshly-surfaced bugs from
scaffold-driven testing.

---

## Sprint A — shared-root-cause fixes (high leverage)

Each PR in this sprint unblocks multiple tests. Ship first.

### A.1 — BUG-015 window column case *(unblocks 3)*

**Root cause (hypothesis).** Codegen emits `WINDOW_START` / `WINDOW_END`
uppercase in the SELECT list and possibly `GROUP BY`. Flink's
`TUMBLE` / `HOP` / `CUMULATE` TVFs produce lowercase output columns
(`window_start`, `window_end`, `window_time`), so the references don't
resolve. The `ecom-revenue-analytics` error (`Column 'category' not
found`) additionally suggests the `GROUP BY` clause for windowed
aggregations may be missing the window columns.

**Fix.** Locate the uppercase literals in `src/codegen/sql-generator.ts`
(windowed-aggregation emitter). Change to lowercase. Verify `GROUP BY`
clauses for windowed aggregations include `window_start, window_end`.

**Rebaseline.** `src/__tests__/__snapshots__/benchmark-pipelines.test.ts.snap`
and `src/codegen/__tests__/__snapshots__/*.snap` will change. Eyeball
each diff — snapshots can encode bugs, don't rubber-stamp.

**Verification.**
```bash
flink-reactor cluster up
REQUIRE_SQL_GATEWAY=1 pnpm vitest run src/cli/templates/__tests__/template-explain.test.ts
```
Expect 3 `↓` → `✓`. Remove the three entries from `SKIP` under the
"Window column case mismatch" comment block.

### A.2 — Iceberg REST catalog wiring *(unblocks 5)*

**Root cause (to verify).** Templates that use `IcebergSink` emit
`CREATE CATALOG <name> WITH ('type' = 'iceberg', 'catalog-impl' = ...,
'uri' = '...')`. DDL fails at `executeStatement` time because either
(a) the Iceberg REST catalog JAR isn't on Flink's classpath, or (b)
the emitted URI doesn't match the running endpoint, or (c) S3 endpoint
configuration is missing for the seaweedfs warehouse.

**Investigation checklist.**
1. Read `src/cli/cluster/Dockerfile.flink` — confirm `iceberg-flink-runtime`
   + `iceberg-aws-bundle` are present (lines 16-19). Add the Iceberg
   REST catalog JAR if missing.
2. Scaffold `cdc-lakehouse` locally; inspect the generated
   `CREATE CATALOG` SQL. Confirm URI is `http://iceberg-rest:8181` (for
   cluster-internal DNS) — if it's `localhost:8181`, fix the emitter
   to use the cluster hostname.
3. Confirm the warehouse path is `s3://flink-state/warehouse` and that
   `docker-compose.yml` sets matching `AWS_ACCESS_KEY_ID` / endpoint on
   the Flink services (not just on `iceberg-rest` and `seaweedfs-init`).

**Files likely touched.**
- `src/cli/cluster/Dockerfile.flink` — maybe add a JAR
- `src/codegen/sql-generator.ts` — possibly adjust URI/config emission
- `src/cli/cluster/docker-compose.yml` — maybe propagate S3 creds to
  Flink services

**Risk.** Touches shared cluster infra. Bring the cluster down fully
(`flink-reactor cluster down -v`) and up before testing. If this
change breaks non-Iceberg templates, bisect by scaffolding each in turn.

**Verification.** All 5 `medallion-*` / `lakehouse-*` / `cdc-to-*`
entries move from `↓` to `✓`. Remove from `SKIP`.

### A.3 — Temporal-join column ambiguity *(unblocks 2)*

**Root cause.** `bank-fraud-detection` uses a temporal join on
`accountId`; `grocery-order-fulfillment` uses one on `storeId`. Both
fail with `Column 'X' ambiguous`. The temporal-join emitter selects
the join column without qualifying which side it came from.

**Fix.** In `src/codegen/sql-generator.ts`, find the temporal-join
emitter. Compare against the regular-join emitter (BUG-002 `[FIXED]`
shows the correct pattern). Qualify the `SELECT` list so ambiguous
columns use `left_alias.col` / `right_alias.col`.

**Verification.** Both entries removed from `SKIP`.

### A.4 — Multi-pair StatementSet type mismatch *(unblocks 3)*

**Root cause (to confirm).** All three `pump-*` pipelines wrap multiple
`INSERT INTO` pairs in `EXECUTE STATEMENT SET BEGIN ... END`. The
first pair passes EXPLAIN; later pairs fail column-type validation.
Likely either (a) the DataGen source emits a schema that doesn't
match every downstream sink, or (b) a shared SET statement at the top
(e.g., from a common config) conflicts with a per-pair sink DDL.

**Investigation.** For `pump-ecom`, split the generated
`EXECUTE STATEMENT SET` into individual `INSERT INTO` statements,
EXPLAIN each independently. The one that fails tells you where the
type mismatch is.

**Fix.** Codegen must ensure each `(datagen source, kafka sink)` pair
has matching column types. Likely in the statement-set emitter in
`src/codegen/sql-generator.ts`.

**Verification.** Three `pump-*` entries removed from `SKIP`.

---

## Sprint B — single-pipeline codegen bugs (one PR each)

These touch distinct codegen paths with no shared root cause. Ship
independently; each PR has its own bug file, fix, snapshot rebaseline.

### B.1 — Interval-join table refs (`ecom-order-enrichment`)
Interval-join emits references to tables not registered in the EXPLAIN
session. Similar pattern to BUG-002 (`[FIXED]`); interval-join likely
missed that fix. Check the emitter for where the table alias is
declared vs. referenced.

### B.2 — LookupJoin external table (`ecom-customer-360`)
LookupJoin emits a reference to a table that should be declared via
`CREATE TABLE` but isn't included in the pipeline's DDL output.
Investigate: does the template actually declare the lookup table, or
is the codegen dropping it? Fix is probably in the join-chain emitter.

### B.3 — BroadcastJoin intermediate table (`rides-surge-pricing`)
BroadcastJoin codegen references an intermediate `demand` table that
isn't registered — it's a materialized intermediate node. Fix: emit
the intermediate as a `CREATE VIEW` (or inline it) before the
referencing join. See BUG-009 (`[FIXED]`, `bugs/009-chained-joins-
reference-intermediate-node-id.md`) for the pattern.

### B.4 — Route-after-window type mismatch (`bank-compliance-agg`)
Route branch after a windowed aggregation produces columns whose types
don't match the sink DDL. Likely related to BUG-014 (`[FIXED]`).
Re-investigate: windowed aggs produce `window_start/end TIMESTAMP(3)`
columns; the route branch sinks may not account for them.

### B.5 — STDDEV_POP in windowed context (`iot-predictive-maintenance`)
`STDDEV_POP` inside a sliding-window aggregate fails EXPLAIN. May need
explicit cast or may expose a type-inference gap in the aggregate
emitter. If `STDDEV_POP` itself is unsupported in streaming + windowed
context, document the Flink limitation and consider a rewrite using
`SQRT(AVG(x*x) - AVG(x)*AVG(x))`.

---

## Sprint C — investigate, then decide

### C.1 — MATCH_RECOGNIZE EXPLAIN (`rides-trip-tracking`)

**Investigation first.** Flink's SQL Gateway historically has not
supported `EXPLAIN` on `MATCH_RECOGNIZE` queries. Confirm by:
1. Scaffolding `ride-sharing`, synthing `rides-trip-tracking`.
2. Submitting the DML to `client.executeStatement` (no EXPLAIN) — if
   that succeeds, the SQL is valid and the issue is EXPLAIN-side.
3. Searching Flink's issue tracker for `MATCH_RECOGNIZE EXPLAIN`.

**If Flink limitation.** Leave this pipeline permanently in `SKIP`
with a comment pointing at the upstream issue. Add a separate
`describe` block that validates `MATCH_RECOGNIZE` pipelines via
`executeStatement` (ephemeral session, DROP after) instead of EXPLAIN.

**If codegen bug.** Fix and un-skip.

---

## Uniform per-PR workflow

Every PR in sprints A / B / C follows this shape:

1. **Reproduce locally.**
   ```bash
   flink-reactor cluster up
   REQUIRE_SQL_GATEWAY=1 pnpm vitest run \
     src/cli/templates/__tests__/template-explain.test.ts --reporter=verbose
   ```
   Capture the exact Flink error for the target pipeline(s).

2. **Write / update the bug record** in `bugs/0XX-...md`. Use
   [`014-route-branch-partition-column-type-mismatch.md`](./014-route-branch-partition-column-type-mismatch.md)
   as the template: Affected Examples → Flink Error → Root Cause →
   Where to Fix → Verification.

3. **Fix the codegen** — usually in `src/codegen/sql-generator.ts`
   (or an adjacent emitter).

4. **Rebaseline snapshots.** `pnpm vitest run -u` — review the diff
   carefully. Snapshots can encode bugs, so rebaselining blindly
   re-encodes them.

5. **Remove entries from `SKIP`** in
   `src/cli/templates/__tests__/template-explain.test.ts`. Leave the
   `// BUG-0XX` comment in place if it helps historical navigation,
   but delete the set-entry line itself.

6. **Mark the bug as FIXED** — append `[FIXED]` to the bug file's H1
   title (matches the BUG-001 through BUG-014 convention).

7. **Verify full suite.**
   ```bash
   pnpm typecheck
   pnpm vitest run
   pnpm lint
   ```
   Any drive-by snapshot invalidations should be addressed in the same
   PR, not punted.

8. **PR title / body.**
   - Title: `fix(codegen): <short description> [BUG-0XX]`
   - Body includes before/after counts, e.g.
     *"Unblocks 2 EXPLAIN tests; SKIP set 19 → 17."*

---

## Shipping order & parallelism

**Recommended order.**

1. Sprint A.1 (BUG-015, window case) — ship first. Cheap, and
   derisks later work: fixing Iceberg (A.2) may expose BUG-015 on
   `medallion-gold`'s windowed aggregation too, so closing A.1 first
   prevents re-surfacing.
2. Sprint A.2 (Iceberg wiring) — medium risk, high leverage (5 tests).
3. Sprint A.3 (temporal joins) — 2 tests, small surface.
4. Sprint A.4 (StatementSet) — 3 tests, shared root.
5. Sprint B.1 → B.5 — any order, one PR each.
6. Sprint C.1 — investigate last; may not produce a fix.

**Parallelism.** Sprints A and B can run across developers without
cross-dependencies. Within Sprint A, A.1 should land first; A.2–A.4
can go in parallel. Within Sprint B, all five are independent.

**Estimated wall time.** One engineer: ~2-3 weeks. Two in parallel:
~1-2 weeks.

---

## Open risks

- **Snapshot sprawl.** Each codegen fix rebaselines snapshots in
  `src/__tests__/__snapshots__/benchmark-pipelines.test.ts.snap` and
  `src/codegen/__tests__/__snapshots__/*.snap`. If diffs get noisy,
  consider splitting `.snap` files per operator so each fix produces
  a contained diff.
- **Cluster flakiness in CI.** Phase 3's `explain` job is the only
  automated signal for these fixes. Docker build cache misses or slow
  Kafka starts could flake it. Keep `explain` as a required status
  check; if flake rate climbs above ~2%, tighten healthchecks or pin
  image versions.
- **Scope creep per PR.** Fixing one codegen bug routinely surfaces
  adjacent issues (e.g., fixing temporal-join aliasing may reveal a
  missing schema-propagation path). **Discipline:** don't grow the
  PR — file a new bug, add its pipeline(s) to `SKIP` if they regress,
  ship the original fix, handle the adjacent issue next.
- **MATCH_RECOGNIZE may stay permanently skipped.** If Sprint C.1
  confirms a Flink limitation, `rides-trip-tracking` lives in `SKIP`
  forever. That's acceptable: its *synth* correctness is still covered
  by Phase 1's `template-scaffold-synth.test.ts`, which is the more
  important signal.

---

## Tracking

As each PR lands, update this document:

- [x] A.1 — BUG-015 window column case (2 tests; uncovered BUG-025 on `ecom-revenue-analytics`)
- [x] A.2 — Iceberg REST catalog wiring (4 tests; uncovered BUG-018 affects `lakehouse-ingest`)
- [x] A.3 — Temporal-join column ambiguity (codegen shipped; uncovered BUG-027 + BUG-028, neither pipeline unblocked yet)
- [x] A.4 — Multi-pair StatementSet (4 tests: `pump-ecom`, `pump-iot`, `pump-lakehouse`, `lakehouse-ingest`)
- [x] B.1 — Interval-join table refs (codegen + template shipped; uncovered BUG-029, pipeline still skipped)
- [x] B.2 — LookupJoin external table (1 test: `ecom-customer-360`)
- [x] B.3 — BroadcastJoin intermediate (1 test: `rides-surge-pricing`)
- [x] B.4 — Route-after-window (1 test: `bank-compliance-agg`)
- [x] B.5 — STDDEV_POP windowed (codegen + template shipped; uncovered BUG-030, pipeline still skipped)
- [ ] C.1 — MATCH_RECOGNIZE EXPLAIN (1 test, may remain skipped)
- [ ] BUG-025 — `ecom-revenue-analytics` schema missing `category` (1 test; surfaced by A.1)
- [ ] BUG-027 — MatchRecognize MEASURES type inference (1 test; surfaced by A.3)
- [ ] BUG-028 — Temporal join partial PK coverage in `grocery-order-fulfillment` template (1 test; surfaced by A.3)
- [ ] BUG-029 — Multiple rowtime cols in Kafka sink for `ecom-order-enrichment` (1 test; surfaced by B.1)
- [ ] BUG-030 — TemporalJoin on windowed-agg output loses time-attribute through CTE alias (1 test; surfaced by B.5)

**Done when:** the checkbox list above is complete *and* the `SKIP`
set in `template-explain.test.ts` contains only items documented as
Flink-side limitations.
