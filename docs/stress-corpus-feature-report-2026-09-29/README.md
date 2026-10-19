# Stress corpus: what it tests, what it doesn't, and how complex it is

The stress corpus lives in `core/src/test/resources/stress/` plus the 11 linked projects under `projects/`. This report
measures it against the feature inventories in legend-engine and legend-pure: the EMIT taxonomy and EMIT models, the
PCT function inventory, and legend-pure's primitive types. It covers queries, constraints, tests, mappings and stores,
in two kinds of complexity: size and combination.

First written 2026-09-29. Updated 2026-10-19 against main at `167018c4f`: the stress suites were re-run, every number
was re-derived with [`tools/run.sh`](tools/run.sh), and what moved is listed in [Since 2026-09-29](#since-2026-09-29).

## Summary

1. **The corpus is wide in relational mapping and store features, and narrow nearly everywhere else.** It tests 43 of
   the 117 non-scaffolding EMIT features with a passing, populated test in lite. Another 12 are reached only by
   skipped, failing or untested services, 9 exist in the model with no service reaching them, and 53 are absent. Every
   feature it tests is also in EMIT. What the corpus adds is scale on those features: single-table inheritance is
   exercised by 1,310 passing services, against one EMIT model.
2. **Service parameters: there is one parameter in the whole corpus.** `surface::ParameterisedService` takes one
   `String[1]`, and lite never runs it because the runner only picks up `stress::` services. No service takes an
   Integer, Float, Decimal, Boolean, Number, Date, StrictDate, DateTime or StrictTime parameter, and none takes an
   enumeration.
3. **Query constants are mostly placeholders.** 2,166 of the 2,188 String literals are the `' '` in `$x.a > ' '`. All
   1,600 Integer literals are `limit(n)` arguments, 1,597 of them `25`. Only 2 Float literals are comparison values;
   the rest are arguments to qualified properties. There are no Decimal, DateTime, StrictTime, Date or Number literals
   anywhere in a query, and **no enumeration literal in any query**. Enum literals appear only inside two function
   bodies. With no DateTime anywhere, milestoning is only ever tested at day granularity: the 18 dated `all()` queries
   pass StrictDate literals or `%latest`.
4. **M2M is effectively untested.** There are 5 model-to-model class mappings with 17 property mappings, against 2,358
   relational class mappings. Only 4 services reach M2M, and **all 4 are skipped in lite**. EMIT has 23 M2M class
   mappings in 16 models. Relation (`~func`) mappings are thin too: 2 in the corpus against 56 in 26 EMIT models.
5. **Only service tests exist.** There are 4,739 service tests, each with one `EqualToJson` assertion. There are no
   mapping test suites, against 161 in EMIT, and the one function test suite (4 tests) is never run by lite.
6. **Constraints are nearly absent: 8 in total, on 3 of 2,330 classes.** Each is a single comparison against one
   property. The richest combination is two Float properties compared to each other.
7. **The query vocabulary is small.** Service queries use 25 platform functions, and the whole corpus touches 63 of
   the 331 PCT function names. No query uses `let`, `or`, `in`, `if`, `exists`, `map`, string, date or math functions,
   `olapGroupBy`, TDS joins, window functions or the relation API.
8. **Size complexity tops out modestly.** The widest projection is 68 columns (median 6). The biggest graph fetch has
   19 leaves, and the deepest nests 4 levels below the root. No query comes near "hundreds of columns".
9. **Combination complexity is highest in the mappings, not the queries.** Ten class mappings in the combination
   matrix each combine 7 mapping features. The richest service reaches 12 EMIT features. Under EMIT's own
   domain-count scoring, one service in 4,742 rates "advanced".
10. **Much of the deep material doesn't reach the database with data.** 2,200 of 4,736 expected results are empty and
    280 more are null at their deepest level. Of 1,554 four-join projections, 10 have a value at the bottom; of 397
    four-level trees, 16 do. `DenseMapping` (711 property mappings), the surface mappings, 32 of the 390 joins and the
    union, merge and inheritance operations are never reached by any service.

## Since 2026-09-29

About 1,800 commits reached main between the first edition and this update. Almost none of them touched the corpus,
so every gap below is still open.

- **The corpus barely changed.** Two generated files lost stale header comments (`60-dense-store.pure`,
  `93-testdata.pure`), and five expected results in `94-fanout-services.pure` changed. legend-engine still builds the
  same 7,643-element protocol document from it, and every count of the corpus's own content in §2 and §4–§9 is
  unchanged (apart from the corrections below).
- **Those five expectations were wrong; lite was right.** The services use `dateDiff` in hours, and the corpus's
  Python oracle counted hour boundaries, as legend-engine's SQL does. legend-pure defines the unit as elapsed time
  with the remainder dropped, which is what lite computes (finding F58 in `docs/UPSTREAM_FINDINGS.md`, 2026-10-05). The
  expectations now follow Pure, so lite passes the five. The oracle lists them in its `ENGINE_QUARANTINE`, because
  legend-engine should now fail them; no engine run has confirmed that yet.
- **Lite's results.** On DuckDB, 4,705 tests pass, 15 fail and 16 are skipped, against 4,700, 20 and 16 before. The
  five `dateDiff` services are the whole difference. Since 2026-10-05 gate 10 also enforces its H2 lane, which opens
  a fresh session per test as legend-engine does: 4,676 pass, 44 fail and 16 are skipped (§6).
- **Tested counts.** The five services reach several features, so some ✅ counts in §3 rose by 1 to 5 (derived
  properties 1,516 → 1,521, primary keys 581 → 585). No feature changed status.
- **EMIT moved.** This update reads a legend-engine checkout 53 commits newer (`35930373b41`, 2026-10-16). `emit.md`
  dropped `execution:connection-time-zone` and added `execution:graph-fetch` and `store:relational-zoned-timestamp`,
  so the taxonomy has 117 non-scaffolding features instead of 116. The corpus tests graph fetch (57 passing services
  with rows) and has no zoned timestamp column, which makes the tally 43 tested and 53 absent. There are 134 models
  instead of 130: seven were added (five relation-mapping graph fetch models, one reading a zoned timestamp column,
  one M2M model) and three were removed when legend-engine reverted its time-zone fixes (legend-engine #5341).
  Between them, those three carried service parameters, service tests, milestoning and connection time zones, which
  is why EMIT's counts of all four fell (§3, §4.3, §6).
- **PCT has 331 function names instead of 324.** The 7 new ones are relation set operations (`union`, `unionAll`,
  `intersect`, `intersectAll`, `except`, `exceptAll`) and `generateSeries`. The corpus uses none of them.
- **Corrections.** None of these came from the scripts.
  - The first edition gave 3 tested services for self-joins (§3, "Beyond EMIT's taxonomy"). The script counts 9, with
    10 services reaching a self-join. The 3 matches the `Pillar_LongerPillar` figure in §8, which is probably where it
    came from.
  - It gave the seed as 5,159 rows across 436 tables (§2, §6). Those figures are the generated header of
    `93-testdata.pure`, which counts the generator's own table set. The protocol document has 4,942 rows in 393 tables
    for `stress::TestData`, and 5,014 rows in 410 tables across all 11 relational data elements.
  - It named two bitemporal tables (§3, Milestoning). There are three, as its own "3 tables, all bitemporal" says:
    the third, `SURFACE_BITEMP`, is in a surface store no service reaches. Its one snapshot table was described as two.
- **The scripts.** `tools/run.sh` failed under Git Bash on Windows: the engine runner read `/d/...` paths as
  `D:\d\...`, and a second run couldn't overwrite the read-only pass list Bazel writes. It also kept its own copy of
  the linked-project list. It now spells paths for the host, replaces the pass list, and reads the list from
  `core/stress.bzl`. `profile.py` tags the new `execution:graph-fetch`. Re-run on the first edition's inputs
  (legend-engine `1d40dd7251d`, legend-pure `5fed02095`, and today's pass list less the five `dateDiff` services), the
  scripts reproduce every number in the first edition that they produce.

Two things built since use the corpus or extend it, and neither changes the measures here:

- **The wrong-rows tool** (`tools/wrongrows/`, plan decision D23) runs every suite through both engines, on the seeds
  and on a generated damaged data set (a duplicate row, an all-NULL row, an orphan per join and a row of type extremes
  per table), and compares their rows. Its first run found legend-engine counting 1 over an empty association
  navigation (register S22) and a lite loader defect, since fixed. The corpus's own expectations still describe only
  the seeds, so §4.6 stands.
- **A separate `#SQL{...}#` stress corpus** (`core/src/test/resources/sql-stress/`, 83 generated queries with joins of
  up to 50 tables, nesting to 50 levels and 2,000-deep expressions) stresses SQL islands, not Pure queries over a
  mapping. §4.4's sizes are about the latter.

## 1. Method

**Corpus model.** All 233 files (202 stress files and 31 files from the 11 linked projects) were parsed and compiled by
legend-engine 4.145.0 (`bazel run //tools/engine-runner:testable -- <files> --pmcd-json=…`). Parsing took 16.2 s and
compiling 7.0 s. Every count in this report comes from walking that protocol JSON (59 MB, 7,643 elements), not from
regex over source text. Legend-engine loads the four files legend-lite excludes (`29-money`, `55-canonical-store`,
`71-mapping-surface2`, `75-surface-gaps`), so their constructs are counted.

**Reaching a feature.** Each service query's property paths, and each graph fetch tree, was resolved through the service
mapping's include closure to the set implementation, property mapping, association mapping, join and table it
actually uses. A feature counts as *reached* only if a query gets to it.

**Tested.** A feature is *tested* when at least one service that reaches it passes in lite with a **populated**
expected result. The pass list is gate 10's DuckDB lane (`//core:stress_suites`). Services whose expected result is
empty or hollow (null at the deepest level) don't count, because they can only detect extra rows.

**Denominators.**
- The EMIT taxonomy in `legend-engine/docs/emit/emit.md` §6.2: 117 non-scaffolding `domain:capability` features
  (including 3 that EMIT models use but the §6.2 table doesn't list) and 10 scaffolding features.
- The 134 EMIT models (`*.emit.yaml`). Their `.pure` files, 312 of them once the persistence models and the
  deliberate compile-failure fixture are left out, were parsed in a single legend-engine run to measure them the
  same way.
- The PCT function inventory: 331 function names tagged `<<PCT.function>>` across legend-pure and legend-engine.
- legend-pure's primitive types in `m3.pure`: String, Boolean, Byte, Number, Integer, StrictTime, Date, StrictDate,
  DateTime, LatestDate, Float and Decimal.
- The protocol roster (`docs/protocol-roster.tsv`, 1,047 tags).

The EMIT models come from a legend-engine checkout newer than the pinned release. No EMIT feature used here depends
on that difference.

## 2. The corpus at a glance

| Element | Count | Notes |
|---|---|---|
| Services | 4,742 | 4,735 in `stress::`, 7 in `surface::`. 5 have no tests; 2 `surface::` services have tests lite never runs |
| Classes | 2,330 | 1,849 subclasses (1,837 one level deep, 12 two levels) |
| Associations | 250 | 1 with a qualified property |
| Enumerations | 21 | largest `core_types::CtCountry` (26 values) |
| Measures / profiles | 3 / 2 | |
| Functions | 21 | 1 with a test suite |
| Mappings | 179 | 2,370 class mappings, 5,894 property mappings |
| Databases | 30 | 437 tables, 4,843 columns, 390 joins, 1,858 filters, 7 views |
| Connections / runtimes | 20 / 26 | |
| Data elements | 14 | 11 relational (5,014 rows in 410 tables), the largest `stress::TestData` (4,942 rows in 393 tables) |
| Service tests | 4,739 in 4,737 suites | all `EqualToJson`, one assertion each |

## 3. Coverage against the EMIT taxonomy

Key: ✅ tested (the number is passing services with populated results) · ◐ reached only by skipped, failing or
untested services · ○ in the model but no service reaches it · ✗ absent. "EMIT" is the number of EMIT models tagged
with the feature.

| Domain | ✅ | ◐ | ○ | ✗ | Total |
|---|---|---|---|---|---|
| grammar | 8 | 1 | 1 | 0 | 10 |
| mapping (relational, M2M, generic) | 13 | 3 | 7 | 7 | 30 |
| mapping (relation `~func`) | 1 | 0 | 0 | 12 | 13 |
| store | 9 | 1 | 1 | 12 | 23 |
| milestoning | 5 | 1 | 0 | 1 | 7 |
| execution | 7 | 6 | 0 | 9 | 22 |
| persistence | 0 | 0 | 0 | 12 | 12 |
| **all** | **43** | **12** | **9** | **53** | **117** |

EMIT's own models cover 99 of these 117. They don't cover the XStore cross-store mapping, M2M local properties, merge
operations, literal lists, relation binding transformers, full and right outer joins, service stores, flat-data stores,
all-versions-in-range, BigQuery, hosted services, Snowflake apps, schema sets, post-validations, bindings, or
external-format specifications. Every feature the corpus tests is also in EMIT.

### Grammar

| Feature | Corpus | Services reaching → tested | EMIT |
|---|---|---|---|
| association | 250 | 2,284 → ✅ 338 | 14 |
| class-inheritance | 1,849 subclasses | 3,571 → ✅ 1,310 | 7 |
| constraint | 8, on 3 classes | 23 (via graph fetch) → ✅ 2 | 2 |
| derived-property | 206 | 2,093 → ✅ 1,521 | 9 |
| enumeration | 21 | 59 → ✅ 55 | 5 |
| function | 21 | 6 → ✅ 5 | 25 |
| measure | 3 | 1 (`MU0_MonetaryTrade`, skipped) → ◐ | 1 |
| nested-association | yes | 2,065 → ✅ 154 | 4 |
| profile | 2 | metadata only → ○ | 1 |
| qualified-property | 89 | 1,387 → ✅ 995 | 2 |

### Mapping: relational, M2M and generic

| Feature | Corpus | Services reaching → tested | EMIT |
|---|---|---|---|
| relational-single-table-inheritance | 1,848 | 3,571 → ✅ 1,310 | 1 |
| relational-primary-key | 276 | 1,741 → ✅ 585 | 1 |
| relational-association-implementation | 238 | 2,274 → ✅ 332 | 1 |
| mapping-include | 158 includes | almost every service (via `AllMapping`) → ✅ | 3 |
| enumeration-mapping | 13 | 56 → ✅ 52 | 6 |
| relational-embedded | 30 | 26 → ✅ 24 | 1 |
| relational-literal | 138 | 18 → ✅ 18 | 1 |
| store-union | 1 | 4 → ✅ 4 (`U1_TradePartitioned`, `G1_UnionTree`, …) | 2 |
| relational-distinct | 3 | 4 → ✅ 3 | 1 |
| aggregation-aware-mapping | 1 | 2 → ✅ 2 (`A0_FactDetail`, `A1_FactRollup`) | 1 |
| relational-inline-embedded | 1 | 2 → ✅ 2 | 1 |
| relational-otherwise-embedded | 4 | 4 → ✅ 2 (`O1_CounterpartyOtherwise` fails) | 1 |
| mapping (generic) | 179 | ✅ | 13 |
| cross-store (XStore) | 1 | 2 (`X0`, `X1`, skipped: two stores) → ◐ | 0 |
| m2m-transform | 17 property mappings | 4, all skipped → ◐ | 4 |
| m2m-chained-relational | 2 mappings via ModelChainConnection | 3, all skipped → ◐ | 2 |
| m2m-local-property | 1 (`75-surface-gaps`) | none → ○ | 0 |
| operation-mapping (inheritance) | 1 (`71-mapping-surface2.pure:65`) | none → ○ | 4 |
| router-union | 1 (`71-mapping-surface2`) | none → ○ | 1 |
| operation-mapping-merge, -merge-validation | 1 each (`74-surface-last`) | none → ○ | 0 |
| relational-literal-list | 2 | none → ○ | 0 |
| relational-main-table-alias (`scope` blocks) | `DenseMapping` (120 blocks) | none; no service uses `DenseMapping` → ○ | 1 |
| relational-group-by (class-mapping `~groupBy`) | — | ✗ (views have `~groupBy`, class mappings don't) | 2 |
| relational-joined-table-inheritance | — | ✗ | 1 |
| relational-polymorphic-query (`subType()`) | — | ✗ | 1 |
| relational-table-alias-column | — | ✗ | 1 |
| m2m-target-instantiation (`^Target(...)`) | — | ✗ | 2 |
| m2m-derived-source-property | — | ✗ | 1 |
| m2m-complex-property-passthrough | — | ✗ | 2 |

### Mapping: relation (`~func`)

Only one of the 13 features is covered: **relation-model-join**, by `J0_TradeBookModelJoin` and its sibling SP service,
both passing (`58-modeljoin-store.pure:29`). The mapping uses two `~func` class mappings with plain column property
mappings and one `ModelJoin`. None of these are covered:

- binding transformer
- embedded, inline embedded
- expression right-hand side
- filter, group-by
- local property, primary key
- `~src`
- union
- window function
- XStore association

EMIT has 56 relation class mappings across 26 models.

### Store

| Feature | Corpus | Services reaching → tested | EMIT |
|---|---|---|---|
| relational-filter | 1,856, plus 2 multigrain | 3,578 → ✅ 1,316 | 5 |
| relational-inner-join (a Join traversed) | 390 joins | 2,316 → ✅ 369 | 13 |
| relational-multi-table | 437 tables | 2,315 → ✅ 368 | 8 |
| relational-left-outer-join | implicit: to-one navigation over orphan and NULL keys | ✅ 338 (by the association navigations) | 1 |
| relational-dyna-function | 17 joins, views, 440 property mappings | 62 → ✅ 59 | 1 |
| relational-inline-view | 7 views | 1,086 → ✅ 27 | 4 |
| relational-nested-join | 314 join chains | 18 → ✅ 17 | 1 |
| relational-cross-table-filter (`~filter` via a join) | 6 | 6 → ✅ 5 | 1 |
| relational-cross-schema | 5 named schemas | 1 → ✅ 1 | 3 |
| relational-semistructured-binding | 1 binding transformer (`62-mapping-features.pure:126`) | 2 (hier services, skipped or failing) → ◐ | 5 |
| relational-semistructured | 2 columns (`SemiStructured`, `Json`) | none → ○ | 13 |
| semistructured navigation, flatten, explode, array functions, array index, wildcard path, array to-many | — | ✗ | 1–5 each |
| relational-zoned-timestamp (a `TIMESTAMP` column the database holds with a zone) | — | ✗ | 1 |
| outer-join (full), right-outer-join | — | ✗ (one `(OUTER)` hop in a surface mapping, no service) | 0 |
| service-store, flat-data-store | — | ✗ | 0 |

### Milestoning

| Feature | Corpus | Services → tested | EMIT |
|---|---|---|---|
| milestoning (the generic marker) | 6 milestoned tables (3 of them bitemporal) and 1 snapshot table | 20 → ✅ 16 | 8 |
| business-temporal | 6 tables, 3 classes | 20 → ✅ 16 | 7 |
| processing-temporal | 3 tables, all bitemporal (no processing-only table or class) | 6 → ✅ 5 | 6 |
| bi-temporal | 2 classes | 6 → ✅ 5 | 3 |
| point-in-time-query | `getAll(%date)` and dated milestoned properties | 19 → ✅ 16 | 7 |
| all-versions-query | 1 (`surface::AllVersionsService`, no tests) | ◐ | 1 |
| all-versions-in-range-query | — | ✗ | 0 |

Snapshot milestoning is present: one table, `SURFACE_SNAPSHOT`, carries both a business and a processing snapshot.
EMIT has no tag for it.

**Dated queries.** 18 services start from a dated `all()`, all in `94-fanout-services.pure` and all passing in lite.
Fifteen expect rows. Three (`PL8`, `SC2`, `MD6`) ask for a date before the first version and expect none, which checks
that nothing leaks through.

| Class | Milestoning | Services | Dates passed |
|---|---|---|---|
| `counterparty::RatingVersion` | business | `F10`–`F13_Ratings…` (line 6620 on), `F14_RatingsLatest`, `G2_MilestonedTree` (a graph fetch) | `%2019-01-01`, `%2021-01-01`, `%2024-06-06` and `%2024-06-07` (the day before and the day of a change), `%latest` ×2 |
| `core_ratings::RatingVersion` | business | `PL6`–`PL8_RatingsAsOf` (line 5503 on) | `%2024-02-01`, `%2024-08-01`, `%2023-06-01` |
| `schedule::TierVersion` | business | `SC0`–`SC2_ScheduleAsOf` (line 4597 on) | `%2024-06-01`, `%2024-02-01`, `%2023-06-01` |
| `products::InstrumentRating` | bitemporal | `B0_RatingAsBelievedThen` (line 6905), `B1`, `B2` | two dates each, e.g. `.all(%2024-02-01, %2024-02-01)` |
| `timeseries::ObservationRevision` | bitemporal | `MD4`–`MD6_RevisionAsOf` (line 3527 on) | two dates each, e.g. `.all(%2024-05-20, %2024-04-30)` |

One more service navigates a milestoned property with a date: `$x.ratingVersions(%2024-08-01)->count()` in
`PL9_CounterpartyRatingCount` (line 5682).

**Milestoning is only ever day-grained.** Every date above is a StrictDate literal or `%latest`, and there is no
DateTime literal anywhere in the corpus. The processing dimension is day-grained in the store as well: all three
bitemporal tables declare their processing columns as `DATE`, not `TIMESTAMP`. The two that services reach,
`INSTR_RATING_BI` (`30-store.pure:267`) and `TS_REVISION_BI` (`30-store.pure:446`), have a StrictDate infinity date;
the third, `SURFACE_BITEMP` (`66-store-surface.pure:63`), is reached by no service. So none of these is tested:

- a DateTime passed to `all()` or to a milestoned property
- processing time at time-of-day precision, such as a version superseded later on the same day
- a DateTime compared against a `TIMESTAMP` milestone column, or a DateTime infinity date
- anything that depends on a connection's time zone (no connection sets one)
- a milestoning date passed as a service parameter (`all($asOf)`) rather than as a literal
- a class milestoned on processing time alone
- `allVersions()` (the one query that uses it has no tests) and `allVersionsInRange()`

EMIT covers some of this. Its date tests pass DateTime values (16 DateTime service parameters across 4 models), and
one model, `relational-zoned-timestamp-column`, names a connection time zone (two connections, `UTC` and
`America/New_York`).

### Execution

| Feature | Corpus | Status |
|---|---|---|
| service, service-test, test-data, shared-test-data, data-element | 4,742 services; `Reference` to `###Data` | ✅ 2,227 |
| graph-fetch | 447 graph fetch services | ✅ 57 |
| plan-generation | lite plans every service it runs | ✅ (lite's equivalent) |
| service-parameter | 1 (`73-service-surface.pure:42`) | ◐, never run (`surface::`) |
| multi-execution-service | 2 (`N4_TradeMultiExecution`, `surface::MultiRouted`) | ◐ (skipped / no tests) |
| post-validation | 1 (`surface::PostValidatedService`) | ◐, no tests |
| binding, external-format-binding | 1 binding | ◐ |
| external-format (test data) | 1 (`F37_SubstringPure`) | ◐, skipped |
| file-generation, model-generation, schema-set, serialization-date-time-format | — | ✗ |
| legacy mapping and service tests | — | ✗ |
| BigQuery function, hosted service, Snowflake app | — | ✗ |

The taxonomy no longer has `connection-time-zone`. No corpus connection sets `timeZone` either way.

### Persistence

All 12 persistence features are absent. The corpus has no `Persistence` element. EMIT covers all 12 in 9 models.

### Beyond EMIT's taxonomy

EMIT has no tags for query shape or for most join-condition forms. Among passing, populated services, the corpus
exercises:

| Construct | Tested services |
|---|---|
| Joins whose condition contains `or` | 28 |
| Non-equality joins | 42 |
| Multi-column joins | 39 |
| Self-joins | 9 |
| Multigrain filters | 4 |
| Views with `~groupBy` | 27 |
| Property mappings using a dynafunction (47 distinct functions) | 23 |
| Explicit target set ids | 40 |
| To-many navigation | 104 |
| `isEmpty` / `isNotEmpty` on to-many properties | 60 / 8 |

## 4. Queries

### 4.1 Vocabulary

Service queries use 25 platform functions:

- `getAll`, `getAllVersions`
- `filter`, `project`, `groupBy`, `agg`
- `sort`, `ascending`, `descending`, `limit`, `first`
- `graphFetch`, `serialize`
- `count`, `sum`, `max`, `min`, `average`
- `equal`, `greaterThan`, `lessThan`, `and`, `not`
- `isEmpty`, `isNotEmpty`

They also call 7 user functions. Query shapes break down as:

- plain projection: 2,161
- projection with a filter: 1,584
- `groupBy` (with or without a filter): 543
- `graphFetch`: 447

The filter predicates are `greaterThan` in 2,168 places (almost all `> ' '`), `equal` 11, `and` 63, `not` 4 and
`lessThan` 1.

Across every Pure expression in the corpus, 63 of the 331 PCT function names appear. That counts service queries,
function bodies, derived properties, constraints, M2M transforms, merge validations, XStore and ModelJoin conditions,
measures, and the names of mapping and store dynafunctions:

| PCT category | Used / total | Used |
|---|---|---|
| essential/string | 10 / 26 | contains, endsWith, left, replace, reverseString, startsWith, toLower, toString, toUpper, trim |
| essential/math | 8 / 23 | abs, cbrt, log, mod, round, sign, sin, sqrt |
| essential/date | 5 / 17 | adjust, dateDiff, datePart, dayOfMonth, monthNumber |
| essential/lang | 4 / 12 | cast, if, newUnit, toDecimal |
| essential/collection | 3 / 36 | contains, groupBy, sort |
| essential/meta | 0 / 35 | |
| grammar/functions | 17 / 37 | comparisons, arithmetic, `and`/`not`/`or`, `filter`, `first`, `getAll`, `getAllVersions`, `isEmpty` |
| standard/collection | 6 / 8 | and, count, in, max, min, or |
| standard/math | 4 / 28 | average, max, min, sum (no stdDev, median, percentile, corr, …) |
| standard/date | 2 / 5 | max, min |
| standard/tbd (bit ops) | 3 / 8 | bitAnd, bitShiftLeft, bitXor |
| unclassified/string | 2 / 18 | lpad, splitPart |
| unclassified/flow | 1 / 1 | coalesce |
| relation | name matches only / 74 | no query uses the relation API; 2 functions build `#>{}#` relations for the ModelJoin mapping. None of the 7 names added since 2026-09-29 (the set operations, `generateSeries`) |
| variant, hash, runtime, data-quality, quant | 0 | |

Most of the string, math and date names come from the combination matrix's mapping dynafunctions and from derived
properties, **not from queries**. The window-function work in `scripts/corpus/probe_relation.py` was run against the
engine but never became corpus services.

### 4.2 Constants in queries

Every literal in the 4,742 service queries:

| legend-pure type | Literals | Services | Where they appear |
|---|---|---|---|
| String | 2,188 | 2,120 | 2,166 are the `' '` in `x > ' '` (a not-null guard). About 8 are real comparisons (`status == 'EXECUTED'`), plus legacy `project` column names |
| Integer | 1,600 | 1,600 | **all** are `limit(n)`; 1,597 are `limit(25)` |
| Float | 1,396 | 1,388 | 1,386 are qualified-property arguments (`notionalIn(0.79)` 1,302 times); 2 are comparisons (`quantity > 500.0`) |
| StrictDate | 23 | 17 | milestoning dates: `getAll(%2024-…)` 22 times, `ratingVersions(%date)` once |
| LatestDate | 2 | 2 | `getAll(%latest)` |
| Boolean | 2 | 2 | `== false` |
| Decimal, DateTime, StrictTime, Date, Number, Byte | 0 | 0 | — |
| **Enumeration** | **0** | **0** | — |

Enum literals appear only in function bodies (11 occurrences): the `CtCurrency` values in
`core_types::ctMinorUnits` and `trading::Side.BUY` in `stress::rules::isBuy`. Derived properties add Float (97),
String (9) and Integer (1) literals. Constraints add Float (5), Integer (1) and String (1).

EMIT for comparison: its service queries use String, DateTime and StrictDate literals, and its mapping-test queries
use String, StrictDate, enum (4), Boolean and Integer. Neither suite covers every primitive type, but EMIT has
DateTime and enum constants and the corpus has neither.

### 4.3 Service parameters

| Type | Services with a parameter of this type |
|---|---|
| String | 1: `surface::ParameterisedService` (`String[1]`, 2 tests with parameter values). Never run by lite |
| Integer, Float, Decimal, Number, Boolean, Date, StrictDate, DateTime, StrictTime | 0 |
| any enumeration | 0 |
| any multiplicity other than `[1]` | 0 |

EMIT has 26 service parameters in 4 models: DateTime (16) and StrictDate (10), all `[1]`. Neither suite has an
Integer, Float, Decimal, Boolean or enum service parameter.

Function parameters are richer, but none is supplied by a test except `tradesByStatus(String)`. Across the 21
functions: Float 15, class-typed 7, enumeration 2 (`CtCurrency`, and `CcSettlementCycle` in `core_calendar`),
StrictDate 1, Integer 1, Boolean 1, String 1. They are called from derived properties and queries with literal or
property arguments.

### 4.4 Size

| Measure | Median | 90th percentile | Max | Where |
|---|---|---|---|---|
| Projected columns | 6 | 8 | 68 | `Q11_TradeMonster9Joins` (`92-services.pure:1046`, 8 related classes, passes) |
| Queries with more than 50 columns | — | — | 19 | `CB_C*` and `SP_Combo*` (about 60 columns each, one per combination cell) |
| Value-spec nodes per query | 29 | 45 | 288 | |
| Graph fetch leaves | — | — | 19 | `SP_ComboScopedGraph` (flat) |
| Graph fetch nesting | — | — | 4 levels below the root | 397 `DSTree_*` services, one chain each |
| Widest nested tree | — | — | 18 nodes, 14 leaves, depth 2 | `GG_*Tree` |

Project-column buckets: 452 with none (graph fetch and similar), 1,245 with 1–5, 2,995 with 6–10, 19 with 11–20,
12 with 21–50, and 19 with 51–100. No query has more than 68. No graph fetch uses subtype trees, qualified
properties inside the tree, or `graphFetchChecked`.

### 4.5 Combination

- **Distinct functions per query:** median 4, max 14.
- **Literal types per query:** 0 in 1,253 queries, 1 in 1,885, 2 in 1,568, and 3 in 36.
- **EMIT features reached per service** (excluding execution and scaffolding): 289 services reach none and 1,090
  reach 10. The maximum is 12, in `SP_defTradeCount` (`94-fanout-services.pure:290078`) and
  `SP_defClearedTradeEmpty`: an association and nested associations, derived and qualified properties, an
  enum-mapped column, a user function, OR and multi-column joins, and a mapping dynafunction, all in one passing
  query.
- **EMIT's own complexity score** (distinct domains): 402 basic, 4,339 intermediate and 1 advanced
  (`PL9_CounterpartyRatingCount`, `94-fanout-services.pure:5671`, which combines milestoning with an association and a
  filter). Almost every service touches the execution, grammar, mapping and store domains, so this score can't
  separate simple services from complex ones in this corpus. The feature counts above are the more useful measure.
- **The most stacked query**, `F32_TradeRollupEverything` (`94-fanout-services.pure:79`), combines a two-condition
  filter, a user function and an enum column as group keys, a derived property as an aggregate source, and
  count/sum/max with sort and limit. It **fails in lite and in the engine** (F14: grouping on an enum-mapped column
  groups by the database code). Its sibling `F30_TradeEverything` passes.

### 4.6 Depth that doesn't reach data

| | Count |
|---|---|
| Expected results that are empty | 2,200 of 4,736 |
| Expected results null at their deepest level | 280 |
| Four-join projections with a value at the bottom | 10 of 1,554 |
| Four-level graph fetch trees with a populated bottom level | 16 of 397 |

Every empty expectation sits on a table that has seed rows, so it still catches rows that leak through a filter. But
the deep query families mostly check no navigated value at all. The fix belongs in `scripts/corpus/deepstack.py` and
the seed data.

## 5. Constraints

All 8 constraints in the corpus:

| Class | Constraint | Expression | Types | Extras |
|---|---|---|---|---|
| `trading::Trade` | quantityIsPositive | `quantity > 0.0` | Float | |
| `trading::Trade` | priceIsNonNegative | `price >= 0.0` | Float | |
| `surface::Limit` | positiveAmount | `amount > 0.0` | Float | |
| `surface::Limit` | bounded | `amount < …` | Float | enforcement `Error`, externalId `LIMIT-001`, message |
| `surface::Limit` | softCheck | `!(currency == …)` | String | enforcement `Warn`, message |
| `largeexp::ExposureLine` | grossCoversNet | `grossExposure >= netExposure` | Float and Float | |
| `largeexp::ExposureLine` | creditRiskMitigationIsNonNegative | `crmEffect >= 0.0` | Float | |
| `largeexp::ExposureLine` | lineNumberIsPositive | `lineNumber > 0` | Integer | |

- **Size:** every constraint has 4 or 5 expression nodes and touches at most 2 properties.
- **Combination:** none combines functions, and none crosses types. No constraint mentions a date, Boolean, enum or
  Decimal, a collection (`exists`, `forAll`, `size`), a navigated property, a string function or an `if`.
- **Execution:** constraints only run under graph fetch (finding F32 in the corpus notes). 23 services graph-fetch a
  class with constraints; 2 of them pass with rows.
- **Function pre- and post-conditions:** none.
- **EMIT for comparison:** 6 constraints in 2 models, using `length` and `<=`, so EMIT is thin here too.

## 6. Tests

| | Stress corpus | EMIT (134 models) |
|---|---|---|
| Service test suites / tests | 4,737 / 4,739 | 85 tests in 13 models |
| Tests per suite | 1, except 2 suites with 2 | |
| Assertions per test | exactly 1 | |
| Assertion types | `EqualToJson` only | `EqualToJson` only |
| Serialization formats | `PURE_TDSOBJECT` 4,292; default 447 (graph fetch) | `PURE_TDSOBJECT` 55, `PURE` 30 |
| Service test data | `Reference` 4,745; `ExternalFormat` 1 | `Reference` 49, inline relational CSV 5, `ExternalFormat` 1 |
| Tests that pass parameters | 2 (never run by lite) | 57 parameter values in 4 models (DateTime 37, StrictDate 20) |
| Mapping test suites | **0** | **161 suites, 171 tests in 75 models** (`Reference` 148, `ModelStore` 25) |
| Legacy mapping and service tests | 0 | 4 and 1 |
| Function tests | 1 suite, 4 tests (`95-function-tests.pure:8`); lite doesn't run it | none found |

- **Size:** expected payloads have a median of 119 characters and a maximum of 74 KB, and 29 are over 10 KB. The
  seed is one shared `###Data` element, `stress::TestData`, of 4,942 rows across 393 tables, with the adversarial
  properties A1–A12 from `scripts/corpus/seed.py`.
- **Combination:** every test combines exactly one suite, one test, one `EqualToJson` assertion and one `Reference`
  to shared data. There are no tests with several assertions, no `EqualTo`, `EqualToTDS` or `Relation` assertions, no
  inline CSV data, and no `ModelStore` data.
- **Results in lite, DuckDB** (`//core:stress_suites`, floor 4,705): 4,705 pass, 15 fail and 16 are skipped, of
  4,736. The 15 failures fall into four known buckets:
  - graph DateTime spelling (6): six `GG_*Tree` services (finding F24)
  - services the engine also fails (5): `CB_NotEqualsNull`, `CF_Confluence`, `F32_TradeRollupEverything`,
    `F37_SubstringSql` and `O1_CounterpartyOtherwise`
  - decimal division (2): `LE0_ExposureLines` and `DSLocal_ExposureLine`
  - the hier binding (2): `H_IssuerProd` and `SP_IssuerProdIssuerGraph`

  The first edition's fifth bucket, `dateDiff` in hours (5), is gone because the expectations were corrected (see
  [Since 2026-09-29](#since-2026-09-29)). The skips are 10 multi-store suites, 3 whose runtime lives in an excluded
  file, 2 multi-execution tests, and 1 suite with ExternalFormat test data. The 3 `surface::` service tests (2 with
  parameters) are outside the runner's `stress::` filter.
- **Results in lite, H2** (`//core:stress_suites_h2`, a fresh session per test as legend-engine runs them, enforced
  since 2026-10-05, floor 4,676): 4,676 pass, 44 fail and the same 16 are skipped. Every DuckDB failure also fails on
  H2. The 29 H2-only failures are 15 combination cells whose bit operations H2's dialect refuses, 9 services over
  `core-units`' unit and conversion tables, and 5 others (`CB_C14`'s `REVERSE`, the two scoped combination
  services, `CV4_CurveShape` and `F38_FirstDayTypes`).

## 7. Mappings

### Relational

2,358 relational class mappings and 5,835 relational property mappings. The constructs used:

| Construct | Count |
|---|---|
| `~filter` | 1,849 (6 through a join) |
| `extends` | 1,848 |
| `~mainTable` | 510 |
| `~primaryKey` | 276 |
| Root marker `*` | 205 |
| Explicit target set ids | 109 |
| `~distinct` | 3 |
| Embedded / otherwise-embedded / inline property mappings | 30 / 4 / 1 |
| Enumeration transformers | 24 |
| Binding transformer | 1 |
| Literals / literal lists | 138 / 2 |
| Join chains of 1 / 2 / 3 hops | 630 / 313 / 1 |
| Typed join hops | 1 `(OUTER)`, plus one `(INNER)` on a `~filter` |
| Association mappings | 238 relational, 1 XStore, 1 ModelJoin |
| Includes | 158 (1 with store substitution) |
| Local properties | 4 |
| Enumeration mappings | 13 (71 String, 4 Integer and 3 enum source values) |
| Operations | 1 each of store union, router union, inheritance, merge; 1 aggregation-aware |

Property mappings also use 47 distinct dynafunctions, most of them from the combination matrix, 12 times each.

- **Size:** the largest mappings are `stress::DenseMapping` (60 class mappings, 711 property mappings, **no service
  uses it**) and `combo::ComboMapping` (22 class mappings, 665 property mappings). The largest class mappings are
  `combo::C0` and `combo::C12`, with 59 property mappings each. The median class mapping has 0 property mappings of
  its own, because 1,848 are `extends` subclass sets.
- **Combination:** features per class mapping break down as 0 in 222 class mappings, 1 in 161, 2 in 1,861, 3 in 101
  and 4–7 in 25. The richest are 10 `combo::C*` class mappings with 7 features each (embedded properties,
  dynafunctions, join chains, literals, a filter or filter via join, a primary key or `extends`). The matrix is
  documented at the top of `64-combinations.pure`.

### M2M

| | Stress corpus | EMIT |
|---|---|---|
| M2M class mappings | 5 (`canonical::M2MMapping`, `canonical::MoneyMapping`, `surface::PureSurface`, `surface::GapPureSurface`, `sub::PM`) | 23 in 16 models |
| M2M property mappings | 17 | |
| Transform functions | `times`, `cast` + `newUnit` (measure), `substring` | |
| `~filter` / explode / local property | 1 / 1 / 1, all in surface mappings no service reaches | |
| Enumeration transform, `^Target(...)` instantiation, nested M2M sets, M2M union or merge | none | present |
| Services reaching M2M | 4: `M1_TradeCanonical`, `M2_CanonicalWithEnum`, `MU0_MonetaryTrade` (chained over relational through `ModelChainConnection`), and `F37_SubstringPure` (`JsonModelConnection`) | |
| Run by lite | **0**. The three canonical services' runtime lives in excluded `55-canonical-store.pure`; `F37_SubstringPure` needs ExternalFormat test data | |
| ModelStore test data | 1 data element, used by no test | 25 mapping-test data blocks in 10 models |

**Verdict:** relational coverage is broad and deep. M2M coverage is close to zero. In lite it is exactly zero, since
nothing M2M executes, and on the engine side it is four services with simple transforms.

### Relation (`~func`)

2 class mappings (7 property mappings) and 1 ModelJoin, in `58-modeljoin-store.pure`. They are reached by 2 passing
services. EMIT has 56 relation class mappings in 26 models covering 13 relation-mapping features; the corpus touches
one of them.

## 8. Stores

| | |
|---|---|
| Databases / tables / columns | 30 / 437 / 4,843 (661 NOT NULL) |
| Column types used (19) | Varchar 2,483, Double 727, Date 588, Bit 381, Decimal 243, Integer 211, Timestamp 111, Char 33, SmallInt 25, BigInt 21, Numeric 8, Float 4, Other 2, and Binary, Varbinary, TinyInt, Real, SemiStructured, Json 1 each |
| Primary keys | every table: 410 single-column, 17 two-column, 10 three-column |
| Joins | 390. 363 single equality; 15 multi-column (13 two-column, 2 three-column); 12 without an equality (ranges and functions) |
| Join-condition functions | `and` 22, `or` 5, `>=` 7, `<` 7, `>` 3, `isNull` 3, `isNotNull` 2, `concat` 3, `toUpper` 3, `in` 1, `!=` 1, `<>` 1; 4 self-joins |
| Joins no mapping uses | 32 of 390: the 8 `*_Instrument` subclass joins, 5 `dense_*` joins, 8 surface `j_*` joins, 3 in `core_fx`, 2 in `hier`, and `TierVersion_Trade`, `ReconItem_Payment`, `ReconItem_Movement`, `OtcTrade_Counterparty`, `Counterparty_Classification`, `surface_AnsiNotEqual` |
| Filters | 1,856, plus 2 multigrain |
| Views | 7: 6 with `~groupBy`, 2 with `~distinct`, 1 with `~filter` |
| Milestoning | business 6, processing 3, business snapshot 1, processing snapshot 1 |
| Schemas / includes / tabular functions | 5 named / 16 / 1 |
| Connections | 16 relational (DuckDB 11, H2 2, Postgres, Snowflake, Databricks). Auth: test ×11, h2Default, snowflakePublic, delegatedKerberos, userNamePassword, apiToken. 2 post-processor kinds, 1 `queryGenerationConfigs`, 1 `quoteIdentifiers`, 1 timeout. **No `timeZone`** |
| Model connections | 2 JSON, 1 XML, 1 ModelChain |
| Runtimes | 25 engine runtimes, 1 local, 1 embedded in a service |
| Other store types | none (no ServiceStore, flat data, MongoDB, Elasticsearch, …) |

- **Size:** `store::DB` has 338 tables, 3,987 columns, 297 joins and 1,819 filters. The widest table is
  `core_instrument::Store` `CI_INSTRUMENT` with 105 columns; the median table has 11.
- **Combination:** the most complex join conditions are
  - `dense_OrMixed`: `(a > b) or (a is null and b is not null)`, never used by a mapping
  - `Combo_Or`: an `or` with a null test and a sentinel literal, used by 17 services in the combination matrix
  - `Pillar_LongerPillar`: a three-column self-join whose third condition is an inequality, used by 3 passing
    services
- **Semi-structured columns:** declared twice, never navigated.

## 9. The class model

| | |
|---|---|
| Property types | String 2,450, Float 1,169, StrictDate 633, Boolean 493, Integer 466, DateTime 162, class-typed 138, enum 25, Date 3, and Decimal, Number, Byte, StrictTime, a measure, a precise primitive and `Relation` 1 each |
| Multiplicities | `[1]` 3,177, `[0..1]` 2,349, `[*]` 20, `[1..*]` 1; no other bounds |
| Derived / qualified properties | 206 / 89 (qualified-property parameters: Float 88, String 1) |
| Default values | 4 (one class, `surface::Counterparty`) |
| Stereotypes / tagged values | temporal on 5 classes; a few governance tags |
| Associations | 250. `[1]`–`[*]` 161, `[0..1]`–`[*]` 64, `[1]`–`[0..1]` 9, `[*]`–`[0..1]` 8, `[0..1]`–`[0..1]` 4, `[*]`–`[*]` 4 |
| Inheritance | 1,849 subclasses, at most 2 levels deep |

- **Size:** the widest classes are `combo::C0` and `combo::C12` with 59 properties.
- **Combination:** the richest class is `largeexp::ExposureLine` (`8110-largeexp.pure:32`): 7 property types,
  constraints, and derived and qualified properties.
- **Missing:** Decimal and DateTime-heavy models, `[m..n]` multiplicities, generic classes, and function pre- and
  post-conditions.

At the protocol level, the corpus uses 132 of the 1,047 roster tags. Many of the rest are execution-plan nodes and
other stores that no user model can contain.

## 10. Gaps, in priority order

1. **Service parameters of every type.** Add `stress::` services taking String, Boolean, Integer, Float, Decimal,
   Number, Date, StrictDate, DateTime and StrictTime parameters, plus an enumeration, at `[1]`, `[0..1]` and `[*]`.
   Give each tests with `parameters:`. Also consider having the runner pick up `surface::` services, so the three
   existing tests run.
2. **Constants of every type in predicates.** Replace the `> ' '` guard idiom where the point is a real comparison.
   Add filters comparing against Decimal (`1.5d`), DateTime, StrictTime, Date, Number and Boolean literals and against
   enum values (`Side.BUY`), plus `in` lists, `or`, and date arithmetic.
3. **Milestoning with time of day.** Add a bitemporal table whose processing columns are `TIMESTAMP`, with a
   DateTime infinity date and seed rows superseded more than once within a day. Query it with DateTime
   literals and with a date passed as a service parameter (`all($asOf)`), both at the root and through a
   milestoned property. Add a class milestoned on processing time alone, a connection that sets `timeZone`, a
   `TIMESTAMP` column the database holds with a zone (EMIT's new `store:relational-zoned-timestamp`), and tested
   `allVersions()` and `allVersionsInRange()` queries.
4. **M2M.** Add an M2M block that lite can run, with `ModelStore` or JSON test data, enumeration transforms, `^`
   instantiation, class-typed and nested set implementations, `~filter`, union and merge. Bring
   `55-canonical-store.pure` back into lite once Measure/Unit support lands.
5. **Mapping and function test suites.** None run. Add mapping `testSuites` (EMIT's main relational test form)
   and a runner for them, and run the existing function test. The pieces are listed in
   `docs/DEFERRED_TEST_EXECUTION.md`.
6. **Deep queries that reach data.** Seed the broken chains and have `deepstack.py` reject any query that returns no
   rows on the seed, as `combos.py` already does. 1,520 four-join projections and 105 four-level trees currently
   return nothing, and 276 more trees never populate their deepest level.
7. **Constraints that combine.** Add constraints mixing functions and types: dates against dates, enums, strings with
   `startsWith`/`length`, `exists`/`forAll` over to-many properties, `if`, navigated properties. Add graph fetch
   services that check them.
8. **Query vocabulary.** Add olap and window functions, `extend`, TDS joins, `distinct`/`take`/`drop`/`slice`,
   `exists`, `map`, `if`, string/date/math functions inside queries, `subType()`, and the relation API, including
   its new set operations.
9. **Declared but unreached.** Add services for `DenseMapping` (its whole point is an invariance check against the flat
   mapping), the union, merge and inheritance operations, the literal-list mappings, and the 32 unused joins.
10. **Store breadth.** Semi-structured navigation, and full and right outer joins. (The connection `timeZone` and the
    zoned timestamp column are in item 3.)
11. **Test shape.** Suites with several tests, tests with several assertions, and the other assertion kinds
    (`EqualTo`, `Relation`).
12. **Skipped suites.** Multi-store test runtimes (F-A), multi-execution binding, and ExternalFormat provisioning.

## Appendix: reproducing this

The legend-engine and legend-pure releases are pinned in `release.MODULE.bazel` (4.145.0 and 5.99.0). The EMIT
models, `emit.md` and the PCT sources come from local checkouts. This update used legend-engine `35930373b41`
(2026-10-16) and legend-pure `57f94a6c1` (5.108.0 and 17 commits). The first edition used legend-engine `1d40dd7251d`
and a legend-pure 5.105.1-SNAPSHOT (`5fed02095` reproduces its PCT count). The scripts are in [`tools/`](tools/), and
`tools/run.sh` runs all of them in order:

```
bazel test //core:stress_suites          # the pass list the scripts judge against (gate 10, DuckDB)
docs/stress-corpus-feature-report-2026-09-29/tools/run.sh <dir holding legend-engine/ and legend-pure/> <work dir>
```

On Windows, run both from Git Bash with `BAZEL_SH` set to Git's `bash.exe` and `MSYS_NO_PATHCONV=1`, and set
`PYTHON=python` where there is no `python3`.

| Script | What it produces | Report sections |
|---|---|---|
| `run.sh` | the whole pipeline, including both engine parses (the corpus, and the EMIT models in one document) | all |
| `lib.py` | shared: the model index and the value-specification walker over the protocol document named by `PMCD_JSON` | — |
| `emit_census.py` | the EMIT descriptors: features, stores, complexity | 1, 3 |
| `emit_concat.py`, `emit_analyze.py` | the EMIT models parsed as one document and measured like the corpus | 4.2, 4.3, 6, 7 |
| `pct_inventory.py` | the PCT function names, by category | 4.1 |
| `expectations.py` | each expected result classified as populated, hollow or empty | 3 (tested), 4.6 |
| `deep_populated.py` | whether the deepest navigated value is ever populated | 4.6 |
| `analyze.py` | the element census: services, parameters, literals, classes, constraints, mappings, stores | 2, 4, 5, 7, 8, 9 |
| `profile.py` | each service's reach through its mapping, in EMIT's taxonomy plus query shape | 3, 4.5 |
| `coverage.py` | function coverage against PCT, literals per context, service-test shapes | 4.1, 4.2, 6 |
| `complexity.py` | size and combination distributions | 4.4, 4.5, 7, 8, 9 |
| `literal_sites.py` | where each literal type appears in the queries | 4.2 |
| `unused_joins.py` | joins no mapping, view or filter references | 8 |
| `matrix.py` | the EMIT feature matrix; its "declared" column is hand-entered from `analyze.py`'s output | 3 |

Some statuses in §3 were adjusted by hand where the static resolution can't see the reach. The four M2M services,
`MU0_MonetaryTrade` (measure), `J0_TradeBookModelJoin` (ModelJoin), the implicit left-outer-join coverage and
`mapping-include` are each explained where they appear.
