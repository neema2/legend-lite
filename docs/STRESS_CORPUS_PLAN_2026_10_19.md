# Stress corpus improvement plan

Proposed 2026-10-19. The user took the five decisions on 2026-10-21 ([Decisions](#decisions)), and Phase 0 started
that day (`docs/IN_FLIGHT.md`, the twelfth line). This plan turns the gaps in the
[stress corpus feature report](stress-corpus-feature-report-2026-09-29/README.md) (§10) into ten phases of work. Each
phase lists the files it changes, how its expected results are computed, roughly how many services it adds, and the
report numbers that say it is done. A session that starts a phase adds a line to `docs/IN_FLIGHT.md` first.

## Goal and measures

The corpus exists to catch wrong answers. The report found it wide in relational mapping and store features and narrow
nearly everywhere else. Its queries filter on `' '` rather than on typed values, it takes almost no parameters, and its
deep queries mostly return nothing. Its milestoning is day-grained, its queries use 25 platform functions, and its M2M
and relation mappings are close to absent. This plan widens it along those axes.

**Progress is judged on legend-engine, not on lite.** The report counts a feature as tested when a service reaching
it passes *in lite*. The corpus can promise only expected results that legend-engine agrees with; whether lite passes
them is lite's burn-down. Each target below therefore counts services that pass on legend-engine with a populated
expected result, at the one release every test uses (D3: `release.MODULE.bazel`'s `LEGEND_ENGINE_RELEASE`, 4.145.0
on 2026-10-21). Lite's results are reported beside them but are not a target.

| Measure (report section) | 2026-10-19 | Target | Phase |
|---|---|---|---|
| Service parameter types (§4.3) | 1 (String, never run by lite) | every type with a literal syntax at `[1]`; `[0..1]` and `[*]` for at least 4 | 1 |
| Literal types in service queries (§4.2) | 6 of 12 | 11 of 12 (Byte has no literal) | 1, 3 |
| Enumeration literals in queries (§4.2) | 0 | at least 30 | 1 |
| Filters using `or` / `in` / `not` (§4.1) | 0 / 0 / 4 | at least 20 each | 1 |
| Filters comparing against a real value instead of `' '` (§4.1, §4.2) | 14 | at least 1,500 | 2 |
| Four-join projections with a value at the bottom (§4.6) | 10 of 1,554 | at least 1,400 | 2 |
| Four-level graph fetch trees populated at the bottom (§4.6) | 16 of 397 | at least 360 | 2 |
| Empty expected results (§4.6) | 2,200 of 4,736 | at most 900, each in a declared emptiness family | 2 |
| DateTime literals / milestoned queries at time of day (§3, §4.2) | 0 / 0 | at least 40 / at least 20 | 3 |
| Platform functions in service queries (§4.1) | 25 | at least 150 | 4 |
| PCT function names used anywhere (§4.1) | 63 of 331; at the pin, 64 of 321 | at least 150 | 4 |
| M2M services lite runs rather than skips (§7) | 0 | at least 30 | 5 |
| Relation (`~func`) mapping features covered (§3) | 1 of 13 | at least 10 | 6 |
| Features in the model that no service reaches (○, §3) | 9 | at most 2 | 7 |
| Constraints / constraints combining types or functions (§5) | 8 / 0 | at least 20 / at least 10 | 8 |
| Mapping test suites (§6) | 0 | at least 50 | 8 |
| EMIT features covered, judged on the engine (§3) | 43 of 109 at the pin (lite: 37), 2026-10-21 | at least 80 of 109 | all |

The 80 sits below what can be reached. At the pin (D3) the EMIT taxonomy has 109 non-scaffolding features, not the
report's 117, which were counted from a newer legend-engine checkout. 22 of the 109 are out of reach for this plan:
the 12 persistence features, 8 absent execution features, and the service and flat-data stores (see
[Not in this plan](#not-in-this-plan)). That leaves 87, and the margin of 7 allows for constructs the pinned engine
refuses. The baseline's 43 and 37 are `matrix.py`'s automatic count; the report's §3 adjusts a few statuses by hand
where its static resolution can't see the reach.

## Rules for every batch

1. **Expected results come from the oracle** (`scripts/corpus/oracle.py`), never from either engine's output. Where
   the oracle can't compute a construct, extend it first or use an invariance twin (rule 2). `build.py` refuses a
   service the oracle can't evaluate, and that stays.
2. **Prefer invariance twins where an evaluator would be expensive:** a service whose expected result must equal
   another's. Examples are a parameter against the same literal, an M2M mapping against the relational query that
   computes the same values, and `DenseMapping` against the flat mapping. `build.py` already fails when a group in
   `battery.INVARIANCE_GROUPS` disagrees, and new twins join that list.
3. **Every new or changed service runs on legend-engine before it lands:** `bazel run //scripts/corpus:changed --
   --out changed.txt`, then `bazel run //scripts/corpus:run -- --only changed.txt`. It must pass. If it fails, either
   it is quarantined in `ENGINE_QUARANTINE` (with a reproduction under `scripts/corpus/repro/` and an entry in
   `docs/UPSTREAM_FINDINGS.md`, as `quarantine.py` requires), or the oracle is wrong and gets fixed. After a seed
   change `changed` lists every testable, and the whole corpus runs: `bazel run //scripts/corpus:run -- --jobs 4`
   took 88 minutes on a 20-core Windows desk (2026-10-21). Build nothing while it runs, since a JVM that starts
   while Bazel rewrites the runner's JDK or jars dies.
4. **A new service expects rows.** An empty or hollow expected result is allowed only in a declared emptiness family:
   empty subtypes, `isEmpty` ends, dates before the first version, and filters built to exclude everything. From
   Phase 2 on, a `build.py` check enforces this.
5. **Lite's failures don't block a landing.** Raise `MIN_PASS` in `StressServiceSuitesTest` and
   `StressServiceSuitesH2Test` by what passes, with a dated note (guard files move only with a dated justification).
   Record new failures by bucket in [Progress](#progress).
6. **Each feature a phase makes executable gets a predicate in `scripts/corpus/executed.py`,** so that `build.py`'s
   "feature no longer executed" check holds it.
7. **Regenerate and prove.** Regenerate with `bazel run //core:update_stress_corpus`. Prove with
   `bazel test //:generated //scripts/corpus:all //core:stress_suites //core:stress_suites_h2 //core:corpus_differential_test`,
   and run `//gates:local` before pushing. Then re-run the report's `tools/run.sh` and record the measures.

## Decisions

Taken by the user on 2026-10-21. Each entry states the question as it was put, then the answer.

- **D1, the engine verdict.** Rule 3 needs a way to run a named set of services on legend-engine. Today that means
  `bazel run //tools/engine-runner:testable -- <files> --testable=…` (the same binary `tools/run.sh` uses). The Bazel
  launcher for `run.py` and the sharded `engine_stress` test (P3-25) were drafted but never landed. The docs place the
  draft on a `bazel/exec` branch (Exec-2 in `docs/build-inventory/program/PHASE_8.md`). That branch is on neither
  `origin` nor `upstream`, and `docs/BUILD_REBUILD_DESIGN_2026_10_05.md` records the P3-25 part as uncommitted even
  there. **Decided: this plan takes Exec-2 over as Phase 0** ("go ahead"), announced in `IN_FLIGHT.md`. No clone on
  this desk holds `bazel/exec` (no worktree, no local or remote ref, 2026-10-21), so Phase 0 rewrites the work from
  P3-25's entry in `docs/BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md`.
- **D2, seed churn.** Phase 2 adds seed rows, which changes the expected result of most services over those tables:
  thousands of lines of generated diff in one commit. **Decided: accept it.** The files are generated, and a full
  engine run checks them.
- **D3, the engine version.** **Decided: one engine version for every test, and the corpus names none of its own.**
  The version is `release.MODULE.bazel`'s `LEGEND_ENGINE_RELEASE`, which is 4.145.0 on 2026-10-21, and
  `//tools/bump` moves it. Everything this plan judges with is keyed on that pin: the engine runner's jars
  (`@maven_runner`), and the legend-engine and legend-pure source trees the report reads its EMIT and PCT
  denominators from (`@legend_engine_src`, `@legend_pure_src`). Constructs that need a newer legend-engine wait for
  the bump: PCT's relation set operations and `generateSeries`, and the EMIT models for zoned timestamps and relation
  graph fetch. One consequence: the report's 2026-10-19 denominators came from hand checkouts (legend-engine
  35930373b41, legend-pure 57f94a6c1), not from the pin. Phase 0 re-measures them at the pin.
- **D4, `surface::` services.** Lite's runner takes only `stress::` services (`StressSuites.java:63`), so the 3
  `surface::` tests never run there. `run.py` runs every service that has tests. **Decided: only `stress::`, for
  now.** Lite's filter stays, and the engine run takes the same set (the `stress::` services that have tests, plus the
  `stress::` function testables), so both engines judge one list. New services go under `stress::`.
- **D5, the `> ' '` guards.** Phase 2 rewrites the 2,166 `> ' '` filters that `deepstack.py`, `stacks.py` and
  `spread.py` write, so that they compare against a seeded value. The guard also tests NULL handling through outer
  joins. **Decided: rewrite them, keeping about a tenth as they are.**
- **D6, a platform's last-ulp `cbrt` (raised 2026-10-21 by Phase 0).** On Windows, legend-engine's DuckDB returns
  `cbrt(325.0)` as the double next above the correctly rounded root the oracle expects (0.56 ulp from the true root),
  and three combination cells fail (`docs/UPSTREAM_FINDINGS.md`, F60). The earlier engine runs were on macOS. glibc's
  `cbrt` isn't correctly rounded either, so Linux may fail them too; that hasn't been measured. The options:
  - (a) change the seed or the generator so `cbrt` cells take inputs with exact cube roots. Every platform then
    agrees, and the cells still test `cbrt`, though not its last bit.
  - (b) give `quarantine.py` per-platform entries.
  - (c) accept three unexpected verdicts on Windows.

  The corpus is for catching wrong answers, and a C library's last ulp isn't one. `exactmath` exists to make expected
  results platform-independent, and that works only when the engine's result is too. **Decided (2026-10-23): (a).**
  Done on `stress_test/corpus`: the twelve `cbrt` cells read perfect cubes, and their roots are whole numbers.

## Phases

### Phase 0: the engine verdict and an engine-judged measure (about 2.5 days)

- Write P3-25 (D1). `scripts/corpus/run.py` becomes a `py_binary` that launches `//tools/engine-runner:testable` from
  runfiles and takes `--only <file of service names>`. `engine_stress` becomes a manual, sharded `py_test`. The two
  issues Exec-2 found in the unlanded draft apply to the rewrite: `:run` must be `manual`, and `engine_stress` needs a
  `shard_count`, because a full run is longer than an `enormous` test's hour.
- `scripts/corpus/changed.py` lists the services whose text or expected result differs between the committed
  generated files and a fresh generation. That list is what a batch runs on the engine.
- `run.py` writes the engine's pass list. The report's `profile.py` and `matrix.py` take it as a second "tested"
  column beside lite's.
- **Done when:** `bazel run //scripts/corpus:run -- --only <list>` works on Windows and Linux, and the engine-judged
  EMIT count is recorded as this plan's baseline.

**What was built (2026-10-21).**

- `//scripts/corpus:run` (`run.py`, a manual `py_binary`) starts `//tools/engine-runner:testable` from its runfiles.
  It judges each testable against `quarantine.py` and writes `engine-pass.txt` in lite's pass-list form, plus
  `engine-verdicts.tsv`, `engine-report.tsv` and each JVM's log. The testables are the `stress::` services that have
  tests, plus the function testables (D4). The corpus's files and linked projects come from `core/stress.bzl`'s one
  list, so `tools/wrongrows/engine-rows.sh` became one line over it, and the third `LINKED_PROJECTS` copy is gone.
- `//scripts/corpus:engine_stress` is the same program as a manual `py_test` in `//gates:heavy`, with 8 shards.
- `//scripts/corpus:changed` lists the testables a batch must run, comparing the corpus with a git revision (default
  `HEAD`) element by element. It lists the new and changed services. When any other element changed or was deleted
  (a seed, a mapping, a class), it lists every testable. Whole-line comments change nothing.
- `TestableMain` runs testables one at a time and writes each one's result to `--report` as it finishes. A testable
  past `--limit` is reported HUNG and the JVM halts, and `run.py` starts a new JVM on the rest. A JVM's death counts
  only when it happens twice: against the testable running both times, or against the batch if it never reached a
  testable. Arguments can come from an `@<file>`, since 233 file paths and 200 names overflow Windows' command line.
- The report's `run.sh` reads legend-engine and legend-pure from the pinned trees (D3). Given the engine's pass list,
  `profile.py` and `matrix.py` add an engine-judged column.
- Tests: `:run_test` covers the batch loop's hang, crash and silent-JVM paths. `:changed_test` covers the element
  comparison. The runner's smoke test now executes a service and checks the report. `:tools_build_test` keeps the
  two manual tools compiling.

**The baseline (2026-10-21, legend-engine 4.145.0, Windows x64, main at `adcb420b7` plus this phase).** The whole
corpus ran in 88 minutes with `--jobs 4`. Of 4,736 testables:

| Verdict | Testables | |
|---|---|---|
| PASS | 4,699 | |
| KNOWN-FAIL | 32 | every quarantined service still fails as recorded. F58's five, quarantined on 2026-10-05 "to be confirmed by `run.py`", are confirmed |
| NOT RUN | 1 | `X1_ExternalEntityProjection`, F15's hang |
| FIXED | 1 | `H_IssuerBinding`: F27 is fixed upstream by 4.145.0, so its quarantine entry is removed. Regenerating the corpus after the removal changed no file |
| REGRESSION | 3 | `CB_C0`, `CB_C2`, `CB_C4`: F60, the platform's `cbrt` (D6). Since D6 (a) they pass |

That makes 4,700 testables passing on the engine, against lite's 4,705 passing tests on DuckDB (15 fail, 16 skipped,
the same as the report). After D6 (a), the 26 testables that use the combination model were run again on Windows:
25 pass, 1 is known to fail (F28), and none is unexpected. Lite's pass list didn't change. 14 more first came back DIED: JVMs that crashed on starting while a concurrent `bazel test`
rewrote the JDK under them. Alone, all 14 passed. `run.py` now counts a JVM's death only when it happens twice.

At the pin the report's tools give:

- EMIT: 114 models, and 109 non-scaffolding features. 43 are tested on legend-engine and 37 in lite (`matrix.py`, before
  §3's hand adjustments).
- The six features the engine tests and lite doesn't are the ones lite's runner skips: cross-store, M2M transform,
  semi-structured binding, binding, external format and multi-execution.
- PCT: 321 function names, 64 of them used.

**Not done.** `:run` hasn't been run on Linux. CI builds it in the `core` lane (`:tools_build_test`) and runs the
runner's smoke test on every platform, but the full run is only measured on Windows. `engine_stress` was checked
sharded on three services, not on the whole corpus.

**Found on the way.** legend-engine's service test runner keeps every test's pooled DuckDB connection, and each later
test's data setup gets slower: twelve services sharing `stress::RT` went from 2.4 s to 38 s, one after another
(`docs/UPSTREAM_FINDINGS.md`, F59). The runner now closes the pools after every testable, as legend-engine's own test
helpers do. Each of those twelve then took 1.7 s, so batches of 40 were no longer needed. The finding also explains
the old pool exhaustion behind them.

### Phase 1: typed constants and parameters (about 6 days, about 300 services)

Report §4.2, §4.3 and §10 items 1–2. Real queries filter on typed values and take parameters. The corpus filters on
`' '` and has one parameter, which lite never runs.

- **Model.** Add a hand-written domain pack, `8112-typedvalues.pure`, holding a class, a store and a mapping; the
  mapping is included from `89-all-mapping.pure`. The class has one property of each primitive type, at `[1]` and at
  `[0..1]`: String, Boolean, Integer, Float, Decimal, Number, Date, StrictDate, DateTime, StrictTime and Byte. It also
  has an enumeration property mapped from codes. Each property maps to a column of the matching SQL type. A to-one
  association from `trading::Trade`, with some trades orphaned, makes the same values reachable one hop away over an
  outer join.
- **Seed** (`seed.py`). For each type, values that make every operator discriminate:
  - NULL, ties, and the boundary value with its neighbours (a date and the days either side; midnight and
    `23:59:59`)
  - negative numbers, zero and extremes
  - Decimals whose float rounding differs
  - strings that differ by case, carry an apostrophe or have a trailing space
  - enumeration codes
- **Reader and oracle** (`query.py`, `oracle.py`).
  - Literals of every type. Today `_literal` reads String, Boolean, Integer and Float.
  - Predicates as a tree with `or`, `not` and `in`, under three-valued logic. Today a filter is a list of ANDed
    comparisons.
  - Service parameters bound by name: at `[1]`; at `[0..1]` in the optional-filter form
    `$p->isEmpty() || $x.v == $p->toOne()`; and at `[*]` as `$x.v->in($ps)`.
- **Emitter** (`emit.py`). The service function's parameter list, and the test's `parameters:` block.
- **Generator.** A new `literals.py`, called from `build.py`, writes a new generated file,
  `99-typed-services.pure`, added to `STRESS_GENERATED` in `core/stress.bzl`.
  - The comparison matrix covers the nine ordered types with a literal (String, Integer, Float, Decimal, Number, Date,
    StrictDate, DateTime, StrictTime) under `==`, `!=`, `<`, `<=`, `>` and `>=`, plus Boolean under `==` and `!=` and
    the enumeration under `==`, `!=` and `in`. Each comparison appears at the root and one hop away, as a literal
    service and as a parameter twin with the same expected result. That makes 236 services.
  - About 40 services combine `or`, `and` and `not` over nullable columns, where three-valued logic decides the
    answer.
  - About 20 `[0..1]` and `[*]` parameter services each have two tests: value given and value absent, or one value
    and several.
- **Lite.** `ServiceTestRunner` already binds parameters as let-bound variables. Under D4 the runner keeps its
  `stress::` filter, so the new services are `stress::` services.
- **Done when:** the parameter, literal, enumeration-literal and `or`/`in`/`not` rows reach their targets, and every
  new service passes on the engine or is quarantined with a finding.

### Phase 2: data that reaches the bottom of deep queries (about 5 days; changes existing services)

Report §4.6 and §10 item 6. 2,480 of 4,736 expected results are empty or hollow, and the deep query families mostly
check no navigated value.

- **Seed spines.** `deepstack.py` and `graphs.py` expose each four-hop chain and four-level tree with the tables and
  joins along it. A new seed pass, `spines.py`, is called from `seed.py`. It adds or links the fewest rows that give
  each chain at least two complete paths ending in a non-null value. It removes no adversarial break: A1 (orphan
  foreign key), A2 (null foreign key) and A11 (a broken second hop) stay.
- **Selective filters (D5).** `deepstack.py`, `stacks.py` and `spread.py` pick the comparand from the seeded values of
  the filtered column, so that the predicate keeps between a fifth and four fifths of the non-null rows, instead of
  `> ' '`. Both sides of each `DSDeep`/`DSPost` invariance pair take the same comparand.
- **`limit(25)`.** 1,597 queries use it, and on small tables it's a no-op. Where a sort precedes the limit, pick a
  limit below the result size, on a sort key without ties. Ties (A5) make the cut-off depend on order, and the oracle
  must keep refusing that.
- **A `build.py` check.** An empty or hollow expected result outside the declared emptiness families fails the build.
  `combos.py` already rejects an empty matrix.
- **The engine run is a full one,** because the seed changed.
- **Done when:** the three §4.6 rows and the deep-filter row reach their targets.

### Phase 3: milestoning at time of day (about 4 days, about 50 services; after Phase 1)

Report §3 (Milestoning) and §10 item 3. Every milestoned query today is day-grained.

- **Store, model and seed.**
  - A bitemporal table whose processing columns are `TIMESTAMP`, with a DateTime infinity date.
  - A class milestoned on processing time alone.
  - Seed rows superseded twice within one day, and once exactly at midnight.
- **Queries.**
  - `all()` with DateTime literals before, at and after each change; the instant of a change is the boundary that
    matters.
  - `all($asOf)` with StrictDate and DateTime parameters.
  - Milestoned property navigation with a DateTime.
  - A tested `allVersions()`. `allVersionsInRange()` is probed on the pinned release first.
- **Probes first.** A connection with `timeZone`, and a zoned timestamp column, start as probes, because time zones
  have moved upstream: legend-engine #5341 reverted its time-zone fixes. If lite differs deliberately from any service
  that lands, that difference gets a row in `docs/SEMANTICS_REGISTER.md`.
- **Oracle.** `_milestone_rows` compares DateTime milestones and handles processing-only classes and `allVersions()`.
- **Done when:** the DateTime and time-of-day rows reach their targets, a processing-only class is tested, and
  all-versions-query is covered.

### Phase 4: the function vocabulary (about 8 days, about 400 services)

Report §4.1 and §10 item 8. The oracle computes 256 of the 262 function names in legend-engine's SQL registry, and 166
of those have run on the engine and agree (`docs/FUNCTIONS_EXECUTED.tsv`, `scripts/corpus/functions.py`). The probes
ran them on small seeds of their own. No corpus service uses them, so gate 10 never tests lite on them.

- **Oracle.**
  - `evaluate` applies a scalar function to a projected value or inside a filter, and an aggregate inside `groupBy`,
    through the implementations the probes already use.
  - Relation shapes go through `RELATION_IMPL` and the TDS registry: `extend`, `rename`, `select`, `distinct`,
    `slice`, `take`, `drop`, window functions, joins, `olapGroupBy` and TDS joins.
- **Generator.** A new `vocab.py`, with its own generated file:
  - Each of the 166 agreeing functions appears at least twice over corpus data: once in a projection, and once in a
    filter or an aggregate. Each runs on a column with NULLs and boundary values, which the probes' seeds lack.
  - Each relation and TDS shape runs over a seeded class. Window functions run over A5's ties, where `rank`,
    `denseRank` and `rowNumber` differ.
  - `let`, `if`, `exists`, `map` and `subType()` appear inside queries.
- **The 23 functions recorded as disagreeing with the oracle are adjudicated first.** Each turns out to be an oracle
  fix or an upstream finding, and neither goes into a service until decided. Re-running their probes needs the same
  launcher repair as `run.py`, which is P7-03.
- **Done when:** the platform-function and PCT rows reach their targets, and window, olap, TDS-join and relation-API
  services pass on the engine.

### Phase 5: M2M (about 5 days, about 40 services)

Report §7 (M2M) and §10 item 4. The corpus has 5 M2M class mappings, and lite runs none of them.

- **Split `55-canonical-store.pure`.** Move its M2M mapping and `ModelChainConnection` runtimes into a file without the
  Money measure, so that `M1_TradeCanonical` and `M2_CanonicalWithEnum` load in lite. Only the measure part stays in
  `StressExclusions`, until lite has Measure and Unit types.
- **A new `m2m::` pack, chained over relational sources** through `ModelChainConnection`. Its test data therefore stays
  relational, which lite's runner provisions; the runner skips every other kind. The pack covers EMIT's M2M features:
  - transforms and derived source properties
  - `^` target instantiation and complex property passthrough
  - enumeration transforms
  - class-typed properties with nested set implementations
  - `~filter`, union and merge
- **Expected results by invariance.** Each M2M service has a relational twin that computes the same values from the
  source, following `battery.py`'s `_m2m_pair`. The oracle needs no M2M evaluator.
- **Done when:** at least 30 M2M services pass on the engine, and lite runs them (pass or fail, not skip). Covered:
  m2m-transform, m2m-chained-relational, m2m-target-instantiation, m2m-derived-source-property and
  m2m-complex-property-passthrough.

### Phase 6: relation (`~func`) mappings (about 5 days, about 40 services)

Report §3 and §7. One of the 13 relation-mapping features is covered.

- **A new `relmap::` pack.** It holds relation class mappings over tables that already have seed rows, at least one
  per feature: `~src`, filter, group-by, window function, expression right-hand sides, primary key, local property,
  embedded and inline embedded, union, binding transformer and XStore association. Each is probed on the pinned
  release first (D3).
- **Expected results.** Where the feature allows, each relation-mapped class has a relationally mapped twin over the
  same table. Otherwise the oracle evaluates the `~func` body through `RELATION_IMPL`.
- **Done when:** at least 10 of the 13 features are covered by services that pass on the engine.

### Phase 7: declared but unreached (about 3 days, about 120 services)

Report §3 (the ○ rows), §8 (unused joins) and §10 item 9.

- **`DenseMapping`.** Run the flat mapping's queries through `stress::DenseMapping` as well, with each pair an
  invariance group. That is the mapping's stated purpose: 60 class mappings, 120 `scope` blocks and 711 property
  mappings. It also covers relational-main-table-alias.
- **Union, merge and inheritance operations, and the literal-list mappings.** The merge operations are in
  `74-surface-last.pure`, which lite loads. The router union and the inheritance operation move out of
  `71-mapping-surface2.pure`, which lite excludes for an unrelated M2M construct, into a file it loads. Each then gets
  services.
- **The 32 unused joins.** Each gets a mapping and a service, or is deleted. Decoration is what `executed.py` was
  written against.
- **Done when:** at most 2 features remain ○, and every join is used.

### Phase 8: constraints and test shapes (about 4 days)

Report §5, §6 and §10 items 5, 7 and 11.

- **Constraints that combine.** About 15, comparing:
  - a date against a date, or an enumeration value
  - a string through `startsWith` or `length`
  - to-many properties through `exists` or `forAll`
  - an `if`, or a navigated property

  Each gets seed rows that violate it, and graph fetch services that check it (`graphFetchChecked` if the pinned
  release runs it, probed first). The oracle evaluates the constraint over the fetched graph.
- **Mapping test suites.** About 50, each mirroring an existing service with the same query, data and expected
  result, so the oracle computes nothing new. The engine runs them. Lite needs a mapping-test runner first
  (`docs/DEFERRED_TEST_EXECUTION.md`), which is product work outside this plan.
- **Test shapes.** Add suites with several tests, tests with several assertions, and `EqualTo` and `EqualToTDS`
  assertions, on a few dozen existing services. Lite skips `EqualTo` today.
- **Done when:** the constraint and mapping-suite rows reach their targets.

### Phase 9: store breadth (about 3 days)

Report §3 (Store) and §10 item 10.

- **Semi-structured data.** Seed the two declared semi-structured columns with JSON, and add navigation, flatten and
  array-index services. Probe on the engine's H2 test database first, since EMIT's semi-structured models may target
  other databases.
- **Outer joins.** Full and right outer joins in property-mapping join chains.
- **Done when:** every store feature the pinned engine runs is covered.

## Not in this plan

- **Persistence** (12 EMIT features): lite has no persistence runtime.
- **Hosted services, Snowflake apps, BigQuery functions, file and model generation, and schema sets:** a test suite on
  a local database can't judge any of them.
- **Legacy mapping and service tests:** the deprecated test forms. The report's ninth absent execution feature,
  serialization-date-time-format, is not in the pinned release's taxonomy.
- **Service stores and flat-data stores:** lite has neither.
- **Performance and scale benchmarks:** a separate concern. The `#SQL` corpus in `core/src/test/resources/sql-stress/`
  covers scale for SQL islands.

## Lite work this plan will surface

None of this is corpus work. It is listed so that each failing or skipped service has somewhere to go:

- Measure and Unit types (`29-money.pure`, `55-canonical-store.pure`).
- The M2M explosion and local-property collision that lite's normalizer refuses (`71-mapping-surface2.pure`,
  `75-surface-gaps.pure`).
- `ServiceTestRunner` gaps:
  - provisioning across several stores (10 skips)
  - multi-execution services (2)
  - ExternalFormat test data (1)
  - `EqualTo` and `Relation` assertions
  - runners for mapping tests and function tests
- The new failures each phase finds, recorded by bucket in [Progress](#progress).

## Order and size

```
Phase 0 ─┬─ Phase 1 ── Phase 3
         ├─ Phase 2
         ├─ Phase 4
         ├─ Phase 5
         ├─ Phase 6
         ├─ Phase 7
         ├─ Phase 8
         └─ Phase 9
```

**Recommended order:** 0, 1, 2, 3, 4, 7, 5, 6, 8, 9.

- Phases 1 and 2 add the most power to detect wrong answers per day of work.
- Phase 7 is cheap.
- Phases 5 and 6 depend most on what lite supports.

**Size.** About 45 working days in all. The corpus grows from 4,742 services to about 5,700. That adds about a fifth
to gate 10's time and about 10 minutes to a full engine run.

**Risks.**

- **The oracle grows.** `oracle.py` is 2,747 lines, and every extension is another place to be wrong. Rule 3 is the
  check: a disagreement is adjudicated, never resolved by copying an engine's answer.
- **The wrong-rows tool's damaged data follows the seed.** `tools/wrongrows/damage.py` derives it from the seed, so
  Phase 2 changes it too; re-run the tool after Phase 2.
- **Generated files are already large.** `94-fanout-services.pure` has 291,278 lines, so new generators write files of
  their own rather than growing it.

## Progress

| Phase | Started | Landed | Services added | Quarantined on the engine | New lite failure buckets | Measures after |
|---|---|---|---|---|---|---|
| 0 | 2026-10-21 | — | 0 | none added; F27's entry removed (fixed upstream); F60's 3 unexpected settled by D6 (a) | none | EMIT 43 of 109 on the engine, 37 in lite; PCT 64 of 321 |
| 1 | — | — | — | — | — | — |
| 2 | — | — | — | — | — | — |
| 3 | — | — | — | — | — | — |
| 4 | — | — | — | — | — | — |
| 5 | — | — | — | — | — | — |
| 6 | — | — | — | — | — | — |
| 7 | — | — | — | — | — | — |
| 8 | — | — | — | — | — | — |
| 9 | — | — | — | — | — | — |
