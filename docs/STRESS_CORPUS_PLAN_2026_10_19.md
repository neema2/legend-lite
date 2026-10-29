# Stress corpus improvement plan

Proposed 2026-10-19. The user took the five decisions on 2026-10-21 ([Decisions](#decisions)), and Phase 0 started
that day (`docs/IN_FLIGHT.md`, the twelfth line). Phase 1 started and was done on 2026-10-23, on `stress_test/corpus`.
Phase 2 started and was done on 2026-10-27. Phase 3 started and was done on 2026-10-28, and Phase 4 started that day
and was done on 2026-10-29, in six stages (see its record).
This plan turns the gaps in the [stress corpus feature report](stress-corpus-feature-report-2026-09-29/README.md)
(§10) into ten phases of work. Each phase lists the files it changes, how its expected results are computed, roughly
how many services it adds, and the report numbers that say it is done. A session that starts a phase adds a line to
`docs/IN_FLIGHT.md` first.

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
- **D7, how the oracle compares an absent value (raised 2026-10-23 by Phase 1).** Phase 1 asked for predicate
  trees "under three-valued logic", and the oracle followed SQL: a comparison with a NULL operand was UNKNOWN, and
  a filter kept only TRUE. Before writing a tree, Phase 1 probed `!`, `||`, `in` and `!=` over NULLs on the pinned
  engine, and checked legend-pure. legend-pure defines it otherwise: an ordered comparison over an absent value is
  false (`greaterThan(left:Number[0..1], right:Number[1])` is `$left->isNotEmpty() && ...`, documented "a missing
  value never satisfies the comparison"), and `==` is collection equality, so `[] == 'x'` is false and `[] != 'x'`,
  its negation, is true. Two-valued, and `!` is plain negation. legend-engine lowers it so, except in F64 and F65;
  legend-lite too (`NullSemantics`, after the engine's `processNotEqual`). The two readings differ only on `!=` among
  the flat comparisons, which is what F28 had recorded as an engine defect ("`>` should not be excluding it either")
  -- under Pure's definition `>` excludes and `!=` keeps, so F28 was the oracle's error. Under rule 3 ("or the oracle
  is wrong and gets fixed"), with legend-pure as the spec: **the oracle compares as legend-pure defines**
  (`oracle._cmp`), a store's join conditions and Filters, which are SQL, keep SQL's rule (`oracle._sql_cmp`), F28
  is withdrawn, and `CB_NotEqualsNull` is out of quarantine (it passes on the engine and in lite). **Taken by the
  Phase 1 session on that evidence; the user may reverse it.** Reversing it is one function, and the generated
  expectations follow.
- **D8, the oracle's group-by `sum` over nothing (raised 2026-10-29 by Phase 4, stage 3; reversed by the user the
  same day).** The oracle's group-by `sum` followed SQL, NULL over a group whose values are all absent, while its
  `IMPL["sum"]`, which the probes use, was 0. Stage 3 read legend-engine's Pure, `sum` as `$numbers->plus()` and a
  relation groupBy's map dropping an absent value, as the group reducing an empty collection: it made the group-by
  `sum` 0 over nothing, moved 24 deep-stack grouped totals from null to 0, and recorded the engine's NULL as F75.
  The review the user asked for found that premise wrong. `groupBy` is a native, and both of legend-engine's Pure
  runtimes (the compiled `RelationNativeImplementation.performMapReduce`, the interpreted `AggregationShared`) drop
  the absent values and answer nothing for a group with none left, never handing the reducer an empty collection.
  PCT `test_Extend_Filter_Select_Pivot_GroupBy_Extend_Sort` asserts it for `plus()` (`UK,3000,null`), and neither the
  core manifests nor DuckDB's exclude it; the legacy TDS groupBy's body does the same (`tds.pure`: `if
  ($vals->isEmpty(), | ^TDSNull(), ...)`). **Decided (the user, 2026-10-29): reversed, with `count` kept at 0.** A
  group whose values are all absent has an absent aggregate, `sum` and `joinStrings` included, as SQL's are; its
  `count` is 0, Pure's own `count` of nothing, which SQL and both engines answer and no PCT contradicts (the natives'
  shortcut would make it absent too). F75 is withdrawn: its 28 services left quarantine and pass on the engine.
  `oracle._reduce_group`, `_group` and `_rel_group` hold the rule.
- **D9, `splitPart`'s index base (raised 2026-10-29 by Phase 4, stage 5).** `splitPart` is legend-engine's Pure
  function, `$str->toOne()->split($token)->at($part)`, zero-based (its PCT: `'Hello World'->splitPart(' ', 0)` is
  `'Hello'`). In a query the pinned engine converts it (`processSplitPart`: "sql is 1 index based, pure is 0 index
  based"), and a probe answered zero-based. A relational property mapping's `splitPart` is the dynafunction, which
  every dialect extension writes verbatim as SQL's one-based `split_part`, and lite reads it so (`RelOpTranslator`,
  part - 1); the oracle had one, one-based, reading, which 12 cells of the combination matrix assert, and the engine
  and lite pass them. The options were two readings by context, Pure's everywhere (substring's precedent: the matrix
  cells move and `split` leaves `combos.XFORM`), or splitPart left out of queries. **Decided (the user, 2026-10-29):
  two readings.** The dynafunction keeps SQL's one-based reading in a property mapping, so no expectation moved, and
  a query's `splitPart` is Pure's (`oracle.QUERY_IMPL`). Under that reading F37's property-mapping half, `substring`'s,
  is the same case: **the user took the same reading for it (2026-10-29)**, which withdrew that half of F37 (Phase
  4's record, after "Phase 4 done").

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
    comparisons. (Done under legend-pure's two-valued rule instead: D7.)
  - Service parameters bound by name: at `[1]`; at `[0..1]` in the optional-filter form
    `$p->isEmpty() || $x.v == $p->toOne()`; and at `[*]` as `$x.v->in($ps)`.
- **Emitter** (`emit.py`). The service function's parameter list, and the test's `parameters:` block.
- **Generator.** A new `literals.py`, called from `build.py`, writes a new generated file,
  `99-typed-services.pure`, added to `STRESS_GENERATED` in `core/stress.bzl`.
  - The comparison matrix covers the nine ordered types with a literal (String, Integer, Float, Decimal, Number, Date,
    StrictDate, DateTime, StrictTime) under `==`, `!=`, `<`, `<=`, `>` and `>=`, plus Boolean under `==` and `!=` and
    the enumeration under `==`, `!=` and `in`. Each comparison appears at the root and one hop away, as a literal
    service and as a parameter twin with the same expected result. That makes 236 services. (StrictTime has no
    ordering in legend-pure, so 220: see the record below.)
  - About 40 services combine `or`, `and` and `not` over nullable columns, where three-valued logic decides the
    answer.
  - About 20 `[0..1]` and `[*]` parameter services each have two tests: value given and value absent, or one value
    and several.
- **Lite.** `ServiceTestRunner` already binds parameters as let-bound variables. Under D4 the runner keeps its
  `stress::` filter, so the new services are `stress::` services.
- **Done when:** the parameter, literal, enumeration-literal and `or`/`in`/`not` rows reach their targets, and every
  new service passes on the engine or is quarantined with a finding.

**What was built (2026-10-23).**

- `8112-typedvalues.pure`: the enumeration `typedvalues::Grade` (mapped from codes, one code unmapped), the class
  `typedvalues::TypedValue` with a property of every primitive type that has a relational column at `[1]` and at
  `[0..1]`, its own store (`typedvalues::TypedStore`, included by `store::DB`), a mapping included from
  `89-all-mapping.pure`, and a to-one association from `trading::Trade` (seven of the twenty trades have no typed
  value). StrictTime and Byte have no relational column type in the grammar; they read a VARCHAR and a TINYINT and
  live on a sibling class over the same rows, `typedvalues::TimeAndOctet`, because lite refuses a StrictTime over a
  VARCHAR when it compiles a class mapping, and on `TypedValue` that refusal failed every service over it.
- `seed.TYPED_VALUE`: fifteen rows written by column. Every comparand has a tie and both neighbours (42 between 41
  and 43; noon with a second either side; 2024-02-29 between month ends), with negatives, zero, INTEGER's and
  TINYINT's extremes, strings that differ by case, carry an apostrophe or end in a space, Decimals a double cannot
  tell apart (`1.0000000000000000`, `…0001`, and `1.000000000000000001`), and an unmapped grade code. One row is
  absent in every optional column; otherwise each optional column has NULLs of its own, so a predicate over two of
  them meets one absent side as well as both.
- `query.py`, `oracle.py`, `emit.py`: DateTime, StrictTime, Decimal and enumeration literals; predicate trees
  (`And`, `Or`, `Not`, `in`, `isEmpty`); service parameters at `[1]`, `[0..1]` (the optional-filter form) and `[*]`,
  each test binding its own values and carrying its own expected result; Decimals rendered digit for digit, since
  the engine's JSON assertion compares numbers by value.
- `literals.py` writes `99-typed-services.pure` (added to `STRESS_GENERATED`): 306 services. The comparison matrix
  is 220 (every operator each type has, at the root and one hop away, each as a literal and as a parameter twin
  that must return the same rows: `TL_`/`TP_`); 40 predicate trees (`TQ_`); 20 enumeration services (`TE_`: each
  value under `==` and `!=`, an `in` over three, and `||` of two, at both sites); 10 optional-parameter services
  with a given and an absent test (`TO_`); 10 to-many parameter services with several values and one (`TM_`); the
  Decimal-scale pair (`TL_`/`TP_AmountScale18_Eq_Root`); and four projection controls (`TV_`). `literals.check()`
  fails the build when a comparand keeps all of a root's rows or none, when a control or an absent parameter
  filters anything, or when fewer than 20 trees turn on how an absent value compares. The literal/parameter pairs,
  and `TE_PrimeOrWatch_*` with `TL_Grade_In_*`, join the invariance check.
- The pack is hidden from the generators that walk the model (`literals.FIXTURE_TABLES`, `literals.walked`), in
  `build.py`, `executed.base_specs`, `differential.py` and `quarantine.check_f24` alike. Walked, it displaced five
  `DS*_CustodiedCryptoHolding` services under the generators' caps and changed seventeen dense services reaching it
  from a trade; hidden, regenerating changed no existing service but `CB_NotEqualsNull`'s expectation and the data
  element.
- `executed.py` holds nine new features (rule 6): parameters at each multiplicity, Decimal, DateTime and
  enumeration literals, and filters using `||`, `!` and `in`; all nine are executed by passing services.
- `tools/engine-runner`: an ERROR line carries the exception's causes, since a suite that fails to start names its
  reason only there (announced in `docs/IN_FLIGHT.md`).

**Found on the way.** Probed on the pinned engine before the oracle was written, then confirmed by the services:

- **D7 (below): the oracle compared an absent value as SQL does, and legend-pure does not.** F28 is withdrawn.
- **F61:** a Decimal literal is cast to `DECIMAL(32,16)`, so one with 18 fractional digits is rounded before it is
  compared: a wrong answer. The parameter twin is bound exactly and passes.
- **F62:** a `Number` service parameter fails with a NullPointerException (Number is not an external parameter type;
  the fall-through reads a context the test runner never sets).
- **F63:** a StrictTime literal or parameter in a relational query fails plan generation; projecting one works.
- **F64:** inside `!(a || b)` or `!(a && b)`, an `==` or `in` over an absent value reaches SQL bare and reads as
  UNKNOWN, so the row is dropped where Pure keeps it: a wrong answer, and inconsistent with the engine's own `!(a ==
  x)`, which is right.
- **F65:** an enumeration parameter under `!=` or `!in` drops the absent rows that the literal keeps: a wrong answer.
- A partial-date literal (`%2024-06`, the only literal whose type is `Date`) fails in relational SQL ("Date has no
  day"), and Pure's comparison of dates of different precision is not one the oracle should guess, so no service
  uses one.
- Each is minimized under `scripts/corpus/repro/` (`decimal-literal-scale`, `number-parameter`,
  `stricttime-relational`, `negated-composite-equality`, `enum-parameter-negation`) and recorded in
  `docs/UPSTREAM_FINDINGS.md`; `repro/not-equals-null/` now asserts Pure's reading and passes.

**Corrections to the targets.**

- The literal-type row's 11 of 12 cannot be reached at the pin: Number has no literal syntax either (as Byte has
  none), `Date`'s own literal fails in relational SQL, and StrictTime's fails plan generation (F63). The reachable
  count is 8 of 12 passing on the engine (StrictTime's literal is written, and quarantined).
- StrictTime is not ordered in legend-pure (no `greaterThan` or `lessThan` takes one), so the matrix has eight
  ordered types, not nine, and 220 services, not 236.
- "Every type with a literal syntax at `[1]`": the engine accepts nine (String, Boolean, Integer, Float, Decimal,
  Date, StrictDate, DateTime, and an enumeration); Number (F62) and StrictTime (F63) are written and quarantined.

**The engine verdict (2026-10-23, legend-engine 4.145.0, Windows x64).** The whole corpus (`bazel run
//scripts/corpus:run -- --jobs 4`, 88 minutes): 4,973 pass, 58 known-fail (quarantined), 1 not run (F15, which
hangs), 0 unexpected, of 5,032 testables. The ten enumeration services written after it (`TE_*_Ne_*` and
`TE_PrimeOrWatch_*`, below) ran alone and pass, so 5,042 in all. No pass of Phase 0's baseline is lost;
`CB_NotEqualsNull` passes (F28 withdrawn), and so do `CB_C0`, `CB_C2` and `CB_C4` (D6 (a)). Of the 306 typed
services 279 pass and 27 are quarantined: F61 1, F62 12, F63 8, F64 3, F65 3.

**The measures** (the report's `tools/run.sh` at the pin; each counts services that pass on the engine with a
populated expected result):

- Service parameter types: nine at `[1]`, at `[0..1]` and at `[*]` alike (String, Boolean, Integer, Float, Decimal,
  Date, StrictDate, DateTime and the enumeration), over 112 services. Every type the engine accepts as a parameter
  is covered; Number (F62) and StrictTime (F63) are written and quarantined.
- Literal types in service queries: 8 of 12 (DateTime and Decimal added). The target is corrected above.
- Enumeration literals in queries: 33 services. The first commit made 23, so it added ten more: each value under
  `!=` at both sites, and `PRIME || WATCH` at both sites, the twin of `TL_Grade_In_*`.
- Filters using `or` / `in` / `not`: 29 / 25 / 27. `profile.py` counts a `!=` as `not` and reports 76; the 27
  are filters with an explicit `!`.
- EMIT 43 of 109 on the engine and 37 in lite, as before: the EMIT features Phase 1 reaches (service parameters,
  enumerations) were reached already. PCT names 66 of 321 (64 before: `lessThanEqual` and `toOne`).
- The measure needed one fix on `stress_test/report`: `expectations.py` read a test only when its first key was
  `serializationFormat` or `asserts`, so a test opening with `parameters:` had no expected result. Before the fix
  the parameter row counted 10 of the 112 services, the `TO_` services by their `absent` tests, which bind
  nothing.

**Lite.** On DuckDB 4,849 tests pass (4,705 before: 143 typed tests and `CB_NotEqualsNull`, which now expects
Pure's reading); on H2 4,820 (4,676 before). No previously passing test fails. Both `MIN_PASS` ratchets moved by
that, in two steps, each with a dated note. Lite's 183 failing typed tests fall into five buckets:

- **Parameters (151 tests: every `TP_`, `TO_` and `TM_`).** `ServiceTestRunner.body()` unwraps the service's lambda
  only when it takes no parameters, so a parameterized query runs as `let p = …;` followed by the lambda itself, and
  the store resolver reports `getAll(...) unresolved`. The plan's "already binds parameters" holds for the binding,
  not for the body.
- **StrictTime over a VARCHAR (6: `TL_Cutoff_*` and the two `TV_TimeAndOctet_*` controls).** Lite's
  class-mapping check refuses `cutoff: StrictTime` read from a VARCHAR (`expected StrictTime, got String`); the
  engine accepts it, and the grammar has no TIME column to use instead.
- **A trailing space trimmed (20).** TV-04's name `'alpha '` reads as `'alpha'` in lite on both databases, so
  every comparison on it, and both projection controls, differ.
- **`!` over a navigation that lands on nothing (3).** `!($x.typedValue.quantity > 42)` drops the seven trades with
  no typed value (`TQ_NotGt_Hop`, `TQ_NotLe_Hop`, `TQ_NotAnd_Hop`); the engine keeps them.
- **F64's shape (3).** `TQ_NotOr_Root`, `TQ_NotOr_Hop` and `TQ_AndNotOr_Root` lose the rows the engine loses: lite
  follows the engine's lowering there, not legend-pure's definition.

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

**What was built (2026-10-27).**

**The cause, traced first.** It was not depth. 394 of the 400 `DSDeep` services expected no rows, and of 397
four-level trees 16 had an object at the bottom.

- 360 of the `DSDeep` roots were taxonomy subtypes the seed holds no row of. Ranked by reach alone, the first 400
  navigable roots were mostly those, and each of their services asserted the `~filter` and nothing deeper, which the
  taxonomy service over the same subtype already asserts.
- Of the chains that did start from rows, 362 broke at one hop, business unit to legal entity. `BUSINESS_UNIT`, an
  extension table the expansion derives from `DESK`, has two rows: `BU-EQ` with no legal entity (A2) and `BU-CR` with
  `LE-0001-GONE` (A1). Both breaks are deliberate, and there was no third row whose chain held.
- Chains through a trade's order broke at once: `TRADE.ORDER_ID` is absent in every row.
- Every hop these chains cross is a foreign key into a primary key, a self-join's included, so two moves cover them.

**The pieces.**

- **`spines.py`, a new seed pass.** The plan placed it in `seed.py`. It is called from `flat.all_tables` instead,
  after the expansion, because the chains run through expanded tables.
  - Its targets are the 722 chains the generators read, each generator exposing its own (`spine_targets`):
    `deepstack.py` 400, `stacks.py`'s dense services 206, `graphs.py`'s trees 116.
  - Two moves. RELINK points a row's foreign key at another parent that completes the chain. CLONE copies a row under
    a new key (`-SP1`), linked onward, for a parent to point at, where every row of a table is load-bearing.
  - Each chain wants three complete paths (`WANT`), not the plan's two, and two distinct values where the data has
    them. A filter that keeps between a fifth and four fifths leaves a limit only what it kept, and a limit needs two
    rows to cut.
  - Chains are taken most constrained first, and the links of a satisfied chain's witnesses are frozen: a later move
    may point such a key only at a parent that gives the witnessed chain the same value. The first version had no
    freeze, and re-pointed one business unit's legal entity 5,818 times, each chain wanting its own value through the
    same row.
  - Result: 508 relinks and 11 clones (`DESK` 3, `BUSINESS_UNIT` 3, `SALES_PERSON` 2, `BOOK`, `PORTFOLIO` and
    `CLIENT_COVERAGE` 1 each), in 65 tables.
  - 668 of the 722 chains can be spined. Of the rest, 43 dense chains end on a derived property, 9 chains cross a
    join with a general condition, and 2 trees cross a join into a column that is not the target's key.
  - Of the 668, 30 `DSDeep` chains and 6 dense ones reach 2 of 3 paths, 2 dense ones none, and 6 trees 0, 1 or 2;
    every other chain has its three. The populated rows below count what the services then expect, which is what
    matters.
- **What the pass never does,** each proved in the build by `spines.check()` (reported with the seed's checks):
  - remove an absent (A2) or dangling (A1, A11) key, or the last parent with no children (A3), that the seed placed;
  - touch a primary key, invent a leaf value, add a row to the root of the chain it is completing, or edit a column a
    derived table reads (`TRADE_FLAT`, the partitions, the pre-aggregate: `check()` recomputes them from the spined
    tables);
  - move a parent's last child away over a to-many a service counts: a battery service, or one of `aggregates.py`'s
    (`aggregates.counted`, read from the seed before the pass). The first full engine run found this. The pass had
    moved trader TRD-001's one daily P&L to TRD-002, so `F5_TraderChildCounts` expected 0 for TRD-001 and met F6 (a
    count over an empty to-many is 1). And three aggregate services had left the corpus, since `aggregates.py` takes
    only ends whose every parent has children. Two are back. `AA_SalesPersonCounts` stays out: a clone is a new
    parent with no children, and one of the two sales-person clones has no sales credit. Holding every parent to a
    child instead left 49 `DSDeep` chains short of three paths, against 30.
- **`selective.py` (D5).**
  - A `> ' '` guard becomes a comparison against a value of its own column under `>`, `>=`, `<`, `<=` or `==`,
    keeping between a fifth and four fifths of the rows whose value is present. Each still drops the absent rows, as
    the guard did. A tenth stay as written (`kept()`, by a hash of the service's name), for the guard's test of an
    outer join's NULL.
  - Of the 2,116 generated services with a filter, 1,716 compare against values only, 249 keep their guard as the
    kept tenth, and 151 keep one because no comparand leaves the result populated. Guards fall from 2,166 to 417.
  - A limit after a sort becomes about half the rows the query returns without it, moved down past a tie the oracle
    refuses. 1,614 queries carry a limit. 116 still read `limit(25)`, where the result has fewer than two rows or no
    smaller limit keeps it populated.
- **`deepstack.py`.**
  - The deep variants root only at classes whose `all()` sees rows. A root with fewer than two rows ranks last, and
    a root with none gets no variant. So 105 roots' `DSDeep`, `DSPost`, `DSGroup`, `DSSub` and `DSTree` services
    were replaced by 105 others.
  - `DSDeep` projects its local column. `DSPost` takes `DSDeep`'s comparands and limit, so each pair is now a
    checked invariance (`deepstack.invariance_groups`, through `build.py`'s `_invariant`). Before, the two carried
    different conjuncts, and nothing compared them.
- **`emptiness.py` (rule 4).** A spec states its family (`Spec.emptiness`). `build.py` fails on an empty or hollow
  expected result outside a family, and on a declared one that has rows, so a declaration cannot outlive its reason.
  Its classification is the report's own (`tools/expectations.py`). The families:
  - an empty subtype (`taxonomy.py` and `hier.py`'s bare projections);
  - before the first version (`MD6_RevisionAsOf`, `SC2_ScheduleAsOf`, `PL8_RatingsAsOf`);
  - excludes everything (`CB_EqualsNull`).

  The plan's fourth family, `isEmpty` ends, has no member: `tomany.py` projects its test as a column, so those
  services expect rows of booleans.
- **Rule 6.** Phase 2 makes no feature newly executable: filters against values, limits and four-hop navigation all
  ran before. So `executed.py` gains no predicate, and the new properties are held by `emptiness.check()` and
  `spines.check()` instead. (`executed.base_specs` does not include `deepstack.py`'s services, and that is left as
  it was.)
- **`quarantine.py`.** F58's two `DSLocal_` entries are removed and nothing is added. `DSLocal_MiddleofficeConfirmation`
  and `DSLocal_RegulatorySubmission` now filter on a real value with a limit that cuts, so neither keeps a row on
  which the two `dateDiff` readings differ, and both pass on the engine. F58 stays witnessed by `MO2_Confirmations`
  and the two `REGX_` services. One file outside the owned ones changed:
  `repro/datediff-hours-off-boundary/README.md`'s list of where F58 shows, announced in `docs/IN_FLIGHT.md`.

**The engine verdict (2026-10-27, legend-engine 4.145.0, Windows x64).** The whole corpus ran three times, since the
seed changed (rule 3):

- First run: 5,000 pass, 56 known-fail, 1 not run (F15), and 3 unexpected, of 5,060 testables. One was a regression
  (`F5_TraderChildCounts`, above). The other two were `DSLocal_` services that F58 no longer caught.
- After the counted-to-many rule: 5,003 pass, 0 unexpected.
- After the aggregate ends were added to that rule (`bazel run //scripts/corpus:run -- --jobs 4`, 77 minutes): 5,005
  pass, 56 known-fail (quarantined), 1 not run (F15, which hangs), 0 unexpected, of 5,062 testables.
  `F5_TraderChildCounts` passes, `F3_DeskChildCounts` stays known-fail under F6, and the two aggregate services that
  came back, `AA_BusinessUnitCounts` and `AA_MTDPnLCounts`, pass.

No pass of Phase 1's is lost: every service that passed then and is still in the corpus passes. 527 services left the
corpus and 547 came in, and all 547 pass on the engine. 525 of those that left are the replaced deep roots' variants. The other two are
`DSLocal_Portfolio`, which filtered on a column absent in every row and expected nothing (a deep variant's local
column must now hold a value), and `AA_SalesPersonCounts`.

**The measures** (the report's `tools/run.sh` at the pin, given the engine's pass list; "on the engine" means a service
that passes there with a populated expected result):

- Filters comparing against a real value: 2,040 comparisons in 1,920 services, and 2,023 in 1,908 on the engine
  (target 1,500). The report counted 14. With `tools/filter_values.py`, Phase 1's corpus has 198 comparisons in 170
  services (181 on the engine), most of them Phase 1's typed literals. Guards (`> ' '`) fall from 2,166 to 417, in
  401 services. Comparisons against a parameter stay at 119, 101 on the engine.
- Four-join projections with a value at the bottom: 1,553 of 1,553, every one passing on the engine (target 1,400;
  10 before).
- Four-level graph fetch trees populated at the bottom: 397 of 397, every one passing on the engine (target 360; 16
  before).
- Empty expected results: 572 of 5,082 tests, each in a declared family (target at most 900; 2,200 of 5,062 before).
  None is hollow, against 280 before. The 572 are 568 empty subtypes (566 `taxonomy.py`, 2 `hier.py`), 3 before the
  first version and `CB_EqualsNull`.
- Navigating queries with an expectation: the deepest level is populated in 2,327 of 2,340.
- EMIT 43 of 109 on the engine and 37 in lite, and PCT names 66 of 321, as before: Phase 2 changes data and
  comparands, not features.
- The measure needed tools on `stress_test/report`. `tools/filter_values.py` is new: for each comparison in a
  query's filter, whether it compares against a value, the `' '` guard or a parameter, counted overall, among lite's
  passes and among the engine's. `deep_populated.py` takes the engine's pass list too, and `run.sh` passes it to both.

**Lite.** On DuckDB 4,869 tests pass (4,849 before); on H2 4,840 (4,820). 527 passing tests left the corpus with
their services, the 547 that came in pass on both databases, and no test that passed and is still in the corpus
fails. Both `MIN_PASS` ratchets rose by 20, each with a dated note. `//scripts/corpus:all` and
`//core:corpus_differential_test` pass.

**Not done.**

- The wrong-rows tool (`tools/wrongrows/`) was not re-run. Its damaged data follows the seed, as the Risks note says,
  and it needs engine runs in rows mode.
- The 54 chains `spines.py` cannot reach are listed above. They are dense chains ending on a derived property, chains
  across a general-condition join, and two trees across a non-key join.

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

**The probes, first (2026-10-28, legend-engine 4.145.0, Windows x64).** Before any service, 41 probe services over
a scratch model (a processing-temporal class with `TIMESTAMP` columns and a DateTime infinity date, a bitemporal one
with `DATE` business and `TIMESTAMP` processing columns, a non-temporal class navigating to both) asked what the
pinned engine does. It does what the oracle now computes, with four exceptions:

- A version is `[in, out)` at a time of day: at the instant of a change the new version answers. A StrictDate where
  the columns are `TIMESTAMP`s reads as its midnight. `%latest` selects the DateTime infinity date. `all($p)` takes a
  DateTime, a StrictDate or a Date parameter. A milestoned property takes a DateTime literal, a parameter, or a date
  read from the row (`$x.limit($x.updatedAt->toOne())`). A property with no date of its own takes its parent's
  (date propagation). Sub-second boundaries hold. `allVersions()` reads every version, and on a navigation its count
  and `isEmpty()` are right.
- **`Class.allVersionsInRange(...)` does not parse.** The engine's grammar refuses it on a class
  (`DomainParseTreeWalker.allOrFunction`: "is not supported"). The function form, `Class->getAllVersionsInRange(a,
  b)`, runs: a version overlaps when it starts at or before the end and ends after the start, as legend-engine's
  own range tests assert. The property form, `$x.limitAllVersionsInRange(a, b)`, parses and runs.
- **A connection's `timezone` cannot be tested** (F67). The service test runner rebuilds the connection from the
  store and its test data, and the zone is not copied, so a DuckDB connection declaring `America/New_York` reads and
  compares exactly as one declaring none. No service depends on a time zone.
- **A zoned timestamp column has no type at the pin.** `RelationalDataType` has `TIMESTAMP` and `DATE` only. It waits
  for the bump (D3).
- A graph fetch keys a dated milestoned property with its date: `limit(2024-06-03T09:30:00+0000)`. The first run
  showed a propagated one keyed the same way, with the date it took.

**What was built (2026-10-28).**

- `8113-intraday.pure`, a hand-written pack with its own store (included by `store::DB`) and mapping (included from
  `stress::AllMapping`):
  - `intraday::BookLimit` and `intraday::BookStatus`, milestoned on processing time alone, over `TIMESTAMP` columns
    with a DateTime infinity date.
  - `intraday::MarkRevision`, bitemporal: business time a `DATE` and processing time a `TIMESTAMP`.
  - Two classes that are not milestoned and navigate to them: `intraday::LimitCheck`, a pre-trade limit check
    (`$x.bookLimit($x.checkedAt)`, the limit in force when it ran), and `intraday::ValuationRun`
    (`$x.mark($x.runAt, $x.valuationDate)`, both dates from the row).
  - `BookLimit.status`, processing-temporal to processing-temporal, where a date propagates.
  - The plan had a bitemporal table with `TIMESTAMP` processing columns and a processing-only class. This has both,
    and a second processing-only class for propagation.
- The navigations start at the pack's own classes, not at `trading::Trade`. The first version gave Trade the two
  milestoned associations, and the engine stopped compiling the corpus: a milestoned property on a class breaks
  the compile of an XStore mapping on it, and `external::EntityMapping` is one (F66).
- `seed.py`, five tables. Book limits change at 14 instants:
  - twice within one day and once exactly at midnight;
  - in the last second of a day;
  - with an hour's gap, and withdrawn with no successor;
  - one book never had a limit.
  
  Limit checks run a second before a change, at it, in the gap, after the withdrawal, and against no book (A2) and a
  missing book (A1). Marks are published at 17:30, made official minutes later and restated at midnight, and one
  lives for one second. `seed.check()` holds each shape (A19) and proves that no key's versions overlap, so every
  navigation is to-one.
- The oracle (`oracle.py`, `query.py`, `emit.py`):
  - Milestones are compared as instants. A DateTime against a `DATE` column is refused, since it was not probed.
  - `%latest` reads the table's own infinity date.
  - All-versions roots and version ranges.
  - Root dates bound from parameters.
  - A navigation into a milestoned class takes each hop's date: literal, parameter, read from the row, or
    propagated, as `query.Hop` carries it. Before, a hop onto versioned rows was refused as a fan-out.
  - Aggregates over `AllVersions` and `AllVersionsInRange` ends.
  - Graph trees with dated properties.
  - The emitter refuses `%latest` beside another date in a property's arguments. Pure's grammar takes it there only
    when every argument is `%latest` (M3's `functionExpressionLatestMilestoningDateParameter`). One such service
    stopped the whole corpus loading on the engine.
- `intraday.py` writes `99-intraday-services.pure` (`STRESS_GENERATED`), 90 services:

  | Family | Services | What they read |
  |---|---|---|
  | `ITL_` / `ITP_` | 27 / 14 | the book limits one second before and at every change instant; at each, the same instant as a DateTime parameter |
  | `ITS_` / `ITD_` | 4 | StrictDates (literal and parameter) and a Date parameter beside the DateTime they mean |
  | `ITL_Latest`, `ITL_Year2099` | 2 | `%latest`, and a far instant that must agree |
  | `ITV_` / `ITR_` | 7 | `allVersions()`, and four ranges as literals and one as parameters |
  | `ITN_` | 14 | dated navigations from checks and runs |
  | `ITX_` | 7 | date propagation, beside the same navigation dated explicitly |
  | `ITM_` | 13 | the bitemporal marks |
  | `ITG_` | 2 | graph fetch with a dated property, and with a propagated one |

  Each literal and its parameter twin, each StrictDate and the DateTime it means, and each propagated date and the
  explicit one join the invariance check. `intraday.check()` fails the build when a reading a second before a change
  equals the reading at it, or when fewer than 20 services read at a time of day.
- The pack is hidden from the generators that walk the model (`literals.FIXTURE_TABLES`): regenerating changed no
  existing service, only the data element.
- `executed.py` holds nine features (rule 6), each executed by passing services:
  - a processing-temporal root
  - a DateTime milestoning date
  - a milestoning date parameter
  - `allVersions()`
  - `getAllVersionsInRange`
  - a dated navigation
  - a date read from the row
  - an `AllVersions` navigation
  - date propagation
- One empty expected result, `ITM_BeforeFirst`, is declared before the first version (rule 4).

**Found on the way.**

- **F66:** a class with a milestoned property breaks the compile of any XStore association mapping on it. To find an
  XStore end's class mappings, `PropertyMappingBuilder` searches the class's `_originalMilestonedProperties()` when
  it has any, and `_propertiesFromAssociations()` only otherwise. A compile failure, so one service cannot pin it;
  the repro is `scripts/corpus/repro/xstore-milestoned-property/`.
- **F51 through milestoning:** `isEmpty()` over `$x.bookLimitAllVersionsInRange(a, b)` returns each check once per
  version in the range. The range adds `IN_Z`/`OUT_Z` predicates to the join, so it is not a single key equality.
  `bookLimitAllVersions->isEmpty()` and `count()` over the range are right. `ITN_NoLimitInRange` is quarantined
  under F51; the repro is `scripts/corpus/repro/milestoned-range-isempty/`.
- **F67:** a connection's `timezone` never reaches a service test (above); the repro is
  `scripts/corpus/repro/connection-timezone-test/`.

**The engine verdict (2026-10-28, legend-engine 4.145.0, Windows x64).** The whole corpus (`bazel run
//scripts/corpus:run -- --jobs 4`, 89 minutes): 5,094 pass, 57 known-fail (quarantined), 1 not run (F15, which
hangs), 0 unexpected, of 5,152 testables. Of the 90 new services 89 pass, and `ITN_NoLimitInRange` is known-fail
under F51. Every service that passed after Phase 2 passes.

**The measures** (the report's `tools/run.sh` at the pin, given the engine's pass list; "on the engine" means a service
that passes there with a populated expected result):

- DateTime literals: 66 services on the engine (target 40), of 68 in the corpus; 16 before, all Phase 1's typed
  literals. Lite: 64.
- Milestoned queries at a time of day: 72 on the engine (target 20), of 74; none before. A milestoning date counts
  when it is a DateTime literal, a parameter some test binds to a DateTime, or a DateTime read from the row.
  Milestoned queries of any grain: 100 on the engine, of 105 (16 and 19 before). Lite: 54 at a time of day, 81 in
  all.
- A processing-only class is tested: `BookLimit` and `BookStatus`. The EMIT row processing-temporal counts 91
  services on the engine.
- EMIT 45 of 109 on the engine (43 before) and 39 in lite (37): all-versions-query (4 services on the engine) and
  all-versions-in-range-query (5) are covered now.
- PCT names 66 of 321, as before: `getAll` and `getAllVersions` were counted already, and `getAllVersionsInRange` is
  not a PCT function. Literal types stay at 8 of 12: Phase 3's literals are DateTimes and StrictDates, which Phase 1
  had brought in.
- Phase 2's measures hold: filters against a value 2,042 (2,025 on the engine), and the deepest level populated in
  2,347 of 2,360 navigating queries.
- The measure needed one tool on `stress_test/report`: `tools/milestoned_time.py` (new) counts, for every service,
  whether it carries a DateTime literal, passes a milestoning date (to `all()`, to `getAllVersionsInRange`, to a
  milestoned property, in a lambda or a graph fetch tree), and whether that date is at a time of day; overall, among
  lite's passes and among the engine's. `run.sh` runs it.

**Lite.** On DuckDB 4,939 tests pass (4,869 before), and on H2 4,910 (4,840). 70 of the 90 new services pass on
both databases, and no test that passed before fails. Both `MIN_PASS` ratchets rose by 70, each with a dated note.
The 20 that fail are Phase 1's parameter bucket: every service taking a parameter (`ITP_`, `ITSP_`, `ITD_`,
`ITRP_`, `ITXP_`, `ITMP_`, `ITN_LimitParam0921`) fails with `store resolution left getAll(...) unresolved`. No
new bucket. Lite answers F51's shape correctly (`ITN_NoLimitInRange` passes), and it differs from Pure deliberately
on none of these services, so `docs/SEMANTICS_REGISTER.md` gains no row.

**Not done.**

- The connection time zone and the zoned timestamp column, for the reasons above.
- `.allVersionsInRange(...)` on a class root, which the pinned grammar refuses. The function form stands in.

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

Phase 4 lands in stages, each committed on `stress_test/corpus` when it is green.

**Stage 1: the 23 disagreements, adjudicated (2026-10-28, `0bb5fca71`).** Before any went into a service, each
was put in front of the pinned engine again, twice: in a property mapping, by the probes, and in a query, where this
phase's services use them. Each answer was set beside the function's definition in legend-pure, or in
legend-engine's Pure where the function is legend-engine's. Each disagreement turned out to be an oracle or probe
fix, the engine's error under a finding, or both:

| Function | What disagreed | Verdict |
|---|---|---|
| `substring` | the oracle read one-based with an exclusive end (`'lp'` for `('alpha', 2, 4)`) | oracle fix: legend-pure's zero-based, exclusive end (`'ph'`), as `77-substring-paths.pure` always asserted. At the pin a query's `substring` is Pure's (`pureToSQLQuery`'s new `processSubstr`); a property mapping's is still SQL's, so F37 stands there |
| `regexpIndexOf` | the oracle read one-based with 0 for a miss; the probe asked for group 1 of a pattern with no group | oracle and probe fix: zero-based, -1 for a miss (legend-engine's definition), which the engine answers. And the engine's: F69 (DuckDB finds the matched text's first occurrence, not the match) and F68 (-1 for an absent value through `map`) |
| `regexpExtract` | the probe asked for group 1 of a pattern with no group; the oracle ignored `extractAll` and the group | probe and oracle fix. And the engine's: F70 (no match is `''`, not nothing) |
| `currentUserId` | the oracle answered the OS user | oracle fix: refused. legend-engine leaves the value unspecified (its PCT asserts only that there is one), the engine's relational answer is the database's `CURRENT_USER` (`'duckdb'` in a service test), and a Bazel action must not read an identity |
| `generateGuid` | the probe compared two fresh GUIDs | probe fix: compared by shape, which agrees |
| `hashCode` | the oracle refused (an unspecified value), and the probe counted "no oracle value" as agreement | probe fix: "RUNS; no oracle value". And the engine's: F68 (a hash for an absent value through `map`) |
| `now`, `today` | the probe compared two readings of the clock | probe fix: "RUNS; no oracle value". `today()` renders a StrictDate as a DateTime (F48's addendum) |
| `firstDayOfThisMonth`, `...Quarter`, `...Year` | as above | the engine's: F48, a reflected `ZonedDateTime`, confirmed in a query at the pin |
| `dayOfYear` | 3 for 2024-06-03 | the engine's: F35, confirmed in a query at the pin |
| `adjust`, `firstDayOfWeek` | a DateTime string for a StrictDate | the engine's: F38; the values agree, and `adjust` over a DateTime agrees outright |
| `count`, `map`, `joinStrings` over a to-many | 1, NULL, NULL over an empty one | the engine's: F6 |
| `first`, `sortBy`, `greatest`, `least`, `and`, `or` over a to-many | one row per child | the engine's: F43, not applied; the last four join F43 at the pin |

- **The probes run again** (the Bazel workplan's P7-03, for these two). `probe_functions.py` and `probe_collection.py`
  start the engine through one `run.launch()` and are manual `py_binary`s, `bazel run
  //scripts/corpus:probe_functions` and `:probe_collection`, which refresh their rows of
  `docs/FUNCTIONS_EXECUTED.tsv` in the checkout. The other fourteen probes still name the Maven-era runner.
- **Their bookkeeping, fixed.** The scalar probe counted "no oracle value" as agreement, and credited every transform
  `combos.py` could spell, used or not: `substring`, which `combos.XFORM` leaves out for F37, was recorded as
  disagreeing and agreeing at once. Each disagreement the adjudication settled is recorded with its side and finding
  (`ADJUDICATED` in each probe).
- **Found on the way:** F68, F69 and F70, each minimized under `scripts/corpus/repro/` (`map-over-absent`,
  `regexpindexof-text-position`, `regexpextract-no-match`). F35, F37, F38, F43 and F48 gained dated addenda for the
  pin.
- No generated service uses any of the four functions whose oracle changed, so regenerating the corpus changed no
  file, and lite's ratchets did not move.
- **The engine verdict (2026-10-28, legend-engine 4.145.0, Windows x64).** The whole corpus (`bazel run
  //scripts/corpus:run -- --jobs 4`, 93 minutes, the first run through the refactored launcher): 5,094 pass, 57
  known-fail (quarantined), 1 not run (F15, which hangs), 0 unexpected, of 5,152 testables, as after Phase 3.
- **Lite and the gate.** With the corpus unchanged, lite passes 4,939 tests on DuckDB and 4,910 on H2, as after
  Phase 3. `bazel test --lockfile_mode=error //gates:local //:generated //scripts/corpus:all //core:stress_suites
  //core:stress_suites_h2 //core:corpus_differential_test`: 394 pass and 2 are skipped. 57 fail on this desk alone,
  none depending on a changed file: 54 diff tests (the `update_*` ones and `pins_test`) that `fc.exe` cannot open
  here, each pair identical ignoring CR, and 3 datacube browser tests that time out.

**Stage 2: scalar functions in a query (2026-10-28, `b9f0d212c`).** The first part of the generator, over
`typedvalues::TypedValue` (Phase 1's pack), whose fifteen rows hold a tie on each comparand with both neighbours,
negatives, zero, INTEGER's two extremes, strings that differ by case, end in a space or carry an apostrophe,
Decimals a double cannot tell apart, a leap day, midnight and the last second of a day, and an absent value in every
optional column.

- **`vocab.py`** writes `99-vocab-services.pure` (in `STRESS_GENERATED`, the layout, `core/BUILD.bazel`'s count and
  `trading_model.py`'s `SERVICES_AND_DATA`): 258 services, 86 cases over 76 function names. Each case is three
  services:
  - `VP_`: the function projected over a required property, every boundary value and no absent one;
  - `VN_`: the same function through `map` over the optional property, `$x.opt->map(v|$v->f())`, which is how Pure
    applies a function to an optional value. legend-pure's `map` over an empty collection is empty, so an absent
    value stays absent whatever the function;
  - `VF_`: the function in a filter, against a comparand that keeps between a fifth and four fifths of the rows. A
    Boolean function is the filter itself.

  A transcendental result (`exp`, `log`, `log10`, `sin`, `cos`, `tan`, `atan`) is projected as a comparison with a
  threshold its value clears by a margin, `->greaterThan(t)`, never raw, so no platform's last bit reaches an
  expectation (F60's lesson). `vocab.check()` fails the build when a filter keeps every row or none, or when a
  `VN_` meets no absent value.
- **The reader, emitter and oracle.** `query.Apply` is a chain of functions applied to a path's value, the four
  arithmetic operators written infix, inside `map` for an optional value; `Proj` and `Pred` carry one. `emit.py`
  writes it, and `oracle._apply` evaluates it as legend-pure defines it, through `oracle.IMPL`.
- **Oracle fixes, each where the oracle had followed SQL rather than legend-pure:**
  - `indexOf` is zero-based with -1 for a miss, not `strpos`.
  - `dayOfWeekNumber` is ISO. By its own comment it had followed the engine's DuckDB lowering, which made agreement
    with the engine circular.
  - `mod` is never negative and `rem` takes the dividend's sign. The oracle had refused negative operands as
    "dialects differ".
  - `round` is half-even. The oracle had refused every tie.
  - `toString` of a Boolean is `'true'`, not Python's `'True'`.

  Regenerating changed no existing service.
- **A Decimal comparand never takes more than 16 fractional digits.** The first run's three Decimal filters met
  F61 (Phase 1's finding, which its own services pin).
- **`executed.py`** holds four features (rule 6), each executed by passing services: a function in a projection, in
  a filter, through `map`, and a chain of them.
- **The scalar probe**, run again with this oracle: `indexOf` and `dayOfWeekNumber` now disagree in a property
  mapping too, and are recorded as F72's and F74's.

**Found on the way** (each minimized under `scripts/corpus/repro/`, with its services quarantined):

- **F71:** Integer arithmetic over an `INTEGER` column is 32-bit in the SQL, where Pure's Integer is 64-bit.
  `abs(-2147483648)` and `2147483647 + 1` overflow, and `floor(1e10)` is cast to INT32 (8 services;
  `integer-arithmetic-32-bit`).
- **F72:** `indexOf` in a query is SQL's `strpos`, one-based with 0 for a miss, where the same query's
  `substring` is Pure's (3 services; `indexof-strpos`).
- **F73:** `round` takes a tie away from zero; Pure's is half-even (2 services; `round-half-even`).
- **F74:** `dayOfWeekNumber` counts from Sunday; Pure's is ISO (3 services; `dayofweeknumber-sunday-first`).
- F35 (3 services), F38 (4: the projections; the filters pass) and F68 (2: `VN_PlusString` and `VN_RegexpIndexOf`)
  are pinned again. In all, 25 of the 258 are quarantined.

**The engine verdict (2026-10-28, legend-engine 4.145.0, Windows x64).** The whole corpus (`bazel run
//scripts/corpus:run -- --jobs 4`, 96 minutes): 5,327 pass, 82 known-fail (quarantined), 1 not run (F15), 0
unexpected, of 5,410 testables. Of the 258 new services 233 pass and 25 are quarantined, and every service that
passed after stage 1 passes. Before the run, the vocabulary alone ran from a scratch copy of the corpus, and that
is how its findings and fixes above were found.

**Lite.** On DuckDB 5,135 tests pass (4,939 before) and on H2 5,079 (4,910). The 196 and 169 vocabulary services
that pass account for the whole rise, and no test that passed fails. Both `MIN_PASS` ratchets rose by that, each
with a dated note. Lite rounds half-even and numbers ISO weekdays, as Pure does. Its failing vocabulary services fall
into these buckets:

- **TV-04's trailing space trimmed** (Phase 1's bucket). It reaches most string functions now: `toUpper`, `length`,
  `lpad`, `reverseString`, the filters on `endsWith` and `matches`...
- **New on both databases:** 32-bit overflow (F71's shape), `indexOf` as `strpos` (F72's), and `map` over an
  absent value applying the function to NULL (F68's: `VN_Contains` false, `VN_DecodeBase64` `''`, `VN_PlusString`
  `'!'`).
- **New on H2 only:**
  - functions the dialect lacks: the bit operations, base64, `levenshteinDistance`, `reverseString`, `regexpCount`
    and `regexpIndexOf`;
  - a `weekOfYear` that is not ISO, and a `firstDayOfWeek` that starts on Sunday.

**The measures after stage 2** (the report's `tools/run.sh` at the pin, given the engine's pass list):

- Platform functions in service queries: 112 distinct names (target 150; the report counted 25). 107 are in services
  that pass on the engine, and 100 in services that pass in lite.
- PCT function names used anywhere: 99 of 321 (target 150; 66 before).
- EMIT 45 of 109 on the engine and 39 in lite, as before: a function applied in a query is no EMIT feature.

**Stage 3: aggregates in a groupBy (2026-10-29, `94c3f5a0c`).** Each aggregate the probes agree on, written as
legend-pure defines it, or legend-engine's Pure where the function is legend-engine's
(`core_functions_standard/math/aggregator`), never as SQL computes it.

- **The definitions decide the edge cases.** A relation groupBy's map is `{T[1]->K[0..1]}`, so an absent value
  maps to nothing, and a group whose values are all absent reduces an empty collection. Over one, Pure's `sum` is
  0 (`plus()` of nothing), `count` 0 and `joinStrings` `''`; `max`, `min`, `first`, `last` and `percentile` are
  absent; `average`, `median`, `mode`, the variances, the standard deviations and `wavg` fail ("Mean of an empty
  set", `toOneMany()`, `at()`), which no expected result can state, so the oracle refuses them there. Over one
  value, Pure's sample variance is 0 (`variance`: `if ($numbers->size() == 1, | 0, ...)`).
- **A grouping key in the seed.** `TYPED_VALUE.SEGMENT` (`TypedValue.segment`, 8112-typedvalues.pure): segment A
  holds TV-02 alone, the one row absent in every optional column, so a group whose values are all absent exists
  for each of them; B, C and D each hold present and absent values. No other column or expected result changed,
  but the seed did, so the whole corpus ran.
- **`vocab.py`'s second part** writes 77 services into `99-vocab-services.pure`, over 41 cases:
  - `VG_`: the aggregate over a required property, keyed on `active` (groups of nine and six);
  - `VGN_`: over the optional property, keyed on `segment` for an aggregate Pure defines over nothing, or on
    `active` (groups with absent and present values) for one it fails on;
  - `VGS_`: the sample variance and standard deviation keyed on `segment`, whose group A holds one value.

  The pair aggregates (`maxBy`, `minBy`, `wavg`, the covariances, `corr`) map two required properties through
  `rowMapper` or `wavgRowMapper`. An inexact result (a variance, a correlation, a mean of Decimals) is compared
  in the reducer, `agg|$agg->stdDevSample()->greaterThan(t)`, with t between the groups' values by a margin (F60's
  lesson); the engine lowers the composed reducer. `mode` is over `metric`, whose groups each have one most
  frequent value: Pure's is the smallest of the most frequent, and a database's choice among a tie is its own.
- **`query.Agg`** carries an aggregate's arguments, a second source with its mapper, and steps after it; it is a
  `(name, source, function)` tuple, so every generator's plain triples read as before. `emit.py` writes it and
  `oracle._reduce_group` evaluates it through `_REDUCERS` and `_PAIR_REDUCERS`.
- **One oracle, one semantics.** The oracle's group-by `sum` had followed SQL, NULL over a group with no values,
  while its `IMPL["sum"]`, used by the probes, was already 0. It now reads the Pure definition everywhere, and 24
  grouped totals written before Phase 4 (`DSAgg_*` and `DSGroup_CashFlow`, each over a navigation that lands on
  nothing for one group) changed their expectation from null to 0. Its group-by `average` over nothing is
  refused, as Pure fails there; no service met one. This is the plan's D8, taken by this
  session on legend-pure's definition as D7 was; the user may reverse it. (The user reversed it on 2026-10-29, on
  the groupBy natives' and their PCT's evidence: see D8 and stage 6.)
- **`executed.py`** holds four more features (rule 6): an aggregate in a groupBy, one with arguments, one over
  pairs, and one followed by a function.

**Found on the way** (each minimized under `scripts/corpus/repro/`, with its services quarantined):

- **F75:** in a groupBy, `sum` and `joinStrings` over a group whose values are all absent answer SQL's NULL, where
  Pure's are 0 and `''` (`count`, 0, agrees). 28 services: the four `VGN_` ones and the 24 deep-stack totals
  (`aggregate-over-nothing`). Withdrawn on 2026-10-29 with D8's reversal: the NULL is legend-engine's own definition.
- **F76:** the sample variance and standard deviation of one value answer NULL, where Pure's are 0 (2 services;
  `sample-variance-of-one`).

**The engine verdict (2026-10-29, legend-engine 4.145.0, Windows x64).** The seed changed, so the whole corpus ran
(`bazel run //scripts/corpus:run -- --jobs 4`, 95 minutes): 5,374 pass, 112 known-fail (quarantined), 1 not run (F15),
0 unexpected, of 5,487 testables. Of the 77 new services 71 pass and 6 are quarantined; the 24 moved totals are
known-fail; every other service that passed after stage 2 passes.

**Lite.** On DuckDB 5,178 tests pass (5,135 before) and on H2 5,117 (5,079): 67 and 62 of the 77 new services
pass, and the 24 deep-stack totals, which lite answers with SQL's NULL as the engine does, now fail. No other test
that passed fails. Both `MIN_PASS` ratchets rose, each with a dated note. Lite's failing aggregate services:

- **on both databases:** F75's and F76's shapes; `joinStrings` over TV-04's trailing space (Phase 1's bucket);
  `last` ("an instance literal has no row scope for $agg"); `wavgRowMapper`, an unknown function;
- **on H2 only:** `ARG_MAX` and `ARG_MIN`, which H2 lacks (`maxBy`, `minBy`); `AVG` of integers to eleven
  digits; and `first`, which takes another row of the group (the query fixes no order within a group, and the
  oracle reads the seed's, as it does for `first` over a to-many).

**The gate.** `bazel test --lockfile_mode=error //gates:local //:generated //scripts/corpus:all //core:stress_suites
//core:stress_suites_h2 //core:corpus_differential_test`: 394 pass and 2 are skipped. 58 fail on this desk alone, none
depending on a changed file: 55 diff tests that `fc.exe` cannot open here, each pair identical ignoring CR (checked
one by one), and 3 datacube browser tests that time out. Stages 4 and 5 met the same 58. The probes, run again after
each oracle change, changed no verdict, and `//scripts/corpus:functions_gate` passes.

**Stage 4: relation and TDS shapes (2026-10-29, `939d4c40b`).** Over `trading::Trade`, whose trades of
2024-06-03 hold A5's tie: four share a notional of 250,000, so `rank`, `denseRank` and `rowNumber` differ there.

- **`query.Spec.relation`**, a list of operations after the projection and any groupBy, each written in the
  Spec's paradigm by `emit.py` and evaluated by `oracle._relation_ops`: `Extend`, `Window`, `Rename`, `Select`,
  `Distinct`, `Sort`, `Cut` (take, drop, limit, slice, paginated), `Concatenate` and `Join` (the other side a Spec
  of its own, written in place). An ordinal operation must follow a total order, or the oracle refuses it; so must
  `rowNumber` and `ntile`.
- **`vocab.py`'s third part** writes 40 services: each shape as a Relation (`VR_`) and as a TDS (`VT_`):
  `extend` (arithmetic, and a string function), `rename` (`renameColumns`), `select` (`restrict`), `distinct`, the
  ordinal operations across the tie, `concatenate` (Relation only: the TDS form errored in the probe), inner and
  left joins onto the typed values that name a trade, and the windows, partitioned by trade date and ordered by
  notional: `rank`, `denseRank`, `percentRank`, `cumulativeDistribution`, `rowNumber` and `ntile` through
  `over()`, and `sum`, `max` and `count` over the partition. The TDS windows are `olapGroupBy`, which sorts by one
  key (`SortInformation[0..1]`) and ranks with `rank`, `denseRank` and `rowNumber` only; in legend-engine's Pure
  each of those is `fail('not implemented')`, a marker for the SQL, with no definition of its own.
- **`executed.py`** holds eleven more features (rule 6), one per operation family and paradigm.

**Found on the way:**

- **F37, in one more place:** a TDS `extend`'s `substring` is still SQL's one-based reading (`'-0001'` for
  `'TRD-0001'->substring(4)`), where the Relation `extend` of the same query answers Pure's `'0001'` (1 service,
  `VT_ExtendString`; `substring-tds-extend`).
- **F77:** a window `count` over a partition is written as a bare `count(...)` with no `OVER`, and DuckDB refuses
  the statement; the window's `sum` and `max`, and the TDS `olapGroupBy` count, pass (1 service,
  `VR_WindowCount`; `window-count`).

**The engine verdict (2026-10-29).** No seed changed, so `bazel run //scripts/corpus:changed` listed the 40 new
services and nothing else, and `bazel run //scripts/corpus:run -- --only` judged them: 38 pass, 2 known-fail, 0
unexpected.

**Lite.** On DuckDB 5,213 tests pass (5,178 before) and on H2 5,152 (5,117): 35 of the 40 on each, and no test that
passed fails. Both `MIN_PASS` ratchets rose, each with a dated note. The others: the one-argument `substring` read
one-based in an `extend` of either paradigm, and `olapGroupBy`'s ranking `func(y|$y->rank())` ("a bare lambda has no
type outside a call position").

**Stage 5: the rest of the scalars, and control flow (2026-10-29, `0496e6966`).**

- **19 scalar cases** in stage 2's form (`VP_`, `VN_`, `VF_`; 57 services): `left`, `right`, `char` (after
  `ascii`), `jaroWinklerSimilarity`, `bitShiftLeft`, `parseFloat` and `parseDecimal` (after `toString`),
  `toDecimal`, `cbrt`, `sinh`, `cosh` and `tanh` (over `atan`'s bounded range), `asin` and `acos` (over `sin`'s),
  `atan2`, `cot`, `dayOfWeek` (an enumeration result, compared with `==` only), `firstMillisecondOfSecond` and
  `timeBucket`. The oracle's `parseDecimal` returns a Decimal, as legend-pure's does (no service had parsed one).
  **Not `splitPart`:** legend-pure's is zero-based (`split($token)->at($part)`, and `at` raises out of range),
  where the oracle's, which the combination matrix asserts in property mappings, is the relational dynafunction's
  one-based reading: F37's question for another function, left for a decision of its own. Not `formatDate`, whose
  format language is a question of its own, nor `hashCode`, whose value is unspecified.
- **Control flow** (`VC_`, 7 services): `if` projected over a comparison, a Boolean and an absent value (0, or the
  value itself through `toOne()`: the coalescing idiom), as `query.If`; a top-level `let` a filter compares with,
  an Integer and a Date (`Spec.lets`, read by a predicate as a Param); and `exists` over a to-many end,
  `positions::Book.trades`, three books of nine having a trade over 400,000, and its negation, which keeps the
  books with none.
- **Probed and left for the next session:** a `let` inside a column lambda (`r:x|let q = $x.g; $q + '!';`)
  errors in the router ("Cannot cast a collection of size 2 to multiplicity [1]"); `map` then `sum` over an empty
  to-many answers null where Pure's `sum` is 0 (F75's shape); `subType()` needs a mapped subclass hierarchy.
- **`executed.py`** holds three more features (rule 6): `if`, `let`, and `exists` over a to-many.

**Found on the way:**

- **F78:** `bitShiftLeft` of a negative Integer errors on DuckDB (`(cast(%s as BIGINT) << cast(%s as BIGINT))`;
  "Cannot left-shift negative number"), where legend-pure's is Java's shift on a long (3 services;
  `bitshiftleft-negative`).
- **F79:** `parseDecimal` is `cast(%s as decimal)`, which DuckDB reads as DECIMAL(18,3): three fractional digits
  (2 services; `parsedecimal-scale`).

**The engine verdict (2026-10-29).** `changed` listed the 64 new services and nothing else: 59 pass, 5 known-fail, 0
unexpected.

**Lite.** On DuckDB 5,264 tests pass (5,213 before) and on H2 5,197 (5,152): 51 and 45 of the 64, and no test that
passed fails. Both `MIN_PASS` ratchets rose, each with a dated note. The others: `bitShiftLeft` of a negative number
(F78's shape), `parseDecimal` cast to DECIMAL(5,2), TV-04's trimmed trailing space (`left`, `right`,
`jaroWinklerSimilarity`), and `asin` and `acos` through `map` over an absent value answering `true` (F68's shape); on
H2 also the functions it lacks (`cbrt`, `jaroWinklerSimilarity`, `time_bucket`, the bit shifts).

**The measures after stage 5** (the report's `tools/run.sh` at the pin, given the engine's pass list: stage 3's
whole run with stages 4 and 5's `--only` runs, nothing else having changed):

- Platform functions in service queries: 167 distinct names (target 150; 112 after stage 2, and the report
  counted 25). 162 are in services that pass on the engine, and 150 in services that pass in lite.
- PCT function names used anywhere: 146 of 321 (target 150; 99 after stage 2).
- EMIT 45 of 109 on the engine and 39 in lite, as before: a function in a query is no EMIT feature.

**Where the stage-5 session stopped (2026-10-29).** All five stages are committed. Phase 4 is not done: its
platform-function row and its window, olap, TDS-join and relation-API services are, but the PCT row is four names
short of 150. What is left (all of it taken up by stage 6, below):

- **Four PCT names or more.** Unused and in reach (`coverage.json`'s `pct_by_category`): `parseBoolean`, `pi`,
  `toDegrees`, `toRadians`, `mean`, `xor`, `between`, `greatest`, `least`, `toUpperFirstCharacter`,
  `toLowerFirstCharacter`, `regexpReplace`, `date(y, m, d)`, `head`, `tail`, `at`, `size`, `eq`, `compare`, and the
  relation windows `lag`, `lead` and `nth`. Each goes in stage 2's or stage 4's form, after its probe agrees or its
  disagreement is adjudicated.
- **`splitPart`, a decision.** legend-pure's is zero-based (`split($token)->at($part)`, raising out of range); the
  oracle's is the relational dynafunction's one-based reading, which the combination matrix (`combos.py`) asserts
  in property mappings. Reading Pure's would move those cells, F37's case for another function.
- **Control flow, the rest.** `subType()` needs a mapped subclass hierarchy; `map` then `sum` over an empty to-many
  answers null where Pure's `sum` is 0 (probed: F75's shape, to be pinned when a service carries it); and a `let`
  inside a column lambda (`r:x|let q = $x.g; $q + '!';`) errors in legend-engine's router ("Cannot cast a
  collection of size 2 to multiplicity [1]"), a finding to minimize when a service carries it.
- After each oracle change, `bazel run //scripts/corpus:probe_functions` (and `:probe_collection`) again. The other
  fourteen probes, the aggregate probe among them, still name the Maven-era runner (P7-03).

**The branch rebased (2026-10-29).** `stress_test/corpus` was rebased onto upstream `main` (9de4d3a87) with no
conflict, so the stages' commits have new hashes; the ones above are the rebased ones (stage 1 `0bb5fca71`, stage 2
`b9f0d212c`, stage 3 `94c3f5a0c`, stage 4 `939d4c40b`, stage 5 `0496e6966`).

**D8 reversed (2026-10-29, `5cabca0a4`).** The user asked for D8 to be reviewed, and reversed it on the review's
evidence (see D8): a groupBy never reduces a group whose values are all absent, so its aggregate is absent, `sum` and
`joinStrings` included, and its `count` is 0. `oracle._reduce_group` and `_group` hold the rule, and `_rel_group`
(the probes' groupBy) too; a window still reduces its frame. F75 is withdrawn, and `repro/aggregate-over-nothing/`
now shows the rule.

- **The engine verdict.** No seed changed; `changed` listed the 28 services whose expectations moved back to null
  (the 24 deep-stack totals, `VGN_SumQuantity`, `VGN_SumRate`, `VGN_SumAmount` and `VGN_JoinStringsName`): 28 pass,
  0 unexpected. The repro's three services pass.
- **Lite.** On DuckDB 5,291 tests pass (5,264 before) and on H2 5,224 (5,197): the 24 totals and the three
  `VGN_Sum` services, which lite answers with NULL; `VGN_JoinStringsName` still meets TV-04's trimmed trailing space.
  No test that passed fails. Both `MIN_PASS` rose, each with a dated note.
- **The gate** met the desk's known reds alone: the same 55 diff tests (the stress corpus's outputs compared equal
  ignoring CR) and 5 datacube browser tests (4 timeouts and `cube_page_shell_test`, none depending on the corpus;
  the rebase brought the flow-mode work). The probes, run again, changed no verdict.

**Stage 6: the last PCT names, and the gaps stage 5 left (2026-10-29, `880243bbb`).** Every candidate was first put in
front of the pinned engine in a small model of its own: `xor`, `pi`, `toDegrees`, `toRadians`,
`toUpperFirstCharacter`, `toLowerFirstCharacter`, `between`, `regexpReplace`, `date(y, m, d)`, `greatest`, `least`,
`mean`, `lag`, `lead`, `nth` and a query's `splitPart` answered as Pure defines them. `parseBoolean` is refused in a
query as in a mapping (F36), and `lag($r, 2)` answers the previous row (F80). Most are legend-engine's own Pure
functions with bodies, which the engine inlines in a query.

- **13 scalar cases** in stage 2's form (`VP_`, `VN_`, `VF_`; 39 services): `xor`, `toDegrees`, `toRadians`, `pi()`
  as an argument (`$x.rate * pi()`, `query.Nullary`), `toUpperFirstCharacter`, `toLowerFirstCharacter`, `greatest`
  and `least` over a list of the value and a literal (`[$x.quantity, 7]->greatest()`), `between`, `regexpReplace`
  with `replaceAll` false and true, `date(year, 6, 1)`, and `splitPart('e', 0)` (D9: a query's is Pure's, zero-based,
  `oracle.QUERY_IMPL`; refused where Pure's and SQL's splits differ, an empty piece or a token of several
  characters). The transcendental ones are compared with a threshold, as before.
- **`mean`** in stage 3's form (`VG_` and `VGN_`, over `quantity` and `rate`; 4 services): legend-engine's Pure is
  `$numbers->average()`.
- **Windows** in stage 4's form, Relation only (olapGroupBy has none of them; 4 services): `lag`, `lead`, `nth($w,
  $r, 2)` and `lag($r, 2)`, each reading another row's notional (`Window.read`) over each trade date ordered by
  notional and id. The oracle's `nth` reads the window's frame, the partition's start through the current row, as
  its PCT does.
- **Control flow** (2 services): `VC_LetColumn`, a `let` inside a column lambda (`Apply.let`), and
  `VC_MapSumTrades`, `$x.trades->map(t|$t.notional)->sum()` over `positions::Book` (`Proj.over`), whose six books
  with no trades sum legend-pure's 0. `subType()` is left for Phase 7 (the user's decision): the corpus's one mapped
  hierarchy, `hier::`, maps a root set beside subclass sets, where a cast of a root-set object is Pure's failure and
  the engine's routing, so a service first needs a definition of it. It is no PCT name, so no row waits on it.
- **Oracle fixes, each where it had followed SQL rather than legend-engine's Pure:** `between` over an absent
  operand is false (its body's comparisons, D7), not NULL; `regexpReplace` honours `replaceAll`, where it had
  replaced every match whatever the flag. Neither function had reached a service, and the probes, run again, changed
  no verdict.
- **`executed.py`** holds three more features (rule 6): a function over a list, a function as an argument, and a
  window's offset function.

**Found on the way** (each minimized under `scripts/corpus/repro/`, with its services quarantined):

- **F80:** a window's `lag` or `lead` with an offset other than 1 is written with no offset (`processOffset` reads
  only its sign), so `lag($r, 2)` answers the previous row (1 service, `VR_Lag2`; `lag-offset`).
- **F81:** a `let` inside a column lambda fails in the router, "Cannot cast a collection of size 2 to multiplicity
  [1]", braced or not; a top-level `let` passes (1 service, `VC_LetColumn`; `let-in-column`).
- **F82:** `sum` over a to-many navigation that lands on nothing answers SQL's NULL, where legend-pure's collection
  `sum` is 0: a plain function over an empty collection, as `count()` is in F6, not a groupBy (1 service,
  `VC_MapSumTrades`; `sum-over-empty-to-many`).
- **F68, five more:** through `map` over an absent value, `toUpperFirstCharacter` and `toLowerFirstCharacter` answer
  `''`, `greatest` and `least` of `[NULL, 7]` 7, and `between` false (5 services, the `VN_` ones; their `VP_` and
  `VF_` pass).

**The engine verdict (2026-10-29).** `changed` listed the 49 new services and nothing else: 41 pass, 8 known-fail, 0
unexpected. The three repros reproduce as described.

**Lite.** On DuckDB 5,324 tests pass (5,291 after D8) and on H2 5,248 (5,224): 33 and 24 of the 49, and no test that
passed fails. Lite answers `VR_Lag2` right, where the engine is F80. Both `MIN_PASS` rose, each with a dated note.
Its failing stage-6 services:

- **on both databases:** TV-04's trimmed trailing space (Phase 1's bucket: `toUpperFirstCharacter`,
  `toLowerFirstCharacter`, `regexpReplace`, `splitPart`, the `let` column), `map` over an absent value (F68's shape:
  `between`, `greatest`, `least`), and the sum over no trades as NULL (F82's shape);
- **on H2 only:** no `MAKE_DATE` (`date`), `greatest` and `least` over a list ("reached a dialect without a list
  encoding"), `regexp_replace` refusing the `g` flag and, asked for the first match, replacing every one, and `AVG`
  of integers to eleven digits (`mean`).

**The gate.** `bazel test --lockfile_mode=error //gates:local //:generated //scripts/corpus:all
//core:stress_suites //core:stress_suites_h2 //core:corpus_differential_test`: 398 pass and 2 are skipped. 59 fail
on this desk alone, none depending on a changed file: the same 55 diff tests as before (the stress corpus's
outputs compared equal to the checkout ignoring CR) and 4 datacube browser tests that time out. The probes, run
again after the oracle's fixes, changed no verdict, and `//scripts/corpus:functions_gate` passes.

**Phase 4 done (2026-10-29).** Every row it set out to move has reached its target, and its window, olap, TDS-join
and relation-API services pass on the engine. The measures after stage 6 (the report's `tools/run.sh` at the pin,
given the engine's pass list: stage 3's whole run with the `--only` runs of stages 4 to 6 and D8, nothing else
having changed):

- **PCT function names used anywhere: 161 of 321** (target 150; 146 after stage 5, 64 at the pin before the plan).
  Stage 6 added `between`, `date`, `greatest`, `lag`, `lead`, `least`, `mean`, `nth`, `pi`, `regexpReplace`,
  `toDegrees`, `toLowerFirstCharacter`, `toRadians`, `toUpperFirstCharacter` and `xor`.
- **Platform functions in service queries: 183** distinct names (target 150; 167 after stage 5, and the report
  counted 25). 178 are in services that pass on the engine, and 165 in services that pass in lite.
- EMIT 45 of 109 on the engine and 39 in lite, as before: a function in a query is no EMIT feature.
- **The engine's standing verdict**, adding those runs to stage 3's: 5,540 pass, 99 known-fail, 1 not run (F15),
  0 unexpected, of 5,640 testables. Phase 4 added 488 services (258, 77, 40, 64 and 49 in stages 2 to 6).
- **Lite**: 5,324 tests pass on DuckDB and 5,248 on H2 (4,939 and 4,910 before the phase).

**F37's property-mapping half withdrawn (2026-10-29, `12795b348`).** The user took D9's reading for `substring` too: a
relational mapping's `substring` is the relational dynafunction, which every dialect extension writes as SQL's
`SUBSTRING` (one-based, the third argument a length), and legend-pure's is a different function of the same name; the
engine itself converts a query's into the dynafunction's arguments (`processSubstr`), as it does `splitPart`'s.

- **The oracle** reads a mapping's `substring` as SQL's (`_sql_substring`, refusing a start before 1 or a negative
  length, which dialects read apart) and a query's as legend-pure's (`QUERY_IMPL`). `probe_functions.py` drops
  `substring`'s F37 adjudication.
- **`77-substring-paths.pure`:** `F37_SubstringSql` asserts `'lpha'` (its test renamed `one_based_length`) and left
  quarantine; `F37_SubstringPure`, an M2M mapping evaluated in memory, keeps Pure's `'ph'`. F37 stands for its other
  half, a TDS `extend`'s `substring` answering SQL's in a query (`VT_ExtendString`).
- **The combination matrix** writes `sub` again (left out since Phase 0 for F37): 12 cells of 956, each
  `substring(col, 2, 4)`, which reads `'-S-N'` from `'R-S-NN1-001'` where Pure's would be `'S-'`. The cells repack the
  matrix's classes, so `64-combinations.pure`, `98-combination-execution.pure` and the deep stacks over them changed.
- **Lite (cross-area, the user's choice):** the new cells met a lite gap. `RelOpTranslator` passed the `substring`
  dynafunction's operands bare, so a column reached through a join, `[0..1]`, failed to type against Pure's strict
  `substring(String[1], ...)` ("multiplicity [0..1] is not compatible with [1]"), sinking `CB_C7`, `CB_C9`, `CB_C10`
  and `SP_ComboC9Group`: DuckDB would have fallen 5,324 to 5,321. Asked, the user chose to fix lite rather than lower
  the floor: the case now wraps its operands in the dyna lane's `toOne`, as the generic dynafunction path does.
- **The engine verdict.** The model changed, so the whole corpus ran (`bazel run //scripts/corpus:run -- --jobs 4`, 80
  minutes): 5,541 pass, 98 known-fail, 1 not run (F15), 0 unexpected, of 5,640 testables. Every repacked combination
  service passes, and `F37_SubstringSql` with them.
- **Lite.** On DuckDB 5,325 tests pass and on H2 5,249: `F37_SubstringSql` on each, and no test that passed fails
  (`CB_C7`, `CB_C9` and `CB_C10` still meet H2's bit operations). Both `MIN_PASS` rose by one, each with a dated note.
- **The gate** met the desk's known reds alone: the same 55 diff tests (the stress corpus's outputs equal to
  the checkout ignoring CR) and 4 datacube browser timeouts; core's own tests pass with the translator's change.
  The probes, run again, changed `substring`'s verdict alone, from disagreeing under F37 to `ok`, and
  `//scripts/corpus:functions_gate` passes. No measure moves: `substring` was already a PCT name in use, and a
  mapping's function is no query's.

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
| 1 | 2026-10-23 | — | 306 | F61–F65 (27 services) added; F28's entry removed (withdrawn, D7) | parameterized queries (151 tests), StrictTime over a VARCHAR (6), a trailing space trimmed (20), `!` over an absent navigation (3), F64's shape (3) | parameter types 9 at `[1]`, `[0..1]` and `[*]`; literal types 8 of 12; enumeration literals 33; `or`/`in`/`!` 29/25/27; EMIT 43 of 109 on the engine, 37 in lite; PCT 66 of 321 |
| 2 | 2026-10-27 | — | net +20: 547 in, 527 out, nearly all the deep variants re-rooted at classes with rows | none added; F58's two `DSLocal_` entries removed (they pass) | none | filters against a value 2,040 (2,023 on the engine); four-hop projections populated 1,553 of 1,553; four-level trees 397 of 397; empty 572 of 5,082, all declared, hollow 0; EMIT 43 of 109 on the engine, 37 in lite; PCT 66 of 321 |
| 3 | 2026-10-28 | — | 90 | `ITN_NoLimitInRange` under F51 added; F66 and F67 found, each with a repro | none (Phase 1's parameter bucket takes 20 more tests) | DateTime literals 66 and milestoned queries at a time of day 72, on the engine; a processing-only class tested; all-versions-query covered; EMIT 45 of 109 on the engine, 39 in lite; PCT 66 of 321 |
| 4 | 2026-10-28 | 2026-10-29, six stages | stage 1 (`0bb5fca71`) 0; stage 2 (`b9f0d212c`) 258; stage 3 (`94c3f5a0c`) 77; stage 4 (`939d4c40b`) 40; stage 5 (`0496e6966`) 64; D8 reversed (`5cabca0a4`) 0; stage 6 (`880243bbb`) 49 | 25 vocabulary services under F35, F38, F68, F71-F74 (stage 2); 30 under F75 and F76, 24 of them deep-stack totals whose expectation moved to Pure's 0 (stage 3); 2 under F37 and F77 (stage 4); 5 under F78 and F79 (stage 5); F75 withdrawn with D8's reversal, its 28 services out (2026-10-29); 8 under F68, F80, F81 and F82 (stage 6); F37's property-mapping half withdrawn, `F37_SubstringSql` out (`12795b348`); F68-F82 found, each with a repro | 32-bit overflow (F71's shape), `indexOf` as `strpos` (F72's), `map` over an absent value (F68's), an aggregate over nothing as SQL's NULL (F76's; F75's withdrawn), `last` and `wavgRowMapper` in a groupBy, `substring(start)` one-based in an `extend`, `olapGroupBy`'s ranking `func`, `parseDecimal` as DECIMAL(5,2), `sum` over no trades as NULL (F82's); on H2 alone, functions the dialect lacks (`MAKE_DATE`, `greatest`/`least` over a list), a non-ISO `weekOfYear`, a Sunday-first `firstDayOfWeek`, `AVG` of integers to eleven digits, and `regexpReplace` refusing `replaceAll` or replacing every match for the first | platform functions in service queries 183 (178 on the engine, 165 in lite); PCT 161 of 321; EMIT 45 of 109 on the engine, 39 in lite |
| 5 | — | — | — | — | — | — |
| 6 | — | — | — | — | — | — |
| 7 | — | — | — | — | — | — |
| 8 | — | — | — | — | — | — |
| 9 | — | — | — | — | — | — |
