# Stress corpus improvement plan

Proposed 2026-10-19. The user took the five decisions on 2026-10-21 ([Decisions](#decisions)), and Phase 0 started
that day (`docs/IN_FLIGHT.md`, the twelfth line). Phase 1 started and was done on 2026-10-23, on `stress_test/corpus`.
Phase 2 started and was done on 2026-10-27.
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
| 1 | 2026-10-23 | — | 306 | F61–F65 (27 services) added; F28's entry removed (withdrawn, D7) | parameterized queries (151 tests), StrictTime over a VARCHAR (6), a trailing space trimmed (20), `!` over an absent navigation (3), F64's shape (3) | parameter types 9 at `[1]`, `[0..1]` and `[*]`; literal types 8 of 12; enumeration literals 33; `or`/`in`/`!` 29/25/27; EMIT 43 of 109 on the engine, 37 in lite; PCT 66 of 321 |
| 2 | 2026-10-27 | — | net +20: 547 in, 527 out, nearly all the deep variants re-rooted at classes with rows | none added; F58's two `DSLocal_` entries removed (they pass) | none | filters against a value 2,040 (2,023 on the engine); four-hop projections populated 1,553 of 1,553; four-level trees 397 of 397; empty 572 of 5,082, all declared, hollow 0; EMIT 43 of 109 on the engine, 37 in lite; PCT 66 of 321 |
| 3 | — | — | — | — | — | — |
| 4 | — | — | — | — | — | — |
| 5 | — | — | — | — | — | — |
| 6 | — | — | — | — | — | — |
| 7 | — | — | — | — | — | — |
| 8 | — | — | — | — | — | — |
| 9 | — | — | — | — | — | — |
