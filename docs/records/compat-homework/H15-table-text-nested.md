# H15: a relation's text, at the program's last step or used further (compat homework, 2026-11-19)

**The ruling it serves** (the user, about 18:12 UTC, for B2):
- Where a relation's text is the program's LAST step (an assert side's root, or a query's root), the legend-pure route
  takes it over. The database returns typed rows, and Java prints legend-pure's text.
- Where the text is used FURTHER inside the query (`->toString()->length()`, a concatenation, a comparison inside a
  filter), the database computes lite's own text, and the difference is recorded.

**Checked against the rules of record** (`docs/records/compat-rules-of-record-2026-11-19.md`): this is a
measurement. No product file moves. The probe is a scratch commit on this branch, never for `main`.

## 0. The answer

**54 relation-text nodes are used further, every one of them in the corpus. Everywhere else every relation text is the
program's last step.**

| source | programs typed | tests typed | relation-text nodes | ROOT | NESTED |
|---|---|---|---|---|---|
| PCT channel A (DuckDB's 5 suites, H2 relation) | 3,187 | 1,566 | **0** | 0 | 0 |
| PCT channel B, DuckDB relation | 938 | 469 | 373 | 373 (365 tests) | 0 |
| PCT channel B, DuckDB standard | 410 | 205 | 58 | 58 (58 tests) | 0 |
| PCT channel B, DuckDB essential, grammar, unclassified | 1,150 | 575 | 0 | 0 | 0 |
| PCT channel B, H2 relation | 469 | 469 | 373 | 373 (365 tests) | 0 |
| corpus, DuckDB lane | 5,778 | 2,591 | 452 | 398 (253 tests) | **54 (46 tests)** |
| corpus, H2 lane (the same tests) | 5,778 | 2,591 | 452 | 398 (253 tests) | **54 (46 tests)** |
| lite's own core tests | 50,331 | 3,013 | 20 sites | 20 (16 tests) | 0 |
| the pure/v1 net's 15 requests | 30 | 15 requests | 0 | 0 | 0 |

**The 54 NESTED nodes** (each corpus lane; §4):
- **41 `toCSV(...)->replace('\n', ',')`**, one in each of 41 calendar tests (`testCalendarFunctions.pure`). The
  result is compared by `assertEquals` with a string literal. **The expected answer depends on legend-pure's
  spelling, byte for byte.** Under the ruling's NESTED rule, lite's own text would change the verdict unless it spells
  the CSV exactly as legend-pure does.
- **13 `println(toCSV(...))`** in 5 `meta::pure::tds::tests::extensions` tests. These are printed for debugging, and
  **no verdict reads them**.

**ROOT, by assert family:**

| source | family | nodes |
|---|---|---|
| PCT channel B (DuckDB; H2 relation the same) | `assertEquals` | 431 (`toString`, all on the actual side) |
| corpus (each lane) | `assertEquals` | 387: `toString` 256 (127 expected, 129 actual), `toCSV` 131 (14 expected, 117 actual) |
| corpus (each lane) | `assertSameElements` | 11 (`toCSV`, actual side) |
| core | the query's root | 16 |
| core | `assertEquals` | 3 |
| core | `assertSameElements` | 1 |

No other assert family reaches a relation's text. No ROOT node reaches its assert through a `let`, an `if`, a
collection or an `eval`: every one is a direct argument of the assert.

## 1. Which calls make a relation's text (read from the code, not from memory)

The Lowerer answers three calls by the plan style's `lowering.RelationTexts` facet. They are `TextRefusals.relation`'s
three callers (`core/src/main/java/com/legend/lowering/Lowerer.java`, the `scalar` switch, lines 4303, 4311 and 4317
on `f832a6671`):

| call | typed node | the facet's member |
|---|---|---|
| a relation's `toString` | `TypedNativeCall` to `meta::pure::functions::relation::toString` | `relationToString` |
| `toCSV` | `TypedNativeCall` to `meta::relational::tests::csv::toCSV` (`PlatformTypes.TO_CSV`) | `toCsv` |
| a TDS's `csv` property | `TypedPropertyAccess` of `csv` whose source is a relation (`Render.csvOverRelation`: `Type.relationSchema` of the source's type) | `csvProperty` |

A scalar's `toString` is a different typed node: the overload is resolved before lowering. That is why the count is
taken on typed trees.

## 2. Method

- **The probe** (`H15-probes/H15Census.java.txt`, committed as `PROBE (H15, ...)` on this branch): called once per
  program from `TypedQuery.body()`, where every program lite plans is typed once.
  - It walks the typed body with each node's parents. Before the walk it inlines the user calls
    (`UserCallInliner.inlineBody`, as G½ does).
  - When the inliner refuses a body, the probe walks the body as typed and also each called function's own typed body
    (`SpecCompiler.compile`, once per function per program). That was round 2.
  - The inliner refuses many bodies by name, about 70% of the corpus's (4,036 of 5,778 per lane). Round 2 walked
    34,933 helper bodies and found no relation text in any of them, so its counts are round 1's.
- **The classes.** A node's text is followed up its parents:
  - a `let` to each use of its variable; an `if` branch, a collection element, and `eval` of a lambda pass the text on
    to their own parent;
  - **ROOT:** an argument of a `meta::pure::functions::asserts::*` native, or a statement's root (the program's last,
    or an unread statement);
  - **NESTED:** any other parent. That includes an `if` condition and a lambda inside another call. The consuming node
    is named.
- **The populations run** (`bazel test --spawn_strategy=local`, at `f832a6671` plus the probe):
  - PCT: channel A's recordings `//pct:edge_duckdb_{essential,grammar,relation,standard,unclassified}` and
    `//pct:edge_h2_relation`, and channel B's `//pct:edge_channel_b_{essential,grammar,relation,standard,unclassified}`
    and `//pct:edge_channel_b_h2_relation`. Channel A types only the lambda given to `eval`, because the PCT's asserts
    run in legend-pure. Channel B types the whole body.
  - The corpus: both host passes, `//spec:judge_host_duckdb` and `//spec:judge_host_h2`, under the compatibility
    rules (the host judge).
  - pure/v1: `//core:edge_pure_v1`.
  - Core: `//core:edge_cases` and the 69 test targets of `//gates:core`, without the `_judge_both` and
    `_judge_database` copies (the same bodies under the other judges) and without the architecture target.
- **Labels and de-duplication.** Each row is labelled with the running test (`legend.test.current`, else
  `legend.test.id`). Rows are de-duplicated by test, call, class, parent chain, statement and source line. A test typed
  twice (its facts, then its plan) counts once.

## 3. ROOT

- **PCT.** Only channel B sees a relation's text: the PCT's `assertEquals(<expected #TDS text>, $res->toString())`.
  Its 431 nodes are all on the actual side, in 423 tests, 365 of them in the relation suite and 58 in the standard
  suite. Channel A's queries hold no relation text at all, in 3,187 programs.
- **The corpus.** 398 nodes in 253 tests, each lane:
  - `assertEquals` over a relation's `toString`, 256: the expected side is often itself `#TDS...#->toString()`
    (127), against `$result.values->...->toString()` (129);
  - `assertEquals` over `toCSV`, 131;
  - `assertSameElements` over `toCSV`, 11. That is H10 §9.6's (and H11's) "11 in the lanes": the other 11 of H11's 22
    are `<<test.ExcludeAlloy>>` milestoning tests that the lanes do not type.
- **Core.** 20 sites in 16 test methods:
  - query roots: `RenderCsvIntegrationTest` (8), `EdgeCasesGoldenTest` (2), `DynamicPivotTextRefusalTest` (2),
    `FloatTextTest`, `TaggedListEdgeTest`, `TaggedListTest` and `AggOrderNullPlacementTest` (1 each);
  - assert sides: `BodyRouterParityTest`'s `assertEquals` (3) and `assertSameElements` (1).

## 4. NESTED: every test

**Each corpus lane has the same 54 nodes in 46 tests.**

### 4.1 `toCSV(...)->replace('\n', ',')`, compared by `assertEquals`: 41 nodes, 41 tests. The verdict DEPENDS on legend-pure's spelling

All 41 are in `meta::relational::tests::functions::pureToSqlQuery::calendarAggregations`, and each has one of these
shapes:

```
assertEquals(['hireType,annualized,Campus,9.812850678733032,Lateral,9.267692307692307,'], toCSV($result.values)->replace('\n', ','));
assertEquals(['hireType,wtd,pwa,Campus,2.16,0.206880733946,Lateral,3.24,0.188073394497,'], toCSV($result.values)->replace('\n', ','));
assertEquals(['ytd,0.13,'], toCSV($result.values)->replace('\n', ','));
```

- **The consuming function** is `replace` (`meta::pure::functions::string::replace`). Its result is the side
  `assertEquals` compares.
- **Why the verdict depends on the spelling.** The expected literal is legend-pure's CSV text with each line end written
  as `,`. It pins:
  - the header row's column names;
  - `,` between cells;
  - a line end after every row, the last one included (the trailing `,`);
  - each Float in legend-pure's spelling (`9.812850678733032`, `0.206880733946`, `1.65`).

  Any other spelling of the CSV, for example lite's own text computed by the database, would fail the assert unless it
  is byte-identical. Lite has no CSV text of a relation of its own today: the product refuses one by name,
  `TextRefusals.relation("toCSV")`.
- **The tests:** testAnnualizedValue, testCmeValue, testCwValue, testCw_FmValue, testCyminus2Value,
  testCyminus3Value, testGroupbyAfterCalendar, testJoinAfterCalendar, testMtdValue, testNestedAttributeCalendar,
  testP12MtdValue, testP12WaValue, testP12WtdValue, testP4WaValue, testP4WtdValue, testP52WaValue, testP52WtdValue,
  testPmaValue, testPmaValueOnStartYear, testPmtdValue, testPqtdValue, testPriordayValue, testPrioryearValue,
  testPwValue, testPw_FmValue, testPwaValue, testPwaValueOnStartYear, testPwtdValue, testPymtdValue, testPyqtdValue,
  testPytdValue, testPywaValue, testPywaValueOnStartYear, testPywtdValue, testQtdValue, testReportenddayValue,
  testSubclassDate, testSubclassValue, testUnionWithWtdAndPwa, testWtdValue, testYtdValue.
- **How H11 read them.** H11 counted these 41 as a relation text at the side's root, in a presenter form (`CSVJOIN`,
  its §A3). Typed, the text feeds `replace` first, so under this ruling they are NESTED.

### 4.2 `println(toCSV(...))`: 13 nodes, 5 tests. The verdict does NOT depend on the spelling

| test (`meta::pure::tds::tests::extensions::`) | nodes |
|---|---|
| `columnValueDifferenceTest` | 4 |
| `rowValueDifferenceTest` | 4 |
| `iqrClassifyTest` | 2 |
| `zScoreTest` | 2 |
| `testExtendDigest_InMemory` | 1 |

- The consuming function is `println`. The text is written to the log, and no assert reads it.
- Under the NESTED rule the database must still produce lite's own text, which the product refuses by name today. So
  these 5 tests depend on lite having a relation text of its own to PLAN, not to pass their verdicts.

## 5. Cross-checks

- **H11's judge census** (`H11-data/*-judge.tsv` on `claude/compat-relation-text-spike`: lite's parser, no types):
  - **PCT:** 431 `toString` sides in 423 tests. The same, exactly.
  - **Corpus, test by test:**
    - a relation's `toString`: the same 127 tests. The 3 extra nodes are in two union tests with the same side twice,
      which H11 keyed once by its text;
    - `toCSV` at the root: 126 tests here against H11's 145. All 19 of H11's other tests are tests the lanes do not
      type, including the 11 `ExcludeAlloy` `assertSameElements`. The 2 extra nodes are `testFilter`'s repeated side;
    - the 41 `replace` tests: the same 41, NESTED here;
    - H11's one TDS `csv`, `testEnumInRelation` (`<<test.AlloyOnly>>`): typed, it is
      `$result.values->cast(@TDS<Any>).csv`. The cast erases the schema, so by the Lowerer's own rule
      (`Render.csvOverRelation`) it is not a relation's text, and the facet is not asked.
- **H2's query-side census** (`H2-text-in-queries.md`: lite's parser, a taint interpreter) found no relation print
  inside a PCT query. That agrees with channel A's 0.

## 6. Limits

- **Helpers that would not type.** In the bodies the inliner refused, 6,759 helper bodies would not type, so their
  contents were not seen. Every one of H11's relation-text sides in a test the lanes type was found (§5), so none of
  them sits in such a helper.
- **What was not run:**
  - 11 core targets backed by Postgres failed as root (`initdb` refuses root), so programs they would have typed are
    not counted;
  - `//pure-protocol:twins_test` did not build: TeaVM cannot compile the probe's `ProcessHandle`;
  - the judge-mode copies of the core targets (the same bodies) were left out;
  - the SQLite lanes and the warehouse corpus were not asked for.
- **The counting unit is a call in the typed tree** (after inlining), not an assert side, so a side holding two calls
  counts twice.
- **Core's denominators count a program once per target that types it** (several targets run the same classes on
  different databases). Its 20 sites are de-duplicated across targets.

## 7. Files

- `H15-probes/H15Census.java.txt`: the probe, round 2's form.
- `H15-probes/h15_analyze.py.txt`: the analysis.
- `H15-data/nodes.tsv`: every relation-text node, de-duplicated, with source, test, call, class, parent chain,
  statement, source line, argument type and how it was walked.
- `H15-data/pct-a-programs.tsv`: channel A's programs per lane, all with 0 nodes.
