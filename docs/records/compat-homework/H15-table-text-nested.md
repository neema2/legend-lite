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
- `H15-data/calendar-floats.tsv`: §8's check of the 41 calendar tests: each expected literal, its Float cells, the most
  decimals one needs, lite's ruled Float text of the same values, and the verdict on `main` per lane.

## 8. The 46 tests with NESTED nodes: today, under B2, and whether any lite text could pass (the owner's follow-up)

**Sources.**
- `main`'s verdicts are its committed host-judge ledgers, `spec/src/test/resources/edge/corpus_{duckdb,h2}_host_judge-host.tsv`
  at `b75d6c73a`. Under rule 4 the host judge is compatibility's judge.
- `main`'s ledgers carry no messages. The reasons are read from the same ledgers run at #124's head (`f832a6671`,
  which records each non-PASS message), whose verdicts for these 46 tests are identical to `main`'s on both lanes.
- The lanes' database-judge fail rosters (`rcorpus/{duckdb,h2}-fail-roster.txt`) name none of the 46 on DuckDB and 7 on
  H2.

### 8.1 The answer

| | DuckDB: pass of 46 | H2: pass of 46 |
|---|---|---|
| (1) `main` today | **39** (calendar 34 of 41, extensions 5 of 5) | **32** (calendar 32, extensions 0) |
| (2a) B2 as ruled, lite as it is (the product refuses `toCSV` by name) | **5** (calendar 0: all 41 refused; extensions 5) | **0** |
| (2b) B2 if lite had a CSV text (legend-pure's layout, lite's ruled Float text) | **37** (calendar 32, extensions 5) | **30** (calendar 30) |

- **(3)** 9 of the 41 calendar tests expect a Float with more than six decimals (12 to 16). No lite text can pass
  those. The other 32 need six or fewer, and lite's Float text of the same values reproduces their expected strings
  byte for byte.
- **(4)** The 5 extensions tests are not affected by B2. `println` is an inert diagnostic on both doors: its argument
  is never planned or evaluated. So no relation text is made there, neither legend-pure's nor lite's, and nothing needs
  to be taken over.

### 8.2 Today on `main` (1)

**The 41 calendar tests:**

| lane | pass | fail at `assertSameSQL` (before the text) | fail at the `toCSV(...)->replace` assert |
|---|---|---|---|
| DuckDB | 34 | 4 | 3 |
| H2 | 32 | 4 | 5 |

- **`assertSameSQL` failures**, on both lanes: `testP12WaValue`, `testP52WaValue`, `testPwaValue` and
  `testUnionWithWtdAndPwa`. Each is "sql-text ROW verdict ... golden rows vs ours diverged": the golden SQL's rows on
  H2 are rounded to 12 decimals (`0.383333333333`), lite's are not.
- **Text failures on both lanes** (`testPmaValueOnStartYear`, `testPwaValueOnStartYear`, `testPywaValueOnStartYear`):
  the expected Floats are rounded to 12 or 13 decimals, and lite's text prints the full double:
  - expected `0.236666666667`, got `0.23666666666666666` on DuckDB;
  - on H2, got `0.236666666666666666`.
- **Text failures on H2 only:**
  - `testMtdValue`: expected `3.0`, got `3`;
  - `testSubclassValue`: expected `10.0`, got `10`.

  The H2 cell comes back whole, not as a Float.

**The 5 extensions tests:**
- On DuckDB, all 5 pass.
- On H2, none passes:
  - `columnValueDifferenceTest` and `rowValueDifferenceTest` fail at their fourth assert. That assert is
    `assertEquals($relationalResult->toCSV(), $inMemoryResult->toCSV())`, a ROOT pair of relation texts (§3), so it
    is not the `println`;
  - `iqrClassifyTest`, `testExtendDigest_InMemory` and `zScoreTest` fail before any assert:
    `DialectCapability: UNNEST reached a dialect without an unnest placement`. That is an H2 gap, not a text.

### 8.3 Under the ruled B2 (2)

**(2a) As lite is today.** NESTED means lite's own text, and lite has no CSV text of a relation:
- the product refuses `toCSV` by name (`TextRefusals.relation("toCSV")`);
- the ruled change of ~18:05 (H14) gives a relation's `toString` a database text, but leaves `toCSV` and a TDS's `csv`
  untouched (H14 §2).

So all 41 calendar bodies are refused at planning on both lanes. That turns DuckDB's 34 passes and H2's 32 into
failures by name. The 5 extensions tests keep today's outcomes (§8.5).

**(2b) If lite had a CSV text.** This assumes legend-pure's CSV layout (header row, `,` between cells, a line end after
every row, an empty cell for a null), with lite's ruled Float text: six decimals; trailing zeros dropped; a whole value
keeping `.0` (the user, ~18:05, H14 §6's V2).
- **DuckDB, 32 of 41:** the two lost against today are `testAnnualizedValue` and `testCmeValue`, which need 15 and 16
  decimals.
- **H2, 30 of 41:** the same two are lost. `testMtdValue` and `testSubclassValue` presumably still fail there, because
  the H2 cell is not a Float (today's `3` and `10`).
- **Under the earlier Float text V1** (whole values trimmed to `3`), `testMtdValue` and `testSubclassValue` would also
  fail on DuckDB: 30.

### 8.4 Could ANY lite text pass the 41? (3)

Lite's Float text is six decimals at most, so an expected Float with more digits cannot be matched by any lite text.
The cell-by-cell check is `H15-data/calendar-floats.tsv`.

| expected Floats need | tests | the tests | on `main` today |
|---|---|---|---|
| **more than 6 decimals** | **9** | `testAnnualizedValue` (15), `testCmeValue` (16) | both pass on both lanes: lost under any lite text |
| | | `testP12WaValue`, `testP52WaValue`, `testPwaValue`, `testUnionWithWtdAndPwa` (12 or 13) | fail first at `assertSameSQL` |
| | | `testPmaValueOnStartYear`, `testPwaValueOnStartYear`, `testPywaValueOnStartYear` (12 or 13) | fail at this text today |
| **6 or fewer** | **32** | the other 32 (for example `1.65`, `0.8575`, `0.259`; `3.0` and `10.0` whole) | lite's V2 text of the same values equals the expected string byte for byte |

### 8.5 `println(<relation text>)` as a last step (4)

**There is nothing to take over: lite never makes the text.** `println` (and `print`) is an inert diagnostic on both
doors: "no statement, the argument never evaluated". The code:
- `planner/ProductStatements` answers the statement as a constant with no value, and the test road does the same;
- `testplan/BodyDriver`'s `println` arm answers `TypedSide.Scalar(null, "String")`;
- `resolver/StoreResolver` passes the statement through unresolved;
- `lowering/Scalars` writes a `println` inside an expression as `NULL`.

So under B2 the 13 `println` nodes produce no text of any kind, and no verdict reads them. Treating
`println(<relation text>)` as a last step would change nothing at the door: the takeover would have no rows to print.
Their 5 tests' outcomes come from their other asserts:
- 2 of them compare `toCSV` against `toCSV` at a ROOT, which the takeover covers;
- the other 3 hold no relation text in an assert.

**One record gap, for the owner.** legend-pure's `println` writes the text to its console; lite writes nothing.
Register row S128 records `print`'s inertness (R18), but no row names `println`. A divergence that is not a row is a
defect by the upstream-fidelity tenet. A one-line amendment to S128 (or a row of its own) would close it.

## 9. `toCSV`'s layout, read from the pinned sources (the owner's second follow-up)

**Whose function it is.** `toCSV` is legend-engine's, not legend-pure's: `meta::relational::tests::csv::toCSV`, over a
legacy `TabularDataSet`, in legend-engine 4.145.0's
`legend-engine-xt-relationalStore-core-pure/.../core_relational/relational/helperFunctions/helperFunctions.pure`
(below "hf"). Its three overloads chain:
- `toCSV(t)` is `toCSV(t, false)` (hf:198-201);
- that is `toCSV(t, SimpleDateTimeFormat(), ISO8601DateFormat(), renderTdsNull)` (hf:203-206);
- whose body is hf:208-212.

The date formats are legend-engine's `core/pure/corefunctions/dateExtension.pure` (below "de"). legend-pure 5.99.0
supplies `joinStrings` and a Float's text.

| item | the rule | source |
|---|---|---|
| **the header line** | each column's name through `toCSVString` (so escaped as a cell is), joined by `,`, then `\n` | hf:210 |
| **the separator** | `,` between cells, and nothing around it | hf:210-211 |
| **line ends** | each row's cells joined by `,`; the rows joined by `joinStrings('', '\n', '\n')`, so a `\n` after EVERY row, the last included. With no rows the text is the header line and a lone `\n` (`"id\n\n"`), because `joinStrings` of an empty list is its prefix and suffix | hf:211; legend-pure `platform/pure/essential/string/toString/joinStrings.pure:72` (`'[]'` for an empty list); lite's `RenderCsvIntegrationTest.emptyRelationRendersHeaderAndBlank` pins `"id\n\n"` |
| **an empty (null) cell** | a `TDSNull` prints as nothing (`''`), or as `TDSNull` under `toCSV(t, true)`. An empty String prints as nothing too, so the two cannot be told apart | hf:232 |
| **quoting and escaping** | a cell whose text holds `,`, `"`, `\n` or `\r` is wrapped in `"`, each `"` inside doubled. Nothing else is escaped. Applied to every non-date cell, to a date-time and to the header's names; NOT to a date without an hour | hf:245-250 (`escapeCSVString`); applied at hf:234 and hf:241, not at hf:242 |
| **a Float cell** | `$a->toString()`, the value's own text: its canonical `BigDecimal` (at least one fraction digit, trailing zeros dropped) printed `toPlainString()`, never with an exponent (`1.65`, `3.0`, `9.812850678733032`) | hf:234; legend-pure `m4/.../primitive/FloatCoreInstance.java:29-46` (`canonicalizeBigDecimal`), `simple/SimpleFloatCoreInstance.java:32-39` (`getName`) |
| **a Date cell (no hour)** | `format('%t{yyyy-MM-dd}', d)` (`ISO8601DateFormat()`), not escaped | hf:233, hf:238-243 (`formatDateTime`, `hasHour()` false); de:389-392 |
| **a DateTime cell (with an hour)** | `format('%t{yyyy-MM-dd HH:mm:ss}', d)` (`SimpleDateTimeFormat()`), then escaped: a space, not `T`; whole seconds, subseconds dropped; no zone | hf:233, hf:238-243 (`hasHour()` true); de:384-387 |

**Against `toString`'s cells:**
- **A Float cell:** `toCSV` prints exactly the value's `toString()` (hf:234). That is also the text a relation's
  `#TDS` `toString` gives a Float cell: H11's table, `TestTDS.java:235-258` with the same `canonicalizeBigDecimal`.
- **Date and date-time cells differ.** `toCSV` formats them with the two `%t` patterns: `2014-12-01` and
  `2014-12-01 13:00:00`. A relation's `toString` prints every date with three subsecond digits and `+0000` (H11's
  table, `TestTDS.java:201-212`), and a DateTime's own `toString` is its ISO text with `T`.
- **A null differs.** `toCSV` prints nothing (or `TDSNull`).
- **A Boolean, Integer, String or enum** prints its `toString()` in both, CSV-escaped in `toCSV`.

**A TDS's `csv`** is a different thing: a stored `String` property of legend-pure's `TDS<T>` class (legend-pure
`platform_dsl_tds/tds.pure:16-19`), with no Pure printer. Its text is whatever built the TDS: a `#TDS` literal's own
text, or for an execution result the engine's. Its one corpus use, `testEnumInRelation` (`AlloyOnly`), expects a
header joined by `, ` and no line end after the last row. That is unlike `toCSV`.

## 10. A draft amendment to register row S128, for the user's word (`println`)

S128 records `print`'s inertness (R18). No row names `println`. The draft, one line appended to S128's lite column:

> AMENDED 2026-11-19 (H15 §8.5; for the user's word): `println` likewise. legend-pure's `println`
> (`platform/pure/essential/io/println.pure:17-26, 43-62`, `<<PCT.platformOnly>>`) writes its value and a newline to
> standard output, through `print`. Lite plans it as no statement, its argument never evaluated, on both doors
> (`ProductStatements`, `BodyDriver`'s `println` arm, `StoreResolver`; `NULL` inside an expression, `Scalars`), and so
> writes nothing.
