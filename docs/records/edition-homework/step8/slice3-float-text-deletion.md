# Step 8, slice 3: the Float text SQL is deleted (2026-11-26) -- BUILT AND MEASURED, NOT LANDED

*Written by the step 8 session (`claude/edition-step8`). Built on `claude/edit-ahead` at `b7c39067` (merged into the
step's branch as `9846cc91`, after slice 2 at `83263642`); the slice is `9846cc91..a25238ed` plus the review's fixes and this record. It
lands after slice 2 has landed, as a clean merge onto the base the orchestrator names, and after the user has seen
the register row below.*

## What it is

Tenet 1 as the user amended it on 2026-11-25, applied to a Float's text at the compatibility doors:

- **a print at a last step is Java's**, written with upstream's text from the double the database computed (slice 2:
  the value lift and the tail, `slice2-value-lift-and-tail.md`);
- **a print inside a store query at legend-engine's door is a row of legend-engine's table** (the user, 2026-11-26:
  "agreed on float/date text for engine");
- **a print the program goes on computing with is lite's own text**, written by the database (the user, 2026-11-26,
  about 13:57: "yes that's right but remember we have a list of chained functions that are okay after that you came
  up with").

So the SQL that existed only to build legend-pure's Float print inside a query has no reader left, and goes: from the
product, and from the edge.

## What changed

**The row** (the orchestrator, 2026-11-26: "the row is yours to write"):

- `lowering/NamedRule`: `FLOAT_TEXT_BY_CAST` and `FLOAT_TEXT_BY_CAST_OF_A_DOUBLE`, named by what they write.
- `lowering/NamedRules`: both lower to the database's cast of the operand to text. A finite Float LITERAL operand is
  written as legend-engine's literal processor writes it: its shortest digits as an exact numeric, bare, and on the
  databases of `PureText.ENGINE_FLOAT_LITERAL_AS_DOUBLE` (H2) those digits cast to a double. The digits are read from
  the literal as written; nothing is computed from a row (`shortestDigits`, pinned equal to the result writer's own
  text of a double by `NamedRulesFloatTextTest`; negative zero is left out there, an exact numeric has none).
- `edge/EngineTable`: `toString` of a Float answers the row on every database of `PureText.ENGINE_DATABASES`, inside a
  store query alone (a call the planner stamped as outside one has no row, as for every row of the table).
- Evidence for the row: legend-engine 4.145.0 writes `toString` as `cast(%s as varchar)` on every database
  (`docs/records/edition-homework/lanes/engine-templates-2026-11-26.md`), and the three recorded tests that hold it:
  PCT's legend-engine lane on DuckDB (`testFloatToStringWithNegativeExponent`), on H2
  (`testFloatToStringWithPositiveExponent`, `1.3421E8`, failing as legend-engine's H2 does), and the corpus's
  validation showcase (`Quantity not in range: 1000000`).
- **No `format` row.** legend-engine 4.145.0 has no `format` in a store query's SQL, so there is nothing to match;
  `format`'s `%s` and `%f` of a Float inside a store query is lite's text (agreed with the orchestrator).

**Deleted at the edge:**

- `edge/PurePrintForms`: the Float arm of the pass, `FloatLiteral` and `floatOf`. The pass now rewrites nothing for a
  Float.
- `edge/PureText`: the twelve `floatToText` overrides.
- `edge/EdgePureOnlyTexts`: the Float branch of legend-pure's `%r` and `toRepresentation`, and `heldAsInteger`. A
  Float's `%r` used further is lite's own text.

**Deleted from the product:**

- `sql/dialect/FloatShortestText.java` (184 lines), `SqlFn.FLOAT_SHORTEST_TEXT` and `SqlFn.FLOOR_RAW`, their arms in
  the twelve dialects (Databricks's own `CASE` too), the hooks `spellsFloatShortestText` and `readsLongTextOnce`,
  `floatShortestTextByCast`, Snowflake's `floatShortestText` and `javaNonFinite`, BigQuery's override, and the two
  names' entries in `SqlTyping`, `MySql`, `SQLite`, the may-skip set and `AnsiSqlRenderer.alreadyWhole`.

**Lines, main sources:** 483 out, 139 in, over 24 files (the product's `sql/` 303 out and 20 in; the edge 177 out and
73 in; the lowering 46 in for the row). Slice 1's objection is gone with it: `FLOOR_RAW` has no reader, so no guard
needs an exception, and Snowflake's and BigQuery's SQL gets shorter, not longer.

## What was measured

On the scratch checkpoint `a25238ed` (the base `b7c39067`), 2026-11-26. Every recorded answer file was re-recorded
(`//pct:update_edge_contract`, `//spec:update_edge_corpus`, `//core:update_edge_pure_v1`, `//core:update_edge_cases`,
`//pct:update_engine_lane`), the diff read line by line and the files restored; then the lanes were run.

**Answers: no Float text line moved, anywhere.**

- pure/v1's answers and the edge cases: no line moved.
- PCT's contract files, the corpus's host answers and the engine lane: the lines that moved are the base's, not this
  slice's. The base's recorded files are behind its own code: `testArcCosineError` and `testArcSineError` (four
  PCT files), the `dateDiff` SQL texts and `testToSQLStringSplitPart` (the corpus's two files), `testDateDiffWeeks`,
  `testSplitPartEmptyToken`, `testDecimalToDecimal` and `testDoubleToDecimal` (the engine lane). The three tests
  that hold the row did not move: the engine lane's `testFloatToStringWithNegativeExponent` (DuckDB) and
  `testFloatToStringWithPositiveExponent` (H2), and the corpus's validation showcase.
- Pass: `//pct:pct_duckdb` (5 suites), `//pct:pct_h2`, `//pct:pct_sqlite` (5 suites), `//pct:pct_channel_b`'s
  legend-pure lanes (essential, grammar, standard, relation, unclassified, H2's relation) and its lite lanes
  (standard, relation, unclassified), both corpus verdicts (`//spec:corpus_duckdb_verdict`, `//spec:corpus_h2_verdict`),
  `//core:core_tests_integration`, `//spec:spec_tests`, the two PCT route tests.

**Six targets fail, and fail on the base without this slice** (run on `9846cc91` the same hour, the two logs
compared line by line): `//core:lean_sql_baseline_test`, `//pct:pct_channel_b_lite_essential` (its judge characters
ratchet, 904295 against 904191), `//pct:pct_channel_b_lite_grammar` (two ledger lines), `//spec:lean_sql_corpus_duckdb`,
`//spec:lean_sql_corpus_h2` and `//spec:update_rcorpus_duckdb_0_test` (the roster's `testToSQLStringSplitPart`). Four
of the six fail with the very same lines on both.

**A SIZE PIN THIS SLICE GROWS (stopped here, reported; the user's word is needed).** The two corpus size tests each
show three more lines with the slice than on the base, all of one kind, "plan JSON chars":

| Family (its largest case) | DuckDB | H2 | Growth |
|---|---|---|---|
| `meta::pure::tds::tests::extensions` (`columnValueDifferenceTest`) | 299546 to 299585 | 312811 to 312850 | +39 |
| `...aggregationAware::testRewrite::objectGroupBy` | 133221 to 133299 | 140832 to 140910 | +78 |
| `...validation::showcase::standalone` (`validateAllConstraints`) | 243214 to 243331 | 249280 to 249397 | +117 |

The SQL's own characters (`chars`, `repeated chars`) did not move for these families on either lane: the statements
are the same text. What grew is the plan's JSON spelling of the tree (`executionplan.MirJson`), by 39 characters for
each Float `toString` inside a store query (one, two and three of them): before, the plan's tree held the product's
node (`{"fn":"FLOAT_TO_TEXT","args":[x]}`) and the edge's dialect re-spelled it as the cast when it wrote the SQL;
now the tree holds the cast itself (`{"value":x,"target":...,"reason":"user"}`), which is what the SQL always was.
A cast has no shorter spelling in a plan. Total: 234 characters on each lane, of about 676 thousand in these three
families. Not measured: the same pin on `//core:lean_sql_baseline_test` (channel A's product plans) shows no line of
this slice's (its lines are the base's, the same on both).


## The difference it makes, with no recorded test on it

1. **At legend-pure's door, a Float's text used further is lite's own**: six decimals, trailing zeros trimmed, a
   whole value keeping one. The register row below. A text followed only by the tail's functions (`replace`,
   `length`, `trim`, `startsWith`, `endsWith`, `contains`, `indexOf`, with literal arguments) is still a last step and
   keeps legend-pure's text.
2. **At legend-engine's door (pure/v1), a Float's `toString` outside any store query** (code the planner stamps as
   legend-engine's own Java, whose meaning is Pure's) is lite's own text too. Before this slice it was the database's
   cast (`0.30000000000000004` for `0.1 + 0.2` on DuckDB, `1.0E-5` forms on H2); legend-engine's own answer there is
   legend-pure's print. So before and after, lite differs from legend-engine at this place on some inputs; the text
   that differs has changed. Such a text is always used further at that door: pure/v1 answers no scalar today
   (`execute: a SCALAR result's serialization is unprobed`, the recorded case `value` of `pure_v1_requests.tsv`;
   `EngineAnswers.engine` refuses a VALUE shape), so there is no last-step print for that door to lift. Read
   2026-11-26 for the slice the orchestrator named "pure/v1's last-step lift for a stamped print": it has nothing to
   lift until pure/v1 writes a scalar answer, which is its own decision (the JSON form is unprobed). No recorded
   pure/v1 request holds a Float's `toString` or `format` (17 cases read).
3. **Inside a store query at legend-engine's door, three texts that were the cast are lite's own** (found by the
   independent review, 2026-11-26; none has a recorded test, none is measured on a server lane):
   - on a database legend-engine does not have (SQLite, MariaDB, MySQL, MongoDB) the row does not answer, as every
     row that is legend-engine's by what it writes (`PureText.ENGINE_DATABASES`): there is no legend-engine answer
     to match there;
   - `format`'s `%s` and `%f` of a Float: legend-engine has no `format` in a store query's SQL (no row, above);
   - the `toString` of a Float reached through a Number or an Any (a mixed selection's candidate,
     `Scalars.pureToString` to `MixedEncoding.perCandidate`): the row reads the operand's declared type, Float
     alone. legend-engine writes `cast(%s as varchar)` for it too, so this one IS a new difference from
     legend-engine inside the ruling's words; covering it means asking the table inside the mixed path. The
     orchestrator, 2026-11-26: recorded as open and not built in this slice; the mixed path is reworked in step 6
     (run-time types), which owns it (the plan record's carried-forward list).
4. **SQLite's `toRepresentation(1.0e20)`** was `9223372036854775807.0` at the edge (the deleted composition cast a
   whole value through a 64-bit integer). It is `100000000000000000000.0` on every lane now, pinned by
   `PureOnlyTextTest.aFloatsRepresentationUsedFurtherAtTheEdgeIsLitesOwnText`.

## The register row (numbered when the slice lands; the user's words are in it)

| # | Kind | The difference | Lite's answer | Evidence |
|---|---|---|---|---|
| S(next) | outcome (a Float's text used further at a compatibility door; RULED by the user 2026-11-26, "yes that's right but remember we have a list of chained functions that are okay after that you came up with", under tenet 1 as amended 2026-11-25) | legend-pure writes a Float's `toString`, `format`'s `%s`, `%f` and `%r` and `toRepresentation` by its own print wherever the call stands: every digit, never an exponent (`CompiledSupport`, `DecimalFormat("0.0")` with up to 340 fraction digits). | At legend-pure's door (PCT's legend-pure run, the corpus's test code outside `execute()`), a Float's `toString` at a LAST STEP (a query's last statement, or an assert's own argument, with a tail of `replace`, `length`, `trim`, `startsWith`, `endsWith`, `contains`, `indexOf` over literals) is legend-pure's, written in Java from the double the database computed. A Float's text the program goes on computing with is lite's own, written by the database: six decimals, trailing zeros trimmed, a whole value keeping one. So `format('%r', [1.0/3.0]) + ';'` is `0.333333;` where legend-pure writes `0.3333333333333333;`, and `[(0.1 + 0.2)->toString()]` holds `0.3` where legend-pure's holds `0.30000000000000004`. Inside a store query at legend-engine's door a Float's `toString` is the database's cast, a row of legend-engine's table (the user, 2026-11-26: "agreed on float/date text for engine"), unchanged on every database legend-engine has (H2, DuckDB, Postgres, Trino, Snowflake, ClickHouse, Databricks, BigQuery). Three texts there that were the cast too are lite's own now: on a database legend-engine does not have (SQLite, MariaDB, MySQL, MongoDB), where there is no legend-engine answer to match; `format`'s `%s` and `%f` of a Float, which legend-engine has no SQL for; and the `toString` of a Float reached through a Number or an Any, where legend-engine writes the same cast and the row, which reads the operand's declared type, does not answer: lite's text there, legend-engine's the cast, owned by the edition program's step 6 (run-time types), which reworks the mixed path (no test holds it). Outside a store query at legend-engine's door (pure/v1, code that is legend-engine's own Java) a Float's `toString` is lite's own text, where legend-engine writes legend-pure's print; it is always used further there, pure/v1 answering no scalar today. | Measured 2026-11-26 (`docs/records/edition-homework/step8/moves-if-float-text-used-further-is-lites.md` and `slice3-float-text-deletion.md`): no recorded answer of PCT (both channels; DuckDB, H2, SQLite), the corpus (DuckDB, H2), pure/v1 or the edge cases moves. The examples are worked from the two texts' rules, not run. The server and cloud lanes are not measured. |

## The line for `docs/DIFFERENCES_FROM_LEGEND_ENGINE.md` (a new numbered section when the slice lands)

> ### N. A Float's text used further in a program is lite's own (the edition program's step 8, 2026-11-26)
> - **You write** a Float's `toString()`, or `format` with `%s`, `%f` or `%r`, or `toRepresentation()`, and go on
>   computing with the text (join it, put it in a list, compare it).
> - **legend-pure and legend-engine's own Java write** every digit of the double: `0.30000000000000004` for
>   `0.1 + 0.2`, `0.000000013421` for `1.3421e-8`.
> - **lite writes** its own text, computed by the database: six decimals, trailing zeros trimmed, a whole value
>   keeping one (`0.3`, `0.0`).
> - **Unchanged:** a `toString` that is the program's last step, or is followed only by `replace`, `length`, `trim`,
>   `startsWith`, `endsWith`, `contains` or `indexOf` with literal arguments, is legend-pure's text at legend-pure's
>   door (PCT, the corpus's test code). A `toString` inside a store query at legend-engine's door is the database's
>   cast, as legend-engine writes it.
> - **At legend-engine's door**, a Float's `toString` in code outside a store query is lite's text too (it is always
>   used further there: that door answers no scalar today). Inside a store query, `format`'s `%s` and `%f` of a
>   Float, and the `toString` of a Float held in a Number or an Any, are lite's text, where the `toString` of a
>   Float is the database's cast.
> - **Ruled:** by the user on 2026-11-26: "yes that's right but remember we have a list of chained functions that are
>   okay after that you came up with"; "agreed on float/date text for engine".
> - **Measured:** no recorded PCT, corpus, pure/v1 or edge-case answer moves (DuckDB, H2, SQLite).
> - **Register:** S(next).

## The guards it moved, each with a dated note in the file

- `SqlFnSortedTest`: `FLOAT_SHORTEST_TEXT` and `FLOOR_RAW` out of PRODUCT; `FLOAT_TO_TEXT` out of the functions both
  sides write; `FloatShortestText` out of the writer lists. Shrinks only.
- `EdgeSpellingHookTest`: the edge dialects' override pins lose `floatToText` (H2's is `spellingPasses` alone).
- `ArchitectureTest.PLANNER_ENTRIES`: the two new `NamedRule` constants, as every row's rule is listed.
- `EdgeOnlyNodesCensusTest` (a comment), `SnowflakeDialectTest`, `BigQueryDialectTest`, `MongoDbPlanTest`,
  `RaiseTextTest`: the tests of the deleted names go, or name another node where the name was only an example.
- Generated: `native-claims.tsv`; `docs/dialect-capability/*`. **The dialect table regenerated on `b7c39067` also
  carries lines that are the base's, not this slice's** (`CAST_INT`, `CAST_FLOAT`, `ACOS_IN_DOMAIN`, `ASIN_IN_DOMAIN`,
  `DECIMAL_RAW`, `ROUND_RAW`'s kind): the base's table was behind its code. This slice's own lines there are the two
  deleted names' rows, one per dialect. To be regenerated again on the landing base and read the same way.

## The tests

- New: `edge/EngineFloatTextRowTest` (the row's SQL planned for every implemented database, read from `PureText`'s
  sets, no database named; a Float held as an integer is the very statement that prints the column as an Integer),
  `lowering/NamedRulesFloatTextTest` (the literal's digits; the rule itself: an operand that is no finite literal
  cast as it is, a finite literal by its digits, read as a double first under the one rule alone),
  `PureOnlyTextTest.aFloatsRepresentationUsedFurtherAtTheEdgeIsLitesOwnText`.
- Re-pointed, each with a dated note: `FloatTextTest` (a Float's text in an edge query is lite's own; the window
  test's Float cells; the integer-held test removed), `AuditRound3Test.floatReprLargeBand`,
  `TypeInferenceIntegrationTest.testFloatToStringWithNegativeExponent` (`0.0` in an edge query that is used further).

## Not this slice's

- Three tests fail on the base without this slice: `ArchitectureTest.protocolNodesAreConstructedOnlyByTheParserAndTheNormalizer`
  and `callsAreBuiltOnlyByTheBuilder` (fixed on `claude/edit-ahead` at `9d880deb`, after the base), and
  `PureV1SqlTextTest.anOrdinaryRequestIsUntouchedAndASqlTextRequestAnswersLitesSql` (the orchestrator's).
- The DATE's row and `TDSNull`'s row, pure/v1's last-step lift, `+` in the tail, and a last-step `format` lift are
  later slices.
