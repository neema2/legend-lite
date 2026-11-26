# Step 8: what moves if a DateTime's text at the edge is lite's own (measured 2026-11-26)

*Measured by the step 8 session on `claude/edition-step8` at the scratch checkpoint `5a3c87e3d` (the Float deletion
slice on `claude/edit-ahead`'s `b7c39067`), for the orchestrator, who takes the list to the user. Nothing is built.
The same method as the Float's (`moves-if-float-text-used-further-is-lites.md`).*

## What the edge writes today

- **A DateTime literal's text** is its written text and `+0000`, a constant the edge's lowering writes as a String
  literal (`edge/EdgeDateTimeTexts`, the two-form hook `lowering/DateTimeTexts`; legend-pure prints a literal at its
  written precision). No SQL computes it.
- **Any other DateTime's text** is the product's node `DATETIME_TO_TEXT`, which a pass of the edge's twelve dialects
  rewrites to legend-pure's print composed in SQL (`edge/PurePrintForms`' date arm, `edge/PureDateTimePrint`: the ISO
  text, the fraction with its trailing zeros trimmed, `+0000`).
- Both at every compatibility door, inside a store query at legend-engine's door too, where legend-engine's own
  template is `cast(%s as varchar)`.
- A StrictDate's `toString` has no edge SQL: the product's text is legend-pure's.

## The probe

Two edits at the edge, never committed (source: `probe-date-text-is-lites.patch.txt`): the pass no longer rewrites
`DATETIME_TO_TEXT`, and the edge's two-form hook answers as the product's does (the node, for a literal too). So every
DateTime text of every compatibility plan is lite's own, nine fraction digits and `Z`. It lifts nothing and has no
row, so it shows more than would move. Every recorded answer file was re-recorded, the diff read against the base's
own stale lines (known from the Float slice's measurement) and the tree restored; `//pct:pct_duckdb`,
`//pct:pct_h2`, `//pct:pct_sqlite`, `//pct:pct_channel_b`, `//pct:pct_channel_b_h2`, `//spec:corpus_duckdb`,
`//spec:corpus_h2` and `//core:core_edge_tests` were run on it.

## Everything that moved under the probe

Five PCT tests, **every case a DateTime LITERAL**:

| # | The test | The expression | Today | Under the probe |
|---|---|---|---|---|
| 1 | `string::tests::toString::testDateTimeToString` (4 cases) | `%2014-01-01T00:00:00.000+0000->toString()` and the same at two, one and no fraction digits | `2014-01-01T00:00:00.000+0000`, `...00.00+0000`, `...00.0+0000`, `2014-01-01T00:00:00+0000` | `2014-01-01T00:00:00.000000000Z` |
| 2 | `string::tests::toString::testDateTimeWithTimezoneToString` (8 cases) | a literal written with a zone, at four, three and one fraction digits and to the minute | `2014-01-01T00:00:00.0000+0000` ... `2014-01-01T00:00+0000` | the same `Z` text |
| 3 | `string::tests::toString::testSimpleDateToString` (1 case) | a literal to the millisecond | `2014-01-02T01:54:27.352+0000` | `2014-01-02T01:54:27.352000000Z` |
| 4 | `string::tests::format::testSimpleFormatDate` (2 cases) | `format('... %s', [a DateTime literal])` | `... 2014-01-01T00:00:00.000+0000` | `... 2014-01-01T00:00:00.000000000Z` |
| 5 | `string::tests::format::testFormatRepr` (case 002) | `format('... %r', [a DateTime literal])` | `... %2014-01-01T00:00:00.000+0000` | `... %2014-01-01T00:00:00.000000000Z` |

Where each is held:

- **PCT channel A, DuckDB and SQLite** (`duckdb_essential_answers.tsv`, `sqlite_essential_answers.tsv` and their
  outcome files): all five pass today, fail under the probe.
- **PCT channel B, DuckDB and SQLite**: the same five, PASS to ERROR.
- **PCT's legend-engine lane, DuckDB and H2**: tests 1 to 3 are `DEFECT WRONG_ANSWER` for legend-engine itself and
  PASS for lite today (`LITE_FIXES`); under the probe `BOTH_FAIL`. Tests 4 and 5 are `REFUSES` for legend-engine
  (it has no `format` in a store query) and PASS for lite today (`LITE_ANSWERS`); under the probe `BOTH_FAIL`.

**Nothing else moved.** No corpus line on either lane; no line of pure/v1's answers or of the edge cases; no other
PCT case. `//pct:pct_h2` passes under the probe (the legend-pure essential suite is not run on H2 on this base).
Two of core's edge unit tests fail under it, as they must: `TypeInferenceIntegrationTest.testDateTimeToString` (a
literal) and `FloatTextTest.anEdgeTableTextUsedFurtherWritesItsCellsAsTheEdgeWritesTheirValues` (its DateTime
cells, written to go red when the pass is deleted).

## What that means

1. **The SQL composition holds no recorded answer.** Every moved case is a literal, whose text is a constant the
   edge writes without any SQL. So `PureDateTimePrint` and the pass's date arm (about 50 lines, and the pass itself
   once `TDSNull` no longer needs it) can be deleted with no recorded answer moving, **provided a literal's text
   stays its written text**: the edge's two-form hook kept for the literal alone, or the same constant written at
   the door.
2. **What then changes, with no test on it,** is a DateTime that is NOT a literal (a column, a computed value):
   - at a last step at legend-pure's doors: nothing, if the value lift takes a DateTime's `toString` and Java prints
     the rule the SQL wrote (the fraction trimmed, `+0000`);
   - used further at legend-pure's doors: lite's own text, nine fraction digits and `Z`, where today it is the SQL's
     `+0000` text. The Float's ruling in the user's words was about a Float; this is the same tenet for a date and
     needs its own register row;
   - inside a store query at legend-engine's door: see 3.
3. **The row at legend-engine's door needs one decision.** The user ruled a date's text inside a legend-engine store
   query a row of legend-engine's table (2026-11-26, "agreed on float/date text for engine"). legend-engine writes
   `cast(%s as varchar)` (`2014-01-01 00:00:00` on DuckDB), and by PCT's own record it FAILS tests 1 to 3 with it.
   - **(A) The row writes legend-engine's cast for every DateTime, a literal too**, as the Float's row writes
     legend-engine's literal processor on H2 and fails `testFloatToStringWithPositiveExponent` as legend-engine's H2
     does. Then tests 1 to 3 move from `LITE_FIXES` to `BOTH_FAIL` on both engine lanes: six recorded lines.
   - **(B) The row answers a DateTime that is no literal; a literal keeps its written text.** No recorded line
     moves; lite goes on passing three tests legend-engine fails, by a text legend-engine does not write.
   - Either way tests 4 and 5 (`format`) keep today's answer only while a literal's text stays its written text
     inside `format`: legend-engine refuses `format`, so there is no row, and lite's own text of the literal would
     move them to `BOTH_FAIL` on the engine lanes and fail them on PCT's legend-pure run (channel A and B).
4. **Not measured:** the server and cloud lanes; legend-engine's cast text per database for the row (the lanes'
   reading has it for Snowflake and Databricks: "a timestamp keeps its microseconds").

## The slice this suggests (proposed, not built)

- Delete `edge/PureDateTimePrint` and the pass's date arm; MongoDB's `dateTimeFormat` override at the edge.
- Keep a literal's written text (the hook's literal branch).
- Widen the value lift to a DateTime's `toString` at a last step at legend-pure's doors, Java printing the trimmed
  rule, with a parity test against the SQL it replaces, run before the SQL is deleted.
- A row for a non-literal DateTime's `toString` inside a store query at legend-engine's door, on the databases whose
  cast text a lane has read; and (A) or (B) above for a literal, by the user's word.
- A register row for the text used further, posted before it lands.
