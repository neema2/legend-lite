# Step 8: the repair of slice 2 after its independent audit (2026-11-26)

*Written by the step 8 session (`claude/edition-step8`). The audit is
`docs/records/edition-homework/audits/step8-slice2-independent-2026-11-26.md` on `claude/edit-ahead`: a reviewer
outside this session read slice 2 as it sits in the next batch (`d5080e3f`), found no blocker and seven should-fix
(S1 to S7) with eight notes (N1 to N8). The orchestrator asked for one repair commit set on the pull request's branch
(`claude/modest-galileo-h6swr4`, `4d44bf54`), merged into this branch as `90d2759b5`. The repair is the commits
after that merge: `90d2759b5..` to the commit this record lands in.*

## What the repair changes

| The audit's item | What was wrong | What the repair does |
|---|---|---|
| S3 (confirmed) | `PureRelationTextWriter.end` applied the print's tail and handed back an Integer or a Boolean: a result writer computing an answer the plan never typed. | The writer takes no tail and hands back the text alone. The DOOR that lifted the print applies the tail to it: `PrintedRelations.Lifted.answer` and `Side.answer`, called by `PctPlanRoute` (PCT's channel A) and `UpstreamPrints.PRINTER` (channel B, the corpus). |
| S3, the guard (N2) | `WriterCarriageGuardTest` listed `PrintTail.java` among the writers it scans, but its vocabulary sees none of what the tail computes. | `PrintTail.java` leaves that scan (it is no writer's part), dated. A new test, `WriterCarriageGuardTest.noWriterAppliesAPrintsTail`, fails if any file of the result library, of exec, or of the edge's writers and printers names the tail. What holds the tail itself is unchanged: its closed set is pinned (`PrintTailTest`), and it is the edge's one registered class that computes a value (`JavaEvalLedgerTest`; the user, 2026-11-26: "yes should add guard"). |
| S2 (plausible) | At a test body's door a printed side is read as a query of its own before the body runs. For a value, the order guard was off, so a value from an unrepeatable source was read twice. | A value's print is not lifted there where a second reading may answer another value: its operand, through its lets, calls `now()`, `today()`, `random()` or `generateGuid()`, or holds a node whose order view is INCIDENTAL (`compiler.spec.OrderView`: rows of a store or of an execution frame with no sort, or past a grouping, a join, a union or a pivot). It is left as written, the database's. PCT's channel A needs no such rule: there the lifted program is the one query, run once. |
| S6 (mechanism confirmed) | A Float LITERAL's last-step print went to the database as a literal and came back as a double: each database's parse of the literal reached the text. | The door writes a literal's print itself, from the typed literal (`PureValueText.pureFloat` of its double), where it stands or through the let it is bound to. Nothing is run for it. A computed value is still the database's to compute. |
| S5 (plausible) | At legend-engine's door a relation's print took the whole tail to Java: `indexOf` moved from the door's own rule (the database's position, register S134) to Java's, `trim` and `length` too, on a reason that was recalled and never asked of legend-engine. | Restored. legend-engine's door lifts a relation's print with its `replace`s alone (the user's ruling of 2026-11-24, "also do the replace in java too"), as before slice 2; a print followed by another call of the set is left to the run there. The engine runner was NOT asked: if the tail is wanted at that door, that question goes to it first. |
| S7 | The tests held easy inputs; the parity test compared with the product's SQL and skipped the inputs where Java and SQL part. | Below. |
| N1 | The record's "no recorded answer moved". | Corrected in `slice2-value-lift-and-tail.md`: two H2 PCT pins of expected failures began to pass, and why the measurement missed them. |
| N8 | `PrintTail`'s javadoc credited the typer with a guarantee that `PrintTail.beneath` gives. | Corrected. |

## S1 and S4: read, not changed

**S1, "two printers in one body".** This is the design the user ruled on 2026-11-26 ("yes that's right but remember
we have a list of chained functions that are okay after that you came up with"; register S428): legend-pure's text
at a last step, written in Java; lite's own text where the program goes on computing. What the audit saw at
`d5080e3f` was the state between slice 2 and the deletion slice: the middle of a chain still printed the database's
cast. With the deletion slice (on this branch) the audit's own examples answer, run on DuckDB
(`UpstreamPrintsTest.aPrintAtALastStepAndTheSamePrintUsedFurtherInOneBody`):

- `let x = 1.0E-5 * 1.0; assertEquals($x->toString()->length(), $x->toString()->length() + 0)`: 7 and 7. Passes.
  (At `d5080e3f`: 7 against the cast's 5.) Lite's text of a Float is one text on every database, so H2 answers the
  same.
- `let x = 12345678.5 * 1.0; assertEquals('v=12345678.5', 'v=' + $x->toString())`: passes (the cast on H2 wrote
  `v=1.23456785E7`).
- Where the two still part, by the row: past six decimals. `x = 1.0E-7 * 1.0`: at a last step `0.0000001`, 9; used
  further `0.0`, 3.

**S4, one function, two meanings one step apart.** The tail's `trim`, `length` and `indexOf` are Pure's (Java's);
the same call one step away from a print is lite's SQL. No register row covers what lite's SQL does there: the four
divergences below are the PRODUCT's, older than step 8, and the tail made them visible. They are drafted for the
user in the last section; none is added to the register, because a row the user did not rule is a defect too.

## What each lane's database answers where Java and lite's SQL part (measured 2026-11-26)

Run at legend-pure's door (the edge's own plan of the call) on each lane's database; Java's column is legend-pure's
own Java, run on the build's JDK. Held by `PrintTailParityTest.whereJavaAndLitesSqlPart_eachIsHeld`.

| The call | Java (legend-pure) | DuckDB | H2 | SQLite |
|---|---|---|---|---|
| `'a😀b'->length()` | 4 | 3 | 4 | 3 |
| `'a😀b'->indexOf('b')` | 3 | 2 | 3 | 2 |
| `'abc'->replace('', 'x')` | `xaxbxcx` | `abc` | `abc` | `abc` |
| `'\ta\n'->trim()->length()` | 1 | 3 | 3 | 3 |
| `'abcb'->indexOf('b', 2147483648)` | 1 | -1 | refused by the database ("Numeric value out of range") | -1 |
| `'abc'->indexOf('')`, `indexOf('', 5)` | 0, 3 | the same | the same | the same |
| `startsWith('')`, `endsWith('')`, `contains('')` | true | true | true | true |
| `'abcb'->indexOf('b', 99)`, `indexOf('b', -5)` | -1, 1 | the same | the same | the same |

## The tests the audit asked for

- `PrintTailTest.whereADatabaseAnswersOtherwiseJavasIsLegendPures`: the tail's listed cases (a character above
  U+FFFF, the empty needle in each member, a control character and a no-break space in `trim`, a start past an int).
- `PrintTailParityTest`: each member now also against the EDGE's own plan of the call (what legend-pure's door
  answered before the tail was lifted), not the product's alone; and the table above.
- `PureRelationTextTest.aFloatsValuePrintIsLegendPuresOnTheBuildsJdk`: `-0.0`, `1.0E7`, `1.0E-4`, `1.0E15`, `1.0E16`,
  `9007199254740993.0`, `1.7976931348623157E308` (309 digits and `.0`), `4.9E-324`, `1.0E23`, `2.0E23`,
  `0.1 + 0.2`, NaN, the infinities, and a Float carried as a Long, an int, a BigInteger and a Decimal.
- `UpstreamPrintsTest`: a value from an unrepeatable source is left (`random()` read twice; `random()` under an
  arithmetic; an `at(0)` past a grouping); a literal's print is written with nothing read; the S1 bodies.
- `PrintedRelationsTest`: a literal's print at the door, through a let too; legend-engine's door's rule, member by
  member.
- `PureRelationTextTest.theWriterPrintsAValue_andTheDoorAppliesTheTail`,
  `WriterCarriageGuardTest.noWriterAppliesAPrintsTail`.

Not added: an assert under the right of `||` whose Float side raises (the audit's last behaviour case). The
pre-read of such a side does raise where Pure would short-circuit; it is the same for a printed relation's side
since B3 step 2b, and is listed below for the orchestrator rather than changed here.

## The JDK (the audit's N6)

- The build pins `remotejdk_25` (`.bazelrc`: `--java_runtime_version=remotejdk_25`); the texts above were run on
  25.0.4.
- legend-pure 5.99.0 compiles for Java 8 and its build accepts JDK 11, 17, 21 or 25 (`pom.xml`:
  `java.version.range`). Its print of a Float is `DecimalFormat("0.0")`, so legend-pure's own answer for some doubles
  is whichever JDK it runs on.
- One fact for the user: `2.0E23` prints `199999999999999980000000.0` on JDK 25.0.4 (run here). The audit gave two
  other texts for it from memory of other JDKs. The test holds lite's print to the build's JDK, value by value; a
  JDK bump that changes a digit turns it red.

## What was measured

On the repair's tree (the pull request's branch `4d44bf54` merged, the deletion and date-time slices, then the
repair), 2026-11-26:

- **No recorded answer moved.** All five recordings were re-run (`//pct:update_edge_contract`,
  `//spec:update_edge_corpus`, `//core:update_edge_pure_v1`, `//core:update_edge_cases`, `//pct:update_engine_lane`)
  and left no file changed: PCT's contract on DuckDB and SQLite, channel B's outcomes, the corpus's answers on both
  lanes, pure/v1, the edge cases, the engine lane. So the narrower lift (S2), the literal written at the door (S6)
  and legend-engine's door's rule (S5) change no recorded answer; a Float literal's print is the same text whether
  the database handed the double back or the door wrote it.
- **Pass (108 of 109 targets):** `//gates:pct_h2` (H2's five PCT suites), `//pct:pct_duckdb`, `//pct:pct_h2`,
  `//pct:pct_sqlite`, `//pct:pct_channel_b`, `//pct:pct_channel_b_h2`, both corpus lanes, the three SQL size pins,
  `//core:core_tests_integration`, `//spec:spec_tests`, `//core:guardrails`, `//core:guardrails_edge`, `//core:census`,
  `//docs:all`, the edge tests on DuckDB, H2 and SQLite, the engine lane's tests, the answer-file checks,
  `//parser-equivalence:update_ratchets_test`.
- **The one failure, a generated ledger, regenerated:** `native-claims.tsv` (`bazel run //core:update_generated`):
  three lines, `today`, `random` and `generateGuid` now also named by `UpstreamPrints` (the rule of S2).
- **A measured count re-pinned:** parser-equivalence's `own_corpus.matched`, 3359 to 3368 (nine more of step 8's
  tests' model literals match the reference grammar), under the user's standing word of 2026-11-24, "parser bumps
  are always okay", with its dated line in `PeRatchets`.
- **No pin, ceiling or size went up** but that count.
- **Not measured here:** the server and cloud lanes; MongoDB after the repair (its lane was run before and after
  the date-time deletion, `slice4-date-time-lift-and-row.md`; the repair touches no dialect).

## Drafted for the user (not in the register: none is ruled)

Each is a behaviour of lite's PRODUCT that differs from Pure's definition and has no register row. Found by the
tail's parity tests; measured on DuckDB, H2 and SQLite on 2026-11-26.

1. **`trim` strips spaces alone.** Pure's `trim` is Java's `String.trim`: every character up to U+0020 at either end
   (a tab, a line break). Lite's SQL `trim` strips spaces: `'\ta\n'->trim()` keeps its tab and its line break on all
   three databases; `'id,name\n1,A\n'->trim()` keeps its last line break.
   - Choice A, follow Pure: trim that set of characters, one native call per value on the databases that take a
     character set (`TRIM(BOTH chars FROM x)`), which is proportionate; to be read per database.
   - Choice B, record lite's answer as a row.
2. **`length` and `indexOf` count as the database counts.** Pure counts UTF-16 units. They part only on a character
   above U+FFFF: `'a😀b'->length()` is 3 on DuckDB and SQLite, 4 on H2 (and in Pure); `indexOf('b')` 2 against 3. So
   the product's own answer differs between databases here, which the row would have to say.
   - Choice A, follow Pure everywhere: a UTF-16 count is no native call on most databases (disproportionate SQL for
     a rare difference: the 2026-11-22 amendment's case for taking the database's native answer).
   - Choice B, record it, with the databases' answers as a known quirk.
3. **`replace('', x)` leaves the text as it is**, where Pure puts `x` at every position (`'abc'->replace('', 'x')` is
   `xaxbxcx`). All three databases answer `abc`.
4. **`indexOf(s, from)` with a start past an int** is -1 on DuckDB and SQLite and refused by H2's own conversion,
   where legend-pure wraps the start (`intValue()`) and searches from the beginning. An artifact of legend-pure's
   implementation; a candidate for "lite's answer, recorded".
5. **A Float a store holds as an integer, printed at a last step at legend-pure's door, is `1000000.0`** (the audit's
   N4): Pure's meaning for a Float. legend-engine's own result carries the column's integer, which prints
   `1000000` in its corpus. A difference from legend-engine at that door with no row; no corpus case is known to
   meet it. (Inside a store query at legend-engine's door the text is `1000000`, the row of S428.)

Also for the orchestrator, not a row: the pre-read of a printed side raises where Pure short-circuits (`a || b` with
the print under `b`); and the audit's N7, the same print being Java's when written inline and the database's when
written in a helper function or bound by a let and then asserted unchanged.
