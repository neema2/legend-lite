# B2's progress record (from 2026-11-24)

The one session building all of stage B's B2 (B2a, then B2b), on the user's instruction of 2026-11-24 ("one extra
session for all of b2 ... at extra high effort ... make sure that session is doing interim audits and reviews at check
points"), relayed by the item 3 / B3 session. Branch `claude/compat-b2-nufdlk`. The plan: the plan of record's B2
(`docs/records/compat-stage-b-plan-2026-11-20.md` on `claude/compat-program`) and its B2a/B2b split
(`docs/records/compat-stage-b-plan-2026-11-24.md`). Every slice is measured first, independently reviewed before it is
pushed (a subagent that did not write the code, given the code and the measurements, not the conclusion), gated
(`//gates:local`, plus the lanes it touches), and recorded here.

## The rulings this work builds on

| Ruling | The user's words | What it decides |
|---|---|---|
| 2026-11-19 ~18:05 | "Agree with all four table printing recommendations" | lite's route prints a relation's `toString` in the database; a whole Float `2.0`; a date with a time lite's standard; it lands with B2 (no two-form switch) |
| 2026-11-19 ~18:12 | "Yes record it" | a last-step table text is legend-pure's, printed in Java from typed rows; a text used further is lite's, the difference recorded |
| 2026-11-19 ~18:36 | "Null is good" | a printed table's empty cell reads `null` |
| 2026-11-22 | "Yes always pretty print to iso nanos" | a DateTime's text has nine fraction digits (S312 amended) |
| 2026-11-23 | "I think we do it as 2.0" | Q10: trailing zeros trimmed down to one decimal place (S311 amended) |
| 2026-11-24 | (relayed) slice 2 held until B2b, option C | lite's route's table `toString` lands with B2b, with no temporary two-form facet |
| 2026-11-24 (relayed ~12:57) | "Agree on reword catalog - keep errors close to pure when cheap/fast so don't have to reword everything on compatibility" | the product's assert-failure catalog takes legend-pure's sentences where cheap (a B3 step): B2b builds NO edge-side rewording of assert words; S398's retirement mostly goes away, a few tweaks at most, named by the B3 session |
| 2026-11-24 (relayed ~13:43) | "Q6 fine", "Q7 fine" | Q6: the checked graph's violation sentence follows the catalog rule (legend-pure has none, so lite's own words; legend-engine's at the edge only where a compatibility test checks it). Q7: the defects cell dropped |
| 2026-11-24 (relayed ~13:43) | "I don't think we should keep adding random flags to plan? I thought we already have mechanism for this that substring/indexof use" | no new plan fact for Q8 or the string entry: the door (the A3 walker over the typed query, edge/PureMeaningRewrite, B3's UpstreamTestBodies) LIFTS an assert whose side is a relation's own toString/toCSV out of the body; the harness checks it afterwards through lite's public door with B2b's printer as the receiver (B3 builds the lift; B2b's S4 hands it the printer). On pure/v1 the string entry is the door's output receiver |
| 2026-11-24 (relayed ~13:43) | (the B3 session's direction) | F31, legend-engine's plain parse casts, moves into legend-engine's door rewrite (A3's pattern, EngineSubstitutes) when PurePrintForms goes, not to B4; the calendar's 41 toCSV-then-edit cases wait for the user's ruling after B3 step 1b (lite's text assumed; nothing depending on it lands) |
| 2026-11-24 (relayed ~13:51) | "I think toString and toCsV with more string functions like replace should also be all considered part of the toString rewrite if that's a good idea / not crazy - that might also fix these lost tests?" | in a compatibility test body, a side starting with a relation's own toString/toCSV followed by further string functions is lifted too: the harness prints the rows with B2b's printer and substitutes the text for the call (amends ~18:12 for compatibility test bodies only) |
| 2026-11-24 (relayed ~13:53) | "No I'm saying also do the replace in java too it's really just more string functions on the CSV" | a print followed by `replace` calls with literal arguments is one printing step in Java (the corpus's 41 calendar cases are all `toCSV(...)->replace('\n', ',')`): the printer's entry takes typed rows and a list of literal (target, replacement) pairs; anything else after a print falls back to the substituted text run as lite |
| 2026-11-24 ~06:20 | "Why are you asking questions like should I build oracle and raise limits? Could have been done ... if just kept building" | the direct, measured cost of a ruling already made is done and reported; only a new difference from upstream or a new semantics question goes to the orchestrator first |

## B2a

### Slice 1: the product bug B2's prototype exposed (verified, no code)

The window-inside-a-list lowering defect (H11 §8 P1): fixed on `main` before this work (`e201769df`, `bf2e9c4c3`, merged
in #128); `SelectMergeQueriedOperandsTest` green, PCT SQLite's `testExtendJoinStringOnNull` and
`testOLAPAggStringWithPartitionAndUnboundedWindow` pass in PCT channel A on SQLite and are in no channel A ledger
(2026-11-24). Channel B's SQLite outcomes pin the second as an ERROR of another cause (`regexp_extract_all`,
`edge/channel_b_sqlite_relation_outcomes.tsv`); Postgres was not run here.

### Slice 3: a whole Float keeps one fraction digit (`2.0`)

**The rule** (S311 amended, the row beside S311 in `docs/records/d-register-rows-2026-11-16.md`): six decimals, the
trailing zeros trimmed, a whole value keeping one (`2.0`, `0.0`, `100000000000000000000.0`). The database computes it:
each dialect's six-digit text has a whole value's `.000000` marked `.#` before the trailing trim and written `0` after
(`AnsiSqlRenderer.trimmedSix`, `trimmedSixStandard`); Snowflake from 1e32 and MySQL from 1e60 append `.0`; MongoDB
trims only a text with a point and ends an integer text in `.0`. A Float the store holds as an integer takes the same
text (`LiteralSpelling.floatPrint`); the edge's text of it stays legend-engine's cast.

**Measured (2026-11-24):**

| Where | What moved |
|---|---|
| core (`//core:core_tests`, the SQLite and H2 lanes) | 5 cases re-pinned: `FloatTextTest.aFloatsTextIsSixDecimalsTrimmed` (DuckDB, H2, SQLite), `TypeInferenceIntegrationTest.testFloatToStringWithNegativeExponent` (each lane), `AuditRound3Test.floatReprLargeBand`, `BigQueryDialectTest`, `SnowflakeDialectTest`; `MongoDbPlanTest`'s two product texts; a new case, `RelationalMappingIntegrationTest.testManyColumnTypes` (a BIGINT under a Float property, `42.0`) |
| PCT under lite's rules | `testFloatToStringWithExcessTrailingZeros`, `testFloatToStringWithPositiveExponent` pass and leave the LiteRules Essential ledger and channel B's duckdb-lite rows; `testFloatToStringWithNegativeExponent`'s text `0` becomes `0.0` |
| compatibility lanes | unchanged: `pct_duckdb` (5 suites), `pct_sqlite` (5), `pct_h2`, channel B (5), `//spec:corpus_duckdb`, `//spec:corpus_h2` |
| `//core:product_raise_probe` | every Float text right on DuckDB, H2, SQLite, Postgres, MariaDB, MySQL, ClickHouse, Trino, MongoDB; the arithmetic cases still failing on ClickHouse, Trino and Postgres fail identically on the base |
| guards, UP on the user's word of 2026-11-24 ~06:20 (the direct cost of the 2026-11-23 ruling) | the lean-SQL baseline's 24 rows (`toString`, `format`, every database; +33 a value, +21 under the standard trim); channel B's lite Essential judge pins, +264 characters and plan JSON |

**Gated (2026-11-24):** at b694378e2 `//gates:local` was red on the parser-equivalence floor alone (3344 -> 3345 for
the new test's model, re-pinned in d3b10b173) and on its 14 Postgres-backed targets, which refuse root in a cloud
container and pass run unprivileged (`tools/postgres/as_unprivileged.sh`); then green, `//core:core_tests_lanes`, `//pct:pct_channel_b_lite_essential`, `//pct:edge_net`
(40 of 40), `//tools/guards:shrink_only_check`; before them (08e37643b) both corpora, every PCT lane named above,
`//core:mongodb_plan_test` and the product probe on MongoDB.

**A wrong value fixed on the way** (on the base too): MongoDB printed 1e33 as `1` (Decimal128's 34 digits: `$round`
keeps no fraction there, and the trailing-zero trim ate the integer's zeros).

**After B2a's end design audit (2026-11-24):** an integer-held Float above 2^53 prints exactly on H2, Postgres,
Snowflake and MongoDB and as its double elsewhere -- recorded in the S311 row as a known difference by database (a cast
to DOUBLE on the four was built, then reverted on the independent re-review's must-fix: Postgres's and MongoDB's
double-to-decimal casts keep 15 digits, so `1234567890123456` would have printed `1234567890123460.0`); the integer-held check
the product no longer reads moved into the edge (`EdgePureOnlyTexts`; `ReprParts.heldAsInteger` and its two guard rows
gone, a shrink); pinned: the edge's integer-held digits (`FloatTextTest.anIntegerHeldFloatsTextAtTheEdgeIsLegendEnginesCast`: the SQL each edge dialect writes for the product's node is legend-engine's plain cast, pinned with no fixture write, before B2b deletes the pass that keeps it), `format`'s `%f` of a whole value and a negative whole value. **For the
user's word:** the integer-held Float's text on lite's route moved from `1000000` (legend-engine's cast) to `1000000.0`
(Pure's, one text per type) -- the session's reading of the 2026-11-23 ruling, posted case by case. **Known, outside
B2a:** a value whose static type is `Number` prints through `DECIMAL_TO_TEXT`, not `FLOAT_TO_TEXT`, so "one rule for
every Float" holds for a value typed Float; a Number holding a double is the product track's (or B2b's printer reads
it by its value at the edge).

### Slice 4: a date with a time prints lite's nine-digit DateTime text -- HELD (option C)

**The finding (measured 2026-11-24 on DuckDB, SQLite and H2):** lite types every abstract-`Date` SQL slot `TIMESTAMP`
(`lowering/PureSql`: "an ABSTRACT Date slot rides TIMESTAMP and decodes as a DateTime"), so a rule keyed on the slot
(H14's D1, `Scalars.pureToString`) cannot tell a day from a midnight date-time. A `Date` column of days, which prints
`2000-10-25` today on all three (as legend-pure does), printed `2000-10-25T00:00:00.000000000Z` under it. A mixed column
already differs by database today: DuckDB and H2 print a day `2000-10-25 00:00:00`, SQLite `2000-10-25` (a parity gap,
G2). H14's own record named the right rule ("the DateTime text when the value has a time, the day otherwise"); the
spike met only tables whose values had times.

**Decided (the B3 session, 2026-11-24, under the user's standing word "You don't need to ask me about moving tasks
around if it's the right thing to do"; option C, presented as the recommendation):** the item waits until an abstract
Date carries its kind beside the value (B3's one-equality step, the equality rule's "types known only at run time carry
their type beside the value"); its text then follows the carried kind -- a day its day, a date-time the nine-digit text
(S312 as amended 2026-11-22), one text on every database. Until then nothing changes, and today's day behaviour is
pinned: `AbstractDateDayTextTest` (each lane). The D1 change and its probe are kept on the local branch `wip-slice4`,
not landed.

## B2a closed (2026-11-24)

Items 1 and 3 done; item 2 (a relation's `toString` printed by the database on lite's route) moved to B2b by the
user's ruling (option C, no temporary two-form facet); item 4 held as above.

## B2b

### S1: legend-pure's text of a relation written in Java from typed rows (2026-11-24)

`edge/PureRelationText` writes a relation's `toString` (`#TDS`), legend-engine's `toCSV` of a TDS, a TDS's `csv` property, and a program's literal `replace` calls on the printed text (the user, 2026-11-24); each
rule read from the pinned sources (legend-engine 4.145.0's `toString.pure`, `s.pure`, `helperFunctions.pure`); a cell it
has no rule for refused by name. `edge/PureRelationTextWriter` is a run's receiver: a relation's typed rows held in
their order and printed once whole, the text handed on as one String (the door's receiver, S2, and the harness's lifted
check, S4, take it).

**The independent review of S1 (2026-11-24) and its fixes:** the typed header (`toString(rel, true)`) printed `[1]`
where legend-pure prints `[0..1]` (lite types a `#TDS` column by its values) and a store column's plain type where
legend-engine prints the precise primitive: it is not written (the door leaves it to the run, which refuses it, as
before). The Float print is legend-pure's own (`CompiledSupport`: `0.0` for either zero, else `DecimalFormat("0.0")`
with 340 fraction digits), and a Float column's cell is read as the double it declares, whatever kind the database held
(a decimal included). A date-time keeps the edge's TDS rule (at least three fraction digits, the finer ones trimmed),
not a cut to three. An enumeration's value is not quoted as a String. A date toCSV's formats cannot write is refused;
an empty `replace` target is Java's (Pure's) replace. A mixed (tagged) column is refused when the columns are
announced, by a named product error. **toCSV's date-time cells follow the pinned `helperFunctions` rule
(`yyyy-MM-dd HH:mm:ss`), where today's SQL writes Pure's DateTime text (OUTPUT_LAYER_DESIGN's "gap 4", no register row):
measured on the corpus before anything routes to the printer (S4).**

**Measured (2026-11-24):** `PureRelationTextParityTest`, each lane (DuckDB, H2, SQLite), the Java text against the
edge's SQL text (`edge/RelationTextLowering`) for the same relation run in the edge's style: equal on Integer, String,
Boolean, empty cells, Float (fractions, whole, small), StrictDate, DateTime (subsecond and whole), an abstract Date
holding times, a String with a brace or a bracket and a quote, a name needing quotes, no rows, and a group's sum. **Two
places where the edge's SQL text is WRONG and the printer writes legend-pure's:** an abstract Date column of days (the
SQL keyed on the TIMESTAMP slot and wrote `2014-01-02T00:00:00.000+0000` on all three databases; the typed row carries
the day, legend-pure prints `2014-01-02`), and a Float of 1e20 on SQLite (the SQL's shortest-digits decomposition wrote
`9223372036854775807.0`, an integer cast saturating -- a wrong value). Both are pinned to legend-pure's text; they are
corrected as B2b routes a relation's text through the printer. **And the Floats where the databases' SQL texts disagree
with each other and with legend-pure:** `-0.0` (DuckDB `-0.0`), `4.030184897929827E17` (H2 `...700.0`), `1e23`
(DuckDB `99999999999999991611392.0`, H2 `100000000000000000000000.0`, SQLite `9223372036854775807.0`): the printer
writes legend-pure's one text on every database. Equal on all three: microsecond date-times, Decimal, Number of
integers and fractions, a group's average.

**Guards:** `JavaEvalLedgerTest`'s edge register lists the two new files with their reason; `WriterCarriageGuardTest`
scans both and registers `PureRelationText`'s representation operations by name (a date's own precision, a text's
escaping, a quoted name, a Float column's cell read as the double it declares) -- the direct cost of B2 itself.
**A known divergence the printer keeps, as the SQL did** (the re-review, 2026-11-24): a date-time's text keeps every
fraction digit past the third (`.123456+0000`), where legend-engine's TestTDS reads a date to the millisecond
(`.123+0000`); the goldens hold whole seconds and milliseconds, which both rules print alike -- a register row for
B2b's end.

### S2: the door's lift of a final print (2026-11-24)

`edge/PrintedRelations` reads a typed query once. It lifts the query where its last statement prints a relation and
that relation's columns are known to the compiler. The prints it lifts:
- a relation's `toString` (or `toString(rel, false)`);
- the relational test helper's `toCSV` (`meta::relational::tests::csv::toCSV`, `PlatformTypes.TO_CSV`): its one- and
  two-argument forms, and the four-argument form with the default formats, which the helper's two-argument form passes
  and the lowering reads as that form;
- a TDS's `csv` property.

Any number of `replace` calls with literal arguments may follow the print. The lift answers the query that computes the
relation (`TypedQuery.withBody`: model, options and parameters kept) and the print the run's receiver performs (S1's
`PureRelationTextWriter`). These stay the run's, as today:
- `toString(rel, true)`;
- `toCSV` with other formats;
- a `replace` whose target or replacement is no literal;
- a print that is not the last step;
- a `csv` read over a cast to a TDS class (the resolver's, `Anchors.tdsCsvRead`);
- a relation whose columns the database names when the plan runs (a raw grid, a dynamic pivot whose columns nothing
  names), and a relation with no columns. The lowering answers each by its own rules.

The lift reads the lowering's own predicate for the default formats (`RelationTextLowering.isDefaultCsvFormat`). When
S5 and S6 delete `RelationTextLowering`, the predicate moves to `PrintedRelations`.

The plan of record's B2, E0 = A ("E0 - yes"), and the user's rulings of 2026-11-19 ~18:12 and 2026-11-24.

**The doors, measured (2026-11-24):**
- **PCT channel A** (`PctPlanRoute.execute`), for legend-pure's and legend-engine's audiences: a lifted case runs the
  relation into the printer and answers its one String. Lite's own rules keep the run's answer (S5 prints a relation in
  the database). **No PCT case ends in a print:** no channel A answer on any edge-net lane is a printed table. The
  relation suites print on the host side, in the assert, which is S4's (the test-body `Printed` judgment, after B3).
  The lanes were measured on `54d922e92`, before `6fc717f9f` narrowed the lift; the narrowing only lifts less, and
  nothing was lifted. The lanes are `//pct:pct_duckdb`, `pct_sqlite`, `pct_h2`, `pct_duckdb_lite`, the engine lanes on
  DuckDB and H2, and `edge_net`. All 67 of their test targets pass and no ledger or golden moved. `PctPlanRoutePrintTest` runs the branch end to end: a day column
  printed as legend-pure prints it, and `toCSV(true)` with a literal replace.
- **pure/v1's execute: not wired.** It refuses every value answer today (`EngineAnswers.engine`, the Facts' value
  refusal), so a lifted print has no envelope to answer in. legend-engine's envelope for a String value is S7's work,
  read from the engine runner then, never guessed.
- **The service test runner: not wired.** None of the 59 files in the pinned 4.145.0 sources that declare a Service ends
  its query in a print, so nothing is measured there.

**A window with two printers** (the review of 54d922e92). Until B2b routes every print through the printer (S4, then
S5 and S6 delete `RelationTextLowering`), a print the door does not lift is still the edge's SQL text. Examples:
`toString()->replace($s, ',')`, `toString()->length()`, a raw grid's `toCSV`. That text differs from the printer's
where `PureRelationTextParityTest` names it:
- an abstract Date column of days;
- a Float of 1e20 on SQLite;
- `-0.0`, `4.03e17` and `1e23`.

The same relation can therefore print two ways, depending on whether what follows the print is a literal `replace`.

**A mixed column** (the review of `6fc717f9f` asked whether the door lifts a print the SQL text answered). A column
whose cells a mixed selection tags by kind is known only once the query is planned, so the door cannot see it. Today
such a column is refused by name on both roads: `extend(~m:x|[1, 2.5]->max())` printed is refused when the SQL is
rendered ("a tagged value reached the renderer"), lifted or not, because a tagged value reaches the renderer only at a
value's root. `PctPlanRoutePrintTest.aMixedSelectionInAColumnIsRefusedOnBothRoads` pins both roads. The printer's own
refusal of a tagged column (`requirePrintable`) waits for the day a relation's column can be one, so this case needs no
other rule now.

No PCT or corpus case meets the two-printer window today, since no case ends in a print at channel A. It closes with
S4 and S6.

## Audits

| When | Scope | Findings | Fixed |
|---|---|---|---|
| 2026-11-24 | B2b S2's fix round, `6fc717f9f` and `8551b293d`, a subagent, read-only | no must-fix; the four fixes confirmed (`knownColumns` is at least as strict as the lowering's special cases, and lifts a pivot whose columns a cast names), and the native-claims census right. Should-fix: a mixed (tagged) column passes `knownColumns`, so the door lifts its print and the printer refuses it where the SQL text answered it. Nits: the toCSV route test passed on either road; four unpinned shapes; the record's lane count and when the lanes ran; the predicate's home when `RelationTextLowering` goes | the mixed column measured: refused by name on both roads today (a tagged value reaches the renderer only at a value's root), pinned on both; the route test proves the lifted branch (a Float the two roads print differently); a pivot named by a cast pinned as lifted; the record corrected. The raw grid, the no-column relation and the zero-argument format functions (absent from lite's catalog) stay unpinned |
| 2026-11-24 | B2b S2's design, `54d922e92`, a subagent, read-only, given the code, the plan of record and the tenets | no must-fix; lifted forms, the lifted query, the route's answer and the tenets clean. Should-fix: the one-plan measurement planned the print's query, not the relation's; a dynamic pivot's and a raw grid's prints lifted where the lowering refuses or answers them by its own rule; the four-argument toCSV with the default formats not lifted; the window with two printers unrecorded. Nits: a redundant `withOptions`; a `csv` over a cast not lifted (kept: the resolver's); toCSV's argument not checked to be a relation. Tests: toCSV's forms, the `csv` property, a non-literal replacement, the options, the kept statements, a dynamic pivot | all, in this round; the cast-wrapped `csv` stays the run's |
| 2026-11-24 | B2a's end design audit, `96e7c1fc4..54f8be8e3`, a subagent, read-only, given the plan of record, the tenets and the record | no must-fix; verdict "sound to build B2b on". Should-fix: `heldAsInteger` left in the product for the edge alone; an integer-held Float above 2^53 printed per database; the integer-held text change not posted for the user's word; the record's slice 4 and slice 1 wording; the S311 row's cloud-lane, per-value and MongoDB claims; the edge's integer-held digits unpinned. Nits: stale javadocs, the gate wording, the ledger header | all, in this round (the user's word on the integer-held text asked) |
| 2026-11-24 | B2b S1's diff (`54f8be8e3..527d3e8a9`), a subagent, read-only, given the code, the pinned sources and the requirement | 1 must-fix (the typed header's multiplicity); should-fix: the typed header's precise types, the date-time's digits against the edge's rule, legend-pure's Float formatter, a decimal under Float, a Number of integers' javadoc, toCSV's date-time cells against today's SQL, mixed columns; nits: dates the CSV cannot write, enumerations' quoting, the empty replace, the writer's refusals' timing | all, in this round; toCSV's date-time cells measured on the corpus at S4 |
| 2026-11-24 | the fix round's diff (`527d3e8a9..41d52b690`), a narrow re-review, read-only | 1 must-fix: the integer-held Float's cast to DOUBLE on H2, Postgres, Snowflake and MongoDB lost digits on Postgres and MongoDB (15-digit casts) and the claim of one text everywhere was false; should-fix: that cast unpinned, the date rule's divergence from TestTDS's millisecond cut unrecorded; nits: a String in an Any column not quoted, a DecimalFormat per cell, two stale record lines. The printer's rules, the moves and the guards found clean | the cast reverted and the difference recorded; the rest fixed |
| 2026-11-24 | slice 3's diff (`96e7c1fc4..396961c73`), an independent subagent, read-only, given the code and the requirement | 5 must-fix: MongoDB printed `NaN.0`/`Infinity.0` (an unconditional `.0`); channel B's ledger row read `got "00.0"` (a slicing slip); channel B's lite Essential judge pins not re-pinned (+264); `MongoDbPlanTest`'s two product texts stale; a Float held as an integer printed `2` beside a computed `2.0` (one text per type broken). 1 should-fix: the probe's whole-value check too loose (a lost sign or a wrong digit count passed). Nits: stale javadocs, long lines. No defect in the shared helpers' SQL (simulated on every six-digit text kind, the non-finite texts included); the edge untouched | all, in `08e37643b` and the slice's last commit; the probe pins -2.0 and 1e20 exactly, and NaN and the infinities exactly |
