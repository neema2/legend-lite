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
| 2026-11-24 (relayed ~18:59) | "Int as float agree, parser bumps are always okay" | a Float the store holds as an integer prints a Float's text on lite's route (`1000000.0`), as slice 3 built it (S311's row records it); every parser-equivalence move with the tests' models approved, a standing word cited beside the floor's history (`PeRatchets.HISTORY`) |
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
gone, a shrink); pinned: the edge's integer-held digits (`FloatTextTest.anIntegerHeldFloatsTextAtTheEdgeIsLegendEnginesCast`: the SQL each edge dialect writes for the product's node is legend-engine's plain cast, pinned with no fixture write, before B2b deletes the pass that keeps it), `format`'s `%f` of a whole value and a negative whole value. **Ruled by
the user, 2026-11-24 ("Int as float agree"):** the integer-held Float's text on lite's route moved from `1000000`
(legend-engine's cast) to `1000000.0` (Pure's, one text per type), the session's reading of the 2026-11-23 ruling. **Known, outside
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
S5 and S6 delete `RelationTextLowering`), a print the door does not lift is still written by SQL. Examples:
`toString()->replace($s, ',')`, `toString()->length()`, a raw grid's `toCSV`. That text differs from the printer's
where `PureRelationTextParityTest` names it:
- an abstract Date column of days;
- a Float of 1e20 on SQLite;
- `-0.0`, `4.03e17` and `1e23`.

The same relation can therefore print two ways, depending on whether what follows the print is a literal `replace`.
(Since S5a, a relation's `toString` used further is lite's own text, computed by the database, with the edge's Float
and DateTime cells until S6: see S5a's window. The SQL this paragraph measured for `toString` is deleted; for `toCSV` it
stands until S5b.)

**A mixed column** (the review of `6fc717f9f` asked whether the door lifts a print the SQL text answered). A column
whose cells a mixed selection tags by kind is known only once the query is planned, so the door cannot see it. Today
such a column is refused by name on both roads: `extend(~m:x|[1, 2.5]->max())` printed is refused when the SQL is
rendered ("a tagged value reached the renderer"), lifted or not, because a tagged value reaches the renderer only at a
value's root. `PctPlanRoutePrintTest.aMixedSelectionInAColumnIsRefusedOnBothRoads` pins both roads. The printer's own
refusal of a tagged column (`requirePrintable`) waits for the day a relation's column can be one, so this case needs no
other rule now.

**The CSV prints, measured** (B2b's midpoint audit, 2026-11-24; `PureRelationTextParityTest`, DuckDB, H2 and
SQLite):
- The helper's `toCSV()` and `toCSV(true)`: the Java and SQL texts are equal on every measured carrier (Integer,
  String, Boolean, an empty cell, Float, StrictDate, Decimal, Number, a quoted name, no rows, a group's sum and
  average, several rows in no asked order), except a cell with a time of day. That cell is **gap 4**: the printer
  writes helperFunctions' `SimpleDateTimeFormat` (`2014-01-01 10:20:30`), the edge's SQL Pure's TDS DateTime text
  (`2014-01-01T10:20:30.123456+0000`). Pinned on both roads (`aToCsvsDateTimeCellIsTheWindowsGap4`). S4 reads it on the
  corpus, with legend-pure's and legend-engine's answers from the pinned sources, and posts it for the user's word.
- A TDS's `csv` property applies to a TDS value alone: after any relation function, `.csv` reads a column named csv.
  The edge's SQL refuses a TDS literal's `csv` by name (its rows reach the renderer as a mixed list); the printer
  writes legend-engine's text (`i, s\n3,c\n1,a\n2,TDSNull`), so the door's lift answers what the SQL refused. Pinned
  (`aTdsLiteralsCsvIsRefusedByTheSqlAndWrittenByThePrinter`).
- Not measured here: an integer-held or decimal-held Float in a CSV (no store in this test) and a to-many cell (no TDS
  literal holds one; `docs/CSV_DIFFERENTIAL.md` mechanism 3). S4 measures both on the corpus.

No PCT or corpus case meets the two-printer window today, since no case ends in a print at channel A. It closes with
S4 and S6.

### S3: a checked graph's defect sentences, the one catalog's on every door (2026-11-24)

- **S3a (Q7, "Q7 fine").** The edge's twelve dialects overrode `raiseMessage` to write a checked graph's
  `CONSTRAINT_UNEVALUABLE` defect cell in legend-engine's words (`PureMessages.defectCell`). That text is
  byte-identical to the one catalog's sentence the product writes since BE. The overrides and `defectCell` are
  deleted, so no edge dialect overrides `raiseMessage` now.
- **S3b (Q6, "Q6 fine", read with the user's word of 2026-11-24 the orchestrator relayed: "keep errors close to pure
  when cheap/fast so don't have to reword everything on compatibility").** A constraint with no `~message` no longer
  carries a literal the resolver wrote (`GraphEmission.constMsgCol`, legend-engine's sentence in the core). Its
  `CheckedConstraint.message` is null, and the lowering (`CheckedEnvelope`) writes the defect's message as the one
  catalog's sentence: `ErrorKind.CONSTRAINT_VIOLATED`, "Constraint :[%1$s] violated in the Class %2$s". That is
  legend-pure's own sentence (`DefaultConstraintHandler.java:90`, cited by the orchestrator), and legend-engine's
  agrees. It is a plain literal in the plan, as the resolver's was. No audience rewords it, so the plan carries no
  record; a `RaiseText` record was tried first and measured +2,398 characters of plan JSON on DuckDB's checked graph
  fetches (the lean-SQL baseline, shrink-only). It is an `ErrorKind` that is never raised (`isRaise` excludes it), so
  `RaiseKind` and the generated host tables are unchanged.
- **No text changes on any door.** `GraphFetchCheckedIntegrationTest` keeps its pins, and the edge needs no
  override.
- **Guards:** `CompatSpellingGuardTest` lists "violated in the Class" and registers `error/ErrorKind.java`'s one hit
  as RULED; `TerminalRowsFrozenTest` freezes the row with the ruling; `EdgeSpellingHookTest`'s edge arms no longer
  list `raiseMessage` (a shrink). `ErrorKindTest` pins the sentence and that it is never raised. `TypedSpecChildrenTest`
  pins a graph whose constraints mix a message and none.
- **One rule for a printed side** (the orchestrator's request, G1): `PrintedRelations.side(TypedSpec)` reads one
  expression as a print with its literal replaces. The door's `lift` and B3's test-body lift (`edge.UpstreamPrints`,
  switching to it) read the same rule.

### B2b's midpoint design audit (2026-11-24)

The verdict: sound to continue. What it asked, and where each stands:
- The CSV prints were unmeasured: measured above.
- `CONSTRAINT_UNEVALUABLE` rides as a record (`RaiseText.asData`) beside S3's plain literal. It stays: its
  `asData` is the plan-types guard's registered prose residue (`PlanTypesAreDataTest.PROSE_RESIDUE`), which S7
  removes by wording the defects in Java from typed rows; making it a literal now would close the residue by
  relabelling it (G4). Agreed by the orchestrator, 2026-11-24.
- **Carried into S5/S6's brief:** once `RelationTextLowering` is deleted, `PrintedRelations.side` needs a rule for the
  shapes `knownColumns` leaves to it (a raw grid's toCSV is answered from the grid's report today; a dynamic pivot whose
  columns nothing names): relax `knownColumns`, since the writer receives the columns at run time, or a named refusal
  measured on the corpus.
- **Carried into S4/S6's brief:** S6 deletes the value-print passes, so a last-step scalar print needs the same lift
  and one legend-pure value printer (G1). Decide at S4 whether `Side` covers value prints and extract one value
  printer, rather than adding a second Float printer.
- Nits, fixed: the printer's String rule (a String cell is printed only where a column holds text; under a number, a
  Boolean or a date it is refused by name, invariant 4); `JavaEvalLedgerTest`'s reason names the ruled Java `replace`;
  `PctPlanRoute`'s comment back above the main run.
- A nit recorded, not changed: a Number column's fractional cell arrives as a BigDecimal and prints as a Float, so a
  Decimal held in a Number column loses its scale (`1.50D` prints `1.5`), as the SQL did. It belongs with slice 4's
  "the type carried beside the value" item.
- **For the user's word after S4's measurement** (the orchestrator puts them together): (a) the TDS date-time text's
  digits past the millisecond (`.123456+0000`, where legend-engine's TestTDS cuts to `.123+0000`) and (b) toCSV's
  date-time cells (gap 4) -- each with the expression, legend-pure's answer read from the pinned sources,
  legend-engine's, lite's, and the cases each moves.

### S4 prepared: the two date-time questions, read from the pinned sources (2026-11-24)

- **(a) A date-time cell past the millisecond in a relation's `toString`.** legend-engine's relation TDS (the Pure
  runtime's `TestTDS`, which a `#TDS` literal builds in PCT's reference run) stores a date-time as epoch nanoseconds
  and reads it back to the millisecond: `DateFunctions.fromDate(new Date(value / 1000000))` (legend-engine 4.145.0,
  `legend-engine-pure-runtime-java-extension-shared-functions-relation/.../TestTDS.java:209`). Its print is
  therefore `2014-01-01T10:20:30.123+0000` for `...30.123456`. Lite's printer keeps every digit,
  `2014-01-01T10:20:30.123456+0000`, as the edge's SQL does today. Pure's DateTime carries any number of fraction
  digits, so the cut is the TDS storage's artifact: a reasoned departure, posted for the user's word with the cases it
  moves once S4 has measured them.
- **(b) Gap 4, a `toCSV` date-time cell.** The helper writes `format('%t{yyyy-MM-dd HH:mm:ss}', $date)` and
  escapes it (legend-engine 4.145.0, `core_relational/relational/helperFunctions/helperFunctions.pure:238-243`; its
  two-argument form passes `SimpleDateTimeFormat()`, `dateExtension.pure:384-387`). The printer writes exactly that.
  Only the edge's SQL diverged (Pure's TDS text), so (b) is a correction, not a departure: no register row. No
  relational-store test in the pinned sources asserts a `toCSV` date-time cell (a grep of the 231 `toCSV` uses for a
  date-time literal finds none), so S4 expects to move no corpus case; it measures that through B3's lift.

### S4: measured through B3's lift (2026-11-24, on B3's step 2b, `9b1e194fd`, merged here as a fast-forward)

B3's step 2b routes every printed assert side through this branch's rule (`PrintedRelations.side`) and printer
(`PureRelationTextWriter`); its round was green (292 of 293). The two date-time questions, counted over the pinned
4.145.0 sources (a source scan, narrower than a run):
- **(a), digits past the millisecond in a printed table: no case moves.** No PCT relation suite holds a date-time with
  a nonzero digit past the millisecond (their nine-digit date-times are all `.000000000`, which both rules print
  `.000`), and no printed expectation in the relational store's tests holds one. The difference stays real for such a
  value (`#TDS t:DateTime 2014-01-01T10:20:30.123456#->toString()`: legend-engine's TDS `...30.123+0000`, lite
  `...30.123456+0000`), so it goes to the user as a departure with zero cases moved.
- **(b), gap 4: no case moves.** No `toCSV` expectation in the relational store's tests holds a date-time cell.
- **Not the printer's, for B3:** the two places the relational corpus meets a value past the millisecond are
  comparisons, not prints. `relation/tests.pure`'s `testDateTimeInclusiveRangeQuery` (lines ~1220-1232) and the test
  beside it (~1250) compare a database value `.123456789` with an expected `#TDS` literal's `.123+0000` through
  `assertTdsEquivalent`: equal under legend-engine's TDS (read to the millisecond), unequal under lite's rule of every
  digit the database holds (S181, S399). That is the equality rule's, B3's to check.

### S5a: a relation's toString is lite's own text, computed by the database (2026-11-24, `1d6da1b6b` and its fix round)

The user's rulings of 2026-11-19 ~18:05 ("Agree with all four table printing recommendations") and ~18:36 ("Null is
good"), with ~18:12's "Yes record it" for a text used further in a query; the orchestrator's go of 2026-11-24 (S5a; Q3:
an abstract Date held as a timestamp keeps today's text, the scalar toString's G2 gap). From H14's spike
(`docs/records/compat-homework/H14-slice-shape-note.md` on `claude/compat-table-text-db-spike`).

- **The product writes it** (`lowering.ProductRelationText`): the Lowerer's relation `toString` arm is one scalar
  subquery the database computes, a `STRING_AGG` of the relation's rows in the relation's own order
  (`Render.liftOrder`, with hidden keys for a sort by a dropped column or an expression). The layout is legend-pure's
  `#TDS` text. Each cell is the product's own text node of its value: an empty cell `null` (`'null'` under a Variant),
  a Variant's text and a String holding a brace or a bracket quoted (`\` and `'` escaped), every other cell its
  scalar `toString()`.
- **Refused by name:** `toString(rel, true)`; a raw grid (its columns exist only when the plan runs); a pivot whose
  columns nothing names; a mixed column (its cells' kinds differ by row); a DISTINCT or star select ordered by a key
  that is not one of its columns (the old `withSortOutputs` added a hidden column to such a select and changed its
  rows).
- **The edge's toString SQL is deleted:** `RelationTextLowering`'s `relationToString`, `lowerToString`, `tdsString`,
  `tdsCell`, `dateOnly`, `dateTimeTextTds` (-184 lines), and the `RelationTexts` facet's `relationToString` member
  (`OpenClassMembersTest`, a shrink). A last-step print on legend-pure's and legend-engine's doors is the doors' Java
  printer's (`PrintedRelations`, S2 and B3's lift); a print used further is lite's text.
- **One text on each lane:** `ProductRelationTextTest` pins DuckDB, H2 and SQLite: the layout and nulls, Floats
  (`2.0`, `0.00001`, `25000000.0`), a StrictDate and a DateTime (lite's nine digits and `Z`), an abstract Date held
  as a timestamp (the database's timestamp text, G2), quoted names, a braced String holding a quote and a backslash,
  no rows, a sort by a dropped column, a Number column (`21.0` beside `2.5`, the record's known Number item), a stored
  relation and a group's sum and average; and the refusals of `toString(rel, true)` and a mixed column.
- **A Decimal cell is its database's scale** (register S400, ruled 2026-11-23): `1.50` beside `-0.125` prints `1.500`
  on DuckDB, `1.50` on H2 and `1.5` on SQLite (measured 2026-11-24), as the product's scalar Decimal text does. It is
  not pinned as one text on every lane.
- **A Number column on a DECIMAL slot** prints the database's fixed scale (`81.180`), as the product's scalar Number
  text does; the deleted edge SQL cast it to DOUBLE (`81.18`). It stays with the record's known Number item.
- **A relation with no sort** takes the database's aggregate order, as the deleted SQL did. A parallel aggregate does
  not promise it; every pinned case sorts.

**The window until S6** (the review's must-fix 1). Under the edge's style the edge dialect's value passes
(`edge.PurePrintForms`, `spellingPasses`) rewrite every `FLOAT_TO_TEXT` and `DATETIME_TO_TEXT` node in a plan,
including the ones inside this subquery. A table text used further at the edge (`toString()->length()`,
`toString()->replace($s, ',')`) therefore writes lite's layout with the edge's Float and DateTime cells: legend-engine's
in-query cast for a Float (`1e-05` on DuckDB, `1.0E-5` on H2) and legend-pure's `+0000` form for a DateTime. That is
neither lite's text nor legend-pure's. No PCT or corpus case meets it (H11 §13.1; a last-step table is the Java
printer's). It is pinned as the edge's own per-value text
(`FloatTextTest.anEdgeTableTextUsedFurtherWritesItsFloatCellsAsTheEdgeWritesAFloat`), so S6's deletion of the passes
moves it to lite's text by name. Until S5a this test pinned legend-pure's print, which the deleted edge SQL wrote (PR
#118's fix round).

**Moves:**
- **Channel B's lite lane** (`duckdb-lite`, lite's own rules; `ledger.tsv`): of the 423 rows that expected the
  deleted refusal, 390 leave because lite's text is legend-pure's (Relation 335, Standard 55), and both judges agree on
  every test. 33 remain, re-recorded (Relation 30, Standard 3: `over`'s range intervals, `asOfJoin`,
  `testExtendMaxDate`, `testExtendMinDate`, `testDayOfWeek_Relation`), each for one reason. An untyped `#TDS` column of
  zone-suffixed timestamps is the abstract Date (`TdsChecker`, as legend-pure's TDS inference), and such a Date cell
  held as a timestamp prints the database's timestamp text (`2024-01-29 00:32:34`) where legend-pure prints its DateTime
  text (`2024-01-29T00:32:34.000+0000`). That is the scalar toString's G2 gap, held with B2a's item 4 (Q3).
- **Channel B's lite judge pins** (`ratchets.tsv`, `bazel run //pct:update_ratchets`): the 390 tests the product
  refused now plan their database judge. Relation goes from 104 to 469 statements, 631,211 to 1,329,666 characters and
  12,328,650 to 24,792,467 plan-JSON characters; Standard from 166 to 224 statements, 721,186 to 819,169 characters and
  14,214,651 to 16,098,309. That is the direct, measured cost of the ruled change (the rule of 2026-11-24 ~06:20: done
  and reported). Essential shrinks by 272 characters of SQL and plan JSON (a shrink, pre-approved); no other row moves.
- The edge cases golden: EDGE-2 (`toCSV` of a date-time) moves to the helper's `yyyy-MM-dd HH:mm:ss`, S4's
  correction (b), now that the golden's runner lifts a last-step print as the doors do (`EdgeCasesGoldenTest.answer`).
- The lean SQL baseline: DuckDB's `toString_Relation_1__String_1_` row is new (2 statements, 725 characters), where the
  product refused; the refused count falls from 11 to 9.

## Audits

| When | Scope | Findings | Fixed |
|---|---|---|---|
| 2026-11-24 | B2b S5a, `1d6da1b6b`, a subagent, read-only, given the code, H14's spike and the rulings | 2 must-fix: under the edge's style the edge dialect's value passes re-spell the table text's Float and DateTime cells, the pin of that deleted and nothing recorded, the javadoc's "whatever the plan's style" false; channel B's lite ledger stale (423 rows expect the deleted refusal). Should-fix: tests (a sort by a dropped column, a Variant cell, a quote or a backslash in a braced String, a Number or Decimal cell, a stored relation, the refusals). Nits: stale wording (`JavaEvalLedgerTest`, `TextRefusals`, `Render`, `RelationTextLowering`, the parity test, `AggOrderNullPlacementTest`), double blank lines, a Number on a DECIMAL slot unpinned, a relation with no sort. Invariants, the order's lift, the cells' rules and the guards found clean | the window recorded and pinned as the edge's own text (`FloatTextTest`), the javadoc corrected; the ledger re-measured (390 leave, 33 re-recorded, the G2 gap) and the judge pins moved by the measured cost; the tests added (a Decimal is its database's scale, S400, so it is not one text on every lane; a Variant cell is channel B's lite lane's, whose Variant tests now pass); the wording and blank lines fixed; the Number and no-sort nits recorded. A DISTINCT select ordered by an expression stays unpinned (no Pure relation function writes one) |
| 2026-11-24 | B2b's MIDPOINT design audit, `54f8be8e3..607d24dcc`, a subagent, read-only, given the plan of record, the tenets and the record | verdict "sound to continue"; no must-fix. Should-fix: the CSV prints unmeasured against the SQL text; two mechanisms in one defect object; `side()` at S5/S6; legend-pure's value prints at S6. Nits: a String cell under any column type; the ledger's reason; a Decimal in a Number column; the route's comment; two differences not yet register rows | the CSV measured and pinned (gap 4; a TDS literal's csv); the record kept, with the orchestrator's agreement (G4); S5/S6 and S4/S6 items carried; the nits fixed or recorded; (a) and (b) for the user's word after S4 |
| 2026-11-24 | B2b S3, `ba8acecdb`, a subagent, read-only | must-fix: the generated host tables (`lite-raise.ts`, `_type_facts.py`) list every `RaiseKind`, and a new one was added without regenerating them. Questions 1-5 clean: S3a's text byte-identical for every id; the old message a bare literal, the children walk in step, the simple name identical, the guard edits legitimate. Nits: two stale comments (`SqlExpr.RaiseText`, `RaiseTextTest`); the null message's children walk unpinned | the must-fix made moot: `CONSTRAINT_VIOLATED` is no `RaiseKind` (the gate also measured the record's plan growth, so the sentence is a plain literal); the comments corrected; the children walk pinned |
| 2026-11-24 | B2b S2's fix round, `6fc717f9f` and `8551b293d`, a subagent, read-only | no must-fix; the four fixes confirmed (`knownColumns` is at least as strict as the lowering's special cases, and lifts a pivot whose columns a cast names), and the native-claims census right. Should-fix: a mixed (tagged) column passes `knownColumns`, so the door lifts its print and the printer refuses it where the SQL text answered it. Nits: the toCSV route test passed on either road; four unpinned shapes; the record's lane count and when the lanes ran; the predicate's home when `RelationTextLowering` goes | the mixed column measured: refused by name on both roads today (a tagged value reaches the renderer only at a value's root), pinned on both; the route test proves the lifted branch (a Float the two roads print differently); a pivot named by a cast pinned as lifted; the record corrected. The raw grid, the no-column relation and the zero-argument format functions (absent from lite's catalog) stay unpinned |
| 2026-11-24 | B2b S2's design, `54d922e92`, a subagent, read-only, given the code, the plan of record and the tenets | no must-fix; lifted forms, the lifted query, the route's answer and the tenets clean. Should-fix: the one-plan measurement planned the print's query, not the relation's; a dynamic pivot's and a raw grid's prints lifted where the lowering refuses or answers them by its own rule; the four-argument toCSV with the default formats not lifted; the window with two printers unrecorded. Nits: a redundant `withOptions`; a `csv` over a cast not lifted (kept: the resolver's); toCSV's argument not checked to be a relation. Tests: toCSV's forms, the `csv` property, a non-literal replacement, the options, the kept statements, a dynamic pivot | all, in this round; the cast-wrapped `csv` stays the run's |
| 2026-11-24 | B2a's end design audit, `96e7c1fc4..54f8be8e3`, a subagent, read-only, given the plan of record, the tenets and the record | no must-fix; verdict "sound to build B2b on". Should-fix: `heldAsInteger` left in the product for the edge alone; an integer-held Float above 2^53 printed per database; the integer-held text change not posted for the user's word; the record's slice 4 and slice 1 wording; the S311 row's cloud-lane, per-value and MongoDB claims; the edge's integer-held digits unpinned. Nits: stale javadocs, the gate wording, the ledger header | all, in this round; the integer-held text ruled by the user 2026-11-24 ("Int as float agree") |
| 2026-11-24 | B2b S1's diff (`54f8be8e3..527d3e8a9`), a subagent, read-only, given the code, the pinned sources and the requirement | 1 must-fix (the typed header's multiplicity); should-fix: the typed header's precise types, the date-time's digits against the edge's rule, legend-pure's Float formatter, a decimal under Float, a Number of integers' javadoc, toCSV's date-time cells against today's SQL, mixed columns; nits: dates the CSV cannot write, enumerations' quoting, the empty replace, the writer's refusals' timing | all, in this round; toCSV's date-time cells measured on the corpus at S4 |
| 2026-11-24 | the fix round's diff (`527d3e8a9..41d52b690`), a narrow re-review, read-only | 1 must-fix: the integer-held Float's cast to DOUBLE on H2, Postgres, Snowflake and MongoDB lost digits on Postgres and MongoDB (15-digit casts) and the claim of one text everywhere was false; should-fix: that cast unpinned, the date rule's divergence from TestTDS's millisecond cut unrecorded; nits: a String in an Any column not quoted, a DecimalFormat per cell, two stale record lines. The printer's rules, the moves and the guards found clean | the cast reverted and the difference recorded; the rest fixed |
| 2026-11-24 | slice 3's diff (`96e7c1fc4..396961c73`), an independent subagent, read-only, given the code and the requirement | 5 must-fix: MongoDB printed `NaN.0`/`Infinity.0` (an unconditional `.0`); channel B's ledger row read `got "00.0"` (a slicing slip); channel B's lite Essential judge pins not re-pinned (+264); `MongoDbPlanTest`'s two product texts stale; a Float held as an integer printed `2` beside a computed `2.0` (one text per type broken). 1 should-fix: the probe's whole-value check too loose (a lost sign or a wrong digit count passed). Nits: stale javadocs, long lines. No defect in the shared helpers' SQL (simulated on every six-digit text kind, the non-finite texts included); the edge untouched | all, in `08e37643b` and the slice's last commit; the probe pins -2.0 and 1e20 exactly, and NaN and the infinities exactly |
