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
- **The answers (2026-11-25, relayed by the orchestrator):** (a) is ruled by the user, "Yes keep for past
  milliseconds" -- register row S402, a reasoned departure with 0 cases moved. (b) needs no ruling: the edge's toCSV
  date-time cell is the helper's `yyyy-MM-dd HH:mm:ss`, a correction (the edge cases golden's EDGE-2 moved to it with
  S5a's fix round). Q1 for S5b: the product's toCSV date-time cell is lite's DateTime text, the S312 ruling.
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
  no rows, a sort by a dropped column, a Number column (`21.0` beside `2.5`, the record's known Number item) and a
  group's sum and average; and the refusals of `toString(rel, true)` and a mixed column. A stored relation's text is
  `RenderCsvIntegrationTest`'s (under the edge's style until S5b moved that class to the product).
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
printer's). It is pinned two ways (`FloatTextTest.anEdgeTableTextUsedFurtherWritesItsCellsAsTheEdgeWritesTheirValues`,
the re-review of 2026-11-25): each Float and DateTime cell is the edge's own text of that value (relative: the same pass
writes both sides), and the table is not lite's text (absolute), so S6's deletion of the passes turns the test red by
name. Until S5a this test pinned legend-pure's print, which the deleted edge SQL wrote (PR #118's fix round).

**Moves:**
- **Channel B's lite lane** (`duckdb-lite`, lite's own rules; `ledger.tsv`): of the 423 rows that expected the
  deleted refusal, 390 leave because lite's text is legend-pure's (Relation 335, Standard 55), and both judges agree on
  every test. 33 remain, re-recorded (Relation 30, Standard 3: `over`'s range intervals, `asOfJoin`,
  `testExtendMaxDate`, `testExtendMinDate`, `testDayOfWeek_Relation`), each for one reason. A `#TDS` column of the
  abstract Date (inferred from zone-suffixed timestamps, `TdsChecker` as legend-pure's TDS inference, or declared
  `Date`, as `testExtendMaxDate` and `testExtendMinDate` do) holds its cells as timestamps, and such a cell prints the database's timestamp text (`2024-01-29 00:32:34`) where legend-pure prints its DateTime
  text (`2024-01-29T00:32:34.000+0000`). That is the scalar toString's G2 gap, held with B2a's item 4 (Q3). Each row
  holds the whole actual text (the re-review's S1, 2026-11-25: read by a probe of the full failure, never committed),
  and each test's expected text equals it but for those date spellings, so no other difference hides behind a row.
- **Channel B's lite judge pins** (`ratchets.tsv`, `bazel run //pct:update_ratchets`): the 423 tests the product
  refused now plan their database judge (one statement each: Relation +365, Standard +58). Relation goes from 104 to 469 statements, 631,211 to 1,329,666 characters and
  12,328,650 to 24,792,467 plan-JSON characters; Standard from 166 to 224 statements, 721,186 to 819,169 characters and
  14,214,651 to 16,098,309. That is the direct, measured cost of the ruled change (the rule of 2026-11-24 ~06:20: done
  and reported). Essential shrinks by 272 characters of SQL and plan JSON (a shrink, pre-approved), its statement count unchanged:
  a drift inside the 1% tolerance from the commits since the pin was set (`24b054c53`: B2b S1-S4 and B3's step 2b,
  merged), not attributed to one of them -- no Essential test prints a relation, so S5a is not its source. No other
  row moves.
- The edge cases golden: EDGE-2 (`toCSV` of a date-time) moves to the helper's `yyyy-MM-dd HH:mm:ss`, S4's
  correction (b), now that the golden's runner lifts a last-step print as the doors do (`EdgeCasesGoldenTest.answer`).
- The lean SQL baseline: DuckDB's `toString_Relation_1__String_1_` row is new (2 statements, 725 characters), where the
  product refused; the refused count falls from 11 to 9.
- `SqlFnSortedTest` (the full round, `//core:guardrails_edge`): `lowering.ProductRelationText` joins the product's writers
  of COALESCE, GREATER, IS_NULL, OR, REPLACE, STRPOS and the aggregate STRING_AGG (the direct cost of the ruled change,
  dated in the test); SUBSTRING leaves the functions both sides write, as the edge's toString SQL that wrote it is
  deleted (a shrink).

### S5b: the helper's toCSV is lite's own CSV, computed by the database (2026-11-25)

The rulings this slice builds on:
- the user, 2026-11-19 ~20:05: "toCSV yes toString or another easy database sql that makes sense";
- the layout the orchestrator agreed on 2026-11-24;
- a date-time cell is lite's DateTime text: S312, "Yes always pretty print to iso nanos", relayed by the orchestrator on 2026-11-25 as Q1;
- the CSV's nulls and Strings, ruled by the user on 2026-11-25 and relayed by the orchestrator: "For csv we can do empty cell for null and "" or '' empty string and the toCSV(true) do null for null and quoted strings?" (register S420).

**The question asked first.** The agreed layout had `toCSV(t, true)` write `TDSNull`. Two guards refused that spelling in a product class:
- `CompatSpellingGuardTest` and `CompatConstantPoolTest` both flagged it.
- S313 and S316 hold `TDSNull` as the edge's spelling.

The question went to the orchestrator with (A) write it, (B) refuse it, and (C) ignore the flag. The user's design replaced all three: lite writes its own explicit null under the flag. Nothing in the product spells `TDSNull`, and no guard moves.

**The product's `toCSV`** (`lowering.ProductRelationText.csv`) is one scalar subquery the database computes, in the relation's own order (`Render.liftOrder`, as in S5a).
- **Layout:** the helper's. The header is the names joined `,`; each row is its cells joined `,`. There is a newline after the header and after every row, so no rows gives the header and two newlines. (Superseded by Q-C, 2026-11-25: every row ends with its own newline, so no rows is the header line alone; see "The CSV's two questions, ruled".)
- **A name** that holds a comma, a quote or a line break is quoted at planning, its quotes doubled.
- **Under `toCSV(t)` and `toCSV(t, false)`** (the four-argument form, with formats, is refused by name since the
  review's fix round: the orchestrator's ruling of 2026-11-25, Q-A):
  - a null is an empty field;
  - an empty String is `""`;
  - any other cell is quoted only where it holds a comma, a quote or a line break.
- **Under `toCSV(t, true)`:**
  - a null is the word `null`;
  - every String cell is quoted (`""`, `"null"`, `"a"`), and every Variant cell since the review's fix round (a
    Variant's JSON null `"null"`, never the null word: the orchestrator's ruling of 2026-11-25, Q-B);
  - a number, a Boolean, a date or any other non-String cell is written as under `toCSV(t)`.
- **Each cell** is the product's text of its value (S311, S312): `2.0`, `0.00001`, `2014-01-01T10:20:30.123000000Z`.
- **The same rule in the CSV output format:** `result/CsvWriter` writes an empty String as `""`, so it is never read as the null.

**A raw grid's CSV** (`executeInDb`) is the grid's report, read when the plan runs (the planner-driver seam program's S3c).
- It moved from the edge with its layout and cells unchanged but for the two ruled changes below. The keyed plan
  admits an integer's and a text's columns alone, and their database text is the product's.
- An empty text there is a String's, so it is written `""`.
- Under `toCSV(t, true)` a raw grid is refused by name. The flag quotes a String cell alone, and the grid's column types exist only when the plan runs.

**Refused by name:**
- the four-argument form (with formats of its own until the review's fix round; every four-argument form since);
- a `renderTdsNull` that is no literal;
- a raw grid under the flag;
- a pivot whose columns nothing names;
- a relation whose plan names no column. Until S5b the edge refused this as a dynamic pivot's text; S5a's `toString` now refuses it by name too, where it used to throw an internal error;
- a mixed column;
- a DISTINCT or star select ordered by a key that is no column.

**The edge's `toCSV` SQL is deleted.**
- From `RelationTextLowering`: `toCsv`, `lowerToCsv`, `gridCsv`, the five-argument `csv`, `dynamicPivotText` and `hasDynamicPivot`. The TDS `csv` property's SQL stays until S5c.
- From the facet: the `toCsv` member (`OpenClassMembersTest`, a shrink).
- A last-step `toCSV` on legend-pure's and legend-engine's doors is printed by the doors' Java printer (`PrintedRelations`, B3's lift), in the helper's own text, with `TDSNull` under the flag.
- A `toCSV` used further is lite's text, inside S5a's window: the edge's Float and DateTime cells until S6 (its Float
  cell the database's own full-precision text since 2026-11-25: "toCSV's Float cell exact", below).

**Moves** (measured by the fast check on the merge with B3 step 4; the full round's are added below when it lands):
- The lean SQL baseline: DuckDB's two `toCSV` rows are new (`toCSV_TabularDataSet_1__String_1_`, 2 statements, 2,143
  characters; `toCSV_TabularDataSet_1__Boolean_1__String_1_`, 1 statement, 660 characters), where the product
  refused; the refused count falls from 9 to 6.
- `SqlFnSortedTest`: `ProductRelationText` joins the writers of EQUAL (the empty String's test), dated on its line.
- No core, edge or channel B lite test moved with the CSV rule: no existing CSV output held an empty String. The
  rule is pinned by `ResultWritersTest.anEmptyStringIsQuoted_aNullAnEmptyField`, `ProductRelationTextTest` and
  `RenderCsvIntegrationTest`.

**The full round** on `d7af55aae`, S5b alone (B3's step 5a set aside on `b2-merge-b3-5a` until B3's fix): every lane
green -- the local gate, core and its lanes, the edge on H2 and SQLite, every PCT lane, channel B, the engine lanes,
`edge_net`, and the corpus on DuckDB and H2, whose verdicts held. The Postgres, `is_numeric` and datacube targets
failed only as root and passed on the unprivileged rerun. Pushed 2026-11-25 and reported to the orchestrator. The
corpus and PCT were read for an unlifted `toCSV` (the review's item): none moved.

**The review's fix round** (below, in the audits table), landed with S5c in one commit, so each pushed commit is green
(the LENGTH writers balance only with both). Its moves: the lean SQL baseline's two `toCSV` rows on DuckDB shrink --
2,143 -> 1,372 and 660 -> 406 characters, their repeated text 515 -> 240 and 90 -> 0, their plan JSON 44,096 -> 28,125
and 13,324 -> 8,028 -- as a number's, a Boolean's and a date's text is no longer escaped.

### S5c: the edge's relation texts and their facet deleted (2026-11-25)

The plan was agreed by the orchestrator on 2026-11-25: the door peels the type-level TDS casts and lifts that one
`.csv` to the Java printer's TDS_CSV form. `RelationTextLowering` and the `RelationTexts` facet are deleted. The
product keeps refusing a TDS's csv by name.

- **Deleted.** `edge/RelationTextLowering` (the TDS csv property's SQL, the last of it) and `lowering/RelationTexts`
  (the plan style's B17 facet, with its forwarders `pureToString` and `kindType`).
  - The Lowerer loses its `relationTexts` field and `withRelationTexts`. `PlanEnv.configure` no longer asks for the
    facet.
  - `PlanStyleFacets.KINDS` loses the kind, and the edge's style (`PureText.facet`) its answer.
- **The product** (amended the same day by the user's ruling: a TDS's `csv` is lite's CSV, as `toCSV(t)` writes it;
  "S5c amended", below). A TDS's `csv` is refused by name on every plan style (`TextRefusals.tdsCsv`, at the Lowerer's two
  arms): "a TDS's csv is legend-engine's text, printed only as a query's last step at legend-pure's and legend-engine's
  doors: the product writes a relation's rows by its result writers (JSON, CSV, Arrow), and its text by toString and
  toCSV". A relation's `toString` and `toCSV` are lite's own texts (S5a, S5b).
  - The second arm is the review's: a `#TDS` literal's `csv` was taken by the TDS class's field read and its rows
    reached the renderer as a mixed list (an internal wall before S5c too). It is now refused by the same sentence.
  - pure/v1's execute refuses every value answer today (`EngineAnswers.engine`), so the facet's SQL text it loses
    moved no answer there.
- **The door.** `PrintedRelations.side` peels the type-level TDS casts (`ResultEnvelopeSplice.peelTdsCasts`, the
  resolver's own rule) before it reads a TDS's csv. testEnumInRelation's
  `$result.values->cast(@TDS<Any>).csv` is then lifted to the Java printer's TDS_CSV form.
  - The lift's default-format test reads the literal text the lowering reads: the zero-argument format functions are
    not in lite's catalog.
- **The edge's list text** (`PureListTexts`) reads the same `pureToString` through the representation's facade,
  `lowering.ReprParts`. This is a dated listed-caller entry in `ArchitectureTest.PLANNER_ENTRIES`, and the one guard
  row that is not a shrink.
- **Guards, each a dated shrink:**
  - `OpenClassMembersTest`: the facet's entry.
  - `ArchitectureTest`: the facet's planner entries, its planted host door and its facet rule.
  - `HookRulingTest`: the facet's ruling, its facade and its implementer.
  - `HookLambdasTest`: `PlanEnv`'s facet reaches 2 -> 1.
  - `EdgeSeparationTest`: B17's class and its four dependency rows.
  - `PlanStyleSurfaceTest`: the kind.
  - `JavaEvalLedgerTest`: `RelationTextLowering.java`'s row.
  - `PlantedHostDoors`: the planted implementer.
  - `SqlFnSortedTest`: ENDS_WITH, LENGTH, MINUS and STRPOS leave the functions both sides write (`RelationTextLowering`
    was their one edge writer).
  - `EdgeOnlyMembersTest`: `DateFmt#TIME_ONLY`, whose one reader went; the format is deleted with it.

**Moves** (the full round on `8da71e1f8`, and the pins it found):
- **Every lane green:**
  - the local gate, core and its lanes, the edge on H2 and SQLite;
  - every PCT lane, channel B and the engine lanes, with no roster, ledger or golden moved;
  - `edge_net`, and the corpus on DuckDB and H2, whose verdicts held. `testEnumInRelation`'s
    `$result.values->cast(@TDS<Any>).csv` is lifted to the Java printer's TDS_CSV form at the door.
  - The Postgres, `is_numeric` and datacube targets failed only as root and passed on the unprivileged rerun.
- **The corpus's lean SQL** (`//spec:lean_sql_corpus_duckdb`, `_h2`), family `mapping::enumeration`:
  - "queries refused" 0 -> 1 on both lanes;
  - statements 5 -> 4;
  - chars 3,570 -> 2,994 (H2 4,039 -> 3,427);
  - plan JSON 64,595 -> 52,626 (H2 67,793 -> 55,241).
  - Why: the harness plans each body as the edge admits it, without B3's print lift, and `testEnumInRelation`'s TDS csv
    is no SQL since S5c. The run lifts it and sends the relation read alone.
  - The refused count's growth is posted to the user through the orchestrator, with both our leans on re-pinning it.
    It lands only on the user's word. Other families shrink in plan size by a few characters (pre-approved).
  - A prototype that measured bodies as the run sends them (B3's lift applied) moved 25+ families up, the SQL the
    lift's reads send. It was reverted, and the finding is B3's.
- **The parser floor:** `own_corpus.matched` 3345 -> 3346. One more of the fix round's test models (the enumeration
  model in `ProductRelationTextTest`) matches the reference grammar; approved by the user's standing word, "parser bumps
  are always okay" (`PeRatchets.HISTORY`).
- **The core lean baseline:** DuckDB's two `toCSV` rows shrink (the review fixes, above).
- **Guards:** the dated shrinks listed above. The one entry that grows is `PLANNER_ENTRIES`' `ReprParts#pureToString`
  caller.

### S5c amended: a TDS's csv is lite's CSV, as toCSV(t) writes it (2026-11-25)

The user asked why the product refuses a TDS's `.csv` instead of computing it like `toCSV`. The orchestrator
recommended that a relation's or a `#TDS` literal's `.csv` lower exactly as `toCSV(t)`, with a register row for the
spelling, and the user ruled ".csv in product, yes same as toCSV". This replaces S5c's refusal by name and the lean
re-pin question (the refused count's growth).
- **The product:** both of the Lowerer's TDS csv arms (a TDS value's and a relation's, the resolver peeling the
  type-level casts as before) write `ProductRelationText.tdsCsv`: the relation's CSV by `toCSV(t)`'s one rule (S420,
  Q-C), computed by the database. `TextRefusals.tdsCsv` is deleted.
  - The review of `d6e94cdfe` (below, the audits table): the arms, the door and the resolver's anchor read a TDS's
    csv by one test (`PlatformTypes.isTdsCsvRead`): the class's `String[1]` property, never a relation's column named
    csv (`[*]`), which reads its cells. A value whose columns the compiler does not know (a cast to `TDS<Any>` the
    resolver does not peel: a `#TDS` literal's cast, `stringToTDS`) is refused by name, by `toCSV` alike, which reached
    an internal error there until then.
- **The doors:** unchanged. A last step's `csv` is legend-engine's text, printed in Java (`PrintedRelations`, B3's lift
  for testEnumInRelation); one used further is lite's.
- **Tests:** `RenderCsvIntegrationTest.aTdsCsvIsLitesCsvAsToCsvWritesIt` (a stored relation cast to a TDS, and a
  `#TDS` literal, each equal to its `toCSV()`); `PureRelationTextParityTest.aTdsLiteralsCsvIsLitesUnliftedAndUpstreamsWhenPrinted`.
- **Register S405**; the differences page's §14.
- **The lean guard:** the corpus's lean SQL plans `testEnumInRelation`'s body again (`abbc25278`): family
  `mapping::enumeration`, refused 1 -> 0, DuckDB 4 -> 5 statements, 2,994 -> 3,570 characters, 52,626 -> 64,595 plan
  JSON; H2 4 -> 5, 3,427 -> 4,039, 55,241 -> 67,793 -- the figures before S5c, exactly. Regenerated, not restored: the
  corpus pin counts a body's product statements, and the assert's `csv` text is in the judge's statement, so the body's
  one counted statement is its relation read, the same SQL as before S5c.

### S6: the edge's value print passes, measured; legend-pure's door built (2026-11-25)

**The measurement.** A temporary probe turned off the edge's three print passes and kept F31's parses:
- the Float text (legend-engine's in-query cast with its literal processors);
- the DateTime `+0000` composition;
- the TDSNull joins.

Every compatibility lane ran on it: PCT A on DuckDB, SQLite and H2, the engine lanes, `edge_net`, the edge tests, and
the corpus on DuckDB and H2. Nothing was committed.
- **PCT channel A:** `testFloatToStringWithNegativeExponent` on DuckDB and SQLite (lite's six decimals print `0.0`).
- **The engine lanes:** the same case on DuckDB (PASS -> FAIL); on H2 `testFloatToStringWithPositiveExponent`
  FAIL -> PASS (LITE_FIXES: Pure's `134210000.0` where legend-engine's H2 cast prints `1.3421E8`).
- **The corpus:** DuckDB +114 failing, H2 +109; the unordered registers -65 each (those bodies failing).
  - 112 / 107 are legend-engine's `TDSNull`. Its empty TDS cell is an object that prints, joins and sorts as
    `TDSNull`. The shapes on DuckDB: 76 a row's cells joined, 10 a cell's toString, 8 both, 16 others.
  - 2 / 2 are the validation showcase: an integer-held Float in a constraint message used further, legend-engine
    `1000000`, lite `1000000.0`.
- **DateTime:** no move anywhere. A literal's text is the lowering's fold (`EdgeDateTimeTexts`).

**The plan** (posted to the orchestrator 2026-11-25):
- **S6a:** delete the value passes, and lift a last step's value print at the doors.
- **S6b:** legend-engine's TDSNull at its door before planning.
- **S6c:** F31's parses to legend-engine's door.

The orchestrator agreed S6a's design. Then the user questioned whether legend-engine's door should give lite's text at
all, and the orchestrator relayed: hold S6a's legend-engine-door parts (the passes' deletion, which both doors share
since `PureText.PURE_V1` and the corpus plan with `UPSTREAM`'s dialects; the two validation bodies; register S404, its
draft kept) and all of S6b; legend-pure's door may proceed. The full S6a was built and is kept aside for the ruling.

**S6a, legend-pure's door, built** (its lift HELD again the same day, with the passes: the design audit of S5a to S6a
below removed the lift and kept the cell printer):
- **One value printer (G1):** `edge/PureValueText`, the table printer's cell rules moved unchanged from
  `PureRelationText` (legend-pure's Float, the TDS date), and the value print of a Float, the same `pureFloat`.
- **The lift:** `PrintedRelations.liftValue` reads `x->toString()` as a last step, `x` a Float, with literal replaces,
  and the writer's new `VALUE` form prints it. PCT's route (`PctPlanRoute`) asks it at legend-pure's door alone. The one
  rule (`PrintedRelations.side`) and B3's test-body lift read relations alone, so legend-engine's door, the corpus and
  every test body are unchanged.
  - A Float only: it is legend-pure's door's measured need (H2's exponent cases), and a DateTime lift would write the
    DateTime rule twice while the pass stands.
- **Guards:** `WriterCarriageGuardTest`: `PureRelationText`'s `atLeast` and `doubleValue` move to `PureValueText`, newly
  scanned (a move, no growth). `JavaEvalLedgerTest`: `PureValueText.java` is registered beside the table printer, a
  move.
- **Tests:** `PrintedRelationsTest.aLastStepsFloatPrintIsLiftedAtLegendPuresDoor`,
  `PureRelationTextTest.aLastStepsFloatIsLegendPuresPrint` and the writer's VALUE form, PCT's
  `PctPlanRoutePrintTest.aFinalFloatToStringAnswersLegendPuresPrintOfTheValue` (a computed `1e23` proves the lift ran:
  the database's cast writes an exponent).
- **Expected moves:** at legend-pure's door, a last step's Float prints legend-pure's text on every database. H2's two
  exponent cases move toward Pure; the round measures them.

### The merge of B3 step 5a and its client fix (2026-11-25, `0720d8735`)

B3's step 5a (`b0df6ea84`: a comparison across known types refused by the product, answered by each door) and its
client fix (`96f0d33c0`: a `#TS` island's lifted translation typed by the island's rule, the datacube's presets in one
type, IQ's settings cases as islands) merged into B2's branch. It replaces the merge set aside on `b2-merge-b3-5a`
(`70b8a028d`) for the datacube and python regressions the fix answers.
- **Conflicts:** the register (B3's S401 note beside B2's S402 and S420); the parser floor (each side 3345 -> 3346, a
  model of its own; the merged tree 3347, `PeRatchets.HISTORY`, the standing word on parser bumps); the measured pins
  (channel B's judge ratchets, core's and the corpus's lean SQL), regenerated on the merged tree and checked against
  both parents: none grew past the larger.
- **The full round:** every lane green (646 tests; the local gate with `--lockfile_mode=error`, core and its lanes, the
  edge on H2 and SQLite, every PCT lane, channel B, the engine lanes, `edge_net`, the corpus on DuckDB and H2). The 14
  Postgres, `is_numeric` and datacube targets failed only as root and passed on the unprivileged rerun.

### The CSV's two questions, ruled (2026-11-25)

The review of S5c asked two semantics questions, posted to the user through the orchestrator with both options each
(the expression, upstream's answer, lite's):
- **Q-C**, `#TDS ... #->filter(<none>)->toCSV()` and a one-column relation's one null row. Upstream (legend-engine's
  helper, helperFunctions.pure:208-212): `s\n\n` for both. Lite until then: `s\n\n` for both too. (A) keep the
  helper's layout and the ambiguity; (B) every row ends with its own newline, so no rows is `s\n` and the null row
  `s\n\n`, as `result/CsvWriter` writes the header alone. Lean: B.
- **Q-D**, an enumeration's value named `null` under `toCSV(t, true)`. Upstream: the helper writes the value's name
  `null`, as it writes any value, and `TDSNull` for a null. Lite: `null` for both, since its null word is `null` and an
  enumeration's cell is not quoted unless its name holds a comma, a quote or a line break. (A) keep the collision and
  record it; (B) quote every enumeration's cell under the flag. Lean: A.

The user ruled both the same day ("Okay to both csv ones"):
- **Q-C (B): no rows is the header line alone.** Every row ends with its own newline (`lowering.ProductRelationText`,
  the relation's and the raw grid's CSV), so a one-column relation's one null row (`s\n\n`) is told from none (`s\n`).
  legend-engine's helper writes `s\n\n` for both: `joinStrings('', '\n', '\n')` over zero rows is `'\n'` (read in the
  pinned sources, helperFunctions.pure:208-212, and legend-pure's `joinStrings` tests). The departure is recorded in
  S420 and on the differences page. Pinned: `ProductRelationTextTest` (no rows, with and without the flag; one null
  row), `RenderCsvIntegrationTest.emptyRelationRendersTheHeaderLineAlone`, and the raw grid's in `TaggedListTest`
  (no rows `A\n`, one null row `A\n\n`).
  - **Moves:** core's lean SQL, DuckDB's `toCSV(t)` row 1,372 -> 1,378 characters (each row's newline concatenated,
    the separator empty) and `toCSV(t, renderTdsNull)` 406 -> 405, its plan JSON 8,028 -> 8,024: the ruling's direct
    cost. The corpus's lean SQL did not move. No PCT or corpus verdict moves: the one corpus golden of an empty CSV,
    `testEmbeddedMapping.pure:202`'s `testIsEmpty` (`'name,firm\n\n'`), is a printed side B3's lift reads and the
    Java printer writes in legend-engine's layout (the host's record shows the assert made `assertSameElements`,
    which `UpstreamPrints` writes only for a side it lifts).
  - **Not lifted, so lite's text at every door:** a raw grid's and an unnamed pivot's `toCSV` as a last step
    (`PrintedRelations.knownColumns`): an empty grid prints its header line alone where legend-engine writes a second
    newline. Recorded in S420 and on the differences page (the review of `e4bfa8677`).
- **Q-D (A): an enumeration's value named `null` under `toCSV(t, true)` is written `null`, as a null is.** An
  enumeration's cell is never quoted; the collision is kept and recorded in S420 and on the differences page. No code
  change.

### Pushed: `d7af55aae..7c2ac303d` (2026-11-25)

S5c, the CSV rulings, the TDS csv amendment (S405), S6a's legend-pure door, the merge of B3 step 5a, and each one's
review fixes. Gates: the local gate (`--lockfile_mode=error`) and the full round on `7c2ac303d`, every lane green (646
tests, 1 skipped). The 14 Postgres, `is_numeric` and datacube targets failed only as root and passed on the
unprivileged rerun. No PCT, edge-contract or engine-lane ledger moved: no lane reports a newly passing case. Reported
to the orchestrator.

### S7: measured; two questions posted (2026-11-25)

**S7a, the checked graph's UNEVALUABLE record** (`SqlExpr.RaiseText.asData`, `PlanTypesAreDataTest.PROSE_RESIDUE`).
The record's kind and operands have no reader. Since S3a deleted the edge's `raiseMessage` overrides, the one reader
is `AnsiSqlRenderer.site`, and it writes `asData` back as a plain string literal. `SqlRewriter`, `Windows` and
`MirJson` copy it. The options, as posted:
- **(A)** a plain literal, as S3b wrote `CONSTRAINT_VIOLATED`'s sentence (the sentence folds the constraint's id, a
  model fact, and the database decides per row whether the defect appears). The SQL stays byte-identical, the plan
  JSON shrinks, and `PROSE_RESIDUE` loses its entry. The midpoint audit called this relabelling (G4).
- **(B)** the defects worded in Java from typed rows, the midpoint audit's intent. The database builds a checked
  answer as JSON (`lowering/CheckedEnvelope`, the product's too), so this is a redesign of the checked answer on the
  product path.

Recommended: (A).

**S7b, legend-engine's envelope for a value on pure/v1's execute.** Lite refuses every value answer by name
(`EngineAnswers`' VALUE arm). legend-engine's answer for a store-bound count, as the engine runner answered it
(`tools/engine-runner/src/test/resources/pure_v1/engine_*_answers.tsv`), is a `dataTypeBuilder` envelope whose one
column is named by the database's own label for upstream's unaliased SQL expression (`COUNT(*)` on H2,
`count_star()` on DuckDB). Lite aliases its columns, and upstream's SQL text is never promised (CP-4). The options,
as posted:
- **(A)** keep the refusal;
- **(B)** answer the envelope with lite's column name, a new difference recorded;
- **(C)** (B) after asking the engine runner for the other value shapes (a constant, a String print, a Float).
  legend-engine answers a host-side constant through another result type.

Recommended, if values are to be answered: (C). It touches legend-engine's door, so it is treated as covered by the
hold.

**S6c is held with S6a's legend-engine parts.** The B3 session's direction moves F31 into legend-engine's door
rewrite when `PurePrintForms` goes, and that deletion is what the user is weighing. The rewrite also reaches
store-bound code alone, where the SQL pass reaches every parse in an edge plan, so the measurement belongs with
that ruling.

### S6c measured, and legend-engine's value answers asked (2026-11-25)

**S6c's measurement.** At the orchestrator's request ("measure, but don't build yet": a design under discussion may
move legend-engine's meanings into an edition the typer reads), a temporary probe turned off the edge's F31 pass
(`UnguardedParses`) at every door and kept the other passes. It ran on every compatibility lane: the edge tests and
their H2 and SQLite integration lanes, PCT A on DuckDB, SQLite and H2, the engine lanes, `edge_net`, and the corpus on
DuckDB and H2. Nothing was committed. Two things moved:
- **legend-engine's door, the pure/v1 golden net on H2, `raise.parseInteger`, away from legend-engine.** With F31
  (today, pinned) lite answers legend-engine's text, `JdbcSQLDataException: Data conversion error converting "abc";
  SQL statement: SELECT CAST('abc' AS BIGINT) ...`. Without F31 it answers the product guard's `Raised: Could not
  convert string 'abc' to INT64`. Its plan text moves with it.
- **`InterimEdgeSitesTest`:** the edge plans' raise sites with a computed operand, 4 -> 5 (the parse guard's site).

No PCT ledger moved on any database, so nothing at legend-pure's door, and neither engine lane, the corpus nor the
edge integration lanes moved. The one measured case is a store-bound parse (inside an `extend` over a store relation),
so a door rewrite, or an edition rule, that reaches store-bound code keeps it as it is. Posted; held for the ruling.

**legend-engine's value answers** (for S7b; the engine runner, legend-engine 4.145.0 on H2, the pure/v1 goldens'
trades model; a temporary runner target, not committed):
- `...->size()->toString()->from(RT)`: a `dataTypeBuilder` of type String, the relational activity, and one column
  named `CAST(COUNT(*) AS CHARACTER VARYING)`, value `"128"`.
- `...->size()->toFloat()->from(RT)`: type Float, the column `CAST(COUNT(*) AS FLOAT)`, value `128.0`.
- `...->select(~[year])->limit(1)->toString()->from(RT)`: type String, SQL `select top 1 cast(year as varchar)`,
  value `"2023"`. legend-engine's `toString` of a relation in a store query casts its first column, where Pure's prints
  the table: a meaning difference if values were answered.
- `->toCSV()` over a Relation: refused at compile ("Function does not exist 'toCSV(Relation<(year:Int)>[1])'"), its
  `toCSV` TDS-only.
- A constant (`|'abc'`, `|1.5`, `|7`): planned as a Java platform node (generated Java, `return "abc";`). The runner
  could not run it (its classpath lacks legend-engine's plan-dependencies package), so legend-engine's envelope of a
  constant is unknown.

Each answered column is named by H2's label of legend-engine's unaliased SQL expression, which lite cannot write
without upstream's SQL. Posted to the orchestrator with S7b.

### The design audit of S5a to S6a, and its fixes (2026-11-25)

An independent design audit of `1d6da1b6b..7c2ac303d` (a subagent, read-only) returned "sound to continue", with one
must-fix. Each finding was checked against the code and the pinned sources before it was acted on.
- **Must-fix, posted to the user: S405 went to the user without Pure's answer.** legend-pure's `csv` of a TDS is its
  source text: the names joined `", "`, then the rows exactly as written, with indentation kept and the closing
  newline trimmed (5.99.0's `TDSExtension.java:91, 214-277`; its tests `tds.pure:45, 62`). So
  `#TDS\n  i:Integer, s:String\n  3, c\n#.csv` is `i, s\n  3, c` in Pure, `i,s\n3,c\n` in lite's product, and
  `i, s\n3,c` at legend-pure's door (legend-engine's layout). S405 states it. Posted with three options: keep the
  ruling and record the difference; a literal's csv answers Pure's source text in the product (a planning-time
  fold); or legend-pure's door alone answers it.
- **Posted to the user: a raw grid's last-step `toCSV` at the doors.** Not lifted (`knownColumns`), so lite's CSV at
  both doors. Recorded on a review's word only, never ruled. Options: keep; refuse by name; or relax `knownColumns`
  so the door prints upstream's text from the columns the run announces.
- **S6a's value lift held with the passes.** It lifted a last-step Float `toString` at legend-pure's door, moved no
  measured case, and left a print used further to the passes. `(1.0E-5*1.0)->toString()` printed `0.00001` while
  `...->toString()->length()` read the cast's `1e-05`: two printers at one door (G1). Before S6a the door wrote the
  cast everywhere.
  - Removed: `PrintedRelations.liftValue`, the writer's `VALUE` form, PCT's route choice, `PureValueText.value`,
    and their tests.
  - Kept: `PureValueText`, the cell printer, as a move.
  - The full S6a stays in the worktree's stash for the ruling.
- **`Presenter.legendPure(Object, String)` deleted.** It was a second legend-pure Float printer with no caller since
  BE (`84a063611`). Its Float was `carriedFloat`, which is not legend-pure's DecimalFormat (`1e23`: `100...0.0`
  against `99999999999999990000000.0`). `PureValueText` is the edge's Pure printer. B3's homework named
  `legendPure` as a cheap Pure printer; B3's branch has no caller.
- **toString over a value whose columns are unknown is refused by name.** A cast to `TDS<Any>` reached an internal
  error. The CSV writer's refusal is shared (`ProductRelationText.known`) and pinned.
- **S311 amended for toString's and toCSV's Float cell** (program text, six decimals: `0.1234567` is `0.123457` and
  `1.0E-7` is `0.0`; toCSV's since the user's ruling of 2026-11-25 the database's own full-precision text: "toCSV's
  Float cell exact", below). The CSV output format writes a returned Float exactly and ends a line with CRLF. S420's "two
  writers" sentence now states what the two share. Pinned in `ProductRelationTextTest`.
- **Wording:** a pivot whose columns nothing names is refused by name, never "lite's text at every door"
  (`ProductRelationText`, S420, the differences page).
- **Recorded, not changed:**
  - The literal processor in `PurePrintForms` prints a Float literal by `carriedFloat`, which differs from
    legend-pure's print on values like `1e23`. That is legend-engine's door's processor, held.
  - A Float inside a list or a map under `Presenter.LEGEND_PURE` (the pure/v1 Arrow route's structures) is
    `CellText.element`'s `carriedFloat` too (the narrow review of `0dba5d604`). It is the same `1e23` gap, left
    with the doors' ruling.
  - The lift policy is coded in two harnesses (`PctPlanRoute`, `EdgeCasesGoldenTest`). One edge entry point would
    keep the doors from drifting; for when the doors' design is ruled (the orchestrator's "edition" question).
  - Two DateTime printers exist, `Presenter.pureZoned` in Java and `PureDateTimePrint` in SQL. They are one each
    side of the held passes.
  - Channel B's lite Relation judge pins roughly doubled at S5a (`08c2f9e90`), a measured and dated move.

### toCSV's Float cell exact (2026-11-25)

**Ruled** by the user the same night ("okay make exact", relayed by B3, the v2 amendments' item 15). "Exact" means each
database's own full-precision text of the double (its cast), not six decimals, and not Java's double printing ported to
SQL.

**First built** as the Float carrier's cast (a local version, never pushed), the text a list's Float
element has. The slice's review found that four databases' carrier COMPOSES its text in SQL (Snowflake, BigQuery,
MariaDB/MySQL, MongoDB) and four re-spell their cast (Postgres, ClickHouse, Databricks, Trino). B3 then decided (B)
under the user's objection to text algorithms composed in SQL: a node for the database's own full-precision text,
spelled per dialect with the database's own cast or format function, no composition; the list carrier left alone.

**Built as (B):**
- `SqlFn.FLOAT_FULL_TEXT`, one operand, a double, NULL kept NULL; an arm in every dialect: the plain cast to text on
  DuckDB, H2, Postgres, Trino, Databricks (`STRING`) and BigQuery (`STRING`); `CAST AS CHAR` on MariaDB and MySQL;
  `toString` on ClickHouse (its CAST to a non-Nullable String refuses a NULL); SQLite's `printf('%!.17g')` with its
  NULL `CASE` (its cast writes fifteen digits); `$toString` on MongoDB; on Snowflake its DECFLOAT cast, NaN and the
  infinities by its plain cast, one `CASE` between the two (its format models cap a FLOAT at sixteen digits, measured
  2026-10-24, so no Snowflake format function writes seventeen: posted, and B3 agreed (b) the same night).
- `lowering/ProductRelationText.exactFloat` writes it over the double (a value the database holds as an integer or a
  decimal is read as a double first, a `DECLARED` cast). A relation's `toString` keeps S311's six decimals.
- `SqlTyping` (text), `MAY_SKIP_AN_ARGUMENT` (SQLite's and Snowflake's `CASE`), MySQL's delegation list, and
  `SqlFnSortedTest`'s `PRODUCT` (a new function only product code writes, with a dated note: the standing rule).
- `CodeShapeGuardrailTest`'s method limit caught `BigQuery.call` and `Postgres.call` at 252 lines (limit 250): their
  arms take one line each, the spelling stated in `SqlFn`'s javadoc.

**Measured exact** (every cell reads back as the double the database held, a literal and a computed one):
- on the lanes, pinned (`ProductRelationTextTest.aCsvsFloatCellIsTheDatabasesExactTextOfTheDouble`): DuckDB `1e-05`,
  `25000000.0`, `-0.04999999999999999`; H2 `1.0E-5`, `2.5E7`; SQLite `1.0e-05`, `-0.049999999999999989`. The same
  texts as the carrier version: these three databases' carrier was their own text already.
- by a temporary probe on the embedded databases (the same test, six more values, never committed): Postgres `2`,
  `1e-05`, `25000000` (its shortest digits while the session's `extra_float_digits` is above zero: pgjdbc sets 3); MariaDB and MySQL `2`, `0.00001`, `0.0000000001`; ClickHouse `2`, `0.00001`, `1e-7`; Trino
  `2.0E0`, `1.0E-5`, `2.5E7`. No re-spelling was needed for exactness on any of them.
- MongoDB, pinned to read back on its manual lane after the review (`MongoDbPlanTest`, seven doubles, the round of
  2026-11-25): `1e+20`, `1.5e-08`, `2`, `0.30000000000000004`, `123456789.12345679`, `1e-300`.
- **Found by the probe, not this slice's:** ClickHouse held the literal `123456789.12345679` one unit in the last place
  low (`123456789.1234568`). Its text is exact: the computed cell beside it, the held value plus 0.2, read back to the
  bit. So it is the literal's parse, likely ClickHouse's imprecise float parsing (`precise_float_parsing`, off by
  default). Posted to B3.
- Unmeasured as this node until their lanes run (a register note, not a pin): Snowflake (seventeen digits always in
  exponent form, `0.1` as `1.0000000000000001E-01`, the seventeenth digit double-rounded for 80 of 1,946 doubles that
  still read back, measured 2026-10-24), BigQuery (its cast's seventeen digits read back on its lane, 2026-10-21)
  and Databricks.
- NaN and the infinities: each database's spelling (DuckDB `nan`, `inf`; SQLite `Inf`), which Java's parser does not
  read.

**Records:** S311's amendment (`d-register-rows`), S420's two-writers sentence, the differences page's §14.

**Tests:** `ProductRelationTextTest`, the per-lane pins and the read-back; `RenderCsvIntegrationTest`'s Float cell,
`csvTextPrintsAFloatCellAsTheDatabasesExactText` (until then `0.00001`); `FloatTextTest`, the Float CSV the same at
every door (the edge's passes do not touch the node).

**The fast check:** green but for the method limit, fixed. The core lean pins did not move: no pinned toCSV case has a
Float column.

### S7a built, S7b kept (2026-11-25)

**Ruled** by the user through B3: S7a "go with A"; S7b "fine", the refusal kept.

**S7a (A), built:** the checked graph's unevaluable-constraint defect is a plain string literal, as S3b wrote
`CONSTRAINT_VIOLATED`'s sentence (`lowering/CheckedEnvelope`: `ErrorKind.CONSTRAINT_UNEVALUABLE.sentence` of the
constraint's id, a model fact; the database still decides per row whether the defect appears).
- `SqlExpr.RaiseText` loses `asData` (now `(kind, operands)`); `SqlRewriter`, `Windows` and `MirJson` (writer and
  reader) no longer carry it; `AnsiSqlRenderer.site` loses its branch.
- `RaiseKind.CONSTRAINT_UNEVALUABLE` is gone, and `ErrorKind.isRaise` excludes the kind (no plan raises it).
- The SQL is byte-identical; the plan JSON carries a string literal where it carried the record.
- Guards: `PlanTypesAreDataTest.PROSE_RESIDUE` loses `SqlExpr$RaiseText.asData` (a shrink, dated);
  `RaiseTextTest`'s components and its moved sentences; `InterimEdgeSitesTest`'s condition; the generated host tables
  (`engine-client/src/generated/lite-raise.ts`, `python/legend_lite/_type_facts.py`) lose the kind, checked by their
  diff tests.

**S7b:** pure/v1's execute keeps refusing a value answer by name (`EngineAnswers`' VALUE arm). Nothing changed.

## Audits

| When | Scope | Findings | Fixed |
|---|---|---|---|
| 2026-11-25 | toCSV's Float cell exact, the carrier version (local, never pushed; reworked to B3's (B)), a subagent, read-only | verdict: the code right on the three lanes run (the outer carrier cast fires, the inner cast plain, only Float cells change, the read-back exact). 1 must-fix: the register row and the record said MariaDB, MySQL, BigQuery and MongoDB write their own casts and Snowflake was the one exception -- BigQuery, MariaDB/MySQL and MongoDB compose their text in SQL too. Should-fix: FloatTextTest's toCSV half passed by coincidence, the edge window closed for toCSV's Float cell. Nits: Postgres, ClickHouse, Databricks and Trino re-spell their casts; NaN and the infinities unspelled; a DECIMAL-held Float's conversion; the inner cast's reason; RenderCsvIntegrationTest's values cannot tell 15 digits from 17 | every dialect's text stated as the code writes it (javadoc, S311's row, the record), the composed ones posted to the user; FloatTextTest re-pointed (the Float CSV the same at every door) and the window's closing recorded (ProductRelationText, the differences page); NaN's spelling and the DECIMAL conversion stated; the inner cast DECLARED; the 15/17 distinction left to ProductRelationTextTest's computed column |
| 2026-11-25 | toCSV's Float cell as B3's (B) (`SqlFn.FLOAT_FULL_TEXT`) and S7a, both commits, a subagent, read-only (a whole-repo scan, no Bazel) | verdict: no must-fix; every dialect's arm the database's own text, NULL kept, the operand a double, the edge leaving the node alone, the records matching the pins, S7a's SQL byte-identical. Should-fix: Snowflake's DECFLOAT cast unguarded in a CASE whose branches may all be evaluated (NaN would raise); the differences page's "at every door" (a last step's toCSV at the compatibility doors is printed in Java). Nits: no plan-version bump for S7a's removed field and kind; two stale `asData` comments and the commit message's "count"; a test javadoc and two record lines from the first version; Postgres's exactness resting on `extra_float_digits`; MongoDB unprobed | all fixed: the DECFLOAT cast reads a non-finite value as NULL (`IFF`); the page says the product and a toCSV used further; `PlanJson.VERSION` 61 with plan-runner.ts and _runs.py; the comments, javadoc and record pointers; the setting named; MongoDB's `$toString` pinned to read back (seven doubles, `MongoDbPlanTest`, its manual lane). The review's own catch beside it: the first round failed to build, six `RaiseText` calls still passing the record's null, fixed in S7a |
| 2026-11-25 | the design audit's fixes, `0dba5d604` and the record `978edd653`, a subagent, read-only | verdict "sound"; no must-fix. Should-fix: `PrintedRelationsTest`'s class javadoc still named the lift. Nits: S405's "closing newline trimmed" true of a `#TDS` literal alone (`stringToTDS` keeps it); the record's S6a section read as live; `asFloat`'s one-valued parameter; `Presenter`'s javadoc broader than the code (a Float in a list under `LEGEND_PURE` is `carriedFloat`); S420's DateTime example no difference between the two writers. Checked fine: the lift fully removed, PCT's route byte-identical to before S6a, the deletion's helpers and the planted reach, `known` before any plan is built, the docs' Pure answer and S311's claims, no pin moved | all, in the next commit |
| 2026-11-25 | B2b's design audit of S5a to S6a, `1d6da1b6b..7c2ac303d`, a subagent, read-only, given the tenets and the record | verdict "sound to continue"; 1 must-fix: S405 ruled without Pure's answer (a TDS's csv is its source text), and legend-pure's door's literal csv (legend-engine's layout) unrecorded. Should-fix: toCSV's Float cell six decimals where the CSV output format writes the value exactly, unrecorded (S311); S6a's value lift opened two printers at legend-pure's door and moved no case; `Presenter.legendPure` a second Float printer with no caller; toString over unknown columns an internal error; a raw grid's last-step toCSV at the doors never ruled. Notes: the pivot wording; the corpus pin moved ahead of its ruling (restored before the push); the lift policy in two harnesses; two DateTime printers; the probe in the tree. Found fine: tenet 6, tenets 1-2 on the product path, the guards, the lean figures, the Java replace, S6a's move | S405 posted again with Pure's answer and recorded; the raw grid posted; the value lift held with the passes; `legendPure` deleted; toString's refusal shared and pinned; S311 amended, S420 corrected, pinned; the wording fixed; the harness duplication and the DateTime printers recorded for the doors' ruling (`0dba5d604`) |
| 2026-11-25 | the TDS csv amendment and S6a's legend-pure door, `d6e94cdfe`, `abbc25278`, `e70bd764a`, a subagent, read-only | 2 must-fix: a relation's column named csv (`String[*]`, the typer's column read) was taken by the TDS csv arm, which checked the name alone, and answered the relation's CSV (refused by name under S5c; the door's lift and the resolver's anchor had the same test); the corpus lean pin's figures equal the pre-S5c ones, suspected restored by hand. Should-fix: the refused shape misnamed ("the legacy TabularDataSet class", which declares no csv; the shapes that reach it are a `TDS<Any>` cast the resolver does not peel), and toCSV reaching an internal error there; `WriterCarriageGuardTest`'s comment described the held full S6a. Nits: `liftValue` read no multiplicity; stale text (the lift's javadocs, S402's citation, `PlanEnv`'s orphaned javadoc, history notes in the present tense, the record's pointers); the remaining refusal a `NotImplementedException`, as `ProductRelationText`'s other refusals | one test, `PlatformTypes.isTdsCsvRead` (the `String[1]` property, never a `[*]` column read), in both Lowerer arms, the door and the anchor, pinned (`RenderCsvIntegrationTest`'s column named csv, `PrintedRelationsTest.aColumnNamedCsvIsNotATdsCsv`); the pin was regenerated, and the identical figures are explained (the assert's csv is the judge's statement; the counted one is the relation read, unchanged); a value whose columns the compiler does not know refused by name in the shared CSV writer, by toCSV and a TDS's csv alike, pinned, its shape named in S405 and the record; the guard comment and the stale text fixed; `liftValue` reads a to-one operand. Kept: the refusal's exception type, the CSV writer's own |
| 2026-11-25 | the CSV rulings, `e4bfa8677`, a subagent, read-only | 1 must-fix: core's lean SQL pin not re-pinned in the commit (DuckDB's toCSV(t) 1,372 -> 1,378 characters). Should-fix: a stale javadoc (no rows "the header and two newlines"); the raw grid's empty CSV unpinned; the doors' scope overclaimed (a raw grid is not lifted, so its CSV is lite's at every door); Q-C's and Q-D's options not recorded; no measurement recorded. Nits: Q-D's "never quoted"; S420's Q-C sentence had no subject; S5b's no-rows line not marked superseded; the differences page's When | all fixed in `413960af9`: the pin re-pinned and dated in the commit (the ruling's direct cost); the grid's `A\n` and `A\n\n` pinned; the raw grid's exception stated in S420, the differences page and ProductRelationText's javadoc; the options recorded as posted; the measurement (no PCT or corpus verdict moves) recorded |
| 2026-11-25 | B2b S5b's fix round and S5c, `d7af55aae..3a4fe1d81`, a subagent, read-only | 3 must-fix, each a guard the deletion left red: `SqlFnSortedTest` (ENDS_WITH, LENGTH, MINUS and STRPOS no longer written by the edge), `EdgeOnlyMembersTest` (`DateFmt#TIME_ONLY`, now read by nothing), `PrintedRelationsTest` (a csv over a cast to a TDS is now lifted). Should-fix: an empty relation and one null row in a one-column CSV both `s\n\n`, where `result/CsvWriter` writes the header alone (a semantics question); the refusal's sentence named legend-pure's text and the edge; pure/v1's execute does not lift; the moves unmeasured; the PAD SPACE fix pinned on no MariaDB or MySQL lane. Nits: a comment on the four-argument form, an enum value spelled `null` under the flag, stale comments, merge markers in the fix round's commit. The fixes (a), (b), (c), (d) clean, `peelTdsCasts` clean, no loosening | the three guards (the first two dated shrinks, the deleted format with its pin; the third a lifted cast); the sentence names legend-engine's text and the doors, and a `#TDS` literal's csv, which reached the renderer as a mixed list, is refused by it too; pure/v1 refuses every value answer today, recorded; the moves measured by the full round; a plan pin of `CHAR_LENGTH` on MariaDB and MySQL (`sql/dialect/CsvEmptyStringTest`: a dialect's test, the one place a test may name both databases); the comments corrected; the merge markers squashed out. Posted to the orchestrator: the empty relation (Q-C) and the enum named `null` (Q-D) |
| 2026-11-25 | B2b S5b, `d7af55aae`, a subagent, read-only | 1 must-fix: on MariaDB and MySQL (PAD SPACE) the empty test `= ''` matched a String of spaces, written `""` (a value lost). Should-fix: the four-argument form admitted and its formats ignored (invariant 4); every non-String cell escaped (each date repeated 6 times, lean SQL); S312 and S316 not amended for toCSV; the window unpinned for toCSV; test gaps (a raw grid under the flag and its empty text, CR/LF, a quoted header name, Variant and enumeration cells, the toCSV refusals); the corpus and PCT to read for an unlifted toCSV. Nits: wording (Render, the parity test's title, a test name, DIFFERENCES §14), S420's reader evidence (pandas lossy; DuckDB's quoted nulls), an Any cell's empty text, a Variant's JSON null under the flag, a late-bound relation's refusal words, a date carried as text | the empty test reads the length, folded into the escape (an Any cell's empty text with it); a number's, a Boolean's and a date's text unescaped (a fold of a type's fact); the four-argument form refused by name (Q-A) and a Variant quoted under the flag (Q-B), both ruled by the orchestrator 2026-11-25 and named in S420; S312 and S316 amended for S5b; FloatTextTest's window pin covers toCSV; the tests added (TaggedListTest's raw grid CSV, its empty text and its flag refusal; ProductRelationTextTest's CR/LF, spaces, a quoted header name, an enumeration, the mixed-column and four-argument refusals); S420's readers quoted from PostgreSQL's and DuckDB's documentation; the wording fixed; the corpus and PCT read on the full round of `d7af55aae`, no unlifted `toCSV` moved. Left as recorded: a Variant cell unpinned in the lane tests (no Variant on H2 and SQLite there), a late-bound relation's refusal words, a date carried as text (probe at S6) |
| 2026-11-24 | B2b S5a, `1d6da1b6b`, a subagent, read-only, given the code, H14's spike and the rulings | 2 must-fix: under the edge's style the edge dialect's value passes re-spell the table text's Float and DateTime cells, the pin of that deleted and nothing recorded, the javadoc's "whatever the plan's style" false; channel B's lite ledger stale (423 rows expect the deleted refusal). Should-fix: tests (a sort by a dropped column, a Variant cell, a quote or a backslash in a braced String, a Number or Decimal cell, a stored relation, the refusals). Nits: stale wording (`JavaEvalLedgerTest`, `TextRefusals`, `Render`, `RelationTextLowering`, the parity test, `AggOrderNullPlacementTest`), double blank lines, a Number on a DECIMAL slot unpinned, a relation with no sort. Invariants, the order's lift, the cells' rules and the guards found clean | the window recorded and pinned as the edge's own text (`FloatTextTest`), the javadoc corrected; the ledger re-measured (390 leave, 33 re-recorded, the G2 gap) and the judge pins moved by the measured cost; the tests added (a Decimal is its database's scale, S400, so it is not one text on every lane; a Variant cell is channel B's lite lane's, whose Variant tests now pass); the wording and blank lines fixed; the Number and no-sort nits recorded. A DISTINCT select ordered by an expression stays unpinned (no Pure relation function writes one) |
| 2026-11-24 | B2b's MIDPOINT design audit, `54f8be8e3..607d24dcc`, a subagent, read-only, given the plan of record, the tenets and the record | verdict "sound to continue"; no must-fix. Should-fix: the CSV prints unmeasured against the SQL text; two mechanisms in one defect object; `side()` at S5/S6; legend-pure's value prints at S6. Nits: a String cell under any column type; the ledger's reason; a Decimal in a Number column; the route's comment; two differences not yet register rows | the CSV measured and pinned (gap 4; a TDS literal's csv); the record kept, with the orchestrator's agreement (G4); S5/S6 and S4/S6 items carried; the nits fixed or recorded; (a) and (b) for the user's word after S4 |
| 2026-11-24 | B2b S3, `ba8acecdb`, a subagent, read-only | must-fix: the generated host tables (`lite-raise.ts`, `_type_facts.py`) list every `RaiseKind`, and a new one was added without regenerating them. Questions 1-5 clean: S3a's text byte-identical for every id; the old message a bare literal, the children walk in step, the simple name identical, the guard edits legitimate. Nits: two stale comments (`SqlExpr.RaiseText`, `RaiseTextTest`); the null message's children walk unpinned | the must-fix made moot: `CONSTRAINT_VIOLATED` is no `RaiseKind` (the gate also measured the record's plan growth, so the sentence is a plain literal); the comments corrected; the children walk pinned |
| 2026-11-24 | B2b S2's fix round, `6fc717f9f` and `8551b293d`, a subagent, read-only | no must-fix; the four fixes confirmed (`knownColumns` is at least as strict as the lowering's special cases, and lifts a pivot whose columns a cast names), and the native-claims census right. Should-fix: a mixed (tagged) column passes `knownColumns`, so the door lifts its print and the printer refuses it where the SQL text answered it. Nits: the toCSV route test passed on either road; four unpinned shapes; the record's lane count and when the lanes ran; the predicate's home when `RelationTextLowering` goes | the mixed column measured: refused by name on both roads today (a tagged value reaches the renderer only at a value's root), pinned on both; the route test proves the lifted branch (a Float the two roads print differently); a pivot named by a cast pinned as lifted; the record corrected. The raw grid, the no-column relation and the zero-argument format functions (absent from lite's catalog) stay unpinned |
| 2026-11-24 | B2b S2's design, `54d922e92`, a subagent, read-only, given the code, the plan of record and the tenets | no must-fix; lifted forms, the lifted query, the route's answer and the tenets clean. Should-fix: the one-plan measurement planned the print's query, not the relation's; a dynamic pivot's and a raw grid's prints lifted where the lowering refuses or answers them by its own rule; the four-argument toCSV with the default formats not lifted; the window with two printers unrecorded. Nits: a redundant `withOptions`; a `csv` over a cast not lifted (kept: the resolver's); toCSV's argument not checked to be a relation. Tests: toCSV's forms, the `csv` property, a non-literal replacement, the options, the kept statements, a dynamic pivot | all, in this round; the cast-wrapped `csv` stays the run's |
| 2026-11-24 | B2a's end design audit, `96e7c1fc4..54f8be8e3`, a subagent, read-only, given the plan of record, the tenets and the record | no must-fix; verdict "sound to build B2b on". Should-fix: `heldAsInteger` left in the product for the edge alone; an integer-held Float above 2^53 printed per database; the integer-held text change not posted for the user's word; the record's slice 4 and slice 1 wording; the S311 row's cloud-lane, per-value and MongoDB claims; the edge's integer-held digits unpinned. Nits: stale javadocs, the gate wording, the ledger header | all, in this round; the integer-held text ruled by the user 2026-11-24 ("Int as float agree") |
| 2026-11-24 | B2b S1's diff (`54f8be8e3..527d3e8a9`), a subagent, read-only, given the code, the pinned sources and the requirement | 1 must-fix (the typed header's multiplicity); should-fix: the typed header's precise types, the date-time's digits against the edge's rule, legend-pure's Float formatter, a decimal under Float, a Number of integers' javadoc, toCSV's date-time cells against today's SQL, mixed columns; nits: dates the CSV cannot write, enumerations' quoting, the empty replace, the writer's refusals' timing | all, in this round; toCSV's date-time cells measured on the corpus at S4 |
| 2026-11-24 | the fix round's diff (`527d3e8a9..41d52b690`), a narrow re-review, read-only | 1 must-fix: the integer-held Float's cast to DOUBLE on H2, Postgres, Snowflake and MongoDB lost digits on Postgres and MongoDB (15-digit casts) and the claim of one text everywhere was false; should-fix: that cast unpinned, the date rule's divergence from TestTDS's millisecond cut unrecorded; nits: a String in an Any column not quoted, a DecimalFormat per cell, two stale record lines. The printer's rules, the moves and the guards found clean | the cast reverted and the difference recorded; the rest fixed |
| 2026-11-24 | slice 3's diff (`96e7c1fc4..396961c73`), an independent subagent, read-only, given the code and the requirement | 5 must-fix: MongoDB printed `NaN.0`/`Infinity.0` (an unconditional `.0`); channel B's ledger row read `got "00.0"` (a slicing slip); channel B's lite Essential judge pins not re-pinned (+264); `MongoDbPlanTest`'s two product texts stale; a Float held as an integer printed `2` beside a computed `2.0` (one text per type broken). 1 should-fix: the probe's whole-value check too loose (a lost sign or a wrong digit count passed). Nits: stale javadocs, long lines. No defect in the shared helpers' SQL (simulated on every six-digit text kind, the non-finite texts included); the edge untouched | all, in `08e37643b` and the slice's last commit; the probe pins -2.0 and 1e20 exactly, and NaN and the infinities exactly |
