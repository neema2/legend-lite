# Step 8: what was done about the outside audit of slices 3 and 4 (2026-11-26)

*The audit: `docs/records/edition-homework/audits/step8-slices3-4-independent-2026-11-26.md` on `claude/edit-ahead`
(2 blockers, 4 should-fix, 5 nits, over `git diff 4d44bf54 90d2759b5`). This record says what each item became. The
amendments to S428 and S429 below are DRAFTS for the user: nothing was added to the register.*

## The blockers

**B1, fixed.** `UpstreamPrints.anyPrinted` read a side with no lets in hand, so a body whose one print was a computed
DateTime bound by a `let` was returned untouched and its text was lite's. It now reads through the body's lets, as
the walk after it does. Test: `UpstreamPrintsTest.aComputedDateTimesPrintIsJavas_andOneThatReadsTheClockIsLeft`, the
audit's own body (`let d = <computed>; assertEquals('...25+0000', $d->toString())`).

**B2 (a), fixed.** A Float's `toRepresentation` at a last step at legend-pure's doors is lifted as its `toString` is:
legend-pure defines it so (`toRepresentation.pure`: `n:Number[1] | $n->toString()`; a Decimal's, a date's and a text's
are other texts and are not lifted). Tests: `PrintedRelationsTest.aFloatsToStringIsLiftedAtLegendPuresDoorsAlone`,
`PctPlanRoutePrintTest` (`toRepresentation(0.1 + 0.2)` answers `0.30000000000000004`), and the recorded case
`float.toRepresentation`.

**B2 (b), not built: for the user.** A last-step `format` of a computed Float is lite's text today. What a lift would
take, read in legend-pure's `PureStringFormat.format`:

- `%s`, `%r` and `%f` of a Float are its `toString` text put in the template; `%d` an Integer's digits; `%%` a percent.
  None rounds. So a lift of `format(<a literal template>, [args])` at a last step is a template filled in Java from
  one row of typed values: each Float by legend-pure's print, each text as it is (`%r` of a text quoted and escaped,
  a re-spelling), about 100 lines in `PrintedRelations` and one more form of the value writer, with its tests.
- `%.Nf` ROUNDS in Java, `%0Nd` pads and `%t{...}` is a date-format interpreter: a template that holds one of them
  would stay the run's (a rounding in Java is a computation, tenet 3), as would a template that is no literal.

Until the user rules, the moves are the draft amendment's rows 1 to 4 below.

## The literal the lift could not see: probed, fixed

The audit could not settle whether a DateTime literal under a native call or a cast reaches the lowering as a bare
literal. **It does** (probed 2026-11-26 on DuckDB, the lowering's own answer at legend-pure's door with the lift
off): `[lit]->toOne()`, `lit->cast(@DateTime)`, `lit->toOne()`, `[a, b]->at(0)` and `[lit]->first()->toOne()` each
print the literal's WRITTEN text (`2014-01-01T00:00:00.00+0000`), which is legend-pure's own answer and the base's.
The door lifted all five (a native's call or a cast at the root) and Java printed the instant, its written zeros
trimmed (`2014-01-01T00:00:00+0000`). An `if(true, |lit, |lit)` is not folded (lite's text) and was not lifted.

Fixed in `PrintedRelations.mayBeASpelledDate`: a DateTime's print is lifted only where no date the program SPELLED
can be the operand's value. The compiler folds literal structure and nothing else, so a spelled date reaches the
operand's place only through nodes that hand a value on. The rule errs to yes: a node hands a date on unless its own
value is no date, it is `adjust` (which computes a new instant), or it is a relation's column or a lambda's own
parameter. A date-typed node the door does not know (a user function's call, a class's property) is taken to be
one. Where the rule says yes the print is the run's: a literal the lowering reads prints its written text; a
computed value prints lite's text, the difference S429 already carries ("a last step the door cannot read").
Tests: `PrintedRelationsTest.aDateTimesToStringIsLiftedUnlessItIsALiteral` (the five shapes, through a let, and a
list of a literal and a computed value), `PctPlanRoutePrintTest` (three of them answer the written text).

## The should-fix items

**S1 and S4: drafts, below.** **S2: a probe for the lanes**, `probe-engine-date-time-cast-on-a-lane.md` beside this
record (the expression, the door, the two temporary edits, the command, what to read in the answer, Snowflake's
session setting: lite sets none). The row's set is kept as ruled until a lane shows otherwise.

**S3: read; lite matches.** legend-pure writes a year as the number it is, unpadded: `DateFormat.append`
(`legend-pure-m4`, `.../coreinstance/primitive/date/DateFormat.java:240`: `safeAppendable.append(date.getYear())`, an
`int`). `PureValueText.dateTime` writes `999-03-04T10:20:30.5+0000` for such an instant: held by
`PureRelationTextTest.aComputedDateTimeIsItsSecondsAndTrimmedFraction`. What the deleted SQL wrote for such a year on
each database was not measured (no recorded test has one).

## The nits

1. Stale "no literal" comments: `ArchitectureTest.PLANNER_ENTRIES`, `core/edge_tests.bzl`, slice 4's tests section.
2. `docs/dialect-capability/reach.tsv` still lists `FLOOR_RAW`: see "Measured" below.
3. The product's DateTime digits: `SqlFn.DATETIME_TO_TEXT` said six, `DateTimeText` nine. Nine since the user's
   amendment of 2026-11-22 on every SQL database; MongoDB, which holds milliseconds, writes six. Both notes and
   `NamedRule.DATE_TIME_TEXT_BY_CAST`'s now say so.
4. A runtime with no single database gets the call's own rule for `toString`, as a `WHERE_WRITTEN` row does: left.
5. A core test that RUNS the Float row against a database: the recorded cases `float.*` of `EdgeCasesGoldenTest`
   under the edge's legend-engine style run a Float's `toString` on every in-process database; the row itself needs
   a store query, held as run text by PCT's legend-engine lane and the corpus. A lane twin of
   `EngineDateTimeTextTest` for a Float is not added here.

## Drafts for the user: one table

Each line is an answer that moved with slices 3 and 4 and has no words in its row, or a product behaviour the tail's
parity tests found that has no row. "Base" is the tree before step 8's deletions (`4d44bf54`); its Float and
DateTime texts are READ from the deleted code by the outside audit, not run here. "New" is RECORDED on DuckDB
(`edge_cases_answers.tsv`; H2's and SQLite's lines are beside it). legend-engine's column is its SQL template, read
in the pinned 4.145.0 sources.

RECORDED_TABLE

## Measured

MEASURED
