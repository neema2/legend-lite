# Step 8, slice 4: a DateTime's last-step print and its row at legend-engine's door (2026-11-26) -- BUILT, NOT LANDED

*Written by the step 8 session (`claude/edition-step8`), on top of the Float deletion slice's scratch line (base
`claude/edit-ahead` at `b7c39067`). The measurement behind it is `moves-if-date-text-is-lites.md`. Nothing is
deleted here: the SQL composition that prints a DateTime at the edge (`edge/PureDateTimePrint`, the date arm of
`edge/PurePrintForms`) stays until the user rules on what its deletion does to a computed DateTime's text used
further (the orchestrator's question to the user, 2026-11-26).*

## What it is

Tenet 1 as the user amended it on 2026-11-25, and the user's ruling of 2026-11-26, "agreed on float/date text for
engine", for a DateTime that is NOT a literal:

- **(b) at legend-pure's doors, its `toString` at a last step is Java's.** The door lifts the print
  (`edge/PrintedRelations`), the database computes the instant, and `edge/PureValueText.dateTime` writes the text the
  edge's SQL composition writes today: the seconds, the fraction without its trailing zeros (none: no point), and
  `+0000`. With the tail after it (`edge/PrintTail`), as for a Float.
- **(d) inside a store query at legend-engine's door, its `toString` is a row of legend-engine's table:** the
  database's own cast of the timestamp to text, which is what legend-engine's `cast(%s as varchar)` writes
  (`lowering/NamedRule.DATE_TIME_TEXT_BY_CAST`), on the databases whose text of that cast has been run beside the
  door's (`PureText.ENGINE_DATE_TIME_CAST_READ`): DuckDB and H2, run here. On every other database the call keeps its
  own rule. **Postgres and Snowflake are not in the set yet, though their lanes have read the cast**
  (`../lanes/engine-templates-2026-11-26.md`: "a timestamp keeps its microseconds"): a set that names them is one
  more line of `PureText` naming a database, which `DialectBoundaryTest` counts (6 today), and a count that goes up
  needs the user's word. Reported to the orchestrator; the row widens to them, and later to Trino, ClickHouse,
  Databricks and BigQuery as lanes read theirs, when that is ruled.

**A literal is left exactly as it is today**, at every door: its written text, at its written precision, and `+0000`
(`%2014-01-01T00:00:00.00` prints `...00.00+0000`, as legend-pure does), a constant the edge's lowering writes
(`edge/EdgeDateTimeTexts`). It is not lifted, because no instant a database returns carries a written precision; and
it has no row at legend-engine's door, where the user has still to choose (`moves-if-date-text-is-lites.md`, 3: (A)
or (B)).

**What the door lifts, exactly** (the independent review of 2026-11-26, its first must-fix): the lowering reads the
operand AFTER the planner has inlined it, where a variable, a user function's call, a derived property or a mapped
constant may each have become a literal the door cannot see. So the door lifts only a computation whose root stays a
computation, a native's call or a cast, where it stands or through the `let` it is bound to
(`compiler.spec.typed.Lets.bound`); every other operand is left to the run, as before. At the body door (PCT's
channel B, the corpus) a value that reads the clock (`now()`) is left too: read as a query of its own it would draw
its own instant, where the assert's statement draws one for both its sides.

## Why no recorded answer changes

- **(b):** the Java print is held to the SQL's text. PCT's route test
  (`PctPlanRoutePrintTest.aFinalDateTimeToStringThatIsNoLiteralIsItsSecondsAndTrimmedFraction`) was first run against
  the SQL composition, before the lift existed, and passed; the same answers are held to the Java print now. The
  measurement found no recorded test on a DateTime that is no literal.
- **(d):** it changes an answer no recorded test holds: a column's or a computed DateTime's `toString` inside a store
  query at legend-engine's door was the edge's `+0000` text and is the database's cast on the four databases of the
  row. Run 2026-11-26 through the door on each lane:

  | The value | DuckDB and H2 (the row) | SQLite (no row: the door's text as before) |
  |---|---|---|
  | `2014-01-01T10:20:30.25` | `2014-01-01 10:20:30.25` | `2014-01-01T10:20:30.25+0000` |
  | `2014-01-02T00:00:00` | `2014-01-02 00:00:00` | `2014-01-02T00:00:00+0000` |
  | `2014-01-01T10:20:30.123456` | `2014-01-01 10:20:30.123456` | `2014-01-01T10:20:30.123456+0000` |

## What was measured

MEASURED-SECTION

## What the review found (Opus, high effort, given the diff and the rules)

Two must-fix, both fixed before the slice was measured again:

1. A literal the planner makes after the door has looked (above). Pinned: `PrintedRelationsTest` (a function's call
   and a `let` of one are not lifted), `PctPlanRoutePrintTest` (`f::d()->toString()` answers the literal's written
   text).
2. A DateTime the database holds as a day (a DATE column under a DateTime property) arrived as a day and was refused,
   where the SQL prints its midnight: the printer writes `<day>T00:00:00+0000`. Pinned: `PureRelationTextTest`.

Should-fix, done: the clock at the body door; stale comments in six files; the lane test asks the edge whether the
row answers on its database (`PureText.dateTimeCastRead`), so the row is seen to answer; the body door's tests; the
parity test below. Left, recorded: a year outside 1000 to 9999 is written by its digits in Java and padded to four
by some databases' SQL (no test, no pin).

## The tests

- `integration/PureRelationTextParityTest.aComputedDateTimesLastStepPrintIsTheEdgesSqlText`: THE PARITY TEST, on
  every lane: the text the edge's SQL composition writes and the text the door's lift prints in Java from the instant
  the same database computed, equal value by value. Kept in the tree while the SQL exists (the orchestrator's word);
  when the SQL is deleted its SQL half goes and the texts stay pinned.
- `UpstreamPrintsTest.aComputedDateTimesPrintIsJavas_andOneThatReadsTheClockIsLeft`: the body door.

- `PctPlanRoutePrintTest.aFinalDateTimeToStringThatIsNoLiteralIsItsSecondsAndTrimmedFraction`: the door's answers, a
  literal directly and through a `let` among them, and a tail's.
- `PrintedRelationsTest.aDateTimesToStringIsLiftedUnlessItIsALiteral`: what is lifted and what is not (a literal, a
  literal through a `let`, the abstract Date, a StrictDate, legend-engine's door's rule).
- `PureRelationTextTest.aComputedDateTimeIsItsSecondsAndTrimmedFraction`: the Java text, value by value.
- `edge/EngineDateTimeTextRowTest`: the row's SQL planned for every implemented database, read from `PureText`'s
  sets, no database named: where the row answers, the statement is the very one that prints the same value declared
  an abstract Date, the plain cast; a literal, a day, a value outside any store query and every database off the set
  keep the door's plan as written.
- `integration/EngineDateTimeTextTest`: the row run on each lane's database.

## The guards it moved

- `ArchitectureTest.PLANNER_ENTRIES`: `lowering.NamedRule#DATE_TIME_TEXT_BY_CAST`, as every row's rule is listed,
  dated.
- `core/edge_tests.bzl`: the two new tests listed.
- `DialectBoundaryTest`: no pin moved (the row's set aliases the two databases already named on one line).
- `WriterCarriageGuardTest`: no pin moved (the value printer's two checks of a date's time of day are one helper).

## The register row for (d), drafted (numbered when it lands)

| # | Kind | The difference | Lite's answer | Evidence |
|---|---|---|---|---|
| S(next) | outcome (a DateTime's `toString` inside a store query at legend-engine's door; RULED by the user 2026-11-26, "agreed on float/date text for engine") | legend-engine writes `toString` as the database's `cast(%s as varchar)` for every type (legend-engine 4.145.0, each database's extension): a timestamp's text is the database's own, `2014-01-02 10:20:30.25`, where legend-pure prints `2014-01-02T10:20:30.250+0000`. | At legend-engine's door, inside a store query, the `toString` of a DateTime that is no literal is the database's own cast, a row of legend-engine's table, on the databases whose text of it has been run beside the door's (H2 and DuckDB; Postgres and Snowflake, read by their lanes, when that is ruled); elsewhere, and outside a store query, the call keeps its own rule. A DateTime LITERAL's text is its written text and `+0000` at every door, as legend-pure prints it and legend-engine does not: PCT's legend-engine lane passes `testDateTimeToString`, `testDateTimeWithTimezoneToString` and `testSimpleDateToString` with it, which legend-engine itself fails (the user's choice between that and legend-engine's cast is open). | Run 2026-11-26 on DuckDB and H2 (`docs/records/edition-homework/step8/slice4-date-time-lift-and-row.md`). No recorded test holds a DateTime that is no literal (`moves-if-date-text-is-lites.md`). |

## What waits

- The SQL composition's deletion, and with it a computed DateTime's text used further becoming lite's own: the
  user's word.
- A literal at legend-engine's door, (A) or (B): the user's word.
- Found on the way, recorded against step 6.2 by the orchestrator: the date functions are declared to return the
  abstract `Date`, so `%2014-01-01T00:00:00.000+0000->adjust(1, DAYS)->toString()` is no DateTime's text and prints
  the database's plain cast at legend-pure's door (`2014-01-02 00:00:00` on DuckDB), where legend-pure prints
  `2014-01-02T00:00:00.000+0000`.
