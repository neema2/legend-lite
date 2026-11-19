# H9: phase 1's measures (the compatibility program's slice 0.4 (f), (g), (i), and the Operand's coverage)

B1 (`session_0195rQEnAa5CYXHygAxEFh3Y`), 2026-11-19, on the scratch branch `claude/compat-b1-homework` (never merged).
Asked by the program's owner (`session_0195rQEoSpv1FxC3WndEk5ZH`) under the user's GO of 2026-11-19 ("do as much
homework, research, measurements right now up front as possible and write down all the learning/outcomes/details
immediately"). The plan is r5, `docs/records/compat-program-plan-2026-11-19.md` on `claude/compat-program` (r6 at `af6d5a9a`). The
branch is cut from `claude/compat-s0` (PR #123's head, `ef9e9bfa3`), so the probes run against the hardened net.

Every number here is reproducible from a committed probe or a committed table. Counts first, then the details.

## The counts

| item | the count | what it decides |
|---|---|---|
| 0.4 (f) BARE, by position | **95 of 98 at the literal root**; 3 in a relation column (1 never executed); 0 in a mapped list, a Pair or a new instance | 1.3 is the 4-day case |
| 0.4 (f) COMPOSED and FEEDS over Float, Decimal or DateTime, content read | **0 of 22**; one COMPOSED Pair print (PCT `testPairCollectionToString`) that 1.3's rule refuses | 1.3's refusal list: that one test |
| 0.4 (g) G7 over the net | **6 tests**, each on DuckDB and SQLite, in channels A and B (24 outcome lines); the corpus, pure/v1, H2 relation and the edge cases **0**. Every value they print is known at planning in Pure (literals only) | 1.2 keeps all 6 exact if its `Operand` reads a literal under a cast and a literal list's size; otherwise they fail under A3 |
| 0.4 (i) Floats in database-written JSON | **0 databases round** (DuckDB, H2, SQLite; a graph fetch's serialize and its checked form; 23 hard doubles each). SQLite writes 17 significant digits: they round-trip, but 5 of 24 are not the shortest. `-0.0` is lost by H2's and SQLite's storage, not their JSON writers | no product fix for JSON; a formatter re-shortening SQLite's digits is exact. Postgres not measured: its `toVariant` path may round (below) |
| Operand's coverage | the literal kinds raises carry over the net are **String, Integer, Float, Decimal**, all in the `Operand`; **no** Date, Timestamp, Boolean, null, Time, Array or Struct literal occurs. Not carried as literals today: a literal **under a cast** (date text cast to a temporal, an Integer or Float cast to DOUBLE: 94 renders), which `wordable` calls computed | the `Operand` needs a rule for a cast over a literal, or the 5 G7 tests over one lose their value |

## 0.4 (f) The prints, by position and by use

**Source.** H2's census, `docs/records/compat-homework/H2-text-in-queries.md` and its data
(`H2-data/pct-comb.tsv`, `H2-data/corpus-comb.tsv`, branch `claude/compat-homework-h2` at `a0d5f1ea`). The probe here
is a filter over those rows (`H9-probes/bare_positions.py.txt`); no new parse.

**Reproducing H2's 98.** The rows of class `RETURNED-BARE` whose function is a spelling function (`toString`, `format`,
`formatDate`, `char`; no other spelling function occurs on the query side) are 103: PCT essential 89, standard 3,
unclassified 9, the corpus 2. H2's table excludes the 5 whose operand is a String (identity prints: essential's
`testStringToString` 2, `format` over Strings alone 3: its column "text-only toString/format"). That leaves 98:
essential 84, standard 3, unclassified 9, the corpus 2.

### The 98 BARE sites, by position

| position | sites | tests |
|---|---|---|
| **at the literal root** (the print is the query's answer: `$f->eval(|x->toString())`) | **95** | PCT essential 84, standard 2, unclassified 9 |
| inside a returned structure | **3** | see below |
| of which in a mapped list, a Pair's side or a new instance's property | **0** | |

The 3 nested ones are all a relation's COLUMN:

| test | site | type, origin | note |
|---|---|---|---|
| PCT standard `testDayOfWeek_Relation` | `extend(~dayOfWeek: x|$x.date->toOne()->dayOfWeek()->toString())` | enum `DayOfWeek`, DATA | an enum's name: the same text in every audience (R0) |
| corpus `testStrictDateProject` | `project(~[id:t|$t.id, date:t|$t.date->toString()])` | StrictDate, DATA | a projected column: 1.3's "a TDS column's declared type changes with the print" |
| corpus `testToSQLStringChar` | `toSQLString(|Person.all()->project(p|char($p.age->toOne()), 'char'), ...)` | Integer `char`, DATA | **never executed**: the query is only `toSQLString`'s argument; its assert is SQL text (CP-4's class) |

**The 95 at the root, by type** (every operand a literal, or an instance PCT builds from literals):

| type | sites | by function |
|---|---|---|
| Float | 34 | `toString` 14, `format` 20 (`%f`, `%.Nf`, `%s`) |
| DateTime | 24 | `toString` 13, `format` 10 (`%s`, `%t`, `%t{...}`, `%r`), `formatDate` 1 |
| StrictDate or Date | 8 | `toString` 6, `format` 1, `formatDate` 1 |
| Integer | 15 | `char` 9, `format` 4 (`%d`, `%05d`), `toString` 2 |
| Boolean | 4 | `toString` 2, `format` 2 |
| Pair | 3 | `toString` 2, `format` 1 |
| List | 3 | `toString` 2, `format` 1 |
| a class, an enumeration, an enum value | 4 | `toString` (one over a class whose `toString` the test defines: `ClassWithComplexToString`) |

**So 1.3 is the 4-day case:** 95 of 98 at the literal root, 3 in a relation column (one never executed), none in a
mapped list, a Pair or a new instance.

### The 8 COMPOSED and 14 FEEDS sites: which print a Float, a Decimal or a DateTime and have their content read

**None.** Every one of the 22 is over an Integer, an enum, a Pair or an empty list:

| class | test | type, origin | what reads it |
|---|---|---|---|
| FEEDS | PCT essential `testRemoveDuplicatesPrimitiveNonStandardFunction` (4 sites) | Integer and String, a mixed literal list | `removeDuplicates`' comparator `==` |
| FEEDS | PCT essential `testRemoveDuplicatesByPrimitive` (2) | Integer and String | `removeDuplicatesBy`'s key |
| FEEDS | PCT essential `testRemoveDuplicatesEmptyListExplicit` (3) | the empty list (no element is ever printed) | comparator and key |
| FEEDS | PCT relation `testExtendFilter` (1) | Integer, a column | `filter ==` after `+` with a String column |
| FEEDS | PCT relation `testMixColumnNamesRenameExtend` (2) | Integer, a column | `filter contains` after `+` |
| FEEDS | corpus `testFilterUsingToStringFunction` (1) | Integer, a column | `filter ==` |
| FEEDS | corpus `testQualifierProperty` (1) | enum `PartyType`, in a derived property | `filter startsWith` after `+` |
| COMPOSED | PCT essential `testMatchManyWithMany` (1) | Integer, `size()` | `'address:' + ...` returned |
| COMPOSED | PCT essential `testPairCollectionToString` (1) | **Pair**, computed | `'(' + $pairs->map(p|$p.toString())->joinStrings(',') + ')'` returned |
| COMPOSED | PCT grammar `testMultiPlusWithFunctionExpressions` (3) | Integer, literal and computed | `toString(1) + ' ' + ...` returned |
| COMPOSED | PCT relation `testSimpleRelationProject`, `testSimpleExtendStrShared`, `testSimpleExtendStr_MultipleExpressions` (1 each) | Integer, a column | `$c.str + $c.val->toString()` returned |

**One finding for 1.3's rule.** `testPairCollectionToString` COMPOSES a Pair's print into a returned text, and a Pair's
text differs by audience (R5: its JSON object in LITE, `<a, b>` in PURE and ENGINE). Under 1.3's rule ("refused by name
where its type's text differs") it is the one upstream composed print the rule refuses, unless the translator rewrites
a Pair's print as a structural text it can spell in every audience. Its expected value is `'(<a, b>,<a, 1>)'`. It is
not over a Float, a Decimal or a DateTime, so it is outside 0.4 (f)'s question as asked, but it is the case 1.3's
"expected: none beyond H2's 14 Integer and enum sites" misses. Of the other 21: 18 print an Integer or an enum, whose
text is the same in every audience (R0), and the 3 over the empty list print nothing (no element exists).

## 0.4 (g) G7 over the net: the computed-operand raises PCT and the corpus reach

**Probe.** `H9-probes/apply_probes.py.txt`, applied on this branch in `6f0a9c4ba` (never merged). It does two things:
- `PureMessages.sentence`, which writes legend-pure's sentence in SQL for a raise `PureMessages.wordable` calls
  computed (the interim), prefixes its text with the marker `<<G7>>`. A checked graph's defect text and a user's
  `fail` are not marked;
- every raise a dialect writes is noted by kind and operand shape (the census, next section).

Then `//gates:edge_net` re-recorded: PCT channels A and B on DuckDB (5 suites), H2 (relation) and SQLite (5 suites),
the corpus's host ledgers on DuckDB and H2, pure/v1 and the edge cases. A line moves exactly when an observed message
came from such a raise. The moved lines are `H9-data/g7_moved_lines.tsv`.

**Result: 6 tests, all PCT essential, on DuckDB and SQLite alike, in channel A (answers and outcomes) and channel B:**

| test | its body (pinned legend-pure) | the raise | operand shape in lite's SQL |
|---|---|---|---|
| `testAtError` | `[1, 2]->at(2)` | INDEX_OUT_OF_BOUNDS | offset `IntLit`, size a `Call` (the literal list's length) |
| `testDayOfMonthError` | `%2017->dayOfMonth()` | DATE_PART_ABSENT | the date as `Cast(StringLit -> TEMPORAL_TEXT)` |
| `testHourError` | `%2017->hour()` | DATE_PART_ABSENT | the same |
| `testMinuteError` | `%2017->minute()` | DATE_PART_ABSENT | the same |
| `testSecondError` | `%2017->second()` | DATE_PART_ABSENT | the same |
| `testSquareRootError` | `sqrt(-1.0)` | SQRT_OF_NEGATIVE | `Cast(FloatLit -> DOUBLE)` |

All 6 PASS today on both databases, in both channels, because the edge words them in SQL with the value. H2's
relation suite, the corpus (both lanes: no ledger row moved, so no test's verdict or message did), pure/v1's 15
requests and the edge cases reach **none**.

**What it means for 1.2.** A3 says no run-time operand value in a message. **Every value these 6 print is a Pure
literal, known at planning.** Lite's lowering makes them look computed: a literal under a cast, and a literal list's
size computed in SQL. So:
- if 1.2's `Operand` reads a cast over a literal as that literal, and a literal list's size as an Integer, all 6 keep
  legend-pure's exact sentence. The presenter words them, and nothing changes in the net;
- if not, each is "legend-pure's sentence without the value" under the register row 1.2 names, and all 24 outcome
  lines move from PASS to a message mismatch.

`InterimEdgeSitesTest`'s own set (rem by a column, rem by 0, sqrt of a cast literal, at by a column) has the truly
computed cases. **No PCT or corpus test in the net reaches a truly run-time operand.**

**Coverage limits.** The net has H2 PCT's relation suite only, so H2's essential suite, which holds these 6, is not in
it. Nor are the other databases' PCT lanes: Postgres, MariaDB, MySQL, ClickHouse and the cloud lanes.

## The Operand's coverage: every literal kind `RaiseSite.operands()` produces

**Probe.** `RaiseCensus` (`H9-probes/RaiseCensus.java.txt`) notes each `RaiseText` at the renderer's three
`raiseMessage` call sites, before the style's override words it. That gives the dialect, the kind, its form (a raise,
a placed text, or as-data), and each operand's shape: a literal's record, a cast over a literal, or `Computed:<record>`.
It was run with the net above: 5,017 renders (`H9-data/raise_census.tsv`). Edge styles account for 4,525 of them, the
product's for 492.

**Operand shapes, over every render:**

| shape | renders |
|---|---|
| `StringLit` | 4,135 (placed texts mostly: VALUE_NOT_HELD, LIST_LENGTHS_DIFFER, the cast and instance kinds) |
| `IntLit` | 465 |
| `FloatLit` | 36 |
| `DecimalLit` | 21 |
| a literal under a cast: `Cast(IntLit -> DOUBLE)` 36, `Cast(StringLit -> TEMPORAL_TEXT)` 32, `Cast(FloatLit -> DOUBLE)` 32 | 100 |
| computed: `Call` 217, `Column` 67, `Cast` over a computed value 32, `ScalarSubquery` 28 | 344 |

**Answer.** Plan 1.2's `Operand` (a typed literal per kind: Integer, Float, Decimal, String, date text, Boolean; or
`Computed`) **carries every bare literal the net's raises hold**. No Date, Timestamp, Boolean, null, Time, Array or
Struct literal occurs as a raise operand here.

What it does not carry as a literal:
1. **a literal under a cast** (100 renders, in DATE_PART_ABSENT and SQRT_OF_NEGATIVE). Today's `wordable` calls these
   computed, and that is the interim. The `Operand` needs a rule: a `Cast` over a literal is that literal with the Pure
   kind of the literal, not of the cast (`Cast(IntLit -> DOUBLE)` is the Integer the user wrote). Without it, 5 of G7's
   6 tests lose their value;
2. **a literal list's size** (INDEX_OUT_OF_BOUNDS's `Call`): known at planning only when the list is a literal. This is
   the planner's to fold, if 1.2 wants `testAtError` exact.

**Kinds by operand form** (the census's own grouping):
- all literal: CAST_IMPOSSIBLE, INSTANCE_NOT_OF, MATCH_FAILURE, LIST_LENGTHS_DIFFER, VALUE_NOT_HELD, CSV_TOO_WIDE,
  CONSTRAINT_UNEVALUABLE (as data), DATE_COMPONENT_INVALID, TEXT_NOT_DATE, TEXT_NOT_INTEGER, and the no-operand ones
  (RANGE_STEP_ZERO, SHIFT_OUT_OF_RANGE, INSERT_OUT_OF_BOUNDS, DIVIDE_BY_ZERO's bare form);
- sometimes computed: DIVIDE_BY_ZERO (8 of 193 by a column), INDEX_OUT_OF_BOUNDS (every size), SIZE_NOT_MULTIPLICITY
  (12 of 154: the size), SLICE_BOUNDS_INVERTED (23 of 61), SQRT_OF_NEGATIVE (32 over a computed value), TEXT_NOT_FLOAT
  (5 of 10, the product's).

**Limits.** The census is the net's renders, not the core test suite's. A literal kind the core tests alone reach would
not show here. The static construction sites, 56 across `lowering/` and `sql/dialect/`, read the same way, but were
not proven exhaustive by the census.

## 0.4 (i) Floats inside database-written JSON

**Probe.** `H9-probes/FloatJsonRoundTripProbeTest.java.txt` (in `6f0a9c4ba` and `5315fd997`). It covers each
in-process database: DuckDB, H2 and SQLite, each a session of lite's test backend with lite's dialect.
- **The values:** 24 hard doubles, stored as `DOUBLE PRECISION` from their shortest round-trip text: 0.1+0.2, 1/3,
  sqrt(2), pi, 1e300, the smallest subnormal and the largest double, MIN_NORMAL, whole values past 2^53 and 10^15,
  -0.0 (stored by `CAST('-0.0' AS DOUBLE PRECISION)`), small and large exponents.
- **The forms:** two, `graphFetch(...)->serialize(...)` and `graphFetchChecked(...)->serialize(...)`.
- **The check:** each JSON number's text read back with `Double.parseDouble`, the same bits or not.

Results are in `H9-data/float_json.tsv`.

**Result.**

| database | round-trips (of 24, per form) | not the shortest text | lost |
|---|---|---|---|
| DuckDB | 24 | 1 (`4.9E-324` written `5e-324`, which is in fact shorter: not a defect) | none |
| H2 | 23 | 0 | `-0.0`: H2 stores it as `0.0` (read back by plain JDBC as `0.0`), so the JSON writer is not at fault |
| SQLite | 23 | 5 (17 significant digits: `0.33333333333333332`, `1.4142135623730952`, `3.1415926535897931`, `4.9406564584124654e-324`, `-0.66666666666666663`) | `-0.0`, lost by storage as in H2 |

**Answer: no database's JSON writer rounds a Float**, in either form, on the three in-process databases.
- SQLite's 17 digits are lite's own choice (E-v3, 2026-11-16: SQLite's own JSON writes 15 and rounds). They round-trip,
  so a formatter printing the shortest is exact.
- The `-0.0` loss is H2's and SQLite's storage, a question of the stored value, not of JSON.
- **No product fix is needed for JSON**, in the plan's sense.

**Not measured, by reading only:**
- Postgres's graph fetch writes `json_build_object` over a `float8`, which Postgres 12 and later spell shortest.
- Its `toVariant` path (`Postgres.doubleJsonb`) writes a whole finite double as `to_jsonb(round(CAST(x AS NUMERIC),
  1))`. Postgres's `float8 -> numeric` cast keeps 15 significant digits, so a whole double past 10^15 with more than 15
  significant digits would round there, for example `12345678901234567.0`. That is a Variant, not a graph fetch: a
  candidate product defect for the variant track, to be measured on embedded Postgres before it is called one.
- MariaDB's, MySQL's, Trino's, ClickHouse's, Snowflake's, Databricks's and BigQuery's JSON writers were not measured.
