# The edition program: where it stands, what is left, and every open decision (2026-11-26, 22:15 UTC)

*Written by the orchestrator at the user's request ("Where are we with whole program? What's left across all steps in
detail and plain English. Is there a consolidated list of all open decisions with problem, pros/cons,
cost/implications, and alternatives"). Until now the open decisions were spread over several messages and the plan
record; this page is the one list. The plan is `docs/records/edition-plan-to-finish-stage-b-2026-11-25.md`.*

## 1. Where each step stands

| Step | What it is | State |
|---|---|---|
| 1 to 5 | the ruled leftovers; the two proofs; legend-engine's table of meanings; lite strict inside functions; legend-engine's meanings row by row | **closed**, on the program branch |
| 6 | one equality everywhere: a value compared by its type and its value, in queries and in test asserts alike, and the test judges' separate comparison code deleted | **two of seven pieces landed or landing**; the big deletions are ahead |
| 7 | reserved | unused |
| 8 | upstream's text written by Java at the edge instead of by SQL | two slices landed; two more built, with two blockers found by audit |
| 9 | test machinery moved out of the product | one slice landed; one more built and audited; the bulk waits for steps 6 and 8 |
| 10 | clean up and lock (delete the old switches, turn the guards on) | not started |
| 11 | land on `main` | the user's word |

## 2. What is left, step by step

### Step 6: the one equality

| Piece | What it does | State | What is left |
|---|---|---|---|
| first slice | `assertSize` counts with the product's own `size` | **landed** | nothing |
| 6.1 | within one type, a value is compared by what it is, not how it was written (a Decimal by its number, a date-time by its instant); a list written in the query needs no list in the SQL; the compiler itself compares only Strings, Booleans, enumeration values and whole numbers | **on the pull request**, verified locally and on eleven databases, CI running | CI green, then the program branch moves |
| 6.2 | values whose type is only known at run time | **about two thirds built and tested** (branch `claude/step-6-2`): numbers of different types in a mixed list answer false instead of being refused; an upcast value is compared by the type it holds; equal numbers of different types sort Integer, Float, Decimal; sorting a mixed list is cheaper than today; BigQuery can sort one | the host judge's matching tie-break (decision D4); `max`/`min` (D2); `<` and `>` (D3); a value carried as JSON compared by its JSON kind; a count of how often such comparisons occur in the tests; then the full run, the six lanes, an outside audit |
| 6.3 | pairs, lists and maps compared by the types inside them; class instances compared by their run-time class first; functions written for "any relation" held to the same strict rule | not started | all of it; needs D5 |
| 6.4 | the product's `==` finished and put in one place: two lists compared as rows on every database (H2 included), instances by their typed keys instead of a text of them, tables by column | not started | all of it. The largest slice of step 6 |
| 6.5 | a test assert becomes the plain comparison it means, planned like any query; the database judge's own comparison code is deleted (about 1,300 to 1,600 lines) | not started | all of it; needs 6.4 and the SQLite item first |
| 6.6 | the leftovers: the tolerance assert as its own formula, nanoseconds, the last special cases; the size pins shrink | not started | all of it |
| SQLite's date-time text | SQLite has no date type, so every date-time lite writes there takes one canonical text, and comparing texts compares instants | built, in its full run | its report, an outside audit, then it lands before 6.5; decisions D11 and D12 |

### Step 8: presentation at the edge

| Piece | State | What is left |
|---|---|---|
| slices 1 and 2 (a Float's text and the printed table's text written by Java from typed values) | landed | the repair of slice 2: four fixes in progress after the outside re-check (one changes answers: ordinary computed values lost legend-pure's text) |
| slice 3 (the SQL that composed legend-pure's Float text deleted) and slice 4 (the date-time text) | built; outside audit found **two blockers** | the fixes (a computed date-time held in a `let` is not printed legend-pure's way; `toRepresentation` of a Float); decisions D7 to D10; a probe on six database lanes |
| the `TDSNull` row | not started | small |

### Step 9: test machinery out of the product

| Piece | State | What is left |
|---|---|---|
| slice 1 (15 test-harness classes moved) | landed | nothing |
| slice 2 (the corpus's two dialect hooks moved, 819 lines) | built; outside audit found no blocker, eight fixes | the fixes in progress; decision D13 |
| the rest (about 45 classes; the SQL-text check as test tooling) | not started | after steps 6 and 8 land, because they edit the same files |

### Step 10: clean up and lock (not started)

In order: no line outside a dialect names a database (the edge's six lines move onto per-database dialect objects);
the compiler's constant folds cut back to the rule, the list measured first; the old switch machinery deleted (the
plan style, the feature flags in the lowering, the forms object, the edge's dialect subclasses); the guards turned
on so the build fails if audience knowledge returns to the product; legend-engine's meaning refused by name on
databases where it was never checked; the carried items (two parses as table rows, an enumeration by name, the
keyed-count row, a way for an SDLC project to take legend-engine's meaning, PCT on compiled legend-pure).

### Step 11: land

Full runs on every gate, an outside audit of the whole branch, one pull request to `main`, the user's word.

**Not before step 6B (added 2026-11-27, the user: "how do make sure that A does not survive after B? I'm super
nervous we're just going to be stuck with a bunch of garbage A code if we merge to main").** Step 6 landed on the
program's branch as "A": one code for a comparison of two whole sides, a second for two single values, and the test
checker's own assert planning still there. Nothing of step 6 goes to `main` until step 6B has landed on the
program's branch, and step 11 checks all three of:
1. the acceptance measure (`tools/equality-measure/run.sh`: every assert against its plain comparison) differs only
   where a database has no form for the value, each such body named;
2. the list of comparison codes B's design starts from (every place that decides how two values compare, with its
   lines) has reached the design's target, and the code B's design names for deletion is gone;
3. the test checker plans an assert as the plain comparison it means, with no comparison code of its own (slice 6.5
   of this page, as first planned).

### Rough size of what is left (an estimate, not a measurement)

Step 6 is the long pole: 6.2 about half a day more, 6.3 about a day, 6.4 two days, 6.5 a day, 6.6 half a day. Steps 8
and 9 finish their current slices in parallel; step 9's bulk is one to two days in its own session after 6.5. Step 10
about two days. Step 11 a day of runs plus CI. Roughly a week and a half of working sessions if the decisions below
come back quickly.

## 3. Every open decision

Each has: the problem, the options with what each costs, and the orchestrator's recommendation. Answer by number.

### Step 6's decisions, with what upstream answers (measured 2026-11-26, about 22:40 UTC)

*Every upstream answer here was RUN on upstream's own code that evening (interpreted legend-pure 5.99.0, compiled
legend-pure 5.99.0, legend-engine 4.145.0 store-less and inside an H2 store query) or READ in the pinned sources:
`docs/records/edition-homework/upstream-step6/answers.md`, with the probes and raw output beside it. The orchestrator's
first recommendations on D2 and D3 were made before this measurement and are changed below.*

**What upstream does, in one paragraph.** Pure keeps two different ideas apart. EQUALITY is by type and value in
interpreted legend-pure (`1 == 1.0` false, `3 == 3D` false); compiled legend-pure loosens it to "the same printed
digits" (`3 == 3D` true, `1 == 1.0` false); legend-engine's own generated Java depends on the shape of the code
(`1 == 1.0` true between two literals, false through a `Number`, false inside `contains`). ORDERING is by value in all
three, with no regard to type: `1 < 1.0` false, `1 <= 1.0` true, `1 >= 1.0` true, `compare(1, 1.0)` 0. Inside a
database query legend-engine leaves everything to the database, equality included: `intCol == floatCol` is
`I is not distinct from F`, true where the numbers are equal.

**D1. Numbers of different types inside a mixed list.**

| | interpreted Pure | compiled Pure | legend-engine store-less | lite today | lite, slice 6.2 as built |
|---|---|---|---|---|---|
| `[3, 'a']->contains(3D)` | false | **true** | false | refused (an error) | false |
| `3->in([3D, 'a'])` | false | **true** | false | refused | false |
| `[1.5, 'a']->contains(1.5D)` | false | false | false | refused | false |
| `[1, 'a']->contains(1.0)` | false | false | false | false | false |
| `[3, 'a'] == [3D, 'a']` | false | **true** | **true** | refused | false |
| `[3, 3D]->removeDuplicates()->size()` | 2 | 2 | 2 | 2 | 2 |

Inside a store query legend-engine writes `I in (3, 'a')` and H2 refuses it when it runs.
- *Option (a), answer false at every door:* agrees with interpreted legend-pure on every row and with legend-engine
  on all but the list-against-list row; disagrees with compiled legend-pure on three rows. No SQL (the refusal's
  code is deleted). One register row amended (S127). Compiled legend-pure contradicts itself here anyway: it says
  `3 == 3D` yet `removeDuplicates` keeps both.
- *Option (b), keep refusing:* no answer that differs from anyone, but a program every upstream runtime runs gets an
  error, and which lists are refused depends on their shape.
- *Option (c), compiled legend-pure's rule (compare the printed digits):* matches compiled legend-pure, disagrees
  with interpreted legend-pure and legend-engine, costs a text comparison in SQL, and breaks the ruled equality rule.
- *What it locks in:* slices 6.4 and 6.5 build `==` and the asserts on this rule. If PCT is later run against
  compiled legend-pure (step 10), any PCT test that relies on `3 == 3D` inside a mixed list would differ; none is
  known.
- *Recommendation:* (a).

**D2. `max` and `min` over a list when equal numbers have different types.**

| | all three upstream runtimes | lite today |
|---|---|---|
| `[3, 3.0]->max()` | Float 3.0 | **Integer 3** |
| `[3.0, 3]->max()` | Integer 3 | **Float 3.0** |
| `[3, 3.0]->min()` | Float 3.0 | **Integer 3** |
| `[3.0, 3]->min()` | Integer 3 | **Float 3.0** |
| `max(3, 3.0)`, two arguments | Integer 3 (the left one) | a compile error (two known types) |

Upstream's rule, read in `max.pure` and `min.pure`: the list form folds from the front and keeps the LATER element on
a tie; the two-argument form keeps the left. **Lite today keeps the FIRST, the opposite of all three,** under a test
whose comment says it is Pure's rule (`TaggedSelectionTest`). Found by this measurement. Inside a store query
legend-engine writes a `CASE` and the database answers the Float.
- *Option (a), match upstream: the last of the tied elements.* Fixes an existing difference; no recorded
  difference afterwards; the answer depends on the list's order, as upstream's does. A small SQL change (the last
  position of the winning value instead of the first), to be measured.
- *Option (b), the type order (the Float for `max`, the Integer for `min`, wherever they sit).* The answer does not
  depend on list order; agrees with upstream in two of the four rows and differs in two; needs a register row.
- *Option (c), leave it:* differs from every upstream runtime on all four rows.
- *Recommendation:* (a). (Changed: the first recommendation, (b), assumed upstream kept the first; it keeps the
  last, in all three runtimes.)

*Added after the rulings (about 23:30 UTC), read in the pinned sources by the doors' session and then RUN on all
three upstream runtimes (`upstream-step6/answers.md`, group E):* upstream has TWO `min`s. The one over a list of
numbers (and of dates) is a fold, which keeps the LAST written of a tie (`[3, 3.0]->min()` is the Float). The
generic one, which `least` calls and a list that is not all numbers takes, is `sort()->first()`: the FIRST written
(`[3, 3.0]->least()` and `[3, 3.0, 'a']->min()` are the Integer). Both `max`s and `greatest` keep the last written.
The rule for the other two modes follows each function's own body. A day and its midnight date-time are NOT a tie
upstream (the day is the smaller, so `max` is the date-time and `min` the day, in either order); lite refuses that
pair by name at every door today, which this slice does not change.

**D3. `<`, `<=`, `>`, `>=` between values whose types are known only at run time.**

| | all three upstream runtimes | in an H2 store query (legend-engine) |
|---|---|---|
| `1 < 1.0` | false | by value |
| `1 <= 1.0` | true | by value |
| `1 >= 1.0` | true | by value |
| `compare(1, 1.0)` | 0 | no SQL translation |
| `1->cast(@Number) < 1.0->cast(@Number)` | false | |

- *Option (a), by value then by type (`1 < 1.0` true, `1 >= 1.0` false):* exactly one of `<`, `==`, `>` holds, but
  it **differs from every upstream runtime and from the database** on equal numbers of different types, and costs
  an extra condition in SQL.
- *Option (b), by value alone:* what all three upstream runtimes and the database do; zero SQL. `1 <= 1.0` and
  `1 >= 1.0` are both true while `1 == 1.0` is false: that is Pure's own design (ordering is numeric, equality is
  typed), not an accident of lite's.
- *Recommendation:* (b). (Changed from (a).) Where both types are known when the query is compiled, lite's door
  keeps its compile error, as ruled.

**D3b. Sort, and the ruling of 2026-11-25 ("among equal magnitudes Integer < Float < Decimal"), to look at again.**

| | all three upstream runtimes | lite today | lite, slice 6.2 as built |
|---|---|---|---|
| `[3.0, 3, 1]->sort()` | [1, 3.0, 3] | [1, 3.0, 3] | **[1, 3, 3.0]** |
| `[3, 3.0, 1]->sort()` | [1, 3, 3.0] | [1, 3, 3.0] | [1, 3, 3.0] |
| `[3D, 3.0, 3]->sort()` | [3D, 3.0, 3] | not measured | [3, 3.0, 3D] |

Upstream's sort is stable and compares equal numbers as equal, so ties keep the list's order. The ruling was made
before this was measured; as built it makes lite differ from all three on any list where a Float is written before
an equal Integer.
- *Option (a), keep the list's order on a tie (upstream's):* no difference from anyone; deterministic; the cheapest
  SQL (the position is already the last sort key). The tie-break written today comes out; the cheaper sort (the
  redundant sort removed, MySQL 16,826 to about 7,900 characters) stays.
- *Option (b), keep the ruled type order:* the answer does not depend on the order the list was written in; one
  register row; the compatibility doors answer it too (nothing after the typer knows the audience, so they cannot
  undo it without a second sort function).
- *Recommendation:* (a), reversing the 2026-11-25 ruling on the numbers. The dates' half of that ruling (a day before
  a date-time at the same instant) is legend-pure's own order and stays.

**D4. The host judge's size limit.** Needed whichever way D2 and D3b go: to match two lists as multisets the host
judge sorts both and compares position by position, and with ties `[3, 3.0]` against `[3.0, 3]` it would say
"different". That tie-break is inside the judge, not a program-visible order. Its file is exactly at its limit of
319 lines. *Options:* raise the limit by about four lines; or reshape the file to make room (time and risk for
nothing); or leave the judges able to disagree. *Recommendation:* raise it.

**D5. A function written for `Number`, called with a specific type.**
`amongWholes(x: Number[1]) { $x->in([0, 1]) }`, `amongFloats(x: Number[1]) { $x->in([0.0, 1.0]) }`,
`same(x: Number[1], y: Number[1]) { $x == $y }`.

| | interpreted Pure | compiled Pure | legend-engine store-less | legend-engine in an H2 store (columns) | lite today |
|---|---|---|---|---|---|
| `amongWholes(1.0)` | false | false | false | by value: the rows where F is 0.0 or 1.0 | **true** |
| `amongFloats(1)` | false | false | false | by value | **true** |
| `1.0->in([0, 1])`, written directly | false | false | false | `F in (0, 1)`: by value | a compile error (two literals); over a Float column, by value (the literal rule) |
| `same(1, 1.0)` | false | false | **true** | `I is not distinct from F`: by value | not measured |

Upstream gives two answers to the same code: false when nothing is in a database, by value when the values are
columns. Lite has one meaning everywhere, so it has to pick.
- *Option (a), a literal written against a `Number` reads as the value's own number type (by value):* the same
  answer as legend-engine gives inside a database, which is where lite's queries run; the same answer whether the
  comparison is written directly over a column (your literal rule of 2026-11-26) or inside a function; free in SQL.
  Differs from all three store-less answers for a literal argument (`amongWholes(1.0)` true against false). One
  register row.
- *Option (b), strict (a Float is never one of two Integers):* matches all three store-less answers; but the same
  comparison then answers differently inside a function (false) and written directly over a Float column (true by
  the literal rule), and it differs from legend-engine inside a database; costs SQL to carry each value's type.
- *For two values that are not literals* (`same`): interpreted and compiled Pure say false; legend-engine says true
  both store-less and in a database. Lite's equality rule says false (two types). Slice 6.3 builds this; it is a
  difference from legend-engine at lite's own door, already covered by S127's ruling ("the principled fix for the
  product").
- *Recommendation:* (a) for literals; the equality rule (false) for two non-literal values.

**D6. Counting a table's cells in an assert** (`assertSize($t.rows.values, 4)`): lite counts the rows where Pure
counts the cells. Not measured upstream (legend-engine plans no assert). *Recommendation:* out of step 6 unless a
half-day narrow fix proves out; recorded either way.

### Step 8's (they block its two later slices)

**D7. `format('%s' | '%r' | '%f', ...)` of a computed Float at legend-pure's door.**
- *Problem:* it answered legend-pure's text (`0.3333333333333333`) and now answers lite's (`0.333333`), because the
  SQL that composed legend-pure's text is deleted and only `toString` is printed by Java. No PCT test covers it.
- *Options:* (a) Record it as a difference: no code, a visible difference for anyone formatting Floats at that
  door. (b) Java at the edge applies the format to the typed values: matches legend-pure; a small format reader in
  Java, and `%f` would round in Java. (c) Bring back the deleted SQL for this case: undoes the point of the step.
- *Recommendation:* (b) for `%s` and `%r` (pure spelling, no rounding) and (a) for `%f`, **after** the session
  reports what (b) costs. Not yet decided.

**D8. Five places where lite's SQL and legend-pure give different answers for text functions** (older than this
work; found because Java at the edge now does it legend-pure's way).
- `trim`: lite strips spaces; Pure strips tabs and newlines too. `length` and `indexOf` on characters such as
  emoji: Pure counts two units, databases count one character. `replace` with an empty search text. `indexOf`
  starting past the integer range (H2 raises). A Float held in an integer column prints `1000000.0`.
- *Options for each:* fix the product to match Pure (SQL on every call of that function), or record it.
- *Recommendation:* fix `trim` (ordinary inputs; a task card is already filed); record the other four (rare inputs,
  and matching costs SQL on every call). The exact answers arrive as one table from the session.

**D9. Rows of the register to amend for answers that moved in step 8's later slices.** A Float inside a printed
list or table that the program goes on using (`1e-05` became `0.00001`); `%r` of a Float in an integer column; at
legend-engine's door a computed date-time inside `format` or a list's text (`+0000` became `Z`); a year below 1000
(to be checked against legend-pure's source first). *Recommendation:* accept each as lite's own text in the middle
of a chain, which is what you ruled ("lite in the middle"); the rows just have to say so. The table arrives with D8.

**D10. The date-time text at legend-engine's door on six databases where it was never run** (Postgres, Trino,
Snowflake, ClickHouse, Databricks, BigQuery).
- *Options:* (a) **keep the six and measure them on the lanes** (lite writes the same cast legend-engine writes, so
  the answer should be the same); (b) narrow to DuckDB and H2 until measured.
- *Recommendation:* (a); the probe is being written.

### SQLite's date-time text

**D11. A user's own SQLite table whose date-times are stored in another text form.**
- *Problem:* everything lite writes into SQLite will be in the one canonical text. A user's existing column holds
  whatever their application wrote.
- *Options:* (a) **compare it as stored (today's behaviour), record the gap, and do the lean fix in the database
  follow-up** (normalize only tables the plan did not load itself). (b) Normalize every stored date-time column at
  every read now: correct for user tables, but two core size rows go up (1,891 to 2,687 and 903 to 1,503) on tables
  that need nothing. (c) Do the lean fix now: about a day, outside the SQLite dialect.
- *Recommendation:* (a).

**D12. Size rows that will go up on SQLite.** `parseDate` (its result has to be normalized at run time; the number is
being re-measured) and a date-time parameter (the bound value is trimmed in SQL: 62 to 206 characters before
re-measuring). *Options:* accept each with your word, or for the parameter have the plan say which text form to
bind so no SQL is needed (leaner, but a change to the plan's format). *Recommendation:* decide when the numbers
arrive.

### Step 9's

**D13. A hidden server switch goes away.** `-Dlegend.exec.engineScanOrder=true` on the shipped server used to add
the test corpus's scan-order pass to queries on DuckDB. *Options:* (a) **remove it** (it is test machinery; nothing
documents it); (b) keep a product switch for it. *Recommendation:* (a).

### Told to you, not yet answered

**D14. Two date-times at the same instant written at different precisions tie in a sort** and keep the list's
order (they are one value now). Before, the written precision ordered them. *Recommendation:* accept; it follows
from the instant rule you ruled.

### Product bugs found on the way (not Stage B; each has a task card)

**D15. MySQL and MariaDB compare text ignoring trailing spaces** (`'a' == 'a '` is true over a column; filters,
`in`, `distinct`, `startsWith` and `endsWith` are wrong the same way). Lite's own session setting causes it.
*Options:* (a) start the task card now in its own session (it touches only those two dialects, so it does not
collide with Stage B); (b) first item of the database follow-up after B; (c) pull it into B. *Recommendation:* (a).
The other cards waiting: BigQuery's untyped NULL in channel B; lite's `trim`; H2's 135 PCT gaps; the flaky DataCube
test on macOS.

### Coming later (nothing to decide today)

- Step 10: which constant folds stay in the compiler (two still compare Decimals and dates in Java: a map's `get`
  and `groupBy`); the before-and-after of moving the edge's six database-naming lines; legend-engine's own
  pre-evaluation of some functions in PCT's legend-engine run; SQLite's Decimal arithmetic under legend-pure's
  meaning; DataCube's legend-engine workarounds; the wording of the rules of record.
- Step 11: the word to open the pull request to `main`.
- The database follow-up after Stage B: whether to upgrade the databases as part of it (DuckDB 2.0, Postgres 19,
  ClickHouse 26.8, MariaDB 11.8, MySQL 9.7); whether H2's list operations belong to it; the design for ClickHouse's
  settings on the connection.
