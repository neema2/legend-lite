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

### Rough size of what is left (an estimate, not a measurement)

Step 6 is the long pole: 6.2 about half a day more, 6.3 about a day, 6.4 two days, 6.5 a day, 6.6 half a day. Steps 8
and 9 finish their current slices in parallel; step 9's bulk is one to two days in its own session after 6.5. Step 10
about two days. Step 11 a day of runs plus CI. Roughly a week and a half of working sessions if the decisions below
come back quickly.

## 3. Every open decision

Each has: the problem, the options with what each costs, and the orchestrator's recommendation. Answer by number.

### Needed now (they block slice 6.2 or 6.3)

**D1. Numbers of different types inside a mixed list** (`[3, 'a']->contains(3D)`, `[1.5, 'a']->contains(1.5D)`).
- *Problem:* lite's rule is that values of different types are never equal. Compiled legend-pure answers the first
  one true (it compares the printed digits); interpreted legend-pure answers false. Today lite refuses these with an
  error, and whether it refuses depends on the list's shape. Inside a mixed list the types are known only at run
  time, so legend-pure's door cannot rewrite the comparison.
- *Options:* (a) **answer false at every door.** Consistent, no SQL (the refusal's code goes), one recorded
  difference from compiled legend-pure. (b) Keep refusing. No wrong-looking answer, but a valid program errors, and
  inconsistently. (c) Compare the printed digits in SQL as compiled legend-pure does. Matches it; costs SQL on every
  such comparison and breaks lite's own rule.
- *Recommendation:* (a). Built, waiting for the word.

**D2. `max` and `min` when equal numbers have different types** (`[3, 3.0]->max()`).
- *Problem:* legend-pure keeps whichever came first in the list. Lite's sort now orders equal numbers Integer, Float,
  Decimal (your ruling of 2026-11-25).
- *Options:* (a) **follow the type order:** `max` is the Float and `min` the Integer wherever they sit. `sort`,
  `max` and `min` agree and the answer does not depend on list order; one recorded difference from legend-pure; the
  SQL cost is not measured yet and comes back to you if it is more than a few characters. (b) Keep legend-pure's
  first-in-list. Free, matches legend-pure; but `[3, 3.0]->max()` is `3` while `[3, 3.0]->sort()->last()` is `3.0`.
- *Recommendation:* (a), because the equality rule's own text says ordering, `max`, `min` and `sort` hold to one
  order.

**D3. `<` and `>` between two values whose types are known only at run time** (an Integer 1 and a Float 1.0 reached
through `Number` or `Any`).
- *Problem:* `1 == 1.0` is false (two types). If `<` compares by value alone, then none of `<`, `==`, `>` holds.
- *Options:* (a) **by value, then by type** (`1 < 1.0` is true): exactly one of the three always holds, and it
  agrees with sort. Costs one extra condition in the SQL of such comparisons only. (b) By value alone: free, but
  incoherent as above. (c) Refuse them with an error: safe, but programs that run in Pure fail.
- *Recommendation:* (a). Where both types are known when the query is compiled this stays a compile error, as today.

**D4. The host judge's size limit.**
- *Problem:* the host judge (the second, independent checker of test asserts) needs the same tie-break, or the two
  judges can disagree on `[3, 3.0]` against `[3.0, 3]`. Its comparison file sits exactly at its line limit (319).
- *Options:* (a) **raise the limit by about four lines.** (b) Make room by reshaping other code in the file: keeps
  the number, costs time and risk for no gain. (c) Leave the host judge without it: a known disagreement.
- *Recommendation:* (a).

**D5. A comparison inside a general-purpose function** (slice 6.3). `f(x: Number[1]) { $x->in([0, 1]) }` called as
`f(1.0)`.
- *Problem:* today the database answers by value: true. The strict run-time reading says a Float is never one of two
  Integers: false. The literal rule says a number literal reads as the type of the value it is written against:
  true.
- *Options:* (a) **a literal written against a `Number` reads as whatever number type the value has at run time**
  (true): what a user expects, what the database does for free, what landed in the last batch. (b) Strict (false):
  the purest reading of "different types never equal", but `1.0->in([0, 1])` written directly is true by the
  literal rule, so the same code would answer differently inside a function; and it costs SQL to carry the types.
- *Recommendation:* (a). Two values that are not literals and hold different number types still compare as
  different (D1's rule).

**D6. Counting a table's cells in an assert** (`assertSize($t.rows.values, 4)`).
- *Problem:* lite counts the rows (2) where Pure counts the cells (4). Older than this program. Inside the compiler
  "a table's cells" and "a table's rows" are the same thing by a decision of 2026-08-24, so nothing later can tell
  them apart.
- *Options:* (a) **leave it recorded, outside Stage B.** One rare assert shape stays wrong. (b) A narrow rewrite of
  that one assert when it is typed: perhaps half a day, not yet probed. (c) Give cells their own representation:
  large, and reopens the August decision.
- *Recommendation:* (b) if the probe shows it is small, else (a). Never (c) inside Stage B.

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
