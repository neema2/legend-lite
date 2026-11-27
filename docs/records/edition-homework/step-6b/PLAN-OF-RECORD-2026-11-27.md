# Step 6B: the plan of record (2026-11-27, 22:30 UTC)

*Written by the orchestrator at the user's word ("Write down the full new order and plan and design so we don't lose
it then we start"). It replaces the withdrawn design (`design-2026-11-27.md`) and every plan said in conversation.
A session with no memory of today starts here, then reads `measure-positions-2026-11-27.md` and
`reading-notes-2026-11-27.md` beside it.*

## 0. Where things stand

- **Step 6 "A" is landed.** The program branch `claude/compat-b3-nufdlk` and the draft pull request #141's branch are
  both at `c81297490`; CI run 46069955591 on that commit passed every lane (22:16 UTC). A's own account is
  `../landing-step-6-closing-2026-11-27.md`.
- **Step 8 runs in another account** on `claude/edition-step8`, by `../step8/HANDOFF-to-another-account-2026-11-27.md`
  (on that branch). Its section 6 says whose files are whose; B keeps to its side of it.
- **The measuring tool** (section 3) is on the scratch branch `claude/step-6-numbers` at `6bf041cb6`, not on the
  program branch: its model text entered the parser-equivalence corpus and failed `//parser-equivalence:parser_parity`.
  Slice 0 brings it back properly.
- Nothing of step 6 goes to `main` before B is done (`../open-decisions-2026-11-26.md`, step 11).

## 0a. What changed after the plan was first told to the user (the same evening)

The user asked "Is it the right expert design!", and four flaws were fixed before anything was built:
1. **End to end from slice 1.** The first telling left the test checker untouched until slice 6, which is how step 6
   ran out of road. Each slice now also moves the asserts for the kinds it covers.
2. **No special H2 work in slice 1.** Two writings only: lists on DuckDB, and the generic rows writing H2 uses.
3. **The measure checks right answers,** not only agreement, and is itself reviewed once.
4. **Bind once is scoped honestly:** inside one comparison and one assert for certain; a `let` read twice only if it
   fits slice 2's cap.

## 1. What B must achieve (the user's vision, confirmed 2026-11-27)

1. Wherever two values are compared (a query's answer, a function's body, a filter, a join, a deduplication, an
   ordering, a test's assert) the same rules decide, by type and value (AGENTS.md, "The equality rule").
2. The database computes the answer, from one set of decisions in the product.
3. The test checker has no comparison code of its own: an assert is the comparison it means.
4. legend-pure's and legend-engine's looser meanings are their own functions, chosen by the type-checker; the
   lowering does not ask who is asking.
5. Nothing is computed twice, and the SQL is lean.

Accepted by the user: the Java checker stays separate, as the independent second judge ("Agree with last 2"); a
database that cannot do a comparison refuses it by name.

**The user's words this plan must honour:**
- "isn't that just a dialect rendering of single representation of rules? Isn't that what the dialect is for?"
- "we did a bunch of homework that showed almost every database supporting lists or arrays now for single types or
  struct/map/variant for mixed" (`docs/records/database-recheck-2026-11-26/`: read it, do not redo it).
- "Why running things twice at all ever?" and "Why does computed once drop out of B?" (it does not).
- "Can we move that code into product so both can do it?" (what only the test checker could compare).
- "Why is moving == into type checker different plan? Is that not fundemental to this success".
- "I would do duckdb and h2 or postgres - honestly SQLite and MySQL are very low priority could even do both row and
  list on duckdb for example of want to check the idea of both".
- "I'm weary of spending a lot more time doing research given that the last two times we did so much research and
  still somehow built completely the wrong thing anyway": so the calibration of section 5.

## 2. The design

### 2.1 One "same values" instruction, written by the dialect

The product's SQL model gets ONE typed instruction: **"these two sides hold the same values"**. It carries:
- the two **sides**. A side is a VALUE (one value, a list, a map, an instance) or a QUERY (a table's rows). Which it
  is depends on what the side is, never on where the comparison stands;
- the **kind** of the elements, as section 2.2 decides it;
- **in order**, or **in any order** (`hasSameElements`).

The lowering builds it in one place for `==` wherever it stands. **Each dialect writes it its own way** (AGENTS.md
invariant 3): a database that holds typed lists compares the two lists; the generic writing reads each side's
elements as rows and compares the rows; a database that holds a list as JSON reads each element back as its type
first. A dialect that cannot write a case refuses it by name.

What exists today and what is wrong with it:
- The product already has the start: `SqlFn.SAME_IN_ORDER` / `SAME_IN_ANY_ORDER`, written per dialect by
  `sql/dialect/SameValuesForm`. But the lowering hands it BOTH a list and two row sub-queries, after the planner has
  turned each side into a statement of its own (`planner/RootComparison`, the row framing of
  `lowering/SideEqualities`).
- A side that reads the row it stands in cannot be a statement of its own, so a SECOND code compares it
  (`lowering/Equalities`, `lowering/TaggedLists`, the `contains` and `in` rules of `lowering/Scalars`), with rules of
  its own. Which code answers is decided by where the comparison stands. That is why the measure finds different
  answers in different places, and why a comparison inside a DuckDB list lambda fails at run.
- B: the lowering hands the instruction the two SIDES and nothing else. The planner-level comparison statement and
  the second code go. Whether that is the existing instruction with its arguments changed or a new typed record is
  decided in slice 1's half page, from the code, not here.

### 2.2 One place that decides the rules

The decisions already exist once, in `SideEqualities.pair` and the helpers it calls (read 2026-11-27): which types
can be equal; an Integer and a Decimal by value; a Decimal by its number; a date-time by its instant; a year and a
year-month; a value typed only at run time by the type it holds (a tag beside the value); a JSON-carried value by
its JSON kind; an instance by its class and then its keys or its identity; a map by its entries in any order; what
an empty side means. That function reads each side's plan only for the SQL type its value is carried in, so the
same decision can be asked about two expressions. (The empty side of two SINGLE values is decided elsewhere today,
`Equalities.emptySidesAsPure` and `Lowerer.asCondition`; it comes under the same decision.) **B makes it the one decision** every comparison asks, and deletes
the hand-written copies (the single-value ladder, the mixed-list equality, the membership ladder).

The lowering reads types and carriers; it infers none (invariant 2), names no SQL function and holds no SQL text
(invariants 2 and 3a). A new instruction is a new typed record with an arm in every dialect; the generic writing is
the base dialect's, so a dialect that adds nothing inherits it.

### 2.3 Bind once

ONE typed instruction: **"bind this value once and read it by name"**, written by each dialect (a one-row derived
table, a lateral, a common table expression). Used where a value would otherwise be written, and computed, twice:
- a dialect's own writing of "same values" that reads a side more than once (its length, then its elements; any
  order);
- an assert, whose verdict and failure record both read the two sides;
- a `let` that both sides of a comparison read.

Certain in B: the first two. The third changes how a `let` is written (today its expression is written again at
every use, measured 2026-11-27): tried inside slice 2's two-day cap, else it is the first slice of the lean-SQL
follow-up. Binding every `let` and every list operation once is that follow-up's, with this building block.

### 2.4 An assert is the comparison it means

`assertEquals(a, b)` is `a == b`; `assertSameElements` is `hasSameElements`; `assertContains(c, v)` is
`c->contains(v)`; `assertSize(c, n)` is `c->size() == n`; `assertEmpty(x)` is `x->isEmpty()`; `assert(c)` is `c`.
The test planner plans that comparison with the product's own instruction, and a failure record that reads the same
bound sides (2.3). This is done **kind by kind and family by family from slice 1 on**, so the test checker's own
code shrinks at every slice and is deleted when nothing calls it (about 1,300 to 1,600 lines:
`testplan/DatabaseJudge`'s gatekeeping and side planning, the comparison members of `lowering/VerdictSql`). The Java
checker is not touched except to keep agreeing: both checkers run in the measure. `assertTdsEquivalent` and
`assertJsonStringsEqual` have meanings of their own (a tolerance per cell; a JSON comparison) and stay as they are.

### 2.5 The doors

A door's comparison (legend-pure's, legend-engine's) becomes its own function, chosen by the type-checker in the one
package the edition design names (`compiler.spec.edition`; it does not exist yet). The lowering's two lookups of the
door for a comparison (`Scalars.doorsRule` for `==`, `EditionTable.emptySideIsFalse`) and the "three number types"
argument threaded through the mixed-list code go. Upstream's answers are RUN on upstream, never assumed.

### 2.6 Databases

- **DuckDB first:** it writes the instruction over typed lists.
- **The generic rows writing second,** which H2 uses. No special H2 array work in B: where the generic writing needs
  a row's own list read as rows and H2 refuses it (a correlated unnest), the comparison is refused by name on H2.
- **Both ways on DuckDB:** a test writes the same comparison as lists and as rows there and requires one answer.
- **Postgres** gets its list writing in slice 3, through its lane.
- **SQLite, MySQL, MariaDB:** low priority. They keep working through the generic writing, unoptimised; slice 8
  fixes them cheaply or refuses by name.

### 2.7 Not in B

Binding every `let` and list operation once (the lean-SQL follow-up); H2's missing list lambdas (the database
research's D7); rebuilding ordering, grouping and join keys (they are MEASURED in B; a contradiction found goes to
the user).

## 3. How we know it is done

1. **The measure reads zero** on DuckDB and H2 (`tools/equality-measure`, "the position matrix": one comparison asked
   in every place it can stand): no comparison true in one place and false in another, none answered in only some
   places, and every answer the EXPECTED one. At A it read, of 72 comparisons, 9 and 5 contradictions and 28 and 6
   partly answered (DuckDB, H2). Its count is pinned in a test that may only go down.
2. **Assert against plain comparison** (`tools/equality-measure/run.sh`): today 80 of 1,733 bodies differ on at least
   one database; at the end only bodies a database refuses by name, each listed.
3. **Deleted:** the test checker's own comparison planning; the planner's comparison statement; the second
   comparison code.
4. **SQL size pins** equal or lower at every slice. A pin that would rise stops the slice for the user's word.
5. Step 11's three checks (`../open-decisions-2026-11-26.md`).

## 4. The slices, in order

Each lands on the program branch by itself, with the measure lower.

**Slice 0: the measure, back and wider (about two hours).**
- The tool back on the program line. Its model text moves out of Java string literals (a resource file), so it stays
  out of the parser-equivalence corpus; `//parser-equivalence:all` is run before anything is pushed.
- An EXPECTED answer for every comparison, by the equality rule; a comparison whose expected answer is not plain
  from the rule is asked of the user, not guessed.
- Sides that read store columns: empty values, optional and to-many properties, a column of each type, two columns
  of two types.
- One independent review of the measure itself (is it a fair test of section 1?).
- The counts recorded as the baseline, and pinned.
- The loop's own tooling (section 5, item 5): the local round made from CI's lane list, and the one script that
  re-runs every recorded file.
- Slice 0 is tooling only. It stays on the scratch branch and reaches the pull request with slice 1's landing: one
  CI run for both.
- Two checks owed: which test suites run through lite's own door with the database checker, and which through a
  compatibility door with the Java checker (how well the suites protect B); CI's result recorded in A's summary.

**Slice 1: the instruction, for exact numbers and strings, end to end (one to two days).**
- `==` of single values and lists of Integers, Decimals and Strings is the one instruction wherever it stands.
- Written for DuckDB as lists; the generic rows writing for the rest; the both-ways test on DuckDB.
- The test checker's `assertEquals` and `assert` for these kinds use the same instruction over its sides (the first
  cut of 2.4).
- Should fix (measured): Decimal lists at two scales false on H2 where the assert passes; the DuckDB lambda error.
- Should show: DuckDB's SQL for a list comparison from about 600 characters to well under 100.
- Checked too: a join and a filter on DuckDB and on Postgres with the null-safe comparison A writes, against the
  plain one: does the database keep its fast path? The result goes to the user.

**Slice 2: bind once (one to two days).** Section 2.3. Checked by printing the SQL (the value written once) and by
size. Needs the user's ruling on the time-series refusal A lost (A's summary, item 5) first: it is the same question
of when a failing side raises.

**Slice 3: the other kinds (two to three days; may split).** Dates at each written precision; values typed only at
run time; mixed lists; maps; pairs; class instances; nested lists; and their asserts. Postgres's list writing.
Should fix: `1->cast(@Any) == '1'->cast(@Any)` true through `let`s; a year through a filter; maps read from a row
(refused today).

**Slice 4: tables (one day).** Two queries as sides: columns by name, rows in order; any order for
`hasSameElements`. The planner's comparison statement is deleted here if nothing else calls it.

**Slice 5: the other comparing functions (two days).** `contains`, `in`, `indexOf`, the list deduplications and
`hasSameElements` ask the same decisions; `assertContains` and `assertSameElements` with them. Ordering, grouping
and joins are measured; contradictions go to the user.

**Slice 6: the rest of the asserts (two days).** Size, empty, the plain condition, tolerance, `eq`, `is`, instance-of;
the failure record over bound sides; the test checker's gatekeeping and side planning deleted.

**Slice 7: the doors (one to two days).** Section 2.5, and the hour and minute date at the doors (needs the user's
ruling first: A's summary, item 4).

**Slice 8: lock (one day).** Guards (one class builds a comparison; the lowering cannot read the door for one; no
second path), pins at zero, the lanes on every database, the low-priority databases, the register rows, the landing
summary, step 11's checks.

About two to three weeks of sessions. Re-estimated after slices 1 and 3.

## 5. How each slice runs

1. **Read** at most two hours, of the code the slice touches only.
2. **Half a page** before building: what changes, and which measured cases it fixes. No other design document.
3. **Build** in the fast loop: the measure (40 seconds) and the touched tests after each change.
4. **Independent review of the diff** (a fresh subagent, given the diff and the rules, never the conclusion).
5. **Before any push to the pull request:** the local round is CI's own lane list (`gates/BUILD.bazel`), parser
   equivalence included, and one script re-runs every recorded file (`update_*`). CI must find nothing a local run
   would have.
6. **One push to the pull request per slice landing.** Never to clear a flake: the user is asked to re-run the lane
   (this session cannot re-run or cancel a run: GitHub answers 403).
7. **Lanes once,** on the final commit: Postgres and the others a slice can touch.
8. Then the program branch moves and the pin moves down.

**Stop rules.** The count does not move as the half page predicted; a slice passes two days; a size pin would rise;
a recorded answer moves that the half page did not name. Each means: stop and tell the user.

## 6. Evidence and where it is

| What | Where |
|---|---|
| The measure's results at the program branch before step 6 and at A, and B's work list | `measure-positions-2026-11-27.md`, `positions/` |
| My reading of the comparison code, file by file, and three SQL probes | `reading-notes-2026-11-27.md` |
| Still unread by me (read within each slice's two hours) | the test checker's SQL builder and other asserts, how it plans a side, the Java checker, ordering and deduplication rules, instance equality, the type-checker's rules, the doors' tables, each dialect's comparison arm |
| What each database holds for lists, and every "cannot" checked | `docs/records/database-recheck-2026-11-26/` |
| The withdrawn first design and its review (kept as evidence) | `design-2026-11-27.md`, `review-1-of-the-first-design.md` |
| A's landing, its five items for the user, the DuckDB lambda error | `../landing-step-6-closing-2026-11-27.md` |

## 7. Open, for the user

1. The five items of A's landing summary. Two are needed by a slice: the time-series refusal (before slice 2) and
   the hour and minute date at the doors (before slice 7).
2. Whether binding every `let` once is in B or the follow-up (recommended: the follow-up, beyond a `let` a
   comparison's two sides read).
