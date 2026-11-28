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

## 6a. How well the existing suites protect B (checked 2026-11-27, slice 0; read in the build files, not run)

| Suite | Door | Checker | Protects |
|---|---|---|---|
| The two relational corpus lanes (`spec/corpus.bzl`: `-Dlegend.judge.mode=host`) | legend-engine's | the Java checker | the doors' rules (slice 7) and the Java checker; not lite's own comparison |
| PCT channel B per push (`ChannelB`: `JudgeMode.HOST`; BOTH only in the manual "twice" runs) | legend-pure's | the Java checker | the same |
| `//core:core_tests_judges` (per push, in CI's core lane): core's body-running packages under DATABASE alone and BOTH | lite's own | the database checker, and both | **the test checker's database side: slices 1 to 6** |
| Core's own tests | lite's own | as each test asks | the product's comparison where a test happens to compare |
| The position matrix (slice 0) | lite's own; both doors once widened | both | every place a comparison can stand: **the main protection, because nothing else asks one comparison in every place** |

So a slice that passes every lane can still be wrong in a place no suite asks: the measure is not optional.

## 7. Open, for the user

1. The five items of A's landing summary. Two are needed by a slice: the time-series refusal (before slice 2) and
   the hour and minute date at the doors (before slice 7).
2. Whether binding every `let` once is in B or the follow-up (recommended: the follow-up, beyond a `let` a
   comparison's two sides read).

## 8. Status log (newest last; a session resuming reads this first)

**2026-11-28, 00:10 UTC.** Scratch branch `claude/step-6-numbers`.

- **Slice 0: done** except the CI test that pins the counts (below). The measure: `tools/equality-measure`
  (`positions.py`, two model files, `positions-baseline.tsv`); reviewed (`review-2-of-the-measure.md`).
- **Slice 1: built, reviewed (`review-3-of-slice-1.md`), corrected, measured** (`slice-1-half-page.md`). Its last
  product commit is `cbb7ac779`; `c7e033094` adds its record and baseline. NOT yet pushed to the pull request.
  To land it: (1) CI's core and checks lanes here on `cbb7ac779` (running since 23:47 UTC: `scratchpad/b106.out`;
  the first run, before the review's fixes, failed only the 13 Postgres-backed tests a root container cannot start:
  rerun those through `tools/postgres/as_unprivileged.sh`); (2) the other CI lanes on the same commit
  (`tools/ci/pre-push-round.sh corpus_duckdb corpus_h2 pct_duckdb pct_h2 pct_channel_b parser_equivalence stress`,
  in a worker session); (3) `tools/ci/rerecord.sh` and every changed line read; (4) one push of the landing commit to
  `claude/modest-galileo-h6swr4`; (5) CI green, then the program branch moves.
- **Ahead of slice 3, on top of slice 1 (commits after `c7e033094`; measured by the no-Bazel loop only, not built by
  Bazel, not reviewed):** lists of Booleans, Floats, days and date-times and a list against `[]` join the instruction
  (`SideEqualities.comparedAsValues`); two values typed at run time held in two SQL types are never equal
  (`Equalities.heldApart`); a list of dates carried as texts against a written year or a held date-time
  (`Equalities.equal`). The measure after them (106 comparisons): DuckDB 86 right, 2 partly, 15 fail, 2 wrong, 1
  contradicts; H2 84 right, 10 refused, 3 partly, 7 fail, 1 wrong, 1 contradicts. These commits must NOT ride with
  slice 1's landing: they land with slice 3, after its own review and lanes.
- **The no-Bazel loop** (`scratchpad/fast.sh`; if the scratchpad is gone: compile the files that differ from the
  build tree's commit with the toolchain's `javac` against `bazel-bin/core/*.jar`, then run
  `bazel-bin/core/equality_measure --main_advice_classpath=<classes> --positions <dir>` with
  `BUILD_WORKSPACE_DIRECTORY` set to the edit tree, then `positions.py`): about 90 seconds a turn, usable while Bazel
  runs. `--sql '<body>'` prints one query's SQL per database the same way.
- **Next in the fast loop (DuckDB's remaining 20):** the lambda places for kinds the planner still plans as a
  statement -- lists typed `Number` or `Any`, mixed lists, lists of pairs (3), maps (3), tables (4). The lowering asks
  the planner first (`Lowerer.scalarInner`, the `wholeSides` arm) and the value path second; the instruction over
  values should be tried first wherever both sides have a value (by what the SQL carries), the planner's statement
  only for a side that has none (a table). Then: `empty and value` and `nested lists` where a side reads the
  element; two never-equal columns inside a function of `Any` (refused as "not supported yet"); a computed column
  value kept as a value answering NULL; `t::sameT` given an enumeration and a String.
- **Owed:** the CI test that pins the counts (port `positions.py`'s classification to Java so the tool and the test
  share it; the model files and baseline as test resources); the measure on a lane's database (Postgres first); the
  both-ways test on DuckDB; the two AGENTS.md sentences for the rulings the user confirmed ("I think both rulings are
  correct as is": a pair, a list and a map are values compared by what they hold; for numbers typed at run time the
  same type means the same family), which need the user's yes to the wording.

**2026-11-28, 00:36 UTC.**

- **Slice 1's landing commit is the branch `claude/step-6b-slice-1` (`c77deac20`)**: `c7e033094` plus the fix CI's
  checks lane found when run here on the review's fixes (MongoDB's arm for the instruction must be one expression and
  its function table within 250 lines: the two-value form went to a helper, `sameInOrderElseRefused`) and the
  dialect capability pages regenerated for it. The guards, the edge guards and the docs tests pass on it.
- Running on that commit since 00:35 UTC (`scratchpad/b108.out`): the CI lanes not yet run here (both corpora, PCT
  on DuckDB and H2, channel B, parser equivalence, stress), then the 13 Postgres-backed core tests unprivileged.
  When green: `tools/ci/rerecord.sh` read, one push of `c77deac20` (or its successor) to the pull request's branch,
  CI, then the program branch moves. The scratch branch `claude/step-6-numbers` holds the same commits merged with
  the work ahead of slice 3, which does not ride with this push.
- **Slice 3's remaining kinds need their half page before more code:** a mixed list, a list typed `Number` or `Any`,
  a list of pairs, a map and a table are planned as statements by the planner first (`Lowerer.scalarInner`), so
  inside a DuckDB list lambda they fail. The value path answers the first three on DuckDB, but on a database without
  list lambdas (H2) only the planner's rows do, and the instruction as it is carries two plain values, not a mixed
  list's comparison. The half page has to say what the instruction carries for a mixed list (its tag and slots) so
  that each dialect can write it from values or from rows.

**2026-11-28, 00:45 UTC. Mixed lists move to a part 2 of B** (the user: "Why don't we move mixed list to the end of
the whole program or even a part 2 of B so we can fix everything else without blocking on that? Once everything else
works well we can revisit"; the orchestrator agreed).

- **What moves:** every value the SQL carries as a tag and one slot per kind: a list whose elements are of several
  types (`[1, 'a']`), and a single value picked among several types (`[1, 2.0]->max()`). What the "same values"
  instruction carries for them, and how each dialect writes it from values or from rows, is part 2's design.
- **What does not move:** everything else in sections 2 to 4, in the same order: lists typed `Number` or `Any` whose
  values the SQL holds in one plain type, pairs, maps, tables, binding once, the other comparing functions, the
  asserts, the doors, the guards.
- **Three conditions, so that part 2 is not forgotten and part 1 is not hollow:**
  1. The mixed rows of the measure are named (`mixed list ...`, `max of a mixed list`) and counted apart. Part 1 is
     done when every OTHER row is right everywhere on DuckDB and H2; the mixed rows are pinned and may not get worse.
  2. The code that compares mixed values today (the planner's statement for a tagged side, `SideEqualities.tagged`,
     and `lowering/TaggedLists`' equality) stays, but only they may reach it: a guard holds that nothing else does,
     so part 1 still deletes the second code for every other kind.
  3. Whether `main` waits for part 2 is the user's call at step 11. The orchestrator's recommendation: it need not,
     provided the measure shows mixed values give the right answer or a refusal by name in every place, never a
     wrong answer.
- At this date the mixed rows on DuckDB: four right everywhere, `mixed list computed` failing inside a list lambda
  (a database error, not a wrong answer), `max of a mixed list` refused where a side reads the lambda's element.

**2026-11-28, 01:15 UTC.**

- **Slice 1 is pushed to the pull request** (`c77deac20`, 01:06 UTC; the user: "We're really running out of time",
  "Did we do all the stuff in parallel"): CI is running on it. Before the push, run here on that commit: CI's core and
  checks lanes (green but for the 13 Postgres-backed tests a root container cannot start, re-running unprivileged in
  `scratchpad/b108.out`), and both corpora, PCT on DuckDB and H2, channel B, parser equivalence and stress (58 of
  58). The heavy session (`session_0195rQEn4deZCv8NwxsaY3Me`) was asked for lite's own PCT lane
  (`//pct:pct_duckdb_lite`) and channel B on H2, the commit against `c81297490`. When CI is green the program branch
  moves to `c77deac20`.
- **The time-series refusal** (A's summary, item 5; the user asked "What's the right semantics for that query"): the
  right answer is the refusal -- D8 is the user's ruling, and the last date's figure is built from returns that read
  the unordered rows. Fix on the branch `claude/ts-refusal-fix` (`048a29f7a`, stacked on slice 1, not built when
  this was written): the one-point form of `vol_swap_volatility` checks D8 on the row it keeps. Its build, the
  regenerated library text and parity ledger (back to REFUSED for `vol_swap_volatility default @BTCUSD`) and tests
  are queued (`scratchpad/batch109.sh`, output `b109.out`, the regenerated files as `b109-all.patch`). If the
  DataCube bundle budget (148,500) is passed by the added text, that is the user's word or a trim in the same
  commit. Other words with a one-point default form: check each for the same exposure.
- **Slice 2, first task, designed and not written:** the pin test. Port `positions.py`'s classification to Java
  inside the measure (it writes `positions.md` and `positions-counts.tsv` itself; `positions.py` goes), add
  `tools/equality-measure/BUILD.bazel` exporting the two model files and the baseline, and a JUnit test in core's
  lane that runs the matrix in-process and requires the counts to EQUAL the baseline (worse fails; better fails
  too, asking for the pin to be lowered in the same commit). Then bind once (section 2.3).

**2026-11-28, 01:35 UTC** (written as 01:45; the clock read 01:35).

- **Slice 1's heavy lanes are back clean** (the heavy session, 01:15 UTC): `//pct:pct_duckdb_lite` 5 of 5 suites and
  1,249 cases identical on `c77deac20` and `c81297490`; `//pct:pct_channel_b_h2` 440 pass and the 29 ledgered
  refusals on both, row for row. CI on `c77deac20` was still running at 01:20. **The database lanes were not asked to
  run slice 1's commit** (a miss against "every database lane, every major change"): they run once, on slice 2's
  final commit, which holds slice 1's product change and nothing else of the product's Java.
- **The time-series fix, corrected** (`claude/ts-refusal-fix`, `eec9c872e`, merged into slice 2's branch). The first
  cut (`048a29f7a`) was wrong twice: it passed the window's maximum (`[0..1]`) to `lines::once` (`[1]`), a compile
  error that refused all four one-point cases, and its text put the DataCube bundle 76 bytes over the 148,500 pin.
  Corrected: `->toOne()` on the maximum, and the comment above it shortened so the pin holds with no move (the
  budget test passes). Regenerated (library text, both adversarial ledgers, the two reports): the one moved case is
  `vol_swap_volatility default @BTCUSD`, PASS back to REFUSED with D8's own message; the other three one-point
  cases stay PASS; `//core:ts_gsquant_parity_test`, the oracle's four generated-file tests and the bundle budget
  pass (6 of 6). No other word of the library has a one-point form (`line_end` is read in this function alone), and
  the parity ledger, which runs every word over the line with duplicated dates, moved only this case when A landed.
- **Slice 2 (branch `claude/step-6b-slice-2`, stacked on slice 1; `e2d76a653` when this was written).**
  - *The pin.* `positions.py` is now `PositionReport.java` (the same report: the Java one over slice 1's run gave
    the same `positions.md` and counts, 0 lines different). The run writes a third file, `positions-kinds.tsv`: one
    line per comparison that is not right everywhere, per database, plus the places left out and how many
    comparisons were asked. `//core:equality_positions_pin_test` (CI's core lane) requires the run to EQUAL
    `tools/equality-measure/positions-baseline.tsv` line for line -- stronger than the counts first planned, which
    one comparison getting better could hide another getting worse behind. Built and run: passes in 49 s; fails by
    name with a baseline line removed. The fresh run on this branch gives slice 1's counts exactly.
  - *Two probes:* `--sql '<body>'` (the product's statement) and `--assert-sql '<body>'` (what a test body sends
    under the database judge), per database; `-Dmeasure.databases=Postgres` runs the matrix on a lane's database.
  - *Bind once, checked by printing the SQL (section 2.3).* On DuckDB a comparison's side is written ONCE in every
    place probed (a query's answer, under `!`, in an `if`, two computed sides, `contains`, `!=`). An assert's two
    sides are each bound once already (two named row sets, read by the verdict and by the failure record). Three
    things are NOT once, none of them the comparison's own writing: (1) **a `let` read twice is written twice**
    (`let a = <list>; $a == $a`: the list twice) -- the planner splices a value let into each reader (D75,
    `planner/LetPlanner`: a let is a staged table or a splice); binding it once changes that model for every
    statement and every database, which does not fit slice 2's cap, so by the plan's own rule (section 2.3) it is
    the first slice of the lean-SQL follow-up, with the user told; (2) a list operation's own bounds check
    (`->at(0)`: the list's length, then its element), the same follow-up's; (3) SQLite's reading of a JSON list
    as rows writes the list three times (the dialect's unnest), low priority by the user's word.
  - *Found by the assert probe, for slice 6:* an assert of two lists still unpacks each list to rows and packs it
    again (about 900 characters on DuckDB where the product's `==` is 80): the assert is planned from each side's
    statement of rows (`SideEqualities.values`), not from the product's instruction over two values. The rules are
    the same code's; the writing is not. It goes when asserts are planned as the comparison they mean.
  - *Round and review:* CI's core, checks, parser-equivalence and DataCube lanes on `e2d76a653` here (Bazel's cache
    re-runs what the change touches; `scratchpad/b111.out`), and an independent review of the slice's diff, both
    running at 01:45. Then the database lanes on the final commit: PCT's two channels per database (slice 1's
    product change), the time-series replays on ClickHouse, Postgres and Snowflake (the library's text moved), and
    the matrix on Postgres (new).
- **Slice 3, started beside it** (the scratch branch, with slice 2 merged in): its half page is
  `slice-3-half-page.md`.

**2026-11-28, 02:30 UTC.**

- **Slice 1** (`c77deac20`): CI 54 of 55 lanes green, none red; `macos / warehouse` still running since 01:26. The
  program branch moves when it finishes.
- **Slice 2** (`claude/step-6b-slice-2`, `e951b6d5d`).
  - *Independent review* (an Opus reviewer, the diff and the rules): the port is faithful; its main finding was the
    pin -- one kind per comparison let a comparison get worse inside its kind unseen (about 85 of the 96 lines).
    Fixed: the pin now holds what was asked (the places, each comparison's expected answer and sides) and, for every
    comparison not right everywhere, EVERY outcome with its places (386 lines). Also fixed from it: an answer in a
    left-out place is judged; answers are read in their exact shape; two answers the rule itself makes different are
    no contradiction (one comparison per database left "contradicts" for "right"); one time limit for a hand run and
    the pin; the one-point form's two window columns in one step (one window level fewer; the ledgers do not move).
  - *Its other finding, not slice 2's:* about sixty other forms of the time-series library put the D8 check on rows
    a later filter drops (`helpers::rampedPeriod`, `helpers::ramp`, `sharpe_ratio`'s r form, weaker in
    `bollinger_bands`). For the user; its own change.
  - *Regenerated and run on the reviewed commit:* the pin, the parity test, the oracle's generated files and the
    bundle budget, 7 of 7.
  - *The local round* (CI's core, checks, parser-equivalence and DataCube lanes on `1a035a242`): one real failure,
    `//core:census` -- the measure's "named databases" setting named a database outside its owners. That setting
    could not work anyway (below), so it is taken out (`e951b6d5d`); the census, the guards, the pin and the checks
    lane are re-running on that commit (`scratchpad/b113.out`). The other failures are the Postgres-backed tests a
    root container cannot start.
  - *The database lanes on `1a035a242`* (slice 1's product change and slice 2's library text), each against its own
    last round (`3bee40e22`): **no test's outcome differs** on BigQuery, Databricks, SQLite, MongoDB, MariaDB, MySQL,
    ClickHouse, Postgres (channel B) and Trino; the time-series replays and documents pass on Postgres and
    ClickHouse. Set apart, both older than this work: MariaDB channel B's list-order flake; a MongoDB report line
    (the dynamic pivot compared in order with no sort). Snowflake had not reported at 02:30.
  - *The matrix on Postgres did not run:* the measure builds the storeless runtime, which a server's connector
    refuses without a server's coordinates. It needs the measure to start the server and declare it
    (`StorelessRuntime.onServer`): slice 3's, with the lane.
- **Slice 3** (the scratch branch, `claude/step-6-numbers`; measured by the no-Bazel loop, not Bazel-built).
  - Independent review of part 3a (`review-4-of-slice-3a.md`): two blockers (the rows form lost the instant reading
    for date-times; a side typed Nil was dropped, so `[]->toOne() == 1` answered false where Pure raises), three
    should-fix (the "never equal" decision was a second classifier off SQL types; a literal against a column was
    exempted too widely; the doors changed unrecorded), five notes. All fixed but two, recorded as open: two lists
    of written date-times at two precisions of one instant are compared as texts; a value read through a `let`
    whose upcast is gone, against a column, still reads as the column's type (the lowering sees no record of the
    comparison having been kept; root cause not found).
  - Built beside it: DuckDB refuses by name a sub-query inside a list's function; a comparison kept for its call
    keeps its sides until the call's rule answers (an enumeration against a String in a generic function is that
    call's compile error); the resolver carries an answered kept comparison; lists typed `Number` that each hold
    one kind take the value form, the planner standing aside by the same reading; the measure knows "must raise".
  - Measured (107 comparisons): **DuckDB 95 right everywhere, 11 answered in some places and refused by name in
    the others, 1 failing (the `map` defect, below), none wrong, none contradicting; H2 89 right; SQLite 89.**
  - Found, not equality's, for the user: a `map` over an empty value can answer a value (the store resolver's
    composition; `slice-3-half-page.md`).

**2026-11-28, 02:55 UTC.**

- **Slice 1 has landed:** CI run 46091392535 on `c77deac20` green on every lane ("gates green", 02:30 UTC); the
  program branch `claude/compat-b3-nufdlk` is at `c77deac20`.
- **Every database lane has reported on `1a035a242`** (slices 1 and 2): no test's outcome differs from each lane's
  last round on BigQuery, Databricks, Snowflake, SQLite, MongoDB, MariaDB, MySQL, ClickHouse, Postgres and Trino;
  the time-series replays, documents and island tests pass on Postgres, ClickHouse and Snowflake.
- **Slice 2** (`claude/step-6b-slice-2`, `c529b4599`): two more misses found by its last checks and fixed, both
  build files, no product code: the measure's new package did not declare the repository's guard targets
  (`guards_package()`), which kept the guards from LOADING; and `tools/ci/pre-push-round.sh` printed failed tests
  alone, so the first round read as "only the known ones" with ten loading errors in its log -- it prints Bazel's
  errors now. The census, the guards, the pin, the checks lane and parser equivalence are re-running on `9ed3b1321`
  (`scratchpad/b115.out`). The local round's other fourteen failures are the targets a root container cannot run
  (twelve Postgres-backed tests, `is_numeric_probe_test`, DataCube's `verify_app_test`), which CI holds.
- **Slice 3** (`claude/step-6-numbers`, `25c9d78c9`): a second independent review (`review-5-of-slice-3.md`) found
  three more blockers in the first fix pass, two of them regressions against the landed code (a promoted SQL type
  read as a value's kind; two computed lists of date-times carried as texts; sides declared the abstract Date).
  Fixed, each as a row of the measure (110 comparisons): DuckDB 97 right everywhere, 12 answered in some places and
  refused by name in the others, 1 failing (the `map` defect), none wrong, none contradicting; H2 91; SQLite 91. A
  third review, asked one question (which expressions answer differently from the landed code, and is each new
  answer the rule's), is running. Left open and recorded: a comparison in a user's function is not read by
  `heldApart` (as on the landed code); `in` and `contains` kept for their call (slice 5); StrictTime; a value read
  through a `let` whose upcast is gone, against a column.
- **The measure on a server database:** the value matrix now runs on a named test backend (`-Dmeasure.backends`,
  the backend declares its own runtime); it rides with slice 3 and the Postgres lane tries it then.

**2026-11-28, 03:05 UTC.**

- **Slice 2 is pushed to the pull request** (`c529b4599`, 02:55 UTC) after its last checks passed here on
  `9ed3b1321` (the census, the guards, the pin, CI's checks lane and parser equivalence: 201 pass, 1 skipped, no
  build error). CI is running; the program branch moves to it when CI is green.
- **Slice 3, first Bazel build** (`scratchpad/batch114.sh` on the scratch branch): the measure built by Bazel gives
  the no-Bazel loop's numbers exactly (DuckDB 97 / H2 91 / SQLite 91 of 110). Its local round (CI's Java lanes) is
  running. A third review is reading it.
- **Slice 5's baseline, measured ahead** (a third matrix in the measure: `in`, `contains`, `indexOf` and
  `assertContains` asked the 44 comparisons of two single values; each must answer as `==` does). On DuckDB 35 of 44
  answer as `==` everywhere. Not so, for slice 5:
  - **wrong:** two values declared `Number` holding an Integer and a Float: `contains` and `indexOf` answer true
    (`in` answers false, rightly); a day and a date-time under the abstract `Date`: every function answers true;
  - **failing when the query runs:** a date built to the hour against a written date-time, in every function;
  - **refused by name in some places:** values declared `Any` under `contains` over a computed list and `indexOf`;
    the mixed-list row (part 2);
  - **asserts:** `assertContains` under the database checker is unjudged for several, and refused under the Java
    checker for values declared `Number` or `Any`.
  One thing that looked wrong and is not: `['a ']->contains('a')` is true -- a written list of one text IS that
  text, and `contains` of a text and a text is the STRING function (a substring's test), by the functions'
  signatures on legend-pure too. The matrix asks lists of two.

**2026-11-28, 03:25 UTC.**

- **A wrong answer in LANDED slice 1, found by slice 3's third review and run:** `[1, 2]->map(x | if($x == 1, |1,
  |[])) == [1]` is false on DuckDB (the value form compares the raw SQL list, whose empty slot is a NULL; the
  statement it replaced compared rows with those dropped). Told to the user. Fixed on the slice 3 branch
  (`CollectionLanes.compacted`, `holdsNoEmptySlot`): it lands with slice 3; three rows of the measure hold it.
- **Slice 2's CI** (`c529b4599`): `linux / checks` red on a download outage (`archive.mariadb.org` 503 for the
  embedded MariaDB; the 23 tests it reached passed). One comment on the pull request says so; the user is asked to
  re-run that job. No push to clear it.
- **Slice 3** (`675d1b8dc`): three independent reviews, records `review-4`, `review-5`, `review-6`; every blocker
  fixed, each with a row of the measure. The first Bazel round (on an earlier commit) caught the product's SQL size
  pin growing for `[]->forAll(e | $e == 0)` and the cast-and-case census growing by two: the new rule for a side
  typed Nil had fired for ONE value typed Nil (an empty list's element); it now reads a list typed Nil alone and
  the query is as it was. The round restarted on `675d1b8dc` (`scratchpad/b114.out`); a fourth, narrow review of
  the last fix pass is running. Measure (113 comparisons of the first two matrices): DuckDB 97 right everywhere,
  12 answered in some places and refused by name in the others, 4 failing somewhere (the `map` defect's row, and
  the three new empty-slot rows, which are right in every product place and fail under the Java checker's
  `assertEquals` alone), none wrong, none contradicting; with the third matrix (the other comparing functions,
  slice 5's baseline) DuckDB 132 of 157. (A first writing of this entry said "100 of 113": a miscount, corrected.)
  Open and recorded in `review-6`: the planner's reading of a `Number` side's carrier on three cloud databases; a
  NULL list against `[]` on DuckDB; and, seen in the measure, the Java checker erroring on `assertEquals` of a list
  with an empty slot (not checked whether as on the landed code).

**2026-11-28, 03:40 UTC.**

- **Slice 2's CI** (`c529b4599`, run 46100396174): `linux / checks` and `windows / checks` both red at ANALYSIS on
  the embedded MariaDB's download (`archive.mariadb.org`: 503, then a read time-out); the tests each reached
  passed. One comment on the pull request; the user is asked to re-run those jobs. This session cannot re-run a job
  (GitHub refuses) and pushes nothing to clear it.
- **Slice 3** (`675d1b8dc`): the Bazel-built measure equals the no-Bazel loop's again (132 / 123 / 121 of 157 with
  the third matrix). Its round restarted at 03:23 on that commit; at 03:34, 119 of 354 with no failure beyond the
  root-container ones. A fourth, narrow review of the last fix pass is running.
- **The no-Bazel loop had a fault, found and fixed** (`scratchpad/fast2.sh`): it put every jar of `bazel-bin/core`
  on the compile path in name order, and two older aggregate jars (`libcore_next`, `server_deploy`) hold stale copies
  of the lowering's classes, which won over the fresh ones whenever a changed class was not itself recompiled. It
  showed as "cannot find symbol" for a method that exists. Now newest first, aggregates left out. Earlier loops
  were not affected in their answers: every class they changed was compiled from source in front.
- **Slice 5, first cut** (`claude/step-6b-slice-5`, `41c0dd6a4`, worktree `/home/user/ll-s5`; stacked on slice 3;
  measured by the no-Bazel loop, not reviewed, not Bazel-built): `contains`, `in` and `indexOf` read their list and
  needle through `Equalities.member` -- the kinds the query's text names (never equal: no member, both sides
  evaluated), date-times written as texts by their instants. On DuckDB the third matrix has no wrong answer and no
  contradiction (two before), and a date built to the hour is found among held date-times (a run error before);
  the first two matrices do not move. Left in it: values declared `Any` under `contains` over a computed list and
  `indexOf` (refused by name), the asserts (slice 6), the mixed-list row (part 2).

### 2026-11-28, about 04:25 UTC

- **Slice 2's CI** (`c529b4599`, run 46100396174): 52 of 55 jobs green. The three red are `linux / checks` and
  `windows / checks`, both at ANALYSIS on the MariaDB download mirror (not the change; one comment on the pull
  request says so), and the roll-up `gates green` that follows from them. This session cannot re-run a job. The
  checks lanes ran green on macOS in CI and on Linux here before the push. Slice 3's push will run them again over
  slice 2's code; the program branch stays at slice 1 (`c77deac20`) until a run is green on those two.
- **Slice 3's final commit is `4b77dadaf`** (the scratch branch `claude/step-6-numbers`; `d2ca88623` adds a record
  only). What changed since the last entry:
  - The first Bazel round found ONE real failure: `//core:guardrails`, the carrier guard (175 direct list-function
    sites in the lowering against a pin of 172). Fixed at its cause, no pin moved: a list of date-times read as
    instants is `InstanceEquality.equalValues`, and "the list holds no element" is one helper, `Scalars.noElements`.
    The measure is byte for byte what it was.
  - A fifth independent review (`review-8-of-slice-3.md`) of the last fixes: nothing that holds. Its one medium
    finding (a door's root comparison of date-time lists) is refuted by the code: the lowering asks the planner only
    where no door named a rule (`Lowerer.wholeSides`, `wholeAnswer`).
  - The Bazel-built measure on `4b77dadaf`: DuckDB 133 right of 158, H2 123, SQLite 122; the same 465 lines the
    no-Bazel loop gives; the baseline is committed.
  - The round on `4b77dadaf` (`scratchpad/batch114.sh`, started 03:58): running. Failures so far are two of the
    known root-container Postgres tests. Then the lanes, then one push.
- **Slice 5** (`claude/step-6b-slice-5`, `0e97fbc3b`, stacked on slice 3; the no-Bazel loop; an independent review
  running). Built today, each from a measured case (`slice-5-half-page.md`, "Built"): a list's kind named through
  sort, take, drop and the deduplications; two empty optional enumerations equal; a mapped list's empty slots
  dropped where it is built; a year in a Date-declared list as no instant; `hasSameElements` of two list values as
  the instruction in any order; the held type read through one list, the type-checker's
  (`EqualityClasses.valuesOf`), which also answers the comparison inside a function of `Any`. Measured: DuckDB 181
  right of 223, none wrong, 3 contradicting before the last fix (two of them the row kept as `?` for the user: a
  literal under an upcast), 6 failing (four at `assertEquals` under the Java checker, task 29: the type-checker
  types a `map` whose function may answer nothing with its source's count; one the assert's own, slice 6; one the
  store defect, task 26).
- **Found, not equality's, for the user:** task 29 above. **To rule:** two additions in
  `measure-gaps-2026-11-28.md` (a literal the query itself upcast).
- **What is left of part 1 after slices 3 and 5, said plainly.** The answers: on DuckDB the measure has no wrong
  answer and no contradiction the rule settles; what is not "right everywhere" is refused by name inside a list's
  function (tables, maps, pairs, lists of `Any`: a statement of their own, which DuckDB runs none of there) or is an
  assert's (slice 6). The structure: plain kinds are ONE instruction wherever they stand, for `==`, for
  `hasSameElements` and for the asserts (`SideEqualities.values` writes the same instruction over each side bound
  once). Still two builders of that instruction (`Equalities.sameValues` from two expressions,
  `SideEqualities.values` from two statements), the planner's statement for every other kind
  (`RootComparison` over `SideEqualities.pair`), and the test checker's own gatekeeping (`DatabaseJudge`,
  `AssertVerdicts`). Those are slices 4, 6, 7 and 8.
