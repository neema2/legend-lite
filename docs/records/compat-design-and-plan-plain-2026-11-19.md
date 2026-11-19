# The compatibility program: the design and the plan, in plain English (2026-11-19)

This is the readable version. It replaces the front page of `compat-program-plan-2026-11-19.md`, which is a stale
working draft. Every number here was measured today; the evidence is in `compat-program-log-2026-11-19.md` and the
homework records it names.

## 1. What this is for

Lite can run legend-engine and legend-pure programs. Today it does that through about 50 switches inside its own
compiler and planner, each asking "am I serving legend-engine right now?". Those switches make every change to lite's
core harder, and they hide bugs.

The aim: **lite's core has one meaning and no such switches. Everything that is specific to legend-engine or
legend-pure sits at the edge, where a request comes in and where an answer goes out.**

## 2. The design

```
a legend-engine or legend-pure request
      |
  [ THE DOOR ]        reads legend-engine's project, model and query formats, as today
      |
  [ LITE'S CORE ]     compiles, plans and runs it, exactly as it would for a lite user. One meaning: Pure's.
      |
  [ THE FORMATTER ]   turns lite's typed result into legend-engine's or legend-pure's exact output, in Java
      |
legend-engine's or legend-pure's answer
```

**The four parts:**

1. **Lite's core means one thing: Pure's own definitions.** Where Pure and legend-engine define a function
   differently, the core follows Pure (ruled: `substring` and `indexOf`).
2. **The door keeps accepting legend-engine input unchanged:** its grammar, its JSON protocol, its project files, its
   server addresses. It does not copy legend-engine's refusals: if lite can answer, it answers (ruled).
3. **Formatters produce upstream's output after the query has run.** legend-engine's result formats, legend-pure's
   printed text and each side's error wording are written in Java, from lite's typed results. Today much of this is
   built inside SQL; that goes.
4. **Test checking the legend-pure way lives at the edge.** Lite's own judge decides pass or fail and records why;
   legend-pure's wording comes from a formatter. The fallback that re-runs legend-engine's expected SQL and compares
   the answers stays, unchanged in behaviour (ruled: "one of the crown jewels").

**What the door does NOT do:** translate programs. Phase 0 measured that a translator would win back 4 tests per
database in legend-engine's corpus. So none is built, unless decision D1 below says otherwise.

## 3. What stays exactly as it is

- Loading and compiling legend-engine projects: models, mappings, stores, runtimes, connections.
- The legend-engine server routes Studio uses, and legend-engine's result format (a prototype reproduced it byte for
  byte from a formatter).
- Lite's own API, its own SQL, its own tests.
- The fallback for SQL-text tests.
- The prelude and the parser (ruled).

## 4. What we lose, and what breaks

**Will lite still take a legend-engine project and run it? Yes.** It loads, compiles and runs as today, and answers in
legend-engine's formats. On legend-engine's own function test suite (PCT), lite's legend-engine route passes more than
legend-engine does on four of the five places measured, and fewer on one:

| PCT suite | lite | legend-engine | lite behind on |
|---|---|---|---|
| DuckDB, all five suites (1,249 tests) | 1,212 | 1,005 | 1 test |
| H2 relation (469) | 448 | 411 | 0 |
| H2 essential (345) | 252 | 131 | 6 |
| H2 standard (205) | **113** | **134** | **27** |

The 34 tests where lite is behind are gaps in lite's own H2 support that exist today; this program does not cause
them. Lite refuses by name rather than answering wrongly: `greatest`, `least`, `mode`, the max and min of dates, the bit
operations, splitting a string into rows, and two functions H2 does not have under the names lite uses (cube root,
reverse). They are with the product track.

**What changes for such a project** (each measured, or marked "not yet measured"):

| # | What changes | Who notices | How big |
|---|---|---|---|
| 1 | **`substring` and `indexOf` inside a query sent to the database follow Pure**, not legend-engine's database-flavoured meaning. `'abcdef'->substring(1, 3)` is `'bc'` in Pure and `'abc'` under legend-engine on H2. **The answer changes silently; there is no error** | a legend-engine project whose queries call these with positions | legend-engine's whole test corpus has 5 such calls (4 tests per database). Lite's own 62 projects have 0. legend-engine itself treats its behaviour as a mistake, corrected by a flag that PCT always sets |
| 2 | **The SQL lite shows is lite's own**, not legend-engine's spelling (in "show SQL", plans and `toSQLString`) | anyone reading or comparing SQL text | 143 of 2,761 corpus tests (5%) check only that text and stop counting; 3 more join them once `indexOf` follows Pure (their text check was always true before), so 142 on H2 and 146 on DuckDB. Every other SQL-text test still passes by its answer |
| 3 | **legend-engine's old H2 "legacy mode" is gone** | a model that relies on it | 1 corpus test (it compares a yes/no column with the number 1) |
| 4 | **Maths that legend-engine leaves unchecked raises lite's clear error** instead of returning null or the database's own error (square root of a negative, division by zero) | a query that relied on getting null | measured: 1 answer on the server route (square root of a negative: an error where legend-engine gives null); 0 corpus tests; on legend-engine's PCT route, 6 tests move and all 6 move from failing to PCT's expected answer (legend-engine itself fails or refuses each). **Depends on decision D2** |
| 5 | **Float literals are ordinary floating-point numbers**, not exact decimals (`0.1 + 0.2` is `0.30000000000000004`, where legend-engine on H2 and DuckDB gives `0.3`) | results with literal arithmetic | measured: 0 corpus tests lost and 2 GAINED on H2 (the decimal printed `52` where legend-engine's expected value is `52.0`); 0 answers on the server route (only the SQL text shown changes); 0 PCT tests move. **Depends on decision D2** |
| 6 | **legend-engine's quirks in `isNumeric`, `parseDecimal` and `parseJson`** are answered Pure's way | queries using those on odd input | measured on the corpus: `isNumeric` 0 tests; `parseDecimal` 2 per database (legend-engine cuts to two decimal places); `parseJson` 1 lost on DuckDB and 1 GAINED on H2 (today's copy of the quirk is itself broken on H2). **Depends on decision D1** |
| 7 | **Plans cannot yet be handed to legend-engine to run, or taken from it** (export and import are deferred) | no one today: it does not exist yet | a deferred goal, not a loss |
| 8 | **On SQLite, legend-pure programs' Decimal maths**: kept in floating point by one small rule at the door (ruled), or refused | PCT on SQLite | 5 tests if the rule were dropped |
| 9 | **legend-engine's feature flags in a request** (its "corrected substring" and "legacy null equality" options) stop switching lite's core. They are read at the door: the first is now always on; the second is refused by name | a request that sets the legacy flag | 7 corpus tests set a flag; 1 depends on the legacy one, and it checks SQL text only |
| 10 | **Error text**: where legend-engine's message is its database's own error over legend-engine's SQL, lite gives its own clear message in legend-engine's envelope. Lite's errors on these routes carry no source position today, and still will not | a client that reads error text; Studio's underlining | unchanged from today, stated for honesty |
| 11 | **A program written against lite's legend-engine route may not run on legend-engine itself**, because lite answers where legend-engine refuses (99 of its 110 refused PCT tests on DuckDB, 28 of 36 on H2) | someone moving back to legend-engine | by the ruling |
| 12 | **For lite's own users, `substring` with a negative start raises** (Pure's rule), and a saved query or cube using `substring`/`indexOf` changes its answer | lite users with such saved queries | lite's own 62 projects: 0 uses; deployed stores cannot be counted from here |

**One thing the door must keep doing** (measured, not a loss): legend-engine's corpus sets up its test data with SQL
written for H2. To run it on DuckDB, the door converts that setup SQL. Without it, all 2,452 DuckDB corpus tests fail at
setup. That conversion stays, at the door.

**Being re-checked:** Studio's "generate plan" for an ordinary query is built from lite's own plan in legend-engine's
plan envelope (`PureV1Routes.generatePlan`). That was read from the code. B2's check of the compiled code finds one path
to the SQL-spelling code A2 deletes: a query that itself asks for SQL text (`toSQLString`, `planToString`, the DDL
functions) sent through the legend-engine server. B2 is confirming it by a run, counting the recorded requests that
take it, and naming what such a request answers after A2.

**Not measured by anyone yet:** other legend-engine databases (Snowflake, Postgres, Databricks and the rest) for the
rows above; real deployed legend-engine projects (legend-engine's six public showcase projects have 0 calls of the
affected functions); Studio end to end; very large results through the Java formatters.

**What does not work today and still will not:** legend-engine's mapping test suites and service tests, cross-store
queries, external formats, and databases lite has no dialect for (DB2, Oracle, SQL Server and others). On H2, the 34
functions listed above are refused until the product track adds them.

**What gets better:** where legend-engine refuses or answers wrongly, lite answers correctly: on DuckDB's PCT, 109
wrong answers fixed and 99 of legend-engine's 110 refusals answered; on H2's relation suite, 9 and 28 of 36 (245 such
tests were read; all but 2 check exact values).
Stage A adds 13 more on the legend-engine route (7 from `substring`/`indexOf`, 6 from the unchecked maths).

## 5. The plan

Two stages, with a stop between them. Every step is a pull request, run on every operating system, reviewed by a
session that wrote none of it, and merged only on the user's word.

### Stage A: measured and low risk (about 2 to 3 weeks)

The order: A1; then A2 and A3 in either order; then A4.

| Step | What | How we know it worked |
|---|---|---|
| A1 | **The safety nets.** A recorded snapshot of every answer on the legend-engine and legend-pure routes; a check against legend-engine's own published test results; PCT as lite's own test suite under both of lite's judges | PR #123 (up), the legend-engine check (built), the PCT suite (the product track is building it). Each net is first shown to catch a planted mistake |
| A2 | **Delete the code that copies legend-engine's SQL spelling** (the three printers, about 3,000 lines, and the plan-printout code used only by the corpus, about 3,500 more) | the corpus: every test that passed by its answer still passes; exactly the text-only tests (142 on H2, 146 on DuckDB once A3 is in) and the 6 plan-structure tests move, listed by name; asking for legend-engine's SQL on a database lite has no dialect for is refused by name; lite's SQL size only shrinks |
| A3 | **Fix lite's own Cypher, SQL and IQ frontends** (47 places write `substring`/`indexOf` the old way), **then switch the core to Pure's meaning.** First: the product track's fix for comparing text with a number. Needs the user's word on: the negative-start error, one guard ceiling, 17 of lite's own tests that pin the old meaning, and PCT's 3 known-failure rows | lite's frontend checks unchanged (prototyped: Cypher 4,267, IQ 5,984, SQL 1,651 cases, none regress); PCT's 3 known `indexOf` failures pass; the corpus loses the 4 tests of row 1 |
| A4 | **Take legend-pure-style checking out of lite's database judge only.** legend-pure's comparison rules stay in the Java-side check until stage B (the corpus needs them: 69 tests on DuckDB, 29 on H2). Lite's two judges keep lite's own rules | PCT unchanged (measured: legend-pure's special comparison rules change 0 PCT verdicts); lite's judges agree on the new PCT suite |

**Stage A has been run as a dry run, all its changes together, on the real suites** (2026-11-19, the record is
`H12-combined-dry-run.md` on `claude/compat-translator-spike`):

| Suite | Today | With all of stage A | What moved |
|---|---|---|---|
| legend-engine's corpus on H2 | 2,289 pass | 2,135 pass | 157 stop passing: 146 are SQL-text and plan-printout checks (not promised), 8 are `substring`/`indexOf` (4 of them also text checks), 3 are the small quirks and the legacy mode. 3 start passing |
| legend-engine's corpus on DuckDB | 2,452 pass | 2,290 pass | 162 stop passing: 150 SQL-text and plan-printout checks, 9 `substring`/`indexOf` (5 of them also text checks), 3 quirks. None start passing |
| PCT on DuckDB, all five suites, both ways of judging | as recorded | unchanged, but 3 known failures now pass | the 3 are `indexOf` tests |
| legend-engine's PCT route (DuckDB five suites, H2 three) | as pinned | 22 more pass (13 DuckDB, 9 H2) | every one moves to PCT's expected answer; none moves the other way |
| the server route's 28 recorded answers | as recorded | 8 lines move | 1 answer (square root of a negative), 1 error's words, 6 lines of SQL text |

Every moved test was predicted from the single measurements, except 3 per corpus database: SQL-text tests whose check
was always true under the old `indexOf`. Nothing that was predicted to fail passed, and nothing else moved.

**Stop. Review the results before stage B.**

### Stage B: the bigger moves (about 3 to 4 weeks), only after its prototypes pass

The order inside it, by the independent reviewer: B1, then B3, then B2 (the formatter prototype rides the very hooks
B3 reshapes, so building B2 first would mean writing it twice).

| Step | What | How we know it worked |
|---|---|---|
| B1 | **Errors become plain records** (what kind, where, which literal values). Each side's formatter words them. Upstream wording never enters lite's plans | every pinned error message on every route unchanged, except a short named list. **Prototype running: on DuckDB's PCT, every pinned message identical except 6 tests whose literal the lowering wraps (a fix in lite's own lowering is being measured); five of today's switches deleted; no change to lite's core** |
| B2 | **Formatters replace SQL-built text** (about 2,000 lines deleted): legend-pure's printed tables and values, legend-engine's result envelopes | PCT's 423 printed-table tests and legend-engine's result goldens byte-identical. **Prototyped: 431 of 431 printed tables byte-identical on DuckDB, 342 of 342 on H2 (4 more tests pass), 419 of 420 on SQLite, 427 of 430 on Postgres; no difference caused by the printer on any; no change to lite's core. It exposed one product bug (now with the product track) that must be fixed first. Size: 6 to 8 days** |
| B3 | **The fallback and legend-pure-style checking move behind three narrow connections**, with four small rewrites of legend-engine's test code at the door | both corpus result lists byte-identical. **Prototype running now** |
| B4 | **Remove what is left**: the legacy H2 settings, the float-literal and unchecked-maths switches (per D2), DataCube's legend-engine workarounds moved to its legend-engine side | each removal's moved tests listed before it lands |
| B5 | **Delete the switch machinery itself, legend-engine's feature flags in the core included, and turn on the guards**: the build fails if a mode switch or upstream-specific code returns to the core | the counts of switches (49 parameters, 50 calls today) and of upstream code in the core (142 places today) both read zero |

**Deferred:** exporting plans to legend-engine and importing its plans. **Moved to the product track:** lite's own
rules copied across Python and the browser tab.

## 6. What was open (all ruled 2026-11-19 ~13:25 UTC: "Okay agree with all")

**Ruled:** the goal's sentence, yes; D1, Pure's way, loud; D2, remove both shortcuts; and a "differences from
legend-engine" page. The plan to build from is now `compat-final-plan-2026-11-19.md`. The text below is kept as it was
put to the user.

**First, one sentence for the user to rule, because the goal has changed** (the independent reviewer's first
condition). The project's rulebook says lite does exactly what legend-pure and legend-engine do, unless that is too
expensive. Today's rulings follow a different rule: **"Lite follows Pure. legend-engine's formats are matched, and its
answers where they agree with Pure. The rest is recorded."** D1 and D2 follow from that sentence; without it they are
exceptions to the rulebook.

**Then two decisions:**

- **D1. Where legend-engine departs from Pure's own definition, copy legend-engine or answer Pure's way?** (Rows 1 and
  6 above.) Answering Pure's way means no translator at all. Copying means a small typed rewrite at the door, which a
  prototype has shown can be built without touching lite's core.
  **A middle option, from the independent reviewer: Pure's way, but LOUD.** The one silent change is `substring` and
  `indexOf` in a query sent to the database. The legend-engine door can detect that case (it does not rewrite it) and
  warn, or refuse by a switch, when the request has not set legend-engine's own "corrected" option. A start position
  of 0 on H2 is unaffected, which is the commonest use.
- **D2. Keep the two shortcuts that copy legend-engine's number behaviour?** (Rows 4 and 5.) **The count is in**
  (B2's (j) and the combined dry run agree). Removing both changes **7 answers that match legend-engine today**:
  - 4 on DuckDB where legend-engine returns a value and Pure raises an error: an out-of-range `slice` (empty list),
    `rem` by zero (null), a `range` with step zero (empty list), a shift right by 63 or more bits (0);
  - 1 on the server route: `sqrt` of a negative on H2 (null);
  - 2 Float sums and products on DuckDB, and the same 2 on H2, where legend-engine's exact decimals give `1.7` and
    lite's ordinary floating point gives `1.7000000000000002`.

  A user who wants legend-engine's answer can write it in the query: 5 of the 7 with an ordinary `if` guard, the 2
  Float ones by using Decimal. Removing gains 6 PCT tests on the legend-engine route (PCT expects the error) and 2
  corpus tests on H2, and loses none. 3 more errors keep their status and change only their words.
  **H2 counted too:** 4 more PCT tests gain (they expect the error), and the only H2 answers that stop matching are the same 2 Float cases. Three H2 answers (`sqrt` of a negative giving NaN, bit shifts by 63 that wrap) are matched by neither lite nor any guard, today or after; they are lite H2 gaps on the product track. **Not counted:** other databases.
  **Recommendation: remove both.** It is what the goal's sentence says, and PCT, legend-pure's own test suite, agrees
  with Pure in all 6 error cases.

**Stage B's three prototypes:** B2's table printer in Java has passed (four databases). B3's move of the fallback
has passed its first check (PCT and the H2 corpus identical; one design question open, where the four small rewrites
of corpus test code are applied). B1's error records is running.

**One sequencing constraint:** the product track is fixing bugs in the same judge and number code. Its judge fixes land
before A4.

## 7. The safeguards

- Nothing merges without the user's word.
- A step that turns out to need a new switch in lite's core stops and comes to the user.
- Every difference from legend-engine or legend-pure that a step creates is listed for the user before it lands.
- The stop after stage A.
