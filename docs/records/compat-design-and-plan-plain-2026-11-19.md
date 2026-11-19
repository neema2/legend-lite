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
legend-engine's formats. On legend-engine's own function test suite (PCT), lite's legend-engine route already passes
more than legend-engine does: 1,212 against 1,005 on DuckDB, 448 against 411 on H2.

**What changes for such a project** (each measured, or marked "not yet measured"):

| # | What changes | Who notices | How big |
|---|---|---|---|
| 1 | **`substring` and `indexOf` inside a query sent to the database follow Pure**, not legend-engine's database-flavoured meaning. `'abcdef'->substring(1, 3)` is `'bc'` in Pure and `'abc'` under legend-engine on H2. **The answer changes silently; there is no error** | a legend-engine project whose queries call these with positions | legend-engine's whole test corpus has 5 such calls (4 tests per database). Lite's own 62 projects have 0. legend-engine itself treats its behaviour as a mistake, corrected by a flag that PCT always sets |
| 2 | **The SQL lite shows is lite's own**, not legend-engine's spelling (in "show SQL", plans and `toSQLString`) | anyone reading or comparing SQL text | 143 of 2,761 corpus tests (5%) check only that text and stop counting. Every other SQL-text test still passes by its answer |
| 3 | **legend-engine's old H2 "legacy mode" is gone** | a model that relies on it | 1 corpus test (it compares a yes/no column with the number 1) |
| 4 | **Maths that legend-engine leaves unchecked raises lite's clear error** instead of returning null or the database's own error (square root of a negative, division by zero) | a query that relied on getting null | measured: 1 answer on the server route (square root of a negative); 0 corpus tests; 16 PCT test cells on 8 databases by legend-engine's own results (lite's PCT count is running). **Depends on decision D2** |
| 5 | **Float literals are ordinary floating-point numbers**, not exact decimals (`0.1 + 0.2` is `0.30000000000000004`, where legend-engine on H2 and DuckDB gives `0.3`) | results with literal arithmetic | measured: 0 corpus tests lost and 2 GAINED on H2 (the decimal printed `52` where legend-engine's expected value is `52.0`); 0 answers on the server route. The PCT count is running. **Depends on decision D2** |
| 6 | **legend-engine's quirks in `isNumeric`, `parseDecimal` and `parseJson`** are answered Pure's way | queries using those on odd input | measured on the corpus: `isNumeric` 0 tests; `parseDecimal` 2 per database (legend-engine cuts to two decimal places); `parseJson` 1 lost on DuckDB and 1 GAINED on H2 (today's copy of the quirk is itself broken on H2). **Depends on decision D1** |
| 7 | **Plans cannot yet be handed to legend-engine to run, or taken from it** (export and import are deferred) | no one today: it does not exist yet | a deferred goal, not a loss |

**One thing the door must keep doing** (measured, not a loss): legend-engine's corpus sets up its test data with SQL
written for H2. To run it on DuckDB, the door converts that setup SQL. Without it, all 2,452 DuckDB corpus tests fail at
setup. That conversion stays, at the door.

**What does not work today and still will not:** legend-engine's mapping test suites and service tests, cross-store
queries, external formats, and databases lite has no dialect for (DB2, Oracle, SQL Server and others).

**What gets better:** where legend-engine refuses or answers wrongly, lite answers correctly (153 fixes and 55 of 59
refusals answered on DuckDB's PCT, each checked against legend-pure's expected value).

## 5. The plan

Two stages, with a stop between them. Every step is a pull request, run on every operating system, reviewed by a
session that wrote none of it, and merged only on the user's word.

### Stage A: measured and low risk (about 2 to 3 weeks)

| Step | What | How we know it worked |
|---|---|---|
| A1 | **The safety nets.** A recorded snapshot of every answer on the legend-engine and legend-pure routes; a check against legend-engine's own published test results; PCT as lite's own test suite under both of lite's judges | PR #123 (up), the legend-engine check (built), the PCT suite (the product track is building it). Each net is first shown to catch a planted mistake |
| A2 | **Delete the code that copies legend-engine's SQL spelling** (about 3,000 lines) | the corpus: every test that passed by its answer still passes; exactly the 143 text-only tests move |
| A3 | **Fix lite's own Cypher, SQL and IQ frontends** (47 places write `substring`/`indexOf` the old way), **then switch the core to Pure's meaning** | lite's frontend checks unchanged (prototyped: Cypher 4,267, IQ 5,984, SQL 1,651 cases, none regress); PCT's 3 known `indexOf` failures pass; the corpus loses the 4 tests of row 1 |
| A4 | **Take legend-pure-style checking out of lite's database judge.** Lite's two judges keep lite's own rules | PCT unchanged (measured: legend-pure's special comparison rules change 0 PCT verdicts); lite's judges agree on the new PCT suite |

**Stop. Review the results before stage B.**

### Stage B: the bigger moves (about 3 to 4 weeks), only after two prototypes pass

| Step | What | How we know it worked |
|---|---|---|
| B1 | **Errors become plain records** (what kind, where, which literal values). Each side's formatter words them. Upstream wording never enters lite's plans | every pinned error message on every route unchanged, except a short named list |
| B2 | **Formatters replace SQL-built text** (about 2,000 lines deleted): legend-pure's printed tables and values, legend-engine's result envelopes | PCT's 423 printed-table tests and legend-engine's result goldens byte-identical. **Prototyped: 431 of 431 printed tables byte-identical on DuckDB, with no change to lite's core; 4 more H2 tests pass** |
| B3 | **The fallback and legend-pure-style checking move behind three narrow connections**, with four small rewrites of legend-engine's test code at the door | both corpus result lists byte-identical. **Prototype running now** |
| B4 | **Remove what is left**: the legacy H2 settings, the float-literal and unchecked-maths switches (per D2), DataCube's legend-engine workarounds moved to its legend-engine side | each removal's moved tests listed before it lands |
| B5 | **Delete the switch machinery itself and turn on the guards**: the build fails if a mode switch or upstream-specific code returns to the core | the counts of switches (49 parameters, 50 calls today) and of upstream code in the core (142 places today) both read zero |

**Deferred:** exporting plans to legend-engine and importing its plans. **Moved to the product track:** lite's own
rules copied across Python and the browser tab.

## 6. What is still open

**Two decisions for the user, each waiting on a count that is running now:**

- **D1. Where legend-engine departs from Pure's own definition, copy legend-engine or answer Pure's way?** (Rows 1 and
  6 above.) Answering Pure's way means no translator at all. Copying means a small typed rewrite at the door, which a
  prototype has shown can be built without touching lite's core.
- **D2. Keep the two shortcuts that copy legend-engine's number behaviour?** (Rows 4 and 5.) Removing them is cleaner;
  keeping them matches legend-engine in those cases.

**Two prototypes, about a day each, running now:** B2's table printer in Java, and B3's move of the fallback.

**One sequencing constraint:** the product track is fixing bugs in the same judge and number code. Its judge fixes land
before A4.

## 7. The safeguards

- Nothing merges without the user's word.
- A step that turns out to need a new switch in lite's core stops and comes to the user.
- Every difference from legend-engine or legend-pure that a step creates is listed for the user before it lands.
- The stop after stage A.
