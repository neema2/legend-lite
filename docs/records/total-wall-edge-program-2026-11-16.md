# The edge program: compatibility at the edge, a slim and fast product (2026-11-16)

**The single plan of record for streams D, E-Java and E-SQL.** The user asked for it on 2026-11-16 ("let's write it
down in detail so we all on same page and then execute against it"). It supersedes
`docs/records/total-wall-plan-2026-11-16.md`, which stays as the history of how we got here.

Where this plan conflicts with a worker's own plan (`d-plan`, `e-java-plan`, `e-sql-plan`), this plan wins.

## 0. How we use this document

- **Every worker re-reads §1 to §5 and its own phase before starting each item.**
- **Every correction or decision goes into §12 (the log), dated.** A chat message alone is not enough.
- **When a change affects work in progress, the orchestrator interrupts the worker.**
- **Where it lives:** branch `claude/total-wall-orch`, file `docs/records/total-wall-edge-program-2026-11-16.md`.
  Read it with:

  `git fetch origin claude/total-wall-orch && git show origin/claude/total-wall-orch:docs/records/total-wall-edge-program-2026-11-16.md`

## 1. The goal

In the user's words: **"everything compatibility-related to the edge"**, with **"product and test code being super slim
and fast, then compatibility at hopefully mostly at java edge on top of clean typed results on platform"**.

When the program is done, these are true:

1. **The product answers in lite's own typed forms.**
   - A Float's text is six decimals (S311).
   - A DateTime is ISO with `Z` (S312).
   - An error is its kind.
   - The SQL is lean, and the database does the work.
   - No legend-pure or legend-engine spelling is computed for a product answer, in SQL or in Java.
2. **A legend-engine client gets exactly upstream's bytes.** That covers pure/v1, PCT, the relational corpus and
   engine-style service tests.
   - The edge makes them, mostly by formatting typed results in Java.
   - Inside a query it uses exactly legend-engine's own SQL, and only where legend-engine computes inside the query.
3. **The test code is slim and fast.**
   - The judges compare typed values.
   - A failure's message is formatted in Java at the edge.
   - No canon or print text is built in SQL.
4. **The separation is mechanical.** Product code cannot reach the edge, by the build. Every deliberate difference
   from upstream is a register row the user ruled.

## 2. The design (the contract)

### 2.1 The product returns typed results only
- Every cell is a typed value with its Pure type:
  - a Float as a double;
  - a DateTime as an instant with its precision;
  - StrictDate, Decimal, String, Boolean, null;
  - lists and structures typed.
- Every error is a **kind plus its operands as typed values**, never a sentence.
- A run-time raise can return no rows, so its SQL raises a **structured marker** carrying the kind and its operands
  as data. The plan declares the kinds each statement can raise.

### 2.2 The edge is Java over typed results
- **One presenter.** A Java formatter family turns a typed value into legend-pure's or legend-engine's text:
  - the Float print;
  - dates with `+0000`;
  - `TDSNull`;
  - lists and instances;
  - JSON tokens.
- Every edge writer goes through it: pure/v1 JSON, Arrow, CSV, the TDS writer, the service-test runner.
- Error sentences come from the kind and its operands, in Java (`PureMessages`).
- The edge runs no SQL and reaches no database.

### 2.3 Edge lowering variants, only where upstream computes inside the query
- A short, LISTED set (kind B, §6), for example a Float's `toString` inside an expression.
- The edge's dialect writes exactly legend-engine's SQL for it, read from the pinned extensions.
- Each item is listed with its PCT moves before it lands.

### 2.4 Separation by the build
- Product targets cannot depend on the edge.
- Product tests run without the edge on the classpath.
- `CompatSpellingGuardTest` stays as a small check on literal spellings.

### 2.5 The checks (§5)
- **The safety net.** An edge golden net is recorded at a reference commit, and every push keeps its difference
  empty.
- **The product probe** pins the product's forms on every database.

## 3. The rulings that govern (verbatim)

### Product and edge
- (2026-11-15/16) "everything compatibility-related to the edge".
- (2026-11-16) "product and test code being super slim and fast, then compatibility at hopefully mostly at java edge
  on top of clean typed results on platform".
- (S311, 2026-11-10) "let's go back to 6 ... and use decimal where need". Reaffirmed 2026-11-16: "I thought for product
  we are printing formatted to 6 decimals across all backends".
- (2026-11-16) "Have we been wasting time trying to print floats in the database again? Didn't we agree to do all the
  compatibility in JAVA? And for toString inside an expression use engine cast( as varchar)".
  - **Settled the same day.** The product's Float text is S311's one simple call per database. An edge expression's
    Float `toString` is legend-engine's cast. legend-pure's print of a value is Java at the edge.
- (S312, 2026-11-10) "ISO 8601 in UTC, six fraction digits and `Z`".
- (S314, 2026-11-10) "`%s %d %f %t{}` in the product; `%r` and padding at the edge".
- (U5, 2026-11-15/16) "should not really have any exceptions"; "they probably are not needed, probably bad code remnants
  of compatibility that we should not have".
- (G6) "The database does the work. Small Java formatting, the appeals and the referee are allowed only at the edge."
- (Upstream fidelity, AGENTS.md) At the edge, exactly upstream. Its answer is read from the pinned sources or asked of
  the engine runner, never recalled.

### Lean SQL
- (2026-11-15) "the leanest fastest most expert SQL for the product ... subselects only when needed, repetition only
  when needed ... most simple and fast sql everywhere".
- Speed first among CORRECT forms. Same answers always. Pins shrink only.

### Process
- (2026-11-16) "don't stop until whole thing is done and working end to end".
- (2026-11-16) "we need fast dev loop too".
- (2026-11-16) Calibration: "over indexed on one at a time and running all tests all the time".
- (2026-11-16) CI: "only run Linux as a bespoke/throw away CI"; "do (a) the throwaway version".
- Nothing merges without the user's word.
- PRs don't edit shared docs; records go in `docs/records/`.
- Credentials are never read or printed.

## 4. What we will not do

1. **No SQL that reproduces upstream's printed text,** in the product, the judges or the edge.
2. **No exactness engineering in a product PRINT form.** One simple call per database, with differences at the
   extremes recorded as register rows.
   - (A value CARRIED inside the query is different: it must read back exactly, because same answers come first.)
3. **No session setting sent for a form's sake.**
4. **No probe stricter than the ruling.**
5. **No lane round on a commit that is about to change.**
6. **No orchestrator paraphrase without the user's words beside it.**
7. **No correction by chat message alone** (§0).
8. **No pin grows without a dated reason.** A transitional growth names the step that takes it back.
9. **No fallbacks** (G7).
10. **No new mechanism outside this plan without asking** (§5.3).
11. **No two writers on one branch.**

## 5. How we check

### 5.1 The checks by level

| When | What runs | Who | Cost |
|---|---|---|---|
| every edit | the edited package's tests | the worker | seconds to minutes |
| every push | the tests that depend on the changed files (Bazel's dependency query over //core, //pct, //spec, //datacube, //query-store, //engine-client, //warehouse, Python), plus `//core:guardrails` and `//core:census`, **plus the edge golden net** once it exists | the worker | 10 to 20 minutes |
| every finished piece | the **Linux-only throwaway CI** on d-stage (`.github/workflows/stage-linux.yml`, every lane on Linux, run on each push to d-stage) | automatic | about 45 minutes, behind the work |
| every merged phase | a **lane round** for the databases CI doesn't run (MariaDB, MySQL, ClickHouse, Trino, MongoDB, SQLite, Snowflake, Databricks, BigQuery), run by the six lane sessions | the orchestrator | hours, behind the work |
| big phases (§7) | one **independent audit** by two read-only reviewers | the orchestrator | behind the work |
| each PR | the full three-platform CI and one independent review | the orchestrator | once |

- A finding from the work running behind comes back as **one batch**. Only a regression stops the line.
- **The orchestrator reports to the user once per phase,** not finding by finding.

### 5.2 The safety nets
- **The edge golden net** (Phase 0). Every upstream-facing output on DuckDB, H2 and SQLite is recorded as golden
  files at PR1's frozen head `26717de291` (D's base):
  - PCT channel A outcomes and texts;
  - channel B outcomes;
  - both relational corpora;
  - pure/v1 responses.

  One local target diffs against them, normalized (trace ids masked, unordered rows sorted). After Phase 1, every
  push keeps the difference empty, except a ruled move, which re-records the golden with the ruling cited.
- **The product probe** (`//core:product_raise_probe`) runs on every database: typed raises, the Float and DateTime
  forms, carriers that read back.

### 5.3 A worker stops and asks the orchestrator before building or pushing, when
- a product form needs more than one simple call per database, or is slower than the form it replaces;
- anything would print upstream's text in SQL;
- it adds a mechanism this plan doesn't name: a plan field or format bump, a hook or facet, a session statement, a
  class family, a probe;
- the golden net, PCT, a corpus or channel B moves at the edge;
- a pin grows other than by a file moving, or a register row is added or changed;
- an item runs past twice its size;
- two rulings seem to conflict.

## 6. The classification table (the compatibility ledger)

Every compatibility difference is one of four kinds:
- **A**: how a value is shown;
- **B**: a computation inside the query;
- **C**: protocol and plan text;
- **D**: the judges.

The table below is the known list. Phase 0's drift list and the guard's remaining rows complete it; each new row
gets the same columns.

| # | Kind | Item | Upstream | Product (ruled) | Edge mechanism | State |
|---|---|---|---|---|---|---|
| 1 | A | a Float value's text (result cells, pure/v1) | legend-pure's print | S311 six decimals | the Java presenter | the presenter: Phase 3. SQL `FLOAT_PRINT` arms deleted after Phase 4 |
| 2 | A | a DateTime value's text | `+0000`, written precision | S312 ISO `Z` | presenter (`EngineCells`, `LEGEND_PURE`) | done (W4, W8); host Arrow route fixed `3f1695d65` |
| 3 | A | an empty TDS cell, a join with an empty element | `TDSNull` | empty, skipped (S313, S316) | presenter | done (D1.3) |
| 4 | A | legend-engine's JSON cell tokens and envelope | engine's writer | lite's writers | `edge.EngineCells`, `PureV1Writer` at the edge | done (W8) |
| 5 | A | error sentences | upstream's words | the kind, lite's sentence | `PureMessages`, Java | done for kinds (W5, W7); the query store's words fixed (`24c06a6d9`, `c34b3d5f5`) |
| 6 | A | an instance's `toString`, `format` `%r` and `%0Nd`, `toRepresentation` | legend-pure's print | refused by name (S314; `toRepresentation` §10 item 8) | presenter | instance done; `%r` and `toRepresentation`: Phase 3 |
| 7 | A | a pivot's generated column name | quoted | bare (S315) | edge writers | done |
| 8 | B | a Float's `toString` inside an expression | legend-engine's cast per database | S311 one call | edge lowering variant | Phase 5; PCT moves listed and ruled first |
| 9 | B | a raise sentence's operand (`PureMessages` in SQL) | upstream's sentence | the kind | **a structured raise marker; the sentence in Java** | Phase 2 |
| 10 | B | ClickHouse's product `format('%s', Float)` | — | S311 one call (it's toString) | — | Phase 3 (a product fix) |
| 11 | B | H2's quoting of reserved words, and the session's `NON_KEYWORDS` | legend-engine's lexicon | the same rule, **RULED a product row (§10 item 9, 2026-11-16)** | — | restored, `9c28a1a7e` (Phase 1) |
| 12 | B | DuckDB's engine scan order | legend-engine's order | none | `StableScanOrder` at the edge | done (W6) |
| 13 | C | generatePlan, `toSQLString`, a plan handle's nodes, an execute's activity SQL | legend-engine's text | none | `edge.EnginePlans`, `EngineText` | step 1 done (`d4bab2df6`); the rest: Phase 6 |
| 14 | C | the pure/v1 envelope, errorType, error type names | engine's | lite's | `PureV1Api` edge overload, `PureMessages.typeName` | done (`3f1695d65`) |
| 15 | D | engine-text asserts (golden `toSQLString`/plan text) | upstream's | — | the edge's arm behind one facade | Phase 4a (S2c) |
| 16 | D | a failed verdict's message (`%r`, `TDS_NULL_CELL`, `SideShapes`) | legend-pure's print | — | Java at the edge, the presenter | Phase 4b |
| 17 | D | canons that decide verdicts (`FLOAT_CANON`, `CanonicalRenderSql`, `VerdictSql.canonOf`) | legend-pure's text | — | typed comparisons in SQL (one tolerance constant) | Phase 4b |
| 18 | D | rendered-text goldens (`toString`/`toCSV` against a String) | legend-pure's text | — | the edge parses the golden into typed rows at planning | Phase 4b |
| 19 | carrier | a value carried as text inside a query (lists, JSON) | — | must read back exactly | none (product correctness) | SQLite fixed (E-SQL); the rest measured by the probe: Phase 8 |
| 20 | carrier | the shortest-text emulations (Snowflake's, BigQuery's, SQLite's and MariaDB's `floatText`; ClickHouse's regexp) | — | — | — | deleted once no judge or carrier reaches them: Phase 8 |

## 7. The work, by phase

**Owners:**
- **D**: `session_01XmdJ7kRamr4Sexmcrguc94`, branch `claude/total-wall-d-stage`, the only writer there.
- **E-Java**: `session_01XmdJ7mGZpJB7BmQRiqXNSR`, branch `claude/total-wall-d-judges`, cut from d-stage, judge files
  only. D merges it.
- **E-SQL**: `session_01XmdJ7jKZMVTrr2TX1aqSzN`, branches `claude/total-wall-edge-contract` (the net) and
  `claude/total-wall-e-sql-v3` (lean SQL).

**Parallel tracks:**
- Phase 0 (E-SQL), Phase 1 (D) and Phase 4a (E-Java) run at the same time.
- Then D does 2, 3, 5, 6 and 7 in order, while E-Java does 4b, and E-SQL does 8, then 10.

### Phase 0: the safety net (E-SQL; about one day)
- **Build** `//pct:edge_contract_test` (or a runner plus test) on `claude/total-wall-edge-contract`. It takes the
  outputs the tests already print and adds no new harness, recorded at `26717de291`:
  - PCT A on DuckDB, H2 and SQLite;
  - channel B on DuckDB;
  - both corpora;
  - pure/v1 if cheap.
- **Run it against d-stage**, through a throwaway local merge, and send the **drift list**: per difference, the test,
  before and after.
- **Done when** the target exists, the goldens are committed, and the drift list is delivered. D merges the branch
  into d-stage in Phase 1.

### Phase 1: the edge identical to upstream again (D)
- Fix the open audit items:
  - M4: Windows.java's sentence as a kind;
  - A-M1: restore H2's pre-W6 product quoting exactly, one guard row for the ruling;
  - S1 to S6;
  - S8: the edge receiver's refusals at planning;
  - A-S2 to A-S5;
  - the probe's `-0.0` and `NaN` cases;
  - the throwaway `stage-linux.yml`.
- Fix every item on Phase 0's drift list, except a listed ruled move.
- Merge the net into d-stage. From then on it runs on every push.
- **Done when:** the golden net is empty, Linux CI is green on d-stage, and one lane round is clean. Then an
  independent audit runs behind.

### Phase 2: typed raises with operands (D)
- Every raise that carries a value in its sentence raises a structured marker: kind plus operands as data.
- The edge (and the product) word it in Java.
- `PureMessages`' SQL-built sentences go.
- **Done when:** no sentence is built in SQL, the golden net is empty, and the probe passes.

### Phase 3: the Java presenter at the edge (D)
- One presenter family for kind A, used by every edge writer.
- The edge no longer asks SQL for a printed value.
- Refuse the product's `%r` and `toRepresentation` by name.
- ClickHouse's `format('%s')` becomes S311's one call.
- **Done when:** every edge print of a value is Java, and the golden net is empty.

### Phase 4: the judges typed (E-Java on `claude/total-wall-d-judges`; D merges each piece when green)
- **4a (S2c).** One public facade in `testplan`. The engine-text arm and `SqlTextVerdicts` move to the edge.
  `AssertVerdicts` ≤ 1,520 lines and `SqlTextVerdicts` ≤ 1,157. The `EngineTestText#arm` allowance goes.
- **4b (W10), approved with five conditions** (no verdict flips; message parity across both judges; shrink-only pins;
  one tolerance constant; deferred goldens counted):
  - messages in Java via the presenter;
  - canons as typed comparisons;
  - goldens parsed by the edge.
- Then D deletes the `FLOAT_PRINT` and `FLOAT_CANON` arms.
- **Done when:**
  - channel B is unchanged under HOST, DATABASE and BOTH;
  - the golden net is empty;
  - no canon or print text is built in SQL;
  - an independent audit runs behind.

### Phase 5: the in-query items as legend-engine's SQL (D)
- For each kind-B row:
  1. list the PCT moves on DuckDB, H2 and SQLite A/B;
  2. send them to the orchestrator, and to the user's batch;
  3. on the ruling, implement the move and re-record the goldens with the ruling cited.
- **Done when** every B row is built, and its moves are ruled and re-recorded.

### Phase 6: legend-engine's plan text at the edge (D)
- Finish W9's planner half: the remaining engine-text rows in the planner, `PlanAllocations`, `ProductStatements`,
  `RawSqlBoundary` and `PlanNode`'s trace id.
- **Done when:** the planner names no upstream text, and the golden net is empty.

### Phase 7: separation by the build, and the guard to zero (D)
- Product targets have no edge dependency, and product tests run without the edge.
- Every remaining `CompatSpellingGuardTest` row is reviewed. The default is to move it or delete it; a survivor goes
  to the user with evidence.
- **Done when:** the guard is at zero or holds only user-ruled rows, and the build enforces the separation.

### Phase 8: exact carriers and the emulations deleted (E-SQL)
- Every text carrier must read back exactly, measured by the probe on every database. On a database whose cast
  doesn't read back, the carrier carries the typed value.
- The shortest-text emulations (row 20) are deleted once unreached.
- **Done when:** the probe's carrier case passes on every database, and the emulations are gone.

### Phase 9: landing
1. An independent review of the whole of D.
2. One lane round.
3. Remove `stage-linux.yml`.
4. PRs: PR1 first (`claude/total-wall-e`, frozen at `26717de291`), then D's stacked PRs by phase.
5. The full three-platform CI on each.
6. The user's merge word for each.

### Phase 10: after D (each its own PR, review and CI)
- E-SQL's lean product SQL, E-v2 to E-v5. E-v2 closed at `20cc9df83`, E-v3 is in progress.
- E-Java's v2 (lean and readable judges, item 8 parked on `-e-v3`) and v3 (the Java-evaluation ledger shrunk).

## 8. Where we are (2026-11-16, about 15:00)

| Stream | Branch and head | State |
|---|---|---|
| D | `claude/total-wall-d-stage` @ `c34b3d5f5` | Done: the Float revert (`4ff4f86e7`, lane round 66 clean on 12 databases), W9 planner step 1, audit fixes B-M1, B-M2, M3, A-M2, A-M3, A-M4, A-S1, S7, and stage (a)'s reds. Next: Phase 1's remaining audit items and `stage-linux.yml`. |
| E-Java | `claude/total-wall-d-judges` (to cut) | Phase 4a (S2c's facade). v2 parked on `claude/total-wall-e-v3`: statements met (≤ 1.6 per test); item 8 in progress. |
| E-SQL | `claude/total-wall-edge-contract` (new); `claude/total-wall-e-sql-v3` @ `1bc91dd5c` plus the SQLite carrier fix | Phase 0 (the net) first. |
| PR1 | `claude/total-wall-e` @ `26717de291` (frozen) | Lands first in Phase 9. |
| CI | draft PR #112 closed | Linux-only throwaway CI from D's next push. |

## 9. Parked (outside this plan)
- The cross-language fix batch (resumes after D lands).
- PR #102 (waiting on its author).
- Deephaven.
- D3 (engine plans in and out).
- The Decimal print form.
- The layered edge.
- The date-format fidelity PR.
- Lineage rebuilt upstream's way.

## 10. Rulings for the user

**RULED by the user, 2026-11-16, about 15:10 ("I agree with all", on the orchestrator's recommendations):**

1. **E's rows S341, R3 and R4: refusals by name for now.** They replaced wrong answers, and the "kinds kept" carrier
   (Phase 10) is to replace them with legend-pure's answers. The refusals:
   - `type()` of a value whose kind is known only at run time;
   - a mixed list's extreme where elements of different kinds tie;
   - a computed Integer beyond 2^53 in a mixed list's order.

   R5 and R6 are internal, with no user-visible answer. E1's refusals were ruled on 2026-11-10.
2. **An unused derived column that can raise:** lite prunes it and answers the rows. It becomes a product register
   row. The edge follows legend-engine once its answer has been read from the engine runner.
3. **The CSV writer's Float cell keeps the exact value (`3.0`).** A CSV is data. S311's six decimals govern
   `toString`, not a carried value.
4. **A coarsely written DateTime prints S312's one form** (six digits and `Z`). The edge keeps legend-pure's written
   precision.
5. **A value finer than a microsecond: its text is cut to six digits.** The value itself is carried exactly.
6. *(rule later)* The B-row PCT moves (Phase 5), listed when measured.
7. *(with the user, after an explanation)* H2 `toOne` over a list, refused by name today. Options:
   - (a) refuse, as today;
   - (b) give H2 a list encoding;
   - (c) `toOne` by counting, no list: lean on every database.

   The recommendation is (a) now plus (c) in E-SQL's lean SQL.
8. *(with the user, after an explanation)* The product's `toRepresentation` and `format('%r')`. Options:
   - (a) refuse by name (S314 extended);
   - (b) a simple product form;
   - (c) keep legend-pure's text (against the design).

   The recommendation is (a).
9. **H2's quoting rule and `NON_KEYWORDS` stay in the product as a ruled row.** They decide which column a declared
   name reads in H2 data made legend-engine's way. Restored by D, `9c28a1a7e`.
10. *(rule later)* Guard survivors (Phase 7) and deferred goldens (Phase 4b).

## 11. Timeline (rough, working hours)

| Phase | Hours | Notes |
|---|---|---|
| 0 | 8 | in parallel with Phase 1 |
| 1 | 6 to 10 | in parallel with Phase 0 |
| 2 | 6 | |
| 3 | 8 | |
| 4a | 16 | in parallel with D's phases |
| 4b | 20 | in parallel with D's phases |
| 5 | 4, plus the ruling | |
| 6 | 10 | |
| 7 | 8 | |
| 8 | 6 | |
| 9 | 8, plus CI | |

About four working days to D's landing, with the three workers in parallel.

## 12. Log

| When (UTC) | Entry |
|---|---|
| 2026-11-16 15:15 | **The user ruled §10 items 1-5 and 9** ("I agree with all"). Items 7 and 8 were explained, and await the user's choice. D's Phase 1: A-M1 restored (`9c28a1a7e`, H2's text byte-identical to before W6, one guard row); `stage-linux.yml` committed (`2ed80afe6`); `//core:lean_sql_baseline_test`'s plan-JSON growth approved for one re-pin, on condition that one case confirms it is the per-statement raises field (SQL characters only shrank). |
| 2026-11-16 15:00 | Written, at the user's word. It supersedes `total-wall-plan-2026-11-16.md`, which holds the day's earlier log. |
