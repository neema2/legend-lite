# The IQ scale program: any Sybase IQ query, any size, on DuckDB and Snowflake (2026-10-22)

**The user's ask (2026-10-22):** scale the `#IQ{...}#` island with more features, fixes and complex queries, focused
on DuckDB and Snowflake. The goal is a converter that is "super bulletproof" for any Sybase IQ query of any complexity
and scale, run as a loop that keeps adding features and finding and fixing bugs until everything that makes sense
works.

This continues the tenth line (`docs/IN_FLIGHT.md`, "A tenth line"; the study `docs/IQ_SQL_STUDY_2026_10_13.md`; the
census `docs/IQ_CENSUS_CHECKLIST.md`). Everything those documents rule still holds. This file adds three things: the
targets the user named, the loop, and the queue the loop works from.

---

## 0. What "bulletproof" means here

For every IQ query handed to the island, on **DuckDB** and on **Snowflake**, exactly one of these holds:

1. **IQ's answer**: the same rows, types and order that SAP IQ 17.1 returns;
2. **IQ's error**: the error IQ itself raises, by its code;
3. **a refusal by name**: lite says, with the position, which construct it cannot answer exactly.

A wrong answer, a crash, or a database error that IQ would not raise is a bug. The refusal rate is the backlog, and
the loop's job is to shrink it. The line's own rule stays: zero FAIL, zero BUG, and Java never computes a value.

"Any scale" means the census's budgets (`IQ_CENSUS_CHECKLIST.md`, "Scale") hold on both targets: hundreds of joins,
16 MB of SQL text, 16,000 projected expressions, a 20,000-value IN list, 1,000-deep expressions and 1,000-branch
UNIONs. Where a target cannot hold a shape, lite either reshapes its SQL so the target can, or refuses it by name. It
never sends SQL that is known to fail.

## 1. Where the line stands (main at bff1c703, 2026-10-22)

- **Five landings merged**, the last being PR #63, landing 5. 32,000 lines of translator: `protocol/sql/iq/` plus the
  shared `SqlToPure`.
- **Parity:** 4,100 cases, which are 8,994 runs across the profiles, judged against snapped answers from SQL
  Anywhere 17 and the live IQ 17.1. On DuckDB: 5,984 PASS, 1,509 BOTH_ERROR, 1,378 REFUSED, 78 SKIP, and 42 runs that
  are listed iq17 divergences (19 FAIL, 15 ERROR, 8 BUG). Postgres is the same within one run.
- **Corpora**, judged against the live IQ:
  - the SE suite: 10,763 PASS, 1,526 PASS_UNORDERED and 366 FAIL, all of them ties, clocks or D-N3;
  - TPC-H: 9,131 of 9,427;
  - TPC-DS: 8,403 of 8,647;
  - JOB: 4,554 of 4,811;
  - BIRD: 1,047 PASS and 389 matching errors.
- **Snowflake: rendered, never run.** Only the showcase's SQL is pinned for Snowflake (`showcase/snowflake.sql`, render
  only). No IQ case has ever executed on a Snowflake. One known wrong answer is already recorded: Snowflake's
  `INT_DIVIDE` arm rounds `a / b` to six places before it truncates (checklist, landing 5's review).

## 2. The gaps between the line and the ask

| # | Gap | Why it matters |
|---|---|---|
| G1 | **No Snowflake execution lane** | Half the targets the user named are untested. Every dialect difference (collation, blank padding, NULL order, decimal scale on division, timestamp precision, integer division, string-to-number reads, identifier case) is unmeasured there. |
| G2 | **The oracle is only reachable from the user's Mac** | The live IQ's password is in the macOS Keychain, and SQL Anywhere 17 is installed on the Mac. A cloud loop cannot snap a new case's answer, so new IQ semantics cannot be judged from the cloud. |
| G3 | **The refusals** | 1,378 parity runs and the corpora's refused queries. These are the feature backlog. |
| G4 | **Scale on Snowflake** | Every scale budget was measured on DuckDB only. Snowflake's limits on statement size, expression depth, IN lists and compile time are unmeasured. |
| G5 | **Statements** | The island reads one SELECT. Real IQ workloads arrive as scripts: `SELECT ... INTO #t`, `DECLARE`, `IF`, temp tables, batches. The study's M4 left them out by decision. |

## 3. The pillars, in order

### P1. Snowflake as a first-class target (no oracle needed)

Every committed answer was snapped from IQ or SQL Anywhere, so running the existing 8,994 runs on Snowflake judges
Snowflake against IQ with no new snapping.

- **P1.1 The lane.** Add `//core:iq_parity_snowflake_test`, modeled on `IqParityPostgresTest`:
  - it runs over `SnowflakeBackend`, seeds into a schema of its own, and runs cases in parallel;
  - it is manual, since it needs credentials, as the PCT cloud lanes are;
  - `ledger.tsv` gains a `snowflake` column, and the divergence list and its guards apply as on the other lanes.
- **P1.2 The triage.** Each run whose status differs from DuckDB's is classified as one of:
  - a Snowflake dialect bug: fix it in `sql/dialect/Snowflake.java` or a Snowflake rule class;
  - a semantic difference that needs Snowflake's own spelling of IQ's rule;
  - a construct Snowflake cannot express exactly: refuse it there by name. Snowflake's per-target refusals are
    named per target, so DuckDB keeps answering.

  Each fix is pinned by the run that found it.
- **P1.3 The known bug first:** an exact `INT_DIVIDE` on Snowflake, `(a - MOD(a, b)) / b`. After it, the Postgres front
  end's estimate can go too (checklist, landing 5's review).
- **P1.4 Snowflake's scale:** the census budgets measured on Snowflake (§0), with each limit either worked around in
  the SQL or refused by name.
- **P1.5 The showcase and the canonical benchmarks** (TPC-H's 22 and TPC-DS's 99, as rewritten for IQ) executed on
  Snowflake. Snowflake's answers are compared with DuckDB's, which IQ judged (P2).

### P2. Oracle-free bug hunting at scale

The loop needs bugs found faster than a person can snap answers. Three judges need no IQ:

- **P2.1 DuckDB against Snowflake.** The same island runs on both targets. DuckDB's answers are IQ-judged on the
  parity set and the corpora, so a disagreement is a bug, usually Snowflake's.
- **P2.2 Metamorphic checks**, as SQLancer runs them, on both targets:
  - TLP: `WHERE p`, `WHERE NOT p` and `WHERE p IS NULL` partition the rows, and the same holds for HAVING, DISTINCT
    and aggregates;
  - NoREC: a filter must count the same rows as the sum of its CASE;
  - other equivalences: `GROUP BY` against `DISTINCT`, a join against the same join written as a subquery, and a
    derived table against its CTE.

  These find translator bugs without knowing IQ's answer.
- **P2.3 A generator in the repository** (`tools/iq-fuzz/`, our own code): random IQ queries over the parity schema,
  every one ordered by a unique key, with depth and width as knobs so that it also produces scale. The private fuzzer
  stays private. This one is written fresh from the grammar the island reads.

### P3. Features: refusals turned into answers (oracle needed for new semantics)

These are ranked by the corpora's counts (checklist) and taken top-down:

1. WITH RECURSIVE where IQ answers it, lowered to `meta::legend::lite::recurse`, which is now on main. Checklist 1.8
   was waiting only for it.
2. Dropping the derived-table columns no one reads before translating them, as IQ does (FORMAT, STRING_AGG and
   errors in unread columns). This is the checklist's "open: dropping any unread column".
3. IQ's error order in general: each held error carries its phase, and the earliest phase's error is raised.
4. PIVOT: IQ 17.1 reads it in both engines.
5. A DOUBLE written as text (`%#.18g`): DuckDB's printf, and Snowflake's `TO_CHAR` if a format can spell it,
   otherwise refused per target.
6. TPC-H Q8 and Q14: the long division's intermediate values computed once. This needs the Lowerer to keep a SELECT
   level for a complex column that is read more than once (checklist, "Measured, not built").
7. The fuzz round 8 findings: cross-kind comparisons beside NULL, the typing of COALESCE with a NUMERIC constant, and
   date-string codes.
8. STDDEV and VARIANCE with IQ's own formula, if its arithmetic can be composed exactly; otherwise they stay refused.
9. Decisions the user owns, which stay refused until decided: ODBC escapes, D-N3's widening past a BIGINT, and the
   38-digit ceiling (decided: refuse).

### P4. Scale and robustness

- UNION ALL and join chains past 1,000 hit DuckDB's parser depth. Emit balanced trees, or flat lists where the shape
  allows, so depth grows as log n.
- Each P1.4 limit gets a regression case that sits at the limit.
- Plan time, memory and SQL size are budgeted per shape in a harness test, so a slow quadratic path turns a test red
  rather than a user's query.

### P5. Statements and scripts (the user's decision first)

This would be a statement front end above the island: `SELECT ... INTO #temp` and temp tables, `DECLARE`/`SET`
variables, `IF`/`BEGIN...END`, batches, `CREATE TABLE AS` and `INSERT ... SELECT`. Each would be translated to a
sequence of target statements on DuckDB and Snowflake. The study's M4 and the island's "one SELECT" rule put this out
of scope, so it waits for the user to say yes.

## 4. The oracle (G2): the decision this program needs

New IQ semantics need IQ's answer. There are three ways to get it:

| Option | What it takes | What the loop can do |
|---|---|---|
| **A. The Mac relays** (recommended) | The user keeps a Claude Code session open on the Mac, connected by Remote Control. The SAP SQL Anywhere session (`session_01WGRGLrXnieo9JV8Ff8Pacb`) has the tools and the Keychain. The cloud loop pushes cases to the branch, then asks that session to `snap.sh --write` them and push the answers back. | Everything. No credential leaves the Mac. |
| **B. The oracle in the cloud** | Add the live IQ's host and password as environment secrets, allow the host in the environment's network policy and the data lake's allowlist, and install a Linux SQL Anywhere 17 client (dbcapi). | Everything, unattended. It changes the line's credential posture, which so far has been "never written anywhere". |
| **C. No oracle** | Nothing. | P1, P2 and P4 in full. P3 only where IQ's answer is already committed, or the construct is refused by name. |

Until the user picks one, the loop runs under C.

## 5. The loop

Each iteration is one slice:

1. **Sync.** Fetch `origin/main` and merge it if it moved. Read `IN_FLIGHT` for new claims on the files the slice
   will touch.
2. **Pick.** Choose, in order:
   - a red lane or a broken guard;
   - the top open item of §6's queue;
   - the top new finding of the last differential or fuzz run.
3. **Build** the change with its unit test and its parity cases. A case whose answer is not snapped waits for the
   oracle, marked in the queue.
4. **Judge.** Run `//core:iq_parity_test` (DuckDB), `//core:iq_parity_postgres_test`, `//core:iq_syntax_cases_test`,
   `//core:guardrails` and `//core:core_tests`. A `SqlToPure` edit also runs `//core:sql_parity_test` and
   `//core:sql_parity_postgres_test`. Under P1, also run the Snowflake lane, over the touched case files at least. A Lowerer or
   dialect edit also runs `//spec:corpus_duckdb` and `//spec:corpus_h2`.
5. **Record:**
   - the ledger columns;
   - the checklist's status for the item, or §6 here;
   - a line in §7's log;
   - a `SEMANTICS_REGISTER` row for any new deliberate difference.
6. **Commit and push** to `claude/iq-scale`, one slice per commit, naming its item.
7. **Land:** every five slices or so, or at the end of a day:
   - `bazel test //gates:local`;
   - an independent review by a fresh agent given the diff;
   - a PR, merged by the user.

   Cross-area edits (`sql/dialect/`, `lowering/`, `compiler/`) are announced in `IN_FLIGHT` before the first edit.

The loop does not stop at an empty queue: the lanes, the differential and the fuzzer refill it. It stops when the user
says so.

## 6. The queue

| # | Item | Pillar | Needs | Status |
|---|---|---|---|---|
| Q1 | Bazel in this container; DuckDB and Postgres IQ lanes green as the baseline | — | — | done |
| Q2 | Snowflake's exact `INT_DIVIDE` | P1.3 | — | done, measured on an account by Q3 |
| Q3 | `//core:iq_parity_snowflake_test` and the ledger's `snowflake` column | P1.1 | Snowflake credentials | done: the column filled from the run at b26592b3 (5955b476); 5,982 runs PASS on both |
| Q3b | The Snowflake plan census: every run DuckDB plans is planned for Snowflake, render only | P1.1 | — | done: 6,236 of 6,236 |
| Q4 | First Snowflake triage: every status that differs from DuckDB's | P1.2 | Q3 | done: 282 runs differed, 18 now (5 iq17 PASS where DuckDB raises, 3 refused by name, 10 diverging: 3 listed iq17, 7 SQL Anywhere judges, a decision) |
| Q5 | The DuckDB-against-Snowflake differential runner and TLP/NoREC checks | P2.1, P2.2 | Q3 | TLP done on DuckDB (Q6); the differential built for Snowflake (`//core:iq_fuzz_snowflake_test`, its first run waits on the user) and for the embedded Postgres (`//core:iq_fuzz_postgres_test`: seeds 1 and 2, 2,000 shapes, no finding) |
| Q6 | The generator: `IqFuzz` and `//core:iq_fuzz_test` | P2.3 | — | done; the rounds feed the queue (seeds 1-3 clean after slice 3) |
| Q21 | DATEADD of a computed timestamp of no known year under the iq17 profiles: refused for SQL Anywhere 17's defect past the year 7910 (531 of round 3's refusals); whether IQ 17.1 shares it decides the iq17 reading | P3 | oracle | open |
| Q14 | A stored REAL or FLOAT column read: compared, computed, shown | P3 | oracle | open. Measured: under IQ 17.1's engine `rl = 0.1` in a WHERE holds for a stored REAL (the constant compared in single precision), while SQL Anywhere 17 compares as doubles, and a computed REAL compares as a double under both (`reals_compared_as_doubles`) |
| Q15 | The corpus coverage lane rebuilt in the cloud (SEDE, SQLStorm and Public BI fetched; BIRD needs disk) | P3 | the user's approval to run the research repo's scripts | open |
| Q16 | Very wide joins on DuckDB: per statement, `SET disabled_optimizers = 'join_order'` past ~64 joined relations | P4 | an exec-seam hook (another line's) | open: a grouping in the lowering (2026-10-23) ran 350 joins in 12.6 s (were 210 s) and was reverted on an independent review's findings (its SELECT * read the tables' undeclared columns; see the log). Measured on DuckDB 1.4.4, plain SQL, a chain of self-joins over 11 rows: 100 joins 0.6 s, 200 joins 19 s, 350 joins 181 s; with the join-order optimizer off 0.15 s, 0.5 s, and 700 joins in 2.8 s; in groups of 32 reading only the columns used past them, 200 joins in 0.12 s |
| Q17 | DuckDB's own depth limits (`max_expression_depth` 1,000, the planner's tree depth 1,000): UNION ALL past ~1,000 branches, 1,000 nested parentheses or CASEs, 700+ joins. Raising the setting is unsafe on small-stack callers (DuckDB's parser recurses natively); balanced UNION ALL emission is the way | P4 | — | open |
| Q18 | `SqlToPure` and `IqParser` recursion: stack use per nesting level, for callers other than the server (the browser planner) | P4 | — | measured (slice 4's log, `IqFuzzTest.theDefaultStacksReach`); making the layers iterative is open, below the correctness items |
| Q19 | Core's DuckDB 1.4.4 → 1.5.x: 1.4.4 loses rows from a UNION ALL nested in a UNION ALL under parallel execution (slice 3's log). Latent for the islands: lite's lowering writes a nested UNION ALL as one flat union, and the fuzzer's nested-union partition check (3,000 shapes) found no loss. Another shape that nests a union inside a union would meet it | P1 | the build line's DuckDB bump, the user's call | proposed |
| Q20 | The fuzzer's set operations: nested and parenthesized UNION, UNION ALL, INTERSECT, EXCEPT, so that Q19's class is reached by the generator | P2 | — | done: nested set operations of every kind, and the set identities (DISTINCT, UNION, INTERSECT/EXCEPT [ALL]) over every shape |
| Q24 | Snowflake's correlated subqueries: a correlation two derived tables down, a scalar subquery of no aggregate, and lite's NOT EXISTS forms of NOT IN and ALL are "Unsupported subquery type" there (59 runs), and a COUNT read through the nesting is NULL for no row | P1.2 | the sibling's measurements | done (2026-10-23): every Snowflake shape of the lane answers or is refused by name; the last, a CTE read in a scalar subquery, by the one-group ANY_VALUE (f5186929) |
| Q25 | IQ 16's BIGINT overflow (-158) on Snowflake, whose NUMBER(38) does not overflow (5 runs BUG). The island checks no BIGINT arithmetic itself; under iq17, where IQ answers past 64 bits, DuckDB's overflow is the lane's ERROR and Snowflake passes | P1.2, P3 | a decision (with Q26's): a run SQL Anywhere judges whose only difference is an error Snowflake does not raise | open: the check of 38df46ba reverted (2f40358c), its 3^n growth timed out on Snowflake |
| Q26 | TOP and START AT compute the items of rows they skip (IQ raises for them); Snowflake evaluates the select list after the limit (3 runs BUG) | P1.2 | the sibling's measurements | open |
| Q27 | Nested integer division by a column grows exponentially: `it / sm / sm` doubles a level twice on DuckDB (the translator's NULL guard repeats the inner quotient, unmerged where IQ raises for a zero divisor; the zero guard's message copies the dividend) and six times on Snowflake (its exact quotient writes the dividend twice). Needs a common-subexpression hoist (the operand as a computed column) or a single-copy exact quotient for Snowflake (`TRUNC(CAST(a AS NUMBER(38, 19)) / b)`, by its division scale rule, to be measured) | P4, P1.3 | the sibling's measurement for the Snowflake half | done: the Snowflake half (2026-10-23, the scale-19 quotient, measured); DuckDB's (2026-10-24, the translator's operand hoist, under IQ's engine once Q30 measured it: eight divisions 7.9 KB, sixteen 17.1 KB) |
| Q28 | Planning a long UNION ALL grows faster than linearly: 1.9 s at 1,000 branches, 4.0 s at 2,000, 16.6 s at 4,000 (the scale probe at twice its sizes, 2026-10-23) | P4 | a profile | done (2026-10-23): 4,000 branches planned in 1.4 s |
| Q30 | Whether IQ computes an operand beside a NULL one (`NULL + x / 0`: IQ's divide-by-zero, or NULL): the translator's merged NULL guard skips a raising operand in a strict operation's body where another operand is NULL (Postgres raises there), and an early hoist that computed such operands for every row changed an item's answer with its size (reverted) | P3, P4 | the live IQ | done (2026-10-24): measured on IQ 17.1 and SQL Anywhere 17 (`lite/null_beside_a_raise`); IQ's engine computes every operand, SQL Anywhere's front end reads from the left to the first NULL, and the translator follows each. The second batch (2026-10-24, `lite/null_beside_a_raise_read_where`): a comparison's operands and a strict function's arguments under IQ's engine, the front end over tables, IQ's own front end by GREATER. Open: the other functions IQ computes an argument of beside a NULL one (measured for LEFT; the catalog's strict functions of two arguments follow it) |
| Q29 | Planning a long join chain: `Fold.sourceColumn` scanned a join's every output at each level of its recursion, for a pivot identity's check | P4 | a profile | done (2026-10-23): 700 joins planned in 4.0 s (were 9.7 s); 1,200 in 8.7 s, the rest spread over the translator's taken names and the typer, past DuckDB's depth limit anyway |
| Q7 | Snowflake's scale limits, measured and handled | P1.4, P4 | Q3 | the lane built (`//core:iq_scale_snowflake_test`, 2026-10-23); its run waits on the user's approval of warehouse time |
| Q8 | WITH RECURSIVE through `recurse` | P3.1 | oracle | open |
| Q9 | Unread derived columns dropped | P3.2 | oracle (some answers are committed) | open |
| Q10 | Balanced UNION and join emission past 1,000 | P4 | — | the union half done (`BalancedUnions`, 2026-10-23): a UNION ALL of 4,000 runs on DuckDB; joins open |
| Q11 | The error order's general form | P3.3 | oracle | open |
| Q12 | PIVOT | P3.4 | oracle | open |
| Q13 | Q8/Q14's division: a SELECT level for a reread complex column | P3.6 | — (judged by committed TPC-H answers off-repo) | open |

## 7. Log

- 2026-10-22: the program written; branch `claude/iq-scale` from main bff1c703.
- 2026-10-22, slice 1:
  - **Baseline.** Bazel runs in the cloud container; the DuckDB and Postgres IQ lanes are green (67 s, 55 s).
  - **Snowflake's `INT_DIVIDE` is exact for two integers:** `(a - MOD(a, b)) / b`.
  - **The Snowflake lane** (`IqParitySnowflakeTest`, manual) is built, with the ledger's `snowflake` lane. Its first
    run waits for the account's credentials. Fetching them from AWS Secrets Manager into the session needs the user's
    approval.
  - **The plan census:** every IQ parity run is planned for Snowflake, render only. Snowflake refused 27 runs DuckDB
    plans: the IQ island's month-name pattern, `dec(?:e(?:m...)?)?`, whose replacement reads numbered groups.
    Snowflake's dialect now makes a literal pattern's non-capturing groups plain and renumbers the groups that a
    replacement or an extraction reads. A reference past `\9` is refused by name. The census is now 6,236 of 6,236
    planned.
  - **The fuzzer** (`IqFuzz`, `//core:iq_fuzz_test`):
    - Its first round refused 611 of 1,500 queries for a REAL column of table `r` that no query read. Now a REAL or
      FLOAT column no one reads, and no NATURAL or USING join compares, is never read. This fixed
      `m1e/owner_qualified_real_table_is_refused` on both profiles (REFUSED → PASS against the committed answers).
    - Its one TLP finding was IQ's measured self-comparison (`x >= x IS UNKNOWN` is two-valued under its engine),
      which the generator now avoids.
- 2026-10-22, slice 2, scale:
  - **The scale probe** (`IqFuzzTest.theCensusShapesPlanAndRunAtScale`) found StackOverflowError at the JDK's
    default 1 MB stack. The cases: a 500-branch UNION ALL, 350 joins, 500 nested parentheses, 1,000 nested
    functions or CASEs, an AND of 5,000. Every layer recursed: `IqParser`, `IqAliases`, `IqDialect`, `SqlToPure`,
    the typer and the lowerer. The census's "done" sizes had been measured on a 512 MB stack (the private measurement
    runner).
  - **The server now runs its requests on one thread with a 512 MB stack** (`LegendHttpServer.REQUEST_STACK_BYTES`).
    Requests are still one at a time, as the JDK's dispatcher ran them, and the stack is reserved, not committed.
  - **On that stack, the probe has no crash:**
    - UNION ALL of 500, 350 joins, 1,000 nested functions, a CASE of 1,000 WHENs, an IN list of 60,000, 16,000
      select items, an AND of 16,000, 200 nested derived tables and a chain of 200 CTEs all plan and run.
    - What fails is DuckDB's own limits (Q17) and its join-order optimizer's time (Q16): 350 joins plan in lite in
      3.2 s, then run in DuckDB in about 170 s.
- 2026-10-22, slice 3, fuzz round 2 (seed 2, 3,000 shapes, depth 4: 15,000 queries, 2,140 partitioned):
  - **A crash, fixed in `SqlToPure.assemble`:** `TOP 5 CAST((SELECT MAX(...) ...) AS VARCHAR(12)) AS c3 ... ORDER
    BY 1` failed to compile ("relation has no column '_sql_1'"). The subquery's column, computed once, came after the
    limit, while the ORDER BY key computed from the item read it before the sort. A sort key computed from an item's
    value now carries the item's columns computed once before it, and each is computed once.
  - **A TLP mismatch that is IQ's own:** a NULL-rejecting predicate outside a derived table makes IQ read the LEFT
    JOIN inside it as inner before a window there numbers its rows (measured, `IqDialect`). The generator's windows
    now read inner joins only.
  - **The fuzzer's classification:** a database's Binder, Parser or Catalog error wrapped as a DataError is now a
    DBERROR, or a DBLIMIT for a depth limit, not a named refusal. 13 Binder Errors had been counted as refusals; the
    next round names them.
  - **The generator** writes fewer shapes IQ rejects: no subquery inside a conditional or an expression (a
    correlated scalar subquery is a select item of its own), no comma join before an ON, no untyped NULL in
    arithmetic, DATEADD by days.
  - **Round 3** (seed 3, 15,000 queries, 2,776 partitioned, refusals 4,001 → 952): 24 DBERRORs, all DuckDB's
    "Aggregate with only constant parameters has to be bound in the root subquery". Fixed in `Lowerer.filter`: a
    filter over a grouped select whose HAVING would carry an aggregate that reads no column (`MIN(-1)`, `COUNT(*)`)
    into a subquery filters the grouped rows from outside. Any Pure `groupBy` then `filter` with a correlated
    subquery reached it. Postgres would bind such an aggregate to the subquery's own rows.
  - **Found and fixed:** slice 1's `IqDialect.reading` passed the 250-line method limit (`//core:guardrails`, which
    the loop had not run). The NATURAL/USING scan is a method of its own, and `//core:guardrails` is now in the
    loop's judge set.
  - **Found, another line's (Q19): DuckDB 1.4.4 loses rows from a UNION ALL nested in a UNION ALL under parallel
    execution.** `SELECT id FROM t WHERE id = 1 UNION ALL (SELECT id FROM t WHERE id = 2 UNION ALL SELECT id FROM t
    WHERE id = 3)` returns 2 rows with 4 threads, every time, and 3 with 1 thread. Wrapping the inner union in a
    subquery doesn't help. DuckDB 1.5.2 answers 3, every time. Core pins `duckdb_jdbc` 1.4.4.0, and the warehouse
    already runs 1.5.5.1.
- 2026-10-23, slice 4, the Snowflake lane and fuzz round 4:
  - **The Snowflake lane ran** (the sibling session, on `claude/iq-scale`): 8,991 runs. Against the DuckDB column:
    5,719 PASS, 1,501 BOTH_ERROR, 1,376 REFUSED and 78 SKIP alike; 282 differ (208 PASS -> FAIL, 59 PASS -> ERROR,
    8 BOTH_ERROR -> BUG, 5 ERROR -> PASS, 2 FAIL -> ERROR). Its first try failed in seeding: the INSERT named the table
    bare, which Snowflake folds to T (fixed, `dialect.storedName`).
  - **Most of the 208 were the seed's zone:** the seed's connection never ran the dialect's session setup, so each
    bound TIMESTAMP was read in the account's zone (America/Los_Angeles) and stored 8 h early. The seed now runs
    `sessionSetup()` first, as every query's session does.
  - **Fixed in Snowflake's dialect:** ASCII_CODE is `unicode()` (Snowflake's `ascii` is the first UTF-8 byte: 'é'
    195); a decimal literal no NUMBER holds (the island's single-precision bounds) is the DOUBLE DuckDB reads it as; a
    percentile's level is written as a bare constant.
  - **Open:** Q24 (correlated subqueries), Q25 (BIGINT overflow), Q26 (TOP's skipped rows), and NUMBER(*)'s
    `ROW_NUMBER() OVER ()` (Snowflake needs an ORDER BY).
  - **Fuzz round 4** (seed 4, 2,000 shapes, depth 4, with set operations and set identities): no crash, no TLP
    finding. Its 6 set-identity findings were the checker's: IQ's collation makes 'a' and 'a ' one row, and the
    checker compared text. It compares collation keys now. Its 32 "no column" errors were the generator's: a
    predicate drawn after `complex()` reused a correlation name (IQ's -143, rightly).
  - **Measured on the way:** INTERSECT and EXCEPT are IQ's -1001030 under its engine, refused rightly; INTERSECT ALL
    and EXCEPT ALL are "not supported yet" in the shared translator (front-end queries only, so low in the queue).
  - **Q18, the default stack's reach** (`theDefaultStacksReach`: plan only, for DuckDB, on a 1 MB stack, the JDK's
    default; the server plans on 512 MB). The largest size of a doubling series that plans, and the layer that
    overflows at the next:
    - UNION ALL of 256 (the typer, Pure's `concatenate` chain);
    - 512 joins (the typer);
    - 128 nested parentheses (the SQL renderer);
    - 256 nested functions or CASEs (`IqParser`);
    - a CASE of 512 WHENs (the typer);
    - an AND or OR of 2,048, and a `||` of 1,024 (`IqAggregates.shape`);
    - 128 nested derived tables or CTEs (the typer);
    - an IN list of 131,072 and 32,768 select items, the series' ends.

    These limits bind any caller that is not the server: the browser's planner (TeaVM, `//wasm`) above all.
  - **The flattening pass** (`ScalarSubqueriesFlattened`, Snowflake): a scalar subquery's chain of one-column derived
    tables is its innermost select, so `(SELECT COUNT(*) ... WHERE u.t_id = t.id)` reaches Snowflake as written.
  - **Fuzz round 4, rerun** with the checker's fixes: 26,000 queries, 1,846 partitioned, no finding.
  - **More of Q24 and Q4, built and held by the Snowflake plan census:**
    - `ScalarSubqueriesAggregated`: a correlated scalar subquery of no aggregate is `CASE WHEN COUNT(*) > 1 THEN
      error ELSE ANY_VALUE(x) END`, its rows' one value, NULL over none and raised over more, as DuckDB raises.
    - `OuterJoinSubqueryFilters(true)`: a LEFT JOIN conjunct holding a subquery of any kind that reads only the
      null-supplying side filters that side, as Snowflake evaluates a subquery predicate in a WHERE alone.
    - A ranking window with no order is `ORDER BY NULL` (NUMBER(*)).
  - **Fuzz round 5** (seed 5, `sqla17-default`, 1,500 shapes: 19,500 queries, 1,042 partitioned): no finding after one
    fix to the checker. A sqla17 profile's database keeps trailing blanks and folds case; the checker's key had dropped
    the blanks for every profile.
  - **The differential** (`IqFuzzSnowflakeTest`, P2.2): the fuzzer's queries answered by DuckDB in process and by the
    account, compared as multisets; a DIFF, a Snowflake error where DuckDB answers, or a raise in DuckDB alone is a
    finding.
  - **Q25's iq17 half stays as it is.** IQ types an integer operation by its operands' data (measured: `id + 1` over
    ids 1 to 7 is a SMALLINT). The parity table's `bg` holds 2^63 - 1, and there `bg + 1` is a NUMERIC(20,0). Lite types
    it by the operands' types, as a BIGINT. If the same data rule makes `bg + 1` a BIGINT over everyday values (not
    measured yet: an oracle question), widening to NUMERIC by type would turn `(bg + 1) / 2` into a NUMERIC quotient
    where IQ divides integers. Today only 64 bits' edge diverges: DuckDB raises its own overflow where IQ answers, the
    known D-N3. The iq16 half (SQL Anywhere's -158) is a question of Snowflake's NUMBER(38): a range check on every BIGINT
    operation there, or the divergence named.
  - **The lane's rerun at 955a705a** (the sibling): PASS -> FAIL 208 to 0, PASS -> ERROR 59 to 37, PASS -> PASS 5,719 to
    5,949. Then, for the 37:
    - the oversized literals' digits counted past a negative scale (3.4028234663852886E38 is 17 digits of scale -22);
    - `ExistsOverOneRow`, `NotExistsAsAll` and `ExistsUnnested`: an EXISTS over a one-row source or an ungrouped
      aggregate needs no subquery; the island's NOT IN and ALL in a WHERE are Snowflake's native ALL; an EXISTS's TOP,
      START AT, pass-through derived tables and outer-only conjuncts are unnested.
  - **Fuzz round 6** (seed 6, `iq16-default`, 1,500 shapes: 19,500 queries, 1,406 partitioned): no finding. Every
    profile family has a clean round now.
  - **The rerun at b799442a:** PASS -> ERROR 37 to 6, PASS -> PASS 5,980. Of the rest, a run SQL Anywhere judges may not
    diverge (the guard file's rule), so each is fixed or refused by name:
    - **Q25, done:** a BIGINT result past its range is checked on every database (`checkedInteger`). DuckDB and Postgres
      raised their own overflow, while Snowflake's NUMBER(38) answered SQL Anywhere's -158. Fuzz round 4, rerun with
      the check, is unchanged: 9,528 refused, 16,421 answered, no new refusal for size.
    - `RedundantNullGuards`: a WHERE's `IS NOT NULL` of a column it compares is dropped. The outer column's guard
      kept the correlated ALL from Snowflake.
    - `SnowflakeSubqueryRefusals`, by name: the island's NOT IN read outside a WHERE's condition, and a correlated
      scalar subquery of a row limit (SQL Anywhere's TOP in a subquery).
    - Open, a decision: `top_computes_a_distincts_items_for_every_row@sqla17`. Snowflake computes a DISTINCT TOP's
      items for the rows it keeps only, so SQL Anywhere's -628 in a row the limit drops is not raised.
  - **An independent review of the passes** (a fresh agent, given the code, not the reasoning) confirmed three bugs,
    each fixed and pinned in `SnowflakeSubqueryPassesTest`:
    - a column read beside an ungrouped aggregate was flattened into a row per source row;
    - a FROM-less explosion (sa_rowgenerator's) was taken as one row, so `NOT EXISTS (SELECT * FROM sa_rowgenerator(5,
      1))` was NOT TRUE;
    - a HUGEINT literal past 38 digits was rounded to a DOUBLE. It is now refused by name.
  - **The rerun at 06f29b3a:** the 5 iq16 overflow runs pass, but `m0/a_sum_of_ten_bigint_terms` timed out (120 s). A
    range check at every level of a chain copied its operand three times a level (3^9). The check is reverted
    (2f40358c). Its fuzz comparison had missed this: the generator's chains are short.
  - **The scale probe now holds SQL growth:** sums of 10, 50 and 200 integer and BIGINT terms, with any shape whose
    DuckDB SQL is a hundred times its island failing. It then found the doubling under the check: a BIGINT chain's
    NULL guards, unmerged past 64 bits, made a 10-term sum 34,867 characters (2^9). Under IQ's engine they now merge
    as below a BIGINT, since IQ raises nothing there either (D-N3). It is 387 characters now, and 200 terms are 5,137.
  - **The IQ harness counts a dialect's `DialectCapability` as REFUSED.** It is the dialect's refusal by name, as the
    island's `NotImplementedException` is; it had been an ERROR.
  - **Fuzz round 7** (seed 7, 2,000 shapes, depth 4, the generator's wider catalogue: DATEPART, DAYS/MONTHS/YEARS,
    DAYNAME/DATENAME/DATEFORMAT, CONVERT styles, CHARINDEX/LOCATE, STUFF/INSERTSTR/REPLICATE/STRING, ISNULL/NULLIF,
    GREATER/LESSER, TRUNCNUM/SQRT/POWER, AVG/LIST/ROLLUP, ROW_NUMBER/LAG): 26,000 queries, 1,713 partitioned, no
    finding. Its errors are IQ's own, raised by name: a CONVERT style invalid for a date, LIST beside ROLLUP, LESSER in
    a window.
  - **The differential against the embedded Postgres** (`IqFuzzPostgresTest`, P2.2): the fuzzer's queries answered by
    DuckDB and by Postgres 16, here, with no account. Two verdicts are not findings:
    - UNORDERED: LAG, ROW_NUMBER or a ROWS frame over an order with ties, whose rows may rightly differ;
    - RAISE_ORDER: an error one database raises for a row the other never computes. Postgres computes a windowed
      derived table's columns for every row before the filter outside it.

    Seeds 1 and 2, 1,000 shapes each: 4,961 queries answered alike, no finding.
  - **The Snowflake column is in the ledger** (the lane's run at b26592b3; 5955b476). 18 runs differ from DuckDB's
    column:
    - 5 iq17 BIGINT overflows PASS, Snowflake answering IQ 17.1's NUMERIC;
    - 3 are refused by name;
    - 3 iq17 runs are listed (`sf-lazy`, `sf-internal`);
    - 7 runs SQL Anywhere judges wait on a decision: 5 iq16 BIGINT overflows, sqla17's DISTINCT TOP error in a
      dropped row, and the iq16 internal error.
- 2026-10-23, a second independent review (a fresh agent, given the code): six confirmed bugs, each fixed and
  pinned:
  1. **A grouped filter isolated too broadly, and lost its sort** (`Lowerer.filter`, plain Pure too): any subquery
     in a filter over a grouped select that projected a no-column aggregate isolated it, with a plain isolation
     dropping the sort before it. It now isolates only when the folded predicate's subquery holds one of the grouped
     select's own no-column aggregates (`SqlProbes.carriesAggregateOfNoColumn`), with `Sorts.isolateOrdered`.
     `GroupedFilterSubqueryTest` checks the plans on every in-process database; its rows alone passed on the old
     code, DuckDB keeping a small input's order by chance.
  2. **A REAL read only in a named WINDOW counted as unread** (`IqDialect.reading`), so its table was not refused;
     a CHAR read only there was not padded. The scan now reads the windows' keys (`readExpressions`).
  3. **Snowflake's exact quotient wrote each operand twice** (eight divisions by 2: 843,095 characters). Over a
     literal divisor of at most a million, `CAST(TRUNC(a / b) AS BIGINT)` is exact (a fraction of at most 0.999999
     survives the six places), and `quotient` has no zero guard for a literal divisor that is not zero, as `rem`
     had none. Fifty divisions by 2 are 480 characters on DuckDB and 1,510 on Snowflake; the scale probe now holds
     both targets' SQL to a hundred times the island. Division by a column stays exponential (Q27).
  4. **A group the pattern does not open** (`REGEXP_EXTRACT('xab', '(?:a)b', 1)`, `\2` against `(?:a)(b)`) read a
     group the plain pattern opened. It is refused by name.
  5. **The bracket scans misread `[](]` and `[[:alpha:](]`**, counting the class's `(` as a group. One scanner
     (`Snowflake.live`) now marks the pattern's live characters for every scan, and the conversion of `(?:` keeps an
     escaped `\(?:` and a class's `[(?:]` as written (the old replace dropped their `?` and `:`).
  6. **A handler's Error left the client waiting** on the request thread's executor. `route` now answers whatever a
     handler lets through with a 500 and closes the exchange (`HandlerFailureTest`); `stop()` lets the request in
     flight finish for up to five seconds. Not changed: the executor's queue is unbounded where the JDK's dispatcher
     had the listen backlog.
  - **The local gate's census** caught this program's test files naming databases in identifiers (`onDuckDb`,
    `SNOWFLAKE`, `everyRunPlansForSnowflakeAsForDuckDb`): renamed to the platform and the cloud target
    (`everyRunPlansForTheCloudTargetAsForThePlatform`), the lane's name read from `IqParity.PLATFORM_LANE`. The
    capability matrix is regenerated for `ASCII_CODE`'s `unicode`.
- 2026-10-23, Q10 (the union half of Q17): **a union of more than 64 branches is written as nested groups**
  (`BalancedUnions`, a render pass every dialect runs after its own): each group of 64 branches is read through a
  derived table, `SELECT * FROM (b1 UNION ALL ... b64) AS u1 UNION ALL SELECT * FROM (...) AS u2`, recursively. A
  database parses a flat chain as a tree as deep as its branches, and DuckDB's depth of 1,000 refused the scale
  probe's UNION ALL of 2,000. A union, deduplicating or not, is associative, and its columns are its first branch's
  either way. A derived table rather than a parenthesized set operation, which SQLite does not read.
  - `BalancedUnionsTest`: 5,000 branches of UNION ALL and of UNION, their rows, on every in-process database.
  - The scale probe at twice its sizes: UNION ALL of 1,000, 2,000 and 4,000 run (637, 1,274 and 2,548 rows).
  - Lite's own planning grows faster than the union: 1.9 s, 4.0 s and 16.6 s (Q28).
- 2026-10-23, **Q27's Snowflake half: integer division writes each operand once.** By a literal of at most a
  million, `CAST(TRUNC(a / b) AS BIGINT)`; by anything else, `CAST(TRUNC(CAST(a AS NUMBER(38, 19)) / NULLIF(b, 0)) AS
  BIGINT)`. The sibling measured the second on an account: the BIGINT extremes, both signs, divisors of 3e18 and a
  zero divisor (NULL), every row exact, typed `NUMBER(38,19)`. Snowflake rounds the quotient to 19 places, and a
  BIGINT quotient that is not an integer is at least 1/|b| (1.08e-19) from one, more than the rounding's 0.5e-19;
  scale 18 would not be safe. DuckDB's half stays open: the translator's NULL guard repeats a nested operand where
  IQ's divide-by-zero error must not be lost, which needs the operand computed once (a common-subexpression hoist).
  - The sibling's lane run at cd3d257f: unchanged from b26592b3 (0 new; the 7 runs SQL Anywhere judges still wait on
    the user's decision).
- 2026-10-23, **fuzz round 8** (seed 8, 2,000 shapes, depth 3, NoREC's two queries per shape included): 30,000
  queries, 1,807 partitioned, no NoREC or TLP finding. One DBERROR, fixed:
  - `LIST(x0.c2, ',' ORDER BY x0.c2)` over a derived table's `UCASE('')` folded the key to `''`, and DuckDB refuses a
    non-integer literal in an ORDER BY ("has no effect"). `ConstantAggregateOrder`, a render pass every dialect runs,
    drops a constant key from a string or list aggregate (a percentile's key is its value, and stays).
    `ConstantAggregateOrderTest` on every in-process database.
- 2026-10-23, **Q28: planning a long UNION ALL is linear.** Thread dumps of the lens on a UNION ALL of 4,000 branches
  found `IqDialect.checked` choosing the statement's engine (`engineOf`, a walk of the whole statement) again for
  each of its queries, and collecting each set operation's whole tree of branches: quadratic in the chain. The engine
  is now chosen once, and each operation adds its own operands. The scale probe at twice its sizes: 1,000, 2,000 and
  4,000 branches plan in 1.0, 0.7 and 1.4 s (were 1.9, 4.0 and 16.6 s); the 4,000 run in 4.2 s (was 33 s). The probe
  at its own sizes: every shape plans in under 2 s but 700 joins (9.7 s, then DuckDB's depth limit); DuckDB's join
  order for 350 joins is still 210 s (Q16).
- 2026-10-23, **Q29: a long join chain plans faster.** Thread dumps of the lens on 700 joins found
  `Fold.sourceColumn` scanning the whole join's outputs at every level of its recursion, for each column read, only to
  decide whether a quoted pivot identity (`'2011__|__newCol'`) should be stripped. The scan now runs only for such a
  name. 700 joins plan in 4.0 s (were 9.7 s), 1,200 in 8.7 s; what remains is spread over the translator's
  taken-name sets, rebuilt at each join, and the typer's relation types. 700 joins are past DuckDB's tree depth
  anyway (Q17), and 350 still take its join-order optimizer 210 s (Q16).
- 2026-10-23, **Q7's lane: the scale probe's shapes on a real Snowflake account** (`IqScaleSnowflakeTest`,
  `//core:iq_scale_snowflake_test`, manual like the other account lanes). Each shape of `IqFuzzTest.scaleShapes` is
  planned for and run on the account in a schema of its own, on the server's request stack, with a statement timeout
  (`-Diq.scale.timeout`, 300 s): its plan time, SQL size, run time and outcome go to `iq-scale-snowflake.tsv`. A limit
  of Snowflake's is reported; only lite's own crash fails it.
- 2026-10-23, **Q16: a long join chain is read in groups** (`Lowerer.asLeftJoinSide`): past 32 relations, the chain
  so far is the next join's left side as a derived table (`SELECT *`), so DuckDB orders each group's joins on their
  own. Measured with plain SQL on DuckDB 1.4.4: 200 joins 21.7 s and 350 joins 197 s as one chain; 3.1 s and 10.3 s
  in groups of 32 reading every column; 0.12 s at 200 in groups of 32 reading only the columns used past them. The
  scale probe's 350 joins now run in 12.6 s (were 210 s), and planning is faster at every size (700 joins 2.6 s,
  were 4.0 s). Groups listing their columns for `SubselectPrune` to narrow were tried and dropped: its rounds narrow
  a nesting each, and 1,200 joins planned in 188 s. The chain is the next join's left operand either way, so its rows
  are the same for every join kind. `JoinChainGroupsTest`: 40 joins with a LEFT join past a group's edge, on every
  in-process database.
- 2026-10-23, **fuzz round 9** (seed 9, 2,000 shapes, depth 4, `iq16-default`): 30,000 queries, 1,718 partitioned, no
  finding.
- 2026-10-23, **a third independent review** (a fresh agent, given the code of cd3d257f..298b6bd5) confirmed three
  regressions, each fixed, and one suspected, fixed too:
  1. **BalancedUnions moved a root union's branches off the root**, where a dialect converting the root's values as it
     writes them no longer did: on SQLite, a DECIMAL(10,2) of a 65-branch root union came back as 1.5, a Double
     (MariaDB's floats, BigQuery's NULLs and Trino's rows by the same mechanism). The grouping is DuckDB's own pass
     now, after its root conversions. `LongUnionRootValuesTest` (every in-process database) fails on the old
     placement; `DuckDbBalancedUnionsTest` keeps the 5,000 branches.
  2. **The join chain's `SELECT *` group read every physical column**: a declared column of one table named like an
     undeclared one of another read the wrong one (DuckDB and SQLite answered A's undeclared Z for B's), names
     differing in case collided, and a prefixed join after the chain listed `t92.*` in the physical order. Reverted:
     Q16 is open again. `LongJoinChainTest` holds any later grouping to those rows.
  3. **Snowflake's scale-19 quotient of a HUGEINT operand** (a SUM of BIGINTs) can pass the 19 integer digits
     NUMBER(38, 19) holds: such an operand keeps the remainder form (`SnowflakeIqScaleTest`).
  - Checked and found sound: `IqDialect.checked`'s engine and branches, `Fold.sourceColumn`, `ConstantAggregateOrder`.
- 2026-10-23, **fuzz round 10** (seed 10, 2,000 shapes, `sqla17-default`): 30,000 queries, 1,203 partitioned, one
  DBERROR (5 queries of one shape), fixed. A grouped derived table whose key is computed (`x2.j + 6`) filtered by an
  EXISTS reading the key was folded into HAVING, carrying `j + 6` into the subquery, where DuckDB binds no grouped
  expression ("column j must appear in the GROUP BY clause"). The review's narrowing of the isolation (fix 1 above)
  had exposed it: the broad rule had isolated this query by chance. The probe
  (`SqlProbes.carriesComputedProjection`) now isolates for any grouped projection that is no plain column, an
  aggregate or a computed key; `GroupedFilterSubqueryTest` pins a computed key under a sort. The round's rerun: no
  finding.
- 2026-10-23, **Q27's DuckDB half, the design** (not built): a nested division by a column copies its dividend three
  times a level on DuckDB. The translator's NULL guard tests the operand and reads it (`if(X->isEmpty() || ...,
  |[], |X->toOne() ...)`), and `quotient`'s zero guard writes it into its message. The guard cannot merge the
  operand's own (as it does where nothing raises), because IQ evaluates the operand, and raises its divide-by-zero,
  where the other operand is NULL. A form linear in size that keeps the raise needs the operand bound once, which SQL
  does only as a column: `SqlToPure` would hoist such an operand into an `extend` before the item that reads it, when
  it reads the select's own row alone (no aggregate, window, or other scope's variable). It changes the shared
  translator's Pure for both SQL islands, so it waits for a slice of its own; dropping the dividend from `quotient`'s
  message alone would take the base from 3 to 2.
- 2026-10-23, **fuzz round 11** (seed 11, 2,000 shapes, `iq17-tds`, ansinull Off): 30,000 queries, 1,807 partitioned,
  no finding.
- 2026-10-23, **Q24's residual** (`cte/a_cte_in_a_scalar_subquery`, Snowflake's internal error 370001): lite sends a
  correlated aggregate subquery (`CASE WHEN COUNT(*) > 1 THEN <raise> ELSE ANY_VALUE(n) END`) whose WHERE still guards
  the outer column (`t0.id IS NOT NULL`): `RedundantNullGuards` reads a subquery's top select, and runs before
  `ScalarSubqueriesFlattened` brings the guarded WHERE up to it. Four read-only variants asked of the sibling, to
  tell the guard from the raise.
  - **Measured** (the sibling, VALUES-based CTEs, nothing created): the internal error is the `CASE WHEN COUNT(*) > 1
    ... ELSE ANY_VALUE(n) END` wrapper over a correlated grouped derived table; the guards make no difference, and
    `ANY_VALUE(n)` alone answers rightly. `ScalarSubqueriesAggregated` now writes `ANY_VALUE` alone where the WHERE
    equates each of a grouped derived table's keys to a value of no column of its own (a correlation or a constant):
    one group at the most, so the count never fires (`SnowflakeSubqueryPassesTest`; a WHERE fixing no key keeps it).
    The lane's next run will show the run's status.
- 2026-10-23, **fuzz round 12** (seed 12, 1,000 shapes, depth 5): 15,000 queries, 808 partitioned, one TLP finding,
  the fuzzer's own: the predicate read LESSER, which runs a statement over IQ's tables in SQL Anywhere's front end
  (measured on IQ 17.1), so its partitions ran in the front end and the shape alone in IQ's engine, where a DATEADD
  of a DATE is a DATE (the front end's a TIMESTAMP). The TLP check now skips a predicate that reads GREATER or LESSER
  where the shape does not.
  - The round's rerun with the check corrected: 803 partitioned, no finding.
- 2026-10-23, **the Snowflake lane at 476bb731** (the sibling): `cte/a_cte_in_a_scalar_subquery` passes on both
  profiles (ERROR -> PASS), and nothing else moved. 5,984 runs pass on both; no run passes on DuckDB and errs on
  Snowflake any longer. The ledger's snowflake column and the divergences follow (the `sf-internal` cause is gone).
  16 runs still differ from the duckdb column: 5 iq17 BIGINT overflows Snowflake answers as IQ 17.1 does, 3 refused
  by name, 2 listed iq17 runs (`sf-lazy`), and 6 runs SQL Anywhere judges that wait on the user's decision (the five
  iq16 BIGINT overflows and sqla17's DISTINCT TOP).
- 2026-10-23, **fuzz round 13** (seed 13, 2,000 shapes, `iq17-odbc`): 30,000 queries, 1,814 partitioned, no finding.
- 2026-10-23, **the generator widened** (a655f837): a division of two integer expressions, products of two (integer
  and NUMERIC) and a NUMERIC difference, a correlated scalar read of no aggregate, a grouped derived table's key
  computed and its HAVING on SUM or MAX, and DENSE_RANK, NTILE, LEAD, FIRST_VALUE over a frame and AVG over a
  partition in the crash-only window query. **Fuzz round 14**, its first (seed 14, 2,000 shapes): 65 findings, all
  DuckDB's "More than one row returned by a subquery", which is IQ's error too: the classifier counts it as one now
  (55de6994). The rerun: 30,000 queries, 1,771 partitioned, no finding. The widened catalogue's new refusals are
  IQ's -1001030 for a subquery inside a conditional (630) and NUMERIC widths IQ's own rules decide.
- 2026-10-23, **Q27's DuckDB half, tried and reverted.** An early trigger for the translator's operand hoist (an item
  past 2,000 Pure nodes and 50 times its SQL computed its long operands once, as columns before it) took eight
  divisions by a column from 852 KB of DuckDB's SQL to 7.6 KB (d889ea5f). **A fourth independent review** found the
  hoist computes an operand for every row where the unhoisted item skips it, inside a strict operation's merged NULL
  guard, beside a NULL operand: `nm / NULLIF(sm, 0) / NULLIF(sm, 0) + (it + bg + id) / sm` over a row with `sm = 0`
  raised the divide-by-zero once hoisted and answered NULL when written smaller. An answer that depends on an item's
  size is wrong either way; which one is right is IQ's to say (Postgres raises: a strict operator reads its
  arguments). Reverted (b66e4d3f); Q30 measures it on the live IQ, then the merge is fixed and the hoist comes back.
  The review found the other two changes sound (`carriesComputedProjection`'s isolation, Snowflake's one-group
  `ANY_VALUE`).
- 2026-10-24, **Q30 measured, and the NULL guards follow it.** The Mac session snapped the nine cases of
  `lite/null_beside_a_raise` on both oracles (521ed8a0; the live IQ 17.1.263.7761, each answer that differs from SQL
  Anywhere's confirmed by a second run). IQ's engine raises its divide-by-zero (-1001007) in all eight queries over `t`,
  the NULL operand on either side: a NULL constant, a column, NULLIF, ABS of it, a product, a NULL quotient, in WHERE
  too. SQL Anywhere 17 reads from the left and stops at the first NULL: NULL for `NULLIF(sm, 0) + it / sm` (and no row
  in the WHERE case), -628 for `it / sm + NULLIF(sm, 0)`. Without FROM (the front end under both profiles), NULL. lite
  answered NULL in all nine.
  - **The fix** (`SqlToPure.strict` with each operand's raise, `IqDialect.mayRaise`): an operand that may raise is
    tested whole where another operand's NULL test could skip it. Under IQ's engine every such test is read: the first
    of a CASE's tests, alone, or a sum of them when there are several. A test inside an OR may be read last (DuckDB orders
    an OR's operands by cost), and a sum may be folded once one of its terms is a constant (`CASE WHEN NULL IS NULL THEN
    1 ELSE 0 END + ... > 0` is always true by DuckDB's statistics, so it never read the quotient; measured). Under SQL
    Anywhere's front end each is read in its place, nested. An operand that cannot raise (a column, a constant, an
    aggregate's or a window's value, IQ's widening + - *, a division by a nonzero integer constant) merges its guard as
    before, so a chain of them stays linear. The integer, NUMERIC and DOUBLE operations use it, and an untyped NULL
    beside an operand that may raise reads the operand first.
  - **The hoist back** (d889ea5f, Q27's DuckDB half): an item past 2,000 Pure nodes and 50 times its SQL computes its
    long operands once, as columns before it, where the dialect computes every operand (`computesEveryOperand`:
    Postgres, IQ's engine; not SQL Anywhere's front end, which stops at a NULL). The fourth review's finding is IQ's
    answer now: the hoisted item and the item written smaller both raise. Scale probe: eight divisions by a column
    7.9 KB of DuckDB's SQL (were 852 KB), sixteen 17.1 KB; a new shape, a sum of quotients by a column, 6.0 KB at
    eight and 12.7 KB at sixteen.
  - **The cases.** The seven over `t` run under iq17-default alone: lite follows IQ's engine for an IQ database's
    tables, which SQL Anywhere, judging their iq16 runs, does not compute (its answers, dropped from the answers file:
    NULL for the six with the NULL operand to the left, -628 for `it / sm + NULLIF(sm, 0)`). The WHERE case is the
    harness's D-E2 skip (a raise beside AND depends on the plan; IQ raised there). `IqNullBesideARaiseTest` pins both
    rules on every in-process database, the front end's under sqla17-default. The DuckDB and Postgres lanes: the seven
    are BOTH_ERROR, and no other run moved.
- 2026-10-24, **Q30's second batch** (`lite/null_beside_a_raise_read_where`, eleven cases; the Mac session's snap,
  1724d477, each live IQ answer confirmed by a second run and a `--check` run). The result:
  - **SQL Anywhere's front end over a table** (sqla17-default): it reads from the left to the first computed NULL, as
    measured under iq16 (NULL for `NULLIF(sm, 0) + it / sm` and `NULLIF(it, 0) * (bg / sm)`, -628 with the division
    first). But a NULL literal makes the item NULL on either side: `it / sm + NULL` is NULL, folded before anything
    is read. The untyped NULL's read of its other operand (ebe932aa's `nullBeside`) is gone: the front end folds
    the NULL, and IQ's engine refuses it (-1009145).
  - **IQ's own front end** (a query over IQ's tables that reads GREATER, iq17-default): it reads as SQL Anywhere
    does, with SQL Anywhere's -628. lite already reads such a statement by the front end's rules.
  - **IQ's engine on a comparison and a function**: `NULLIF(sm, 0) < it / sm` raises -1001007 in either order, and so
    does `LEFT(NULLIF(vc, vc), it / sm)`. Before the fix, only the order with the quotient first raised: the
    comparison's AND read the NULL test first and stopped. `IqDialect.computedFirst` reads each operand that may raise
    first, alone or in a sum, where another may be NULL. It wraps a comparison (but one with a NULL literal) and the
    catalog's functions of two arguments or more that are NULL when an argument is (`IqFunctions.STRICT`: LEFT,
    RIGHT, SUBSTRING, REPEAT, LOCATE, CHARINDEX, ROUND, TRUNCNUM, MOD, REMAINDER, POWER). A constant or a variable
    raises nothing when read: a pinned plan, the showcase's `vc <> @skip`, keeps its SQL.
  - The lanes: the eleven pass or are BOTH_ERROR on DuckDB and Postgres, and nothing else moved.
    `IqNullBesideARaiseTest` gains the comparison and MOD.
- 2026-10-25, **the user's redirect: common SQL patterns first, with the live IQ session.** The work is ranked by how
  many real-world queries a gap touches, measured by the corpus lane (`iq-sql-research/corpus-lane`; the corpora and
  their rows stay out of this repository). Stress shapes no real query writes (Q16's 350-join chains, Q17, Q18) are
  set aside. Scripts (P5: DECLARE, SET, SELECT INTO, IF) stay "not now", the user's call, although they are the
  largest refusal class (1,374 queries). The Mac session (the live IQ) runs the judged lane and snaps the cases a fix
  needs, and takes the duplicate output names (206 queries) on `claude/iq-scale-mac`.
  - **Coverage at dc45fbfd** (empty tables, DuckDB): SEDE 11,708 queries, 7,065 answered, 2,233 refused, 2,371 IQ's
    own error (T-SQL IQ does not take), 39 database errors; SQLStorm 18,251: 5,841, 272, 12,138, 0; Public BI 646:
    620, 3, 23, 0. Past scripts and duplicate names, the largest refusals are decimals: a CAST to DECIMAL or NUMERIC
    with no precision (84, IQ's NUMERIC(126,38)), an AVG of a NUMERIC (25), a quotient of counts times 1.0 (28), and
    a checked AVG or SUM in a CTE or derived table (21). 366 "here is not supported" are mostly corpus text that is
    no SQL (a title fused into WITH, broken quoting), which IQ rejects too.
  - **The live IQ's answers to those shapes** (`lite/common_decimals`, 19 cases, snapped by the Mac session): a
    CAST with no precision is a NUMERIC(126,38) (32767.000... at 38 places), its SUM one too; its AVG a
    NUMERIC(127,59); its quotient by an integer a NUMERIC(127,49); a quotient of two such IQ's own error (-1009145,
    -1001073). Counts times 1.0: COUNT(sm) * 1.0 / COUNT(*) a NUMERIC(42,21), a ratio of two such a NUMERIC(42,22),
    one with an epsilon a NUMERIC(54,22), each of a value 38 digits hold. AVG of a CASE of ones a NUMERIC(35,16), an
    AVG of such AVGs a NUMERIC(65,37) (the inner 16 places carried).
  - **Fixed** (this commit): a checked AVG or SUM read from a query around it is checked where it is read, not
    refused (the overflow it guards, past a DOUBLE's 1.8e308 or an integer type's range, no real data meets);
    `an_average_of_a_double_in_a_cte` passed with it, and goes back to the Mac session rewritten without its TOP (the
    determinism lint reads no derived table's group key as unique). A DOUBLE's or a NUMERIC's zero divisor raises
    "Cannot divide by zero" (`IqDialect.raisedIf`, `fail` with its message), not toOne's "Cannot cast a collection of
    size 0 to multiplicity [1]" (the corpus's 31 database errors: CAST(COUNT(x) AS FLOAT) / COUNT(*) over the empty
    tables, IQ's error too). A CHAR is padded by IQ's store whichever engine reads it: a query over IQ's tables that
    GREATER or LESSER moves into the front end lost the pad (the Mac session's re-judging of fuzz round 3: 'ab' for
    'ab   ', 4 runs); a dialect of pinned rules, the engine-neutrality guard's, keeps its engine's store. The Mac
    session's Q21 merged (79cf2723: DATEADD of a timestamp of no known year checked row by row past 7910).
  - **Next:** the decimals past 38 digits whose values fit (the CAST with no precision, its SUM; the count ratios),
    typed as IQ types them and computed in lite's 38, refused at run time where a value does not fit.
- 2026-10-25, **the user's breadth ask, and the real-life census.** "How are we doing with all the scalar and
  aggregate and window functions, GROUP BY and HAVING, every OVER/PARTITION/ROWS/RANGE form, string, math, date and
  time functions, all join types, lots of joins, nested subqueries, combinations?" Decimals past 38 digits are set
  aside (DECFLOAT would not help: DuckDB has none, and Snowflake's DECFLOAT also holds 38 significant digits).
  - **The census** (`lite/census_*`, 317 cases in eleven files, iq17-default): one representative use per construct a
    report writes, over the parity tables, deterministic. The functions are every analytic one IQ 17.1 lists
    (`iq-sql-research/iqrules/iq_function_names.txt` by its categories, the system, web, file and encryption ones
    aside); every window frame form; GROUP BY forms (expression, CASE, ROLLUP, CUBE, GROUPING SETS); every join type
    (and chains of 6 and 10, APPLY, `*=`); subqueries (scalar, correlated, IN, EXISTS, ANY and ALL, three deep, CTE
    chains); set operations and row limits; report-shaped combinations. The Mac session snapped them from the live IQ
    (fd59eb33): 302 answered, 15 IQ's errors.
  - **Before** (translation alone): 278 answered, 33 refused, 6 of IQ's errors claimed. **After** this slice, judged
    against the live IQ: 295 PASS and 15 BOTH_ERROR of 317 (97.8%), 6 refused (ISDATE, INTTOHEX and HEXTOINT, SOUNDEX
    and DIFFERENCE, SIMILAR, STR of a DOUBLE, LIST(DISTINCT) of no GROUP BY key), 1 FAIL listed (a share of total
    over a window SUM of an INTEGER: IQ types the SUM by its data, D-N3; its EXPRTYPEs asked). Every IQ error the
    census meets lite models (INTERSECT, EXCEPT, GROUPING SETS, TOP in a subquery and APPLY under IQ's engine, USING,
    NULLS LAST, a RANGE frame over two keys or a date, a set operation's ORDER BY by a constant's alias).
  - **Added:** SIN, COS, TAN, COT, ASIN, ACOS, ATAN, ATAN2, DEGREES, RADIANS (a value outside the domain refused at
    run time), WIDTH_BUCKET of integers, OCTET_LENGTH, BIT_LENGTH, STR_REPLACE, HASH (MD5, SHA256), DATEFLOOR,
    DATECEILING and DATEROUND of a TIMESTAMP; STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE, VAR_POP, VAR_SAMP, CORR,
    COVAR_POP, COVAR_SAMP and the nine REGR_* by IQ's own one-pass formulas in DOUBLE (the Mac session measured them,
    and the forms were checked against its digits: n * SUM(x*y) - SUM(x) * SUM(y) for the slope, CORR and R2, the
    sums about the means for the rest); MEDIAN of an INTEGER answered (a group whose values differ by more than 2^31 - 1
    refused at run time, where IQ's midpoint wraps), its midpoint cut toward zero under IQ's engine (D-F17 fixed); a
    NUMERIC dividend over a scale-0 decimal divisor is a NUMERIC quotient (IQ divides whole an integer only: 100.0 *
    COUNT(*) / SUM(COUNT(*)) OVER () is its NUMERIC(46,23)). 42 older runs went from refused to PASS with them.
  - **The corpus lane at dc45fbfd, judged by the Mac session against the live IQ:** SEDE and SQLStorm 12,906 queries,
    10,923 PASS and 1,544 PASS_UNORDERED (96.6%), 208 FAIL after the tie-break: 151 ties (ROW_NUMBER or TOP over equal
    keys), 20 the clock, 24 NUMERIC places past 38 digits, 9 IQ's names of an unaliased item over a derived table, 4
    to classify. BIRD 350 PASS and 132 BOTH_ERROR of 500, 3 FAIL (ties); TPC-H 19 PASS of 20, 1 BOTH_ERROR; TPC-DS 64
    PASS and 19 BOTH_ERROR of 87, 4 FAIL (the share of total's places). No wrong answer to a query IQ answers that is
    a translation's defect.
- 2026-10-25, **the census's second wave: the permutations, and a share of a total typed as IQ computes it.**
  - **The cases** (`lite/census2_*`, iq17-default, snapped by the Mac session from the live IQ, d925a0ac, each
    confirmed by a second run): each of eight aggregates over each of eight frame forms, partitioned or not (120); the
    ranking and offset functions by three orders (56); each join kind by four kinds of condition, read as rows, grouped
    and windowed, and chains of 3 to 16, inner and mixed (59); subqueries by kind, correlation and depth (20); GROUP BY
    keys by HAVING forms (8: each MIN(s) reads the select list's alias s, SUM(v), so IQ raises -156, a nested
    aggregate, and lite does; the four HAVING ... AND cases the D-E2 lint flags were dropped); twenty shares of a total.
  - **Judged at 0c17fe0b, translation alone:** 246 PASS and 18 BOTH_ERROR of 287. Joins 59 of 59, subqueries 20 of 20;
    frames 112 and 8 refused (MAX of a string over a frame that starts after the partition's first row: IQ answers NUL
    bytes, its defect, which the snaps show); ranking 48 and 6 of IQ's -1005028 (CUME_DIST of a composite key), lite
    raising each. 14 FAIL: 12 shares' places, 2 LEADs.
  - **A share of a total**, from the Mac session's executed types (EXPRTYPE describes the front end's rules only, not
    the engine's): IQ's engine computes a sum in a narrower type than it shows, and a quotient by it counts those digits
    (`IqType.quotientDigits`). A window SUM in its argument's digits and one more (an INTEGER's 11: 100.0 * v / SUM(v)
    OVER () has 12 places; a BIGINT's 20; a NUMERIC(p,s)'s a NUMERIC(p+1,s)), two more over an aggregate (SUM(SUM(v))
    OVER () 13, of COUNTs 22, of a SMALLINT's sum 21); a group SUM of an INTEGER or a BIGINT in one more (1.0 *
    COUNT(*) / SUM(v) has 12 places), of a SMALLINT the BIGINT it shows. An INTEGER's total is an integer: an integer
    by it divides whole (v * 100 / SUM(v) OVER () is the BIGINT 38). NULLIF takes the shown NUMERIC(20,0) (21 places).
    18 of the 20 shares PASS, and the census's share_of_total and windows/share_of_the_total_is_refused (IQ's BIGINT 23).
  - **Listed:** LEAD's string default, cut by IQ 17.1 to a length its earlier rows leave ('none' shown 'n' or 'no':
    iq-defect); a window total over a derived table's aggregate column, typed far wider by IQ (a NUMERIC(8,2) sum's
    divides as 21 digits) by a rule not measured yet (derived-total). Its probes (`lite/census2_shares_derived`, 10)
    and the grouping matrix written again (`lite/census2_grouping_keys`, 16) are with the Mac session to snap.
  - **Also:** DATEFLOOR, DATECEILING and DATEROUND refused outside IQ's engine (its front end's -134) and to a multiple;
    m1review's MEDIAN of SMALLINT extremes the live IQ's alone, its front-end twin (sqla17-default) SQL Anywhere 17's -1;
    the Mac session's duplicate output names merged (1cf35a18), presentName stripping a repeated name's mark only (5
    pivot tests had broken).
  - **Census 2 now:** 279 PASS, 14 BOTH_ERROR, 8 refused (IQ's defect), 8 FAIL listed, of 309.
  - **The Mac session's benchmark re-judges at dc45fbfd:** tpcds_iq 7,884 PASS and 366 unordered (FAIL 384 to 364, the
    moves ties), job_iq unchanged (4,811 PASS), job_rows 3,453 PASS. tpch_iq (9,427 queries) died of OutOfMemoryError:
    not the translation. tpch-4324 joins ON r.rank = 1, which reads one side only, a cartesian product of tens of
    millions of rows (IQ's own -1005015, Max_Cartesian_Result); its judge timed out and cancelled, but the executor kept
    filling rows until the heap was gone, and the queries after it died. **Fixed:** the executor's tabular and
    collection reads stop where the reading thread is interrupted, every 1,024 rows (`Executor.stopIfCancelled`,
    `CancelledResultTest`), closing the statement. IQ's Max_Cartesian_Result is not modeled (it reads IQ's estimates).
  - **Through a derived table** (`lite/census2_shares_derived`, 10, and the grouping matrix again,
    `lite/census2_grouping_keys`, 16 PASS; snapped at 87edb356): a derived table's or a CTE's column is read as the type
    IQ shows, a sum's narrower one staying inside (`InputDialect.readAcross`, IQ's: WITH s AS (SELECT k, SUM(v) AS sv
    ...) SELECT 1.5 / sv has 22 places, where 1.0 * COUNT(*) / SUM(v) has 12). 6 of 10 PASS; a SUM over a derived
    aggregate column is typed far wider (a NUMERIC(8,2) sum's total a NUMERIC(23,2), a COUNT's 35 digits, an INTEGER
    sum's a NUMERIC(29,0)), listed (derived-total, 6 runs) until its rule is measured.
  - **Census 3** (`lite/census3_idioms`, 59 report idioms, each combining several features) is with the Mac session to
    snap: lite answers 58 and refuses one (a UNION's ORDER BY by a name, which it reads as IQ's -1005012).
  - **Ratios as reports write them** (the Mac session's tpch_iq re-judge: tpch-22676 PASS to REFUSED; the coverage
    lane's 16 refusals past the size bound): CASE WHEN up + down > 0 THEN ROUND(CAST(up AS FLOAT) / (up + down) * 100,
    2) ELSE 0 END, each DOUBLE operation's checks repeating its operands and IQ's REAL rounding five times more.
    **Fixed:** a DOUBLE operation on integers, NUMERICs or REALs computes no overflow or underflow check (none can
    happen there); an integer's CAST to REAL is only rounded, raises nothing and keeps its NULL guard
    (`IqDialect.exactReal`); a one-WHEN searched CASE's long result operands are computed once where its condition
    holds (`SqlToPure.hoistOperands`; the scale probe's guarded ratio of 8 and 16 quotients, 8 and 16 KB).
    CAST(up AS FLOAT) / (up + down) is 4.8 KB of DuckDB's SQL (38 KB before), the CASE of it 15.5 KB (refused before).
    The coverage lane at 5329aeb8: the size bound's refusals 16 to 6 (SEDE 5: DATEDIFF chains, LOG and POWER, nested
    CHARINDEX; SQLStorm 1); SEDE 7,256 answered, SQLStorm 5,893, no new database error. The ratios' cases,
    `lite/census3_ratios` (8), go to snap with census 3.
    69954201: every branch of a searched CASE (its ELSE, its later WHENs, each under the earlier ones, nested as the
    CASE reads them) hoists its long operands where it is taken.
  - **Census 4** (`lite/census4_frames`, 96): six aggregates over eight more RANGE and ROWS forms, partitioned or not.
    The statistical aggregates over a window were refused (32); they are now computed as over a group, IQ's one-pass
    sums each over the frame (`IqAggregates.statisticOverWindow`; STDDEV(v) OVER (ORDER BY id ROWS 2 PRECEDING) is
    Python's statistics.stdev of each frame to the last digit). Through the lens 86 of 96 answered, 10 refused (MIN of
    strings over a frame after the partition's first row, IQ's NUL bytes). With census 3 on claude/iq-census-4, to snap
    when the live IQ answers again (it drops connections, -832, since the afternoon).
  - **Census 3 and 4 judged** (snapped at cbbe9744, merged 971c8023): idioms 57 PASS of 59 (IQ errs on both others: a
    UNION's ORDER BY by a name, -1005012, as lite predicted; LAG(COUNT(*)), -1005028), ratios 8 of 8, frames 86 PASS
    and 10 refused. **Fixed:** a COUNT(*) anywhere in LAG's or LEAD's first argument is IQ's -1005028 (the live IQ's
    probes: LAG(COUNT(*) + 0) and LEAD(COUNT(*), 1, 0) refused, LAG(COUNT(id)), LAG(SUM(v)), LAG(MAX(v)) answered); a
    whole NUMERIC of up to 18 digits converts to a DOUBLE (CAST(SUM(b) AS FLOAT)).
  - **The realistic reports** (`core/src/test/resources/iq-reports`, a second parity root over a retail schema: 107
    report queries, `tools/iq-reports`; snapped at 69249c87, all 107 answered): 98 PASS on DuckDB and on Postgres, 9
    FAIL, each a quotient's places only. **Measured** (the live IQ's probes): IQ's engine sizes a SUM -- a group's, a
    window's, a scalar subquery's -- by its input's estimated rows, its digits the argument's and those of the rows
    less one (SUM(COUNT(*)) OVER () GROUP BY a column: NUMERIC(21,0) over 5 rows, (22,0) over 11 to 100, (23,0) over
    619, (24,0) over 1,196 to 1,500, (25,0) over 15,000 to 60,175). The census's "+1 for a group's SUM, +2 for a
    window's over one" is this rule over its 9 filtered rows; the corpus's 9 new places FAILs (sede-892182 and the
    others) and the reports' 9 are it over more rows. Data-typed as D-N3 is: listed (row-typed, 9 runs), not modeled;
    derived-total is likely the same rule over a derived table's estimate. Part D (48 more: aging, ledgers,
    commissions, cohorts, gaps and islands, GROUPING SETS and CUBE, percentiles, ANY and ALL) follows to snap.
  - **Reports parts D and E** (2026-10-26; `tools/iq-reports/queries_d.py`, 48, pushed to snap at 1c529dfe;
    `queries_e.py`, 38 over five more tables -- exchange rates, promotions, a web shop's events, support tickets,
    inventory -- held until D's answers land, its schema change making every reports answer stale). Lite answers 153
    of D's 155 first, 185 of E's 193; **fixed:** a DISTINCT with a window ordered by a qualified column crashed ("not
    an input scope"); a position in or a length of a VARCHAR(n) is typed 0 to n, so a count computed from it needs no
    32-bit check (URL parsing by nested LOCATE and SUBSTRING was past the size bound, now 3 to 4 times smaller); a
    GROUP BY key past the bound, and a CASE nested in an item, have their long operands computed once; LIST ordered by
    several keys or by an expression (each nullable key after one of its NULL's place, IQ's NULL first ascending). The
    determinism lint reads a SELECT DISTINCT's items, a tagged UNION ALL and many-to-one joins as keys. E stands at
    189 of 193: MEDIAN of a BIGINT (not measured), a quotient past 38 digits (set aside), and one IQ error lite
    predicts (a derived table's scalar subquery read in a CASE, -1001030). The fixes' cases: `lite/census5_report_fixes`
    (9), at claude/iq-census-5 to snap.
  - **Reports part E judged** (the root re-snapped at c3e5333c over the five new tables; 192 of 193 answered, the 193rd
    IQ's -1001030 as lite predicts): 35 of E's 38 PASS on DuckDB and Postgres, one BOTH_ERROR, one refused (a quotient
    past 38 digits), one row-typed (a quotient by a derived table's total, listed). The root: 180 of 193 PASS, 11
    row-typed, none wrong. Parts F (22: EXISTS cohorts, DATEADD of a DATEDIFF, a median by row numbers, ABC classes,
    basket pairs, a twelve-month pivot) and G (15: time on page by LEAD, bounces, days sales outstanding, formatted
    invoices) follow to snap; lite answers all of F, and all of G but one it reads as IQ's -156 (SUM(units) AS units
    beside MAX(units), the alias read inside the aggregate). The lint reads an outer join's ON for its NULL-supplying
    side (662a9ad1).
  - **Reports parts F, G and H judged** (snapped at a50271cd, 55 cases): 50 PASS on DuckDB and Postgres;
    abc_xyz_matrix is IQ's -156 as lite predicts (IQ reads a select-list alias inside an aggregate: SUM(units) AS units
    beside MAX(units)); capped_attainment, LESSER over a NUMERIC quotient, is refused (the statement runs in SQL
    Anywhere's front end, whose quotient lite reads only where it is shown); three quotients by a total are row-typed,
    listed. **Fixed** on the way (54fe2883): a timestamp cut to its hour by DATEADD of a DATEDIFF is checked row by row
    for IQ's dropped hours past 7910, where it was refused; a CASE's compared operand is computed once. The root: 230
    of 248 PASS, 14 row-typed, 2 refused, 2 IQ's errors, no wrong value.
  - **Reports part I judged** (two tables of international suppliers, UTF-8 names and cities; snapped at 343e67fc,
    their bytes and characters checked against schema.yaml row by row): 15 of 15 PASS on DuckDB and Postgres -- UTF8BIN
    order, ASCII-only UPPER and LOWER, LENGTH against BYTE_LENGTH, LIKE's _ over a multibyte character, LEFT, RIGHT and
    SUBSTRING by characters, transliteration by REPLACE. The root: 245 of 263 PASS, 14 row-typed, 2 refused, 2 IQ's
    errors, no wrong value. The live IQ dropped connections intermittently during the snap; every query answered on a
    retry, none is a crasher.
  - **Reports part J judged** (23 analyst pitfalls, snapped at 311607ee): 22 PASS on DuckDB and Postgres -- NOT IN over
    a NULL (no row), an outer join's NULL side filtered in WHERE against in ON, COUNT(*) against COUNT(column), integer
    / and MOD of negatives cut toward zero, ROUND at a half, blank padding (a leading blank counts), NULL groups and
    order, DISTINCT over NULLs; the 23rd, COALESCE((SELECT SUM ...), 0), is IQ's -1001030 as lite predicts. The root: 267
    of 286 PASS, 14 row-typed, 2 refused, 3 IQ's errors, no wrong value.
  - **Reports part K judged** (IQ's own idioms, 29 cases snapped at 38febb18): IF ... ENDIF and IFNULL labels, STR,
    STUFF, REPLICATE, PATINDEX, INSERTSTR and STRING text, CONVERT's date styles, DATEFORMAT's pictures, DAYNAME,
    MONTHNAME and DATENAME, YEARS, MONTHS, WEEKS and DAYS, ARGN, the REGR_ family and RANGE frames over a value: 27 PASS
    and 2 IQ's own errors on DuckDB and Postgres. **Measured and built**: DATEFLOOR, DATECEILING and DATEROUND to a
    multiple of the part -- MINUTE's counted from the hour and HOUR's from the day, a multiple dividing 60 or 24 (else
    IQ's -1001010 for each row: 7 minutes, 5 hours); DAY's, MONTH's, QUARTER's and YEAR's from 0000-01-01, as DAYS and
    MONTHS count (7 days from 2025-12-18 is 2025-12-13); and WEEK's start, the Sunday on or before. **Fixed**: a month's,
    quarter's or year's start read as a timestamp on Postgres was a timestamptz (date_trunc of the DATE Postgres writes
    for a date-grained date_trunc, typed TIMESTAMP after DuckDB's), now cast first. The root: 294 of 315 PASS, 14
    row-typed, 2 refused, 5 IQ's errors, no wrong value.
  - **Reports part L judged** (whole reports in one statement, 10 cases snapped at 64ea0989): a monthly management
    pack from five CTEs, a customer health dashboard, a profitability waterfall, a sales league table, a fulfilment SLA
    report, a reorder plan picking each product's supplier, a cohort revenue triangle, a unified activity timeline and
    a price-change audit -- 9 PASS on DuckDB and Postgres; last-touch attribution is IQ's -1001030 as lite predicts
    (IQ reads the CTE's correlated scalar subquery inside the CASE that reads its column). The lint now covers a grouped
    query's keys from a table its unique column joins (e6a33666). The root: 303 of 325 PASS, 14 row-typed, 2 refused,
    6 IQ's errors, no wrong value.
  - **TPC-H re-judged at 42b998b2** (tpch_iq, 9,427 runs, with its tie-break suite): 7,956 PASS, 722 PASS_UNORDERED,
    9 PLACES; of 499 FAILs 129 remain after the tie-break, 116 ties and 13 row counts under TOP or RANK ties -- no
    wrong value; 21 IQ's own plan limits (19 -1005015, 2 -1006000); 15 refused past 38 digits. **Fixed**: tpch-22676,
    PASS before and refused past the size bound since -- LEFT JOIN s ON c.key = CAST(SUBSTRING(CAST(s.key AS VARCHAR),
    1, 3) AS INTEGER) -- a join condition's compared operand that reads one side alone is computed once, a column of
    that side before the join (0f3b90cf).
  - **The Snowflake lane at ac51fd9d** (the sibling session's run, its message refused for want of a trusted device; read
    from its transcript): 6,825 runs PASS on both; 9 that PASS on DuckDB are Snowflake's compile error "Unsupported
    subquery type cannot be evaluated" -- a correlated scalar subquery compared in a WHERE or an outer join's ON, an IN
    within an IN correlated to the outer query, an EXISTS over a GROUP BY expression -- 4 of them matched DuckDB at
    476bb731; 3 refused by name; 8 IQ's errors lite answers on Snowflake (the iq16 BIGINT overflows and a lazy
    TOP), unchanged. The report suite's run waits on the user's approval in that session.
  - **Unread columns passed along** (the corpus lanes' 4 refusals: tpcds-4908 and tpcds-26588's CONCAT_WS(...) AS
    full_address, job-1742 and job-22017's CAST(... AS TEXT), each in a CTE column the statement never reads; IQ
    answers them all): lite already read a derived table's unknown call as a NULL string where its column is read
    nowhere. Now an outer query's own items no one reads read nothing (a column one more CTE passes along, by name or
    under a `*`, is read nowhere too), an item read reads the items its alias names, and a CAST to LONG VARCHAR (TEXT)
    in such a column is a NULL string as the calls are. Read in the output, a WHERE, a top-level `*` or another
    item, IQ's -265 and -1001006 stay; a missing name stays -143.
  - **The unread-column cases snapped** (lite/corpus_gaps, 9 cases, the Mac's snap c71e92d5): 5 PASS, 4 BOTH_ERROR
    (CONCAT_WS read by the output's `*`, a WHERE, another item's alias, IQ's -265; CAST AS TEXT read, -1001006). The
    Mac's run of the corpus lanes' 4 at bdf89bda: tpcds-4908, job-1742, job-22017 PASS, tpcds-26588 PASS_UNORDERED.
  - **Reports part M judged** (22 analyst patterns, e97ab7a8 and 2a785c8f): gaps and islands, 30-minute sessions,
    market baskets, as-of exchange rates, promotion windows, latest snapshots, retention, top three per category,
    percentiles, HOURS/MINUTES SLAs, TOP ... START AT, NOT EXISTS, e-mail and phone cleanup, badges, LEAD and
    LAST_VALUE, CUME_DIST bands -- 20 PASS on DuckDB and Postgres; GROUPING SETS and INTERSECT over IQ tables are
    IQ's -1001030 as lite predicts. The evaluation-order lint (D-E2) no longer counts integer + - * under IQ's engine
    for an iq17 run (it widens, D-N3); three joins that compared CAST(ts AS DATE) now compare the DATE with the
    TIMESTAMP (re-snapped, the same rows).
  - **The bitwise operators** `&`, `|`, `^` and prefix `~` (were refused by name): read at the research's measured
    precedence (one level, left to right, tighter than `*` and `+`; a sign looser, an operand's own after an
    operator; `~` tighter still) and translated for SMALLINT, INTEGER and BIGINT operands to Pure's bitAnd, bitOr,
    bitXor and bitNot, typed as the larger operand; a NULL operand is NULL; a BIT, a COUNT, a NUMERIC, a DOUBLE or a
    string refused by name. The Mac's probe on IQ's engine over the parity table matches lite on all 7 rows. The
    syntax cases: accept PARSED 3,403 -> 3,437; a condition as an operand is IQ's -131. Eight cases to snap
    (claude/iq-snap 676fa5b1).
  - **The Mac's re-judge at aac1ff9a** (moves against 42b998b2): the 15 starved TPC-H followers PASS alone, tpch_iq's
    TIMEOUTs 24 -> 6 (the six that led each run of followers, heavy themselves); tpcds-1626 REFUSED -> PLACES,
    tpcds-6742 ERROR -> PASS; 37 tpcds_iq and 25 tpcds_tb percentages FAIL -> PLACES. **A judge defect it found, fixed
    here**: the unordered and places checks ran the query again, so a TOP or ROW_NUMBER over ties drew its rows again
    and a lucky draw was read as PLACES (44 of tpch_iq's 53 named no places). Both now read the rows the judge
    judged, PLACES needs a cell that differs uncut, and the in-order places cut takes any row count (tpcds-5378's
    8,303). Checked with part M's answers doctored: rows reversed are PASS_UNORDERED, a mean with two places more
    PLACES.
  - **The bitwise cases snapped** (lite/bitwise, 8 cases, 15 runs, 6b0a02d3): all 15 PASS on DuckDB and Postgres once
    a GROUP BY key `id & 1` is read as the select list's (the shape compare named an operator by its place). IQ
    answers `nm & 1` with the BIGINT of each NUMERIC cut toward zero (-99999999.99 & 1 is 1), which lite now reads
    for a NUMERIC of at most 18 digits before its point (was refused by name).
  - **A quotient read at fewer places** (the realistic reports' part N, `CAST(AVG(months) / 12.0 AS NUMERIC(5,1))`,
    refused at run time past a 2-digit dividend; the TPC-H lane's 15 run-time refusals of the same kind): IQ cuts its
    quotient toward zero at its scale, so one a CAST to NUMERIC(p, n) or an integer, a ROUND(q, n) or a TRUNCNUM(q, n)
    reads -- directly or times a power of ten -- is computed at the n + 1 places read (TRUNCNUM's n), where its
    steps hold a dividend of up to 18 digits. lite/reports_tickets/agent_workload REFUSED -> PASS.
  - **The corpus judge, from the Mac's subset re-judge at 74962876** (the tie lottery gone: tpch_iq's 53 PLACES 37
    FAIL, 5 PASS_UNORDERED, 5 PASS, 6 PLACES that name places): rows the judge judged FAIL and that match in order at
    the fewer places are PLACES (21 tpcds_iq cells equal but for their places, ROUND(x, 2) at the quotient's scale,
    FAILed); a cell's places alone count as a difference; where IQ shows the fewer places, lite's value cut half up
    there matches too (tpcds-29313: IQ rounds its AVG).
  - **Reports parts N and O judged** (41 cases, 51a1b4c3): 39 PASS on DuckDB and Postgres -- receivables aging, a
    ledger's running balance, headcount over a calendar, salary bands, spans of control, commission tiers, fiscal
    quarters, integrity checks, SKU parsing, pivots, Pareto classes, rank movement, cross-tabs, region-only products,
    funnels, days of cover. LIST(DISTINCT x ORDER BY x) of a column no GROUP BY names is refused (IQ answers it:
    'enterprise/retail/smb'); the 7-day moving average over sa_rowgenerator is IQ's crasher (a window over it runs
    to a cancel), its calendar now from order_items' ids, to re-snap. What they found, fixed here:
    - a quotient computed at the places a CAST reads carried those places where its type said IQ's scale, and the
      CAST's cut, x - rem(x, 0.1), read two scales -- DuckDB's remainder of two scales is a double's, so 100.00 came
      out 99.9: the value is padded to IQ's scale again (its dividend held to what that leaves);
    - a statement whose own FROM reads sa_rowgenerator runs in SQL Anywhere's front end beside IQ tables (measured:
      DATEADD of a DATE there is a TIMESTAMP; IQ's engine types it a DATE); and there a quotient a CAST, a ROUND or a
      TRUNCNUM reads is its exact quotient cut at the places read (was refused but as a shown item), a CAST's
      whatever SQL Anywhere would type its operands;
    - the determinism lint reads a plain SELECT over a grouped CTE as keyed by that CTE's GROUP BY keys.
  - **A ratio of two totals by long division** (the TPC-H lane's 11 run-time refusals at 68caeac5: `total_revenue /
    NULLIF(total_supplycost, 0)`, two SUMs IQ types NUMERIC(36,4), its quotient NUMERIC(60,26..29), whose steps at
    that scale held a dividend of 2 to 6 digits): where IQ's scale holds no dividend of a BIGINT's 18 digits, the
    quotient is divided as an AVG is (IqFunctions.longQuotient), x * 10^s2 by |y| * 10^s2 in chunks of at most 13
    places, each step within 38 digits, exact at IQ's full scale; refused at run time only past a dividend of up to
    30 digits or a divisor of 19 there. A tried alternative -- such a quotient shown at fewer places -- turned twelve
    PASSing ratios of counts (small dividends at IQ's scale) to FAIL, and was dropped. Checked against the exact
    quotient of the reports' data (Electronics' 5456107.5826089044167015633).
  - **The long division grows with its chunks** (the Mac's re-judge at 29eb033b: of tpch_iq's 11 ratios 2 PASS, 2
    PLACES -- lite's scale 27 where IQ's row-typed sums gave 24, the digits alike -- and 7 refused past the size bound
    at 38,696 to 138,588 nodes, the bare `total_revenue / c` alone 41,345): each chunk's quotient read the rest before
    it through whole()'s estimate and correction, and the next rest that quotient again, so each chunk multiplied the
    expression. Now each chunk's rest is the remainder of the rest before it times 10^k by the divisor (both
    databases' remainder of two decimals of one scale is exact; the rests are of the larger scale, the divisor
    padded to it), its digits that value less its remainder by the divisor, a whole number below 10^13 that a
    double's estimate finds to within far less than a half: linear in the chunks. The AVGs' long division is the
    same function. Checked exact against Python's decimals on dividends of 14 digits and divisors of 16, negative
    either side, quotients from 1e-18 to 8e8 (the 29 places of NUMERIC(60,29)); `COALESCE(r / NULLIF(c, 0), 0)` over
    two such sums now translates.
  - **REGEXP** (the Mac's probes on IQ 17.1 over a VARCHAR and a CHAR(5)): `x [NOT] REGEXP p` matches the whole
    string ('Hello' REGEXP 'h.*' false, 'x  ' REGEXP '[a-z]+' false), with case, groups and alternation ('H(ello|i)'),
    [a-z] by character ('Résumé' not in it), a CHAR(n) blank padded ('ab   ' REGEXP 'ab *'); [[:upper:]] matches É.
    Read for a constant pattern every database means alike as a whole-string match (IqFunctions.wholePattern:
    REGEXP_SUBSTR's atoms with groups and alternation -- a whole string matches or not whichever alternative a search
    prefers --, ASCII alone in a bracket class, counts to 255), Pure's matches with (?s); refused by name: a POSIX
    or backslash class, a group's (?, an empty group or alternative, a case-insensitive database, a pattern no
    constant, a value no string. SIMILAR TO over IQ's tables is IQ's -660 under its engine ("Query Decomposition:
    Unknown Node Type"): still refused by the parser, naming it (parsing it would read three of SQL Anywhere's
    rejected patterns the syntax ratchet pins). lite/regexp (12 cases) on claude/iq-snap to snap.
  - **Reports part P** (19, `tools/iq-reports/queries_p.py`, claude/iq-reports 9f1a006a, to snap): data quality and
    operations -- contact details validated by REGEXP, SKUs audited against their pattern, an order's flags packed
    into a bitmask and read back with &, LEAD's gap to the next order, FIRST_VALUE and LAST_VALUE over a whole
    partition, GREATER and LESSER of list and sold prices, masked e-mails by REPLICATE and STUFF, payments reconciled
    against order totals, tickets against an SLA, overlapping promotions, gaps in the rate feed, the second-highest
    salary by nested subqueries, days of cover to a run-out date. Lite answers all 19. Writing them found a report
    pitfall IQ shares, not a lite bug: `SUM(value) AS value` beside a later `SUM(value)` reads the alias, a nested
    aggregate, IQ's -156 (lite says so). LIST(DISTINCT x ORDER BY x) of a non-key stays refused: the platform's
    group-lambda aggregate carries an ORDER BY or a DISTINCT, not both (GroupLambdaAggs), and Postgres orders a
    DISTINCT aggregate only by its argument.
  - **A WHERE's long compared operands computed once** (the Mac's re-judge at 29eb033b: TPC-DS's Q4 PASS -> refused
    at 62,032 nodes, tpch-6646 and tpch-671 too): under IQ's engine a comparison reads each operand three times (its
    NULL tests and its value: every operand computed, Q30), so Q4's `CASE WHEN a.t > 0 THEN b.t / a.t ELSE NULL END >
    CASE WHEN c.t > 0 THEN d.t / c.t ELSE NULL END`, two long divisions, passed the bound. As a select item's and a
    JOIN ON's already are, a WHERE past the bound now has each top-level conjunct's long operands computed once, a
    column extended before the filter, where the conjuncts before it hold (NULL elsewhere, where the WHERE drops the
    row anyway, so nothing raises where the conjuncts would not have reached it; not in a correlated subquery, the
    resolver's to decorrelate), then is translated again. tpch-6646's and tpch-671's shapes translate at 104534ff
    already (the linear long division). And where a quotient's whole part is refused from 10^(38 - s) <= 10^15
    (scales of 23 and up: TPC-H's ratios of NUMERIC(36,4) sums), the long division's first rest is the remainder of
    |x| by y and its whole part that difference by y as a double rounds it, in place of whole()'s three estimates.
  - **LIST(DISTINCT x [, d] [ORDER BY x [DESC]]) of a string under IQ's engine** (region_directory_formatted's
    LIST(DISTINCT c.segment, '/' ORDER BY c.segment), 'enterprise/retail/smb' on IQ 17.1): each text without its
    trailing spaces once (in a case-sensitive database a collation key is one such text), in those texts' byte order
    (IQ's blank-padded order but for a character below a space, a group holding one refused at run time by an aggregate
    of its own, so that the list's DISTINCT and ORDER BY read one deterministic value: DuckDB and Postgres match a
    DISTINCT aggregate's ORDER BY to its argument, and no expression holding a raise). In Pure,
    `$y->distinct()->sort()->joinStrings(d)` (`->reverse()` for DESC) in the aggregate's reduce; the Lowerer reads a
    reduce's values sorted by themselves as the SQL aggregate's ORDER BY its own value. And a COUNT, IQ's UNSIGNED
    BIGINT, is a REPEAT's count as a BIGINT is (REPEAT('#', COUNT(c.id))). Moves: census_aggregates/list_distinct and
    m1gaps/list_distinct_of_strings_is_refused REFUSED -> PASS (iq17), reports' region_directory_formatted REFUSED ->
    PASS, on DuckDB and Postgres.
  - **sa_rowgenerator anywhere moves the statement to the front end** (the Mac's seven probes on IQ 17.1 over
    ll_employees: e.id + 2147483647 raises the front end's -158 with the generator in the FROM, joined, or only in a
    WHERE's IN subquery, where IQ's engine widens; DATEADD(DAY, 1, a DATE) is a TIMESTAMP with it, a DATE without):
    readsRowGenerator reads every query of the statement, no longer its own FROM alone; a TOP in such a statement's
    subquery is the front end's to read. lite/row_generator_engine (6 cases) on claude/iq-snap to snap. Merged: the
    REGEXP answers (all 12 as lite predicted on IQ 17.1; SQL Anywhere 17 matches a CHAR(5) unpadded, D-S1), part P's
    19 (all PASS on DuckDB and Postgres) and the 7-day moving average (SKIP -> PASS). Three part P queries put a
    raising operation beside OR in a WHERE or ordered a window by too few keys (the parity lints): rewritten on
    claude/iq-reports 08ac905c, to re-snap.
  - **Reports part Q** (11, `tools/iq-reports/queries_q.py`, snapped at 4c30f365): RFM codes by NTILE, UTM source and
    medium parsed from landing URLs, an hour-by-weekday heat map, signup-cohort retention, each region's distinct
    segments, channels and cities listed, a fiscal year from July, the top three products per category, the median
    days to ship, a reorder plan against supplier lead times, a sales-rep leaderboard, orders with tickets waiting.
    All 11 PASS on DuckDB and Postgres. What they found, fixed here: NTILE(n)'s UNSIGNED INT now carries its range 1
    to n, so CAST(NTILE(3) OVER (...) AS VARCHAR(1)) reads (its arithmetic stays refused by name).
  - **D-A1 measured** (the Mac's probes on IQ 17.1): LISTs of one query each keep their own order -- segments,
    channels and cities each in its own -- but for a later LIST of the same value and the same ORDER BY keys as an
    earlier one in another direction, which IQ computes as the earlier one (LIST(id ORDER BY id), LIST(id ORDER BY id
    DESC) both ascending; DESC first, both descending; LIST(id ORDER BY sm, id) and (sm DESC, id) both the first's;
    grouped alike). Under iq17 profiles lite now answers every other combination and refuses that one by name; IQ 16's
    engine, not measured, refuses as before (SQL Anywhere 17, its judge, raises -904). m1c's
    two_lists_ordered_differently_under_the_iq_engine_are_refused REFUSED -> PASS (iq17).
  - **The corpus BUG verdicts, classified** (the Mac at fcbf12ec: 21 tpch_iq, 8 tpcds_iq, 2 job_rows, IQ raises where
    lite answers), all listed, none modeled, each hanging on IQ's estimates, plan or data: -1005015 (23: a JOIN whose ON
    has no equality between its sides -- a one-sided filter, an IN or a scalar subquery, an inequality -- whose
    estimated product passes Max_Cartesian_Result; the same shapes over smaller inputs answer, many PASS); -1009145
    (5: IQ's own two-phase SUM(SUM(x)) of a NUMERIC(7,2) under a multi-fact LEFT JOIN fan-out, its partial sum sized
    from the row estimate: the listed iq-sum-cast); -1006000 (2: a hash-join internal error, a CTE filtered by a
    scalar AVG over another CTE read through a correlated EXISTS); -1001043 (1: an internal error evaluating a CTE
    column that reuses its source column's name). Reports part R (11, windows and sets) on claude/iq-reports
    17a66983 to snap; lite refuses a quotient by a window AVG (NUMERIC(30,23): a step past 38 digits) and a ROLLUP
    ordered by keys that are no select items (GROUPING(...)), and claims INTERSECT and EXCEPT as IQ's -1001030.
