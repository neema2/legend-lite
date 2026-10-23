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
| Q24 | Snowflake's correlated subqueries: a correlation two derived tables down, a scalar subquery of no aggregate, and lite's NOT EXISTS forms of NOT IN and ALL are "Unsupported subquery type" there (59 runs), and a COUNT read through the nesting is NULL for no row | P1.2 | the sibling's measurements | open |
| Q25 | IQ 16's BIGINT overflow (-158) on Snowflake, whose NUMBER(38) does not overflow (5 runs BUG). The island checks no BIGINT arithmetic itself; under iq17, where IQ answers past 64 bits, DuckDB's overflow is the lane's ERROR and Snowflake passes | P1.2, P3 | a decision (with Q26's): a run SQL Anywhere judges whose only difference is an error Snowflake does not raise | open: the check of 38df46ba reverted (2f40358c), its 3^n growth timed out on Snowflake |
| Q26 | TOP and START AT compute the items of rows they skip (IQ raises for them); Snowflake evaluates the select list after the limit (3 runs BUG) | P1.2 | the sibling's measurements | open |
| Q27 | Nested integer division by a column grows exponentially: `it / sm / sm` doubles a level twice on DuckDB (the translator's NULL guard repeats the inner quotient, unmerged where IQ raises for a zero divisor; the zero guard's message copies the dividend) and six times on Snowflake (its exact quotient writes the dividend twice). Needs a common-subexpression hoist (the operand as a computed column) or a single-copy exact quotient for Snowflake (`TRUNC(CAST(a AS NUMBER(38, 19)) / b)`, by its division scale rule, to be measured) | P4, P1.3 | the sibling's measurement for the Snowflake half | the Snowflake half done (2026-10-23, the scale-19 quotient, measured); DuckDB's open |
| Q28 | Planning a long UNION ALL grows faster than linearly: 1.9 s at 1,000 branches, 4.0 s at 2,000, 16.6 s at 4,000 (the scale probe at twice its sizes, 2026-10-23) | P4 | a profile | done (2026-10-23): 4,000 branches planned in 1.4 s |
| Q29 | Planning a long join chain: `Fold.sourceColumn` scanned a join's every output at each level of its recursion, for a pivot identity's check | P4 | a profile | done (2026-10-23): 700 joins planned in 4.0 s (were 9.7 s); 1,200 in 8.7 s, the rest spread over the translator's taken names and the typer, past DuckDB's depth limit anyway |
| Q7 | Snowflake's scale limits, measured and handled | P1.4, P4 | Q3 | the lane built (`//core:iq_scale_snowflake_test`, 2026-10-23); its first run asked of the sibling |
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
