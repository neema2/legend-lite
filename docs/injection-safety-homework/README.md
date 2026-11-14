# Stream S homework: the byte-identity snapshot and the S8 measurement (2026-11-14)

This is evidence for `docs/INJECTION_SAFETY_PHASE0_2026_11_14.md`. The orchestrator set two conditions:

1. **Byte identity before S2.** Take a corpus-wide SQL snapshot before S2 starts, and diff it after each step. Nothing
   may change except the named fixes.
2. **S8 measurement.** Measure how many statements lite plans hold more than one statement.

The scripts here are kept as `.txt` files, as the other homework folders keep probes. Run them from the repository
root.

## The scripts

| File | What it does |
|---|---|
| `snapshot.sh.txt <out> <tag>` | Runs every `//core:core_tests_*` package and PCT with the sent-SQL dump on (`LEGEND_LITE_DUMP_SQL`). PCT covers channel A on DuckDB and SQLite (five suites each), H2's relation suite, and channel B (five suites). For each target it keeps the unique sent statements in `<out>/<tag>/<target>.gz` |
| `corpus_snap.sh.txt <out> <tag>` | Runs the corpora through `//spec:corpus_one` (DuckDB and H2, host pass). Its runner writes the pass's whole output to a log in `java.io.tmpdir`, about 1.8 GB with the dump on. That log goes to `/dev/shm`, is read, and is then deleted (a cloud container's disk allowance is too small to hold it) |
| `extract_sql.py.txt <log or -> <out.gz>` | Turns a log into unique statements. It masks the run-specific text: execution ids, UUIDs, sandbox and temporary paths. It treats a line that starts with `[` or `'[` (PCT's own lines) as the end of a statement |
| `diff_snapshots.py.txt <before> <after>` | Lists, per target, the statements only one side sent. It exits 1 when any target moved |
| `multi_statement.py.txt <gz>...` | The S8 measurement: counts statements holding a `;` outside a string, a quoted identifier, a comment or a dollar-quoted body, with more text after it. Its lexer is approximate; the check S8 ships must be exact per dialect |

## The baseline

Taken on `9d9f98999`, stream S after Phase 1, the `CsvSeed` fix and C4's `c53c834e0` merged. It is not committed: it
is deterministic and can be regenerated on that commit.

- **Determinism:** `pct_duckdb_grammar` run twice gave 316 unique statements both times, with 0 different.
- **The test targets:** 45 targets (30 core packages, 15 PCT lanes), 20,099 distinct statements.
- **The corpora:** the DuckDB host pass, 7,386 distinct statements; the H2 host pass, 7,000.
- **Total:** 34,485 distinct sent statements, by origin:

  | Origin | Statements |
  |---|---|
  | other (queries) | 25,522 |
  | let | 3,459 |
  | referee_ours | 2,054 |
  | system | 1,815 |
  | body | 767 |
  | statement | 440 |
  | seed | 351 |
  | session | 77 |

**Gaps:**
- The baseline holds SENT statements only. SQL that is rendered but never sent is pinned by the golden-text tests,
  which turn red themselves if it moves.
- A corpus's DATABASE-judge pass is not covered. `corpus_one` refuses a whole database pass: it needs the host pass's
  outputs, which only the build action provides.

## S8: statements that hold more than one statement

326 of the 34,485 statements hold more than one statement:

- **323 are `[sql:body]`:** a test body's own `executeInDb` text, the user's raw SQL, run as written (S307), for example
  `BEGIN TRANSACTION; Create Table m3d_a(i INT); Insert into m3d_a values (1); COMMIT;`. By lane:

  | Lane | Count |
  |---|---|
  | the DuckDB corpus | 155 |
  | the H2 corpus | 125 |
  | `core_tests_testplan` | 19 |
  | `core_tests_exec` | 12 |
  | each `core_tests_integration*` lane | 5 |
- **3 are extractor artifacts:** a `SELECT 42 AS value` followed by a test's own printed line.
- **0 were written by lite's planner.**

MariaDB's and MySQL's raise read-back is two statements the plan's target declares
(`MariaDb.raiseReadBack`: `SELECT @legend_raise` and `SET @legend_raise = NULL`), each a single statement, sent
separately by the seam (`JdbcStatements.held`).

The orchestrator ruled on 2026-11-14 that the check covers the statements the planner writes. A body's `executeInDb`
text is a typed category in the plan, which the check skips by name. A guard holds that this category never appears on
the product road.

The lexer's cost per statement on the planning path is measured when S8 starts.
