# Lane round 1: Postgres and Trino (2026-11-25)

*Measured by the Postgres and Trino lane (`session_0195rQEnn8FAUPKYnpH5gmBg`, Legend Dev, Opus 5.5 at medium effort,
measure only). Base `296732e2`, candidate `2cd0b2f9` (step 1 of the plan: the edge's DECIMAL Float literal deleted).*

## Trino

Identical per test, channels A and B, all five suites, both commits. Channel A 345/137/469/205/93 all passing; channel
B Essential PASS=339 ERROR=6, the other four all passing. The only text that differs is Trino's query ids inside four
errors.

## Postgres

| Suite | A base | A candidate | B base | B candidate |
|---|---|---|---|---|
| Essential | 345/0 | 345/0 | PASS=334 ERROR=11 | same |
| Grammar | 137/0 | 137/0 | PASS=131 ERROR=6 | same |
| Relation | 469/0 | 469/0 | PASS=467 ERROR=2 | same |
| Standard | 205/0 | **204/1** | PASS=198 ERROR=7 | **PASS=197 ERROR=8** (the target fails: not ledgered) |
| Unclassified | 93/0 | 93/0 | PASS=92 ERROR=1 | same |

**The one difference, on both channels: `meta::pure::functions::math::tests::stdDev::testFloatStdDev`.**
- Base: PASS. Candidate: `AssertFailed: expected: 3.14, actual: 3.1399999999999997`.
- The statement, `[3.14, 6.28, 9.42]->stdDevSample()`:
  - base: `STDDEV_SAMP("__x") ... FROM unnest((SELECT CAST(ARRAY[3.14, 6.28, 9.42] AS DECIMAL(3, 2)[])))`;
  - candidate: `STDDEV_SAMP("__x") ... FROM unnest((SELECT CAST(ARRAY[3.14::float8, 6.28::float8, 9.42::float8] AS DOUBLE PRECISION[])))`.
- On the base Postgres computed in NUMERIC, exactly 3.14. On the candidate it computes in float8, and its
  `STDDEV_SAMP` answers 3.1399999999999997. DuckDB and SQLite answer 3.14 over doubles
  (`pct/src/test/resources/edge/duckdb_standard_answers.tsv:212`, `sqlite_standard_answers.tsv:212`).
- Channel B's ledger block: "channel B postgres Standard does not match its ledger (1): FAILS, not ledgered:
  meta::pure::functions::math::tests::stdDev::testFloatStdDev :: ERROR AssertFailed: expected: 3.14, actual:
  3.1399999999999997".
- Other plan text changed (`VALUES` now `CAST(1.1 AS DOUBLE PRECISION)`), with no other outcome or message moved.

**For the user's ruling** (not yet ruled): this is the deletion's one measured cost outside DuckDB. The product's own
Postgres dialect already wrote the double, so lite's product answers the same on Postgres today; the compatibility
door now matches it.

**Timings:** setup 436 s; builds base 86 s (an underestimate), candidate 4 s; `pct_postgres` 129/126 s,
`pct_channel_b_postgres` 32/31 s, `pct_trino` 256/265 s, `pct_channel_b_trino` 213/224 s. Disk free: 41 GB.

**What the lane found wrong or missing in the docs:**
1. A background task reported "completed" while its processes kept running, and a later chain ran twice. What worked:
   `setsid nohup` plus a watch on a timings file.
2. The Postgres recipe's `--run_under="<copy>/as_unprivileged.sh ubuntu"` holds a space: in a script it must be one
   array element. The script must be copied outside the workspace, and `chmod o+x /root` was needed.
3. `docs/PCT_CORE_CLOSEOUT_START_HERE_2026_11_06.md` section 9.3 does not say that a channel B suite is one JUnit test
   whose result is its census, diff and ledger lines.

## Follow-up: is it Postgres itself? (the user's question, 2026-11-25)

Yes. Two independent checks:

**Computed outside any database.** The exact standard deviation of the three doubles is 3.139999999999999902…, and the
nearest double to that is 3.14: the correctly rounded answer. In plain double arithmetic the two-pass method, the
sum-of-squares formula and Welford's running update all give 3.14; the Youngs-Cramer running update, which Postgres
uses for `float8`, gives 3.1399999999999997, the value the lane saw.

**Run on the server with no driver and no Java.** The tests' own embedded server (PostgreSQL 16.15), `psql` over a
Unix socket:

| Statement | Answer | Bits |
|---|---|---|
| `stddev_samp` over the three `float8` values | 3.1399999999999997 | `40091eb851eb851e` |
| `sqrt(sum((x - m) * (x - m)) / 2)` over the same values | 3.14 | `40091eb851eb851f` |
| `stddev_samp` over the three as `numeric` | 3.1400000000000000 | |
| `3.14::float8` | | `40091eb851eb851f` |

Postgres's own `stddev_samp` over doubles is one unit in the last place below the correct double. The two-pass
formula on the same server is correct.
