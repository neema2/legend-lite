# A3 piece 2: the gate after its approved moves (2026-11-20)

The user gave their word in the translator session on 2026-11-19: "approved: push both A3 branches and make piece 2's
moves". The moves are made on local `claude/compat-a3-switch`, every count as measured in `../README.md`:
- `72b94baf`: PCT −63 and the 9 re-pins.
- `09183550`: corpus +7 and −4 per database, and the 5 edge-net re-records.

Piece 1 is pushed: `claude/compat-a3-frontends` at `a4c1f2b1`.

## The gate on `09183550`

The run was `//gates:local`, the corpus lanes, the parity suites, the core suites the re-pins touch, both edge nets,
and every PCT Essential lane runnable here. Results: `gate-summary.txt`, and `gate-postgres-as-ubuntu.txt` for the
root-refusing set.

- **Green:** everything else, including:
  - the 9 re-pinned tests;
  - both edge nets and the corpus verdicts;
  - channel A on SQLite, MariaDB, MySQL, ClickHouse, Trino and MongoDB; channel B on SQLite, ClickHouse and Trino;
  - `//gates:pct_duckdb`, `//gates:pct_channel_b`;
  - the 12 Postgres and DataCube tests that refuse root, plus both Postgres PCT lanes, all run as `ubuntu`.
- **Red, a guard piece 2 moves that my measurement missed:** the lean-SQL baseline, G3's shrink-only pins
  (`//core:lean_sql_baseline_test`). `//core:query_capture_test` reads the same `lean-sql/baseline.tsv` and fails on
  the same growth. Neither was in the fast-loop list behind `../README.md`'s numbers.
  - 59 pins grow, none shrink (`lean-sql-growth.txt`).
  - `indexOf(s, t)` on 11 channel A databases and core: the `- 1` on STRPOS.
  - `indexOf(s, t, from)`: +1 char each, since a miss is -1.
  - Core `substring`, both forms: 199 → 539 chars, plan JSON 4676 → 10032. This is S383's guard,
    `CASE WHEN <bound> > LENGTH(s) THEN ERROR('LG#n') ELSE ... END`. The raise's operands sit in the plan's site
    table.
  - Core sort keyed by `substring`: 330 → 471 chars.
  - Core TypedExtend +106 chars, TypedProject +97 chars.
  - **Pending the user's word**; the re-pin is `bazel run //core:update_lean_sql_baseline`.
- **Red, and not A3's:** channel B MariaDB and MySQL. They fail the same way at A2's base `6551c203`, without A3
  (`a2-base-channel-b-{mariadb,mysql}.txt`, run in this container on 2026-11-20):
  - **MySQL:** the same 9 tests on both commits. They are decimal-scale judge disagreements, for example
    `expected: 3.8D actual: 3.800000000000000000000000000000D`.
  - **MariaDB:** list-order judge disagreements, and the set changes from run to run: 9 tests at the base (zip, match,
    split, fold) and 15 at A3's head (sort, slice, take, tail, add, concatenate, fold).
  - **Correction to `../README.md`:** it says every measured lane fails on exactly the 3 `indexOf` rows and nothing
    else. That holds for every lane except channel B MariaDB and MySQL. For those two, I read only the "PASSES,
    ledgered as failing" lines and missed these pre-existing "FAILS, not ledgered" lines. The 3 `indexOf` rows'
    removal holds on both.
  - No ledger row was added for them; that is not mine to move.

## S383's guard is already as small as the fold ruling allows

Asked by the compat owner, 2026-11-20.

**What is already folded:**
- a literal start ≥ 0 gets no start check;
- a literal end ≥ a literal start gets no end-before-start check;
- the 1-argument form with literal 0 gets no guard at all.

The core tests keep only the bound against `LENGTH(s)`. Folding `LENGTH('a literal')` would be Java computing a value.

**Literal-only folds not yet made:**
- a negative literal start, which always raises;
- a literal end below a literal start, which always raises;
- a literal end ≤ 0, which can never exceed the length.

None occurs in the core tests, so they would remove nothing from the 199 → 539.
