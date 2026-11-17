# F50: Postgres's exact Float remainder, slimmed (PARKED, ready; F, 2026-11-17)

Parked by the orchestrator (the finish line comes first). Not wired in: a design and its measurement, for pickup after
the finish line, or sooner if Phase 5b's size needs it (under 5b a Float literal is a DOUBLE, so PCT's rem family
reaches `Postgres.exactRemainder`; on d-stage 885328d0 its literals are DECIMAL and take a NUMERIC `MOD`).

## Measured (embedded Postgres, local; Java `%` bit for bit)

The harness (`H.java`) checks a formula template (`{x}`, `{y}`) over 4,728 pairs: NaN, ±Infinity, ±0.0, 1e300 rem 7,
1e17 rem 3, 0.3 rem 0.1, 1.7 rem 0.1, MIN_VALUE, denormals, ±MAX, 2^53+1, 4,000 random bit patterns and near-multiples;
plus the grouped select by an expression (the Cypher island's `WITH a * 1.5 AS g ... g % 2.0`). `C.java` puts every
operand as a constant (0, -0, ±Infinity, NaN, NULL, 4.9e-324, 1e300) on either side, to catch plan-time folding
errors. `T.java` times `sum()` over 1M rows (jit off, no parallel workers: the embedded build lacks both).

| form | template chars | x/y reads | wrong | grouped | constants | 1M rows |
|---|---|---|---|---|---|---|
| today (`current.sql`) | 3,767 | 29/27 | 0 | runs | ok | 6.8-7.5 s |
| (A) bits via float8send (`bits.sql`) | 1,094 | 10/9 | 0 | runs | - | 15.0-15.5 s: rejected, 2x slower |
| (B) the ln method reshaped (`ln.sql`) | 1,289 | 12/13 | 0 | runs | ok | 6.1-6.5 s |

## Design (B), to build

- `exponent(v)` without its guards: the CASE frame already keeps 0, ±Infinity, NaN and NULL out of the remainder
  branch, and Postgres folds a constant WHEN to TRUE and drops the ELSE (`C.java` proves every constant case).
- The common power `greatest(ky, 0)` from the divisor alone: `|x| >= |y|` in that branch, so `kx <= ky`.
- The sign as `sign(x) * r` (a -0.0 result keeps its sign).
- PLUS: no `CAST(op AS DOUBLE PRECISION)` around an operand already typed DOUBLE (kept for an Integer operand of a
  mixed rem): one column statement about 4,463 -> about 1,700 chars (-62%).
- Pins to add when built: the edge list as a Postgres arm test (`PostgresArmTest` has the embedded server), and a
  dialect SQL-text test on the operand read counts.

Run the harness: start a Postgres on port 55432 (user `pg`, trust), then
`java -cp postgresql-42.7.x.jar H.java ln.sql` (and `C.java`, `T.java current.sql ln.sql`).
