# The facts step 5's rows waited on, measured (2026-11-26)

*Measured by a separate session (measure only: no tracked file touched) on `46f943fe`, for the rows of
`research-engine-rows.md` whose "Not verified" list asked for them. This is its report as it sent it, set out.*

**Instruments.** A plain JDBC runner on the jars at lite's pins, each sha256 checked against
`tools/deps/jars_table.bzl`: H2 2.5.252 and DuckDB JDBC 1.5.6.0 (DuckDB v1.5.6), with unpatched H2 2.1.214 beside
them for comparison. JVM flags as `tools/junit/defs.bzl:51-54` (GMT, en_US). H2 in MODE=REGULAR and MODE=LEGACY, the
same answers. Sources read: legend-engine 4.145.0 and legend-pure 5.99.0 at their tags. Abbreviations:
TPM = `toPostgresModel.pure`, SDD = `sqlDialectDefaults.pure`, HSD = `h2SqlDialect.pure`, DX = `duckdbExtension.pure`,
ED = `extensionDefaults.pure`, H2E = `h2Extension2_1_214.pure`.

## A. H2 2.5.252, lite's

1. **Bare DECIMAL has scale 0.** `CAST(3.8 AS DECIMAL)` = 4; `CAST(CAST(3.8 AS DOUBLE) AS DECIMAL)` = 4;
   `CAST(8 AS DECIMAL)` = 8; `CAST('123.450021' AS DECIMAL)` = 123. Each column is `DECIMAL(100000,0)`; the cast
   rounds half up (8.5 to 9, -3.5 to -4). Unpatched 2.1.214 gives the same. **So 2.5.252 gives legend-engine's `4`
   for `toDecimal(3.8)`.**
2. **`DATEDIFF(WEEK)` under en_US counts Sunday-start week boundaries crossed:** all 14 expected values of
   legend-pure's `testDateDiffWeeks` (`dateDiff.pure:83-96`). Saturday to Sunday 1, Sunday to Saturday 0, Sunday to
   the next Sunday 1, Monday to Sunday 1, Sunday to Monday 0. **It depends on the locale:** under en_GB or de_DE
   Saturday to Sunday is 0 and Sunday to Monday 1 (Monday weeks); `ISO_WEEK` gives Monday weeks under any locale.
   2.1.214 the same. So it is Pure's answer only because the tests pin en_US.
3. **`ACOS` and `ASIN` out of range raise** for the whole query: `Invalid value "2.0" for parameter "ACOS() argument"`,
   SQLState 90008, on a literal, a stored DOUBLE and a cast alike. Unpatched 2.1.214 answers NaN. (DuckDB 1.5.6
   raises `Invalid Input Error: ACOS is undefined outside [-1,1]`.) So 2.5.252 raises where legend-engine's 2.1.214
   gives NaN.
4. **`ROUND` sends a half away from zero:** `ROUND(CAST(0.125 AS DOUBLE), 2)` = 0.13 (DOUBLE);
   `ROUND(CAST(2.5 AS DOUBLE))` = 3.0; `ROUND(CAST(-2.5 AS DOUBLE))` = -3.0; `ROUND(2.5)` = 3 (NUMERIC(1,0));
   `ROUND(16.5)` = 17; `ROUND(CAST(1.005 AS DOUBLE), 2)` = 1.01.
5. **`MOD` returns its divisor's type and first casts the dividend to it, rounding:** `MOD(7.5, 5)` = 3 (INTEGER: 7.5
   becomes 8); `MOD(-7, 3)` = -1; `MOD(7, -3)` = 1; `MOD(7.4, 5)` = 2; `MOD(7.5, 5.0)` = 2.5. 2.1.214 the same. So a
   Float or Decimal `rem` with an Integer divisor is 3 on H2, not 2.5.

## B. DuckDB 1.5.6, lite's

6. **Bare DECIMAL is `DECIMAL(18,3)`:** `CAST(3.8 AS DECIMAL)` = 3.800; `CAST(8 AS DECIMAL)` = 8.000;
   `CAST('123.450021' AS DECIMAL)` = 123.450. (The literal `3.8` alone is `DECIMAL(2,1)`.) So legend-engine's
   `toDecimal` has scale 3 on DuckDB and scale 0 on H2.
7. **`split_part` against Pure's `splitPart`** (the same arguments, Pure's index 0-based):

   | DuckDB | Pure |
   |---|---|
   | `split_part('a,b,,c', ',', 3)` = `''` | index 2 = `'c'` |
   | `split_part('abc', '', 1)` = `'a'` (an empty separator splits into characters) | index 0 = `'abc'` |
   | `split_part('abc', ',', 2)` = `''` | index 1: out of range, an error |
   | `split_part('a,b', ',', 0)` = `''` | index -1: an error |
   | `split_part('a,b', ',', -1)` = `'b'` | index -2: an error |

   Pure's `splitPart` is `str->split(token)->at(part)`; its `split` is Java's `StringTokenizer`
   (`CompiledSupport.java:207-218`): it drops empty tokens and reads the token as a set of characters. legend-engine
   adds 1 to the index (`pureToSQLQuery.pure:3197-3204`) and writes `split_part(%s, %s, %s)` (DX:276). The Pure side
   was read from its code and `StringTokenizer` run; Pure itself was not run.
8. **`date_diff`:** `'week'`, Saturday to Sunday = 0: DuckDB's week is elapsed days over 7 (Sunday to Monday 0, Sunday
   to Sunday 1). Hour, minute and second count boundaries crossed (10:59:59 to 11:00:01 is 1 hour); `datesub` gives
   the elapsed 0. On H2 `DATEDIFF` WEEK, HOUR, MINUTE, SECOND and DAY each count boundaries (1 for the same pairs).
   So DuckDB's week differs from Pure's (Pure expects 1 for 2015-07-04 to 07-05; DuckDB gives 0).

## C. What legend-engine writes

9. **`isNumeric`.**
   - H2: a SELECT goes through the dialect translation (`relationalMappingExecution.pure:440-443`). TPM:367 writes
     `regexp_full_match(s, '[-]?[0-9]+(\.[0-9]+)?')`, which SDD:966-970 with SDD:894-897 turns into
     `s ~ '^(?:[-]?[0-9]+(\.[0-9]+)?)$'` (HSD:398 keeps that default). The legacy `(lower(%s) = upper(%s))`
     (H2E:232) applies only to statements that are not SELECTs. H2 2.5.252 and 2.1.214 both accept the `~` form.
   - DuckDB: `(lower(%s) = upper(%s))` (DX:238).
   - Answers for `'123'`, `'12.5'`, `'-3'`, `'+3'`, `'1e5'`, `'abc'`, `''`, `' 1'`, `'1,000'`, `'１２'` (full-width):

     | Form | Answers |
     |---|---|
     | H2, legend-engine's `~` form | T T T F F F F F F F |
     | DuckDB, legend-engine's `lower = upper` | T T T T F F T T T T |
     | lite's product `IS_NUMERIC` (both databases) | T T T T T F F F F F |
     | the edge's present `LOWER(s) = UPPER(s)` (both databases) | T T T T F F T T T T |

   **So the edge's form is legend-engine's on DuckDB and not on H2**, where legend-engine's answer is a regular
   expression that refuses `'+3'`, `'1e5'`, `''` and the rest. Register S370 is stale for H2.
10. **The other templates.**
    - H2 (translation route): `parseDecimal` TPM:401 `cast(x as decimal(5,2))`; `toDecimal` TPM:406
      `cast(x as decimal)`; `parseJson` TPM:471, then HSD:422 `legend_h2_extension_json_parse(x)` (Java:
      `LegendH2Extensions.java:116`); `dateDiff` TPM:397 (units TPM:734-748), then HSD:410 and HSD:549-553
      `datediff(<unit>, a, b)`; `splitPart` TPM:417, then HSD:388 `legend_h2_extension_split_part`
      (`LegendH2Extensions.java:201`).
    - DuckDB: `parseDecimal` DX:256 through ED:721-727 (`cast(%s as decimal)` of one argument,
      `cast(%s as DECIMAL(p,s))` of three); `toDecimal` DX:282 `cast(%s as decimal)`; `parseJson` DX:259
      `cast(%s as JSON)`; `dateDiff` DX:213 `datediff('<unit>', a, b)`, units DX:478-483.
    - So the one-argument `parseDecimal` is `decimal(5,2)` on H2 (two places, overflowing past 999.99) and
      `decimal(18,3)` on DuckDB.

## D. What legend-engine answers (the engine runner)

**Instrument.** `bazel run //tools/engine-runner:testable` (legend-engine 4.145.0): Service test suites over one
model (a Relational class on table `T(K, S)`, the mapping `num: isNumeric([m::DB]T.S)`, a Relational data seed, a
connection of type H2), each suite on its own seeded H2. One trap: a service query with a `let` is written
`{|let ...; <query>;};` with the inner `;`.

11. **Code outside any store query runs in legend-engine's own Java, with Pure's meaning.**
    - `{|let x = 16.5->round(); let y = 'abcdef'->substring(1, 3); let z = 'abc'->indexOf('b');
      m::Row.all()->filter(r|$r.k == 'r01')->project([r|$r.k, r|$x, r|$y, r|$z], ['k','x','y','z']);}` passes against
      `x = 16`, `y = 'bc'`, `z = 1`: Pure's.
    - The same three expressions written inside the projection answer `17`, `'abc'`, `2`: the database's.
    - `{|let w = 5->rem(0); ...}` raises in a Java plan node: `java.lang.RuntimeException: Cannot divide by zero at
      org.finos.legend.engine.plan.dependencies.util.Library.rem(Library.java:631)`.
    - **So the stamp's premise holds, asked of legend-engine itself:** a call outside any store query takes Pure's
      meaning and raises Pure's error; the same call inside the store query takes the database's (`round` half away
      from zero, `substring` and `indexOf` with no index adjustment).
12. **`isNumeric` as a `###Relational` dynaFunction on H2**, through the engine: `'123'` true, `'12.5'` true, `'-3'`
    true, `'+3'` false, `'1e5'` false, `'abc'` false, `'1,000'` false, `'１２'` false: the regular expression of C9,
    not `lower = upper` (which is true of `'+3'`, `'1,000'` and `'１２'`). The CSV seed stored the empty and the
    leading-blank rows with their quote characters, so those two were tested by direct SQL (C9) alone. The engine's
    SQL text was not captured: the runner does not print it.

## What surprised the measuring session, by weight

1. H2 2.5.252 raises on `acos`/`asin` out of range where legend-engine's 2.1.214 answers NaN: H2's version changes
   this answer.
2. H2's `DATEDIFF(WEEK)` follows the JVM's locale; only the tests' en_US pin makes it Pure's.
3. H2's `MOD` casts the dividend to the divisor's type: a Float `rem` with an Integer divisor is 3, not 2.5.
4. DuckDB's `date_diff('week')` is elapsed days over 7, not week boundaries.
5. legend-engine's one-argument `parseDecimal` is `decimal(5,2)` on H2 and `decimal(18,3)` on DuckDB.
6. Bare DECIMAL is scale 0 on H2 and scale 3 on DuckDB: `toDecimal(3.8)` is 4 and 3.800.
7. DuckDB's `split_part` with an empty separator splits into characters, where Pure's `split` drops empty tokens and
   reads a multi-character token as a set of characters.
