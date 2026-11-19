# Differences from legend-engine (draft)

*A scratch draft (census B2, 2026-11-19), to land with A3. Written from `docs/SEMANTICS_REGISTER.md` at `main` and the
rows the compatibility program adds (`compat-final-plan-2026-11-19.md` and the plain plan's "what we lose", on
`claude/compat-program`). Each entry is marked **measured** (with where) or **read** (from the pinned sources).*

**Who this is for:** a team bringing a legend-engine project to legend-lite.

**The one rule behind every entry:** lite follows Pure's own definitions. It matches legend-engine's formats, and its
answers wherever they agree with Pure. Where they do not, the difference is listed here, with what to write if you
need legend-engine's answer.

**When:**
- **today:** lite already behaves this way;
- **stage A:** the next few weeks;
- **stage B:** after stage A, once ratified.

## Where your answers can change

### 1. `substring` and `indexOf` count from 0, as Pure does
- **You write:** `'abcdef'->substring(1, 3)`; `'abc'->indexOf('b')`.
- **legend-engine** (on H2 and DuckDB) answers `'abc'` and `2`: the database's 1-based meaning, and 0 when the text is
  not found. Its corrected-substring option fixes `substring` only, never `indexOf`.
- **lite answers** `'bc'` and `1`, and `-1` when the text is not found.
- **Why:** Pure defines both 0-based, with `substring`'s end excluded (legend-pure
  `essential/string/slice/substring.pure:53`, `essential/string/index/indexOf.pure:39`).
- **To get legend-engine's answer:** `substring($a - 1, $a - 1 + $b)` for legend-engine's `substring($a, $b)`; and
  `indexOf(...) + 1`, which also turns Pure's -1 into legend-engine's 0.
- **You will be told.** The legend-engine door warns (a log line and a response header) whenever a query sent to the
  database calls either one. A setting makes it refuse instead. A report lists every such call in your project when it
  loads.
- **When:** stage A.
- **Measured:** legend-engine's corpus has 5 such calls (4 tests per database); lite's own 62 projects have none.
  Evidence: H12, the combined dry run, on `claude/compat-translator-spike`.
- **Register:** S130, S134 (rewritten), plus a new row.

### 2. The SQL you are shown is lite's own
- **You write:** `toSQLString(...)`, `planToString(...)`, a `toDDL` function, or read an execute result's
  `activities[].sql`.
- **legend-engine** prints its own SQL spelling.
- **lite prints** the SQL it actually runs, on all 12 databases it has a dialect for. Today it prints only H2's, DB2's
  and Composite's legend-engine text.
- **For a database lite has no dialect for** (DB2, Composite, Oracle, SQL Server and others), lite refuses by name:
  "SQL dialect for database type 'DB2' is not implemented yet".
- **Why:** SQL text is never promised. A test that checks SQL text is still judged, by running legend-engine's
  expected SQL and comparing answers.
- **To get legend-engine's answer:** compare results, not SQL strings.
- **When:** stage A.
- **Measured:** of legend-engine's 2,761 corpus tests, 147 on H2 and 152 on DuckDB stop counting, because they check
  only the text. That becomes 150 and 155 once item 1 lands. Every other SQL-text test passes by its answer.
  Evidence: `A2-roster.tsv` and F1's probe record.
- **Register:** a new row (CP-4).

### 3. Maths legend-engine leaves unchecked raises Pure's error

legend-engine writes no check for these, so the database answers. Lite raises Pure's error.

| You write | legend-engine answers | lite answers | To get legend-engine's answer |
|---|---|---|---|
| `[1,2,3,4,5]->slice(3, 2)` (DuckDB) | `[]` | Pure's error | `if($lo > $hi, \|[], \|$x->slice($lo, $hi))` |
| `5->rem(0)` (DuckDB) | `null` | Pure's error | `if($d == 0, \|[], \|$x->rem($d))` |
| `range(0, 5, 0)` (DuckDB) | `[]` | Pure's error | `if($step == 0, \|[], \|range(...))` |
| `1->bitShiftRight(63)` (DuckDB) | `0` | Pure's error | `if($n > 62, \|0, \|$x->bitShiftRight($n))`; `-1` for a negative `$x` |
| `sqrt(-2.0)` (H2, through the legend-engine server) | `null` | Pure's error | `if($x < 0, \|[], \|sqrt($x))` |
| `x / 0` (DuckDB, through the legend-engine server) | `null` | Pure's error | `if($d == 0, \|[], \|$x / $d)` |

- **The rewrites are per database.** On H2, `x / 0` already raises under legend-engine, so the guard would be wrong
  there. On DuckDB, `sqrt(-2.0)` already raises, likewise.
- **Why:** Pure defines these operations to raise (PCT, legend-pure's own suite, expects the error in every case).
- **When:** stage B.
- **Measured:** H8 (j), (j.3), (j.4). legend-engine's DuckDB divide answer is its own (B1-R's oracle); lite's DuckDB
  server route was not run. DuckDB 1.5.6 answers the unchecked divide with `Infinity`, so only the guard matches
  legend-engine there.
- **Register:** a new row (D2).

### 4. Float literals are floating point, not exact decimals
- **You write:** `1.1 + 0.6`.
- **legend-engine** (H2, DuckDB) answers `1.7`: it writes the literal bare, and the database computes it as a decimal.
- **lite answers** `1.7000000000000002`: a Float is a double.
- **Why:** Pure's Float is floating point.
- **To get legend-engine's answer:** write Decimal literals, `1.1D + 0.6D`, or use `toDecimal()`.
- **When:** stage B.
- **Measured:** 2 PCT answers move per database (PCT accepts both). 2 corpus tests on H2 start passing: the decimal
  printed `52` where the expected answer is `52.0`. Evidence: H8 (j), (j.4).
- **Register:** S20, S9.

### 5. `toOne()` of an empty collection raises
- **legend-engine** answers `null`: it writes no check.
- **lite raises an error**, as Pure defines ("Cannot cast a collection of size 0 to multiplicity [1]"). Today it is
  worded `Raised: SIZE_NOT_MULTIPLICITY`; Pure's words come with stage B's error formatter.
- **To get legend-engine's answer:** `$x->first()`.
- **When:** today.
- **Measured:** the pure/v1 net's `raise.toOneOfEmpty`, against legend-engine's own answer (B1-R's oracle).
- **Register:** a row to add.

### 6. `isNumeric`, `parseDecimal`, `parseJson` on unusual input
- **legend-engine on H2:**
  - `isNumeric` is "has no cased letter", so `' 1 '`, `''` and `'-'` count as numeric;
  - `parseDecimal` cuts to `DECIMAL(5,2)`, so `'123.450021'` becomes `123.45`;
  - `parseJson` patches concatenated objects.
- **lite** answers each Pure's way: numeric text by its full form, an exact decimal, a JSON parse.
- **When:** stage B.
- **Measured:** corpus: `isNumeric` 0 tests, `parseDecimal` 2 per database, `parseJson` 1 lost on DuckDB and 1 gained
  on H2. Evidence: H5 task 0.
- **Register:** S370, S365, plus a row to add (`parseJson`).

### 7. H2 runs in its regular mode
- **legend-engine** opens H2 with `MODE=LEGACY`, which allows comparing a yes/no column with a number.
- **lite** opens H2 with H2's own defaults. Lite writes every null ordering explicitly, so no ordering changes.
- **To keep the comparison working:** compare with `true` and `false`, not with 1 and 0.
- **When:** stage B.
- **Measured:** 1 corpus test (a mapping filter `active = 1` on a `BIT` column). Evidence: H8 (h).
- **Register:** S368.

### 8. legend-engine's feature flags
- **The corrected-substring flag** is read at the door and is always on in effect, because item 1 makes Pure's
  meaning the only one.
- **The legacy null-equality flag** is refused by name.
- **The enum push-down flag** is accepted and changes nothing, since lite always transforms enums in SQL.
- **The two flags lite has never implemented** stay refused by name, as today.
- **When:** stage B.
- **Read:** H8 (a.3). 7 corpus tests set a flag, and 1 needs the legacy flag; it checks SQL text only.
- **Register:** S130, plus a row to add.

### 9. Error messages
- **legend-engine's message** is often its database's own error over legend-engine's SQL.
- **lite gives** its own clear message, in legend-engine's error envelope and with the same status. Neither carries a
  source position on these routes.
- **When:** today; the words move to a formatter in stage B.
- **Measured:** the pure/v1 net (for example divide by zero: 500 on both, different words).

### 10. lite answers where legend-engine refuses
- **legend-engine** refuses some functions at planning ("No SQL translation exists ..." and the like).
- **lite answers them**, and checks those answers against PCT's expected values.
- **What it means for you:** a query written against lite may not run back on legend-engine.
- **When:** today.
- **Measured:** on DuckDB, 99 of legend-engine's 110 refused PCT tests answered and 109 wrong answers fixed; on H2,
  28 of 36. Evidence: B1-R's lane, H7.
- **Register:** a blanket row to add.

### 11. The last digit of some maths on Windows
- **What happens:** DuckDB and SQLite compute `sinh`, `cosh`, `tanh`, `cbrt` and `pow` with the operating system's
  maths library. On Windows, the last digit or two of a Double can differ from Linux.
- **Why:** this is the database's own platform, not lite's choice.
- **To avoid it:** compare such results with a tolerance.
- **When:** today.
- **Measured:** 30 lines in the edge net on Windows, all last digits; H2 matched. Evidence: PR #123's Windows run.

## Gaps lite has today (not differences by design)

- **H2 functions lite does not write yet** are refused by name, never answered wrongly: `greatest`, `least`, `mode`,
  max and min of dates, the bit operations, splitting a string into rows, cube root, reverse, and list positions
  (`at`, `toOne` over a list).
  - These are 33 PCT tests on H2, being added by the product track.
  - H2 also answers `sqrt(-1.0)` with NaN and wraps a 63-bit shift. Pure has no NaN, so no query can reproduce that,
    and lite does not write shifts on H2 at all.
  - **Measured:** B1-R's lane; H8 (j.4).
- **Not supported yet:**
  - legend-engine's mapping test suites and service tests;
  - cross-store queries;
  - external formats;
  - databases lite has no dialect for;
  - handing plans to legend-engine, or taking plans from it (deferred).

## What is the same

- **Your project** loads, compiles and runs as on legend-engine: models, mappings, stores, runtimes, connections.
- **The database computes every answer.** Only the output's formatting happens in Java: legend-engine's printed
  tables, envelopes and error sentences, written from lite's typed values.
- **The legend-engine server routes Studio uses** answer in legend-engine's own formats. That covers the result
  envelopes, and the printed tables, byte for byte in a prototype (431 of 431 tables on DuckDB, 342 of 342 on H2).
- **Studio's "generate plan" for an ordinary query** uses none of the code these changes remove (checked in the
  compiled code; none of the 15 recorded requests is affected). Only a query that itself asks for SQL text is
  affected, and that is item 2.
- **Tests that check SQL text** are still judged, by their answers, on legend-engine's own H2.
- **Floats are doubles**, as in legend-engine. A 20-digit Float literal loses digits in both (only legend-pure's
  interpreter keeps them; S120).
- **On legend-engine's own function suite (PCT)**, lite's legend-engine route passes more tests than legend-engine
  on DuckDB, H2 relation and H2 essential, and fewer on H2 standard:

  | | lite | legend-engine |
  |---|---:|---:|
  | DuckDB | 1,212 | 1,005 |
  | H2 relation | 448 | 411 |
  | H2 essential | 252 | 131 |
  | H2 standard | 113 | 134 |

  H2 standard is behind by the gaps above.
