# Differences from legend-engine

*Written 2026-11-19 by the compatibility program, from `docs/SEMANTICS_REGISTER.md` and the rows the program adds (its
plan: `docs/records/compat-final-plan-2026-11-19.md`). Each entry is marked **measured** (with where) or **read** (from
the pinned sources), and names its register rows. The register is the record; this page is its reading for a team
moving a project. A register id marked provisional is settled when the program's batch lands.*

**Who this is for:** a team bringing a legend-engine project to legend-lite.

**The one rule behind every entry** (the user's ruling, 2026-11-19, and AGENTS.md's upstream-fidelity tenet): "Lite
follows Pure. legend-engine's formats are matched, and its answers where they agree with Pure. The rest is recorded."
Where they do not agree, the difference is listed here, with what to write if you need legend-engine's answer.

**When:**
- **today:** lite behaved this way before the compatibility program;
- **stage A:** lands with this page, the compatibility program's first batch;
- **stage B:** after stage A, once the user ratifies it.

## Where your answers can change

### 1. `substring` and `indexOf` count from 0, as Pure does
- **You write:** `'abcdef'->substring(1, 3)`; `'abc'->indexOf('b')`.
- **legend-engine** (on H2 and DuckDB) answers `'abc'` and `2`: the database's 1-based meaning, and 0 when the text is
  not found. Its corrected-substring option fixes `substring` only, never `indexOf`.
- **lite answers** `'bc'` and `1`, and `-1` when the text is not found, with or without that option.
- **Why:** Pure defines both 0-based, with `substring`'s end excluded (legend-pure
  `essential/string/slice/substring.pure:53`, `essential/string/index/indexOf.pure:39`).
- **Out of range, lite raises Pure's error** in the database, where legend-engine passes the positions to SQL unchecked
  (H2 clamps past the end):
  - a start below 0;
  - an end past the length;
  - an end before the start.
- **To get legend-engine's answer:** `substring($a - 1, $a - 1 + $b)` for legend-engine's `substring($a, $b)`; and
  `indexOf(...) + 1`, which also turns Pure's -1 into legend-engine's 0.
- **You will be told:**
  - **The legend-engine door warns** whenever a query sent to the database calls either one: a log line, and a
    response header, `x-legend-lite-pure-meaning`.
  - **A server setting makes it refuse instead:** `LEGEND_LITE_PURE_MEANING=refuse`.
  - **When it is quiet:** for `substring`, when the request sets legend-engine's corrected option; `indexOf` always
    warns.
  - **A literal start of 0 is exempt** where it is measured equal to Pure's answer: on H2 both forms, on DuckDB the
    2-argument form only.
  - **A report lists every such call in your project when it loads,** and in your saved queries.
- **Lite's other languages keep SQL's meaning:** `#Cypher`, `#SQL`, `#IQ` and a `###Relational` mapping's own functions
  write SQL's `substring` and position by name. Nothing you write there changes.
- **When:** stage A.
- **Measured:**
  - Legend-engine's corpus: each database's fail roster gains the same 7 tests. Four are answers; three are SQL-text
    tests that the old `indexOf` hid.
  - PCT's 3 known `indexOf` failures pass.
  - Lite's own 62 projects have no such call.
  - Evidence: H12, the combined dry run, and A3's measures on `claude/compat-translator-spike`.
- **Register:** S130 and S134 (rewritten), S383 (provisional).

### 2. The SQL you are shown is lite's own
- **You write:** `toSQLString(...)`, `planToString(...)`, a `toDDL` function, or read an execute result's
  `activities[].sql`.
- **legend-engine** prints its own SQL spelling.
- **lite prints** the SQL it actually runs, on all 12 databases it has a dialect for. Before stage A it printed only
  H2's, DB2's and Composite's legend-engine text.
- **For a database lite has no dialect for** (DB2, Composite, Oracle, SQL Server and others), lite refuses by name:
  "SQL dialect for database type 'DB2' is not implemented yet".
- **Why:** SQL text is never promised. A test that checks SQL text is still judged, by running legend-engine's
  expected SQL and comparing answers.
- **To get legend-engine's answer:** compare results, not SQL strings.
- **When:** stage A.
- **Measured:** of legend-engine's 2,761 corpus tests, 147 on H2 and 152 on DuckDB stop counting, because they check
  only the text. That becomes 150 and 155 once item 1 lands. Every other SQL-text test passes by its answer. Studio's
  "generate plan" for an ordinary query answers byte for byte as before. Evidence: A2's corpus rosters, exact against
  `A2-roster.tsv`, and `server/PureV1SqlTextTest`.
- **Register:** S380 (provisional), which supersedes S4.

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
- **Register:** a row comes with stage B's step that removes the shortcuts (B4, the user's D2).

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
- **Register:** S384 (provisional).

### 6. `isNumeric`, `parseDecimal`, `parseJson` on unusual input
- **legend-engine on H2:**
  - `isNumeric` is "has no cased letter", so `' 1 '`, `''` and `'-'` count as numeric;
  - `parseDecimal` cuts to `DECIMAL(5,2)`, so `'123.450021'` becomes `123.45`;
  - `parseJson` patches concatenated objects.
- **lite** answers each Pure's way: numeric text by its full form, an exact decimal, a JSON parse.
- **When:** stage B.
- **Measured:** corpus: `isNumeric` 0 tests, `parseDecimal` 2 per database, `parseJson` 1 lost on DuckDB and 1 gained
  on H2. Evidence: H5 task 0.
- **Register:** S370, S365; `parseJson`'s row comes with stage B.

### 7. H2 runs in its regular mode
- **legend-engine** opens H2 with `MODE=LEGACY`, which allows comparing a yes/no column with a number.
- **lite** opens H2 with H2's own defaults. Lite writes every null ordering explicitly, so no ordering changes.
- **To keep the comparison working:** compare with `true` and `false`, not with 1 and 0.
- **When:** stage B.
- **Measured:** 1 corpus test (a mapping filter `active = 1` on a `BIT` column). Evidence: H8 (h).
- **Register:** S368.

### 8. legend-engine's feature flags
- **The corrected-substring flag** changes nothing, because item 1 makes Pure's meaning the only one. It only quiets
  the door's `substring` warning. **When:** stage A.
- **The legacy null-equality flag** is refused by name. **When:** stage B.
- **The enum push-down flag** is accepted and changes nothing, since lite always transforms enums in SQL. **When:**
  stage B.
- **The two flags lite has never implemented** stay refused by name, as today.
- **Read:** H8 (a.3). 7 corpus tests set a flag, and 1 needs the legacy flag; it checks SQL text only.
- **Register:** S130 (rewritten); the other flags' row comes with stage B's flag step (B5).

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
- **Measured on the corpus:** no runnable verdict moves (census B2's H8 (k)).
- **Register:** S385 (provisional).

### 11. The last digit of some maths on Windows and macOS
- **What happens:** DuckDB and SQLite compute their maths functions with the operating system's maths library. On
  Windows and on macOS, the last digit or two of a Double can differ from Linux, legend-engine's own platform for its
  results:
  - **Windows:** `sinh`, `cosh` and `tanh` (the same values on DuckDB and SQLite), `cbrt` and `pow`;
  - **macOS:** `acos`, `asin` and `cot` (DuckDB and SQLite), `cbrt` (DuckDB), `sinh` and `cosh` (DuckDB and SQLite),
    each exactly one unit in the last place.
- **H2** matched on both: it computes in Java.
- **Why:** this is the database's own platform, not lite's choice. Lite does not recompute the database's answer.
- **To avoid it:** compare such results with a tolerance.
- **When:** today.
- **Measured:** the edge golden net on each operating system. On Windows, 30 lines (17 named facts,
  `pct/src/test/resources/edge/overlay_windows.tsv`); on macOS, 31 lines (16 named facts, `overlay_macos.tsv`). No
  order, locale, path or line ending moved on either. Evidence: PR #123's runs on both, its overlays.
- **Register:** S382 (provisional).

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
