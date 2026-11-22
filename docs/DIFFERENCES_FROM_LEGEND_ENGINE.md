# Differences from legend-engine

*Written 2026-11-19 by the compatibility program, from `docs/SEMANTICS_REGISTER.md` and the rows the program adds (its
plan: `docs/records/compat-final-plan-2026-11-19.md`). Each entry is marked **measured** (with where) or **read** (from
the pinned sources), and names its register rows. The register is the record; this page is its reading for a team
moving a project. A register id marked provisional is settled when the program's batch lands.*

**Who this is for:** a team bringing a legend-engine project to legend-lite.

**The one rule behind every entry** is the goal the user ruled on 2026-11-19 (~13:25, "Okay agree with all"), now
AGENTS.md's upstream-fidelity tenet: lite follows Pure; legend-engine's formats are matched, and its answers where they
agree with Pure; the rest is recorded. Where they do not agree, the difference is listed here, with what to write if
you need legend-engine's answer.

**When:**
- **today:** lite behaved this way before the compatibility program;
- **stage A:** lands with this page, the compatibility program's first batch;
- **stage B:** after stage A, once the user ratifies it.

## Where your answers can change

### 1. `substring` and `indexOf` count from 0, as Pure does
- **Which meaning you get depends on the door your request comes through, never on a setting:** lite's own route
  always answers Pure's; legend-engine's door answers legend-engine's, reading legend-engine's corrected-substring
  option exactly as legend-engine does.
- **You write**, in a query sent to the database: `$p.name->substring(1, 3)` and `$p.name->indexOf('b')` inside a
  `project`, over names `'abcdef'` and `'abc'`.
- **legend-engine** (on H2 and DuckDB) answers `'abc'` and `2`: the database's 1-based meaning, and 0 when the text is
  not found. Its corrected-substring option fixes `substring` only, never `indexOf`.
- **At legend-engine's door, lite answers legend-engine's** `'abc'` and `2`, on H2 and DuckDB, with no setting (the
  user's ruling of 2026-11-20 ~07:55, "Delete the settings, make engine only mode for engine for everything"):
  - each `substring` and 2-argument `indexOf` in a query sent to the database is sent as the database's own SUBSTRING
    over the arguments as given, and its own position function;
  - `substr`, and `substring` under legend-engine's corrected option, are sent as the database's SUBSTRING over
    `start + 1` and `end - start`, as legend-engine's processSubstr writes them;
  - so out of range the answer is the database's too, cut short as legend-engine's is (`'FX'->substr(1, 3)` is `'X'`);
  - on any other database such a request is refused by name until legend-engine's answer is checked there;
  - a host-side `let`, which legend-engine computes before the query, keeps Pure's meaning;
  - an M2M chain's transforms, a `~func` mapping and a graph fetch tree's qualified property are refused by name: the
    door cannot reach them to rewrite them;
  - legend-engine refuses the 3-argument `indexOf`; lite answers it Pure's way (item 10).
- **On lite's own route**, lite answers `'bc'` and `1`, and `-1` when the text is not found, with or without that option.
- **Why:** Pure defines both 0-based, with `substring`'s end excluded (legend-pure
  `essential/string/slice/substring.pure:53`, `essential/string/index/indexOf.pure:39`).
- **Out of range, lite's own route raises Pure's error** in the database, where legend-engine passes the positions to SQL
  unchecked (H2 clamps past the end):
  - a start below 0;
  - an end past the length;
  - an end before the start.
- **To get legend-engine's answer on lite's own route**, in range only (a start of 1 or more, and an end within the
  text): `substring($a - 1, $a - 1 + $b)` for legend-engine's `substring($a, $b)`, and `substring($a - 1)` for its
  `substring($a)`. At a start of 0, H2 reads position 1 while the rewrite starts at -1 and raises Pure's error; past the
  end, H2 clamps while Pure raises. For `indexOf`, `indexOf(...) + 1`, which also turns Pure's -1 into legend-engine's
  0.

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
- **Measured:** of legend-engine's 2,761 corpus tests, 145 on H2 and 149 on DuckDB stop passing, as the merged batch's
  lanes measured before item 1 joined it: H2's fail roster goes 276 to 421, DuckDB's 108 to 257. Their kinds
  (`A2-roster.tsv`):
  - **text only:** 139 on H2 and 143 on DuckDB check only the SQL text;
  - **plan structure:** 6 on each check legend-engine's plan structure (a `supportsStream` flag on its plan's
    parameter-validation node);
  - **answers the text had hidden** (items 12 and 13) are not among them. The user ruled them legend-engine defects
    before (2026-09-05 and 2026-09-08), so they are accepted divergences again (the user, 2026-11-19 ~20:35). Four
    of batch 144's rows go back on the accepted rosters, and two H2 rows are new. The accepted rosters go H2 41 to
    44 and DuckDB 46 to 49. Each of those rows left its fail roster on the merged batch, so each witness matched.
- **With item 1 in the same batch**, each fail roster gains its 7 more. Three of them are SQL-text tests that the old
  `indexOf` hid, and four are item 1's answers. The expected totals are H2 276 to 428 (+152) and DuckDB 108 to 264
  (+156). The figure of record is the judges' measurement on the batch's final head, still pending.
- Every other SQL-text test passes by its answer.
- Studio's "generate plan" for an ordinary query answers byte for byte as before.
- Evidence: A2's corpus rosters, exact against `A2-roster.tsv`, and `server/PureV1SqlTextTest`.
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
- **Why:** Pure defines these operations to raise. PCT, legend-pure's own suite, expects the error for the first five
  rows.
- **For `x / 0`** PCT has no test, and legend-pure 5.99.0 raises for every zero divisor. Compiled legend-pure, the
  mode legend-engine runs Pure in and the one compatibility follows (S127), raises "Cannot divide 0 by zero"
  (`CompiledSupport.java:1481-1486`). The interpreter has no check of its own and fails on Java's arithmetic instead:
  with a Float or Decimal operand, `BigDecimal` raises "Division by zero" (`NumericAccumulator.java:893-896`); an
  Integer over the Integer 0 gives `Infinity` (`:1562-1565`, then `:1156-1161`), which `Divide.java:56` cannot make a
  Pure Float (a `NumberFormatException`, `ModelRepository.java:547-550`). lite raises its own division-by-zero error.
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

### 5. `toOne()` of an empty collection raises on lite's own doors
- **legend-engine** answers `null`: it writes no check (its `processNoOp`), and an empty collection is SQL's `null`.
- **lite raises an error** on its own doors, as Pure defines, in lite's own sentence ("collection size does not match
  the multiplicity: [1]"). At legend-pure's and legend-engine's doors it reads legend-pure's sentence ("Cannot cast a
  collection of size 0 to multiplicity [1]"), worded from the raise's record after the run (stage B's B1,
  2026-11-20); a size only the database counted prints `<computed>` there (register S389).
- **At legend-engine's door** (the user, 2026-11-20 ~13:10, "toOne yes needs to be fixed for engine"), a database-side
  `toOne()` of a value that is always empty (`[]`, `[]->cast(@Integer)`) answers legend-engine's `null` on H2 and
  DuckDB; on a database where legend-engine's answer is not yet checked, the request is refused by name. A `toOne()` of
  a value read from a row already answers `null` on both doors (lite lets a row's value through unchecked, as
  legend-engine does). Two cases keep lite's raise at that door:
  - a literal list of two or more, which legend-engine refuses at planning with Pure's sentence;
  - a value computed in the query that is empty at run time but not always empty. legend-engine's answer for it is not
    yet asked.
- **To get legend-engine's answer on lite's own doors:** `$x->first()`.
- **When:** today.
- **Measured:** the pure/v1 net's `raise.toOneOfEmpty`, against legend-engine's own answer (B1-R's oracle): at
  legend-engine's door both answer `null`.
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
- **The corrected-substring flag** is read at legend-engine's door only, as legend-engine reads it: without it,
  `substring` is sent with its positions as given; with it, as legend-engine's corrected form (item 1). Lite's own
  route ignores it: `substring` is Pure's there. **When:** stage A.
- **The legacy null-equality flag** is refused by name. **When:** stage B.
- **The enum push-down flag** is accepted and changes nothing, since lite always transforms enums in SQL. **When:**
  stage B.
- **The two flags lite has never implemented** stay refused by name, as today.
- **Read:** H8 (a.3). 7 corpus tests set a flag, and 1 needs the legacy flag; it checks SQL text only.
- **Register:** S130 (rewritten); the other flags' row comes with stage B's flag step (B5).

### 9. Error messages
- **legend-engine's message** is often its database's own error over legend-engine's SQL.
- **lite gives,** in legend-engine's error envelope and with the same status, wherever lite answers legend-engine's
  API (lite's server, and the Python host's `pure/v1` execute):
  - for an error lite raises at run (division by zero, `toOne` on the wrong size, a bad date, ...) and for a kinded
    compile error, legend-pure's sentence, worded from the error's record after the run (stage B's B1, 2026-11-20). A
    value only the database held prints `<computed>` ("Cannot divide <computed> by zero" where the dividend is a
    column; register S389);
  - for any other compile error, lite's own sentence; for a database's own error, the database's words.
  - Neither side points into your code (legend-engine's can carry a position in legend-engine's own Pure).
  - lite's own screens (the browser tab, lite's own API, the Python library) give lite's own sentence, as they do for
    every error, including where the tab runs a plan it asked a legend-engine server for.
  - A query's accessor over a database the model does not hold reads "Can't find database", legend-engine's words for
    a mapping's reference, where legend-engine says "The store … can't be found." (register S391, proposed).
- **When:** stage B's B1.
- **Measured:** the pure/v1 net (for example divide by zero: 500 on both, different words); legend-engine wraps a
  check its Pure plans fail as a `PureAssertFailException` with the assert's location, and refuses some at planning in
  its own words (`%2017->hour()`: "Ensure the target system understands Year or Year-month semantic.").
- **Register:** S390 (proposed), S389 (`<computed>`), S391 (proposed).

### 10. lite answers where legend-engine refuses
- **legend-engine** refuses some functions at planning ("No SQL translation exists ..." and the like).
- **lite answers them**, and checks those answers against PCT's expected values.
- **What it means for you:** a query written against lite may not run back on legend-engine.
- **When:** today.
- **Measured:** on DuckDB, 99 of legend-engine's 110 refused PCT tests answered and 109 failures fixed; on H2,
  28 of 36. Evidence: B1-R's lane, H7.
- **Measured on the corpus:** no runnable verdict moves (census B2's H8 (k)).
- **A close case, where legend-engine accepts the query but computes no value:** a `graphFetch` whose `serialize`
  tree names a qualified property the fetch tree lacks.
  - legend-engine writes that property as `null`, or fails partway through the response.
  - lite computes the property from its definition, as for any qualified property.
  - This is on main today; A3 found it.
  - Ruled by the user on 2026-11-19 (~23:20, "Yes to A"): recorded as a difference, not refused.
  - Register: S388 (provisional id).
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

### 12. `joinStrings` with a separator joins with it, as Pure does
- **You write**, in a query sent to the database: `[$r.firstName, ' ', $r.lastName]->joinStrings('|')`, or with a
  prefix, separator and suffix, `->joinStrings('[', ',', ']')`.
- **legend-engine's SQL** concatenates the separator, and the prefix and suffix, as further elements:
  `concat(first, ' ', last, '|')`. It answers `'Anthony Allen|'`, and `'Anthony Allen[,]'` for the 3-part form.
- **lite answers** Pure's: `'Anthony| |Allen'` and `'[Anthony, ,Allen]'`.
- **A hash over such a join differs with it** (`->joinStrings('|')->hash(HashType.MD5)`).
- **Why:** Pure's `joinStrings` puts the separator only between the strings (legend-pure
  `essential/string/toString/joinStrings.pure:18-21`).
- **To get legend-engine's answer:** concatenate yourself, for example `$first + ' ' + $last + '|'`.
- **Ruled:** by the user on 2026-09-05, as a legend-engine defect ("quarantine/bucket those as engine bugs"), and the
  accepted divergence confirmed on 2026-09-08 (`docs/GATES.md`, batches 67b and 144).
- **When:** today. From 2026-09-21 until A2 the corpus test passed on its SQL text alone. Since A2 its rows are
  compared again.
- **Measured:** the corpus's `testToSQLStringForTDSStringJoin` and `testHashFunctions`, on H2 and DuckDB (class b of
  `A2-roster.tsv`). With this batch they are accepted divergences, on the accepted rosters (item 2).
- **Register:** S386 (provisional).

### 13. `firstDayOfWeek` is the Monday on every database, as Pure defines it
- **Pure defines it** as the most recent Monday (legend-engine's own `core/pure/corefunctions/dateExtension.pure:202`,
  `mostRecentDayOfWeek(DayOfWeek.Monday)`).
- **legend-engine on H2 answers the first day of the week in the Java locale H2 runs under.** It writes
  `date_trunc('week', ...)`, and H2 starts that week on the locale's first day.
  - Under `en_US` that is the Sunday: `2014-11-30` for Thursday `2014-12-04`.
  - Under `en_GB` or `fr_FR` it is the Monday, `2014-12-01`.
- **legend-engine on DuckDB answers the Monday**, as lite does.
- **lite answers the Monday** on every database, whatever the locale.
- **Ruled:** by the user on 2026-09-05, as a legend-engine defect ("quarantine/bucket those as engine bugs"), and the
  accepted divergence confirmed on 2026-09-08 (`docs/GATES.md`, batches 67b and 144).
- **When:** with this batch (A5's H2 fix in #127; before it lite's H2 followed the locale too). From 2026-09-21 until
  A2 the corpus test passed on its SQL text alone; since A2 its rows are compared again.
- **Measured:**
  - The corpus's `testToSqlGenerationFirstDayOfWeek` on DuckDB (class b of `A2-roster.tsv`). Its rows were appealed
    against legend-engine's H2 under `en_US`: `2014-11-30` there, `2014-12-01` from lite.
  - With this batch it is an accepted divergence on both databases (item 2). On H2 the merged batch's lane accepts
    it with legend-engine's answer as the witness, `2014-11-30`. That lane ran before item 1 joined; item 1 does not
    touch this test. The judges' measurement on the final head is the figure of record, still pending.
  - Both databases were asked directly under `en_US`, `en_GB` and `fr_FR`
    (`docs/records/compat-homework/A3-piece4/first-day-of-week-probe.md`, on `claude/census-b2`).
- **Register:** S387 (provisional).

### 14. A table's text used inside a query (stage B)
- **You write** a relation's `toString()` or `toCSV()`.
- **Where that text is the program's last step** (an assert's side; every PCT test), compatibility prints legend-pure's
  text from lite's rows, as legend-pure itself does.
- **Where the text is used further** inside the query (`->toString()->length()`), the database computes lite's own
  text:
  - a whole Float prints `2.0`;
  - a date with a time prints `2000-10-25T06:30:00.000000Z`, where legend-pure prints `+0000`;
  - an empty cell prints `null`.
- **`toCSV` too:** lite's route prints a relation's `toCSV` in the database, a standard CSV layout with lite's own
  value texts. Lite keeps the `toCSV` name. Compiling legend-engine's own `toCSV` program instead waits on four gaps
  in lite (H15 §11).
- **Ruled:** by the user on 2026-11-19: ~18:05, "Agree with all four table printing recommendations"; ~18:12, "Yes
  record it, and count the nested cases"; ~18:36, "Null is good"; ~20:05, "toCSV yes toString or another easy
  database sql that makes sense".
- **When:** stage B (B2).
- **Measured:** count pending; the nested cases are counted before B2 is built.
- **Register:** a row comes with B2.

### 15. Two date-times at the same instant are equal, whatever their written precision (item 3, stage B)
- **You write** `assertEquals(%2020-01-01T10:00:00.000, %2020-01-01T10:00:00)`, or compare a literal written to the
  second with a timestamp column's value.
- **legend-pure** says not equal: a date's written precision is part of its value (`AbstractPureDate.equals`).
  **legend-engine** reads every timestamp back at nine subsecond digits, so a literal written to the second fails
  against its own column (73 expected failures in its relational PCT manifests). **read**
- **Lite's product** compares a date by its kind and its instant, as the database does: equal. A day is never a
  date-time, and a year never a year-month, as in Pure. **measured** (`AssertBarTest`)
- **On legend-engine's and legend-pure's doors** lite keeps Pure's answer (the host judge's compatibility forms).
- Register S399.

## Gaps lite has today (not differences by design)

- **H2 functions lite does not write yet** are refused by name, never answered wrongly: `greatest`, `least`, `mode`,
  max and min of dates, the bit operations, splitting a string into rows, cube root, reverse, and list positions
  (`at`, `toOne` over a list).
  - These are 33 PCT tests on H2 in the pinned suites (Essential 6, Standard 27); Unclassified, measured once, adds 10.
    The product track is adding them.
  - H2 also answers `sqrt(-1.0)` with NaN and wraps a 63-bit shift. Pure has no NaN, so no query can reproduce that,
    and lite does not write shifts on H2 at all.
  - **Measured:** B1-R's lane; H8 (j.4).
- **`weekOfYear` and `dateDiff(..., DurationUnit.WEEKS)` on H2 follow the Java locale.** lite writes H2's
  `extract(WEEK ...)` and `DATEDIFF(WEEK, ...)`, and H2 starts their week on the locale's first day: Sunday
  2014-11-30 is week 49 under `en_US` and week 48 under `en_GB`.
  - lite's tests pin `en_US` (`tools/junit/defs.bzl:52-54`). The product server (`//core:server`) pins no locale, so
    it answers by its machine's.
  - A defect, for the product track. Measured: `first-day-of-week-probe.md` (item 13).
- **Not supported yet:**
  - legend-engine's mapping test suites and service tests;
  - cross-store queries;
  - external formats;
  - databases lite has no dialect for;
  - handing plans to legend-engine, or taking plans from it (deferred).

## What is the same

- **Your project** loads, compiles and runs as on legend-engine: models, mappings, stores, runtimes, connections.
- **The database computes every answer.** After stage B, only the output's formatting happens in Java:
  legend-engine's printed tables, envelopes and error sentences, written from lite's typed values. Today a printed table
  is still built in SQL, and an error reads lite's own code (item 5).
- **The legend-engine server routes Studio uses** answer in legend-engine's own result envelopes (the pure/v1 golden
  net).
- **A printed table** whose text is the program's last step matches legend-pure's byte for byte in stage B's
  prototype: 431 of 431 tables on DuckDB, 342 of 342 on H2. A table's text used further is item 14.
- **Studio's "generate plan" for an ordinary query** uses none of the code these changes remove (checked in the
  compiled code; none of the 15 recorded requests is affected). Only a query that itself asks for SQL text is
  affected, and that is item 2.
- **Tests that check SQL text** are still judged, by their answers, on legend-engine's own H2.
- **A Float value read from the database is a double**, as in legend-engine (a Float literal is item 4). A 20-digit
  Float literal loses digits in both (only legend-pure's interpreter keeps them; S120).
- **On legend-engine's own function suite (PCT)**, measured before stage A, lite's legend-engine route passes more
  tests than legend-engine on DuckDB, H2 relation, H2 essential and H2 grammar, and fewer on H2 standard and H2
  unclassified:

  | | lite | legend-engine |
  |---|---:|---:|
  | DuckDB | 1,212 | 1,005 |
  | H2 relation | 448 | 411 |
  | H2 essential | 252 | 131 |
  | H2 standard | 113 | 134 |
  | H2 grammar (measured once, not pinned) | 119 | 77 |
  | H2 unclassified (measured once, not pinned) | 73 | 82 |

  H2 standard and unclassified are behind by the gaps above. Item 1 moves 12 more PCT tests to PCT's answer: 7 on DuckDB
  and 5 on H2.
