# Audit: SQLite date-time held as one text per instant (`9adb84bf4..origin/claude/sqlite-datetime-form`)

**Verdict: BLOCKER 0, SHOULD-FIX 6** (plus pre-existing notes, which this change did not cause).

## Method

I compiled `core/src/main/java/com/legend/sql/**` and `base/` at the audited revision (into `audit-sqlite/dcls`) and at the
base `9adb84bf4` (into `bcls`) with plain `javac`. The SQL package depends on nothing else. I built the MIR by hand,
rendered it with `new SQLite().render(...)`, and ran the SQL on SQLite 3.53.1 (`sqlite-jdbc-3.53.1.0`). I ran DuckDB 1.5.6
(`duckdb_jdbc-1.5.6.0`) beside it where a reference answer was needed.

A "run-time" value was made by rendering a typed `Column` and replacing it in the text with a quoted value. The dialect
sees a column, not a `StringLit`, so the SQL path is taken. Every output quoted below is what the database returned.

The scratch programs are in `/tmp/claude-0/-home-user-legend-lite/37e604d0-f16d-5c80-a65e-84a7c2121e8e/scratchpad/audit-sqlite/`:
- `h/H.java`: the harness.
- `h/Cases.java`: Q2.
- `h/Q1.java`, `h/Q3.java`, `h/Q3b.java`, `h/Q4.java`, `h/Q5.java`, `h/Q6.java`, `h/Q7.java`, `h/Q8.java`, `h/Q9.java`,
  `h/QL.java`: one per question.
- `h.sh new|base <method>`, `run.sh sqlite|duck <file.sql>`: the runners.
- `q2.new.txt` / `q2.base.txt`, `q4.new.txt` / `q4.base.txt`: the raw outputs.

---

## Q1. The core claim: holds for 0000-9999

`Q1.java` built 4,015 instants: 15 edge cases, plus 4,000 random ones. Half of the random ones fall across 0000-9999 and
half within 5 seconds of each other, so there are many near-ties. Their fractions have 0 to 9 digits. Each was written as
a literal with nine digits through `timestampLit`. SQLite then compared every pair.

Result: **16,120,225 pairs; order mismatches 0, equality mismatches 0; no non-canonical text** (no trailing zero, no bare
point, no `T`).

The edge cases were:
- a whole second against `.000000001`, `.05`, `.1`, `.25`, `.5`;
- `00:00:59.999999999` against `00:01:00`;
- `2013-12-31 23:59:59.9` against `2014-01-01 00:00:00`;
- `0999-12-31 23:59:59.9` against `1000-01-01`;
- `0000-01-01` and `9999-12-31 23:59:59.999999999`.

`.1` and `.10` cannot both exist: `timestampLit` strips trailing zeros.

**Not covered: years outside 0000-9999.** These are silently mis-ordered, never refused (the note says so). Measured:
- `'-2-06-01 00:00:00' < '-1-06-01 00:00:00'` is 0;
- `'10000-01-01 00:00:00' > '9999-12-31 23:59:59'` is 0.

Equality across spellings of a wide year is not one text either:
- the literal `+10000-...` is respelled `10000-...`;
- a text `+10000-...` read at run keeps its `+`;
- a literal at year 0000 shifted by an offset gives `-1-12-31 ...` in Java but `-001-12-31 ...` from SQLite's `strftime`.

The parameter path refuses years outside 1-9999 (`PlanParameters.dateTime`). Rare and documented.

## Q2. Is every source canonical? Yes, for every source lite writes from canonical inputs

| source | SQL written (abridged) | whole s | .5 | .123456789 | .100 | pre-1970 | midnight |
|---|---|---|---|---|---|---|---|
| `timestampLit` | Java at planning: `'2014-01-01 10:20:30.1'` | yes | yes | yes | yes (`.1`) | yes | yes |
| DateLit cast to TIMESTAMP | `('2014-01-01' \|\| ' 00:00:00')` | n/a | n/a | n/a | n/a | yes | yes |
| NOW | `rtrim(rtrim(strftime('%Y-%m-%d %H:%M:%f','now'),'0'),'.')` | yes (`...:46.322` measured; the trim covers `.000`) | | | | | |
| FROM_EPOCH_SECONDS | `datetime(n,'unixepoch')` | yes | n/a | n/a | n/a | yes | yes |
| FROM_EPOCH_MS | `shortest(strftime(...%f, ms/1000.0,'unixepoch'))` | yes | yes | n/a (ms) | yes | yes (`-14182939750` gives `1969-07-20 20:17:40.25`; `-1` gives `.999`) | yes |
| DATE_TRUNC DAY/HOUR/MINUTE/SECOND | `strftime('%Y-%m-%d %H:00:00', t)` and similar | yes | yes | yes | yes | yes | yes |
| DATE_TRUNC MILLISECOND | `shortest(strftime(...%f, t))` | yes | yes | yes (`.123`) | yes | yes | yes |
| DATE_TRUNC YEAR/QUARTER/MONTH/WEEK | `strftime('%Y-%m-01', t)` and similar | a DAY text (`2014-01-01`), as before: the base's contract, a Date | | | | | |
| ADD_INTERVAL YEAR/MONTH | `datetime(substr(t,1,19), printf('%+d months',n), 'floor') \|\| substr(t,20)` | yes | yes | yes | yes | yes | yes |
| ADD_INTERVAL WEEK..SECOND (new path) | `(datetime(unixepoch(substr(t,1,19)) + n*U, 'unixepoch') \|\| substr(t, 20))` | yes | yes | yes (all 9 digits kept; the base cut to 6) | yes | yes | yes |
| ADD_INTERVAL MILLI/MICROSECOND | `fromMicros(micros(t)+n*U)` = `shortest(strftime('%Y-%m-%d %H:%M:%S.', (us-frac)/1e6,'unixepoch') \|\| printf('%06d',frac))` | yes | yes | yes, but digits 7-9 dropped (`.123456789` + 1 ms gives `.124456`; pre-existing) | yes | yes | yes |
| ADD_INTERVAL NANOSECOND | refused by name | n/a | | | | | |
| TIME_BUCKET whole-second unit, constant size > 0 (new path) | `datetime(((m - ((m % W + W) % W)) + (origin)), 'unixepoch')`, `m = unixepoch(substr(t,1,19)) - origin` | yes | yes | yes | yes | yes | yes |
| TIME_BUCKET non-constant size / ms | `fromMicros(...)` | yes | yes | yes | yes | yes | yes |
| TIME_BUCKET MONTH/YEAR | `date('1970-01-01', printf(...)) \|\| ' 00:00:00'` | yes | yes | yes | yes | yes | yes |
| MAKE_TIMESTAMP | `shortest(printf('%04d-%02d-%02d %02d:%02d:%09.6f', ...))` | yes | yes | rounded to 6 digits (`.123457`; pre-existing) | yes | yes | yes |
| DATE_AT_TIME | `shortest(d \|\| ' ' \|\| tm \|\| substr('.', 1 + (instr(tm,'.') > 0)))` | yes | yes | yes | yes | n/a | yes |
| parseDate, constant | Java at planning | yes | yes | yes | yes | yes | yes |
| parseDate, column | `instantOf(...)` at run | yes | yes | yes | yes | yes | yes |
| cast VARCHAR to TIMESTAMP / TIMESTAMPTZ (USER constant, DECLARED constant, column) | Java, or `instantOf` / `instantRead(utcOf(...))` | yes | yes | yes | yes | yes | yes (offsets `Z`, `+01:00`, `+0100`, `-0530` applied) |
| JSON (Variant) to TIMESTAMP | `instantOf(... ->> '$')` | yes | yes | n/a | yes | yes (with `Z`) | n/a |
| DATE_TIME placeholder | `rtrim(rtrim(CAST(? AS TEXT),'0'),'.')` | yes | yes | yes | yes | yes | yes |
| Date placeholder | `(substr(CAST(? AS TEXT),1,19) \|\| rtrim(rtrim(substr(CAST(? AS TEXT),20),'0'),'.'))` | yes | yes | yes | yes | n/a | yes; a day stays `2014-01-10` |
| judge arm, a typed value | as held (a day becomes its midnight) | yes | yes | yes | yes | | yes |
| judge arm, a text | `shortest(coalesce(strftime(...), WIDE) \|\| '.' \|\| d)` | yes | yes | yes | yes | | yes |

Sources that can emit a non-canonical text:
- **None from canonical input written by lite.** `strftime` truncates in 3.53.1 and never rounds: `23:59:59.9999` stays
  `23:59:59`, measured. So reading a text through `strftime` cannot roll it into the next second.
- `MAKE_TIMESTAMP` with seconds 59.9999999 gives `10:20:60`. This is pre-existing: the base gave `10:20:60.`, so the new
  code at least removes the bare point.
- `DATE_AT_TIME` with a TIME text that has no seconds (`10:20`, only possible from a store's own TIME column) gives
  `2014-01-01 10:20`.
- `instantOf` keeps more than 9 digits when they come from a run-time text: `'...30.1234567891'` is held with 10. This is
  as before: S181 is not enforced on the product read, and the judge refuses it. Order and equality still agree with the
  instant.
- Every source that is not canonical when its input is not (a store's own column, `T`, `.500`) stays not canonical. That
  is the recorded gap.

## Q3. Arithmetic: correct

- **adjust, seconds to weeks.** Checked:
  - n = ±1 on all six probe values;
  - -86401 s from `1970-01-01 00:00:00.5`, which gives `1969-12-30 23:59:59.5`;
  - +1 h from `1969-12-31 23:30:00.75`, which gives `1970-01-01 00:30:00.75`;
  - -36500 days with 9 digits;
  - +1 s on a day text, which gives `2014-01-01 00:00:01`;
  - `9999-12-31 23:59:59.5` + 1 s, which raises `DATE_PAST_YEARS` as before.

  All equal DuckDB's answers. `substr(t,20)` is `''` for a whole second. The fraction is carried correctly, because the
  floored second plus a positive fraction is the instant, so negative n and values before 1970 are fine.
- **adjust, months and years.** `2020-01-31 +1 month` gives `2020-02-29`. `2020-03-31 23:59:59.999999999 -1 month` gives
  `2020-02-29 ...`. `2020-02-29 +1 year` gives `2021-02-28`. `-4 years` gives `2016-02-29`. `2019-01-31 +13 months` gives
  `2020-02-29`. All equal DuckDB.
- **timeBucket, seconds path.** The floor is correct for values before 1970: `((m % W) + W) % W`. 7 hours on
  `1969-07-20 20:17:40.25` gives `19:00:00`, and on `2014-01-01 00:00:00` gives `2013-12-31 20:00:00`. Both equal DuckDB's
  `time_bucket(..., origin 1970-01-01)`. The fraction is dropped. The output is identical to the base on every probe.
- **dateDiff, hours/minutes/seconds.** `floor(floor(us/1e6)/k) = floor(us/(1e6·k))`, so the new formula is the base's
  exactly. On nine pairs with fractions straddling a boundary and before 1970 (for example `1969-12-31 23:59:58.5` to
  `23:59:59.4` gives second = 1, and `1900-03-01 00:00:00.5` to `1900-02-28 23:59:59.5` gives -1 at every unit), new =
  base = DuckDB at all five units. No finding.

## Q4. Spelling a constant text at planning (commit 1fc6f4955)

### (a) Does the Java path give what the SQL path gives? Mostly; 4 shape families diverge

`Q4.java` ran 70 texts: parseDate of a constant against parseDate of a column, and a TIMESTAMP and a TIMESTAMPTZ cast of
each. All of these agreed:
- `T` against a space, no seconds, an hour alone;
- `Z`, `z`, `+0000`, `+00:00`, `-0530`, `-05:30`;
- a fraction of 1 to 10 digits, and 10 digits ending in zero;
- `24:00:00`, second 60, Feb 30, Feb 29 in a non-leap year, month 13, month 0, day 0, hour 25;
- leading or trailing spaces, a tab;
- unpadded `2014-1-1`, a year of 2 digits, years 0000 and 0001, 9999 with nine digits;
- a 5-digit year, `-0001`, an empty text, `abc`;
- offsets that cross midnight and 1970.

Agreement holds by construction for the invalid shapes: whenever Java refuses or is unsure, it returns null and the SQL is
written.

Divergent, measured (constant answer first, column answer second):

| text | constant (Java) | column (SQL) |
|---|---|---|
| `2014-01-01T10Z` (an hour, then `Z`) | `2014-01-01 10:00:00` (TS cast, and TZ cast: the literal parseDate route) | parseDate: `2014-01-01 10Z00:00` (garbage); TS cast: `2014-01-01 10Z00:00` |
| `2014-01-01T10:20:30+23:59` (any offset of 15:00 to 23:59, which SQLite does not read) | TS: `2013-12-31 10:21:30` | TS cast: `2014-01-01 10:20:30` (the offset silently dropped by the fallback); TZ cast: `2013-12-31 10:21:30` |
| `0000-01-01T00:30:00+01:00` | `-1-12-31 23:30:00` | `-001-12-31 23:30:00` |
| `9999-12-31T23:30:00-01:00` | `10000-01-01 00:30:00` | TS cast: `9999-12-31 23:30:00` (the offset dropped); TZ cast: NULL |

The first row is reachable through the product: `parseDate('2014-01-01T10Z')` on a constant takes the lowering's zoned
route (`TIMEZONE(UTC, CAST(lit AS TIMESTAMPTZ))`), while `parseDate($r.col)` holding the same text takes `PARSE_DATE(col)`.

**SHOULD-FIX 3:**
- `PARSED_LITERAL` should accept only the offsets the SQL path reads: |hh| ≤ 14, and `Z` only after minutes (or
  `instantRead` should learn `HHZ`).
- `constantInstant` should return null when the offset moves the year outside 0000-9999.
- Add a test that renders every shape both ways and compares the two.

### (b) What Java now evaluates, for the owner to rule on

For a `StringLit` under `PARSE_DATE`, or under a USER cast to TIMESTAMP or TIMESTAMPTZ, the dialect at planning:
1. strips leading and trailing spaces;
2. matches the text against a grammar: a year of 1 to 4 digits, a month and day of 1 to 2 digits, then optionally
   `[ T]HH[:MM[:SS[.f{1,9}]]]` and `Z|±HH[:]MM`;
3. validates the calendar date with `LocalDate.of` (month lengths and leap years), the time of day with `LocalTime.of`
   (hour ≤ 23, minute and second ≤ 59), and the offset with `LocalTime.of` (hour ≤ 23, minute ≤ 59);
4. pads the date fields;
5. in `timestampLit`, also reached by any `TimestampLit` carrying an offset:
   - pads a missing minute or second;
   - **applies the offset by calendar arithmetic** (`LocalDateTime.minusSeconds`), which can cross a day, month or year;
   - respells the year (four digits, or `Integer.toString` outside 0000-9999);
   - strips the fraction's trailing zeros and drops `Z`.

So Java parses a date text, judges whether it is a real date, and shifts an instant by an offset. Each of these is
"date text ... dates" in the fold rule's list.

What it is not: it never reads a row. It only touches a constant written in the query, the same class of work as spelling
a literal. The fallback to SQL keeps the refusal with the database: an invalid text is still raised by the statement.

What it saves, measured on one call:
- parseDate of a constant: 954 characters on the base, 36 now;
- the zoned constant route: 1,443 on the base, 36 now.

The run-time path for a column got larger:
- parseDate: 936 on the base, 1,864 now;
- zoned: 1,425 on the base, 2,082 now.

See SHOULD-FIX 6.

## Q5. The parameter: correct

Bound `'uuuu-MM-dd HH:mm:ss.SSSSSSSSS'` texts. The DateTime placeholder `rtrim(rtrim(CAST(? AS TEXT),'0'),'.')` gives:
- `.500000000` → `.5`;
- `.000000000` → whole;
- `10:20:00.000000000` → `10:20:00` (the second's zeros are safe because the point is always present);
- `2014-01-10 10:20:30.000000000` → `...:30`;
- 9 digits kept.

The Date placeholder gives:
- the day `2014-01-10` → `2014-01-10` (not trimmed to `2014-01-1`);
- `2010-10-10` → unchanged;
- date-times trimmed as above.

`= '2014-01-01 10:20:30.5'` is 1 exactly for the `.5` bind. The DateTime form is safe only because
`PlanParameters.dateTime` admits only a `LocalDateTime` for a DateTime parameter. The Date form binds the parameter twice
per use (4 binds for 2 uses), which is correct.

## Q6. The judge's arm

Every typed value lite makes is now equal under the judge to a text of the same instant. Measured: a literal `.500`, a
seconds adjust, a TZ cast of a column, a day in a Date slot, against texts `.500`, `.5`, `.500000000`, `11:20:30.5+01:00`,
`.5Z`, `.500+0000`. All are 1.

**SHOULD-FIX 1 (judge parity):** a TIMESTAMP-typed value lite did not write is now compared as held. A store's own column
holding `2014-01-01 10:20:30.500` or `2014-01-01T10:20:30.5`, compared with any of the texts above:

| | judge's equality | typed side read as |
|---|---|---|
| base | 1 | `...30.500000000` |
| new | **0** | `...30.500` and `2014-01-01T10:20:30.5` |

The host judge reads the same cell as a `LocalDateTime` and would say equal. So on such a column the two judges now
disagree, against G2 and tenet 5. Before the change both judges agreed.

I could not determine whether any test-road table on SQLite is filled other than by lite's loaders (for example raw setup
SQL). If none is, the risk is latent.

Fix: either keep the full reading for a typed value whose origin is a physical column the plan did not load, or record
the verdict change in the register and pin it with a test.

## Q7. Readers that assumed a fixed width: right for 19-character and fractional texts

Each reader was run on `2014-01-01 10:20:30`, `.5`, `.123456789`, `1969-07-20 20:17:40.25`, a day, and a store's
`T...500`. The output is **identical to the base** in every case:
- `micros` and `fraction` now read from character 21 (`EPOCH_MS` 1388571630000/…500/…123; pre-1970 -14182939750);
- hour and second extraction;
- STRFTIME with `%g`/`%f`/`%n` (`...30000000000000000000` for a whole second, `...500500000500000000`);
- TIME of a date-time (`substr(t,12)`);
- text of a date-time.

Also checked:
- `julianday` (the RANGE key) is width-agnostic.
- Hash, toVariant (`json_quote`) and map keys read the text, so they now see one text per instant, an improvement.
- Character 21 is wrong only for texts lite does not write: a 5-digit or negative year, where `unixepoch` is NULL anyway,
  and a store's text with an offset and no fraction. For example `...30+05:00` now adds 5 µs in `micros`, where the base
  used `instr('.')` and added 0. This is store-gap only.

A new TIME asymmetry: `timeLit` now trims (`%10:20:30.500` becomes `'10:20:30.5'`), but a TIME written as text by a
non-ISO loaded cell (a DECLARED cast) or a store's TIME column is not trimmed. A text `10:20:30.500` that matched the
literal on the base no longer does. This is minor; it falls under the store gap.

## Q8. The tests

Both tests run SQL on SQLite:
- `SqliteDateTimeTextOrderTest` uses `jdbc:sqlite::memory:` and SQLite only.
- `DateTimeInstantSideBySideTest` runs through `Lean.run` on `TestBackends` for SQLite and for DuckDB.

**SHOULD-FIX 4:**
- Since 1fc6f4955, every "text read as a date-time" writing in `SqliteDateTimeTextOrderTest`
  (`Cast(StringLit, TIMESTAMP, USER)`) is spelled by Java at planning: `2014-01-01T00:00:00.05-0530` renders
  `SELECT '2014-01-01 05:30:00.05'`, measured in `Q8.java`. The test no longer exercises `instantOf` at all, although its
  javadoc says it does.
- Its order check uses only 20 literals (400 pairs) and no SQL-made value.
- The only run-time `instantOf` coverage left is the side-by-side test's single `parseDate` of a column (two rows).

Missing:
- the run-time read in the order test (use `Reason.DECLARED`, or a VALUES column);
- the new whole-second adjust and timeBucket paths;
- dateDiff;
- a DateTime and a Date parameter;
- FROM_EPOCH_MS, NOW, DATE_AT_TIME, MAKE_TIMESTAMP;
- a value before 1970 in the side-by-side test;
- a Variant string read as a DateTime;
- a constant against a column of the same text (Q4a);
- a pin of the store-column gap, including the judge (Q6).

## Q9. Other findings

**SHOULD-FIX 2: the fallback in `instantRead` turns unreadable text into a plausible, wrong instant. This is newly
reachable in the product.**

Whenever SQLite's `strftime` cannot read the text, `instantRead` writes the day plus the first 8 characters after the
space, and silently drops the rest. The same happens in `parseDate` (the date-valid guard passes) and in the
TIMESTAMP/TIMESTAMPTZ casts.

| input text | new | base |
|---|---|---|
| `2014-01-01 10:20:30xyz` | `2014-01-01 10:20:30` | `...30xyz` |
| `...30.5junk` | `...30.5` | as written |
| `...30.5.6` | `...30.5` | as written |
| `...30+01` (an hour-only offset; DuckDB's TIMESTAMPTZ applies it) | `...10:20:30` (offset lost) | as written |
| `...30 +0100` | `...10:20:30` | as written |
| `...30+15:00` | `...10:20:30` | as written |
| `2014-02-30T10:00:00` (via SQLite's own normalization) | **`2014-03-02 10:00:00`** (cast) | `2014-02-30 10:00:00` |
| `2014-01-01 1:2:3` | `2014-01-01 1:2:3:00` | as written |
| `''` | `' 00:00:00'` | `''` |

So the claim "becomes garbage rather than raising, as before" is not accurate. Before, the garbage stayed visibly
garbage: it compared equal to no real instant. Now some of it becomes a real instant that compares equal to genuine
values. The judge's text arm already behaved this way on the base; the change spreads it to product casts and to
`parseDate` of a column.

Fix:
- Limit the fallback to the shapes it exists for: a year outside 0000-9999, and an hour alone.
- Otherwise keep the text as written, or better, raise `TEXT_NOT_DATE` in `parseDate`.
- For Feb 30 in the cast path, compare `date(day)` with `day`, as `parseDate` already does.

**SHOULD-FIX 5: the store-column note says "as before", which is inaccurate.** A store column written with trailing
zeros, for example by SQLite's own `strftime('%Y-%m-%d %H:%M:%f')`, which always writes `.SSS`:

| | `col == %2014-01-01T10:20:30.000` |
|---|---|
| base | true (the literal kept `.000`) |
| new | false (the literal is now `...:30`) |

The gap is real and recorded, but its register row should say that this match is lost, not that nothing changed.

**SHOULD-FIX 6: the run-time SQL grew.** parseDate of a column went from 936 to 1,864 characters; the zoned cast of a
column from 1,425 to 2,082. A non-ISO loaded cell (a DECLARED cast) now carries `instantOf` too. The project's rule is
that SQL size pins may only go down, so check any pin that covers these (I could not run the pins). Folding `padded`
into `instantRead`, so the text is not read twice, would recover most of it.

**Note: a cross-database divergence, now visible.** `parseDate($r.col)` holding `2014-01-01T10:20:30+01:00`:
- SQLite now answers `09:20:30` (Pure's answer: the offset applied);
- DuckDB's dialect writes `CAST(col AS TIMESTAMP)`, which DuckDB 1.5.6 answers `10:20:30` (offset ignored, measured;
  only TIMESTAMPTZ applies it).

The DuckDB side is pre-existing. It needs a register row or a DuckDB fix.

**Checked and clean:**
- No `default ->` arm was added; the `placeholder` switch is exhaustive over `DeclaredKind`.
- `constantInstant` and `timestampLit` return a literal's text, which is allowed for a method that spells a literal.
  Every bound piece is written through `SqlWriter`.
- No NULL appears where the base raised: the ADD_INTERVAL year guard still raises; TIMESTAMPTZ `+0960` was NULL on the
  base too.
- `timeBucket` with a huge constant size throws an unnamed `ArithmeticException` from `multiplyExact` at planning
  (minor).

**Pre-existing, not caused by this change:**
- A sub-second adjust drops fraction digits 7-9 (S181's "never cut").
- MAKE_TIMESTAMP rounds to 6 digits and can write second 60.
- A lowercase `t` separator (`2014-01-01t10:20:30`) silently loses the time in `padded`; DuckDB raises.
- DATE_TRUNC YEAR/QUARTER/MONTH/WEEK return a day text under a TIMESTAMP SQL type.

## What I could not determine

- Whether any SQLite test-road table is filled other than by lite's loaders. This decides whether SHOULD-FIX 1 is live
  today.
- Whether SQL-size pins cover the run-time parseDate or cast (SHOULD-FIX 6).
- legend-pure's own answers for `parseDate` of `...T10Z`, `+HH` offsets and trailing junk. I did not read the pinned
  sources.
- The full pipeline was not run (no Bazel). The dialect was exercised directly from sources compiled at the audited
  revision, so lowering choices are inferred from reading `Scalars.java` and `RowLoad.java`.
