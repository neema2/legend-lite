# Timeseries v1: the program, fixes first, then the features (2026-11-01)

**Status: the cut is ratified, except for intraday** (the user, 2026-11-01: "Looks great for v1 and v1.1!"). Written
2026-11-01 because the user asked: "Create a roadmap for all the fixes first and then all of these awesome features -
write it down so we can run the program". Ruled the same day:

- **P3.** The pull request is stacked on #81 ("stack").
- **P4.** The databases are DuckDB, Postgres and Snowflake ("Duckdb, postgres and snowflake is enough for now").
- **P6.** The holiday calendar is built in the platform, in v1.1 ("We should build holiday calendar in platform that
  can be part of v1.1").

- **P1b.** Intraday moves to v1.1 with the calendar ("agreed intraday can also move to 1.1").
- **P2.** Option D, with every tile zooming. This was read from "Yes do it", the answer to that question and to
  watching #86, so it is to be confirmed before B7. The design that governs the language is still `docs/TS_ONE_TREE_PLAN_2026_10_31.md`: its
decisions D1 to D13, and its rulings in §9. That plan's §6a is the state this program starts from. This document is
the order of the work from that point on, and covers everything past it as well.

A session picking the program up reads, in this order:

1. this document's §1 (the state) and §6 (the status table), to find the next item;
2. that item's brief, in §3 or §4;
3. §7, the rules;
4. §8, the checks.

Every item starts and ends green, and each commit updates §6.

## 0. What v1 is

A quant writes a timeseries one way, using gs-quant's words, in any of three places:

- in `#TS` text;
- in a Python notebook;
- in DataCube's timeseries tile.

They get the same numbers in all three. The database computes those numbers: the warehouse their data is in, at the
size their data is. The tile shows each series as the `#TS` statement and the Python program a person would write.
A page of charts behaves like a dashboard:

- a period chosen once moves every tile;
- a click on a chart filters the page, and so does a slicer;
- a title, a note and an image explain the page.

v1 is done when every one of the following holds, each proved by a test rather than claimed:

| # | v1 holds when | Proved by |
|---|---|---|
| V1 | **The language is the library's.** Every word means what gs-quant means, or is a ruled departure written in `docs/SEMANTICS_REGISTER.md` and the oracle's register. No rule of the language lives in Java. Core's parser is the only reader, on every surface | `TsTranslatorGuardrailTest`, extended (A1); the oracle; a guard that `datacube/src` holds no timeseries grammar (A3) |
| V2 | **The surfaces are one.** `#TS`, the Python DSL and the tile read and print one another. Show #TS and Show Python cover every series of every example page exactly (the text reaches a fixpoint and the rows are equal), with no refusals left | the parity tests' counts, pinned at zero |
| V3 | **It runs on real warehouses.** The frozen answers hold on DuckDB, Postgres and Snowflake (P4) | the warehouse lanes (B1) |
| V4 | **It holds at real size.** The scale benchmarks stay within their budgets | the measure targets (B2) |
| V5 | **Every word has a second opinion.** Each word has an outside oracle (gs-quant, TA-Lib, statsmodels or pandas), or is named as having none | the oracle's report (B5) |
| V6 | **Calendars and clocks: v1.1, not v1** (P6, P1b). The platform's holiday calendar, time zones, trading sessions and `SAMPLE BY` under a day | B6's tests, in v1.1 |
| V7 | **The page is a dashboard.** The page period, cross-filtering, slicers, drill-through and bookmarks; text, title, image and filter tiles | B7's and B8's tests |
| V8 | **The gate.** `//gates:local`, the Postgres lanes and the Python suites are green, and CI is green on every OS on the pull request that lands v1 | §8 |

## 1. Where it stands (2026-11-01)

The work so far is on the branch `claude/timeseries-language`. It contains PR #81's branch (`claude/timeseries-platform`)
plus 53 more commits, and is behind `main` by none. The commits are listed in the one-tree plan's §6a.

**Done:**

- **The words.** All 81 words are `timeseries::line::` functions in `projects/timeseries/model.pure`.
- **The line rules.** These are `timeseries::lines::` functions: a line, `sample`, `periodsOf`, `spine`, `someValued`,
  `latest`, `ordered` and `perYear`.
- **The binding.** The translator binds a word's parameters by type and position, never by name. It is guarded by
  `TsTranslatorGuardrailTest`.
- **Spellings.** A word's other spellings are tags in the library (`timeseries::Words.alias`).
- **Show #TS (D12).** All 210 series on the gallery and markets pages print exactly. On the class page, 7 of 10 print;
  3 are refused, because they read an enumeration as names.
- **The frozen answers.** 210 series (271,838 points) on the gallery and markets pages, and 10 series (17,749 points)
  on the class page.
- **The gs-quant oracle.** 145 series agree and 1 differs (register row S130). 39 have no gs-quant counterpart.
- **The vocabulary.** D3, R3, R14 and R15 are ruled and built.
- **Generated from core's export.** The guide (`docs/timeseries/WORDS.md`) and the tile's word list
  (`src/generated/ts-words.ts`).

**Not done.** Each of these is an item below:

- **Java still decides some rules of the language** (A1). The translator, `core/src/main/java/com/legend/protocol/ts/`,
  is 4,324 lines. `TsToPure` is 1,426 of them and `TsParser` 773. The rest are `TsWords` (406), `TsLines` (388),
  `TsShow` (370), `TsFunctions` (323), `TsPrinter` (315), `TsCatalog` (226) and `TsAst` (97). The rules it still
  decides are below. The rule numbers are the `#TS` spec's (`docs/TIMESERIES_LANGUAGE_2026_10_29.md`); they are not
  the one-tree plan's rulings, which reuse the letters.
  - **R2 and R6, how lines meet.** `TsLines` picks which `lines::` function to call, by what kinds of line they are
    and by `FILL`.
  - **R7, items that read items.** An item that reads an earlier item. `TsToPure` also folds a later item's line into
    an earlier one when it was built from it. (Replaced 2026-11-03 by one table per base: A1, R7.)
  - **R10, struct fields.** A field's answer column is named `<item>_<field>`.
  - **R5 and R9.** The rename to one date and the `latest` substitution.
  - **The column cap.** An answer had at most 12 columns, because the library's `someValued` had only 12 overloads.
    **Lifted 2026-11-02** (move 5 below).
  - **Refusals of meaning written in Java.** A grep for lines that throw or refuse finds 173 across `protocol/ts`:
    `TsToPure` 61, `TsParser` 51, `TsFunctions` 18, `TsShow` 18, `TsPrinter` 12, `TsWords` 8, `TsLines` 3 and
    `TsCatalog` 2. Not all of these are meaning rules; the census in A1 sorts them.
- **The plan's step 4 is not built** (A4). Under D2, `log`, `floor` and `ceil` are meant to be refused in text. Today
  `log(x)` is SQL's base 10 (`PostgresFunctions.java:99`). `Window(w, r)` and `type => logarithmic` are also missing.
  Step 4 needs rulings R1, R7 and R9.
- **One reader (S3) and the library's text (S4)** (A3, A5). The tile still has its own formula grammar and its own
  `TS_SIGNATURES`. It also sends 279 KB of library text with every model.
- **Show #TS** cannot yet print an enumeration or a calculated column (A6).
- **H2 runs out of memory on deep `SAMPLE BY` queries** (A7), so those tests run only on DuckDB.
- **No independent review and no full gate since commit `07ce4597`** (A2). The branch has never run CI: `gate.yml` runs
  on a pull request and on `main` only.
- **Not started.** The Python DSL (D8) and Show Python (D13), the warehouses, scale, calendars and time zones, an
  oracle for the words gs-quant lacks, page interactivity, and new tile kinds.

## 2. The cut (ruling P1)

| | In | Why |
|---|---|---|
| **Part A, the fixes** | A1 to A10 (§3), except A7 | finish what the one-tree plan set out to do before anything is built on it |
| **Part B, v1** | B1 to B8 (§4), except B6 | without these it is not a timeseries product a desk would adopt |
| **Part C, v1.1** | the platform's holiday calendar (B6a, P6) and intraday with it (B6b, P1b); trend lines, forecasts and annotations on the general charts; units and currency (C2 of the parity note); time intelligence and the time spine (C3); live data; a crosshair linked across tiles; the service: share, permissions, versions, schedules, alerts; H2's deep plans (A7: H2 is not a v1 target, P4) | real value, but none of it blocks a quant's first week |
| **Part D, not v1** | AI and augmented analytics; maps; storytelling | the gap analysis's tier 5, and nothing timeseries-specific |

Part C's items come from `docs/DATACUBE_BI_GAP_ANALYSIS_2026_10_24.md` §4 (tiers 2 to 4) and
`docs/DATACUBE_BI_EXCEL_PARITY_2026_10_24.md` §3.6 and §3.10. Each gets its own brief when its turn comes.

## 3. Part A: the fixes, in order

### A1. The language's last rules move out of Java

- **Why.** The user's goal, 2026-11-01: "get all the logic out of java ... this program goal is not complete until
  everything works without the java". What remains is listed in §1.
- **What.** First take a census: every rule and every refusal in `protocol/ts`, each put in one of three classes.
  1. **Syntax.** The parser's, and it stays in Java.
  2. **A fact about a source or a column.** Name to source, column to line. It stays in Java.
  3. **A rule of meaning.** It moves into the library.

  Then move every rule in the third class:
  - **How lines meet (R2, R6).** Today `TsLines` picks the `lines::` meeting function by the lines' kinds and by
    `FILL`. Instead, the translator makes one call, and the library's overloads, chosen by the line's type, decide.
    `FILL` already reaches the library as `timeseries::Fill`.
  - **Items reading items (R7).** Today Java folds a later item's line into an earlier one. That fold is a library
    function over the items' lines, or the type checker's work.
  - **Struct fields (R10).** A field's name comes from the word's declared return type. The rename that the translator
    writes is bookkeeping, which the guard allows. The naming rule itself is either written in the library or
    recorded as the language's surface syntax, and the census says which, with its reason.
  - **The 12-column cap.** Either `someValued` takes a `ColSpecArray` (which needs a compiler that reads a row through
    one), or the cap stays as a named limit of the library. This is ruling P7. **Ruled and built 2026-11-02:** the
    array `read`; the cap is gone.
  - **Refusals of meaning.** Each becomes a Pure `fail` in the library or a type error, keeping the same words.
- **Where.** `core/src/main/java/com/legend/protocol/ts/{TsToPure,TsLines,TsWords,TsFunctions}.java`,
  `projects/timeseries/model.pure`, and `TsTranslatorGuardrailTest`.
- **Done when:**
  - every rule is in the library, or is in the one-tree plan's §6a list of what stays in Java, each with its reason;
  - the guard is extended: no Java branch on a line's kind picks a library function, and the translator calls one
    meeting function;
  - the frozen answers are unchanged;
  - every refusal test passes with the same words.
- **Size.** Large.
- **Riskiest unknown.** Whether overloads chosen by a line's type can express the broadcast case: a line with no
  instrument, met with a line per instrument.
- **The census (2026-11-01).** An agent that wrote none of the code read `TsToPure`, `TsLines`, `TsFunctions`,
  `TsWords`, `TsCatalog` and `TsIslands`. It sorted 22 rules and every distinct refusal into the three classes.
  - **Two stay in Java.** Each is recorded with its reason in the one-tree plan's §6a.
    - The R7 fold: an item read from a later item's line that was built from it is one line. It depends on which
      relation expression is which, and the library sees only values. Without the fold, a line with several rows on a
      date, met with itself, would multiply its rows. (Replaced 2026-11-03 by one table per base, which is binding:
      which read is which.)
    - The R10 naming of a struct's fields, `<item>_<field>`. Pure cannot build a column name from strings. The field
      names already come from the word's return type.
  - **Moves, cheapest first:**
    1. Dead code, and the span table that defaulted an unknown unit to seconds. **Done.**
    2. One copy of the "meet on no instrument" refusal (there were three). **Done.**
    3. FILL's spelling in one place, and the library's `fill` always called (its `none` leaves a line as it is).
       **Done.**
    4. The meeting variants (`meet` or `meetOne`, `periodsOf` or `periodsOfOne`, `onto` or `ontoOne`) as one function
       each, taking `timeseries::Keyed`. **Done.**
       - The library now says what each kind means. A line is every instrument's exactly when it has no instrument
         and the answer is by instrument. Two lines of no instrument now meet on their key `'all'` as well, with the
         same rows.
       - The translator passes the kind as data and calls each function from one place. A guard holds this:
         `linesMeetByOneLibraryFunctionEachTheirKindItsArgument`.
       - It was pinned first by a broadcast over lines keyed by an enumeration, met and under FILL
         (`TsIslandTest.aLineOfNoInstrumentIsEveryInstrumentsWhateverTheKeysType`).
       - Left for move 7: a word's own broadcast, `TsWords.each`, which calls `helpers::each`. A Pure `if` there would
         need the two keys of one type, and a line of no instrument is keyed by a String.
    5. The 12-column cap (P7). **Done** (below).
    6. The period aggregates' vocabulary (`TAKEN`) read from `timeseries::Taken`. **Done.**
       - `TsLines.TAKEN`, a hand copy of the enumeration plus a Java-only alias `mean -> avg`, is deleted.
       - The enumeration's value is now `mean`, the canonical word of R15, where it was `avg`.
       - A period's aggregate is a word whose canonical name is a value of `timeseries::Taken`, resolved through
         the library's own alias tags, so `avg(x)` under `SAMPLE BY` is `mean(x)`'s, as everywhere else.
    7. A condition in a line's place, and a timed line's broadcast, written in the library. **Done 2026-11-02, by
       the brief's other outcome: each recorded as what stays in Java, with its reason** (the one-tree plan's §6a,
       now an explicit list of seven).
       - The condition's flags are a per-point expression, which the guard allows.
       - A word's broadcast (`TsWords.each`) cannot be a Pure `if`: its branches are typed by two different keys,
         and the other ways cost a cast that does not hold or a join on every word's argument.
       - The timed refusal fires only for a timed basket mixing a member of no instrument with members per
         instrument. It had no test; now `TsIslandTest.aLineOfNoInstrumentAccruedBesideLinesPerInstrumentIsRefusedByName`
         pins it, words and all.
       - The guard is extended: `theTranslatorBranchesOnALinesKindOnlyWhereNamed` lists every `if` on a line's kind
         (three lines: the instruments gathered, two refusals and the broadcast), so a new one fails the build.
    8. A Float line, and one path with and without FILL. These are the riskiest: every word's signature changes.
       **Waits on the user's word**: it is the largest move, and no rule the census found depends on it.
  - **Three decisions, ruled 2026-11-02** (the user: "Okay agree"). **Built.**
    - **`latest` follows the spec's R9:** the last date of every source the query reads, after its `WHERE`, so
      there is one date for the query whichever items it answers. It is computed by the library's
      `lines::latestOf` and `laterOf`; the old `lines::latest` is deleted. Before, it was the answer's own last
      date. A test pins the difference (`latestIsTheSourcesLastDateWhicheverItemsAreRead`).
    - **A subscript names its instrument by its value.** A condition on the `BY` column itself (`close[symbol =
      'SPX']` with `BY symbol`) is refused, with its fix `close['SPX']`. Before, the code read it quietly as one
      line. Conditions on other columns, and on a source with no `BY`, are unchanged; the spec's text follows in A9.
    - **A window counts any whole number of observations from 1,** as gs-quant's `Window(w)` does. The cap of
      100,000 had no recorded reason, and is gone.
  - **The 12-column cap (move 5, P7). Done 2026-11-02** (the user: "Yes array read").
    - **Before:** the answer's some-value filter, `lines::someValued`, had 12 overloads, one for each number of
      items, because Pure has no parameter that takes any number of columns, and a list literal of a row's
      columns, `[$r.a, $r.b]`, is refused upstream when a column may be empty (each element must be `[1]`).
    - **Built:** `meta::legend::lite::read<T,Z>(row:T[1], columns:ColSpecArray<Z⊆T>[1]):Any[*]`, a row's values in
      several columns as one list, a column with no value contributing none. `someValued` is one function,
      `$t->filter(r|$r->read($items)->isNotEmpty())`, and the translator passes the items' columns as one `~[...]`;
      its cap and refusal are deleted.
      - Typed by `ReadChecker` as a call of the overload over the column reads (each `Typer.rowColumn`'s): the
        columns' common supertype, the multiplicity the sum of theirs, as a list literal's.
      - Lowered to no SQL list, as proposed. `isEmpty` and `isNotEmpty` over it are the cells' null tests (an AND
        of `IS NULL`, an OR of `IS NOT NULL`), which every database spells, so the library's filter runs on
        DuckDB, H2 and SQLite alike. The Lowerer hands the rule the cells, each lowered as the row's column is
        (`Scalars.overCells`, beside `format`'s arm). Any other use (`size`, a value) is refused by name; `size`
        can be written the same way when move 8 needs it.
      - A first build made the list (`CompactList` of an `ArrayLit`), and the carrier purity ratchet refused it: a
        new list built before the dialect (`CarrierPurityRatchetTest`, the `ArrayLit` pin 42). Building no list
        keeps the pin where it was.
      - Register S114 amended; the census row; the not-upstream overload count 69 -> 70.
      - Tests: `ExtendCheckerTest$ReadTests` (some, none, the SQL equal to the hand-written null tests, another use
        refused, a column the row has not, no column); `TimeseriesLibraryTest.aRowsSeveralColumnsAreOneListThatIsEmptyWhereNoneHasAValue`
        (every in-process database); `TsIslandTest.aQueryAnswersAnyNumberOfItemsARowKeptWhereAnyHasAValue` (13
        items, a row kept by the 13th item's value). That last one runs on the platform's database alone: 13 lines
        meeting is a deep query, and H2's parser runs out of memory on it, the known H2 limit (A7, v1.1).
  - **R7, one table per base (ruled 2026-11-03: "Okay let's go"; part 1 done 2026-11-03).** It replaces the R7 fold.
    - **The bug it fixes.** Every read of a source's column built its own table, so two items over one column were
      two tables joined on (instrument, date). Where a date has several rows (a source read with no `BY`), the join
      multiplied them: `SELECT CLOSE AS c, CLOSE * 2 AS d FROM PRICES ON DAY` answered 48 rows for 24 points. The
      fold hid the simplest cases only, by which item was read last.
    - **Part 1, built.** A column read twice is one table (`TsLines.Writer.read`, keyed by what the read reads).
      Every per-point expression and every item over it is a column added to that table, and every item is read
      from the table's newest form (`Lines.grown`, `Lines.newest`). An item's name is a copy of its value's column,
      so a later item can still read the value. Parts of one base meet with no join. The fold (`descends`) is
      deleted.
      - Answers: the probe's per-point case 48 -> 24 rows; the frozen answers unchanged (the parity suite, the
        class page's, the tile's counted series with the fold deleted, the whole `#TS` suite).
      - SQL: three items over `CLOSE['AAA']` 3,728 -> 635 characters; `bollinger_bands` with a figure of its
        fields 6,038 -> 2,510; queries with no repeated read unchanged. Run times unchanged within noise.
      - Test: `TsIslandTest.theItemsOfOneColumnAreOneTableEachRowOnce` (every in-process database).
    - **Part 2, the slice (2026-11-03): `mean`'s window of w observations.** A word that keeps its line's rows has a
      *rows form* in the library (`timeseries::rows::mean`): the word over a table's value column, its answer one more
      column of that table. The translator calls it where the library has one; any other word still makes its own
      line.
      - **What it needed: points.** A word's answer is not a point of its ramp's rows (gs-quant's `apply_ramp` drops a
        word's first w points), and a point may have no value (a return's first). One column cannot say both, and the
        next word's ramp counts points: `avg(returns(close), 2)` starts on the third date, `avg(avg(x, 3), 3)` on the
        seventh. So a value that is not a point of every row carries its points as another column. The rules are the
        library's (`rows::mean` counts points, `rows::both` and `rows::masked` make a figure a point where all its
        parts are, `rows::points` gives a value's own line to a word with no rows form). The translator passes the
        column names (`Lines.presence`).
      - Answers: `CLOSE AS c, avg(CLOSE, 2) AS m` with no `BY` 46 -> 24 rows. The frozen answers unchanged (parity,
        class page, tile); the gs-quant oracle unchanged (145 agree, 1 the ruled difference).
      - SQL: `spread, avg(spread, 3)` 8,312 -> 2,523 characters; `c, avg(c, 3)` 3,483 -> 1,116; a mean of means with a
        figure of a word and a word of it 14,244 -> 6,430. Run times fell with them (5 runs: 372 -> 269 ms, 110 -> 87,
        258 -> 137).
      - Tests: `TsIslandTest.theItemsOfOneColumnAreOneTableEachRowOnce` (the word), and
        `aWordOfAWordCountsFromTheFirstPointOfWhatItReads` (a mean of means, a figure of a word, a word of that figure,
        each against gs-quant's ramp).
    - **Part 3, next: every word that keeps its rows gets a rows form,** each held to the frozen answers and the
      oracle. Then the line forms of those words are written through their rows forms, so each has one body.
  - **Duplicates where lines meet (the time model's D1, ruled 2026-11-03; its proof step 3). Done 2026-11-03.**
    - Two lines meeting where one has several rows on a date pair as a lookup (each row with the other's one); where
      both do, the database refuses while the query runs, naming the fix (`SAMPLE BY 1d` and how to take a date's
      rows). The check is the library's: `lines::meet` (every meeting: expressions, the words over several lines,
      candles) and `lines::onto` (the answer's rows spread over items of several lines) count each side's rows on each
      period (`lines::perPeriod`) and fail where both are several (`lines::single`, `fail()`). The spreading carries
      the most rows a line spread so far has (`seen`, `lines::most`) instead of counting the growing table again at
      each step: counting it again nested the plan deeper per item, and H2 ran out of memory on five items.
    - The cost, on the gallery's twelve queries (`//datacube:timeseries_sql_measure`): only the two whose lines meet
      change -- `beta_63` 3,856 -> 4,515 bytes, 25 -> 53 ms; `ratio_step` 6,234 -> 7,962 bytes, 36 -> 72 ms. The
      other ten are byte for byte as before.
    - **Not built yet: the skip where uniqueness is known** (D2: a declared key, `SAMPLE BY`). Under `SAMPLE BY` the
      counts are 1, so nothing is refused, but the check is still paid.
    - The spec's R6 says it (D7). Test: `TsIslandTest.linesOfSeveralRowsOnADateMeetAsALookupAndBothSoAreRefused`
      (every in-process database: the lookup's 24 rows; both refused by name, in an expression and as two items).
    - Found on the way: `p.CLOSE - q.CLOSE` over two aliases of one table is one line (R7's one read), 0 on each row,
      not two lines meeting.
  - **Moments meeting days (the time model's D4; its proof steps 1 and 4). Today's answer recorded 2026-11-03; the
    refusal waits on a construct.**
    - Today (`TsIslandTest.aLineOfMomentsMeetingALineOfDaysAsItIsToday`, every in-process database, a `TICKS` table of
      two moments a day): an expression of both and a word of both (`correlation`) find **no row, silently**; as two
      items the type checker refuses ("cannot unite" a `StrictDate` period with a `DateTime` one) without naming the
      fix. The fix D4's refusal will name, `SAMPLE BY 1d` and `last(...)`, answers right on DuckDB and SQLite; H2 has no
      `ARG_MAX`, which a day's last moment reads (a gap of H2's dialect, for A7).
    - Why it is not built: lite refuses nothing at plan time (a `fail()` the compiler can decide raises only when the
      query runs, measured), and the meetings the library cannot see are the dangerous ones -- a word's own meeting of
      its lines (`(key, date, value)`, no grain) and the answer's spreading (refused by the type checker before any
      library call). The precise way: Pure picks a function by a column's type (a meeting of days, of moments, and a
      refusal for the mix), the broadcast's overloads by column type (2a) carried to a column a `ColSpec` names. The
      other way is the grain passed as data (as `Keyed` is) to every meeting, the words' own included, which changes
      every word of two lines. To decide after 2a.

### A2. Checkpoint: an independent review and the full gate

- **Why.** No review and no full gate have run since `07ce4597`, and the branch has never run CI.
- **What:**
  - **An architecture review.** It is done by an agent that wrote none of the code, and it covers `07ce4597` to the
    head, checked against:
    - AGENTS.md's invariants: 1 to 8, and 3a;
    - the execution tenet: the translator computes no value;
    - the world map: natives, platform semantics, programs;
    - upstream fidelity: every divergence is a register row;
    - the guards.

    Its findings are verified, then fixed, each pinned by a test.
  - **The full gate.** `bazel test --lockfile_mode=error //gates:local`, the Postgres lanes, and the Python suites.
  - **CI on every OS.** CI runs only on a pull request, so this needs the user's word (ruling P3): a PR for this
    branch, either against `main` or stacked on #81.
- **Done when** the review's findings are fixed or answered, the gate is green, and CI is green or every failure is
  explained.
- **Size.** Medium.

### A3. One reader (S3, the plan's step 3)

- **Why.** D1: core's parser is the only reader. Today the tile's formula box has its own grammar
  (`datacube/src/timeseries-expr.ts`), its own signatures (`TS_SIGNATURES`) and its own translator
  (`datacube/src/timeseries-ts.ts`).
- **What:**
  - The formula box parses through the planner the tab already holds (WASM). This needs an entry point that parses
    one item and recovers from errors, so completions still work.
  - Core answers the facts a drawing needs (a series' unit, level, candle and problems) from the catalog.
  - The tile keeps each series as its item text, and "Build this tile" prints `#TS` items.
  - `timeseries-expr.ts`'s grammar, `timeseries-ts.ts` and `TS_SIGNATURES` are deleted, and
    `timeseries-query.ts`'s use of them is rewritten.
- **Done when:**
  - those files are deleted;
  - a guard shows `datacube/src` has no timeseries grammar;
  - the frozen answers hold, as do the tile's suites, the Python suites, and the bundle and cold-start budgets.
- **Size.** Large.
- **Riskiest unknowns.** The entry point's latency on each keystroke, and the roughly 25 analyses that have to move.

### A4. The words' remainder (the plan's step 4, plus the rest of #117)

- **Why.** D2 to D5 and §4 of the one-tree plan, as ruled, are not built. The 8 scalars are still per-point SQL, not
  words of the library. That is the remainder of #117.
- **What:**
  - **The scalars become library words** (`abs`, `exp`, `sqrt`, `ln`, `log10` and the rest), so the catalog, the
    Python stubs and the guide list them.
  - **D2's refusals.** `log(x)`, `floor(x)` and `ceil(x)` are refused in text, and the message gives both fixes:
    "write ln(x) (natural, gs-quant's log) or log10(x)", and "greatest(x, 0) for gs-quant's floor; round_down(x) to
    round".
  - **`returns`** takes gs-quant's values (`type => logarithmic`, with `log` as an alias).
  - **`Window(w, r)`** is written `w => 63, r => 10`.
  - **`beta`'s first three values.** Ruled under R1.
  - **`zscores` and `winsorize`** stay causal, each a named departure in the register.
  - **Two lines meeting.** Ruled under R9.
- **Needs** rulings R1, R7 and R9 (the one-tree plan's §9).
- **Done when:**
  - the oracle's rows turn to passes or named departures;
  - the frozen answers change only where a ruling says, each change with a dated reason.
- **Size.** Medium.

### A5. The library's text off the wire (S4)

- **Why.** A platform library belongs to the planner, never to a client's copy. Today
  `datacube/src/pure-library-text.ts` (279 KB) is sent with every model, both from the tile and from the headless path.
- **What.** The planner resolves `projects/timeseries` the way it resolves any project's dependency. Clients send their
  own model and nothing else. The file is deleted.
- **Done when:**
  - the file is gone;
  - the bundle budgets are pinned back down, with a dated reason;
  - the headless path and the tile both plan without it.
- **Size.** Medium.

### A6. Show #TS, complete

- **Why.** D12 promised every series. Today 3 class series are refused because they read an enumeration as its
  values' names, and any calculated column is refused too.
- **What:**
  - **Enumerations.** `#SQL`'s reader learns to read an enumeration cast to text as `toString`. That is a core change
    to `protocol/sql`.
  - **Calculated columns.** Each `extend` is printed as the Postgres expression that the reader reads back to the same
    meaning, one native at a time.
- **Done when:**
  - the class page's refused count is 0, and the pinned count is shrunk to 0;
  - a test page with calculated columns prints exactly.
- **Size.** Medium, and it grows with the number of natives.

### A7. H2's deep plans (moved to v1.1)

- **Moved.** H2 is not a v1 database (P4: DuckDB, Postgres and Snowflake), so this waits for v1.1.
- **Why.** H2 plans nested derived tables exponentially. As a result, `SAMPLE BY` queries with several items run out
  of memory, and those tests run only on DuckDB. That leaves the engine-style printer path untested on them.
- **What:**
  - Find the shape. `ProjectionWrapsFold` (commit `07ce4597`) folded one case.
  - Fold the rest in H2's dialect passes or in the lowering, without changing the engine-style printer's text.
- **Done when** the deep tests run on H2 within the lane's heap.
- **Size.** Small to medium.

### A8. The ratchets, reviewed

- **Why.** This program raised ceilings, each with a dated reason:
  - `BARE_NAME_ARMS_MAX`, from 194 to 197, for `TsShow`;
  - `SqlTextRatchetTest`'s register entry for `protocol/ts/TsPrinter.java`;
  - any others that `git log -S` turns up on the guard files.
- **What.**
  - Re-read each one.
  - Where a table can replace the arms, shrink the ceiling back. For example, `TsShow`'s protocol names could become
    one map keyed by `PlatformTypes`.
  - Re-pin each ceiling to its measurement.
- **Done when** every raise this program made has been reverted, or re-justified with its measurement.
- **Size.** Small.

### A9. The documents

- **What:**
  - Version 4 of the `#TS` spec (`docs/TIMESERIES_LANGUAGE_2026_10_29.md`). It replaces version 3's rules wherever the
    one-tree plan changed them, and its numbering stops clashing with the plan's rulings.
  - The guide for quants (`docs/TIMESERIES_GUIDE_FOR_QUANTS_2026_10_30.md`), with every example generated from the
    catalog and run.
- **Done when** every example in both documents runs in a test.
- **Size.** Small.

### A10. Close Part A

- **What:** a second independent review of A3 to A9, the full gate, and CI on the PR. Then the user decides whether
  Part A lands.

## 4. Part B: v1, in order

The order rests on two points:

- **Proving comes before building.** The warehouses and scale come first, because what they find can change the
  library's shapes, and everything after is built on those shapes.
- **Python comes before the oracle and calendars.** The DSL is the second surface, and both the oracle and the
  calendars are tested through it.

### B1. The warehouses

- **Why.** Today `#TS` is tested only on in-process engines (`TestBackends.inProcess()`).
- **What.** A lane that runs the frozen answers and `//core:ts_island_test`'s cases on two more databases, chosen by
  P4 (DuckDB already runs them):
  - Postgres, embedded, through `tools/postgres/as_unprivileged.sh`;
  - Snowflake, on the account `docs/CLOUD_BACKENDS_PCT_2026_10_11.md` uses. Its dialect is done and its PCT lanes
    are green there (`//pct:pct_snowflake_<suite>`). The account's properties are an environment secret.
- **Done when** each lane is green, or every difference is a register row.
- **Size.** Medium.

### B2. Scale, with budgets

- **What.** These benchmarks:

  | Benchmark | Size |
  |---|---|
  | daily | 2,000 instruments × 20 years, about 10M rows |
  | intraday | 100 instruments × one year of minute bars, about 10M rows |
  | a wide query | 20 items |
  | sampled | `SAMPLE BY 1w` and `1mo` |
  | a basket | 500 members |

  For each, measure the plan time, the SQL's size and the run time, on DuckDB and on Postgres. Also measure the tile's
  cold open with 100,000 points (the tile's point limit, drawn with LTTB sampling).

  The budgets are pinned in a measure target, as `//datacube:timeseries_sql_measure` and `ScaleCostsDuckDbTest` pin
  theirs. The first measurement proposes them and the user rules (P5).
- **Done when** the budgets are pinned and met, and a regression fails the target.
- **Size.** Medium.

### B3. The Python DSL (D8)

- **What.** `legend_lite.timeseries`:
  - gs-quant's functions over series, with stubs generated from the catalog;
  - its sources: a grid, a class, a table, or a DataFrame (loaded first);
  - `frame()`.

  A program prints `#TS` text and runs through core's native library, the one reader.
- **Done when:**
  - gs-quant's own examples, ported, run and match;
  - the oracle also runs over the DSL;
  - the Python suites are green.
- **Size.** Large.
- **Riskiest unknown.** Expressing lines of several instruments in gs-quant's single-series terms.

### B4. Show Python (D13)

- **What.** The DSL program for each series, shown in the existing Python code box: no new tab and no change to the
  layout (the user, 2026-11-01).
- **Done when:**
  - every series on every example page prints, runs and gives the same rows, and refusals are counted;
  - a DOM test shows the program is in the existing box.
- **Size.** Medium.

### B5. A second opinion for the words gs-quant lacks

- **What.** The oracle (`tools/ts-oracle/oracle.py`) gains pinned references:

  | Reference | For |
  |---|---|
  | TA-Lib | RSI, MACD, ATR, stochastic, OBV, Keltner, Donchian, Bollinger |
  | statsmodels | OLS alpha, beta and r², ADF (`dickey_fuller`), half-life, cointegration |
  | pandas | the rolling and exponential families; percentile rank |
- **Done when** each of the 39 rows with no gs-quant counterpart (NO-GSQUANT) is either AGREE, a named departure, or
  listed with "no oracle" and the reason.
- **Size.** Medium.

### B6. Calendars and clocks

Split in two:

- **B6a, the holiday calendar: v1.1, built in the platform** (P6, ruled 2026-11-01).
  - The platform ships each exchange's calendar as model data: its trading days, holidays, trading hours and time
    zone. A user's model can add to it or override it.
  - Windows and `SAMPLE BY` periods can be counted in business days (gs-quant's `b` tenors).
- **B6b, intraday: v1.1, with the calendar** (P1b, ruled 2026-11-01). Intraday is not just more rows. Today a
  date-time source can already be read, windows already take `h`, `min` and `s`, and a rate already accrues over the
  hours between ticks. What intraday adds:
  - **`SAMPLE BY` under a day.** Today it takes `d`, `w`, `mo`, `q` and `y` only (`TsParser.PERIODS`). Intraday adds
    `5min` and `1h` bars. Each bar is labelled by its start or its end, aligned to the session's open (the first
    hourly bar runs 9:30 to 10:30, not 9:00 to 10:00), and a partial bar needs a rule.
  - **Sessions and gaps.** A lag across the close compares the day's last bar with the next morning's first. Words
    such as VWAP and "since the open" reset each session. Overnight and intraday returns are different series.
  - **Time zones.** A tick stamped in UTC belongs to its exchange's day. FX rolls at 5pm New York, and daylight saving
    moves the boundary. The model has no time zones today.
  - **Annualization.** For 5-minute returns this would be √(252 × 78), but gs-quant refuses to annualize several points
    a day (`econometrics.py:497`). `#TS` either refuses the same way or uses a ruled rule.
  - **The axis.** Charts like TradingView's skip the hours the market is closed. A plain time axis draws nights and
    weekends as empty gaps. The tile's drawing and its Draw tool are by the day today (`timeseries-view.ts`).
  - **Volume.** A year of minute bars is about 100,000 points per instrument, so the database must sample to the
    screen. A zoom then asks again at a finer bar, as TradingView loads a finer interval when you zoom in.

  The trading hours and the zone are the same platform data as B6a's calendar, which is why intraday is recommended to
  move with it.
- **Size.** Large.

### B7. The page as a dashboard

- **What:**
  - **The page period** (ruling P2; option D is recommended in `docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md`'s Phase
    5 note). It comes in two steps:
    1. a linked window that every timeseries tile shows;
    2. grids and charts that opt in through a date column they are given.

    The gestures (P2's research, §5) are these:
    - a drag across any linked tile sets the period, and every linked tile zooms;
    - a double-click zooms back out;
    - range buttons on the page (1M 3M 6M YTD 1Y 5Y Max);
    - the Measure box's "Show this period everywhere";
    - a tile can be unlinked, and then keeps its own range.
  - **Cross-filter.** A click on a series' instrument, or on a bar's category, filters the page's grids.
  - **Slicers.** These are B8's filter tile.
  - **Drill-through.** A point opens the rows behind it: its grid, filtered to that date and instrument.
  - **Bookmarks.** A named page state: the filters, the period and the zoom.

  The source is the gap analysis's tier 1.
- **Done when** each gesture works the same from the screen and from Python, through the same commands, as every
  timeseries change does.
- **Size.** Large.

### B8. Tile kinds

- **What:** text (Markdown), title, image, and filter. A filter tile, the slicer, offers a column's values or a range of
  dates. The source is the gap analysis's §2.6.
- **Size.** Medium.

## 5. The rulings asked

| # | Question | Recommended | Needed by |
|---|---|---|---|
| P1 | The v1 cut (§2) | **ruled 2026-11-01**: "Looks great for v1 and v1.1!" | -- |
| P1b | Intraday (B6b) in v1, or in v1.1 with the platform's calendar | **ruled 2026-11-01**: v1.1 ("agreed intraday can also move to 1.1") | -- |
| P2 | The page period: options A to E, and whether the brushed tile zooms too | D, every tile zooms (below); read as agreed from "Yes do it" (2026-11-01), to be confirmed | B7 |
| P3 | A pull request for this branch, so CI runs on every OS, and its base | **ruled 2026-11-01**: stacked on #81 ("stack") | -- |
| P4 | The databases | **ruled 2026-11-01**: DuckDB, Postgres and Snowflake ("enough for now") | -- |
| P5 | The scale budgets | proposed from the first measurement | B2 |
| P6 | Where holidays come from | **ruled 2026-11-01**: built in the platform, v1.1 | -- |
| P7 | The 12-column cap: lift it or keep it | **ruled 2026-11-02** ("Yes array read"): lifted by `read` over a `ColSpecArray`. Built | -- |
| R1, R7, R9 | The one-tree plan's §9: §4's rows, D2 as narrowed, and two lines meeting | as that plan recommends | A4 |
| 2026-11-03 | The roadmap of 2026-11-03 | **ruled 2026-11-03** ("Okay let's go"), every recommendation: the tenet "Java parses and binds; Pure decides" (into `docs/TENETS.md`); R7 as columns on one table; the broadcast by overloads on a column's type with an `All` key type (2a); the time model's D1 to D7 as recommended; Phase 1 before the A2 review; a PR for this branch at A2; intraday stays v1.1; move 8 folded into the R7 work once the slice passes | -- |

**P2: what other charts do** (the user asked, 2026-11-01: "what do other great charts do like Yahoo finance or trading
view?").

| Product | What it does |
|---|---|
| Yahoo Finance | Each chart has its own range buttons (1D 5D 1M 6M YTD 1Y 5Y Max) and comparisons drawn on that one chart. Nothing links one chart to another |
| TradingView | A layout of several charts can sync five things: the symbol, the interval, the crosshair, the time and the date range. Each is a switch, and symbol and interval can sync within groups of charts. Indicators are computed over the whole history; the range only changes what is shown |
| Grafana (the dashboard precedent) | One time picker for the dashboard. Dragging across any time-series panel sets it, so every panel zooms, and a double-click zooms out. A panel can override it with its own relative time, and each panel's query reads the dashboard's range to filter |

Option D is this combination:

- a page period that every timeseries tile shows, with the data kept whole so rolling windows keep their history;
- the brushed tile zooms too, as in TradingView's date-range sync and Grafana;
- a tile can be unlinked;
- range buttons on the page;
- in step 2, grids and charts filter by it through a date column they are given, as Grafana's queries read its range.

So the recommendation changes from "shaded" to "every tile zooms".

Sources: TradingView, "How to sync the charts of my layout?" (https://in.tradingview.com/support/solutions/43000629992);
Grafana, time range controls (https://docs.aws.amazon.com/grafana/latest/userguide/dashboard-time-range-controls.html,
https://grafana.com/docs/grafana/latest/dashboards/shortcuts/).

## 6. Status

Update this table in the same commit as the work.

| Item | State | Last commit |
|---|---|---|
| A1 the last rules out of Java | moves 1 to 7 done, and the three decisions of 2026-11-02 built (move 5, the 12-column cap, by the array `read`; move 7 by the §6a list of what stays in Java, with a guard). R7 one table per base: part 1 done, part 2 (a window word as a column) next; move 8 folds into it | 7b23e09f, c841d939, 69796cc5, 5dc88c85, 911c08fa, e336c4d6 |
| A2 review and gate | waiting on A1. #86 was folded into #81 on 2026-11-02, with main (#85) merged. #81 waits on #83 before it lands, and the v1 work is stacked on it with no PR. #86's last CI was green on every OS | af758d34 |
| A3 one reader | | |
| A4 the words' remainder | waiting on R1, R7, R9 | |
| A5 the library's text off the wire | | |
| A6 Show #TS complete | | |
| A7 H2's deep plans | moved to v1.1 (P4) | |
| A8 the ratchets | | |
| A9 the documents | | |
| A10 close Part A | | |
| B1 to B8 | after Part A; B6 in v1.1 (P6, P1b) | |

## 7. The rules (the user's, each learned the hard way)

- **Branches.** Work on `claude/timeseries-language`; commit and push there. Never push to `main`, and never merge
  #81.
- **Nothing outward without the user's explicit ask** (the user, 2026-11-02, after a PR was opened on an answer read
  as a go-ahead: "Who asked you to open a PR? You're taking slots from other sessions"):
  - no new PR, no PR comment, no CI run, no push to another PR's branch;
  - one branch, one PR: #81 now carries the work, with #86 folded in. The v1 work is stacked on it in
    `claude/timeseries-language`, which has no PR, so its pushes start no CI;
  - an answer to a question about a PR is not permission to open one.
- **Heavy suites one at a time** (`--local_test_jobs=1`), and their times reported with each commit. Run side by side
  they slowed each other, and one timed out (2026-11-02).
- **Commit trailers.** A commit ends with the session's `Co-Authored-By` and `Claude-Session` lines. A model identifier
  appears in no repository artifact.
- **Upstream fidelity.** Every difference from gs-quant, legend-pure or legend-engine is posted to the user before it
  lands, then written in `docs/SEMANTICS_REGISTER.md`. A ruled decision is not reopened.
- **Guard ceilings** move only with a dated reason and a measurement.
- **Layout.** Don't change it without asking. Show #TS and Show Python go in the existing boxes.
- **Shell.** Edit files with the file tools, not with a shell edit. Run one plain command per call, with no chained or
  destructive commands, so the session does not stop to ask.
- **Green at every step.** Each item starts and ends green on §8's standing checks. A change of a number on purpose
  changes its frozen rows, with a dated reason.

## 8. The checks, exact

**Standing checks, every commit:**

```
bazel test //core:ts_island_test //core:timeseries_library_test //core:guardrails
bazel test //datacube:timeseries_ts_parity_test //datacube:stress_markets_test //datacube:timeseries_queries_test //datacube:timeseries_view_dom_test
bazel test //python:engine_test //python:markets_test
```

**Regenerate** after a change to the library or the catalog:

```
bazel run //datacube:update_generated
bazel run //docs:update_ts_words
```

**The Postgres lanes.** Copy the wrapper outside the workspace first (`docs/CLOUD_SESSIONS.md`):

```
bazel test <target> --strategy=TestRunner=standalone --run_under="$SCRATCH/as_unprivileged.sh ubuntu"
```

**The gs-quant oracle**, by hand (`tools/ts-oracle/oracle.py`'s header):

```
bazel test //datacube:timeseries_ts_parity_test
python3 -m venv /tmp/gsq && /tmp/gsq/bin/pip install gs-quant
/tmp/gsq/bin/python -I tools/ts-oracle/oracle.py bazel-testlogs/datacube/timeseries_ts_parity_test/test.outputs/oracle-input.json
```

**The full gate**, at A2, at A10 and before any landing:

```
bazel test --lockfile_mode=error //gates:local
```

**Traps already hit:**

- **H2 runs out of memory** on deep `SAMPLE BY` queries. Deep tests use the platform database, DuckDB, until A7.
- **A probe test left in a test file leaks into the gate.** The dialect-leak census flags a database named in a test.
- **Hygiene.** A library body's working columns are renamed fresh. So a column made inside a body can be read by name
  elsewhere only if it is in a declared return type.
- **`SAMPLE BY` dates.** Under `SAMPLE BY`, `date` is the period's day and `time` is the last tick's. A line's number in
  time is always seconds since 1970.
- **Multi-statement lambdas** need a trailing `;` when parsed through `Compiler.parseQuery`.
