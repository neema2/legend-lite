# Timeseries: every open decision, and the order to finish v1 (2026-11-07)

Written because the user asked, 2026-11-07: "Write this down so we remember all of this and let's do it all in order -
keep going, don't stop". It records the seventeen decisions put to the user on 2026-11-06 (each with its options, their
pros and cons, their cost, and the recommendation), and the order the work runs in. **Each decision proceeds on its
recommendation** under that instruction; the user may overturn any of them, and a register row built on one says so.
Costs: S under half a day, M one to three days, L a week or more.

The state this starts from: #81's branch `claude/timeseries-platform` at `63e935149`, its CI green on every OS
(2026-11-06, run 42293045049); the work branch `claude/timeseries-language` the same. The program is
`docs/TS_V1_PROGRAM_2026_11_01.md`; the SQL track's handoff `docs/TS_SQL_HANDOFF_2026_11_06.md`.

## 1. The rules for the run

- Work, commit and push on `claude/timeseries-language` (no pull request, so a push starts no CI). #81's branch moves,
  and CI runs, only when the user asks for it. Never merge #81; never push to `main`.
- Each item ends green on its checks, run locally one at a time (`--local_test_jobs=1`), their times in its commit. The
  checks CI runs that the local list missed before (2026-11-06) are in every round that touches them: the parser
  ratchet (`//parser-equivalence:update_ratchets_test`), the datacube spec and headless tests, the dialect matrix, the
  lane guard (`bazel query 'tests(//...) except tests(//gates:lanes) except tests(//gates:heavy)'` answers nothing).
- An outward action (another session, a PR, a comment) only with the user's word. Decision 11's message to the
  Snowflake session is covered by this instruction (it was asked as "your OK to send that session the task").
- Upstream fidelity: upstream's answer read from the pinned sources (`@legend_engine_src`, `@legend_pure_src` in
  Bazel's external cache; gs-quant 2.2.3's source), never recalled.

## 2. The decisions

### A. Rulings on what is built, or what A4 waits on

**1. S151, what `x / 0` gives** (built as (a)).
- (a) Nothing at that date: a chart of `close / volume` draws its other dates; the tile's behaviour. Cost: done. Con: not
  Postgres's (an error) nor gs-quant's (infinity); Show #TS of a grid's Pure `divide` answers nothing where the grid
  answers its database's value.
- (b) Postgres's error: one zero fails the chart; a writer adds `nullif`. Cost S (a revert).
- (c) gs-quant's ±infinity: a chart cannot draw it; SQLite and Postgres cannot make it. Cost M.
- **Recommended (a).** Proceeding: S151 records the instruction.

**2. What a comparison gives** (`ma50 > ma200`).
- (a) true / false, as Postgres and gs-quant; `#TS` today. Cost 0; the tile switches when it stores text (A3 step 3).
- (b) 1 / 0, the tile's: arithmetic on a condition works directly; diverges from both upstreams.
- **Recommended (a).** Nothing to build until A3 step 3.

**3. R9, two lines of different dates meeting** (`x / y`).
- (a) The dates both have (today). Cost 0.
- (b) gs-quant's step (each side's last value carried). Faithful; every frozen answer of lines with different dates
  moves. Cost M.
- (c) pandas' union (every date either has, none where a side has none). Cost M, the same churn.
- Alternative: (a) by default, gs-quant's step asked for with `FILL(PREV)`, which already gives it; a register row.
- **Recommended: (a) with the alternative.** Proceeding: a register row naming `FILL(PREV)` as the way to gs-quant's.

**4. R1's four open rows.**

| Row | gs-quant 2.2.3 | `#TS` today | Options | Recommended |
|---|---|---|---|---|
| `Window(w, r)` | size and ramp apart | the size only (the ramp is the size) | `w => 63, r => 10` in text (S) / leave out | add it (the Python DSL needs it) |
| `returns` types | `SIMPLE`, `LOGARITHMIC`, `ABSOLUTE` | `simple`, `log` | `logarithmic` (with `log` an alias) and `absolute` (S) / keep | add them |
| `beta`'s first three | blanked (`econometrics.py:1054`) | given | copy gs-quant (S) / a departure row | copy |
| `sharpe_ratio` | a 365.25-day CAGR over a Marquee risk-free curve | annualized mean over deviation, no risk-free rate | a departure row (0) / a risk-free series argument and the CAGR (M) / refuse | a departure row now |

**5. The scalar functions as library words** (`abs`, `exp`, `sqrt`, `power`, `not`): SQL's today, the same mathematics as
gs-quant's. (a) Leave them SQL's (no meaning differs). (b) Library words (completions and documents). Cost S-M.
**Recommended (a).**

**6. To-many navigation.** Ruled 2026-11-06 ("No it's fine then"): kept refused, never exploded. Upstream's `project()`
explodes a to-many path (a left outer join, a row per element: `testAssociationToMany.pure`, 13 rows for 7 people); a
timeseries reads the many side as a source of its own, navigating to one. A register row records it. Cost S.

### B. The primitives missing before B3 and B7

**7. Scalar parameters** (`FILTER date >= $from`, `avg(close, $w)`). Today `#TS` refuses them ("#TS has no parameters");
`$x` names only a relation in FROM.
- (a) Pure variables in `#TS`: the enclosing lambda's parameters and lets, typed as the `#SQL` island reads them. One
  mechanism for the page period, slicers, Python functions and saved queries. Cost M.
- (b) A `#TS` `PARAMS` clause: a second mechanism beside Pure's. Cost M.
- (c) None: filter the source outside, which changes the numbers (the windows lose their history). Cost 0.
- **Recommended (a).**

**8. Values as a word's arguments** (guide-06, `rebase(close, latest - 3y)`).
- (a) A query-level value (`latest`, and a parameter once 7 is built) wherever a date argument is taken, computed once
  a query: rebase, change, index, a window from a date. Cost M, designed with 7.
- (b) In FILTER only, as today; refused elsewhere naming the fix. Cost 0.
- (c) The tile passes a literal date (today's tile). Works for the tile only.
- **Recommended (a).**

**9. A3 step 3, the tile stores `#TS` text.**
- 9a. Pages saved before (version 6, trees): (a) refused, as every older version (recommended: nobody uses the tile
  yet, the user 2026-10-31); (b) upgraded once. Cost 0 / S.
- 9b. Completions: (a) the words and their parameters; (b) and the grid's columns and instruments (recommended: little
  more than (a)).
- 9c. The step itself, L: the page's opening hydrates each series' text through core (asynchronous where the read is
  synchronous today), and a series' stored text names the series it reads (`ma50 - ma200`), never inlines them.

**10. Calendars and clocks.**
- (a) All in v1.1, as ruled. The Python DSL then refuses gs-quant's business-day windows (`'22b'`) or carries rows.
- (b) Business days before B3; time zones and intraday v1.1. Cost L.
- (c) A narrow primitive now: business days as weekdays (no holidays), so `b` windows read; the holiday calendar v1.1
  as ruled. Cost M.
- **Recommended (c).**
- **Corrected 2026-11-07, read in gs-quant 2.2.3's source: (a) stands as ruled, nothing to build.** gs-quant has no
  business-day windows: every window string goes through `_to_offset` (`timeseries/helper.py:206`), whose pattern is
  `(\d+)([hdwmy])` (`helper.py:104`) -- `'22b'` is "invalid tenor" there. Its `b` is `RelativeDate`'s, a date to fetch
  data from, not a window. The recommendation above was recalled, not read.

### C. Proving and the warehouses

**11. Snowflake runs of `#TS`.** The harness has no workspace on Snowflake.
- (a) A schema per session (`CREATE SCHEMA`, dropped at close): needs the role's CREATE SCHEMA. Cost S.
- (b) Temporary tables in one session: no grant needed; a harness change. Cost S-M.
- (c) One dedicated schema, reset each run: runs can collide.
- **Recommended (a), (b) if the role cannot.** Run by the Snowflake lanes' session.

**12. The frozen answers on Postgres and Snowflake** (V3; today DuckDB-WASM only).
- (a) A Java replayer: each of the 210 series' Show #TS statement over the samples loaded into the database, against
  the frozen digests. Cost M.
- (b) Through Python's engine against those databases. Cost M-L.
- (c) Skip; the island lanes stand in. V3 unmet.
- **Recommended (a).**

**13. P5, the scale budgets.** Proposed from B2's first measurement; ruled then.

### D. The product and its demos

**14. Class-first demos.** The user, 2026-10-27: "start all the charts from Class models". Today 3 tiles (10 series)
read classes; 210 series read tables.
- (a) The gallery and markets pages over the trading model's classes, after the class page's opening cost is fixed
  (11.9 s against 2.4 s, measured 2026-10-27; likely the 4 MB model compiled twice). Cost M + M.
- (b) Keep tables; the class page the showcase. Cost 0.
- (c) Both sources per page. Cost M; twice the frozen answers.
- **Recommended (a).**

**15. P2, the page period (B7).** Read as option D from "Yes do it" (2026-11-01).
- (A) each grid filtered by its own date column; (B) a page filter for class grids; (D) a page period: every timeseries
  tile shows it as its window, its data whole, grids and charts opting in through a date column -- recommended; (E) a
  linked window alone. The brushed tile zooms too (recommended: every tile alike). B7 is L; it needs 7 for FILTER.

**16. ClickHouse.** spec-15 never finishes there; the filter push-down setting may cost pruning on MergeTree tables.
(a) Not a v1 database (P4): spec-15 listed unsupported there, the setting measured when ClickHouse becomes one --
recommended, cost 0. (b) A staged temporary table for a relation read many times. Cost M-L.

### E. Process

**17. Landing #81** is the user's alone: CI green on every OS (2026-11-06). Not merged, no review asked, without the
user's word.

## 3. The order

Each phase ends green; the status column is updated in the commit that does the work.

| # | Item | Decision | State |
|---|---|---|---|
| 1 | Record the rulings: S151's row, R9's row (FILL(PREV) the way to step), to-many's row | 1, 3, 6 | done 2026-11-07: S151 ruled, S152 (R9: the dates both have; `FILL(PREV)` gs-quant's step but its backfill, read in gs-quant's `datetime.py` `align`), S153 (to-many refused; its message names the fix, no longer "a later FLATTEN"); the spec says a WITH body is one source and a join a navigation |
| 2 | R1's rows: `Window(w, r)`; `returns`' `logarithmic` and `absolute`; `beta`'s first three blanked; `sharpe_ratio`'s departure row | 4 | done 2026-11-07 but two parts moved: `beta` to date from its third return as gs-quant's (S154, `w = 2` one point earlier, named), `sharpe_ratio`'s row (S155); `absolute` was there already. **Moved to item 13 (B3)**: `Window(w, r)` -- re-costed M-L, not S: a ramp apart from the window touches both forms of some 30 words, and gs-quant's `rolling(w, 0)` computes partial windows after the ramp -- and `logarithmic` as gs-quant's spelling, both of which the Python DSL brings (`Window`, `Returns.LOGARITHMIC`) |
| 3 | A10: an independent review of `1e8d02b63` and after; its findings fixed | -- | done 2026-11-07: the third review (no blocking finding) -- a connection element's table-mapping post-processors were dropped (a query read the tables as written): now refused by name until applied (a roadmap item below); the Postgres lane ran its deep cases on DuckDB: run on Postgres, an as-of lag was refused, the as-of emulation's key finder fixed for every dialect (as-of joins run on Postgres, H2 and SQLite); a platform function declared by a model and its library refused as the one text; PlanMaker's staged let takes the CTE opt-out; Postgres's raise as a boolean in CASE and NOT; tests made real; nits |
| 3b | Apply a connection element's post-processors as upstream does (postprocessor.pure: table and schema mappers, the relational mapper, CTE extraction), lifting item 3's refusal | -- | |
| 4 | Scalar parameters in `#TS` | 7 | done 2026-11-07: a variable of the enclosing Pure no FROM reads is a value (spec R14) -- in FILTER, WHERE, an expression, a word's argument (a window too: `avg(x, $w)` answers as `avg(x, 3)` on H2, DuckDB, SQLite and Postgres) -- the plan's parameter, typed by the variable; Show #TS prints it; a list, an optional value, a name not in scope and `$1` refused by name |
| 5 | Values as a word's arguments; guide-06 runs | 8 | done 2026-11-07 (the user: "Why cant we pass scalar?"): `latest` in a word's date argument is a scalar -- upstream's relation `map` reduced (`$src->map(r|$r.date)->max()`, the latest of the sources'), moved by a span, passed to the word's own date overload; lite's lowering writes a reduction of a relation's map as one aggregate, `(SELECT MAX(date) FROM ...)`, where it built a list first (which H2 and SQLite cannot hold); `rebase` takes its date second as the spec writes it (`rebase(x, date, base = 100)`; gs-quant has no rebase; `index` keeps gs-quant's `initial` second). Every one of the documents' 29 examples runs. The rule is the library's (`timeseries::lines::lastDate`, `later`, `day`; the translator only calls them, TsTranslatorGuardrailTest); two sources' `latest` is the later of their last dates (`GREATEST`, every dialect); the tile's budget 130,500 -> 132,000 for the library's text (measured 130,640) |
| 6 | Business days as a primitive (`b` windows, weekdays) | 10 | not needed (2026-11-07): gs-quant has no business-day windows (`_to_offset`, `(\d+)([hdwmy])`; decision 10 corrected), so (a) stands as ruled -- calendars and clocks v1.1 |
| 7 | B2: scale at ten million rows on DuckDB and Postgres; budgets proposed | 13 | measured 2026-11-07, docs/TS_SCALE_2026_11_07.md (`//core:ts_scale_measure`, `_postgres`, manual): DuckDB answers one line in 81 ms, every line of 10M rows in 2.9 s, a basket of 500 in 9.8 s; a basket of 500 overflowed the typer's stack, fixed (N relations put together as a balanced tree). The Postgres research's question answered: lite's correlated `MAX` and the experts' `LATERAL` are alike (66-73 ms a line with an index, past 90 s without); lite's as-of is quadratic per line wherever there is no ASOF JOIN (each probe scans the line's CTE, no index reaches it) -- item 7b. On Postgres the 100 lines' query grew a backend to 11 GB (OOM-killed): open. The budgets proposed (§3 there), for the user to rule (P5) |
| 7b | As-of on the databases without an ASOF JOIN as one sorted window pass (O(n log n), plain SQL), not a correlated `MAX` per row | -- | done 2026-11-07 (414d6cc8d): `CarrierStrategies.asOfByWindow`; one line on Postgres 6.6 s -> 52 ms with the index (472 ms without), the experts' hand forms' speed; PCT on Postgres, H2 and SQLite, the H2 corpus, the integration lanes green |
| 8 | The class page's opening cost, then the gallery and markets pages over classes | 14 | the cost measured 2026-11-07 (verify_charts, the planner's requests traced): the markets example over the trading model's classes draws its first sheet 5.9-6.2 s after the click (11.9 s on 2026-10-27; A5 and this item's fix), the one over the sample's table 1.4 s. The fix: a project database asked for once a model (engine-client's `engineFor` asked the planner for the 1.9 MB of Data statements again for each saved query, 0.4 s). What remains is the 4.2 MB model read three times in the worker: `grammarToJson/model` for its data spaces and enumerations (0.8 s), held (1.4-1.6 s), and held again under the library (1.4-1.6 s) -- the core's question of answering both from the held model (a library layered over a held model; the elements a held model's), item 8b |
| 8a' | The markets page and the gallery copied over classes (§4) | 14 | done 2026-11-07 (81cc9ee6f, 7a13f5347): `markets-on-classes` and `gallery-on-classes`, the same views and sheets (built by the same functions), each grid a saved query of the markets project -- `MarketQuote`, `CurvePoint`, and the new `markets::MacroObservation` (the project's own `markets-model.pure`, its Data the samples' curve and macro series); every series of both answers as the pages over the samples froze it (stress-markets.test: the same digests); both draw in the browser (verify_charts: 6 and 69 timeseries; the first sheet 1.9 s and 3.8 s after the tab's first project page, which took 7.8 s with the bigger Data -- a once-a-tab cost, not optimized, §4); a series of two project grids read together over the project's model (projectTogether) |
| 8b | The library over a held model, and a project's elements from it: the class page's two repeated reads of its model (about 2.3 s of its 6 s) | -- | the library half done 2026-11-07 (ed067b2ca), page-side, no core change: a project's saved query and class planned over the library from the start (`ModelOptions.overLibrary`: the tab's planner given the joined text, held as the two apart under it, so its typing, its project data and every query find the one hold) -- the class page's first sheet 5.9-6.2 s -> 4.8 s (verify_charts; the tables' page 1.3 s); the startup budgets +100 bytes each, dated. Open: the elements read (`grammarToJson/model`, 0.8 s), the model parsed for its data spaces and enumerations beside the hold |
| 9 | The frozen answers on Postgres (the replayer); ClickHouse's spec-15 listed | 12, 16 | done 2026-11-07 (the replayer; ClickHouse a v1 database since that day, §4): `TsReplayTest` replays every gallery and markets series' Show #TS statement (210, held to `datacube/test/fixtures/timeseries/show-ts.txt` by the parity test) over the pages' samples (`//datacube:ts_replay_samples`) on DuckDB and on the lane's database, each answer held to DuckDB's within 1e-9 of its series' scale: Postgres 210 of 210 (27.7 s), ClickHouse 210 of 210 (18.3 s) once its dialect widened an integer beside a Float64 and stopped counting from a null map, and lead_lag's shift is one LAG of a signed lag (ClickHouse's dialect choosing lagInFrame or leadInFrame by its sign); Snowflake's `//core:ts_replay_snowflake_test` (manual) is the Snowflake session's to run. H2 and SQLite are no lanes (P4); measured before the ruling, SQLite matched all 210 and H2 ran three statements past a 60 s bound. ClickHouse's spec-15 is no longer listed: item 9b |
| 9b | The documents' examples on ClickHouse, spec-15 included (it never finishes there; decision 16 (b), a staged relation read many times, or another fix), ClickHouse now a v1 database | 16 | done 2026-11-07: the documents' examples run on each v1 database beside DuckDB, held to its answers (TsDocumentsTest's lanes, `ts_documents_<db>_test`): ClickHouse 29 of 29, Postgres 28 of 29 (spec-16 past 120 s there, listed UNRUN: item 18). spec-15 never finished on ClickHouse because it inlines a WITH at each read (111 KB of SQL, 2.8 MB inlined): a relation the statement reads more than once is now staged once as a session's temporary table where the database writes each read again (`SqlDialect.writesEachReadOfAWithAgain`, PlanMaker.stagedShared; 3.6 s); the steps run unbracketed where the driver takes no transaction (a sixth driver fact). ClickHouse's tables are legend-engine's MergeTree, a keyed one sorted by its key (S157, ruled). The lanes moved to ClickHouse 26.3 LTS (ruled, §4), where two identical raises in a WHERE were made one eagerly computed expression: each raise is now its own (rand(n)). ClickHouse's PCT, the replay (210) and the documents (29) green on 26.3 |
| 10 | Snowflake: its workspace, then the island and the replayer there, by the Snowflake session | 11 | the island 2026-11-07: 43 of 57, then 55 of 57 at c77bb9948 (the as-of's parenthesized condition; java.time binds by a connector fact; PERCENTILE_CONT of a level that is no literal interpolated; CORR and the covariances under an ordered frame from its sums; a ROWS frame past 1000 rows as RANGE over the rows' numbers), the last two since fixed: lead_lag's LAG of ABS(k) (one LAG of a signed k now), and a frame offset from a Pure variable, which Snowflake takes only as a literal or a session variable (measured: a bind and an expression are syntax errors), refused by name -- open: a bound value through SET var = ? before the query, a plan step the runner does not take yet. Done 2026-11-07 by the Snowflake session's runs: the island 57 of 57, the gallery's replay 210 of 210 (`ts_replay_snowflake_test`), and the documents' examples 29 of 29 as on DuckDB (`ts_documents_snowflake_test` at a7dab2c05, 529 s; the lane sets no statement bound): spec-13 130.5 s, spec-15 79.7 s, guide-03 70.2 s, guide-13 43.0 s, guide-07 39.6 s, spec-02 22.2 s, spec-16 13.1 s, the other 22 under 4.3 s each (DuckDB about 6 s for all 29) -- almost all compile time, item 18b |
| 11 | A3 step 3, then steps 4 and 5: the tile stores `#TS` text; one reader | 2, 9 | step 3's store done 2026-11-07 (c23e4dee5 .. 5c66ec1ac): a series' own `#TS` text (another series named, `A`; every parameter told; gs-quant's spellings the language's) written by `timeseries-text.ts` and read back from core's tree, proved over every example series and 18 forms; the page document version 7 (6 refused, 9a); core reads every text -- a page's on opening (the host's legend-lite module, `QueryReading.tsItem`), the editor's as it is typed, Python's (headless `timeseriesMade`/`Edit`); the examples in `#TS`; core's `tsItem` answers a text with its tree or an error, never one alone. Open in step 3: the box's completions (9b) and its error's range; then steps 4 and 5, step 5 keeping the thin builder (§4, the builders) |
| 12 | B5: a second oracle for the words gs-quant lacks | -- | |
| 13 | B3: the builders -- Python's DSL and TypeScript's, `#TS` and the chart together, from one catalog (§4, ruled 2026-11-07) | -- | |
| 14 | B4: Show Python and Show TypeScript, each the builder's code | -- | |
| 15 | B7: the page as a dashboard (option D) | 15 | |
| 16 | B8: the tile kinds | -- | |
| 17 | v1's gate: every check, then CI on #81 when the user asks | 17 | |
| 18b | The warehouses' remaining costs, found by the replays (2026-11-07): Snowflake COMPILES the gallery for 18 minutes and executes it in 9 s (99.1% compile, read from its query history by the Snowflake session; the exponential family worst, macd C 344 s of compile -- 21 nested SELECT levels, windows inside windows, each level's values read several times; its EXPLAIN alone, which compiles and runs nothing, 369.1 s of compile and 6 ms of execution, measured by the Snowflake session 2026-11-07 over the gallery's samples; the documents' spec-13 130.5 s and spec-15 79.7 s there); Postgres's calendar-period pair words (spec-16, past 120 s) | -- | open: an A/B of macd C's compile time without lite's own level merging (SelectMerge, SelectLayers) on Snowflake first: the Snowflake session holds the baseline (369 s, EXPLAIN) and a harness that compiles a statement's text |
| 18 | F3: Postgres's wide table (the 100 lines' statement grows a backend past memory, at 1M rows as at 10M) -- the spread read once and pivoted, not a join a line (the lean SQL) | -- | added 2026-11-07 (the user: "Add fixing postgres to roadmap") |

## 4. Rulings of 2026-11-07, the second set

Asked with pros, cons and costs (the decisions 1-10 of the message of that day); the user's words:

- **Class-first pages: (a')**, "I like A' that's fine - basically copies of the charts that start with classes": the
  markets page and the gallery copied over the trading model's classes (each grid a saved query of the markets project,
  its columns the samples' names, so every series reads as written), the catalog opening the copies; the sample pages
  stay as the frozen answers' fixtures, unchanged.
- **The gallery's Dates sheet: a macro class**, "Add macro class obviously!": a class of the macro series and its Data in
  the markets project, so the copy's every grid starts from a class (recommended (a), keeping that grid a table, was the
  cost-weighted answer and the wrong one for a class-first page).
- **P5, the scale budgets: (b)**, "Pin sql size and plan is fine": each benchmark's SQL size (its measure + 5%) and plan
  time pinned in the scale targets; run times reported, not pinned.
- **F3: on the roadmap** (row 18).
- **Answers across databases: a tolerance, smartly** ("I think we need some smart way to do tolerance or bad idea?"):
  DuckDB's frozen digests stay exact (the gate); another database's answers are compared with DuckDB's, run beside them
  in the same replay, point by point -- the same dates and lines exactly, each figure within 1e-9 of the series' own
  scale (its largest magnitude), so a sum taken in another order passes and a wrong figure does not; a difference of
  meaning is a ledger row, named.
- **The model's reads: not optimized** ("Let's not optimize model read if only one time cost"): the elements read and
  the hold are once per tab and project (the planner and `once(projectElements)` keep them), so 8b's rest is dropped.
- **The test Postgres's version: later** ("Can do postgres upgrade later").
- **The bundle budgets raised under this run: kept** ("Budget is fine").
- **Snowflake (item 10): the warm Snowflake session** ("Yes snowflake send to warm session"): sent 2026-11-07 to the
  Snowflake lanes' session -- the workspace, a manual island lane, its report.
- **CI on #81: when useful, not waited on** ("If need CI go for it but I wouldn't wait on it").
- **The v1 databases: all four** (the user, "Why are we doing anything with h2? We said focus on duckdb clickhouse and
  snowflake didn't we?", then, asked against P4's record of DuckDB, Postgres and Snowflake: "All four"): DuckDB,
  Postgres, ClickHouse and Snowflake. H2 and SQLite are no v1 databases (the replay's lanes are the other three);
  ClickHouse's decision 16 reopens as item 9b.
- **ClickHouse: 26.3 LTS** (asked against the pin, 25.8.2.29, the 2025 LTS; 26.3 the current LTS, 26.6 the newest):
  "Upgrade to 26.3 LTS". Pinned 26.3.60.2. Its `WITH ... AS MATERIALIZED` (experimental, off by default) would compute
  a shared relation once natively; the planner's staging stays until it is not experimental.
- **A keyed ClickHouse table** (legend-engine's `ORDER BY tuple()` refused beside a PRIMARY KEY): "Sort by its key"
  (register S157).
- **The builders: one thin typed builder, `#TS` and the chart together, Python and TypeScript from one catalog**
  (asked: "Should we keep/evolve the typescript to be a typed builder for TS like we want for python or not worth it",
  then "what would be really cool I think would be to have an integrated #TS and charting api together in typescript --
  the #TS typed part can still be thin I think? But feels like you can do gs-quant / #TS and build the chart in nice
  way"; on the recommendation below, "Yes record it, then keep going in order"):
  - *The `#TS` part, thin and write-only*: each word a typed function generated from core's catalog
    (`datacube/src/generated/ts-words.ts`, the Pure library's words: their parameters, kinds, options, whether given),
    a window `number | 'all' | span`, an option its words' union; a term prints itself (`timeseries-text.ts`
    `seriesText`) and core reads it back -- core stays the one reader, the builder never parses (step 5's guard: no
    tokenizer, no word table by hand in `datacube/src`). TypeScript writes arithmetic as methods (`.div()`, `.minus()`,
    `.gt()`: no operator overloading); Python's DSL has the operators.
  - *The chart part*: a typed face over the tile's spec -- sources by grid, `plot(term, {pane, style, ...})` returning a
    handle another series reads by name (`A`), grain, range, view, panes, annotations -- each call one of the tile's
    changes (`timeseries-edit.ts` TsChange) through `editTimeseries`, told as the page's commands (`addTimeseries`,
    `setTimeseries`): a third front of the one rule, beside the screen and Python, never a path of its own.
  - *What it moves*: the series editor's function picker, candles and sliders -- a gesture's meaning decided in screen
    code today (`ui/timeseries-view.ts`; the one-API plan's S8) -- move into the builder, exported headless.
  - *Guards*: the change-parity ledger (`one-api-ledger.test.ts`, the timeseries review's A8) holds the builder to every
    change kind too; the builder is no part of the page's startup bundle.
  - *Where*: B3 becomes the builders (Python's DSL and TypeScript's, generated from the one catalog); B4 prints each
    tile as the builder's code (Show Python, Show TypeScript; the generated program run and its spec read back, as
    `timeseries-code.test.ts` does now); A3 step 5 deletes the tile's parser (`parseExpr`), its translator
    (`timeseries-ts.ts`) and the hand-kept `TS_SIGNATURES` (the generated signatures in their place), and keeps the
    tree and `seriesText` as the builder's core. Cost: M beside B3.

