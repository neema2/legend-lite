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
| 11 | A3 step 3, then steps 4 and 5: the tile stores `#TS` text; one reader | 2, 9 | step 3's store done 2026-11-07 (c23e4dee5 .. 5c66ec1ac): a series' own `#TS` text (another series named, `A`; every parameter told; gs-quant's spellings the language's) written by `timeseries-text.ts` and read back from core's tree, proved over every example series and 18 forms; the page document version 7 (6 refused, 9a); core reads every text -- a page's on opening (the host's legend-lite module, `QueryReading.tsItem`), the editor's as it is typed, Python's (headless `timeseriesMade`/`Edit`); the examples in `#TS`; core's `tsItem` answers a text with its tree or an error, never one alone. The box's completions and its error's place done (c047d41b0). Step 4 parked (§4); next the order of §4 (the order to one reader and the builders): generated signatures (done 2026-11-07), the term builder (done 2026-11-07), the parser deleted (done 2026-11-08), the statement writer |
| 12 | B5: a second oracle for the words gs-quant lacks | -- | done 2026-11-08 (docs/TS_GSQUANT_ORACLE_2026_11_01.md, B5): each of the 39 rows gs-quant could not read is read by TA-Lib, statsmodels or pandas, from the word's doc -- all 39 agree (every field of a struct); the oracle AGREE 183, DEPARTS 2 (S145, S150), NOT-CHECKED 25, no DIFFER; two of the library's sentences corrected (`total_return` is no gs-quant word; `percent_change` a share) |
| 13 | B3: the builders -- Python's DSL and TypeScript's, `#TS` and the chart together, from one catalog (§4, ruled 2026-11-07) | -- | done 2026-11-08 with `Window(w, r)` (§4's order, step 5) |
| 13b | The risk-free data model: a cash index per currency (overnight rates compounded actual/360), `sharpe_ratio` and `excess_returns` reading it as a line, the builders' `currency` the page's source, gs-quant's 365.25-day annualized return (S155 retired), its sample data, the oracle on gs-quant's own functions | -- | added 2026-11-08 (the user). Steps 1-2 done 2026-11-08: `sharpe_ratio` is gs-quant's `get_ratio_pure` of the line as excess-return levels (S155 retired; its calendar form as of a period back, S162, the user's ruling); a risk-free Sharpe is `sharpe_ratio(excess_returns(x, cash.index['USD']), w)`; `cash_index(rate, day_count)` (actual/360 or actual/365 fixed, gs-quant's `DayCountConvention` names); the `cash` sample (gs-quant's seven currencies' overnight rates, synthetic, shaped like SOFR's to SWESTR's, and their indices) and `markets::CashRate` in the markets project; the gallery's Sharpe ratios over dollar cash and excess returns over it -- gs-quant itself agrees on all four (the oracle: AGREE 184, DEPARTS 2, no DIFFER), Postgres and ClickHouse replay 210 of 210. **Done 2026-11-08** with step 3, the builders' `currency` and `curve_type`: gs-quant's `sharpe_ratio(x, RiskFreeRateCurrency.EUR, 252)` (`PRICES`, its default) writes `sharpe_ratio(excess_returns(x, cash.index['EUR']), w => 252)` -- the page's cash line by convention, the source aliased `cash`, its column `index` -- and `curve_type=CurveType.EXCESS_RETURNS` writes `sharpe_ratio(x, w => 252)`; Show Python and TypeScript print them back so, each typed as gs-quant's enumerations. Left: gs-quant's `excess_returns(x, Currency.USD)` (its `Currency`, not the builders' yet) |
| 13c | Words across a date's instruments (kdb's `by date`, as `rank` is): `zscores`, `winsorize` and `percentile` of each date's cross-section, the factor researcher's normalization -- no look-ahead, another question than a line's history | -- | added 2026-11-08 (the user: "Go" on the recommendation) |
| 14 | B4: Show Python and Show TypeScript, each the builder's code | -- | done 2026-11-08 (§4's order, step 5) |
| 15 | B7: the page as a dashboard (option D) | 15 | |
| 16 | B8: the tile kinds | -- | |
| 17 | v1's gate: every check, then CI on #81 when the user asks | 17 | |
| 18b | The warehouses' remaining costs, found by the replays (2026-11-07): Snowflake COMPILES the gallery for 18 minutes and executes it in 9 s (99.1% compile, read from its query history by the Snowflake session; the exponential family worst, macd C 344 s of compile -- 21 nested SELECT levels, windows inside windows, each level's values read several times; its EXPLAIN alone, which compiles and runs nothing, 369.1 s of compile and 6 ms of execution, measured by the Snowflake session 2026-11-07 over the gallery's samples; the documents' spec-13 130.5 s and spec-15 79.7 s there); Postgres's calendar-period pair words (spec-16, past 120 s) | -- | open: an A/B of macd C's compile time without lite's own level merging (SelectMerge, SelectLayers) on Snowflake first: the Snowflake session holds the baseline (369 s, EXPLAIN) and a harness that compiles a statement's text |
| 18 | F3: Postgres's wide table (the 100 lines' statement grows a backend past memory, at 1M rows as at 10M) -- the spread read once and pivoted, not a join a line (the lean SQL) | -- | added 2026-11-07 (the user: "Add fixing postgres to roadmap") |
| 19 | Beyond gs-quant: a survey of what quants use that gs-quant lacks -- kdb+/q's verbs, the other quant libraries' -- each found function or feature weighed, the ones wanted proposed as words of the library; after every item above, a task of its own | -- | added 2026-11-08 (the user: "find any other functions/features that quants need that might not be in gs-quant but in kdb or other quant libraries that we would want to add - that can be after everything else as separate task") |

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
- **The order to one reader and the builders** (asked "Is that the right plan", then "Looks good to me"):
  A3 step 4 (the problems from the compiler) parked ("Skip for now"); then, so nothing is deleted and made again
  (the user: "before we delete tile grammar check the plan we just wanted to do typescript API so we don't delete and
  then create similar thing twice ... let's be thoughtful of the order"):
  1. the words' signatures and facts generated from core's catalog -- with gs-quant's spellings (above) -- in place of
     the hand-kept `TS_SIGNATURES` and `TS_FUNCTIONS`; **done 2026-11-07**: `datacube/src/generated/ts-signatures.ts`
     (each word's lines and their roles, its parameters' kinds, the library's stated defaults and limits --
     `timeseries::Words.given`/`.limit`, each default proved its call without it by
     `TsIslandTest.everyWordsStatedDefaultIsItsCallWithoutIt` -- whether every call gives one, and gs-quant 2.2.3's own
     default, read by `tools/ts-oracle/signatures.py` into `projects/timeseries/gs-quant-signatures.json`) and
     `ts-facts.ts` (the words' facts, a module of their own, fetched with a tile); SQL's eight per-point words keep a
     small hand table until A4. The library names its lines by role (`total_return`'s price and dividends, the
     benchmark words' benchmark). A parameter neither told nor defaulted is left out (an annualization factor: the
     frequency's; a compare's method: the tile's align); one every call gives is refused when absent; a picked word
     begins with gs-quant's value where the language has none (a mean's window `all`, gs-quant's `Window(None, 0)`;
     a power's 1). Found on the way: a cube alone (`CubeApp`'s own board) had no `#TS` reader since 11b, so its
     timeseries tile said so and drew nothing -- `CubeAppBaseOptions.readText` passes its host's; the Query app's cube
     has none yet (its planner, `WasmGrammar`, does not read `#TS`), and says so;
  2. the builder's term layer: the typed word functions generated from 1, columns, series by name, numbers, operators;
     **done 2026-11-07**: `datacube/src/timeseries-builder.ts` (the module a person imports; no part of any startup
     bundle) -- each word a function of gs-quant 2.2.3's name (`max_`, `if_`, `relative_strength_index`,
     `basket_series`), its parameters gs-quant's names, order and defaults, its keyword arguments a trailing object
     (`volatility(spx, 22, Returns.LOGARITHMIC)`, `volatility(spx, { annualization_factor: 52 })`), generated by
     `tools/ts-builder/make.mts` from core's catalog and gs-quant's own signatures into
     `src/generated/ts-builder-words.ts`; gs-quant's `Window` (its size, `Window(None, 0)` every observation, a period;
     a ramp apart from its size refused until item 13) and enumerations (`Returns`, `Interpolate`, ...); columns
     (`column('close', 'SPX')`), series by id (`series('A')`), pandas' methods for arithmetic (`.div`, `.sub`, `.gt`);
     SQL's per-point words gs-quant's algebra's (`log`, `abs_`, `power`, `not_`). Where gs-quant names a parameter or
     spells an option otherwise the library says so as data (`timeseries::Words.gsParam`, `gsValue`, `gsAlso`); every
     other difference is a row of `test/timeseries-builder.test.ts`'s ledger with its reason (a gs-quant parameter the
     language computes none of taken at gs-quant's value alone -- `assume_zero_mean`, `sharpe_ratio`'s risk-free
     `currency`; `var_` for TypeScript's reserved `var`; `union`/`append`/`prepend` a list of two; the library's
     `annualization_factor` by name where gs-quant infers it; `smoothed_moving_average`'s window told, gs-quant's
     whole-series seed none the language computes; `lag`'s `LagMode.EXTEND` taken though its future dates are not made
     -- gs-quant's `TRUNCATE` the language's own). Each word's default call is text core reads back as its own tree;
     its names, order and defaults are held to gs-quant's own file;
  3. the tile's code and its tests moved onto it (no `parseExpr`), then the tile's parser and printers deleted -- every
     formula the tile holds core's or the builder's; **done 2026-11-08**: every test builds its series with the builder
     or writes `#TS` that core reads (`readSeriesTexts`) -- the queries, the Pure library spike, stress-markets, the
     two measures, the chart tests (their series as the builder's code), the Python engine's fixture; the parity test's own-text cases are built
     with the builder, a tree no text reader made. The marks' and the shade's rows are read by core as a column's
     brackets (`timeseries-text.ts` `readMatches`; written `'AAPL', side = 'BUY'`, the language's spelling, by
     `matchesText`). The gs-quant oracle (`tools/ts-oracle/oracle.py`) reads each series' tree, every parameter told,
     and no text: AGREE 144, DIFFER 2 (the same two), no ERROR (the text path had sent `all` as a zero window). Then
     `timeseries-expr.ts`'s tokenizer, `parseExpr`, `exprText`, `exprSource`, `matchesText` and `parseMatches` deleted
     (425 lines). The guard (`test/guardrails.test.ts`, "the timeseries language has one reader, core's"): none of that
     grammar in `datacube/src`, and `TS_FUNCTION_NAMES` core's generated catalog's and SQL's per point -- the catalog's
     words the tile draws none of yet (`rank`, `round_down`, `round_up`, `value_at`) named, only allowed to shrink.
     Begun beside it: the builder's source (`timeseries-builder-source.ts`, a tree as the builder's code, refused in
     words where gs-quant has no call of its meaning), B4's Show TypeScript's core;
  4. the statement writer, once: a tile's settings and its series' texts as one `#TS` statement (the translator's
     adapting -- a frequency's aggregate, the compare's date, a split's subscripts, the tile's per-point rules -- merged
     with `seriesText`), the tile's and the builders'; its semantic questions (a condition drawn as 1 or 0, the tile's
     bounds where a line has none, a frequency's aggregate inferred or written) put to the user before it is built;
     **done 2026-11-08** on the rulings below: core reads a condition in a series' value as 1 or 0 (`TsToPure` `valued`,
     S160) and two-argument `floor`/`ceil` as gs-quant's bound (library words, the parser's D2 refusal narrowed to one
     argument, S161); the tile's statement's items are written by the series' own writer (`timeseries-text.ts`
     `statementItem`: `seriesText` but each column as the statement reads it -- its source, its split's subscript, its
     period's aggregate under the frequency, the role a word reads it by -- and a base's `from`), the translator's
     per-point `CASE`s, its `greatest`/`least` and its own word spelling gone; six Show #TS statements changed (the
     conditions and the bounds), every frozen answer the same, replayed on Postgres and ClickHouse;
  5. B3's chart face and Python's twin, then B4. **B3 done 2026-11-08, `Window(w, r)` with it** (a ramp apart from its
     size, moved here from item 2; ruled "add it" 2026-11-07): the library's `r` on every windowed word (31 written by a
     script from each word's two forms; `mean`, `bollinger_bands` and `winsorize` by hand):
     gs-quant's `rolling(w, 0)` then `apply_ramp` -- each figure before the w-th point over the points to date, none of
     the first r, nothing where the window is longer than the line (`helpers::ramp` stitching the word's own to-date and
     windowed forms; `bollinger_bands` by its frame's partial windows; `winsorize`, whose window gs-quant reads for its
     ramp alone, its first r dropped); the builders write `Window(63, 10)` as `w => 63, r => 10` and print it back,
     Python's and TypeScript's (`winsorize(x, 2.5, 22)` as `w => whole, r => 22`). Refused, in words: a calendar
     window's or a calendar ramp (`Window('3m', 10)`), `winsorize`'s size apart from its ramp (it only empties a shorter
     line), `smoothed_moving_average`'s (S159: its seed no window). Checked against gs-quant's rule, computed by hand, on
     the in-process databases and Postgres (`TsIslandTest.aWindowsRampApartFromItsSizeIsGsQuants`); B5's oracle runs
     gs-quant itself. The chart face (`datacube/src/timeseries-chart.ts`
     `Chart`, exported with the builder) -- each method one of the tile's changes made by `editTimeseries`, a plotted
     series a handle a later term reads by name; the change-parity ledger holds it to every change kind (`state` listed:
     a frozen tile is the page's). Python's twin (`python/legend_lite/timeseries.py`): gs-quant's names, signatures,
     enumerations and operators, generated from the same catalog as TypeScript's (`make.mts` emits `_ts_words.py`), each
     term a gs-quant call told as JSON that headless DataCube makes with the TypeScript builder's own functions
     (`timeseries-builder-wire.ts`) -- one mapping to the tile's tree -- plotted by `Timeseries.plot`/`edit` and
     `page.timeseries`; Python's chart face is its `Timeseries` methods, already one per change. **B4 done 2026-11-08**:
     the Code window and `Timeseries.code` print each series as gs-quant's call of it (`timeseries-builder-source.ts`, in
     TypeScript or Python, each option gs-quant's enumeration's member), its `#TS` text where gs-quant has no call of its
     meaning (a z-score to date; a compare by the tile's align); every gallery series as Python's code is held with its
     text (`test/fixtures/timeseries/builder-python.json`) and run in Python by `//python:ts_builder_test`; every gallery
     tile's TypeScript program, run, builds its tile; `legend_lite.timeseries.text(term)` says a term's text;
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
  - *Written and read by people, and gs-quant's own spelling* (the user, 2026-11-07: "let's make sure though that the
    typescript API we build can also be written/read by humans and is close if not identical to gs-quant"): the builder's
    code is what a person writes and what Show Python and Show TypeScript print -- it reads as a gs-quant notebook
    does. Each word's name, its parameters' names, order and defaults, and its types (`Window`, `Returns`, ...) are
    gs-quant 2.2.3's: read from gs-quant itself (its signatures, `inspect.signature`, in the oracle's environment,
    tools/ts-oracle), never recalled; carried as the library word's data beside its `gsQuant` tag (which names the
    function already) and generated into both builders. Python's is gs-quant's call as written, its operators too;
    TypeScript's the same calls (`volatility(spx, Window(22, 0), Returns.LOGARITHMIC)`), its keyword arguments a
    trailing object, its arithmetic methods (`spx.div(ndx)`: TypeScript has no operator overloading) -- the one
    difference it cannot avoid. A word gs-quant has not is the library's own name; every other difference from
    gs-quant's signature is a listed row with its reason, and a test (a manual lane, as the oracle's) holds each
    generated signature to gs-quant's. The `#TS` text the builder writes stays the language's spelling (`w => 22`).


- **The statement writer's three questions** (step 4 of the order to one reader, asked 2026-11-08 before it is built):
  - *A condition*: "1 or 0, as gs-quant" -- a condition whose value is a series' value (an item, a word's argument, an
    operand of arithmetic) is 1 or 0, nothing where a side is nothing; one a clause tests (`WHERE`, `FILTER`, `CASE
    WHEN`) stays SQL's boolean (register S160).
  - *floor and ceil*: "Two-arg = gs-quant's" -- `floor(x, value)` and `ceil(x, value)` are gs-quant's bound, nothing
    where `x` is nothing; one argument stays refused (D2), `round_down`/`round_up` SQL's rounding (register S161).
  - *A frequency's aggregate*: asked "How do expert systems like kdb do this", answered from their documents -- kdb's
    bar query writes each column's aggregate (`select last price, sum size by 10 xbar time.minute from trade`), a
    column selected without one is a nested list, never refused, and only its empty select applies `last` to every
    column; gs-quant's `bucketize(series, aggregate_function, period)` and Flux's `aggregateWindow(every, fn)` ask for
    it; QuestDB's `SAMPLE BY` takes a bare column for a key -- then "Language explicit, tile writes it": `#TS` keeps R5
    (a bare column under `SAMPLE BY` refused); a series' text stays as typed (`close['SPX']`), and the tile's
    frequency writes each column's aggregate into the statement (`last`, a candle's or a technical's roles: open
    `first`, high `max`, low `min`, volume `sum`), a column whose text says its own keeping it; Show #TS prints them.
- **gs-quant's defaults, aligned** (the user, 2026-11-08: "What are the other differences? Should we try to align the
  defaults then we can change our code to not use the defaults if that makes sense", then on the recommendations
  "Okay I agree with everything, let's go"):
  - *lag*: gs-quant's `LagMode`, `extend` the default (its last values on the calendar days after the line's end),
    `truncate` the option ("Yes extend default to align with truncate option, can refreeze with truncate option"); our
    series and tests that meant the old lag write `truncate`. A rows form now says the argument it computes alone
    (`timeseries::Words.when`), so truncate keeps lag's rows form.
  - *Windows*: `range`, `volatility`, `median` (22), `bollinger_bands` (20), `correlation`, `beta`, `cov` (63) and
    `sharpe_ratio` (252) default to every observation to date, gs-quant's `Window(None, 0)`; `percentile`'s `n` and
    `smooth_spikes`' `threshold` are given in every call, as gs-quant's (a picker begins them with the library's
    `timeseries::Words.begun`: 50, 0.5). `compare` already took gs-quant's `step` where no FILL says otherwise.
  - *Kept as they are, rows of the register*: one-argument `sum`/`mean`/`max`/... without SAMPLE BY refused, not
    gs-quant's to date (S158: one meaning per spelling); `smoothed_moving_average` 14, gs-quant's default reading the
    future (S159).
  - *percentile, zscores and winsorize: the expert way* (asked "What is the EXPERT way to do this and zscore and
    winsorize?", then "Did you look at kdb before answering" -- kdb spells a whole group's statistic (`avg`, broadcast
    by `update ... by`), a running one (`avgs`) and a moving one (`mavg`), never leaving it to a default -- then "Go"):
    to date unless the whole line is said, for all three (`percentile(x, n)` and `all` to date since 2026-11-08, where
    it had been the whole line's); `whole` the whole line's, the future read (`percentile(x, n, whole)` new); the
    builders write gs-quant's whole-series calls as `whole`, visibly (S150's builder half, reversed); the tile holds
    `whole` as a window (`TsParamShape.whole`). Cross-sectional versions -- across a date's instruments, kdb's `by date`
    -- a roadmap item (13c).
  - *A risk-free data model*: a roadmap item after B3 (13b) -- a cash index per currency from overnight rates, the
    words reading it as a line, the builders' `currency` mapped to the page's source, gs-quant's arithmetic checked by
    the oracle (the user: "We should have a risk free data model with data to implement those").
