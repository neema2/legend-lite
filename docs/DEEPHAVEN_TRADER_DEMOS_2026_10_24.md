# Trader screens on Deephaven: the plan (2026-10-24)

> The Deephaven line's demo program, on `claude/deephaven-3-browser` (the line's handoff: `docs/DEEPHAVEN_START_HERE.md`;
> its design: `docs/DEEPHAVEN_PLAN_2026_10_22.md`, whose decisions this continues from DH28). Nothing here goes to `main`
> before the licence answer (the start-here page's rule).

## 0. What it is

The user, 2026-10-24: "build a bunch of realistic trader screens that demo realistic features at realistic speeds -- a
bunch of cool/realistic/expert demos using as much platform as possible -- if need to do things outside of platform in
bespoke scripts that's okay to start but write them down as things to think about pushing into platform as well"; "a
realistic stock ticker with real ticking stock chart"; and "any feature missing for deephaven you can claim here and
own/do ... can also add chart here".

So each screen is a model (a `###Relational` store over Deephaven's tables, classes and mappings where they serve),
Pure queries, and a DataCube page (grids, pivots, charts, KPIs) -- the platform -- and what the platform cannot yet do is
either built into it here (the target, DataCube) or done bespoke and written down in §3 with the platform's home for it.

## 1. Decisions

| # | question | decision |
|---|---|---|
| DH34 | the trading model | **one: the stress corpus's** (the user, 2026-10-25: "one trading model for everything"). Its 20-domain financial model -- instruments, books, desks, traders, trades, parent orders and child executions, positions, P&L, limits, greeks, quotes, FX, volatility -- every expectation its Python oracle's, run on DuckDB and H2 and against legend-engine. The screens query its classes through `stress::AllMapping`; the market world publishes its tables live (not its own `market::Store`); what the screens need that it lacks is added to it, through its generators and its oracle, never beside it (§6) |
| DH28 | the data | **a simulator always; a public crypto feed where it can be reached, the simulator in its place where not** (the user: "only do both if crypto is a runtime dependency that could be blocked by enterprise so that people who cannot access the data can still see a demo"). Measured from a cloud session, 2026-10-24: Coinbase's and Kraken's public market data answer, Binance's answers 451 -- a feed can be refused by where a person is, not only by a firewall. The page says which world it shows. Real-time equities data is exchange-licensed: never in this public repository |
| DH29 | time on Deephaven | **Pure's DateTime is UTC wall-clock**, as on every SQL lane (DuckDB's `TIMESTAMP`); on Deephaven an `Instant`, a StrictDate a `LocalDate`. Every calendar function reads the instant in UTC (`timeZone("UTC")` in the formula); `timeBucket` is `lowerBin` from the Unix epoch (a week's from Monday 1969-12-29, DuckDB's origin); a calendar unit Deephaven's bins do not hold (months, years) is refused by name until a case |
| DH30 | `now()` | **refused by name on Deephaven** until a live view's sliding window is designed: Deephaven evaluates a filter on a row when the row changes, not as time passes, so `ts > now() - 5 minutes` would keep a row that has aged out -- a wrong answer at a tick (invariant 4, DH27) |
| DH31 | who owns what | **this line owns every missing Deephaven feature and whatever its demos need, charts included, on this branch** (the user, 2026-10-24). What touches another line's files is noted in `docs/IN_FLIGHT.md` when it lands |
| DH33 | what a formula may call | **Deephaven's own function library, and the instance methods the server's validator permits** -- measured on 42.7 (2026-10-24, K1): a formula calling `DayOfWeek.getValue()` was refused ("User expressions are not permitted to use method getValue() on class java.time.DayOfWeek"); the defaults (`dh-defaults.prop`, `ColumnExpressionValidator.allowedMethods.*`) permit any instance method of the boxed primitives, `String`, `BigInteger`, `BigDecimal`, `Number`, `Instant`, `LocalTime`, `LocalDate` and `ZonedDateTime`, and `toString`, `equals` and `hashCode`. So the target writes a library function where one answers (`dayOfWeekValue`), a permitted method otherwise, and nothing else; a server configured tighter refuses by name, as here |
| DH32 | bespoke work | **allowed to start, never silent**: each piece a row of §3 -- what it does, why it is bespoke, the platform's home for it |

## 2. Landings, in order

Each lands with its tests and what it measured, as the line's other landings (`docs/DEEPHAVEN_PLAN_2026_10_22.md` §9).

| # | content | judge |
|---|---|---|
| K1 | **built 2026-10-24** (§5). **Time on Deephaven** (DH29, DH30): a DateTime literal, in a formula and in a filter the server reads without compiling (DH25); `timeBucket` as `lowerBin`; a date-time's calendar parts (year to second, day of year, ISO day of week) in UTC; `datePart`; `firstDayOf...` and the truncations DataCube's time grains write; `adjust` by a fixed unit; the epoch conversions | the dialect's tests; answers worked out by hand on the pinned server (`datacube/test/deephaven/page.ts`) |
| K2 | **built 2026-10-24** (§5). **The market world**: a realistic equity simulator and a crypto feed handler that gives way to it (DH28), as the server's startup scripts; since 2026-10-25 publishing the corpus's own tables, queried through its classes (DH34, §6) | the world's figures checked on the pinned server; the fallback taken when the feed is refused |
| K3a | **built 2026-10-24** (§5). **A candle's open and close on Deephaven**: Pure's `minBy` and `maxBy` over a whole-number order (a trade's sequence number), SQL's `ARG_MIN` and `ARG_MAX`, as the server's `sortedFirst` and `sortedLast` | the dialect's tests; DuckDB's answer on the pinned server |
| K3 | **built 2026-10-24** (§5); its ticking in a browser is K4's page. **A candlestick chart** in DataCube: open, high, low, close and volume by time bucket, drawn again at its grid's tick with its axes held (C2) | DataCube's chart tests; the chart ticking in a browser check |
| K4 | **the ticker**: a watchlist (last, change, bid/ask, volume, day VWAP, high/low), the tape, time and sales, and a live candle chart for the symbol picked | a browser check of every part ticking; the speeds of §4 measured |
| K5 | **built 2026-10-25** (§6). **The runtime error on Deephaven**: the lowering's guard (Pure fails on `sqrt` of a negative, on an Integer division by zero) writes an `ERROR` no Deephaven formula holds yet; a formula that fails by name, so `sqrt` and Integer `/` plan | the dialect's tests; a failing row failing the view, said, on the server |
| K6 | **built 2026-10-24** (§5), but for NTH_VALUE; EMA and delta 2026-10-25 (§6). **Windows as `updateBy`**: running and moving figures (by rows and by time), lag and lead, first and last, the ranks; top-N per group (a row number filtered, SQL's QUALIFY: no `headBy` needed); DataCube's window functions and show-as | each against DuckDB's answer |
| K7 | **as-of joins as `aj`** (DH20) | the same |
| K8 | **the market map**: a treemap of every name, sized by value, coloured by the day's change, ticking | a browser check |
| K9 | **real-time P&L with limits**: day P&L against the prior close by desk and book, KPI tiles that turn red past a limit | the same |
| K10 | **the execution blotter**: parent and child orders, fills, % filled, slippage against arrival in basis points, by strategy and venue | the same |
| K11 | **the options desk**: greeks as Pure functions run by Deephaven as formulas (needs K5's `sqrt`), vega by expiry and strike | the same |
| K12 | **FX e-trading**: a rates ladder, cross rates by live joins, RFQ hit ratio | the same |
| K13 | **TCA and markouts** (needs K7) | the same |

Alongside, as the demos reach them: the smaller target gaps (`round`, `mod`, `substring`, `contains`, a number as text,
percentile; RIGHT and FULL joins shaped or refused for good -- `docs/DEEPHAVEN_START_HERE.md` "Gaps measured"); the tab's
check of a table's declaration against the server's (DH16 in the browser route) before any table another program
publishes; the server's version checked at sign-in.

## 3. The bespoke ledger

Each row is a thing done outside the platform, to push into it.

| piece | what it does | why bespoke | the platform's home |
|---|---|---|---|
| `risk.groovy` (D0) | the risk demo's reference data and prices | lite writes no rows into Deephaven yet | the modelled publisher (L5) over writes (W, Wr, L3w) |
| `market/market.groovy`'s simulator (K2) | the equity market: names, prices that move together, quotes and trades at each name's pace | lite writes no rows into Deephaven yet, and a market's rows come from no source of record | a feed the store declares (DH16), its rows written by the platform (L3w, Wr) from a simulator that is a program of its own |
| `market/market.groovy`'s Coinbase feed (K2) | crypto from Coinbase Exchange's public feed, the simulator in its place where it is refused | no feed ingestion in lite | a feed declared in the store and started by the platform; Kafka into Deephaven in production |
| `market/market.groovy`'s `world` table (K2) | what each part of the world is and why (simulated, live, connecting) | the page has no other way to learn it | the feed's own status, a system table of the platform's |

## 4. Realistic speeds

Targets each landing measures, not claims: 500 names; at the open about 20,000 quotes a second and 3,000 to 5,000
trades a second over all of them, a busy name a few hundred; a day's tape of a few million prints, the server keeping
a bounded history; a cell painted at most about ten times a second however fast its value moves; and the line's own
budget held (DH27: a tick on screen within about 10 ms of its rows; measured so far 5-18 ms, p50).

## 5. As built

**K1, time on Deephaven (2026-10-24).** The target (`Deephaven.java`, "Dates and times") writes a date-time literal as
the instant its UTC wall-clock names (`parseInstant(`...Z`)`), and, compared with a column in a filter, in quotes -- a
range filter the server reads as written (DH25); `timeBucket` as `lowerBin(t, n * length)`, a week's with its Monday
offset, a date's at its UTC midnight and back to its date; a calendar part as the library's own UTC function
(`year(t, timeZone(`UTC`))`, `dayOfWeekValue`, ISO's week through `LocalDate.get`), a whole number, a null kept null;
`datePart` as `toLocalDate`; a truncation to a day or finer as `lowerBin`, to a week or coarser as its first day, a
date (the platform's dialects' contract, `AnsiSqlRenderer.dateTruncDateGrained`); `adjust` by a fixed unit as
`plus(t, n * length)`, by a month or a year on the UTC calendar (`toZonedDateTime(t, UTC).plusMonths(n)`); a cast
between a date and a date-time; `now()` and `today()` refused by name (DH30), a bucket of months or years too (DH29).

Measured on the pinned 42.7 (`datacube/test/deephaven/page.ts`, 37 checks): thirteen queries over five rows -- a null
date-time, a null date, a leap day, an instant just before the epoch (its five-minute bucket the one before it) --
answer exactly what DuckDB 1.4.4 answers to the planner's DuckDB SQL over the same rows (a match, a membership and
Pure's null-safe `!=` on dates and date-times among them); a filter on a date-time and a
date never filtered on before compiles nothing on the server (written as formulas, the same filter compiled twice).

Found on the way: the server's validator refuses a method its allowlist lacks (DH33: `DayOfWeek.getValue()`); the
tab's live reader did not read Pure's `Date` (what `datePart` and `adjust` answer) and now reads it by the value's
form (`engine-client/src/deephaven-rows.ts`); the server's log is kept among a test's outputs, since a refusal says why
only there (`EmbeddedDeephaven.keepLog`). Not built: `dateDiff` and the epoch conversions (they divide whole numbers,
K5's), months and years in `timeBucket`, a date-time's fraction of a second as a part.

**K3a, a candle's open and close (2026-10-24).** Pure's `minBy` and `maxBy` -- `rowMapper(price, sequence)`, the value
at the least and the greatest of a whole-number order, lowered to SQL's `ARG_MIN` and `ARG_MAX` -- are the server's
`sortedFirst` and `sortedLast` aggregations by the order's column (`Deephaven.sorted`; the group writer reads the
order's column as it reads an argument, `DeephavenBatch.input`; the message table gains `AggSpec.AggSpecSorted` and
its column, checked against Deephaven's definitions). SQL skips a row whose order is null where the server sorts it
first, so a value or an order that may be null is refused by name. Measured: a day's candle -- open, high, low,
close and count by `timeBucket` -- answers on the pinned server exactly DuckDB's answer (`page.ts`, 38 checks). Pure's
`first()` in a group is SQL's `ANY_VALUE`, any row's: never a candle's open.

**K2, the market world (2026-10-24).** The market's Deephaven (`datacube/demo/deephaven/market/`, its own application
so the risk demo's tests never run its clock) holds `instrument`, `quote` (each name's latest top of book, a blink
table's `lastBy`), `trade` (every print, append-only; a ring of the latest `MARKET_HISTORY` for a long or stormy run)
and `world`, published every 20 ms by a `TablePublisher`. Equities are simulated (500 names over eleven sectors by
default, 8,000 quotes and 800 trades a second, the largest names busiest; prices on a market, a sector and a name's
own factor, rare jumps; a cent tick, spreads by liquidity, trades at the bid, the ask or the midpoint off-exchange,
round lots mostly). Crypto is Coinbase Exchange's public ticker and matches where the server reaches them -- eight
coins, their 24 hours' open the prior close -- and simulated, from the last price, where it does not; asked again a
minute later. The model is `market/model.ts`.

Measured: `//datacube:deephaven_market_test` (9 checks, a 50-name world, the feed's address refusing): the world says
crypto is simulated because its feed was refused; every name is an instrument and quoted, its bid under its ask by at
least its tick; every trade a known name's, well formed; trades at the configured pace; every minute's candle (K1's
bucket, K3a's open and close) holds its open and close between its low and high; every name's VWAP between its low
and high; every coin traded by the simulator; the busiest quote ticking -- each a Pure query planned in the tab and
answered by the server. By hand, the server started with this cloud session's proxy: "the market world's crypto is
live: Coinbase Exchange" (the server's log). Found on the way: the server's log is kept when it fails to start too
(a Groovy compile error said why only there); a Groovy decimal is a `BigDecimal` unless written `1.5d`. Since DH34 the
world's tables are the corpus's and its checks are queries over the corpus's classes (§6).

**K3, its first part: a date-time by a period finer than a day (2026-10-24).** DataCube's periods (`TimeGrain`) gain a
date-time's second, minute, 5, 15 and 30 minutes and hour: its bucket from the epoch, the platform's `timeBucket`
(`calc-presets.ts` grainStart), its end the bucket `adjust`ed by its step (a click filters to the bucket the database
meant); named by its start's time of day ("09:35"). A date (no time of day) is offered none of them -- the header
menu and the chart's form list `grainsOf(type)` -- and is refused one by name, by the chart's rules and the grid's
preset alike. Measured: four prints in three five-minute buckets, planned and run on DuckDB (`chart-queries.test.ts`);
K1 ran the same buckets on the pinned Deephaven. The candlestick kind follows PR #72: its open and close are #72's
`minBy` and `maxBy` over an order (DataCube's own aggregates, not a copy of them here; K3a made Deephaven run them).

**K6, windows on Deephaven (2026-10-24).** A select's windows are computed after its rows are filtered and grouped and
before its QUALIFY and its outputs read them (SQL's order), each into a column the outputs read by name
(`DeephavenBatch.windows`; `Deephaven.Scope#computed`). Windows over the same partition and order are one pass of the
server's: one sort (a key that may be null after its null flag, as a select's sort), one `updateBy` grouped by the
partition holding every running and rolling operation, one aggregate across each tie and one across each partition,
each joined back. Running figures are the server's cumulative sum, min and max; a running count of a value that may
be null its `countWhere`, of every row (and a row's number) the sum of a column of ones; a running average their
quotient. SQL's default frame (RANGE to the row) counts a row's ties with it: each tie takes the figure at the last of
them (an aggregate's `last` by the partition and the order), where ROWS takes the rows as they are. A frame of rows is
the server's rolling operation over ticks, the row one of those behind it (`rows(-2, -1)` is 3 behind and -1 ahead);
a RANGE of time along one ascending date-time that is never null, over its nanoseconds (a fixed unit; a month refused).
LAG and LEAD are the one row a rolling group that far away holds, read by the library's `first` (DH33: a vector's own
methods are not the validator's). A rank is the number of the first of a row's ties; a dense rank the running count
of the ties begun; percent rank, cumulative distribution and NTILE figures of those and the partition's count (NTILE's
whole-number quotients exact, as the server's `/` is a fraction's). FIRST_VALUE and LAST_VALUE are the partition's
first or last row's, a tie's last, the row's own, or a rolling group's first or last. A whole partition's figure is its
aggregate joined back. A filter over a window (QUALIFY: the top of each group) reads the window's column with no
compiling (DH25). The message table gains `updateBy` and its specs, checked against Deephaven's definitions.

Measured: 21 window queries over nine prints made for the edges -- two names and a print of none, a tie at 09:31,
prints exactly a minute and exactly thirty seconds apart, null prices -- answer on the pinned server exactly DuckDB
1.4.4's answers (`datacube/test/deephaven/page.ts`, 59 checks): running figures with ties, a running VWAP, a minute's
and thirty seconds' windows of time (both bounds inclusive, a tie inside its window), the previous, next and second
previous print and the previous name, moving figures of rows, a row number with nulls first, a name's whole figures
and a print's share of them, the lowest so far, a running volume of minute bars (a window over a group), a price's
change from the previous print (a window in a formula), the two biggest prints of each name, the ranks and NTILE, and
first and last values over every frame kind. Disabling the tie step turns the ties check red (600 where DuckDB says 700).
On the market world's live tape (`//datacube:deephaven_market_test`, now 10 checks): every name's running VWAP inside
its running range, and a previous print for all but the first of each name. Every one of DataCube's 15 window
functions, with each frame it takes, and its 6 show-as at a cube's levels plan for a Deephaven runtime
(`window-columns.test.ts`). Not built: EMA and delta (Deephaven's own, no Pure function names them yet), NTH_VALUE,
LAG with a default, a RANGE of numbers, a distinct or ordered aggregate over a window; each refused by name. A
window's speed on a realistic tape is K4's to measure.

**K3, the candlestick (2026-10-24).** A new kind of DataCube chart: a price's open, high, low and close per category
-- a period, by K3's grains -- in that order (`chart-spec.ts candleMeasures`), the open and the close #72's `minBy` and
`maxBy` by an order column (a print's number), the high and the low its greatest and least, each the database's; and,
given one, a volume, a column's sum drawn as bars beneath on an axis of its own. A chart's value gains a second column
(`ChartMeasure.weight`, #72's `Measure.weight` for an aggregate of two), so any kind may plot the price at the last of
an order; a value naming one for an aggregate that reads one column, or naming none for one that needs it, is refused
by name. The order is the chart's to be told: a candlestick made from another kind takes its value's column and asks
for its order (none is guessed). Drawn by ECharts' candlestick series, rising green and falling red (the theme's third
series colour and its `bad`), a period with no price drawn as none; one series, upright, its own figures (no split, no
show-as, no value's own mark or axis); a click filters to its period; a kind of one value made from it keeps its close.
The form asks the price, the order ("Open and close by") and the volume; the gallery shows one (each month's first,
highest, lowest and last sale's revenue, and its units). Measured: six prints in three five-minute buckets, planned and
run on DuckDB (`chart-queries.test.ts`); the same chart's query planned for a Deephaven runtime is the server's
`lowerBin` and `sortedFirst` and `sortedLast` by the number (`window-columns.test.ts`), K3a's measured shapes. The
kinds' chunk grows by ECharts' candlestick (68,000 -> 70,500, measured 70,117) and the startup pages by its rules
(grid 358,500 -> 359,000). Python reaches it by a value's dict (`{'column': 'px', 'fn': 'minBy', 'weight': 'seq'}`).

**The scalars (2026-10-25), first of the corpus's order (§6).** A screen's computed column on Deephaven, each a formula
of the server's own library or of a permitted method (DH33): Pure's `round` half-even, as SqlFn's ROUND is (`rint`, to a
literal scale by its power of ten and back -- SQL's ROUND_EVEN's two steps); `mod` floored and `rem` truncated (Java's
`%`); the platform's SUBSTRING (one-based, a start below 1 held at 1 as H2 holds it and DuckDB's SubstringClamp does,
each index held to the string's length) by String's `substring`; STRPOS (contains) by `indexOf`; a whole number as
text by Java's concatenation; and a DECIMAL column (a `BigDecimal`, DeephavenBatch's DDL) to a Float by its own
`doubleValue` -- the arithmetic of 1,582 corpus services over a Float property on a DECIMAL column. A float's text and
a date-time's are refused by name (Java's and SQL's differ: 1.0E7 against 10000000.0). Measured: the dialect's tests,
and four answers on the pinned server equal to DuckDB's over the same rows (`page.ts` SCALARS, a BigDecimal table among
them: 63 checks); the corpus planned on a Deephaven runtime goes from 2,304 to 3,885 of its 4,776 services. What they
refuse next, by count: a JSON value as text (459: graph fetch's rows), `dateDiff` (270), K5's whole-number division and
error (82), a join with no equality (27), a schema (27), `EXISTS` (15), K7's as-of joins.

**The aggregates (2026-10-25), second.** The statistics the server has no aggregation for, each computed from what the
group gathered, SQL's rules kept (read from the pinned server's own library, 42.7, its bytecode): a correlation and the
covariances by the library's `cor` and `cov` over the group's two vectors (`group`, a new AggSpec in the message table)
and a count of the pairs where neither is null -- the server's `cov` is the population's (its sums over n), the
sample's rescaled by n / (n - 1), and either of fewer than two pairs is null; the population variance and deviation
from the server's sample variance, rescaled by the count (0 of one value, where the server's is not a number); the
interpolated percentile (SQL's PERCENTILE_CONT -- the library's `percentile` rounds to the nearest rank) from the
group's vector sorted (nulls first: the library's `sort`) and two of its places read by `nth`, weighted; a mode by its
own aggregation (each value counted by the keys and the value, the greatest count's taken by `sortedLast`) joined back by
the keys. And `minBy`/`maxBy` where a value or an order may be null: SQL's skip the row (DuckDB's `arg_min` skips a null
value as well as a null order), so such a row's value is made null and its order sorts it out of the way -- the greatest
there is for `sortedFirst`, the server's null (first) for `sortedLast`; an order with no greatest value written (a
decimal) is refused by name. H2's `minBy` spelling, made this morning, now skips a null value too (Postgres's still
skips the null order only). Measured: the dialect's tests and three more answers on the pinned server equal to DuckDB's,
each statistic rounded to six places as a screen shows it (66 checks) -- but one: a single pair's correlation is null,
SQL's rule and the oracle's, where DuckDB's `corr` answers NaN. The corpus on a Deephaven runtime: 3,888 of 4,776
services, 15 of the 21 trader features (the six left: K5's whole-number division, K7's as-of joins, a time range over
a date-time that may be null).

## 6. The one trading model (DH34)

**Measured first (2026-10-25):** every stress-corpus service on its runtime, 4,675, planned on DuckDB and on a Deephaven
runtime over the same store (a scratch probe). DuckDB plans all of them; Deephaven 2,235. Its refusals, by count:
`DECIMAL` columns (1,582: the target has no decimal column; Deephaven's is `BigDecimal`), a value as text (440),
`dateDiff` (267), K5's whole-number division and runtime error (81), joins with no equality (26), schema-qualified
tables (27), `EXISTS` (15), a LEFT join's extra condition (2). Over the trading domains alone (trading, market data,
products, positions, P&L, risk, org, reference data, counterparty) 262 of 335 plan; all 12 hand-written core trading
services do. That count orders the Deephaven work (§2).

**What the screens needed that the corpus lacked, added (2026-10-25):** the public tape (`marketdata::MarketTrade`,
`MARKET_TRADE`: a print's sequence, time, price, quantity, aggressor, venue, condition -- the market's trade, not the
bank's); a quote's sizes (`MarketQuote.bidSize`, `askSize`); requests for quote (`trading::Rfq`, `RFQ`, with their
client and dealer); an order's trader and book; an option's own instrument (its positions and greeks); a volatility
surface's underlying instrument. Seeded as the corpus's next ring is, by `expand.py` (a NULL key, a dangling one, a
parent with no children, by construction); the generators read the new links, so 81 services over the trading classes
are new -- deep stacks over orders, executions, fills, quotes, the tape, options and surfaces among them -- every
expectation the oracle's. The deep-stack cap moved 400 -> 416 so that no root's deep variants were dropped for them
(`deepstack.py`, dated); one budgeted spread service changed its choice (`SP_defClearedTradeEmpty` ->
`SP_defTradeExceptionEmpty`, the greedy's own, by feature gain). Measured: every gate of the corpus green -- the stress
suites on DuckDB (4,785 of 4,813 tests pass) and H2 (4,756), the differential, the generators' gates, the diff
tests -- and all 81 new services pass on both. Not yet run against legend-engine (the corpus's engine lane needs its
runner and an engine).

**The trader features as services (2026-10-25, `scripts/corpus/features.py`, `99-trader-services.pure`):** 21 services,
one feature family each, so a target lacking a feature fails that service and no other. The scalars (Pure's round with
a scale, mod, substring -- Java's contract, zero-based, F37 --, contains, toString, toEpochValue); the statistics (wavg,
minBy and maxBy over a rowMapper, the sample and population standard deviations and variances, median, percentile,
corr and the covariances, mode, a distinct count); windows (running and moving figures by rows and by a time range,
lag and lead, first, the ranks, ntile, an unordered whole-partition window, top two per name as SQL's QUALIFY); five-
minute candles; a fixed-clock window; and as-of joins both ways (the quote prevailing at a print, the next quote after
it, and a booked trade's slippage against the mid). The query spec learnt the shapes (`query.py`: an expression
projection, `Window`, `AsOf`, relation extends) and the oracle evaluates each from SQL's and Pure's stated semantics:
frames with peers, NULLs left out of a frame's aggregate, ntile's larger first tiles, a LEFT as-of join; it refuses a
tie in a window's order where the answer reads positions, two quotes at the winning instant, a bimodal mode, a
half-way rounding.

**The market's day (`scripts/corpus/tape.py`):** written by hand as the core seed is, 13 quotes and 24 prints for AAPL,
MSFT and HSBA over the hand-written trades' own hours, appended to the expansion ring's rows AFTER the expansion (a
table seeded before it leaves the ring's candidate list and shifts every later table's generated values). Its shapes
are checked at build time: a print before the first quote, prints at a quote's instant, two prints in one second, prints
with no aggressor, a quote with no sizes.

**What the services found, fixed (each with its test):** the store resolver had no arm for an as-of join over two class
queries ("class query under TypedAsOfJoin is not resolvable yet": now the join's own structural arm,
`AsOfJoinCheckerTest.OverClassQueries`); the lowering lowered a scalar directly around a group's reducer but not through
the `toOne` and `cast` Pure's typing puts between them (`$y->corr()->toOne()->cast(@Float)->round(6)`:
`GroupByCheckerTest.testRoundedStatistics`); H2 has no ARG_MIN, ARG_MAX or time_bucket, which its dialect spelled by
name or refused (now Postgres's ordered array and MariaDB's floored epoch: `H2ArgExtremeAndTimeBucketTest`). And one
the corpus states rather than fixes: a statistic is approximate in every database -- DuckDB's sample variance of the
three gilt prices (within 0.0075 of each other) is 178 units in the last place from the exact one, its AAPL correlation
3 -- so the services ask for six places, as a screen shows them, and the oracle computes the exact value and refuses a
rounding within a millionth of a step of half-way. Measured: all 21 pass on DuckDB (4,806 of 4,834) and H2 (4,777),
the old failures unchanged (15 and 44). Not yet run against legend-engine or on Deephaven (task: the corpus on
Deephaven).

Not services, with the reason: an exponential moving average and a tick-to-tick delta (no Pure function names either);
`now()`-relative windows (the corpus never reads the clock: a fixed instant here, the live ones on the live tape);
`joinStrings` per group (its order is the database's).

**The market world publishes the corpus's tables (2026-10-25).** `market.groovy` publishes `EXCHANGE`, `SECTOR`,
`INSTRUMENT`, `EQUITY`, `MARKET_QUOTE` (each name's latest quote, a blink table's `lastBy('QUOTE_ID')`) and
`MARKET_TRADE` (the tape) in the corpus's own columns -- every one `30-store.pure` declares, in its order: ids `INST-`,
`EQ-` and `SEC-` with the sector's GICS code, listings on XNAS and XNYS, a quote carrying its day so far (open, high,
low, last, volume), a print its sequence, venue, condition (`I` an equity's odd lot) and aggressor (none off the
exchange) -- and its `world` beside them; `market::Store` and its classes are gone. The page holds the one trading
model (`//scripts/corpus:trading_model`: the linked projects and every stress file legend-lite's suites load, less
`StressExclusions`, read from its own source, and the services and test data; 2.27 MB) in the WebAssembly planner once,
4.4 s, and plans each check as a query over the corpus's classes (`marketdata::MarketTrade`, `products::Instrument`,
...) through `stress::AllMapping`, on a runtime whose `store::DB` is the market's Deephaven: 5 ms a query after the
first (68 ms). Measured: `//datacube:deephaven_market_test`, 12 checks, green on the first live run -- the ten before,
now class queries, and two new: every quote's day consistent (its open and last inside its low and high), and every
equity's sector and listing reached through the corpus's associations.

Found on the way, fixed: the corpus declared `products::Equity_Instrument` and its join but mapped neither end (now
`ProductsMapping`'s). The generators then take an equity's instrument as they take any link: deep, grouped, posted and
tree stacks and a dense stack on `products::Equity`, the deep cap 416 -> 417 so that CustodiedCryptoHolding keeps its
own (`deepstack.py`, dated); by the generators' own rules `D_CorporateActionDense` now repeats `D_ProductsEquityDense`'s
paths and `D_MarketTradeDense` falls under five features (its deep chains are taken first by the equity's), so both
are dropped -- the tape's deep navigation stays in its four deep variants and the TF services -- and the spread
generator's greedy stops a service sooner (20 of its budget of 90): `SP_defDailyPnLEmpty`'s combinations are closed now
by the services before it. And a boolean `if` as
a condition reached Deephaven with no shape (an off-exchange print has no side, any other a buyer's or a seller's):
now a ternary of the branches' conditions (`DeephavenDialectTest.anIfOfTwoConditionsIsTheConditionOfTheBranchItTakes`).
Measured: the stress suites on DuckDB (4,808 of 4,836 pass) and H2 (4,779), their failures unchanged (15 and 44), all
seven equity stacks passing on both; the corpus's gates, its diff tests and the differential green.

**K5, its first part (2026-10-25).** A formula has no throw, so the platform's runtime error (`ERROR`, the lowering's
guard where Pure fails: a zero divisor, a bound) is the library's `parseLong` of its message inside the raise envelope
(`RaisedErrors`' U+001F marks), which fails naming the text, typed as its CASE is -- never `parseDouble`, which trims
the marks. A whole-number quotient cut toward zero (`INT_DIVIDE`: the formula language's `/` answers a double) is the
library's `floorDiv` of the dividend less its remainder, an exact multiple, with no double between. `dateDiff`'s
calendar parts are SQL's crossings in UTC (a day's by the dates' `toEpochDay`, months and years by the library's
parts), its milliseconds floored and its hours, minutes and seconds the epoch milliseconds' quotient, each instant's
milliseconds cut toward zero as DuckDB's `epoch_ms` cuts them (measured 1.4.4: half a millisecond before the epoch is
0, where Java's `toEpochMilli` floors it to -1); `toEpochValue` is that from the epoch. A boolean `if` as a condition
is a ternary of the branches' conditions. Measured on the pinned server (`page.ts`, 70 checks): quotients of negative
numbers, `dateDiff` in days, months, years, hours, minutes and milliseconds and `toEpochValue` (an instant before the
epoch, nulls) answer exactly DuckDB's answers; guards no row takes compile there and answer; a row that divides by
zero fails the query. The server logs Pure's message (`NumberFormatException: For input string: "Cannot divide 3 by
zero"`, the marks around it) and hands the client only its reference ("Details Logged w/ID '...'"); the lane's driver
reads the server's log and holds it to that (an operator's path: the reference a person reports leads to Pure's
message, in the envelope `RaisedErrors` reads). Found on the way,
fixed: the live reader never said that the server failed a table -- the library fires `requestfailed`, which nothing
heard, so a page or a DataCube grid waited for rows that would not come; `follow` now takes a failure callback, and
the engine says it as it says rows it cannot read. Still refused: `EPOCH_SECONDS` (a fractional epoch), a date
difference in weeks or microseconds.

**K5, done: a Float's text, so `sqrt` (2026-10-25).** `sqrt`'s guard prints the negative it refuses, and a Float's
`toString` is the same text, Pure's (`FloatPrintText`'s contract: the shortest round-trip digits, written plainly).
Deephaven spells it itself: Java's own text where it is plain (shortest digits since JDK 19, the server on 25); where
Java writes an exponent, a whole value below 1e38 as its exact digits (the double's own `BigDecimal`) and `.0`, a
fraction in [1e-17, 2^53) as its digits' `BigDecimal`, trailing zeros stripped; outside those bands Java's exponent
(`1.5E-18`, where DuckDB writes `1.5e-18`). legend-engine's text (`FLOAT_PRINT_ENGINE`) is Java's `Double.toString`
itself. Measured first: the server's validator permits `BigDecimal`'s constructors and statics and `Double`'s text on
42.7. Measured on the pinned server (`page.ts`, 72 checks): plain small and large values, whole values past 2^53 by
their exact digits, negatives and a null answer exactly DuckDB's text; `sqrt` answers; `sqrt` of a negative fails,
the server's log holding "Unable to compute sqrt of -100.0" under the reference the page was handed (the driver's
check). Found, not fixed here (a defect of the shared spelling, for `main`): the portable spelling every SQL dialect
uses rounds a fraction past 18 decimal places (`DECIMAL(38,18)`: 50/2^20 = 0.0000476837158203125 prints
0.000047683715820313), which the contract's shortest digits do not; Deephaven writes the exact digits, and the page's
values are ones both write exactly.

Measured, the corpus on a Deephaven runtime (the throwaway probe of §6): 4,170 of 4,778 services plan, from 3,888;
`dateDiff` (270), whole-number division (55) and the runtime error (27) are gone, and `EXISTS` shows 87 (from 15:
services that stopped at `dateDiff` reach it now). Of the 21 trader feature services, 16 plan; the rest are K7's three
as-of joins and two windows of time over a time that may be null (live time windows).

**EMA and delta (2026-10-25).** A tick-to-tick delta needs nothing new: a price less the previous print's (`lag`)
already plans on Deephaven and is checked on the server (K6). An exponential moving average had no Pure function
(legend-pure and legend-engine know only the simple moving average, a window's `average` over a frame), so the platform
gains one, by the user's choice a name of its own rather than an overload of `average`, whose signature is upstream's
verbatim and which is order-free where an EMA is not: `meta::legend::lite::ema(numbers:Number[*], alpha:Float[1]):
Float[0..1]`, user-facing (`LITE_SURFACE`, its ceiling 4 -> 5, dated; census row; S105). It folds an ordered window's
values in its order: the first, then `alpha * value + (1 - alpha) * so far`, a null skipped, a NaN carried, null with
none -- a 20-period EMA is `alpha = 2 / 21`. The MIR gains `SqlAgg.Fn.EMA`, every dialect an arm: DuckDB folds the
window's values as a list (`list_reduce` over `list(CAST(x AS DOUBLE)) OVER (...)`: a fold's accumulator is its list's
element type, measured 1.4.4), Deephaven runs its own tick-decayed EMA (`updateBy` `ema`, the running frame, alpha a
literal in (0, 1) as the server's `-1 / ln(1 - alpha)` ticks, nulls skipped and a NaN poisoning, as the fold does: the
server's default skips a NaN), the rest refuse it by name; the lowering refuses one where its values have no order (a
group's, an unordered window's). Measured: `EmaTest` executes it on DuckDB (two partitions, a leading null, a null
mid-run, a whole-number column); on the pinned server two EMAs of each name, a null skipped, answer DuckDB's answers to
six places (`page.ts`, 73 checks); the corpus's `TF_TapeEma` (each name's fast and slow EMA of its price and an EMA of
its size over the tape) passes on DuckDB (4,809 of 4,837, the old failures unchanged at 15) and is refused by name on H2
(4,779; 45 failures, the 44 before and this one); on the market world's live tape every name's running EMA stays inside
its running range (`//datacube:deephaven_market_test`). Not built here: a
time-decayed EMA (irregular ticks; Deephaven has one) and a weighted moving average, each its own small function if a
screen needs it. The catalog's generated lines, whose generators read the legend-pure checkout this cloud session cannot
fetch: `Pure.java`'s overload groups replayed by the generator's own rule (`NativesGenerator.withOverloadGroups`, over
the committed file) come out identical; `native-claims.tsv`'s one row is written as `ClaimsGenerator` writes it, and is
checked only where the checkouts are (`//core:update_generated_tests`; CI runs on `main` and PRs, not this branch).
