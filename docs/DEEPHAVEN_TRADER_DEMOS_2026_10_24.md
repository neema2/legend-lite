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
| K4 | **built 2026-10-25** (§7). **the ticker**: a watchlist (last, change, bid/ask, volume, day VWAP, high/low), the tape, time and sales, and a live candle chart for the symbol picked | a browser check of every part ticking; the speeds of §4 measured |
| K5 | **built 2026-10-25** (§6). **The runtime error on Deephaven**: the lowering's guard (Pure fails on `sqrt` of a negative, on an Integer division by zero) writes an `ERROR` no Deephaven formula holds yet; a formula that fails by name, so `sqrt` and Integer `/` plan | the dialect's tests; a failing row failing the view, said, on the server |
| K6 | **built 2026-10-24** (§5), but for NTH_VALUE; EMA and delta 2026-10-25 (§6). **Windows as `updateBy`**: running and moving figures (by rows and by time), lag and lead, first and last, the ranks; top-N per group (a row number filtered, SQL's QUALIFY: no `headBy` needed); DataCube's window functions and show-as | each against DuckDB's answer |
| K7 | **built 2026-10-25** (§6). **as-of joins as `aj`** (DH20) | the same |
| K8 | **built 2026-10-25** (§7). **the market map**: a treemap of every name, sized by value, coloured by the day's change, ticking | a browser check |
| K9 | **built 2026-10-25** (§7). **real-time P&L with limits**: day P&L against the prior close by desk and book, KPI tiles that turn red past a limit | the same |
| K10 | **built 2026-10-25** (§7). **the execution blotter**: parent and child orders, fills, % filled, slippage against arrival in basis points, by strategy and venue | the same |
| K11 | **built 2026-10-25** (§7). **the options desk**: greeks as Pure functions run by Deephaven as formulas (needs K5's `sqrt`), vega by expiry and strike | the same |
| K12 | **built 2026-10-25** (§7). **FX e-trading**: a rates ladder, cross rates by live joins, RFQ hit ratio | the same |
| K13 | **built 2026-10-25** (§7). **TCA and markouts** (needs K7) | the same |
| K14 | **built 2026-10-25** (§7). **The stock chart** (the user, 2026-10-25: "the stock ticker page with beautiful line graph ticking with the stock price"). K4 drew a coin's line (BTC-USD's last print each second against its EMA, over the minute the world has run), not a stock's, and for one symbol written into the page. So: **a symbol picked** for the sheet -- any name on the watchlist, equities first -- that every tile on it follows (today a page filter reaches grids over a class only: extended to a query grid by its column); **the day from the open**, not from when the world started -- the market world opening each name's session with its morning's prints (a bounded history, task 14's quote history), so a line has a day to draw; **the chart a trader expects**: the last price as a smooth line over a soft fill, green above the prior close and red below it, the prior close a dashed line, the last price tagged on the right axis and moving with it, volume bars beneath, a crosshair, ranges of a minute, five, an hour and the day (the bins a range asks), no point markers, drawn again each second with its axes held (C2); and the KPI tiles beside it (last, change, day's range, VWAP) for the symbol picked | a browser check: the line moves every second for the symbol picked, ends at the quote's last, sits on the right side of the prior close in its colour; another symbol picked redraws every tile; screenshots by eye |
| K15 | **built 2026-10-25** (§7). **The news feed** (the user, 2026-10-25: "a ticking news feed that can be customized to general news, sector news or even specific tickers or keywords"). A news tile as a terminal's: each headline with its time, its source, its tickers as chips, its category (earnings, guidance, M&A, analyst rating, macro, regulatory, management, product, crypto) and its weight -- a flash in red, a story, a brief -- the newest sliding in at the top, a headline's ticker opening K14's chart. **Customized** by what the feed is filtered by, kept with the page: the whole market, a sector, the watchlist's names or tickers named, a keyword in the headline or its body (needs `contains` on Deephaven, a gap of §2's), a category or a weight. **The news is the world's own**: the market world writes a story for each name's jump and macro headlines that move the whole market (DH28's simulator always, its stories tied to its prices, so a headline and its move agree); a public feed where one can be reached -- SEC EDGAR's latest filings (8-K and the like, a public record) for the general channel, its place the simulator's where refused (DH28). **The tile**: drawn from a grid's rows as the KPI and the treemap are (a mark of its own, cards not cells), the grid itself its table view | the market lane: every story's tickers known, a jump's story within a second of its move; a browser check: the feed ticks, each filter (sector, tickers, keyword) keeps only its stories, a click opens the chart; screenshots |

Alongside, as the demos reach them: the smaller target gaps (`round`, `mod`, `substring`, `contains`, a number as text,
percentile; RIGHT and FULL joins shaped or refused for good -- `docs/DEEPHAVEN_START_HERE.md` "Gaps measured"); the tab's
check of a table's declaration against the server's (DH16 in the browser route) before any table another program
publishes; the server's version checked at sign-in.

## 3. The bespoke ledger

Each row is a thing done outside the platform, to push into it.

| piece | what it does | why bespoke | the platform's home |
|---|---|---|---|
| `risk.groovy` (D0) | the risk demo's reference data and prices | lite writes no rows into Deephaven yet | the modelled publisher (L5) over writes (W, Wr, L3w) |
| `market/market.groovy`'s simulator (K2) | the equity market: names, prices that move together, quotes and trades at each name's pace; since K14 the session's morning before the world starts (`MARKET_MORNING`), on the tape as prints, a bounded number of them each that much larger | lite writes no rows into Deephaven yet, and a market's rows come from no source of record | a feed the store declares (DH16), its rows written by the platform (L3w, Wr) from a simulator that is a program of its own |
| `market/market.groovy`'s newsroom (K15) | the newswire: a story for each name's jump at its instant (its category by chance, its wording by its sign, its size in the headline, a FLASH from 3%), macro and sector stories each moving the market or its sector by the shock its headline names, briefs (an analyst's note, a filing, an appointment, a dividend, a buyback) nudging their name or nothing, crypto's briefs from each coin's price against its prior close; `MARKET_NEWS` a minute besides the jumps, `MARKET_JUMPS` a name a day; every name, broker, fund and wire made up | lite writes no rows into Deephaven yet, and a newswire's stories come from a vendor's feed | a feed the store declares (DH16), a vendor's wire or the simulator a program of its own, its rows written by the platform (L3w, Wr) |
| `market/market.groovy`'s EDGAR filings (K15) | SEC EDGAR's latest 8-K filings read each minute, each a brief at its own time -- asked only with the person's own contact (`MARKET_FILINGS_AGENT`: EDGAR's fair-access policy refuses a client without one), the newsroom's own filings in its place without it or when refused | no feed ingestion in lite | a feed declared in the store and started by the platform |
| `market/market.groovy`'s Coinbase feed (K2) | crypto from Coinbase Exchange's public feed, the simulator in its place where it is refused | no feed ingestion in lite | a feed declared in the store and started by the platform; Kafka into Deephaven in production |
| `market/market.groovy`'s `world` table (K2) | what each part of the world is and why (simulated, live, connecting) | the page has no other way to learn it | the feed's own status, a system table of the platform's |
| `market/market.groovy`'s bank (K9-K13) | the bank's own day: desks, traders, books, positions at the prior close, limits; parent orders worked by algorithms into fills and routes on the market's own prices; FX prices; clients' RFQs quoted and done or missed; a listed option chain settled by Black and Scholes | lite writes no rows into Deephaven yet, and a bank's books and blotters come from its systems of record | each its own source -- an OMS's orders and fills, a position keeper, a pricing engine -- declared in the store and published by the platform (L5) |
| `market/traders.ts`, the host (K4) | the page around DataCube's: the trader screens, each written by `screens.ts`, listed in DataCube's own Open window (its library, its examples' place) and opened as a saved page is; Save and Open on the tab's own store | the screens were pages of a demo of their own, and the tab's app (`datacube-app.ts`) opened no grid on Deephaven, so the host built the Open window's frame the app builds (about 60 lines). Since the trading desk (2026-10-25) the app itself opens them (`market/host.ts`): this host is the screens' test bench (`//datacube:deephaven_traders_test`'s per-grid tick timings) until those checks move onto the app | the app, as `//datacube:deephaven_desk` serves it |
| `market/host.ts`'s project and elements (the trading desk) | the market's project (the one trading model and the market's part, a page-made file) added to the tab's saved-query projects; the model's elements answered from the trading corpus's | the screens' saved queries name a project no SDLC or depot holds yet; the app reads a project's elements through the drop-in surface (pure/v1's `grammarToJson`), which refuses lite's own Pure (the Deephaven connection), and the market's part declares no data space or enumeration -- all the app reads them for | the screens as a project's saved queries in a model home; a lite-surface reader of a model's elements in the module |

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

**K7, as-of joins (2026-10-25).** Pure's `asOfJoin` (the IR's `ASOF_LEFT`) is the server's own as-of join: `aj` where
each left row takes the right row with the greatest as-of value not after its own (`l >= r`, or `l > r` strictly),
`raj` the least not before it (`l <= r`, `l < r`), the key equalities matched exactly, the comparison read from the left
whichever way it is written; a left row with none keeps nulls. The carrier strategies had rewritten every as-of join
for a dialect without native lists into the portable LEFT join with a pick, which the batch refused; a new capability
(`Caps.nativeAsOf`, Deephaven's alone) leaves Deephaven's alone. A right row whose key or as-of value may be null is
filtered out first; a reverse join's left time that may be null is matched through a helper column as the last whole
second the server converts (its nanoseconds end 2262-04-11T23:47:16.854775807Z and the very last overflows, measured),
since the server orders a null first and `raj` would take the first row. The protocol's `AjRajTablesRequest` joins the
message table, checked against the server's definitions. Measured on the pinned server (`page.ts`, 76 checks): each
print's prevailing quote (one at the print's very instant included) and the next quote strictly after it, by name, and
the next quote after a time that may be null, answer exactly DuckDB's `ASOF JOIN`; the corpus on a Deephaven runtime plans
4,173 services (the throwaway probe), all three of the trader as-of services among them. Not built: a live as-of check
on the market world's tape (its quotes table keeps each name's latest quote only, `lastBy`; the simulator's quote
history is item 8's).

**Found, not fixed (a platform finding for the user):** an as-of join's key equality is written null-safe by the
lowering (`IS NOT DISTINCT FROM`, Pure's `==` holding for two empty values), and the databases disagree on it: H2's
as-of emulation (a LEFT join with a pick) matches a null key with a null key, DuckDB 1.4.4's `ASOF JOIN` matches no null
key under the same condition (measured 2026-10-25, a throwaway probe: a print and a quote of no name, matched on H2,
unmatched on DuckDB). The corpus's oracle states DuckDB's rule ("a NULL key or instant matches nothing"), and Deephaven
follows it (a right key that may be null filtered out). Which is the platform's is a ruling: Pure's `==` says match;
SQL's equality and the oracle say no.

**The whole corpus on Deephaven, against the oracle (task 15, 2026-10-25).** Every stress-corpus test is planned on a
Deephaven runtime over the same store (`//core:stress_deephaven cases`: its plan, its result type, its enumerations and
the oracle's expectation, and the corpus's seed as tables), the tables loaded into the pinned server
(`test/deephaven/corpus/corpus.groovy`), each plan sent from the pinned Chromium as DataCube sends one and its rows read
as DataCube reads them (`test/deephaven/corpus-page.ts`), and every answer judged by the stress suites' own judge
(`//core:stress_deephaven judge`, `TestAssertions.equalToJson`). `//datacube:deephaven_corpus_test` (manual) holds the
passes to a floor. Measured: 4,191 of the 4,195 tests that plan pass (the floor 4,191); every plan sent in about 90 s.
The four that do not: `CF_Confluence`, `O1_CounterpartyOtherwise` and `F32` fail on DuckDB too (the stress suites'
ledgers), and `TF_TapeEma`'s slow EMA differs from SQL's in its last digits -- the server's EMA decays by ticks
(`-1 / ln(1 - alpha)`), the fold by the value's own weight, equal in exact arithmetic and not in a double's (stated, not
fixed). 653 have no plan on Deephaven, by reason: a graph fetch's JSON tree (462), `EXISTS` (87), another store than
`store::DB` (44), a schema-qualified table (27), a join with no equality (27), and six others. That is the order of the
Deephaven work left (task 14).

**What the corpus found in the dialect, fixed (each with its `DeephavenDialectTest`, which fails without it):** a
DOUBLE's arithmetic took a DECIMAL column as the server's BigDecimal and divided at its precision (1666666.66666667
where SQL answers 1666666.6666666667) -- each operand is its double value first; a COALESCE of a DECIMAL and a double
was a condition the server refuses ("Incompatible types in condition operation") -- written in doubles; an average of
a DECIMAL column kept the column's scale (2.905 where SQL answers 2.9045833333333335) -- averaged in doubles; and a BIT
compared with 0 or 1 (a store filter's `IS_INTERPOLATED = 0`) matched nothing -- compared with false or true. The
lane's first run passed 633; the plans that never reached the server (an enumeration column the reader refused outside
its try), answers lost to a flush, and four tables the fixture refused (single-digit hours, a header-only table) were
the lane's own, fixed before the dialect's.

## 7. The trader screens (the user, 2026-10-25)

"Keep going and building and fixing until we have all the trader screen demos saved as datacube pages user can open
and see from datacube -- can be different tabs or different saved pages similar to our chart pages."

**The host and the pages.** Each screen is a saved DataCube page (`datacube/demo/deephaven/market/screens.ts`), written
by DataCube's own writers (`writeCube`, `writePageOf`) as the example pages are: its grids and charts, a sheet (a tab)
per view of the desk. Every grid's source is a saved query over the one trading model's classes, its text and its
execution context (`stress::AllMapping` on `market::LiveRuntime`); its columns come from the tab's planner. The host
(`traders.ts`, `bazel run //datacube:deephaven_traders`) is DataCube's own page over the market world's Deephaven, and
its Open… and Save are DataCube's (2026-10-25): Open… is the app's window (`CubeLibrary`) -- the pages saved in this
browser, searched, sorted, opened, deleted, or a page file opened -- and, under them where the app lists its example
pages (the chart pages), the eight trader screens (the News monitor among them since K15), each with what it shows; Save, Save As… (the app's `saveDialog`) and
Export ▸ Page File keep the page as a person left it, in the tab's own store of saved pages by its rules
(`src/services/browser-pages.ts`, moved out of the tab's services so both open it: legend-lite's SavedCubes, compiled to
WebAssembly). A screen picked opens as a saved page is opened -- each grid made again over its source as it is now, by
`openCube` -- and so does a saved page; the menu's Trader screens ▸ opens them too. Every grid has its own reader and
ticks from the rows it follows. The test opens every screen from Open…, saves the first by Save As… under a name of its
own, opens it from Open…, and finds it there again after the tab is reloaded.

**K4, the ticker (2026-10-25).** Two sheets. *Watchlist*: every name's quote (last, bid, ask and their sizes, the day's
open, high and low, volume, the change against the prior close in green and red, the spread in basis points); the tape
(time and sales, to the millisecond, the newest first, read by a window); each name's day from its prints, grouped by
DataCube (prints counted, VWAP as a size-weighted average, high, low, last by sequence, turnover). *BTC-USD*: two KPI
tiles (last, change on the day), the coin's quote, one-minute candles with volume, the last print each second against
its EMA (lite's `ema`, over the tape in the source), and the coin's prints with their EMA. The screen needed three things
of DataCube, built into it: a chart's value axis fitted to its values (`fitAxis`: a price's line, which a zero baseline
flattened), a measure's own title in the legend (`title`: "last print", not "maxBy of price by seq"), and a second's
fraction in a format code (`ss.000`, Excel's: the digits stored, cut). Measured (`//datacube:deephaven_traders_test`,
the pinned Chromium, a world of 100 names, 2,000 quotes and 400 prints a second, crypto simulated): five grids and four
charts, each sheet ticking with nothing sent or fetched, a tick on screen 20-25 ms after its rows (p50, five grids
ticking on one thread); the screen open in 6.4 s from a cold tab, 0.5 s again from the page saved; the page live in
10 s from a cold tab (its planner's module 3.5 s, the screen 6.4 s).

**The cold open, fixed in the planner (2026-10-25).** First measured at 20 s: DataCube types each grid's queries by
`pure/v1`'s `lambdaRelationType`, whose request carries the model's text, and the tab's module compiled it again for
each distinct query -- 2.7 s each for the 2.3 MB trading model, in a tab that held it already (595 typing calls, 64 s
summed). The contract is unchanged: `PureV1Api` takes a host's `Models` (a text model compiled for one use, by default
for it alone), and the tab's and Python's boundary answers a request whose model text it holds over the held model
(`HeldModels.withText`, by content hash, under the model's lock), compiled no more. Measured: those calls 2.2 s summed,
the screen 6.4 s cold. A server holds nothing, so compiles per request as before.

**K8, the market map (2026-10-25).** Two sheets. *Map*: every equity a block, by sector, sized by its value at the last
price (shares out by the last) and coloured by its change on the day -- grey at none, green up, red down, full at 2% --
its ticker and change written on it; beside it the grid it draws, by sector then name, every sector open, a sector's
change its names' value-weighted average (DataCube's `wavg`). *Movers*: the day's breadth (advancing and declining
names, KPIs), each sector's value-weighted change (bars), and the top gainers, losers and most active names by turnover
(three lists, each a window over every name re-sorted at every tick). DataCube gained a treemap coloured by value
(`colour`, a column the grid shows; `options.colourReach`, its reach either side of zero; the editor's Colour by and
Colour reach), taken away when the chart becomes another kind.

**Found and fixed on the way (a DataCube defect, every page with sheets):** a grid first drawn on a sheet not shown
measured no height, and a tick draws only the rows on screen by the height its last draw read -- so, shown, each tick
redrew its first row alone, the rest kept from older blocks: a list re-sorted at every tick painted a name twice, at two
prices. The grid now draws again when its height changes (its sheet shown, its tile grown). The browser check reads
every list's view and the rows painted on screen at each tick and refuses a name shown twice (210 views and screens,
none), and the grid's own test ticks a grid hidden and shows it.

**The bank's own day, published (2026-10-25; item 8).** The market world publishes the corpus's tables the remaining
screens read, every column each declares: a business unit, six desks (TMT, healthcare and financials, cyclicals and
defensives, digital assets, equity derivatives, FX), three traders a desk, ten books, 188 positions at the prior close
(an equity book's names its desk's sectors, crypto's the coins, the derivatives books' listed options), each desk's
day's loss and gross limits, twenty FX clients; a listed chain on the four largest names (three expiries, nine strikes,
calls and puts, each an instrument of its own, settled at the prior close by Black and Scholes on a smile); and ticking,
parent orders (0.8 a second, `MARKET_ORDERS`) worked by VWAP, TWAP, POV, IS and dark algorithms into fills and routes on
the market's own quotes, ten FX pairs, and the clients' requests for quotes (2 a second, `MARKET_RFQS`) quoted after the
desk's response and done, missed, expired or declined. The source keeps no figure a screen should compute (slippage,
utilisation, live greeks: null), so the database computes each. Checked on the pinned server through the corpus's
classes (`//datacube:deephaven_market_test`, 19 checks): the desks, books and traders by their associations; every
position its book's and on a listed instrument; each desk's limits; every order's fills summing to its filled
quantity; ten pairs, bid under ask; requests consistent with their states; the chain.

**K9, P&L and limits (2026-10-25).** *P&L*: the firm's day P&L (a KPI against zero), by desk and by sector, and a grid by
desk, book and name -- every cash position marked to its live mid against the prior close (positions joined to quotes
by the database). *Limits*: each desk's day P&L against its day's loss limit and its gross against its gross limit,
each used as a share (a heatmap from green to red), a status (a breach at the limit, a warning at four fifths), the
desks past either (KPIs that turn red above zero), and each desk's loss limit left (red under a fifth).

**K10, the execution blotter (2026-10-25).** *Blotter*: orders working, notional filled and the day's slippage against
arrival (KPIs); every parent order as its algorithm works it -- % filled, its fills' average price and its slippage in
basis points, from its fills grouped by order and joined to it -- and its fills as they print. *Algorithms and venues*:
slippage by algorithm, notional by venue, and the fills by algorithm and venue.

**K11, the options desk (2026-10-25).** *Chain*: every listed option priced live by Black and Scholes -- functions of the
market's model (`market::options`, `datacube/demo/deephaven/market/model.ts`), programs the compiler inlines, so the
database computes every price and greek from the underlying's live mid -- with its change against its settlement, and
the smile (implied volatility by strike over spot, each expiry a line). *Risk*: the derivatives books' dollar delta,
vega, theta and day P&L (KPIs), vega by expiry and underlying (a heatmap), and the books' greeks by underlying and expiry.
Checked on the server (`//datacube:deephaven_market_test`): put-call parity on every option within a millionth of the
spot (the normal distribution's approximation), every delta in range.

**What the options desk found in the platform, fixed (2026-10-25):**
- *Deephaven: a chain of calculated columns grew in each formula as it went.* SQL wants each extend's column written
  as its whole expression where a later one reads it, so the lowering writes it there; Deephaven compiles each formula
  as written, and a price's Black and Scholes reached 30,000 characters and 109 s for the server to compile (and
  `sqrt`'s runtime-error guard prints its operand into its message, a dozen times more). Within one `view` a later
  column now names an earlier one (`DeephavenBatch.view`: the server evaluates a view's columns in order); the market
  lane went from 171 s to 49 s, and the 76 answers on the server are unchanged. The program helps too: the screen's
  source computes the square root, the discount, d1, the density and the four normal distributions once each, as
  columns, and the price and greeks are small formulas over them.
- *A page's server bases (B1) on a big model.* Each new base re-declared the page's whole model, so the trader host
  read every source as written; with no base, each distinct query of a grid shipped the whole Black and Scholes source
  and the server compiled it again (53 s to open the options desk). The bases can now be declared in a model of their
  own (`ServerBaseOptions.connectionOf`: the connection's text and their Database, nothing of the page's), so the page's
  planner holds the trading model once; a Boolean column is declared `BIT`. Every trader screen's grid now reads a base.
- *A grid's runner held one base at a time*: a chart of calculated columns of its own (the smile's bins) reads another
  base than its grid's, so the two took turns at every tick -- 175 moves a minute, each a hold, a release and its
  queries planned again. It now holds the few its grid and charts read (the one read least lately let go past four).
- *A heatmap's labels* were raw floats (a vega's 17 digits): each is now its value as the grid writes its column.

Measured (`//datacube:deephaven_traders_test`, five screens, the pinned Chromium, a world of 100 names): each screen
open in 1.7 to 6.9 s (the options desk 13.5 s on a server that has not compiled its formulas yet), every sheet ticking
with nothing sent or fetched, a tick on screen 21 to 51 ms after its rows (p50); the risk demo's cube and board tests
pass on the changed bases.

**K12, FX e-trading (2026-10-25).** *Rates*: the desk's ladder (each pair's bid, ask and mid, its spread and its change
on the day in pips, the time of its price to the millisecond), each pair's change as bars, and each cross against the
rate its legs imply -- EUR/JPY as EUR/USD by USD/JPY, GBP/JPY likewise, EUR/GBP as EUR/USD over GBP/USD, by live joins
of the legs on a key every row shares -- the difference in pips. *RFQs*: the hit ratio, the notional done and the desk's
average response (KPIs; the response red past 250 ms), the hit ratio by the spread quoted (it falls as the spread
widens), every request newest first through its states, and each client's hit ratio (a heatmap), notional asked and
done, average spread and response. Found on the way, fixed in the Deephaven dialect: a pip's size by its pair -- an
if of two float literals, typed DECIMAL by their digits -- was cast to a double by a BigDecimal's own `doubleValue`,
which a primitive has not (the server refused the batch); only a value that reads a DECIMAL column is a BigDecimal now.
Found looking at the screen (2026-10-25): the market world priced each cross on a walk of its own, so in a minute EUR/JPY
stood 17.5 pips off the rate its legs implied -- a free lunch no desk shows. Each cross is now its legs' product (or
ratio) with a basis of its own of a few tenths of a pip that reverts, priced again whenever a leg is, its prior close
its legs'; the market lane checks every cross within 3 pips of its legs (21 checks).

**K13, TCA and markouts (2026-10-25).** *TCA*: the day's slippage against arrival and markout after 5 seconds (KPIs),
markouts by algorithm 1, 5 and 30 seconds after the fill, TCA by algorithm and venue, and slippage by venue. *Fills*: TCA
by broker, and every fill with its slippage, its markouts and its commission in basis points. Each markout is the
market's price at the horizon -- the latest print of the fill's name at or before it, by an as-of join to the tape (K7) --
from the fill's price, positive when the market moved the fill's way; every figure notional-weighted (`wavg`). A fill
younger than a horizon reads the latest print so far: a live window (`now()`) would leave it out, and is not shaped on
Deephaven yet (DH30).

Measured (`//datacube:deephaven_traders_test`, all seven screens in one run, the pinned Chromium, a world of 100
names): each screen open in 2 to 8 s, every sheet ticking with nothing sent or fetched, a tick on screen 14 to 51 ms
after its rows (p50), no name painted twice on any list.

**The bundle budgets (2026-10-25), signed off by the user the same day (D9: "bundle budget is fine").** The screens' DataCube steps -- an axis fitted to its
values, a treemap coloured by a value and its reach, a heatmap's labels, a measure's title, a second's fraction in a
format code, a grid drawn again when its height changes -- grew the scripts the budgets measure: the grid-only page
359,000 -> 359,500 (measured 359,107), a first chart 304,000 -> 304,500 (304,219), a notebook's cube 366,500 -> 367,000
(366,801); each dated in `datacube/test/bundle-budget.test.ts`.

**K14, the stock chart (2026-10-25).** The Ticker's first sheet is now *Stock*: the watchlist down the left, and beside
it the name picked in it -- its last against its prior close ("85.07 \u25BC 1.85 (2.1%) below prior close", red), VWAP,
the day's high and low and volume (KPIs); its chart; its prints and its quote. **The chart**: the last price each minute
of the day from the open, a line over a soft fill, green above the prior close and red below it, the prior close a
dashed line, the latest price tagged on the value axis in its side's colour with a dot on the latest mark, the name's
EMA beside it (lite's `ema`, K6), the volume beneath in a pane of its own, each bar its price's side's colour; spans of
1m, 5m, 15m and 1h by the second and the whole day by the minute, a span's window following the latest mark as it
ticks; one-minute candles behind it in a tab. **A name picked**: a click on the watchlist's row (or its arrow keys) takes
the stock's two grids to it -- the prints, whose views are the KPIs and both charts, and the quote -- each a step of its
own grid, undone as any, kept when the page is saved. **The day from the open**: the market world opens each equity's
session before it starts (two hours by default, `MARKET_MORNING`), the morning's prints on the tape (at most
`MARKET_MORNING_PRINTS`, each that much larger, so a minute's volume is a minute's), busiest at the open; each quote's
day (open, high, low, volume) the morning's and the live prints'.

What it took of the platform, built into it: in DataCube's chart -- a measure in a pane beneath on the same time across
(`pane: 'below'`), a reference line and a threshold at a measure's value on the latest mark (`of`: the prior close), the
latest mark pinned (`last`), spans of time each with its grain (`spans`, `span`), a KPI said against a reference line,
a tick's window kept where it was left (a person's zoom too: it was lost at every tick) and a chart's scale and window
drawn afresh when what it reads changes; in DataCube's page -- a grid linked to others (`GridView.link`, Deephaven's
own dashboards' linker), the row picked reported by the grid. Found on the way and fixed: a threshold on a line chart
failed to draw at all on ECharts 6 (a piece open at one end gives a line's gradient no stop; one at the largest number
none a canvas can place); a chart's tick read every row of every result it follows each time (the whole day by the
second, thousands of rows a second: 190 ms a tick) -- the day is drawn by the minute; the chart theme was read from the
page's styles at every draw (a sixth of a tick); a style per volume bar (a visual map colours them). Each found by a
browser profile of the ticking page (`--test_env=TRADERS_PROFILE=1`, the screen alone with `TRADERS_ONLY`).

Measured (`//datacube:deephaven_traders_test`, the pinned Chromium, a 100-name world with half an hour of morning on a
tape of 126,000 prints): the Ticker open in 6.4 s, its sheets ticking 30-45 ms after their rows (p50), nothing sent or
fetched; a name picked moves the quote, the prints, the KPIs and the line to it (its last the quote's); 5m shows the
latest 300 s by the second, its window ending at the latest mark; the candles draw when their tab is shown; the page
saved with its name picked and its span, and opened again so. The market lane checks the morning (22 checks): the tape
starts at the session's open, every quote's volume its prints'.

**K15, the news feed (2026-10-25).** A new screen, *News*, and a news tile on the Stock sheet. **The News monitor**: a
news terminal's panes, each a feed of the newswire customised by its own grid's filter -- the whole market's, macro and
sector stories, the five largest names', Technology's, and a keyword's ("buyback", typed in the pane's own box, any
case, in the headline or the body) -- and beside them the name a card picks, its price today against its prior close.
Each card as a terminal shows a story: its time, its weight (a FLASH in red, a BRIEF quiet), its source and category,
its headline, two lines of its body, its tickers as chips, its tone on its edge (green above zero, red below, as strong
as it is far from zero); a story crossing comes in at the top, lit a moment. A card picked, or a ticker chip on it,
takes the chart beside the panes to that name (the feed's link: K14's, on a chart). A second sheet, *The wire*: every
story as a grid, and the day's news by category (its stories and their average tone, a bar of each). **On the Stock
sheet**: the name's own stories, a feed in front of its grid, followed with the name picked on the watchlist.

**The news is the world's own** (the market world's newsroom, §3's ledger): a story for each jump at its instant, macro
and sector stories that move the market or the sector by the shock their headline names, and briefs, so a headline and
its move agree -- the market lane checks it: every story's tickers are the world's, a story about a name names it, a
sector's names its leaders, a macro story neither, and each jump of 2% or more moved its name's price its way by at
least half the headline's size from the print before it to the print after. SEC EDGAR's latest filings join the wire
where the person gives EDGAR the contact its fair-access policy asks for (`MARKET_FILINGS_AGENT`); without one the world
says so (its `world` table: filings simulated, and why) and files its own names' filings.

What it took of the platform, built into it: **in the one trading model** (DH34), `marketdata::NewsStory` (NEWS_STORY)
with a story's name and its sector as associations, seeded by expand.py, and the day's newswire written by hand beside
the market's tape -- each story where the tape moved, two at one instant, a sector story, a macro story -- with four
feature services the oracle answers (a keyword by `toLower` and `contains`, a name's feed, a sector's, the day by
category): 35 new corpus tests, all passing on DuckDB and H2; `contains` and `toLower` already planned on Deephaven
(STRPOS by `indexOf`, LOWER by `toLowerCase`: the scalars, above). **In DataCube's chart**: a kind of its own, `feed` --
its slots each a column of the cube (`feed`: headline, time, body, source, tags, weight, tickers, tone, key), its query
the cube's rows under its filter, the newest first and a tie by its key, its cards a chunk of their own drawn again at
each tick with the ones it had kept (ui/chart-feed.ts); its rules, its reader and its options form as any kind's; a
keyword box that puts its words on the feed's grid as the feed's own selection, said on its chip ("headline or body
contains “buyback”"); and the gallery's feed of the sales sample. **In DataCube's page**: a chart's link
(`ChartView.link`), a card or a ticker chip picked taking the grids it names to that name. The grid script grew 1.5 KB
gzipped (361,000 -> 363,000, within the user's sign-off, D9).

Measured (`//datacube:deephaven_traders_test`, 8 screens, a 100-name world at 90 stories a minute): the News screen open
in 1.6 s, its panes' 2,600 morning stories each kept to its own (macro and sectors, the five largest, Technology, the
keyword), a story reaching the top of its pane read from the rows followed -- nothing sent or fetched; "dividend" typed
into the keyword pane's box keeps only its stories and is saved with the page; a ticker chip clicked charts that name;
the Stock sheet's news followed the name picked. The market lane: 24 checks.

**The trading desk, in DataCube itself (the user, 2026-10-25: "a real saved page just like charts in real datacube (on
this branch) that opens like 8-10 tabs all with different realistic trading screens").** One saved page, ten tabs, each a
screen of a trading floor: *Markets* (the market map, breadth, each sector's value-weighted change, the top gainers,
losers and most active), *Stock* (K14: the name picked on the watchlist, its day from the open, its candles, quote,
prints and news), *News* (K15's monitor), *Order flow* (new: bought against sold each minute, the net flow by sector and
by name, the block prints -- 10,000 shares or a million dollars in one print, never a bunched one -- time and sales,
the notional by venue), *Crypto* (new: the coins, majors first, their 24 hours' change; the coin picked, its last five
minutes by the second and its EMA, its candles and prints; each coin's net flow; the crypto book marked live),
*Blotter* (K10), *TCA* (K13), *P&L and risk* (K9: the firm's, desks', books' and names' day P&L, each desk against its
limits), *Options* (K11: the chain priced by Black and Scholes, the smile, the books' greeks), *FX* (K12: the ladder, the
crosses against their legs, the RFQs and hit ratios). 35 grids and 57 charts, every grid a saved query over the one
trading model run on the market's Deephaven, ticking.

*It is DataCube's own page* (the one `//datacube:serve` serves, its own bundle): Open… lists it first under Examples,
above the chart pages, and the address `#example=trading-desk` opens it straight away. What made that possible, in the
app: **a host's grids whose rows change under them** (`services.ts` `LiveGrid`, `LiveRows`: each grid made over such rows
-- a copy too -- takes its own runner and its own reader on the server, which follows what that grid's views read, and the
host says when they changed: a tick, or the whole grid again once the connection is back); **a host's own example pages**
(`OpenDataCube.examples`) and **an example named in the address**; and **a saved query run by a host's runner loads no
copy of its project's rows into the tab** (`sources.ts` `openedRecord`: it loaded the trading model's whole seed into
DuckDB for rows the server serves). **The market, a host of the app** (`market/host.ts`, its own chunk, fetched only where
`config.json` names a market: page-config.ts `market`): the market's project (the one trading model and the market's
part), its model held once by the tab's own module, the server signed in to, every grid on its runtime run through its
bases (B1) -- and nothing else of the tab's services changed: a file, a sample, the chart pages, Save and Open are the
tab's. `bazel run //datacube:deephaven_desk` serves DataCube's site with the market world's server named (its
`desk-site.mjs`) and prints two addresses: the desk itself (`…/demo/index.html#example=trading-desk`), and DataCube's own
start, where Open…'s Examples hold the desk first. Save or Save As… keeps it as a page of one's own, in the browser's
store of saved pages, which Open… lists and opens again over the market as it is then. The old trader host
(`traders.ts`) stays as the screens' test bench for now (§3).

**What the desk's screenshots showed, fixed (2026-10-25):** *The market rose 10-20% in half an hour* (93 names
advancing, 6 declining): the newsroom's briefs moved their names only up where they moved them (a stake disclosed, a
buyback), drawn by activity, so the largest names took hundreds of up-nudges at a busy wire's pace; and every macro or
sector story moved prices, so the wire's rate set the market's volatility. The market's moves now come at a day's pace
whatever the wire's (six macro shocks a day, two a sector, two nudges a name; the rest "little changed", said so), and a
brief moves as often down as up (a stake cut, a share offering). Measured on the same world: 55 names advancing and 46
declining after half an hour, moves of a tenth of a percent to 2.8%, the largest name 0.8% up across its prior close.
*Blocks were 44% of the tape*: a thinned morning's print stands for several (the tape keeps a share of the morning's
prints, each that much larger), so it is now the tape's own bunched trade (condition B), never a block; the desk counts
34 blocks in 140,944 prints. *A tab's name was cut* ("Mark…", "Optio…") with half the bar empty: measured in the browser, the tabs' strip came
out at 802px of the bar's 1,529 free -- Chrome sizes it by its tabs' intrinsic widths, which their 160px flex basis does
not set -- so ten tabs took 76px each and the empty status beside them the rest. A tab's width is now 160px too
(`app.css`), its basis as before: ten tabs are 149px each on a 1680px screen, three are 160px, and on a narrow one they
shrink alike as designed. *The coins were listed by volume in coins* (DOGE first, BTC last):
by price now, the majors first. *Every grid's drag zones* took two rows of each tile: a screen's grids are saved with
them folded (`showDragZones`), their chevron bringing them back.

**A tab not shown does not tick (2026-10-25).** Ten tabs are 35 grids, and every one ran again at each change of its
rows, shown or not: a tick reached the screen 100 ms after its rows (the old host's lane, the desk opened there), where
one screen's took 20-50. A live grid's tick now waits while no sheet shown shows its rows -- the grid itself, or a chart
of it, in front of a stack or behind another tile (`CubePage.showsGrid`: a feed in front reads the grid stacked behind
it) -- and runs once its sheet is shown (`PageApp`'s `onSheets`); each tick is a measure on the browser's timeline
(`dh:tick`, the market host's: from its rows to the grid painted again).

**Nor does it read its first view (2026-10-25).** With the ticks held, the first tab still drew in 14.5 s and ticked at
1.3 s while the other nine tabs' 31 grids read their first views on the server at once. Lazy about work (docs/TENETS.md):
a host's live grid on a sheet not shown opens at once and reads its rows once a sheet showing it is shown
(`CubeAppOptions.opensWhen`; the page's `ready` waits on none of it), so a tab's grids are read when it is first shown.

The market's model's elements (what a saved query reads of its project: pure/v1's `grammarToJson` over the 2.3 MB
corpus) are read as the tab starts, not when the first saved query opens (eager about knowledge): the desk's open, 10.7
s, became 8.3.

Measured (`//datacube:deephaven_desk_test`, manual: the pinned Chromium at 1680x1050, a world of 100 names, 2,000 quotes
and 400 prints a second, half an hour of morning, crypto simulated): the desk open in 8.3 s from DataCube's own page,
its first tab drawn 8.2 s later and ticking at 54 ms (p50); every other tab read and drawn when first shown in 0.2 to
3.3 s -- the options desk 9.3 s, its Black and Scholes formulas compiled by the server the first time -- and each tab's
ticks on screen 6 to 54 ms after their rows (p50, 142 to 577 ticks in five seconds). Open…'s Examples list the desk
first. Saved by Save As… as "My trading desk", DataCube opened again on its own start, the desk found in Open…'s saved
pages and opened from there in 14.3 s: its ten tabs, the first drawn and ticking, Crypto read when shown. A screenshot
of each tab is the test's output. Not yet: the desk's open is the page written (each screen's sources typed, the market
asked for its largest names) and its 35 saved queries opened one after another, each parsed and typed again.

**Found, not fixed (2026-10-25):** `//datacube:deephaven_board_test`'s reopen check failed twice in five runs with the
same code that passed it the other three (and on the pushed commit): the saved board opened again, its book's first view
landed with a window of 0 rows (`{"offset":0,"total":0}`), the next tick filling it. The board's path is the risk demo's
(ServerBases over its own host), untouched here; a base let go by the page's old grids and held again by its new ones
(same key) is the likely race. The check reads the first view; a person would see the book empty for a tick.
