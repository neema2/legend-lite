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
| K2 | **the market world**: a realistic equity simulator and a crypto feed handler that gives way to it (DH28), as the server's startup scripts; a store, classes and mappings over their tables | the world's figures checked on the pinned server; the fallback taken when the feed is refused |
| K3 | **a candlestick chart** in DataCube: open, high, low, close and volume by time bucket, drawn again at its grid's tick with its axes held (C2) | DataCube's chart tests; the chart ticking in a browser check |
| K4 | **the ticker**: a watchlist (last, change, bid/ask, volume, day VWAP, high/low), the tape, time and sales, and a live candle chart for the symbol picked | a browser check of every part ticking; the speeds of §4 measured |
| K5 | **the runtime error on Deephaven**: the lowering's guard (Pure fails on `sqrt` of a negative, on an Integer division by zero) writes an `ERROR` no Deephaven formula holds yet; a formula that fails by name, so `sqrt` and Integer `/` plan | the dialect's tests; a failing row failing the view, said, on the server |
| K6 | **windows as `updateBy`**: running and moving figures (by rows and by time), lag and lead, first and last; top-N per group as `headBy`; DataCube's window functions and show-as | each against DuckDB's answer |
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

