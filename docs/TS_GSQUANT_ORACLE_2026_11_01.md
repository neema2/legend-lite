# The gs-quant oracle (2026-11-01)

Step 2 of the one-tree plan (`docs/TS_ONE_TREE_PLAN_2026_10_31.md`): the frozen answers hold lite to what it
answered; the oracle holds it to what gs-quant answers. Every series of the gallery and markets pages (210) is
recomputed with `gs_quant.timeseries` itself (gs-quant 2.2.3), on the same sample data, from the same formula, and
compared point by point with lite's answer, to 1e-6 relative.

How to run it, and what each status means: `tools/ts-oracle/oracle.py`'s header. Its input is written by
`//datacube:timeseries_ts_parity_test` to its undeclared outputs (`oracle-input.json`). gs-quant is not a locked
requirement of the repository: the oracle is a manual check, as the corpus lanes are. The full report of this run:
`tools/ts-oracle/report-2026-11-01.txt`.

Since 2026-11-08 (the order's step 3, `docs/TS_DECISIONS_2026_11_07.md` §4) the oracle reads each series' **tree**,
every parameter told as lite computed it, and no text: the tile's own grammar it parsed with Python's is deleted, and
the report shows the series' #TS. Reading the tree, it sends gs-quant lite's own values for parameters a series leaves
to its default, and `all` as `Window(None, 0)` (the text path sent it as a zero window, which gs-quant refuses): on
that day **AGREE 144, DIFFER 2** (the same two), NO-GSQUANT 39, NOT-CHECKED 25, no ERROR (`sum B`,
`sum(volume['AAPL'], all)`, now agrees).

## B5: the second oracle (2026-11-08)

The 39 series of a word gs-quant has no function for are now each read by a pinned outside reference (the oracle's
header says how to install them): TA-Lib 0.6.8 (its C library 0.6.4) for `atr`, `stochastic`, `obv`, Keltner's and
Donchian's channels; statsmodels 0.14.6 for `alpha`, `r_squared`, `hedge_ratio`, `residual` (least squares) and
`dickey_fuller`, `half_life` (`adfuller` of no lags and a constant; the Ornstein-Uhlenbeck slope); pandas for the rest
(`rebase`, `percent_change`, `drawdown`, `sharpe_ratio`, `tracking_error`, `information_ratio`, the captures,
`lead_lag`, `vwap`, `total_return`, a date's `sum`, `ohlc`'s four lines). Each is computed from the word's own
definition, its doc in `projects/timeseries/model.pure`, by the reference's own window rule -- a value once the window
holds w observations (TA-Lib's lookback, pandas' `rolling(w)`, statsmodels' `RollingOLS`), a line's w-th return its
(w + 1)-th date, as gs-quant's `x[w:]` -- and every field of a struct is compared (a channel's lower, upper and width;
a candle's four), not its upper alone. `excess_returns` over a rate is gs-quant's own after all (`econometrics.py`
computes a float rate offline, actual/360). The two differences gs-quant's oracle found are reported as the language
names them (DEPARTS: S145, S150).

**AGREE 183, DEPARTS 2, NOT-CHECKED 25, no DIFFER, no ERROR** -- by oracle: gs-quant 147, pandas 26, statsmodels 6,
TA-Lib 5, gs-quant and pandas 1 (`weighted_sum` of two `rebase`s). The report: `tools/ts-oracle/report-2026-11-08.txt`.
The 25 NOT-CHECKED remain as above (a from-date, a grain, a split, an alignment). The references' first readings found
nothing in lite but two of the library's sentences: `total_return` was credited to gs-quant (2.2.3 has none: the
library's own, now said so) and `percent_change` said "in percent" of a share (its unit a ratio, shown in percent:
said so). Of the library's 86 words, six no oracle reads yet, none of them in a gallery series: `rank`, `round_down`,
`round_up`, `seasonally_adjusted`, `trend`, `value_at`.

**After the review of 2026-11-08** the oracle reads a window's ramp (`r`: gs-quant's `Window(w, r)`; it read none, and
no gallery series carried one -- the gallery's `ramp` tile now does: a 63-day z-score from its first day, a 22-day
volatility from its sixth, a 63-day Sharpe ratio from its eleventh, each the whole line), and a reference answers its
own result: where the word's sentence departs from it (obv from 0, Keltner's middle line from the first close,
stochastic's none where the high is the low) lite is reported DEPARTS with that sentence, never AGREE by an adjusted
reference. **AGREE 185, DEPARTS 4, NOT-CHECKED 26, no DIFFER, no ERROR** -- the three ramps agree with gs-quant at
every point; obv and Keltner's channels depart as their sentences say (stochastic's high never meets its low in the
sample); `across`, a split, is NOT-CHECKED.

## gs-quant runs Show Python's calls (2026-11-08)

The review of #81's commits found that the Python the tile shows (B4: `datacube/test/fixtures/timeseries/
builder-python.json`, every gallery series and every word's own call) was read back only by legend-lite's own builder:
two of its rows were calls gs-quant raises on (`filter_(x, operator='empty', value=0)`, `vol_swap_volatility(x,
n_days=0)`). `tools/ts-oracle/calls.py` now runs every row with gs-quant itself -- each word's arguments bound to
gs-quant's own signature, the call made on sample lines, DataCube's own words (gs-quant has none) answering a sample
line, a call needing gs-quant's service (a currency's cash) bound and not run. Its first run refused 65 rows; all but
two were DataCube's own words or the sample's dates, and the printer's fixes it found are in: `filter_(x)` for the
empty operator, `n_days` always told, `Window(None, 0)` for a window to date, and `excess_returns_pure(x, y)` for
excess returns over another line (gs-quant's `excess_returns` reads an asset, a currency or a rate, never a line).
**253 run (the ramp tile's three among them: `Window(63, 0)`, `Window(22, 5)`, `Window(63, 10)`), 4 bound, 1
refused for the sample alone (`if_`'s own call on prices), none refused.** Show Python is the typed wrapper's program
(`from legend_lite.timeseries import ...`: gs-quant's names, signatures and enumerations over `#TS` terms), so its
operators are the wrapper's -- two lines compare on the dates both have (S152) -- never plain pandas', which raises on
two series of different dates; the check's sample lines compare as the wrapper does (the user, 2026-11-08: "our python
was going to show our typed gs-quant wrapper of #TS").

## What it found

First run (2026-11-01): AGREE 119, DIFFER 27 (26 by a window's first value, O1; compare, O2), NO-GSQUANT 39,
NOT-CHECKED 25. After O1 and O2 below (the same day): **AGREE 145, DIFFER 1**, NO-GSQUANT 39, NOT-CHECKED 25.
2026-11-06, after D2's look-ahead rule (A4, S150): **AGREE 144, DIFFER 2**, both named departures -- `winsorize A`
(`winsorize(returns(close[EURUSD]), 1.5)`) holds each return within 1.5 deviations of the returns to date, where
gs-quant holds it within the whole series' (164 of its 1,303 points differ; `winsorize(x, 1.5, whole)` is gs-quant's).

| | Series | |
|---|---|---|
| AGREE | 144 | every date lite answers gs-quant answers, the same |
| DIFFER | 2 | `compare(close[SPX], mean(close[SPX], 200))` on the first 200 dates: gs-quant's step backfills the mean (R12, below); `winsorize(returns(close[EURUSD]), 1.5)`: to date, where gs-quant's reads the whole series (S150, 2026-11-06) |
| NO-GSQUANT | 39 | a word gs-quant has no function for (`rebase`, `drawdown`, `percent_change`, the technicals, `dickey_fuller`, `half_life`, `lead_lag`, `hedge_ratio`), or one that asks its service (`sharpe_ratio`'s risk-free rate, `alpha`, `r_squared`, `tracking_error`, `information_ratio`: report APIs), or a form the oracle does not read yet (`volume[AAPL].sum`) |
| NOT-CHECKED | 25 | a tile setting the oracle does not model yet: a from-date (13), a SAMPLE BY grain (7), a split per instrument (3), an alignment (2) |

## O1 and O2, matched (the user, 2026-11-01: "Why would we not match gs-quant for 1&2? Any good reason?")

**O1. A window's first value: gs-quant's.** gs-quant's `mean(x, 50)` (and `std`, `var`, `max`, `min`, `range`, `sum`,
`zscores`, `max_drawdown`, `median`, `percentile`, `percentiles`, `product`, `bollinger_bands`) normalizes an integer
window `w` to `Window(w, w)`: rolled over `w` observations, and its first `w` points dropped (`helper.apply_ramp`,
`x[w:]`), so its first value is at the `w + 1`-th observation. lite's had been at the `w`-th. Each of those words'
integer form now ends in `timeseries::line::helpers::ramped(w)`: the line from its `(w + 1)`-th row, its earlier rows
dropped, as gs-quant drops them (a nested window then starts where gs-quant's does: `mean(mean(x, 5), 5)` from the
11th point). The 26 series agree. Building it found a lowering defect, fixed with it: a filter after a filter on a
window column was put in the same select's `WHERE`, which the database runs before the window, so the row numbers
counted only the rows the later filter kept (`lowering/Fold.filterSlot`: beside a `QUALIFY` it joins it; beside a
window column alone the select is wrapped).

**O2. compare: gs-quant's step, but for its backfill.** gs-quant's `compare(x, y, method=STEP)` aligns by
`ffill().bfill()` on the union of dates (`timeseries.datetime.align`). lite's `compare(x, y)` now does the same --
every date either has, each line's last value at or before it, `method` gs-quant's (`intersect`, `nan`, `zero`,
`step`) -- but for the `bfill`: before a line's first value gs-quant gives that first value, a later one. That is the
future read, which the user ruled out on 2026-10-31 (R12, docs/TS_ONE_TREE_PLAN_2026_10_31.md §11: "`STEP` is an
alias of `PREV` ... a named departure"), and it is the whole of this series' difference: where the 200-day mean has no
value, gs-quant compares the close with the mean's first value (-1), lite gives 0 (gs-quant's own answer for a
comparison with NaN). The language's step is the same everywhere (`FILL(STEP)`, `interpolate(x, y, step)`,
`compare`): `docs/SEMANTICS_REGISTER.md` S145.

