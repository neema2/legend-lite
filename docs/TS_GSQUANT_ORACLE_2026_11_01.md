# The gs-quant oracle (2026-11-01)

Step 2 of the one-tree plan (`docs/TS_ONE_TREE_PLAN_2026_10_31.md`): the frozen answers hold lite to what it
answered; the oracle holds it to what gs-quant answers. Every series of the gallery and markets pages (210) is
recomputed with `gs_quant.timeseries` itself (gs-quant 2.2.3), on the same sample data, from the same formula, and
compared point by point with lite's answer, to 1e-6 relative.

How to run it, and what each status means: `tools/ts-oracle/oracle.py`'s header. Its input is written by
`//datacube:timeseries_ts_parity_test` to its undeclared outputs (`oracle-input.json`). gs-quant is not a locked
requirement of the repository: the oracle is a manual check, as the corpus lanes are. The full report of this run:
`tools/ts-oracle/report-2026-11-01.txt`.

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

