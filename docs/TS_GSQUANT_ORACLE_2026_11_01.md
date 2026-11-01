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

| | Series | |
|---|---|---|
| AGREE | 119 | every date lite answers gs-quant answers, the same |
| DIFFER, the first value | 26 | the same values, but lite's first one an observation before gs-quant's (below, O1) |
| DIFFER, compare | 1 | `compare(close[SPX], mean(close[SPX], 200))` before the mean has a value (below, O2) |
| NO-GSQUANT | 39 | a word gs-quant has no function for (`rebase`, `drawdown`, `percent_change`, the technicals, `dickey_fuller`, `half_life`, `lead_lag`, `hedge_ratio`), or one that asks its service (`sharpe_ratio`'s risk-free rate, `alpha`, `r_squared`, `tracking_error`, `information_ratio`: report APIs), or a form the oracle does not read yet (`volume[AAPL].sum`) |
| NOT-CHECKED | 25 | a tile setting the oracle does not model yet: a from-date (13), a SAMPLE BY grain (7), a split per instrument (3), an alignment (2) |

## The rulings asked

**O1. A window's first value.** gs-quant's `mean(x, 50)` (and `std`, `var`, `max`, `min`, `range`, `sum`, `zscores`,
`max_drawdown`, `median` ...) normalizes an integer window `w` to `Window(w, w)`: rolled over `w` observations, and
its first `w` values dropped (`apply_ramp`). Its first value is at the `w + 1`-th observation. lite's first value is
where the window first holds `w` observations, the `w`-th. Every value both answer is the same; lite answers one
point more, at the start. Can lite do exactly as gs-quant does? Yes: a ramp of `w + 1` in the library's words
(`timeseries::line::`), one place. The frozen answers of those 26 series would change by their first point. Asked:
gs-quant's (drop the first point), or lite's as it is (ledgered in `docs/SEMANTICS_REGISTER.md`).

**O2. compare where one side has no value.** gs-quant's `compare(x, y, method=STEP)` puts `y` onto `x`'s dates by
step first: before `y`'s first value, its first value. `mean(close[SPX], 200)` has none for 199 dates; gs-quant
compares the close with the mean's first value there (-1), lite answers 0 (the language's lines meet where both have a
value, R6, and `compare` of none is 0). Can lite do exactly as gs-quant does? Yes: `compare` stepping its second line
as `interpolate(..., step)` does. Asked: gs-quant's, or lite's as it is.

Neither is changed: both are the user's to rule (the upstream-fidelity tenet).
