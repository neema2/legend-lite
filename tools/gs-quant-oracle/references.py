# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""THE REFERENCES (docs/TS_DECISIONS_2026_11_07.md item 14f, step 4): each of DataCube's own words -- a word gs-quant has
no function for -- as an outside reference computes it from the word's definition (its doc in
projects/timeseries/model.pure): TA-Lib, statsmodels or pandas, pinned with gs-quant (pins.env). snap.py binds each word's
name to its function here, so Show Python's call of the word (calls.tsv: `atr(column('high', 'NVDA'), ..., w=14)`)
answers as gs-quant's calls do, and //core:ts_gsquant_parity_test judges lite against it. Moved from
tools/ts-oracle/oracle.py's second oracle (B5), which they replace.

Each function takes the call as Show Python writes it: its lines, then its parameters by the builder's names, a window
`w` as gs-quant's -- a count, a calendar period ('3m'), Window(None, 0) (or 0) every point to date, Window(w, r) a ramp
apart from its size -- read by the language's rule for the word (`over`). Each answers its reference's own result; a
word whose sentence departs from its reference says so in mapping.yaml (`known: sentence`).
"""

import math

import numpy as np
import pandas as pd
import statsmodels.api as sm
import talib
from gs_quant.timeseries import returns as gs_returns
from gs_quant.timeseries.econometrics import _get_annualization_factor
from gs_quant.timeseries.helper import Window
from statsmodels.tsa.stattools import adfuller

#: the references each word is computed by, for the ledger (mapping.yaml's `ref:` names the same)
REFERENCES = {}


def reference(name):
    def mark(f):
        REFERENCES[f.__name__] = name
        return f
    return mark


UNITS = {"d": "days", "w": "weeks", "m": "months", "y": "years"}


def offset(tenor):
    """A calendar period as Show Python writes it ('3m', '1y', '2w', '10d'): a pandas DateOffset."""
    return pd.DateOffset(**{UNITS[tenor[-1]]: int(tenor[:-1])})


def ramp_of(w, r):
    """The word's window with its ramp told apart (`r=10` beside `w=20`), as Window(w, r)."""
    if r is None:
        return w
    if isinstance(w, Window):
        raise ValueError("a ramp told twice: in its window and by name")
    return Window(None if w in (0, None) else w, r)


def over(df, w, f, extra=0, least=1, first=None, before=0):
    """f of each date's window of df's rows (a series or a frame) -- the language's rule for a DataCube word, which the
    references count alike (TA-Lib's lookback, pandas' rolling(w), statsmodels' RollingOLS):
      a count n         the last n observations, a value once the window holds them (and `extra` more: a difference's
                        first value);
      0, Window(None,r) every observation to date, once it holds `least` (its first r points none);
      '3m'              the dates after three months back, to this one, from the line's first date (`first`, else df's)
                        that period on -- gs-quant's apply_ramp of a DateOffset (item 14g); `extra` rows before it too;
      Window(n, r)      gs-quant's: a window not yet full takes those it has (rolling(n, 0)), its first r points none,
                        nothing where the line is shorter than n -- its points counted on the line x itself, `before`
                        of them before df's first (a line's returns: its first price; gs-quant's apply_ramp of the
                        answer interpolated onto x, as lite's helpers::ramp)."""
    idx = df.index
    vals = []
    if isinstance(w, str):
        off = offset(w)
        start = (idx[0] if first is None else first) + off
        for i, d in enumerate(idx):
            lo = int(np.searchsorted(idx.values, (d - off).to_datetime64(), side="right"))
            part = df.iloc[max(0, lo - extra): i + 1]
            vals.append(f(part) if d >= start and len(part) >= least + extra else np.nan)
    elif isinstance(w, Window) and w.w is not None:
        n, r = int(w.w), int(w.r)
        if len(df) + before < n:
            return pd.Series(np.nan, index=idx, dtype=float)
        for i in range(len(df)):
            part = df.iloc[max(0, i - n + 1 - extra): i + 1]
            vals.append(f(part) if i + before >= r and len(part) >= least + extra else np.nan)
    elif w in (0, None) or isinstance(w, Window):
        r = int(w.r) if isinstance(w, Window) and w.r is not None else 0
        for i in range(len(df)):
            part = df.iloc[: i + 1]
            vals.append(f(part) if i + before >= r and len(part) >= least + extra else np.nan)
    else:
        n = int(w) + extra
        for i in range(len(df)):
            vals.append(f(df.iloc[i - n + 1: i + 1]) if i + 1 >= n else np.nan)
    return pd.Series(vals, index=idx, dtype=float)


def met(*lines):
    """Lines on the dates every one has (the library's helpers::meet)."""
    return pd.concat(lines, axis=1, join="inner")


def own_returns(*lines):
    """Each line's simple returns on its own dates, then met (the pair words' returns, S1c, L6); the first date of the
    lines' prices met, which a calendar window's ramp counts from (the language's: the line's first date); and how many
    of those prices come before the returns' first, which a Window(w, r)'s ramp counts on."""
    rs, prices = met(*[gs_returns(x).dropna() for x in lines]), met(*lines).index
    return rs, prices[0], before(prices, rs)


def before(prices, df):
    """How many of a line's dates come before df's first: a ramp's points are counted on the line itself."""
    return int(prices.get_loc(df.index[0])) if len(df) else 0


def fit(y, x):
    """statsmodels' least squares of y on x and a constant."""
    if len(y) < 2:
        return None
    return sm.OLS(np.asarray(y, dtype=float), sm.add_constant(np.asarray(x, dtype=float), has_constant="add")).fit()


def annual(x, factor=None):
    """The periods a year: the factor told, else the ones the dates infer (gs-quant's _get_annualization_factor)."""
    return float(factor) if factor is not None else _get_annualization_factor(x)


# -- a line against its start ----------------------------------------------------------------------------------------

@reference("pandas")
def percent_change(x):
    """A share of its first value."""
    return x / x.iloc[0] - 1


@reference("pandas")
def drawdown(x, w=0, r=None):
    """Each value's drawdown from the greatest of its window's."""
    return over(x, ramp_of(w, r), lambda s: s.iloc[-1] / s.max() - 1)


# -- the technicals: TA-Lib's, where it has the word -------------------------------------------------------------------

@reference("TA-Lib")
def atr(high, low, close, w=14):
    m = met(high, low, close)
    h, lo, c = m.T.values
    return pd.Series(talib.ATR(h, lo, c, timeperiod=int(w)), index=m.index)


@reference("TA-Lib")
def stochastic(high, low, close, w=14):
    """TA-Lib's fast %K (its own result: the word's sentence gives none where the window's high and low are one)."""
    m = met(high, low, close)
    h, lo, c = m.T.values
    k, _ = talib.STOCHF(h, lo, c, fastk_period=int(w), fastd_period=1, fastd_matype=0)
    return pd.Series(k, index=m.index)


@reference("TA-Lib")
def obv(close, volume):
    """TA-Lib's on-balance volume, from the first volume (the word's sentence: from 0, StockCharts')."""
    m = met(close, volume)
    c, v = m.T.values
    return pd.Series(talib.OBV(c, v), index=m.index)


@reference("TA-Lib")
def keltner_channels(high, low, close, w=20, a=10, k=2):
    """TA-Lib's EMA of the close, k of its ATR either side (the word's sentence: its mean from the first close)."""
    m = met(high, low, close)
    h, lo, c = m.T.values
    mid = pd.Series(talib.EMA(c, timeperiod=int(w)), index=m.index)
    atr_ = pd.Series(talib.ATR(h, lo, c, timeperiod=int(a)), index=m.index)
    lower, upper = mid - float(k) * atr_, mid + float(k) * atr_
    return pd.DataFrame({"lower": lower, "upper": upper, "width": upper - lower})


@reference("TA-Lib")
def donchian_channels(high, low, w=20):
    m = met(high, low)
    if isinstance(w, int) and not isinstance(w, bool) and w > 0:
        lower = pd.Series(talib.MIN(m.iloc[:, 1].values, timeperiod=w), index=m.index)
        upper = pd.Series(talib.MAX(m.iloc[:, 0].values, timeperiod=w), index=m.index)
    else:
        lower = over(m.iloc[:, 1], w, lambda s: s.min())
        upper = over(m.iloc[:, 0], w, lambda s: s.max())
    return pd.DataFrame({"lower": lower, "upper": upper, "width": upper - lower})


@reference("pandas")
def vwap(price, volume, w=20, r=None):
    m = met(price, volume)
    return over(m, ramp_of(w, r), lambda d: (d.iloc[:, 0] * d.iloc[:, 1]).sum() / d.iloc[:, 1].sum()
                if d.iloc[:, 1].sum() != 0 else np.nan)


@reference("pandas")
def ohlc(open, high, low, close):  # noqa: A002 -- Show Python's own name for the line
    m = met(open, high, low, close)
    m.columns = ["open", "high", "low", "close"]
    return m


# -- returns' statistics (pandas' arithmetic over the language's windows) ----------------------------------------------

@reference("pandas")
def tracking_error(x, benchmark, w=63, annualization_factor=None, r=None):
    rs, first, ahead = own_returns(x, benchmark)
    active = rs.iloc[:, 0] - rs.iloc[:, 1]
    return over(active, ramp_of(w, r), lambda s: s.std(), least=2, first=first, before=ahead) \
        * math.sqrt(annual(active, annualization_factor)) * 100


@reference("pandas")
def information_ratio(x, benchmark, w=63, annualization_factor=None, r=None):
    rs, first, ahead = own_returns(x, benchmark)
    active = rs.iloc[:, 0] - rs.iloc[:, 1]
    return over(active, ramp_of(w, r), lambda s: s.mean() / s.std() if len(s) > 1 and s.std() != 0 else np.nan,
                first=first, before=ahead) * math.sqrt(annual(active, annualization_factor))


def capture(x, benchmark, w, r, up):
    rs, first, ahead = own_returns(x, benchmark)

    def f(d):
        on = d[d.iloc[:, 1] > 0] if up else d[d.iloc[:, 1] < 0]
        return on.iloc[:, 0].mean() / on.iloc[:, 1].mean() if len(on) and on.iloc[:, 1].mean() != 0 else np.nan
    return over(rs, ramp_of(w, r), f, first=first, before=ahead)


@reference("pandas")
def up_capture(x, benchmark, w=63, r=None):
    return capture(x, benchmark, w, r, True)


@reference("pandas")
def down_capture(x, benchmark, w=63, r=None):
    return capture(x, benchmark, w, r, False)


@reference("pandas")
def lead_lag(x, y, lag=1, w=63, r=None):
    rx = gs_returns(x).dropna()
    ry = gs_returns(y).dropna().shift(int(lag)).dropna()
    m = met(rx, ry)
    prices = met(x, y).index
    return over(m, ramp_of(w, r), lambda d: d.iloc[:, 0].corr(d.iloc[:, 1]) if len(d) > 1 else np.nan,
                first=prices[0], before=before(prices, m))


# -- regressions: statsmodels' least squares ---------------------------------------------------------------------------

@reference("statsmodels")
def alpha(x, y, w=63, annualization_factor=None, r=None):
    rs, first, ahead = own_returns(x, y)

    def f(d):
        m = fit(d.iloc[:, 0], d.iloc[:, 1])
        return np.nan if m is None else m.params[0]
    return over(rs, ramp_of(w, r), f, least=2, first=first, before=ahead) * annual(rs.iloc[:, 0], annualization_factor)


@reference("statsmodels")
def r_squared(x, y, w=63, r=None):
    rs, first, ahead = own_returns(x, y)

    def f(d):
        m = fit(d.iloc[:, 0], d.iloc[:, 1])
        return np.nan if m is None else m.rsquared
    return over(rs, ramp_of(w, r), f, first=first, before=ahead)


@reference("statsmodels")
def hedge_ratio(x, y, w=63, r=None):
    lv = met(x, y)

    def f(d):
        m = fit(d.iloc[:, 0], d.iloc[:, 1])
        return np.nan if m is None else m.params[1]
    return over(lv, ramp_of(w, r), f)


@reference("statsmodels")
def residual(x, y, w=63, r=None):
    lv = met(x, y)

    def f(d):
        m = fit(d.iloc[:, 0], d.iloc[:, 1])
        return np.nan if m is None else d.iloc[-1, 0] - (m.params[0] + m.params[1] * d.iloc[-1, 1])
    return over(lv, ramp_of(w, r), f)


@reference("statsmodels")
def dickey_fuller(x, w=0, r=None):
    """adfuller's t of b in dx = a + b x[-1], no lags, a constant, over the window's differences (w + 1 values)."""
    def f(s):
        return np.nan if len(s) < 4 else adfuller(s.values, maxlag=0, regression="c", autolag=None)[0]
    return over(x, ramp_of(w, r), f, extra=1, least=3)


@reference("statsmodels")
def half_life(x, w=0, r=None):
    def f(s):
        if len(s) < 4:
            return np.nan
        m = fit(s.diff().iloc[1:], s.shift(1).iloc[1:])
        b = m.params[1]
        return -math.log(2) / math.log(1 + b) if -1 < b < 0 else np.nan
    return over(x, ramp_of(w, r), f, extra=1, least=3)


#: the words this module answers, by Show Python's name
WORDS = {name: globals()[name] for name in REFERENCES}
