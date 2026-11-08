# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""THE GS-QUANT ORACLE (docs/TS_ONE_TREE_PLAN_2026_10_31.md, step 2).

The frozen answers hold lite to what it answered; this holds it to what gs-quant answers. Every series of the
gallery and markets pages is recomputed here with gs_quant.timeseries itself, on the same sample data, from the
same tree, and compared point by point with lite's answer.

Its input is what //datacube:timeseries_ts_parity_test writes to its undeclared outputs (oracle-input.json):
each series' tree (inlined, every parameter told: what lite computed) and its #TS text (shown in the report), its
tile's settings, lite's answer, and the samples. It reads the tree, never a text: no grammar of its own (the order to
one reader, docs/TS_DECISIONS_2026_11_07.md §4, step 3). Run it with gs-quant
installed (it is not one of the repository's locked requirements -- a manual check, as the corpus lanes are):

    bazel test //datacube:timeseries_ts_parity_test
    python3 -m venv /tmp/gsq && /tmp/gsq/bin/pip install gs-quant
    /tmp/gsq/bin/python -I tools/ts-oracle/oracle.py \\
        bazel-testlogs/datacube/timeseries_ts_parity_test/test.outputs/oracle-input.json > report.txt

THE SECOND ORACLE (B5, docs/TS_V1_PROGRAM_2026_11_01.md): a word gs-quant has no function for is computed from the
word's own definition (its doc in projects/timeseries/model.pure) by a pinned outside reference -- TA-Lib, statsmodels
or pandas -- each by its own window rule (a value once the window holds w observations: TA-Lib's lookback, pandas'
rolling(w), statsmodels' RollingOLS; of a line's returns its w-th return, gs-quant's x[w:]). Install them beside
gs-quant, pinned:

    /tmp/gsq/bin/pip install gs-quant==2.2.3 TA-Lib==0.6.8 statsmodels==0.14.5

Each series is reported with the oracle that answered it (gs-quant, TA-Lib, statsmodels, pandas, or gs-quant with one
of them) as AGREE (every date lite answers the oracle answers, every field of a struct, the same to 1e-6 relative),
DIFFER (values or dates differ: the first differences shown), DEPARTS (a difference the language names: its register
row or its word's own sentence, below), NO-ORACLE (no oracle reads it, and why), or NOT-CHECKED (a tile setting the
oracle does not model yet: a split, a grain, an alignment, a from-date).
"""

import io
import json
import math
import re
import sys

import numpy as np
import pandas as pd
import gs_quant.timeseries as ts
import statsmodels.api as sm
import talib
from gs_quant.timeseries.econometrics import _get_annualization_factor
from gs_quant.timeseries.helper import Returns, Window
from statsmodels.tsa.stattools import adfuller

SPAN = re.compile(r"\b(\d+)(d|w|mo|q|y)\b")


class NoGsQuant(Exception):
    """A word gs-quant has no function for (or one that needs its service)."""


def frames(samples):
    out = {}
    for cube, text in samples.items():
        df = pd.read_csv(io.StringIO(text))
        out[cube] = df
    return out


def window(w):
    """A window as gs-quant takes it: a count of observations, a span ('1y'), every one to date (the tree's 0:
    Window(None, 0)), or the whole line (the tree's 'whole': gs-quant's None)."""
    if w == "whole":
        return None
    if w is None or w == "all" or w == 0:
        return Window(None, 0)
    if isinstance(w, str):
        n, unit = SPAN.fullmatch(w).groups()
        return {"d": f"{n}d", "w": f"{n}w", "mo": f"{n}m", "q": f"{3 * int(n)}m", "y": f"{n}y"}[unit]
    return int(w)


class Evaluator:
    def __init__(self, data, sources):
        self.data = data
        self.sources = {s["alias"]: s for s in sources}
        self.default = sources[0]["alias"]

    def line(self, alias, column, key):
        src = self.sources[alias]
        df = self.data[src["cube"]]
        rows = df[df[src["key"]].astype(str) == str(key)] if src["key"] else df
        s = pd.Series(rows[column].values, index=pd.to_datetime(rows[src["date"]]), name=f"{column}[{key}]")
        return s.dropna().sort_index().astype(float)

    def eval(self, node):
        kind = node["kind"]
        if kind == "number":
            return node["value"]
        if kind == "column":
            where = node.get("where") or []
            if len(where) != 1 or "column" in where[0]:
                raise NoGsQuant(f"a line the oracle does not pick: {node['column']} of {where or 'every row'}")
            if node.get("fn", "last") != "last":
                raise NoGsQuant(f"a period's {node['fn']} of {node['column']}")
            return self.line(node.get("source", self.default), node["column"], where[0]["value"])
        if kind == "binary":
            a, b = self.eval(node["left"]), self.eval(node["right"])
            if isinstance(a, pd.Series) and isinstance(b, pd.Series):
                a, b = a.align(b, join="inner")
            op = {"+": lambda x, y: x + y, "-": lambda x, y: x - y, "*": lambda x, y: x * y, "/": lambda x, y: x / y,
                  "<": lambda x, y: (x < y).astype(float), ">": lambda x, y: (x > y).astype(float),
                  "<=": lambda x, y: (x <= y).astype(float), ">=": lambda x, y: (x >= y).astype(float),
                  "==": lambda x, y: (x == y).astype(float), "!=": lambda x, y: (x != y).astype(float),
                  "and": lambda x, y: ((x != 0) & (y != 0)).astype(float),
                  "or": lambda x, y: ((x != 0) | (y != 0)).astype(float)}[node["op"]]
            return op(a, b)
        if kind == "call":
            return self.call(node["fn"], [self.eval(a) for a in node["args"]], dict(node.get("params") or {}))
        raise NoGsQuant(f"no reading of a {kind}")

    def call(self, name, a, kw):
        x = a[0] if a else None
        arg = lambda i, k, d=None: a[i] if len(a) > i else kw.get(k, d)  # noqa: E731
        windowed = {"mean": ts.mean, "avg": ts.mean, "sum": ts.sum_, "min": ts.min_, "max": ts.max_, "std": ts.std,
                    "var": ts.var, "median": ts.median, "volatility": ts.volatility, "zscores": ts.zscores,
                    "max_drawdown": ts.max_drawdown, "product": ts.product,
                    "smoothed_moving_average": ts.smoothed_moving_average}
        if name in windowed:
            return windowed[name](x, window(arg(1, "w")))
        if name == "range":
            return ts.range_(x, window(arg(1, "w")))
        if name == "percentiles":
            return ts.percentiles(x, w=window(arg(1, "w")))
        if name == "percentile":
            return ts.percentile(x, float(arg(1, "n", 50)), window(arg(2, "w")))
        if name == "returns":
            kind = {"log": "logarithmic"}.get(arg(2, "type", "simple"), arg(2, "type", "simple"))
            return ts.returns(x, int(arg(1, "obs", 1)), Returns(kind))
        if name == "diff":
            return ts.diff(x, int(arg(1, "obs", 1)))
        if name == "lag":
            from gs_quant.timeseries.analysis import LagMode
            obs = arg(1, "obs", 1)
            return ts.lag(x, int(obs) if not isinstance(obs, str) else window(obs), LagMode(kw.get("mode", "extend")))
        if name == "prices":
            return ts.prices(x, arg(1, "initial", 1))
        if name == "index":
            return ts.index(x, arg(1, "initial", 1))
        if name in ("change", "annualize", "geometrically_aggregate", "first", "last", "count", "day", "month",
                    "year", "quarter", "weekday"):
            return {"change": ts.change, "annualize": ts.annualize, "geometrically_aggregate": ts.geometrically_aggregate,
                    "first": ts.first, "last": ts.last, "count": ts.count, "day": ts.day, "month": ts.month,
                    "year": ts.year, "quarter": ts.quarter, "weekday": ts.weekday}[name](x)
        if name in ("exponential_moving_average", "exponential_std", "exponential_volatility",
                    "exponential_spread_volatility"):
            return getattr(ts, name)(x, float(arg(1, "beta", 0.75)))
        if name == "macd":
            return ts.macd(x, int(arg(1, "m", 12)), int(arg(2, "n", 26)), int(arg(3, "s", 1)))
        if name == "rsi":
            return ts.relative_strength_index(x, int(arg(1, "w", 14)))
        if name == "bollinger_bands":
            return ts.bollinger_bands(x, window(arg(1, "w", 20)), float(arg(2, "k", 2)))
        if name == "winsorize":
            return ts.winsorize(x, float(arg(1, "limit", 2.5)))
        if name == "vol_swap_volatility":
            return ts.vol_swap_volatility(x, window(arg(1, "w")))
        if name in ("correlation", "beta", "cov"):
            return {"correlation": ts.correlation, "beta": ts.beta, "cov": ts.cov}[name](x, a[1], window(arg(2, "w", 63)))
        if name == "if":
            return ts.if_(x, a[1], a[2])
        if name == "compare":
            return ts.compare(x, a[1])
        if name in ("union_of", "union", "append", "prepend"):
            return {"union_of": ts.union, "union": ts.union, "append": ts.append, "prepend": ts.prepend}[name]([x, a[1]])
        if name == "smooth_spikes":
            from gs_quant.timeseries.analysis import ThresholdType
            return ts.smooth_spikes(x, float(arg(1, "threshold", 0.5)), ThresholdType(arg(2, "type", "percentage")))
        if name == "basket":
            from gs_quant.timeseries.backtesting import RebalFreq
            return ts.basket_series(a[0::2], [float(w) for w in a[1::2]],
                                    rebal_freq=RebalFreq(str(kw.get("rebalance", "daily")).lower()))
        if name == "interpolate":
            from gs_quant.timeseries.helper import Interpolate
            return ts.interpolate(x, a[1], Interpolate(arg(2, "method", "intersect")))
        if name == "sharpe_ratio":
            # the line read as excess-return levels: gs-quant's sharpe_ratio of curve_type EXCESS_RETURNS, whose pure
            # core needs no session (econometrics.get_ratio_pure)
            from gs_quant.timeseries.econometrics import get_ratio_pure
            w = window(arg(1, "w", 0))
            return get_ratio_pure(x, w if w is not None else None)
        if name == "excess_returns":
            # over a cash index, or a rate accrued actual/360 (gs-quant's own, offline)
            return ts.excess_returns_pure(x, a[1]) if isinstance(a[1], pd.Series) else ts.excess_returns(x, float(a[1]))
        if name == "weighted_sum":
            return ts.weighted_sum(a[0::2], [float(w) for w in a[1::2]])
        if name in ("abs", "exp", "log", "sqrt"):
            return {"abs": ts.abs_, "exp": ts.exp, "log": ts.log, "sqrt": ts.sqrt}[name](x)
        if name in ("floor", "ceil"):
            # gs-quant's floor and ceil hold a series at a value (max(x, v), min(x, v)), as the tile's do
            return (ts.floor if name == "floor" else ts.ceil)(x, float(arg(1, "value", 0)))
        if name == "power":
            return ts.power(x, float(arg(1, "y", 2)))
        if name == "not":
            return ts.not_(x)
        if name == "filter":
            from gs_quant.timeseries.algebra import FilterOperator
            op = arg(1, "operator", None)
            # the library's value is 0 unless given (filter_values'); gs-quant asks for one with an operator
            return ts.filter_(x, None if op in (None, "empty") else FilterOperator(op),
                              None if op in (None, "empty") else float(arg(2, "value", 0)))
        if name == "consecutive":
            from gs_quant.timeseries.analysis import SignDirection
            return ts.consecutive(x, SignDirection(arg(1, "direction", "positive")))
        raise NoGsQuant(name)


class NoOracle(Exception):
    """A series no oracle reads, and why."""


def over(df, w, f, extra=0, least=1):
    """f of each window of df's rows (a series or a frame): the last w observations (and `extra` more: a difference's
    first value), a value once the window holds them -- the references' own rule (TA-Lib's lookback, pandas'
    rolling(w), statsmodels' RollingOLS), the words' docs' "a value once it holds w"; of a line's returns, its w-th
    return is its (w + 1)-th date, as gs-quant's x[w:] -- or every one to date (all), once it holds `least`."""
    w = window(w)
    if w is None:
        raise NoOracle("the whole line: no reference reads one")
    if isinstance(w, str):
        raise NoOracle("a calendar window: no reference counts one alike")
    n = None if isinstance(w, Window) else w + extra
    vals = []
    for i in range(len(df)):
        if n is None:
            vals.append(f(df.iloc[: i + 1]) if i + 1 >= least + extra else np.nan)
        else:
            vals.append(f(df.iloc[i - n + 1: i + 1]) if i + 1 >= n else np.nan)
    return pd.Series(vals, index=df.index, dtype=float)


def met(*lines):
    """Lines on the dates every one has (the library's helpers::meet)."""
    return pd.concat(lines, axis=1, join="inner")


def own_returns(*lines):
    """Each line's simple returns on its own dates, then met (the pair words' returns, S1c, L6)."""
    return met(*[ts.returns(x).dropna() for x in lines])


def fit(y, x):
    """statsmodels' least squares of y on x and a constant."""
    if len(y) < 2:
        return None
    return sm.OLS(np.asarray(y, dtype=float), sm.add_constant(np.asarray(x, dtype=float), has_constant="add")).fit()


def annual(x):
    """The periods a year the dates infer, gs-quant's rule (econometrics._get_annualization_factor)."""
    return _get_annualization_factor(x)


class Second(Evaluator):
    """gs-quant's evaluator, and each word it lacks from a pinned reference -- the word's definition, its doc's, computed
    by TA-Lib, statsmodels or pandas (B5)."""

    def __init__(self, data, sources):
        super().__init__(data, sources)
        self.used = set()

    def eval(self, node):
        if node["kind"] == "column" and node.get("fn", "last") != "last":
            # a column's aggregate per date (a period's, of no SAMPLE BY: a date's rows)
            src = self.sources[node.get("source", self.default)]
            where = node.get("where") or []
            if len(where) != 1 or "column" in where[0]:
                raise NoOracle(f"a line the oracle does not pick: {node['column']} of {where or 'every row'}")
            df = self.data[src["cube"]]
            rows = df[df[src["key"]].astype(str) == str(where[0]["value"])] if src["key"] else df
            agg = {"sum": "sum", "max": "max", "min": "min", "avg": "mean", "mean": "mean", "first": "first",
                   "count": "count"}.get(node["fn"])
            if agg is None:
                raise NoOracle(f"a date's {node['fn']}")
            self.used.add("pandas")
            out = rows.groupby(pd.to_datetime(rows[src["date"]]))[node["column"]].agg(agg)
            return out.dropna().sort_index().astype(float)
        return super().eval(node)

    def call(self, name, a, kw):
        f = getattr(self, "w_" + name, None)
        if f is None:
            self.used.add("gs-quant")
            return super().call(name, a, kw)
        ref, out = f(a, kw)
        self.used.add(ref)
        return out

    # -- the technicals: TA-Lib's, where it has the word ------------------------------------------------------------
    def w_atr(self, a, kw):
        h, l, c = met(*a[:3]).T.values
        idx = met(*a[:3]).index
        return "TA-Lib", pd.Series(talib.ATR(h, l, c, timeperiod=int(kw.get("w", 14))), index=idx)

    def w_stochastic(self, a, kw):
        m = met(*a[:3])
        h, l, c = m.T.values
        k, _ = talib.STOCHF(h, l, c, fastk_period=int(kw.get("w", 14)), fastd_period=1, fastd_matype=0)
        # none where the high and the low are one (the word's sentence; TA-Lib's gives 0)
        hh = pd.Series(h, index=m.index).rolling(int(kw.get("w", 14))).max()
        ll = pd.Series(l, index=m.index).rolling(int(kw.get("w", 14))).min()
        return "TA-Lib", pd.Series(k, index=m.index).where(hh != ll)

    def w_obv(self, a, kw):
        m = met(*a[:2])
        c, v = m.T.values
        # TA-Lib's starts at the first volume; the word's at 0 (StockCharts', its sentence): TA-Lib's less that volume
        return "TA-Lib", pd.Series(talib.OBV(c, v) - v[0], index=m.index)

    def w_donchian_channels(self, a, kw):
        m = met(*a[:2])
        w = kw.get("w", 20)
        if not isinstance(window(w), int):
            raise NoOracle("Donchian's channels of no count of observations: TA-Lib's MAX and MIN take one")
        lower = pd.Series(talib.MIN(m.iloc[:, 1].values, timeperiod=int(w)), index=m.index)
        upper = pd.Series(talib.MAX(m.iloc[:, 0].values, timeperiod=int(w)), index=m.index)
        return "TA-Lib", pd.DataFrame({"lower": lower, "upper": upper, "width": upper - lower})

    def w_keltner_channels(self, a, kw):
        m = met(*a[:3])
        h, l, c = m.T.values
        w, k = int(kw.get("w", 20)), float(kw.get("k", 2))
        # the close's exponential mean of span w from the first close (pandas' ewm, adjust=False), drawn once it holds
        # w closes; k of TA-Lib's average true range over a either side
        mid = m.iloc[:, 2].ewm(span=w, adjust=False).mean()
        mid.iloc[: w - 1] = np.nan
        atr = pd.Series(talib.ATR(h, l, c, timeperiod=int(kw.get("a", 10))), index=m.index)
        lower, upper = mid - k * atr, mid + k * atr
        return "TA-Lib", pd.DataFrame({"lower": lower, "upper": upper, "width": upper - lower})

    def w_vwap(self, a, kw):
        m = met(*a[:2])
        return "pandas", over(m, kw.get("w", 20), lambda d: (d.iloc[:, 0] * d.iloc[:, 1]).sum() / d.iloc[:, 1].sum()
                              if d.iloc[:, 1].sum() != 0 else np.nan)

    def w_ohlc(self, a, kw):
        m = met(*a[:4])
        m.columns = ["open", "high", "low", "close"]
        return "pandas", m

    # -- a line against its start ----------------------------------------------------------------------------------
    def w_rebase(self, a, kw):
        x = a[0]
        return "pandas", x / x.iloc[0] * float(kw.get("base", 100))

    def w_percent_change(self, a, kw):
        # a share of its first value (the word's unit is a ratio, shown in percent)
        x = a[0]
        return "pandas", x / x.iloc[0] - 1

    def w_drawdown(self, a, kw):
        return "pandas", over(a[0], kw.get("w", 0), lambda s: s.iloc[-1] / s.max() - 1)

    def w_total_return(self, a, kw):
        x = a[0]
        if isinstance(a[1], pd.Series):
            raise NoOracle("total_return over a line of dividends: no sample has one")
        days = pd.Series(x.index, index=x.index).diff().dt.days
        growth = (x / x.shift(1) + float(a[1]) * days / 365).fillna(1.0)
        return "pandas", x.iloc[0] * growth.cumprod()

    # -- returns' statistics (gs-quant's window rule, pandas' arithmetic) ------------------------------------------
    def w_tracking_error(self, a, kw):
        r = own_returns(a[0], a[1])
        active = r.iloc[:, 0] - r.iloc[:, 1]
        return "pandas", over(active, kw.get("w", 63), lambda s: s.std()) * math.sqrt(annual(active)) * 100

    def w_information_ratio(self, a, kw):
        r = own_returns(a[0], a[1])
        active = r.iloc[:, 0] - r.iloc[:, 1]
        return "pandas", over(active, kw.get("w", 63), lambda s: s.mean() / s.std() if len(s) > 1 else np.nan) \
            * math.sqrt(annual(active))

    def capture(self, a, kw, up):
        r = own_returns(a[0], a[1])

        def f(d):
            on = d[d.iloc[:, 1] > 0] if up else d[d.iloc[:, 1] < 0]
            return on.iloc[:, 0].mean() / on.iloc[:, 1].mean() if len(on) else np.nan
        return "pandas", over(r, kw.get("w", 63), f)

    def w_up_capture(self, a, kw):
        return self.capture(a, kw, True)

    def w_down_capture(self, a, kw):
        return self.capture(a, kw, False)

    def w_lead_lag(self, a, kw):
        lag = int(kw.get("lag", 1))
        rx = ts.returns(a[0]).dropna()
        ry = ts.returns(a[1]).dropna().shift(lag).dropna()
        r = met(rx, ry)
        return "pandas", over(r, kw.get("w", 63), lambda d: d.iloc[:, 0].corr(d.iloc[:, 1]) if len(d) > 1 else np.nan)

    # -- regressions: statsmodels' least squares ---------------------------------------------------------------------
    def w_alpha(self, a, kw):
        r = own_returns(a[0], a[1])

        def f(d):
            m = fit(d.iloc[:, 0], d.iloc[:, 1])
            return np.nan if m is None else m.params[0]
        return "statsmodels", over(r, kw.get("w", 63), f, least=2) * annual(r.iloc[:, 0])

    def w_r_squared(self, a, kw):
        r = own_returns(a[0], a[1])

        def f(d):
            m = fit(d.iloc[:, 0], d.iloc[:, 1])
            return np.nan if m is None else m.rsquared
        return "statsmodels", over(r, kw.get("w", 63), f)

    def w_hedge_ratio(self, a, kw):
        lv = met(a[0], a[1])

        def f(d):
            m = fit(d.iloc[:, 0], d.iloc[:, 1])
            return np.nan if m is None else m.params[1]
        return "statsmodels", over(lv, kw.get("w", 63), f)

    def w_residual(self, a, kw):
        lv = met(a[0], a[1])

        def f(d):
            m = fit(d.iloc[:, 0], d.iloc[:, 1])
            return np.nan if m is None else d.iloc[-1, 0] - (m.params[0] + m.params[1] * d.iloc[-1, 1])
        return "statsmodels", over(lv, kw.get("w", 63), f)

    def w_dickey_fuller(self, a, kw):
        def f(s):
            if len(s) < 4:
                return np.nan
            return adfuller(s.values, maxlag=0, regression="c", autolag=None)[0]
        return "statsmodels", over(a[0], kw.get("w", 0), f, extra=1)

    def w_half_life(self, a, kw):
        def f(s):
            if len(s) < 3:
                return np.nan
            m = fit(s.diff().iloc[1:], s.shift(1).iloc[1:])
            b = m.params[1]
            return -math.log(2) / math.log(1 + b) if -1 < b < 0 else np.nan
        return "statsmodels", over(a[0], kw.get("w", 0), f, extra=1)


# The differences the language names: a series' register row or its word's own sentence. A DIFFER of one of them is
# reported as DEPARTS.
NAMED = {
    "if-compare B": "S145: lite's step does not backfill before a line's first value (gs-quant's align bfills)",
    "winsorize A": "S150: to date, where gs-quant reads the whole series (winsorize(x, 1.5, whole) is gs-quant's)",
}


def lite_fields(entry):
    """lite's answer per field: `value` of a line, each of a struct's (ts_value_lower: lower)."""
    cols = {c["name"]: c["values"] for c in entry["columns"]}
    dates = cols.get("date") or cols[entry["sources"][0]["date"]]
    out = {}
    for name, values in cols.items():
        if not name.startswith("ts_value"):
            continue
        field = "value" if name == "ts_value" else name[len("ts_value_"):]
        out[field] = {pd.Timestamp(d).tz_localize(None).normalize(): float(v) for d, v in zip(dates, values)
                      if v is not None and not (isinstance(v, float) and math.isnan(v))}
    return out


def compare_fields(entry, got):
    lite = lite_fields(entry)
    if isinstance(got, pd.Series):
        got = got.to_frame("value")
    if not isinstance(got, pd.DataFrame):
        return "NOT-CHECKED", f"the oracle answers {type(got).__name__}"
    missing = [f for f in lite if f not in got.columns]
    if missing:
        return "NOT-CHECKED", f"the oracle has no {', '.join(missing)}"
    results = [(f, compare(lite[f], got[f])) for f in lite]
    bad = [(f, r) for f, r in results if r[0] != "AGREE"]
    if not bad:
        return "AGREE", "; ".join(f"{f} {r[1]}" for f, r in results) if len(results) > 1 else results[0][1][1]
    return "DIFFER", "; ".join(f"{f}: {r[1]}" for f, r in bad)


def lite_points(entry):
    cols = {c["name"]: c["values"] for c in entry["columns"]}
    dates = cols.get("date") or cols[entry["sources"][0]["date"]]
    value = next((c for c in ("ts_value", "ts_value_upper", "ts_upper") if c in cols), None)
    if value is None:
        raise NoGsQuant("no value column: " + ", ".join(cols))
    out = {}
    for d, v in zip(dates, cols[value]):
        if v is not None and not (isinstance(v, float) and math.isnan(v)):
            out[pd.Timestamp(d).tz_localize(None).normalize()] = float(v)
    return out


def compare(lite, gsq):
    if isinstance(gsq, pd.DataFrame):
        gsq = gsq["upper"] if "upper" in gsq.columns else gsq.iloc[:, -1]
    if not isinstance(gsq, pd.Series):
        return "NOT-CHECKED", f"gs-quant answers {type(gsq).__name__}"
    g = {pd.Timestamp(k).normalize(): float(v) for k, v in gsq.dropna().items()}
    common = sorted(set(lite) & set(g))
    bad = [d for d in common if abs(lite[d] - g[d]) > 1e-6 * max(1.0, abs(g[d]))]
    only_lite = sorted(set(lite) - set(g))
    only_gsq = sorted(set(g) - set(lite))
    if not bad and not only_lite and not only_gsq:
        return "AGREE", f"{len(common)} points"
    notes = [f"{len(common)} common, {len(bad)} differ, {len(only_lite)} lite's alone, {len(only_gsq)} gs-quant's alone"]
    for d in bad[:3]:
        notes.append(f"{d.date()}: lite {lite[d]:.10g}, gs-quant {g[d]:.10g}")
    if only_lite:
        notes.append(f"lite alone from {only_lite[0].date()}")
    if only_gsq:
        notes.append(f"gs-quant alone from {only_gsq[0].date()}")
    return "DIFFER", "; ".join(notes)


def main(path):
    data = json.load(open(path))
    frames_ = frames(data["samples"])
    tally = {}
    lines = []
    for sid, e in sorted(data["series"].items()):
        why = [k for k in ("split", "grain", "align", "anchor") if e.get(k)]
        if why:
            status, note = "NOT-CHECKED", ", ".join(why)
        else:
            oracle = "gs-quant"
            try:
                try:
                    got = Evaluator(frames_, e["sources"]).eval(e["tree"])
                    status, note = compare(lite_points(e), got)
                except NoGsQuant:
                    # the second oracle: the words gs-quant lacks from the references (B5)
                    second = Second(frames_, e["sources"])
                    got = second.eval(e["tree"])
                    oracle = "+".join(sorted(second.used, key=str.lower))
                    status, note = compare_fields(e, got)
            except (NoGsQuant, NoOracle) as n:
                status, note, oracle = "NO-ORACLE", str(n), "-"
            except Exception as x:  # noqa: BLE001 -- the report says what failed
                status, note = "ERROR", f"{type(x).__name__}: {str(x)[:120]}"
            if status == "DIFFER" and sid in NAMED:
                status, note = "DEPARTS", f"{NAMED[sid]} -- {note}"
        if status == "NOT-CHECKED":
            oracle = "-"
        tally[status] = tally.get(status, 0) + 1
        lines.append(f"{status:11} {oracle:11} {sid:28} {e['formula'][:70]:70} {note}")
    print("\n".join(lines))
    print("\n" + ", ".join(f"{k} {v}" for k, v in sorted(tally.items())))
    by = {}
    for ln in lines:
        by[ln.split()[1]] = by.get(ln.split()[1], 0) + 1
    print("by oracle: " + ", ".join(f"{k} {v}" for k, v in sorted(by.items())))
    # the differences of one kind, said once: lite's first value a point before gs-quant's
    first = [ln.split()[1] + " " + ln.split()[2] for ln in lines if ln.startswith("DIFFER")
             and "0 differ, 1 lite's alone, 0 gs-quant's alone" in ln]
    if first:
        print(f"of the DIFFER, {len(first)} differ only in lite's first value, one observation before gs-quant's:"
              f" {', '.join(first)}")


if __name__ == "__main__":
    main(sys.argv[1])
