# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""THE GS-QUANT ORACLE (docs/TS_ONE_TREE_PLAN_2026_10_31.md, step 2).

The frozen answers hold lite to what it answered; this holds it to what gs-quant answers. Every series of the
gallery and markets pages is recomputed here with gs_quant.timeseries itself, on the same sample data, from the
same formula, and compared point by point with lite's answer.

Its input is what //datacube:timeseries_ts_parity_test writes to its undeclared outputs (oracle-input.json):
each series' formula (inlined), its tile's settings, lite's answer, and the samples. Run it with gs-quant
installed (it is not one of the repository's locked requirements -- a manual check, as the corpus lanes are):

    bazel test //datacube:timeseries_ts_parity_test
    python3 -m venv /tmp/gsq && /tmp/gsq/bin/pip install gs-quant
    /tmp/gsq/bin/python -I tools/ts-oracle/oracle.py \\
        bazel-testlogs/datacube/timeseries_ts_parity_test/test.outputs/oracle-input.json > report.txt

Each series is reported as AGREE (every date lite answers gs-quant answers, the same to 1e-6 relative), DIFFER
(values or dates differ: the first differences shown), NO-GSQUANT (a word gs-quant has no function for, or one that
needs its service), or NOT-CHECKED (a tile setting the oracle does not model yet: a split, a grain, an alignment, a
from-date).
"""

import ast
import io
import json
import math
import re
import sys

import pandas as pd
import gs_quant.timeseries as ts
from gs_quant.timeseries.helper import Returns, Window

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
    """A window as gs-quant takes it: a count of observations, a span ('1y'), or every one to date (None)."""
    if w is None or w == "all":
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
        if isinstance(node, ast.Expression):
            return self.eval(node.body)
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            return -self.eval(node.operand)
        if isinstance(node, ast.Subscript):
            target = node.value
            key = self.eval(node.slice)
            if isinstance(target, ast.Name) and target.id.startswith("__"):
                alias, column = target.id[2:].split("__", 1)
                return self.line(alias, column.rstrip("_"), key)
            return self.line(self.default, target.id.rstrip("_"), key)
        if isinstance(node, ast.BinOp):
            a, b = self.eval(node.left), self.eval(node.right)
            op = {ast.Add: lambda x, y: x + y, ast.Sub: lambda x, y: x - y, ast.Mult: lambda x, y: x * y,
                  ast.Div: lambda x, y: x / y, ast.Pow: lambda x, y: x ** y}[type(node.op)]
            if isinstance(a, pd.Series) and isinstance(b, pd.Series):
                a, b = a.align(b, join="inner")
            return op(a, b)
        if isinstance(node, ast.Compare):
            a, b = self.eval(node.left), self.eval(node.comparators[0])
            if isinstance(a, pd.Series) and isinstance(b, pd.Series):
                a, b = a.align(b, join="inner")
            op = {ast.Lt: lambda x, y: x < y, ast.Gt: lambda x, y: x > y, ast.LtE: lambda x, y: x <= y,
                  ast.GtE: lambda x, y: x >= y, ast.Eq: lambda x, y: x == y}[type(node.ops[0])]
            return op(a, b).astype(float)
        if isinstance(node, ast.Call):
            name = node.func.id.rstrip("_")
            args = [self.eval(a) for a in node.args]
            kw = {k.arg: self.eval(k.value) for k in node.keywords}
            return self.call(name, args, kw)
        raise NoGsQuant(f"no reading of {ast.dump(node)[:60]}")

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
        if name in ("diff", "lag"):
            n = int(arg(1, "obs", 1))
            return ts.diff(x, n) if name == "diff" else x.shift(n).dropna()
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
        if name == "excess_returns" and isinstance(a[1], pd.Series):
            return ts.excess_returns_pure(x, a[1])
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


def formula_ast(text):
    py = re.sub(r"\b(\w+):(\w+)\[", r"__\1__\2[", text)
    # a line's key as text (10Y, SPX); a column a Python keyword names (yield) as its name and an underscore
    py = re.sub(r"\[([A-Za-z0-9_.]+)\]", r"['\1']", py)
    py = re.sub(r"\b(yield|if|not)\b", r"\1_", py)
    py = SPAN.sub(r"'\1\2'", py)
    return ast.parse(py, mode="eval")


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
            try:
                got = Evaluator(frames_, e["sources"]).eval(formula_ast(e["formula"]))
                status, note = compare(lite_points(e), got)
            except NoGsQuant as n:
                status, note = "NO-GSQUANT", str(n)
            except Exception as x:  # noqa: BLE001 -- the report says what failed
                status, note = "ERROR", f"{type(x).__name__}: {str(x)[:120]}"
        tally[status] = tally.get(status, 0) + 1
        lines.append(f"{status:11} {sid:28} {e['formula'][:70]:70} {note}")
    print("\n".join(lines))
    print("\n" + ", ".join(f"{k} {v}" for k, v in sorted(tally.items())))
    # the differences of one kind, said once: lite's first value a point before gs-quant's
    first = [ln.split()[1] + " " + ln.split()[2] for ln in lines if ln.startswith("DIFFER")
             and "0 differ, 1 lite's alone, 0 gs-quant's alone" in ln]
    if first:
        print(f"of the DIFFER, {len(first)} differ only in lite's first value, one observation before gs-quant's:"
              f" {', '.join(first)}")


if __name__ == "__main__":
    main(sys.argv[1])
