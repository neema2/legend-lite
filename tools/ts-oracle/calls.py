# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""GS-QUANT RUNS SHOW PYTHON'S CALLS (the review of 2026-11-08, its finding 6: the Python the tile shows was read back
only by legend-lite's own builder, never by gs-quant -- `filter_(x, operator='empty', value=0)` and
`vol_swap_volatility(x, n_days=0)` were held as gs-quant's calls, and gs-quant raises on both).

Each row of datacube/test/fixtures/timeseries/builder-python.json that has Python code (every gallery series and every
word's own call, as //datacube:timeseries_builder_test holds them) is run here with gs_quant.timeseries itself: each word
the gs-quant function its signature names (projects/timeseries/gs-quant-signatures.json), its arguments bound to
gs-quant's own signature (inspect.signature(...).bind: a keyword gs-quant has not, an argument too many) and the call
made on sample lines -- so a value gs-quant raises on (an operator it has no member for, a window of 0) is reported. A
call that needs gs-quant's service (a Marquee session: a currency's cash for sharpe_ratio) is bound and not run.
column(...) and series(...) are the builder's own (a line of the tile's sources, a series of the tile's), a sample line
each here; column(...).period(...) is the tile's frequency's value, the same line. A word gs-quant has none of
(DataCube's own: rebase, drawdown, atr, ... -- the builder's words of no gs-quant function, read from
datacube/src/generated/ts-builder-words.ts) is no gs-quant call: it answers a sample line, its arguments its own.

Run it with gs-quant installed (not one of the repository's locked requirements -- a manual check, as the oracle is):

    /tmp/gsq/bin/python -I tools/ts-oracle/calls.py datacube/test/fixtures/timeseries/builder-python.json \\
        projects/timeseries/gs-quant-signatures.json datacube/src/generated/ts-builder-words.ts

It prints a line per row -- RUNS (every call gs-quant's, or DataCube's own), BOUND (a call needs gs-quant's service),
SAMPLE (gs-quant refuses the sample line, not the call: a word's own call made on prices where it reads 1s and 0s), or
REFUSED (gs-quant's words) -- and exits 1 when any row is refused.
"""

import enum
import importlib
import inspect
import json
import pkgutil
import re
import sys
import zlib

import numpy as np
import pandas as pd
import gs_quant.timeseries as ts
from gs_quant.errors import MqRequestError, MqUninitialisedError
from gs_quant.timeseries.helper import Window

# seven years of business days: trend and seasonally_adjusted read two whole cycles of a year, and the gallery's splice
# reads dates from 2023
DATES = pd.bdate_range('2018-01-01', periods=1800)

#: the rows whose call gs-quant refuses for the sample line alone, each with why (a word's own call, made by
#: //datacube:timeseries_builder_test's sampleCall on price lines)
SAMPLE_ONLY = {
    'word if_': "gs-quant's flags are 1s and 0s; the sample call's flags are prices",
}


class Line(pd.Series):
    """A sample line, and the builder's .period(...) on it (the tile's frequency's value: the same line here). Its
    comparisons are the builder's (legend_lite.timeseries' Term, the program's own import): two lines compare on the dates
    both have (S152), where plain pandas raises on two series of different dates -- the program's operators are the
    wrapper's, never pandas' (the user, 2026-11-08: "our python was going to show our typed gs-quant wrapper of #TS")."""

    @property
    def _constructor(self):
        return Line

    def period(self, fn):
        return self

    def _met(self, other, op):
        if isinstance(other, pd.Series):
            a, b = self.align(other, join='inner')
            return Line(op(pd.Series(a), pd.Series(b)))
        return Line(op(pd.Series(self), other))

    def __lt__(self, other):
        return self._met(other, lambda a, b: a < b)

    def __le__(self, other):
        return self._met(other, lambda a, b: a <= b)

    def __gt__(self, other):
        return self._met(other, lambda a, b: a > b)

    def __ge__(self, other):
        return self._met(other, lambda a, b: a >= b)


def sample(*key):
    """A line of positive prices, the same for the same name: a random walk seeded by it."""
    rng = np.random.default_rng(zlib.crc32(repr(key).encode('utf-8')))
    return Line(100 * np.exp(np.cumsum(rng.normal(0, 0.01, len(DATES)))), index=DATES)


class Row:
    """What a row's run needed: gs-quant's service (a call bound, not run), DataCube's own words."""

    def __init__(self):
        self.bound_only = []
        self.own = []


def gs_call(name, fn, row):
    """gs-quant's `fn`: its arguments bound to its signature, then called; a call that needs gs-quant's service is
    recorded and answers a sample line, so the call around it runs on."""
    sig = inspect.signature(fn)

    def call(*args, **kwargs):
        sig.bind(*args, **kwargs)
        try:
            return fn(*args, **kwargs)
        except (MqUninitialisedError, MqRequestError) as e:
            row[0].bound_only.append(f'{name}: {type(e).__name__}')
            return sample('service', name)

    return call


def modules():
    yield ts
    for info in pkgutil.iter_modules(ts.__path__):
        try:
            yield importlib.import_module(f'gs_quant.timeseries.{info.name}')
        except Exception:  # noqa: BLE001 -- a module that cannot load offline names nothing here
            continue


def enumeration(name):
    for m in modules():
        e = getattr(m, name, None)
        if isinstance(e, type) and issubclass(e, enum.Enum):
            return e
    raise SystemExit(f'gs-quant has no enumeration {name}')


def own_words(builder_words_path):
    """The builder's words of no gs-quant function (`gsFunction: null`), by the name its code calls them."""
    text = open(builder_words_path, encoding='utf-8').read()
    own_shapes = set(re.findall(r"^const (\w+): WordShape = \{ word: '[^']+', gsFunction: null\b", text, re.M))
    return sorted(name for name, shape in re.findall(r"^  (\w+): (\w+_SHAPE),$", text, re.M) if shape in own_shapes)


def own_call(name, row):
    def call(*args, **kwargs):
        row[0].own.append(name)
        return sample('own', name, len(args))

    return call


def namespace(signatures, builder_words_path, row):
    ns = {'Window': Window, 'column': lambda name, *picks: sample('column', name, picks),
          'series': lambda sid: sample('series', sid)}
    for name in own_words(builder_words_path):
        ns[name] = own_call(name, row)
    for sigs in signatures['words'].values():
        for s in sigs:
            path = s['function']
            module, _, name = path.rpartition('.')
            ns[name] = gs_call(name, getattr(importlib.import_module(f'gs_quant.timeseries.{module}'), name), row)
    # the builder's per-point words, gs-quant's algebra's; excess returns over a line, gs-quant's excess_returns_pure
    # (the printer's name for excess_returns of two lines)
    for name in ('log', 'exp', 'sqrt', 'abs_', 'power', 'not_', 'excess_returns_pure'):
        ns[name] = gs_call(name, getattr(ts, name), row)
    for name in signatures['enums']:
        ns[name] = enumeration(name)
    return ns


def main(fixture_path, signatures_path, builder_words_path):
    rows = json.load(open(fixture_path, encoding='utf-8'))
    signatures = json.load(open(signatures_path, encoding='utf-8'))
    holder = [Row()]
    ns = namespace(signatures, builder_words_path, holder)
    counts = {'RUNS': 0, 'BOUND': 0, 'SAMPLE': 0, 'REFUSED': 0}
    for r in rows:
        code = r.get('python')
        if code is None:
            continue
        holder[0] = Row()
        try:
            eval(code, {'__builtins__': {}}, dict(ns))  # noqa: S307 -- the fixture's own code, run on purpose
            verdict = 'BOUND' if holder[0].bound_only else 'RUNS'
            note = '; '.join([*holder[0].bound_only, *(f"{w}: DataCube's own" for w in sorted(set(holder[0].own)))])
        except Exception as e:  # noqa: BLE001 -- gs-quant's refusal, whatever its class, is the report
            verdict = 'SAMPLE' if r['series'] in SAMPLE_ONLY else 'REFUSED'
            note = SAMPLE_ONLY.get(r['series']) or f'{type(e).__name__}: {str(e)[:300]}'
        counts[verdict] += 1
        print(f'{verdict:8} {r["series"]}: {code}' + (f'\n         {note}' if note else ''))
    print(f'\n{counts["RUNS"]} run, {counts["BOUND"]} bound (gs-quant\'s service), {counts["SAMPLE"]} refused for the '
          f'sample line alone, {counts["REFUSED"]} refused')
    return 1 if counts['REFUSED'] else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1], sys.argv[2], sys.argv[3]))
