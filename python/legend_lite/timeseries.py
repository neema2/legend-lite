"""THE #TS BUILDER IN PYTHON (B3's Python twin, docs/TS_DECISIONS_2026_11_07.md §4, the builders): a series written as a
gs-quant notebook writes one -- ``from legend_lite.timeseries import *`` then ``volatility(column('close', 'SPX'), 22)``,
``spx / ndx - 1``, ``if_(spx > mean(spx, 200), spx, 0)`` -- and plotted on a timeseries tile (``Timeseries.plot(term)``).
gs-quant 2.2.3's names, parameters, defaults, enumerations and Window; Python's operators, as pandas' series take them.
DataCube's builder makes each term the tile's and writes its #TS text (``text(term)``); core reads it."""

import json

from ._ts_terms import ColumnTerm, Term, Window, abs_, column, exp, log, not_, power, series, sqrt
from ._ts_words import *  # noqa: F403 -- gs-quant's words, generated from the catalog
from ._ts_words import __all__ as _words
from ._ts_words import excess_returns

__all__ = ['ColumnTerm', 'Term', 'Window', 'abs_', 'column', 'excess_returns_pure', 'exp', 'log', 'not_', 'power',
           'series', 'sqrt', 'text', *_words]


def excess_returns_pure(price_series: Term, spot_curve: Term) -> Term:
    """gs-quant's excess_returns_pure: ``price_series``' excess returns over another line's levels (a cash index, a
    benchmark's prices) -- excess_returns of two lines, by the name gs-quant calls it (its excess_returns reads an asset,
    a currency or a rate; the review of 2026-11-08)."""
    return excess_returns(price_series, spot_curve)


def text(term: Term) -> str:
    """``term``'s #TS text: what a tile keeps of it, written by DataCube's builder and read by core (the one reader) --
    refused, in DataCube's words, where it is no series (headless timeseriesTermText)."""
    from .datacube import _session  # DataCube's headless engine: the page extras' (pip install legend-lite[datacube])

    said = json.loads(_session.headless().call('timeseriesTermText', json.dumps(term._wire)))
    if 'problems' in said:
        raise ValueError('; '.join(said['problems']))
    return said['text']
