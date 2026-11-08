"""THE #TS BUILDER IN PYTHON (B3's Python twin, docs/TS_DECISIONS_2026_11_07.md §4, the builders): each word gs-quant
2.2.3's name and signature -- held to gs-quant's own (projects/timeseries/gs-quant-signatures.json) -- its operators
pandas', and a term plotted on a timeseries tile written by DataCube's builder and read by core, as TypeScript's is."""

import inspect
import json
import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

import legend_lite as ll
from legend_lite import _ts_words, datacube
from legend_lite import timeseries as ts

#: gs-quant's defaults the builders take otherwise, each with its reason: a whole-series seed the language computes none
#: of, the library's 14 (a row of the TypeScript builder's ledger, datacube/test/timeseries-builder.test.ts); the dates
#: interpolate reads a line in every call (gs-quant's None interpolates x over its own dates: x itself); vol_swap's n_days
#: always told (the review of 2026-11-08: gs-quant's None is one figure of the whole series at its last date, which the
#: language does not say -- the library's gsTold fact, a row of the TypeScript builder's ledger)
DEFAULTS_OTHERWISE = {('smoothed_moving_average', 'w'), ('interpolate', 'dates'), ('vol_swap_volatility', 'n_days')}


def gs_default(d):
    """gs-quant's default as Python writes it."""
    if 'window' in d:
        return f'Window({d["window"][0]!r}, {d["window"][1]!r})'
    if 'enum' in d:
        return f'{d["enum"]}.{d["member"]}'
    return repr(d['value'])


class Signatures(unittest.TestCase):
    def test_each_word_is_gs_quants_name_and_signature(self):
        fixture = json.loads(Path(os.environ['LEGEND_LITE_GS_QUANT_SIGNATURES']).read_text(encoding='utf-8'))
        checked = 0
        for word, signatures in fixture['words'].items():
            gs = signatures[0]
            name = gs['function'].rsplit('.', 1)[1]
            fn = getattr(_ts_words, name, None)
            if fn is None:
                continue  # SQL's per point, the builder's own (_ts_terms: log, exp, sqrt, abs_, power, not_)
            params = [p for p in gs['params'] if p['kind'] not in ('VAR_POSITIONAL', 'VAR_KEYWORD')]
            ours = list(inspect.signature(fn).parameters.values())[:len(params)]
            self.assertEqual([p.name for p in ours], [p['name'] for p in params], f'{name}: gs-quant\'s parameters, in order')
            self.assertEqual([p.kind.name for p in ours], [p['kind'] for p in params], f'{name}: by place or by name, as gs-quant')
            for p, g in zip(ours, params):
                if g['default'] is None:
                    self.assertIs(p.default, inspect.Parameter.empty, f'{name}.{p.name} is required, as gs-quant\'s')
                elif (word, p.name) not in DEFAULTS_OTHERWISE:
                    self.assertEqual(repr(p.default), gs_default(g['default']), f'{name}.{p.name}: gs-quant\'s default')
            checked += 1
        self.assertGreater(checked, 50)

    def test_its_enumerations_and_window_are_gs_quants(self):
        self.assertEqual([m.value for m in ts.Returns], ['absolute', 'logarithmic', 'simple'])
        self.assertEqual(repr(ts.Window(None, 0)), 'Window(None, 0)')
        self.assertEqual(ts.Window(22).r, 22, "gs-quant's ramp is its size unless told")


class Terms(unittest.TestCase):
    def test_operators_and_methods_are_pandas(self):
        spx = ts.column('close', 'SPX')
        self.assertEqual((spx / ts.series('B') - 1)._wire, {
            'term': 'method', 'name': 'sub', 'y': 1,
            'x': {'term': 'method', 'name': 'div', 'x': {'term': 'column', 'name': 'close', 'picks': ['SPX']},
                  'y': {'term': 'series', 'id': 'B'}}})
        self.assertEqual((5 + spx)._wire['name'], 'radd')
        self.assertEqual(((spx > 100) & ~(spx < 50))._wire['name'], 'and')
        self.assertEqual((-spx)._wire, {'term': 'method', 'name': 'rsub', 'x': spx._wire, 'y': 0})
        self.assertEqual(spx.add(1)._wire, (spx + 1)._wire)
        with self.assertRaisesRegex(TypeError, 'no truth value'):
            bool(spx > 1)

    def test_a_word_tells_what_was_given_and_leaves_its_defaults_to_datacube(self):
        spx = ts.column('close', 'SPX')
        self.assertEqual(ts.volatility(spx, 22, ts.Returns.LOGARITHMIC)._wire, {
            'term': 'word', 'name': 'volatility', 'lines': [spx._wire], 'kw': {'w': 22, 'returns_type': 'logarithmic'}})
        self.assertEqual(ts.percentile(spx, 90, w=ts.Window('1y'))._wire['kw'], {'n': 90, 'w': {'term': 'window', 'w': '1y', 'r': '1y'}})
        self.assertEqual(ts.column('volume', venue='NYSE').period('sum')._wire,
                         {'term': 'column', 'name': 'volume', 'picks': [{'venue': 'NYSE'}], 'period': 'sum'})


class Plotted(unittest.TestCase):
    """A term plotted on a timeseries tile: DataCube's builder writes its text, core reads it -- what TypeScript's
    builder writes of the same call."""

    def setUp(self):
        self._site = os.environ.get('LEGEND_LITE_SITE')
        os.environ['LEGEND_LITE_SITE'] = tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))
        datacube._session = datacube._Session()

    def tearDown(self):
        for view in list(datacube._session.pages):
            view.close()
        if datacube._session._headless is not None:
            datacube._session._headless.close()
        if self._site is None:
            os.environ.pop('LEGEND_LITE_SITE', None)
        else:
            os.environ['LEGEND_LITE_SITE'] = self._site

    def test_a_term_is_written_by_datacubes_builder_and_read_by_core(self):
        page = ll.Page('Markets')
        days = pd.to_datetime(['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']).date
        px = page.grid(pd.DataFrame({'symbol': ['SPX'] * 4, 'day': days, 'close': [100.0, 101.0, 99.5, 102.0]}), name='prices')
        spx = ts.column('close', 'SPX')
        tile = page.timeseries({'px': px}, [spx, ts.volatility(spx, 2, ts.Returns.LOGARITHMIC)])
        self.assertEqual([s['text'] for s in tile.series], ["close['SPX']", "volatility(close['SPX'], w => 2, type => 'log')"])
        tile.plot(ts.series('A') / ts.series('B') - 1, name='ratio', pane=1)
        self.assertEqual((tile.series[-1]['id'], tile.series[-1]['text'], tile.series[-1]['name']), ('C', 'A / B - 1', 'ratio'))
        tile.edit('C', ts.if_(spx > ts.mean(spx, 2), spx, 0))
        self.assertEqual(tile.series[-1]['text'], "if(close['SPX'] > mean(close['SPX'], 2), close['SPX'], 0)")
        # a ramp apart from a window's size: gs-quant's Window(w, r), the library's r (item 13)
        tile.plot(ts.volatility(spx, ts.Window(5, 2)))
        self.assertEqual(tile.series[-1]['text'], "volatility(close['SPX'], w => 5, type => 'simple', r => 2)")
        # what the builder refuses, in its words: a calendar window's ramp apart from its period
        with self.assertRaisesRegex(ValueError, r"series E: mean.*a calendar window's ramp"):
            tile.plot(ts.mean(spx, ts.Window('3m', 10)))

    def test_a_tiles_python_is_gs_quants_calls_and_run_builds_the_tile_again(self):
        # B4's Show Python (timeseries-code.ts): each series gs-quant's call of it, the program run builds the same tile
        page = ll.Page('Code')
        days = pd.date_range('2024-01-01', periods=8, freq='D').date
        prices = pd.DataFrame({'symbol': ['S'] * 8 + ['T'] * 8, 'day': list(days) * 2,
                               'close': [100.0 + i for i in range(8)] + [50.0 - i for i in range(8)]})
        px = page.grid(prices, name='prices')
        s, t = ts.column('close', 'S'), ts.column('close', 'T')
        tile = page.timeseries({'px': px}, [s, {'term': ts.returns(t, 1, ts.Returns.LOGARITHMIC), 'name': 'log returns', 'pane': 1},
                                            ts.basket_series([ts.series('A'), t], [0.5, 0.5], rebal_freq=ts.RebalFreq.WEEKLY),
                                            ts.zscores(s, 3) > 1],
                               title='Two')
        code = tile.code()
        self.assertIn('from legend_lite.timeseries import ', code)
        self.assertIn("'term': returns(column('close', 'T'), obs=1, type=Returns.LOGARITHMIC)", code)
        self.assertIn("basket_series([series('A'), column('close', 'T')], weights=[0.5, 0.5], rebal_freq=RebalFreq.WEEKLY)", code)
        self.assertIn("zscores(column('close', 'S'), w=3) > 1", code)
        shown = []
        shim = type('ll', (), {'Page': ll.Page, 'show': staticmethod(shown.append)})
        ran = {'ll': shim, 'prices_frame': prices, 'print': lambda *a, **k: None}
        exec(compile(code.replace('import legend_lite as ll\n', ''), 'tile.py', 'exec'), ran)  # noqa: S102 -- DataCube's program
        made = ran['page'].timeseries_tiles
        cubes = lambda spec: {**spec, 'sources': [{**x, 'cube': None} for x in spec['sources']]}  # noqa: E731
        self.assertEqual(cubes(made[0].spec), cubes(tile.spec))

    def test_every_gallery_series_as_pythons_builder_code_writes_its_text(self):
        # B4's Show Python, held by datacube/test/timeseries-builder.test.ts: each source run here is a term whose text,
        # DataCube's builder's and read by core, is the series' own
        rows = json.loads(Path(os.environ['LEGEND_LITE_BUILDER_PYTHON']).read_text(encoding='utf-8'))
        names = {k: getattr(ts, k) for k in ts.__all__}
        wrong = []
        for row in rows:
            if 'python' not in row:
                continue
            term = eval(row['python'], {'__builtins__': {}}, names)  # noqa: S307 -- the fixture's code, DataCube's own
            written = ts.text(term)
            if written != row['text']:
                wrong.append(f"{row['series']}: {row['python']}\n    wrote {written}\n    not   {row['text']}")
        self.assertEqual(wrong, [])
        self.assertEqual(ts.text(ts.mean(ts.column('close'), ts.Window(50, 10))), 'mean(close, 50, 10)')
        self.assertEqual(ts.text(ts.winsorize(ts.column('close'), 2.5, 22)), 'winsorize(close, limit => 2.5, w => whole, r => 22)')
        # gs-quant's Sharpe ratio over a currency's cash, the page's cash line, or of excess returns already (item 13b)
        self.assertEqual(ts.text(ts.sharpe_ratio(ts.column('close'), ts.RiskFreeRateCurrency.EUR, 252)),
                         "sharpe_ratio(excess_returns(close, cash.index['EUR']), w => 252)")
        self.assertEqual(ts.text(ts.sharpe_ratio(ts.column('close'), w=252, curve_type=ts.CurveType.EXCESS_RETURNS)),
                         'sharpe_ratio(close, w => 252)')
        with self.assertRaisesRegex(ValueError, 'smoothed_moving_average takes no ramp'):
            ts.text(ts.smoothed_moving_average(ts.column('close'), ts.Window(20, 5)))
        self.assertGreater(sum(1 for r in rows if 'python' in r), 200)
