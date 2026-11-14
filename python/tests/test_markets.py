"""THE MARKETS EXAMPLE OVER THE TRADING MODEL'S CLASSES ON PYTHON (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11,
Phase 6's D6): the screen's example page (datacube/src/examples/markets-classes.ts, as BUILD.bazel :markets_page writes
it) loaded over the markets project -- the one trading model and the markets sample's days as its Data -- as this
host's depot gives it, each grid a saved query read through stress::AllMapping, its rows the project's own; each tile's
series and marks read by the screen's one rule (headless timeseriesQuery), every figure the database's."""

import json
import os
import tempfile
import unittest
import warnings
from pathlib import Path
from unittest import mock

import pandas as pd

import legend_lite as ll
from legend_lite import _headless, datacube


class MarketsOverClasses(unittest.TestCase):
    """One page, loaded once: the 4 MB model compiled for it, its tiles asked of it."""

    @classmethod
    def setUpClass(cls):
        cls._site = os.environ.get('LEGEND_LITE_SITE')
        os.environ['LEGEND_LITE_SITE'] = tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))
        datacube._session = datacube._Session()
        # the project as the site lists it (datacube:markets_project): the trading model, the project's own elements and
        # its Data
        files = [{'path': 'trading-model.pure', 'pureCode': Path(os.environ['LEGEND_LITE_MARKETS_MODEL']).read_text('utf-8')},
                 {'path': 'markets-model.pure', 'pureCode': Path(os.environ['LEGEND_LITE_MARKETS_OWN']).read_text('utf-8')},
                 {'path': 'markets-data.pure', 'pureCode': Path(os.environ['LEGEND_LITE_MARKETS_DATA']).read_text('utf-8')}]
        answer = _headless.answer_fetch
        cls.asked = []

        # the host's depot: the markets project's version, as the site's config.json lists its files
        def depot(method, url, body):
            if url.startswith('/depot/api/projects/dependencies/pure'):
                cls.asked.append(json.loads(body))
                return 200, json.dumps([{'groupId': 'demo', 'artifactId': 'markets', 'versionId': '0.0.0', 'files': files}])
            return answer(method, url, body)
        cls._depot = mock.patch.object(_headless, 'answer_fetch', depot)
        cls._depot.start()
        cls.page = ll.Page.load(json.loads(Path(os.environ['LEGEND_LITE_MARKETS_PAGE']).read_text('utf-8')))
        # each grid's rows as they are (a tile reads its source's rows, never its groups): grouped by none
        for grid in cls.page.grids:
            grid.group()

    @classmethod
    def tearDownClass(cls):
        cls._depot.stop()
        for view in list(datacube._session.pages):
            view.close()
        if datacube._session._headless is not None:
            datacube._session._headless.close()
        if cls._site is None:
            os.environ.pop('LEGEND_LITE_SITE', None)
        else:
            os.environ['LEGEND_LITE_SITE'] = cls._site

    def tile(self, title):
        return next(t for t in self.page.timeseries_tiles if t.title.startswith(title))

    def test_each_grid_a_saved_query_of_the_project_its_rows_its_data(self):
        self.assertIn([{'groupId': 'demo', 'artifactId': 'markets', 'versionId': '0.0.0'}], self.asked)
        quotes, trades = self.page.grids
        self.assertEqual([quotes.title, trades.title], ['Quotes (MarketQuote)', 'Trades (Trade)'])
        rows = quotes.to_pandas()
        self.assertEqual(len(rows), 13_040, "a quote a day of each of the ten instruments, the sample's 1,304 business days")
        self.assertEqual(sorted(rows['ticker'].unique()), ['AAPL', 'EURUSD', 'GLD', 'JPM', 'MSFT', 'NDX', 'NVDA', 'SPX', 'TLT', 'XOM'])
        # each instrument's sector and exchange through the model's associations
        self.assertEqual(set(rows[rows['ticker'] == 'XOM'][['sector', 'exchange']].itertuples(index=False, name=None)),
                         {('Energy', 'New York Stock Exchange')})
        self.assertEqual(sorted(trades.to_pandas()['side'].unique()), ['BUY', 'SELL'], 'its side its enumeration\'s name')

    def test_a_line_per_instrument_of_a_sector_through_the_associations(self):
        quotes = self.page.grids[0].to_pandas()
        days = quotes[quotes['ticker'] == 'AAPL']['quoteTime'].nunique()
        frame = self.tile('Technology').frame('A')
        self.assertEqual(sorted(frame.columns), ['AAPL', 'MSFT', 'NVDA'], 'the sector\'s three instruments, a line each')
        self.assertEqual([len(frame[c].dropna()) for c in sorted(frame.columns)], [days] * 3, 'every day of each')
        # a line of the quotes' grid, as the grid holds it: Apple's close each day
        close = self.tile('Apple').frame('A')
        aapl = quotes[quotes['ticker'] == 'AAPL'].sort_values('quoteTime')
        self.assertEqual([(pd.Timestamp(d).date(), float(v)) for d, v in close.iloc[:, 0].items()],
                         [(pd.Timestamp(d).date(), float(v)) for d, v in zip(aapl['quoteTime'], aapl['close'])])

    def test_the_trades_as_marks_each_a_row_of_the_trades_grid(self):
        tile = self.tile('Apple')
        with warnings.catch_warnings(record=True) as said:
            warnings.simplefilter('always')
            marks = tile.marks()
        self.assertEqual([str(w.message) for w in said], [], 'no cut: fewer than the most a set draws')
        trades = self.page.grids[1].to_pandas()
        for name, side in (('Bought', 'BUY'), ('Sold', 'SELL')):
            mine = marks[marks['name'] == name]
            rows = trades[(trades['ticker'] == 'AAPL') & (trades['side'] == side)].sort_values('tradeDate', ascending=False)
            self.assertGreater(len(rows), 20, f'{side}: the sample\'s Apple trades')
            self.assertEqual([(pd.Timestamp(d).date(), float(v), float(t)) for d, v, t in zip(mine['date'], mine['value'], mine['text'])],
                             [(pd.Timestamp(d).date(), float(p), float(q)) for d, p, q in zip(rows['tradeDate'], rows['price'], rows['quantity'])],
                             f'{name}: each Apple {side} at its price on its day, its quantity beside it, latest first')
        kinds = [(q['kind'], q.get('marks')) for q in tile.queries() if q['kind'] == 'marks']
        self.assertEqual(kinds, [('marks', 0), ('marks', 1)], 'listed with its every query')


if __name__ == '__main__':
    unittest.main()
