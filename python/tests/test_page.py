"""ll.Page (legend_lite.page; docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md): a DataCube page built in Python -- its
grids over frames, their charts, sheets, layouts and stacks -- written as DataCube's own page document; a calculated
column checked by the compiler at the line that adds it; a filter, an aggregate or a chart outside DataCube's words
refused there; saved and loaded again as it was; shown in a tab and live from then on (a change served again, a frame
updated moving its version), and read back as the open page says it is."""

import json
import os
import tempfile
import unittest
import urllib.error
import urllib.request

import pandas as pd

import legend_lite as ll
from legend_lite import datacube


def trades():
    return pd.DataFrame({'region': ['EMEA', 'AMER', 'EMEA'], 'desk': ['FX', 'Rates', 'Rates'],
                         'notional': [100.0, 250.0, 75.5], 'pnl': [1.5, -2.0, 0.25]})


def desks():
    return pd.DataFrame({'desk': ['FX', 'Rates'], 'head': ['Ana', 'Ben']})


class Paged(unittest.TestCase):
    """Pages on a session of their own, with a site of its own and no browser."""

    def setUp(self):
        self._site = os.environ.get('LEGEND_LITE_SITE')
        os.environ['LEGEND_LITE_SITE'] = tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))
        datacube._session = datacube._Session()

    def tearDown(self):
        for view in list(datacube._session.pages):
            view.close()
        if self._site is None:
            os.environ.pop('LEGEND_LITE_SITE', None)
        else:
            os.environ['LEGEND_LITE_SITE'] = self._site

    def built(self):
        page = ll.Page('Q3 review')
        grid = page.grid(trades(), name='trades', rows=['region'], measures={'notional': 'sum'},
                         filter=[('desk', 'notEqual', 'Equity')], sort=[('notional', 'desc')])
        chart = grid.chart('bar', x='region', y=[('notional', 'sum')], title='Notional by region')
        return page, grid, chart


class Document(Paged):
    def test_a_page_is_datacubes_own_document_one_cube_per_grid_over_its_frame(self):
        page, grid, chart = self.built()
        doc = page.to_dict()
        self.assertEqual((doc['kind'], doc['version'], doc['name']), ('datacube.page', 3, 'Q3 review'))
        cube = doc['cubes'][0]
        self.assertEqual(cube['id'], grid.id)
        self.assertEqual(cube['cube']['source']['_type'], 'frame')
        self.assertEqual(cube['cube']['source']['name'], 'trades')
        # the columns as the compiler types the frame
        self.assertEqual([(c['name'], c['type']) for c in cube['cube']['query']['columns']],
                         [('region', 'String'), ('desk', 'String'), ('notional', 'Float'), ('pnl', 'Float')])
        query = cube['cube']['query']
        self.assertEqual(query['rows'], ['region'])
        self.assertEqual(query['measures'], [{'name': 'notional', 'column': 'notional', 'fn': 'sum'}])
        self.assertEqual(query['sorts'], [{'column': 'notional', 'direction': 'desc'}])
        self.assertEqual(query['filter'], {'kind': 'and', 'children': [
            {'kind': 'condition', 'column': 'desk', 'operator': 'notEqual', 'value': 'Equity'}]})
        self.assertEqual([(v['id'], v['kind'], v['cube']) for v in doc['views']],
                         [(grid.id, 'grid', grid.id), (chart.id, 'chart', grid.id)])
        self.assertEqual(doc['views'][1]['spec'], {'version': 1, 'mark': 'bar', 'x': 'region',
                                                   'y': [{'column': 'notional', 'fn': 'sum'}], 'options': {}})
        # one sheet, the chart beside its grid
        self.assertEqual(doc['sheets'], [{'id': 'sheet-1', 'layout': {'kind': 'bands', 'fit': True, 'bands': [
            {'height': 1.0, 'node': {'split': 'row', 'parts': [
                {'node': {'tile': grid.id}, 'size': 0.5}, {'node': {'tile': chart.id}, 'size': 0.5}]}}]}}])

    def test_sheets_layouts_and_stacks(self):
        page, grid, chart = self.built()
        other = page.grid(desks(), name='desks')
        summary = page.sheet('Summary')
        summary.add(chart, other)
        summary.layout([other], [chart])
        doc = page.to_dict()
        self.assertEqual([(s['id'], s.get('name')) for s in doc['sheets']], [('sheet-1', None), ('sheet-2', 'Summary')])
        self.assertEqual(doc['sheets'][1]['layout']['bands'], [
            {'height': 0.5, 'node': {'tile': other.id}}, {'height': 0.5, 'node': {'tile': chart.id}}])
        # two tiles in one place, as tabs
        page.stack(other, chart)
        self.assertEqual(page.to_dict()['sheets'][1]['layout']['bands'], [{'height': 1.0, 'node': {'stack': [other.id, chart.id]}}])
        with self.assertRaisesRegex(ValueError, 'one sheet'):
            page.stack(grid, chart)
        summary.arrange('side-by-side')
        self.assertEqual(summary.tiles, [other, chart])

    def test_a_calculated_column_is_checked_by_the_compiler_where_it_is_added(self):
        page, grid, _ = self.built()
        # a dataframe's columns can be empty: arithmetic takes them toOne(), as DataCube's compiler asks
        grid.calculate('margin', '$x.pnl->toOne() / $x.notional->toOne()')
        derived = page.to_dict()['cubes'][0]['cube']['query']['derived']
        self.assertEqual(derived[0]['name'], 'margin')
        self.assertEqual(derived[0]['lambda']['_type'], 'lambda')
        with self.assertRaisesRegex(ll.LegendError, 'nope'):
            grid.calculate('wrong', '$x.nope * 2')
        with self.assertRaisesRegex(ll.LegendError, 'multiplicity'):
            grid.calculate('nullable', '$x.pnl / $x.notional')
        # a chart can plot it, as any column
        grid.chart('line', x='region', y=[('margin', 'average')])

    def test_names_outside_datacubes_words_are_refused_where_they_are_written(self):
        page, grid, _ = self.built()
        with self.assertRaisesRegex(ValueError, "not one of DataCube's filter operators"):
            grid.filter(('desk', 'isNot', 'FX'))
        with self.assertRaisesRegex(ValueError, 'not a column'):
            grid.filter(('nope', 'equal', 'FX'))
        with self.assertRaisesRegex(ValueError, 'takes no value'):
            grid.filter(('desk', 'isEmpty', 'FX'))
        with self.assertRaisesRegex(ValueError, "not one of DataCube's aggregates"):
            grid.measure('pnl', 'total')
        with self.assertRaisesRegex(ValueError, "not one of DataCube's charts"):
            grid.chart('radar', x='region')
        with self.assertRaisesRegex(ValueError, 'not a column'):
            grid.group('nope')
        # nested groups and a negation, as the filter editor writes them
        grid.filter(('or', [('region', 'equal', 'EMEA'), ('not', ('desk', 'in', ['FX', 'Rates']))]))
        self.assertEqual(page.to_dict()['cubes'][0]['cube']['query']['filter']['children'][1],
                         {'kind': 'not', 'child': {'kind': 'condition', 'column': 'desk', 'operator': 'in', 'value': ['FX', 'Rates']}})

    def test_saved_and_loaded_again_as_it_was_what_it_does_not_model_kept(self):
        page, grid, chart = self.built()
        summary = page.sheet('Summary')
        summary.add(chart)
        path = os.path.join(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR')), 'q3.page.json')
        page.save(path)
        saved = json.loads(open(path, encoding='utf-8').read())
        # a field a newer writer added, kept through a load and a save
        saved['variables'] = [{'name': 'asOf'}]
        saved['cubes'][0]['cube']['query']['keepGroupedColumns'] = True
        again = ll.Page.load(saved, frames={'trades': trades()})
        self.assertEqual(again.to_dict(), saved)
        with self.assertRaisesRegex(ValueError, 'frames trades'):
            ll.Page.load(saved, frames={})


class Live(Paged):
    def get(self, path):
        web = datacube._session.web()
        request = urllib.request.Request(web.url + path, headers={'Authorization': web.authorization})
        try:
            with urllib.request.urlopen(request, timeout=30) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode('utf-8')

    def test_shown_in_a_tab_served_and_each_change_served_again(self):
        page, grid, _ = self.built()
        tab = ll.show(page, browser=False)
        self.assertIn(f'/engine.html?page={page._key}#token=', tab.url)
        status, said = self.get(f'/page.json?page={page._key}')
        self.assertEqual((status, said['version'], said['page']), (200, 1, page.to_dict()))
        # a change in Python: served again, its version moved
        page.sheet('More')
        self.assertEqual(self.get(f'/version.json?page={page._key}')[1]['version'], 2)
        # a frame updated: the frame's version moves, the page's not
        before = self.get(f'/version.json?page={page._key}')[1]
        grid.update(trades().iloc[:2])
        after = self.get(f'/version.json?page={page._key}')[1]
        self.assertEqual(after['version'], before['version'])
        self.assertGreater(after['frames']['trades'], before['frames']['trades'])
        tab.close()
        self.assertEqual(self.get(f'/page.json?page={page._key}')[0], 404)

    def test_read_back_as_the_open_page_says_it_is(self):
        page, _, _ = self.built()
        self.assertIs(page.read(), page, 'not open: itself')
        ll.show(page, browser=False)
        self.assertIs(page.read(), page, 'open, but it said nothing yet')
        # the open page says what it is now (a sheet renamed in DataCube), as demo/engine-page.ts posts it
        open_now = page.to_dict()
        open_now['sheets'][0]['name'] = 'Renamed in DataCube'
        answer = datacube._session.engine().answer('POST', '/page.json', f'page={page._key}', json.dumps(open_now))
        self.assertEqual(answer.status, 204)
        self.assertEqual(page.read().sheets[0].name, 'Renamed in DataCube')
