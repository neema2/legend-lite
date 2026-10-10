"""ll.Page (legend_lite.page; docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md): a DataCube page built in Python -- its
grids over frames, their charts, sheets, layouts and stacks -- written as DataCube's own page document; a calculated
column checked by the compiler at the line that adds it; a filter, an aggregate or a chart outside DataCube's words
refused there; tiles placed and taken off as DataCube's layout places them; saved and loaded again as it was, what it
does not model kept; shown in a tab and live from then on (a change served again, a frame updated moving its version),
and read back into the same page as the open page says it is."""

import copy
import datetime
import json
import os
import tempfile
import unittest
import urllib.error
import urllib.request
from decimal import Decimal

import pandas as pd

import legend_lite as ll
from legend_lite import datacube


def trades():
    return pd.DataFrame({'region': ['EMEA', 'AMER', 'EMEA'], 'desk': ['FX', 'Rates', 'Rates'],
                         'notional': [100.0, 250.0, 75.5], 'pnl': [1.5, -2.0, 0.25]})


def desks():
    return pd.DataFrame({'desk': ['FX', 'Rates'], 'head': ['Ana', 'Ben']})


def numbers(tree):
    """The exact numbers in a protocol tree, wherever they are."""
    if isinstance(tree, dict):
        return [n for v in tree.values() for n in numbers(v)]
    if isinstance(tree, list):
        return [n for v in tree for n in numbers(v)]
    return [tree] if isinstance(tree, Decimal) else []


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

    def bands(self, sheet):
        return sheet.page.to_dict()['sheets'][sheet.page.sheets.index(sheet)]['layout']['bands']


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
        # one sheet: the grid in a band of its own, the chart beside it, as DataCube places them
        self.assertEqual(doc['sheets'], [{'id': 'sheet-1', 'layout': {'kind': 'bands', 'fit': True, 'bands': [
            {'height': 0.5, 'node': {'split': 'row', 'parts': [
                {'node': {'tile': grid.id}, 'size': 0.5}, {'node': {'tile': chart.id}, 'size': 0.5}]}}]}}])

    def test_the_document_is_a_copy_and_its_numbers_exact(self):
        page, grid, _ = self.built()
        doc = page.to_dict()
        doc['cubes'][0]['cube']['query']['rows'].append('desk')
        doc['sheets'][0]['layout']['bands'].clear()
        self.assertEqual(page.to_dict()['cubes'][0]['cube']['query']['rows'], ['region'], 'what is done to it is not done to the page')
        self.assertEqual(len(page.to_dict()['sheets'][0]['layout']['bands']), 1)
        # a calculated column's decimal stays its digits, in the document and the file
        grid.calculate('scaled', '$x.notional->toOne() * 1.50D')
        lambda_ = page.to_dict()['cubes'][0]['cube']['query']['derived'][0]['lambda']
        self.assertIn('1.50', [str(v) for v in numbers(lambda_)])
        self.assertIn('1.50', page.to_json())
        path = os.path.join(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR')), 'q3.page.json')
        page.save(path)
        self.assertIn('1.50', open(path, encoding='utf-8').read())
        again = ll.Page.load(path, frames={'trades': trades()})
        self.assertEqual(again.to_json(), page.to_json())
        # a value JSON cannot carry is refused where it is given
        with self.assertRaises(TypeError):
            grid.configure({'when': datetime.date(2024, 1, 31)})

    def test_tiles_are_placed_and_taken_off_as_datacube_places_them(self):
        page, grid, chart = self.built()
        sheet = page.sheets[0]
        # four places to a band at most: a chart beyond goes into a band of its own below its grid's
        more = [grid.chart('line', x='region', title=f'line {i}') for i in range(3)]
        self.assertEqual([len(b['node']['parts']) if 'split' in b['node'] else 1 for b in self.bands(sheet)], [4, 1])
        # a grid goes into a band of its own at the bottom
        other = page.grid(desks(), name='desks')
        self.assertEqual(self.bands(sheet)[-1], {'height': 0.5, 'node': {'tile': other.id}})
        # taken off: its neighbours close over its place, a band it emptied gone
        page.remove(more[2])
        self.assertEqual([len(b['node']['parts']) if 'split' in b['node'] else 1 for b in self.bands(sheet)], [4, 1])
        page.remove(more[1])
        self.assertEqual(self.bands(sheet)[0]['node']['parts'][2], {'node': {'tile': more[0].id}, 'size': 1 / 3})
        # a grid removed takes the charts that follow it
        page.remove(grid)
        self.assertEqual((page.grids, page.charts), ([other], []))
        self.assertEqual(self.bands(sheet), [{'height': 0.5, 'node': {'tile': other.id}}])
        for gone in (grid, chart):
            with self.assertRaisesRegex(ValueError, 'no longer on its page'):
                gone.title = 'again'

    def test_a_layout_of_splits_and_sizes_is_kept_as_tiles_come_and_go(self):
        page, grid, chart = self.built()
        other = page.grid(desks(), name='desks')
        sheet = page.sheets[0]
        # the layout in the document's own words: the grid at 70%, the chart over the other grid beside it
        sheet.layout_from({'kind': 'bands', 'fit': False, 'bands': [{'height': 1.2, 'node': {'split': 'row', 'parts': [
            {'node': {'tile': grid.id}, 'size': 0.7},
            {'node': {'split': 'column', 'parts': [{'node': {'tile': chart.id}, 'size': 0.4},
                                                   {'node': {'tile': other.id}, 'size': 0.6}]}, 'size': 0.3}]}}]})
        self.assertFalse(page.to_dict()['sheets'][0]['layout']['fit'])
        # a grid added keeps it, in a band of its own at the bottom
        heads = page.grid(desks(), name='heads')
        bands = self.bands(sheet)
        self.assertEqual(bands[0]['node']['parts'][0], {'node': {'tile': grid.id}, 'size': 0.7})
        self.assertEqual(bands[1], {'height': 0.5, 'node': {'tile': heads.id}})
        # one taken off: the column it was in gives its share to the tile left, which takes the column's place
        page.remove(chart)
        self.assertEqual(self.bands(sheet)[0], {'height': 1.2, 'node': {'split': 'row', 'parts': [
            {'node': {'tile': grid.id}, 'size': 0.7}, {'node': {'tile': other.id}, 'size': 0.3}]}})
        # a chart goes beside its grid, its band's places evened out, as DataCube adds one
        added = other.chart('pie', x='desk', y=[('desk', 'count')], title='Desks')
        self.assertEqual([p['node'] for p in self.bands(sheet)[0]['node']['parts']],
                         [{'tile': grid.id}, {'tile': other.id}, {'tile': added.id}])
        self.assertEqual([p['size'] for p in self.bands(sheet)[0]['node']['parts']], [1 / 3] * 3)
        with self.assertRaisesRegex(ValueError, 'not a layout DataCube draws'):
            sheet.layout_from({'kind': 'bands', 'fit': True, 'bands': [{'height': 1, 'node': {'split': 'row', 'parts': [
                {'node': {'tile': grid.id}, 'size': 0.7}, {'node': {'tile': other.id}, 'size': 0.7}]}},
                {'height': 1, 'node': {'split': 'row', 'parts': [{'node': {'tile': added.id}, 'size': 0.5},
                                                                 {'node': {'tile': heads.id}, 'size': 0.5}]}}]})
        with self.assertRaisesRegex(ValueError, 'left out'):
            sheet.layout_from({'kind': 'bands', 'fit': True, 'bands': [{'height': 1, 'node': {'tile': grid.id}}]})

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
        self.assertEqual(chart.sheet, summary)
        # two tiles in one place, as tabs, where the first was (the band the second emptied gone)
        page.stack(other, chart)
        self.assertEqual(self.bands(summary), [{'height': 0.5, 'node': {'stack': [other.id, chart.id]}}])
        with self.assertRaisesRegex(ValueError, 'one sheet'):
            page.stack(grid, chart)
        summary.arrange('side-by-side')
        self.assertEqual(summary.tiles, [other, chart])
        self.assertIs(page.sheets[1], summary, 'the same handle each time')

    def test_a_chart_plots_what_it_is_told_and_a_frozen_one_stays_when_its_grid_goes(self):
        page, grid, chart = self.built()
        chart.plot('line', split='desk')
        chart.options({'labels': True})
        spec = page.to_dict()['views'][1]['spec']
        self.assertEqual(spec, {'version': 1, 'mark': 'line', 'x': 'region', 'y': [{'column': 'notional', 'fn': 'sum'}],
                                'split': 'desk', 'options': {'labels': True}})
        chart.plot(x=None, frozen=True)
        self.assertNotIn('x', page.to_dict()['views'][1]['spec'])
        with self.assertRaisesRegex(ValueError, 'a chart has a title'):
            chart.title = None
        # its grid removed, a frozen chart stays, over its grid's cube (DataCube's detached chart), as a following one
        # does not
        follows = grid.chart('pie', x='region')
        page.remove(grid)
        self.assertEqual((page.grids, page.charts, chart.grid), ([], [chart], None))
        doc = page.to_dict()
        self.assertEqual([c['id'] for c in doc['cubes']], [grid.id])
        self.assertEqual([v['id'] for v in doc['views']], [chart.id])
        with self.assertRaisesRegex(ValueError, 'no longer on its page'):
            follows.options({'labels': True})
        with self.assertRaisesRegex(ValueError, 'has no grid'):
            chart.plot('bar')
        # and a page so loads again
        self.assertEqual(ll.Page.load(doc, frames={'trades': trades()}).to_dict(), doc)

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
        # one reads the calculated columns before it, typed as the grid runs them
        grid.calculate('margin_pct', '$x.margin * 100')
        self.assertEqual([d['name'] for d in page.to_dict()['cubes'][0]['cube']['query']['derived']], ['margin', 'margin_pct'])
        # a chart can plot it, as any column
        grid.chart('line', x='region', y=[('margin', 'average')])

    def test_names_and_values_outside_datacubes_words_are_refused_where_they_are_written(self):
        page, grid, _ = self.built()
        before = page.to_dict()
        with self.assertRaisesRegex(ValueError, "not one of DataCube's filter operators"):
            grid.filter(('desk', 'isNot', 'FX'))
        with self.assertRaisesRegex(ValueError, 'not a column'):
            grid.filter(('nope', 'equal', 'FX'))
        with self.assertRaisesRegex(ValueError, 'takes no value'):
            grid.filter(('desk', 'isEmpty', 'FX'))
        with self.assertRaisesRegex(ValueError, 'a list of values, in their order'):
            grid.filter(('desk', 'in', {'FX', 'Rates'}))
        with self.assertRaisesRegex(ValueError, 'not several'):
            grid.filter(('desk', 'equal', ['FX']))
        for wrong in (None, float('nan'), object(), datetime.datetime(2024, 1, 31)):
            with self.assertRaisesRegex(ValueError, 'takes text, a number'):
                grid.filter(('notional', 'greaterThan', wrong))
        with self.assertRaisesRegex(ValueError, "not one of DataCube's aggregates"):
            grid.measure('pnl', 'total')
        with self.assertRaisesRegex(ValueError, 'a measure named'):
            grid.measure('notional', 'average')
        with self.assertRaisesRegex(ValueError, "not one of DataCube's charts"):
            grid.chart('radar', x='region')
        with self.assertRaisesRegex(ValueError, 'not a column'):
            grid.group('nope')
        with self.assertRaisesRegex(ValueError, 'is a name'):
            page.sheet('  ')
        with self.assertRaisesRegex(ValueError, 'is a name'):
            grid.chart('bar', title='')
        self.assertEqual(page.to_dict(), before, 'nothing refused changed the page')
        # what it takes: nested groups and a negation, as the filter editor writes them; a date and a decimal as their
        # exact text, a relative date as DataCube keeps one
        grid.filter(('or', [('region', 'equal', 'EMEA'), ('not', ('desk', 'in', ['FX', 'Rates']))]))
        self.assertEqual(page.to_dict()['cubes'][0]['cube']['query']['filter']['children'][1],
                         {'kind': 'not', 'child': {'kind': 'condition', 'column': 'desk', 'operator': 'in', 'value': ['FX', 'Rates']}})
        grid.filter([('notional', 'greaterThan', Decimal('12.30')), ('region', 'equal', datetime.date(2024, 1, 31)),
                     ('region', 'lessThan', {'relative': 'today'}), ('notional', 'lessThan', 10)])
        self.assertEqual([c['value'] for c in page.to_dict()['cubes'][0]['cube']['query']['filter']['children']],
                         ['12.30', '2024-01-31', {'relative': 'today'}, 10])

    def test_a_grid_refused_leaves_the_page_and_the_frames_as_they_were(self):
        page, _, _ = self.built()
        before = page.to_dict()
        for given in ({'rows': ['nope']}, {'measures': {'head': 'total'}}, {'filter': ('head', 'nope', 1)},
                      {'sort': [('head', 'sideways')]}):
            with self.assertRaises(ValueError):
                page.grid(desks(), name='desks', **given)
            self.assertEqual(page.to_dict(), before)
            self.assertNotIn('desks', datacube._session.frames, 'a frame served for the grid refused is served no more')
        self.assertEqual(page.grid(desks(), name='desks').id, 'grid-2', 'the ids go on as if nothing was refused')

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
        # the dict it was loaded from is its own: what is done to it is not done to the page
        saved['sheets'][1]['name'] = 'Changed after'
        self.assertEqual(again.sheets[1].name, 'Summary')
        with self.assertRaisesRegex(ValueError, 'frames trades'):
            ll.Page.load(saved, frames={})

    def test_a_page_as_datacube_writes_one_loads_and_changes_with_what_it_does_not_model_kept(self):
        # what the UI writes beside what Python does: a column's kind and aggregate, an open group, a chart's
        # selection, the options all filled, a stack, a split of a split with its sizes, a cube's field it keeps
        page, grid, chart = self.built()
        other = page.grid(desks(), name='desks')
        doc = page.to_dict()
        columns = doc['cubes'][0]['cube']['query']['columns']
        columns[0].update({'kind': 'dimension'})
        columns[2].update({'kind': 'measure', 'aggregate': 'average', 'excludedFromPivot': True})
        doc['cubes'][0]['cube']['tree'] = {'open': [['EMEA']], 'showTotals': True}
        doc['cubes'][0]['cube']['unknown'] = {'fromANewerWriter': 1}
        doc['cubes'][0]['cube']['configuration'] = {'reportTitle': 'Trades', 'showRootAggregation': True}
        chart_view = next(v for v in doc['views'] if v['kind'] == 'chart')
        chart_view['selection'] = [{'kind': 'condition', 'column': 'region', 'operator': 'equal', 'value': 'EMEA'}]
        chart_view['spec']['options'] = {'orientation': 'vertical', 'stack': 'none', 'sort': 'none', 'limit': 25,
                                         'labels': False, 'legend': True}
        doc['sheets'][0]['layout'] = {'kind': 'bands', 'fit': True, 'bands': [
            {'height': 0.62, 'node': {'split': 'row', 'parts': [
                {'node': {'stack': [grid.id, other.id]}, 'size': 0.35}, {'node': {'tile': chart.id}, 'size': 0.65}]}}]}
        loaded = ll.Page.load(doc, frames={'trades': trades(), 'desks': desks()})
        self.assertEqual(loaded.to_dict(), doc)
        # changed in Python: what it does not model is still there
        loaded.grids[0].group('desk')
        loaded.charts[0].options({'labels': True})
        after = loaded.to_dict()
        self.assertEqual(after['cubes'][0]['cube']['query']['columns'], columns)
        self.assertEqual(after['cubes'][0]['cube']['unknown'], {'fromANewerWriter': 1})
        after_chart = next(v for v in after['views'] if v['kind'] == 'chart')
        self.assertEqual(after_chart['selection'], chart_view['selection'])
        self.assertEqual(after_chart['spec']['options'], {**chart_view['spec']['options'], 'labels': True})
        self.assertEqual(after['sheets'], doc['sheets'])

    def test_a_document_python_cannot_work_on_is_refused_saying_why(self):
        page, _, _ = self.built()
        doc = page.to_dict()
        elsewhere = copy.deepcopy(doc)
        elsewhere['cubes'][0]['cube']['source'] = {'_type': 'warehouseTable', 'name': 'trades'}
        with self.assertRaisesRegex(ValueError, 'reads no frame'):
            ll.Page.load(elsewhere, frames={'trades': trades()})
        unplaced = copy.deepcopy(doc)
        unplaced['sheets'][0]['layout']['bands'] = []
        with self.assertRaisesRegex(ValueError, 'every view has its place'):
            ll.Page.load(unplaced, frames={'trades': trades()})
        with self.assertRaisesRegex(ValueError, 'version 2'):
            ll.Page.load({**doc, 'version': 2}, frames={'trades': trades()})


class Live(Paged):
    def get(self, path):
        web = datacube._session.web()
        request = urllib.request.Request(web.url + path, headers={'Authorization': web.authorization})
        try:
            with urllib.request.urlopen(request, timeout=30) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode('utf-8')

    def said(self, page, document, version=None):
        """The open page saying what it is now, as demo/engine-page.ts posts it: the version it opened, its document."""
        version = datacube._session.engine().page_versions(page._key)['version'] if version is None else version
        return datacube._session.engine().answer('POST', '/page.json', f'page={page._key}&version={version}',
                                                 json.dumps(document)).status

    def test_shown_in_a_tab_served_and_each_change_served_again(self):
        page, grid, _ = self.built()
        tab = ll.show(page, browser=False)
        self.assertIn(f'/engine.html?page={page._key}#token=', tab.url)
        status, said = self.get(f'/page.json?page={page._key}')
        self.assertEqual((status, said['version'], said['page']), (200, 1, page.to_dict()))
        # a change in Python: served again, its version moved
        page.sheet('More')
        self.assertEqual(self.get(f'/version.json?page={page._key}')[1]['version'], 2)
        page.name = 'Q3, again'
        self.assertEqual(self.get(f'/page.json?page={page._key}')[1]['page']['name'], 'Q3, again')
        # a frame updated: the frame's version moves once, the page's not
        before = self.get(f'/version.json?page={page._key}')[1]
        grid.update(trades().iloc[:2])
        after = self.get(f'/version.json?page={page._key}')[1]
        self.assertEqual(after['version'], before['version'])
        self.assertEqual(after['frames']['trades'], before['frames']['trades'] + 1)
        # a page shown keeps a grid: DataCube shows no empty page
        with self.assertRaisesRegex(ValueError, 'a grid at least'):
            page.remove(grid)
        tab.close()
        self.assertEqual(self.get(f'/page.json?page={page._key}')[0], 404)

    def test_an_empty_page_is_not_shown(self):
        with self.assertRaisesRegex(ValueError, 'a grid at least'):
            ll.show(ll.Page('Empty'), browser=False)
        self.assertEqual(datacube._session.pages, [])

    def test_read_back_into_the_same_page_as_the_open_page_says_it_is(self):
        page, grid, chart = self.built()
        self.assertIs(page.read(), page, 'not open: itself')
        ll.show(page, browser=False)
        self.assertIs(page.read(), page, 'open, but it said nothing yet')
        # the open page says what it is now: a sheet renamed, the grid regrouped, the chart closed, in DataCube
        open_now = page.to_dict()
        open_now['sheets'][0]['name'] = 'Renamed in DataCube'
        open_now['cubes'][0]['cube']['query']['rows'] = ['desk']
        open_now['views'] = [v for v in open_now['views'] if v['id'] != chart.id]
        open_now['sheets'][0]['layout']['bands'][0]['node'] = {'tile': grid.id}
        self.assertEqual(self.said(page, open_now), 204)
        version = datacube._session.engine().page_versions(page._key)['version']
        self.assertIs(page.read(), page)
        self.assertEqual(page.sheets[0].name, 'Renamed in DataCube')
        # the same page, its handles still good: the grid is the grid, regrouped; the chart closed there is gone here
        self.assertIs(page.grids[0], grid)
        self.assertEqual(page.to_dict()['cubes'][0]['cube']['query']['rows'], ['desk'])
        self.assertEqual(page.charts, [])
        with self.assertRaisesRegex(ValueError, 'no longer on its page'):
            chart.options({'labels': True})
        # read quietly (the open page has it already); a change after serves it with DataCube's changes kept
        self.assertEqual(datacube._session.engine().page_versions(page._key)['version'], version)
        grid.measure('pnl', 'sum')
        served = self.get(f'/page.json?page={page._key}')[1]
        self.assertEqual(served['page']['sheets'][0]['name'], 'Renamed in DataCube')
        self.assertEqual(served['page']['cubes'][0]['cube']['query']['rows'], ['desk'])

    def test_what_the_open_page_says_of_a_page_since_replaced_is_not_read(self):
        page, _, _ = self.built()
        ll.show(page, browser=False)
        stale = page.to_dict()
        stale['sheets'][0]['name'] = 'Of the page before'
        page.sheet('More')
        self.assertEqual(self.said(page, stale, version=1), 409)
        self.assertEqual([s.name for s in page.read().sheets], [None, 'More'])
