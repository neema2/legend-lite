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
from pathlib import Path

import pandas as pd

import legend_lite as ll
from legend_lite import datacube
from legend_lite import page as page_module
from legend_lite._types import plain_type


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
        # DataCube's own new chart of the grid -- across, its row group; up, its measure -- following it
        chart = grid.chart('bar', title='Notional by region')
        return page, grid, chart

    def bands(self, sheet):
        return sheet.page.to_dict()['sheets'][sheet.page.sheets.index(sheet)]['layout']['bands']


class Document(Paged):
    def test_a_page_is_datacubes_own_document_one_cube_per_grid_over_its_frame(self):
        page, grid, chart = self.built()
        doc = page.to_dict()
        self.assertEqual((doc['kind'], doc['version'], doc['name']), ('datacube.page', 4, 'Q3 review'))
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
        # a list of one condition, in the filter editor's normal form: a group of one is that one, as DataCube writes it
        self.assertEqual(query['filter'],
                         {'kind': 'condition', 'column': 'desk', 'operator': 'notEqual', 'value': 'Equity'})
        self.assertEqual(json.loads(page.to_json())['cubes'][0]['cube']['query']['filter'], query['filter'],
                         'read back as DataCube reads it: the same')
        self.assertEqual([(v['id'], v['kind'], v['cube']) for v in doc['views']],
                         [(grid.id, 'grid', grid.id), (chart.id, 'chart', grid.id)])
        # made as DataCube makes a new chart (_made): what it was not told is DataCube's own -- the grid's row group
        # across, its measure up, a category leading with the largest -- and, told nothing of its grouping, it follows
        # the grid
        spec = doc['views'][1]['spec']
        self.assertEqual({k: v for k, v in spec.items() if k != 'options'},
                         {'version': 2, 'mark': 'bar', 'x': 'region', 'y': [{'column': 'notional', 'fn': 'sum'}]})
        # told its grouping, a chart keeps it, as a hand on it in DataCube's options freezes one -- unless told otherwise:
        # then it follows the grid, and one told what the grid would replace is refused, never quietly dropped
        grid.chart('bar', x='desk')
        self.assertTrue(page.to_dict()['views'][-1]['spec']['frozen'])
        grid.chart('bar', x='region', frozen=False)
        self.assertNotIn('frozen', page.to_dict()['views'][-1]['spec'])
        with self.assertRaisesRegex(ValueError, "follows its grid takes 'region' across, not 'desk'"):
            grid.chart('bar', x='desk', frozen=False)
        self.assertEqual(spec['options']['sort'], {'by': 'y', 'direction': 'desc'})
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
        with self.assertRaises(ValueError):
            page.save(path, name='a file carries its page\'s name')
        # a value JSON cannot carry is refused where it is given
        with self.assertRaises(TypeError):
            grid.configure({'when': datetime.date(2024, 1, 31)})

    def test_saved_as_datacubes_save_keeps_it_into_this_users_saved_pages(self):
        # page.save() with no path: legend-lite's saved-page rules in the native library (B6), over ll.home()/pages --
        # what DataCube's Open… lists when it is shown from Python, the tab's own Save keeping it the same way
        from legend_lite.engine import Engine
        page, grid, _ = self.built()
        grid.calculate('scaled', '$x.notional->toOne() * 1.50D')
        first = page.save()
        engine = Engine(ll.Frames())
        listed = json.loads(engine.answer('POST', '/api/pure/v1/query/dataCube/search', '', '{}').body)
        self.assertIn((first, page.name, 'this user'), [(q['id'], q['name'], q['owner']) for q in listed])
        opened = engine.answer('GET', f'/api/pure/v1/query/dataCube/{first}', '', None)
        self.assertEqual(opened.status, 200)
        # its content the page document, its decimal's digits kept
        self.assertEqual(json.loads(opened.body)['content'], json.loads(page.to_json()))
        self.assertIn(b'1.50', opened.body)
        # saved again: over the copy it was saved as, its new name taken
        self.assertEqual(page.save(name='Q3, again'), first)
        again = json.loads(engine.answer('GET', f'/api/pure/v1/query/dataCube/{first}', '', None).body)
        self.assertEqual(again['name'], 'Q3, again')
        # deleted meanwhile (in DataCube's Open…): saved again as a new one under its id
        self.assertEqual(engine.answer('DELETE', f'/api/pure/v1/query/dataCube/{first}', '', None).status, 204)
        self.assertEqual(page.save(), first)
        # another page is another saved page; one DataCube would not open (no grid) is not saved, in DataCube's words
        other = ll.Page('Other')
        with self.assertRaisesRegex(ValueError, 'it has no cube'):
            other.save()
        other.grid(trades(), name='other_trades')
        self.assertNotEqual(other.save(), first)
        with self.assertRaises(ValueError):
            page.save(name=' ')

    def test_tiles_are_placed_and_taken_off_as_datacube_places_them(self):
        page, grid, chart = self.built()
        sheet = page.sheets[0]
        # four places to a band at most: a chart beyond goes into a band of its own below its grid's
        more = [grid.chart('line', title=f'line {i}') for i in range(3)]
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

    def test_a_sheet_is_arranged_as_datacube_arranges_it(self):
        # DataCube's own arrangements (bands.ts arrange, headless): the layout picker's every shape, in reading order
        page, grid, chart = self.built()
        more = [grid.chart('line', title=f'line {i}') for i in range(2)]
        sheet = page.sheets[0]
        sheet.arrange('rows:1-3')
        self.assertEqual([len(b['node']['parts']) if 'split' in b['node'] else 1 for b in self.bands(sheet)], [1, 3])
        self.assertEqual([b['height'] for b in self.bands(sheet)], [0.5, 0.5])
        sheet.arrange('focus-left')
        bands = self.bands(sheet)
        self.assertEqual((len(bands), bands[0]['node']['parts'][0]), (1, {'node': {'tile': grid.id}, 'size': 0.5}))
        self.assertEqual(sheet.tiles, [grid, chart, *more])

    def test_a_grid_over_a_frame_of_other_columns_is_rebuilt_as_datacube_rebuilds_one(self):
        # DataCube's reopening over the frame as it is now (cube-document.ts rebuilt, headless): a gone column left out
        # of everything that read it, said in DataCube's words; a new one hidden; the page one DataCube opens
        page = ll.Page('changed')
        grid = page.grid(trades(), name='changed_trades', rows=['region', 'desk'], measures={'notional': 'sum'})
        with self.assertWarnsRegex(UserWarning, 'left out grouping by desk'):
            grid.update(trades().drop(columns=['desk']).assign(book='B1'))
        cube = page.to_dict()['cubes'][0]['cube']
        self.assertEqual(cube['query']['rows'], ['region'])
        self.assertNotIn('desk', [c['name'] for c in cube['query']['columns']])
        self.assertEqual(cube['configuration']['columns']['book'], {'hidden': True})
        self.assertEqual([c['name'] for c in cube['source']['columns']], ['region', 'notional', 'pnl', 'book'])
        ll.Page.load(page.to_dict(), frames={'changed_trades': trades().drop(columns=['desk']).assign(book='B1')})

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
        with self.assertRaisesRegex(ValueError, 'cannot be laid out: band 0: shares summing to 1.4'):
            sheet.layout_from({'kind': 'bands', 'fit': True, 'bands': [{'height': 1, 'node': {'split': 'row', 'parts': [
                {'node': {'tile': grid.id}, 'size': 0.7}, {'node': {'tile': other.id}, 'size': 0.7}]}},
                {'height': 1, 'node': {'split': 'row', 'parts': [{'node': {'tile': added.id}, 'size': 0.5},
                                                                 {'node': {'tile': heads.id}, 'size': 0.5}]}}]})
        with self.assertRaisesRegex(ValueError, 'no sheet has a place for'):
            sheet.layout_from({'kind': 'bands', 'fit': True, 'bands': [{'height': 1, 'node': {'tile': grid.id}}]})

    def test_a_sheet_gone_is_refused_its_layout_and_an_empty_one_laid_out_in_no_bands_is_as_it_was(self):
        # (the audit of neema2/legend-lite#47) an empty sheet laid out in no bands is as it is -- no shape names no
        # bands -- and a sheet no longer on its page is refused, layout_from too, as every sheet call refuses one
        page, grid, chart = self.built()
        empty = page.sheet('Empty')
        self.assertIs(empty.layout(), empty)
        self.assertEqual(self.bands(empty), [])
        # taken out as reading the open page takes out a sheet closed there (Page.read: the document, then its handles)
        page._doc['sheets'] = [s for s in page._doc['sheets'] if s['id'] != empty.id]
        page._forget({empty.id})
        with self.assertRaisesRegex(ValueError, 'no longer on its page'):
            empty.layout_from({'kind': 'bands', 'fit': True, 'bands': []})

    def test_a_grid_beside_others_is_capped_as_datacube_caps_a_source_added_beside_others(self):
        # DataCube's one rule (gridCapped), of the page's grids counting the new one: the page's first grid is not capped,
        # one beside it is
        page, grid, chart = self.built()
        other = page.grid(desks(), name='desks')
        cubes = {c['id']: c['cube'] for c in page.to_dict()['cubes']}
        self.assertNotIn('maxRows', cubes[grid._view['cube']]['configuration'])
        self.assertIsInstance(cubes[other._view['cube']]['configuration']['maxRows'], int)

    def test_an_empty_list_of_conditions_is_no_filter(self):
        # the filter editor's normal form: an empty group is none, so [] takes the filter off, as None does
        page, grid, chart = self.built()
        grid.filter([])
        self.assertNotIn('filter', page.to_dict()['cubes'][0]['cube']['query'])

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
        # onto a tile stacked already, as DataCube drops one on its middle: its place keeps its tabs, the new one after
        third = other.chart('pie', x='desk', y=[('desk', 'count')], title='Desks')
        self.assertEqual(self.bands(summary)[0]['node']['parts'][1]['node'], {'tile': third.id})
        page.stack(other, third)
        self.assertEqual(self.bands(summary), [{'height': 0.5, 'node': {'stack': [other.id, chart.id, third.id]}}])
        with self.assertRaisesRegex(ValueError, 'one sheet'):
            page.stack(grid, chart)
        summary.arrange('side-by-side')
        self.assertEqual(summary.tiles, [other, chart, third])
        self.assertEqual(self.bands(summary), [{'height': 1, 'node': {'stack': [other.id, chart.id, third.id]}}])
        # the layout picker's shapes, as DataCube names them; one it does not name is refused in its words
        with self.assertRaisesRegex(ValueError, 'not a layout: diagonal'):
            summary.arrange('diagonal')
        self.assertIs(page.sheets[1], summary, 'the same handle each time')

    def test_a_chart_takes_the_spec_version_2_slots_in_their_own_words(self):
        page, grid, _ = self.built()
        chart = grid.chart('bar', x='region', y=[
            ('notional', 'sum'),
            {'column': 'pnl', 'fn': 'average', 'mark': 'line', 'axis': 'right', 'showAs': {'kind': 'movingAverage', 'rows': 3}},
        ], label=('pnl', 'max'), tooltip=[('notional', 'max')],
            layers=[{'kind': 'line', 'value': 100, 'label': 'Target'}, {'kind': 'stat', 'stat': 'average'},
                    {'kind': 'band', 'axis': 'y', 'from': 0, 'to': 50}])
        spec = page.to_dict()['views'][-1]['spec']
        self.assertEqual(spec['version'], 2)
        self.assertEqual(spec['y'][1], {'column': 'pnl', 'fn': 'average', 'mark': 'line', 'axis': 'right',
                                        'showAs': {'kind': 'movingAverage', 'rows': 3}})
        self.assertEqual(spec['label'], {'column': 'pnl', 'fn': 'max'})
        self.assertEqual([layer['kind'] for layer in spec['layers']], ['line', 'stat', 'band'])
        # bins on a number across, kept while it stays across and gone with it
        binned = grid.chart('bar', x='notional', bins={'width': 50}, y=[('pnl', 'count')])
        self.assertEqual(page.to_dict()['views'][-1]['spec']['bins'], {'width': 50})
        binned.plot(x='region')
        self.assertNotIn('bins', page.to_dict()['views'][-1]['spec'])
        # re-plotted, a measure's own words survive
        chart.plot('line')
        self.assertEqual(page.to_dict()['views'][-2]['spec']['y'][1]['mark'], 'line')
        # every word one of DataCube's, and nothing DataCube would refuse to draw, refused at the line that names it, in
        # DataCube's own words (its reader and its rules, asked: _made) -- each chart told its column across, so frozen
        # as a hand on it in DataCube freezes it, and judged as written
        for bad, why in [({'grain': 'decade'}, 'grain "decade" is not one of'),
                         ({'grain': 'month'}, "'region' is not a date"),
                         ({'bins': {'width': 3}}, "'region' is not a number"),
                         ({'y': [{'column': 'notional', 'fn': 'sum', 'mark': 'pie'}]}, "measure 1's mark"),
                         ({'y': [{'column': 'notional', 'fn': 'sum', 'axis': 'top'}]}, "measure 1's axis"),
                         ({'y': [{'column': 'notional', 'fn': 'sum', 'showAs': {'kind': 'forecast'}}]}, 'shown as something'),
                         ({'y': [{'column': 'margin', 'fn': 'value'}]}, 'no group-level column'),
                         ({'y': [('notional', 'wavg')]}, 'no weighted average'),
                         ({'layers': [{'kind': 'sparkle'}]}, 'layer 1 is of a kind'),
                         ({'layers': [{'kind': 'stat', 'stat': 'mode'}]}, "layer 1's statistic"),
                         ({'layers': [{'kind': 'line', 'value': True}]}, 'layer 1 has no value'),
                         ({'layers': [{'kind': 'stat', 'stat': 'average', 'measure': 2}]}, 'reads value 3'),
                         ({'layers': [{'kind': 'extremes'}], 'options': {'stack': 'percent'}}, '100% stack'),
                         ({'options': {'stack': 'stacked100'}}, 'stack "stacked100" is not one of'),
                         ({'bins': {'size': 3}}, 'neither a width nor a count'),
                         ({'bins': {'width': '10'}}, 'neither a width nor a count'),
                         ({'bins': {'width': 0}}, 'not a positive number'),
                         ({'bins': {'count': 2.5}}, 'not a whole number'),
                         ({'options': {'limit': 0}}, 'limit 0 is not a whole number'),
                         # what Python's arguments carry goes as given: DataCube's reader judges its shape too
                         ({'y': [('notional',)]}, 'measure 1 is not a column and an aggregate'),
                         ({'y': [{'column': 'notional', 'fn': 'sum', 'colour': 'red'}]}, 'no "colour" in measure 1'),
                         ({'y': 'notional'}, 'its y "notional" is not a list of measures'),
                         ({'options': {'stak': 'stacked'}}, 'no "stak" in its options'),
                         ({'options': 'wide'}, 'its options "wide" are not an object'),
                         ({'layers': [{'kind': 'line', 'value': 1, 'mesure': 1}]}, 'no "mesure" in layer 1'),
                         ({'bins': {'width': 5, 'cnt': 3}}, 'no "cnt" in its bins'),
                         ({'bins': 50}, 'neither a width nor a count')]:
            with self.subTest(bad=bad), self.assertRaisesRegex(ValueError, why):
                grid.chart('bar', x='region', **{'y': [('notional', 'sum')], **bad})
        with self.assertRaisesRegex(ValueError, 'no reference lines'):
            grid.chart('scatter', x='notional', y=[('pnl', 'sum')], layers=[{'kind': 'line', 'value': 1}])
        # small multiples and a bubble (batch 3), each refused where DataCube does not draw it
        grid.chart('bar', x='region', facet={'column': 'desk'})
        self.assertEqual(page.to_dict()['views'][-1]['spec']['facet'], {'column': 'desk'})
        grid.chart('scatter', x='notional', y=[('pnl', 'sum')], size=('notional', 'sum'))
        self.assertEqual(page.to_dict()['views'][-1]['spec']['size'], {'column': 'notional', 'fn': 'sum'})
        for mark, given, why in [('pie', {'x': 'region', 'facet': {'column': 'desk'}}, 'not drawn as small multiples'),
                                 ('bar', {'x': 'region', 'facet': {'column': 'region'}}, 'across or split already'),
                                 ('bar', {'x': 'region', 'facet': {'side': 'desk'}}, 'names neither a row nor a column'),
                                 ('bar', {'x': 'region', 'facet': {'column': 'desk', 'rows': 'pnl'}}, 'no "rows" in its facet'),
                                 ('bar', {'x': 'region', 'facet': {'column': 'desk'}, 'options': {'brush': True}}, 'no brush'),
                                 ('bar', {'x': 'region', 'size': ('pnl', 'sum')}, 'Only a scatter'),
                                 ('bar', {'x': 'region', 'facet': {'row': 'desk', 'column': 'desk'}}, 'two columns'),
                                 ('waterfall', {'x': 'region', 'y': [{'column': 'pnl', 'fn': 'sum', 'showAs': {'kind': 'rank'}}]}, 'shown as something else'),
                                 ('matrix', {'y': [{'column': 'pnl', 'fn': 'sum', 'showAs': {'kind': 'rank'}}, ('notional', 'sum')]}, 'plots rows')]:
            with self.subTest(mark=mark, given=given), self.assertRaisesRegex(ValueError, why):
                grid.chart(mark, **given)
        # a live chart follows its grid -- regrouped the way the grid is (DataCube's followCube), its measure the grid's --
        # so two values told to a chart told to follow are refused: the grid's measure would replace them
        with self.assertRaisesRegex(ValueError, "follows its grid takes sum of 'notional' as its values"):
            grid.chart('bar', x='region', y=[('notional', 'sum'), ('pnl', 'sum')], frozen=False,
                       layers=[{'kind': 'line', 'value': 1, 'measure': 1}])
        # and a chart written fine cannot be changed into one DataCube refuses: by plot or by options
        lined = grid.chart('bar', x='region', y=[('notional', 'sum'), ('pnl', 'sum')],
                           layers=[{'kind': 'stat', 'stat': 'average'}, {'kind': 'line', 'value': 1, 'measure': 1}])
        kept = page.to_dict()['views'][-1]['spec']
        with self.assertRaisesRegex(ValueError, 'no reference lines'):
            lined.plot('scatter', x='notional')
        with self.assertRaisesRegex(ValueError, 'reads value 2'):
            lined.plot('bar', y=[('notional', 'sum')])
        with self.assertRaisesRegex(ValueError, '100% stack'):
            lined.options({'stack': 'percent'})
        self.assertEqual(page.to_dict()['views'][-1]['spec'], kept, 'a refused change leaves the chart as it was')

    def test_charts_are_refused_exactly_where_datacube_refuses_them(self):
        """DataCube's chart rules, as cases (datacube/test/fixtures/chart-rules.json), which chart-spec.ts is held to:
        Python refuses exactly the same charts by asking DataCube's own code (page._made), never by rules of its own. A
        frame makes the cases' String, Float, StrictDate and DateTime columns; a Date and a StrictTime are calculated
        columns' (Pure's firstDayOfMonth returns a Date, and a time of day is written %10:30:00), judged with the
        compiler's types, as DataCube judges them. No frame carries a StrictTime (the compiler's catalog reads a
        database's TIME as text), and on this plane DataCube adds no StrictTime column either (its compile lowers the
        cube, which has no SQL type for a time of day: docs/PYTHON_ASKS_DATACUBE_2026_10_15.md) -- so the StrictTime
        column comes as a page saved where one is lowered (a server's plane) holds it, loaded. Every case is asked,
        none skipped."""
        rules = json.loads(Path(os.environ['LEGEND_LITE_CHART_RULES']).read_text('utf-8'))
        made = {'String': ['EMEA', 'APAC'], 'Float': [1.5, 2.5], 'StrictDate': [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
                'DateTime': [pd.Timestamp('2024-01-01 09:30'), pd.Timestamp('2024-01-01 10:30')]}
        framed = [c for c in rules['columns'] if c['type'] in made]
        page = ll.Page('rules')
        grid = page.grid(pd.DataFrame({c['name']: made[c['type']] for c in framed}), name='rules')
        types = {c.name: plain_type(c.type) for c in grid._page._session.frames[grid.frame].columns}
        self.assertEqual(types, {c['name']: c['type'] for c in framed}, 'the frame is typed as the cases say')
        days = next(c['name'] for c in framed if c['type'] == 'StrictDate')
        dated = next(c['name'] for c in rules['columns'] if c['type'] == 'Date')
        grid.calculate(dated, f'$x.{days}->toOne()->firstDayOfMonth()')
        timed = next(c['name'] for c in rules['columns'] if c['type'] == 'StrictTime')
        self.assertEqual({c['type'] for c in rules['columns']} - set(made), {'Date', 'StrictTime'}, 'the cases\' types')
        with self.assertRaisesRegex(ValueError, 'STRICT_TIME'):
            grid.calculate(timed, '%10:30:00')
        # the compiler lowers no time of day yet (docs/PYTHON_ASKS_DATACUBE_2026_10_15.md, "A finding for the compiler
        # line"): when it does, the column is grid.calculate's like the Date's, this refusal goes, and so does the page
        # document made by hand below
        doc = page.to_dict()
        doc['cubes'][0]['cube']['query']['derived'].append({'name': timed, 'lambda': ll.parse('x|%10:30:00')})
        page = ll.Page.load(doc, frames={'rules': grid._page._session.frames['rules'].given})
        cube = page.grids[0]._view['cube']
        for case in rules['cases']:
            with self.subTest(case=case['case']):
                if case['refused']:
                    with self.assertRaises(ValueError):
                        page_module._made(page, cube, case['spec'])
                else:
                    page_module._made(page, cube, case['spec'])

    def test_batch_3_kinds_are_written_and_refused_as_datacube_draws_them(self):
        page, grid, _ = self.built()
        # each new kind, written with what it needs
        grid.chart('waterfall', x='desk')
        grid.chart('dumbbell', x='region', y=[('pnl', 'sum'), ('notional', 'sum')])
        # a flow's second column is its own grouping, not the grid's: told it, the chart keeps it (frozen)
        grid.chart('sankey', x='region', split='desk')
        grid.chart('radar', x='desk', y=[('notional', 'sum'), ('pnl', 'sum'), ('notional', 'max')])
        grid.chart('gauge', options={'target': 1000})
        grid.chart('sunburst')
        grid.chart('matrix', y=[('notional', 'sum'), ('pnl', 'sum')])
        marks = [v['spec']['mark'] for v in page.to_dict()['views'] if v['kind'] == 'chart']
        self.assertEqual(marks[-7:], ['waterfall', 'dumbbell', 'sankey', 'radar', 'gauge', 'sunburst', 'matrix'])
        # and what DataCube would refuse to draw, refused at the line that writes it, in its words
        for mark, given, why in [('sankey', {'x': 'region'}, 'needs a second column'),
                                 ('funnel', {'x': 'region', 'split': 'desk'}, 'one series'),
                                 ('dumbbell', {'x': 'region'}, 'two values'),
                                 ('radar', {'x': 'region'}, 'three values'),
                                 ('matrix', {'x': 'region', 'y': [('pnl', 'sum'), ('notional', 'sum')]}, 'has no column across'),
                                 ('matrix', {'y': [('pnl', 'sum')]}, 'two to 5'),
                                 ('gauge', {}, 'needs a target'),
                                 ('gauge', {'x': 'region', 'options': {'target': 1}}, 'one value'),
                                 ('pareto', {'x': 'region', 'options': {'sort': {'by': 'x', 'direction': 'asc'}}}, 'largest'),
                                 ('calendar', {'x': 'region'}, 'runs along dates'),
                                 ('rose', {'x': 'region', 'layers': [{'kind': 'line', 'value': 1}]}, 'no reference lines'),
                                 ('chord', {'x': 'region', 'split': 'desk', 'tooltip': [('pnl', 'sum')]}, 'no further measures')]:
            with self.subTest(mark=mark, given=given), self.assertRaisesRegex(ValueError, why):
                grid.chart(mark, **given)

    def test_a_chart_plots_what_it_is_told_and_a_frozen_one_stays_when_its_grid_goes(self):
        page, grid, chart = self.built()
        # a hand on its split freezes it, as in DataCube's options
        chart.plot('line', split='desk')
        chart.options({'labels': True})
        spec = page.to_dict()['views'][1]['spec']
        self.assertEqual({k: v for k, v in spec.items() if k != 'options'},
                         {'version': 2, 'mark': 'line', 'x': 'region', 'y': [{'column': 'notional', 'fn': 'sum'}], 'split': 'desk',
                          'frozen': True})
        self.assertTrue(spec['options']['labels'])
        # a line with nothing across, as written, is no chart DataCube draws: refused, and the chart kept as it was
        with self.assertRaisesRegex(ValueError, 'Pick a column across'):
            chart.plot(x=None)
        self.assertEqual(page.to_dict()['views'][1]['spec'], spec)
        with self.assertRaisesRegex(ValueError, 'a chart has a title'):
            chart.title = None
        # its grid removed, a frozen chart stays, over its grid's cube (DataCube's detached chart), as a following one
        # -- told nothing of its grouping -- does not
        follows = grid.chart('pie')
        self.assertNotIn('frozen', page.to_dict()['views'][-1]['spec'])
        page.remove(grid)
        self.assertEqual((page.grids, page.charts, chart.grid), ([], [chart], None))
        doc = page.to_dict()
        self.assertEqual([c['id'] for c in doc['cubes']], [grid.id])
        self.assertEqual([v['id'] for v in doc['views']], [chart.id])
        with self.assertRaisesRegex(ValueError, 'no longer on its page'):
            follows.options({'labels': True})
        # and a page so loads again
        self.assertEqual(ll.Page.load(doc, frames={'trades': trades()}).to_dict(), doc)
        # a detached chart is still a chart of its cube: changed, it is made and judged against it
        chart.plot('bar')
        self.assertEqual(page.to_dict()['views'][0]['spec']['mark'], 'bar')

    def test_a_calculated_column_is_checked_by_the_compiler_where_it_is_added(self):
        page, grid, _ = self.built()
        # a dataframe's columns can be empty: arithmetic takes them toOne(), as DataCube's compiler asks
        grid.calculate('margin', '$x.pnl->toOne() / $x.notional->toOne()')
        derived = page.to_dict()['cubes'][0]['cube']['query']['derived']
        self.assertEqual(derived[0]['name'], 'margin')
        self.assertEqual(derived[0]['lambda']['_type'], 'lambda')
        # added as DataCube's column editor adds one: a measure unless told, as the editor starts one (a number summed
        # when grouped, anything else its unique value), declared as told; refused in DataCube's and the compiler's
        # words -- a column it does not have, a multiplicity, a name the grid has already, a kind DataCube has not
        self.assertEqual(derived[0]['kind'], 'measure')
        grid.calculate('loss', '$x.pnl->toOne() < 0', kind='dimension')
        self.assertEqual(page.to_dict()['cubes'][0]['cube']['query']['derived'][-1]['kind'], 'dimension')
        with self.assertRaisesRegex(ValueError, "a column's kind is 'dimension' or 'measure', not 'sum'"):
            grid.calculate('odd', '$x.pnl', kind='sum')
        with self.assertRaisesRegex(ValueError, 'nope'):
            grid.calculate('wrong', '$x.nope * 2')
        with self.assertRaisesRegex(ValueError, 'multiplicity'):
            grid.calculate('nullable', '$x.pnl / $x.notional')
        with self.assertRaisesRegex(ValueError, "'margin' is already a column"):
            grid.calculate('margin', '$x.pnl')
        with self.assertRaisesRegex(ValueError, 'a calculated column needs a name'):
            grid.calculate(' ', '$x.pnl')
        # one reads the calculated columns before it, typed as the grid runs them
        grid.calculate('margin_pct', '$x.margin * 100')
        self.assertEqual([d['name'] for d in page.to_dict()['cubes'][0]['cube']['query']['derived']],
                         ['margin', 'loss', 'margin_pct'])
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
        with self.assertRaisesRegex(ValueError, 'one at least'):
            grid.filter(('desk', 'in', []))
        # an empty group is no filter only at the top (filter([]), below); inside a filter it is refused, never dropped
        with self.assertRaisesRegex(ValueError, "an 'and' needs one condition at least"):
            grid.filter(('or', [('desk', 'equal', 'FX'), ('and', [])]))
        with self.assertRaisesRegex(ValueError, "an 'or' needs one condition at least"):
            grid.filter(('not', ('or', [])))
        with self.assertRaisesRegex(ValueError, "'contains' is not offered on notional, a Float column"):
            grid.filter(('notional', 'contains', '1'))
        with self.assertRaisesRegex(ValueError, 'no overload'):
            grid.filter(('region', 'lessThan', {'relative': 'today'}))
        for wrong in (None, float('nan'), object(), datetime.datetime(2024, 1, 31)):
            with self.assertRaisesRegex(ValueError, 'takes text, a number'):
                grid.filter(('notional', 'greaterThan', wrong))
        with self.assertRaisesRegex(ValueError, "not one of DataCube's aggregates"):
            grid.measure('pnl', 'total')
        with self.assertRaisesRegex(ValueError, 'a measure named'):
            grid.measure('notional', 'average')
        with self.assertRaisesRegex(ValueError, 'is not one this DataCube draws'):
            grid.chart('violin', x='region')
        with self.assertRaisesRegex(ValueError, 'not a column'):
            grid.group('nope')
        # DataCube's one name rule (page-names.ts givenName): a tile's or a page's blank is refused, in its words
        with self.assertRaisesRegex(ValueError, "a tile's name is not blank"):
            grid.chart('bar', title='')
        with self.assertRaisesRegex(ValueError, "a page's name is not blank"):
            page.name = '  '
        with self.assertRaisesRegex(ValueError, 'is a name, not 3'):
            grid.title = 3
        self.assertEqual(page.to_dict(), before, 'nothing refused changed the page')
        # ... a sheet's blank is none of its own (named after its first grid), as the screen's rename makes it; a name
        # is kept trimmed, as the screen keeps one
        blank = page.sheet('  ')
        self.assertIsNone(blank.name)
        blank.name = ' Summary '
        self.assertEqual(blank.name, 'Summary')
        blank.name = ''
        self.assertIsNone(blank.name)
        # what it takes: nested groups and a negation, as the filter editor writes them; a date and a decimal as their
        # exact text, a relative date as DataCube keeps one
        grid.filter(('or', [('region', 'equal', 'EMEA'), ('not', ('desk', 'in', ['FX', 'Rates']))]))
        self.assertEqual(page.to_dict()['cubes'][0]['cube']['query']['filter']['children'][1],
                         {'kind': 'not', 'child': {'kind': 'condition', 'column': 'desk', 'operator': 'in', 'value': ['FX', 'Rates']}})
        grid.filter([('notional', 'greaterThan', Decimal('12.30')), ('region', 'equal', datetime.date(2024, 1, 31)),
                     ('notional', 'lessThan', 10)])
        self.assertEqual([c['value'] for c in page.to_dict()['cubes'][0]['cube']['query']['filter']['children']],
                         ['12.30', '2024-01-31', 10])
        dated = page.grid(pd.DataFrame({'day': [datetime.date(2024, 1, 1)], 'qty': [1]}), name='dated')
        dated.filter(('day', 'lessThan', {'relative': 'today'}))
        self.assertEqual(page.to_dict()['cubes'][1]['cube']['query']['filter']['value'], {'relative': 'today'})

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

    def test_a_name_served_already_is_shared_by_its_own_frame_and_refused_to_another(self):
        page = ll.Page('Two over one')
        df = trades()
        first = page.grid(df, name='trades', rows=['region'])
        second = page.grid(df, name='trades', rows=['desk'])
        self.assertEqual([g.frame for g in page.grids], ['trades', 'trades'])
        served = datacube._session.frames['trades']
        # another frame under the name: refused (grid.update replaces a frame), the name serving what it served
        for other, mode in ((trades(), 'live'), (df, 'snapped')):
            with self.assertRaisesRegex(ValueError, r'grid\.update\(frame\) replaces a frame'):
                page.grid(other, name='trades', mode=mode)
        self.assertIs(datacube._session.frames['trades'], served)
        self.assertEqual(page.grids, [first, second])
        # and a refused grid over its own frame takes nothing away from the frame the others show
        with self.assertRaises(ValueError):
            page.grid(df, name='trades', rows=['nope'])
        self.assertIs(datacube._session.frames['trades'], served)

    def test_no_handle_meets_a_new_tile_under_its_old_ones_id(self):
        # (and a handle gone is refused wherever it is given: here, to a layout)
        page, grid, chart = self.built()
        later = grid.chart('line', x='region', title='later')
        page.remove(later)
        again = grid.chart('pie', x='region', title='again')
        self.assertNotEqual(again.id, later.id, 'an id the page held is not given out again')
        with self.assertRaisesRegex(ValueError, 'no longer on its page'):
            later.title = 'still the old one?'
        with self.assertRaisesRegex(ValueError, 'no longer on its page'):
            page.sheets[0].layout([grid, chart, again, later])
        page.remove(grid)
        other = page.grid(desks(), name='desks')
        self.assertNotEqual(other.id, grid.id)
        with self.assertRaisesRegex(ValueError, 'no longer on its page'):
            grid.group('desk')
        # a tile stacked with itself is no stack; one taken off, or not a tile, is refused before anything is asked
        with self.assertRaisesRegex(ValueError, 'two tiles or more'):
            page.stack(other, other)
        with self.assertRaisesRegex(ValueError, 'no longer on its page'):
            page.stack(other, later)
        with self.assertRaisesRegex(ValueError, 'a stack is of tiles'):
            page.stack(other, 'chart-1')

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
        chart_view['spec']['options'] = {'orientation': 'vertical', 'stack': 'none', 'sort': {'by': 'x', 'direction': 'asc'},
                                         'limit': 25, 'labels': False, 'legend': 'bottom'}
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
        # a live chart written in the UI, regrouped from Python: across, the grid's first row group, in that column's
        # order (a category's: the largest first); what else the UI wrote stays
        self.assertEqual(after_chart['spec']['x'], after['cubes'][0]['cube']['query']['rows'][0])
        self.assertEqual(after_chart['spec']['options'], {**chart_view['spec']['options'], 'labels': True,
                                                          'sort': {'by': 'y', 'direction': 'desc'}})
        self.assertEqual(after['sheets'], doc['sheets'])

    def test_a_kpi_is_a_chart_of_the_grid_its_number_and_its_trend(self):
        page, grid, _ = self.built()
        # with no trend (x None: a KPI keeps the column across of the chart it was made from, as DataCube's options do)
        kpi = grid.chart('kpi', x=None, options={'target': 300, 'better': 'lower'}, title='Notional')
        spec = page.to_dict()['views'][-1]['spec']
        self.assertEqual({k: v for k, v in spec.items() if k != 'options'},
                         {'version': 2, 'mark': 'kpi', 'y': [{'column': 'notional', 'fn': 'sum'}]})
        self.assertEqual((spec['options']['target'], spec['options']['better']), (300, 'lower'))
        kpi.plot(x='region')
        self.assertEqual(page.to_dict()['views'][-1]['spec']['x'], 'region')

    def test_told_none_a_chart_has_no_split_as_it_has_no_trend(self):
        page = ll.Page('two rows')
        grid = page.grid(trades(), name='trades', rows=['region', 'desk'], measures={'notional': 'sum'})
        # DataCube's own new chart of a grid grouped twice: its second row group the split
        grid.chart('bar')
        self.assertEqual(page.to_dict()['views'][-1]['spec']['split'], 'desk')
        # told none, one series: a hand taking the split away, so the chart keeps it so (frozen)
        grid.chart('bar', split=None)
        spec = page.to_dict()['views'][-1]['spec']
        self.assertNotIn('split', spec)
        self.assertTrue(spec['frozen'])
        # and told to follow the grid as well, refused: the grid would split it again
        with self.assertRaisesRegex(ValueError, "follows its grid takes 'desk' as its split, not none"):
            grid.chart('bar', split=None, frozen=False)

    def test_a_document_is_read_as_datacube_reads_it_and_refused_in_its_words(self):
        # DataCube's own reader (page-document.ts readPage, headless): Python keeps no copy of its rules
        page, _, _ = self.built()
        doc = page.to_dict()
        unplaced = copy.deepcopy(doc)
        unplaced['sheets'][0]['layout']['bands'] = []
        with self.assertRaisesRegex(ValueError, 'no sheet has a place for grid-1'):
            ll.Page.load(unplaced, frames={'trades': trades()})
        with self.assertRaisesRegex(ValueError, r'written by a newer version \(99 > 4\)'):
            ll.Page.load({**doc, 'version': 99}, frames={'trades': trades()})
        twice = copy.deepcopy(doc)
        twice['sheets'].append({'id': 'sheet-1', 'layout': {'kind': 'bands', 'fit': True, 'bands': []}})
        with self.assertRaisesRegex(ValueError, 'two sheets are sheet-1'):
            ll.Page.load(twice, frames={'trades': trades()})
        cubes = copy.deepcopy(doc)
        cubes['cubes'].append(copy.deepcopy(cubes['cubes'][0]))
        with self.assertRaisesRegex(ValueError, 'two cubes share an id'):
            ll.Page.load(cubes, frames={'trades': trades()})
        listed = copy.deepcopy(doc)
        listed['views'][0]['cube'] = ['grid-1']
        with self.assertRaisesRegex(ValueError, 'shows no cube of this page'):
            ll.Page.load(listed, frames={'trades': trades()})
        # an older page DataCube reads is read, brought to DataCube's own version
        self.assertEqual(ll.Page.load({**doc, 'version': 3}, frames={'trades': trades()}).to_dict()['version'], 4)
        # and what Python adds: a page in Python is over frames
        elsewhere = copy.deepcopy(doc)
        elsewhere['cubes'][0]['cube']['source'] = {'_type': 'remoteFile', 'name': 'trades.parquet',
                                                   'url': 'https://example.com/trades.parquet',
                                                   'columns': doc['cubes'][0]['cube']['source']['columns']}
        with self.assertRaisesRegex(ValueError, 'reads no frame'):
            ll.Page.load(elsewhere, frames={'trades': trades()})


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
        """The open page saying what it is now, as DataCube's app posts it (src/app/datacube-app.ts): the version it
        opened, its document."""
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
        # DataCube opens no page without a grid: refused in its words, then Python's call that makes one, and never
        # written either
        with self.assertRaisesRegex(ValueError, r'^cannot read saved page: it has no cube -- in Python, page\.grid\(frame\) '
                                                r'adds one$'):
            ll.Page('Empty').to_json()
        with self.assertRaisesRegex(ValueError, 'it has no cube'):
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

    def test_a_tile_closed_in_datacube_and_its_id_given_again_there_is_not_its_old_handles(self):
        page, grid, chart = self.built()
        ll.show(page, browser=False)
        # DataCube closed the chart, and later made another under the same id (a page opened afresh counts from the
        # ids it has)
        open_now = page.to_dict()
        open_now['views'] = [v for v in open_now['views'] if v['id'] != chart.id]
        open_now['sheets'][0]['layout']['bands'][0]['node'] = {'tile': grid.id}
        self.assertEqual(self.said(page, open_now), 204)
        page.read()
        again = copy.deepcopy(open_now)
        again['views'].append({**page.to_dict()['views'][0], 'id': chart.id, 'kind': 'chart', 'title': 'new there',
                               'spec': {'version': 1, 'mark': 'pie', 'y': [{'column': 'notional', 'fn': 'sum'}], 'options': {}}})
        again['sheets'][0]['layout']['bands'].append({'height': 0.5, 'node': {'tile': chart.id}})
        self.assertEqual(self.said(page, again), 204)
        page.read()
        with self.assertRaisesRegex(ValueError, 'no longer on its page'):
            chart.title = 'the old chart'
        self.assertEqual(page.charts[0].title, 'new there')

    def test_a_page_opening_on_it_opens_it_as_it_is_open_now(self):
        page, _, _ = self.built()
        ll.show(page, browser=False)
        open_now = page.to_dict()
        open_now['sheets'][0]['name'] = 'Renamed in DataCube'
        self.assertEqual(self.said(page, open_now), 204)
        # another tab, or the first reloaded: the page as the open one said it is, read into Python or not
        self.assertEqual(self.get(f'/page.json?page={page._key}')[1]['page']['sheets'][0]['name'], 'Renamed in DataCube')
        page.read()
        ll.show(page, browser=False)
        self.assertEqual(self.get(f'/page.json?page={page._key}')[1], {'version': 1, 'page': page.to_dict()})

    def test_shown_again_and_closed_and_shown_again(self):
        page, grid, _ = self.built()
        first = ll.show(page, browser=False)
        # shown again in another tab: the page as it is, the tab open on it not opened again
        ll.show(page, browser=False)
        self.assertEqual(self.get(f'/version.json?page={page._key}')[1]['version'], 1)
        # closed everywhere and shown again: its version goes on, so the first showing's tab says nothing taken
        for view in list(datacube._session.pages):
            view.close()
        del first
        ll.show(page, browser=False)
        self.assertEqual(self.get(f'/version.json?page={page._key}')[1]['version'], 2)
        self.assertEqual(self.said(page, page.to_dict(), version=1), 409)

    def test_a_frozen_chart_left_without_its_grid_follows_its_frame(self):
        page, grid, chart = self.built()
        chart.plot(frozen=True)
        page.grid(desks(), name='desks')
        page.remove(grid)
        ll.show(page, browser=False)
        before = self.get(f'/version.json?page={page._key}')[1]['frames']['trades']
        datacube._session.register(trades().iloc[:1], 'trades', 'live')
        self.assertEqual(self.get(f'/version.json?page={page._key}')[1]['frames']['trades'], before + 1)

    def test_what_the_open_page_says_of_a_page_since_replaced_is_not_read(self):
        page, _, _ = self.built()
        ll.show(page, browser=False)
        stale = page.to_dict()
        stale['sheets'][0]['name'] = 'Of the page before'
        page.sheet('More')
        self.assertEqual(self.said(page, stale, version=1), 409)
        self.assertEqual([s.name for s in page.read().sheets], [None, 'More'])
