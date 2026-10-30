"""ll.Page (legend_lite.page; docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md): a DataCube page built in Python -- its
grids over frames, their charts, sheets, layouts and stacks -- written as DataCube's own page document; a calculated
column checked by the compiler at the line that adds it; a filter, an aggregate or a chart outside DataCube's words
refused there; tiles placed and taken off as DataCube's layout places them; saved and loaded again as it was, what it
does not model kept; shown in a tab and live from then on (a change served again, a frame updated moving its version),
and read back into the same page as the open page says it is."""

import copy
import datetime
import gc
import hashlib
import json
import os
import re
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
import warnings
from decimal import Decimal
from pathlib import Path
from unittest import mock

import pandas as pd

import legend_lite as ll
from legend_lite import _headless, _home, _json, _pool, datacube
from legend_lite.keeper import page_hash
from legend_lite import page as page_module
from legend_lite._types import plain_type


def groups_of(view, measure):
    """A grouped view's level-1 groups: label -> the measure's value."""
    labels = [row[[c['name'] for c in view.columns].index('__tree')] for row in view.rows]
    values = [row[[c['name'] for c in view.columns].index(measure)] for row in view.rows]
    return {labels[i]: values[i] for i, t in enumerate(view.tree) if t['level'] == 1}


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


def page_edit(**command):
    """An open page's edit of the page: a page command, as it writes one."""
    return {'page': command}


def grid_edit(grid, **command):
    """An open page's edit of one of its grids: the grid's id and the grid command, as it writes one."""
    return {'grid': grid.id, 'command': command}


class Paged(unittest.TestCase):
    """Pages on a session of their own, with a site of its own and no browser."""

    def setUp(self):
        self._site = os.environ.get('LEGEND_LITE_SITE')
        os.environ['LEGEND_LITE_SITE'] = tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))
        datacube._session = datacube._Session()

    def tearDown(self):
        for view in list(datacube._session.pages):
            view.close()
        # its DataCube engine (headless) closed with it: a session dropped open leaves that engine's folder to the garbage
        # collector, which warns about it under whichever later test it reaches (CorpusPage counts every warning)
        if datacube._session._headless is not None:
            datacube._session._headless.close()
        if self._site is None:
            os.environ.pop('LEGEND_LITE_SITE', None)
        else:
            os.environ['LEGEND_LITE_SITE'] = self._site

    def writes(self, page):
        """Every write ``page``'s keeper settles from here on, as the edits it was -- Python's own and an open page's
        (keeper.py settle: the host applies the commands, docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4) -- as ``since(v)``:
        the page's version now, and the edits of each write that moved it past ``v``, in order."""
        keeper = datacube._session.keeper
        written = []
        settle = keeper.settle

        def settling(key, base, edits, *rest, **named):
            settled = settle(key, base, edits, *rest, **named)
            written.append((settled.version, copy.deepcopy(edits)))
            return settled
        keeper.settle = settling
        return lambda at: {'version': keeper.current(page._key)[0],
                           'commands': [e for version, edits in written if version > at for e in edits]}

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
    def test_a_grid_in_one_call_is_the_commands_datacube_orders_and_reads(self):
        # (Z8) its parts as the commands that make them, in a person's order (grid-commands.ts setUpCommands); a part not
        # read as DataCube reads one refused in its words, the page as it was
        page = ll.Page('Set up')
        page.grid(trades(), name='set_trades', rows=['region'], columns=['desk'], measures=[('notional', 'sum')],
                  sort=[('region', 'asc')], dimensions=['pnl'])
        # a measure by the steps a person makes it by (Column Properties' Aggregation): no command of its own (F72)
        self.assertEqual([c['kind'] for c in page._commands], ['column', 'group', 'pivot', 'column', 'sorts'])
        with self.assertRaisesRegex(ValueError, r'^a measure is a pair: its column and its aggregate, not '
                                                r'\["notional","sum","pnl"\]$'):
            page.grid(trades(), name='odd_trades', measures=[('notional', 'sum', 'pnl')])
        self.assertEqual(len(page.grids), 1)

    def test_a_page_is_datacubes_own_document_one_cube_per_grid_over_its_frame(self):
        page, grid, chart = self.built()
        doc = page.to_dict()
        self.assertEqual((doc['kind'], doc['version'], doc['name']), ('datacube.page', 6, 'Q3 review'))
        cube = doc['cubes'][0]
        self.assertEqual(cube['id'], grid.id)
        self.assertEqual(cube['cube']['source']['_type'], 'frame')
        self.assertEqual(cube['cube']['source']['name'], 'trades')
        # the columns as the compiler types the frame
        self.assertEqual([(c['name'], c['type']) for c in cube['cube']['query']['columns']],
                         [('region', 'String'), ('desk', 'String'), ('notional', 'Float'), ('pnl', 'Float')])
        query = cube['cube']['query']
        self.assertEqual(query['rows'], ['region'])
        # a measure is its column's Aggregation, in Column Properties, as the screen sets it: no measures list
        self.assertNotIn('measures', query)
        self.assertEqual(cube['cube']['configuration']['columns']['notional'], {'aggregateFn': 'sum'})
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
        # one sheet: the grid in a band of its own, the chart in a band under it, sharing the screen the grid's band had
        # alone, as DataCube places them (bands.ts add; the user, 2026-10-23: a new chart goes below, not beside)
        self.assertEqual(doc['sheets'], [{'id': 'sheet-1', 'layout': {'kind': 'bands', 'fit': True, 'bands': [
            {'height': 0.5, 'node': {'tile': grid.id}}, {'height': 0.5, 'node': {'tile': chart.id}}]}}])

    def test_the_document_is_a_copy_and_its_numbers_exact(self):
        page, grid, _ = self.built()
        doc = page.to_dict()
        doc['cubes'][0]['cube']['query']['rows'].append('desk')
        doc['sheets'][0]['layout']['bands'].clear()
        self.assertEqual(page.to_dict()['cubes'][0]['cube']['query']['rows'], ['region'], 'what is done to it is not done to the page')
        self.assertEqual(len(page.to_dict()['sheets'][0]['layout']['bands']), 2)
        # a calculated column's decimal stays its digits, in the document and the file
        grid.calculate('scaled', '$x.notional->toOne() * 1.50D')
        lambda_ = page.to_dict()['cubes'][0]['cube']['query']['derived'][0]['lambda']
        self.assertIn('1.50', [str(v) for v in numbers(lambda_)])
        self.assertIn('1.50', page.to_json())
        path = os.path.join(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR')), 'q3.page.json')
        page.save(path)
        self.assertIn('1.50', open(path, encoding='utf-8').read())
        # as DataCube's Export > Page File writes it (headless pageFile, B5): its text, one line, byte for byte (no newline
        # of Python's own: the S7 audit's Y-4)
        self.assertEqual(open(path, encoding='utf-8').read().count('\n'), 0)
        # classes kept in a project: refused in DataCube's words, as Export > Page File refuses it
        modelled = ll.Page.load({**page.to_dict(), 'model': {'project': 'demo:trades', 'workspace': 'datacube',
                                                             'package': 'demo', 'tables': {}}}, frames={'trades': trades()})
        with self.assertRaisesRegex(ValueError, r"^This page's classes are in demo:trades: a page file cannot carry them "
                                                r'yet -- save the page where its classes are kept\.$'):
            modelled.save(os.path.join(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR')), 'modelled.page.json'))
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
        # saved again: over the copy it was saved as, its new name taken -- by the page too, its content so named, as
        # DataCube's Save renames it (B4: the record by DataCube's rule, savedRecord)
        self.assertEqual(page.save(name='  Q3, again '), first)
        again = json.loads(engine.answer('GET', f'/api/pure/v1/query/dataCube/{first}', '', None).body)
        self.assertEqual(again['name'], 'Q3, again')
        self.assertEqual(again['content']['name'], 'Q3, again')
        self.assertEqual(page.name, 'Q3, again')
        self.assertEqual(page._page_commands[-1], {'kind': 'renamePage', 'name': 'Q3, again'})
        # deleted meanwhile (in DataCube's Open…): refused, as DataCube's Save is refused, never made again under its id
        # (the S7 audit's Y-8: Python made it again)
        self.assertEqual(engine.answer('DELETE', f'/api/pure/v1/query/dataCube/{first}', '', None).status, 204)
        with self.assertRaisesRegex(ValueError, f"the page was not saved: Can't find query with ID '{first}'"):
            page.save()
        # another page is another saved page; one with nothing on it is not saved, as DataCube's Save refuses it (B14)
        other = ll.Page('Other')
        with self.assertRaisesRegex(ValueError, r'^There is nothing on this page to save: add a data source first\.$'):
            other.save()
        other.grid(trades(), name='other_trades')
        self.assertNotEqual(other.save(), first)
        with self.assertRaises(ValueError):
            page.save(name=' ')

    def test_a_page_loaded_from_another_pages_document_is_a_page_of_its_own(self):
        # (the S7 confirmation audit's Y2-3) the document carries the record the other page was saved as; loaded, a page
        # holds none -- as the tab's Open... of a file or text -- so its save is a new saved page, the other's as it was
        from legend_lite.engine import Engine
        page, _, _ = self.built()
        first = page.save()
        other = ll.Page.load(page.to_dict(), frames={'trades': trades()})
        self.assertNotIn('record', other.to_dict())
        other.name = 'Q4'
        second = other.save()
        self.assertNotEqual(second, first)
        engine = Engine(ll.Frames())
        self.assertEqual(json.loads(engine.answer('GET', f'/api/pure/v1/query/dataCube/{first}', '', None).body)['name'],
                         'Q3 review', "the other page's saved page as it was")
        # opened by its id, it holds that record, by its command (savedAs): saved again, over it
        opened = ll.Page.open(second, frames={'trades': trades()})
        self.assertEqual(opened.to_dict()['record'], second)
        self.assertEqual(opened.save(), second)

    def test_a_page_nobody_named_is_unnamed_and_saved_under_its_first_sheets_name(self):
        # one rule (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D11, B3): unnamed, as the tab's New Page -- no name in the
        # page -- and saved, its saver naming nothing, under the name DataCube's Save suggests: its first sheet's, its
        # grid's report title (page-document.ts suggestedName), the page then so named
        from legend_lite.engine import Engine
        page = ll.Page()
        self.assertEqual(page.name, '')
        self.assertEqual(page.to_dict()['name'], '')
        page.grid(trades(), name='unnamed_trades')
        saved = page.save()
        engine = Engine(ll.Frames())
        record = json.loads(engine.answer('GET', f'/api/pure/v1/query/dataCube/{saved}', '', None).body)
        self.assertEqual(record['name'], 'unnamed_trades')
        self.assertEqual(page.name, 'unnamed_trades')
        # a name given is still judged by DataCube's one rule
        with self.assertRaisesRegex(ValueError, "a page's name"):
            ll.Page('  ')
        # written as a file unnamed: named as the tab's Export > Page File names it (pageFile: its first sheet's), the file
        # as DataCube writes it, byte for byte (the S7 audit's Y-4: '' and a newline of Python's own)
        unnamed = ll.Page()
        unnamed.grid(trades(), name='unnamed_file_trades')
        path = os.path.join(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR')), 'unnamed.page.json')
        unnamed.save(path)
        written = Path(path).read_text('utf-8')
        self.assertEqual(json.loads(written)['name'], 'unnamed_file_trades')
        self.assertFalse(written.endswith('\n'))
        again = ll.Page.load(path, frames={'unnamed_file_trades': trades()})
        self.assertEqual(again.name, 'unnamed_file_trades')
        self.assertEqual(again.grids[0]._query, unnamed.grids[0]._query)
        # a Cube File (a cube saved alone) loads as the tab's Open... opens one: a page of its one grid (the S7 audit's Y-5)
        alone = ll.Page.load(unnamed.to_dict()['cubes'][0]['cube'], frames={'unnamed_file_trades': trades()})
        self.assertEqual(len(alone.grids), 1)
        self.assertEqual(alone.grids[0]._query, unnamed.grids[0]._query)

    def test_tiles_are_placed_and_taken_off_as_datacube_places_them(self):
        page, grid, chart = self.built()
        sheet = page.sheets[0]
        # under its grid, beside its grid's other charts, four to a band at most: a chart beyond goes into a band of its
        # own under its grid's
        more = [grid.chart('line', title=f'line {i}') for i in range(4)]
        self.assertEqual([len(b['node']['parts']) if 'split' in b['node'] else 1 for b in self.bands(sheet)], [1, 1, 4])
        # a grid goes into a band of its own at the bottom
        other = page.grid(desks(), name='desks')
        self.assertEqual(self.bands(sheet)[-1], {'height': 0.5, 'node': {'tile': other.id}})
        # taken off: its neighbours close over its place, a band it emptied gone
        page.remove(more[3])
        self.assertEqual([len(b['node']['parts']) if 'split' in b['node'] else 1 for b in self.bands(sheet)], [1, 4, 1])
        page.remove(more[2])
        self.assertEqual(self.bands(sheet)[1]['node']['parts'][2], {'node': {'tile': more[1].id}, 'size': 1 / 3})
        # a grid removed takes the charts that follow it
        page.remove(grid)
        self.assertEqual((page.grids, page.charts), ([other], []))
        self.assertEqual(self.bands(sheet), [{'height': 0.5, 'node': {'tile': other.id}}])
        for gone in (grid, chart):
            with self.assertRaisesRegex(ValueError, 'no longer on its page'):
                gone.title = 'again'

    def test_each_page_edit_is_the_command_datacubes_screen_builds_and_its_ids_are_datacubes(self):
        # (docs/DATACUBE_COMMANDS_PAGE_2026_10_18.md) each page edit is a page command applied by DataCube (headless
        # pageApply): the page records each as applied, the id DataCube gave a new tile or sheet in it
        page, grid, chart = self.built()
        done = len(page._page_commands)
        again = grid.chart('line', title='again')
        extra = page.sheet('Extra')
        chart.title = 'Renamed'
        made = page._page_commands[done:]
        self.assertEqual([c['kind'] for c in made], ['addChart', 'addSheet', 'renameTile'])
        self.assertEqual((made[0]['id'], made[0]['near'], made[1]['id']), (again.id, grid.id, extra.id))
        self.assertEqual(made[2], {'kind': 'renameTile', 'tile': chart.id, 'title': 'Renamed'})
        # a tile moved onto its own sheet stays where it is, as the screen's Move to Sheet does (it moved it to the bottom)
        before = page.to_dict()['sheets']
        page.sheets[0].add(grid)
        self.assertEqual(page.to_dict()['sheets'], before)
        # a chart taken off and a new one made: never under the id the old one had
        page.remove(again)
        self.assertNotEqual(grid.chart('pie', x='region', title='later').id, again.id)

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

    def test_a_page_saved_before_a_grids_aggregates_were_its_columns_is_refused_in_datacubes_words(self):
        # no older page is read (the user, 2026-10-17): a version 1 cube -- every cube saved before a grid's aggregates
        # became its columns' Aggregation -- is refused by its version, and a measures list in a cube of this one by
        # name; never converted, never opened showing other numbers than it showed
        saved = ll.Page('counts')
        saved.grid(trades(), name='old_counts', rows=['region'])
        old = saved.to_dict()
        old['cubes'][0]['cube']['version'] = 1
        with self.assertRaisesRegex(ValueError, r'saved by an older version \(1 < 2\)'):
            ll.Page.load(old, frames={'old_counts': trades()})
        listed = saved.to_dict()
        listed['cubes'][0]['cube']['query']['measures'] = [{'name': 'notional', 'column': 'notional', 'fn': 'average'}]
        with self.assertRaisesRegex(ValueError, "'query.measures': a grid carries no measures list"):
            ll.Page.load(listed, frames={'old_counts': trades()})
        # a calculated column whose lambda the protocol cannot read: refused as the page it is in (a ValueError), never
        # a JavaScript exception
        saved.grids[0].calculate('twice', '$x.notional->toOne() * 2')
        broken = saved.to_dict()
        broken['cubes'][0]['cube']['query']['derived'][0]['lambda']['body'] = 'oops'
        with self.assertRaisesRegex(ValueError, 'calculated column "twice"\'s lambda'):
            ll.Page.load(broken, frames={'old_counts': trades()})

    def test_a_live_frame_changed_in_place_is_the_cube_a_filter_a_column_and_a_chart_are_checked_against(self):
        # a Live frame's column deleted in place: the next filter takes the cube DataCube opens over the frame now (its
        # calculated column that read the gone column left out, said), so a column of that name can be made again, once
        frame = trades()
        page = ll.Page('in place')
        grid = page.grid(frame, name='in_place', rows=['region'])
        grid.calculate('twice', '$x.notional->toOne() * 2')
        del frame['notional']
        with self.assertWarnsRegex(UserWarning, r'in_place: left out calculated column twice \(it uses notional\)'):
            grid.filter(('desk', 'equal', 'FX'))
        grid.calculate('twice', '$x.pnl->toOne() * 2')
        self.assertEqual([d['name'] for d in page.to_dict()['cubes'][0]['cube']['query']['derived']], ['twice'])

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
        # a chart goes under its grid, as DataCube adds one: a band of its own there, the band under holding a grid
        added = other.chart('pie', x='desk', y=[('desk', 'count')], title='Desks')
        self.assertEqual([b['node'] for b in self.bands(sheet)[1:]], [{'tile': added.id}, {'tile': heads.id}])
        self.assertEqual(self.bands(sheet)[0]['node']['parts'][1], {'node': {'tile': other.id}, 'size': 0.3})
        with self.assertRaisesRegex(ValueError, 'cannot be laid out: band 0: shares summing to 1.4'):
            sheet.layout_from({'kind': 'bands', 'fit': True, 'bands': [{'height': 1, 'node': {'split': 'row', 'parts': [
                {'node': {'tile': grid.id}, 'size': 0.7}, {'node': {'tile': other.id}, 'size': 0.7}]}},
                {'height': 1, 'node': {'split': 'row', 'parts': [{'node': {'tile': added.id}, 'size': 0.5},
                                                                 {'node': {'tile': heads.id}, 'size': 0.5}]}}]})
        # a layout leaving a tile of the sheet out, refused by the layout command (page-commands.ts): every tile once
        with self.assertRaisesRegex(ValueError, "a sheet's layout holds its tiles, each once"):
            sheet.layout_from({'kind': 'bands', 'fit': True, 'bands': [{'height': 1, 'node': {'tile': grid.id}}]})
        # its shape read as a saved page's sheet is, refused in the page reader's words -- never kept to be refused only
        # when the page is saved, nor a JavaScript error (the phase 1 audit's 1)
        with self.assertRaisesRegex(ValueError, "the layout's fit is not true or false"):
            sheet.layout_from({'kind': 'bands', 'fit': 'yes', 'bands': [{'height': 1, 'node': {'tile': grid.id}}]})
        with self.assertRaisesRegex(ValueError, "the layout's bands are not a list"):
            sheet.layout_from({'kind': 'bands', 'fit': True})

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
        # (a new chart is put under its grid's band first, as DataCube adds one)
        third = other.chart('pie', x='desk', y=[('desk', 'count')], title='Desks')
        self.assertEqual(self.bands(summary)[1]['node'], {'tile': third.id})
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
        # a live chart follows its grid -- regrouped the way the grid is (DataCube's followCube), each value on the grid's
        # Aggregation of its column -- so a value told another aggregate and told to follow is refused: the grid's
        # Aggregation would replace it; values on the grid's Aggregations follow it, two of them too
        with self.assertRaisesRegex(ValueError, "follows its grid takes sum of 'notional', sum of 'pnl' as its values, "
                                                "not max of 'notional', sum of 'pnl'"):
            grid.chart('bar', x='region', y=[('notional', 'max'), ('pnl', 'sum')], frozen=False)
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
        compiler's types, as DataCube judges them. Every case is asked, none skipped."""
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
        # a time of day is lowered as SQL's TIME (main's 5f7d6ed, 2026-10-17): calculated like the Date
        grid.calculate(timed, '%10:30:00')
        cube = grid._view['cube']
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

    def test_batch_4_a_box_plot_plots_one_columns_five_figures_as_datacube_draws_it(self):
        page, grid, _ = self.built()
        # told the kind alone, the five figures of what DataCube's own new chart would plot: the grid's measure
        grid.chart('boxplot', x='desk')
        spec = [v['spec'] for v in page.to_dict()['views'] if v['kind'] == 'chart'][-1]
        five = ('min', 'percentile25', 'median', 'percentile75', 'max')
        self.assertEqual((spec['mark'], [(m['column'], m['fn']) for m in spec['y']]),
                         ('boxplot', [('notional', fn) for fn in five]))
        grid.chart('boxplot', x='region', y=[('pnl', fn) for fn in five])
        # and what DataCube would refuse to draw, refused in its words
        for given, why in [({'y': [('pnl', 'sum')]}, 'five figures of one column'),
                           ({'y': [('desk', fn) for fn in five]}, 'not a number'),
                           ({'split': 'region'}, 'one series')]:
            with self.subTest(given=given), self.assertRaisesRegex(ValueError, why):
                grid.chart('boxplot', x='desk', **given)

    def test_batch_5_maps_are_written_and_refused_as_datacube_draws_them(self):
        page = ll.Page('Maps')
        sales = pd.DataFrame({'country': ['USA', 'FRA', 'GBR'], 'city': ['New York', 'Paris', 'London'],
                              'lat': [40.71, 48.86, 51.51], 'lon': [-74.01, 2.35, -0.13], 'revenue': [500.0, 300.0, 200.0]})
        grid = page.grid(sales, name='sales_geo', rows=['country'], measures={'revenue': 'sum'})
        # a map of each country by its code; a bubble at each city, where its latitude and longitude say
        grid.chart('map', x='country')
        grid.chart('bubblemap', x='city', at={'lat': 'lat', 'lon': 'lon'})
        specs = [v['spec'] for v in page.to_dict()['views'] if v['kind'] == 'chart']
        self.assertEqual([s['mark'] for s in specs], ['map', 'bubblemap'])
        self.assertEqual(specs[1]['at'], {'lat': 'lat', 'lon': 'lon'})
        # a map asks for the world's countries, not DataCube's first fifty
        self.assertEqual(specs[0]['options']['limit'], 250)
        for mark, given, why in [('map', {'x': 'revenue'}, 'not text'),
                                 ('bubblemap', {'x': 'city'}, 'latitude'),
                                 ('bubblemap', {'x': 'city', 'at': {'lat': 'city', 'lon': 'lon'}}, 'not a number'),
                                 ('bubblemap', {'x': 'city', 'at': {'lat': 'lat', 'height': 'lon'}}, 'its places'),
                                 ('bar', {'x': 'city', 'at': {'lat': 'lat', 'lon': 'lon'}}, 'places nothing')]:
            with self.subTest(mark=mark, given=given), self.assertRaisesRegex(ValueError, why):
                grid.chart(mark, **given)
        # a chart made a bubble map by plot, told its places; and its places changed (the phase 6 audit)
        bar = grid.chart('bar', x='city')
        bar.plot('bubblemap', at={'lat': 'lat', 'lon': 'lon'})
        bubbles = [v['spec'] for v in page.to_dict()['views'] if v['kind'] == 'chart'][-1]
        self.assertEqual((bubbles['mark'], bubbles['at']), ('bubblemap', {'lat': 'lat', 'lon': 'lon'}))
        with self.assertRaisesRegex(ValueError, 'longitude'):
            bar.plot(at={'lat': 'lat'})

    def test_batch_3_leftovers_are_written_and_refused_as_datacube_draws_them(self):
        page = ll.Page('Leftovers')
        tasks = pd.DataFrame({'desk': ['FX', 'FX', 'Rates'], 'region': ['EMEA', 'AMER', 'EMEA'],
                              'day': pd.to_datetime(['2024-01-02', '2024-03-04', '2024-02-01']).date,
                              'pnl': [1.5, 2.0, 0.5]})
        grid = page.grid(tasks, name='tasks', rows=['desk'], measures={'pnl': 'sum'})
        grid.chart('waffle', x='desk')
        # a Marimekko lays its columns out in their order across: DataCube's new chart of one takes that order
        grid.chart('marimekko', x='region', split='desk')
        grid.chart('gantt', x='desk', y=[('day', 'min'), ('day', 'max')])
        specs = [v['spec'] for v in page.to_dict()['views'] if v['kind'] == 'chart']
        self.assertEqual([s['mark'] for s in specs], ['waffle', 'marimekko', 'gantt'])
        self.assertEqual(specs[1]['options']['sort'], {'by': 'x', 'direction': 'asc'})
        for mark, given, why in [('marimekko', {'x': 'region'}, 'made of'),
                                 ('marimekko', {'x': 'region', 'split': 'desk', 'options': {'sort': {'by': 'y', 'direction': 'desc'}}}, 'order by category'),
                                 ('waffle', {'x': 'desk', 'y': [('pnl', 'sum'), ('pnl', 'max')]}, 'one value'),
                                 ('gantt', {'x': 'desk'}, 'min or max of a date column'),
                                 ('gantt', {'x': 'desk', 'y': [('day', 'min'), ('pnl', 'max')]}, 'min or max of a date column')]:
            with self.subTest(mark=mark, given=given), self.assertRaisesRegex(ValueError, why):
                grid.chart(mark, **given)

    def test_a_charts_state_dynamic_pinned_or_frozen_as_its_pill_puts_it(self):
        # the user, 2026-10-23: "frozen charts don't freeze filters" -- pinned keeps the grouping, frozen the filter too
        page, grid, chart = self.built()
        self.assertEqual(chart.state, 'dynamic')
        grid.filter(('region', 'equal', 'EMEA'))
        emea = page.to_dict()['cubes'][0]['cube']['query']['filter']
        chart.state = 'frozen'
        self.assertEqual(chart.state, 'frozen')
        spec = page.to_dict()['views'][1]['spec']
        self.assertEqual((spec['frozen'], spec['kept']), (True, {'filter': emea}), "the grid's filter as it was, kept")
        # the grid filtered otherwise since: the frozen chart keeps what it kept
        grid.filter(('region', 'equal', 'AMER'))
        self.assertEqual(page.to_dict()['views'][1]['spec']['kept'], {'filter': emea})
        chart.state = 'pinned'
        self.assertEqual(chart.state, 'pinned')
        self.assertNotIn('kept', page.to_dict()['views'][1]['spec'])
        chart.state = 'dynamic'
        self.assertNotIn('frozen', page.to_dict()['views'][1]['spec'])
        with self.assertRaisesRegex(ValueError, "a chart's state is one of dynamic, pinned, frozen, not \"stuck\""):
            chart.state = 'stuck'
        self.assertEqual(chart.state, 'dynamic')

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
        self.assertEqual((page.grids, page.charts), ([], [chart]))
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

    def test_a_text_tile_titles_and_notes_a_page(self):
        # Power BI's text box: a page's title or a note, in DataCube's small Markdown (page-text.ts), by the commands its
        # menus send (addText, setText), saved and reopened as it is; refused in DataCube's words
        page, grid, _ = self.built()
        note = page.text('# Q3 sales\n\nBy **desk**.', title='Notes')
        self.assertEqual((note.id, note.text, note.title), ('text-1', '# Q3 sales\n\nBy **desk**.', 'Notes'))
        self.assertIn(note, page.texts)
        self.assertIs(note.sheet, grid.sheet)
        note.text = 'Updated'
        self.assertEqual(next(v for v in page.to_dict()['views'] if v['id'] == 'text-1')['text'], 'Updated')
        with self.assertRaisesRegex(ValueError, 'a text tile has words: its text is blank'):
            note.text = '   '
        other = page.sheet('More').text('Second sheet')
        self.assertEqual(other.sheet.name, 'More')
        page.remove(other)
        self.assertNotIn(other, page.texts)

    def test_a_timeseries_tile_reads_many_grids_and_says_every_figure_in_datacubes_words(self):
        # DataCube's timeseries viewer (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md): series of two grids along one time
        # axis, each an expression in gs-quant's vocabulary read by DataCube's parser, by the commands its viewer sends
        # (addTimeseries, setTimeseries); each grid's date and instrument columns DataCube's to choose
        page = ll.Page('Markets')
        days = pd.to_datetime(['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']).date
        prices = pd.DataFrame({'symbol': ['SPX'] * 4, 'day': days, 'close': [100.0, 101.0, 99.5, 102.0]})
        yields = pd.DataFrame({'tenor': ['10Y'] * 4, 'obs_date': days, 'yield': [4.0, 4.1, 3.9, 4.2]})
        px = page.grid(prices, name='prices')
        rates = page.grid(yields, name='rates')
        tile = page.timeseries({'px': px, 'rates': rates}, ['px:close[SPX]', 'rates:yield[10Y]', 'correlation($A, $B, 3)'],
                               range={'last': '1y'})
        self.assertEqual(tile.id, 'timeseries-1')
        self.assertIn(tile, page.timeseries_tiles)
        self.assertIs(tile.sheet, px.sheet)
        self.assertEqual([(s['alias'], s['cube'], s['date'], s.get('key')) for s in tile.spec['sources']],
                         [('px', px.id, 'day', 'symbol'), ('rates', rates.id, 'obs_date', 'tenor')])
        self.assertEqual([s['id'] for s in tile.series], ['A', 'B', 'C'])
        self.assertEqual(tile.series[2]['expr'], {'kind': 'call', 'fn': 'correlation', 'args': [
            {'kind': 'ref', 'series': 'A'}, {'kind': 'ref', 'series': 'B'}], 'params': {'w': 3}})
        tile.plot('volatility($A, 2)', pane=1, name='SPX vol')
        self.assertEqual(tile.series[-1]['id'], 'D')
        tile.options(view={'compare': 'rebase'}, grain='week')
        self.assertEqual((tile.spec['view'], tile.spec['grain']), ({'compare': 'rebase'}, 'week'))
        tile.unplot('D')
        self.assertEqual(len(tile.series), 3)
        # refused in DataCube's words: what its parser cannot read, what DataCube does not compute, a word it has not
        with self.assertRaisesRegex(ValueError, r'series D: there is no function vola'):
            tile.plot('vola($A)')
        with self.assertRaisesRegex(ValueError, 'an iterative procedure'):
            tile.plot('smooth_outliers($A)')
        with self.assertRaisesRegex(ValueError, 'frequency "hour"'):
            tile.options(grain='hour')
        # its values, every figure the database's (headless timeseriesQuery, the viewer's own queries): a grid's series,
        # one across both grids (read together over one model of their frames), a window kept by the database -- weekly
        # and rebased, as it was just told, one week from 100, dated at its last observation (the date its value is of);
        # then daily, as the rows are
        weekly = tile.frame('A')
        self.assertEqual(([d.strftime('%Y-%m-%d') for d in weekly.index], list(weekly['A'])), (['2024-01-05'], [100.0]))
        tile.options(view=None, grain=None)
        a = tile.frame('A')
        self.assertEqual(list(a.columns), ['A'])
        # none named: the tile's first, by DataCube's rule (headless seriesOf), its answer naming it (the review's §9 A)
        self.assertTrue(tile.frame().equals(a), 'its first series, A')
        self.assertEqual([d.strftime('%Y-%m-%d') for d in a.index], ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'])
        self.assertEqual(list(a['A']), [100.0, 101.0, 99.5, 102.0])
        both = tile.frame('C')
        # the correlation of the two's simple returns over the last 3 dates, where both have them: one value, the 5th
        returns = (pd.Series([101.0 / 100.0, 99.5 / 101.0, 102.0 / 99.5]) - 1, pd.Series([4.1 / 4.0, 3.9 / 4.1, 4.2 / 3.9]) - 1)
        self.assertAlmostEqual(both['C'].dropna().iloc[-1], returns[0].corr(returns[1]), places=9)
        kept = tile.frame('A', start='2024-01-03', end='2024-01-04')
        self.assertEqual(list(kept['A']), [101.0, 99.5], 'the window\'s dates alone, kept by the database')
        with self.assertRaisesRegex(ValueError, 'has no series Z'):
            tile.frame('Z')
        # a candle per week: its first, highest, lowest and last close, each the database's
        candles = page.timeseries({'px': px}, ['ohlc(close[SPX], close[SPX], close[SPX], close[SPX])'], grain='week')
        week = candles.frame()
        self.assertEqual((list(week.columns), week.iloc[0].tolist()), (['open', 'high', 'low', 'close'], [100.0, 102.0, 99.5, 102.0]))
        page.remove(candles)
        # a grid's own: its date and instrument columns chosen, its first series a line per instrument
        alone = px.timeseries()
        self.assertEqual(alone.series, [{'id': 'A', 'expr': {'kind': 'column', 'column': 'close'}, 'split': 'symbol'}])
        # a price per instrument compared from 100, as the screen's Create ▸ Timeseries makes it (timeseries-expr.ts
        # firstPlot); a column per instrument, every one 100 at its first date
        self.assertEqual(alone.spec['view'], {'compare': 'rebase'})
        first = alone.frame()
        self.assertEqual((list(first.columns), first['SPX'].iloc[0]), (['SPX'], 100.0))
        # saved and read back as it is
        again = ll.Page.load(page.to_dict(), frames={'prices': prices, 'rates': yields})
        self.assertEqual(again.timeseries_tiles[0].spec, tile.spec)
        page.remove(alone)
        self.assertNotIn(alone, page.timeseries_tiles)

    def test_a_timeseries_tables_are_its_screens_each_figure_the_databases(self):
        # the expert tables (2026-10-26): the screen's statistics panel's Summary, Risk, Drawdowns and Correlations tabs,
        # each the same query (headless timeseriesQuery's table), held here to pandas' own figures
        page = ll.Page('Tables')
        days = pd.date_range('2024-01-01', periods=12, freq='D').date
        s = [100.0, 102.0, 101.0, 104.0, 99.0, 97.0, 103.0, 106.0, 105.0, 108.0, 104.0, 110.0]
        t = [50.0, 50.5, 51.0, 50.0, 49.0, 49.5, 51.5, 52.0, 51.0, 53.0, 52.5, 54.0]
        prices = pd.DataFrame({'symbol': ['S'] * 12 + ['T'] * 12, 'day': list(days) * 2, 'close': s + t})
        px = page.grid(prices, name='prices')
        tile = page.timeseries({'px': px}, ['close[S]', 'close[T]', 'returns(close[S])'])
        # under a year: its growth not annualized, said as the screen says it (the E9 review's 10: a NaN said nothing)
        with self.assertWarnsRegex(UserWarning, 'A: these dates span under a year, so its growth is not annualized'):
            stats = tile.statistics()
        self.assertEqual(list(stats.index), ['A', 'B', 'C'])
        self.assertEqual((stats.loc['A', 'count'], stats.loc['A', 'last'], stats.loc['A', 'high']), (12, 110.0, 110.0))
        rs = pd.Series(s).pct_change().dropna()
        self.assertAlmostEqual(stats.loc['A', 'skew'], rs.skew(), places=9)
        self.assertAlmostEqual(stats.loc['A', 'kurtosis'], rs.kurt(), places=9)
        self.assertAlmostEqual(stats.loc['A', 'worst'], rs.min(), places=12)
        self.assertAlmostEqual(stats.loc['A', 'var'], rs.quantile(0.05), places=12)
        self.assertTrue(pd.isna(stats.loc['C', 'vol']), 'a return is no level: no volatility')
        drawdowns = tile.drawdowns()
        self.assertEqual(sorted(set(drawdowns['line'])), ['A', 'B'], 'the levels alone')
        deepest = drawdowns[drawdowns['line'] == 'A'].iloc[0]
        self.assertEqual([str(deepest[k])[:10] for k in ('peak', 'trough', 'recovered')], ['2024-01-04', '2024-01-06', '2024-01-08'])
        self.assertAlmostEqual(deepest['depth'], 97.0 / 104.0 - 1, places=12)
        self.assertEqual((deepest['rank'], deepest['decline'], deepest['recovery']), (1, 2, 2))
        corr = tile.correlations()
        self.assertEqual(list(corr.index), ['A', 'B', 'C'])
        rt = pd.Series(t).pct_change().dropna()
        self.assertAlmostEqual(corr.loc['A', 'B'], rs.corr(rt), places=9)
        self.assertAlmostEqual(corr.loc['B', 'A'], rs.corr(rt), places=9)
        self.assertAlmostEqual(corr.loc['A', 'C'], 1.0, places=9, msg="a level's returns with the same returns")
        # each line with itself the database's (the E9 review's 13)
        self.assertAlmostEqual(corr.loc['B', 'B'], 1.0, places=12)
        # over a window: the dates kept by the database before every figure
        late = tile.statistics(start='2024-01-06')
        self.assertEqual((late.loc['A', 'count'], late.loc['A', 'first']), (7, 97.0))
        # no dates asked: the tile's range, as its screen's tables read it (the E9 review's 7: every date had been read)
        tile.options(range={'from': '2024-01-06'})
        ranged = tile.statistics()
        self.assertEqual((ranged.loc['A', 'count'], ranged.loc['A', 'first']), (7, 97.0))
        self.assertEqual(tile.statistics(start='2024-01-01').loc['A', 'count'], 12, 'dates asked: those')
        # two series of one name: each line apart, by its id
        tile.edit('A', 'close[S]', name='Same')
        tile.edit('B', 'close[T]', name='Same')
        self.assertEqual(list(tile.correlations().index), ['Same (A)', 'Same (B)', 'C'])

    def test_a_timeseries_combines_two_calendars_by_its_align_and_reads_gs_quants_functions(self):
        # the next phases' 1 (2026-10-26, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11): a tile's align, Python's
        # options(align=...) as the viewer's Settings sets it, and one of gs-quant's remaining functions, each the database's
        page = ll.Page('Calendars')
        days = pd.date_range('2024-01-01', periods=6, freq='D').date
        daily = pd.DataFrame({'symbol': ['S'] * 6, 'day': list(days), 'close': [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]})
        sparse = pd.DataFrame({'symbol': ['T'] * 2, 'day': [days[0], days[3]], 'close': [10.0, 20.0]})
        px = page.grid(pd.concat([daily, sparse]), name='prices')
        tile = page.timeseries({'px': px}, ['close[S] - close[T]'])
        self.assertEqual(list(tile.frame()['A']), [90.0, 83.0], 'the dates both have')
        tile.options(align='step')
        self.assertEqual(tile.spec['align'], 'step')
        self.assertEqual(list(tile.frame()['A']), [90.0, 91.0, 92.0, 83.0, 84.0, 85.0], 'every date, T its last close')
        with self.assertRaisesRegex(ValueError, 'alignment "sideways" is not one of'):
            tile.options(align='sideways')
        tile.plot('weekday(close[S])')
        self.assertEqual(list(tile.frame('B')['B']), [d.weekday() for d in days])

    def test_a_timeseries_swaps_an_instrument_shows_its_query_and_its_lines_side_by_side(self):
        # the next phases' 2 (2026-10-26, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11): the screen's Swap, Query
        # window and Data tab -- swap() by the tile's one rule, query() the compiler's print and the plan's SQL (nothing
        # run), table() every series' frame side by side by date
        page = ll.Page('Swap')
        days = pd.date_range('2024-01-01', periods=4, freq='D').date
        prices = pd.DataFrame({'symbol': ['S'] * 4 + ['T'] * 4, 'day': list(days) * 2,
                               'close': [100.0, 101.0, 102.0, 103.0, 10.0, 11.0, 12.0, 13.0]})
        px = page.grid(prices, name='prices')
        tile = page.timeseries({'px': px}, ['close[S]', 'sma(close[S], 2)'])
        tile.edit('A', 'close[S]', name='S close')
        tile.swap('S', 'T')
        self.assertEqual([s['name'] if 'name' in s else None for s in tile.series], ['T close', None], 'a name saying it says the other')
        self.assertEqual(list(tile.frame('A')['T close']), [10.0, 11.0, 12.0, 13.0])
        with self.assertRaisesRegex(ValueError, 'no series reads S'):
            tile.swap('S', 'T')
        q = tile.query('B')
        self.assertTrue(q['pure'].lstrip().startswith('|'), q['pure'])
        self.assertIn("'T'", q['pure'], 'the instrument swapped in, in the query DataCube wrote')
        self.assertIn('SELECT', q['sql'].upper())
        both = tile.table()
        # (sma is gs-quant's alias of mean: the screen writes the expression by the catalog's own name)
        self.assertEqual(list(both.columns), ['T close', 'mean(close[T], 2)'], "named as the screen's Data tab names its columns")
        self.assertEqual(list(both['T close']), [10.0, 11.0, 12.0, 13.0])
        self.assertTrue(pd.isna(both['mean(close[T], 2)'].iloc[0]), 'a mean of two has none on the first date')
        self.assertEqual(list(both['mean(close[T], 2)'].iloc[1:]), [10.5, 11.5, 12.5])
        self.assertEqual(len(tile.table(start='2024-01-03')), 2, "the dates kept as frame keeps them")
        # the Phase 2 review's 3: the screen's lines alone -- none hidden, no condition -- over the tile's range, an index's
        # base its first date shown; frame and query as the screen draws them, every date, that base
        tile.plot('rebase(close[T])', name='Rebased')
        tile.plot('close[T] > 11', name='Up')
        tile.hide('B')
        tile.options(range={'from': '2024-01-02'})
        shown = tile.table()
        self.assertEqual(list(shown.columns), ['T close', 'Rebased'])
        self.assertEqual(len(shown), 3, "the tile's range")
        self.assertEqual(shown['Rebased'].iloc[0], 100.0, 'its base the first date shown')
        drawn = tile.frame('C')
        self.assertEqual((len(drawn), drawn['Rebased'].iloc[1]), (4, 100.0), 'every date, its base the first shown')
        self.assertIn('2024-01-02', tile.query('C')['pure'], "the query the screen's Query window shows: its base")
        # the Phase 2 review's 5: an instrument column of numbers swapped by numbers, the spaces round a name not of it
        coded = page.grid(pd.DataFrame({'code': [10] * 4 + [20] * 4, 'day': list(days) * 2, 'close': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]}), name='coded')
        numbered = page.timeseries({'c': {'grid': coded, 'key': 'code'}}, ['close[10]'])
        numbered.swap(10, 20)
        self.assertEqual(list(numbered.frame()['A']), [5.0, 6.0, 7.0, 8.0])
        tile.swap(' T ', ' S ')
        self.assertEqual(list(tile.frame('A')['S close']), [100.0, 101.0, 102.0, 103.0])

    def test_a_timeseries_views_are_its_screens_each_figure_the_databases(self):
        # the next phases' 3 (2026-10-26, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11): the screen's Scatter,
        # Seasonality, Histogram, Curve and Calendar, each the same query (headless timeseriesQuery's table), held here to
        # numpy's and pandas' own figures
        import numpy as np
        page = ll.Page('Views')
        months = pd.date_range('2022-01-31', periods=30, freq='ME').date
        s = [100.0 + 3 * i + (5.0 if i % 12 == 6 else 0.0) for i in range(30)]
        t = [50.0 + 1.5 * i + (i % 3) for i in range(30)]
        px = page.grid(pd.DataFrame({'symbol': ['S'] * 30 + ['T'] * 30, 'day': list(months) * 2, 'close': s + t}), name='prices')
        tile = page.timeseries({'px': px}, ['close[S]', 'close[T]'], grain='month')
        # a scatter of the first two, T up and S across: y's least squares on x, the database's
        sc = tile.scatter()
        self.assertEqual((list(sc.columns), len(sc)), (['close[S]', 'close[T]'], 30))
        slope, intercept = np.polyfit(s, t, 1)
        fit = sc.attrs['regression']
        self.assertAlmostEqual(fit['slope'], slope, places=9)
        self.assertAlmostEqual(fit['intercept'], intercept, places=6)
        self.assertAlmostEqual(fit['r2'], pd.Series(s).corr(pd.Series(t)) ** 2, places=9)
        self.assertEqual(fit['n'], 30)
        self.assertEqual(len(tile.scatter(start='2024-01-01')), 6, "the dates kept as the screen's window keeps them")
        # seasonality by month: 2024 against the year before's range at each month
        sea = tile.seasonality('A', years=1)
        self.assertEqual(sorted(set(sea['year'])), [2023, 2024])
        late = sea[sea['year'] == 2024].set_index('place')
        before = dict(zip(range(1, 13), s[12:24]))
        self.assertEqual(list(late.index), [1, 2, 3, 4, 5, 6])
        for m in late.index:
            self.assertEqual((late.loc[m, 'low'], late.loc[m, 'high'], late.loc[m, 'mean']), (before[m],) * 3, f'month {m}: 2023 alone before')
        self.assertEqual(sea.attrs['latest'], 2024)
        with self.assertRaisesRegex(ValueError, 'years 0 is not a whole number of 1 to 20'):
            tile.seasonality('A', years=0)
        # a histogram of 5 bins: every value counted, its 5th percentile pandas' (linear, as the database's continuous)
        h = tile.histogram('A', bins=5)
        self.assertEqual((int(h['count'].sum()), h['low'].iloc[0]), (30, min(s)))
        self.assertAlmostEqual(h['high'].iloc[-1], max(s), places=9)
        self.assertAlmostEqual(h.attrs['p5'], pd.Series(s).quantile(0.05), places=9)
        self.assertEqual(h.attrs['n'], 30)
        # the calendar: each month's return from the month before's, each year's from the year before's, latest first
        cal = tile.calendar('A')
        self.assertEqual(list(cal.index), [2024, 2023, 2022])
        self.assertAlmostEqual(cal.loc[2023, 'Mar'], s[14] / s[13] - 1, places=12)
        self.assertAlmostEqual(cal.loc[2023, 'Year'], s[23] / s[11] - 1, places=12)
        self.assertTrue(pd.isna(cal.loc[2022, 'Jan']) and pd.isna(cal.loc[2022, 'Year']), 'no level before the first')
        self.assertTrue(pd.isna(cal.loc[2024, 'Dec']), 'a month it has not')
        # a curve across a split's tenors, in the screen's order (3M before 2Y), as of the latest and a month back
        days = pd.date_range('2024-01-01', periods=70, freq='D').date
        tenors = {'2Y': 4.0, '10Y': 4.5, '3M': 5.0}
        rates = page.grid(pd.DataFrame({'tenor': [k for k in tenors for _ in days], 'day': list(days) * 3,
                                        'rate': [v + i / 100 for v in tenors.values() for i in range(70)]}), name='rates')
        curve = page.timeseries({'r': rates}, [{'expr': 'rate', 'split': 'tenor'}])
        c = curve.curve(dates=['latest', '1m'])
        self.assertEqual((list(c.index), list(c.columns)), (['3M', '2Y', '10Y'], ['latest', '1m']))
        self.assertAlmostEqual(c.loc['10Y', 'latest'], 4.5 + 69 / 100, places=12)
        # a month back from 2024-03-10: 2024-02-10, the 41st day
        self.assertAlmostEqual(c.loc['3M', '1m'], 5.0 + 40 / 100, places=12)
        self.assertEqual(c.attrs['dates'], {'latest': '2024-03-10', '1m': '2024-02-10'})
        self.assertEqual(c.attrs['line_dates']['latest'], {'3M': '2024-03-10', '2Y': '2024-03-10', '10Y': '2024-03-10'}, "each line's own day")
        # the Phase 3 review's 11: a series against itself, two columns
        self.assertEqual(list(tile.scatter('A', 'A').columns), ['close[S] (x)', 'close[S] (y)'])
        with self.assertRaisesRegex(ValueError, 'a curve is of a line per value of a column'):
            tile.curve('A')

    def test_a_timeseries_lists_every_query_it_asks_each_its_pure_and_its_sql(self):
        # the user, 2026-10-26: "show all the pure queries used to create a timeseries chart" -- the screen's Query
        # window's every question, by its one rule (headless timeseriesAsks: timeseries-query.ts tileAsks), each written
        # by the screen's own function (askQuery) and planned, nothing run
        page = ll.Page('Queries')
        days = pd.date_range('2024-01-01', periods=6, freq='D').date
        prices = pd.DataFrame({'symbol': ['S'] * 6, 'day': list(days), 'close': [100.0, 101.0, 99.0, 103.0, 104.0, 102.0]})
        px = page.grid(prices, name='prices')
        tile = page.timeseries({'px': px}, ['close[S]', 'returns(close[S])'])
        asked = tile.queries()
        self.assertEqual([(q['kind'], q.get('series', q.get('source')), q.get('with'), q['serves']) for q in asked], [
            ('bounds', 'px', None, 'chart'), ('series', 'A', None, 'chart'), ('series', 'B', None, 'chart'),
            ('statistics', 'A', None, 'statistics'), ('statistics', 'B', None, 'statistics'), ('drawdowns', 'A', None, 'drawdowns'),
            ('correlations', 'A', 'A', 'correlations'), ('correlations', 'A', 'B', 'correlations'),
            ('correlations', 'B', 'B', 'correlations'), ('calendar', 'A', None, 'calendar')])
        for q in asked:
            self.assertNotIn('refused', q, q['what'])
            self.assertTrue(q['pure'].lstrip().startswith('|'), q['what'])
            self.assertIn('SELECT', q['sql'].upper(), q['what'])
        self.assertEqual(asked[1]['what'], 'Series A (close[S]): its points')
        self.assertIn("'S'", asked[1]['pure'])
        # a return is a call of DataCube's Pure library (datacube/src/pure-library.ts), planned over the model with it
        # after
        self.assertIn('datacube::ts::returns', asked[2]['pure'])
        # a view's question with them, as the screen asks it: after the series', before the panel's
        tile.options(view={'mode': 'histogram'})
        kinds = [(q['kind'], q.get('series')) for q in tile.queries()]
        self.assertEqual(kinds[3], ('histogram', 'A'))
        # a series' points as the screen draws them, as query() reads them, but for the drawing's limit: whole dates, the
        # date it crosses at kept too (timeseries-query.ts seriesQuery, the review's Q12)
        self.assertIn('ts_cut_first <= 100001', asked[1]['pure'])

    def test_a_timeseries_shades_a_column_where_it_is_not_zero_in_one_step(self):
        # Phase 5's shade from a column (FRED's recession bars): a condition series of it, by the tile's one rule
        # (timeseries-edit.ts shade), its periods the database's -- 1 where the column is not 0
        page = ll.Page('Shade')
        days = pd.date_range('2024-01-01', periods=6, freq='D').date
        prices = pd.DataFrame({'symbol': ['S'] * 6, 'day': list(days), 'close': [100.0, 101.0, 99.0, 103.0, 104.0, 102.0],
                               'rec': [0, 1, 1, 0, 1, 0], 'side': ['a'] * 6})
        tile = page.timeseries({'px': page.grid(prices, name='prices')}, ['close[S]'])
        tile.shade('rec', where=[{'value': 'S'}], name='Recessions')
        added = tile.spec['series'][-1]
        self.assertEqual((added['id'], added['name']), ('B', 'Recessions'))
        frame = tile.frame('B')
        self.assertEqual([int(v) for v in frame['Recessions']], [0, 1, 1, 0, 1, 0], 'where it is not 0, each date')
        with self.assertRaisesRegex(ValueError, "plots 'side', which is not a number"):
            tile.shade('side')

    def test_a_timeseries_projects_a_trend_each_figure_the_databases_as_the_screen_draws_it(self):
        # Phase 5's trend projected: the series' least-squares line over its last observations on their day numbers,
        # and its fit at the first, the last and `ahead` past it, by the screen's one rule (projectionQuery)
        import numpy as np
        page = ll.Page('Projection')
        days = pd.date_range('2024-01-01', periods=8, freq='D').date
        close = [100.0, 101.0, 99.0, 103.0, 104.0, 102.0, 106.0, 107.5]
        prices = pd.DataFrame({'symbol': ['S'] * 8, 'day': list(days), 'close': close})
        tile = page.timeseries({'px': page.grid(prices, name='prices')}, ['close[S]'])
        tile.project('A', window=5, ahead='10d', label='Trend')
        self.assertEqual(tile.spec['annotations'], [{'kind': 'projection', 'series': 'A', 'window': 5, 'ahead': '10d', 'label': 'Trend'}])
        got = tile.projections()
        self.assertEqual(list(got.columns), ['name', 'series', 'start', 'start_value', 'last', 'last_value', 'end', 'end_value', 'slope', 'n'])
        r = got.iloc[0]
        x = np.array([pd.Timestamp(d).toordinal() for d in days[-5:]], dtype=float)
        slope, intercept = np.polyfit(x, np.array(close[-5:]), 1)
        self.assertEqual((r['name'], r['series'], int(r['n'])), ('Trend', 'A', 5))
        self.assertEqual([r['start'].date(), r['last'].date(), r['end'].date()], [days[-5], days[-1], days[-1] + pd.Timedelta(days=10)])
        self.assertAlmostEqual(r['slope'], slope, places=9)
        for at, want in (('start_value', x[0]), ('last_value', x[-1]), ('end_value', x[-1] + 10)):
            self.assertAlmostEqual(r[at], intercept + slope * want, places=6, msg=at)
        # listed with the tile's every query, its place told
        asked = [q for q in tile.queries() if q['kind'] == 'projection']
        self.assertEqual([(q['series'], q['projection']) for q in asked], [('A', 0)])
        self.assertIn('SELECT', asked[0]['sql'].upper())
        # refused by the tile's reader, in its words: a series it has not
        with self.assertRaisesRegex(ValueError, 'annotation 2 projects "Z", which is none of the tile\'s series'):
            tile.project('Z')
        # under a compare, in DataCube's words, as the screen leaves it out
        tile.options(view={'compare': 'rebase'})
        with self.assertRaisesRegex(ValueError, "a projection is of a line's own values"):
            tile.projections()

    def test_a_timeseries_takes_a_persons_drawings_by_the_screens_rule(self):
        # Phase 5's drawings (the screen's Draw): a line from a point to a point, a note pinned at one -- annotations of
        # the tile's one rule, read by its one reader (timeseries-spec.ts tsProblem), refused in its words
        page = ll.Page('Drawings')
        days = pd.date_range('2024-01-01', periods=6, freq='D').date
        prices = pd.DataFrame({'symbol': ['S'] * 6, 'day': list(days), 'close': [100.0, 101.0, 99.0, 103.0, 104.0, 102.0]})
        tile = page.timeseries({'px': page.grid(prices, name='prices')}, ['close[S]'])
        line = {'kind': 'line', 'from': {'date': '2024-01-01', 'value': 100.0}, 'to': {'date': '2024-01-06', 'value': 104.5}, 'label': 'Trend'}
        note = {'kind': 'note', 'at': {'date': '2024-01-04', 'value': 103.0}, 'text': 'Earnings'}
        tile.annotate(line).annotate(note)
        self.assertEqual(tile.spec['annotations'], [line, note])
        with self.assertRaisesRegex(ValueError, 'annotation 3 says nothing: a note has its text'):
            tile.annotate({'kind': 'note', 'at': {'date': '2024-01-04', 'value': 103.0}, 'text': ' '})
        with self.assertRaisesRegex(ValueError, "annotation 3's to is not a point: a date"):
            tile.annotate({'kind': 'line', 'from': {'date': '2024-01-01', 'value': 1}, 'to': {'date': 'later', 'value': 2}})
        tile.unannotate(0)
        self.assertEqual(tile.spec['annotations'], [note])

    def test_a_timeseries_marks_a_grids_rows_each_the_databases_as_the_screen_marks_them(self):
        # marks from a grid's rows (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11, Phase 6's D4: the trades; Phase
        # 5's events from a grid): each trade at its price on its day, its quantity beside it, by the screen's one rule
        # (timeseries-query.ts marksQuery), listed with the tile's queries
        page = ll.Page('Marks')
        days = pd.date_range('2024-01-01', periods=6, freq='D').date
        prices = pd.DataFrame({'symbol': ['S'] * 6, 'day': list(days), 'close': [100.0, 101.0, 99.0, 103.0, 104.0, 102.0]})
        trades = pd.DataFrame({'symbol': ['S', 'S', 'S', 'T'], 'day': [days[1], days[3], days[4], days[2]], 'side': ['BUY', 'SELL', 'BUY', 'BUY'],
                               'price': [100.5, 103.2, 103.9, 50.0], 'quantity': [100.0, 300.0, 200.0, 10.0]})
        px, tr = page.grid(prices, name='prices'), page.grid(trades, name='trades')
        tile = page.timeseries({'px': px, 'tr': {'grid': tr, 'date': 'day', 'key': 'symbol'}}, ['px:close[S]'])
        tile.annotate({'kind': 'marks', 'source': 'tr', 'where': [{'value': 'S'}, {'column': 'side', 'value': 'BUY'}],
                       'at': 'price', 'text': 'quantity', 'shape': 'up', 'label': 'Bought'})
        marks = tile.marks()
        self.assertEqual(list(marks.columns), ['name', 'date', 'value', 'text'])
        self.assertEqual([(r.name, r.date.date(), r.value, float(r.text)) for r in marks.itertuples()],
                         [('Bought', days[4], 103.9, 200.0), ('Bought', days[1], 100.5, 100.0)], "each S buy, latest first")
        asked = tile.queries()
        mark = next(q for q in asked if q['kind'] == 'marks')
        self.assertEqual((mark['what'], mark['marks'], mark['source']), ('Marks "Bought": source tr\'s rows at their price', 0, 'tr'))
        self.assertIn("'BUY'", mark['pure'])
        self.assertIn('SELECT', mark['sql'].upper())
        # refused by the tile's one rule, in its words: a source it has not
        with self.assertRaisesRegex(ValueError, "source \"fx\" is none of the tile's"):
            tile.annotate({'kind': 'marks', 'source': 'fx'})

    def test_a_timeseries_writes_the_code_that_builds_it_and_the_python_builds_it_again(self):
        # the user, 2026-10-26: "an option that shows how exactly to build that chart programmatically from TS and python"
        # -- the screen's Code, by its one rule (headless timeseriesCode: timeseries-code.ts tileCode); 2026-10-28: "a
        # generic snippet ... to just create exactly that tile in their own datacube": the Python run makes a page of its
        # own, a grid over the person's frame, and a tile of the very spec over it, each formula's Pure printed
        page = ll.Page('Code')
        days = pd.date_range('2024-01-01', periods=8, freq='D').date
        prices = pd.DataFrame({'symbol': ['S'] * 8 + ['T'] * 8, 'day': list(days) * 2,
                               'close': [100.0 + i for i in range(8)] + [50.0 - i for i in range(8)]})
        px = page.grid(prices, name='prices')
        tile = page.timeseries({'px': px}, ['close[S]', {'expr': 'returns(close[T], type=log)', 'name': "T's log returns", 'pane': 1},
                                            'basket($A, 0.5, close[T], 0.5, rebalance=weekly)'], title='Two',
                               view={'stats': True}, annotations=[{'kind': 'level', 'value': 0, 'pane': 1}])
        code = tile.code()
        self.assertIn('page = ll.Page()', code)
        self.assertIn("prices = page.grid(prices_frame, name='prices')", code)
        self.assertIn('tile = prices.timeseries(', code)
        self.assertIn("'returns(close[T], type=log)'", code)
        for lookup in ('Page.load', 'page.sheets', 'page.grids', '.id =='):
            self.assertNotIn(lookup, code)
        # run, the person's frame given it: a page of its own, its one tile of the same spec over its own grid
        shown, printed = [], []
        shim = type('ll', (), {'Page': ll.Page, 'show': staticmethod(shown.append)})
        ran = {'ll': shim, 'prices_frame': prices, 'print': lambda *a, **k: printed.append(a)}
        exec(compile(code.replace('import legend_lite as ll\n', ''), 'tile.py', 'exec'), ran)
        own = ran['page']
        self.assertIsNot(own, page)
        self.assertEqual(shown, [own])
        made = own.timeseries_tiles
        self.assertEqual(len(made), 1)
        cubes = lambda spec: {**spec, 'sources': [{**s, 'cube': None} for s in spec['sources']]}
        self.assertEqual(cubes(made[0].spec), cubes(tile.spec))
        self.assertEqual(made[0].title, 'Two')
        # each formula's Pure, as the tile writes it
        self.assertEqual([p[0] for p in printed], ['A', 'B', 'C'])
        self.assertTrue(all('datacube::ts::' in p[1] or '#>{' in p[1] for p in printed), printed)
        typescript = tile.code('typescript')
        self.assertIn("kind: 'addTimeseries'", typescript)
        self.assertIn('export function withTile(page: PageDocument, sheet: string)', typescript)
        with self.assertRaisesRegex(ValueError, 'written in python or typescript, not "rust"'):
            tile.code('rust')

    def test_a_timeseries_code_is_a_program_whatever_its_names_say(self):
        # the review of the generated code, 2026-10-28: a title with a line end in it ended its comment and
        # wrote a line of code; two grids over one frame read two frames of the person's; a tile of no instrument column
        # left DataCube to choose one when run
        page = ll.Page('Names')
        days = pd.date_range('2024-01-01', periods=6, freq='D').date
        # a text column, so a tile not told its key would take this one
        prices = pd.DataFrame({'day': list(days), 'symbol': ['S'] * 6, 'close': [100.0 + i for i in range(6)],
                               'open': [99.0 + i for i in range(6)]})
        one = page.grid(prices, name='prices', title='Line\rends\u2028here')
        two = page.grid(prices, name='prices', title='Again')
        tile = page.timeseries({'a': {'grid': one, 'date': 'day', 'key': None}, 'b': {'grid': two, 'date': 'day', 'key': None}},
                               ['a:close', 'b:open'], title='Two\nlines\r\nimport os')
        code = tile.code()
        compile(code, 'tile.py', 'exec')
        self.assertEqual([line for line in code.splitlines() if line.startswith(('import', 'print'))], ['import legend_lite as ll'])
        self.assertIn("page.grid(prices_frame, name='prices')", code)
        self.assertNotIn('prices_2_frame', code)
        ran = {'ll': type('ll', (), {'Page': ll.Page, 'show': staticmethod(lambda p: None)}), 'prices_frame': prices,
               'print': lambda *a, **k: None}
        exec(compile(code.replace('import legend_lite as ll\n', ''), 'tile.py', 'exec'), ran)
        cubes = lambda spec: {**spec, 'sources': [{**s, 'cube': None} for s in spec['sources']]}
        self.assertEqual(cubes(ran['page'].timeseries_tiles[0].spec), cubes(tile.spec))
        self.assertEqual(ran['page'].timeseries_tiles[0].title, tile.title)
        # one source of no instrument column: said as none, and run, none still
        alone = page.timeseries({'grid': two, 'date': 'day', 'key': None}, ['open'])
        code = alone.code()
        self.assertIn("key=None", code)
        ran = {'ll': type('ll', (), {'Page': ll.Page, 'show': staticmethod(lambda p: None)}), 'prices_frame': prices,
               'print': lambda *a, **k: None}
        exec(compile(code.replace('import legend_lite as ll\n', ''), 'tile.py', 'exec'), ran)
        self.assertNotIn('key', ran['page'].timeseries_tiles[0].spec['sources'][0])
        # the second review, 2026-10-28: a title on the program's first line read as its encoding (PEP 263) -- `coding=
        # utf-7` decoded `+ACc-` as a quote and `+AAo-` as a line end inside the title's string. Compiled from its bytes,
        # as a file is, it runs as written
        attack = 'coding=utf-7 +ACc-,+AAo-x=print(+ACc-INJECTED+ACc-),+AAo-#'
        titled = page.timeseries({'grid': two, 'date': 'day', 'key': None}, ['open'], title=attack)
        code = titled.code()
        self.assertNotIn('coding', ''.join(code.splitlines()[:2]))
        printed = []
        ran = {'ll': type('ll', (), {'Page': ll.Page, 'show': staticmethod(lambda p: None)}), 'prices_frame': prices,
               'print': lambda *a, **k: printed.append(a)}
        exec(compile(code.replace('import legend_lite as ll\n', '').encode('utf-8'), 'tile.py', 'exec'), ran)
        self.assertEqual(ran['page'].timeseries_tiles[0].title, attack)
        self.assertFalse(any('INJECTED' in str(a) for a in printed), printed)

    def test_a_timeseries_reads_an_analysts_packs_each_figure_the_databases(self):
        # the next phases' 4 (2026-10-26, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11): technicals of a candle's
        # columns and a rebalanced basket -- the screen's parser and queries, held to pandas
        import numpy as np
        page = ll.Page('Packs')
        days = list(pd.bdate_range('2024-01-31', periods=30).date)
        s_close = [100.0 + i * 0.5 + (i % 3) * 0.7 - (i % 5) * 0.4 for i in range(30)]
        t_close = [50.0 + (i % 4) * 0.6 - i * 0.1 for i in range(30)]
        prices = pd.DataFrame({'symbol': ['S'] * 30 + ['T'] * 30, 'day': days * 2, 'close': s_close + t_close,
                               'high': [c + 1 for c in s_close + t_close], 'low': [c - 1 for c in s_close + t_close],
                               'volume': [1000 + 10 * i for i in range(30)] * 2})
        px = page.grid(prices, name='prices')
        tile = page.timeseries({'px': px}, ['obv(close[S], volume[S])', 'basket(close[S], 0.5, close[T], 0.5)',
                                            'donchian_channels(high[S], low[S], 5)'])
        sc, tc, vol = pd.Series(s_close), pd.Series(t_close), pd.Series([1000 + 10 * i for i in range(30)], dtype=float)
        obv = (np.sign(sc.diff()).fillna(0) * vol).cumsum()
        self.assertEqual(list(tile.frame('A')['A']), list(obv))
        # rebalanced daily: 100 compounded by the weighted returns
        daily = 100 * (1 + 0.5 * sc.pct_change().fillna(0) + 0.5 * tc.pct_change().fillna(0)).cumprod()
        np.testing.assert_allclose(tile.frame('B')['B'].to_numpy(dtype=float), daily.to_numpy(), rtol=1e-9)
        band = tile.frame('C')
        np.testing.assert_allclose(band['upper'].to_numpy(dtype=float)[4:], (sc + 1).rolling(5).max().to_numpy()[4:], rtol=1e-12)
        with self.assertRaisesRegex(ValueError, r'atr reads three series \(high, low and close\)'):
            tile.plot('atr(high[S], low[S])')

    def test_a_timeseries_tables_over_a_range_of_the_last_month_read_it_from_its_sources_latest_date(self):
        # the E9 review's 7: the screen's window of a range of the last of a period -- each source's bounds the database's,
        # the latest of them -- the tables' too (timeseries-query.ts rangeWindow, headless timeseriesWindow)
        page = ll.Page('Range')
        days = pd.date_range('2024-01-01', '2024-02-29', freq='D').date
        prices = pd.DataFrame({'symbol': ['S'] * len(days), 'day': list(days), 'close': [100.0 + i for i in range(len(days))]})
        px = page.grid(prices, name='prices')
        tile = page.timeseries({'px': px}, ['close[S]'])
        tile.options(range={'last': '1m'})
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            stats = tile.statistics()
            # a month back from the latest, 2024-02-29: from 2024-01-29, 32 days
            self.assertEqual((stats.loc['A', 'count'], stats.loc['A', 'first']), (32, 128.0))
            self.assertEqual(tile.statistics(start='2024-01-01').loc['A', 'count'], 60)
            self.assertTrue((tile.drawdowns().empty), 'a rising line has no drawdowns')

    def test_a_timeseries_is_changed_by_its_viewers_one_rule_and_refused_what_its_viewer_cannot_draw(self):
        # the review's A2, A3, A5, A6, A10 (docs/TIMESERIES_REVIEW_2026_10_25.md): every change Python makes is the tile's
        # one rule (timeseries-edit.ts editTimeseries, headless timeseriesEdit) -- the rule its viewer's editors ask -- and
        # what the viewer would say it cannot draw is refused here, in its words
        page = ll.Page('Markets')
        days = pd.to_datetime(['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']).date
        prices = pd.DataFrame({'symbol': ['SPX'] * 4, 'day': days, 'close': [100.0, 101.0, 99.5, 102.0], 'desk': ['A'] * 4})
        yields = pd.DataFrame({'tenor': ['10Y'] * 4, 'obs_date': days, 'yield': [4.0, 4.1, 3.9, 4.2]})
        px = page.grid(prices, name='prices')
        rates = page.grid(yields, name='rates')
        tile = page.timeseries({'px': px, 'rates': rates}, ['close[SPX]', 'returns($A)'])
        # a series another reads is not taken off; a source a series reads -- by a column of no alias too -- is not let go
        with self.assertRaisesRegex(ValueError, r'series B reads \$A'):
            tile.unplot('A')
        with self.assertRaisesRegex(ValueError, 'series A, B read source px'):
            tile.unread('px')
        tile.unread('rates')
        self.assertEqual([s['alias'] for s in tile.spec['sources']], ['px'])
        with self.assertRaisesRegex(ValueError, 'source px is the only grid the tile reads'):
            tile.unread('px')
        # another grid read, by DataCube's rule over its columns; read twice, refused
        tile.read(rates)
        read = tile.spec['sources'][1]
        self.assertEqual((read['cube'], read['date'], read.get('key')), (rates.id, 'obs_date', 'tenor'))
        with self.assertRaisesRegex(ValueError, 'is read already'):
            tile.read(rates)
        # a source renamed: every expression naming it follows
        tile.plot(f"{read['alias']}:yield[10Y]")
        tile.source(read['alias'], rename='curve')
        self.assertEqual(tile.series[2]['expr']['source'], 'curve')
        # a line hidden and shown, an edit in place keeping it hidden; an edit of a series it has not, refused
        tile.hide('C')
        tile.edit('C', 'curve:yield[10Y] * 2', name='twice')
        self.assertEqual((tile.series[2]['name'], tile.series[2]['hidden']), ('twice', True))
        tile.show('C')
        self.assertNotIn('hidden', tile.series[2])
        with self.assertRaisesRegex(ValueError, 'there is no series Q'):
            tile.edit('Q', 'close')
        # an annotation on a pane, and taken off
        tile.annotate({'kind': 'level', 'value': 0, 'pane': 1, 'label': 'zero'})
        self.assertEqual(tile.spec['annotations'], [{'kind': 'level', 'value': 0, 'pane': 1, 'label': 'zero'}])
        tile.unannotate(0)
        self.assertNotIn('annotations', tile.spec)
        # a line per value of any text column, as the viewer's editor offers it
        tile.plot('close', split='desk')
        self.assertEqual(tile.series[-1]['split'], 'desk')
        # what the viewer cannot draw: a column there is not, a text plotted, a series there is not, a date that is text
        with self.assertRaisesRegex(ValueError, "reads 'nosuch', which source px has not"):
            tile.plot('nosuch')
        with self.assertRaisesRegex(ValueError, "plots 'symbol', which is not a number"):
            tile.plot('symbol')
        with self.assertRaisesRegex(ValueError, r'reads \$Z, which there is not'):
            tile.plot('$Z')
        with self.assertRaisesRegex(ValueError, "'symbol' is not a date"):
            page.timeseries({'px': {'grid': px, 'date': 'symbol'}}, ['close'])
        with self.assertRaisesRegex(ValueError, 'is 5, not a word'):
            page.timeseries({'px': {'grid': px, 'date': 5}}, ['close'])
        # the screen's first-tile rule: a grid with a date and no number to plot, refused in the screen's words
        dates = page.grid(pd.DataFrame({'symbol': ['SPX'] * 4, 'day': days}), name='dates')
        with self.assertRaisesRegex(ValueError, 'has no date column with a number to plot along it'):
            dates.timeseries()
        # an alias named `grid`, by alias: two sources, not one
        both = page.timeseries({'grid': px, 'curve': rates}, ['grid:close[SPX]'])
        self.assertEqual([s['alias'] for s in both.spec['sources']], ['grid', 'curve'])

    def test_a_timeseries_is_judged_as_its_viewer_judges_it_and_its_frame_says_what_the_screen_says(self):
        # the second audit's A-S1, A-S2, A-S3, P-3: one judge for both sides -- a tile of no series either may leave, a
        # slot it has not refused by name, no spec set past the rule -- and the frame's notes the drawing's own
        page = ll.Page('Markets')
        days = pd.to_datetime(['2024-01-02', '2024-01-06', '2024-01-11', '2024-01-15', '2024-01-22']).date
        prices = pd.DataFrame({'symbol': ['SPX'] * 5, 'day': days, 'close': [100.0, 101.0, 99.5, 102.0, 103.0]})
        px = page.grid(prices, name='prices')
        tile = page.timeseries({'px': px}, ['close[SPX]', 'volatility($A, 2)'])
        with self.assertRaisesRegex(ValueError, 'there is no "grian" among a tile\'s slots'):
            tile.options(grian='week')
        with self.assertRaises(AttributeError):
            tile.spec = {**tile.spec, 'series': []}
        # dates 4 to 7 days apart are no frequency to annualize by: said, as the screen says it
        with self.assertWarnsRegex(UserWarning, 'its dates are no frequency it can be annualized by'):
            vol = tile.frame('B')
        self.assertTrue(vol['B'].isna().all())
        tile.unplot('B')
        tile.unplot('A')
        self.assertEqual(tile.series, [], 'a tile of no series, as the screen leaves one')

    def test_a_timeseries_frozen_keeps_its_grids_filters_as_they_were_and_follows_them_again(self):
        # a timeseries' states, a chart's words (the user, 2026-10-26): frozen, each source keeps its grid's filter as it
        # is now (timeseries-edit.ts editTimeseries, the viewer's state button's rule), the database reading it whatever
        # the grid is filtered to since (timeseries-spec.ts filterRead, the compiler's rule); dynamic, it follows again
        page = ll.Page('Markets')
        days = pd.to_datetime(['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']).date
        prices = pd.DataFrame({'symbol': ['SPX'] * 4, 'day': days, 'close': [100.0, 101.0, 99.5, 102.0]})
        px = page.grid(prices, name='prices')
        tile = page.timeseries({'px': px}, ['close[SPX]'])
        self.assertEqual(tile.state, 'dynamic')
        px.filter(('close', 'greaterThan', 100.0))
        self.assertEqual(list(tile.frame('A')['A']), [101.0, 102.0], 'it follows its grid')
        tile.state = 'frozen'
        self.assertEqual(tile.state, 'frozen')
        self.assertIn('filter', tile.spec['sources'][0]['kept'])
        px.filter(None)
        self.assertEqual(list(tile.frame('A')['A']), [101.0, 102.0], "its grid's filter as it was, kept")
        tile.state = 'dynamic'
        self.assertNotIn('kept', tile.spec['sources'][0])
        self.assertEqual(list(tile.frame('A')['A']), [100.0, 101.0, 99.5, 102.0], 'it follows its grid again')
        with self.assertRaisesRegex(ValueError, 'is dynamic or frozen, not "pinned"'):
            tile.state = 'pinned'

    def test_a_timeseries_reads_a_snapped_grid_over_its_copy_and_every_point_it_has(self):
        # the review's P1, P3, P5 (docs/TIMESERIES_REVIEW_2026_10_25.md): a series reads a snapped grid over its copy, as
        # every other read of the grid does -- alone (the grid's own path, headless gridTimeseries) or with another grid
        # (one model of the copy's table and the other's: the owner's "snapped into duckdb wasm as a grid snap") -- and
        # Python's frame keeps every point (the drawing's limit is the drawing's)
        page = ll.Page('Snapped')
        days = pd.to_datetime(['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']).date
        prices = pd.DataFrame({'symbol': ['SPX'] * 4, 'day': days, 'close': [100.0, 101.0, 102.0, 103.0]})
        yields = pd.DataFrame({'tenor': ['10Y'] * 4, 'obs_date': days, 'yield': [4.0, 4.0, 4.0, 4.0]})
        px = page.grid(prices, name='prices')
        rates = page.grid(yields, name='rates')
        tile = page.timeseries({'px': px, 'rates': rates}, ['px:close[SPX]', 'px:close[SPX] / rates:yield[10Y]'])
        px.snap()
        px.update(prices.assign(close=[200.0, 202.0, 204.0, 206.0]))
        self.assertEqual(list(px.to_pandas()['close']), [100.0, 101.0, 102.0, 103.0], 'the grid reads its copy')
        self.assertEqual(list(tile.frame('A')['A']), [100.0, 101.0, 102.0, 103.0], 'its series alone, the copy too')
        self.assertEqual(list(tile.frame('B')['B']), [25.0, 25.25, 25.5, 25.75], 'with the other grid, the copy too')
        px.live()
        self.assertEqual(list(tile.frame('A')['A']), [200.0, 202.0, 204.0, 206.0], 'live again, the rows as they are')
        # every point of a series past the drawing's limit (100,000): 2 lines of 50,001 days each
        n = 50_001
        many = pd.DataFrame({'symbol': ['A'] * n + ['B'] * n, 'day': list(pd.date_range('1900-01-01', periods=n).date) * 2,
                             'close': [float(i) for i in range(n)] * 2})
        big = page.grid(many, name='many')
        lines = page.timeseries({'m': big}, [{'expr': 'close', 'split': 'symbol'}]).frame()
        self.assertEqual((lines.shape, int(lines.notna().sum().sum())), ((n, 2), 2 * n))

    def test_a_text_box_points_to_its_files_never_copies_them(self):
        # files opened or dropped on a text box (DataCube's text-files.ts; docs/DATACUBE_UI_REVIEW_2026_10_24.md §8): by
        # the command the screen sends (setTextFiles), each its name, size and fingerprint -- its bytes stay where it was
        page, _, _ = self.built()
        note = page.text('Plans')
        self.assertEqual(note.files, [])
        with tempfile.TemporaryDirectory() as d:
            plan = Path(d, 'plan.md')
            plan.write_bytes(b'# Plan\n')
            trades = Path(d, 'trades.csv')
            trades.write_bytes(b'desk,pnl\nFX,1\n')
            note.files = [plan, str(trades)]
            deck = Path(d, 'deck.pptx')
            deck.write_bytes(b'not text')
            with self.assertRaisesRegex(ValueError, 'deck.pptx is not a kind of file a text box shows: Markdown, text, '
                                                    'CSV or JSON'):
                note.files = [deck]
            with self.assertRaisesRegex(ValueError, 'is in the text box twice'):
                note.files = [plan, plan]
            # one path is no list; a folder is no file (the 2026-10-25 audit: its letters were read as paths)
            with self.assertRaisesRegex(TypeError, r"a text box's files are a list: \[path\] for one"):
                note.files = str(plan)
            with self.assertRaisesRegex(ValueError, 'is a folder: a text box shows files'):
                note.files = [d]
        sha = 'a' * 64
        self.assertEqual(note.files, [
            {'name': 'plan.md', 'size': 7, 'sha256': hashlib.sha256(b'# Plan\n').hexdigest()},
            {'name': 'trades.csv', 'size': 14, 'sha256': hashlib.sha256(b'desk,pnl\nFX,1\n').hexdigest()},
        ])
        saved = next(v for v in page.to_dict()['views'] if v['id'] == note.id)
        self.assertEqual(saved['files'], note.files, 'the page keeps the three, never the bytes')
        # a file as ``files`` gives one, judged as given
        with self.assertRaisesRegex(ValueError, 'a file is its name, size and sha256, not its bytes'):
            note.files = [{'name': 'x.md', 'size': 1, 'sha256': sha, 'bytes': 'x'}]
        note.files = note.files[1:]
        self.assertEqual([f['name'] for f in note.files], ['trades.csv'])
        note.files = []
        self.assertNotIn('files', next(v for v in page.to_dict()['views'] if v['id'] == note.id))

    def test_a_grid_is_edited_as_datacubes_screen_edits_it_and_refused_in_its_words(self):
        # grouping, pivot, Column Properties and sorts by the one module the screen's panels, header menu and drag zones
        # call (grid-commands.ts, headless gridApply: docs/DATACUBE_COMMANDS_STAGE1_2026_10_18.md), the grid compiled as
        # Apply compiles it
        page, grid, _ = self.built()
        before = page.to_dict()
        # an aggregate Aggregation does not offer for the column's type
        with self.assertRaisesRegex(ValueError, "'sum' is not offered on desk, a String column: min, max, joinStrings"):
            grid.measure('desk', 'sum', name='desks')
        # only a weighted average takes a weight, refused rather than dropped; and it takes one (the query's own words)
        with self.assertRaisesRegex(ValueError, r'only an aggregate of two columns \(wavg, .*\) takes a second column'):
            grid.measure('pnl', 'sum', name='p', weight='notional')
        with self.assertRaisesRegex(ValueError, 'uses wavg but has no weight column'):
            grid.measure('pnl', 'wavg', name='w')
        with self.assertRaisesRegex(ValueError, "a sort is 'asc' or 'desc', not 'up'"):
            grid.sort('region', 'up')
        # the screen groups and pivots by dimensions: a number is a measure until Column Properties makes it one
        with self.assertRaisesRegex(ValueError, "'nope' is not a column the row groups can take: the grid's dimensions "
                                                'are desk, region'):
            grid.group('nope')
        with self.assertRaisesRegex(ValueError, "'pnl' is a measure, and the row groups take dimensions"):
            grid.group('region', 'pnl')
        with self.assertRaisesRegex(ValueError, "the row groups name 'region' twice"):
            grid.group('region', 'region')
        # a measure of a grouped column is a calculated column of its own; a count of text is offered nowhere
        with self.assertRaisesRegex(ValueError, r"'count' is not offered on region, a String column: .* \(Show leaf "
                                                r"count counts a group's rows\)"):
            grid.measure('region', 'count', name='trades')
        with self.assertRaisesRegex(ValueError, "'desk' is already a column"):
            grid.measure('pnl', 'sum', name='desk')
        with self.assertRaisesRegex(ValueError, "'nope' is not a column of the grid"):
            grid.measure('nope', 'count', name='n')
        # no Aggregation is assumed for a measure (DataCube's default is its type's: column(name, kind='measure'))
        with self.assertRaises(TypeError):
            grid.measure('pnl')
        # Column Properties' own rules: a calculated column's kind is its editor's; a dimension takes no Aggregation
        with self.assertRaisesRegex(ValueError, "'region' is a dimension, and only a measure takes an Aggregation"):
            grid.column('region', aggregate='unique')
        with self.assertRaisesRegex(ValueError, "'region' is grouped or pivoted on: its kind cannot change"):
            grid.column('region', kind='measure')
        # a measure of a dimension is its Aggregation, which Column Properties refuses a dimension
        with self.assertRaisesRegex(ValueError, "'desk' is a dimension, and only a measure takes an Aggregation"):
            grid.measure('desk', 'unique')
        # configure goes by the same rules for a column's kind and Aggregation; a kind cleared is the type's own again
        # (D19: the command total, so an undo can clear what a command set), unless the column is grouped or pivoted on
        with self.assertRaisesRegex(ValueError, "'count' is not offered on desk"):
            grid.configure({'columns': {'desk': {'kind': 'measure', 'aggregateFn': 'count'}}})
        with self.assertRaisesRegex(ValueError, "'region' is grouped or pivoted on: its kind cannot change"):
            grid.configure({'columns': {'region': {'kind': None}}})
        grid.configure({'columns': {'desk': {'kind': None, 'excludedFromPivot': None}}})
        with self.assertRaisesRegex(ValueError, "desk's excludedFromPivot is true or false"):
            grid.configure({'columns': {'desk': {'excludedFromPivot': 'yes'}}})
        # the settings that decide the tree move it, by configure's own rule, as Properties does on the screen (the
        # grand total shown, the groups opened to the level; saved and read back with the cube)
        grid.configure({'showRootAggregation': True, 'initialExpandToLevel': 1})
        cube = next(c['cube'] for c in page.to_dict()['cubes'] if c['id'] == grid._view['cube'])
        self.assertEqual((cube['tree']['showTotals'], cube['tree'].get('expandTo')), (True, 1))
        grid.configure({'showRootAggregation': False, 'initialExpandToLevel': None})
        # a setting DataCube has not, a column the grid has not, a setting's name misspelt: refused, never kept
        with self.assertRaisesRegex(ValueError, "'maxRow' is not a grid's setting"):
            grid.configure({'maxRow': 5})
        with self.assertRaisesRegex(ValueError, "'notionl' is not a column of the grid"):
            grid.configure({'columns': {'notionl': {'hidden': True}}})
        with self.assertRaisesRegex(ValueError, "'aggregatefn' is not a column's setting"):
            grid.configure({'columns': {'pnl': {'aggregatefn': 'max'}}})
        with self.assertRaisesRegex(ValueError, "'fontsize' is not an appearance setting"):
            grid.configure({'appearance': {'fontsize': 14}})
        # a command DataCube does not know, or a field it does not take, is refused, never ignored (grid-commands.ts)
        with self.assertRaisesRegex(ValueError, r"'pivots' is not a grid's command: group, pivot, "):
            grid._apply([{'kind': 'pivots', 'columns': ['desk']}])
        with self.assertRaisesRegex(ValueError, r"a 'pivot' command takes columns, not pivotOn"):
            grid._apply([{'kind': 'pivot', 'pivotOn': ['desk']}])
        self.assertEqual(page.to_dict(), before, 'nothing refused changed the page')
        # a measure is its column's Aggregation, replacing the one it had; a named one, a calculated column with its own;
        # a weighted average with its weight; a count of a number; a sort on a calculated column
        grid.measure('notional', 'average')
        grid.measure('pnl', 'wavg', name='w_pnl', weight='notional').measure('pnl', 'count', name='trades')
        grid.measure('notional', 'max', name=' top_notional ').sort('w_pnl', 'desc')
        cube = page.to_dict()['cubes'][0]['cube']
        self.assertNotIn('measures', cube['query'])
        self.assertEqual(cube['configuration']['columns']['notional'], {'aggregateFn': 'average'})
        self.assertEqual([d['name'] for d in cube['query']['derived']], ['w_pnl', 'trades', 'top_notional'])
        self.assertEqual(cube['configuration']['columns']['w_pnl'],
                         {'aggregateFn': 'wavg', 'aggregationParameters': ['notional']})
        self.assertEqual(cube['query']['sorts'][-1], {'column': 'w_pnl', 'direction': 'desc'})
        # kept out of the pivot and back in, explicitly: each reaches the query (a measure spread again)
        grid.configure({'columns': {'pnl': {'excludedFromPivot': True}}})
        self.assertIs(next(c for c in grid._query['columns'] if c['name'] == 'pnl')['excludedFromPivot'], True)
        grid.configure({'columns': {'pnl': {'excludedFromPivot': False}}})
        self.assertIs(next(c for c in grid._query['columns'] if c['name'] == 'pnl')['excludedFromPivot'], False)
        # Column Properties' "none": the Aggregation cleared, the column on its type's default again
        grid.column('notional', aggregate=None)
        cube = page.to_dict()['cubes'][0]['cube']
        self.assertNotIn('notional', cube['configuration'].get('columns', {}))
        self.assertNotIn('aggregate', next(c for c in cube['query']['columns'] if c['name'] == 'notional'))
        names = {c['name'] for c in grid.view().columns}
        self.assertTrue({'notional', 'top_notional', 'w_pnl', 'trades'} <= names, names)
        # an integer made a dimension, as Column Properties makes one, is then grouped by
        counted = ll.Page('Counted').grid(trades().assign(year=2024), name='by_year')
        with self.assertRaisesRegex(ValueError, "'year' is a measure, and the row groups take dimensions"):
            counted.group('year')
        counted.column('year', kind='dimension').group('year')
        self.assertEqual(counted._query['rows'], ['year'])
        self.assertEqual(ll.Page('In one go').grid(trades().assign(year=2024), name='by_year_2', rows=['year'],
                                                     dimensions=['year'])._query['rows'], ['year'])

    def test_pandas_missing_value_is_no_nan_in_a_class_sample(self):
        # pandas' own missing value is no NaN in the rows: pa.Table.from_pandas reads it as null (frames.py), so a class's
        # sample writes it as null, as every missing value is -- only a NaN the data holds is refused (test_model)
        missing = ll.Page('Missing').grid(pd.DataFrame({'desk': ['FX', 'EQ'], 'rate': [1.5, float('nan')]}),
                                          name='missing_rate')
        # the sample DataCube's Create a Class… writes, as the writer is sent it (the host's fetch, watched here alone),
        # into a model home of the test's own
        home = tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))
        sent = []
        answer = _headless.answer_fetch

        def watched(method, url, body):
            if url.endswith('/model/createClass'):
                sent.append(json.loads(body))
            return answer(method, url, body)
        with mock.patch.dict(os.environ, {'LEGEND_LITE_HOME': home}), mock.patch.object(_headless, 'answer_fetch', watched):
            missing.create_class('Missing')
        rows = sent[-1]['class']['where']['samples']['missing_rate']['rows']
        self.assertEqual(sorted(r[1] is None for r in rows), [False, True])

    def test_a_sort_is_a_column_or_a_pivots_header_and_an_edit_keeps_its_open_groups(self):
        # a sort names a column the grid has, a calculated one too, or -- pivoted -- a pivot's header, as the screen's
        # Sorts panel and header menu offer them (grid-edits.ts setSorts); a sort of a column replaces the one it had
        detailed = ll.Page('Detailed').grid(trades(), name='detailed_trades', rows=['region'])
        detailed.measure('notional', 'max', name='top').sort('notional', 'desc').sort('top').sort('notional', 'asc')
        self.assertEqual(detailed._query['sorts'], [{'column': 'top', 'direction': 'asc'},
                                                    {'column': 'notional', 'direction': 'asc'}])
        # an add that would change nothing changes nothing, as the menu offers none (grid-edits.ts addSort)
        detailed.sort('top', 'asc')
        self.assertEqual(detailed._query['sorts'][0], {'column': 'top', 'direction': 'asc'})
        page = ll.Page('Sorted')
        grid = page.grid(trades(), name='sorted_trades', columns=['desk'], measures={'notional': 'sum'})
        with self.assertRaisesRegex(ValueError, "the sort names 'nope', which the grid does not have"):
            grid.sort('nope')
        grid.sort('FX__|__notional', 'desc')
        self.assertEqual(grid._query['sorts'], [{'column': 'FX__|__notional', 'direction': 'desc'}])
        grid.sort('FX__|__notional', 'asc')
        grid.pivot()
        self.assertEqual(grid._query['sorts'], [], "unpivoted: the pivot header's sort goes with its headers")
        grid.group('region')
        # its open groups, as DataCube keeps them: the screen's edits leave them, a regrouping too (app.ts #edit) -- a
        # path the new grouping has not is shown closed
        grid.expand('EMEA')
        opened = page.to_dict()['cubes'][0]['cube']['tree']
        self.assertEqual(opened['open'], [['EMEA']])
        grid.sort('region', 'desc')
        self.assertEqual(page.to_dict()['cubes'][0]['cube']['tree'], opened)
        grid.group('desk')
        self.assertEqual(page.to_dict()['cubes'][0]['cube']['tree'], opened)

    def test_a_following_chart_follows_its_grid_and_a_selection_goes_with_the_filter_that_held_it(self):
        # (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S2: V1, V2) a grid's edit kept by DataCube's one rule (page-commands.ts
        # followedPage, headless pageWithCube), as the screen keeps it: its live chart regrouped as the grid is
        page = ll.Page('Following')
        grid = page.grid(trades(), name='following_trades', rows=['region'], measures={'notional': 'sum'})
        grid.chart('bar')
        grid.group('desk')
        spec = page.to_dict()['views'][-1]['spec']
        self.assertNotIn('frozen', spec, 'the chart follows its grid')
        self.assertEqual(spec['x'], 'desk', 'regrouped as its grid is')
        # a chart's selection its grid's filter no longer holds goes with it (the screen's reconcile)
        doc = page.to_dict()
        mark = {'kind': 'condition', 'column': 'desk', 'operator': 'equal', 'value': 'FX'}
        doc['views'][-1]['selection'] = [mark]
        doc['cubes'][0]['cube']['query']['filter'] = mark
        loaded = ll.Page.load(doc, frames={'following_trades': trades()})
        self.assertEqual(loaded.to_dict()['views'][-1].get('selection'), [mark], 'held while the filter holds it')
        loaded.grids[0].filter(None)
        self.assertNotIn('selection', loaded.to_dict()['views'][-1])

    def test_a_calculated_column_is_edited_in_its_place_renamed_with_what_reads_it_and_removed(self):
        # the Edit Column window's OK and Delete, as the commands they build (grid-commands.ts editCalculated,
        # removeCalculated; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md G4, G5)
        page = ll.Page('Calculated')
        grid = page.grid(trades(), name='calculated_trades', rows=['region'])
        grid.calculate('double', '$x.notional->toOne() * 2').calculate('triple', '$x.notional->toOne() * 3')
        grid.sort('double', 'desc')
        grid.edit_calculated('double', '$x.notional->toOne() + $x.notional->toOne()', rename='twice')
        self.assertEqual([c['name'] for c in grid._query['derived']], ['twice', 'triple'], 'in its place')
        self.assertEqual(grid._query['sorts'], [{'column': 'twice', 'direction': 'desc'}], 'its sort follows it')
        self.assertEqual(page._commands[-1]['kind'], 'editCalculated')
        with self.assertRaisesRegex(ValueError, "'desk' is not a calculated column of the grid"):
            grid.edit_calculated('desk', '$x.notional->toOne()')
        # an edit starts from the column as it is, as Edit Column opens on it: a dimension stays one (the S1 audit's 4)
        grid.calculate('label', "$x.desk->toOne() + '!'", kind='dimension')
        grid.edit_calculated('label', "$x.desk->toOne() + '?'")
        self.assertEqual(next(c for c in grid._query['derived'] if c['name'] == 'label').get('kind'), 'dimension')
        grid.remove_calculated('label')
        grid.remove_calculated('triple')
        self.assertEqual([c['name'] for c in grid._query['derived']], ['twice'])
        with self.assertRaisesRegex(ValueError, "'triple' is not a calculated column of the grid"):
            grid.remove_calculated('triple')

    def test_groups_open_and_close_as_the_chevron_and_collapse_all_do_and_are_saved_with_the_page(self):
        # an open command, as the chevron builds one (grid-commands.ts): the page saves the groups its maker opened, so a
        # page shared or reopened shows them open (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md G1, G2)
        page = ll.Page('Opened')
        grid = page.grid(trades(), name='opened_trades', rows=['region', 'desk'])

        def open_paths():
            return page.to_dict()['cubes'][0]['cube']['tree']['open']

        grid.expand('EMEA', 'FX')
        self.assertEqual(open_paths(), [['EMEA'], ['EMEA', 'FX']], 'its parents open with it')
        self.assertEqual(page._commands[-1], {'kind': 'open', 'path': ['EMEA', 'FX'], 'open': True})
        grid.collapse('EMEA')
        self.assertEqual(open_paths(), [], 'the groups under it close with it')
        grid.expand('AMER').expand('EMEA').collapse_all()
        self.assertEqual(open_paths(), [])
        self.assertEqual(page._commands[-1], {'kind': 'collapseAll'})
        # Expand All: every group above the detail rows, saved as the level the groups open to
        grid.expand_all()
        self.assertEqual(page._commands[-1], {'kind': 'expandAll'})
        self.assertEqual(page.to_dict()['cubes'][0]['cube']['tree'].get('expandTo'), 2)
        grid.collapse_all()
        grid.expand('EMEA', 'FX', 'deeper')
        self.assertEqual(open_paths(), [], 'a path deeper than the groups opens nothing')
        with self.assertRaisesRegex(ValueError, 'each text or null'):
            grid.expand('EMEA', 2021)
        with self.assertRaisesRegex(ValueError, 'the grand total is no group'):
            grid.expand()

    def test_the_open_groups_are_set_whole_as_an_undo_puts_a_collapsed_tree_back(self):
        # a tree command (grid-commands.ts; docs/DATACUBE_LIVE_EDITING_2026_10_24.md W1): the saved tree's own shape, set
        # whole -- what an undo of Collapse All tells on the screen, which expand and collapse cannot put back
        page = ll.Page('Tree')
        grid = page.grid(trades(), name='tree_trades', rows=['region', 'desk'])

        def tree():
            return page.to_dict()['cubes'][0]['cube']['tree']

        grid.tree(['EMEA'], ['EMEA', 'FX'], totals=True, expand_to=1, closed=[['AMER']])
        self.assertEqual(tree(), {'open': [['EMEA'], ['EMEA', 'FX']], 'showTotals': True, 'expandTo': 1, 'closed': [['AMER']]})
        self.assertEqual(page._commands[-1]['kind'], 'tree')
        grid.collapse_all()
        self.assertEqual(tree(), {'open': [], 'showTotals': True}, 'Collapse All forgets the level and the closes')
        grid.tree(['AMER'], totals=False)
        self.assertEqual(tree(), {'open': [['AMER']], 'showTotals': False})
        with self.assertRaisesRegex(ValueError, 'the grand total is no group'):
            grid.tree([])
        with self.assertRaisesRegex(ValueError, 'a whole number, 0 or more'):
            grid.tree(['EMEA'], expand_to=-1)

    def test_a_calculated_column_goes_before_the_one_named(self):
        # before= on calculate and edit_calculated (grid-commands.ts; docs/DATACUBE_LIVE_EDITING_2026_10_24.md W1): a
        # column in its place, as an undo puts one back
        page = ll.Page('Before')
        grid = page.grid(trades(), name='before_trades', rows=['region'])
        grid.calculate('a', '$x.notional->toOne() * 2').calculate('c', '$x.notional->toOne() * 3')
        grid.calculate('b', '$x.notional->toOne() * 4', before='c')
        self.assertEqual([c['name'] for c in grid._query['derived']], ['a', 'b', 'c'])
        self.assertEqual(page._commands[-1]['before'], 'c')
        grid.edit_calculated('c', '$x.notional->toOne() * 5', before='a')
        self.assertEqual([c['name'] for c in grid._query['derived']], ['c', 'a', 'b'])
        with self.assertRaisesRegex(ValueError, "'zzz' is not a calculated column at the row level"):
            grid.calculate('d', '$x.notional->toOne()', before='zzz')

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
        # each value as JSON carries it, tagged where JSON has no form for it, and judged by DataCube's reader
        # (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D12): Python checks none of it
        for wrong in (None, float('nan'), float('-inf'), Decimal('NaN')):
            with self.assertRaisesRegex(ValueError, 'takes text, a number'):
                grid.filter(('notional', 'greaterThan', wrong))
        with self.assertRaises(TypeError):
            grid.filter(('notional', 'greaterThan', object()))
        with self.assertRaisesRegex(ValueError, r'a filter is a condition \(column, operator, value\)'):
            grid.filter('desk')
        with self.assertRaisesRegex(ValueError, r'a condition is \(column, operator\) or \(column, operator, value\)'):
            grid.filter(('desk', 'equal', 'FX', 'EQ'))
        with self.assertRaisesRegex(ValueError, "not one of DataCube's aggregates"):
            grid.measure('pnl', 'total')
        with self.assertRaisesRegex(ValueError, 'is not one this DataCube draws'):
            grid.chart('violin', x='region')
        with self.assertRaisesRegex(ValueError, 'not a column'):
            grid.group('nope')
        # DataCube's one name rule (page-names.ts givenName): a tile's or a page's blank is refused, in its words
        with self.assertRaisesRegex(ValueError, "a tile's name is not blank"):
            grid.chart('bar', title='')
        with self.assertRaisesRegex(ValueError, "a page's name is not blank"):
            page.name = '  '
        with self.assertRaisesRegex(ValueError, "a page's name: a page's name is not blank"):
            ll.Page('  ')
        # what is not text: refused in DataCube's words, as its commands' readers refuse it (the S7 confirmation audit's
        # Y2-4: Python refused it in words of its own)
        with self.assertRaisesRegex(ValueError, "a tile's title is text, not 3"):
            grid.title = 3
        with self.assertRaisesRegex(ValueError, "a page's name is text, not null"):
            page.name = None
        with self.assertRaisesRegex(ValueError, "a page's name is text, not 5"):
            ll.Page(5)
        with self.assertRaisesRegex(ValueError, "a sheet's name is text, not 5"):
            page.sheet(5)
        with self.assertRaisesRegex(ValueError, "a tile's title is text, not 5"):
            grid.copy(title=5)
        # ... an arrangement's shape and a calculated column's name too (the third round's 2); an expression is Python's
        # to make its lambda's text of, so it alone is refused here
        with self.assertRaisesRegex(ValueError, "the layout picker's shape is text, not 5"):
            page.sheets[0].arrange(5)
        with self.assertRaisesRegex(ValueError, "a calculated column's name is text, not 5"):
            grid.calculate(5, '$x.notional')
        with self.assertRaisesRegex(ValueError, 'an expression is text, not 5'):
            grid.calculate('five', 5)
        # an edit naming no column is refused, never taken for a new one (the S7 audit's fourth round's 3)
        with self.assertRaisesRegex(ValueError, 'the calculated column edited is named by text, not null'):
            grid.edit_calculated(None, '$x.notional', rename='y')
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
        # a relative date's word is DataCube's to judge (gridFilter), never a list of Python's
        with self.assertRaisesRegex(ValueError, r'takes text, a number, a boolean, a date or a timestamp, \{"relative"'):
            dated.filter(('day', 'lessThan', {'relative': 'yesterday'}))
        # a timestamp, as its exact text (D12): a datetime column compared with a Python datetime
        stamped = page.grid(pd.DataFrame({'at': [datetime.datetime(2024, 1, 31, 10, 30), datetime.datetime(2024, 2, 2, 9, 0)],
                                          'qty': [1, 2]}), name='stamped')
        stamped.filter(('at', 'lessThan', datetime.datetime(2024, 2, 1, 9, 0)))
        self.assertEqual(page.to_dict()['cubes'][2]['cube']['query']['filter']['value'], '2024-02-01T09:00:00')
        self.assertEqual(len(stamped.view().rows), 1)

    def test_a_grid_refused_leaves_the_page_and_the_frames_as_they_were(self):
        page, _, _ = self.built()
        before = page.to_dict()
        commands = list(page._commands)
        # the last: its grouping applied, then its filter refused -- the grouping's command taken back with the grid
        for given in ({'rows': ['nope']}, {'measures': {'head': 'total'}}, {'filter': ('head', 'nope', 1)},
                      {'sort': [('head', 'sideways')]}, {'rows': ['desk'], 'filter': ('head', 'nope', 1)}):
            with self.assertRaises(ValueError):
                page.grid(desks(), name='desks', **given)
            self.assertEqual(page.to_dict(), before)
            self.assertEqual(page._commands, commands, 'no command of the grid refused is kept')
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
        with self.assertRaisesRegex(ValueError, r"'trades' serves another frame already: grid\.update\(frame\) replaces"):
            page.grid(trades(), name='trades')
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
        # a tile stacked with itself is no stack, in DataCube's words (page-commands.ts 'stack'); one taken off, or not a
        # tile -- a Python handle, not a page rule -- is refused before anything is asked
        with self.assertRaisesRegex(ValueError, '^a stack is two tiles or more$'):
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
        # its grid's filter holds it, as a chart's click leaves it (a selection no filter holds goes: followedPage)
        doc['cubes'][0]['cube']['query']['filter'] = chart_view['selection'][0]
        chart_view['spec']['options'] = {'orientation': 'vertical', 'stack': 'none', 'sort': {'by': 'x', 'direction': 'asc'},
                                         'limit': 25, 'labels': False, 'legend': 'bottom'}
        doc['sheets'][0]['layout'] = {'kind': 'bands', 'fit': True, 'bands': [
            {'height': 0.62, 'node': {'split': 'row', 'parts': [
                {'node': {'stack': [grid.id, other.id]}, 'size': 0.35}, {'node': {'tile': chart.id}, 'size': 0.65}]}}]}
        loaded = ll.Page.load(doc, frames={'trades': trades(), 'desks': desks()})
        self.assertEqual(loaded.to_dict(), doc)
        # changed in Python: what it does not model is still there
        loaded.grids[0].group('desk')
        # a part of the grid-wide appearance is merged into what it has, as the screen merges a host's patch
        loaded.grids[1].configure({'appearance': {'fontSize': 14}})
        self.assertEqual(loaded.to_dict()['cubes'][1]['cube']['configuration'].get('appearance'), {'fontSize': 14})
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
        with self.assertRaisesRegex(ValueError, r'written by a newer version \(99 > 6\)'):
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
        # an older page is not read (the user, 2026-10-17), in DataCube's words
        with self.assertRaisesRegex(ValueError, r'saved by an older version \(3 < 6\): it is no longer read'):
            ll.Page.load({**doc, 'version': 3}, frames={'trades': trades()})


class Abilities(Paged):
    """What a person does on the screen, done from Python by the command the screen's gesture builds and applied by
    DataCube's own function (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S6; the names the user's, D21)."""

    def notionals(self, grid):
        view = grid.view()
        at = [c['name'] for c in view.columns].index('notional')
        return [row[at] for row in view.rows]

    def test_its_sorts_are_replaced_one_cleared_and_all_cleared(self):
        # (K1) grid.sorts: the Sorts panel's whole list (sorts); grid.clear_sort: the header menu's Clear Sort (clearSort)
        page = ll.Page('Sorted')
        grid = page.grid(trades(), name='sorted_trades')
        grid.sorts([('notional', 'desc'), 'region'])
        self.assertEqual(grid._query['sorts'], [{'column': 'notional', 'direction': 'desc'},
                                                {'column': 'region', 'direction': 'asc'}])
        self.assertEqual(self.notionals(grid), [250.0, 100.0, 75.5])
        grid.clear_sort('notional')
        self.assertEqual(grid._query['sorts'], [{'column': 'region', 'direction': 'asc'}])
        self.assertEqual(page._commands[-1], {'kind': 'clearSort', 'column': 'notional'})
        # refused in DataCube's words, nothing set
        with self.assertRaisesRegex(ValueError, r"the grid is not sorted by 'desk' \(it is sorted by region\)"):
            grid.clear_sort('desk')
        with self.assertRaisesRegex(ValueError, r"the sorts name 'region' twice"):
            grid.sorts(['region', ('region', 'desc')])
        with self.assertRaisesRegex(ValueError, r"a sort is a column or \(column, 'asc' \| 'desc'\), not 3"):
            grid.sorts([3])
        self.assertEqual(grid._query['sorts'], [{'column': 'region', 'direction': 'asc'}])
        # none: every sort cleared, as Clear All Sorts clears them
        grid.sorts([])
        self.assertEqual(grid._query['sorts'], [])

    def test_a_header_click_s_sort_is_turned_by_the_screen_s_one_rule(self):
        # (the user, 2026-10-22: one sort API the screen's click and a program call) grid.toggle_sort: the header click's
        # command, made by DataCube's rule (headless gridGesture's header.sort), applied by the one rule every sort is
        # (clickSort)
        page = ll.Page('Clicked')
        grid = page.grid(trades(), name='clicked_trades')
        sorts = lambda: [(s['column'], s['direction']) for s in grid._query['sorts']]  # noqa: E731
        # a number from the largest, then the other way, then none
        grid.toggle_sort('notional')
        self.assertEqual(sorts(), [('notional', 'desc')])
        self.assertEqual(page._commands[-1], {'kind': 'clickSort', 'column': 'notional', 'first': 'desc', 'chained': False})
        self.assertEqual(self.notionals(grid), [250.0, 100.0, 75.5])
        grid.toggle_sort('notional')
        self.assertEqual(sorts(), [('notional', 'asc')])
        grid.toggle_sort('notional')
        self.assertEqual(sorts(), [])
        # text from A; chained, after the others; alone, every other taken off
        grid.toggle_sort('notional')
        grid.toggle_sort('desk', chained=True)
        self.assertEqual(sorts(), [('notional', 'desc'), ('desk', 'asc')])
        grid.toggle_sort('region')
        self.assertEqual(sorts(), [('region', 'asc')])
        # the groups' order, as the tree column's header turns it
        grid.group('region')
        grid.toggle_group_sort()
        self.assertEqual(page._commands[-1], {'kind': 'clickTreeSort', 'chained': False})
        self.assertEqual(sorts(), [])
        with self.assertRaisesRegex(ValueError, r"'nope' is no header of the grid \("):
            grid.toggle_sort('nope')

    def test_a_condition_is_added_to_its_filter_and_ed_with_what_it_has(self):
        # (K2) grid.add_filter: a cell's Add Filter (addFilter), the condition built as grid.filter builds one
        page = ll.Page('Filtered')
        grid = page.grid(trades(), name='filtered_trades')
        grid.filter(('desk', 'equal', 'Rates'))
        grid.add_filter(('region', 'equal', 'EMEA'))
        self.assertEqual(sorted(grid.to_pandas()['notional']), [75.5])
        self.assertEqual(page._commands[-1], {'kind': 'addFilter', 'condition': {
            'kind': 'condition', 'column': 'region', 'operator': 'equal', 'value': 'EMEA'}})
        # one condition: a group is filter's to set; a column the grid has not, refused in its words
        with self.assertRaisesRegex(ValueError, 'a filter added is one condition'):
            grid.add_filter(('or', [('region', 'equal', 'EMEA'), ('desk', 'equal', 'FX')]))
        with self.assertRaisesRegex(ValueError, 'nope'):
            grid.add_filter(('nope', 'equal', 'x'))
        self.assertEqual(sorted(grid.to_pandas()['notional']), [75.5])

    def test_a_value_list_keeps_the_values_ticked_in_place_of_the_columns_own_conditions(self):
        # grid.filter_values: the Filter Values window's OK (DataCube's value-filter.ts, the 'filter.values' gesture)
        page = ll.Page('Values')
        grid = page.grid(trades(), name='valued_trades')
        grid.filter([('desk', 'equal', 'Rates'), ('region', 'equal', 'AMER')])
        self.assertEqual(sorted(grid.to_pandas()['notional']), [250.0])
        grid.filter_values('region', ['EMEA', 'AMER'])
        self.assertEqual(sorted(grid.to_pandas()['notional']), [75.5, 250.0], "region's own condition replaced, desk's kept")
        self.assertEqual(page._commands[-1]['kind'], 'refilter')
        grid.filter_values('region', ['EMEA', None])
        self.assertEqual(sorted(grid.to_pandas()['notional']), [75.5], 'the blanks kept too: there are none')
        grid.filter_values('region', None)
        self.assertEqual(sorted(grid.to_pandas()['notional']), [75.5, 250.0], "every value: region's own conditions off")
        with self.assertRaisesRegex(ValueError, "keep at least one value of 'region'"):
            grid.filter_values('region', [])

    def test_a_date_range_relative_to_today_is_two_conditions_the_database_computes(self):
        # grid.filter_date_range: the menu's Filter > Date Range (DataCube's date-ranges.ts)
        page = ll.Page('Dated')
        today = datetime.date.today()
        frame = pd.DataFrame({'day': [today, today - datetime.timedelta(days=400)], 'notional': [1.0, 2.0]})
        grid = page.grid(frame, name='dated')
        grid.filter_date_range('day', 'last30Days')
        self.assertEqual(list(grid.to_pandas()['notional']), [1.0])
        condition = page._commands[-1]['add'][0]
        self.assertEqual(condition['value'], {'relative': 'today', 'offset': -29, 'unit': 'DAYS'})
        # a range from a period's start (firstDayOfYear, adjust): this year's day kept, the one 400 days back not
        grid.filter_date_range('day', 'thisYear')
        self.assertEqual(list(grid.to_pandas()['notional']), [1.0], 'the range replaced the last one')
        grid.filter_date_range('day', 'lastYear')
        self.assertEqual(list(grid.to_pandas()['notional']), [2.0] if (today - datetime.timedelta(days=400)).year == today.year - 1 else [])
        with self.assertRaisesRegex(ValueError, 'a date range is today'):
            grid.filter_date_range('day', 'someday')
        # grid.date_filter: a flow's Date Filter, its filter those dates alone -- between two days, both kept, or a range
        back = (today - datetime.timedelta(days=400)).isoformat()
        grid.date_filter('day', start=back, end=back)
        self.assertEqual(list(grid.to_pandas()['notional']), [2.0])
        grid.date_filter('day', start=back, end=today.isoformat())
        self.assertEqual(sorted(grid.to_pandas()['notional']), [1.0, 2.0])
        grid.date_filter('day', 'last30Days')
        self.assertEqual(list(grid.to_pandas()['notional']), [1.0])
        with self.assertRaisesRegex(ValueError, 'is after its last'):
            grid.date_filter('day', start=today.isoformat(), end=back)
        with self.assertRaisesRegex(ValueError, 'one of them'):
            grid.date_filter('day')

    def test_the_top_n_groups_by_a_measure_are_ranked_by_the_database_each_parents_own(self):
        # grid.top: the menu's Top 10 / Bottom 10 (DataCube's snapshot.ts TopN, the filter.topN gesture)
        page = ll.Page('Ranked')
        grid = page.grid(trades(), name='ranked_trades', rows=['region'], measures={'notional': 'sum'})
        grid.top(1, 'notional')
        self.assertEqual(groups_of(grid.view(), 'notional'), {'AMER': 250.0}, 'AMER 250 over EMEA 175.5')
        self.assertEqual(page._commands[-1], {'kind': 'topN', 'topN': {'by': 'notional', 'n': 1}})
        grid.top(1, 'notional', bottom=True)
        self.assertEqual(groups_of(grid.view(), 'notional'), {'EMEA': 175.5})
        grid.top(None)
        self.assertEqual(groups_of(grid.view(), 'notional'), {'EMEA': 175.5, 'AMER': 250.0})
        with self.assertRaisesRegex(ValueError, 'whole number from 1'):
            grid.top(0, 'notional')

    def test_copy_of_grid_is_a_grid_of_its_own_beside_it_starting_as_it_is(self):
        # (K10) grid.copy: Copy of Grid -- the copy's cube by DataCube's one rule (cube-document.ts cubeCopied), the one
        # the screen's copy writes (one-api-gestures.test.ts holds the two the same)
        page = ll.Page('Copied')
        grid = page.grid(trades(), name='copied_trades', rows=['region'], measures={'notional': 'sum'})
        grid.expand('EMEA')
        copied = grid.copy()
        self.assertEqual(len(page.grids), 2)
        self.assertEqual(copied.sheet.id, grid.sheet.id)
        self.assertEqual(copied._query, grid._query)
        self.assertEqual(copied._cube['tree']['open'], [], 'no group open, as a new grid starts')
        self.assertNotEqual(grid._cube['tree']['open'], [])
        self.assertEqual(groups_of(copied.view(), 'notional'), {'EMEA': 175.5, 'AMER': 250.0})
        # a cube of its own: an edit of the copy leaves the grid as it was
        copied.group('desk')
        self.assertEqual(grid._query['rows'], ['region'])
        # added as the screen adds it: on the sheet of the grid copied (at its bottom: page-commands.ts addGrid)
        self.assertIn(('addGrid', grid.id), [(c['kind'], c.get('near')) for c in page._page_commands])

    def test_a_sheet_is_moved_among_the_sheets_and_removed_with_its_tiles(self):
        # (K8) sheet.move: its tab's Move Left and Move Right (moveSheet); page.remove(sheet): its tab's Delete
        # (removeSheet), its tiles with it
        page = ll.Page('Sheets')
        first = page.sheets[0]
        page.grid(trades(), name='sheet_trades')
        second = page.sheet('Charts')
        third = page.sheet('Notes')
        on_third = page.grid(desks(), name='sheet_desks', sheet=third)
        third.move(0)
        self.assertEqual([s.id for s in page.sheets], [third.id, first.id, second.id])
        third.move(99)
        self.assertEqual([s.id for s in page.sheets], [first.id, second.id, third.id], 'a place past the end is the end')
        # refused in DataCube's words, moveSheet's own (the S7 audit's Y-11: Python spelled them itself)
        with self.assertRaisesRegex(ValueError, 'a sheet\'s place is a number, not "1"'):
            first.move('1')
        with self.assertRaisesRegex(ValueError, "a sheet's place is a whole number, not 1.5"):
            first.move(1.5)
        page.remove(third)
        self.assertEqual([s.id for s in page.sheets], [first.id, second.id])
        self.assertEqual(len(page.grids), 1)
        with self.assertRaisesRegex(ValueError, 'is no longer on its page'):
            on_third.view()
        with self.assertRaisesRegex(ValueError, 'is no longer on its page'):
            third.name
        page.remove(second)
        with self.assertRaisesRegex(ValueError, 'a page has one sheet at least'):
            page.remove(first)

    def test_columns_as_the_column_editor_makes_them(self):
        # (K4) grid.window, grid.child_aggregate, grid.explode and grid.calculate(level='group'): the column editor's
        # own forms, made headless as it makes them (gridColumn: column-draft.ts draftColumn), refused in its words
        page = ll.Page('Columns')
        grid = page.grid(trades(), name='column_trades', rows=['region', 'desk'], measures={'notional': 'sum'})
        grid.window('running', 'sum', column='notional', order=[('notional', 'desc')], frame='running')
        running = next(d for d in grid._query['derived'] if d['name'] == 'running')
        self.assertEqual(running['window'], {'fn': 'sum', 'column': 'notional', 'partition': [],
                                             'order': [{'column': 'notional', 'direction': 'desc'}], 'frame': 'running'})
        grid.child_aggregate('largest', 'max', 'notional')
        largest = next(d for d in grid._query['groupDerived'] if d['name'] == 'largest')
        self.assertEqual(largest['childAggregate'], {'fn': 'max', 'of': 'notional'})
        grid.calculate('doubled', '$x.notional->toOne() * 2', level='group')
        self.assertIn('doubled', [d['name'] for d in grid._query['groupDerived']])
        self.assertEqual(groups_of(grid.view(), 'doubled'), {'EMEA': 351.0, 'AMER': 500.0})
        with self.assertRaisesRegex(ValueError, 'a group-level column has no kind'):
            grid.calculate('kinded', '$x.notional', level='group', kind='measure')
        with self.assertRaisesRegex(ValueError, 'mode'):
            grid.window('middle', 'mode', column='notional')
        with self.assertRaisesRegex(ValueError, "a child aggregate is a group-level column"):
            grid._column({'name': 'rowwise', 'mode': 'children', 'child': {'fn': 'max', 'of': 'notional'}})
        # explode: each row once per element of what its expression gives
        flat = page.grid(trades(), name='explode_trades')
        flat.explode('each', '[1, 2]', kind='dimension')
        self.assertTrue(next(d for d in flat._query['derived'] if d['name'] == 'each')['unnest'])
        self.assertEqual(len(flat.to_pandas()), 6)
        with self.assertRaisesRegex(ValueError, 'a row-level expression only'):
            flat._column({'name': 'late', 'expression': 'x|[1, 2]', 'level': 'group', 'explode': True})

    def test_a_measure_at_its_parent_or_the_grand_total(self):
        # grid.measure_at and grid.show_as's shares: the measure as the parent group or the grand total has it, the
        # database's (snapshot.ts Grain) -- the column editor's Parent or Grand Total and the header menu's Show As
        page = ll.Page('Grains')
        grid = page.grid(trades(), name='grain_trades', rows=['region'], measures={'notional': 'sum'})
        grid.measure_at('whole', 'notional', at='all')
        whole = next(d for d in grid._query['groupDerived'] if d['name'] == 'whole')
        self.assertEqual(whole['grain'], {'of': 'notional', 'at': 'all'})
        self.assertEqual(groups_of(grid.view(), 'whole'), {'EMEA': 425.5, 'AMER': 425.5})
        grid.show_as('notional', 'percentOfTotal')
        shares = groups_of(grid.view(), 'notional_percentOfTotal')
        self.assertAlmostEqual(shares['EMEA'], 175.5 / 425.5)
        self.assertAlmostEqual(shares['AMER'], 250.0 / 425.5)
        with self.assertRaisesRegex(ValueError, 'a measure at a grain is a group-level column'):
            grid._column({'name': 'rowwise', 'mode': 'grain', 'grain': {'of': 'notional', 'at': 'all'}})
        with self.assertRaisesRegex(ValueError, r"its parent's \('parent'\) or the grand total's \('all'\)"):
            grid.measure_at('odd', 'notional', at='everything')

    def test_presets_as_the_header_menu_adds_them(self):
        # (K4) grid.grain, grid.show_as, grid.bins: the header menu's By Month and Show As, by the one rule the menu
        # now asks too (grid-presets.ts presetCommands; its names, a taken one moved on to _2)
        dated = pd.DataFrame({'traded': pd.to_datetime(['2024-01-05', '2024-01-20', '2024-02-03']).date,
                              'notional': [100.0, 250.0, 75.5]})
        page = ll.Page('Presets')
        grid = page.grid(dated, name='dated_trades', measures={'notional': 'sum'})
        grid.grain('traded', 'month')
        grid.grain('traded', 'month')
        self.assertEqual([d['name'] for d in grid._query['derived']], ['traded_month', 'traded_month_2'])
        grid.group('traded_month')
        self.assertEqual(sorted(groups_of(grid.view(), 'notional').values()), [75.5, 350.0])
        grid.show_as('notional', 'runningTotal')
        self.assertIn('notional_runningTotal', [d['name'] for d in grid._query['groupDerived']])
        grid.bins('notional', width=100)
        self.assertEqual([d['name'] for d in grid._query['derived']][-2:], ['notional_bins', 'notional_bins__upper'])
        with self.assertRaisesRegex(ValueError, 'a period is day, week, month, quarter, year, not "fortnight"'):
            grid.grain('traded', 'fortnight')
        with self.assertRaisesRegex(ValueError, 'bins are a width or a count, one of them'):
            grid.bins('notional')

    def test_text_tools_as_a_flow_box_adds_them(self):
        # (M3) grid.split, grid.regex, grid.cleanse: a flow's Text To Columns, RegEx and Data Cleansing, by the one rule
        # its windows ask (text-tool-columns.ts textToolCommands), computed by the database
        names = pd.DataFrame({'who': ['  Ana  Diaz', 'ben ross!', None], 'code': ['FX-1', 'IR-22', 'CR']})
        page = ll.Page('Text tools')
        grid = page.grid(names, name='names')
        grid.split('code', '-', 2)
        grid.regex('code', '[0-9]+', 'extract')
        grid.regex('code', '^FX', name='is_fx')
        grid.cleanse(['who'], blanks=True, punctuation=True, spaces=True, case='upper')
        # an empty value None, however the frame holds it
        rows = [{k: None if pd.isna(v) else v for k, v in r.items()} for r in grid.to_pandas().sort_values('code').to_dict('records')]
        # a part a value has not got, empty (splitPart's String[0..1]); a pattern it does not match, empty
        self.assertEqual([(r['code_1'], r['code_2']) for r in rows], [('CR', None), ('FX', '1'), ('IR', '22')])
        self.assertEqual([r['code_extracted'] for r in rows], [None, '1', '22'])
        self.assertEqual([r['is_fx'] for r in rows], [False, True, False])
        self.assertEqual([r['who_clean'] for r in rows], ['', 'ANA DIAZ', 'BEN ROSS'])
        with self.assertRaisesRegex(ValueError, 'it splits on a delimiter: type one'):
            grid.split('code', '', 2)
        # into rows: each part its own row, by an explode
        tags = page.grid(pd.DataFrame({'id': [1, 2], 'tags': ['a,b,c', 'd']}), name='tags')
        tags.split('tags', ',', name='tag', rows=True)
        self.assertEqual(sorted((int(r['id']), r['tag']) for r in tags.to_pandas().to_dict('records')),
                         [(1, 'a'), (1, 'b'), (1, 'c'), (2, 'd')])
        # tokenize: each match its own row (Alteryx's Tokenize), the database's matches exploded; no match, no row
        coded = page.grid(pd.DataFrame({'id': [1, 2, 3], 'note': ['FX12 and IR7', 'none', None]}), name='coded')
        coded.regex('note', '[0-9]+', 'tokenize')
        self.assertEqual(sorted((int(r['id']), r['note_token']) for r in coded.to_pandas().to_dict('records')), [(1, '12'), (1, '7')])
        with self.assertRaisesRegex(ValueError, 'its mode is match, replace, extract or tokenize, not "find"'):
            grid.regex('code', 'x', 'find')
        with self.assertRaisesRegex(ValueError, 'pick what to do to them'):
            grid.cleanse(['who'], trim=False)

    def test_empties_imputed_as_a_flow_box_fills_them(self):
        # (M3) grid.impute: a flow's Imputation (impute.ts imputeCommands) -- a value, or the column's own average,
        # smallest or largest across every row, the database's
        holes = pd.DataFrame({'desk': ['FX', 'Rates', 'Rates', 'FX'], 'notional': [100.0, None, 300.0, None],
                              'qty': pd.array([1, None, 3, 4], dtype='Int64')})
        page = ll.Page('Imputation')
        grid = page.grid(holes, name='holes')
        grid.impute(['notional'], fill='average')
        grid.impute(['qty'], fill=0)
        rows = grid.to_pandas().sort_values(['desk', 'notional'], na_position='last').to_dict('records')
        self.assertEqual(sorted(r['notional_filled'] for r in rows), [100.0, 200.0, 200.0, 300.0])
        self.assertEqual(sorted(int(r['qty_filled']) for r in rows), [0, 1, 3, 4])
        with self.assertRaisesRegex(ValueError, "only a number's empties are filled"):
            grid.impute(['desk'])
        with self.assertRaisesRegex(ValueError, 'holds whole numbers: fill it with one'):
            grid.impute(['qty'], fill=0.5)
        with self.assertRaisesRegex(ValueError, "'notional_filled' is already a column"):
            grid.impute(['notional'], fill='largest')
        # its median, the database's: 100, 300 and 900's middle
        skewed = page.grid(pd.DataFrame({'n': [100.0, None, 300.0, 900.0]}), name='skewed')
        skewed.impute(['n'], fill='median')
        self.assertEqual(sorted(r['n_filled'] for r in skewed.to_pandas().to_dict('records')), [100.0, 300.0, 300.0, 900.0])

    def test_dates_read_and_taken_apart_as_a_flow_box_does(self):
        # (M3) grid.parse_date, grid.date_part: a flow's DateTime's other two ways (date-tools.ts dateToolCommands),
        # the database's strptime and date parts
        days = pd.DataFrame({'booked': ['03/01/2024', '15/02/2024'], 'n': [1, 2]})
        page = ll.Page('Dates')
        grid = page.grid(days, name='days')
        grid.parse_date('booked', 'dd/MM/yyyy')
        grid.date_part('booked_date', 'month')
        rows = grid.to_pandas().sort_values('n').to_dict('records')
        self.assertEqual([str(r['booked_date'])[:10] for r in rows], ['2024-01-03', '2024-02-15'])
        self.assertEqual([int(r['booked_date_month']) for r in rows], [1, 2])
        with self.assertRaisesRegex(ValueError, 'it reads by a format'):
            grid.parse_date('booked', ' ')
        # a value the format cannot read refuses the rows, in the database's words
        odd = page.grid(pd.DataFrame({'booked': ['03/01/2024', 'not a date']}), name='odd')
        odd.parse_date('booked', 'dd/MM/yyyy')
        with self.assertRaisesRegex(ValueError, 'Could not parse string'):
            odd.to_pandas()
        # a date written as text by a format, the database's strftime: an empty date an empty text
        dated = page.grid(pd.DataFrame({'on': pd.to_datetime(['2024-01-03', None, '2024-02-15']).date, 'n': [1, 2, 3]}), name='dated')
        dated.format_date('on', 'dd.MM.yyyy')
        said = {r['n']: r['on_text'] for r in dated.to_pandas().to_dict('records')}
        self.assertEqual(said[1], '03.01.2024')
        self.assertEqual(said[3], '15.02.2024')
        self.assertTrue(said[2] is None or pd.isna(said[2]), f'an empty date, an empty text: {said[2]!r}')
        with self.assertRaisesRegex(ValueError, 'a format has no'):
            dated.format_date('on', '{yyyy}')

    def test_columns_made_rows_as_a_flow_box_transposes_them(self):
        # (M4) grid.transpose: a flow's Transpose (transpose.ts transposeCommands) -- each row once per column, its name
        # and its value side by side, the keys beside them; the database repeats the rows
        page = ll.Page('Transpose')
        grid = page.grid(pd.DataFrame({'desk': ['FX', 'Rates'], 'q1': [1.0, 2.0], 'q2': [3.0, 4.0], 'note': ['a', 'b']}), name='quarters')
        grid.transpose(['q1', 'q2'], keys=['desk'], name='quarter', value='amount')
        rows = grid.to_pandas()
        self.assertEqual(sorted((r['desk'], r['quarter'], r['amount']) for r in rows.to_dict('records')),
                         [('FX', 'q1', 1.0), ('FX', 'q2', 3.0), ('Rates', 'q1', 2.0), ('Rates', 'q2', 4.0)])
        # the other columns left out: hidden, so what reads it reads the keys, the name and the value
        self.assertTrue(all(grid._cube['configuration']['columns'][c]['hidden'] for c in ('q1', 'q2', 'note')))
        with self.assertRaisesRegex(ValueError, 'its columns hold numbers and text'):
            page.grid(pd.DataFrame({'a': [1.0], 'b': ['x']}), name='mixed').transpose(['a', 'b'])

    def test_a_column_found_among_another_grid_and_replaced_as_find_replace(self):
        # (M2) page.find_replace: a flow's Find Replace (find-replace.ts findReplaceCommands) -- every row of mine joined to
        # theirs on my column equal to theirs, their replacement where found, my own where not; theirs left out
        page = ll.Page('Find Replace')
        flow = page.flow()
        mine = flow.grid(trades(), name='booked_trades')
        names = flow.grid(pd.DataFrame({'desk': ['FX'], 'name': ['Foreign exchange']}), name='desk_names')
        replaced = page.find_replace(mine, names, 'desk', 'desk', 'name')
        # what it hands on, read by the box after it: desk, replaced where found
        rows = flow.grid(input=replaced).to_pandas()
        self.assertEqual(sorted(rows['desk']), ['Foreign exchange', 'Rates', 'Rates'], 'found replaced, not found as it was')
        self.assertNotIn('name', rows.columns, 'theirs left out')
        with self.assertRaisesRegex(ValueError, "the grid has no column 'nope'"):
            page.find_replace(mine, names, 'desk', 'desk', 'name')._preset({'kind': 'findReplace', 'column': 'nope', 'with': 'name'})
        # a key theirs repeats matches its first row only (the join's once, C13): no row of mine repeats
        twice = flow.grid(pd.DataFrame({'desk': ['FX', 'FX'], 'name': ['Forex', 'Foreign exchange']}), name='desk_names_twice')
        once = flow.grid(input=page.find_replace(mine, twice, 'desk', 'desk', 'name')).to_pandas()
        self.assertEqual(sorted(once['desk']), ['Foreign exchange', 'Rates', 'Rates'], 'each row once, the first of theirs (rising)')

    def test_rows_counted_as_count_records(self):
        # (M4) grid.count_records: a flow's Count Records (count-records.ts) -- one row, one column; the box after it reads
        # the count alone
        page = ll.Page('Count Records')
        flow = page.flow()
        source = flow.grid(trades(), name='counted_trades')
        counted = flow.grid(input=source)
        counted.count_records()
        rows = flow.grid(input=counted).to_pandas()
        self.assertEqual(list(rows.columns), ['Count'])
        self.assertEqual([int(v) for v in rows['Count']], [3])
        with self.assertRaisesRegex(ValueError, 'it counts its rows already'):
            counted.count_records()

    def test_rows_checked_as_a_test_box_checks_them(self):
        # (FL8) grid.test: a flow's Test (checks.ts checkCommands) -- one row, each check's failing rows counted by the
        # database: an empty region, a repeated id, a notional outside its bounds
        page = ll.Page('Test')
        frame = pd.DataFrame({'id': [1, 2, 2, 3], 'region': ['EMEA', None, 'APAC', 'EMEA'], 'notional': [10.0, 500.0, 2500.0, 20.0]})
        grid = page.grid(frame, name='checked')
        grid.test([{'kind': 'notEmpty', 'column': 'region'}, {'kind': 'unique', 'column': 'id'},
                   {'kind': 'between', 'column': 'notional', 'min': 0, 'max': 1000}])
        # its row: its counts (the grid read itself, its hidden columns beside them: a box after it reads the counts alone)
        counts = lambda *names: (lambda row: {k: int(row[k]) for k in names})(grid.to_pandas().to_dict('records')[0])
        self.assertEqual(len(grid.to_pandas()), 1)
        self.assertEqual(counts('Rows', 'region_empty', 'id_repeated', 'notional_outside'),
                         {'Rows': 4, 'region_empty': 1, 'id_repeated': 1, 'notional_outside': 1})
        # set again: its checks the new ones, every row passing
        grid.test([{'kind': 'notEmpty', 'column': 'id'}, {'kind': 'between', 'column': 'id', 'min': 1}])
        self.assertEqual(counts('Rows', 'id_empty', 'id_outside'), {'Rows': 4, 'id_empty': 0, 'id_outside': 0})
        self.assertNotIn('region_empty', grid.to_pandas().columns, 'its old checks taken off')
        with self.assertRaisesRegex(ValueError, "'region' is String: only a number is checked within bounds"):
            grid.test([{'kind': 'between', 'column': 'region', 'min': 1}])
        with self.assertRaisesRegex(ValueError, 'holds whole numbers: bound it by whole numbers'):
            grid.test([{'kind': 'between', 'column': 'id', 'min': 0.5}])
        # its values one of a list, a text matching a pattern: each failing row counted, an empty value none
        grid.test([{'kind': 'oneOf', 'column': 'region', 'values': ['EMEA', 'APAC']}, {'kind': 'matches', 'column': 'region', 'pattern': '^E'}])
        self.assertEqual(counts('Rows', 'region_unexpected', 'region_unmatched'), {'Rows': 4, 'region_unexpected': 0, 'region_unmatched': 1})
        self.assertEqual([f['says'] for f in grid.failing], ['region matches ^E: 1 row fails'])
        # its count of rows, checked after grouping: 1 when it is outside its bounds, its rows said
        grid.test([{'kind': 'notEmpty', 'column': 'id'}, {'kind': 'rows', 'min': 5}])
        self.assertEqual(counts('Rows', 'id_empty', 'Rows_outside'), {'Rows': 4, 'id_empty': 0, 'Rows_outside': 1})
        self.assertEqual(grid.failing, [{'check': 'at least 5 rows', 'rows': 4, 'says': 'at least 5 rows: it has 4 rows'}])
        grid.test([{'kind': 'rows', 'min': 1, 'max': 4}])
        self.assertEqual(grid.failing, [], 'set again: its 4 rows within bounds')
        with self.assertRaisesRegex(ValueError, 'a smallest is 1 or more, or left out'):
            grid.test([{'kind': 'rows', 'min': 0}])
        with self.assertRaisesRegex(ValueError, 'a rows check takes no column'):
            grid.test([{'kind': 'rows', 'column': 'id', 'max': 2}])
        # a Test of no rows (its input has none): one row, its count of rows 0 (a grouping by one value is every row
        # aggregated: the review of 2026-10-26's C4) -- a count of rows fails its smallest
        none = page.grid(frame, name='none')
        none.filter(('region', 'equal', 'AMER'))
        tested = page.grid(input=none)
        tested.test([{'kind': 'notEmpty', 'column': 'id'}, {'kind': 'rows', 'min': 1}])
        self.assertEqual(len(tested.to_pandas()), 1)
        self.assertEqual(tested.failing, [{'check': 'at least 1 row', 'rows': 0, 'says': 'at least 1 row: it has 0 rows'}])

    def test_rows_summarised_as_a_flow_box_summarises_them(self):
        # (M1) grid.summarize: a flow's Summarize (summarize.ts summarizeCommands) -- a row of each group, each column
        # aggregated as it is told, every other left out; the database groups and aggregates
        page = ll.Page('Summarize')
        grid = page.grid(pd.DataFrame({'desk': ['FX', 'FX', 'Rates'], 'book': ['a', 'b', 'c'], 'notional': [1.0, 3.0, 5.0]}),
                         name='booked')
        grid.summarize(['desk'], {'notional': 'average'})
        rows = grid.to_pandas()
        self.assertEqual(sorted((r['desk'], r['notional']) for r in rows.to_dict('records')), [('FX', 2.0), ('Rates', 5.0)])
        self.assertTrue(grid._cube['configuration']['columns']['book']['hidden'], 'what it leaves out hidden')
        with self.assertRaisesRegex(ValueError, 'group by a column'):
            grid.summarize([], {'notional': 'sum'})
        with self.assertRaisesRegex(ValueError, "'sum' is not offered on book"):
            grid.summarize(['desk'], {'book': 'sum'})
        # an aggregate of two columns: a weighted average, its weight beside it
        priced = page.grid(pd.DataFrame({'desk': ['FX', 'FX', 'Rates'], 'px': [10.0, 20.0, 5.0], 'qty': [1.0, 3.0, 2.0]}), name='priced')
        priced.summarize(['desk'], {'px': ('wavg', 'qty')})
        self.assertEqual(sorted((r['desk'], r['px']) for r in priced.to_pandas().to_dict('records')), [('FX', 17.5), ('Rates', 5.0)])
        with self.assertRaisesRegex(ValueError, 'reads its weight column: name it'):
            priced.summarize(['desk'], {'px': 'wavg'})

    def test_rows_sampled_as_a_flow_box_samples_them(self):
        # (M3) grid.sample: a flow's Sample (row-sample.ts sampleCommands) -- the first of each group, all but the first,
        # one of every n; the first of all of them its Top N; None, every row again. The database numbers the rows
        page = ll.Page('Sample')
        grid = page.grid(pd.DataFrame({'desk': ['FX', 'FX', 'FX', 'Rates', 'Rates'], 'n': [1, 2, 3, 4, 5]}), name='numbered')
        ns = lambda: sorted(int(v) for v in grid.to_pandas()['n'])
        grid.sample('last', 1, 'n', groups=['desk'])
        self.assertEqual(ns(), [3, 5])
        grid.sample('skip', 2, 'n')
        self.assertEqual(ns(), [3, 4, 5])
        grid.sample('every', 2, 'n', groups=['desk'])
        self.assertEqual(ns(), [1, 3, 4])
        grid.sample('first', 2, 'n')
        self.assertEqual(ns(), [1, 2])
        self.assertEqual(grid._query.get('topN'), {'by': 'n', 'n': 2, 'bottom': True})
        # Select Records: the rows at some places, in the order of n
        grid.sample('records', by='n', ranges='1-2, 5')
        self.assertEqual(ns(), [1, 2, 5])
        with self.assertRaisesRegex(ValueError, "'x' is no range of rows"):
            grid.sample('records', by='n', ranges='x')
        # a random percent, the same rows for its seed each time, others for another
        many = page.grid(pd.DataFrame({'n': list(range(1, 51))}), name='fifty')
        kept = lambda: sorted(int(v) for v in many.to_pandas()['n'])
        many.sample('random', 50, seed=3)
        picked = kept()
        self.assertTrue(5 < len(picked) < 45, picked)
        many.sample(None)
        many.sample('random', 50, seed=3)
        self.assertEqual(kept(), picked, 'the same seed, the same rows')
        many.sample('random', 50, seed=4)
        self.assertNotEqual(kept(), picked, 'another seed, other rows')
        # an oversample: every row of the value of interest, the others to make it its share -- 5 of 50 the 1..5 rows,
        # 25% of the rows: 15 others kept, chosen by the hash and the seed, the same for its seed
        many.sample('oversample', 25, by='n', value=3, seed=1)
        once = kept()
        self.assertIn(3, once)
        self.assertEqual(len(once), 4, f'one of interest, three others: {once}')
        many.sample('oversample', 25, by='n', value=3, seed=1)
        self.assertEqual(kept(), once, 'the same seed, the same rows')
        grid.sample(None)
        self.assertEqual(ns(), [1, 2, 3, 4, 5])
        with self.assertRaisesRegex(ValueError, 'one row of every 1 is every row'):
            grid.sample('every', 1, 'n')
        with self.assertRaisesRegex(ValueError, 'its mode is first, last, skip, every, records, random'):
            grid.sample('shuffle', 1, 'n')

    def test_one_formula_over_several_columns_as_a_flow_box_does(self):
        # (M3) grid.multi_field: a flow's Multi-Field Formula (multi-field-columns.ts multiFieldCommands) -- one expression over
        # $v, a column of each, parsed and typed by the compiler; $x the row, for its other columns
        prices = pd.DataFrame({'name': [' tea ', 'Coffee'], 'shop': ['a ', ' b'], 'price': [2.0, 3.0], 'cost': [1.0, 2.5]})
        page = ll.Page('Multi-Field')
        grid = page.grid(prices, name='prices')
        grid.multi_field(['name', 'shop'], '$v->toOne()->trim()->toUpper()', suffix='_clean')
        grid.multi_field(['price', 'cost'], '$v->toOne() * 2 + $x.price->toOne()')
        rows = grid.to_pandas().sort_values('price').to_dict('records')
        self.assertEqual([(r['name_clean'], r['shop_clean']) for r in rows], [('TEA', 'A'), ('COFFEE', 'B')])
        self.assertEqual([(r['price_new'], r['cost_new']) for r in rows], [(6.0, 4.0), (9.0, 8.0)])
        # the compiler judges each column's: trim is no number's
        with self.assertRaisesRegex(ValueError, 'trim'):
            grid.multi_field(['price'], '$v->toOne()->trim()', suffix='_t')
        with self.assertRaisesRegex(ValueError, "'price_new' is already a column"):
            grid.multi_field(['price'], '$v')
        with self.assertRaisesRegex(ValueError, 'pick a column'):
            grid.multi_field([], '$v')

    def test_a_named_dimension_is_used_and_drilled(self):
        # (K6) grid.dimension, grid.drill, grid.drill_up: a dimension of the grid's configuration (the Dimensions tab's),
        # by dimensions.ts's own rules (useDimension, drillDown, drillUp), as the group command the Rows zone builds
        page = ll.Page('Dimensions')
        grid = page.grid(trades(), name='dimension_trades', measures={'notional': 'sum'})
        grid.configure({'dimensions': [{'name': 'Book', 'columns': ['region', 'desk']}]})
        grid.dimension('Book')
        self.assertEqual(grid._query['rows'], ['region'])
        grid.drill('Book')
        self.assertEqual(grid._query['rows'], ['region', 'desk'])
        grid.drill('Book')
        self.assertEqual(grid._query['rows'], ['region', 'desk'], 'its finest level: no deeper')
        grid.drill_up('Book')
        self.assertEqual(grid._query['rows'], ['region'])
        grid.drill_up('Book')
        self.assertEqual(grid._query['rows'], [])
        with self.assertRaisesRegex(ValueError, r"the grid has no dimension 'Geo' \(its dimensions: Book\)"):
            grid.dimension('Geo')

    def test_a_gesture_is_made_by_the_one_function_the_screens_control_calls(self):
        # (S8: screen code builds no command) grid.gesture, add_group, remove_group, add_pivot, remove_pivot, heatmap:
        # each the gesture the screen's control makes, made by the one function it calls (grid-gestures.ts gridGesture,
        # headless gridGesture), its name the menu's own id
        page = ll.Page('Gestures')
        grid = page.grid(trades(), name='gesture_trades', measures={'notional': 'sum'})
        grid.add_group('region')
        grid.add_group('desk')
        self.assertEqual(grid._query['rows'], ['region', 'desk'])
        self.assertEqual(page._commands[-1], {'kind': 'group', 'rows': ['region', 'desk']})
        told = len(page._commands)
        grid.add_group('desk')
        self.assertEqual(len(page._commands), told, 'grouped by already: nothing changes, nothing is told')
        grid.remove_group('region')
        self.assertEqual(grid._query['rows'], ['desk'])
        grid.add_pivot('region')
        self.assertEqual(grid._query['pivotOn'], ['region'])
        # a pivoted column's heatmap is its measure's: every value's column under it has it
        leaf = next(c['name'] for c in grid.view().columns if c['name'].endswith('notional') and 'EMEA' in c['name'])
        grid.heatmap(leaf)
        self.assertEqual(grid._cube['configuration']['columns']['notional']['heatmap'], {'from': '#ffffff', 'to': '#ff8a65'})
        grid.heatmap(leaf, on=False)
        self.assertNotIn('heatmap', grid._cube['configuration']['columns'].get('notional', {}))
        grid.remove_pivot('region')
        self.assertEqual(grid._query['pivotOn'], [])
        # any gesture, by the menu's own id
        grid.gesture('column.pinLeft', column='desk')
        self.assertEqual(grid._cube['configuration']['columns']['desk'], {'pinned': 'left'})
        grid.gesture('column.unpinAll')
        self.assertNotIn('desk', grid._cube['configuration']['columns'])
        grid.gesture('pivot.addVertical', column='region')
        self.assertEqual(grid._query['rows'], ['desk', 'region'])
        # a value JSON has no form for carried tagged, as add_filter carries it (D12): a decimal's exact text
        grid.gesture('filter.add', column='notional', operator='greaterThan', value=Decimal('99.5'))
        self.assertEqual(page._commands[-1], {'kind': 'addFilter', 'condition': {
            'kind': 'condition', 'column': 'notional', 'operator': 'greaterThan', 'value': '99.5'}})
        grid.gesture('filter.clearAll')
        # a gesture whose own argument is called name (the S8 audit's 5): the gesture's name is positional
        grid.configure({'dimensions': [{'name': 'Book', 'columns': ['region', 'desk']}]})
        grid.gesture('view.dimension', name='Book', move='use')
        self.assertEqual(grid._query['rows'], ['region'])
        # nothing changed, nothing told: a group it is not in taken out, a heatmap it has not taken off (the S8 audit's 6)
        told = len(page._commands)
        grid.remove_group('desk')
        grid.heatmap('notional', on=False)
        grid.gesture('column.unpin', column='desk')
        self.assertEqual(len(page._commands), told)
        # refused in DataCube's words, nothing changed
        with self.assertRaisesRegex(ValueError, r"'nope' is no gesture of a grid's: sort\.asc, "):
            grid.gesture('nope')
        with self.assertRaisesRegex(ValueError, r"'column\.pinLeft' takes column, not side"):
            grid.gesture('column.pinLeft', column='desk', side='left')
        with self.assertRaisesRegex(ValueError, r"'column\.pinLeft' needs column"):
            grid.gesture('column.pinLeft')
        self.assertEqual(grid._query['rows'], ['region'], 'the refusals changed nothing')

    def test_a_charts_mark_is_selected_its_selection_read_and_cleared(self):
        # (K7, D8) chart.select: the mark named by its value across, and its series', as a click selects it (headless
        # chartPick): its conditions on its grid's filter -- the grid's refilter, then the page's select. Named, never
        # toggled: the same mark again stays selected
        page = ll.Page('Selected')
        grid = page.grid(trades(), name='selected_trades', rows=['region'], measures={'notional': 'sum'})
        chart = grid.chart('bar')
        mark = lambda column, value: {'kind': 'condition', 'column': column, 'operator': 'equal', 'value': value}
        chart.select('EMEA')
        self.assertEqual(chart.selection, [mark('region', 'EMEA')])
        self.assertEqual(grid._query['filter'], mark('region', 'EMEA'))
        self.assertEqual(groups_of(grid.view(), 'notional'), {'EMEA': 175.5})
        self.assertEqual(page._commands[-1], {'kind': 'refilter', 'remove': [], 'add': [mark('region', 'EMEA')]})
        self.assertEqual(page._page_commands[-1], {'kind': 'select', 'chart': chart.id, 'conditions': [mark('region', 'EMEA')]})
        told = len(page._page_commands)
        chart.select('EMEA')
        self.assertEqual(chart.selection, [mark('region', 'EMEA')], 'the same mark again stays selected')
        self.assertEqual(len(page._page_commands), told, 'nothing to do')
        # another mark in its place, never piled on
        chart.select('AMER')
        self.assertEqual(grid._query['filter'], mark('region', 'AMER'))
        self.assertEqual(chart.selection, [mark('region', 'AMER')])
        chart.select(None)
        self.assertEqual(chart.selection, [])
        self.assertNotIn('filter', grid._query)
        # refused in DataCube's words, nothing changed
        with self.assertRaisesRegex(ValueError, 'the chart is split by nothing: a mark of it is named by its value across alone'):
            chart.select('EMEA', split='FX')
        with self.assertRaisesRegex(ValueError, 'a selection cleared names no mark: it takes no split'):
            chart.select(None, split='FX')
        with self.assertRaisesRegex(ValueError, r"a mark's value across is text, a number, true or false, a date or a timestamp, not \["):
            chart.select(['EMEA'])
        # a split chart's mark is named by its series too
        grid.group('region', 'desk')
        self.assertEqual(chart._view['spec']['split'], 'desk')
        with self.assertRaisesRegex(ValueError, "the chart is split by desk: name the mark's desk too"):
            chart.select('EMEA')
        chart.select('EMEA', split='Rates')
        self.assertEqual(chart.selection, [mark('region', 'EMEA'), mark('desk', 'Rates')])
        self.assertEqual(groups_of(grid.view(), 'notional'), {'EMEA': 75.5})
        # a chart whose marks are no values across, and one whose grid was removed
        scatter = grid.chart('scatter', x='notional', y=[('pnl', 'sum')])
        with self.assertRaisesRegex(ValueError, "a scatter chart's marks are not named by a value across"):
            scatter.select(100.0)
        frozen = grid.chart('bar', x='desk', split=None)
        page.remove(grid)
        with self.assertRaisesRegex(ValueError, f"'{frozen.id}' is detached \\(its grid was removed\\): its marks filter no grid"):
            frozen.select('FX')

    def test_a_chart_is_drilled_into_a_mark_and_back_up(self):
        # (K9, D8) chart.drill, chart.drill_up, chart.drilled: the double-click's drillDown and the crumbs' drillUp
        # (headless chartDrill), as the chart's setChart; a selection on a drilled chart takes its path with it
        page = ll.Page('Drilled')
        grid = page.grid(trades(), name='drilled_trades', rows=['region', 'desk'], measures={'notional': 'sum'})
        chart = grid.chart('bar', split=None)
        self.assertEqual(chart.drilled, {})
        chart.drill('EMEA')
        self.assertEqual(chart.drilled, {'region': 'EMEA'})
        self.assertEqual(page._page_commands[-1]['kind'], 'setChart')
        self.assertEqual(page._page_commands[-1]['spec']['drill'], {'levels': ['region', 'desk'], 'path': ['EMEA']})
        with self.assertRaisesRegex(ValueError, r'the chart is drilled to desk, the last of its levels \(region, desk\): nothing is below it'):
            chart.drill('FX')
        chart.select('Rates')
        mark = lambda column, value: {'kind': 'condition', 'column': column, 'operator': 'equal', 'value': value}
        self.assertEqual(chart.selection, [mark('region', 'EMEA'), mark('desk', 'Rates')], 'its path with the mark')
        chart.select(None)
        chart.drill_up()
        self.assertEqual(chart.drilled, {})
        self.assertNotIn('drill', chart._view['spec'])
        with self.assertRaisesRegex(ValueError, 'the chart is not drilled: it is at its top level'):
            chart.drill_up()
        chart.drill('AMER')
        with self.assertRaisesRegex(ValueError, 'the chart is drilled 1 level down: it goes up 1 at most'):
            chart.drill_up(2)
        with self.assertRaisesRegex(ValueError, 'a chart goes up 1 level or more, a whole number, not 0'):
            chart.drill_up(0)
        # a chart that does not drill, in DataCube's words
        flat = page.grid(trades(), name='flat_trades', rows=['region'], measures={'notional': 'sum'})
        with self.assertRaisesRegex(ValueError, r"nothing is below region in the grid's groupings \(region\)"):
            flat.chart('bar').drill('EMEA')
        with self.assertRaisesRegex(ValueError, 'a kpi chart does not drill: its marks are not its categories across'):
            flat.chart('kpi').drill('EMEA')

    def test_show_grid_shows_a_charts_grid_or_makes_one_of_its_own_grouping_it_follows(self):
        # (K11, D7, D8) chart.grid(): the grid a following chart follows, the page as it is; a frozen or a detached
        # chart's grid of its own grouping and query made beside it in one step (headless chartOwnGrid: chart-spec.ts
        # chartGrid, the screen's Show Grid), the chart following it from then on, drawn as it was
        page = ll.Page('Shown')
        grid = page.grid(trades(), name='shown_trades', rows=['region'], measures={'notional': 'sum'})
        follows = grid.chart('bar')
        before = page.to_dict()
        self.assertEqual(follows.grid().id, grid.id)
        self.assertEqual(page.to_dict(), before, 'the page as it is')
        # its grid opened to a level by its settings: a grid of its own opens to it too, as the screen's Show Grid's does
        # (the one tree rule, cube-document.ts newGridTree: the S7 confirmation audit's G2-5)
        grid.configure({'initialExpandToLevel': 1})
        # a frozen chart, its mark selected: a grid of its own grouping, its values the grid's Column Properties
        frozen = grid.chart('bar', x='desk', y=[('pnl', 'max')])
        frozen.select('Rates')
        own = frozen.grid()
        self.assertNotEqual(own.id, grid.id)
        self.assertEqual(own._query['rows'], ['desk'])
        self.assertEqual(next(c['cube'] for c in page.to_dict()['cubes'] if c['id'] == own._view['cube'])['tree'].get('expandTo'), 1)
        self.assertEqual(groups_of(own.view(), 'pnl'), {'FX': 1.5, 'Rates': 0.25}, "its values: each desk's most pnl")
        self.assertEqual([c['kind'] for c in page._page_commands[-3:]], ['addGrid', 'followGrid', 'setChart'])
        self.assertEqual(frozen._view['cube'], own._view['cube'])
        spec = frozen._view['spec']
        self.assertNotIn('frozen', spec, 'it follows its own grid')
        self.assertEqual((spec['mark'], spec['x']), ('bar', 'desk'), 'drawn as it was')
        # its selection off its old grid's filter, as select(None) takes it, and never on its own grid's
        self.assertEqual(frozen.selection, [])
        self.assertNotIn('filter', grid._query)
        self.assertNotIn('filter', own._query)
        # following its own grid from now on
        own.group('region')
        self.assertEqual(frozen._view['spec']['x'], 'region')
        self.assertEqual(frozen.grid().id, own.id)
        # a detached chart (its grid removed) shown in one of its own: the cube it kept let go
        lone = grid.chart('line', x='region', split=None)
        page.remove(grid)
        made = lone.grid()
        self.assertEqual(made._query['rows'], ['region'])
        self.assertEqual(sorted(c['id'] for c in page.to_dict()['cubes']), sorted([own._view['cube'], made._view['cube']]))
        self.assertNotIn('frozen', lone._view['spec'])
        # every chart has a grid (the user, 2026-10-23): a pinned scatter takes nothing of its grid's grouping, so its
        # grid is the one it reads; frozen -- the filter it keeps its own -- its rows in one of its own, ungrouped, its
        # plotted columns shown
        scatter = own.chart('scatter', x='notional', y=[('pnl', 'sum')], frozen=True)
        self.assertEqual(scatter.grid().id, own.id)
        scatter.state = 'frozen'
        rows = scatter.grid()
        self.assertNotEqual(rows.id, own.id)
        self.assertEqual(rows._query['rows'], [])
        self.assertEqual(scatter.state, 'dynamic', 'it follows its own grid')

    def test_saved_pages_are_saved_as_new_listed_and_deleted_as_datacubes_save_as_and_open_do(self):
        # (K13-K15) the tab's Save As, Open…'s list and its Delete, from Python: each by DataCube's own rule (headless
        # pageRecord's Save As, pageSearch) and the store's own rules
        from legend_lite.engine import Engine
        page = ll.Page('Saved as')
        page.grid(trades(), name='saved_as_trades', rows=['region'], measures={'notional': 'sum'})
        first = page.save()
        # Save As: a new saved page, the first left as it was; the page that one from now on, saved again over it
        copy = page.save_as('Saved as, again')
        self.assertNotEqual(copy, first)
        self.assertEqual(page.to_dict()['record'], copy)
        self.assertEqual(page.name, 'Saved as, again')
        engine = Engine(ll.Frames())
        stored = lambda id: json.loads(engine.answer('GET', f'/api/pure/v1/query/dataCube/{id}', '', None).body)
        self.assertEqual(stored(first)['name'], 'Saved as')
        self.assertEqual(stored(copy)['content']['record'], copy)
        self.assertEqual(page.save(), copy)
        # listed as Open… lists them: searched by its id or by text in its name, this user's first, newest first
        listed = ll.Page.saved()
        self.assertLessEqual({first, copy}, {q['id'] for q in listed})
        self.assertNotIn('content', listed[0])
        self.assertEqual([q['id'] for q in ll.Page.saved(first)], [first])
        self.assertEqual({q['id'] for q in ll.Page.saved('saved as, AGAIN')}, {copy})
        self.assertEqual(len(ll.Page.saved(limit=1)), 1)
        created = [q['createdAt'] for q in ll.Page.saved(sort='SORT_BY_CREATE')]
        self.assertEqual(created, sorted(created, reverse=True), 'newest first')
        with self.assertRaisesRegex(ValueError, 'a sort is one of SORT_BY_VIEW, SORT_BY_UPDATE, SORT_BY_CREATE'):
            ll.Page.saved(sort='newest')
        # deleted as Open… deletes one: gone from the list, and each page saved as it no saved page any more -- saved
        # again, a new one, as the tab's page is told (savedAs of none)
        other = ll.Page.open(copy, frames={'saved_as_trades': trades()})
        ll.Page.delete(copy)
        self.assertNotIn(copy, {q['id'] for q in ll.Page.saved()})
        self.assertNotIn('record', page.to_dict())
        self.assertNotIn('record', other.to_dict())
        self.assertEqual(page._page_commands[-1], {'kind': 'savedAs', 'record': None})
        self.assertNotIn(page.save(), (first, copy))
        self.assertEqual(stored(first)['name'], 'Saved as', 'another saved page, left as it was')
        with self.assertRaisesRegex(ValueError, "no saved page 'nope' in this user's saved pages"):
            ll.Page.delete('nope')

    def test_a_deleted_saved_page_is_told_to_every_page_saved_as_it_whatever_one_of_them_says(self):
        # (R7) the record is gone from the store already: a page that fails to hear it stops no other page hearing it,
        # its failure raised after every page was told
        page = ll.Page('Told')
        page.grid(trades(), name='told_trades', rows=['region'], measures={'notional': 'sum'})
        id = page.save()
        other = ll.Page.open(id, frames={'told_trades': trades()})
        with mock.patch.object(datacube._current(), 'page_objects', return_value=[page, other]), \
                mock.patch.object(page, '_record_gone', side_effect=RuntimeError('the page could not hear it')):
            with self.assertRaisesRegex(RuntimeError, 'the page could not hear it'):
                ll.Page.delete(id)
        self.assertNotIn('record', other.to_dict(), 'told, after the page before it failed')

    def test_the_sessions_pages_are_kept_and_read_whole_while_pages_are_made_on_another_thread(self):
        # (R7) Page.delete reads the session's pages while another thread may be making one: each read a whole set,
        # never a set changing size under it
        class Made:
            pass
        session = datacube._current()
        made: list[Made] = []
        def making() -> None:
            for _ in range(50_000):
                one = Made()
                made.append(one)
                session.page_made(one)
        interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)
        try:
            maker = threading.Thread(target=making)
            maker.start()
            while maker.is_alive():
                session.page_objects()
            maker.join()
        finally:
            sys.setswitchinterval(interval)
        self.assertLessEqual(set(map(id, made)), set(map(id, session.page_objects())))

    def test_a_saved_page_is_opened_by_its_id_as_datacubes_open_opens_it(self):
        # (K12) ll.Page.open: the record from this user's saved pages, opened as Open… opens it (headless pageOfRecord)
        from legend_lite._home import THIS_USER, pages_folder, store_call
        page = ll.Page('To open')
        page.grid(trades(), name='open_trades', rows=['region'], measures={'notional': 'sum'})
        id = page.save()
        opened = ll.Page.open(id, frames={'open_trades': trades()})
        self.assertEqual(opened.name, 'To open')
        self.assertEqual(opened.grids[0]._query, page.grids[0]._query)
        self.assertEqual(groups_of(opened.grids[0].view(), 'notional'), {'EMEA': 175.5, 'AMER': 250.0})
        # saved again, over the record it was opened from
        self.assertEqual(opened.save(), id)
        with self.assertRaisesRegex(ValueError, "no saved page 'nope' in this user's saved pages"):
            ll.Page.open('nope')
        with self.assertRaisesRegex(ValueError, 'the page reads the frames open_trades: give them'):
            ll.Page.open(id)
        # a record deleted since (in DataCube's Open...): refused, as the tab's Save is, never made again under its id (the
        # S7 audit's Y-8)
        self.assertEqual(store_call('lite_saved_cubes', pages_folder(), 'DELETE', f'/{id}', '', '', THIS_USER)[0], 204)
        with self.assertRaisesRegex(ValueError, 'the page was not saved'):
            opened.save()
        self.assertEqual(store_call('lite_saved_cubes', pages_folder(), 'GET', f'/{id}', '', '', THIS_USER)[0], 404)
        # a cube saved alone, before pages: a page of its one grid, as the tab opens one
        cube = page.to_dict()['cubes'][0]['cube']
        status, _ = store_call('lite_saved_cubes', pages_folder(), 'POST', '', '',
                               _json.dumps({'id': 'old-cube', 'name': 'To open', 'content': cube}), THIS_USER)
        self.assertEqual(status, 200)
        alone = ll.Page.open('old-cube', frames={'open_trades': trades()})
        self.assertEqual(len(alone.grids), 1)
        self.assertEqual(alone.grids[0]._query, cube['query'])
        # saved again over that record, as the tab's Open... holds it -- never a second one (the S7 audit's Y-8)
        self.assertEqual(alone.save(), 'old-cube')


class Live(Paged):
    def get(self, path):
        web = datacube._session.web()
        request = urllib.request.Request(web.url + path, headers={'Authorization': web.authorization})
        try:
            with urllib.request.urlopen(request, timeout=30) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode('utf-8')

    def said(self, page, edits, version=None):
        """The open page writing the edits it made -- an undo or a redo among them, as the commands between
        (docs/DATACUBE_LIVE_EDITING_2026_10_24.md W5: no page is written whole) -- against the version it is at (the
        page's now, unless given). Its answer's status."""
        return self.wrote(page, edits, version)[0]

    def wrote(self, page, edits, version=None):
        """As ``said``: the answer's status, and the version it made (None when refused)."""
        status, settled = self.settled(page, edits, version)
        return status, (settled['version'] if status == 200 else None)

    def settled(self, page, edits, version=None):
        """As ``said``: the answer's status, and the answer whole (``{version, hash}``, and ``page`` and ``held`` when the
        keeper made another page of it; refused, its words). Each write with its id, as the tab's: its writer, its
        number (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md)."""
        version = datacube._session.engine().page_versions(page._key)['version'] if version is None else version
        self._seq = getattr(self, '_seq', 0) + 1
        answer = datacube._session.engine().answer('POST', '/page.json', f'page={page._key}&version={version}',
                                                   _json.dumps({'edits': edits, 'writer': 'tab-1', 'seq': self._seq}))
        return answer.status, (json.loads(answer.body) if answer.status == 200 else answer.body.decode('utf-8'))

    def version(self, page):
        return self.get(f'/version.json?page={page._key}')[1]['version']

    def test_shown_in_a_tab_served_and_each_change_served_again(self):
        page, grid, _ = self.built()
        tab = ll.show(page, browser=False)
        self.assertIn(f'/engine.html?page={page._key}#token=', tab.url)
        status, said = self.get(f'/page.json?page={page._key}')
        self.assertEqual((status, said['page']), (200, page.to_dict()))
        at = said['version']
        # a change in Python: its version moved
        page.sheet('More')
        self.assertEqual(self.version(page), at + 1)
        page.name = 'Q3, again'
        self.assertEqual(self.get(f'/page.json?page={page._key}')[1]['page']['name'], 'Q3, again')
        # a frame updated: the frame's version moves once, the page's not
        before = self.get(f'/version.json?page={page._key}')[1]
        grid.update(trades().iloc[:2])
        after = self.get(f'/version.json?page={page._key}')[1]
        self.assertEqual(after['version'], before['version'])
        self.assertEqual(after['frames']['trades'], before['frames']['trades'] + 1)
        # its last grid taken off: the page shown is followed empty, as the tab's blank page is (D2, B6)
        page.remove(grid)
        self.assertEqual(self.get(f'/page.json?page={page._key}')[1]['page']['cubes'], [])
        tab.close()
        self.assertEqual(self.get(f'/page.json?page={page._key}')[0], 404)

    def test_a_change_in_python_is_written_as_the_commands_it_was(self):
        # (docs/DATACUBE_LIVE_COMMANDS_2026_10_18.md; S4) a change in Python is written to its keeper as the page's and the
        # grids' commands, in order, as DataCube's screen builds them
        page, grid, _ = self.built()
        tab = ll.show(page, browser=False)
        since = self.writes(page)
        at = self.version(page)
        self.assertEqual(since(at), {'version': at, 'commands': []})
        page.sheet('More')
        grid.group('desk')
        moved = since(at)
        self.assertEqual(moved['version'], at + 2)
        self.assertEqual(moved['commands'][0], {'page': {'kind': 'addSheet', 'id': page.sheets[1].id, 'name': 'More'}})
        self.assertEqual(moved['commands'][1], {'grid': grid.id, 'command': {'kind': 'group', 'rows': ['desk']}})
        # a grid added: its cube document in its addGrid, and the commands that set it after, as one write
        before = self.version(page)
        other = page.grid(trades(), name='orders', rows=['desk'])
        moved = since(before)
        self.assertEqual(moved['version'], before + 1, 'one write')
        added = moved['commands'][0]['page']
        self.assertEqual((added['kind'], added['id'], added['document']['source']['name']), ('addGrid', other.id, 'orders'))
        self.assertEqual(moved['commands'][1:], [{'grid': other.id, 'command': {'kind': 'group', 'rows': ['desk']}}])
        # a refused change writes nothing
        with self.assertRaises(ValueError):
            page.name = '   '
        self.assertEqual(since(before + 1), {'version': before + 1, 'commands': []})
        # a frame whose columns changed reopens its cubes: each cube opened again is its page command (rebuilt: P11),
        # DataCube's document of it as it is now
        grid.update(trades().drop(columns=['pnl']))
        moved = since(before + 1)['commands']
        self.assertEqual([(c['page']['kind'], c['page']['cube']) for c in moved], [('rebuilt', grid._view['cube'])])
        self.assertEqual(moved[0]['page']['source'], page.to_dict()['cubes'][0]['cube']['source'])
        self.assertNotIn('pnl', [c['name'] for c in moved[0]['page']['source']['columns']])
        tab.close()

    def test_what_is_written_is_commands_the_open_pages_too(self):
        # (the phase 3 audit's 5, 8 and test gaps; D19 W5) a refused grid writes nothing, and none of it later; the open
        # page's own write is its commands, served to another view as Python's are, and Python's next change, on top of
        # it, is its commands; a grid's edit over a Live frame that changed in place, its cube opened again first
        # (rebuilt: P11)
        page, grid, _ = self.built()
        tab = ll.show(page, browser=False)
        since = self.writes(page)
        at = self.version(page)
        with self.assertRaises(ValueError):
            page.grid(trades(), name='refused', rows=['nope'])
        self.assertEqual(since(at), {'version': at, 'commands': []}, 'the refused grid wrote nothing')
        page.sheet('Next')
        self.assertEqual([c['page']['kind'] for c in since(at)['commands']], ['addSheet'], 'and none of it later')
        before = self.version(page)
        renamed = page_edit(kind='renameSheet', sheet=page.sheets[0].id, name='Renamed in a view')
        self.assertEqual(self.wrote(page, [renamed]), (200, before + 1))
        self.assertEqual(since(before), {'version': before + 1, 'commands': [renamed]}, "the open page's write is its commands")
        page.sheet('After the write')
        self.assertEqual([c['page']['kind'] for c in since(before + 1)['commands']], ['addSheet'],
                         "Python's change on top of it, its commands")
        self.assertEqual(page.sheets[0].name, 'Renamed in a view')
        # a grid over a Live frame (a function) whose rows gained a column in place: its edit opens the cube again over
        # the frame as it is -- its own command, rebuilt -- and then is its commands
        holder = {'df': trades()}
        live = page.grid(lambda: holder['df'], name='livef')
        at = self.version(page)
        holder['df'] = trades().assign(extra=1)
        live.group('desk')
        moved = since(at)['commands']
        self.assertEqual([c.get('page', c.get('command'))['kind'] for c in moved], ['rebuilt', 'group'])
        self.assertIn('extra', [c['name'] for c in moved[0]['page']['source']['columns']])
        self.assertEqual(moved[1], {'grid': live.id, 'command': {'kind': 'group', 'rows': ['desk']}})
        tab.close()

    def test_an_empty_page_is_shown_and_never_saved(self):
        # (D2, B6, B14) a page of no grid yet is shown, as the tab's blank page is, and followed as Python adds to it; it
        # is never saved nor written as a file, refused as DataCube's Save and Page File refuse it, in their words
        empty = ll.Page('Empty')
        nothing = r'^There is nothing on this page to save: add a data source first\.$'
        with self.assertRaisesRegex(ValueError, nothing):
            empty.to_json()
        with self.assertRaisesRegex(ValueError, nothing):
            empty.save(Path(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))) / 'empty.json')
        tab = ll.show(empty, browser=False)
        self.assertEqual(datacube._session.pages, [tab])
        self.assertEqual(self.get(f'/page.json?page={empty._key}')[1]['page']['cubes'], [])
        empty.grid(trades(), name='t')
        self.assertEqual(len(self.get(f'/page.json?page={empty._key}')[1]['page']['cubes']), 1)
        tab.close()

    def test_what_the_open_page_writes_is_the_page_python_holds(self):
        # (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md) one copy of the page: a write from the open page is Python's page at
        # once -- no read, its handles still good
        page, grid, chart = self.built()
        ll.show(page, browser=False)
        since = self.writes(page)
        open_now = [page_edit(kind='renameSheet', sheet=page.sheets[0].id, name='Renamed in DataCube'),
                    grid_edit(grid, kind='group', rows=['desk']), page_edit(kind='removeTile', tile=chart.id)]
        at = self.version(page)
        self.assertEqual(self.wrote(page, open_now), (200, at + 1))
        self.assertEqual(page.sheets[0].name, 'Renamed in DataCube')
        # the same page, its handles still good: the grid is the grid, regrouped; the chart closed there is gone here
        self.assertIs(page.grids[0], grid)
        self.assertEqual(page.to_dict()['cubes'][0]['cube']['query']['rows'], ['desk'])
        self.assertEqual(page.charts, [])
        with self.assertRaisesRegex(ValueError, 'no longer on its page'):
            chart.options({'labels': True})
        # a change after: on top of DataCube's, as its commands
        grid.measure('pnl', 'sum')
        served = self.get(f'/page.json?page={page._key}')[1]
        self.assertEqual(served['page']['sheets'][0]['name'], 'Renamed in DataCube')
        self.assertEqual(served['page']['cubes'][0]['cube']['query']['rows'], ['desk'])
        self.assertEqual([c['grid'] for c in since(at + 1)['commands']], [grid.id])
        # a write that changes nothing (an undo and its redo: the commands between, both ways) is no new version
        self.assertEqual(self.wrote(page, [grid_edit(grid, kind='group', rows=['region']), grid_edit(grid, kind='group', rows=['desk'])]),
                         (200, at + 2))

    def test_a_tile_closed_in_datacube_and_brought_back_there_is_a_new_tile_never_its_old_handles(self):
        page, grid, chart = self.built()
        ll.show(page, browser=False)
        # DataCube closed the chart, and later undid that: its undo adds a chart again under a fresh id of its own, under
        # its tab's tag (B12: an id the page has held is never given out again -- docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md,
        # the middle version: nothing is renamed)
        self.assertEqual(self.said(page, [page_edit(kind='removeTile', tile=chart.id)]), 200)
        self.assertEqual(page.charts, [])
        added = lambda id: [page_edit(kind='addChart', id=id, near=grid.id, title='new there', columns=None,
                                      spec={'version': 2, 'mark': 'pie', 'y': [{'column': 'notional', 'fn': 'sum'}], 'options': {}})]
        status, why = self.settled(page, added(chart.id))
        self.assertEqual(status, 409, 'its old id, which the page has held: refused in words, never given out afresh')
        self.assertIn(f"'{chart.id}' is an id the page has held already", why)
        self.assertEqual(self.said(page, added('chart-ta-2')), 200)
        with self.assertRaisesRegex(ValueError, 'no longer on its page'):
            chart.title = 'the old chart'
        self.assertEqual((page.charts[0].id, page.charts[0].title), ('chart-ta-2', 'new there'))

    def test_a_page_opening_on_it_opens_it_as_it_is_now(self):
        page, _, _ = self.built()
        ll.show(page, browser=False)
        open_now = [page_edit(kind='renameSheet', sheet=page.sheets[0].id, name='Renamed in DataCube')]
        self.assertEqual(self.said(page, open_now), 200)
        # another tab, or the first reloaded: the page as the open one wrote it, which is Python's too
        self.assertEqual(self.get(f'/page.json?page={page._key}')[1]['page']['sheets'][0]['name'], 'Renamed in DataCube')
        ll.show(page, browser=False)
        self.assertEqual(self.get(f'/page.json?page={page._key}')[1], {'version': self.version(page), 'page': page.to_dict(),
                                                                  'held': ['chart-1', 'grid-1', 'sheet-1'],
                                                                  'hash': page_hash(datacube._session.keeper.current(page._key)[1])})

    def test_shown_again_and_closed_and_shown_again(self):
        page, grid, _ = self.built()
        first = ll.show(page, browser=False)
        at = self.version(page)
        # shown again in another tab: the page as it is, its version the same
        ll.show(page, browser=False)
        self.assertEqual(self.version(page), at)
        # closed everywhere and shown again: the keeper kept it as it was -- a tab still at its version writes it, one
        # behind has its edits made again on top (combined, answered with the page they made), one at a version the page
        # never had is refused
        for view in list(datacube._session.pages):
            view.close()
        del first
        ll.show(page, browser=False)
        self.assertEqual(self.version(page), at)
        renamed = [page_edit(kind='renamePage', name='Renamed there')]
        self.assertEqual(self.said(page, renamed, version=0), 409)
        status, combined = self.settled(page, renamed, version=at - 1)
        self.assertEqual((status, combined['version'], combined['page']['name']), (200, at + 1, 'Renamed there'))
        # what the writer's screen is set to (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md): the page the keeper
        # made, every id it has held, and its hash
        self.assertEqual(combined['page'], datacube._session.keeper.current(page._key)[1])
        self.assertEqual(combined['held'], datacube._session.keeper.held(page._key))
        status, plain = self.settled(page, renamed, version=at + 1)
        self.assertEqual((status, plain['version']), (200, at + 1))
        self.assertEqual(sorted(plain), ['hash', 'version'], 'the writer\'s own page: nothing to set its screen to')
        # the page's hash with every answer (W17): what the page open on it must hash to
        self.assertEqual(plain['hash'], page_hash(datacube._session.keeper.current(page._key)[1]))
        self.assertEqual(self.get(f'/page.json?page={page._key}')[1]['hash'], plain['hash'])
        self.assertEqual(page.name, 'Renamed there')

    def test_a_frozen_chart_left_without_its_grid_follows_its_frame(self):
        page, grid, chart = self.built()
        chart.plot(frozen=True)
        page.grid(desks(), name='desks')
        page.remove(grid)
        ll.show(page, browser=False)
        before = self.get(f'/version.json?page={page._key}')[1]['frames']['trades']
        datacube._session.register(trades().iloc[:1], 'trades')
        self.assertEqual(self.get(f'/version.json?page={page._key}')[1]['frames']['trades'], before + 1)

    def test_a_write_against_a_version_the_page_moved_on_from_is_made_again_on_top_or_refused(self):
        # (S4, H1) a write made at a version the page has moved on from is its edits made again on the page as it is
        # now; one that no longer applies there -- its grid taken off meanwhile -- is refused whole, in DataCube's words
        page, grid, _ = self.built()
        ll.show(page, browser=False)
        at = self.version(page)
        stale = [page_edit(kind='renameSheet', sheet=page.sheets[0].id, name='Of the page before')]
        page.sheet('More')
        status, combined = self.settled(page, stale, version=at)
        self.assertEqual((status, combined['version']), (200, at + 2))
        self.assertEqual([s.get('name') for s in combined['page']['sheets']], ['Of the page before', 'More'],
                         "the page the keeper made: Python's sheet, and the write on top")
        self.assertEqual([s.name for s in page.sheets], ['Of the page before', 'More'])
        at = self.version(page)
        page.remove(grid)
        status, why = self.settled(page, [grid_edit(grid, kind='group', rows=['desk']), page_edit(kind='renamePage', name='Both or neither')],
                                   version=at)
        self.assertEqual(status, 409)
        self.assertRegex(why, f'the page changed where it is open while this change was made: {grid.id} is no longer on the page')
        self.assertEqual(page.name, 'Q3 review')

    def test_a_write_is_a_page_datacube_reads_over_frames_this_session_serves(self):
        page, _, _ = self.built()
        ll.show(page, browser=False)
        engine = datacube._session.engine()
        sheet = page.sheets[0].id
        added = lambda cube: [page_edit(kind='addGrid', document=cube, sheet=sheet, near=None, title=None, columns=None)]
        status, why = self.settled(page, added({'kind': 'datacube.cube', 'version': 6, 'name': 'Q3'}))
        # refused by DataCube's reader, in its words -- never by a failure of the engine's own
        self.assertEqual(status, 409)
        self.assertNotRegex(why, r'^[A-Za-z]+(Error|Exception):')
        over_nope = copy.deepcopy(page.to_dict()['cubes'][0]['cube'])
        over_nope['source']['name'] = 'nope'
        status, why = self.settled(page, added(over_nope))
        self.assertEqual(status, 409)
        self.assertIn('nope', why)
        # the page whole is no write (D19 W5: an undo is its commands); a page not served, a version not said
        whole = engine.answer('POST', '/page.json', f'page={page._key}&version={self.version(page)}',
                              json.dumps({'restore': page.to_dict()}))
        self.assertEqual(whole.status, 400)
        self.assertIn('never the page whole', whole.body.decode('utf-8'))
        write = '{"edits": [], "writer": "tab-1", "seq": 1}'
        self.assertEqual(engine.answer('POST', '/page.json', 'page=nope&version=1', write).status, 404)
        self.assertEqual(engine.answer('POST', '/page.json', f'page={page._key}', write).status, 400)
        # a write says its id: its writer and its number (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md)
        for unsaid in ('{"edits": []}', '{"edits": [], "writer": "a b", "seq": 1}', '{"edits": [], "writer": "tab-1", "seq": 0}',
                       '{"edits": [], "writer": "tab-1", "seq": true}'):
            self.assertEqual(engine.answer('POST', '/page.json', f'page={page._key}&version={self.version(page)}', unsaid).status,
                             400, unsaid)
        # its numbers exact both ways: a calculated column's 12.30 is 12.30 in Python after the open page's write, which
        # DataCube applied to the page holding it
        page.grids[0].calculate('scaled', '$x.notional->toOne() * 12.30D')
        self.assertIn(Decimal('12.30'), numbers(page.to_dict()))
        self.assertEqual(self.said(page, [page_edit(kind='renamePage', name='Written there')]), 200)
        self.assertEqual(page.name, 'Written there')
        self.assertIn(Decimal('12.30'), numbers(page.to_dict()))

    def test_a_python_change_made_while_the_open_page_wrote_is_made_again_on_top_of_it(self):
        # (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md) a call on the page works on the page as it was when the call began;
        # the open page wrote it meanwhile: its commands are made again on the page as it is now, by DataCube's function
        page, grid, _ = self.built()
        other = page.grid(desks(), name='desks')
        ll.show(page, browser=False)
        since = self.writes(page)
        theirs = [page_edit(kind='renameSheet', sheet=page.sheets[0].id, name='Renamed in DataCube')]
        page._enter()
        try:
            # the call began at this version; DataCube wrote the page meanwhile
            self.assertEqual(self.said(page, theirs), 200)
            at = self.version(page)
            page.sheet('From Python')
            grid.group('desk')
        finally:
            page._leave(failed=False)
        self.assertEqual([s.name for s in page.sheets], ['Renamed in DataCube', 'From Python'])
        self.assertEqual(page.to_dict()['cubes'][0]['cube']['query']['rows'], ['desk'])
        self.assertEqual([('page' if 'page' in c else 'grid') for c in since(at)['commands']], ['page', 'grid'])
        # one that no longer applies there -- the grid closed in DataCube meanwhile -- is refused, the page DataCube's
        gone = [page_edit(kind='removeTile', tile=grid.id)]
        page._enter()
        try:
            self.assertEqual(self.said(page, gone), 200)
            with self.assertRaisesRegex(ValueError, 'the page changed where it is open while this change was made'):
                grid.group('region')
            # refused, the page is the keeper's at once -- even inside the call that made it, which goes on from there
            self.assertEqual(_json.dumps(page.to_dict()), _json.dumps(datacube._session.keeper.current(page._key)[1]))
        finally:
            page._leave(failed=True)
        self.assertEqual(page.grids, [other])

    def test_a_sheet_made_while_the_open_page_made_one_each_keeps_its_own_id(self):
        # (found by the keeper's soak at 60 seeds; docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md, the middle version)
        # the open page wrote a sheet at the number Python's next one takes: under its tab's tag, so each keeps its own
        # id, nothing renamed -- Python's handle is the sheet it made
        page, grid, _ = self.built()
        ll.show(page, browser=False)
        page._enter()
        try:
            n = 1 + max(int(i.split('-')[-1]) for i in page._ids if i.startswith('sheet-'))
            theirs = f'sheet-ta-{n}'
            version = datacube._session.engine().page_versions(page._key)['version']
            answer = datacube._session.engine().answer('POST', '/page.json', f'page={page._key}&version={version}', _json.dumps(
                {'edits': [{'page': {'kind': 'addSheet', 'id': theirs, 'name': 'There'}}], 'writer': 'tab-1', 'seq': 1}))
            self.assertEqual(answer.status, 200, answer.body.decode('utf-8'))
            mine = page.sheet('Mine')
        finally:
            page._leave(failed=False)
        sheets = {s['id']: s.get('name') for s in page.to_dict()['sheets']}
        self.assertEqual((sheets.get(theirs), mine.id, sheets.get(f'sheet-{n}')), ('There', f'sheet-{n}', 'Mine'))
        mine.name = 'Mine, renamed'
        self.assertEqual({s['id']: s.get('name') for s in page.to_dict()['sheets']}[theirs], 'There', "the open page's sheet untouched")

    def test_a_cube_opened_again_while_the_open_page_wrote_is_its_command_made_again_on_that_write(self):
        # (P11) a cube opened again over a frame whose columns moved is its command, rebuilt: made again on top of the
        # open page's write as any command is -- the page as the open page wrote it, the cube as it opens now
        page, grid, _ = self.built()
        ll.show(page, browser=False)
        # the open page regrouped the grid too: the cube opened again over the frame is that cube, its grouping kept
        theirs = [page_edit(kind='renamePage', name='Renamed in DataCube'), grid_edit(grid, kind='group', rows=['desk'])]
        page._enter()
        try:
            self.assertEqual(self.said(page, theirs), 200)
            grid.update(trades().assign(extra=1))
        finally:
            page._leave(failed=False)
        self.assertEqual(page.name, 'Renamed in DataCube')
        cube = next(c['cube'] for c in page.to_dict()['cubes'] if c['id'] == grid._view['cube'])
        self.assertIn('extra', [c['name'] for c in cube['source']['columns']])
        self.assertEqual(cube['query']['rows'], ['desk'], "the open page's grouping kept (the S1 audit's 1)")

    def test_a_call_that_raised_writes_nothing_it_half_did(self):
        page, _, _ = self.built()
        ll.show(page, browser=False)
        at, name = self.version(page), page.name
        # a call that changed the page, then failed
        half = page_module._operation(lambda self: (setattr(self, '_doc', {**self._doc, 'name': 'Half done'}), 1 / 0))
        with self.assertRaises(ZeroDivisionError):
            half(page)
        self.assertEqual((page.name, self.version(page)), (name, at))
        self.assertEqual(datacube._session.keeper.current(page._key)[1]['name'], name)

    def test_a_page_shown_nowhere_takes_no_write(self):
        page, _, _ = self.built()
        tab = ll.show(page, browser=False)
        at = self.version(page)
        tab.close()
        self.assertEqual(self.said(page, [page_edit(kind='renamePage', name='Written after it closed')], version=at), 404)
        self.assertEqual(page.name, 'Q3 review')

    def test_a_frame_a_grid_the_open_page_added_reads_is_followed_at_a_cells_end(self):
        # the page outside any call is the keeper's as it is now: the session reads which frames a shown page reads at
        # a cell's end, and a grid the open page added over a live frame is among them, before Python touches the page
        page, _, _ = self.built()
        ll.show(page, browser=False)
        other = ll.Page('Other')
        other.grid(lambda: desks(), name='livedesks')
        added = page_edit(kind='addGrid', document=other.to_dict()['cubes'][0]['cube'], sheet=page.sheets[0].id,
                          near=None, title=None, columns=None)
        self.assertEqual(self.said(page, [added]), 200)
        engine = datacube._session.engine()
        before = engine.version('livedesks')
        datacube._session.after_cell()
        self.assertEqual(engine.version('livedesks'), before + 1)

    def test_a_loaded_page_is_kept_as_loaded(self):
        # shown at once, what the tab opens is the page as loaded, not the empty page it started as
        page, _, _ = self.built()
        page.sheet('Second')
        again = ll.Page.load(page.to_dict(), frames={'trades': trades()})
        version, kept = datacube._session.keeper.current(again._key)
        # one write: the page started empty (version 1), then loaded whole -- never a half-loaded page between
        self.assertEqual(version, 2)
        self.assertEqual([s.get('name') for s in kept['sheets']], [None, 'Second'])
        self.assertEqual(len(kept['cubes']), 1)

    def test_a_change_asks_datacube_as_often_whatever_the_page_holds(self):
        # (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md) the page's frames, copies and ids come with one call, never one a grid:
        # step 1 asked which frames each grid reads at every write, so a rename of a page of sixty grids made sixty calls
        asked = []
        for grids in (1, 12):
            page = ll.Page(f'Calls {grids}')
            for i in range(grids):
                page.grid(trades(), name=f'calls_{grids}_{i}')
            headless = datacube._session.headless()
            calls, real = [], headless.call
            headless.call = lambda function, *args, **kwargs: (calls.append(function), real(function, *args, **kwargs))[1]
            try:
                page.name = f'Renamed {grids}'
            finally:
                del headless.call
            asked.append(calls)
        self.assertEqual(asked[0], asked[1], 'as many calls for twelve grids as for one')
        self.assertLessEqual(len(asked[1]), 3, f'a rename: its command, what the page reads, its name: {asked[1]}')

    def test_a_page_loaded_over_frames_as_they_were_tells_no_cube_opened_again(self):
        # (the S1 audit's 14) a cube opened over its frame as it was saved is the cube held: no rebuilt, so a page and the
        # tab that opens it never tell each other the same cube back and forth
        page = ll.Page('Unchanged')
        page.grid(trades(), name='same_trades', rows=['region'])
        page.grid(trades(), name='same_desks', rows=['desk'])
        again = ll.Page.load(page.to_dict(), frames={'same_trades': trades(), 'same_desks': trades()})
        self.assertEqual([c['kind'] for c in again._page_commands if c['kind'] == 'rebuilt'], [])
        self.assertEqual(again.to_dict()['cubes'], page.to_dict()['cubes'])

    def test_a_page_loaded_over_frames_that_changed_is_written_once_whole(self):
        # each cube opened again over its frame as it is now (a column gone, one new): the page written once, loaded
        # whole -- never a page with some of its cubes opened again and not the others
        page = ll.Page('Two frames')
        page.grid(trades(), name='first_trades', rows=['region'])
        page.grid(trades(), name='second_trades', rows=['region'])
        changed = trades().drop(columns=['desk']).assign(book='B1')
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            again = ll.Page.load(page.to_dict(), frames={'first_trades': changed, 'second_trades': changed})
        version, kept = datacube._session.keeper.current(again._key)
        self.assertEqual(version, 2)
        self.assertEqual(_json.dumps(kept), _json.dumps(again.to_dict()))
        self.assertEqual([[c['name'] for c in cube['cube']['source']['columns']] for cube in kept['cubes']],
                         [['region', 'notional', 'pnl', 'book']] * 2)


class Joined(Paged):
    """A join's grid on a page Python serves (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17, §18.3): made by page.join as
    the tab's Join… makes one, its rows planned by Python over one model of the two frames it reads, saved and loaded
    over those frames, reopened when one of them is served again, and the page served with both frames' versions."""

    HOW = {'form': 'join', 'kind': 'left', 'on': {'rows': [{'left': 'desk', 'op': '==', 'right': 'desk'}]},
           'prefix': 'd_', 'keep': None}
    # the engine's answers as a tab reads them (Live's), without running Live's own tests again
    get = Live.get
    said = Live.said
    wrote = Live.wrote
    settled = Live.settled

    def joined(self):
        page = ll.Page('Joins')
        mine = page.grid(trades(), name='trades')
        theirs = page.grid(desks(), name='desks')
        return page, page.join(mine, theirs, self.HOW)

    def test_a_join_of_two_frames_grids_its_rows_planned_by_python_over_both(self):
        page, joined = self.joined()
        self.assertEqual(joined.frames, ['trades', 'desks'])
        with self.assertRaisesRegex(ValueError, 'reads the frames trades, desks'):
            _ = joined.frame
        rows = joined.to_pandas()
        self.assertEqual(sorted(zip(rows['region'], rows['desk'], rows['head'])),
                         [('AMER', 'Rates', 'Ben'), ('EMEA', 'FX', 'Ana'), ('EMEA', 'Rates', 'Ben')])
        self.assertEqual(page._frames(), {'trades', 'desks'})
        # edited as any grid is, run by DataCube over the join's base
        joined.group('head')
        self.assertEqual(sorted(r[0] for r in joined.view().rows if r[0] is not None), ['Ana', 'Ben'])
        # a refusal in the compiler's words, nothing added (the join at the bottom of its sheet: the desks' grid second)
        with self.assertRaisesRegex(Exception, 'nope'):
            page.join(page.grids[0], page.grids[1], {**self.HOW, 'on': {'rows': [{'left': 'nope', 'op': '==', 'right': 'desk'}]}})
        self.assertEqual(len(page.grids), 3)

    def test_every_row_beside_every_row_as_append_fields(self):
        # (M2) page.append_fields: a flow's Append Fields, the Join window's Every pair -- a join on no condition
        page = ll.Page('Append Fields')
        mine = page.grid(trades(), name='trades')
        theirs = page.grid(desks(), name='desks')
        paired = page.append_fields(mine, theirs, prefix='d_')
        rows = paired.to_pandas()
        self.assertEqual(len(rows), len(trades()) * len(desks()))
        self.assertIn('d_desk', rows.columns)
        self.assertEqual(paired._cube['source']['how']['on'], {'rows': []})

    def test_alike_text_matched_as_fuzzy_match(self):
        # (M2) page.fuzzy_match: a flow's Fuzzy Match -- a join on text at most so many edits apart, the database's distance
        page = ll.Page('Fuzzy Match')
        mine = page.grid(pd.DataFrame({'who': ['Jon Smith', 'Ana Diaz', 'Bob']}), name='people')
        theirs = page.grid(pd.DataFrame({'name': ['John Smith', 'Anna Diaz', 'Zed'], 'id': [1, 2, 3]}), name='crm')
        matched = page.fuzzy_match(mine, theirs, ('who', 'name'), within=1)
        rows = matched.to_pandas()
        self.assertEqual(sorted(zip(rows['who'], rows['id'].astype(int))), [('Ana Diaz', 2), ('Jon Smith', 1)])
        # each pair scored, the database's: one edit in ten letters, 90; in nine, 89
        self.assertEqual(sorted(zip(rows['who'], rows['score'].astype(int))), [('Ana Diaz', 89), ('Jon Smith', 90)])
        self.assertEqual(len(page.fuzzy_match(mine, theirs, ('who', 'name'), within=0).to_pandas()), 0)
        # every row of mine: no score (a pair's alone)
        every = page.fuzzy_match(mine, theirs, ('who', 'name'), within=1, kind='left').to_pandas()
        self.assertNotIn('score', every.columns)
        # a score asked of every row of mine: refused in words, never dropped (the roadmap audit of 2026-10-26)
        with self.assertRaisesRegex(Exception, 'a score is each matched pair'):
            page.fuzzy_match(mine, theirs, ('who', 'name'), within=1, kind='left', score='sim')
        # one of theirs for each of mine: a lookup's, never a right join's
        with self.assertRaisesRegex(Exception, 'one of theirs for each of mine is a lookup of mine'):
            page.join(mine, theirs, {'form': 'join', 'kind': 'right', 'on': {'rows': [{'left': 'who', 'op': '==', 'right': 'name'}]},
                                     'prefix': 'c_', 'keep': None, 'once': True})

    def test_placed_on_its_first_inputs_sheet_and_named_as_its_grids_show(self):
        # docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z9: as the tab's Join… places and names one (join-flow.ts), by
        # DataCube's rule (page-commands.ts joinInputOf, headless joinCube)
        page = ll.Page('Joins')
        mine = page.grid(trades(), name='trades')
        theirs = page.grid(desks(), name='desks')
        mine.title = 'Book'
        joined = page.join(mine, theirs, self.HOW)
        # a band of its own at the bottom of its first input's sheet, as every new grid (page-commands.ts addGrid; the
        # user, 2026-10-23: a new grid goes below, not beside)
        self.assertEqual([t.id for t in page.sheets[0].tiles], [mine.id, theirs.id, joined.id], 'at the bottom of the sheet')
        self.assertEqual(joined._cube['source']['name'], 'Book \u22c8 desks', "each grid's title, else what it reads")
        on_second = page.sheet('Second')
        other = page.join(theirs, mine, self.HOW, sheet=on_second)
        self.assertEqual([t.id for t in on_second.tiles], [other.id], 'on another sheet: where it is asked for')
        # its first input on another sheet: on that sheet, as the tab places it (the S7 audit's Y-3: it went on the
        # first sheet)
        third = page.sheet('Third')
        far = page.grid(desks(), name='far_desks', sheet=third)
        there = page.join(far, mine, self.HOW)
        self.assertEqual([t.id for t in third.tiles], [far.id, there.id], "on its first input's sheet")

    def test_saved_and_loaded_over_its_frames_and_reopened_when_one_is_served_again(self):
        page, joined = self.joined()
        saved = page.to_dict()
        cube = next(c['cube'] for c in saved['cubes'] if c['cube']['source']['_type'] == 'join')
        self.assertEqual([cube['source'][s]['source']['name'] for s in ('left', 'right')], ['trades', 'desks'])
        with self.assertRaisesRegex(ValueError, 'the page reads the frames desks'):
            ll.Page.load(saved, frames={'trades': trades()})
        again = ll.Page.load(saved, frames={'trades': trades(), 'desks': desks()})
        grid = next(g for g in again.grids if g.frames == ['trades', 'desks'])
        self.assertEqual(len(grid.to_pandas()), 3)
        # its frame served again with a column more: the join opened over it as the tab opens a saved join
        with warnings.catch_warnings(record=True) as said:
            warnings.simplefilter('always')
            next(g for g in again.grids if g.frames == ['desks']).update(desks().assign(floor=[3, 4]))
        self.assertTrue(any('floor' in str(w.message) for w in said), [str(w.message) for w in said])
        # every column of theirs kept (keep None): the new one among them, under its own name (mine has none of it)
        self.assertIn('floor', list(grid.to_pandas().columns))

    def test_a_join_that_no_longer_joins_is_left_out_as_the_tab_leaves_it_out(self):
        # (the S7 confirmation audit's Y2-1) a frame served without the column it joins on: the join fails as its plan
        # does, no refusal -- left out and said, as the tab's Open... leaves it out, the page's other grids opened
        page, joined = self.joined()
        saved = page.to_dict()
        name = next(c['cube']['source']['name'] for c in saved['cubes'] if c['cube']['source']['_type'] == 'join')
        with warnings.catch_warnings(record=True) as said:
            warnings.simplefilter('always')
            loaded = ll.Page.load(saved, frames={'trades': trades(), 'desks': desks().rename(columns={'desk': 'team'})})
        self.assertTrue(any(str(w.message).startswith(f'left out: {name} (') for w in said), [str(w.message) for w in said])
        self.assertEqual(len(loaded.grids), 3, 'kept as the page saved it')
        self.assertEqual(len(loaded.grids[0].to_pandas()), len(trades()))

    def test_a_tab_on_the_page_asks_python_each_of_its_join_windows_answers(self):
        # docs/DATACUBE_ONE_API_PLAN_2026_10_21.md B11: the tab's Join… on a page Python serves, which has no database of
        # its own here -- each of its window's asks answered by DataCube's own code over Python's tables (join.json:
        # headless joinSides, joinQuestions, joinCube), as page.join asks them; applied, the grid it made opened as a
        # saved join of the page is (opened.json)
        page = ll.Page('Joins')
        mine = page.grid(trades(), name='trades')
        theirs = page.grid(desks(), name='desks')
        engine = datacube._session.engine()
        doc = _json.dumps(page.to_dict())

        def asked(op, **more):
            answer = engine.answer('POST', '/join.json', '', _json.dumps(
                {'op': op, 'page': doc, 'mine': mine.id, 'theirs': theirs.id, **more}))
            return answer.status, answer.body.decode('utf-8')

        status, body = asked('sides')
        self.assertEqual(status, 200, body)
        sides = json.loads(body)
        self.assertEqual([sides['left']['name'], sides['right']['name']], ['trades', 'desks'])
        self.assertEqual([c['name'] for c in sides['right']['columns']], ['desk', 'head'])
        self.assertEqual(sides['left']['relation']['_type'], 'func', 'the relation a join reads, as the protocol writes it')
        self.assertEqual(sides['notes'], [])
        status, body = asked('questions', how=self.HOW)
        self.assertEqual(status, 200, body)
        said = json.loads(body)
        self.assertEqual((said['counts']['mine'], said['counts']['matched']), (3, 3))
        self.assertTrue(said['words'], 'and in the window\'s words')
        status, body = asked('cube', how=self.HOW, sheet=page.sheets[0].id)
        self.assertEqual(status, 200, body)
        command = json.loads(body)['command']
        self.assertEqual((command['kind'], command['near'], command['sheet']), ('addGrid', mine.id, page.sheets[0].id))
        self.assertEqual(command['document']['source']['name'], 'trades \u22c8 desks', 'named as page.join names it')
        opened = engine.answer('POST', '/opened.json', '', json.dumps(
            {'cube': json.dumps(command['document']), 'page': page.name, 'gridsOnPage': 3}))
        self.assertEqual(opened.status, 200, opened.body)
        self.assertEqual(sorted(json.loads(opened.body)['frames']), ['desks', 'trades'], 'its rows read over both frames')
        self.assertEqual(json.loads(opened.body)['how'], {}, 'planned over its model alone: no mapping, no project of its own')
        # refused in DataCube's words (the compiler's: a column neither grid has), and a malformed ask in the route's
        status, body = asked('questions', how={**self.HOW, 'on': {'rows': [{'left': 'nope', 'op': '==', 'right': 'desk'}]}})
        self.assertEqual(status, 422)
        self.assertIn('nope', body)
        self.assertEqual(asked('cube', how=self.HOW)[0], 400, 'a grid is added on a sheet')
        self.assertEqual(engine.answer('POST', '/join.json', '', '{"op": "sides"}').status, 400)
        self.assertEqual(len(page.grids), 2, 'asking adds nothing to the page: the tab adds the grid')

    def test_a_tab_on_the_page_asks_python_to_read_a_timeseries_grids_together(self):
        # docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5.2: a timeseries series that combines grids is one query over one
        # model of the tables they read -- on a page Python serves, Python's, opened by DataCube's own rule over them
        # (join.json together: headless joinTogether, join-open.ts openSources); the tab writes the series' query over
        # the relations it answers, and Python's plan-and-run plans and runs it
        page = ll.Page('Markets')
        days = pd.to_datetime(['2024-01-02', '2024-01-03', '2024-01-04']).date
        px = page.grid(pd.DataFrame({'symbol': ['SPX'] * 3, 'day': days, 'close': [100.0, 101.0, 99.5]}), name='prices')
        rates = page.grid(pd.DataFrame({'tenor': ['10Y'] * 3, 'obs_date': days, 'yield': [4.0, 4.1, 3.9]}), name='rates')
        engine = datacube._session.engine()

        def asked(**given):
            answer = engine.answer('POST', '/join.json', '', _json.dumps({'op': 'together', 'page': _json.dumps(page.to_dict()), **given}))
            return answer.status, answer.body.decode('utf-8')

        status, body = asked(grids=[px.id, rates.id])
        self.assertEqual(status, 200, body)
        said = json.loads(body)
        self.assertTrue(said['model'] and said['runtime'], 'one model of both grids\' tables')
        self.assertEqual(len(said['relations']), 2, 'each grid\'s relation over it, in order')
        self.assertTrue(all(isinstance(r, dict) and '_type' in r for r in said['relations']), 'as the protocol writes one')
        self.assertEqual(said['notes'], [])
        # a query over the model, planned and run by Python's plan-and-run: each relation answers its own frame's rows
        for relation, column in zip(said['relations'], ['close', 'yield']):
            tree = {'_type': 'lambda', 'parameters': [], 'body': [relation]}
            ran = engine.answer('POST', '/plan-and-run.json', '', json.dumps(
                {'lambda': _json.dumps(tree), 'model': said['model'], 'runtime': said['runtime']}))
            self.assertEqual(ran.status, 200, ran.body[:300])
            import pyarrow.ipc
            table = pyarrow.ipc.open_stream(ran.body).read_all()
            self.assertEqual(table.num_rows, 3)
            self.assertIn(column, table.column_names)
        # refused in its words: no grids, a tile that is no grid
        self.assertEqual(asked(grids=[])[0], 400)
        status, body = asked(grids=[px.id, 'nope'])
        self.assertEqual(status, 422)
        self.assertIn("'nope' is no grid nor cube of the page", body)
        # the second audit's P-4: a grid removed while a timeseries reads it -- its cube kept for the tile -- read with
        # the other as the tab reads them, not refused as no grid of the page
        page.timeseries({'px': px, 'rates': rates}, ['px:close[SPX] / rates:yield[10Y]'])
        kept = rates.id
        page.remove(rates)
        self.assertIn(kept, [c['id'] for c in page.to_dict()['cubes']], 'its cube kept for the tile')
        status, body = asked(grids=[px.id, kept])
        self.assertEqual(status, 200, body)
        self.assertEqual(len(json.loads(body)['relations']), 2)

    def test_shown_served_with_both_frames_versions(self):
        page, joined = self.joined()
        tab = ll.show(page, browser=False)
        try:
            status, versions = self.get(f'/version.json?page={page._key}')
            self.assertEqual((status, sorted(versions['frames'])), (200, ['desks', 'trades']))
            # the page in DataCube's hands, edited there: a join of frames is a page Python holds
            self.assertEqual(self.said(page, [page_edit(kind='renamePage', name='Joined, renamed there')]), 200)
            self.assertEqual(page.name, 'Joined, renamed there')
        finally:
            tab.close()


class Flowed(Paged):
    """A grid over another grid's rows, read live (docs/DATACUBE_FLOW_MODE_2026_10_24.md §3.1, FL20): a flow's box after
    another, made by page.grid(input=...) as DataCube makes one (headless rowsCube) -- its rows its input's as that grid
    is now, so an edit there reaches it -- saved with its link, loaded over the frames its chain reads, and joined as a
    copy of what it was (a frozen input holds no link)."""

    def chain(self):
        page = ll.Page('Flow')
        source = page.grid(trades(), name='trades')
        big = page.grid(input=source)
        big.filter([('notional', 'greaterThan', 90)])
        return page, source, big

    def test_a_box_reads_its_inputs_rows_as_that_grid_is_now(self):
        page, source, big = self.chain()
        self.assertEqual(big._cube['source']['_type'], 'rows')
        self.assertEqual(big._cube['source']['input'], {'grid': source.id}, 'a live input: the grid it reads, by id')
        self.assertEqual(big.frames, ['trades'], 'what it reads is what its input reads')
        self.assertEqual(sorted(big.to_pandas()['notional']), [100.0, 250.0])
        # an edit of the grid it reads reaches it
        source.filter([('region', 'equal', 'EMEA')])
        self.assertEqual(sorted(big.to_pandas()['notional']), [100.0])

    def test_a_box_after_a_box_and_after_a_grouped_one_its_groups(self):
        page, source, big = self.chain()
        after = page.grid(input=big)
        self.assertEqual(after.frames, ['trades'])
        self.assertEqual(sorted(after.to_pandas()['notional']), [100.0, 250.0], "its input's rows, filtered")
        # a grouped grid's rows at their finest level are its groups (query.ts classLambda)
        big.group('region')
        self.assertEqual(sorted(after.to_pandas()['region']), ['AMER', 'EMEA'])

    def test_refused_in_words(self):
        page, source, _big = self.chain()
        with self.assertRaisesRegex(ValueError, 'take no name'):
            page.grid(input=source, name='renamed')
        with self.assertRaisesRegex(ValueError, 'one of them'):
            page.grid(trades(), input=source)
        other = ll.Page('Other')
        theirs = other.grid(trades(), name='other_trades')
        with self.assertRaisesRegex(ValueError, "another page's"):
            page.grid(input=theirs)
        self.assertEqual(len(page.grids), 2, 'nothing added')

    def test_saved_with_its_link_and_loaded_over_the_frames_its_chain_reads(self):
        page, source, _big = self.chain()
        saved = page.to_dict()
        cube = next(c['cube'] for c in saved['cubes'] if c['cube']['source']['_type'] == 'rows')
        self.assertEqual(cube['source']['input'], {'grid': source.id})
        with self.assertRaisesRegex(ValueError, 'the page reads the frames trades'):
            ll.Page.load(saved, frames={})
        again = ll.Page.load(saved, frames={'trades': trades()})
        box = next(g for g in again.grids if g._cube['source']['_type'] == 'rows')
        self.assertEqual(sorted(box.to_pandas()['notional']), [100.0, 250.0])

    def test_joined_as_a_copy_of_what_it_was_holding_no_link(self):
        page, _source, big = self.chain()
        theirs = page.grid(desks(), name='desks')
        joined = page.join(big, theirs, Joined.HOW)
        left = joined._cube['source']['left']['source']
        self.assertEqual(left['_type'], 'rows')
        self.assertNotIn('grid', left['input'], "a frozen input: what the grid was, its source resolved")
        self.assertEqual(sorted(joined.to_pandas()['notional']), [100.0, 250.0])


class Flows(Paged):
    """A flow sheet (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL4, FL20): page.flow() adds one as Create ▸ Flow does, its
    boxes added by flow.grid(...) -- each after the box it reads, where the palette puts one -- and moved by box.place(x,
    y) as a drag moves one; what only a board takes refused in DataCube's words; saved and loaded as it was."""

    def flowed(self):
        page = ll.Page('Flow')
        flow = page.flow()
        source = flow.grid(trades(), name='trades')
        big = flow.grid(input=source)
        big.filter([('notional', 'greaterThan', 90)])
        return page, flow, source, big

    def boxes(self, page, flow):
        sheet = next(s for s in page.to_dict()['sheets'] if s['id'] == flow.id)
        return sheet['layout']['boxes']

    def test_a_flow_of_boxes_each_after_the_box_it_reads(self):
        page, flow, source, big = self.flowed()
        self.assertTrue(flow.flow)
        self.assertFalse(page.sheets[0].flow)
        self.assertEqual([t.id for t in flow.tiles], [source.id, big.id])
        first, after = self.boxes(page, flow)
        self.assertEqual((first['x'], first['y']), (40, 40))
        self.assertEqual(after['y'], first['y'], 'after the box it reads, on its row')
        self.assertGreater(after['x'], first['x'])
        self.assertEqual(sorted(big.to_pandas()['notional']), [100.0, 250.0])

    def test_a_box_duplicated_reads_what_it_reads(self):
        # M7's Duplicate (page-gestures.ts duplicateCommands; the review of 2026-10-26): a flow's box copied reads what it
        # read, placed after that box, as the canvas's Duplicate places it -- not after the box copied, as a board's grid
        page, flow, source, big = self.flowed()
        twin = big.copy()
        self.assertEqual(twin.sheet, flow)
        self.assertEqual(twin.input, source)
        self.assertEqual(sorted(twin.to_pandas()['notional']), [100.0, 250.0])
        boxes = {b['tile']: b for b in self.boxes(page, flow)}
        self.assertEqual(boxes[twin.id]['x'], boxes[big.id]['x'], 'after the Input, beside its Filter')

    def test_a_timeseries_of_a_box_and_another_grid(self):
        # a timeseries of several grids reads them together, one a flow's box: its input resolved against the page first,
        # as the box is read (headless join.ts sourcesOnHost; the review of 2026-10-26)
        page = ll.Page('Markets')
        days = pd.to_datetime(['2024-01-02', '2024-01-03', '2024-01-04']).date
        prices = pd.DataFrame({'symbol': ['SPX'] * 3, 'day': days, 'close': [100.0, 101.0, 99.5]})
        yields = pd.DataFrame({'tenor': ['10Y'] * 3, 'obs_date': days, 'yield': [4.0, 4.1, 3.9]})
        flow = page.flow()
        raw = flow.grid(prices, name='prices')
        px = flow.grid(input=raw)
        px.filter([('close', 'greaterThan', 99.6)])
        rates = page.grid(yields, name='rates')
        tile = page.sheets[0].timeseries({'px': px, 'rates': rates}, ['px:close[SPX]', 'rates:yield[10Y]', 'correlation($A, $B, 2)'])
        alone = tile.frame('A')
        self.assertEqual(sorted(v for v in alone.iloc[:, 0] if pd.notna(v)), [100.0, 101.0], "the box's rows, its filter kept")
        # a series of both: the two read together, the box's input resolved
        self.assertGreater(len(tile.frame('C')), 0)

    def test_a_box_placed_by_hand(self):
        page, flow, _source, big = self.flowed()
        big.place(300, 120)
        self.assertEqual(self.boxes(page, flow)[1], {'tile': big.id, 'x': 300, 'y': 120})
        with self.assertRaisesRegex(ValueError, 'neither below zero'):
            big.place(-1, 0)
        board = page.grid(trades(), name='board_trades')
        with self.assertRaisesRegex(ValueError, "a tile of a board, placed by its layout"):
            board.place(0, 0)

    def test_what_only_a_board_takes_refused_in_words(self):
        page, flow, source, _big = self.flowed()
        # a text tile on a flow is a comment (M7): placed on its canvas as a box is
        note = flow.text('Trades over 90 only')
        note.place(500, 20)
        self.assertIn({'tile': note.id, 'x': 500, 'y': 20}, self.boxes(page, flow))
        with self.assertRaisesRegex(ValueError, 'is a box of a flow: it moves to another flow sheet'):
            source.move_to(page.sheets[0])
        with self.assertRaisesRegex(ValueError, 'placed on its canvas'):
            flow.layout([source])
        with self.assertRaisesRegex(ValueError, 'takes no sheet='):
            flow.grid(trades(), name='other', sheet=flow)

    def test_saved_and_loaded_as_it_was(self):
        page, flow, _source, _big = self.flowed()
        saved = page.to_dict()
        again = ll.Page.load(saved, frames={'trades': trades()})
        self.assertEqual(again.to_dict()['sheets'], saved['sheets'])
        self.assertTrue(again.sheets[1].flow)

    def test_tidied_as_the_canvas_tidies_it(self):
        # M7: each box a column after the boxes it reads, level with them -- then nothing to do
        page, flow, source, big = self.flowed()
        top = flow.grid(input=source)
        big.place(500, 300)
        flow.tidy()
        places = {b['tile']: (b['x'], b['y']) for b in self.boxes(page, flow)}
        self.assertEqual(places[source.id], (40, 40))
        # both read the Input: level with it, in the order they were down the canvas (the Filter moved below)
        self.assertEqual(places[top.id], (40 + 184 + 64, 40))
        self.assertEqual(places[big.id], (40 + 184 + 64, 40 + 60 + 32))
        steps = len(page._page_commands)
        flow.tidy()
        self.assertEqual(len(page._page_commands), steps, 'tidy already: nothing told')
        with self.assertRaisesRegex(ValueError, 'is a board: its tiles are laid out in bands'):
            page.sheets[0].tidy()

    def test_a_box_relinked_and_frozen(self):
        # docs/DATACUBE_FLOW_MODE_2026_10_24.md M2: a line drawn from another box's output, a box frozen
        page, flow, source, big = self.flowed()
        top = flow.grid(input=source)
        top.sort('notional', 'desc')
        self.assertEqual(top.input, source)
        self.assertEqual(len(top.to_pandas()), 3)
        top.relink(big)
        self.assertEqual(top.input, big)
        self.assertEqual(sorted(top.to_pandas()['notional']), [100.0, 250.0], 'the Filter\'s rows')
        # frozen: an edit of the box it read no longer reaches it
        top.freeze()
        self.assertIsNone(top.input)
        big.filter([('notional', 'greaterThan', 200)])
        self.assertEqual(sorted(top.to_pandas()['notional']), [100.0, 250.0])
        top.relink(big)
        self.assertEqual(sorted(top.to_pandas()['notional']), [250.0])
        with self.assertRaisesRegex(ValueError, 'a relink: .*reads its own rows'):
            big.relink(top)
        # a Filter's False (FL7): the rows its condition leaves out
        top.relink(big, port='false')
        self.assertEqual(sorted(top.to_pandas()['notional']), [75.5, 100.0])
        top.relink(big)
        with self.assertRaisesRegex(ValueError, 'a freeze: .*reads no other grid: it has no input'):
            source.freeze()
        # saved and loaded: the link kept
        again = ll.Page.load(page.to_dict(), frames={'trades': trades()})
        self.assertEqual(next(g for g in again.grids if g.id == top.id).input.id, big.id)

    def test_outputs_written_when_the_flow_runs(self):
        # M6: a box set to write a CSV file (setOutput) writes every row, the columns it shows, when its flow runs
        page, flow, _source, big = self.flowed()
        big.configure({'columns': {'pnl': {'hidden': True}}})
        big.output('big trades.csv')
        box = next(b for b in self.boxes(page, flow) if b['tile'] == big.id)
        self.assertEqual(box['output'], {'format': 'csv', 'name': 'big trades.csv'})
        with tempfile.TemporaryDirectory() as folder:
            written = flow.run(folder)
            self.assertEqual([p.name for p in written], ['big trades.csv'])
            back = pd.read_csv(written[0])
            self.assertEqual(sorted(back.columns), ['desk', 'notional', 'region'])
            self.assertEqual(sorted(back['notional']), [100.0, 250.0])
        with self.assertRaisesRegex(ValueError, 'ends .csv'):
            big.output('big.xlsx')
        with self.assertRaisesRegex(ValueError, 'no folder in it'):
            big.output('out/big.csv')
        with self.assertRaisesRegex(ValueError, 'writes big trades.csv already'):
            big.output('big trades.csv')
        again = ll.Page.load(page.to_dict(), frames={'trades': trades()})
        self.assertEqual(next(b for b in next(s for s in again.to_dict()['sheets'] if s['id'] == flow.id)['layout']['boxes']
                              if b['tile'] == big.id)['output']['name'], 'big trades.csv')
        big.output(None)
        self.assertNotIn('output', next(b for b in self.boxes(page, flow) if b['tile'] == big.id))
        with self.assertRaisesRegex(ValueError, 'is a board'):
            page.sheets[0].run('.')

    def test_a_table_output_is_written_in_one_transaction_each_way(self):
        # M6, FL13: a box set to write a table of the frames' database (grid.output_table) -- made for its rows, appended,
        # its rows deleted then appended, merged by a key, made again -- by DataCube's one table writer, the compiler's
        # write run in one transaction; what the run wrote said
        page, flow, _source, big = self.flowed()
        big.output_table('big_trades')
        box = next(b for b in self.boxes(page, flow) if b['tile'] == big.id)
        self.assertEqual(box['output'], {'format': 'table', 'name': 'big_trades', 'mode': 'create'})
        con = page._session.frames.connection
        table = lambda: sorted(con.execute('SELECT notional FROM big_trades').fetchall())
        with tempfile.TemporaryDirectory() as folder:
            self.assertEqual(flow.run(folder), ['table big_trades'])
            self.assertEqual(table(), [(100.0,), (250.0,)])
            # made already: the database's words, and the table as it was
            with self.assertRaisesRegex(ValueError, 'already exists'):
                flow.run(folder)
            self.assertEqual(table(), [(100.0,), (250.0,)])
            big.output_table('big_trades', 'append')
            flow.run(folder)
            self.assertEqual(table(), [(100.0,), (100.0,), (250.0,), (250.0,)])
            big.output_table('big_trades', 'deleteAppend')
            flow.run(folder)
            self.assertEqual(table(), [(100.0,), (250.0,)])
            big.output_table('big_trades', 'overwrite')
            self.assertEqual(flow.dry_run()['would_write'], ['table big_trades'])
            flow.run(folder)
            self.assertEqual(table(), [(100.0,), (250.0,)])
        with self.assertRaisesRegex(ValueError, 'a merge matches its rows by a key'):
            big.output_table('big_trades', 'merge')
        with self.assertRaisesRegex(ValueError, 'letters, digits and underscores'):
            big.output_table('big trades')
        # a frame's own table is none an Output writes
        big.output_table('trades', 'overwrite')
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValueError, "one of this page's own tables"):
                flow.run(folder)

    def test_an_output_writes_what_the_box_hands_on(self):
        # one writer (flow-output.ts outputCsv, headless gridOutput; the review of 2026-10-26's B2, C1, C9, C10): every row
        # in its sorts' order, the columns it hands on under their names, in its order, each value as it is -- the tab's
        # own text, its BOM and CRLFs
        page, flow, source, _big = self.flowed()
        chosen = flow.grid(input=source)
        chosen.select(['notional', 'region'], rename={'region': 'area'}, order=['notional', 'region'])
        chosen.sort('notional', 'desc')
        chosen.output('chosen.csv')
        phones = flow.grid(pd.DataFrame({'phone': ['+44 20 7946 0958', '-']}), name='phones')
        phones.output('phones.csv')
        with tempfile.TemporaryDirectory() as folder:
            written = {p.name: p.read_bytes().decode('utf-8') for p in flow.run(folder)}
        self.assertEqual(written['chosen.csv'], '\ufeffnotional,area\r\n250,AMER\r\n100,EMEA\r\n75.5,EMEA\r\n')
        self.assertEqual(written['phones.csv'], '\ufeffphone\r\n+44 20 7946 0958\r\n-\r\n', 'no value defused')

    def test_a_select_keeps_and_renames_for_what_reads_it(self):
        # a flow's Select (select-columns.ts selectCommands): the columns it keeps, by the names it gives them -- the box
        # after it reads them so (the relation's rename), its own grid showing them so
        page, flow, source, big = self.flowed()
        chosen = flow.grid(input=big)
        chosen.select(['region', 'notional'], rename={'region': 'area'})
        after = flow.grid(input=chosen)
        rows = after.to_pandas()
        self.assertEqual(sorted(rows.columns), ['area', 'notional'])
        self.assertEqual(sorted(rows['area']), sorted(big.to_pandas()['region']))
        with self.assertRaisesRegex(ValueError, "would both be handed on as 'notional'"):
            chosen.select(['region', 'notional'], rename={'region': 'notional'})
        # retyped: text made whole numbers, handed on by its own name, the box after it reading numbers
        codes = flow.grid(pd.DataFrame({'code': ['1', '22', '333'], 'n': [1, 2, 3]}), name='codes')
        typed = flow.grid(input=codes)
        typed.select(['code', 'n'], retype={'code': 'Integer'})
        read = flow.grid(input=typed).to_pandas()
        self.assertEqual(sorted(read.columns), ['code', 'n'])
        self.assertEqual(sorted(int(v) for v in read['code']), [1, 22, 333])
        self.assertTrue(pd.api.types.is_integer_dtype(read['code']), read.dtypes)
        with self.assertRaisesRegex(ValueError, "'n' \\(Integer\\) cannot be made Boolean"):
            typed.select(['code', 'n'], retype={'n': 'Boolean'})
        # ordered: handed on in its order, the retyped column in its place -- the box after it reads them so
        typed.select(['code', 'n'], retype={'code': 'Integer'}, order=['n', 'code'])
        self.assertEqual(list(flow.grid(input=typed).to_pandas().columns), ['n', 'code'])
        typed.select(['code', 'n'], retype={'code': 'Integer'}, order=['code', 'n'])
        self.assertEqual(list(flow.grid(input=typed).to_pandas().columns), ['code', 'n'], 'its conversion where its column was')
        with self.assertRaisesRegex(ValueError, "its order names no column 'nope'"):
            typed.select(['code'], order=['nope'])

    def test_tools_in_place_hand_their_columns_on_by_their_own_names(self):
        # Alteryx's in place: a Data Cleansing's, a Multi-Field Formula's, an Imputation's columns handed on by the names
        # of the columns they are made of (box-columns.ts inPlaceCommand), the box after them reading those
        page = ll.Page('In place')
        flow = page.flow()
        source = flow.grid(pd.DataFrame({'who': [' ana ', 'Ben'], 'n': [1.0, None], 'm': [2.0, 4.0]}), name='raw')
        tidy = flow.grid(input=source)
        tidy.cleanse(['who'], case='upper', in_place=True)
        tidy.impute(['n'], fill=0, in_place=True)
        tidy.multi_field(['m'], '$v->toOne() * 10', suffix='_x10', in_place=True)
        rows = flow.grid(input=tidy).to_pandas()
        self.assertEqual(sorted(rows.columns), ['m', 'n', 'who'])
        self.assertEqual(sorted(zip(rows['who'], rows['n'], rows['m'])), [('ANA', 1.0, 20.0), ('BEN', 0.0, 40.0)])
        # unsaid, as its window starts: in place on a flow's box; beside its column on a board's grid (the review of
        # 2026-10-26: Python's default was the other way)
        again = flow.grid(input=source)
        again.cleanse(['who'], case='upper')
        self.assertEqual(sorted(flow.grid(input=again).to_pandas().columns), ['m', 'n', 'who'])
        board = page.grid(pd.DataFrame({'who': ['x']}), name='board_who')
        board.cleanse(['who'])
        self.assertIn('who_clean', board.to_pandas().columns)

    def test_a_summarize_gives_its_keys_and_aggregates(self):
        # a box after a Summarize reads a row per group, its keys and its aggregates -- the dimensions it is not grouped
        # by left out (snapshot.ts ungroupedDimensions: Alteryx's Summarize), as the canvas says and the screen reads
        page, flow, source, big = self.flowed()
        by_region = flow.grid(input=big)
        by_region.group('region')
        after = flow.grid(input=by_region)
        rows = after.to_pandas()
        self.assertEqual(sorted(rows.columns), ['notional', 'pnl', 'region'])
        self.assertEqual(sorted(rows['region']), ['AMER', 'EMEA'])

    def test_a_container_frames_boxes(self):
        # M7: a comment with a frame is a container (Alteryx's Tool Container): its size kept by a move, folded or not;
        # a grid is never framed; a frame too small refused; taken off, a comment again
        page, flow, _source, big = self.flowed()
        box = flow.text('Prep')
        box.place(20, 300, width=420, height=160)
        self.assertIn({'tile': box.id, 'x': 20, 'y': 300, 'frame': {'w': 420, 'h': 160}}, self.boxes(page, flow))
        box.place(30, 310)
        self.assertIn({'tile': box.id, 'x': 30, 'y': 310, 'frame': {'w': 420, 'h': 160}}, self.boxes(page, flow))
        # a box placed inside it moves with it, by as much, as the canvas's drag moves it (flow.ts containerMoves; the
        # review of 2026-10-26's B6)
        big.place(60, 360)
        box.place(130, 410)
        self.assertIn({'tile': big.id, 'x': 160, 'y': 460}, self.boxes(page, flow))
        box.place(30, 310)
        self.assertIn({'tile': big.id, 'x': 60, 'y': 360}, self.boxes(page, flow))
        box.place(30, 310, width=420, height=160, folded=True)
        self.assertIn({'tile': box.id, 'x': 30, 'y': 310, 'frame': {'w': 420, 'h': 160, 'collapsed': True}}, self.boxes(page, flow))
        with self.assertRaisesRegex(ValueError, 'a frame is at least 200 by 100'):
            box.place(30, 310, width=50, height=50)
        with self.assertRaisesRegex(ValueError, 'a container is a comment'):
            page._apply_page([{'kind': 'placeBox', 'tile': big.id, 'x': 0, 'y': 0, 'frame': {'w': 300, 'h': 200}}])
        again = ll.Page.load(page.to_dict(), frames={'trades': trades()})
        self.assertIn({'tile': box.id, 'x': 30, 'y': 310, 'frame': {'w': 420, 'h': 160, 'collapsed': True}},
                      next(s for s in again.to_dict()['sheets'] if s['id'] == flow.id)['layout']['boxes'])
        box.unframe()
        self.assertIn({'tile': box.id, 'x': 30, 'y': 310}, self.boxes(page, flow))

    def test_unique_and_its_duplicates(self):
        # M3: a flow's Unique (unique.ts uniqueCommands) -- a row of each key, its first by an order -- and its
        # Duplicates, the filter's False port (FL7): every row after a key's first
        page, flow, source, _big = self.flowed()
        firsts = flow.grid(input=source)
        firsts.unique(['region'], by=('notional', 'desc'))
        self.assertEqual(sorted(firsts.to_pandas()['notional']), [100.0, 250.0], "each region's largest")
        # its row number hidden: a box's rows are the columns it shows, so the box after it has none of it
        after = flow.grid(input=firsts)
        self.assertEqual(sorted(after.to_pandas().columns), ['desk', 'notional', 'pnl', 'region'])
        # and so a Select's (a column hidden): the box after it reads the columns it keeps
        kept = flow.grid(input=source)
        kept.configure({'columns': {'pnl': {'hidden': True}}})
        self.assertEqual(sorted(flow.grid(input=kept).to_pandas().columns), ['desk', 'notional', 'region'])
        dupes = flow.grid(input=source)
        dupes.relink(firsts, port='false')
        self.assertEqual(sorted(dupes.to_pandas()['notional']), [75.5])
        with self.assertRaisesRegex(ValueError, "the grid has no column 'nope'"):
            flow.grid(input=source).unique(['nope'])
        with self.assertRaisesRegex(ValueError, 'a key is at least one column'):
            flow.grid(input=source).unique([])

    def test_a_join_box_reads_both_live_and_is_set_again(self):
        # M2: a join of two boxes of a flow is live (FL6) -- an edit of either reaches it -- and set again by rejoin
        page, flow, source, big = self.flowed()
        on_region = {'form': 'join', 'kind': 'inner', 'on': {'rows': [{'left': 'region', 'op': '==', 'right': 'region'}]},
                     'prefix': 'their_', 'keep': None}
        joined = page.join(big, source, on_region)
        self.assertEqual(joined.sheet, flow)
        self.assertEqual(joined.inputs, {'left': big, 'right': source})
        # named as the canvas names its boxes, as the tab's Join window names it (flow-boxes.ts pageBoxName; the review of
        # 2026-10-26's B3): each untitled, by its tool
        doc = page.to_dict()
        view = next(v for v in doc['views'] if v['id'] == joined.id)
        self.assertEqual(next(c for c in doc['cubes'] if c['id'] == view['cube'])['cube']['source']['name'], 'Filter ⋈ Input')
        # the Filter's two rows (EMEA, AMER) against the three: EMEA twice, AMER once
        self.assertEqual(len(joined.to_pandas()), 3)
        big.filter([('notional', 'greaterThan', 200)])
        self.assertEqual(len(joined.to_pandas()), 1, 'an edit of its Left reaches it')
        joined.rejoin({'form': 'append'})
        self.assertEqual(len(joined.to_pandas()), 1 + 3, 'a Union now: the rows of both')
        with self.assertRaisesRegex(ValueError, 'joins so already'):
            joined.rejoin({'form': 'append'})
        with self.assertRaisesRegex(ValueError, 'is no join'):
            big.rejoin({'form': 'append'})
        joined.relink(source, side='left')
        self.assertEqual(joined.inputs, {'left': source, 'right': source})
        # on a board: kept by value, as before
        board = page.grid(trades(), name='board_trades')
        other = page.grid(trades(), name='board_more')
        copied = page.join(board, other, on_region)
        self.assertEqual(copied.inputs, {})
        # saved and loaded: the links kept
        again = ll.Page.load(page.to_dict(), frames={'trades': trades(), 'board_trades': trades(), 'board_more': trades()})
        self.assertEqual({k: g.id for k, g in next(g for g in again.grids if g.id == joined.id).inputs.items()},
                         {'left': source.id, 'right': source.id})


    def test_a_test_that_stops_a_run_writes_no_output(self):
        # FL8: a Test box set to stop a run (grid.stops_run) -- its checks failing, flow.run writes no Output and says why;
        # set to warn, it writes; its failing checks read as the canvas reads them (grid.failing)
        page, flow, source, _big = self.flowed()
        checked = flow.grid(input=source)
        checked.test([{'kind': 'between', 'column': 'notional', 'min': 0, 'max': 200}, {'kind': 'notEmpty', 'column': 'desk'}])
        self.assertEqual(checked.failing, [{'check': 'notional from 0 to 200', 'rows': 1, 'says': 'notional from 0 to 200: 1 row fails'}])
        out = flow.grid(input=source)
        out.output('trades out.csv')
        checked.stops_run()
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValueError, r'stops the run: notional from 0 to 200: 1 row fails: no Output written'):
                flow.run(folder)
            self.assertEqual(os.listdir(folder), [], 'nothing written')
            checked.stops_run(False)
            self.assertEqual([p.name for p in flow.run(folder)], ['trades out.csv'], 'a Test that warns: written')
        with self.assertRaisesRegex(ValueError, 'stops no run'):
            checked.stops_run(False)
        with self.assertRaisesRegex(ValueError, 'it checks nothing: it is no Test'):
            _ = source.failing
        # saved and loaded: it stops a run still
        checked.stops_run()
        again = ll.Page.load(page.to_dict(), frames={'trades': trades()})
        self.assertTrue(next(b for s in again.to_dict()['sheets'] for b in s['layout'].get('boxes', []) if b['tile'] == checked.id).get('stops'))

    def test_a_flow_runs_in_stages_says_its_messages_and_keeps_its_runs(self):
        # M8: Block Until Done -- the boxes after a box that blocks run, and their Outputs are written, once every Output
        # before it is (headless flowRun: the canvas's Run's stages); a Message's words (a warning warned, an error
        # stopping the run from its stage on); each run kept in flow.runs (the run history, this process's)
        page, flow, source, big = self.flowed()
        source.output('first.csv')
        big.block_until_done()
        later = flow.grid(input=big)
        later.output('later.csv')
        checked = flow.grid(input=big)
        checked.test([{'kind': 'between', 'column': 'notional', 'min': 0, 'max': 200}])
        checked.stops_run()
        with tempfile.TemporaryDirectory() as folder:
            # the Test is after the block: the stage before it written, its own stage's not
            with self.assertRaisesRegex(ValueError, r'stops the run: notional from 0 to 200: 1 row fails: no Output written \(written before it: .*first\.csv\)'):
                flow.run(folder)
            self.assertEqual(os.listdir(folder), ['first.csv'])
            checked.stops_run(False)
            self.assertEqual([p.name for p in flow.run(folder)], ['first.csv', 'later.csv'])
            # a Message: a warning warned
            big.message('{rows} big trades', when='rows', level='warning')
            with self.assertWarnsRegex(UserWarning, re.escape(f'{big.id}: 2 big trades')):
                flow.run(folder)
            # an error, when its rows are as it asks: the run stopped from its stage on -- the block's own stage, so nothing
            big.message('{rows} too many', when='rows', level='error')
            with self.assertRaisesRegex(ValueError, re.escape(f'{big.id} said 2 too many: no Output written')):
                flow.run(folder)
            big.message('none at all', when='noRows', level='error')
            self.assertEqual(len(flow.run(folder)), 2, 'its rows not as it asks: nothing said')
        big.message('{rows} too many', when='rows', level='error')
        dry = flow.dry_run()
        self.assertEqual(dry['messages'], [{'box': big.id, 'level': 'error', 'said': '2 too many'}])
        self.assertEqual(dry['would_write'], [])
        self.assertEqual(set(dry['rows']), {source.id, big.id, later.id, checked.id}, 'every box counted all the same')
        runs = flow.runs
        self.assertEqual([r['dry'] for r in runs], [False, False, False, False, False, True])
        self.assertEqual([Path(w).name for w in runs[0]['written']], ['first.csv'])
        self.assertTrue(runs[0]['stopped'])
        self.assertEqual((runs[1]['stopped'], runs[2]['messages']), ([], [{'box': big.id, 'level': 'warning', 'said': '2 big trades'}]))
        with self.assertRaisesRegex(ValueError, 'says so already'):
            big.message('{rows} too many', when='rows', level='error')
        with self.assertRaisesRegex(ValueError, 'a message is info, warning, error, not "loud"'):
            big.message('x', level='loud')
        big.message(None)
        with self.assertRaisesRegex(ValueError, 'says nothing'):
            big.message(None)
        # saved and loaded: it blocks still
        again = ll.Page.load(page.to_dict(), frames={'trades': trades()})
        self.assertTrue(next(b for s in again.to_dict()['sheets'] for b in s['layout'].get('boxes', []) if b['tile'] == big.id).get('blocks'))
        big.block_until_done(False)
        with self.assertRaisesRegex(ValueError, 'blocks no run'):
            big.block_until_done(False)

    def test_a_cross_tab_with_its_columns_fixed_is_read_by_the_box_after_it(self):
        # FL2: a Cross Tab box (desk by region, its measures summed) -- its columns its values, which a box after it
        # reads once Load values pins them (grid.load_pivot_values: asked of the database), each value's column named
        # by it; pinned by hand to fewer, fewer; taken off, refused in words
        page, flow, source, _big = self.flowed()
        crossed = flow.grid(input=source)
        crossed.group('desk').pivot('region')
        crossed.load_pivot_values()
        after = flow.grid(input=crossed)
        rows = after.to_pandas()
        self.assertEqual(list(rows.columns), ['desk', 'AMER_notional', 'AMER_pnl', 'EMEA_notional', 'EMEA_pnl', 'Total_notional', 'Total_pnl'])
        by_desk = {r['desk']: r for r in rows.to_dict('records')}
        self.assertEqual(by_desk['FX']['EMEA_notional'], 100.0)
        self.assertEqual(by_desk['Rates']['AMER_notional'], 250.0)
        self.assertTrue(pd.isna(by_desk['FX']['AMER_notional']), "FX has no AMER row: its cell empty")
        self.assertEqual(crossed.failing, [], 'every value a column')
        crossed.pin_pivot(['EMEA'])
        self.assertEqual(list(after.to_pandas().columns), ['desk', 'EMEA_notional', 'EMEA_pnl', 'Total_notional', 'Total_pnl'], 'its columns the pinned ones')
        # FL3: a value its rows hold that its columns lack, said by the database's answer -- and, set to stop a run (FL8),
        # no Output written; its values loaded again, written
        self.assertEqual(crossed.failing, [{'check': 'every region has a column', 'rows': 1,
                                            'says': 'region AMER has no column of its own: add it to its columns, or Load values again'}])
        after.output('crossed.csv')
        crossed.stops_run()
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValueError, 'stops the run: region AMER has no column of its own'):
                flow.run(folder)
            self.assertEqual(os.listdir(folder), [], 'nothing written')
            crossed.load_pivot_values()
            self.assertEqual(crossed.failing, [])
            self.assertEqual([p.name for p in flow.run(folder)], ['crossed.csv'])
        # Add to the cast (FL8): the value its columns lack added after those it has
        crossed.pin_pivot(['EMEA'])
        crossed.add_pivot_values(['AMER'])
        self.assertEqual(list(after.to_pandas().columns), ['desk', 'EMEA_notional', 'EMEA_pnl', 'AMER_notional', 'AMER_pnl', 'Total_notional', 'Total_pnl'])
        with self.assertRaisesRegex(ValueError, 'a column for each of them already'):
            crossed.add_pivot_values(['AMER'])
        crossed.pin_pivot(['EMEA'])
        with self.assertRaisesRegex(ValueError, "'EMEA' is pinned twice"):
            crossed.pin_pivot(['EMEA', 'EMEA'])
        crossed.pin_pivot(None)
        with self.assertRaisesRegex(ValueError, 'Load its values to fix them'):
            after.to_pandas()

    def test_number_columns_binned_as_a_binning_box_bins_them(self):
        # M3: grid.binning -- each number column into N bins of equal width over its own range, the database placing each
        # value; the box after it reads each bin's lower edge beside its column, the range and upper edge hidden
        page, flow, source, _big = self.flowed()
        binned = flow.grid(input=source)
        binned.binning(['notional', 'pnl'], 2)
        rows = flow.grid(input=binned).to_pandas()
        self.assertIn('notional_bins', rows.columns)
        self.assertIn('pnl_bins', rows.columns)
        self.assertFalse([c for c in rows.columns if c.endswith('__min') or c.endswith('__upper')], 'its helpers hidden')
        # notional 75.5 to 250: two bins of 87.25, the largest in the last
        self.assertEqual(sorted(zip(rows['notional'], rows['notional_bins'])), [(75.5, 75.5), (100.0, 75.5), (250.0, 162.75)])
        with self.assertRaisesRegex(ValueError, "'region' is String: only a number is put into bins"):
            binned.binning(['region'], 3)
        with self.assertRaisesRegex(ValueError, 'a count of bins is a whole number from 1'):
            binned.binning(['notional'], 0)

    def test_rows_generated_as_a_generate_rows_box_makes_them(self):
        # M3: grid.generate_rows -- each row repeated for each number from one to another (numbers given, or its own
        # columns), or for each day between two of its dates; the database makes every row
        page = ll.Page('Generate Rows')
        flow = page.flow()
        frame = pd.DataFrame({'id': [1, 2], 'n': [3, 1], 'opened': pd.to_datetime(['2024-01-30', '2024-02-28']).date,
                              'closed': pd.to_datetime(['2024-02-02', '2024-03-01']).date})
        source = flow.grid(frame, name='spans')
        made = flow.grid(input=source)
        made.generate_rows('k', 1, 'n')
        rows = flow.grid(input=made).to_pandas()
        self.assertEqual(sorted(zip(rows['id'], rows['k'])), [(1, 1), (1, 2), (1, 3), (2, 1)])
        made.generate_rows('k', 0, 10, step=5)
        self.assertEqual(sorted(flow.grid(input=made).to_pandas()['k']), [0, 0, 5, 5, 10, 10], 'set again: its step')
        dated = flow.grid(input=source)
        dated.generate_rows('day', days=('opened', 'closed'))
        days = flow.grid(input=dated).to_pandas()
        self.assertEqual(len(days), 4 + 3, 'Jan 30 to Feb 2, and Feb 28 to Mar 1 (2024 a leap year)')
        self.assertNotIn('day__day', days.columns, 'its number hidden')
        with self.assertRaisesRegex(ValueError, 'its first is after its last'):
            made.generate_rows('j', 5, 1)
        with self.assertRaisesRegex(ValueError, "'opened' is StrictDate: a bound is a whole number"):
            made.generate_rows('j', 'opened', 3)

    def test_a_dry_run_counts_every_box_and_writes_nothing(self):
        # M6: flow.dry_run -- every box's rows counted as the canvas counts them, the boxes that stop a run judged, the
        # files a run would write named; nothing written. A stopping Test failing: none would be, said why
        page, flow, source, big = self.flowed()
        out = flow.grid(input=source)
        out.output('trades out.csv')
        dry = flow.dry_run()
        self.assertEqual((dry['rows'][source.id], dry['rows'][big.id], dry['rows'][out.id]), (3, 2, 3))
        self.assertEqual((dry['would_write'], dry['stopped']), (['trades out.csv'], []))
        checked = flow.grid(input=source)
        checked.test([{'kind': 'rows', 'max': 1}])
        checked.stops_run()
        dry = flow.dry_run()
        self.assertEqual(dry['would_write'], [])
        self.assertEqual(len(dry['stopped']), 1)
        self.assertRegex(dry['stopped'][0], 'stops the run: at most 1 row: it has 3 rows')
        self.assertEqual(dry['rows'][checked.id], 1, 'its one row of counts')

    def test_a_custom_filter_keeps_the_rows_its_condition_is_true_for(self):
        # FL12: grid.custom_filter -- a condition in Pure over $x, parsed and typed by the compiler, the database judging;
        # its column hidden from the box after it; its False the rest; a condition that is no Boolean refused
        page, flow, source, _big = self.flowed()
        kept = flow.grid(input=source)
        kept.custom_filter("$x.notional->toOne() > 90 && $x.region == 'EMEA'")
        after = flow.grid(input=kept)
        self.assertEqual(list(after.to_pandas()['notional']), [100.0])
        self.assertNotIn('CustomFilter', after.to_pandas().columns, 'its condition hidden from the box after it')
        rest = flow.grid(input=source)
        rest.relink(kept, port='false')
        self.assertEqual(sorted(rest.to_pandas()['notional']), [75.5, 250.0])
        kept.custom_filter("$x.region == 'AMER'")
        self.assertEqual(list(after.to_pandas()['notional']), [250.0], 'set again: its condition the new one')
        with self.assertRaisesRegex(ValueError, 'a condition is true or false, a Boolean: this one is Float'):
            kept.custom_filter('$x.notional->toOne() * 2')
        with self.assertRaises(ValueError):
            kept.custom_filter('$x.nope > 1')

    def test_a_dynamic_replace_takes_its_rules_from_another_grid(self):
        # §4's Dynamic Replace: grid.dynamic_replace -- rules read off another grid's rows (the database reading them), a
        # column, a condition over $x and a value each; of a column's rules the first that holds, its own value where
        # none does; pinned in place; the compiler typing each, refusing in its words, the rule named
        page, flow, source, _big = self.flowed()
        rules = flow.grid(pd.DataFrame({
            'Column': ['region', 'region', 'notional'],
            'Condition': ["$x.region == 'EMEA' && $x.desk == 'FX'", "$x.region == 'EMEA'", '$x.notional->toOne() > 200'],
            'Value': ['Europe FX', 'Europe', '200'],
        }), name='rules')
        box = flow.grid(input=source)
        box.dynamic_replace(rules)
        rows = flow.grid(input=box).to_pandas().sort_values('pnl')
        self.assertEqual(list(rows['region']), ['AMER', 'Europe', 'Europe FX'])
        self.assertEqual(list(rows['notional']), [200.0, 75.5, 100.0])
        self.assertNotIn('region_replaced', rows.columns, 'handed on in its column\'s place')
        # loaded again, from rules given: region's alone, notional shown as it is
        box.dynamic_replace([{'column': 'region', 'condition': "$x.desk == 'Rates'", 'value': 'R'}])
        rows = flow.grid(input=box).to_pandas().sort_values('pnl')
        self.assertEqual(list(rows['region']), ['R', 'R', 'EMEA'])
        self.assertEqual(list(rows['notional']), [250.0, 75.5, 100.0])
        # values as expressions, each Pure over $x
        box.dynamic_replace([{'column': 'notional', 'condition': '$x.notional->toOne() < 90', 'value': '$x.notional->toOne() * 2.0'}],
                            expressions=True)
        self.assertEqual(sorted(flow.grid(input=box).to_pandas()['notional']), [100.0, 151.0, 250.0])
        with self.assertRaisesRegex(ValueError, r"rule 1's value \"many\" is no Float, as 'notional' is"):
            box.dynamic_replace([{'column': 'notional', 'condition': 'true', 'value': 'many'}])
        with self.assertRaisesRegex(ValueError, "rule 1 replaces 'nope', which the grid has not"):
            box.dynamic_replace([{'column': 'nope', 'condition': 'true', 'value': 1}])
        with self.assertRaisesRegex(ValueError, "rule 1's condition"):
            box.dynamic_replace([{'column': 'region', 'condition': '$x.region ==', 'value': 'a'}])
        with self.assertRaisesRegex(ValueError, "its rules have no column 'Nope'"):
            box.dynamic_replace(rules, value='Nope')

    def test_a_parameter_is_read_by_any_pure_on_the_page(self):
        # M8's parameters (an analytic app's inputs): a value of the page any Pure on it reads as $name, bound where each
        # query is sent -- a Custom Filter's condition and a Pure box over it; its value changed, every box reads it so;
        # a pick-list's value one of its choices; written into the page; taken off, a Pure still reading it refused
        page, flow, source, _big = self.flowed()
        page.parameter('min', 'Float', 90.0)
        kept = flow.grid(input=source)
        kept.custom_filter('$x.notional->toOne() > $min')
        after = flow.grid(input=kept)
        self.assertEqual(sorted(after.to_pandas()['notional']), [100.0, 250.0])
        page.set_parameter('min', 200.0)
        self.assertEqual(sorted(after.to_pandas()['notional']), [250.0], 'its value changed: read so')
        page.parameter('region', 'String', 'EMEA', choices=['EMEA', 'AMER'], label='Region')
        picked = flow.grid(input=source)
        picked.pure('$input->filter(x|$x.region == $region)')
        self.assertEqual(sorted(flow.grid(input=picked).to_pandas()['notional']), [75.5, 100.0])
        page.set_parameter('region', 'AMER')
        self.assertEqual(sorted(flow.grid(input=picked).to_pandas()['notional']), [250.0])
        self.assertEqual(page.parameters, {'min': 200.0, 'region': 'AMER'})
        self.assertEqual([p['name'] for p in page.to_dict()['parameters']], ['min', 'region'])
        with self.assertRaisesRegex(ValueError, 'none of its choices'):
            page.set_parameter('region', 'APAC')
        with self.assertRaisesRegex(ValueError, 'is no Float'):
            page.set_parameter('min', 'many')
        with self.assertRaisesRegex(ValueError, "what DataCube's own expressions call"):
            page.parameter('x', 'Integer', 1)
        # a type no parameter has: refused in DataCube's words, never read as a column's type
        with self.assertRaisesRegex(ValueError, 'rate is a String, Integer, Float, Decimal, Boolean, StrictDate, DateTime, not "Money"'):
            page.parameter('rate', 'Money', 1)
        with self.assertRaisesRegex(ValueError, "'nope' is not a parameter"):
            page.set_parameter('nope', 1)
        page.remove_parameter('min')
        with self.assertRaisesRegex(Exception, r"unbound variable '\$min'"):
            after.to_pandas()

    def test_a_parameter_reaches_a_join_a_cache_and_keeps_a_decimal_exact(self):
        # the roadmap audit of 2026-10-26: a join over a box reading a parameter opened with its value (it was unbound);
        # a cached box after it copied again when its value changes (its copy was of the old one); a Decimal's digits kept
        page, flow, source, big = self.flowed()
        page.parameter('region', 'String', 'EMEA')
        picked = flow.grid(input=source)
        picked.custom_filter('$x.region == $region')
        joined = page.join(picked, big, {'form': 'join', 'kind': 'inner', 'on': {'rows': [{'left': 'notional', 'op': '==', 'right': 'notional'}]},
                                         'prefix': 'b_', 'keep': None})
        self.assertEqual(sorted(joined.to_pandas()['notional']), [100.0])
        cached = flow.grid(input=picked)
        cached.cache()
        after = flow.grid(input=cached)
        self.assertEqual(sorted(after.to_pandas()['notional']), [75.5, 100.0])
        page.set_parameter('region', 'AMER')
        self.assertEqual(sorted(after.to_pandas()['notional']), [250.0], 'its cache copied again with the value now')
        page.parameter('rate', 'Decimal', Decimal('1.10'))
        self.assertEqual(next(p for p in page.to_dict()['parameters'] if p['name'] == 'rate')['value'], '1.10')
        self.assertEqual(page.parameters['rate'], Decimal('1.10'))
        # a table written from a cached copy: refused in words (this session's engine holds the copy)
        after.output_table('regions_out')
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValueError, "this box reads a cached box's copy"):
                flow.run(folder)

    def test_a_macro_is_a_sub_flow_used_as_one_box(self):
        # M8's standard macro: page.macro -- boxes, each over the one before it, saved as one Pure function of the first
        # one's input's rows; used after another box by grid.use_macro, a Pure box named after it; a parameter it reads
        # stays $name, read with its value now
        page, flow, source, _big = self.flowed()
        page.parameter('min', 'Float', 90.0)
        first = flow.grid(input=source)
        first.custom_filter('$x.notional->toOne() > $min')
        second = flow.grid(input=first)
        second.calculate('fee', '$x.notional->toOne() * 0.01', kind='dimension')
        page.macro('Big fees', [first, second])
        self.assertEqual(page.macros, ['Big fees'])
        other = flow.grid(input=source)
        other.use_macro('Big fees')
        self.assertEqual(other.title, 'Big fees')
        rows = flow.grid(input=other).to_pandas()
        self.assertEqual(sorted(rows['notional']), [100.0, 250.0])
        self.assertEqual(sorted(round(f, 6) for f in rows['fee']), [1.0, 2.5])
        self.assertNotIn('CustomFilter', rows.columns, 'its boxes\' rows as they hand them on')
        page.set_parameter('min', 200.0)
        self.assertEqual(sorted(flow.grid(input=other).to_pandas()['notional']), [250.0], 'its parameter read with its value now')
        with self.assertRaisesRegex(ValueError, 'reads none of'):
            page.macro('Wrong', [second, first])
        with self.assertRaisesRegex(ValueError, "reads no other box's rows"):
            page.macro('Wrong', [source])
        with self.assertRaisesRegex(ValueError, "'nope' is not a macro"):
            other.use_macro('nope')
        page.remove_macro('Big fees')
        self.assertEqual(page.macros, [])

    def test_a_pure_box_makes_its_rows_a_function_of_its_inputs(self):
        # FL12: grid.pure -- its rows a function of its input's, in Pure over $input, parsed and its rows typed as a
        # relation by the compiler before the page sets it; a box after it reads its rows, its columns the compiler's;
        # taken off, its input's again; refused in the compiler's words, or DataCube's
        page, flow, source, _big = self.flowed()
        box = flow.grid(input=source)
        box.pure("$input->filter(x|$x.region == 'EMEA')->extend(~fee:x|$x.notional->toOne() * 0.01)")
        after = flow.grid(input=box)
        rows = after.to_pandas()
        self.assertEqual(sorted(rows['notional']), [75.5, 100.0])
        self.assertEqual([round(f, 6) for f in sorted(rows['fee'])], [0.755, 1.0])
        grouped = flow.grid(input=source)
        grouped.pure('$input->groupBy(~[region], ~[total:x|$x.notional:y|$y->sum()])')
        totals = flow.grid(input=grouped).to_pandas()
        self.assertEqual(list(totals.columns), ['region', 'total'], 'its columns as the compiler types them')
        self.assertEqual(sorted(zip(totals['region'], totals['total'])), [('AMER', 250.0), ('EMEA', 175.5)])
        # the page keeps its function: written down and read back, its box says it
        cube = next(c['cube'] for c in page.to_dict()['cubes'] if c['id'] == grouped._view['cube'])
        self.assertEqual(cube['source']['pure']['_type'], 'lambda')
        box.pure(None)
        self.assertEqual(len(after.to_pandas()), 3, "taken off: its input's rows again")
        with self.assertRaisesRegex(ValueError, r'its Pure never reads \$input'):
            box.pure('1')
        with self.assertRaisesRegex(ValueError, "its Pure gives a value, not rows: a Pure box's rows are a relation"):
            box.pure('$input->count()')
        with self.assertRaises(ValueError):
            box.pure("$input->filter(x|$x.nope == 1)")
        with self.assertRaisesRegex(ValueError, "a Pure box reads another grid's rows"):
            source.pure('$input->limit(1)')

    def test_a_pure_box_imported_as_boxes_a_step_each(self):
        # FL12's Import Pure: grid.import_pure -- each step of its Pure a box of its own, this one its first, each after it
        # over the one before; the last box's rows the whole's
        page, flow, source, _big = self.flowed()
        box = flow.grid(input=source)
        # a box that read it, and its Output: each the last box's from then on (page-gestures.ts importedCommands)
        reader = flow.grid(input=box)
        box.output('imported.csv')
        boxes = box.import_pure("$input->filter(x|$x.region == 'EMEA')->extend(~fee:x|$x.notional->toOne() * 0.01)->sort(~notional->descending())")
        self.assertEqual(len(boxes), 3)
        self.assertIs(reader.input, boxes[-1], 'what read it reads the last box')
        self.assertIn('fee', reader.to_pandas().columns)
        outputs = {b['tile']: b.get('output') for s in page.to_dict()['sheets'] for b in s['layout'].get('boxes', [])}
        self.assertEqual((outputs[box.id], outputs[boxes[-1].id]), (None, {'format': 'csv', 'name': 'imported.csv'}), 'its Output with its rows')
        self.assertIs(boxes[0], box)
        rows = flow.grid(input=boxes[-1]).to_pandas()
        self.assertEqual(list(rows['notional']), [100.0, 75.5])
        self.assertEqual([round(f, 6) for f in rows['fee']], [1.0, 0.755])
        # each box reads the one before it, each a Pure box of one step
        cubes = {c['id']: c['cube'] for c in page.to_dict()['cubes']}
        self.assertEqual([cubes[b._view['cube']]['source']['input'] for b in boxes[1:]], [{'grid': boxes[0].id}, {'grid': boxes[1].id}])
        self.assertTrue(all(cubes[b._view['cube']]['source']['pure']['body'][0]['function'] in ('filter', 'extend', 'sort') for b in boxes))
        with self.assertRaisesRegex(ValueError, r'its Pure never reads \$input'):
            box.import_pure('1')

    def test_a_sample_hands_on_the_rest(self):
        # M3: a Sample a filter makes hands on the rest by its False (Alteryx's Create Samples, its estimation and holdout):
        # the rows it leaves out, by the same seed -- its rows and the rest every row, none in both
        page, flow, source, _big = self.flowed()
        every = sorted(source.to_pandas()['notional'])
        sampled = flow.grid(input=source)
        sampled.sample('random', 50, seed=2)
        rest = flow.grid(input=source)
        rest.relink(sampled, port='false')
        got, left = sorted(sampled.to_pandas()['notional']), sorted(rest.to_pandas()['notional'])
        self.assertEqual(sorted(got + left), every)
        self.assertEqual(list(rest.to_pandas().columns), list(source.to_pandas().columns), 'its row number and chance hidden')
        sampled.sample('every', 2, 'notional')
        self.assertEqual(len(sampled.to_pandas()) + len(rest.to_pandas()), len(every), 'set again: the rest follows')
        # the first by an order is a Top N, with no filter: no rest (refused as it is set, or as the rest is read)
        with self.assertRaisesRegex(ValueError, 'has no filter: its False port leaves no row out'):
            sampled.sample('first', 1, 'notional')
            rest.to_pandas()

    def test_a_union_of_more_than_two(self):
        # M2: a flow's Union of any number (page-commands.ts setJoin's more): each grid after the first two one input
        # more, each read live; one added as a line let go on it (relink to its next side), one taken off as a line
        # deleted (take_off: page-gestures.ts takeOffCommands); on a board, each kept by value
        page, flow, source, big = self.flowed()
        n_source, n_big = len(source.to_pandas()), len(big.to_pandas())
        union = page.union(big, source, big)
        self.assertEqual(union.sheet, flow)
        self.assertEqual(union.inputs, {'left': big, 'right': source, 'more1': big})
        self.assertEqual(len(union.to_pandas()), 2 * n_big + n_source)
        big.filter([('notional', 'greaterThan', 200)])
        fewer = len(big.to_pandas())
        self.assertLess(fewer, n_big)
        self.assertEqual(len(union.to_pandas()), 2 * fewer + n_source, 'an edit of an input reaches it')
        union.relink(source, side='more2')
        self.assertEqual(list(union.inputs), ['left', 'right', 'more1', 'more2'])
        union.take_off('left')
        self.assertEqual(union.inputs, {'left': source, 'right': big, 'more1': source})
        self.assertEqual(len(union.to_pandas()), 2 * n_source + fewer)
        union.take_off('more1')
        self.assertEqual(union.inputs, {'left': source, 'right': big})
        with self.assertRaisesRegex(ValueError, 'reads two inputs: one taken off is frozen'):
            union.take_off('left')
        with self.assertRaisesRegex(ValueError, 'has no input 5'):
            union.relink(big, side='more3')
        with self.assertRaisesRegex(ValueError, 'a union reads two grids or more, not 1'):
            page.union(big)
        # saved and loaded: its more kept
        union.relink(big, side='more1')
        again = ll.Page.load(page.to_dict(), frames={'trades': trades()})
        self.assertEqual({k: g.id for k, g in next(g for g in again.grids if g.id == union.id).inputs.items()},
                         {'left': source.id, 'right': big.id, 'more1': big.id})
        # on a board: each input kept by value, its rows those of all three
        board = [page.grid(trades(), name=f'board_{i}') for i in range(3)]
        copied = page.union(*board)
        self.assertEqual(copied.inputs, {})
        self.assertEqual(len(copied.to_pandas()), 3 * len(board[0].to_pandas()))

    def test_a_dynamic_input_keeps_the_rows_of_the_input_a_parameter_names(self):
        # M8's Dynamic Input: a Union kept to one input's rows, picked as it runs by a String parameter naming it
        # (dynamic-input.ts pickCommands; join.ts appended: each input's rows filtered by `$source == '<its name>'`, the
        # database keeping one); its value set, the other's rows; every input's again by pick(None)
        page, flow, source, big = self.flowed()
        n_source, n_big = len(source.to_pandas()), len(big.to_pandas())
        union = page.union(source, big)
        with self.assertRaisesRegex(ValueError, 'two of its inputs are named trades: a Dynamic Input picks one by its name'):
            union.pick(True)
        big.title = 'Big'
        union.pick(True)
        self.assertEqual(page.parameters, {'source': 'trades'}, 'a new parameter, its inputs\' names its choices, the first its value')
        self.assertEqual(next(p for p in page.to_dict()['parameters'])['choices'], ['trades', 'Big'])
        self.assertEqual(len(union.to_pandas()), n_source)
        page.set_parameter('source', 'Big')
        self.assertEqual(len(union.to_pandas()), n_big, 'its value set: the other input\'s rows')
        self.assertEqual(sorted(flow.grid(input=union).to_pandas()['notional']), sorted(big.to_pandas()['notional']), 'a box after it reads them')
        # saved and loaded: its pick kept
        again = ll.Page.load(page.to_dict(), frames={'trades': trades()})
        self.assertEqual(len(next(g for g in again.grids if g.id == union.id).to_pandas()), n_big)
        # picked by a parameter of the page's own; every input's again
        page.parameter('which', 'String', 'trades')
        union.pick('which')
        self.assertEqual(len(union.to_pandas()), n_source)
        union.pick(None)
        self.assertEqual(len(union.to_pandas()), n_source + n_big)
        with self.assertRaisesRegex(ValueError, "'nope' is no parameter of the page"):
            union.pick('nope')
        page.parameter('n', 'Integer', 1)
        with self.assertRaisesRegex(ValueError, 'n is no String, but Integer: a Dynamic Input picks an input by its name'):
            union.pick('n')
        with self.assertRaisesRegex(ValueError, 'is no Union'):
            big.pick(True)
        page.set_parameter('which', 'APAC')
        with self.assertRaisesRegex(Exception, 'it picks "APAC", none of its inputs: trades, Big'):
            union.pick('which')
        self.assertEqual(len(union.to_pandas()), n_source + n_big, 'refused: nothing changed')

    def test_a_join_box_hands_on_the_rows_each_side_pairs_with_none(self):
        # M2: a Join's L and R ports (live-inputs.ts gridInput) -- the rows of its Left no row of its Right pairs with,
        # and of its Right no row of its Left: anti-joins the database runs, each read live
        page, flow, source, big = self.flowed()
        on_notional = {'form': 'join', 'kind': 'inner', 'on': {'rows': [{'left': 'notional', 'op': '==', 'right': 'notional'}]},
                       'prefix': 'their_', 'keep': None}
        joined = page.join(big, source, on_notional)
        # a box added straight onto its R, as the canvas adds one on a port (headless rowsCube; the review of 2026-10-26's B5)
        straight = flow.grid(input=joined, port='right')
        self.assertEqual(sorted(straight.to_pandas()['notional']), [75.5])
        self.assertEqual(straight.input, joined)
        with self.assertRaisesRegex(ValueError, "its port 'false', 'left' or 'right', not \"middle\""):
            flow.grid(input=joined, port='middle')
        with self.assertRaisesRegex(ValueError, 'port= is one of another grid'):
            flow.grid(trades(), port='left')
        rest = flow.grid(input=joined)
        rest.relink(joined, port='right')
        self.assertEqual(sorted(rest.to_pandas()['notional']), [75.5], 'the Input\'s rows the Filter left out')
        self.assertEqual(list(rest.to_pandas().columns), list(source.to_pandas().columns), 'its Right\'s columns, as it hands them on')
        big.filter(None)
        self.assertEqual(len(rest.to_pandas()), 0, 'an edit upstream reaches it: every row of the Input matched now')
        rest.relink(joined, port='left')
        self.assertEqual(len(rest.to_pandas()), 0, 'each of the Filter\'s rows has its match')
        # a join set so that its ports would pair nothing is refused while a box reads one
        with self.assertRaisesRegex(ValueError, 'a Union keeps every row of both'):
            joined.rejoin({'form': 'append'})
        with self.assertRaisesRegex(ValueError, 'is no join: it has no Left and Right unmatched'):
            rest.relink(big, port='left')
        # saved and loaded: the port kept
        again = ll.Page.load(page.to_dict(), frames={'trades': trades()})
        self.assertEqual(len(next(g for g in again.grids if g.id == rest.id).to_pandas()), 0)

class Snapped(Paged):
    """Snap as page state (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8): grid.snap() copies the grid's base query by
    the engine every tab's Snap copies through, and the page's snap command says the grid reads the copy -- every call on
    it reads the copy whatever its frame does meanwhile; grid.live() reads it live again; a copy no grid reads is let
    go; a page saved snapped loads live and says so."""

    get = Live.get
    said = Live.said
    wrote = Live.wrote
    settled = Live.settled

    def moving(self):
        """A live frame whose rows are moved between queries: a function, read at each query."""
        state = {'df': trades()}
        return state, (lambda: state['df'])

    def held(self, table):
        engine = datacube._session.engine()
        return engine.copy_of(table) is not None or datacube._session.frames.file_table(table) is not None

    def test_a_snapped_grid_whose_copy_is_no_longer_held_is_refused_in_datacubes_words(self):
        # one rule (headless gridBaseOn: docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z15): a snapped grid reads its copy as
        # the engine holds it, never its frame in its place -- one the session no longer holds is refused, in its words
        import re
        page = ll.Page('Snaps')
        grid = page.grid(trades(), name='trades', rows=['region'], measures={'notional': 'sum'})
        grid.snap()
        shown = len(grid.view().rows)
        gone = re.escape(f"its snap ({grid.snapped['label']}) is no longer held -- go live to read its rows live again, "
                         'or snap it anew')
        with mock.patch.object(datacube._session, 'held_copies', lambda: {}):
            with self.assertRaisesRegex(ValueError, gone):
                grid.view()
            # its live base is its frame's, whatever its copy: what a snap pulls, what a class is made of
            self.assertIn('trades', grid.relation)
        self.assertEqual(len(grid.view().rows), shown, 'held again: its copy read')
        grid.live()
        with mock.patch.object(datacube._session, 'held_copies', lambda: {}):
            self.assertEqual(len(grid.view().rows), shown, 'live: its frame read, no copy asked for')

    def test_a_snapped_grid_reads_its_copy_whatever_its_frame_does_and_live_again_reads_the_frame(self):
        state, frame = self.moving()
        page = ll.Page('Snaps')
        grid = page.grid(frame, name='trades', rows=['region'], measures={'notional': 'sum'})
        self.assertIsNone(grid.snapped)
        self.assertIs(grid.snap(), grid)
        copy = grid.snapped
        self.assertEqual((copy['table'], copy['rowCount'], 'pulledBy' in copy), ('snap', 3, False))
        self.assertRegex(copy['label'], r'^Snap \d\d:\d\d$')
        datetime.datetime.fromisoformat(copy['takenAt'].replace('Z', '+00:00'))
        # the page's own command, as DataCube's Snap writes it, and the document's copies by cube
        self.assertEqual(page._page_commands[-1], {'kind': 'snap', 'grid': grid.id, 'copy': copy})
        self.assertEqual(page.to_dict()['copies'], {grid._view['cube']: copy})
        # the frame moves: the snapped grid reads its copy, another grid over the same frame reads it live
        state['df'] = trades().iloc[:1]
        other = page.grid(frame, name='trades')
        # its rows at their finest level: by region, two (EMEA, AMER); the frame now holds one trade, EMEA's
        self.assertEqual((len(grid.to_pandas()), len(other.to_pandas())), (2, 1))
        self.assertEqual(sorted(r[0] for r in grid.view().rows if r[0] is not None), ['AMER', 'EMEA'])
        self.assertEqual(grid.export('csv').count('AMER'), 1, 'its export reads the copy too')
        # what it is a copy of is its live base: its relation reads the frame, never the copy
        self.assertIn('trades', grid.relation)
        self.assertNotIn('snap', grid.relation)
        # live again: its copy let go, its rows the frame's
        self.assertIs(grid.live(), grid)
        self.assertIsNone(grid.snapped)
        self.assertNotIn('copies', page.to_dict())
        self.assertEqual(page._page_commands[-1], {'kind': 'live', 'grid': grid.id})
        self.assertFalse(self.held('snap'), 'the copy is let go of')
        self.assertEqual(len(grid.to_pandas()), 1)
        self.assertEqual(sorted(r[0] for r in grid.view().rows if r[0] is not None), ['EMEA'])
        with self.assertRaisesRegex(ValueError, 'reads its live rows already'):
            grid.live()

    def test_a_cached_box_is_copied_and_the_box_after_it_reads_the_copy_whatever_its_frame_does(self):
        # (FL10) grid.cache(): the box marked cached on its flow (setCache) and its rows copied (its snap) -- the box after
        # it reads the copy (a link resolved to be opened reads its grid's copy: live-inputs.ts resolvedSource), so the
        # frame moving reaches neither; cache(False): back live, and so the box after it
        state, frame = self.moving()
        page = ll.Page('Cache')
        flow = page.flow()
        box = flow.grid(frame, name='trades')
        after = flow.grid(input=box)
        self.assertFalse(box.cached)
        self.assertIs(box.cache(), box)
        self.assertTrue(box.cached)
        self.assertIsNotNone(box.snapped)
        self.assertEqual(page._page_commands[-2], {'kind': 'setCache', 'grid': box.id, 'cache': True})
        state['df'] = trades().iloc[:1]
        self.assertEqual(len(after.to_pandas()), len(trades()), 'the box after it reads its copy, not the frame as it is now')
        box.cache(False)
        self.assertFalse(box.cached)
        self.assertIsNone(box.snapped)
        self.assertEqual(len(after.to_pandas()), 1, 'live again: the frame as it is now')
        with self.assertRaisesRegex(ValueError, 'is no box of a flow: a cache is a flow'):
            page.grid(frame, name='board').cache()

    def test_a_copy_belongs_to_its_grid_snapped_again_or_removed_it_is_let_go(self):
        page = ll.Page('Snaps')
        grid = page.grid(trades(), name='trades')
        keep = page.grid(desks(), name='desks')
        grid.snap()
        grid.snap()
        self.assertEqual(grid.snapped['table'], 'snap_2')
        self.assertFalse(self.held('snap'), 'the copy it read before is let go of')
        page.remove(grid)
        self.assertFalse(self.held('snap_2'), "a grid removed takes its copy with it")
        self.assertNotIn('copies', page.to_dict())
        self.assertEqual(page.grids, [keep])

    def test_a_snapped_grid_is_checked_against_its_copy_not_its_frame_as_it_is_now(self):
        state, frame = self.moving()
        page = ll.Page('Snaps')
        grid = page.grid(frame, name='trades')
        grid.snap()
        # its frame loses a column: the snapped grid still has it, in an edit and in a chart
        state['df'] = trades().drop(columns=['pnl'])
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            grid.calculate('half', '$x.pnl->toOne() / 2')
            grid.chart('bar', x='desk', y=[('pnl', 'sum')])
        self.assertIn('pnl', list(grid.to_pandas().columns))
        # live again: opened over the frame as it is now, what it left out said
        with self.assertWarnsRegex(UserWarning, 'pnl'):
            grid.live()

    def test_a_class_of_a_snapped_grid_is_of_its_live_base(self):
        # a class maps the tables its rows are read from, never a copy, which lasts only as long as this session (§18.8):
        # what DataCube's Create a Class… sends the writer (the host's fetch, watched here alone), into a model home of
        # the test's own
        page = ll.Page('Snaps')
        grid = page.grid(trades(), name='trades')
        grid.snap()
        home = tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))
        sent = []
        answer = _headless.answer_fetch

        def watched(method, url, body):
            if url.endswith('/model/createClass'):
                sent.append(json.loads(body))
            return answer(method, url, body)
        with mock.patch.dict(os.environ, {'LEGEND_LITE_HOME': home}), mock.patch.object(_headless, 'answer_fetch', watched):
            grid.create_class('Trade')
        self.assertNotIn('snap', json.dumps(sent[-1]['class']['lambda']))
        self.assertEqual(list(sent[-1]['class']['where']['samples']), ['trades'])
        self.assertEqual(page.model['tables'], {grid._view['cube']: ['trades']})

    def test_snap_true_reads_a_frame_once_and_a_snap_refused_leaves_the_page_as_it_was(self):
        state, frame = self.moving()
        page = ll.Page('Snaps')
        grid = page.grid(frame, name='trades', snap=True)
        self.assertEqual(grid.snapped['rowCount'], 3)
        state['df'] = trades().iloc[:1]
        self.assertEqual(len(grid.to_pandas()), 3)
        # the frame stays served as it is: another grid over it reads it live
        self.assertEqual(len(page.grid(frame, name='trades').to_pandas()), 1)
        before = page.to_dict()
        engine = datacube._session.engine()
        real = engine.snap_copy
        engine.snap_copy = lambda *a: (_ for _ in ()).throw(ValueError('5,000,001 rows exceeds the snap limit'))
        try:
            with self.assertRaisesRegex(ValueError, 'exceeds the snap limit'):
                page.grid(desks(), name='desks', snap=True)
        finally:
            engine.snap_copy = real
        self.assertEqual(page.to_dict(), before)
        self.assertNotIn('desks', datacube._session.frames, 'a frame served for the grid refused is served no more')

    def test_a_copy_made_for_a_snap_the_page_refuses_is_let_go(self):
        page = ll.Page('Snaps')
        grid = page.grid(trades(), name='trades')
        real = page._apply_page

        def refused(commands, what=None):
            if commands[0]['kind'] == 'snap':
                raise ValueError('a snap: refused')
            return real(commands, what)

        page._apply_page = refused
        with self.assertRaisesRegex(ValueError, 'a snap: refused'):
            grid.snap()
        self.assertIsNone(grid.snapped)
        self.assertFalse(self.held('snap'), 'the copy belongs to no grid: let go')

    def test_a_page_saved_snapped_loads_live_and_says_so(self):
        page = ll.Page('Snaps')
        grid = page.grid(trades(), name='trades')
        grid.snap()
        label = grid.snapped['label']
        saved = page.to_dict()
        self.assertIn('copies', saved)
        with warnings.catch_warnings(record=True) as said:
            warnings.simplefilter('always')
            again = ll.Page.load(saved, frames={'trades': trades().iloc[:2]})
        self.assertEqual([str(w.message) for w in said],
                         [f'trades was snapped ({label}) when the page was saved: it opens live, as a snap lasts only as '
                          'long as the session that made it'])
        self.assertNotIn('copies', again.to_dict())
        self.assertIsNone(again.grids[0].snapped)
        self.assertEqual(len(again.grids[0].to_pandas()), 2)

    def test_a_join_snapped_reads_its_copy(self):
        state, frame = self.moving()
        page = ll.Page('Snaps')
        mine = page.grid(frame, name='trades')
        theirs = page.grid(desks(), name='desks')
        joined = page.join(mine, theirs, Joined.HOW, snap=True)
        self.assertEqual(joined.snapped['rowCount'], 3)
        state['df'] = trades().iloc[:1]
        self.assertEqual((len(joined.to_pandas()), len(mine.to_pandas())), (3, 1))

    def test_a_copy_a_kept_page_reads_is_kept_and_goes_with_the_page(self):
        # a tab's release (its grid closed, gone live, the tab gone) keeps a copy a page the keeper keeps reads
        engine = datacube._session.engine()
        page = ll.Page('Snaps')
        grid = page.grid(trades(), name='trades')
        grid.snap()
        said = engine.answer('POST', '/snap.json', '', json.dumps({'op': 'release', 'table': 'snap'}))
        self.assertEqual((said.status, json.loads(said.body)),
                         (200, {'kept': 'a page reads it: it goes when nothing reads it any more'}))
        self.assertTrue(self.held('snap'))
        # the page collected (a cell made it again): its copies let go at the session's next safe moment
        del page, grid
        gc.collect()
        self.assertTrue(self.held('snap'), 'never in the collector')
        ll.Page('Another').grid(desks(), name='desks')
        self.assertFalse(self.held('snap'), 'let go at the next page change')

    def test_a_tabs_snap_written_to_the_page_is_the_pages_from_then_on(self):
        page = ll.Page('Snaps')
        grid = page.grid(trades(), name='trades')
        tab = ll.show(page, browser=False)
        try:
            engine = datacube._session.engine()
            # the tab's Snap: a copy it asked Python for, and the page it writes with it (as its grid's copy)
            engine_h, frames = datacube._session.headless(), datacube._session.frames
            with frames.reading():
                spec = grid._spec(engine_h, frames, live=True)
                pull = engine_h.call('gridSnapPull', spec)
            made = json.loads(engine.answer('POST', '/snap.json', '', json.dumps(
                {'op': 'snap', 'model': spec['model'], 'runtime': spec['runtime'], 'pull': pull})).body)
            snapped = page_edit(kind='snap', grid=grid.id, copy={'table': made['table'], 'label': 'Snap 14:02',
                                                                   'takenAt': '2026-10-20T14:02:00.000Z', 'rowCount': made['rowCount']})
            self.assertEqual(self.said(page, [snapped]), 200)
            self.assertEqual(grid.snapped['table'], made['table'])
            self.assertEqual(len(grid.to_pandas()), 3, 'its copy, read in Python')
            # the page's now: the tab's release keeps it; going live lets it go
            released = engine.answer('POST', '/snap.json', '', json.dumps({'op': 'release', 'table': made['table']}))
            self.assertIn(b'kept', released.body)
            grid.live()
            self.assertFalse(self.held(made['table']))
        finally:
            tab.close()

    def test_shown_a_snap_and_going_live_are_written_as_the_pages_commands(self):
        page = ll.Page('Snaps')
        grid = page.grid(trades(), name='trades')
        tab = ll.show(page, browser=False)
        try:
            since = self.writes(page)
            at = self.get(f'/version.json?page={page._key}')[1]['version']
            grid.snap()
            moved = since(at)
            self.assertEqual(moved['commands'], [{'page': {'kind': 'snap', 'grid': grid.id, 'copy': grid.snapped}}])
            self.assertEqual(self.get(f'/page.json?page={page._key}')[1]['page']['copies'],
                             {grid._view['cube']: grid.snapped})
            grid.live()
            self.assertEqual(since(moved['version'])['commands'], [{'page': {'kind': 'live', 'grid': grid.id}}])
            self.assertFalse(self.held('snap'))
        finally:
            tab.close()


class OverFiles(Paged):
    """A page over a file (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md X1-X4, D14): ``page.grid(file=path)`` reads it in as
    the tab reads a file it is given -- once for its bytes, named by their hash, as the page's cube names it -- and the
    page keeper keeps its table while a page reads it, as it keeps a copy (X3); a page saved over a file is loaded given
    its file by name, one that is not the file it was saved over opened all the same and said, as the tab says it. A
    remote file (``page.grid(remote=url)``) is mounted as the tab mounts one, here from a server on this machine."""

    @classmethod
    def setUpClass(cls):
        import functools
        import http.server
        import threading
        cls.served = tempfile.TemporaryDirectory(dir=os.environ.get('TEST_TMPDIR'))
        Path(cls.served.name, 'desks.csv').write_text('desk,qty\nFX,1\nEQ,2\nFX,3\n')

        class Quiet(http.server.SimpleHTTPRequestHandler):
            def log_message(self, *_):
                pass
        cls.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(Quiet, directory=cls.served.name))
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.base = f'http://127.0.0.1:{cls.server.server_address[1]}'

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.served.cleanup()

    def file(self, text, name='trades.csv'):
        folder = Path(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR')))
        path = folder / name
        path.write_text(text)
        return path

    def held(self, sha256):
        return sha256 in datacube._session.frames.read_files()

    def test_a_file_grid_read_once_kept_while_a_page_reads_it_and_let_go_after(self):
        import hashlib
        path = self.file('desk,qty\nFX,1\nEQ,2\nFX,4\n')
        sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
        page = ll.Page('Files')
        grid = page.grid(file=path, rows=['desk'], measures={'qty': 'sum'})
        source = page.to_dict()['cubes'][0]['cube']['source']
        self.assertEqual((source['_type'], source['name'], source['sha256'], source['size']),
                         ('file', 'trades.csv', sha256, path.stat().st_size))
        self.assertEqual(groups_of(grid.view(), 'qty'), {'FX': 5, 'EQ': 2})
        self.assertEqual(page._reads()['files'], [sha256])
        # a second grid over the same bytes reads the same table
        page.grid(file=path)
        self.assertEqual(list(datacube._session.frames.read_files()), [sha256])
        # the tab letting go of it keeps it while a page the keeper keeps reads it (X3, as a copy is kept)
        engine = datacube._session.engine()
        table = datacube._session.frames.read_files()[sha256]
        kept = engine.answer('POST', '/forget.json', '', json.dumps({'name': 'trades.csv', 'table': table}))
        self.assertEqual(json.loads(kept.body), {'kept': 'a page reads it: it goes when nothing reads it any more'})
        self.assertTrue(self.held(sha256))
        # the page collected: let go at the session's next safe moment
        del page, grid
        gc.collect()
        ll.Page('Another').grid(desks(), name='desks_again')
        self.assertFalse(self.held(sha256), 'let go once nothing reads it')

    def test_a_page_saved_over_a_file_loads_given_its_file_and_is_refused_without_it(self):
        path = self.file('desk,qty\nFX,1\nEQ,2\nFX,4\n')
        page = ll.Page('Files')
        page.grid(file=path, rows=['desk'], measures={'qty': 'sum'})
        saved = os.path.join(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR')), 'files.page.json')
        page.save(saved)
        sha256 = page.to_dict()['cubes'][0]['cube']['source']['sha256']
        del page
        gc.collect()
        ll.Page('Another').grid(desks(), name='desks_again')
        self.assertFalse(self.held(sha256))
        with self.assertRaisesRegex(ValueError, r"the page reads the files trades\.csv: give them, files=\{name: path\}"):
            ll.Page.load(saved)
        again = ll.Page.load(saved, files={'trades.csv': path})
        self.assertEqual(groups_of(again.grids[0].view(), 'qty'), {'FX': 5, 'EQ': 2})
        self.assertTrue(self.held(sha256))
        # a file of other bytes under its name: opened over it all the same, said in the tab's words, and the page's cube
        # names the file it reads now
        changed = self.file('desk,qty\nFX,10\nEQ,20\n')
        with self.assertWarnsRegex(UserWarning, r'trades\.csv is not the file this cube was saved over \(its contents differ\)'):
            other = ll.Page.load(saved, files={'trades.csv': changed})
        self.assertEqual(groups_of(other.grids[0].view(), 'qty'), {'FX': 10, 'EQ': 20})
        self.assertNotEqual(other.to_dict()['cubes'][0]['cube']['source']['sha256'], sha256)

    def test_a_page_whose_class_is_not_read_here_opens_without_it_as_the_tab_opens_it(self):
        # (the S7 audit's H-6) a class this host's model home cannot read: left out and said, as the tab's Open... leaves it
        # out -- kept as the page saved it, its other grids opened -- and refused in words when its grid is asked (Python
        # had refused the whole page)
        page = ll.Page('Classless')
        page.grid(trades(), name='classless_trades')
        page.grid(trades(), name='classless_trades_too')
        doc = page.to_dict()
        doc['cubes'][1]['cube']['source'] = {
            '_type': 'class', 'name': 'Trade', 'class': 'trades::Trade', 'project': 'g:trades', 'workspace': 'datacube',
            'revision': 'r1', 'mapping': 'trades::Mapping', 'runtime': 'trades::Runtime',
            'columns': doc['cubes'][1]['cube']['source']['columns']}
        with self.assertWarnsRegex(UserWarning, r"left out: Trade \(Trade is a class of g:trades, which this host's model home "
                                                r'cannot read'):
            loaded = ll.Page.load(doc, frames={'classless_trades': trades()})
        self.assertEqual(len(loaded.grids), 2)
        self.assertEqual(loaded.to_dict()['cubes'][1]['cube']['source']['class'], 'trades::Trade', 'kept as the page saved it')
        self.assertEqual(len(loaded.grids[0].to_pandas()), len(trades()))
        with self.assertRaisesRegex(Exception, "Trade is a class of g:trades, which this host's model home cannot read"):
            loaded.grids[1].view()

    def test_a_remote_file_grid_is_mounted_once_for_its_url_and_one_not_read_is_refused_in_words(self):
        url = f'{self.base}/desks.csv'
        page = ll.Page('Remote')
        grid = page.grid(remote=url, rows=['desk'], measures={'qty': 'sum'})
        source = page.to_dict()['cubes'][0]['cube']['source']
        self.assertEqual((source['_type'], source['name'], source['url']), ('remoteFile', 'desks.csv', url))
        self.assertEqual(groups_of(grid.view(), 'qty'), {'FX': 4, 'EQ': 2})
        self.assertEqual(page._reads()['remotes'], [url])
        # a second grid over the same URL reads the view the first mounted
        remotes = datacube._session.frames.hosted()['remotes']
        page.grid(remote=url)
        self.assertEqual(datacube._session.frames.hosted()['remotes'], remotes)
        with self.assertRaisesRegex(ValueError, 'it takes no name'):
            page.grid(remote=url, name='desks')
        with self.assertRaisesRegex(ValueError, 'one of them'):
            page.grid(remote=url, file=Path(self.served.name, 'desks.csv'))
        # a remote file that is not there: refused in words, nothing mounted for it, the page as it was
        gone = f'{self.base}/gone.csv'
        with self.assertRaisesRegex(ValueError, f'the remote file {gone} is not read: '):
            page.grid(remote=gone)
        self.assertNotIn(gone, datacube._session.frames.hosted()['remotes'])
        self.assertEqual(len(page.grids), 2)

    def test_a_page_refused_as_it_loads_lets_go_of_the_files_it_read_for_it(self):
        path = self.file('desk,qty\nFX,1\nEQ,2\nFX,4\n')
        page = ll.Page('Files')
        page.grid(file=path)
        page.grid(remote=f'{self.base}/desks.csv')
        # its file's bytes changed since: read anew for the load, then the page refused over a remote file that is not
        # there any more -- the file read for it let go of at once
        changed = self.file('desk,qty\nFX,10\n')
        refused = page.to_dict()
        gone = f'{self.base}/gone.csv'
        refused['cubes'][1]['cube']['source'] = {**refused['cubes'][1]['cube']['source'], 'url': gone}
        # the page that read it gone, and its file let go of at the session's next safe moment
        del page
        gc.collect()
        ll.Page('Another').grid(desks(), name='desks_again')
        import hashlib
        held = set(datacube._session.frames.read_files())
        self.assertNotIn(refused['cubes'][0]['cube']['source']['sha256'], held)
        with self.assertRaisesRegex(Exception, f'the remote file {gone} is not read'):
            ll.Page.load(refused, files={'trades.csv': changed})
        self.assertNotIn(hashlib.sha256(changed.read_bytes()).hexdigest(), datacube._session.frames.read_files(),
                         'the file read for it let go of')
        self.assertEqual(set(datacube._session.frames.read_files()), held)

    def test_a_file_grid_joins_a_frames_and_a_source_the_host_does_not_read_is_refused_in_datacubes_words(self):
        path = self.file('desk,qty\nFX,1\nEQ,2\nFX,4\n')
        page = ll.Page('Files')
        mine = page.grid(file=path)
        theirs = page.grid(pd.DataFrame({'desk': ['FX', 'EQ'], 'head': ['Ana', 'Ben']}), name='heads')
        joined = page.join(mine, theirs, {'form': 'join', 'kind': 'left', 'prefix': 'h_', 'keep': None,
                                          'on': {'rows': [{'left': 'desk', 'op': '==', 'right': 'desk'}]}})
        self.assertEqual(len(joined.to_pandas()), 3)
        # Python keeps no list of the sources a page may read (Z14): DataCube says which it cannot read here -- a class
        # its model home does not hold
        doc = page.to_dict()
        doc['cubes'][0]['cube']['source'] = {
            '_type': 'class', 'name': 'Trade', 'class': 'trades::Trade', 'project': 'g:trades', 'workspace': 'datacube',
            'revision': 'r1', 'mapping': 'trades::Mapping', 'runtime': 'trades::Runtime',
            'columns': doc['cubes'][0]['cube']['source']['columns']}
        # left out as the page opens, as the tab's Open... leaves it out (the S7 audit's H-6), and refused in its words
        # when its grid is asked
        with self.assertWarnsRegex(UserWarning, "left out: Trade \\(Trade is a class of g:trades, which this host's model "
                                                'home cannot read at revision r1'):
            loaded = ll.Page.load(doc, frames={'heads': pd.DataFrame({'desk': ['FX', 'EQ'], 'head': ['Ana', 'Ben']})},
                                  files={'trades.csv': path})
        with self.assertRaisesRegex(Exception, "Trade is a class of g:trades, which this host's model home cannot read "
                                               'at revision r1'):
            loaded.grids[0].view()


class OverClasses(Paged):
    """A page over a class (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md X1, X2, X5): a grid over a class Create a Class
    made, as the tab adds one (class-source.ts classAt) -- opened through the class's project at its revision in this
    user's model home (class-source.ts classOpened, the tab's own rule), planned through its mapping by Python's compiler
    and run where its rows are by the engine's own plan-and-run (headless calls.ts ``engine``) -- loaded, read, edited
    and saved in Python."""

    def setUp(self):
        super().setUp()
        home = tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))
        patched = mock.patch.dict(os.environ, {'LEGEND_LITE_HOME': home})
        patched.start()
        self.addCleanup(patched.stop)

    def classed(self):
        """A page whose grid is over the class its frame's grid made, as the tab's Create ▸ Grid over a class writes it."""
        page = ll.Page('Classes')
        path = page.grid(trades(), name='trades').create_class('Trade')
        model = page.model
        doc = page.to_dict()
        cube = doc['cubes'][0]['cube']
        cube['source'] = {'_type': 'class', 'name': 'Trade', 'class': path, 'project': model['project'],
                          'workspace': model['workspace'], 'revision': model['revision'],
                          'mapping': f"{model['package']}::mapping::Model",
                          'runtime': f"{model['package']}::runtime::Python", 'columns': cube['source']['columns']}
        return page, doc

    def test_a_grid_is_added_over_a_class_as_the_tabs_add_a_grid_adds_one(self):
        # (X4) page.grid(cls=...): a class of the page's model at its pin (class-source.ts classAt, the tab's rule), read
        # through the writer's mapping and Python's runtime where this host reads it (headless sourceCube)
        page = ll.Page('Classes')
        path = page.grid(trades(), name='trades').create_class('Trade')
        model = page.model
        grid = page.grid(cls='Trade')
        source = page.to_dict()['cubes'][-1]['cube']['source']
        self.assertEqual((source['_type'], source['name'], source['class'], source['project'], source['revision']),
                         ('class', 'Trade', path, model['project'], model['revision']))
        self.assertEqual((source['mapping'], source['runtime']),
                         (f"{model['package']}::mapping::Model", f"{model['package']}::runtime::Python"))
        self.assertEqual(sorted(c['name'] for c in source['columns']), ['desk', 'notional', 'pnl', 'region'])
        self.assertEqual(sorted(grid.to_pandas()['notional']), [75.5, 100.0, 250.0])
        # by its path, at the revision named
        self.assertEqual(page.grid(cls=path, revision=model['revision']).to_pandas().shape[0], 3)
        # refused in DataCube's words, the page as it was
        before = page.to_dict()
        with self.assertRaisesRegex(Exception, 'the project has no class'):
            page.grid(cls='Nope')
        with self.assertRaisesRegex(ValueError, "a class of other:project is named by its path as DataCube's writer makes one"):
            page.grid(cls='Trade', project='other:project', revision='r1')
        with self.assertRaisesRegex(ValueError, 'a class of other:project is read at a revision of it'):
            page.grid(cls=path, project='other:project')
        with self.assertRaisesRegex(ValueError, "project= and revision= say where a class is read: they go with cls="):
            page.grid(trades(), name='trades', revision='r1')
        self.assertEqual(page.to_dict(), before)
        with self.assertRaisesRegex(ValueError, "the page has no model of its own"):
            ll.Page('Bare').grid(cls='Trade')

    def test_a_tab_on_the_page_asks_python_for_a_grid_over_a_class_as_page_grid_makes_one(self):
        # (K16, K17) the tab's Create ▸ Grid over a class and its Classes, on a page Python serves, which has no compiler or
        # model home of its own here: the class's cube made by DataCube's own code where Python reads it (source.json:
        # headless sourceCube, page.grid's own), then opened as every grid of the page is (opened.json)
        page = ll.Page('Classes')
        path = page.grid(trades(), name='trades').create_class('Trade')
        model = page.model
        engine = datacube._session.engine()

        def asked(given, model=model):
            answer = engine.answer('POST', '/source.json', '', _json.dumps(
                {'model': None if model is None else _json.dumps(model), 'given': _json.dumps(given), 'page': page.name,
                 'gridsOnPage': 2}))
            return answer.status, answer.body.decode('utf-8')

        status, body = asked({'class': 'Trade'})
        self.assertEqual(status, 200, body)
        cube = json.loads(json.loads(body)['cube'])
        grid = page.grid(cls='Trade')
        self.assertEqual(cube, page.to_dict()['cubes'][-1]['cube'], 'the cube page.grid makes, as the tab asks it')
        self.assertEqual(cube['source']['class'], path)
        opened = engine.answer('POST', '/opened.json', '', json.dumps(
            {'cube': json.dumps(cube), 'page': page.name, 'gridsOnPage': 2}))
        self.assertEqual(opened.status, 200, opened.body)
        self.assertEqual(json.loads(opened.body)['how']['mapping'], f"{model['package']}::mapping::Model")
        # a class of another project, by its path at a revision: the same rule's (class-source.ts classGiven)
        status, body = asked({'class': path, 'project': model['project'], 'revision': model['revision']})
        self.assertEqual(status, 200, body)
        # refused in DataCube's words, and a malformed ask in the route's; asking adds nothing to the page
        self.assertEqual(asked({'class': 'Trade', 'project': 'other:project', 'revision': 'r1'})[0], 422)
        self.assertIn("is named by its path as DataCube's writer makes one", asked({'class': 'Trade', 'project': 'other:project', 'revision': 'r1'})[1])
        self.assertIn('the page has no model of its own', asked({'class': 'Trade'}, model=None)[1])
        self.assertEqual(engine.answer('POST', '/source.json', '', '{"given": 1}').status, 400)
        self.assertEqual(len(page.grids), 2)
        self.assertEqual(sorted(grid.to_pandas()['notional']), [75.5, 100.0, 250.0])

    def test_a_page_over_a_class_is_loaded_read_edited_and_saved(self):
        _, doc = self.classed()
        loaded = ll.Page.load(doc)
        grid = loaded.grids[0]
        self.assertEqual(loaded.to_dict()['cubes'][0]['cube']['source']['class'], doc['cubes'][0]['cube']['source']['class'])
        # its rows, planned through the class's mapping and run where they are
        self.assertEqual(sorted(grid.to_pandas()['notional']), [75.5, 100.0, 250.0])
        # edited as any grid is: by DataCube's commands, judged over the class's columns as the compiler types them now
        grid.group('region')
        self.assertEqual(groups_of(grid.view(), 'notional'), {'EMEA': 175.5, 'AMER': 250.0})
        with self.assertRaisesRegex(ValueError, 'nope'):
            grid.group('nope')
        # and saved as it is: still over the class
        saved = loaded.to_dict()
        self.assertEqual(saved['cubes'][0]['cube']['source']['_type'], 'class')
        self.assertEqual(saved['cubes'][0]['cube']['query']['rows'], ['region'])

    def test_the_pages_filters_reach_its_rows_as_they_reach_the_screens_grid(self):
        # (B1) a page filter on the class: every grid over the class reads its rows through it -- what it shows, its rows,
        # its export -- as the screen's page gives its grid (page-filters.ts gridLens)
        _, doc = self.classed()
        path = doc['cubes'][0]['cube']['source']['class']
        doc['filters'] = [{'id': 'filter-1', 'class': path,
                           'condition': {'kind': 'condition', 'column': 'region', 'operator': 'equal', 'value': 'EMEA'}}]
        loaded = ll.Page.load(doc)
        grid = loaded.grids[0]
        self.assertEqual(sorted(grid.to_pandas()['notional']), [75.5, 100.0])
        grid.group('region')
        self.assertEqual(groups_of(grid.view(), 'notional'), {'EMEA': 175.5})
        self.assertNotIn('AMER', grid.export('csv'))
        self.assertIn('EMEA', grid.export('csv'))

    def test_a_charts_mark_on_a_grid_over_the_class_is_a_page_filter_from_the_chart(self):
        # (K7, D8; page-filters.ts classPick, the screen's click's rule) the mark's conditions on the class's properties a
        # page filter from the chart, replacing its last -- its own grid's filter untouched; named, never toggled
        _, doc = self.classed()
        path = doc['cubes'][0]['cube']['source']['class']
        loaded = ll.Page.load(doc)
        grid = loaded.grids[0]
        grid.group('region')
        chart = grid.chart('bar')
        mark = lambda value: {'kind': 'condition', 'column': 'region', 'operator': 'equal', 'value': value}
        chart.select('EMEA')
        self.assertEqual(loaded.filters, [{'id': 'filter-1', 'class': path, 'condition': mark('EMEA'), 'from': chart.id}])
        self.assertEqual(chart.selection, [], 'the mark is the page\'s filter, not its grid\'s')
        self.assertNotIn('filter', grid._query)
        chart.select('EMEA')
        self.assertEqual(len(loaded.filters), 1, 'the same mark again stays')
        chart.select('AMER')
        self.assertEqual(loaded.filters, [{'id': 'filter-1', 'class': path, 'condition': mark('AMER'), 'from': chart.id}])
        chart.select(None)
        self.assertEqual(loaded.filters, [])
        # Show Grid on a frozen chart whose mark is a page filter: the filter off with its selection, as on the screen
        # (followGrid's own rule: the S7 audit's Y-1 -- the screen's Show Grid had left it on)
        frozen = grid.chart('bar', x='region', frozen=True)
        frozen.select('EMEA')
        self.assertEqual([f['from'] for f in loaded.filters], [frozen.id])
        frozen.grid()
        self.assertEqual(loaded.filters, [])

    def test_a_page_filter_is_set_read_and_taken_off_as_the_screens_add_a_filter_sets_one(self):
        # (K7, Z19) page.filter: made by DataCube's code headless (pageFilter), the setPageFilter command the screen's Add
        # a Filter makes -- every grid over the class reading through it; page.filters; page.remove_filter
        _, doc = self.classed()
        loaded = ll.Page.load(doc)
        grid = loaded.grids[0]
        path = doc['cubes'][0]['cube']['source']['class']
        id = loaded.filter('Trade', ('region', 'equal', 'EMEA'))
        self.assertEqual([(f['id'], f['class'], f['condition']) for f in loaded.filters],
                         [(id, path, {'kind': 'condition', 'column': 'region', 'operator': 'equal', 'value': 'EMEA'})])
        self.assertEqual(sorted(grid.to_pandas()['notional']), [75.5, 100.0])
        # by its id, replaced
        self.assertEqual(loaded.filter(path, ('notional', 'greaterThan', 90), id=id), id)
        self.assertEqual(sorted(grid.to_pandas()['notional']), [100.0, 250.0])
        # refused in DataCube's words, nothing set
        with self.assertRaisesRegex(ValueError, 'no grid of the page reads a class "Customer"'):
            loaded.filter('Customer', ('region', 'equal', 'EMEA'))
        with self.assertRaisesRegex(ValueError, 'colour'):
            loaded.filter('Trade', ('colour', 'equal', 'red'))
        self.assertEqual(len(loaded.filters), 1)
        loaded.remove_filter(id)
        self.assertEqual(loaded.filters, [])
        self.assertEqual(sorted(grid.to_pandas()['notional']), [75.5, 100.0, 250.0])

    def test_a_class_whose_table_is_not_served_is_refused_never_read_from_its_sample_rows(self):
        # F28: a class's rows are the tables this host serves -- the sample rows Create a Class wrote into its project are
        # never read in their place, as the tab never reads them (its class rows run on its own database)
        _, doc = self.classed()
        loaded = ll.Page.load(doc)
        grid = loaded.grids[0]
        datacube._session.frames.unregister('trades')
        try:
            for read in (grid.to_pandas, grid.view):
                with self.subTest(read=read.__name__), self.assertRaisesRegex(
                        Exception, 'this engine reads only the tables it serves.*it reads trades, which it does not serve'):
                    read()
        finally:
            datacube._session.register(trades(), 'trades')
        self.assertEqual(sorted(grid.to_pandas()['notional']), [75.5, 100.0, 250.0], 'served again: read again')

    def test_a_class_its_model_home_does_not_hold_is_refused_in_words(self):
        _, doc = self.classed()
        doc['cubes'][0]['cube']['source']['revision'] = 'no-such-revision'
        # left out as the page opens, said in DataCube's words (the S7 audit's H-6) -- and, its only grid, the page not
        # opened, as the tab's Open... refuses a page none of whose sources opened (the S7 confirmation audit's Y2-2)
        with self.assertWarnsRegex(UserWarning, 'left out: Trade .*cannot read at revision no-such-revision'), \
                self.assertRaisesRegex(ValueError, '^not opened: "Classes" -- none of its sources was opened$'):
            ll.Page.load(doc)


class OverWarehouse(Paged):
    """A page over a warehouse table (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md X1, X2, D13): opened by the engine under
    the session it holds for the table's warehouse, signed in by a tab -- Python carries no credentials -- its rows read on
    the warehouse by the engine's own plan-and-run; with no session, refused in words. Against a stand-in for the driver
    (legend_lite.warehouse's Warehouse and connect, as test_engine.py WarehouseOnPython stands in for it)."""

    URL = 'https://wh.example'
    COLUMNS = [{'name': 'desk', 'type': 'VARCHAR', 'logicalType': 'VARCHAR', 'precision': None, 'scale': None,
                'notNull': False},
               {'name': 'qty', 'type': 'INTEGER', 'logicalType': 'INTEGER', 'precision': None, 'scale': None,
                'notNull': False}]

    def setUp(self):
        super().setUp()
        import duckdb
        import pyarrow as pa
        import pyarrow.ipc
        from legend_lite import warehouse
        objects = [{'catalog': 'main', 'databaseType': 'DuckDB', 'schema': 'trading', 'name': 'POSITIONS', 'kind': 'table',
                    'columns': self.COLUMNS}]
        # the warehouse's own database, as a DuckDB warehouse holds the table: the SQL Python planned runs on it, so a
        # wrong plan answers wrong rows (the review's F33: a stand-in that answers the same rows for any SQL proves none)
        held = duckdb.connect()
        held.execute('CREATE SCHEMA trading')
        held.execute('CREATE TABLE trading."POSITIONS" (desk VARCHAR, qty INTEGER)')
        held.execute("INSERT INTO trading.\"POSITIONS\" VALUES ('FX', 10), ('EQ', 5), ('FX', 3)")
        self.addCleanup(held.close)
        self.ran = ran = []

        class Reader:
            def __init__(self, session, catalog='main', clock=None):
                self._session, self.catalog = session, catalog
            session = property(lambda self: self._session)
            principal = property(lambda self: self._session.principal)
            base_url = property(lambda self: self._session.base_url)

            def renew(self, session):
                self._session = session

            def objects(self):
                return objects

            def arrow_chunks(self, sql, on_receipt=None):
                ran.append(sql)
                rows = held.cursor().execute(sql).to_arrow_table()
                sink = pa.BufferOutputStream()
                with pyarrow.ipc.new_stream(sink, rows.schema) as w:
                    w.write_table(rows)
                yield sink.getvalue().to_pybytes()

        def connect(url, user, password):
            return warehouse.Session(url, 'tok', user, '2099-01-01T00:00:00Z'), objects
        for patched in (mock.patch.object(warehouse, 'Warehouse', Reader), mock.patch.object(warehouse, 'connect', connect)):
            patched.start()
            self.addCleanup(patched.stop)

    def page_over_the_table(self):
        """A page whose grid reads the warehouse table, as the tab's grid over it is written."""
        page = ll.Page('Positions')
        page.grid(pd.DataFrame({'desk': ['FX'], 'qty': [1]}), name='positions')
        doc = page.to_dict()
        cube = doc['cubes'][0]['cube']
        cube['source'] = {'_type': 'warehouseTable', 'name': 'trading.POSITIONS', 'warehouse': self.URL, 'catalog': 'main',
                          'schema': 'trading', 'table': 'POSITIONS', 'columns': cube['source']['columns']}
        return doc

    def test_a_page_over_a_warehouse_table_reads_it_under_the_session_a_tab_signed_in(self):
        doc = self.page_over_the_table()
        # no tab signed in to the warehouse: refused in words, never asked for credentials
        with self.assertRaisesRegex(Exception, 'trading.POSITIONS is read on the warehouse at https://wh.example, which no '
                                               'tab of this host has signed in to: sign in to it in DataCube first'):
            ll.Page.load(doc)
        # a tab signs in, as the engine's route takes its sign-in
        engine = datacube._session.engine()
        said = engine.answer('POST', '/warehouse.json', '', json.dumps(
            {'op': 'connect', 'url': self.URL, 'user': 'rita', 'password': 'pw'}))
        self.assertEqual(said.status, 200, said.body)
        loaded = ll.Page.load(doc)
        grid = loaded.grids[0]
        rows = grid.to_pandas()
        self.assertEqual(sorted(zip(rows['desk'], rows['qty'])), [('EQ', 5), ('FX', 3), ('FX', 10)])
        # edited as any grid, judged over the table's columns as the compiler types them now -- its view the warehouse's
        # answer to the plan of what it shows; saved still over it
        grid.group('desk')
        self.assertEqual(groups_of(grid.view(), 'qty'), {'FX': 13, 'EQ': 5})
        grid.filter(('qty', 'greaterThan', 4))
        self.assertEqual(groups_of(grid.view(), 'qty'), {'FX': 10, 'EQ': 5})
        self.assertTrue(all('POSITIONS' in sql for sql in self.ran), self.ran)
        self.assertEqual(loaded.to_dict()['cubes'][0]['cube']['source']['_type'], 'warehouseTable')

    def test_a_grid_is_added_over_a_warehouse_table_as_the_tab_adds_one(self):
        # (X4) page.grid(warehouse=..., table=..., catalog=...): the table as the tab records it (schema.table), read
        # under the session the engine holds for the warehouse (headless sourceCube), refused in words without one
        page = ll.Page('Positions')
        with self.assertRaisesRegex(ValueError, 'which no tab of this host has signed in to'):
            page.grid(warehouse=self.URL, catalog='main', table='trading.POSITIONS')
        engine = datacube._session.engine()
        self.assertEqual(engine.answer('POST', '/warehouse.json', '', json.dumps(
            {'op': 'connect', 'url': self.URL, 'user': 'rita', 'password': 'pw'})).status, 200)
        grid = page.grid(warehouse=self.URL, catalog='main', table='trading.POSITIONS', rows=['desk'],
                         measures={'qty': 'sum'})
        source = page.to_dict()['cubes'][0]['cube']['source']
        self.assertEqual({k: source[k] for k in ('_type', 'name', 'warehouse', 'catalog', 'schema', 'table')},
                         {'_type': 'warehouseTable', 'name': 'trading.POSITIONS', 'warehouse': self.URL, 'catalog': 'main',
                          'schema': 'trading', 'table': 'POSITIONS'})
        self.assertEqual([c['name'] for c in source['columns']], ['desk', 'qty'])
        self.assertEqual(groups_of(grid.view(), 'qty'), {'FX': 13, 'EQ': 5})
        with self.assertRaisesRegex(ValueError, 'a warehouse table is named schema.table, not "POSITIONS"'):
            page.grid(warehouse=self.URL, catalog='main', table='POSITIONS')
        with self.assertRaisesRegex(ValueError, 'a warehouse table is in a catalog: name it'):
            page.grid(warehouse=self.URL, table='trading.POSITIONS')
        with self.assertRaisesRegex(ValueError, 'a class, a saved query and a warehouse table are named as DataCube names'):
            page.grid(warehouse=self.URL, catalog='main', table='trading.POSITIONS', name='mine')

    def test_a_warehouse_grid_is_read_while_every_compiler_thread_waits(self):
        # F29: what DataCube's thread asks of the warehouse (its table opened, as every grid call over it asks) runs on that
        # thread, never waiting on the compiler's pool -- whose threads may all be waiting for DataCube's turn
        doc = self.page_over_the_table()
        engine = datacube._session.engine()
        self.assertEqual(engine.answer('POST', '/warehouse.json', '', json.dumps(
            {'op': 'connect', 'url': self.URL, 'user': 'rita', 'password': 'pw'})).status, 200)
        grid = ll.Page.load(doc).grids[0]
        busy, free = threading.Semaphore(0), threading.Event()
        waiting = [_pool._workers().submit(lambda: (busy.release(), free.wait(60))) for _ in range(_pool._WORKERS)]
        try:
            for _ in waiting:
                self.assertTrue(busy.acquire(timeout=10))
            seen = {}
            reading = threading.Thread(target=lambda: seen.setdefault('view', grid.view()))
            reading.start()
            reading.join(30)
            self.assertFalse(reading.is_alive(), "a warehouse grid's call waited on the compiler's pool")
            self.assertEqual(len(seen['view'].rows), 3)
        finally:
            free.set()


class OverSavedQueries(Paged):
    """A page over a saved query (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md X1, X2, X5): opened as the tab opens one
    (saved-queries.ts savedQueryOpened, its rules 1-4) over its project's version as this host's depot gives it, its
    rows its project's own Data rows, read in the project's own database by the engine's plan-and-run (C5). The depot's
    answer for the version is served here (the host's fetch, watched); everything after it is the product's."""

    PARTY = """Class demo::party::Party
{
  id: Integer[1];
  name: String[1];
}

###Relational
Database demo::party::PartyDB
(
  Schema PARTY
  (
    Table PARTY (ID INTEGER PRIMARY KEY, NAME VARCHAR(200))
  )
)

###Mapping
Mapping demo::party::PartyMapping
(
  *demo::party::Party: Relational
  {
    ~primaryKey ([demo::party::PartyDB] PARTY.PARTY.ID)
    ~mainTable [demo::party::PartyDB] PARTY.PARTY
    id: [demo::party::PartyDB] PARTY.PARTY.ID,
    name: [demo::party::PartyDB] PARTY.PARTY.NAME
  }
)

###Connection
RelationalDatabaseConnection demo::party::Connection
{
  store: demo::party::PartyDB;
  type: DuckDB;
  specification: DuckDB { };
  auth: Test;
}

###Runtime
Runtime demo::party::Runtime
{
  mappings: [demo::party::PartyMapping];
  connections: [demo::party::PartyDB: [db: demo::party::Connection]];
}

###Data
Data demo::party::PartyData
{
  Relational
  #{
    PARTY.PARTY:
      'ID,NAME\\n1,Meridian Capital\\n2,Halberd Securities\\n';
  }#
}
"""

    def setUp(self):
        super().setUp()
        # this user's things (the saved queries' store) in a folder of the test's own
        home = mock.patch.dict(os.environ, {'LEGEND_LITE_HOME': tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))})
        home.start()
        self.addCleanup(home.stop)
        answer = _headless.answer_fetch
        self.asked = []

        def depot(method, url, body):
            if url.startswith('/depot/api/projects/dependencies/pure'):
                self.asked.append(json.loads(body))
                return 200, json.dumps([{'groupId': 'demo', 'artifactId': 'party', 'versionId': '1.0.0',
                                         'files': [{'path': 'party.pure', 'pureCode': self.PARTY}]}])
            return answer(method, url, body)
        patched = mock.patch.object(_headless, 'answer_fetch', depot)
        patched.start()
        self.addCleanup(patched.stop)

    def page_over_the_query(self, version='1.0.0'):
        """A page whose grid reads the saved query, as the tab writes a grid over one."""
        page = ll.Page('Parties')
        page.grid(pd.DataFrame({'name': ['x']}), name='parties')
        doc = page.to_dict()
        cube = doc['cubes'][0]['cube']
        cube['source'] = {'_type': 'savedQuery', 'name': 'Parties', 'columns': cube['source']['columns'], 'query': {
            'name': 'Parties', 'groupId': 'demo', 'artifactId': 'party', 'versionId': version,
            'content': '|demo::party::Party.all()->project(~[name: p|$p.name])',
            'executionContext': {'_type': 'explicitExecutionContext', 'mapping': 'demo::party::PartyMapping',
                                 'runtime': 'demo::party::Runtime'}}}
        return doc

    def test_a_page_over_a_saved_query_reads_its_projects_own_rows(self):
        loaded = ll.Page.load(self.page_over_the_query())
        self.assertEqual(self.asked[-1], [{'groupId': 'demo', 'artifactId': 'party', 'versionId': '1.0.0'}])
        grid = loaded.grids[0]
        self.assertEqual(sorted(grid.to_pandas()['name']), ['Halberd Securities', 'Meridian Capital'])
        # edited as any grid, and saved still over the query
        grid.sort('name', 'desc')
        self.assertEqual([row[0] for row in grid.view().rows], ['Meridian Capital', 'Halberd Securities'])
        self.assertEqual(loaded.to_dict()['cubes'][0]['cube']['source']['query']['versionId'], '1.0.0')

    def test_a_grid_is_added_over_a_saved_query_by_its_id(self):
        # (X4) page.grid(query=id): the query as this user's store holds it, written down as the tab writes one (query-store
        # sharedPart), opened over its project's version as this host's depot gives it (headless sourceCube)
        record = {'id': 'parties-1', 'name': 'Parties', 'groupId': 'demo', 'artifactId': 'party', 'versionId': '1.0.0',
                  'content': '|demo::party::Party.all()->project(~[name: p|$p.name])',
                  'executionContext': {'_type': 'explicitExecutionContext', 'mapping': 'demo::party::PartyMapping',
                                       'runtime': 'demo::party::Runtime'}}
        status, said = _home.store_call('lite_saved_queries', _home.queries_folder(), 'POST', '', '', json.dumps(record),
                                        _home.THIS_USER)
        self.assertEqual(status, 200, said)
        page = ll.Page('Parties')
        grid = page.grid(query='parties-1')
        self.assertEqual(sorted(grid.to_pandas()['name']), ['Halberd Securities', 'Meridian Capital'])
        source = page.to_dict()['cubes'][0]['cube']['source']
        self.assertEqual((source['_type'], source['name'], source['query']['versionId']), ('savedQuery', 'Parties', '1.0.0'))
        self.assertNotIn('id', source['query'], "the query as it is, not where it is stored")
        with self.assertRaisesRegex(ValueError, "no saved query 'nope' in this user's store"):
            page.grid(query='nope')

    def test_a_query_whose_version_the_depot_does_not_hold_is_refused_in_words(self):
        def none(method, url, body):
            return 404, json.dumps({'message': 'no such version'})
        with mock.patch.object(_headless, 'answer_fetch', none), \
                self.assertRaisesRegex(Exception, "Parties is a saved query of demo:party:1.0.0, which this host's depot "
                                                  'does not hold'):
            ll.Page.load(self.page_over_the_query())


class PublishedQueries(Paged):
    """A SAVED QUERY OF A PUBLISHED VERSION, END TO END (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md F33, X4): a project made
    in this user's model home by DataCube's Create a Class…, its workspace reviewed, approved and committed, a version
    published by its SDLC, a query of that version saved in this user's store, and a grid added over the query, read
    through this host's own /depot/api -- nothing stood in for."""

    def setUp(self):
        super().setUp()
        home = mock.patch.dict(os.environ, {'LEGEND_LITE_HOME': tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))})
        home.start()
        self.addCleanup(home.stop)

    def sdlc(self, method, target, body=None):
        status, said = _home.home_call(_home.model_home_folder(), 'sdlc', method, target,
                                       None if body is None else json.dumps(body))
        self.assertLess(status, 300, f'{method} {target}: {said}')
        return json.loads(said) if said else None

    def test_a_query_of_a_published_version_opens_through_the_hosts_depot(self):
        page = ll.Page('Published')
        path = page.grid(trades(), name='trades').create_class('Trade')
        model = page.model
        project = model['project']
        # the workspace Create a Class… wrote, reviewed, approved and committed; a version of the project published
        review = self.sdlc('POST', f'/projects/{project}/reviews',
                           {'workspaceId': model['workspace'], 'title': 'Trade', 'description': 'the class'})['id']
        self.sdlc('POST', f'/projects/{project}/reviews/{review}/approve', {})
        self.sdlc('POST', f'/projects/{project}/reviews/{review}/commit', {'message': 'Trade'})
        version = self.sdlc('POST', f'/projects/{project}/versions', {'versionType': 'MAJOR'})['id']
        self.assertEqual(version, {'majorVersion': 1, 'minorVersion': 0, 'patchVersion': 0})
        group, artifact = project.split(':')
        record = {'id': 'trades-1', 'name': 'Trades', 'groupId': group, 'artifactId': artifact, 'versionId': '1.0.0',
                  'content': f'|{path}.all()->project(~[desk: x|$x.desk, notional: x|$x.notional])',
                  'executionContext': {'_type': 'explicitExecutionContext',
                                       'mapping': f"{model['package']}::mapping::Model",
                                       'runtime': f"{model['package']}::runtime::Python"}}
        status, said = _home.store_call('lite_saved_queries', _home.queries_folder(), 'POST', '', '', json.dumps(record),
                                        _home.THIS_USER)
        self.assertEqual(status, 200, said)
        # the grid over the query, its model read through this host's /depot/api and its columns typed by the compiler
        grid = page.grid(query='trades-1', rows=['desk'], measures={'notional': 'sum'})
        source = page.to_dict()['cubes'][-1]['cube']['source']
        self.assertEqual((source['query']['versionId'], [c['name'] for c in source['columns']]), ('1.0.0', ['desk', 'notional']))
        # its rows are the version's own (the sample Create a Class… wrote, in the version's database) -- F44, declared:
        # that database's tables are made by the DuckDB dialect's DDL, which spells an unquoted column upper case
        # (DuckDb.ddlIdentifier), so a query selecting `t0.desk` unaliased is answered `DESK`, which its plan does not
        # type. When the dialect line closes F44 this fails: assert the rows instead ({'FX': 100.0, 'Rates': 325.5})
        with self.assertRaisesRegex(Exception, "the plan does not type the result column 'DESK'"):
            grid.view()


class CorpusPage(unittest.TestCase):
    """THE SAVED-PAGE CORPUS (docs/DATACUBE_COMMAND_API_HOMEWORK_2026_10_17.md §11, part 3): the corpus's frame page,
    written by DataCube's own writers (datacube/test/fixtures/pages/make.ts), loaded in Python over its frame with
    nothing to say, and written again as the same document -- the page DataCube saved is the page Python keeps."""

    def setUp(self):
        self._site = os.environ.get('LEGEND_LITE_SITE')
        os.environ['LEGEND_LITE_SITE'] = tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))
        datacube._session = datacube._Session()

    def tearDown(self):
        for view in list(datacube._session.pages):
            view.close()
        # its DataCube engine (headless) closed with it: a session dropped open leaves that engine's folder to the garbage
        # collector, which warns about it under whichever later test it reaches (CorpusPage counts every warning)
        if datacube._session._headless is not None:
            datacube._session._headless.close()
        if self._site is None:
            os.environ.pop('LEGEND_LITE_SITE', None)
        else:
            os.environ['LEGEND_LITE_SITE'] = self._site

    @staticmethod
    def frame():
        """The frame the page was made over, typed as its cubes record it: text, a date, a float and an integer."""
        return pd.DataFrame({
            'region': ['EMEA', 'EMEA', 'AMER', 'APAC'],
            'desk': ['Rates', 'FX', 'Rates', 'FX'],
            'traded': [datetime.date(2024, 1, 2), datetime.date(2024, 2, 3), datetime.date(2024, 3, 4), datetime.date(2024, 4, 5)],
            'notional': [100.5, 20.25, 300.75, 4.5],
            'qty': [1, 2, 3, 4],
        })

    def test_the_frame_page_loads_with_nothing_to_say_and_is_written_as_it_was(self):
        saved = json.loads(Path(os.environ['LEGEND_LITE_CORPUS_PAGE']).read_text('utf-8'))
        with warnings.catch_warnings(record=True) as said:
            warnings.simplefilter('always')
            page = ll.Page.load(saved, frames={'trades': self.frame()})
        self.assertEqual([str(w.message) for w in said], [], 'DataCube opens each cube with nothing to say')
        self.assertEqual(page.to_dict(), saved)
        self.assertEqual([g.title for g in page.grids], ['Positions', 'By desk'])
        self.assertEqual(len(page.charts), 2)

    def test_a_cube_reopened_over_a_changed_frame_keeps_what_only_its_query_carries(self):
        # the frame has a column more, so each cube is rebuilt over it (headless gridCube) -- and a column's Aggregation
        # the saved cube holds only in its query (a document written before its configuration said it) is kept, as the
        # screen keeps it (cube-state.ts openedState), not folded away with a configuration that does not name it
        saved = json.loads(Path(os.environ['LEGEND_LITE_CORPUS_PAGE']).read_text('utf-8'))
        positions = saved['cubes'][0]['cube']
        del positions['configuration']['columns']['qty']['aggregateFn']
        frame = self.frame().assign(book=['B1', 'B2', 'B1', 'B2'])
        with warnings.catch_warnings(record=True) as said:
            warnings.simplefilter('always')
            page = ll.Page.load(saved, frames={'trades': frame})
        self.assertTrue(said and all('new column' in str(w.message) and 'book' in str(w.message) for w in said),
                        [str(w.message) for w in said])
        written = page.to_dict()['cubes'][0]['cube']
        qty = next(c for c in written['query']['columns'] if c['name'] == 'qty')
        self.assertEqual(qty.get('aggregate'), 'average', 'the Aggregation the query carried is kept')
        self.assertEqual(written['configuration']['columns']['qty'].get('aggregateFn'), 'average',
                         'and read back into the configuration, as the screen reads it')
        self.assertTrue(written['configuration']['columns']['book']['hidden'], 'the new column is hidden')
