"""ll.Page (legend_lite.page; docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md): a DataCube page built in Python -- its
grids over frames, their charts, sheets, layouts and stacks -- written as DataCube's own page document; a calculated
column checked by the compiler at the line that adds it; a filter, an aggregate or a chart outside DataCube's words
refused there; tiles placed and taken off as DataCube's layout places them; saved and loaded again as it was, what it
does not model kept; shown in a tab and live from then on (a change served again, a frame updated moving its version),
and read back into the same page as the open page says it is."""

import copy
import datetime
import gc
import json
import os
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
        # one rule (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D11, B3): unnamed, as the tab's Blank Page -- no name in the
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
        with self.assertRaisesRegex(ValueError, r'only a weighted average \(wavg\) takes a weight'):
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
        with self.assertRaisesRegex(ValueError, 'median'):
            grid.window('middle', 'median', column='notional')
        with self.assertRaisesRegex(ValueError, "a child aggregate is a group-level column"):
            grid._column({'name': 'rowwise', 'mode': 'children', 'child': {'fn': 'max', 'of': 'notional'}})
        # explode: each row once per element of what its expression gives
        flat = page.grid(trades(), name='explode_trades')
        flat.explode('each', '[1, 2]', kind='dimension')
        self.assertTrue(next(d for d in flat._query['derived'] if d['name'] == 'each')['unnest'])
        self.assertEqual(len(flat.to_pandas()), 6)
        with self.assertRaisesRegex(ValueError, 'a row-level expression only'):
            flat._column({'name': 'late', 'expression': 'x|[1, 2]', 'level': 'group', 'explode': True})

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
