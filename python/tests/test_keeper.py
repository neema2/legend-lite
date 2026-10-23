"""ONE KEEPER PER PAGE (legend_lite/keeper.py; docs/DATACUBE_PAGE_KEEPER_2026_10_20.md): a page written only against
the version it is at, each version with the commands that moved it there, a write that changes nothing no version; and
-- the guard of the whole design -- a page Python and an open page both edit, in any interleaving, never two pages and
never an edit lost: after every step Python's page, the page the open page holds and the keeper's are one; what every
edit that went through said (a name, a sheet, a grid's groups, a tile, a Snap) is on it, in the order the edits were
written; an edit is refused only for a real conflict; and the copies held are exactly those the kept pages read.

The open page here is DataCube's own functions run headless (pageApply, gridApply: what the tab runs), writing as the
tab writes (page.json, against the version it holds, its edits as commands, which the keeper applies -- refused, it
opens the page as the keeper has it) and following as it follows (page.json?since=: the
commands, or the page). The intent each edit carries is kept apart, in a model of its own (``Intent``), never read back
from the mechanism it checks."""

import json
import os
import random
import tempfile
import threading
import unittest
from unittest import mock

import pandas as pd

import legend_lite as ll
from legend_lite import _json, datacube
from legend_lite import keeper as keeper_module
from legend_lite.keeper import PageKeeper, Refused, Stale

# how many interleavings, and of how many steps: the seed of a failure is printed, so it can be run alone (a long soak:
# LEGEND_LITE_KEEPER_SEEDS=500)
SEEDS = int(os.environ.get('LEGEND_LITE_KEEPER_SEEDS', '8'))
STEPS = 30
# a refusal for a conflict the page cannot settle names the page changed where it is open (page.py _commit)
CONFLICT = 'changed where it is open'


def reads(frames=(), copies=(), ids=()):
    """What a page reads, as DataCube says with each write (headless pageReads)."""
    return {'frames': list(frames), 'copies': list(copies), 'ids': list(ids)}


NOTHING = reads()


class Keeper(unittest.TestCase):
    """The keeper alone: storage that never reads a page."""

    def test_a_write_against_the_version_it_is_at_moves_it_one_and_against_another_is_refused(self):
        told = []
        keeper = PageKeeper(on_write=lambda key, version: told.append((key, version)))
        self.assertEqual(keeper.keep('p', {'name': 'a'}, reads(['f']), []), 1)
        self.assertEqual(keeper.write('p', 1, {'name': 'b'}, reads(['f']), [{'page': 'x'}], []), 2)
        with self.assertRaises(Stale) as stale:
            keeper.write('p', 1, {'name': 'c'}, reads(['f']), None, [])
        self.assertEqual(stale.exception.version, 2)
        self.assertEqual(keeper.current('p'), (2, {'name': 'b'}))
        # a write that changes nothing is no version, and told to nobody
        self.assertEqual(keeper.write('p', 2, {'name': 'b'}, reads(['f']), [{'page': 'y'}], []), 2)
        self.assertEqual(told, [('p', 2)])
        # what it reads is part of what it is: the same page over other frames is a version
        self.assertEqual(keeper.write('p', 2, {'name': 'b'}, reads(['f', 'g']), None, []), 3)
        self.assertEqual(keeper.frames('p'), ['f', 'g'])
        with self.assertRaises(KeyError):
            keeper.write('nope', 1, {}, NOTHING, None, [])

    def test_every_id_it_has_held_stays_held(self):
        keeper = PageKeeper()
        keeper.keep('p', {'n': 0}, reads(ids=['grid-1']), ['grid-1'])
        keeper.write('p', 1, {'n': 1}, reads(ids=['grid-2']), None, ['grid-2'])
        keeper.write('p', 2, {'n': 2}, reads(ids=['sheet-1']), None, [])
        self.assertEqual(keeper.held('p'), ['grid-1', 'grid-2', 'sheet-1'], 'a grid taken off: its id held all the same')

    def test_what_moved_it_since_a_version_is_its_edits_or_none(self):
        keeper = PageKeeper()
        keeper.keep('p', {'n': 0}, NOTHING, [])
        self.assertEqual(keeper.changes('p', 1), (1, []))
        self.assertEqual(keeper.changes('p', 0), (1, None), 'kept from version 1: nothing moved it there')
        keeper.write('p', 1, {'n': 1}, NOTHING, ['a'], [])
        keeper.write('p', 2, {'n': 2}, NOTHING, ['b', 'c'], [])
        self.assertEqual(keeper.changes('p', 1), (3, ['a', 'b', 'c']))
        keeper.write('p', 3, {'n': 3}, NOTHING, None, [])
        self.assertEqual(keeper.changes('p', 2), (4, None), 'a change by no command since')
        self.assertEqual(keeper.changes('p', 4), (4, []))
        self.assertEqual(keeper.changes('p', 9), (4, None), 'a version it never had')

    def test_only_its_last_versions_keep_their_edits(self):
        keeper = PageKeeper()
        keeper.keep('p', {'n': 0}, NOTHING, [])
        for n in range(1, 70):
            keeper.write('p', n, {'n': n}, NOTHING, [n], [])
        self.assertEqual(keeper.changes('p', 69 - 63 + 1)[1], list(range(69 - 63 + 1, 70)), 'the last 63 moves')
        self.assertEqual(keeper.changes('p', 2), (70, None), 'older: the page opened again')

    def test_its_document_is_its_own(self):
        keeper = PageKeeper()
        doc = {'views': [{'id': 'grid-1'}]}
        keeper.keep('p', doc, NOTHING, [])
        doc['views'].append({'id': 'grid-2'})
        self.assertEqual(keeper.current('p')[1], {'views': [{'id': 'grid-1'}]}, 'kept: copied in')
        written = {'views': [{'id': 'grid-3'}]}
        edits = [{'page': {'kind': 'x'}}]
        keeper.write('p', 1, written, NOTHING, edits, [])
        written['views'].clear()
        edits[0]['page']['kind'] = 'y'
        self.assertEqual(keeper.current('p')[1], {'views': [{'id': 'grid-3'}]}, 'written: copied in')
        self.assertEqual(keeper.changes('p', 1)[1], [{'page': {'kind': 'x'}}], 'its edits too')

    def test_each_copy_a_page_read_is_the_keepers_and_goes_when_nothing_reads_it(self):
        # docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z17: one model of whose a copy is -- its maker's until a page reads it
        keeper = PageKeeper()
        keeper.keep('p', {}, reads(copies=['snap']), [])
        keeper.keep('q', {}, reads(copies=['snap_2']), [])
        self.assertEqual([keeper.has_read(t) for t in ('snap', 'snap_2', 'snap_3')], [True, True, False],
                         "one no page has read is its maker's")
        self.assertEqual(keeper.letting_go(['snap', 'snap_2', 'snap_3']), [], "each read now, or its maker's")
        keeper.forget('q')
        self.assertEqual(keeper.letting_go(['snap', 'snap_2', 'snap_3']), ['snap_2'], 'read by a page collected only')
        keeper.write('p', 1, {'live': True}, NOTHING, None, [])
        self.assertEqual(keeper.letting_go(['snap', 'snap_3']), ['snap'], 'read by the page as it was only')
        keeper.gone('snap')
        self.assertFalse(keeper.has_read('snap'), 'let go: no longer the keeper\'s')
        self.assertFalse(keeper.served('p'))
        keeper.serve('p', True)
        self.assertTrue(keeper.served('p'))
        keeper.serve('p', False)
        self.assertFalse(keeper.served('p'))


    def test_each_file_a_page_read_is_kept_by_the_same_rule_as_a_copy(self):
        # docs/DATACUBE_ONE_API_PLAN_2026_10_21.md X3: a page's files by their bytes' hash, its remote files by URL -- each
        # kept apart from the copies, by its own key, as a copy is
        keeper = PageKeeper()
        keeper.keep('p', {}, {**reads(copies=['snap']), 'files': ['abc'], 'remotes': ['https://x/y.csv']}, [])
        self.assertEqual([keeper.has_read('abc', 'files'), keeper.has_read('abc'), keeper.has_read('https://x/y.csv', 'remotes')],
                         [True, False, True], 'each kind by its own key')
        self.assertEqual(keeper.letting_go(['abc', 'def'], 'files'), [], 'read now, or its reader\'s')
        keeper.write('p', 1, {'v': 2}, reads(copies=['snap']), None, [])
        self.assertEqual(keeper.letting_go(['abc'], 'files'), ['abc'], 'read by the page as it was only')
        self.assertEqual(keeper.letting_go(['snap']), [], 'the copy is still read')
        keeper.gone('abc', 'files')
        self.assertFalse(keeper.has_read('abc', 'files'))

    def test_a_page_open_on_it_keeps_the_copies_of_the_version_it_shows_and_of_every_one_since(self):
        # the keeper's known limit 6 (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md), closed by Z17: an open page that has not
        # followed yet reads the copies of the version it shows, and follows through every version since
        keeper = PageKeeper()
        keeper.keep('p', {'v': 1}, reads(copies=['snap']), [])
        keeper.serve('p', True)
        self.assertFalse(keeper.follow('p', 'tab', 1), 'a page open: nothing it moved past')
        keeper.write('p', 1, {'v': 2}, reads(copies=['snap_2']), None, [])
        keeper.write('p', 2, {'v': 3}, NOTHING, None, [])
        both = ['snap', 'snap_2']
        self.assertEqual(keeper.letting_go(both), [], 'the tab shows version 1, and follows through 2')
        self.assertFalse(keeper.follow('p', 'tab', None), 'following: the version it said last stands')
        self.assertEqual(keeper.letting_go(both), [])
        self.assertFalse(keeper.follow('other', 'tab', 3), 'a page not kept follows nothing')
        self.assertTrue(keeper.follow('p', 'tab', 2), 'moved: a sweep')
        self.assertEqual(keeper.letting_go(both), ['snap'])
        self.assertTrue(keeper.follow('p', 'tab', 3))
        self.assertEqual(keeper.letting_go(both), both)
        # one that says nothing for FOLLOWED_FOR seconds keeps nothing, nor does a page served nowhere
        keeper.follow('p', 'tab', 1)
        with mock.patch.object(keeper_module, 'FOLLOWED_FOR', -1.0):
            self.assertEqual(keeper.letting_go(both), both, 'silent too long')
        keeper.follow('p', 'tab', 1)
        keeper.follow('p', 'tab', None)
        self.assertEqual(keeper.letting_go(both), [], 'saying no version keeps it open')
        keeper.serve('p', False)
        self.assertEqual(keeper.letting_go(both), both)
        # two pages open: the one furthest behind keeps what it shows
        keeper.serve('p', True)
        keeper.follow('p', 'tab', 3)
        keeper.follow('p', 'widget', 2)
        self.assertEqual(keeper.letting_go(both), ['snap'])


class Settle(unittest.TestCase):
    """A write settled (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4): its edits applied to the page as it is now by the
    combine its host gives (DataCube's pageRebase: at the writer's version nothing to combine, past it made again on
    top) and what they make kept -- the keeper itself applies nothing; a page written whole only as a restore, at its
    version."""

    def combining(self, answers):
        """A combine answering ``answers`` in turn (each a dict, or a function of what it was asked), and what it was
        asked."""
        asked = []

        def combine(page, held, edits, known):
            asked.append((page, held, edits, known))
            answer = answers.pop(0)
            return answer(page, held, edits, known) if callable(answer) else answer
        return combine, asked

    def test_at_its_version_its_edits_are_applied_and_what_they_make_kept(self):
        made = {'page': {'n': 1}, 'held': ['sheet-9'], 'edits': [{'page': 'x, as applied'}], 'renamed': {}, 'reads': NOTHING}
        combine, asked = self.combining([made])
        keeper = PageKeeper(combine=combine)
        keeper.keep('p', {'n': 0}, NOTHING, ['sheet-1'])
        settled = keeper.settle('p', 1, [{'page': 'x'}])
        self.assertEqual((settled.version, settled.combined, dict(settled.renamed)), (2, False, {}))
        self.assertEqual(asked, [({'n': 0}, ['sheet-1'], [{'page': 'x'}], ['sheet-1'])], 'the page as it is, what it knew')
        self.assertEqual(keeper.current('p'), (2, {'n': 1}), 'what the commands made')
        self.assertEqual(keeper.held('p'), ['sheet-1', 'sheet-9'], 'every id they gave out, held (H3)')
        self.assertEqual(keeper.changes('p', 1), (2, [{'page': 'x, as applied'}]), 'the edits as applied')

    def test_an_id_given_out_afresh_at_its_version_has_the_writer_take_the_keepers_page(self):
        made = {'page': {'n': 1}, 'held': ['sheet-3'], 'edits': [], 'renamed': {'sheet-2': 'sheet-3'}, 'reads': NOTHING}
        keeper = PageKeeper(combine=self.combining([made])[0])
        keeper.keep('p', {'n': 0}, NOTHING, [])
        settled = keeper.settle('p', 1, [{'page': 'x'}])
        self.assertEqual((settled.combined, dict(settled.renamed)), (True, {'sheet-2': 'sheet-3'}))

    def test_behind_its_edits_are_made_again_on_top_with_the_ids_it_knew_then(self):
        made = {'page': {'n': 'both'}, 'held': ['sheet-3', 'sheet-4'], 'edits': [{'page': 'x, as applied'}],
                'renamed': {'sheet-3': 'sheet-4'}, 'reads': reads(ids=['sheet-4'])}
        combine, asked = self.combining([made])
        keeper = PageKeeper(combine=combine)
        keeper.keep('p', {'n': 0}, NOTHING, ['sheet-1'])
        keeper.write('p', 1, {'n': 'theirs'}, NOTHING, [{'page': 'y'}], ['sheet-3'])
        settled = keeper.settle('p', 1, [{'page': 'x'}])
        self.assertEqual((settled.version, settled.combined, dict(settled.renamed)), (3, True, {'sheet-3': 'sheet-4'}))
        self.assertEqual(asked, [({'n': 'theirs'}, ['sheet-1', 'sheet-3'], [{'page': 'x'}], ['sheet-1'])],
                         'on the page as it is, every id it holds -- and the ids it held at the writer\'s version')
        self.assertEqual(keeper.current('p'), (3, {'n': 'both'}))
        self.assertEqual(keeper.changes('p', 2), (3, [{'page': 'x, as applied'}]), 'the edits as made again')

    def test_combined_while_another_wrote_it_is_combined_again_on_that(self):
        keeper = PageKeeper()

        def racing(page, held, edits, known):
            # another writer lands while this one is being made again
            keeper.write('p', 2, {'n': 'third'}, NOTHING, [{'page': 'z'}], [])
            return {'page': {'n': 'lost'}, 'held': [], 'edits': edits, 'renamed': {}, 'reads': NOTHING}
        made = {'page': {'n': 'on the third'}, 'held': [], 'edits': [{'page': 'x'}], 'renamed': {}, 'reads': NOTHING}
        keeper.combine, asked = self.combining([racing, made])
        keeper.keep('p', {'n': 0}, NOTHING, [])
        keeper.write('p', 1, {'n': 'theirs'}, NOTHING, [{'page': 'y'}], [])
        settled = keeper.settle('p', 1, [{'page': 'x'}])
        self.assertEqual([a[0] for a in asked], [{'n': 'theirs'}, {'n': 'third'}])
        self.assertEqual((settled.version, keeper.current('p')[1]), (4, {'n': 'on the third'}))

    def test_refused_when_its_edits_do_not_apply_or_the_host_refuses_what_they_make(self):
        combine, _ = self.combining([{'problems': ['grid-2 is no longer on the page']},
                                     {'page': {'n': 'reads x'}, 'held': [], 'edits': [], 'renamed': {}, 'reads': reads(['x'])}])
        keeper = PageKeeper(combine=combine)
        keeper.keep('p', {'n': 0}, NOTHING, [])
        keeper.write('p', 1, {'n': 1}, NOTHING, None, [])
        with self.assertRaisesRegex(Refused, 'grid-2 is no longer on the page'):
            keeper.settle('p', 1, [{'grid': 'grid-2', 'command': {}}])
        with self.assertRaisesRegex(Refused, 'x is not served'):
            keeper.settle('p', 2, [{'page': 'x'}], lambda r: 'x is not served' if 'x' in r['frames'] else None)
        self.assertEqual(keeper.current('p'), (2, {'n': 1}), 'nothing changed')

    def test_a_restore_is_kept_whole_at_its_version_only(self):
        keeper = PageKeeper(combine=self.combining([])[0])
        keeper.keep('p', {'n': 0}, NOTHING, [])
        self.assertEqual(keeper.restore('p', 1, {'n': 'undone'}, NOTHING, ['grid-1']), 2)
        self.assertEqual(keeper.changes('p', 1), (2, None), 'whole: an open page opens it again')
        keeper.write('p', 2, {'n': 'moved'}, NOTHING, [{'page': 'y'}], [])
        with self.assertRaisesRegex(Refused, 'undone or redone'):
            keeper.restore('p', 2, {'n': 'undone again'}, NOTHING, [])
        self.assertEqual(keeper.current('p'), (3, {'n': 'moved'}), 'nothing kept')

    def test_a_write_from_further_back_than_it_keeps_is_refused(self):
        from legend_lite.keeper import STEPS_KEPT
        keeper = PageKeeper(combine=self.combining([])[0])
        keeper.keep('p', {'n': 0}, NOTHING, [])
        for n in range(1, STEPS_KEPT + 2):
            keeper.write('p', n, {'n': n}, NOTHING, [n], [])
        with self.assertRaisesRegex(Refused, 'further than it keeps'):
            keeper.settle('p', 1, [{'page': 'x'}])


def trades():
    return pd.DataFrame({'region': ['EMEA', 'AMER', 'EMEA'], 'desk': ['FX', 'Rates', 'Rates'],
                         'notional': [100.0, 250.0, 75.5], 'pnl': [1.5, -2.0, 0.25]})


def grid_rows(doc, grid):
    view = next(v for v in doc['views'] if v['id'] == grid)
    return next(c['cube'] for c in doc['cubes'] if c['id'] == view['cube'])['query'].get('rows', [])


class Intent:
    """WHAT THE EDITS THAT WENT THROUGH SAID, in the order they were written -- kept apart from the page, by rules of its
    own: a page's name, each sheet's name, the grids and charts on it, each grid's groups and whether it is snapped, the
    titles given. The page must say all of it after every step."""

    def __init__(self, doc):
        self.name = doc['name']
        self.sheets = {s['id']: s.get('name') for s in doc['sheets']}
        self.grids = {v['id']: {'cube': v['cube'], 'rows': grid_rows(doc, v['id']),
                                'snapped': v['cube'] in doc.get('copies', {})}
                      for v in doc['views'] if v['kind'] == 'grid'}
        self.charts = {v['id']: {'cube': v['cube'], 'frozen': bool(v['spec'].get('frozen'))}
                       for v in doc['views'] if v['kind'] == 'chart'}
        self.titles = {}

    def said(self, fact):
        # a tile or a sheet Python made says its id when it is said, not when it was made: made inside a call the open
        # page wrote under, it is given out afresh as the call's edits are made again on top, and its handle follows
        what, *rest = fact() if callable(fact) else fact
        if what == 'name':
            self.name = rest[0]
        elif what == 'sheet':
            self.sheets[rest[0]] = rest[1]
        elif what == 'title':
            self.titles[rest[0]] = rest[1]
        elif what == 'rows':
            self.grids[rest[0]]['rows'] = list(rest[1])
        elif what == 'snapped':
            self.grids[rest[0]]['snapped'] = rest[1]
        elif what == 'grid':
            self.grids[rest[0]] = {'cube': rest[1], 'rows': list(rest[2]), 'snapped': False}
        elif what == 'chart':
            self.charts[rest[0]] = {'cube': rest[1], 'frozen': rest[2]}
        elif what == 'remove':
            tile = rest[0]
            gone = {tile}
            if tile in self.grids:
                # a grid taken off takes the charts that follow it (a frozen one stays, its own)
                cube = self.grids.pop(tile)['cube']
                gone |= {c for c, chart in self.charts.items() if chart['cube'] == cube and not chart['frozen']}
            for id in gone:
                self.charts.pop(id, None)
                self.titles.pop(id, None)
        else:
            raise AssertionError(f'no such fact {fact!r}')

    def check(self, test, doc, where):
        views = {v['id']: v for v in doc['views']}
        test.assertEqual(doc['name'], self.name, f'{where}: its name')
        test.assertEqual({s['id']: s.get('name') for s in doc['sheets']}, self.sheets, f'{where}: its sheets')
        test.assertEqual(sorted(i for i, v in views.items() if v['kind'] == 'grid'), sorted(self.grids), f'{where}: grids')
        test.assertEqual(sorted(i for i, v in views.items() if v['kind'] == 'chart'), sorted(self.charts), f'{where}: charts')
        for id, grid in self.grids.items():
            test.assertEqual(grid_rows(doc, id), grid['rows'], f'{where}: {id} grouped')
            test.assertEqual(views[id]['cube'] in doc.get('copies', {}), grid['snapped'], f'{where}: {id} snapped')
        for id, title in self.titles.items():
            test.assertEqual(views[id].get('title'), title, f'{where}: {id} titled')


class OpenPage:
    """A page open on a Python page, as DataCube's app is to hold one: the page at the version it is at (``base``), what
    is on its screen (``doc``: the base with its own edits not yet written, ``pending``), edited by DataCube's own
    functions (headless), written against its version as commands; refused, it follows what moved the page since and makes
    its own edits again on top."""

    def __init__(self, page):
        self.page = page
        self.engine = datacube._session.engine()
        self.headless = datacube._session.headless()
        said = self.get('')
        self.version, self.base = said['version'], said['page']
        self.doc = self.base
        # each edit not yet written: (its entry -- a page command, or a grid's -- its kind, what it said)
        self.pending = []
        # how many of its writes the keeper made again on top of a page that had moved on; how many times it opened the
        # page again whole for commands that did not apply here
        self.combined = 0
        self.reopened = 0
        # the version its last write made
        self.written = self.version

    def get(self, since):
        answer = self.engine.answer('GET', '/page.json', f'page={self.page._key}{since}', None)
        if answer.status != 200:
            raise AssertionError(f'the page was not read: {answer.status} {answer.body!r}')
        return _json.loads(answer.body)

    def applied(self, doc, command):
        return self.applied_as(doc, command)[0]

    def applied_as(self, doc, command):
        """``command`` applied by DataCube's function (pageApply): the page it makes, and the command as applied (an id
        it made given), which is what the app writes."""
        held = sorted({v['id'] for v in doc['views']} | {s['id'] for s in doc['sheets']} | {c['id'] for c in doc['cubes']})
        said = _json.loads(self.headless.call('pageApply', _json.dumps(doc), json.dumps(held), _json.dumps([command])))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return _json.loads(said['page']), said['commands'][0]

    def grid_applied(self, doc, grid, command):
        from legend_lite.page import _cube_spec
        session = datacube._session
        view = next((v for v in doc['views'] if v['id'] == grid and v['kind'] == 'grid'), None)
        if view is None:
            raise ValueError(f'{grid} is no longer on the page')
        cube = next(c['cube'] for c in doc['cubes'] if c['id'] == view['cube'])
        with session.frames.reading():
            spec = _cube_spec(session, cube, session.frames, self.headless, doc.get('copies', {}).get(view['cube']))
            said = json.loads(self.headless.call('gridApply', spec, _json.dumps([command])))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        # the page as DataCube keeps a grid's edit (headless pageWithCube: its charts following it)
        kept = json.loads(self.headless.call('pageWithCube', _json.dumps(doc), view['cube'], said['cube']))
        return _json.loads(kept['page'])

    def made(self, doc, entry):
        """``entry`` applied to ``doc``: the page it makes, what it said (its facts, read from what it made only where an
        id was given out), and the edit as applied -- what is written."""
        if 'grid' in entry:
            after = self.grid_applied(doc, entry['grid'], entry['command'])
            command = entry['command']
            return after, ([('rows', entry['grid'], command['rows'])] if command['kind'] == 'group' else []), entry
        after, command = self.applied_as(doc, entry['page'])
        return after, self.said(doc, after, command), {'page': command}

    def said(self, doc, after, command):
        """What a page command said, as applied to ``doc`` (making ``after``)."""
        kind = command['kind']
        if kind == 'renamePage':
            return [('name', command['name'])]
        if kind == 'addSheet':
            return [('sheet', command['id'], command['name'])]
        if kind == 'renameSheet':
            return [('sheet', command['sheet'], command['name'])]
        if kind == 'renameTile':
            return [('title', command['tile'], command['title'])]
        if kind == 'removeTile':
            return [('remove', command['tile'])]
        return []

    def follow(self):
        """What moved the page since its version, applied as the app applies it -- the commands, by DataCube's functions,
        or the page whole (and whole too when they do not apply here: a copy a page between read let go meanwhile, known
        limit 6 of the note). Only with nothing pending: its own edits are the keeper's to make again on top."""
        assert not self.pending, 'an open page follows with its own edits written'
        said = self.get(f'&since={self.version}')
        if 'commands' in said:
            try:
                base = self.base
                for entry in said['commands']:
                    base = (self.applied(base, entry['page']) if 'page' in entry
                            else self.grid_applied(base, entry['grid'], entry['command']))
                self.base = base
            except ValueError:
                self.reopened += 1
                said = self.get('')
        if 'page' in said:
            self.base = said['page']
        self.version = said['version']
        self.doc = self.base

    def reopen(self):
        """The page opened again whole, as the keeper has it (what the app does when its write was made again on top,
        or refused)."""
        said = self.get('')
        self.version, self.base, self.doc = said['version'], said['page'], said['page']

    def edit(self, rng):
        """One edit as a person makes one on the page, onto its screen, not yet written: its kind, or None when there was
        none to make or DataCube refused it (a person's refused edit changes nothing)."""
        doc = self.doc
        grids = [v['id'] for v in doc['views'] if v['kind'] == 'grid']
        charts = [v['id'] for v in doc['views'] if v['kind'] == 'chart']
        sheets = [s['id'] for s in doc['sheets']]
        choices = {
            'renamePage': lambda: {'page': {'kind': 'renamePage', 'name': f'Named there {rng.randrange(100)}'}},
            'addSheet': lambda: {'page': {'kind': 'addSheet', 'name': f'There {rng.randrange(100)}'}},
            'renameSheet': lambda: {'page': {'kind': 'renameSheet', 'sheet': rng.choice(sheets),
                                             'name': f'Renamed there {rng.randrange(100)}'}},
            'moveSheet': lambda: len(sheets) > 1 and {'page': {'kind': 'moveSheet', 'sheet': rng.choice(sheets),
                                                               'to': rng.randrange(len(sheets))}},
            'renameTile': lambda: {'page': {'kind': 'renameTile', 'tile': rng.choice(grids + charts),
                                            'title': f'Titled there {rng.randrange(100)}'}},
            # a grid while another is on the page: two taken off at once, one on each side, leave a page of no grid, which
            # both sides write (known limit 5, settled)
            'removeTile': lambda: (charts or len(grids) > 1) and {
                'page': {'kind': 'removeTile', 'tile': rng.choice(charts + (grids if len(grids) > 1 else []))}},
            'group': lambda: grids and {'grid': rng.choice(grids),
                                        'command': {'kind': 'group', 'rows': [rng.choice(['desk', 'region'])]}},
            'addSort': lambda: grids and {'grid': rng.choice(grids), 'command': {
                'kind': 'addSort', 'column': rng.choice(['desk', 'region']), 'direction': rng.choice(['asc', 'desc'])}},
            'pivot': lambda: grids and {'grid': rng.choice(grids),
                                        'command': {'kind': 'pivot', 'columns': rng.choice([[], ['region'], ['desk']])}},
            # a chart's mark clicked: its select and its grid's refilter, written together, as the tab tells them
            'click': lambda: self.click(doc, rng),
            # the tree: a group opened or closed, every group closed, the level the groups open to
            'open': lambda: grids and {'grid': rng.choice(grids), 'command': {
                'kind': 'open', 'path': [rng.choice(['EMEA', 'AMER', 'APAC', 'FX', 'Rates'])], 'open': rng.choice([True, False])}},
            'collapseAll': lambda: grids and {'grid': rng.choice(grids), 'command': {'kind': 'collapseAll'}},
            'level': lambda: grids and {'grid': rng.choice(grids), 'command': {
                'kind': 'configure', 'patch': {'initialExpandToLevel': rng.choice([0, 1, 2])}}},
        }
        kind = rng.choice(sorted(choices))
        entries = choices[kind]()
        if not entries:
            return None
        made = []
        try:
            doc = self.doc
            for entry in entries if isinstance(entries, list) else [entries]:
                doc, facts, entry = self.made(doc, entry)
                made.append((entry, kind, facts))
        except ValueError:
            return None
        self.doc = doc
        self.pending.extend(made)
        return kind

    def click(self, doc, rng):
        """A chart's mark clicked, as the tab tells it (cube-page.ts #select): its page's select -- the mark's condition,
        or none when it is the mark selected already -- and its grid's refilter, the old selection off, the new one on."""
        grid_of = {v['cube']: v['id'] for v in doc['views'] if v['kind'] == 'grid'}
        charts = [v for v in doc['views'] if v['kind'] == 'chart' and v['cube'] in grid_of]
        if not charts:
            return None
        chart = rng.choice(charts)
        mark = {'kind': 'condition', 'column': 'region', 'operator': 'equal', 'value': rng.choice(['EMEA', 'AMER', 'APAC'])}
        old = chart.get('selection', [])
        add = [] if old == [mark] else [mark]
        return [{'page': {'kind': 'select', 'chart': chart['id'], 'conditions': add}},
                {'grid': grid_of[chart['cube']], 'command': {'kind': 'refilter', 'remove': old, 'add': add}}]

    def write(self):
        """Its edits written as the app writes them: the edits it made, against the version it is at -- the keeper applying
        them, made again on top of the page as it is now when it moved on (the page then opened as the keeper made it). What they said, in the order written (each id renamed as the keeper
        gave it out afresh), and the kinds of those let go -- all of them, refused as no longer applying there."""
        if not self.pending:
            return [], []
        edits = [entry for entry, _, _ in self.pending]
        answer = self.engine.answer('POST', '/page.json', f'page={self.page._key}&version={self.version}',
                                    _json.dumps({'edits': edits}))
        if answer.status == 409:
            kinds = [kind for _, kind, _ in self.pending]
            self.pending = []
            self.reopen()
            return [], kinds
        if answer.status != 200:
            raise AssertionError(f'the write was refused: {answer.status} {answer.body!r}')
        said = json.loads(answer.body)
        self.written = said['version']
        renamed = said['renamed']
        facts = [(f[0], renamed.get(f[1], f[1]), *f[2:]) if len(f) > 1 and isinstance(f[1], str) else f
                 for _, _, made in self.pending for f in made]
        self.pending = []
        if said['combined']:
            self.combined += 1
            self.reopen()
        else:
            self.version, self.base = said['version'], self.doc
        return facts, []


PYTHON_KINDS = ('sheet', 'group', 'name', 'snap', 'live', 'renameSheet', 'sort', 'pivot', 'measure', 'chart', 'grid',
                'remove', 'title', 'moveTo', 'expand', 'collapseAll', 'level')


def made(fact):
    """A fact about a tile or a sheet Python made, said with its id when it is said (made inside a call an open page
    wrote under, its id is given out afresh and its handle follows) -- or, taken off by then, as it was made."""
    then = fact()

    def said():
        try:
            return fact()
        except ValueError:
            return then
    return said


def python_edit(page, rng, frame):
    """One edit made in Python: its kind and what it said -- or, refused, its kind and the refusal; (None, None) when
    there was none to make."""
    grids, charts, sheets = page.grids, page.charts, page.sheets
    kind = rng.choice(PYTHON_KINDS)
    try:
        if kind == 'sheet':
            name = f'Python {rng.randrange(100)}'
            sheet = page.sheet(name)
            return kind, [made(lambda: ('sheet', sheet.id, name))]
        if kind == 'name':
            name = f'Named in Python {rng.randrange(100)}'
            page.name = name
            return kind, [('name', name)]
        if kind == 'renameSheet':
            sheet = rng.choice(sheets)
            sheet.name = name = f'Renamed in Python {rng.randrange(100)}'
            return kind, [('sheet', sheet.id, name)]
        if kind == 'title' and grids + charts:
            tile = rng.choice(grids + charts)
            tile.title = title = f'Titled in Python {rng.randrange(100)}'
            return kind, [('title', tile.id, title)]
        if kind == 'remove' and (charts or len(grids) > 1):
            # a grid while another is on the page (OpenPage.edit says why)
            tile = rng.choice(charts + (grids if len(grids) > 1 else []))
            page.remove(tile)
            return kind, [('remove', tile.id)]
        if kind == 'grid':
            rows = [rng.choice(['desk', 'region'])]
            # over a frame the page reads already (its object, as served): no frame served again, no grid opened again
            grid = page.grid(frame[1], name=frame[0], rows=rows)
            return kind, [made(lambda: ('grid', grid.id, grid._view['cube'], rows))]
        if not grids:
            return None, None
        grid = rng.choice(grids)
        if kind == 'group':
            column = rng.choice(['desk', 'region'])
            grid.group(column)
            return kind, [('rows', grid.id, [column])]
        if kind == 'snap':
            grid.snap()
            return kind, [('snapped', grid.id, True)]
        if kind == 'live':
            if grid.snapped is None:
                return None, None
            grid.live()
            return kind, [('snapped', grid.id, False)]
        if kind == 'sort':
            grid.sort(rng.choice(['desk', 'region']), rng.choice(['asc', 'desc']))
            return kind, []
        if kind == 'pivot':
            grid.pivot(*rng.choice([[], ['region'], ['desk']]))
            return kind, []
        if kind == 'measure':
            grid.measure('pnl', rng.choice(['sum', 'max']))
            return kind, []
        if kind == 'chart':
            chart = grid.chart('bar', x=rng.choice(['desk', 'region']), frozen=rng.choice([None, True]))
            return kind, [made(lambda: ('chart', chart.id, chart._view['cube'], bool(chart._view['spec'].get('frozen'))))]
        if kind == 'moveTo':
            grid.move_to(rng.choice(sheets))
            return kind, []
        if kind == 'expand':
            grid.expand(rng.choice(['EMEA', 'AMER', 'APAC', 'FX', 'Rates']))
            return kind, []
        if kind == 'collapseAll':
            grid.collapse_all()
            return kind, []
        if kind == 'level':
            grid.configure({'initialExpandToLevel': rng.choice([0, 1, 2])})
            return kind, []
        return None, None
    except ValueError as refused:
        # refused in DataCube's words, or made against a page that moved on and no longer applying: nothing changed
        return kind, refused


class Converged(unittest.TestCase):
    """The page served from a session of its own (a site of its own), its tabs closed after."""

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

    def built(self, seed):
        """A page of two grids, the first with a chart following it (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S2: every
        side keeps it following by one rule, followedPage), and the frame of its first (its name, its object), for the
        grids Python adds."""
        page = ll.Page('Converges')
        frame = (f'trades_{seed}', trades())
        page.grid(frame[1], name=frame[0], rows=['region'], measures={'notional': 'sum'}).chart('bar')
        page.grid(trades(), name=f'more_{seed}')
        return page, frame

    def copies_read(self, where):
        engine = datacube._session.engine()
        datacube._session.sweep()
        keeper = datacube._session.keeper
        reads = {t for kept in keeper._pages.values() for t in kept.reads['copies']}
        self.assertEqual(reads - set(engine._copies), set(), f'{where}: every copy read is held')
        self.assertEqual(set(engine._copies) - reads, set(), f'{where}: no copy held that no page reads')

    def one(self, page, open_page, intent, where):
        """Python, the open page and the keeper hold one page, which says what every edit written said; the copies held
        are the ones read."""
        kept = datacube._session.keeper.current(page._key)
        open_page.follow()
        self.assertEqual(open_page.version, kept[0], where)
        self.assertEqual(_json.dumps(open_page.doc), _json.dumps(kept[1]), f'{where}: the open page holds the keeper\'s')
        self.assertEqual(_json.dumps(page.to_dict()), _json.dumps(kept[1]), f'{where}: Python holds the keeper\'s')
        intent.check(self, kept[1], where)
        self.followed(kept[1], where)
        self.copies_read(where)

    def followed(self, doc, where):
        """A settled page's charts are as its cubes make them, whoever wrote it (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md
        S2; the S2 audit's 2): each cube's page is a fixed point of DataCube's following (headless pageWithCube, its cube
        as it is), and each chart's selection is on its grid's filter -- a side that followed nothing, or another way,
        fails here even when all three agree."""
        headless = datacube._session.headless()
        for c in doc['cubes']:
            kept = _json.loads(headless.call('pageWithCube', _json.dumps(doc), c['id'], _json.dumps(c['cube'])))
            self.assertNotIn('problems', kept, where)
            self.assertEqual(_json.dumps(_json.loads(kept['page'])), _json.dumps(doc), f'{where}: {c["id"]} followed')
        filters = {c['id']: c['cube']['query'].get('filter') for c in doc['cubes']}

        def conditions(node):
            if node is None:
                return []
            return [n for child in node.get('children', []) for n in conditions(child)] if node['kind'] == 'and' else [node]
        for v in doc['views']:
            if v['kind'] == 'chart' and v.get('selection'):
                on = conditions(filters.get(v['cube']))
                self.assertEqual([c for c in v['selection'] if c not in on], [], f'{where}: {v["id"]}\'s selection on its filter')


class WriteCost(Converged):
    """THE COST OF ONE WRITE (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4: measured on a page of 60 grids, a budget set
    from it): the keeper applies every write as its commands (headless pageRebase), Python's after its own call applied
    them, so a write of Python's costs its call twice over -- a page edit, and a grid edit (its grid planned again) -- and
    grows with the page. ``LEGEND_LITE_WRITE_COST=1`` prints what it measured."""

    # measured 2026-10-21 in the container (the plan's As built, S4): a page write 0.13s, a grid write 0.36s; the budget
    # about ten times that, for CI's slower runners
    BUDGET_SECONDS = {'page': 1.5, 'grid': 3.0}

    def test_one_write_on_a_page_of_sixty_grids(self):
        import time
        page = ll.Page('Sixty')
        frame = trades()
        grids = [page.grid(frame, name=f'sixty_{n}') for n in range(60)]
        took = {}
        start = time.perf_counter()
        page.sheets[0].name = 'Renamed'
        took['page'] = time.perf_counter() - start
        start = time.perf_counter()
        grids[30].group('desk')
        took['grid'] = time.perf_counter() - start
        if os.environ.get('LEGEND_LITE_WRITE_COST'):
            print(f'one write on a page of 60 grids: {took}')
        kept = datacube._session.keeper.current(page._key)[1]
        self.assertEqual(kept['sheets'][0].get('name'), 'Renamed', 'the keeper keeps what the commands made')
        for what, seconds in took.items():
            self.assertLess(seconds, self.BUDGET_SECONDS[what], f'one {what} write took {seconds:.2f}s')


class EmptyPage(Converged):
    """KNOWN LIMIT 5, SETTLED (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md, found by the convergence soak): a page of no grid,
    which Python holds (``ll.Page`` starts as one), is a page the open page's commands make too -- the keeper keeps it,
    as DataCube's reader takes one written live (headless pageWritten: a restore), never one saved."""

    def test_a_page_with_no_grid_written_there_is_pythons(self):
        page, _ = self.built(0)
        tab = ll.show(page, browser=False)
        try:
            open_page = OpenPage(page)
            for grid in [v['id'] for v in open_page.doc['views'] if v['kind'] == 'grid']:
                open_page.doc, facts, entry = open_page.made(open_page.doc, {'page': {'kind': 'removeTile', 'tile': grid}})
                open_page.pending.append((entry, 'removeTile', facts))
            open_page.write()
            self.assertEqual(page.grids, [])
        finally:
            tab.close()


class Converges(Converged):
    """A page Python edits and an open page edits, interleaved at random: one page, every edit's intent on it."""

    def test_python_and_an_open_page_edit_one_page_in_any_order_and_never_hold_two(self):
        made = {k: 0 for k in PYTHON_KINDS}
        made_there = {}
        for seed in range(SEEDS):
            rng = random.Random(seed)
            page, frame = self.built(seed)
            tab = ll.show(page, browser=False)
            open_page = OpenPage(page)
            intent = Intent(page.to_dict())
            for step in range(STEPS):
                where = f'seed {seed}, step {step}'
                who = rng.randrange(4)
                if who == 0:
                    # Python alone
                    kind, said = python_edit(page, rng, frame)
                    if isinstance(said, list):
                        made[kind] += 1
                        for fact in said:
                            intent.said(fact)
                    elif said is not None:
                        self.assertNotIn(CONFLICT, str(said), f'{where}: {kind} refused with nothing in its way')
                elif who == 1:
                    # the open page alone
                    theirs = open_page.edit(rng)
                    facts, refused = open_page.write()
                    self.assertEqual(refused, [], f'{where}: nothing moved the page: {theirs} let go')
                    if theirs:
                        made_there[theirs] = made_there.get(theirs, 0) + 1
                    for fact in facts:
                        intent.said(fact)
                elif who == 2:
                    # the open page writes while a Python call is under way: Python's change made again on top of it
                    page._enter()
                    try:
                        theirs = open_page.edit(rng)
                        facts, _ = open_page.write()
                        kind, said = python_edit(page, rng, frame)
                    finally:
                        page._leave(failed=False)
                    for fact in facts:
                        intent.said(fact)
                    if theirs:
                        made_there[theirs] = made_there.get(theirs, 0) + 1
                    if isinstance(said, list):
                        made[kind] += 1
                        for fact in said:
                            intent.said(fact)
                    elif said is not None and CONFLICT in str(said):
                        # refused only for a real conflict: the tile taken off there (an id given out on both sides is
                        # given out afresh, never refused)
                        self.assertEqual(theirs, 'removeTile', f'{where}: {kind} refused for no conflict: {said}')
                else:
                    # Python writes while the open page holds an edit not yet written: refused, the open page follows
                    # and makes its edit again on top
                    theirs = open_page.edit(rng)
                    kind, said = python_edit(page, rng, frame)
                    if isinstance(said, list):
                        made[kind] += 1
                        for fact in said:
                            intent.said(fact)
                    elif said is not None:
                        self.assertNotIn(CONFLICT, str(said), f'{where}: {kind} refused with nothing written in its way')
                    facts, refused = open_page.write()
                    if refused:
                        # let go only where Python took off what it edited
                        self.assertEqual(kind, 'remove', f'{where}: {theirs} let go for no conflict after {kind}')
                    elif theirs:
                        made_there[theirs] = made_there.get(theirs, 0) + 1
                    for fact in facts:
                        intent.said(fact)
                self.one(page, open_page, intent, where)
            tab.close()
        # every kind of edit went through somewhere, on each side: none of them is never tried
        self.assertEqual([k for k, n in made.items() if n == 0], [], f'Python edits made: {made}')
        there = ('renamePage', 'addSheet', 'renameSheet', 'moveSheet', 'renameTile', 'removeTile', 'group', 'addSort', 'pivot',
                 'click', 'open', 'collapseAll', 'level')
        self.assertEqual([k for k in there if not made_there.get(k)], [], f'open page edits made: {made_there}')


class Threads(Converged):
    """Python's calls and the open page's writes on two threads at once, as they come (Python's own thread, the engine's
    HTTP thread): no deadlock, one page, every edit written on it in its version's order."""

    def test_python_and_an_open_page_on_two_threads_never_hold_two_pages_or_lose_an_edit(self):
        page, frame = self.built(0)
        tab = ll.show(page, browser=False)
        start = page.to_dict()
        written, failures = [], []
        open_page = OpenPage(page)

        def there():
            rng = random.Random(1)
            try:
                for _ in range(40):
                    if open_page.edit(rng) is None:
                        continue
                    facts, _ = open_page.write()
                    written.append((open_page.written, 1, facts))
            except BaseException as e:  # noqa: BLE001 -- said by the main thread
                failures.append(e)

        worker = threading.Thread(target=there, daemon=True)
        worker.start()
        rng = random.Random(2)
        for n in range(40):
            kind, said = python_edit(page, rng, frame)
            if isinstance(said, list):
                written.append((page._version, 0, said))
        worker.join(timeout=600)
        self.assertFalse(worker.is_alive(), 'the open page\'s thread never finished: a deadlock')
        self.assertEqual(failures, [])
        # the two met: each wrote, and the open page was refused for Python's writes at least once (else this proved
        # nothing about two threads)
        self.assertGreaterEqual(sum(1 for w in written if w[1] == 0), 10, 'Python wrote')
        self.assertGreaterEqual(sum(1 for w in written if w[1] == 1), 10, 'the open page wrote')
        self.assertGreater(open_page.combined, 0, 'no write of the open page met one of Python\'s')
        kept = datacube._session.keeper.current(page._key)
        self.assertEqual(_json.dumps(page.to_dict()), _json.dumps(kept[1]), 'Python holds the keeper\'s')
        intent = Intent(start)
        for _, _, facts in sorted(written, key=lambda w: (w[0], w[1])):
            for fact in facts:
                intent.said(fact)
        intent.check(self, kept[1], 'two threads')
        self.copies_read('two threads')
        tab.close()
