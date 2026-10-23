"""Into a project (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B4; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D4, Z4-Z7): a
page's grids made classes, an association and a data space by DataCube's own model actions run headless (headless/
model.ts: the windows' plans over the TypeScript library model-changes/), into this user's model home -- the native
library's SDLC and model writer answering DataCube's fetch (legend_lite._headless.answer_fetch) -- the page's model kept
by the commands DataCube answers. Python keeps none of it.

The library's calls, as this host answers them, are model-changes' cases (model-changes/test/cases, replayed by
model-changes/test/replay.test.ts): ``MODEL_CHANGES_RECORD=1 bazel run //python:model_test`` records them again."""

import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

import pyarrow as pa

import legend_lite as ll
from legend_lite import _headless, _json, datacube
from legend_lite._home import home_call, model_home_folder

# where DataCube's model actions reach this host's own SDLC and writer (headless/model.ts reached)
_SDLC = '/sdlc/api'
_WRITER = '/api/lite/v1/model/'

TRADES = pa.table({'desk': ['FX', 'EQ', 'FX'], 'qty': [1, 2, 4]})
DESKS = pa.table({'desk': ['FX', 'EQ'], 'head': ['Ana', 'Ben']})


class Fetches:
    """Every fetch DataCube's code makes through this host while it is in place (``with Fetches() as f``), each as the
    host answered it -- answer_fetch, wrapped by the test alone -- the SDLC's by its path under the API's root, the
    writer's by its route: the calls a model action makes, in order. ``before_save``: run once, just before the first
    save is asked (another save meanwhile)."""

    def __init__(self, before_save=None):
        self.calls: list[dict] = []
        self.before_save = before_save
        self._answer = _headless.answer_fetch

    def __enter__(self):
        self._patch = mock.patch.object(_headless, 'answer_fetch', self)
        self._patch.start()
        return self

    def __exit__(self, *_exc):
        self._patch.stop()

    def __call__(self, method, url, body):
        if url.startswith(_SDLC + '/'):
            path = url[len(_SDLC):]
            if self.before_save is not None and method == 'POST' and path.endswith('/pureChanges'):
                hook, self.before_save = self.before_save, None
                hook()
            status, text = self._answer(method, url, body)
            self.calls.append({'to': 'sdlc', 'method': method, 'path': path, 'body': body, 'status': status,
                               'answer': text})
            return status, text
        status, text = self._answer(method, url, body)
        if url.startswith(_WRITER):
            self.calls.append({'to': 'writer', 'route': url[len(_WRITER):], 'body': body, 'answer': text})
        return status, text

    def written(self, route):
        """The bodies the writer was sent on ``route``, read as the library reads JSON (a decimal keeps its digits)."""
        return [_json.loads(c['body']) for c in self.calls if c['to'] == 'writer' and c['route'] == route]


def _enc(part: str) -> str:
    """A path segment as encodeURIComponent writes it, as the SDLC client asks."""
    return urllib.parse.quote(part, safe="-_.!~*'()")


def _sdlc(method: str, path: str, body=None, folder: Path | None = None):
    """One call of a model home's SDLC (this user's, unless ``folder``), its answer read: a refusal fails the test."""
    status, text = home_call(folder or model_home_folder(), 'sdlc', method, path,
                             None if body is None else json.dumps(body))
    assert 200 <= status < 300, f'{method} {path}: {status} {text}'
    return json.loads(text) if text else None


def _files(ref: dict, folder: Path | None = None) -> dict[str, str]:
    """The page's project's files at its workspace's current revision, by path."""
    at = f'/projects/{_enc(ref["project"])}/workspaces/{_enc(ref["workspace"])}'
    revision = _sdlc('GET', f'{at}/revisions/CURRENT', folder=folder)['id']
    return {f['path']: f['pureCode'] for f in _sdlc('GET', f'{at}/revisions/{_enc(revision)}/pure', folder=folder)}


def _own_home(test: unittest.TestCase) -> Path:
    """This user's legend-lite folder, the test's own while it runs."""
    home = tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))
    before = os.environ.get('LEGEND_LITE_HOME')
    os.environ['LEGEND_LITE_HOME'] = home

    def restore():
        if before is None:
            os.environ.pop('LEGEND_LITE_HOME', None)
        else:
            os.environ['LEGEND_LITE_HOME'] = before
    test.addCleanup(restore)
    return Path(home)


def _page(test: unittest.TestCase | None = None) -> 'll.Page':
    """A page of two grids, as a person builds one: trades, and desks -- their frames let go after ``test``."""
    page = ll.Page('Trades by desk')
    page.grid(TRADES, name='model_trades')
    page.grid(DESKS, name='model_desks')
    if test is not None:
        for frame in ('model_trades', 'model_desks'):
            test.addCleanup(datacube._session.unregister, frame)
    return page


# -- model-changes' cases, recorded ------------------------------------------------------------------------------------

def _action(cls: dict) -> dict:
    """A class action as the writer's body carries it, read back as the library takes it (model-changes' ClassAction):
    its relation, its source's model and runtime, its names and choices, and where it reads."""
    where = dict(cls['where'])
    runtime = where.pop('runtime')
    rest = {k: v for k, v in cls.items() if k not in ('lambda', 'source', 'where')}
    return {'query': cls['lambda'], 'source': {'model': cls['source']['code'], 'runtime': runtime}, **rest,
            'where': where}


def _case(name: str, fetches: Fetches, page: 'll.Page', before: dict | None, **flags) -> dict:
    """A case of model-changes': what the library was asked (read back from the body the writer was last sent: a save
    made again sends it twice), the calls it made, and where it saved -- the page's model after."""
    route = next(c['route'] for c in reversed(fetches.calls) if c['to'] == 'writer')
    body = fetches.written(route)[-1]
    if before is None:
        made = next(json.loads(c['body']) for c in fetches.calls
                    if c['to'] == 'sdlc' and (c['method'], c['path']) == ('POST', '/projects'))
        project = {'name': made['name'], 'group': made['groupId'], 'description': made['description']}
    else:
        project = before['project']
    ref = page.model
    case = {'name': name, 'function': route, 'project': project, 'workspace': ref['workspace'],
            'package': body['package']}
    if route == 'createClass':
        case['class'] = _action(body['class'])
    else:
        if body['classes']:
            case['classes'] = [_action(c) for c in body['classes']]
        if route == 'createAssociation':
            case['association'] = body['association']
        else:
            if body['associations']:
                case['associations'] = body['associations']
            case['dataSpace'] = body['dataSpace']
    return {**case, **flags, 'calls': fetches.calls,
            'result': {'project': ref['project'], 'workspace': ref['workspace'], 'revision': ref['revision']}}


def record(into: Path) -> None:
    """The cases, recorded from a page's model actions in a fresh model home (this user's: the caller's own), each call
    the library makes as this host answered it."""
    page = _page()
    trades, desks = page.grids
    cases = []
    with Fetches() as f:
        trade = trades.create_class('Trade', columns={'qty': {'name': 'quantity'}})
    cases.append(_case('a class into a project made for it', f, page, None))
    before = page.model
    at = f'/projects/{_enc(before["project"])}/workspaces/{_enc(before["workspace"])}'

    def another_save():
        _sdlc('POST', f'{at}/pureChanges', {
            'message': 'another save', 'revisionId': _sdlc('GET', f'{at}/revisions/CURRENT')['id'],
            'changes': [{'type': 'CREATE', 'path': 'tradesByDesk::Note', 'pureCode': 'Class tradesByDesk::Note {}'}]})
    with Fetches(before_save=another_save) as f:
        desk = desks.create_class('Desk', columns={'desk': 'key'})
    cases.append(_case("a second class, the workspace moved by another save before this one's: made again once", f,
                       page, before, moved=True))
    before = page.model
    with Fetches() as f:
        page.create_association({'class': trade, 'on': 'desk', 'name': 'trades', 'unique': False},
                                {'class': desk, 'on': 'desk', 'name': 'theDesk', 'unique': True}, name='TradeDesk')
    cases.append(_case('an association between the two', f, page, before))
    desks_alone = ll.Page('Desks')
    desks_alone.grid(DESKS, name='model_desks_alone')
    with Fetches() as f:
        desks_alone.create_data_space(title='The desks')
    cases.append(_case('a data space over the project, a class made with it, into a new project', f, desks_alone,
                       None))
    # a class made again as it is: the writer answers no change, so nothing is saved, the revision the one read
    before = page.model
    with Fetches() as f:
        trades.create_class('Trade', columns={'qty': {'name': 'quantity'}})
    cases.append(_case('the first class made again as it is: nothing saved, the revision the one read', f, page,
                       before, unchanged=True))
    into.mkdir(parents=True, exist_ok=True)
    for old in into.glob('*.json'):
        old.unlink()
    for n, case in enumerate(cases, start=1):
        (into / f'{n:02d}.json').write_text(_json.dumps(case, indent=1) + '\n', encoding='utf-8')


class Record(unittest.TestCase):
    """The cases recorded again, when asked (``MODEL_CHANGES_RECORD=1 bazel run //python:model_test``)."""

    def test_record(self):
        if not os.environ.get('MODEL_CHANGES_RECORD'):
            self.skipTest('recorded only when asked: MODEL_CHANGES_RECORD=1 bazel run //python:model_test')
        _own_home(self)
        for frame in ('model_trades', 'model_desks', 'model_desks_alone'):
            self.addCleanup(datacube._session.unregister, frame)
        record(Path(os.environ['BUILD_WORKSPACE_DIRECTORY']) / 'model-changes' / 'test' / 'cases')


# -- the host's fetch --------------------------------------------------------------------------------------------------

class TheHostsFetch(unittest.TestCase):
    """What DataCube's code fetches headless, answered by this process (answer_fetch): its model home and legend-lite's
    API at a path of its own, a server at its address; anything else refused in words, never raised into V8."""

    def setUp(self):
        self.home = _own_home(self)

    def test_its_model_home_at_its_sdlcs_path(self):
        self.assertEqual(_headless.answer_fetch('GET', '/sdlc/api/projects', None), (200, '[]'))
        self.assertTrue((self.home / 'projects').is_dir(), "the user's model home, made where their folder says")

    def test_legend_lites_writer_at_its_apis_path_its_refusal_in_its_words(self):
        status, text = _headless.answer_fetch('POST', '/api/lite/v1/model/createClass', '{}')
        self.assertNotEqual(status, 200)
        self.assertIsInstance(json.loads(text)['message'], str)

    def test_another_path_or_kind_of_address_is_refused_in_words(self):
        status, text = _headless.answer_fetch('GET', '/elsewhere', None)
        self.assertEqual(status, 404)
        self.assertIn('/sdlc/api/', json.loads(text)['message'])
        status, text = _headless.answer_fetch('GET', 'ftp://somewhere/sdlc/api/projects', None)
        self.assertEqual(status, 400)
        self.assertIn('over HTTP', json.loads(text)['message'])

    def test_a_server_is_asked_over_http(self):
        # another model home, served by a server of the test's own: the page's project made there, not in this user's
        elsewhere = Path(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))) / 'projects'
        asked = []

        class Sdlc(BaseHTTPRequestHandler):
            def _answer(self):
                length = int(self.headers.get('Content-Length') or 0)
                body = self.rfile.read(length).decode('utf-8') if length else None
                asked.append((self.command, self.path))
                status, text = home_call(elsewhere, 'sdlc', self.command, self.path[len('/sdlc/api'):], body)
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(text.encode('utf-8'))
            do_GET = do_POST = _answer

            def log_message(self, *_args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Sdlc)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        page = _page(self)
        path = page.grids[0].create_class('Trade', sdlc=f'http://127.0.0.1:{server.server_port}/sdlc/api')
        self.assertEqual(path, 'tradesByDesk::domain::Trade')
        self.assertIn(('POST', '/sdlc/api/projects'), asked)
        self.assertIn('Class tradesByDesk::domain::Trade', '\n'.join(_files(page.model, elsewhere).values()))
        self.assertEqual(_sdlc('GET', '/projects'), [], "nothing in this user's model home")


class TheRepositorysLock(unittest.TestCase):
    """The model home's repository holds its own lock (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md G11): every call of the
    native library's SDLC is made under it, so another kind of process that opens the repository -- the model home's
    server -- and this one never write it at once. Here another process holds the operating system's lock on the
    repository's lock file, and this process's call waits for it."""

    @unittest.skipIf(sys.platform == 'win32', 'the other process takes a POSIX record lock, as the JVM does here')
    def test_a_call_waits_while_another_process_holds_the_repository(self):
        import subprocess
        import time
        folder = Path(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))) / 'projects'
        _sdlc('GET', '/projects', folder=folder)  # the repository made
        holder = subprocess.Popen([sys.executable, '-c', (
            'import fcntl, sys, time\n'
            f'f = open({str(folder / "legend" / "lock")!r}, "a+b")\n'
            'fcntl.lockf(f, fcntl.LOCK_EX)\n'
            'print("held", flush=True)\n'
            'time.sleep(1.0)\n')], stdout=subprocess.PIPE, text=True)
        self.addCleanup(holder.wait)
        self.assertEqual(holder.stdout.readline().strip(), 'held')
        started = time.monotonic()
        _sdlc('GET', '/projects', folder=folder)
        self.assertGreater(time.monotonic() - started, 0.5, 'the call did not wait for the other process')


# -- on the page -------------------------------------------------------------------------------------------------------

class OnThePage(unittest.TestCase):
    """A page's grids made classes, an association and a data space, by DataCube's model actions run headless, into
    this user's model home (the test's own folder): what Studio would open."""

    def setUp(self):
        _own_home(self)

    def test_a_grid_made_a_class_then_an_association_and_a_data_space(self):
        page = _page(self)
        trades, desks = page.grids
        self.assertIsNone(page.model)
        trade = trades.create_class('Trade')
        self.assertEqual(trade, 'tradesByDesk::domain::Trade')
        ref = page.model
        self.assertEqual((ref['workspace'], ref['package']), ('datacube', 'tradesByDesk'))
        self.assertTrue(ref['project'].startswith('datacube:trades-by-desk'), ref)
        self.assertEqual(ref['classes'], {trades._view['cube']: trade})
        # pinned to the revision its class was saved as (page-model.ts pin): where its class grids are read
        at = f'/projects/{_enc(ref["project"])}/workspaces/{_enc(ref["workspace"])}'
        self.assertEqual(ref['revision'], _sdlc('GET', f'{at}/revisions/CURRENT')['id'])
        # each change of the page's model a page command, as the screen tells it (page-model.ts PageModel's commands): a
        # page open on it follows the commands, never reads the page whole (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md P4)
        self.assertEqual([c['kind'] for c in page._page_commands[-3:]], ['modelInto', 'modelPin', 'modelKeep'])
        desk = desks.create_class(columns={'desk': 'key'})
        self.assertEqual(desk, 'tradesByDesk::domain::ModelDesk')
        self.assertEqual(page.model['project'], ref['project'], "the page's one project")
        self.assertNotEqual(page.model['revision'], ref['revision'], 'a class made: the page pinned to the new revision')
        # how many of each end: preset from whether its key is unique over its grid's rows, as the window presets it
        # (model-plans.ts keyUnique: read through the class's relation function, planned and run in this process)
        with Fetches() as f:
            association = page.create_association((trade, 'desk'), (desk, 'desk'), name='TradeDesk')
        self.assertEqual(association, 'tradesByDesk::domain::TradeDesk')
        (asked,) = [b for b in f.written('createAssociation') if 'association' in b]
        self.assertEqual([e.get('unique') for e in asked['association']['ends']], [False, True])
        files = _files(page.model)
        self.assertIn('Class tradesByDesk::domain::Trade', '\n'.join(files.values()))
        self.assertIn('Association tradesByDesk::domain::TradeDesk', '\n'.join(files.values()))
        space = page.create_data_space('TradesByDesk')
        self.assertTrue(space.endswith('TradesByDesk'), space)
        self.assertIn('DataSpace', '\n'.join(_files(page.model).values()))

    def test_a_joins_grid_made_a_class_over_both_its_tables(self):
        # its base a query over two frames' tables (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3): DataCube's action
        # names both, with a sample of each, the writer writes both, and the page keeps both under the grid's cube
        page = _page(self)
        trades, desks = page.grids
        joined = page.join(trades, desks, {'form': 'join', 'kind': 'left', 'prefix': 'd_', 'keep': None,
                                           'on': {'rows': [{'left': 'desk', 'op': '==', 'right': 'desk'}]}})
        with Fetches() as f:
            path = joined.create_class('TradeDesk')
        self.assertEqual(path, 'tradesByDesk::domain::TradeDesk')
        samples = f.written('createClass')[-1]['class']['where']['samples']
        self.assertEqual(sorted(samples), ['model_desks', 'model_trades'])
        self.assertEqual(samples['model_desks']['rows'], [['FX', 'Ana'], ['EQ', 'Ben']])
        self.assertEqual(page.model['tables'], {joined._view['cube']: ['model_trades', 'model_desks']})
        files = '\n'.join(_files(page.model).values())
        self.assertIn('Model_tradesSample', files)
        self.assertIn('Model_desksSample', files)

    def test_a_grid_removed_after_its_class_takes_its_model_with_it(self):
        """A grid made a class, then removed: its table's name and its class leave the page's model with it, as DataCube
        writes a page (page-document.ts keptFor, asked headless) -- DataCube's reader refuses a page whose model keeps a
        name for a cube it no longer holds, so the page would never open again."""
        page = _page(self)
        trades, desks = page.grids
        gone, kept = trades._view['cube'], desks._view['cube']
        trades.create_class('Trade')
        desks.create_class(columns={'desk': 'key'})
        self.assertEqual(set(page.model['tables']), {gone, kept})
        page.remove(trades)
        self.assertEqual(set(page.model['tables']), {kept})
        self.assertEqual(set(page.model['classes']), {kept})

    def test_a_class_sample_is_its_first_rows_as_datacube_writes_them(self):
        # DataCube's own sample (sample-rows.ts sampleOf, run by the action headless), each value as its executor gives
        # it: an integer past 2**53 as text, a decimal its digits at its scale, a date and a timestamp as text, a double
        # as JavaScript writes it (a whole one an integer)
        import datetime
        from decimal import Decimal
        page = ll.Page('Sampled')
        grid = page.grid(pa.table({
            'desk': ['FX', 'EQ'], 'qty': [1, 2], 'big': [2 ** 60, 3], 'notional': [1.5, 2.0],
            'dec': pa.array([Decimal('12.30'), Decimal('1.05')], pa.decimal128(10, 2)),
            'day': [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
            'at': [datetime.datetime(2024, 1, 1, 9, 30, 0, 120000), datetime.datetime(2024, 1, 2, 10, 0)],
            'flag': [True, None]}), name='sampled')
        self.addCleanup(datacube._session.unregister, 'sampled')
        with Fetches() as f:
            grid.create_class('Sampled')
        self.assertEqual(f.written('createClass')[-1]['class']['where']['samples'], {'sampled': {
            'columns': ['desk', 'qty', 'big', 'notional', 'dec', 'day', 'at', 'flag'],
            'rows': [['FX', 1, str(2 ** 60), Decimal('1.5'), '12.30', '2024-01-01', '2024-01-01T09:30:00.12', True],
                     ['EQ', 2, 3, 2, '1.05', '2024-01-02', '2024-01-02T10:00:00', None]]}})

    def test_a_sample_holding_a_nan_or_an_infinity_is_refused_never_written_as_null(self):
        # the sample goes to the route as JSON, which has no NaN or infinity: refused by DataCube's sampleOf, naming the
        # column and the row, before anything is written (it wrote null, a value the data never had)
        page = ll.Page('Odd values')
        nan = page.grid(pa.table({'desk': ['FX', 'EQ'], 'rate': pa.array([1.5, float('nan')])}), name='odd_nan')
        with self.assertRaisesRegex(ValueError, r"the sample's rate in row \d is NaN, which a class's data cannot hold"):
            nan.create_class()
        infinite = page.grid(pa.table({'desk': ['FX'], 'rate': pa.array([float('-inf')])}), name='odd_infinite')
        with self.assertRaisesRegex(ValueError, r"the sample's rate in row 1 is -Infinity"):
            infinite.create_class('Odd')
        # the whole page, as the window makes it: refused, as the window refuses it (model-actions.ts createDataSpaceOf),
        # never made without the grids whose sample it cannot carry
        page.grid(pa.table({'desk': ['FX'], 'qty': [1]}), name='fine')
        with self.assertRaisesRegex(ValueError, r"the sample's rate in row \d is NaN"):
            page.create_data_space()
        self.assertEqual(page.model, None, 'nothing written')

    def test_the_project_made_is_named_as_asked(self):
        page = _page(self)
        page.grids[0].create_class('Trade', project='Desk book')
        self.assertTrue(page.model['project'].startswith('datacube:desk-book'), page.model)

    def test_a_column_role_the_writer_does_not_know_is_refused_in_its_words(self):
        # Python keeps no list of the roles: the writer judges them (CreateClass asOf). The project made before it stays
        # the page's (a project made stays the caller's when a later step fails); no class is written or kept
        page = _page(self)
        with self.assertRaisesRegex(ValueError, r"^legend-lite's createClass: .*the column 'desk' is asked to be 'primary': "
                                                r'a property, a key or omit'):
            page.grids[0].create_class('Trade', columns={'desk': 'primary'})
        self.assertEqual(page.model['tables'], {})
        self.assertNotIn('Class ', '\n'.join(_files(page.model).values()))

    def test_what_a_model_action_is_given_is_read_by_datacube_in_its_words(self):
        page = _page(self)
        with self.assertRaisesRegex(ValueError, "a class's columns are its choices by column, not"):
            page.grids[0].create_class('Trade', columns=['desk'])
        self.assertIsNone(page.model, 'refused before anything was asked')
        page.grids[0].create_class('Trade')
        with self.assertRaisesRegex(ValueError, "an association's end names its class and what it is joined on"):
            page.create_association(('a::A',), ('b::B', 'y'))
        self.assertEqual(len(page.model['classes']), 1, 'the class made; the association refused before it was asked')

    def test_an_association_before_any_class_is_refused_in_words(self):
        with self.assertRaisesRegex(ValueError, 'make them first'):
            _page(self).create_association(('a::A', 'x'), ('b::B', 'y'))

    def test_a_whole_page_made_in_one_go(self):
        page = _page(self)
        space = page.create_data_space()
        self.assertEqual(space, 'tradesByDesk::TradesByDesk')
        self.assertEqual(sorted(page.model['classes'].values()),
                         ['tradesByDesk::domain::ModelDesk', 'tradesByDesk::domain::ModelTrade'])



class AtTheEnd(unittest.TestCase):
    """A process that made DataCube's engine ends as it should: the engine is never kept alive by what it answers
    DataCube's calls through (a weak method of its session's), so it is never left in a cycle with its session to be
    collected as the interpreter ends -- where tearing V8 down crashed the process after every test had passed."""

    def test_a_process_that_made_datacubes_engine_ends_cleanly(self):
        import subprocess
        code = 'from legend_lite import datacube\ndatacube._session.headless()\nprint("made")\n'
        done = subprocess.run([sys.executable, '-c', code], env={**os.environ, 'PYTHONPATH': os.pathsep.join(sys.path)},
                              capture_output=True, text=True, timeout=300)
        self.assertEqual((done.returncode, done.stdout.strip()), (0, 'made'), done.stderr[-2000:])

if __name__ == '__main__':
    unittest.main()
