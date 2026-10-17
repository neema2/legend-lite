"""Into a project (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B4; legend_lite/model.py): the twin of the TypeScript library
model-changes/, held to the same cases (model-changes/test/cases: every call one create makes, to the SDLC and to the
writer, its exact body and the answer it was given -- the TypeScript library replays them too, model-changes/test/
replay.test.ts), then end to end: a page's grids made classes, an association and a data space, into this user's
model home, by the native library's SDLC and writer, the page's model kept.

The cases are recorded here, against the native library, from a page's grids as Python asks DataCube for them
(``Grid._class``): ``MODEL_CHANGES_RECORD=1 bazel run //python:model_test`` writes them into model-changes/test/cases."""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa

import legend_lite as ll
from legend_lite import _json, datacube, model

CASES = sorted(os.environ['LEGEND_LITE_MODEL_CASES'].split(os.pathsep)) if os.environ.get('LEGEND_LITE_MODEL_CASES') else []


class Transcript:
    """An SDLC and a writer whose every call is recorded (``record``) or answered from a case (``replay``): the same
    calls, in order, with the same bodies."""

    def __init__(self, calls: list[dict] | None = None, sdlc: model.Sdlc | None = None) -> None:
        self.calls: list[dict] = [] if calls is None else calls
        self.replaying = calls is not None
        self._real = sdlc
        self._next = 0
        self.before_save = None

    def _expected(self, to: str) -> dict:
        assert self._next < len(self.calls), f'a call more than the case has: the {to}'
        call = self.calls[self._next]
        self._next += 1
        assert call['to'] == to, f'call {self._next}: the case has a call to the {call["to"]}, this one is to the {to}'
        return call

    def sdlc(self) -> model.Sdlc:
        def call(method: str, path: str, body: str | None) -> tuple[int, str]:
            if self.replaying:
                expected = self._expected('sdlc')
                assert (method, path, body) == (expected['method'], expected['path'], expected['body']), \
                    f'call {self._next}: {method} {path} {body!r}, the case has {expected["method"]} {expected["path"]} ' \
                    f'{expected["body"]!r}'
                return expected['status'], expected['answer']
            if self.before_save is not None and method == 'POST' and path.endswith('/pureChanges'):
                hook, self.before_save = self.before_save, None
                hook()
            status, text = self._real._call(method, path, body)
            self.calls.append({'to': 'sdlc', 'method': method, 'path': path, 'body': body, 'status': status, 'answer': text})
            return status, text
        return model.Sdlc(call)

    def writer(self) -> model.ModelWriter:
        real = model.local_writer()

        def post(route: str, body: str) -> str:
            if self.replaying:
                expected = self._expected('writer')
                assert (route, body) == (expected['route'], expected['body']), \
                    f'call {self._next}: the writer\'s {route} with a body not the case\'s'
                return expected['answer']
            text = real._post(route, body)
            self.calls.append({'to': 'writer', 'route': route, 'body': body, 'answer': text})
            return text
        return model._Writer(post)

    def done(self) -> None:
        assert self._next == len(self.calls), f'{len(self.calls) - self._next} calls of the case were not made'


def _page(test: unittest.TestCase | None = None) -> 'll.Page':
    """A page of two grids, as a person builds one: trades, and desks -- their frames let go after ``test``."""
    page = ll.Page('Trades by desk')
    page.grid(pa.table({'desk': ['FX', 'EQ', 'FX'], 'qty': [1, 2, 4]}), name='model_trades')
    page.grid(pa.table({'desk': ['FX', 'EQ'], 'head': ['Ana', 'Ben']}), name='model_desks')
    if test is not None:
        for frame in ('model_trades', 'model_desks'):
            test.addCleanup(datacube._session.unregister, frame)
    return page


def _run(case: dict, sdlc: model.Sdlc, writer: model.ModelWriter) -> model.Created:
    project = case['project'] if isinstance(case['project'], str) else model.NewProject(**case['project'])
    if case['function'] == 'createClass':
        return model.create_class(sdlc, writer, project, case['workspace'], case['package'], case['class'])
    if case['function'] == 'createAssociation':
        return model.create_association(sdlc, writer, project, case['workspace'], case['package'], case['association'],
                                        case.get('classes', []))
    return model.create_data_space(sdlc, writer, project, case['workspace'], case['package'], case['dataSpace'],
                                   case.get('classes', []), case.get('associations', []))


def record(into: Path) -> None:
    """The cases, recorded against the native library's SDLC (a fresh repository) and writer, from a page's grids."""
    folder = Path(tempfile.mkdtemp())
    real = model.local_sdlc(folder)
    page = _page()
    trades, desks = page.grids
    trade, _ = trades._class('Trade', {'qty': {'name': 'quantity'}})
    desk, _ = desks._class('Desk', {'desk': 'key'})
    cases = [
        {'name': 'a class into a project made for it', 'function': 'createClass',
         'project': {'name': 'Trades by desk', 'group': 'datacube', 'description': 'The classes of a DataCube page'},
         'workspace': 'datacube', 'package': 'tradesByDesk', 'class': trade},
    ]
    made = _recorded(cases[0], real)
    project = made.project
    cases.append({'name': "a second class, the workspace moved by another save before this one's: made again once",
                  'function': 'createClass', 'project': project, 'workspace': 'datacube', 'package': 'tradesByDesk',
                  'class': desk, 'moved': True})
    _recorded(cases[1], real, moved=lambda: real.perform_pure_changes(project, 'datacube', {
        'message': 'another save', 'changes': [{'type': 'CREATE', 'path': 'tradesByDesk::Note', 'pureCode': 'Class tradesByDesk::Note {}'}],
        'revisionId': real.revision(project, 'datacube')['id']}))
    cases.append({'name': 'an association between the two', 'function': 'createAssociation', 'project': project,
                  'workspace': 'datacube', 'package': 'tradesByDesk',
                  'association': {'name': 'TradeDesk', 'ends': [
                      {'class': 'tradesByDesk::domain::Trade', 'on': 'desk', 'name': 'trades', 'unique': False},
                      {'class': 'tradesByDesk::domain::Desk', 'on': 'desk', 'name': 'theDesk', 'unique': True}]}})
    _recorded(cases[2], real)
    cases.append({'name': 'a data space over the project, a class made with it, into a new project', 'function': 'createDataSpace',
                  'project': {'name': 'Desks', 'group': 'datacube', 'description': ''}, 'workspace': 'datacube',
                  'package': 'desks', 'dataSpace': {'name': 'Desks', 'title': 'The desks'}, 'classes': [desk]})
    _recorded(cases[3], real)
    # a class made again as it is: the writer answers no change, so nothing is saved, the revision the one read
    cases.append({'name': 'the first class made again as it is: nothing saved, the revision the one read',
                  'function': 'createClass', 'project': project, 'workspace': 'datacube', 'package': 'tradesByDesk',
                  'class': trade, 'unchanged': True})
    _recorded(cases[4], real)
    into.mkdir(parents=True, exist_ok=True)
    for old in into.glob('*.json'):
        old.unlink()
    for n, case in enumerate(cases, start=1):
        (into / f'{n:02d}.json').write_text(_json.dumps(case, indent=1) + '\n', encoding='utf-8')


def _recorded(case: dict, real: model.Sdlc, moved=None) -> model.Created:
    transcript = Transcript(sdlc=real)
    transcript.before_save = moved
    made = _run(case, transcript.sdlc(), transcript.writer())
    case['calls'] = transcript.calls
    case['result'] = {'project': made.project, 'workspace': made.workspace, 'revision': made.revision}
    return made


class Replayed(unittest.TestCase):
    """Each case replayed: Python makes the same calls, in order, with the same bodies, and answers the same."""

    def test_the_cases_are_there(self):
        self.assertGreaterEqual(len(CASES), 5)

    def test_each_case(self):
        for path in CASES:
            # read as the library reads JSON: a decimal keeps its digits
            case = _json.loads(Path(path).read_text(encoding='utf-8'))
            with self.subTest(case=case['name']):
                transcript = Transcript(case['calls'])
                made = _run(case, transcript.sdlc(), transcript.writer())
                transcript.done()
                self.assertEqual({'project': made.project, 'workspace': made.workspace, 'revision': made.revision},
                                 case['result'])
                saves = [c['status'] for c in case['calls'] if c['to'] == 'sdlc' and c['path'].endswith('/pureChanges')]
                if case.get('moved'):
                    self.assertEqual(saves, [409, 200], 'stale once, then made again and saved')
                if case.get('unchanged'):
                    self.assertEqual(saves, [], 'no change, so no save')
                    reads = [c for c in case['calls'] if c['to'] == 'sdlc' and c['path'].endswith('/revisions/CURRENT')]
                    self.assertEqual(made.revision, json.loads(reads[-1]['answer'])['id'], 'the revision the one read')


class Unrecorded(unittest.TestCase):
    """What the cases cannot record from the native library: answers a race makes, said the same as the TypeScript
    library says them (model-changes/test/replay.test.ts)."""

    def test_a_workspace_made_by_another_ask_meanwhile_is_there(self):
        # lite's SDLC refuses a workspace that has commits already with a 500 ("Branch already exists"): read again
        answers = iter([(404, '{"message": "no such workspace"}'), (500, '{"message": "Branch already exists"}'),
                        (200, '{"workspaceId": "w"}')])
        model.ensure_workspace(model.Sdlc(lambda m, p, b: next(answers)), 'g:a', 'w')

    def test_a_workspace_refused_and_still_not_there_is_said(self):
        answers = iter([(404, '{}'), (409, '{"message": "a workspace W differs only in case"}'), (404, '{}')])
        with self.assertRaisesRegex(model.SdlcError, 'differs only in case'):
            model.ensure_workspace(model.Sdlc(lambda m, p, b: next(answers)), 'g:a', 'w')

    def test_a_save_that_changes_nothing_answers_the_revision_read(self):
        # every change the bytes already there: the SDLC's 204, no body (another tab saved the same meanwhile)
        answers = iter([(200, '{}'), (200, '{"id": "r1"}'), (200, '[]'), (204, '')])
        writer = type('W', (), {'create_class': lambda self, m, p, c: {'changes': [{'type': 'MODIFY', 'path': 'a::A',
                                                                                    'pureCode': 'Class a::A {}'}]}})()
        made = model.create_class(model.Sdlc(lambda m, p, b: next(answers)), writer, 'g:a', 'w', 'a', {'name': 'A'})
        self.assertEqual(made.revision, 'r1')

    def test_a_project_made_stays_the_callers_when_a_later_step_fails(self):
        seen = []

        def call(method, path, body):
            if (method, path) == ('GET', '/projects'):
                return 200, '[]'
            if (method, path) == ('POST', '/projects'):
                return 200, '{"projectId": "datacube:trades"}'
            return 500, '{"message": "busy"}'
        with self.assertRaisesRegex(model.SdlcError, 'busy'):
            model.create_class(model.Sdlc(call), None, model.NewProject('Trades', 'datacube'), 'datacube', 'trades',
                               {'name': 'Trade'}, made=seen.append)
        self.assertEqual(seen, ['datacube:trades'])


class TheRepositorysLock(unittest.TestCase):
    """The model home's repository holds its own lock (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md G11): every call of the
    native library's SDLC is made under it, so another kind of process that opens the repository -- the model home's
    server -- and this one never write it at once. Here another process holds the operating system's lock on the
    repository's lock file, and this process's call waits for it."""

    @unittest.skipIf(sys.platform == 'win32', 'the other process takes a POSIX record lock, as the JVM does here')
    def test_a_call_waits_while_another_process_holds_the_repository(self):
        import subprocess
        import tempfile
        import time
        folder = Path(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))) / 'projects'
        sdlc = model.local_sdlc(folder)
        sdlc.projects()  # the repository made
        holder = subprocess.Popen([sys.executable, '-c', (
            'import fcntl, sys, time\n'
            f'f = open({str(folder / "legend" / "lock")!r}, "a+b")\n'
            'fcntl.lockf(f, fcntl.LOCK_EX)\n'
            'print("held", flush=True)\n'
            'time.sleep(1.0)\n')], stdout=subprocess.PIPE, text=True)
        self.addCleanup(holder.wait)
        self.assertEqual(holder.stdout.readline().strip(), 'held')
        started = time.monotonic()
        sdlc.projects()
        self.assertGreater(time.monotonic() - started, 0.5, 'the call did not wait for the other process')


class OnThePage(unittest.TestCase):
    """A page's grids made classes, an association and a data space, by DataCube's request and the library, into this
    user's model home (the test's own folder): what Studio would open."""

    def setUp(self):
        home = tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))
        self._home = os.environ.get('LEGEND_LITE_HOME')
        os.environ['LEGEND_LITE_HOME'] = home
        self.addCleanup(self._restore)
        self.sdlc = model.local_sdlc()

    def _restore(self):
        if self._home is None:
            os.environ.pop('LEGEND_LITE_HOME', None)
        else:
            os.environ['LEGEND_LITE_HOME'] = self._home

    def files(self, ref):
        revision = self.sdlc.revision(ref['project'], ref['workspace'])['id']
        return {f['path']: f['pureCode'] for f in self.sdlc.pure(ref['project'], ref['workspace'], revision)}

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
        desk = desks.create_class(columns={'desk': 'key'})
        self.assertEqual(desk, 'tradesByDesk::domain::ModelDesk')
        self.assertEqual(page.model['project'], ref['project'], "the page's one project")
        # how many of each end: preset from whether its key is unique over its grid's rows, as the window presets it
        association = page.create_association((trade, 'desk'), (desk, 'desk'), name='TradeDesk')
        self.assertEqual(association, 'tradesByDesk::domain::TradeDesk')
        files = self.files(page.model)
        self.assertIn('Class tradesByDesk::domain::Trade', '\n'.join(files.values()))
        self.assertIn('Association tradesByDesk::domain::TradeDesk', '\n'.join(files.values()))
        space = page.create_data_space('TradesByDesk')
        self.assertTrue(space.endswith('TradesByDesk'), space)
        self.assertIn('DataSpace', '\n'.join(self.files(page.model).values()))

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
        # DataCube's own sample (sample-rows.ts sampleOf, headless gridClass), each value as its executor gives it: an
        # integer past 2**53 as text, a decimal its digits at its scale, a date and a timestamp as text, a double as
        # JavaScript writes it (a whole one an integer)
        import datetime
        from decimal import Decimal
        page = ll.Page('Sampled')
        grid = page.grid(pa.table({
            'desk': ['FX', 'EQ'], 'qty': [1, 2], 'big': [2 ** 60, 3], 'notional': [1.5, 2.0],
            'dec': pa.array([Decimal('12.30'), Decimal('1.05')], pa.decimal128(10, 2)),
            'day': [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
            'at': [datetime.datetime(2024, 1, 1, 9, 30, 0, 120000), datetime.datetime(2024, 1, 2, 10, 0)],
            'flag': [True, None]}), name='sampled')
        cls, asked = grid._class()
        self.assertEqual(cls['where']['samples'][asked['table']['table']], {
            'columns': ['desk', 'qty', 'big', 'notional', 'dec', 'day', 'at', 'flag'],
            'rows': [['FX', 1, str(2 ** 60), Decimal('1.5'), '12.30', '2024-01-01', '2024-01-01T09:30:00.12', True],
                     ['EQ', 2, 3, 2, '1.05', '2024-01-02', '2024-01-02T10:00:00', None]]})

    def test_the_project_made_is_named_as_asked(self):
        page = _page(self)
        page.grids[0].create_class('Trade', project='Desk book')
        self.assertTrue(page.model['project'].startswith('datacube:desk-book'), page.model)

    def test_an_association_before_any_class_is_refused_in_words(self):
        with self.assertRaisesRegex(ValueError, 'make them first'):
            _page(self).create_association(('a::A', 'x'), ('b::B', 'y'))

    def test_a_whole_page_made_in_one_go(self):
        page = _page(self)
        space = page.create_data_space()
        self.assertEqual(space, 'tradesByDesk::TradesByDesk')
        self.assertEqual(sorted(page.model['classes'].values()),
                         ['tradesByDesk::domain::ModelDesk', 'tradesByDesk::domain::ModelTrade'])


class Record(unittest.TestCase):
    """The cases recorded again, when asked (``MODEL_CHANGES_RECORD=1 bazel run //python:model_test``)."""

    def test_record(self):
        if not os.environ.get('MODEL_CHANGES_RECORD'):
            self.skipTest('recorded only when asked: MODEL_CHANGES_RECORD=1 bazel run //python:model_test')
        for frame in ('model_trades', 'model_desks'):
            self.addCleanup(datacube._session.unregister, frame)
        record(Path(os.environ['BUILD_WORKSPACE_DIRECTORY']) / 'model-changes' / 'test' / 'cases')


if __name__ == '__main__':
    unittest.main()
