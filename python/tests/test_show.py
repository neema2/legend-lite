"""show(df) (legend_lite.datacube): DataCube on a dataframe -- a page of one grid over it, kept as every Python page is
(docs/DATACUBE_ONE_API_PLAN_2026_10_21.md P12) -- its link, the cube the engine serves for its frame, a frame shown again
or updated, the page told when the frame changes (a notebook cell's end, update, refresh, a Live frame's new columns,
a close) with the grid's edits kept, closed handles, and a plain script's end: it waits when a cube was opened in a
browser (the engine still answering) until Ctrl-C, and ends at once when none was."""

import json
import os
import queue
import signal
import subprocess
import sys
import tempfile
import threading
import types
import unittest
import urllib.error
import urllib.request
from urllib.parse import parse_qs, urlsplit

import pandas as pd

import legend_lite as ll
from legend_lite import datacube


def trades(rows=3):
    return pd.DataFrame({'desk': ['FX', 'EQ', 'RATES', 'FX'][:rows], 'qty': [10.5, 20.0, 1.0, 7.0][:rows]})


class Shown(unittest.TestCase):
    """show() on the process's one session, with a site of its own and no browser."""

    def setUp(self):
        self._site = os.environ.get('LEGEND_LITE_SITE')
        os.environ['LEGEND_LITE_SITE'] = tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))
        datacube._session = datacube._Session()

    def tearDown(self):
        for cube in list(datacube._session.cubes.values()):
            cube.close()
        # its DataCube engine (headless) closed with it: a session dropped open leaves that engine's folder to the garbage
        # collector, which warns about it under whichever later test it reaches (CorpusPage counts every warning)
        if datacube._session._headless is not None:
            datacube._session._headless.close()
        if self._site is None:
            os.environ.pop('LEGEND_LITE_SITE', None)
        else:
            os.environ['LEGEND_LITE_SITE'] = self._site

    def ask(self, path, web=None):
        """The engine's answer to a GET with its token: status and JSON (or text)."""
        web = web or datacube._session.web()
        request = urllib.request.Request(web.url + path, headers={'Authorization': web.authorization})
        try:
            with urllib.request.urlopen(request, timeout=30) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode('utf-8')

    def version(self, cube):
        """Its page's versions as its tab asks them: the frame's, by its name -- or the status, refused."""
        status, said = self.ask(f'/version.json?page={cube.page._key}')
        return (status, said['frames'][cube.name]) if status == 200 else (status, None)


class Show(Shown):
    def test_show_returns_at_once_with_the_cube_s_link(self):
        cube = ll.show(trades(), browser=False)
        url = urlsplit(cube.url)
        # its page's: a page of one grid over the frame, named by it (P12)
        self.assertEqual((url.path, parse_qs(url.query)), ('/engine.html', {'page': [cube.page._key]}))
        self.assertEqual((cube.page.name, [g.frame for g in cube.page.grids]), ('frame', ['frame']))
        self.assertEqual(url.fragment, 'token=' + datacube._session.web().token)
        status, served = self.ask('/cube.json?table=frame')
        self.assertEqual((status, served['version']), (200, 0))
        self.assertIn('#>{frame::DB.frame}#', ll.print_tree({'_type': 'lambda', 'parameters': [], 'body': [served['source']]}))

    def test_snap_true_reads_the_frame_once_by_its_page_s_grid(self):
        # docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8, P12: its page's grid snapped in Python (its Snap, a copy Python's
        # DuckDB keeps, which the page reads); shown again, still read once until its Go Live
        cube = ll.show(trades(), name='once', browser=False, snap=True)
        copy = cube.page.grids[0].snapped
        self.assertEqual(copy['rowCount'], 3)
        self.assertIs(ll.show(trades(2), name='once', browser=False), cube)
        self.assertEqual(cube.page.grids[0].snapped, copy)
        with self.assertRaisesRegex(ValueError, "a page's grids snap one by one"):
            ll.show(ll.Page('P'), browser=False, snap=True)

    def test_each_frame_its_own_name_and_a_name_shown_again_replaces_its_frame(self):
        first, second = ll.show(trades(), browser=False), ll.show(trades(), browser=False)
        self.assertEqual((first.name, second.name), ('frame', 'frame_2'))
        again = ll.show(trades(2), name='frame', browser=False)
        self.assertIs(again, first)
        self.assertEqual(self.version(first), (200, 1))
        self.assertEqual(datacube._session.frames['frame'].execute("->filter(x|$x.qty > 0)").num_rows, 2)

    def test_update_shows_another_frame_its_page_told(self):
        cube = ll.show(trades(), browser=False)
        cube.update(trades(4))
        self.assertEqual(self.version(cube), (200, 1))
        self.assertEqual(datacube._session.frames['frame'].execute("->filter(x|$x.qty > 0)").num_rows, 4)
        # new columns: a new model, which the page opens the cube again over
        cube.update(trades().assign(book=['a', 'b', 'c']))
        self.assertIn('book', self.ask('/cube.json?table=frame')[1]['model'])

    def test_refresh_moves_the_version_the_page_asks(self):
        cube = ll.show(trades(), browser=False)
        self.assertEqual(self.version(cube), (200, 0))
        cube.refresh()
        self.assertEqual(self.version(cube), (200, 1))

    def test_a_frame_s_new_columns_keep_the_grid_s_edits(self):
        # B10, P12: its page is kept as every Python page is -- the grid opened again over the frame's new columns, as it
        # was edited (here in Python; a tab open on it writes its edits to the same page)
        cube = ll.show(trades(), browser=False)
        grid = cube.page.grids[0]
        grid.group('desk')
        cube.update(trades().assign(book=['a', 'b', 'c']))
        self.assertEqual(grid._query['rows'], ['desk'])
        self.assertIn('book', [c['name'] for c in grid._cube['source']['columns']])

    def test_a_live_frame_s_new_columns_are_a_change_its_page_is_told(self):
        df = trades()
        ll.show(df, browser=False)
        self.ask('/cube.json?table=frame')  # served: its model noted
        df['book'] = 'x'
        status, served = self.ask('/cube.json?table=frame')  # read again: a new model
        self.assertIn('book', served['model'])
        self.assertEqual(self.version(datacube._session.cubes['frame']), (200, 1))

    def test_a_closed_frame_s_page_is_told_and_finds_it_gone(self):
        kept, closed = ll.show(trades(), browser=False), ll.show(trades(), browser=False)
        closed.close()
        status, _ = self.version(closed)
        self.assertEqual(status, 404, 'its page served no more: its tab is told')
        # shown again under the name: a new page of its own
        again = ll.show(trades(), name='frame_2', browser=False)
        self.assertIsNot(again, closed)
        self.assertEqual(self.version(again)[0], 200)
        kept.refresh()

    def test_a_closed_cube_refuses_and_the_last_close_stops_the_web_server(self):
        cube = ll.show(trades(), browser=False)
        url = datacube._session.web().url
        cube.close()
        self.assertNotIn('frame', datacube._session.frames)
        with self.assertRaises(urllib.error.URLError):
            urllib.request.urlopen(url + '/version.json', timeout=5)
        for use in (lambda: cube.refresh(), lambda: cube.update(trades()), lambda: cube.url):
            with self.assertRaises(ValueError):
                use()
        self.assertIsNone(datacube._session._web, 'a closed cube started no web server')


class Notebook(Shown):
    def test_after_each_cell_the_cubes_read_at_each_query_are_told(self):
        registered = []
        shell = types.SimpleNamespace(events=types.SimpleNamespace(register=lambda event, f: registered.append((event, f))))
        was = sys.modules.get('IPython')
        sys.modules['IPython'] = types.SimpleNamespace(get_ipython=lambda: shell)
        try:
            ll.show(trades(), browser=False)
            # a stream, read once into a table of its own: nothing of it changes after a cell
            import pyarrow as pa
            table = pa.Table.from_pandas(trades(), preserve_index=False)
            ll.show(pa.RecordBatchReader.from_batches(table.schema, table.to_batches()), browser=False)
            self.assertEqual([event for event, _ in registered], ['post_run_cell'])
            registered[0][1](None)  # a cell ran
            cubes = datacube._session.cubes
            self.assertEqual(self.version(cubes['frame']), (200, 1))
            self.assertEqual(self.version(cubes['frame_2']), (200, 0))
            datacube._session.opened = True
            self.assertTrue(datacube._stays(), 'a notebook process stays')
            self.assertFalse(datacube._session.waits(), 'nothing waits at its end')
        finally:
            if was is None:
                del sys.modules['IPython']
            else:
                sys.modules['IPython'] = was


# a plain script that shows a frame and ends: what it serves, then (if a browser opened) the wait. Its "browser" is
# webbrowser.open made to answer as a browser would, opened or not -- no browser is started
SCRIPT = r'''
import json, sys, webbrowser
if sys.platform == "win32":
    # a person's console delivers Ctrl-C; a test's process ignores it (Bazel starts a test in a process group of its
    # own) and a child inherits that, so the script takes it back, as a script a person started has it
    import ctypes
    ctypes.windll.kernel32.SetConsoleCtrlHandler(None, False)
import pandas as pd
import legend_lite as ll
from legend_lite import datacube
webbrowser.open = lambda url: sys.argv[1] == "opens"
cube = ll.show(pd.DataFrame({"desk": ["FX", "EQ"], "qty": [1.5, 2.5]}), browser=sys.argv[2] == "browser")
web = datacube._session.web()
print(json.dumps({"url": web.url, "authorization": web.authorization}), flush=True)
'''


# Ctrl-C pressed in a Windows console, as a person presses it: this helper attaches to the script's console, ignores
# the Ctrl-C itself, and sends it to every process there -- the script. Windows has no Ctrl-C to send one process (a
# process group's CTRL_C_EVENT is ignored by the group), and a test has no console of its own to press it in
CTRL_C = r'''
import ctypes, sys
kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
kernel32.FreeConsole()
for call, args in ((kernel32.AttachConsole, (int(sys.argv[1]),)), (kernel32.SetConsoleCtrlHandler, (None, True)),
                   (kernel32.GenerateConsoleCtrlEvent, (0, 0))):
    if not call(*args):
        raise SystemExit(f"{call.__name__}: Windows error {ctypes.get_last_error()}")
'''


class PlainScript(unittest.TestCase):
    def script(self, opens, browser):
        """The script running (``python -c``: a plain script, no terminal; on Windows in a console of its own, so Ctrl-C
        can be pressed there), and its output's lines as they come."""
        env = dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path))
        env['LEGEND_LITE_SITE'] = tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))
        console = {'creationflags': subprocess.CREATE_NEW_CONSOLE} if sys.platform == 'win32' else {}
        process = subprocess.Popen([sys.executable, '-c', SCRIPT, opens, browser], stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, text=True, **console)
        lines = queue.Queue()
        threading.Thread(target=lambda: [lines.put(line) for line in process.stdout], daemon=True).start()
        self.addCleanup(lambda: process.poll() is None and process.kill())
        return process, lines

    def until(self, lines, test):
        """The output's lines up to the first ``test`` holds for (it fails after 60 s)."""
        seen = []
        while True:
            line = lines.get(timeout=60)
            seen.append(line)
            if test(line):
                return seen

    def test_a_cube_opened_in_a_browser_keeps_the_engine_answering_until_ctrl_c(self):
        process, lines = self.script('opens', 'browser')
        served = json.loads(self.until(lines, lambda l: l.startswith('{'))[-1])
        self.until(lines, lambda l: 'press Ctrl-C' in l)
        # the wait: the engine still answers what needs the compiler (the frame's cube)
        request = urllib.request.Request(served['url'] + '/cube.json?table=frame',
                                         headers={'Authorization': served['authorization']})
        with urllib.request.urlopen(request, timeout=30) as r:
            self.assertEqual((r.status, sorted(json.loads(r.read()))), (200, ['model', 'runtime', 'source', 'title', 'version']))
        if sys.platform == 'win32':
            subprocess.run([sys.executable, '-c', CTRL_C, str(process.pid)], check=True, timeout=30)
        else:
            process.send_signal(signal.SIGINT)
        self.assertEqual(process.wait(timeout=30), 0)

    def test_with_no_browser_opened_the_end_does_not_wait(self):
        for opens, browser in (('opens', 'nobrowser'), ('fails', 'browser')):
            process, lines = self.script(opens, browser)
            self.assertEqual(process.wait(timeout=60), 0, (opens, browser))
            said = self.until(lines, lambda l: 'ended' in l)
            self.assertTrue(any('the script ended, and its DataCube with it' in l for l in said), said)

    def test_the_interactive_prompt_stays(self):
        self.assertFalse(datacube._stays())
        sys.ps1 = '>>> '
        try:
            self.assertTrue(datacube._stays())
        finally:
            del sys.ps1

    def test_with_no_cube_open_nothing_waits(self):
        session = datacube._Session()
        session.opened = True
        done = threading.Event()
        threading.Thread(target=lambda: (session._at_exit(), done.set()), daemon=True).start()
        self.assertTrue(done.wait(5), 'the end waited with no cube open')
