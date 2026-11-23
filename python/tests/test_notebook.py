"""A notebook's cube (legend_lite.notebook): the widget, its calls over its channel answered by the engine (the same
answers a tab gets over HTTP), its version following the frame, close; and show() in a notebook's kernel -- the cube
under the cell, displayed once in the cell show() ran in, shown again when typed later -- or in a tab when asked
(inline=False). No kernel runs here: the widget's sends are kept, and a kernel's shell stands in where one is asked
for."""

import contextlib
import gc
import io
import json
import os
import re
import sys
import threading
import types
import unittest
from pathlib import Path
from unittest import mock

import IPython
import pandas as pd
import pyarrow as pa
import pyarrow.ipc

import legend_lite as ll
from legend_lite import datacube, notebook
from legend_lite.notebook import DataCube


def trades(rows=3):
    return pd.DataFrame({"desk": ["FX", "EQ", "RATES", "FX"][:rows], "qty": [10.5, 20.0, 1.0, 7.0][:rows]})


class Widget(unittest.TestCase):
    """DataCube widgets on the process's one session, with DataCube's site, their sends kept."""

    def setUp(self):
        self.assertIn("LEGEND_LITE_SITE", os.environ, "the site comes from BUILD.bazel")
        datacube._session = datacube._Session()
        self.sent: list[tuple[DataCube, dict, list]] = []
        self.answered = threading.Condition()

    def tearDown(self):
        for cube in list(datacube._session.cubes.values()):
            cube.close()
        for view in list(datacube._session.pages):
            view.close()
        # its DataCube engine (headless) closed with it: a session dropped open leaves that engine's folder to the garbage
        # collector, which warns about it under whichever later test it reaches (CorpusPage counts every warning)
        if datacube._session._headless is not None:
            datacube._session._headless.close()

    def cube(self, frame=None, **given):
        cube = DataCube(trades() if frame is None else frame, **given)
        sent = lambda content, buffers=None, cube=cube: self.keep(cube, content, buffers or [])
        cube.send = sent  # ty: ignore[invalid-assignment] -- the widget's send stood in for: what it sends is kept
        return cube

    def keep(self, cube, content, buffers):
        with self.answered:
            self.sent.append((cube, content, buffers))
            self.answered.notify_all()

    @staticmethod
    def version(cube):
        """Its frame's version, as its page's widget is told it (``versions``: a frame's cube is a page of one grid, P12)."""
        return cube.versions["frames"][cube.name]

    def call(self, cube, method, path, query="", body=None, call_id="v:0"):
        """A call as the cube's page sends it, answered as the widget answers: its status, type, headers and body."""
        cube._answer({"kind": "call", "id": call_id, "method": method, "path": path, "query": query, "body": body})
        _, content, buffers = self.sent.pop()
        self.assertEqual((content["kind"], content["id"]), ("answer", call_id))
        return content["status"], content["type"], content["headers"], bytes(buffers[0])


class UnexpectedFailures(Widget):
    def test_a_failure_no_route_answers_says_no_held_value(self):
        # the C5 follow-up's check (2026-11-15): the widget's channel answered a failure's raw words; it is the engine's
        # one unexpected answer now, every value the session's frames hold taken out
        from unittest import mock

        cube = self.cube()
        frames = datacube._session.frames
        original = frames.held_values
        token = "X-Amz-Signature=held-0123456789"
        engine = datacube._session.engine()

        def failing(*_args, **_kw):
            raise RuntimeError(f"the call failed: {token}")

        held = lambda: (*original(), token)
        with (
            mock.patch.object(frames, "held_values", held),  # ty: ignore[invalid-argument-type] -- a tuple of str
            mock.patch.object(engine, "answer", failing),
        ):
            status, _, _, body = self.call(cube, "POST", "/snap.json", body="{}")
        said = body.decode("utf-8")
        self.assertEqual(status, 500, said)
        self.assertIn("RuntimeError: the call failed", said)
        self.assertNotIn("held-0123456789", said)


class Calls(Widget):
    def test_its_script_is_the_loader_and_the_page_keeps_datacube_s_module_by_its_hash(self):
        cube = self.cube()
        self.assertRegex(cube._esm, r"as default\b", "an ES module with a default export (anywidget's entry)")
        self.assertLess(len(cube._esm), 20_000, "the loader alone: DataCube comes once per page, over the channel")
        self.assertRegex(cube._module, "^[0-9a-f]{64}$")
        self.assertEqual(self.cube()._module, cube._module, "one module, one hash")
        # a page of one grid over its frame, kept as every Python page is, named by it (P12)
        self.assertEqual(
            (cube.name, cube.page.name, [g.frame for g in cube.page.grids], self.version(cube)),
            ("frame", "frame", ["frame"], 0),
        )
        self.assertEqual(cube.page_key, cube.page._key)

    def test_the_page_s_calls_are_answered_as_a_tab_s_are(self):
        cube = self.cube(name="trades")
        status, kind, _, body = self.call(cube, "GET", "/cube.json", "table=trades")
        self.assertEqual((status, kind), (200, "application/json"))
        served = json.loads(body)
        self.assertEqual(served["title"], "trades")
        # lite's API: a parse, as the cube's client sends it (legend-engine's pure/v1 is the edge's, H5)
        status, _, _, body = self.call(cube, "POST", "/api/lite/v1/query/parse", "", json.dumps({"text": "|1"}))
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["query"]["_type"], "lambda")
        # DataCube's module and its styles, as the loader fetches them
        for name, kind in (("widget.js", "text/javascript; charset=utf-8"), ("widget.css", "text/css; charset=utf-8")):
            status, given, _, body = self.call(cube, "GET", "/" + name)
            self.assertEqual((status, given), (200, kind), name)
            self.assertGreater(len(body), 50_000, name)
        # and its lazy chunks, as the loader fetches one the first time the cube needs it (a chart's code)
        chunks = Path(os.environ["LEGEND_LITE_SITE"]) / "widget-chunks"
        # the chart's own drawing chunk by its name -- chart-render-<hash>.js, not the rarer kinds' or the maps' that share
        # its prefix (2026-10-26: picked by the directory's order, the maps' 39 KB chunk came first on CI)
        render = next(f.name for f in chunks.iterdir() if re.fullmatch(r"chart-render-[A-Z0-9]+\.js", f.name))
        status, given, _, body = self.call(cube, "GET", "/widget-chunks/" + render)
        self.assertEqual((status, given), (200, "text/javascript; charset=utf-8"), render)
        # the chart's drawing, ECharts' charts among it (ECharts' core is a chunk it shares with the rarer kinds')
        self.assertGreater(len(body), 50_000, render)

    def test_a_grid_is_planned_and_run_over_the_channel(self):
        # a page's grid, as the cube's client asks Python for it: planned and run in one call (plan-and-run), its rows an
        # Arrow stream (legend-engine's execute is the edge's, H5)
        cube = self.cube(name="trades")
        table = datacube._session.frames["trades"]
        body = json.dumps(
            {
                "lambda": json.dumps(ll.parse("|" + table.accessor + "->filter(x|$x.qty > 5)")),
                "model": table.model,
                "runtime": table.runtime,
            }
        )
        status, kind, _, answer = self.call(cube, "POST", "/plan-and-run.json", "", body)
        self.assertEqual((status, kind), (200, "application/vnd.apache.arrow.stream"), answer[:300])
        self.assertGreater(pyarrow.ipc.open_stream(pa.py_buffer(answer)).read_all().num_rows, 0)

    def test_a_call_that_is_not_one_is_answered_never_dropped(self):
        cube = self.cube()
        self.assertEqual(self.call(cube, "GET", "/nope.js")[0], 404)
        self.assertEqual(self.call(cube, "PUT", "/cube.json")[0], 405)
        cube._answer({"kind": "call", "id": "v:1", "method": "GET", "path": "cube.json"})
        self.assertEqual(self.sent.pop()[1]["status"], 400)

    def test_a_message_is_answered_on_a_thread_of_its_own(self):
        cube = self.cube()
        cube._received(
            cube,
            {
                "kind": "call",
                "id": "v:7",
                "method": "GET",
                "path": "/version.json",
                "query": f"page={cube.page_key}",
                "body": None,
            },
            [],
        )
        with self.answered:
            self.assertTrue(self.answered.wait_for(lambda: self.sent, timeout=30))
        said = json.loads(bytes(self.sent[0][2][0]))
        self.assertEqual((self.sent[0][1]["id"], said["frames"]), ("v:7", {"frame": 0}))
        cube._received(cube, {"kind": "something else"}, [])
        cube._received(cube, "not a message", [])


class Follows(Widget):
    def test_update_refresh_and_a_live_frame_s_new_columns_move_its_version(self):
        df = trades()
        cube = self.cube(df)
        # nothing returned: a cell ending in cube.update(df) shows no second copy of the cube
        self.assertIsNone(cube.update(trades(4)))
        self.assertEqual(self.version(cube), 1)
        cube.refresh()
        self.assertEqual(self.version(cube), 2)
        live = trades()
        cube.update(live)
        self.call(cube, "GET", "/cube.json", "table=frame")
        live["book"] = "x"
        self.assertIn("book", json.loads(self.call(cube, "GET", "/cube.json", "table=frame")[3])["model"])
        self.assertEqual(self.version(cube), 4, "the new column noticed at the read, and its cube told")

    def test_another_frame_s_change_is_not_its(self):
        first, second = self.cube(), self.cube()
        second.refresh()
        self.assertEqual((self.version(first), self.version(second)), (0, 1))

    def test_a_version_told_late_never_moves_it_back(self):
        cube = self.cube()
        cube.refresh()
        cube.refresh()
        cube._frame_moved("frame", 1)
        self.assertEqual(self.version(cube), 2)

    def test_a_cube_made_again_under_its_name_leaves_the_frame_to_the_new_one_when_the_old_closes(self):
        old, new = self.cube(name="t"), self.cube(name="t")
        old.close()
        self.assertIn("t", datacube._session.frames)
        self.assertIs(datacube._session.cubes["t"], new)
        new.refresh()
        self.assertEqual(self.version(new), 2)
        self.assertIs(new.page, old.page, "one page for its name, shown in both")

    def test_a_frame_a_cube_cannot_be_made_over_fails_once_and_cleanly(self):
        unraised = []
        was = sys.unraisablehook
        sys.unraisablehook = unraised.append
        try:
            with self.assertRaises(TypeError):
                DataCube(42)
            import gc

            gc.collect()
        finally:
            sys.unraisablehook = was
        self.assertEqual([u.exc_value for u in unraised], [], "a failed cube closes with nothing to undo")

    def test_close_takes_the_frame_out_and_the_widget_down(self):
        cube = self.cube()
        cube.close()
        self.assertNotIn("frame", datacube._session.frames)
        self.assertIsNone(cube.comm, "the widget closed: its views leave the output")
        for use in (lambda: cube.refresh(), lambda: cube.update(trades())):
            with self.assertRaises(ValueError):
                use()
        cube.close()
        self.assertEqual(repr(cube), "<DataCube 'frame': closed>")


class InAKernel(Widget):
    """show() where a notebook's kernel runs: its shell stood in for (ipykernel's class, IPython's events)."""

    def setUp(self):
        super().setUp()
        self.registered = []
        zmqshell = types.ModuleType("ipykernel.zmqshell")
        shell_class = type("ZMQInteractiveShell", (), {})
        zmqshell.ZMQInteractiveShell = shell_class  # ty: ignore[unresolved-attribute] -- a stand-in module's name
        self.shell = zmqshell.ZMQInteractiveShell()
        events = types.SimpleNamespace(register=lambda event, f: self.registered.append((event, f)))
        self.shell.events = events  # ty: ignore[unresolved-attribute] -- a stand-in shell's events
        self.displayed = []
        for patch in (
            mock.patch.dict(sys.modules, {"ipykernel.zmqshell": zmqshell}),
            mock.patch.object(IPython, "get_ipython", lambda: self.shell),
            mock.patch("IPython.display.display", self.display),
        ):
            patch.start()
            self.addCleanup(patch.stop)

    def display(self, shown, **given):
        # what reaches the notebook: a widget's view, or the bundle a widget displays itself as
        if isinstance(shown, notebook._EngineWidget):
            shown._ipython_display_()
        else:
            self.displayed.append((shown, given))

    def cell_ends(self):
        for event, f in self.registered:
            if event == "post_run_cell":
                f(None)

    def test_show_puts_the_cube_under_the_cell_once(self):
        cube = ll.show(trades())
        self.assertIsInstance(cube, DataCube)
        self.assertEqual(len(self.displayed), 1)
        bundle, given = self.displayed[0]
        self.assertEqual(bundle["application/vnd.jupyter.widget-view+json"]["model_id"], cube.model_id)
        self.assertTrue(given["raw"])
        # the cell's last line is show(): its result shows no second copy
        cube._ipython_display_()
        self.assertEqual(len(self.displayed), 1)
        # typed in a later cell, it shows again
        self.cell_ends()
        cube._ipython_display_()
        self.assertEqual(len(self.displayed), 2)
        self.assertIsNone(datacube._session._web, "no web server for a notebook")

    def test_a_closed_cube_displayed_again_shows_its_words(self):
        # ipywidgets takes a closed widget's bundle away (Widget.close sets _repr_mimebundle_ to None): displayed in a
        # later cell, the cube says it is closed, as IPython shows any object's words -- it raised a TypeError (found
        # by ty, 2026-11-23)
        cube = ll.show(trades())
        self.cell_ends()
        cube.close()
        cube._ipython_display_()
        bundle, given = self.displayed[-1]
        self.assertEqual(bundle, {"text/plain": "<DataCube 'frame': closed>"})
        self.assertTrue(given["raw"])

    def test_a_name_shown_again_is_the_same_cube_shown_again(self):
        first = ll.show(trades(), name="trades")
        again = ll.show(trades(2), name="trades")
        self.assertIs(again, first)
        self.assertEqual((self.version(first), len(self.displayed)), (1, 2))

    def test_snap_true_reads_the_frame_once_by_its_page_s_grid(self):
        # docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8, P12: its page's grid snapped in Python, read once until its Go Live
        cube = ll.show(trades(), name="once", snap=True)
        self.assertIsNotNone(cube.page.grids[0].snapped)
        self.assertIs(ll.show(trades(2), name="once"), cube)
        self.assertIsNotNone(cube.page.grids[0].snapped, "shown again: still read once, its copy as it was")

    def test_after_each_cell_its_cubes_read_at_each_query_query_again(self):
        # a stream, read once into a table of its own: nothing of it changes after a cell
        import pyarrow as pa

        table = pa.Table.from_pandas(trades(), preserve_index=False)
        live, once = ll.show(trades()), ll.show(pa.RecordBatchReader.from_batches(table.schema, table.to_batches()))
        self.cell_ends()
        self.assertEqual((self.version(live), self.version(once)), (1, 0))

    def test_a_name_moved_to_a_tab_keeps_its_frame_when_its_notebook_cube_closes(self):
        notebook = ll.show(trades(), name="t")
        tab = ll.show(trades(), name="t", browser=False, inline=False)
        self.assertIsInstance(tab, datacube.Cube)
        notebook.close()
        self.assertIn("t", datacube._session.frames)
        self.assertIs(datacube._session.cubes["t"], tab)
        self.assertIsNotNone(datacube._session._web, "the tab's web server stays")

    def test_inline_false_opens_a_tab_from_a_kernel(self):
        cube = ll.show(trades(), browser=False, inline=False)
        self.assertIsInstance(cube, datacube.Cube)
        self.assertEqual(self.displayed, [])
        # its name shown again shows where it showed: in the tab
        self.assertIs(ll.show(trades(), name=cube.name, browser=False), cube)

    def test_outside_a_kernel_show_opens_a_tab(self):
        with mock.patch.object(IPython, "get_ipython", lambda: None):
            self.assertIsInstance(ll.show(trades(), browser=False), datacube.Cube)

    def test_a_frame_moving_under_a_query_never_waits_on_the_session(self):
        # a query held the frames still and found a frame's columns changed (Engine._noted): the page's widget is told on
        # that thread, so it takes no session lock -- another thread registering a frame holds that and waits on the
        # frames (the audit of §18, 2026-10-20: it asked DataCube's code which frames the page reads)
        import threading

        page = ll.Page("Moving")
        page.grid(trades(), name="moving")
        cube = ll.show(page)
        self.assertIsInstance(cube, notebook.PageCube)
        session, engine = datacube._session, datacube._session.engine()
        held, serving, got = threading.Event(), threading.Event(), []

        def registering() -> None:
            with session._lock:
                held.set()
                # what registering a frame does next, once the query holds the frames: the frames' lock
                serving.wait(10)
                taken = session.frames._lock.acquire(timeout=10)
                got.append(taken)
                if taken:
                    session.frames._lock.release()

        other = threading.Thread(target=registering)
        other.start()
        held.wait(10)
        with session.frames.serving():
            serving.set()
            engine.changed("moving")
        other.join(20)
        self.assertEqual(got, [True], "the query let the frames go: its widget waited on nothing")
        self.assertEqual(cube.versions["frames"], {"moving": engine.version("moving")})

    def test_a_page_shows_under_the_cell_once_and_its_widget_is_shown_again(self):
        # a page of no grid yet shows, as the tab's blank page does, its widget following it as Python adds to it (D2)
        page = ll.Page("P")
        first = ll.show(page)
        self.assertIsInstance(first, notebook.PageCube)
        self.assertEqual(len(self.displayed), 1)
        empty = first.versions["version"]
        page.grid(trades(), name="t")
        self.assertGreater(first.versions["version"], empty)
        first._ipython_display_()
        self.assertEqual(len(self.displayed), 1, "the cell's result shows no second copy")
        # shown again: the page's own widget, displayed again here (not a second widget following the page)
        again = ll.show(page)
        self.assertIs(again, first)
        self.assertEqual(len(self.displayed), 2)
        self.assertEqual(datacube._session.pages, [first])
        # after the cell, typed again it shows again (the session tells the page's views a cell ended)
        self.cell_ends()
        first._ipython_display_()
        self.assertEqual(len(self.displayed), 3)
        # a change in Python moves its versions, which its page follows
        before = first.versions["version"]
        page.sheet("More")
        self.assertEqual(first.versions["version"], before + 1)


class WithoutTheExtra(unittest.TestCase):
    def test_without_anywidget_the_import_says_how_to_install_it(self):
        with mock.patch.dict(sys.modules, {"anywidget": None, "legend_lite.notebook": None}):
            del sys.modules["legend_lite.notebook"]
            with self.assertRaises(ModuleNotFoundError) as missing:
                ll.DataCube  # noqa: B018 -- the lazy import is what is tested
        self.assertIn("pip install 'legend-lite[notebook]'", str(missing.exception))


class InMarimo(Widget):
    """show() in a marimo notebook: marimo stood in for -- running_in_notebook(), its runtime context (one per notebook
    session), and its cell lifecycle (the hook its own widgets' channels close by) holding what a cell registers until
    the cell runs again."""

    class Context:
        """A marimo session's runtime context, as marimo keeps one: the running cell, the cell lifecycle registry."""

        def __init__(self, test):
            self.cell_id: str | None = "cell-1"
            self.cell_lifecycle_registry = types.SimpleNamespace(
                add=lambda item: (item.create(self), test.items.append(item))
            )

    def setUp(self):
        super().setUp()
        self.items = []
        self.context = self.Context(self)
        lifecycle = types.ModuleType("marimo._runtime.cell_lifecycle_item")
        item_class = type("CellLifecycleItem", (), {})
        lifecycle.CellLifecycleItem = item_class  # ty: ignore[unresolved-attribute] -- a stand-in module's name
        context = types.ModuleType("marimo._runtime.context")
        context.get_context = lambda: self.context  # ty: ignore[unresolved-attribute] -- a stand-in module's name
        marimo = types.ModuleType("marimo")
        marimo.running_in_notebook = lambda: True  # ty: ignore[unresolved-attribute] -- a stand-in module's name
        self.displayed = []
        notebook._warned.clear()
        for patch in (
            mock.patch.dict(
                sys.modules,
                {
                    "marimo": marimo,
                    "marimo._runtime": types.ModuleType("marimo._runtime"),
                    "marimo._runtime.cell_lifecycle_item": lifecycle,
                    "marimo._runtime.context": context,
                },
            ),
            mock.patch("IPython.display.display", lambda *a, **k: self.displayed.append(a)),
        ):
            patch.start()
            self.addCleanup(patch.stop)

    def tearDown(self):
        for session in list(datacube._marimo_sessions.values()):
            for cube in list(session.cubes.values()):
                cube.close()
            for view in list(session.pages):
                view.close()
        datacube._marimo_sessions.clear()
        datacube._gone.clear()
        super().tearDown()

    def cell_runs_again(self):
        """marimo disposes what the cell registered, before it runs the cell again."""
        items, self.items = self.items, []
        for item in items:
            self.assertTrue(item.dispose(None, False))

    def said(self, run):
        """What ``run`` printed to stderr: legend-lite's warnings."""
        written = io.StringIO()
        with contextlib.redirect_stderr(written):
            result = run()
        return result, written.getvalue()

    def test_show_is_the_cube_for_the_cell_to_show_nothing_added(self):
        cube = ll.show(trades())
        self.assertIsInstance(cube, DataCube)
        self.assertEqual(self.displayed, [], "marimo shows the cell's last expression: show() adds nothing")
        self.assertEqual(len(self.items), 1)

    def test_the_cell_run_again_closes_its_cube_and_frees_its_name(self):
        first = ll.show(trades())
        self.cell_runs_again()
        self.assertTrue(first._closed)
        self.assertNotIn("frame", datacube._current().frames)
        again = ll.show(trades(2))
        self.assertEqual(again.name, "frame", "a cell run ten times shows frame, not frame_10")

    def test_each_marimo_session_its_own_frames_and_names(self):
        # marimo run: every viewer's notebook in this one process, a session each
        first = ll.show(trades(), name="t")
        self.context = self.Context(self)
        second = ll.show(trades(2), name="t")
        self.assertEqual((first.name, second.name), ("t", "t"))
        self.assertIsNot(datacube._current(), datacube._session)
        self.assertEqual(len(datacube._marimo_sessions), 2)
        self.assertEqual(
            first.versions["frames"]["t"], 0, "one viewer showing a name never changes another viewer's cube"
        )

    def test_a_session_let_go_lets_its_cubes_and_frames_go(self):
        cube = ll.show(trades())
        first = id(self.context)
        self.context = self.Context(self)
        self.items.clear()
        gc.collect()
        # its finalizer queues it, and sends nothing where the collector fired (the user's ruling (b) of 2026-11-14, the
        # C4 final fix round's D2): the next call that finds its session lets it go, outside any run
        self.assertIn(
            first, [key for key, _ in datacube._gone], "the first session's context is gone: its session queued"
        )
        self.assertIn(first, datacube._marimo_sessions)
        self.assertFalse(cube._closed)
        datacube._current()
        self.assertEqual(len(datacube._gone), 0)
        self.assertNotIn(first, datacube._marimo_sessions, "the first session let go")
        self.assertTrue(cube._closed)

    def test_a_new_context_that_takes_a_gone_ones_id_never_gets_its_session(self):
        # the C4 second small round's item 2a, 2026-11-14: a session is a context's by identity. Thread A pops a gone
        # session's entry and is held before it takes the lock; a new context that took the gone one's id finds the
        # queue empty and the gone session still under its id -- it gets a session of its own, and A then lets go of
        # the gone one alone
        import weakref

        import pyarrow as pa

        gone_context = self.Context(self)
        gone = datacube._Session()
        gone.context = weakref.ref(gone_context)
        gone.frames.register("gone_t", pa.table({"a": [1]}))
        del gone_context
        gc.collect()
        self.assertIsNone(gone.context(), "the gone context is collected")
        key = id(self.context)
        with datacube._marimo_lock:
            datacube._marimo_sessions[key] = gone
        datacube._gone.append((key, gone))
        popped, resume = threading.Event(), threading.Event()
        real = datacube._release_now

        def held(k, session):
            popped.set()
            resume.wait(10)
            real(k, session)

        with mock.patch.object(datacube, "_release_now", held):
            a = threading.Thread(target=datacube._let_go_gone)
            a.start()
            self.assertTrue(popped.wait(10), "thread A popped the gone session")
            mine = datacube._current()
            resume.set()
            a.join(10)
        self.assertIsNot(mine, gone)
        assert mine.context is not None
        self.assertIs(mine.context(), self.context)
        self.assertIs(datacube._marimo_sessions[key], mine, "the new session stays under the id")
        self.assertEqual(gone.frames._tables, {}, "the gone session let go")
        assert mine.finalizer is not None
        self.assertTrue(mine.finalizer.alive)

    def test_at_exit_every_marimo_session_is_let_go(self):
        # the C4 second small round's item 2c, 2026-11-14: weakref.finalize's exit pass now only queues a session, so
        # an exit handler (_let_go_at_exit, registered at import) calls each live session's finalizer and drains the
        # queue -- every session let go, as before the finalizer only queued
        cube = ll.show(trades())
        session = datacube._current()
        datacube._let_go_at_exit()
        self.assertTrue(cube._closed)
        self.assertNotIn(id(self.context), datacube._marimo_sessions)
        assert session.finalizer is not None
        self.assertFalse(
            session.finalizer.alive, "its finalizer called once: finalize's own exit pass calls it no more"
        )
        self.assertEqual(len(datacube._gone), 0)

    def test_inline_false_still_opens_a_tab(self):
        self.assertIsInstance(ll.show(trades(), browser=False, inline=False), datacube.Cube)

    def test_a_page_is_the_cell_s_output_and_closes_with_its_cell(self):
        page = ll.Page("P")
        page.grid(trades(), name="t")
        cube = ll.show(page)
        self.assertIsInstance(cube, notebook.PageCube)
        self.assertEqual(self.displayed, [], "marimo shows the cell's last expression: show() adds nothing")
        self.assertEqual(len(self.items), 1)
        engine = datacube._current().engine()
        self.assertIsNotNone(engine.page_versions(page._key))
        self.cell_runs_again()
        self.assertTrue(cube._closed)
        self.assertIsNone(engine.page_versions(page._key), "served no more")

    def test_outside_a_cell_the_cube_works_and_says_it_keeps_its_frame(self):
        self.context.cell_id = None
        cube, said = self.said(lambda: ll.show(trades()))
        self.assertIsInstance(cube, DataCube)
        self.assertEqual(self.items, [])
        self.assertIn("outside a marimo cell", said)

    def test_without_marimo_s_lifecycle_the_cube_works_and_says_so(self):
        with mock.patch.dict(sys.modules, {"marimo._runtime.cell_lifecycle_item": None}):
            cube, said = self.said(lambda: ll.show(trades()))
        self.assertIsInstance(cube, DataCube)
        self.assertEqual(self.items, [])
        self.assertIn("no cell lifecycle", said)
