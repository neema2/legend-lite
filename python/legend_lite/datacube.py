"""DataCube on a dataframe: ``show(df)`` shows it -- under the cell in a notebook, in a browser tab anywhere else --
and returns at once.

    import legend_lite as ll
    cube = ll.show(df)          # DataCube on df: each query reads df as it is then
    df.loc[0, 'qty'] = 5         # an in-place change: the cube shows it on its next query
    cube.update(new_df)          # a NEW frame (rebinding `df` does not reach the cube)

The design: docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md. DataCube runs in the browser as the UI only; this process
is its Legend engine (``Engine``, one per process), and every query runs here. A tab reaches it over HTTP
(``WebServer``, started by the first tab); a notebook's cube over its widget's own channel (``legend_lite.notebook``).
In IPython and notebooks the cube is told to query again after each cell, so a change shows by itself; at a plain
``>>>`` prompt, the next click in the cube shows it. A plain script that opened a cube in the browser and ends while
it is open waits there, saying so, until Ctrl-C (or an IDE's Stop).
"""

from __future__ import annotations

import atexit
import os
import sys
import threading
import time
import weakref
import webbrowser
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .engine import Engine, WebServer
from .frames import Frames
from .keeper import PageKeeper

if TYPE_CHECKING:
    from .notebook import DataCube, PageCube
    from .page import Page


def _site() -> Path:
    """DataCube's built pages: LEGEND_LITE_SITE, else the ones shipped inside this package."""
    explicit = os.environ.get('LEGEND_LITE_SITE')
    return Path(explicit) if explicit else Path(__file__).with_name('_site')


def _stays() -> bool:
    """Whether the process stays after its main code: ``python -i``, the interactive prompt, IPython, a notebook."""
    if sys.flags.interactive or hasattr(sys, 'ps1'):
        return True
    ipython = sys.modules.get('IPython')
    return ipython is not None and ipython.get_ipython() is not None


def _marimo() -> bool:
    """Whether this code runs in a marimo notebook, whose cells show their last expression. Read from what is already
    loaded: nothing is imported to ask."""
    marimo = sys.modules.get('marimo')
    running = getattr(marimo, 'running_in_notebook', None) if marimo is not None else None
    return running is not None and running()


def _kernel() -> bool:
    """Whether this process is a notebook's kernel (Jupyter, VS Code, Colab: IPython's kernel), whose front end shows a
    widget under the cell. Read from what is already loaded: nothing is imported to ask."""
    zmqshell = sys.modules.get('ipykernel.zmqshell')
    ipython = sys.modules.get('IPython')
    if zmqshell is None or ipython is None:
        return False
    return isinstance(ipython.get_ipython(), zmqshell.ZMQInteractiveShell)


class PageTab:
    """A page (``ll.Page``) shown in DataCube in a browser tab: its link, and what closes it. The tab follows the page:
    a change made to it in Python opens it again there, a frame updated queries again."""

    def __init__(self, session: _Session, page: Page) -> None:
        self._session = session
        self._page = page
        self._closed = False

    @property
    def page(self) -> Page:
        return self._page

    @property
    def url(self) -> str:
        """The page's link: DataCube's page on the engine, and the engine's token (in the fragment, never sent)."""
        self._open()
        web = self._session.web()
        return f'{web.url}/engine.html?page={self._page._key}#token={web.token}'

    def close(self) -> None:
        """Stops serving the page; the tab keeps what it shows. The last tab closed stops the web server."""
        if not self._closed:
            self._closed = True
            self._session.close_page(self)

    def _page_moved(self, version: int) -> None:
        # the tab asks (version.json?page=): nothing to tell it
        pass

    def _after_cell(self) -> None:
        pass

    def _open(self) -> None:
        if self._closed:
            raise ValueError(f'the page {self.page.name!r} was closed in its tab: show it again')

    def __repr__(self) -> str:
        return f'<DataCube page {self.page.name!r}: {self.url}>' if not self._closed else f'<DataCube page {self.page.name!r}: closed>'


class Cube(PageTab):
    """A frame shown in DataCube in a browser tab (``ll.show(df)``): a page of one grid over it, kept as every Python page
    is (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md P12) -- so a column changed in the frame keeps the grid's edits, made in
    Python or in the tab -- its link, and what changes it."""

    def __init__(self, session: _Session, name: str, page: Page) -> None:
        super().__init__(session, page)
        self.name = name

    def update(self, frame: Any) -> None:
        """Shows another frame in this cube: the page opens it, over its new columns if they changed, the grid's edits
        kept (its page's own update: B10). It returns nothing, as a notebook's cube's does."""
        self._open()
        self._page._update(self.name, frame)

    def refresh(self) -> None:
        """Tells the page to query again: a change the next click would show, shown now."""
        self._open()
        self._session.engine().changed(self.name)

    def close(self) -> None:
        """Takes the frame out of the engine; the tab keeps what it shows. The last tab closed stops the web server. A
        cube its name has since moved to another (shown under a notebook's cell) takes only itself away."""
        if not self._closed:
            self._closed = True
            self._session.close_page(self)
            self._session.close(self.name, self)

    def _open(self) -> None:
        if self._closed:
            raise ValueError(f'the cube {self.name!r} was closed: show the frame again')

    def __repr__(self) -> str:
        return f'<DataCube {self.name!r}: {self.url}>' if not self._closed else f'<DataCube {self.name!r}: closed>'


class _Session:
    """The process's one engine, the frames it serves, the cubes over them (one a name: a tab's or a notebook's), the
    pages shown (``ll.Page``), and the web server the tabs reach it through."""

    def __init__(self) -> None:
        self.frames = Frames()
        self.cubes: dict[str, Cube | DataCube] = {}
        # the pages shown, each where it is shown: a tab's or a notebook's
        self.pages: list[PageTab | PageCube] = []
        # Query's and Studio's links opened (ll.query(), ll.studio()): an app's tab says nothing when it closes, so the
        # web server stays for it while the process lives
        self.apps: dict[str, str] = {}
        self._engine: Engine | None = None
        self._web: WebServer | None = None
        # DataCube's own code over these frames, headless (legend_lite._headless): made on first use
        self._headless: Any = None
        self._lock = threading.RLock()
        self._hooked = False
        # a cube was opened in a browser: a person is looking, so a plain script's end keeps the engine for them
        self.opened = False
        # the one copy of each Python page, versioned (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md): each new version told
        # where its page is shown; each write's edits applied by DataCube's rule (_combine)
        self.keeper = PageKeeper(on_write=self._page_written, combine=self._combine)
        # the keys of pages collected, forgotten by the keeper at the next safe moment (sweep): never in the collector
        self.gone: list[str] = []
        # every Python page of this session not yet collected: a saved page deleted is no saved page any more for each
        # saved as it (Page.delete, as the tab's Open… tells its page: K14) -- added to and read under its own lock (a
        # page made on another thread meanwhile), never held across a call into DataCube (D15)
        self._page_objects: weakref.WeakSet[Any] = weakref.WeakSet()
        self._page_objects_lock = threading.Lock()

    def page_made(self, page: Any) -> None:
        """A Python page of this session, kept (weakly) among its pages."""
        with self._page_objects_lock:
            self._page_objects.add(page)

    def page_objects(self) -> list[Any]:
        """This session's Python pages not yet collected, as they are now."""
        with self._page_objects_lock:
            return list(self._page_objects)

    def engine(self) -> Engine:
        """This session's engine: its rows' plan-and-run (every grid's, D16), and what a tab or a notebook's widget is
        served by -- with DataCube's site when this install carries it (``shown_engine``)."""
        with self._lock:
            if self._engine is None:
                # a page's file is read into the frames' database by DataCube's own code, headless (file.json); every
                # query planned in this process, by Python's own compiler (a host plans in process: the program's §1.1)
                site = _site()
                self._engine = Engine(self.frames, site=site if site.is_dir() else None, headless=self.headless,
                                      keeper=self.keeper)
            return self._engine

    def shown_engine(self) -> Engine:
        """The engine, for what shows a page (a tab, a notebook's widget): DataCube's site with it -- refused in words when
        this install carries none, which reads grids in Python alone."""
        engine = self.engine()
        if engine.site is None:
            raise ValueError(f'the site {_site()} is not a directory: DataCube\'s pages are not in this install')
        return engine

    def headless(self) -> Any:
        """DataCube's own non-screen code, in V8, over these frames' database (the `datacube` extra): made on first
        use, one per session, as one state owner holds a grid's state on screen."""
        with self._lock:
            if self._headless is not None and self._headless.broken:
                # it ran out of its memory: a new one, the frames as they are
                self._headless.close()
                self._headless = None
            if self._headless is None:
                from ._headless import Headless, bundle
                # a call made in a read of the frames (Frames.reading) queries through it (D15)
                self._headless = Headless(bundle(), connection=self.frames.connection, reading=self.frames.reading_now)
                # what DataCube's code asks of the engine beside it (a grid's rows where they are), answered by this
                # session's engine -- by a weak method: the session holds its DataCube engine, never the other way round
                # (any function of this module would reach the session through the module's globals, and an engine in
                # such a cycle is collected at the interpreter's end, where tearing V8 down crashes the process)
                self._headless.answers = weakref.WeakMethod(self._answer_over)
                # every grid's rows are read by the engine's plan-and-run (D16): made with it, before any call asks it
                self.engine()
            return self._headless

    def _answer_over(self, path: str, body: str, over: Any) -> tuple[int, str, bytes]:
        """A call of DataCube's code to the engine (headless calls.ts ``engine``), answered by this session's engine on
        DataCube's thread over the read the call was made in (``Engine.answer_over``: D15) -- made with DataCube's engine
        (``headless``), so it takes no session lock (the call's caller may hold it, waiting)."""
        engine = self._engine
        if engine is None:
            raise RuntimeError('this session made DataCube\'s engine without its own: no grid\'s rows can be read')
        return engine.answer_over(path, body, over)

    def held_copies(self) -> dict[str, dict[str, Any]]:
        """Every snap's copy this session's engine holds, by its table (its model, runtime and relation, as DataCube
        modelled it as it landed: headless grid.ts HeldCopy) -- none made yet, no engine yet: none. Asked as a read of the
        frames begins (a snapped grid's base, docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8): the copies held now, which
        a copy made after the read began is among though the read does not see it (F31). It takes no session lock, only
        the engine's own."""
        engine = self._engine
        return {} if engine is None else engine.held_copies()

    def sweep(self) -> None:
        """The pages collected since forgotten, and each snap's copy no page kept reads any more let go of by the engine
        holding it (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md; engine.py ``sweep``). Called where nothing is held but, at
        most, the session's lock: a page's change, a cell's end."""
        while self.gone:
            self.keeper.forget(self.gone.pop())
        engine = self._engine
        if engine is not None:
            engine.sweep()

    def _combine(self, page: dict[str, Any], held: list[str], edits: list[Any]) -> dict[str, Any]:
        """A writer's edits applied to ``page`` as the keeper has it now, by DataCube's one rule (headless pageRebase:
        page-rebase.ts; made again on top when the page moved on since they were made) -- a grid's over the frames this
        session serves, a snapped grid's over the copy its engine holds: ``{page, held, edits, reads}``, or
        ``{problems}``. The keeper's, asked holding nothing (the session's lock first, then the frames')."""
        from . import _json
        headless = self.headless()
        copies = self.held_copies()
        with self.frames.reading():
            said = _json.loads(headless.call('pageRebase', _json.dumps(page), _json.dumps(held), _json.dumps(edits),
                                             self.frames.hosted(), copies))
        if 'page' in said:
            said['page'] = _json.loads(said['page'])
        return said

    def _page_written(self, key: str, version: int) -> None:
        """A page's new version, written by any of its editors: told where it is shown (a notebook's widget follows it)."""
        for view in list(self.pages):
            if view._page._key == key:
                view._page_moved(version)

    def web(self) -> WebServer:
        """The engine over HTTP, for the tabs: started by the first."""
        with self._lock:
            if self._web is None:
                self._web = WebServer(self.shown_engine())
            return self._web

    def register(self, frame: Any, name: str | None) -> str:
        """The frame registered under ``name`` (the next free one unless given), and a page showing that name told."""
        with self._lock:
            if name is None:
                name = self._next_name()
            # shown by a cube of its name, or read by a page shown
            shown = name.lower() in self.cubes or any(f.lower() == name.lower()
                                                      for view in self.pages for f in view._page._frames())
            self.frames.register(name, frame)
            # an engine is made by what shows a frame (a tab, a notebook's cube, a page shown) or reads a grid (D16):
            # none yet, nothing has been served and nobody is told
            engine = self._engine
            if engine is not None and (shown or engine.seen(name)):
                # shown again under its name (or again after it was closed): a page showing it reads it again
                engine.changed(name)
            return name

    def unregister(self, name: str) -> None:
        """A frame registered for nothing that came of it (a page's grid refused): out of the engine again."""
        with self._lock:
            if name in self.frames and name.lower() not in self.cubes:
                self.frames.unregister(name)

    def frame_page(self, frame: Any, name: str | None, snap: bool) -> tuple[str, Page]:
        """THE PAGE A FRAME IS SHOWN AS (``ll.show(df)``: docs/DATACUBE_ONE_API_PLAN_2026_10_21.md P12): a page of one grid
        over it, kept as every Python page is, and its name -- its name's page when it shows already, its frame replaced
        (each place it is shown follows), else a new page named by it; its grid read once by its Snap when ``snap`` (and
        not snapped already). Built outside the session's lock, as any page is: a snap's copy is made on the engine's own
        threads, which ask for DataCube's code (the session's lock first, then the frames')."""
        from .page import Page
        with self._lock:
            shown = self.cubes.get(name.lower()) if name is not None else None
            if shown is not None and shown._closed:
                shown = None
            named = name if name is not None else self._next_name()
        if shown is not None:
            page = shown._page
            # its frame replaced as a grid's update replaces it: each cube over it opened again, its edits kept (B10)
            page._update(shown.name, frame)
            if snap and page.grids and page.grids[0].snapped is None:
                page.grids[0].snap()
            return shown.name, page
        page = Page(named)
        page.grid(frame, name=named, snap=snap)
        return named, page

    def show(self, frame: Any, name: str | None, snap: bool = False) -> Cube:
        """The frame shown in a tab (frame_page): its name's cube there, the one it has, else a new one -- refused in words
        first where this install carries no DataCube site to show it in (F40)."""
        self.shown_engine()
        name, page = self.frame_page(frame, name, snap)
        with self._lock:
            cube = self.cubes.get(name.lower())
            if not isinstance(cube, Cube) or cube._closed:
                cube = Cube(self, name, page)
                self.adopt(cube)
                self.serve(page, cube)
            return cube

    def adopt(self, cube: Any) -> Any:
        """``cube`` is its name's cube from now on."""
        with self._lock:
            self.cubes[cube.name.lower()] = cube
            self._hook()
            return cube

    def _next_name(self) -> str:
        n = 1
        # a page's file's table holds its name too (the engine's file.json)
        while (name := 'frame' if n == 1 else f'frame_{n}').lower() in self.cubes or self.frames.taken(name):
            n += 1
        return name

    def after_cell(self, *_: Any) -> None:
        """A notebook cell (or an IPython command) has run: each cube, and each frame of a page shown, read at each query
        (a stream's, read once, never changes), queries again, so a change shows."""
        with self._lock:
            # the pages collected meanwhile (a cell made its page again), and the copies no page reads any more: let go
            self.sweep()
            # (each frame's cube is a page shown: P12)
            for view in self.pages:
                view._after_cell()
            frames = {f for shown in self.pages for f in shown._page._frames()}
            for name in frames:
                if self._engine is not None and name in self.frames and self.frames[name].live:
                    self._engine.changed(name)

    def show_page(self, page: Page) -> PageTab:
        """A page served and shown in a tab: a new tab each time it is shown."""
        with self._lock:
            tab = PageTab(self, page)
            self.serve(page, tab)
            return tab

    def serve(self, page: Page, view: PageTab | PageCube) -> None:
        """``page`` served by the engine, shown in ``view`` (a tab's or a notebook's) and told of each change -- a page of
        no grid yet too, which DataCube shows and follows as Python adds to it (D2)."""
        with self._lock:
            # served when it is first shown; shown again, the engine has it as it is (served each time it changed), and
            # a page open on it stays as it is
            if not page._shown:
                # its engine made, to serve it -- with DataCube's site, or refused in words; the page kept already
                self.shown_engine()
                self.keeper.serve(page._key, True)
            page._shown.append(view)
            self.pages.append(view)
            self._hook()

    def close_page(self, view: PageTab | PageCube) -> None:
        """A page's view closed: the page no longer told of its changes there; served no more when it is shown nowhere."""
        with self._lock:
            if view in self.pages:
                self.pages.remove(view)
            page = view._page
            if view in page._shown:
                page._shown.remove(view)
            if not page._shown:
                self.keeper.serve(page._key, False)
            self._stop_web_if_unused()

    def close(self, name: str, cube: Any) -> None:
        """``cube`` closed: when it is still its name's cube, the frame goes out of the engine (a cube the name has moved
        on from leaves the frame to the one it moved to, the audit of step 7, S3); the last tab's cube stops the web
        server."""
        with self._lock:
            if self.cubes.get(name.lower()) is not cube:
                self._stop_web_if_unused()
                return
            del self.cubes[name.lower()]
            if name in self.frames:
                self.frames.unregister(name)
                if self._engine is not None:
                    # its page asks (or is told), finds the frame gone, and stops following it
                    self._engine.changed(name)
            self._stop_web_if_unused()

    def _stop_web_if_unused(self) -> None:
        # a frame's cube in a tab is a page shown there (P12)
        if self._web is not None and not self.apps and not any(isinstance(p, PageTab) for p in self.pages):
            self._web.close()
            self._web = None

    def _hook(self) -> None:
        """Once: the notebook's after-cell nudge, where there is IPython; the wait at a plain script's end."""
        if self._hooked:
            return
        self._hooked = True
        ipython = sys.modules.get('IPython')
        shell = ipython.get_ipython() if ipython is not None else None
        if shell is not None:
            shell.events.register('post_run_cell', self.after_cell)
        atexit.register(self._at_exit)

    def waits(self) -> bool:
        """Whether a plain script's end waits while a cube is open: when one was opened in a browser -- a person is
        looking at it -- in a terminal, an IDE's run window or anywhere else; never a test or a CI job, which opens no
        browser (browser=False, or none to open)."""
        return not _stays() and self.opened

    def _at_exit(self) -> None:
        """A plain script ending with a cube open in a browser waits, saying so, until Ctrl-C (an IDE's Stop): the
        cube is this process's."""
        with self._lock:
            # a frame's cube in a tab is a page shown there (P12)
            open_cubes = [p for p in self.pages if isinstance(p, PageTab)]
            apps = dict(self.apps)
        if not (open_cubes or apps) or _stays():
            return
        if not self.waits():
            # an app's link only printed (never opened here) ends with the script, as it was asked
            if open_cubes:
                print('the script ended, and its DataCube with it (no cube was opened in a browser)', flush=True)
            return
        names = (['DataCube'] if open_cubes else []) + [f'Legend {app.capitalize()}' for app in apps]
        links = '\n  '.join([c.url for c in open_cubes] + [f'Legend {app.capitalize()}: {url}' for app, url in apps.items()])
        print(f'{" and ".join(names)} {"is" if len(names) == 1 else "are"} still open at\n  {links}\npress Ctrl-C to finish',
              flush=True)
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass


_session = _Session()

# marimo's sessions, each its own (_current): marimo's id for one is its runtime context, kept per notebook session (a
# thread of the server's under `marimo run`)
_marimo_sessions: dict[int, _Session] = {}
_marimo_lock = threading.Lock()


def _current() -> _Session:
    """The session show() works in: the process's, or, in a marimo notebook, its marimo session's own. Under `marimo run`
    every viewer's notebook runs in this one process, a thread each: each gets its own frames, names and engine, so one
    viewer's cube never shows another's frame, and they are let go with the viewer's session (marimo's runtime context,
    collected when marimo lets the session go). marimo has no public name for a session: its runtime context is the
    one its own widgets live by."""
    if not _marimo():
        return _session
    try:
        from marimo._runtime.context import get_context
        context = get_context()
    except Exception:
        return _session
    key = id(context)
    with _marimo_lock:
        session = _marimo_sessions.get(key)
        if session is None:
            session = _marimo_sessions[key] = _Session()
            weakref.finalize(context, _release, key)
        return session


def _release(key: int) -> None:
    """A marimo session gone: its cubes closed, its frames out of their database."""
    with _marimo_lock:
        session = _marimo_sessions.pop(key, None)
    if session is None:
        return
    for cube in list(session.cubes.values()):
        try:
            cube.close()
        except Exception:
            # its channel may have gone with the session: the frame is still let go below
            pass
    if session._headless is not None:
        session._headless.close()
    session.frames.close()


def show(frame: Any, name: str | None = None, *, browser: bool = True,
         inline: bool | None = None, snap: bool = False) -> Cube | DataCube | PageTab | PageCube:
    """Shows DataCube on ``frame`` (a pandas or polars DataFrame, an Arrow table, or a function returning one) and returns
    at once: under the cell in a Jupyter notebook; in a marimo notebook, as the cell's output when it is the cell's last
    expression (marimo's way); in a browser tab anywhere else (a plain script that opened one waits at its end until
    Ctrl-C). ``name`` names its table (``frame``, ``frame_2``, ... unless given; showing a name again
    replaces its frame and shows its cube again, in a tab or under this cell, as it first showed -- in marimo, a new
    cube each call, the latest taking the name's frame). Each query reads the frame as it is then; the grid's Snap
    keeps its rows as they are. ``snap=True``: the frame read once, by the grid's Snap as its page opens it -- a copy
    Python's DuckDB keeps, which the grid reads whatever the frame does after, until its Go Live (the frame stays served
    as it is). ``browser=False`` opens no tab: the link is ``cube.url``.
    ``inline=False`` opens a tab from a notebook's kernel too (a console that shows no widget: Spyder's, qtconsole);
    under the cell needs the notebook extra (``pip install 'legend-lite[notebook]'``)."""
    from .page import Page
    if isinstance(frame, Page):
        if snap:
            raise ValueError("a page's grids snap one by one: grid.snap(), or page.grid(df, snap=True)")
        return _show_page(frame, browser=browser, inline=inline)
    if _marimo() and inline is not False:
        # a marimo notebook: the cube for the cell to show, as its last expression (marimo's way: nothing is added to
        # the cell's output, which would show it twice); the cell's next run closes it
        from .notebook import for_marimo
        return for_marimo(frame, name, snap)
    if inline is None:
        # a name shown again shows where it showed; a new one under the cell in a notebook's kernel, else in a tab
        shown_before = _current().cubes.get(name.lower()) if name is not None else None
        inline = not isinstance(shown_before, Cube) if shown_before is not None else _kernel()
    if inline:
        # the notebook extra: without anywidget, the import says how to install it
        from .notebook import shown
        return shown(frame, name, snap)
    session = _current()
    cube = session.show(frame, name, snap)
    print(f'DataCube: {cube.url}', flush=True)
    if browser and webbrowser.open(cube.url):
        session.opened = True
    return cube


def query(*, browser: bool = True) -> str:
    """Legend Query in a browser tab, served by this Python (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md E): its queries
    planned by this Python's planner and run on its DuckDB, its saved queries those kept beside its pages, its projects
    those in this Python's Depot. Returns the tab's link (``browser=False`` opens none)."""
    return _app('query', browser)


def studio(*, browser: bool = True) -> str:
    """Legend Studio in a browser tab, served by this Python (E): its projects in this Python's SDLC and Depot (the
    repository ``ll.home`` keeps), compiled and run here. Returns the tab's link (``browser=False`` opens none)."""
    return _app('studio', browser)


def _app(app: str, browser: bool) -> str:
    """An app's link on this Python's engine: its first load trades the link's token for the cookie its page then
    carries (engine.py, E)."""
    session = _current()
    with session._lock:
        web = session.web()
        url = f'{web.url}/{app}/demo/index.html?token={web.token}'
        # the web server kept for it, and a plain script's end waits for it as for a cube
        session.apps[app] = url
        session._hook()
    print(f'Legend {app.capitalize()}: {url}', flush=True)
    if browser and webbrowser.open(url):
        session.opened = True
    return url


def _show_page(page: Page, *, browser: bool, inline: bool | None) -> PageTab | PageCube:
    """A page (``ll.Page``) shown: under the cell in a notebook's kernel, as the cell's output in marimo (as a frame is),
    else in a browser tab; live from then on."""
    if _marimo() and inline is not False:
        from .notebook import page_for_marimo
        return page_for_marimo(page)
    if inline is None:
        inline = _kernel()
    if inline:
        from .notebook import shown_page
        return shown_page(page)
    session = page._session
    tab = session.show_page(page)
    print(f'DataCube: {tab.url}', flush=True)
    if browser and webbrowser.open(tab.url):
        session.opened = True
    return tab
