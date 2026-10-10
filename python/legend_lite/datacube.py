"""DataCube on a dataframe: ``show(df)`` shows it -- under the cell in a notebook, in a browser tab anywhere else --
and returns at once.

    import legend_lite as ll
    cube = ll.show(df)          # DataCube on df, Live: each query reads df as it is then
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
from .frames import LIVE, Frames

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


class Cube:
    """A frame shown in DataCube in a browser tab: its link, and what changes it."""

    def __init__(self, session: _Session, name: str) -> None:
        self._session = session
        self.name = name
        self._closed = False

    @property
    def url(self) -> str:
        """The cube's link: the page, the frame, and the engine's token (in the fragment, never sent to a server)."""
        self._open()
        web = self._session.web()
        return f'{web.url}/engine.html?table={self.name}#token={web.token}'

    def update(self, frame: Any, mode: str | None = None) -> None:
        """Shows another frame in this cube (Live or Snapped as before, unless ``mode`` says): the page opens it, over
        its new columns if they changed. It returns nothing, as a notebook's cube's does."""
        self._open()
        self._session.register(frame, self.name, self._session.frames[self.name].mode if mode is None else mode)

    def refresh(self) -> None:
        """Tells the page to query again: a change the next click would show, shown now."""
        self._open()
        self._session.engine().changed(self.name)

    def close(self) -> None:
        """Takes the frame out of the engine; the page keeps what it shows. The last tab's cube closed stops the web
        server. A cube its name has since moved to another (shown under a notebook's cell) takes only itself away."""
        if not self._closed:
            self._closed = True
            self._session.close(self.name, self)

    def _after_cell(self) -> None:
        pass

    def _open(self) -> None:
        if self._closed:
            raise ValueError(f'the cube {self.name!r} was closed: show the frame again')

    def __repr__(self) -> str:
        return f'<DataCube {self.name!r}: {self.url}>' if not self._closed else f'<DataCube {self.name!r}: closed>'


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


class _Session:
    """The process's one engine, the frames it serves, the cubes over them (one a name: a tab's or a notebook's), the
    pages shown (``ll.Page``), and the web server the tabs reach it through."""

    def __init__(self) -> None:
        self.frames = Frames()
        self.cubes: dict[str, Cube | DataCube] = {}
        # the pages shown, each where it is shown: a tab's or a notebook's
        self.pages: list[PageTab | PageCube] = []
        self._engine: Engine | None = None
        self._web: WebServer | None = None
        self._lock = threading.RLock()
        self._hooked = False
        # a cube was opened in a browser: a person is looking, so a plain script's end keeps the engine for them
        self.opened = False

    def engine(self) -> Engine:
        with self._lock:
            if self._engine is None:
                self._engine = Engine(self.frames, site=_site())
            return self._engine

    def web(self) -> WebServer:
        """The engine over HTTP, for the tabs: started by the first."""
        with self._lock:
            if self._web is None:
                self._web = WebServer(self.engine())
            return self._web

    def register(self, frame: Any, name: str | None, mode: str) -> str:
        """The frame registered under ``name`` (the next free one unless given), and a page showing that name told."""
        with self._lock:
            if name is None:
                name = self._next_name()
            # shown by a cube of its name, or by a grid of a page shown
            shown = name.lower() in self.cubes or any(g.frame.lower() == name.lower()
                                                      for view in self.pages for g in view._page.grids)
            self.frames.register(name, frame, mode)
            engine = self.engine()
            if shown or engine.seen(name):
                # shown again under its name (or again after it was closed): a page showing it reads it again
                engine.changed(name)
            return name

    def unregister(self, name: str) -> None:
        """A frame registered for nothing that came of it (a page's grid refused): out of the engine again."""
        with self._lock:
            if name in self.frames and name.lower() not in self.cubes:
                self.frames.unregister(name)

    def show(self, frame: Any, name: str | None, mode: str) -> Cube:
        """The frame registered, and its name's cube in a tab: the one it has, else a new one."""
        with self._lock:
            name = self.register(frame, name, mode)
            cube = self.cubes.get(name.lower())
            if not isinstance(cube, Cube):
                cube = self.adopt(Cube(self, name))
            return cube

    def adopt(self, cube: Any) -> Any:
        """``cube`` is its name's cube from now on."""
        with self._lock:
            self.cubes[cube.name.lower()] = cube
            self._hook()
            return cube

    def _next_name(self) -> str:
        n = 1
        while (name := 'frame' if n == 1 else f'frame_{n}').lower() in self.cubes or name in self.frames:
            n += 1
        return name

    def after_cell(self, *_: Any) -> None:
        """A notebook cell (or an IPython command) has run: each Live cube, and each Live frame of a page shown, queries
        again, so a change shows."""
        with self._lock:
            for key, cube in list(self.cubes.items()):
                cube._after_cell()
                if self._engine is not None and key in self.frames and self.frames[key].mode == LIVE:
                    self._engine.changed(cube.name)
            for view in self.pages:
                view._after_cell()
            frames = {g.frame for shown in self.pages for g in shown._page.grids}
            for name in frames:
                if self._engine is not None and name in self.frames and self.frames[name].mode == LIVE:
                    self._engine.changed(name)

    def show_page(self, page: Page) -> PageTab:
        """A page served and shown in a tab: a new tab each time it is shown."""
        with self._lock:
            tab = PageTab(self, page)
            self.serve(page, tab)
            return tab

    def serve(self, page: Page, view: PageTab | PageCube) -> None:
        """``page`` served by the engine, shown in ``view`` (a tab's or a notebook's) and told of each change."""
        page._showable()
        with self._lock:
            self.engine().serve_page(page._key, page.to_dict())
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
            if not page._shown and self._engine is not None:
                self._engine.close_page(page._key)
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
        if self._web is not None and not any(isinstance(c, Cube) for c in self.cubes.values()) \
                and not any(isinstance(p, PageTab) for p in self.pages):
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
            open_cubes = [c for c in self.cubes.values() if isinstance(c, Cube)] + [p for p in self.pages if isinstance(p, PageTab)]
        if not open_cubes or _stays():
            return
        if not self.waits():
            print('the script ended, and its DataCube with it (no cube was opened in a browser)', flush=True)
            return
        links = '\n  '.join(c.url for c in open_cubes)
        print(f'DataCube is still open at\n  {links}\npress Ctrl-C to finish', flush=True)
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
    session.frames.close()


def show(frame: Any, name: str | None = None, *, mode: str = LIVE, browser: bool = True,
         inline: bool | None = None) -> Cube | DataCube | PageTab | PageCube:
    """Shows DataCube on ``frame`` (a pandas or polars DataFrame, an Arrow table, or a function returning one) and returns
    at once: under the cell in a Jupyter notebook; in a marimo notebook, as the cell's output when it is the cell's last
    expression (marimo's way); in a browser tab anywhere else (a plain script that opened one waits at its end until
    Ctrl-C). ``name`` names its table (``frame``, ``frame_2``, ... unless given; showing a name again
    replaces its frame and shows its cube again, in a tab or under this cell, as it first showed -- in marimo, a new
    cube each call, the latest taking the name's frame). ``mode``: Live (the default: each query reads the frame as it
    is then) or ``'snapped'`` (copied once). ``browser=False`` opens no tab: the link is ``cube.url``.
    ``inline=False`` opens a tab from a notebook's kernel too (a console that shows no widget: Spyder's, qtconsole);
    under the cell needs the notebook extra (``pip install 'legend-lite[notebook]'``)."""
    from .page import Page
    if isinstance(frame, Page):
        return _show_page(frame, browser=browser, inline=inline)
    if _marimo() and inline is not False:
        # a marimo notebook: the cube for the cell to show, as its last expression (marimo's way: nothing is added to
        # the cell's output, which would show it twice); the cell's next run closes it
        from .notebook import for_marimo
        return for_marimo(frame, name, mode)
    if inline is None:
        # a name shown again shows where it showed; a new one under the cell in a notebook's kernel, else in a tab
        shown_before = _current().cubes.get(name.lower()) if name is not None else None
        inline = not isinstance(shown_before, Cube) if shown_before is not None else _kernel()
    if inline:
        # the notebook extra: without anywidget, the import says how to install it
        from .notebook import shown
        return shown(frame, name, mode)
    session = _current()
    cube = session.show(frame, name, mode)
    print(f'DataCube: {cube.url}', flush=True)
    if browser and webbrowser.open(cube.url):
        session.opened = True
    return cube


def _show_page(page: Page, *, browser: bool, inline: bool | None) -> PageTab | PageCube:
    """A page (``ll.Page``) shown: under the cell in a notebook's kernel, as the cell's output in marimo (as a frame is),
    else in a browser tab; live from then on."""
    page._showable()
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
