"""DataCube in a notebook: the cube under the cell, a widget (``DataCube``), its calls carried over the widget's own
channel to this process's engine. The design: docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md, "In a notebook".

    cube = ll.show(df)          # in a notebook's kernel: the cube under the cell
    ll.show(df)                 # in a marimo notebook: the cube, as the cell's last expression
    ll.DataCube(df)             # the cube as a widget object (an ipywidgets layout takes it)

Needs the notebook extra, ``pip install 'legend-lite[notebook]'`` (anywidget). The browser never reaches this process
over HTTP: each call the cube makes travels as a message, ``{kind: 'call', id, method, path, query, body}``, answered by
``Engine.answer`` as ``{kind: 'answer', id, status, type, headers}`` with the body as one binary buffer. So the cube
works wherever the notebook does -- this machine, a remote JupyterHub, VS Code, Colab. The frame's version is the
widget's ``version``, which Python sets when the frame changes; the cube reads it again when it moves.

The widget's script is a small loader (DataCube's ``widget-loader.js``): DataCube's module (``widget.js`` and its styles,
``widget.css``, about 0.6 MB) is fetched over the same channel once per notebook page, keyed by its hash (``_module``),
not sent with every widget as anywidget would; its lazy chunks (``widget-chunks/``: a chart's code, ECharts) when the cube
first needs each.
"""

from __future__ import annotations

import hashlib
import sys
import threading
import traceback
from functools import cache
from pathlib import Path
from typing import Any, TypeVar

try:
    import anywidget
    import traitlets
except ModuleNotFoundError as missing:
    raise ModuleNotFoundError(
        "DataCube in a notebook needs anywidget: pip install 'legend-lite[notebook]' "
        '(or ll.show(df, inline=False) for a browser tab)', name=missing.name) from missing

from . import datacube
from .engine import Answer

# DataCube's files for a notebook, in its built site (datacube/BUILD.bazel)
_LOADER = 'widget-loader.js'
_MODULE = ('widget.js', 'widget.css')
_CHUNKS = 'widget-chunks'


@cache
def _loader(site: Path) -> tuple[str, str]:
    """The widget's script (the loader) and the hash its page keeps DataCube's module by."""
    missing = [name for name in (_LOADER, *_MODULE) if not (site / name).is_file()]
    if not (site / _CHUNKS).is_dir():
        missing.append(_CHUNKS + '/')
    if missing:
        raise FileNotFoundError(f"DataCube's notebook files {missing} are not in its site {site}")
    digest = hashlib.sha256()
    # the module, its styles and its lazy chunks (fetched over the channel when the cube first needs each): a page keeps
    # them all by this hash
    chunks = site / _CHUNKS
    for file in [site / name for name in _MODULE] + sorted(chunks.iterdir()):
        digest.update(file.name.encode() + b'\0' + file.read_bytes())
    return (site / _LOADER).read_text('utf-8'), digest.hexdigest()


class _EngineWidget(anywidget.AnyWidget):
    """What a notebook's cube and a notebook's page share: DataCube's module (fetched once a notebook page), its height,
    its calls answered by the session's engine over the widget's channel, and shown once in the cell show() ran in."""

    # anywidget's script, set per widget from DataCube's site (a trait of the class, so anywidget adds none)
    _esm = traitlets.Unicode().tag(sync=True)
    # the hash of DataCube's module: the page fetches it once (widget-loader.js) and keeps it by this
    _module = traitlets.Unicode().tag(sync=True)
    height = traitlets.Int(480).tag(sync=True)

    _session: Any
    _closed: bool
    _shown_here: bool

    def _received(self, _widget: Any, content: Any, _buffers: Any) -> None:
        """A message from the cube's page: a call, answered on a thread of its own, so the kernel never waits on it."""
        if isinstance(content, dict) and content.get('kind') == 'call':
            threading.Thread(target=self._answer, args=(content,), name='legend-lite-call', daemon=True).start()

    def _answer(self, call: dict[str, Any]) -> None:
        try:
            method, path, query, body = call['method'], call['path'], call.get('query', ''), call.get('body')
            if not (isinstance(method, str) and isinstance(path, str) and path.startswith('/')
                    and isinstance(query, str) and (body is None or isinstance(body, str))):
                answer = Answer(400, 'text/plain; charset=utf-8', b'a call is {method, path, query, body} as text')
            else:
                answer = self._session.engine().answer(method, path, query, body)
        except Exception as failure:
            # still an answer: a page waiting on a call never waits forever
            traceback.print_exc()
            answer = Answer(500, 'text/plain; charset=utf-8', f'{type(failure).__name__}: {failure}'.encode())
        if self._closed:
            return
        try:
            self.send({'kind': 'answer', 'id': call.get('id'), 'status': answer.status, 'type': answer.content_type,
                       'headers': dict(answer.headers)}, [answer.body])
        except AttributeError:
            # closed while it answered (ipywidgets drops its channel between its check and its send): no one to tell
            if not self._closed:
                raise

    def _ipython_display_(self, **_: Any) -> None:
        """Displayed as any widget is, but once in the cell ``show()`` displayed it in: there, the cell's own result
        (``ll.show(df)`` as its last line) adds no second copy -- nor does a ``display(cube)`` in that cell, which IPython
        cannot tell apart from it. Typed in a later cell, it shows again."""
        if self._shown_here:
            self._shown_here = False
            return
        from IPython.display import display
        data, metadata = self._repr_mimebundle_()
        display(data, metadata=metadata, raw=True)

    def _after_cell(self) -> None:
        self._shown_here = False


class PageCube(_EngineWidget):
    """A page (``ll.Page``) under a notebook's cell: what ``ll.show(page)`` shows in a notebook -- its sheets, grids and
    charts over the session's frames -- live: a change made to the page in Python opens it again there, a frame updated
    queries again. ``close`` takes it out of the output."""

    # the page's name on the engine (page.json?page=), and its versions -- the page's and each frame's -- as the page
    # follows them (demo/widget.ts)
    page_key = traitlets.Unicode().tag(sync=True)
    versions = traitlets.Dict().tag(sync=True)

    def __init__(self, page: Any, *, height: int = 640) -> None:
        self._closed = True
        session = page._session
        engine = session.shown_engine()
        assert engine.site is not None  # what shows a page has DataCube's site
        loader, module = _loader(engine.site)
        self._session = session
        # the engine the page is served on, kept: its watchers are told from a thread that may hold the frames still,
        # so they ask nothing that takes the session's lock (session.engine() does, as DataCube's own code does)
        self._engine = engine
        self._page = page
        self._shown_here = False
        super().__init__(_esm=loader, _module=module, page_key=page._key, height=height)
        self._closed = False
        self.on_msg(self._received)
        session.serve(page, self)
        self.versions = engine.page_versions(page._key) or {}
        self._unwatch = engine.watch(self._frame_moved)

    @property
    def page(self) -> Any:
        return self._page

    def close(self) -> None:
        """Takes the page out of the output; it is served no more when it is shown nowhere else."""
        if not self._closed:
            self._closed = True
            self._unwatch()
            self._session.close_page(self)
        super().close()

    def _page_moved(self, _version: int) -> None:
        """The page changed in Python: its versions moved, which its page follows (opening the page again)."""
        if not self._closed:
            self.versions = self._engine.page_versions(self._page._key) or {}

    def _frame_moved(self, name: str, _version: int) -> None:
        """A frame changed, told from the thread that changed it -- one serving a query holds the frames still: one of
        the frames the page is served over (the engine's own record of them: never asked of DataCube's code here, which
        takes the session's lock, as registering a frame takes it before the frames') moves its versions."""
        if self._closed:
            return
        versions = self._engine.page_versions(self._page._key) or {}
        if any(f.lower() == name.lower() for f in versions.get('frames', {})):
            self.versions = versions

    def __repr__(self) -> str:
        return f'<DataCube page {self._page.name!r}>' if not self._closed else f'<DataCube page {self._page.name!r}: closed>'


class DataCube(PageCube):
    """DataCube on a frame, under a notebook's cell: what ``ll.show(df)`` shows in a notebook -- a page of one grid over
    it, kept as every Python page is (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md P12: a column changed in the frame keeps
    the grid's edits) -- and a widget object an ipywidgets layout takes. ``name``: as ``show``'s. ``height``: the
    cube's, in pixels (``cube.height = 700`` resizes it). ``snap``: its grid read once, by its Snap. Its handle is a
    tab's: ``update``, ``refresh``, ``close`` (which takes it out of the output)."""

    def __init__(self, frame: Any, name: str | None = None, *, height: int = 480, snap: bool = False) -> None:
        # closed until it is made: ipywidgets closes a widget when it is collected, one whose making failed too, and
        # that close must find nothing to undo (the audit of step 7, S1: a bad frame printed a second traceback)
        self._closed = True
        session = datacube._current()
        self._name, page = session.frame_page(frame, name, snap)
        super().__init__(page, height=height)
        session.adopt(self)

    @property
    def name(self) -> str:
        return self._name

    def update(self, frame: Any) -> None:
        """Shows another frame in this cube: the cube opens it, over its new columns if they changed, the grid's edits
        kept. It returns nothing: a cell ending in ``cube.update(df)`` changes the cube in place and shows no second copy."""
        self._open()
        # its page's own update: each cube over the frame opened again, its edits kept (B10)
        self._page._update(self.name, frame)

    def refresh(self) -> None:
        """Tells the cube to query again: a change the next click would show, shown now."""
        self._open()
        self._session.engine().changed(self.name)

    def close(self) -> None:
        """Takes the frame out of the engine and the cube out of the output. A cube its name has since moved to another
        (shown again in a tab, or made again) takes only itself away: the frame is that other cube's."""
        if not self._closed:
            super().close()
            self._session.close(self.name, self)

    def _open(self) -> None:
        if self._closed:
            raise ValueError(f'the cube {self.name!r} was closed: show the frame again')

    def __repr__(self) -> str:
        return f'<DataCube {self.name!r}>' if not self._closed else f'<DataCube {self.name!r}: closed>'


def for_marimo(frame: Any, name: str | None, snap: bool = False) -> DataCube:
    """``show()`` in a marimo notebook: a new cube over the frame, for the cell to show as its output. marimo carries its
    calls over its own widget channel; when the cell runs again (or is deleted), marimo closes that channel, and the
    cube closes with it -- its frame out of the engine, its name free for the cube the new run shows. marimo has no
    public hook for that: this is its own (``CellLifecycleItem``, by which it closes every widget's channel), held by
    //datacube:marimo_test at the pinned marimo. Without it -- a marimo that moved it, or a show() run outside a cell (a
    UI element's callback) -- the cube works and keeps its frame until ``close()``, and says so once."""
    return _closes_with_its_cell(DataCube(frame, name, snap=snap), 'its frame')


def page_for_marimo(page: Any) -> PageCube:
    """``show(page)`` in a marimo notebook: the page under a new widget, for the cell to show as its output, closed
    with its cell as a frame's cube is (``for_marimo``)."""
    return _closes_with_its_cell(PageCube(page), 'its page served')


_Closable = TypeVar('_Closable', bound=_EngineWidget)


def _closes_with_its_cell(cube: _Closable, kept: str) -> _Closable:
    """``cube`` closed when the marimo cell that made it runs again or is deleted -- through marimo's own hook
    (``CellLifecycleItem``, by which it closes every widget's channel), held by //datacube:marimo_test at the pinned
    marimo. Without it, the cube keeps ``kept`` until ``close()``, and says so once."""
    registry = None
    try:
        from marimo._runtime.cell_lifecycle_item import CellLifecycleItem
        from marimo._runtime.context import get_context
        context = get_context()
        if context.cell_id is not None:
            registry = context.cell_lifecycle_registry
    except (ImportError, AttributeError):
        _warn_once(f'this marimo has no cell lifecycle legend-lite knows: a cube keeps {kept} until cube.close()')
        return cube
    if registry is None:
        _warn_once(f'show() ran outside a marimo cell: its cube keeps {kept} until cube.close()')
        return cube

    class ClosesWithItsCell(CellLifecycleItem):
        def create(self, context: Any) -> None:
            pass

        def dispose(self, context: Any, deletion: bool) -> bool:
            cube.close()
            return True

    registry.add(ClosesWithItsCell())
    return cube


_warned: set[str] = set()


def _warn_once(message: str) -> None:
    if message not in _warned:
        _warned.add(message)
        print(f'legend-lite: {message}', file=sys.stderr, flush=True)


def shown(frame: Any, name: str | None, snap: bool = False) -> DataCube:
    """``show()`` in a notebook: the frame's page (datacube.py ``frame_page``), its name's cube -- the one it has, else a
    new one -- shown under this cell."""
    session = datacube._current()
    cube = session.cubes.get(name.lower()) if name is not None else None
    if isinstance(cube, DataCube) and not cube._closed:
        # its frame replaced (read once when asked): the widget follows its page
        session.frame_page(frame, name, snap)
    else:
        cube = DataCube(frame, name, snap=snap)
    from IPython.display import display
    # shown here, even when it was in this cell already (show() of its name again)
    cube._shown_here = False
    display(cube)
    cube._shown_here = True
    return cube


def shown_page(page: Any) -> PageCube:
    """``show(page)`` in a notebook: the page under this cell, live -- the page's own widget when it has one open (shown
    again here), else a new one."""
    cube = next((v for v in page._shown if isinstance(v, PageCube) and not v._closed), None) or PageCube(page)
    from IPython.display import display
    # shown here, even when it was in this cell already (show(page) again)
    cube._shown_here = False
    display(cube)
    cube._shown_here = True
    return cube
