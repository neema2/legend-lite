"""A DataCube page built in Python: grids over frames, their charts, sheets, layouts and stacks -- everything the page does
in DataCube, said in code (docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md).

    import legend_lite as ll
    page = ll.Page('Q3 review')
    trades = page.grid(trades_df, name='trades', rows=['region', 'desk'], columns=['year'], dimensions=['year'],
                       measures={'notional': 'sum'}, filter=[('region', 'notEqual', 'APAC')])
    trades.calculate('margin', '$x.pnl->toOne() / $x.notional->toOne()')   # Pure, checked by the compiler here
    by_desk = trades.chart('bar', x='desk', y=[('notional', 'sum')], split='region')
    summary = page.sheet('Summary')
    summary.add(by_desk)
    ll.show(page)                       # under the cell in a notebook, else a browser tab; live from then on
    trades.group('desk')                # the page as it is open now: a change made in DataCube is on it already
    page.save()                         # into this user's saved pages, as DataCube's Save keeps it (Open… lists it)
    page.save('q3.page.json')           # DataCube's own page document (Export > Page File writes the same)
    again = ll.Page.load('q3.page.json', frames={'trades': trades_df})

A page IS its document: DataCube's page document (its own version; datacube/src/page-document.ts), the one copy of it. Its
grids, charts and sheets are handles onto it -- each its id -- whose methods edit it in place, each edit checked at the
line that makes it (a column the frame has, a calculated column the compiler types, a word DataCube's vocabulary has).
So whatever DataCube writes in a page is kept as it is, read back from where it is open by taking the open page's
document (``read``), and the document's own words reach what no method names: ``configure`` (a grid's configuration),
``options`` (a chart's), ``layout_from`` (a sheet's bands). Each grid's cube is over its frame by the frame's name
(``{"_type": "frame"}``), served by this process's engine, as ``show(df)`` serves one. A tile is placed and taken off as
DataCube's layout places it (datacube/src/layout/bands.ts): a grid in a band of its own at the bottom, a chart beside its
grid (as many to a band as DataCube's layout takes), a tile taken off leaving its neighbours closing over its place.
"""

from __future__ import annotations

import contextlib
import copy
import datetime
import functools
import itertools
import json
import math
import numbers
import uuid
import warnings
import weakref
from collections.abc import Mapping, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

from ._extra import require_datacube

# DataCube's: the datacube extra's packages, or one sentence saying to install it (_extra.py)
require_datacube()

from . import _commands, _json
from ._types import is_boolean, is_decimal, is_float, is_integer, is_number
from .keeper import Refused, Settled

_KEEP: Any = object()

# each page's key on its session's keeper and engine (page.json?page=): never given twice in a process, so a page
# collected never shares one with a page made after it
_KEYS = itertools.count(1)


def _operation(run: Any) -> Any:
    """A call on a page, or on one of its tiles or sheets: the page as its keeper has it when the call starts (its
    version pinned for the call, a change made in DataCube meanwhile read only by the next call), and -- the call over --
    what it changed written (Page._changed), or, refused, nothing changed (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md)."""
    @functools.wraps(run)
    def call(self: Any, *args: Any, **kwargs: Any) -> Any:
        with (self if isinstance(self, Page) else self._page)._call():
            return run(self, *args, **kwargs)
    return call


def _operations(cls: Any) -> Any:
    """Each public method and property of ``cls`` a page's call (``_operation``)."""
    for name, value in list(vars(cls).items()):
        if name.startswith('_') or isinstance(value, (classmethod, staticmethod)):
            continue
        if isinstance(value, property):
            setattr(cls, name, property(value.fget and _operation(value.fget), value.fset and _operation(value.fset),
                                        value.fdel, value.__doc__))
        elif callable(value):
            setattr(cls, name, _operation(value))
    return cls


def _expression(expression: Any) -> None:
    """A calculated column's expression, which Python makes its lambda's text of (``x|<expression>``): text, or that text
    would be some other (a number, ``x|5``, a lambda DataCube would take) -- its name and the rest DataCube judges."""
    if not isinstance(expression, str):
        raise ValueError(f'an expression is text, not {expression!r}')


def _store_said(said: str) -> str:
    """What the saved pages' store said refusing a call: its ``{"message"}``, else its answer as it came."""
    try:
        return str(json.loads(said).get('message', said))
    except (ValueError, AttributeError):
        return said  # not the store's {"message"}: said as it came


def _given_name(session: Any, part: str, name: Any, what: str) -> str | None:
    """A name a page, a tile or a sheet is given, by DataCube's one rule (page-names.ts givenName, asked headless:
    nameGiven, as the screen's renames ask it): its text trimmed, a page's or a tile's blank refused in DataCube's words,
    a sheet's blank none of its own (None); what is not text refused in DataCube's words too."""
    said = json.loads(session.headless().call('nameGiven', part, name))
    if 'problems' in said:
        raise ValueError(f'{what}: ' + '; '.join(said['problems']))
    return said['name']


def _plain(value: Any, exact: bool = False) -> Any:
    """Document values as DataCube reads them (cube-document.ts): plain numbers, but in a Pure lambda (a calculated
    column's), whose numbers stay exact (``Decimal``, as the compiler writes them)."""
    if isinstance(value, dict):
        inside = exact or value.get('_type') == 'lambda'
        return {k: _plain(v, inside) for k, v in value.items()}
    if isinstance(value, list):
        return [_plain(v, exact) for v in value]
    if isinstance(value, Decimal) and not exact:
        return float(value)
    return value


def _copied(value: Mapping[str, Any]) -> dict[str, Any]:
    """A copy of document values, as JSON carries them (a value JSON cannot carry refused here)."""
    return _carried(dict(value))


def _carried(value: Any) -> Any:
    """Any value as JSON carries it, whatever its shape: what a chart is told goes to DataCube as given, for DataCube's
    reader to judge in its own words (_made)."""
    return _plain(_json.loads(_json.dumps(value)))


# -- filters ----------------------------------------------------------------------------------------------------------

def _sort_given(sort: Any, what: str) -> dict[str, Any]:
    """A sort as Python writes one -- a column (ascending) or ``(column, 'asc' | 'desc')`` -- as the command carries it;
    what the direction is DataCube judges."""
    if isinstance(sort, str):
        return {'column': sort, 'direction': 'asc'}
    if isinstance(sort, (list, tuple)) and len(sort) == 2:
        return {'column': sort[0], 'direction': sort[1]}
    raise ValueError(f"{what} is a column or (column, 'asc' | 'desc'), not {sort!r}")


def _tagged(value: Any) -> Any:
    """A filter as JSON carries it to DataCube (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D12): its tuples and lists as
    lists, each value JSON has no form for tagged -- a date, a decimal, a timestamp (``{"$date": text}``,
    ``{"$decimal": text}``, ``{"$timestamp": text}``: their exact text), a set (``{"$set": [...]}``), a number JSON cannot
    write (``{"$float": "NaN"}``), as a grid's cells come back tagged (``cells``) -- for DataCube's reader to judge
    (headless gridFilter: readGivenFilter): Python checks none of it. A value of a kind JSON cannot carry is refused as
    any value JSON cannot carry is (``TypeError``)."""
    if isinstance(value, (list, tuple)):
        return [_tagged(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return {'$set': [_tagged(v) for v in value]}
    if isinstance(value, Mapping):
        return {k: _tagged(v) for k, v in value.items()}
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, Decimal):
        return {'$decimal': str(value)}
    if isinstance(value, numbers.Real):
        number = float(value)
        if math.isfinite(number):
            return number
        return {'$float': 'NaN' if math.isnan(number) else ('Infinity' if number > 0 else '-Infinity')}
    if isinstance(value, datetime.datetime):
        return {'$timestamp': value.isoformat()}
    if isinstance(value, datetime.date):
        return {'$date': value.isoformat()}
    raise TypeError(f'a filter carries values JSON can write, not {value!r}')


# -- the handles ------------------------------------------------------------------------------------------------------

@_operations
class Tile:
    """A tile of a page (a grid or a chart): its view in the page's document, by its id."""

    _kind = ''

    def __init__(self, page: Page, id: str) -> None:
        self._page = page
        self._id = id
        # its tile gone (removed, or closed in DataCube): it stays refused, whatever later has its id
        self._dead = False

    @property
    def page(self) -> Page:
        return self._page

    @property
    def id(self) -> str:
        return self._id

    @property
    def _view(self) -> dict[str, Any]:
        """Its view in the document now, or refused: it is no longer on the page (closed there, or removed)."""
        view = None if self._dead else \
            next((v for v in self._page._doc['views'] if v['id'] == self._id and v['kind'] == self._kind), None)
        if view is None:
            raise ValueError(f'{self!r} is no longer on its page')
        return view

    @property
    def title(self) -> str | None:
        return self._view.get('title')

    @title.setter
    def title(self, title: str | None) -> None:
        _ = self._view
        # by the command the screen's rename builds, what is not text refused by its reader, in DataCube's words (a chart has a title; a grid's None takes its own off)
        self._page._apply_page([_commands.page_rename_tile(tile=self._id, title=title)], 'a title')

    @property
    def sheet(self) -> Sheet:
        """The sheet it is on."""
        _ = self._view
        return next(self._page._sheet(s['id']) for s in self._page._doc['sheets']
                    if self._id in self._page._tiles(s['layout']))

    def move_to(self, sheet: Sheet) -> None:
        """Onto another sheet of its page, in a band of its own at the bottom (its charts stay where they are)."""
        sheet.add(self)


@_operations
class Grid(Tile):
    """A grid over a frame: its groupings (``group``), pivot (``pivot``), each column's Column Properties (``column``,
    ``measure``), filter, sorts and calculated columns, as the grid's panels set them; its configuration
    (``configure``); its charts (``chart``)."""

    _kind = 'grid'

    @property
    def _cube(self) -> dict[str, Any]:
        return self._page._cube(self._view['cube'])

    @property
    def _query(self) -> dict[str, Any]:
        return self._cube['query']

    @property
    def frame(self) -> str:
        """The name of the frame it is over -- a grid reading one; a join's grid reads several (``frames``)."""
        frames = self.frames
        if len(frames) != 1:
            raise ValueError(f'this grid reads the frames {", ".join(frames)}: see frames')
        return frames[0]

    @property
    def frames(self) -> list[str]:
        """The frames it reads, as DataCube says what a grid reads (cube-document.ts leavesOf, headless cubeLeaves;
        docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3): a frame's grid its one, a join's each of its leaves'."""
        return _cube_frames(self._page._session, self._cube)

    def _apply(self, commands: list[dict[str, Any]]) -> Grid:
        """The grid edited by commands, each the one DataCube's screen builds for the same edit (grid-commands.ts:
        docs/DATACUBE_COMMANDS_STAGE1_2026_10_18.md), each written by its generated builder (``_commands``: D17) --
        ``grid_group``, ``grid_add_sort``, ``grid_configure`` and the rest -- applied by DataCube's own function in order
        (headless gridApply), the grid then compiled as Apply compiles it: refused in DataCube's words, nothing set; else
        the cube document DataCube writes, its open groups kept as the screen keeps them."""
        self._page._current(self._view['cube'], 5)
        engine, frames = self._headless()
        with frames.reading():
            said = json.loads(engine.call('gridApply', self._spec(engine, frames), _json.dumps(commands)))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return self._kept(said, 4)

    def _kept(self, said: dict[str, Any], stacklevel: int) -> Grid:
        """The cube document DataCube wrote for an edit, kept whole -- never a part of it Python sets itself -- in the page
        as DataCube keeps a grid's edit (headless pageWithCube: its charts following it, each selection its filter no
        longer holds taken off), and what opening it over its frame changed, said in DataCube's words, as a load says
        it."""
        cube = _plain(_json.loads(said['cube']))
        kept = json.loads(self._page._session.headless().call(
            'pageWithCube', _json.dumps(self._page._doc), self._view['cube'], said['cube']))
        if 'problems' in kept:
            raise ValueError('; '.join(kept['problems']))
        self._page._doc = _plain(_json.loads(kept['page']))
        # the commands DataCube applied, as the screen would have built them: the page's record of its edits, and what a
        # page open on it applies to this grid. The cube was opened again over its frames before the edit (``_current``:
        # a frame that changed is its own ``rebuilt`` page command), so what changed of it here is these commands
        applied = [_plain(c) for c in said['commands']]
        self._page._commands.extend(applied)
        self._page._note([{'grid': self._id, 'command': c} for c in applied])
        for note in said['notes']:
            warnings.warn(f"{cube['source']['name']}: {note}", stacklevel=stacklevel)
        self._page._changed()
        return self

    # -- headless: DataCube's own code, in Python (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §4) ------------

    def _headless(self) -> tuple[Any, Any]:
        """The session's headless engine, got first (the session's lock, taken and let go), and the frames: a caller then
        reads them (``Frames.reading``: the frames' lock taken only to begin it, D15) -- the order a frame's registering
        takes the two, so neither waits on the other."""
        session = self._page._session
        return session.headless(), session.frames

    def _spec(self, engine: Any, frames: Any, live: bool = False) -> dict[str, Any]:
        """This grid's cube and its base, as the bundle's grid functions take them (in one read of the frames), asked of
        ``engine`` -- got before the read began (``_headless``): getting it takes the session's lock, which a frame's
        registering takes before the frames'. A snapped grid's base is its copy (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8), unless ``live``: what it is
        a copy of (what a snap pulls, what a class is made of)."""
        return _cube_spec(self._page._session, self._cube, frames, engine,
                          None if live else self._page._snapped(self._view['cube']), self._page._doc, self._view['cube'])

    def view(self) -> GridView:
        """What the grid shows, as DataCube computes it: its columns and rows, their groups and totals, its pivot, and
        what its row cap cut -- by DataCube's own code, run headless in V8 (the `datacube` extra), as the grid on screen
        runs it: its configuration folded into its query, its columns laid out as the screen lays them out."""
        import json

        from ._headless import cells
        engine, frames = self._headless()
        with frames.reading():
            # DataCube's cells are exact as it gives them (an Integer or a Float a number, a Decimal its text): read as
            # JSON reads them, an integer past 2^53 and a non-finite float from their tags
            return GridView(cells(json.loads(engine.call('gridView', self._spec(engine, frames)))))

    @property
    def relation(self) -> str:
        """Its rows at their finest level, as Pure: filtered and extended by its calculated columns, grouped by its
        innermost group when it is grouped (a flat grid's rows as they are) -- the relation DataCube's Create a Class
        makes a class of: over its live base, a snapped grid's too (its copy lasts only as long as this session)."""
        engine, frames = self._headless()
        with frames.reading():
            return engine.call('gridRelation', self._spec(engine, frames, live=True))

    def to_pandas(self) -> Any:
        """Every row of ``relation``, as a pandas DataFrame: DataCube says the query over the grid's base (a frame's, a
        join's, a file's, a class's, a saved query's, a warehouse table's; a snapped grid's, its copy's), and the
        engine's plan-and-run -- the one way every grid's rows are read (D16), the tab's and DataCube's own -- plans it
        and runs it where its rows are, typed by the plan, the rows coming to Python from there (never through
        DataCube's engine)."""
        from . import compiler
        from .frames import to_pandas
        engine, frames = self._headless()
        with frames.reading() as over:
            spec = self._spec(engine, frames)
            tree = compiler.parse(engine.call('gridRelation', spec))
            return to_pandas(_engine_rows(self._page._session, tree, spec, over))

    def export(self, kind: str = 'csv', to: str | Path | None = None) -> str | bytes | Path:
        """The grid as its Export menu exports it, by DataCube's own code (the screen's core, run headless): what it
        shows, with its formats, appearance and heatmaps -- ``'csv'`` (raw values), ``'excel'`` (a workbook, the tree
        as outlines), ``'html'``, ``'text'`` (formatted, to paste) or ``'pdf'``: the kinds its menu offers, a kind it
        does not refused in DataCube's words. The grid alone: the screen's HTML, Excel and PDF carry the page's charts
        beside it, drawn by the browser, which Python does not draw (the S7 audit's G-4, said). Answers the file's content
        (text, or bytes for a workbook or a PDF), or, given ``to``, writes it there and answers the path."""
        import base64
        engine, frames = self._headless()
        with frames.reading():
            said = _json.loads(engine.call('gridExport', self._spec(engine, frames), kind))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        content: str | bytes = said['content'] if said['text'] else base64.b64decode(said['content'])
        if to is None:
            return content
        path = Path(to)
        # the bytes as the export wrote them: CSV's own CRLF, no newline translated on any system
        path.write_bytes(content.encode('utf-8') if isinstance(content, str) else content)
        return path

    def create_class(self, name: str | None = None, *, package: str | None = None,
                     columns: Mapping[str, Any] | None = None, documentation: str | None = None,
                     project: str | None = None, sdlc: str | None = None, writer: str | None = None) -> str:
        """Its rows at their finest level (``relation``) made a class of the page's model by DataCube's Create a Class…,
        run headless (headless ``modelClass``; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z4): with everything it needs to
        run (its relation function, mapping, the frames' store, connection and runtime -- the place ``Python`` -- and its
        first rows as sample data), of its live base -- a snapped grid's too: a class maps the tables its rows are read
        from, never a copy (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8) -- into the page's project in this user's
        model home, made with the page's first class. ``name``: the class's (suggested from its source's name, as the
        window suggests it); ``package``: the page's (suggested from the page's name before its first class).
        ``columns``: by column, ``'property'`` (the default), ``'key'`` (kept for an association to join on, not shown)
        or ``'omit'``, or ``{'as', 'name', 'documentation'}``. ``project``: the name of the project made with the page's
        first class (the page's name unless given, as the window offers it). ``sdlc`` and ``writer``: another SDLC's API
        root (``http://host:port/sdlc/api``) or legend-lite server than this user's model home and this process's
        writer. What it changes of the page applied as DataCube applies it (the project made, the revision pinned, the
        grid's tables and class kept: no step of its history); refused in DataCube's or the writer's words
        (``ValueError``) -- the project it made before then still the page's. Answers the class's path."""
        engine, frames = self._headless()
        given = {'place': 'Python', 'cube': self._view['cube'], 'source': self._cube['source']['name'],
                 **_model_given(name=name, package=package, documentation=documentation, columns=columns,
                                project=project, sdlc=sdlc, writer=writer)}
        with frames.reading():
            said = _json.loads(engine.call('modelClass', _json.dumps(self._page._doc),
                                           self._spec(engine, frames, live=True), given))
        return self._page._model_said(said, 'class')

    def group(self, *rows: str) -> Grid:
        """Grouped by ``rows``, outermost first (the Rows zone): dimensions, as the screen offers them
        (``column(name, kind='dimension')`` makes one); none, not grouped."""
        return self._apply([_commands.grid_group(rows=list(rows))])

    def add_group(self, column: str) -> Grid:
        """Grouped by ``column`` too, last, as the menu's Add Vertical Pivot and the columns panel's pick add it (the
        gesture ``pivot.addVertical``); grouped by already, nothing changes."""
        return self._gesture('pivot.addVertical', {'column': column})

    def remove_group(self, column: str) -> Grid:
        """Grouped by ``column`` no more, the other row groups kept in order, as the menu's Remove Vertical Pivot and a
        drag out of the Rows zone take it off (the gesture ``pivot.removeVertical``); not a row group, nothing changes."""
        return self._gesture('pivot.removeVertical', {'column': column})

    def pivot(self, *columns: str) -> Grid:
        """Pivoted on ``columns`` (the Columns zone): dimensions, as for ``group``; none, not pivoted."""
        return self._apply([_commands.grid_pivot(columns=list(columns))])

    def add_pivot(self, column: str) -> Grid:
        """Pivoted on ``column`` too, last, as the menu's Add Horizontal Pivot adds it (the gesture
        ``pivot.addHorizontal``); pivoted on already, nothing changes."""
        return self._gesture('pivot.addHorizontal', {'column': column})

    def remove_pivot(self, column: str) -> Grid:
        """Pivoted on ``column`` no more, as the menu's Remove Horizontal Pivot and a drag out of the Columns zone take
        it off (the gesture ``pivot.removeHorizontal``); not pivoted on, nothing changes."""
        return self._gesture('pivot.removeHorizontal', {'column': column})

    def heatmap(self, column: str, on: bool = True) -> Grid:
        """A heatmap on ``column`` -- or, ``on=False``, off -- as the header menu's Add Heatmap and Remove Heatmap paint
        it (the gestures ``heatmap.add`` and ``heatmap.remove``): on a pivoted column's measure, so every value's column
        under it has it; a pivot total's its own. The heatmap it has already, or none to take off, changes nothing."""
        return self._gesture('heatmap.add' if on else 'heatmap.remove', {'column': column})

    def gesture(self, name: str, /, **args: Any) -> Grid:
        """Any gesture a person makes on the grid, by its name -- the menu's own ids where the menu has it
        (``'column.pinLeft'``, ``'column.unpinAll'``, ``'pivot.measuresFirst'``, ``'columns.reorder'``, ...: DataCube's
        GRID_GESTURES) -- given its arguments by name (``column='desk'``), made by the one function the screen's control
        calls (grid-gestures.ts gridGesture, headless gridGesture): what a gesture means is DataCube's alone (docs/
        DATACUBE_ONE_API_PLAN_2026_10_21.md S8). Refused in DataCube's words: a gesture a grid has not, an argument it
        does not take or is not given. One that changes nothing -- a column added to the row groups it is in already, a
        setting a column has already, a group it is not in taken out -- makes no command. ``name`` is positional, so a
        gesture's own ``name`` argument (``view.dimension``'s, ``column.editCalculated``'s) can be given."""
        return self._gesture(name, args)

    def _gesture(self, name: str, args: Mapping[str, Any]) -> Grid:
        """The commands gesture ``name`` makes on the grid as it is (headless gridGesture), kept as an edit's are; none,
        the grid as it was."""
        self._page._current(self._view['cube'], 5)
        engine, frames = self._headless()
        with frames.reading():
            # each value JSON has no form for tagged, as a filter's is (D12): DataCube untags it
            said = json.loads(engine.call('gridGesture', self._spec(engine, frames), name, _json.dumps(_tagged(dict(args)))))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        if not said['commands']:
            return self
        return self._kept(said, 4)

    def column(self, name: str, *, kind: str | None = None, aggregate: Any = _KEEP, weight: Any = _KEEP) -> Grid:
        """``name``'s Column Properties, as the panel sets them: its ``kind`` (``'dimension'``, which can be grouped and
        pivoted on, or ``'measure'``; a calculated column's is its editor's), its Aggregation (``aggregate``: one the
        panel offers for its type; a measure's only) and a weighted average's ``weight`` column -- None clears either,
        as the panel's "none" does. What is not given is left as it is."""
        change = {**({'kind': kind} if kind is not None else {}),
                  **({'aggregate': aggregate} if aggregate is not _KEEP else {}),
                  **({'weight': weight} if weight is not _KEEP else {})}
        return self._apply([_commands.grid_column(name=name, change=change)])

    def measure(self, column: str, fn: str, name: str | None = None, *, weight: str | None = None) -> Grid:
        """``column`` aggregated by ``fn`` when the grid groups (one Aggregation offers for its type, and none is assumed:
        ``column(name, kind='measure')`` makes a measure at the one DataCube gives its type; ``wavg`` takes a ``weight``
        column, and only it does): ``column``'s own Aggregation, as Column Properties sets it -- replacing the one it
        had; a measure's, as the panel refuses a dimension's (``column(name, kind='measure')`` makes one) -- unless
        ``name`` names another column: then a calculated column of that name reading ``column``, with that Aggregation,
        as the screen would add one. A grouped ``column`` takes no Aggregation of its own: give the measure a ``name``."""
        measure = {'column': column, 'fn': fn, **({'name': name} if name is not None else {}),
                   **({'weight': weight} if weight is not None else {})}
        # made by the steps a person makes it by -- Column Properties' Aggregation, or the column editor's calculated
        # column and then its Aggregation -- built by DataCube (headless gridMeasure: grid-measures.ts measureCommands;
        # the user, 2026-10-22: one API, no command kind of Python's own)
        self._page._current(self._view['cube'], 4)
        engine, frames = self._headless()
        with frames.reading():
            said = json.loads(engine.call('gridMeasure', self._spec(engine, frames), _json.dumps([measure])))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return self._kept(said, 3)

    def filter(self, condition: Any) -> Grid:
        """Its filter: a condition ``(column, operator[, value])``, a list of them (all of them), a group
        ``('and' | 'or', [...])`` or ``('not', ...)`` -- the filter editor's own conditions, set as it sets them
        (DataCube's code, headless: gridFilter): each operator's value as it takes one (none, one, a list, another
        column), one a column of its type is offered, the grid with it compiled, in the editor's normal form (a group of
        one is that one, an empty one none: ``[]`` takes the filter off, as ``None`` does); ``None`` for none."""
        if condition is None:
            return self._apply([_commands.grid_filter(filter=None)])
        self._page._current(self._view['cube'], 4)
        engine, frames = self._headless()
        with frames.reading():
            said = json.loads(engine.call('gridFilter', self._spec(engine, frames), _json.dumps(_tagged(condition))))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        # the cube with it, set by the filter command the filter editor's Apply builds
        return self._kept(said, 3)

    def sort(self, column: str, direction: str = 'asc') -> Grid:
        """A sort, after those it has, as the header menu's Add Ascending and Add Descending add one (grid-edits.ts
        addSort: a sort it had of ``column`` taken first; one it has already changes nothing): ``column`` (one of the
        grid's columns, or a pivot's header) ``'asc'`` or ``'desc'``."""
        return self._apply([_commands.grid_add_sort(column=column, direction=direction)])

    def toggle_sort(self, column: str, *, chained: bool = False) -> Grid:
        """``column``'s sort turned as a click on its header turns it (DataCube's one rule for the click, headless
        gridGesture's ``header.sort``: header-sort.ts headerSortCommand, grid-edits.ts clickSort -- the user, 2026-10-22:
        one sort API the screen's click and a program call): first in its direction by the type its header has -- a
        number or a date from the largest, anything else from A -- then the other way, then no sort. Alone, every other
        sort taken off; ``chained`` (the screen's Ctrl-click), after the others, keeping its place among them. A column
        the grid shows no header of is refused in DataCube's words."""
        return self._gesture('header.sort', {'column': column, 'chained': chained})

    def toggle_group_sort(self, *, chained: bool = False) -> Grid:
        """The groups' order turned as a click on the tree column's header turns it (DataCube's one rule, headless
        gridGesture's ``header.sort``: grid-edits.ts clickTreeSort): the rows ordered by the groups alone, every other
        sort taken off, the other way from the order they show; ``chained``, with sorts applied, the first group column
        chained after them, as a click chains any column."""
        return self._gesture('header.sort', {'column': None, 'chained': chained})

    def sorts(self, sorts: Sequence[Any]) -> Grid:
        """Its sorts, whole and in order, replacing those it has, as the Sorts panel sets them (a ``sorts`` command,
        grid-edits.ts setSorts): each a column, ascending, or ``(column, 'asc' | 'desc')``; ``[]`` clears them all, as
        the header menu's Clear All Sorts does. Refused, nothing set, in DataCube's words."""
        return self._apply([_commands.grid_sorts(sorts=[_sort_given(s, 'a sort') for s in sorts])])

    def clear_sort(self, column: str) -> Grid:
        """``column``'s sort taken off, the others kept as they are, as the header menu's Clear Sort takes it (a
        ``clearSort`` command, grid-edits.ts clearSort): refused, in DataCube's words, when the grid is not sorted by it."""
        return self._apply([_commands.grid_clear_sort(column=column)])

    def add_filter(self, condition: Any) -> Grid:
        """One condition added to its filter, AND-ed with what it has, as a cell's Add Filter adds one (an ``addFilter``
        command): ``(column, operator[, value])``, built as ``filter`` builds one (DataCube's code, headless:
        gridAddFilter) -- a group is ``filter``'s to set. Refused, nothing set, in DataCube's words."""
        self._page._current(self._view['cube'], 4)
        engine, frames = self._headless()
        with frames.reading():
            said = json.loads(engine.call('gridAddFilter', self._spec(engine, frames), _json.dumps(_tagged(condition))))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return self._kept(said, 3)

    def copy(self, *, title: str | None = None) -> Grid:
        """Copy of Grid: another grid beside this one, on its sheet, a cube of its own over the same source, starting as
        this one is now -- its query and its settings, no group open (DataCube's code, headless gridCopied:
        cube-document.ts cubeCopied) -- titled by its number unless given one, as the screen adds it (an ``addGrid``
        command). Its charts are not copied, as the screen copies none."""
        page = self._page
        said = json.loads(page._session.headless().call('gridCopied', _json.dumps(self._cube)))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        applied = page._apply_page([_commands.page_add_grid(document=_plain(_json.loads(said['cube'])), sheet=self.sheet.id,
                                                            near=self._id, title=title, columns=None)], 'a title')
        return page._tile(applied[0]['id'], Grid)

    def expand(self, *path: str | None) -> Grid:
        """The group at ``path`` open, as its chevron opens it (an ``open`` command): its keys from the top group down,
        each the text the grid shows (None, an empty group) -- its parents open with it. Saved with the page, so a page
        shared or reopened shows it open. A path deeper than the row groups opens nothing."""
        return self._apply([_commands.grid_open(path=list(path), open=True)])

    def collapse(self, *path: str | None) -> Grid:
        """The group at ``path`` closed, as its chevron closes it, the groups under it with it (an ``open`` command)."""
        return self._apply([_commands.grid_open(path=list(path), open=False)])

    def collapse_all(self) -> Grid:
        """Every group closed, the expand level's too, as the menu's Collapse All closes them."""
        return self._apply([_commands.grid_collapse_all()])

    def calculate(self, name: str, expression: str, *, kind: str | None = None, level: str = 'row') -> Grid:
        """A calculated column computed per row, in Pure over the row ``$x`` (``'$x.qty * 2'``; a column that can be
        empty, as a dataframe's can, ``->toOne()`` first for arithmetic), added as the grid's own column editor adds one
        (DataCube's code, headless: gridColumn): its name as DataCube takes one, ``kind`` ``'measure'`` or
        ``'dimension'`` as declared (not given, a measure, as the editor starts a new column -- summed when grouped if it
        is a number, its unique value if not), and the grid with it compiled as DataCube would run it -- reading any of
        the grid's columns before it, a window column's too. ``level='group'``: computed after the grouping instead, over
        each group's row (the editor's Group level: no kind), reading the grid's measures as each level aggregates them.
        A mistake fails at this line, in DataCube's and the compiler's words."""
        _expression(expression)
        return self._column({'name': name, 'expression': f'x|{expression}', 'kind': kind, 'level': level})

    def explode(self, name: str, expression: str, *, kind: str | None = None) -> Grid:
        """A calculated column whose expression gives a list, each row repeated once per element of it, the column that
        element -- the column editor's Explode (DataCube's code, headless: gridColumn): row level only, ``kind`` as for
        ``calculate``."""
        _expression(expression)
        return self._column({'name': name, 'expression': f'x|{expression}', 'kind': kind, 'explode': True})

    def window(self, name: str, fn: str, *, column: str | None = None, partition: Sequence[str] = (),
               order: Sequence[Any] = (), frame: Any = None, offset: int | None = None, buckets: int | None = None,
               level: str = 'row', kind: str | None = None) -> Grid:
        """A window column: ``fn`` over the rows around each row, as the column editor's Window makes one (DataCube's
        code, headless: gridColumn) -- reading ``column`` (an aggregate, ``lag``/``lead``, ``first``/``last``),
        restarting for each value of ``partition``, the rows taken in ``order`` (each a column, ascending, or ``(column,
        'asc' | 'desc')``), over ``frame`` (``'running'``, ``'partition'`` or ``{'lastRows': n}``); ``offset`` for
        ``lag``/``lead``, ``buckets`` for ``ntile``. ``level='group'``: over the grid's groups at each level instead (its
        order, none given, the order the grid shows them in). What ``fn`` takes is DataCube's to judge, in its words."""
        given = {'fn': fn, 'partition': list(partition), 'order': [_sort_given(o, 'an order') for o in order],
                 **({'column': column} if column is not None else {}), **({'frame': frame} if frame is not None else {}),
                 **({'offset': offset} if offset is not None else {}), **({'buckets': buckets} if buckets is not None else {})}
        return self._column({'name': name, 'mode': 'window', 'window': given, 'level': level, 'kind': kind})

    def child_aggregate(self, name: str, fn: str, of: str) -> Grid:
        """A group-level column aggregating ``of`` (a measure) over each group's child groups, ``fn`` (``'min'``,
        ``'max'``, ``'average'``...) -- the column editor's Children (DataCube's code, headless: gridColumn)."""
        return self._column({'name': name, 'mode': 'children', 'level': 'group', 'child': {'fn': fn, 'of': of}})

    def grain(self, column: str, grain: str) -> Grid:
        """The date ``column`` taken to its period (``'day'``, ``'week'``, ``'month'``, ``'quarter'``, ``'year'``), a new
        dimension, as the header menu's By Month adds one (DataCube's code: grid-presets.ts presetCommands) -- named
        ``<column>_<grain>``, or that with ``_2``... where the grid has a column of the name."""
        return self._preset({'kind': 'grain', 'column': column, 'grain': grain})

    def show_as(self, column: str, kind: str) -> Grid:
        """The measure ``column`` shown as ``kind`` (``'runningTotal'``, ``'percentOfTotal'``, ``'difference'``,
        ``'percentChange'``, ``'movingAverage'``, ``'rank'``) along the grid's row groups, as group-level columns, as the
        header menu's Show As adds them (grid-presets.ts presetCommands) -- named ``<column>_<kind>``."""
        return self._preset({'kind': 'showAs', 'column': column, 'showAs': kind})

    def bins(self, column: str, *, width: float | None = None, count: int | None = None) -> Grid:
        """The number ``column`` taken to its bin -- ``width`` wide, or ``count`` bins over its range -- a new dimension
        (and its bin's upper edge), the columns a chart's bins are (calc-presets.ts binColumns, by grid-presets.ts
        presetCommands), named ``<column>_bins``. The screen's Bins opens the column editor on the same expression."""
        return self._preset({'kind': 'bins', 'column': column, 'bins': {
            **({'width': width} if width is not None else {}), **({'count': count} if count is not None else {})}})

    def dimension(self, name: str) -> Grid:
        """Grouped by the named dimension ``name``'s first level (its configuration's ``dimensions``: a name and its
        columns, coarsest first), as the menu's View > Dimensions uses one (the gesture ``view.dimension``:
        dimensions.ts useDimension)."""
        return self._gesture('view.dimension', {'name': name, 'move': 'use'})

    def drill(self, name: str) -> Grid:
        """One level deeper into the named dimension ``name`` (dimensions.ts drillDown): its next column grouped by."""
        return self._gesture('view.dimension', {'name': name, 'move': 'down'})

    def drill_up(self, name: str) -> Grid:
        """One level back out of the named dimension ``name`` (dimensions.ts drillUp); out of its first, not grouped."""
        return self._gesture('view.dimension', {'name': name, 'move': 'up'})

    def edit_calculated(self, name: str, expression: str, *, rename: str | None = None, kind: str | None = None) -> Grid:
        """The calculated column ``name`` computed per row by ``expression`` instead, as its Edit Column window's OK
        changes it (an ``editCalculated`` command, built headless as the editor builds it: gridColumn): ``rename`` its
        new name -- what read it (the groups, the pivot, the sorts, the filter, the columns computed from it, its
        settings) reads it by that -- and ``kind`` as for ``calculate``. In its place among the grid's columns."""
        return self._calculated(name if rename is None else rename, expression, kind, name)

    def remove_calculated(self, name: str) -> Grid:
        """The calculated column ``name`` taken out, as Edit Column's Delete takes it (a ``removeCalculated`` command):
        refused, in the compiler's words, while another column reads it."""
        return self._apply([_commands.grid_remove_calculated(name=name)])

    def _calculated(self, name: str, expression: str, kind: str | None, replacing: str | None) -> Grid:
        _expression(expression)
        return self._column({'name': name, 'expression': f'x|{expression}', 'kind': kind, 'replacing': replacing})

    def _column(self, given: Mapping[str, Any]) -> Grid:
        """A column made as the column editor makes one (headless gridColumn: ``given`` its form), the cube with it kept
        as DataCube writes the editor's OK -- the column typed, the state folded -- or refused in its words."""
        self._page._current(self._view['cube'], 5)
        engine, frames = self._headless()
        with frames.reading():
            said = json.loads(engine.call('gridColumn', self._spec(engine, frames), _json.dumps(given)))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return self._kept(said, 4)

    def _preset(self, preset: Mapping[str, Any]) -> Grid:
        """A preset's columns added as the header menu adds them (headless gridPreset), or refused in DataCube's words."""
        self._page._current(self._view['cube'], 4)
        engine, frames = self._headless()
        with frames.reading():
            said = json.loads(engine.call('gridPreset', self._spec(engine, frames), _json.dumps(preset)))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return self._kept(said, 3)


    def configure(self, settings: Mapping[str, Any]) -> Grid:
        """Any of the grid's configuration (its Properties), in its saved document's own keys (``reportTitle``,
        ``maxRows``, ``appearance``, ``columns``: each column's formats and colours, ...), set as the screen's Properties
        set them (a ``configure`` command: the appearance and each column's merged into what they have; None clears a
        setting) and folded into its query as the screen folds it, by a configure command (headless gridApply). A
        column's ``kind``, ``aggregateFn`` and ``aggregationParameters`` go by Column Properties' rules, as ``column()``
        does: a kind is set, never cleared; ``excludedFromPivot`` is True or False."""
        return self._apply([_commands.grid_configure(patch=_copied(settings))])

    def chart(self, mark: str, *, x: Any = _KEEP, y: Sequence[Any] | None = None,
              split: Any = _KEEP, title: str | None = None, frozen: bool | None = None,
              options: Mapping[str, Any] | None = None, grain: str | None = None,
              bins: Mapping[str, float] | None = None, tooltip: Sequence[Any] | None = None,
              layers: Sequence[Mapping[str, Any]] | None = None, label: Any = None, size: Any = None,
              facet: Mapping[str, str] | None = None, at: Mapping[str, str] | None = None) -> Chart:
        """A chart of this grid, beside it: ``mark`` (a mark DataCube draws), its category ``x``, what it plots ``y`` --
        ``[(column, aggregate), ...]``, the measure DataCube's own new chart would plot unless given -- and a ``split``
        (one series per value), each DataCube's own new chart's unless given (None: none); ``frozen`` keeps its own
        grouping rather than following the grid's (and stays when the grid is removed, as in DataCube) -- unless told, a
        chart told its ``x``, ``y`` or ``split`` keeps them, as a hand on them in DataCube's options freezes the chart,
        and one told none of them follows the grid (told them and ``frozen=False``, one the grid would replace is
        refused); ``options``: the chart's Options, in its spec's own keys (one left out is its default).

        The spec's slots, each in its own keys: a measure of ``y`` may be a dict -- ``column``, ``fn`` (an aggregate, or
        ``'value'``: a group-level column of the grid, plotted as it is), its own ``mark``, ``axis`` and ``showAs``
        (``{'kind': ...}``); ``grain`` takes a date across to its period; ``bins`` a number across to its bins
        (``{'width': w}`` or ``{'count': n}``); ``label`` a measure written on each mark instead of its own value;
        ``tooltip`` further measures a hover lists; ``layers`` reference lines, bands, thresholds and pinned extremes
        (each a dict, its ``kind`` saying which); ``size`` a scatter's third column, each point sized by it (a bubble);
        ``facet`` small multiples of bars, lines or areas, a chart per value of ``{'column': ...}`` and/or
        ``{'row': ...}``. A ``'boxplot'`` plots one number column's five figures -- ``min``, ``percentile25``,
        ``median``, ``percentile75``, ``max``, in that order -- taking those of the measure it would plot unless ``y``
        says them. A ``'map'`` colours each country its ``x`` names (an ISO code or a name); a ``'bubblemap'`` puts a
        bubble at each place ``x`` names, where ``at={'lat': ..., 'lon': ...}`` -- two number columns -- says it is.
        Every word is DataCube's (datacube/src/chart-spec.ts): one it does not know -- a mark, an
        axis, a show-as, a grain, a layer's kind or statistic -- is refused by its reader, naming the words it takes.

        DataCube makes the chart -- its own new chart, changed as its options form changes one (a date across runs in time
        order) -- and judges it as it will draw it, by its own rules run in V8 (the datacube extra; the bundle's
        gridChart): what it would refuse to draw is refused here, in its words, and nothing is kept."""
        given = {**_given(mark, x, y, split, frozen), **({'options': _carried(options)} if options is not None else {}),
                 **_slots(grain, bins, tooltip, layers, label, size, facet, at)}
        # made as DataCube makes a new chart (what is not given is its own default), and judged as it will draw it: what
        # DataCube would refuse to draw is refused here, in its words, before the chart is kept
        spec = _made(self._page, self._view['cube'], given)
        # beside this grid, titled by its number unless given one, as the screen adds a chart (page-commands.ts addChart)
        applied = self._page._apply_page([_commands.page_add_chart(near=self._id, spec=spec, title=title, columns=None)],
                                         'a title')
        return self._page._tile(applied[0]['id'], Chart)

    @property
    def charts(self) -> list[Chart]:
        """The charts of this grid, on any sheet."""
        cube = self._view['cube']
        return [c for c in self._page.charts if c._view['cube'] == cube]

    def update(self, frame: Any) -> None:
        """Another frame for this grid (one reading a single frame: ``frame``): a page showing it queries it again, and
        -- its columns changed -- opens again over them, every grid reading the frame with it (a snapped one reads its
        copy still, until it goes live)."""
        _ = self._view
        self._page._update(self.frame, frame)

    # -- Snap (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8): where its rows are read from, as page state ----------------

    def snap(self) -> Grid:
        """Its rows read once: its base query (its every column, as DataCube's Snap pulls it: headless gridSnapPull)
        copied by this session's engine -- as a tab's Snap copies one through it (engine.py ``snap_copy``), a
        warehouse table's pulled on the warehouse -- and the grid reading the copy from then on (the page's ``snap``
        command, written as DataCube's Snap writes it: headless gridSnapped): every call on it, and the page shown,
        read the copy, whatever its frames do meanwhile. Snapped already, it is snapped again (the copy taken anew,
        the old one let go). Refused in the engine's words (a copy past the snap limit, say), the grid as it was."""
        _ = self._view
        page, session = self._page, self._page._session
        engine, frames = self._headless()
        with frames.reading():
            spec = self._spec(engine, frames, live=True)
            pull = engine.call('gridSnapPull', spec)
        # copied outside the frames: the engine holds them itself, on its own thread
        made = session.engine().snap_copy(spec['model'], spec['runtime'], pull)
        try:
            with page._batch(tell=False):
                page._apply_page([json.loads(engine.call('gridSnapped', self._id, _json.dumps(made)))], 'a snap')
            # written: the page reads the copy from now on (a copy it read before let go then: the engine's sweep)
            page._changed()
        except BaseException:
            # the copy belongs to no page: let go
            session.engine().release_copy(made['table'])
            raise
        return self

    def live(self) -> Grid:
        """Its rows read live again: the page's ``live`` command (as DataCube's Go Live), its copy let go, and the grid
        opened over its frames as they are now (what changed meanwhile said, as a load says it). Refused when it reads
        them live already."""
        _ = self._view
        page = self._page
        with page._batch():
            page._apply_page([_commands.page_live(grid=self._id)], 'go live')
            page._current(self._view['cube'], 3)
        return self

    @property
    def snapped(self) -> dict[str, Any] | None:
        """The copy it reads when it is snapped, as the page writes it down -- its ``label`` ("Snap 14:02"), when it was
        ``takenAt`` (an ISO instant), its ``rowCount``, its ``table``, and its ``pulledBy`` when it was pulled from a
        warehouse -- or None: it reads its rows live."""
        copy = self._page._doc.get('copies', {}).get(self._view['cube'])
        return None if copy is None else _copied(copy)

    def __repr__(self) -> str:
        return f'<Grid {self._id}>'


def _given(mark: str, x: Any, y: Sequence[Any] | None, split: Any, frozen: bool | None) -> dict[str, Any]:
    """What a chart was told, in its spec's own keys (chart-spec.ts): DataCube makes the chart of it and judges it
    (_made), so nothing here is a rule of the chart's -- only Python's arguments turned into the spec's keys (an x or a
    split told None is null: none; frozen told False is null: the chart taken live). Its version is DataCube's: the
    chart it starts from (its own new chart, or the one edited) carries it."""
    return {'mark': mark, **({'x': x} if x is not _KEEP else {}),
            **({'y': _measures(y)} if y is not None else {}),
            **({'split': split} if split is not _KEEP else {}),
            **({'frozen': True if frozen else None} if frozen is not None else {})}


def _measure(plotted: Any) -> Any:
    """One measure of a chart in the spec's own keys: a ``(column, aggregate)`` pair is ``{'column', 'fn'}``, Python's
    shorthand; anything else goes as given (a dict may say its own ``mark``, ``axis`` and ``showAs``), for DataCube's
    reader to judge in its words (_made)."""
    if isinstance(plotted, tuple) and len(plotted) == 2:
        return {'column': _carried(plotted[0]), 'fn': _carried(plotted[1])}
    return _carried(plotted)


def _measures(plotted: Any) -> Any:
    """Measures in the spec's own keys (_measure each), or what was given when it is no list, for DataCube to judge."""
    return [_measure(p) for p in plotted] if isinstance(plotted, (list, tuple)) else _carried(plotted)


def _made(page: Page, cube: str, given: Mapping[str, Any], before: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """The chart DataCube makes of what it was told -- what is not told is its own new chart's, and an edit of ``before``
    keeps the rest -- judged as DataCube will draw it (a live chart regrouped the way its grid is) against the chart's cube
    as DataCube opens it: its frame's columns and its calculated ones as the compiler types them now (the bundle's
    gridChart; chart-spec.ts chartFrom, followCube and chartProblems). The chart rules are written once, there: what
    DataCube would refuse to draw is refused here, in its words, and nothing is kept. A frozen chart whose grid was
    removed is asked against its cube, which stays with it. (V8 is the datacube extra's, as this module is: _extra.py.)"""
    page._current(cube, 5)
    session = page._session
    engine, frames = session.headless(), session.frames
    with frames.reading():
        made = _json.loads(engine.call('gridChart', _cube_spec(session, page._cube(cube), frames, engine, page._snapped(cube)),
                                       _json.dumps(given), None if before is None else _json.dumps(before)))
    if made['problems']:
        raise ValueError(' '.join(made['problems']))
    return made['spec']


def _cube_spec(session: Any, cube: Mapping[str, Any], frames: Any, engine: Any,
               snapped: Mapping[str, Any] | None = None, page: Mapping[str, Any] | None = None,
               id: str | None = None) -> dict[str, Any]:
    """A cube document and its base, as the bundle's grid functions take them (in one read of the frames): its base on the
    host as DataCube says it is (headless gridBaseOn, the one rule: docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z15) -- over
    the frames this session serves (a frame's own model, a join's of frames one model of them; never by the cube's kind
    here), or, its grid ``snapped`` (the page's copy for it), over the copy as its engine modelled it as it landed,
    refused in DataCube's words when the session no longer holds it (never read live in its place). The cube document
    never carries its base, so one opened over its copy is written as the live one is. ``page`` and ``id``, the page it
    is on and its id there: what the page's filters are to it is said with its base (``lens``), as the screen's page
    gives its grid (page-filters.ts gridLens: B1)."""
    text = _json.dumps(cube)
    said = json.loads(engine.call('gridBaseOn', text, None if snapped is None else _carried(snapped), frames.hosted(),
                                  session.held_copies(), None if page is None else _json.dumps(page), id))
    if 'problems' in said:
        raise ValueError('; '.join(said['problems']))
    return {'cube': text, **said}


def _engine_rows(session: Any, tree: Any, base: Mapping[str, Any], over: Any) -> Any:
    """A query's rows planned and run by the session's engine where they are (its plan-and-run: the frames' database as
    the read ``over`` sees them, D15; a warehouse; a saved query's project's own, as its ``base`` from DataCube says:
    F28), as Arrow typed by the plan -- every grid's rows, by one plan-and-run (D16). Refused in the planner's or the
    database's words."""
    from . import compiler
    asked = {'lambda': _json.dumps(tree), 'model': base['model'], 'runtime': base['runtime'],
             **({'project': True} if base.get('project') is True else {})}
    status, kind, body = session.engine().answer_over('/plan-and-run.json', json.dumps(asked), over)
    if status == 200 and not kind.startswith('application/json'):
        import pyarrow.ipc
        return pyarrow.ipc.open_stream(body).read_all()
    try:
        said = json.loads(body)
    except ValueError:
        raise ValueError(body.decode('utf-8', errors='replace')) from None
    if isinstance(said, dict) and said.get('error'):
        raise ValueError(said['error'])
    if isinstance(said, dict) and isinstance(said.get('plan'), str):
        compiler.read_planned(said['plan'])
    raise ValueError(body.decode('utf-8', errors='replace'))


def _cube_leaves(session: Any, cube: Mapping[str, Any]) -> list[dict[str, str]]:
    """A cube's leaves as DataCube says what a cube reads (headless cubeLeaves: cube-document.ts leavesOf), in its order:
    each ``{"_type", "name"}`` -- a frame's name its table in the frames' database."""
    return json.loads(session.headless().call('cubeLeaves', _json.dumps(cube)))


def _cube_frames(session: Any, cube: Mapping[str, Any]) -> list[str]:
    """The frames a cube reads, as DataCube says (its leaves), each once, in its order."""
    return list(dict.fromkeys(leaf['name'] for leaf in _cube_leaves(session, cube) if leaf['_type'] == 'frame'))


def _slots(grain: str | None, bins: Mapping[str, float] | None, tooltip: Sequence[Any] | None,
           layers: Sequence[Mapping[str, Any]] | None, label: Any = None, size: Any = None,
           facet: Mapping[str, str] | None = None, at: Mapping[str, str] | None = None) -> dict[str, Any]:
    """The spec's version 2 slots a chart was given, in the spec's own keys and as given: DataCube's reader and rules
    judge each (_made), in their words."""
    given = {'size': None if size is None else _measure(size), 'facet': facet, 'grain': grain, 'bins': bins,
             'label': None if label is None else _measure(label),
             'tooltip': None if tooltip is None else _measures(tooltip), 'layers': layers, 'at': at}
    return {key: _carried(value) for key, value in given.items() if value is not None}


class GridView:
    """A grid's view, as DataCube computed it (``Grid.view()``): its ``columns`` (each a dict: ``name``, ``path``, its
    pivot's values and the measure, ``type``, ``dimension``; a grouped grid's first is ``__tree``, its groups' labels,
    as on screen), its ``rows`` (each a list, one value per column, numbers exact: a Decimal's ``Decimal``), their
    ``tree`` (each row's ``path``, ``level``, and whether it is a ``group``, ``expanded`` or a ``total``),
    the levels the row cap ``truncated``, a flat grid's ``total_rows`` when the cap cut it, and the ``query`` and ``sql``
    that answered it."""

    def __init__(self, view: Mapping[str, Any]) -> None:
        self.columns: list[dict[str, Any]] = list(view['columns'])
        # each value as its column's type says (by legend-lite's lattice, either compiler's name: _types): a Decimal's
        # text (DataCube's exact decimals) a Decimal -- a Number's too, which is one when the database answered an exact
        # decimal (an Integer's quartile) --, a Float a float even when JSON wrote it as a whole number (10.0 is
        # written 10)
        def typed(v: Any, type: str) -> Any:
            if (is_decimal(type) or is_number(type)) and isinstance(v, str):
                return Decimal(v)
            if is_float(type) and isinstance(v, int) and not isinstance(v, bool):
                return float(v)
            return v
        types = [c['type'] for c in self.columns]
        self.rows: list[list[Any]] = [[typed(v, t) for v, t in zip(row, types)] for row in view['rows']]
        self.tree: list[dict[str, Any]] = list(view['tree'])
        self.truncated: list[dict[str, Any]] = list(view['truncated'])
        self.total_rows: int | None = view.get('totalRows')
        self.query: str = view['query']
        self.sql: str = view['sql']

    def to_pandas(self) -> Any:
        """The rows as the grid shows them, a DataFrame with one column per column shown: an Integer or a Boolean
        column with nulls an integer or a boolean still (pandas' nullable types, as ``frames.to_pandas``)."""
        import pandas as pd

        def column(i: int, type: str) -> Any:
            values = [row[i] for row in self.rows]
            integer = is_integer(type)
            # an Integer past 64 bits kept exact, as Python's own rows keep it (frames.to_pandas): pandas' objects
            if integer and any(isinstance(v, int) and not -2 ** 63 <= v < 2 ** 63 for v in values):
                return pd.Series(values, dtype=object)
            nullable = pd.Int64Dtype() if integer else pd.BooleanDtype() if is_boolean(type) else None
            return pd.Series(values, dtype=nullable)
        # by position: a pivot's columns may share a name
        frame = pd.DataFrame({i: column(i, c['type']) for i, c in enumerate(self.columns)})
        frame.columns = [c['name'] for c in self.columns]
        return frame

    def __repr__(self) -> str:
        return f'<GridView: {len(self.rows)} rows, {len(self.columns)} columns{", truncated" if self.truncated else ""}>'


@_operations
class Chart(Tile):
    """A chart of a grid: its mark, what it plots, its options; following its grid unless frozen."""

    _kind = 'chart'

    def grid(self) -> Grid:
        """Its grid, as DataCube's Show Grid shows it (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D7; DataCube's code,
        headless chartOwnGrid: chart-spec.ts chartGrid, the screen's rule): the grid it follows; or -- frozen, or detached
        (its grid removed) -- a grid of its own grouping and query, made beside it, which it follows from then on, drawn
        as it was: its column across and its split the grid's rows, its values the grid's Column Properties, its
        selection, and on a grid over a class its page filter, taken off as ``select(None)`` would (followGrid's own
        rule, the screen's Show Grid's too). Refused in DataCube's words for a
        chart with no grouping of its own (a scatter, a treemap, a map, a gauge)."""
        view = self._view
        page, cube = self._page, view['cube']
        page._current(cube, 3)
        session = page._session
        engine, frames = session.headless(), session.frames
        with frames.reading():
            said = json.loads(engine.call('chartOwnGrid', _cube_spec(session, page._cube(cube), frames, engine,
                                                                     page._snapped(cube)),
                                          _json.dumps(page._doc), self._id))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        if 'grid' in said:
            return page._tile(said['grid'], Grid)
        # one step, as the screen's Show Grid is: the grid added beside the chart, then Show Grid's steps by the one rule
        # the screen's asks (headless pageGesture: page-gestures.ts showGridCommands) -- the chart on its cube (followGrid:
        # its selection and its page filter off by the command's own rule), following it as it was drawn (setChart)
        with page._batch():
            applied = page._apply_page([_commands.page_add_grid(document=_plain(_json.loads(said['cube'])),
                                                                sheet=self.sheet.id, near=self._id, title=None,
                                                                columns=None)])
            made = applied[0]['id']
            steps = json.loads(session.headless().call('pageGesture', 'chart.showGrid', _json.dumps(
                {'chart': self._id, 'grid': made, 'spec': said['spec']})))
            if 'problems' in steps:
                raise ValueError('; '.join(steps['problems']))
            page._apply_page([_plain(c) for c in steps['commands']])
        for note in said['notes']:
            warnings.warn(f'{self._id}: {note}', stacklevel=3)
        return page._tile(made, Grid)

    def plot(self, mark: str | None = None, *, x: Any = _KEEP, y: Any = _KEEP, split: Any = _KEEP,
             frozen: bool | None = None, at: Any = _KEEP) -> Chart:
        """What it plots changed, as DataCube's options form changes it (chart-spec.ts editChart): its ``mark``, ``x``
        (None: none), ``y``, ``split`` (None: none), a bubble map's places ``at`` (``{'lat': ..., 'lon': ...}``; None:
        none) or ``frozen`` (False: following its grid -- a detached chart, its grid removed, has none to follow, and
        DataCube refuses it); what is not given stays, a new column across takes its own order and a grain or bins go
        with the old one, and a live chart told its ``x``, ``y`` or ``split`` freezes unless told ``frozen``. DataCube
        makes the edit and judges it (_made): a detached chart against its cube, which stays."""
        view = self._view
        given = {**({'mark': mark} if mark is not None else {}), **({'x': x} if x is not _KEEP else {}),
                 **({'y': _measures(y)} if y is not _KEEP else {}),
                 **({'split': split} if split is not _KEEP else {}),
                 **({'at': None if at is None else _carried(at)} if at is not _KEEP else {}),
                 **({'frozen': True if frozen else None} if frozen is not None else {})}
        self._page._apply_page([_commands.page_set_chart(chart=self._id,
                                                         spec=_made(self._page, view['cube'], given, view['spec']))])
        return self

    def options(self, settings: Mapping[str, Any]) -> Chart:
        """The chart's Options (``orientation``, ``stack``, ``sort``, ``limit``, ``labels``, ``legend``), in its spec's own
        keys; one left out is the chart's default. DataCube makes the edit and judges it (_made)."""
        view = self._view
        spec = _made(self._page, view['cube'], {'options': _carried(settings)}, view['spec'])
        self._page._apply_page([_commands.page_set_chart(chart=self._id, spec=spec)])
        return self

    # -- what a click does: a selection, a drill (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D8) -----------------------------

    def select(self, value: Any, *, split: Any = _KEEP) -> Chart:
        """Its mark ``value`` selected -- named by its value across and, on a chart split by a column, its series'
        (``split``) -- as a click on the mark selects it (DataCube's code: headless chartPick): the mark's conditions (a
        drilled chart's path with them) put on its grid's filter in place of its last selection's, or, its grid over a
        class, those on the class's properties a page filter from the chart, reaching every other grid over the class
        (page-filters.ts classPick). Named, never toggled: the same mark again stays selected. ``None``: the selection
        cleared. Refused in DataCube's words: a chart whose marks are no values across (a scatter, a treemap), a period
        or a bin across (a range, ending where the database says), a detached chart (its grid removed: it filters
        none)."""
        page = self._page
        _ = self._view
        said = json.loads(page._session.headless().call('chartPick', _json.dumps(page._doc), self._id, _json.dumps(
            {'value': _tagged(value), **({'split': _tagged(split)} if split is not _KEEP else {})})))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        # as the screen tells a click: its grid's refilter, then the page's commands (its page filter, its select)
        with page._batch():
            if said['commands']:
                page._tile(said['grid'], Grid)._apply(said['commands'])
            if said['page']:
                page._apply_page(said['page'])
        return self

    @property
    def selection(self) -> list[dict[str, Any]]:
        """What its selected mark put on its grid's filter, as the page keeps it (a copy): its conditions, each
        ``{"kind": "condition", "column", "operator", "value"}``; none, nothing selected. A mark's conditions on a class's
        properties are its page filter instead (``page.filters``, ``from`` this chart)."""
        return copy.deepcopy(self._view.get('selection', []))

    def drill(self, value: Any) -> Chart:
        """One level down its grid's groupings, into the mark ``value`` across (None: the empty one), as a double-click
        on the mark drills (chart-spec.ts drillDown: headless chartDrill): the chart then across the grouping below,
        within ``value``. Refused in DataCube's words: a chart that does not drill (a period across, a column the grid
        does not group by, the last grouping), or one drilled to its last level."""
        return self._drilled('down', {'value': _tagged(value)})

    def drill_up(self, levels: int = 1) -> Chart:
        """Back up ``levels`` levels, as its crumbs go back (chart-spec.ts drillUp: headless chartDrill). Refused in
        DataCube's words when it is not drilled that far."""
        return self._drilled('up', {'levels': levels})

    @property
    def drilled(self) -> dict[str, Any]:
        """Where it is drilled, as the page keeps it: each grouping above the one shown, and the value drilled into
        there, in order; empty, at its top level."""
        drill = self._view['spec'].get('drill')
        return {} if not drill else dict(zip(drill['levels'], copy.deepcopy(drill['path'])))

    def _drilled(self, move: str, given: Mapping[str, Any]) -> Chart:
        page = self._page
        _ = self._view
        said = json.loads(page._session.headless().call('chartDrill', _json.dumps(page._doc), self._id, move,
                                                        _json.dumps(given)))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        page._apply_page([said['command']])
        return self

    def __repr__(self) -> str:
        return f'<Chart {self._id}>'


@_operations
class Sheet:
    """A sheet of a page: a whole screen of tiles, in places -- each a tile, or a stack of tiles shown one at a time --
    laid out in bands, each band's places side by side (``layout``, ``arrange``, ``layout_from``)."""

    def __init__(self, page: Page, id: str) -> None:
        self._page = page
        self._id = id
        self._dead = False

    @property
    def page(self) -> Page:
        return self._page

    @property
    def id(self) -> str:
        return self._id

    @property
    def _doc(self) -> dict[str, Any]:
        sheet = None if self._dead else next((s for s in self._page._doc['sheets'] if s['id'] == self._id), None)
        if sheet is None:
            raise ValueError(f'{self!r} is no longer on its page')
        return sheet

    @property
    def name(self) -> str | None:
        """Its own name; None: named after its first grid, as DataCube names a sheet."""
        return self._doc.get('name')

    @name.setter
    def name(self, name: str | None) -> None:
        _ = self._doc
        # what is not text refused by renameSheet's reader, in DataCube's words
        self._page._apply_page([_commands.page_rename_sheet(sheet=self._id, name=name)], "a sheet's name")

    def move(self, position: int) -> Sheet:
        """Moved among the page's sheets to ``position`` (0 the first), the others keeping their order, as its tab's Move
        Left and Move Right move it (a ``moveSheet`` command): a place past either end is that end."""
        _ = self._doc
        # a place that is no whole number refused in DataCube's words (moveSheet's rule: the S7 audit's Y-11, Python had
        # spelled the refusal itself)
        self._page._apply_page([_commands.page_move_sheet(sheet=self._id, to=position)])
        return self

    @property
    def tiles(self) -> list[Tile]:
        """Its tiles in reading order, a stack's in its tabs' order."""
        return [self._page._tile_of(t) for t in self._page._tiles(self._doc['layout'])]

    def add(self, *tiles: Tile) -> Sheet:
        """Puts ``tiles`` on this sheet, each at its bottom, from the sheet it was on, as the screen's Move to Sheet
        moves a tile (page-commands.ts moveTile): one already on this sheet stays where it is."""
        _ = self._doc
        for tile in tiles:
            if tile._page is not self._page:
                raise ValueError(f'{tile!r} is another page\'s')
            _ = tile._view
        self._page._apply_page([_commands.page_move_tile(tile=t._id, sheet=self._id, columns=None) for t in tiles])
        return self

    def layout(self, *bands: Sequence[Tile]) -> Sheet:
        """Its places laid out in bands, top to bottom, each band's places side by side and alike, as the layout picker
        lays them out: ``sheet.layout([grid, chart], [other])``. Every tile on the sheet is in one band; a stack is named
        by any of its tiles. (``layout_from`` takes any layout DataCube draws: splits, sizes, heights.)"""
        for band in bands:
            if isinstance(band, Tile) or not band:
                raise ValueError('a band is a list of tiles, one at least: sheet.layout([grid, chart], [other])')
            for tile in band:
                if not isinstance(tile, Tile):
                    raise ValueError(f'a band is a list of tiles, not {tile!r}')
                _ = tile._view
        # the bands as DataCube's layout picker lays them out (headless layoutBands: its rows shape, the places in order)
        said = json.loads(self._page._session.headless().call(
            'layoutBands', _json.dumps(self._doc['layout']), json.dumps([[t._id for t in band] for band in bands])))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        if said['preset'] is None:
            return self  # a sheet with no tiles, laid out in no bands: as it is (no shape names no bands)
        return self._arranged(said['preset'], said['order'])

    def arrange(self, shape: str) -> Sheet:
        """Its places arranged as one of DataCube's layout picker's shapes, in reading order (a stack is one place):
        ``'side-by-side'`` (one band), ``'stacked'`` (a band each), ``'rows:2-1'`` (bands of 2 then 1),
        ``'columns:1-2'``, ``'focus-left'`` (the first half the page, the rest beside it) and the picker's others, as
        DataCube names them -- what is not one refused in DataCube's words."""
        return self._arranged(shape, None)

    def _arranged(self, preset: str, order: list[str] | None) -> Sheet:
        """Its places arranged as the screen's layout picker arranges them (page-commands.ts arrange), in ``order`` (their
        tiles; None: reading order) -- refused in DataCube's words alone (the S7 confirmation audit's Y2-4)."""
        self._page._apply_page([_commands.page_arrange(sheet=self._id, preset=preset, order=order)])
        return self

    def layout_from(self, layout: Mapping[str, Any]) -> Sheet:
        """Its layout in the page document's own words -- ``{"kind": "bands", "fit": ..., "bands": [{"height": ...,
        "node": ...}]}``, each node a ``{"tile": id}``, a ``{"stack": [ids]}`` or a ``{"split": "row" | "column",
        "parts": [{"node": ..., "size": share}]}`` -- as ``page.to_dict()`` shows a sheet's. Every tile on the sheet is in
        it once, and only they: the page with it is read as DataCube reads one, and refused in its words."""
        _ = self._doc  # a sheet no longer on its page refused, as every sheet call refuses one
        if not isinstance(layout, Mapping):
            raise ValueError(f"a sheet's layout is {{'kind': 'bands', 'fit': ..., 'bands': [...]}}, not {layout!r}")
        self._page._apply_page([_commands.page_layout(sheet=self._id, layout=_copied(layout))])
        return self

    def __repr__(self) -> str:
        return f'<Sheet {self._id}>'


# -- the page ---------------------------------------------------------------------------------------------------------

@_operations
class Page:
    """A DataCube page: its grids over frames (``grid``), their charts, its sheets (``sheet``), stacks (``stack``) and
    layouts -- shown with ``ll.show(page)``, live from then on; saved as DataCube's page document (``save``,
    ``to_json``, ``to_dict``) and loaded again (``load``); read back as it is open (``read``)."""

    def __init__(self, name: str | None = None) -> None:
        from .datacube import _current
        self._session = _current()
        # the page, started as DataCube starts a page (headless pageNew: page-document.ts newPage, the tab's Blank Page's
        # too -- unnamed when no name is given, as the tab's, else named by DataCube's rule; no grid yet, its one sheet an
        # empty board: docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D11), and kept by its session's keeper from now on: the
        # one copy of it every editor -- this page, a tab or a notebook widget open on it -- writes, versioned
        # (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md)
        said = json.loads(self._session.headless().call('pageNew', name))
        if 'problems' in said:
            raise ValueError("a page's name: " + '; '.join(said['problems']))
        self._key_name = f'page-{next(_KEYS)}'
        # the document a call works on: the keeper's as the call started (`_version`), changed by the call -- `_dirty`
        # -- until it is written (`_changed`); `_depth`: how deep in calls on the page this thread is
        self._document: dict[str, Any] = _plain(_json.loads(said['page']))
        reads = self._reads()
        self._version = self._session.keeper.keep(self._key_name, self._document, reads, reads['ids'])
        self._dirty = False
        self._depth = 0
        # collected, forgotten by the keeper at the session's next safe moment (never in the collector), its copies with it
        weakref.finalize(self, self._session.gone.append, self._key_name)
        self._session.page_made(self)
        # every grid command DataCube applied for this page's grids, in order, as the screen would have built them
        self._commands: list[dict[str, Any]] = []
        # every page command DataCube applied to this page, in order, as applied (page-commands.ts)
        self._page_commands: list[dict[str, Any]] = []
        # what changed it since it was last served, as the commands it was (a page's, a grid's with its id) -- what a page
        # open on it applies rather than opening it again (docs/DATACUBE_LIVE_COMMANDS_2026_10_18.md); None once it was
        # changed by no command, which the open page follows by opening it again
        self._unserved: list[dict[str, Any]] | None = []
        # each handle given, by its kind and id: the same object each time it is asked for
        self._handles: dict[tuple[type[Any], str], Any] = {}
        # the ids the last commit gave out afresh, old to new (its edits made again on top of an open page's write)
        self._regiven: dict[str, str] = {}
        # every id the page has held, as its keeper has them (DataCube's: pageReads): none is given out again, so no
        # handle meets a new tile under its old one's id
        self._ids: set[str] = set(self._session.keeper.held(self._key_name))
        # where it is shown (datacube.show_page): told each time it changes
        self._shown: list[Any] = []
        self._quiet = 0

    # -- the page's one copy, its keeper's (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md) ------------------------------------

    @property
    def _doc(self) -> dict[str, Any]:
        """The page's document as this call has it: the keeper's as it started, with what the call changed. Asked outside
        any call (a view, the engine), the keeper's as it is now."""
        if self._depth == 0 and not self._dirty:
            self._pull()
        return self._document

    @_doc.setter
    def _doc(self, doc: dict[str, Any]) -> None:
        self._document = doc
        self._dirty = True

    def _pull(self) -> None:
        """The keeper's document taken, when it moved on (an open page wrote it): a handle of a tile or a sheet it no
        longer holds, gone -- the ids it holds as DataCube says (pageReads, kept with it); every id it has held, held."""
        kept = self._session.keeper.now(self._key_name)
        if kept is None or kept[0] == self._version:
            return
        version, doc, holds = kept
        doc = copy.deepcopy(doc)
        self._forget({id for _, id in self._handles} - set(holds))
        self._document, self._version = doc, version
        self._ids |= set(self._session.keeper.held(self._key_name))
        self._unserved = []

    @contextlib.contextmanager
    def _call(self) -> Any:
        """Work on the page as one call (``_operation``; ``load``): the page as the keeper has it when the outermost
        begins, what it changed written once when it ends, let go when it raises -- a public call made inside it (the
        page's ``name``, read while a cube opens) part of it, never a write of a page half made."""
        self._enter()
        try:
            yield
        except BaseException:
            self._leave(failed=True)
            raise
        self._leave(failed=False)

    def _enter(self) -> None:
        if self._depth == 0:
            if not self._dirty:
                self._pull()
            # every id the page has held, as its keeper has it -- at the version this page is at too: a write that left
            # the page as it was holds the ids it gave out with no new version (B12, the S4 audit's 5)
            self._ids |= set(self._session.keeper.held(self._key_name))
        self._depth += 1

    def _leave(self, failed: bool) -> None:
        self._depth -= 1
        if self._depth == 0 and self._dirty and not self._quiet:
            if failed:
                self._discard()
            else:
                self._changed()

    def _discard(self) -> None:
        """What this call changed and did not write, let go: the page the keeper's."""
        self._dirty = False
        self._unserved = []
        self._version = 0
        self._pull()

    def _commit(self) -> None:
        """WHAT CHANGED, WRITTEN (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4): the commands that changed it, against the
        version its call started at, settled by the page's keeper as every editor's write is (keeper.py ``settle``):
        DataCube applies them to the page as it is now (headless pageRebase) and the keeper keeps what they make -- the
        page this call made, when it had not moved on; else the commands made again on top, the page then taken as the
        keeper has it, a handle of a tile or a sheet whose id was given out afresh following it. One that no longer
        applies there (a tile closed there) is refused in words, nothing changed. A page loaded (N1) is written whole, a
        restore, at the version it was made at."""
        keeper = self._session.keeper
        try:
            if self._unserved is None:
                reads = self._reads()
                # admitted as every editor's page written whole is, by the frames this host serves (Frames.unadmitted:
                # the S7 confirmation audit's Y2-6)
                refused = self._session.frames.unadmitted(reads)
                if refused is not None:
                    raise Refused(refused)
                settled = Settled(keeper.restore(self._key_name, self._version, self._document, reads,
                                                 sorted(self._ids | set(reads['ids']))), False, {})
            else:
                # admitted as a tab's write is, by the frames this host serves (Frames.unadmitted: the S7 audit's Y-10)
                settled = keeper.settle(self._key_name, self._version, self._unserved, self._session.frames.unadmitted)
        except Refused as refused:
            self._discard()
            raise ValueError(str(refused)) from None
        self._dirty = False
        self._unserved = []
        if settled.combined:
            self._renamed(settled.renamed)
            # the keeper's page, the open page's write and this change on top of it
            self._version = 0
            self._pull()
        else:
            self._version = settled.version

    def _renamed(self, renamed: Mapping[str, str]) -> None:
        """Each handle of a tile or a sheet whose id was given out afresh (its change made again on top of the page as it
        was then), under its new one."""
        self._regiven = {**self._regiven, **renamed}
        # every handle that moves taken off first: in a chain (sheet-2 -> sheet-3, sheet-3 -> sheet-4) each goes where its
        # own new id says, never onto another's place (the S2 audit's 6, F4)
        moving = [(key, self._handles.pop(key)) for key in [k for k in self._handles if k[1] in renamed]]
        for (kind, old), handle in moving:
            handle._id = renamed[old]
            self._handles[(kind, renamed[old])] = handle
        self._ids.update(renamed.values())

    # -- the page's model (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B4): its classes, into a project ----------------------

    @property
    def model(self) -> dict[str, Any] | None:
        """Where the page's classes are, as the page document keeps it (DataCube's ``PageModelRef``): its project,
        workspace and package, each grid's table and class by its cube, and the revision it is pinned to (the one its
        last model action saved); None before its first class."""
        ref = self._doc.get('model')
        return _copied(ref) if ref else None

    def _model_said(self, said: dict[str, Any], made: str) -> str:
        """A model action's answer (headless ``modelClass``, ``modelAssociation``, ``modelDataSpace``): its record of the
        page applied as DataCube applies it -- ``modelInto`` when it made the page's project, ``modelPin`` to the
        revision it saved, a grid's ``modelKeep`` (page-model.ts PageModel; no step of the page's history: P15) -- even
        when it was refused after its project was made, so the project stays the page's. Refused, in its words
        (``ValueError``); else the path of what it ``made``."""
        if said['commands']:
            self._apply_page(said['commands'])
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return said[made]

    def create_association(self, a: Any, b: Any, *, name: str | None = None, sdlc: str | None = None,
                           writer: str | None = None) -> str:
        """An association between two of the page's classes, by DataCube's Create an Association… run headless
        (headless ``modelAssociation``; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z5): each end a class and what it is
        joined on -- ``(class, on)``, or ``{'class', 'on', 'name', 'multiplicity', 'unique'}`` with the name the other
        class reaches it by and how many of it one of the other has -- preset, as the window presets it, from whether
        its key is unique over its class's rows (read through the class's relation function over the project, planned
        by the native library and run on Python's database) when it says neither. ``sdlc`` and ``writer`` as
        ``create_class`` takes them. Answers the association's path."""
        engine = self._session.headless()
        given = {'place': 'Python', 'ends': _carried([a, b]), **_model_given(name=name, sdlc=sdlc, writer=writer)}
        with self._session.frames.reading():
            said = _json.loads(engine.call('modelAssociation', _json.dumps(self._doc), given))
        return self._model_said(said, 'association')

    def create_data_space(self, name: str | None = None, *, title: str | None = None, description: str | None = None,
                          associations: Sequence[Any] = (), project: str | None = None, sdlc: str | None = None,
                          writer: str | None = None) -> str:
        """The whole page in one go, by DataCube's Create a Data Space… run headless (headless ``modelDataSpace``;
        docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z6): each grid not yet a class made one (named as the window suggests;
        a grid over no table of its own left out, as the window leaves it; a sample a class cannot hold refuses the
        whole, as the window refuses it, before anything is written), the associations given (as ``create_association``
        takes them: pairs of ends, or ``{'name', 'ends'}``) made, and a data space over the page's classes -- one
        revision, the project made with it when the page has none (named ``project``, else after the page). ``name``:
        the data space's (suggested from the page's name). ``sdlc`` and ``writer`` as ``create_class`` takes them.
        Answers the data space's path."""
        engine, frames = self._session.headless(), self._session.frames
        given = {'place': 'Python', 'associations': _carried(list(associations)),
                 **_model_given(name=name, title=title, description=description, project=project, sdlc=sdlc,
                                writer=writer)}
        with frames.reading():
            # each grid as the host reads it: of its live base, a snapped grid's too (a class maps tables, never a copy)
            grids = [{'cube': g._view['cube'], 'source': g._cube['source']['name'],
                      'spec': g._spec(engine, frames, live=True)} for g in self.grids]
            said = _json.loads(engine.call('modelDataSpace', _json.dumps(self._doc), grids, given))
        return self._model_said(said, 'dataSpace')

    @property
    def name(self) -> str:
        return self._doc['name']

    @name.setter
    def name(self, name: str) -> None:
        # the page's name is each cube's too: DataCube writes them (page-commands.ts renamePage), as the screen does
        self._apply_page([_commands.page_rename_page(name=name)], "a page's name")

    @property
    def sheets(self) -> list[Sheet]:
        """Its sheets, in their tabs' order."""
        return [self._sheet(s['id']) for s in self._doc['sheets']]

    @property
    def grids(self) -> list[Grid]:
        return [self._tile(v['id'], Grid) for v in self._doc['views'] if v['kind'] == 'grid']

    @property
    def charts(self) -> list[Chart]:
        return [self._tile(v['id'], Chart) for v in self._doc['views'] if v['kind'] == 'chart']

    # -- building ------------------------------------------------------------------------------------------------------

    def grid(self, frame: Any = None, *, file: str | Path | None = None, remote: str | None = None,
             cls: str | None = None, project: str | None = None, revision: str | None = None, query: str | None = None,
             warehouse: str | None = None, table: str | None = None, catalog: str | None = None,
             name: str | None = None, rows: Sequence[str] = (), columns: Sequence[str] = (),
             measures: Mapping[str, str] | Sequence[tuple[str, str]] | None = None, filter: Any = None,
             sort: Sequence[tuple[str, str]] = (), dimensions: Sequence[str] = (), title: str | None = None,
             sheet: Sheet | None = None, snap: bool = False) -> Grid:
        """A grid over ``frame`` (a pandas or polars DataFrame, an Arrow table, or a function returning one), served
        under ``name`` (``frame``, ``frame_2``, ... unless given) -- or over a ``file`` (its path; ``name`` the file's
        name, its path's unless given), read in as the tab reads a file it is given (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md
        X4: once for its bytes), or a ``remote`` file (its URL), mounted as the tab mounts one (once for its URL) -- in a band of its own at
        the bottom of ``sheet`` (the first unless given) -- or over what the tab adds a grid over beside them (X4), read
        where this host reads it (DataCube's code, headless sourceCube: host-tables.ts leafOnHost) and titled as the tab
        titles it: a class (``cls``, its name or its path), of the page's model at its pin unless ``revision`` names
        another, or of another ``project`` by its path at a ``revision`` (class-source.ts classAt, the tab's Add a Grid's
        rule); a saved query of this user's store, by its id (``query``); a warehouse table (``warehouse``, its address;
        ``table``, ``schema.table``; ``catalog``), read under the session this host holds for that warehouse. Each query
        reads the frame as it is then; the grid's Snap keeps its rows as they are. A
        name served already is this grid's when it serves this same frame (two grids over one frame), and refused
        when it serves another: ``grid.update(frame)`` replaces a frame, every grid
        over it showing the new one. ``rows``, ``columns``, ``measures`` (``{column: aggregate}`` or ``[(column,
        aggregate)]``), ``filter`` and ``sort``: as ``group``, ``pivot``, ``measure``, ``filter`` and ``sort`` say;
        ``dimensions``: columns made dimensions first (``column(name, kind='dimension')``: a number is a measure until
        then, and only a dimension is grouped or pivoted on) -- all checked before the grid is on the page, so a
        mistake leaves the page and the frames as they were. ``snap``: the frame read once, by the grid's Snap
        (``grid.snap()``), as it is now -- the frame stays served as it is, so another grid over it can read it live."""
        if sheet is not None and sheet._page is not self:
            raise ValueError(f'{sheet!r} is another page\'s')
        target = self.sheets[0] if sheet is None else sheet
        _ = target._doc
        if title is not None:
            title = _given_name(self._session, 'tile', title, 'a title')
        session = self._session
        if sum(b is not None for b in (frame, file, remote, cls, query, warehouse)) != 1:
            raise ValueError('a grid is over a frame, a file (file=path), a remote file (remote=url), a class (cls=...), a '
                             'saved query (query=id) or a warehouse table (warehouse=url, table=...): one of them')
        if remote is not None and name is not None:
            raise ValueError("a remote file is named after its URL: it takes no name")
        if (project is not None or revision is not None) and cls is None:
            raise ValueError("project= and revision= say where a class is read: they go with cls=")
        if (table is not None or catalog is not None) and warehouse is None:
            raise ValueError("table= and catalog= name a warehouse table: they go with warehouse=")
        if name is not None and (cls is not None or query is not None or warehouse is not None):
            raise ValueError('a class, a saved query and a warehouse table are named as DataCube names them: they take no '
                             'name')
        # what DataCube reads a class, a saved query or a warehouse table by: the page's model, and what was named
        given = None if cls is None and query is None and warehouse is None else self._given_source(
            cls, project, revision, query, warehouse, table, catalog)
        # what the grid reads: a frame served (``served``, ``new``: served for it), or a file or remote file read in
        # (``read``: as DataCube read it; ``fresh``: read for it) -- or, ``given``, what the host reads elsewhere
        served, new, read, fresh = None, False, None, False
        if file is not None or remote is not None:
            engine = session.engine()
            if file is not None:
                read, fresh = engine.read_file_once(file, name if name is not None else Path(file).name)
            else:
                read, fresh = engine.mount_remote_once(remote)
        elif frame is not None and name is not None and name in session.frames:
            table = session.frames[name]
            if table.given is not frame:
                raise ValueError(f'{name!r} serves another frame already: grid.update(frame) replaces a frame, every grid '
                                 f'over it showing the new one; or give this grid a name of its own')
            served = table.name
        elif frame is not None:
            served, new = session.register(frame, name), True
        # the page as it was, to go back to when the grid is refused
        was = (self._doc, set(self._ids), len(self._page_commands), len(self._commands),
               None if self._unserved is None else list(self._unserved), self._dirty)
        id = None
        try:
            # its cube as DataCube makes a grid's over a frame, a file or a remote file: capped at the row limit when it is
            # not the page's only grid
            cube = (self._source_cube(given, len(self._doc['cubes']) + 1) if given is not None
                    else self._frame_cube(served, len(self._doc['cubes']) + 1)[0] if read is None
                    else self._leaf_cube(read, len(self._doc['cubes']) + 1))
            # on its sheet, as the screen adds a grid (page-commands.ts addGrid): its id after every one held, a band at
            # the bottom, told nothing yet
            with self._batch(tell=False):
                applied = self._apply_page([_commands.page_add_grid(document=cube, sheet=target.id, near=None, title=title,
                                                                    columns=None)], 'a title')
            id = applied[0]['id']
            grid = self._tile(id, Grid)
            # set before it is placed, nothing told meanwhile: its query's parts as one, as DataCube sets them (a new
            # grid's cube has none: nothing to ask when none is given)
            with self._batch(tell=False):
                pairs = list(measures.items() if isinstance(measures, Mapping) else measures or ())
                # as the commands that make them, in the order a person would (headless gridSetUp), each judged as the
                # screen judges it -- a measure by a person's own steps, against the grid the steps before it made
                given = _json.dumps({
                    'dimensions': list(dimensions), 'rows': list(rows), 'columns': list(columns),
                    'measures': [list(p) if isinstance(p, (list, tuple)) else p for p in pairs],
                    'sorts': [list(s) if isinstance(s, (list, tuple)) else s for s in sort]})
                engine, frames = grid._headless()
                if pairs:
                    with frames.reading():
                        said = json.loads(engine.call('gridSetUp', grid._spec(engine, frames), given))
                else:
                    said = json.loads(engine.call('gridSetUp', None, given))
                if 'problems' in said:
                    raise ValueError('; '.join(said['problems']))
                commands = said['commands']
                if commands:
                    grid._apply(commands)
                if filter is not None:
                    grid.filter(filter)
                if snap:
                    # its query as set, copied: the copy holds every column of its base, so a later edit reads it too
                    grid.snap()
        except BaseException:
            # the page as it was (the grid, its id and the commands it was given gone), and a frame served for it served
            # no more
            self._doc, self._ids = was[0], was[1]
            self._dirty = was[5]
            del self._page_commands[was[2]:]
            del self._commands[was[3]:]
            self._unserved = was[4]
            if id is not None:
                self._forget({id})
            if new:
                session.unregister(served)
            if fresh and read is not None:
                session.engine().forget_read(read)
            raise
        self._changed()
        return grid

    def join(self, mine: Grid, theirs: Grid, how: Mapping[str, Any], *, title: str | None = None,
             sheet: Sheet | None = None, snap: bool = False) -> Grid:
        """A grid of ``mine`` joined with ``theirs`` (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17), as the tab's Join…
        makes one: each grid's rows at their finest level, as each is now, combined as ``how`` says -- DataCube's own
        join (join-how.ts), read strictly: ``{'form': 'join', 'kind': 'inner' | 'left' | 'right' | 'full', 'on':
        {'rows': [{'left': 'customer_id', 'op': '==', 'right': 'id'}]}, 'prefix': 'their_', 'keep': None}`` (``keep``,
        the columns of theirs to keep, every one when None), ``{'form': 'semi' | 'anti', 'on': ...}``, ``{'form':
        'asOf', 'match': {...}, 'on': ..., 'prefix', 'keep'}`` or ``{'form': 'append'}``. Written by DataCube (headless
        joinCube) over the frames the two read, judged by the compiler -- a refusal in its words, the page as it was --
        and added beside ``mine`` on its sheet -- or at the bottom of ``sheet``, given -- by the page command every grid is
        added by (DataCube's placement, the tab's Join…'s). Each input is kept
        by value: the join does not follow later edits of the grids joined. ``snap``: the join read once, by its grid's
        Snap (``grid.snap()``); refused, the page is as it was."""
        for g in (mine, theirs):
            if g._page is not self:
                raise ValueError(f'{g!r} is another page\'s')
        if sheet is not None and sheet._page is not self:
            raise ValueError(f'{sheet!r} is another page\'s')
        if sheet is not None:
            _ = sheet._doc
        if title is not None:
            title = _given_name(self._session, 'tile', title, 'a title')
        session = self._session
        engine, frames = session.headless(), session.frames
        # the tables its leaves read, each grid read as the page has it, under the name it shows, and the join's grid placed
        # beside the first, are DataCube's (headless joinCube: host-tables.ts leafTables, joinInputOf): Python hands in
        # what it serves
        with frames.reading():
            made = _json.loads(engine.call('joinCube', _json.dumps({
                'page': self._doc, 'mine': mine.id, 'theirs': theirs.id, 'how': how, 'host': frames.hosted(),
                'sheet': None if sheet is None else sheet.id, 'title': title})))
        for note in made['notes']:
            warnings.warn(note, stacklevel=2)
        was = (self._doc, set(self._ids), len(self._page_commands),
               None if self._unserved is None else list(self._unserved), self._dirty)
        with self._batch(tell=False):
            applied = self._apply_page([_plain(made['command'])], 'a title')
        grid = self._tile(applied[0]['id'], Grid)
        if snap:
            try:
                with self._batch(tell=False):
                    grid.snap()
            except BaseException:
                # the page as it was: the join, its id and its command gone
                self._doc, self._ids = was[0], was[1]
                self._dirty = was[4]
                del self._page_commands[was[2]:]
                self._unserved = was[3]
                self._forget({grid.id})
                raise
        self._changed()
        return grid

    def sheet(self, name: str | None = None) -> Sheet:
        """A new sheet after the last (``name``: its own; None, named after its first grid)."""
        # as the screen adds one -- what is not text refused by addSheet's reader, in DataCube's words (page-commands.ts addSheet): its id after every one the page has held
        applied = self._apply_page([_commands.page_add_sheet(name=name)], "a sheet's name")
        return self._sheet(applied[0]['id'])

    def stack(self, *tiles: Tile) -> None:
        """``tiles`` in one place, as tabs, one shown at a time, where the first one is: all on one sheet. Stacked as
        DataCube stacks a tile dropped on another's middle (bands.ts stackTiles): the first one's place keeps its own
        tabs first, the others follow in the order given, and a tile stacked already brings its stack's others after
        them."""
        strays = [t for t in tiles if not isinstance(t, Tile)]
        if strays:
            raise ValueError(f'a stack is of tiles (grids and charts), not {", ".join(repr(t) for t in strays)}')
        if any(t._page is not self for t in tiles):
            raise ValueError('the tiles of a stack are of this page')
        for t in tiles:
            _ = t._view
        self._apply_page([_commands.page_stack(tiles=[t._id for t in tiles])])

    def remove(self, part: Tile | Sheet) -> None:
        """A tile off the page, its neighbours closing over its place: a grid with its charts, but a frozen one, which
        stays without it (as DataCube keeps one); a chart. Or a sheet, with its tiles, as its tab's Delete removes it (a
        ``removeSheet`` command): refused for the page's last one."""
        if part._page is not self:
            raise ValueError(f'{part!r} is another page\'s')
        before = {v['id'] for v in self._doc['views']}
        if isinstance(part, Sheet):
            _ = part._doc
            # as the screen deletes one (page-commands.ts removeSheet): its grids first, then the rest of its tiles
            self._apply_page([_commands.page_remove_sheet(sheet=part._id)])
            self._forget({part._id} | (before - {v['id'] for v in self._doc['views']}))
            return
        _ = part._view
        # as the screen removes one (page-commands.ts removeTile): a grid with the charts following it, a frozen one
        # staying detached; a chart's selection taken off its grid's filter; the model kept for the cubes still held --
        # its last grid too, a page shown following it empty as the tab's blank page is (D2)
        self._apply_page([_commands.page_remove_tile(tile=part._id)])
        self._forget(before - {v['id'] for v in self._doc['views']})

    def filter(self, cls: str, condition: Any, *, id: str | None = None) -> str:
        """A page filter on a class's property: ``cls`` (its path, or its name) a class a grid of the page reads,
        ``condition`` as a grid's ``filter`` takes one, over the class's properties -- made as the screen's Add a Filter
        makes one (DataCube's code, headless pageFilter) and set on the page (a ``setPageFilter`` command): every grid
        over the class reads through it. ``id``: the page filter it replaces. Answers its id."""
        said = json.loads(self._session.headless().call('pageFilter', _json.dumps(self._doc), _json.dumps(
            {'class': cls, 'filter': _tagged(condition), **({'id': id} if id is not None else {})})))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        applied = self._apply_page([said['command']])
        return str(applied[0]['id'])

    @property
    def filters(self) -> list[dict[str, Any]]:
        """Its page filters, a copy, each as the page document keeps one: ``{"id", "class", "condition", "from"}`` (the
        chart whose click made it, or None)."""
        return copy.deepcopy(self._doc.get('filters', []))

    def remove_filter(self, id: str) -> None:
        """The page filter ``id`` taken off (a ``removePageFilter`` command), as the screen's x on its chip takes it."""
        self._apply_page([_commands.page_remove_page_filter(filter=id)])

    # -- the document --------------------------------------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """The page as DataCube's page document (its own version), a copy: what is done to it is not done to the page. A
        calculated column's numbers are exact (``Decimal``), as the compiler writes them: ``to_json`` and ``save`` write
        them so. It is the page's document as Python holds it; ``to_json`` and ``save`` write it as DataCube writes it
        (its reader's, headless: a chart's options filled, a key DataCube does not know left out)."""
        return _copied(self._doc)

    def to_json(self, indent: int | None = 2) -> str:
        """The page document as JSON text, its numbers exact, as DataCube writes the page it holds -- the saved page it
        is (its ``record``) among it, unnamed as ``''`` -- indented by ``indent`` (None: one line): refused in DataCube's
        words when DataCube's Save would refuse it (a page with nothing on it). A page file, as DataCube's Export ▸ Page
        File writes one -- named as Save names it, no saved record -- is ``save(path)``'s; either, loaded, is a page of
        its own, no saved record (the S7 confirmation audit's Y2-3)."""
        return _json.dumps(self._written(), indent=indent)

    def save(self, path: str | Path | None = None, *, name: str | None = None) -> str | None:
        """Saved. With no ``path``, as DataCube's Save keeps a page: into this user's saved pages (``ll.home()/pages``,
        what Open… lists in DataCube shown from Python, and ``Page.saved()``), by legend-lite's own saved-page rules (the
        native library's, the page's and the server's: docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B6) -- over the copy this
        page was last saved as, else as a new one -- named ``name``, else as DataCube's Save suggests (the page's own
        name; an unnamed page's, its first sheet's: D11); its id is returned. With a ``path``, the page document as a
        file instead (as DataCube's Export > Page File writes it)."""
        if path is not None:
            if name is not None:
                raise ValueError("a page saved as a file takes no name: its document carries the page's own")
            # as DataCube's Export > Page File writes one, by its one rule (headless pageFile): refused in its words when
            # it is none -- nothing on it, or classes kept in a project
            said = json.loads(self._session.headless().call('pageFile', _json.dumps(self._doc)))
            if 'problems' in said:
                raise ValueError('; '.join(said['problems']))
            # the file as DataCube writes it, byte for byte (the S7 audit's Y-4: Python added a newline)
            Path(path).write_text(said['file'], 'utf-8', newline='')
            return None
        return self._saved(name, as_new=False)

    def save_as(self, name: str | None = None) -> str:
        """Saved as a new page in this user's saved pages, as DataCube's Save As saves one (K13): the copy this page was
        saved as left as it is, this page that new one from now on -- saved again, over it -- named ``name``, else as
        DataCube's Save suggests; its id is returned."""
        return self._saved(name, as_new=True)

    def _saved(self, name: str | None, as_new: bool) -> str:
        from ._home import THIS_USER, pages_folder, store_call
        # the record by DataCube's one rule, the Save window's (headless pageRecord): over the saved page it is, which
        # either editor saved it as (the page's `record`, B13), else -- or, Save As, always -- a new one; the page named
        # as it is saved
        made = json.loads(self._session.headless().call('pageRecord', str(uuid.uuid4()), name, _json.dumps(self._doc),
                                                        as_new))
        if 'problems' in made:
            raise ValueError('; '.join(made['problems']))
        id, record = made['id'], made['record']
        # over the record it is saved as, else a new one, as the tab's Save decides (headless pageRecord's `over`:
        # page-document.ts pageSaving -- the S7 confirmation audit's Y2-5) -- a record deleted since (in DataCube's
        # Open…) refused, as the tab's Save is refused, never made again under its id (the S7 audit's Y-8)
        status, said = (store_call('lite_saved_cubes', pages_folder(), 'PUT', f'/{id}', '', record, THIS_USER)
                        if made['over'] else
                        store_call('lite_saved_cubes', pages_folder(), 'POST', '', '', record, THIS_USER))
        if status != 200:
            raise ValueError(f'the page was not saved: {_store_said(said)}')
        # the saved page it is, told (savedAs: an open page saves over it too, B13); saved under a name, the page is that
        # name, as DataCube's Save renames it (renamePage, B4) -- neither a step of its history
        self._apply_page([_commands.page_saved_as(record=id),
                          *([_commands.page_rename_page(name=made['name'])] if made['name'] != self.name else [])])
        return id

    @staticmethod
    def saved(search: str = '', *, sort: str | None = None, limit: int | None = None) -> list[dict[str, Any]]:
        """This user's saved pages (``ll.home()/pages``), as DataCube's Open… lists them (K15): each one's record without
        its page -- its ``id`` (what ``Page.open`` and ``Page.delete`` take), ``name``, ``owner`` and times
        (``createdAt``, ``lastUpdatedAt``, ``lastOpenAt``: epoch milliseconds) -- those ``search`` matches (its id, or
        text in the name or the owner; blank, every one), this user's first, then newest first by ``sort`` (DataCube's
        spellings, as Open…'s menu: ``'SORT_BY_VIEW'``, last opened -- Open…'s first, the one taken when none is given
        --, ``'SORT_BY_UPDATE'``, last saved, ``'SORT_BY_CREATE'``, created), the first ``limit`` (none given, as many
        as the store answers: 100 at most, upstream's cap). The search by DataCube's (headless pageSearch), which and in
        what order by the store's own rules."""
        from ._home import THIS_USER, pages_folder, store_call
        from .datacube import _current
        asked = json.loads(_current().headless().call('pageSearch', search, sort, limit))
        if 'problems' in asked:
            raise ValueError('; '.join(asked['problems']))
        status, said = store_call('lite_saved_cubes', pages_folder(), 'POST', '/search', '', json.dumps(asked['spec']),
                                  THIS_USER)
        if status != 200:
            raise ValueError(f'the saved pages were not read: {_store_said(said)}')
        return list(json.loads(said))

    @staticmethod
    def delete(id: str) -> None:
        """A page saved in this user's saved pages, by its id, deleted as DataCube's Open… deletes one (K14), by the
        store's own rules (only its owner deletes it) -- and each page of this session saved as it no saved page any
        more, as Open… tells the tab's page (``savedAs`` of none, B13): saved again, a new one."""
        from ._home import THIS_USER, pages_folder, store_call
        from .datacube import _current
        status, said = store_call('lite_saved_cubes', pages_folder(), 'DELETE', f'/{id}', '', '', THIS_USER)
        if status == 404:
            raise ValueError(f'no saved page {id!r} in this user\'s saved pages')
        if status not in (200, 204):
            raise ValueError(f'the saved page {id!r} was not deleted: {_store_said(said)}')
        # each told, whatever another says: the record is gone from the store already
        failed: list[BaseException] = []
        for page in _current().page_objects():
            try:
                page._record_gone(id)
            except Exception as failure:
                failed.append(failure)
        if failed:
            raise failed[0]

    @_operation
    def _record_gone(self, id: str) -> None:
        """The saved page ``id`` deleted: this page no saved page any more, if it was that one (savedAs of none)."""
        if self._doc.get('record') == id:
            self._apply_page([_commands.page_saved_as(record=None)])

    @classmethod
    def open(cls, id: str, frames: Mapping[str, Any] | None = None, *,
             files: Mapping[str, str | Path] | None = None) -> Page:
        """A page saved in this user's saved pages (``ll.home()/pages``: what ``save()`` and DataCube's Save keep there,
        what Open… lists), by its id, opened as DataCube's Open… opens it (headless pageOfRecord: a page, or a cube saved
        alone before pages, a page of its one grid) and then as ``load`` opens a page -- its ``frames`` and ``files``
        given as ``load`` takes them. Saved again, it saves over that record."""
        from ._home import THIS_USER, pages_folder, store_call
        from .datacube import _current
        status, said = store_call('lite_saved_cubes', pages_folder(), 'GET', f'/{id}', '', '', THIS_USER)
        if status == 404:
            raise ValueError(f'no saved page {id!r} in this user\'s saved pages')
        if status != 200:
            raise ValueError(f'the saved page {id!r} was not read: {said}')
        opened = json.loads(_current().headless().call('pageOfRecord', said))
        if 'problems' in opened:
            raise ValueError('; '.join(opened['problems']))
        page = cls.load(_plain(_json.loads(opened['page'])), frames, files=files)
        # the saved page it is, by its command (savedAs, no step: B13) -- a page loaded from a document is none (the S7
        # confirmation audit's Y2-3: a page loaded from another's document saved over that one's record)
        page._apply_page([_commands.page_saved_as(record=id)])
        return page

    @classmethod
    def load(cls, document: str | Path | Mapping[str, Any], frames: Mapping[str, Any] | None = None, *,
             files: Mapping[str, str | Path] | None = None) -> Page:
        """A page document (a file, or its dict) as a page again: the document as DataCube reads it (its own version
        only; refused in its words when DataCube would not open it), each cube as DataCube opens it over what this host
        serves now -- what DataCube leaves out of it warned, in its words. Its grids read ``frames`` by their frame names
        (each cube's ``{"_type": "frame", "name": ...}``), each served; the files it reads given by their names
        (``files``: a file's name, its path -- docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D14), each read in, one that is
        not the file a cube was saved over opened over all the same and said, as DataCube's tab says a file picked for
        it; its remote files mounted again by their URLs. A frame or a file not given is refused in words. A grid saved
        snapped opens live, said: its copy lasted only as long as the session that made it (``grid.snap()`` takes one
        anew)."""
        from .datacube import _current
        # read on the session the page will be made on (Page() takes the current one)
        doc = _read_page(_copied(document) if isinstance(document, Mapping) else Path(document).read_text('utf-8'),
                         _current())
        session = _current()
        frames, files = frames or {}, files or {}
        # what it reads, as DataCube says (headless cubeLeaves: each leaf's kind, its name, a file's hash, a remote URL)
        leaves = [leaf for c in doc['cubes'] for leaf in _cube_leaves(session, c['cube'])]
        wanted = sorted({leaf['name'] for leaf in leaves if leaf['_type'] == 'frame'})
        missing = [n for n in wanted if n not in frames]
        if missing:
            raise ValueError(f'the page reads the frames {", ".join(missing)}: give them, frames={{name: frame}}')
        # a file given is read and opened over (one of other bytes said), one not given read where it was read in already
        held = session.frames.read_files()
        named = {leaf['name'] for leaf in leaves if 'sha256' in leaf} & set(files)
        unread = sorted({leaf['name'] for leaf in leaves if 'sha256' in leaf and leaf['sha256'] not in held} - set(files))
        if unread:
            raise ValueError(f'the page reads the files {", ".join(unread)}: give them, files={{name: path}}')
        remotes = sorted({leaf['url'] for leaf in leaves if 'url' in leaf} - set(session.frames.hosted()['remotes']))
        # its name, or none: a page nobody named loads unnamed (D11)
        page = cls(doc['name'] or None)
        # loaded as one call on the page: written once, whole, when it ends (a public call inside, as a cube opens,
        # never writes a page half loaded)
        with page._call():
            for name in wanted:
                page._session.register(frames[name], name)
            # each file given read in, once for its bytes (its table the keeper's while a page reads it: X3), each remote
            # file mounted again -- each read for this page let go of at once when the page is refused; by the engine
            # every grid's rows are read by (D16)
            engine = session.engine()
            read: list[dict[str, Any]] = []
            try:
                given: dict[str, str] = {}
                for name in sorted(named):
                    file, fresh = engine.read_file_once(files[name], name)
                    read += [file] if fresh else []
                    given[name] = file['leaf']['sha256']
                for url in remotes:
                    read.append(engine.mount_remote(url))
                page._opened_all(doc, given)
            except BaseException:
                for file in read:
                    engine.forget_read(file)
                raise
        return page

    def _opened_all(self, doc: dict[str, Any], given: Mapping[str, str]) -> None:
        """The page ``load`` makes of ``doc``: each cube opened as DataCube opens it over what this host serves now
        (``given``: a file's name, the hash of the file given for it), what DataCube says warned at ``load``'s caller."""
        # live: a copy lasts only as long as the session that made it, so a page saved with a snapped grid opens with it
        # reading its frames, each said (DataCube's own rule, headless pageOpenedLive: §18.8)
        opened = json.loads(self._session.headless().call('pageOpenedLive', _json.dumps(doc)))
        for note in opened['notes']:
            warnings.warn(note, stacklevel=3)
        doc = _plain(_json.loads(opened['page']))
        self._doc = doc
        # every id it holds, as DataCube says (pageReads)
        self._ids |= set(self._reads()['ids'])
        # each cube as DataCube opens it over what this host serves now (above): what it leaves out, left out here too
        # and said once, so no later call on the grid sends it -- each its command, none written alone
        left: list[str] = []
        with self._batch(tell=False):
            for c in doc['cubes']:
                self._reopened(c['id'], 4, given, leave_out=True, left=left)
        # none of its sources opened: not opened, in DataCube's words, as the tab's Open... refuses it (headless
        # pageNoneOpened: page-document.ts noneOpened -- the S7 confirmation audit's Y2-2)
        refused = json.loads(self._session.headless().call('pageNoneOpened', doc['name'], len(doc['cubes']) - len(left)))
        if refused is not None:
            raise ValueError(refused)
        # a page loaded is a page made whole, by no command
        self._unnoted()

    # -- shown ---------------------------------------------------------------------------------------------------------

    @property
    def _key(self) -> str:
        """Its name on the engine (page.json?page=): this page object's own."""
        return self._key_name

    def _written(self) -> dict[str, Any]:
        """The page as DataCube saves it, to be saved or written as a file: refused as DataCube's Save refuses it, in its
        words (headless pageSaved) -- a page with nothing on it among them."""
        return _read_page(self._doc, self._session, saved=True)

    def _changed(self) -> None:
        """What changed, written by the page's keeper (``_commit``), its commands with it -- what a page open on it
        applies, or, changed by no command, opens again by -- and the copies no page reads any more let go (the session's
        sweep). Inside a batch, written when the batch ends."""
        if self._quiet:
            return
        if self._dirty:
            self._commit()
        self._session.sweep()

    class _Batch:
        def __init__(self, page: Page, tell: bool) -> None:
            self.page = page
            self.tell = tell

        def __enter__(self) -> None:
            self.page._quiet += 1

        def __exit__(self, kind: Any, *_: Any) -> None:
            self.page._quiet -= 1
            # written once, when it went through; a change that raised writes nothing (the call lets it go)
            if kind is None and self.tell:
                self.page._changed()

    def _batch(self, tell: bool = True) -> Page._Batch:
        """Several changes, told as one (``tell=False``: not told, what changed not being on the page yet)."""
        return Page._Batch(self, tell)

    # -- the document's parts ------------------------------------------------------------------------------------------

    def _apply_page(self, commands: list[dict[str, Any]], what: str | None = None) -> list[dict[str, Any]]:
        """The page edited by commands, each the one DataCube's screen builds for the same edit (page-commands.ts:
        docs/DATACUBE_COMMANDS_PAGE_2026_10_18.md), applied by DataCube's own function (headless pageApply): refused in
        DataCube's words (``what`` before them), nothing changed. Else the page as DataCube leaves it, the ids it has
        held, and the commands as applied -- an id DataCube allocated in each that made a tile or a sheet -- recorded and
        returned."""
        said = json.loads(self._session.headless().call(
            'pageApply', _json.dumps(self._doc), json.dumps(sorted(self._ids)), _json.dumps(commands)))
        if 'problems' in said:
            words = '; '.join(said['problems'])
            raise ValueError(f'{what}: {words}' if what else words)
        page = _plain(_json.loads(said['page']))
        self._doc = page
        self._ids = set(said['held'])
        applied = [_plain(c) for c in said['commands']]
        self._page_commands.extend(applied)
        self._note([{'page': c} for c in applied])
        self._regiven = {}
        self._changed()
        # an id a command made, given out afresh as the write was made again on top of an open page's (_renamed): the
        # caller's handle is made under the id the page gives it, never the one taken
        return [{**c, 'id': self._regiven.get(c['id'], c['id'])} if isinstance(c.get('id'), str) else c for c in applied]

    def _note(self, commands: list[dict[str, Any]]) -> None:
        """Commands that changed the page, for a page open on it to apply (``_changed`` serves them)."""
        if self._unserved is not None:
            self._unserved.extend(commands)

    def _unnoted(self) -> None:
        """The page changed by no command: a page open on it opens it again."""
        self._unserved = None

    def _tiles(self, layout: dict[str, Any]) -> list[str]:
        """A sheet's tiles in reading order, a stack's in its tabs' order, as DataCube reads its layout (bands.ts tiles,
        asked headless)."""
        return json.loads(self._session.headless().call('layoutTiles', _json.dumps(layout)))

    def _forget(self, ids: set[str]) -> None:
        """The handles of tiles and sheets gone, refused from now on."""
        for key in [k for k in self._handles if k[1] in ids]:
            self._handles.pop(key)._dead = True

    def _reads(self) -> dict[str, list[str]]:
        """What the page as this call has it reads, as DataCube says (headless pageReads, one call): its frames, its
        copies, its files (by their bytes' hash), its remote files (by URL), its ids -- what its keeper keeps with it and
        never reads from the page itself."""
        return _json.loads(self._session.headless().call('pageReads', _json.dumps(self._document)))

    def _frames(self) -> set[str]:
        """The frames its cubes read (each cube's leaves: a join's every frame), a frozen chart's whose grid is gone
        included: as its keeper has them when the page is the keeper's (asked outside a call: no call into DataCube),
        else as DataCube says of the page as this call has it."""
        if self._depth == 0 and not self._dirty:
            _ = self._doc
            kept = self._session.keeper.frames(self._key_name)
            if kept is not None:
                return set(kept)
        return set(self._reads()['frames'])

    def _cube(self, id: str) -> dict[str, Any]:
        return next(c['cube'] for c in self._doc['cubes'] if c['id'] == id)

    def _snapped(self, cube: str) -> dict[str, Any] | None:
        """The copy the page says cube ``cube`` (its id) reads, its grid snapped (§18.8), or None: it reads its rows live.
        Where its rows are then read is DataCube's to say (_cube_spec)."""
        return self._doc.get('copies', {}).get(cube)

    def _served(self) -> list[str]:
        return [n for n in self._frames() if n in self._session.frames]

    def _tile(self, id: str, kind: type[Any]) -> Any:
        handle = self._handles.get((kind, id))
        if handle is None:
            handle = self._handles[(kind, id)] = kind(self, id)
        return handle

    def _tile_of(self, id: str) -> Tile:
        view = next(v for v in self._doc['views'] if v['id'] == id)
        return self._tile(id, Grid if view['kind'] == 'grid' else Chart)

    def _sheet(self, id: str) -> Sheet:
        return self._tile(id, Sheet)

    def _update(self, name: str, frame: Any) -> None:
        """Frame ``name`` served again over ``frame``: each grid's cube over it reopened over the frame as the compiler
        types it now (``_reopened``); the page served again when one changed."""
        session = self._session
        session.register(frame, name)
        # every cube reading the frame reopened (a join of it among them), whichever changed: their commands written once
        reading = [c['id'] for c in self._doc['cubes'] if name in _cube_frames(self._session, c['cube'])]
        with self._batch():
            for id in reading:
                self._reopened(id, 4)

    def _reopened(self, id: str, stacklevel: int, given: Mapping[str, str] | None = None, leave_out: bool = False,
                  left: list[str] | None = None) -> bool:
        """The page's cube ``id`` as DataCube opens it over its frame as the compiler types it now (``_frame_cube``:
        cube-document.ts rebuilt -- a new column hidden; a gone one left out; the open groups dropped when the grouping
        changed), taken when it is
        not the cube held -- the frame's columns moved, or DataCube left a part out -- each note warned once, in
        DataCube's words. So the page holds the cube every headless call opens, and no later call sends a part DataCube
        leaves out. Whether it was taken. Asked when a page is loaded (``load``: its frames served), when a frame is
        served again (``_update``), and before a filter, a calculated column or a chart is checked against it
        (``_current``: a frame changed in place). ``given``: as the page loads, each file given for it by its name, the
        hash of the file read in for it (headless gridOpened). ``leave_out``, as a page loads: a class whose rows are not
        read here, a join that no longer joins, left out and said as the tab's Open... leaves it out -- kept as the page
        saved it, refused in words when its grid is next asked (DataCube's rule, headless gridOpened's leaveOut:
        page-document.ts leftOutWhenUnopened; the S7 audit's H-6: Python refused the whole page), its id added to
        ``left``."""
        # opened again over its source only as DataCube's rule says (headless cubeOpensAgain: not a snapped grid's, which
        # reads its copy until it goes live -- the S7 audit's Y-2: Python kept the rule itself)
        if not json.loads(self._session.headless().call('cubeOpensAgain', _json.dumps(self._doc), id)):
            return False
        held = next(c['cube'] for c in self._doc['cubes'] if c['id'] == id)
        label = held['source']['name']
        said = self._opened_cube(held, given or {}, leave_out)
        if isinstance(said, str):
            warnings.warn(said, stacklevel=stacklevel)
            if left is not None:
                left.append(id)
            return False
        notes, taken, opened = said
        # whether it is taken is DataCube's rule (headless gridOpened `taken`): not, the cube stands as it is
        if not taken:
            return False
        for note in notes:
            warnings.warn(f'{label}: {note}', stacklevel=stacklevel)
        # taken by the page command that says so, applied as DataCube applies it: a page open on it opens it again
        self._apply_page([_commands.page_rebuilt(cube=id, source=opened)])
        return True

    def _current(self, cube: str, stacklevel: int) -> None:
        """The cube ``cube`` (its id) as DataCube opens it over its frame now, taken and said (``_reopened``) before a
        call checks a change against it and writes the change into it: a frame changed in place (a column gone, a
        calculated column that read it left out) is the cube DataCube checks, never the one held before."""
        self._reopened(cube, stacklevel + 1)

    def _opened_cube(self, saved: dict[str, Any], given: Mapping[str, str],
                     leave_out: bool = False) -> tuple[list[str], bool, dict[str, Any]] | str:
        """A saved cube as DataCube opens it over the frames as they are now -- a frame's, a join's of frames (headless
        gridOpened, by the rule of the page's ``rebuilt`` command: cube-document.ts cubeOpenedAgain) -- what DataCube
        says of what changed, whether the page takes it (cubeTaken), and the source as it is now, which the page's
        ``rebuilt`` command carries -- or, ``leave_out`` and a source a page leaves out, DataCube's note of it."""
        engine, frames = self._session.headless(), self._session.frames
        with frames.reading():
            made = json.loads(engine.call('gridOpened', _json.dumps(saved), frames.hosted(), self.name,
                                          len(self._doc['cubes']), dict(given), leave_out))
        if 'leftOut' in made:
            return made['leftOut']
        return list(made['notes']), made['taken'], _plain(_json.loads(made['opened']))

    def _frame_cube(self, frame: str, grids_on_page: int,
                    saved: dict[str, Any] | None = None) -> tuple[dict[str, Any], list[str], bool]:
        """A grid's cube over ``frame`` as DataCube makes one (cube-document.ts rebuilt, asked headless: gridCube): a
        new grid's (its rows as they are; capped at the row limit when ``grids_on_page``, the page's grids counting
        it, are more than one: DataCube's gridCapped), or ``saved`` rebuilt over the frame as it is now; what DataCube
        says of what changed; and whether it left a part out."""
        engine, frames = self._session.headless(), self._session.frames
        with frames.reading():
            table = frames[frame]
            spec = {'model': table.model, 'runtime': table.runtime, 'source': table.accessor, 'frame': frame}
            made = json.loads(engine.call(
                'gridCube', spec, None if saved is None else _json.dumps(saved), self.name, frame, grids_on_page))
        return _plain(_json.loads(made['cube'])), list(made['notes']), made['changed']

    def _leaf_cube(self, read: Mapping[str, Any], grids_on_page: int) -> dict[str, Any]:
        """A new grid's cube over a file or remote file read in (``read``: as DataCube read it, the file it is its
        ``leaf``), as DataCube makes one over it where this host reads it (headless leafCube, the file's source as the
        tab records it: cube-document.ts fileSourceOf, remoteSourceOf), named after the file."""
        engine, frames = self._session.headless(), self._session.frames
        with frames.reading():
            made = json.loads(engine.call('leafCube', read['leaf'], frames.hosted(), self.name, grids_on_page))
        return _plain(_json.loads(made['cube']))

    def _given_source(self, cls: str | None, project: str | None, revision: str | None, query: str | None,
                      warehouse: str | None, table: str | None, catalog: str | None) -> dict[str, Any]:
        """What names a new grid's class, saved query or warehouse table, as DataCube reads it (headless givenSource): a
        saved query as this user's store holds it, by its id (its record, which DataCube writes down of it)."""
        if cls is not None:
            return {'class': cls, **({'project': project} if project is not None else {}),
                    **({'revision': revision} if revision is not None else {})}
        if query is not None:
            from ._home import THIS_USER, queries_folder, store_call
            status, said = store_call('lite_saved_queries', queries_folder(), 'GET', f'/{query}', '', '', THIS_USER)
            if status != 200:
                raise ValueError(f'no saved query {query!r} in this user\'s store: {said}')
            return {'record': json.loads(said)}
        return {'warehouse': warehouse, **({'table': table} if table is not None else {}),
                **({'catalog': catalog} if catalog is not None else {})}

    def _source_cube(self, given: Mapping[str, Any], grids_on_page: int) -> dict[str, Any]:
        """A new grid's cube over a class, a saved query or a warehouse table, as DataCube makes one where this host
        reads it (headless sourceCube): over the page's model for a class, titled as the tab titles it."""
        engine, frames = self._session.headless(), self._session.frames
        model = self._doc.get('model')
        with frames.reading():
            made = json.loads(engine.call('sourceCube', None if model is None else _json.dumps(model), _json.dumps(given),
                                          frames.hosted(), self.name, grids_on_page, 'Python'))
        if 'problems' in made:
            raise ValueError('; '.join(made['problems']))
        return _plain(_json.loads(made['cube']))

    def __repr__(self) -> str:
        return f'<Page {self.name!r}: {len(self.grids)} grids, {len(self.charts)} charts, {len(self.sheets)} sheets>'


def _read_page(doc: Any, session: Any, saved: bool = False) -> dict[str, Any]:
    """``doc`` (a page document, or its JSON text as it is) as DataCube reads a page document and would write it again
    (page-document.ts readPage and pageToJson, asked headless, on ``session``'s engine: the page's own) -- or, ``saved``,
    as DataCube saves one (headless pageSaved: refused as its Save refuses it) -- of DataCube's
    own version, every rule of its reader held, and refused in its words otherwise -- Python
    keeps no second copy of them. What its cubes read is DataCube's to judge too, where each is opened over what this
    host serves (headless host-tables.ts onHost: docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z14)."""
    engine = session.headless()
    said = json.loads(engine.call('pageSaved' if saved else 'pageRead', doc if isinstance(doc, str) else _json.dumps(doc)))
    if 'problems' in said:
        raise ValueError('; '.join(said['problems']))
    page: dict[str, Any] = _plain(_json.loads(said['page']))
    return page


def _model_given(**given: Any) -> dict[str, Any]:
    """What a person gave a model action, as JSON carries it: what they did not give left out, for DataCube's default."""
    return {k: _carried(v) for k, v in given.items() if v is not None}
