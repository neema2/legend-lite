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
import hashlib
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
from ._types import UnknownType, is_boolean, is_decimal, is_float, is_integer, is_number
from .keeper import Refused, Settled

_KEEP: Any = object()

# each page's key on its session's keeper and engine (page.json?page=): never given twice in a process, so a page
# collected never shares one with a page made after it
_KEYS = itertools.count(1)

# how many runs of a flow its page keeps (flow.runs), as the canvas keeps a flow's (layout/flow-board.ts RUN_HISTORY)
_RUN_HISTORY = 20


def _parameter_value(type: str, value: Any) -> Any:
    """A parameter's value as the page keeps it: a Decimal's as its digits' text, its scale kept, as the parameters bar
    keeps what is typed (ui/page-parameter-bar.ts) -- never a float's nearest; any other as JSON says it. A type no
    parameter has is DataCube's to refuse, in its words (page-parameters.ts parameterProblem): its value is handed on as
    given, never converted by a guess."""
    try:
        decimal = is_decimal(type)
    except UnknownType:
        return _plain(value)
    if decimal and isinstance(value, (Decimal, numbers.Number)) and not isinstance(value, bool):
        return str(value)
    return _plain(value)


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
    """A tile of a page (a grid, a chart, a text or a timeseries): its view in the page's document, by its id."""

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
        return _cube_frames(self._page._session, self._cube, self._page._doc, self._view['cube'])

    def place(self, x: int, y: int) -> Grid:
        """Its box moved to (``x``, ``y``) on its flow's canvas -- its top-left corner, in the canvas's units, neither
        below zero -- as a box dragged there is moved (page-commands.ts placeBox; docs/DATACUBE_FLOW_MODE_2026_10_24.md
        FL20). A grid on a board is placed by its layout: refused in DataCube's words."""
        _ = self._view
        self._page._apply_page([_commands.page_place_box(tile=self._id, x=x, y=y)])
        return self

    def output(self, name: str | None) -> Grid:
        """This box writes its rows to the CSV file ``name`` -- a flow's Output (page-commands.ts setOutput;
        docs/DATACUBE_FLOW_MODE_2026_10_24.md M6): every row, the columns it shows, each time its flow runs
        (``flow.run(folder)``) -- or, None, no longer. A name with a folder in it, or not ending .csv, and a grid on a
        board, refused in DataCube's words."""
        given = None if name is None else {'format': 'csv', 'name': name}
        self._page._apply_page([_commands.page_set_output(grid=self.id, output=given)])
        return self

    def output_table(self, name: str, mode: str = 'create', *, keys: Sequence[str] | None = None) -> Grid:
        """This box writes its rows into the table ``name`` of its frames' database -- a flow's Output Data to a table
        (page-commands.ts setOutput; docs/DATACUBE_FLOW_MODE_2026_10_24.md M6, FL13), each time its flow runs: ``mode``
        ``'create'`` (made for them; refused when it is there), ``'overwrite'`` (made again), ``'append'``,
        ``'deleteAppend'`` (its rows deleted first) or ``'merge'`` (by ``keys``: a row of the same key updated, a new one
        added) -- the write planned by the compiler and run in one transaction (DataCube's table writer,
        table-output.ts). A name not plain, a frame's, a mode it has not, a merge without keys, refused in DataCube's
        words."""
        given = {'format': 'table', 'name': name, 'mode': mode, **({'keys': list(keys)} if keys else {})}
        self._page._apply_page([_commands.page_set_output(grid=self.id, output=given)])
        return self

    def stops_run(self, stops: bool = True) -> Grid:
        """This Test box stops its flow's run when one of its checks fails -- no Output written, as the canvas's Run writes
        none (page-commands.ts setStops; FL8's refuse) -- or, False, only warns. A Cross Tab whose columns are fixed stops
        it so when a value of its rows has no column (FL3: ``failing``). A grid on a board refused in DataCube's words."""
        self._page._apply_page([_commands.page_set_stops(grid=self.id, stops=stops)])
        return self

    def cache(self, cache: bool = True) -> Grid:
        """This box caches its rows (Alteryx's Cache; docs/DATACUBE_FLOW_MODE_2026_10_24.md FL10): marked so on its flow
        (page-commands.ts setCache), and its rows copied now by this session's engine -- its ``snap`` -- the boxes after it
        and a join of it reading the copy from then on, as a box after a cached box on the canvas reads the page's copy of
        it (live-inputs.ts resolvedSource's ``copies``): a box over a warehouse table joined with one over a frame that
        way. The copy lasts as long as the session (FL11); a page saved and loaded is marked cached and reads live until
        it is cached again. False: no longer -- its copy let go, read live. A grid on a board refused in DataCube's
        words."""
        self._page._apply_page([_commands.page_set_cache(grid=self.id, cache=cache)])
        if cache:
            self.snap()
            self._page._cache_keys[self.id] = self._chain_key()
        else:
            self._page._cache_keys.pop(self.id, None)
            if self.snapped is not None:
                self.live()
        return self

    def _chain_key(self) -> str:
        """What its rows are made of: its cube and every cube it reads through its inputs, live, and the page's
        parameters -- the canvas's chain (layout/flow-board.ts chain): a cached box's copy is of what it was made of."""
        seen: list[str] = []

        def walk(grid: Grid) -> None:
            if grid.id in seen:
                return
            seen.append(grid.id)
            for i in grid.inputs.values():
                walk(i)
        walk(self)
        return json.dumps([[g, self._page._tile(g, Grid)._cube] for g in seen] + [self._page._doc.get('parameters', [])],
                          sort_keys=True, default=str)

    def block_until_done(self, blocks: bool = True) -> Grid:
        """This box blocks its flow's run (Alteryx's Block Until Done; docs/DATACUBE_FLOW_MODE_2026_10_24.md M8): the
        boxes after it run -- their rows read, their Outputs written -- once every Output of the boxes not after it is
        written (``flow.run``'s stages: layout/flow.ts runStages), so a box after it reads what those wrote. False: no
        longer (page-commands.ts setBlocks). A grid on a board refused in DataCube's words."""
        self._page._apply_page([_commands.page_set_blocks(grid=self.id, blocks=blocks)])
        return self

    def message(self, text: str | None, *, when: str = 'always', level: str = 'info') -> Grid:
        """What a run of this box's flow says of it (Alteryx's Message; M8): ``text`` -- ``{rows}`` its rows counted --
        ``when`` its rows are as it asks (``'always'``, ``'rows'``: when it has some, ``'noRows'``: when it has none),
        as ``level``: ``'info'``, ``'warning'`` (``flow.run`` warns), or ``'error'``, which stops the run as a failing Test
        does -- no Output written from its stage on. None: it says nothing (page-commands.ts setMessage). Refused in
        DataCube's words: no words, a ``when`` or a ``level`` it has not."""
        message = None if text is None else {'text': text, 'when': when, 'level': level}
        self._page._apply_page([_commands.page_set_message(grid=self.id, message=message)])
        return self

    @property
    def cached(self) -> bool:
        """Whether this box caches its rows (``cache``): its flow marks it so."""
        for sheet in self._page._doc['sheets']:
            if sheet['layout'].get('kind') == 'flow':
                for box in sheet['layout']['boxes']:
                    if box['tile'] == self.id:
                        return box.get('cache') is True
        return False

    @property
    def failing(self) -> list[dict[str, Any]]:
        """This Test's checks that fail, each ``{'check': words, 'rows': n, 'says': words}`` -- its row read as the canvas
        reads it (DataCube's checks.ts failingChecks, headless gridFailing): ``n`` the rows that fail it (a count of rows's,
        the rows its input has), ``says`` its failing as the canvas says it; none, every row passes. A Cross Tab whose
        columns are fixed (``pin_pivot``) checks that every value of its rows has a column (FL3): failing, ``n`` its values
        with none, ``says`` them. A grid that checks nothing is refused in DataCube's words."""
        engine, frames = self._headless()
        with frames.reading():
            said = json.loads(engine.call('gridFailing', self._spec(engine, frames)))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return said['failing']

    def relink(self, input: Grid, side: str = 'input', port: str | None = None) -> Grid:
        """Its rows read live from grid ``input`` of its page from now on -- a line drawn on its flow from that box's
        output (page-commands.ts setInput; docs/DATACUBE_FLOW_MODE_2026_10_24.md M2) -- its query as it is, the grid then
        opened over ``input`` as it is now (what changed said, as a load says it). ``side``: a join's ``'left'`` or
        ``'right'``, a Union's ``'more1'``, ``'more2'``, ... (its next, one past its last, an input added). ``port='false'``: a Filter's False, the rows its condition leaves out (FL7); ``port='left'`` or
        ``'right'``: a Join's L or R, the rows of that input its condition pairs with no row of the other (live-inputs.ts
        gridInput: an anti-join the database runs). Refused in DataCube's words: a grid that reads no other grid's rows,
        one that would read its own, a False of a grid with no filter, an L or R of a grid that is no join."""
        _ = self._view, input._view
        given = {'grid': input._id, **({'port': port} if port is not None else {})}
        with self._page._batch():
            self._page._apply_page([_commands.page_set_input(grid=self._id, side=side, input=given)], 'a relink')
            self._page._current(self._view['cube'], 3)
        return self

    def freeze(self, side: str = 'input') -> Grid:
        """Its rows read from a copy of what its live input is now, from now on (page-commands.ts setInput with no input:
        a box's Freeze, as a box taken off freezes the boxes that read it): an edit of that grid no longer reaches this
        one. ``side``: a join's ``'left'`` or ``'right'``, a Union's ``'more1'``, .... Refused in DataCube's words when it
        reads a copy already."""
        _ = self._view
        self._page._apply_page([_commands.page_set_input(grid=self._id, side=side, input=None)], 'a freeze')
        return self

    def take_off(self, side: str) -> Grid:
        """A Union's input on ``side`` taken off, those after it each moved up one -- a line into its box deleted on the
        canvas, as the canvas takes it off (headless pageGesture: page-gestures.ts takeOffCommands;
        docs/DATACUBE_FLOW_MODE_2026_10_24.md M2) -- the grid then opened over the inputs left. ``side``: ``'left'``,
        ``'right'``, ``'more1'``, .... A join of two, whose input is frozen rather than taken off (``freeze``), refused in
        DataCube's words."""
        _ = self._view
        page = self._page
        said = json.loads(page._session.headless().call('pageGesture', 'flow.takeOff', _json.dumps(
            {'page': page._doc, 'grid': self._id, 'side': side})))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        with page._batch():
            page._apply_page([_plain(c) for c in said['commands']], 'a take off')
            page._current(self._view['cube'], 3)
        return self

    def pick(self, parameter: str | bool | None) -> Grid:
        """A Union kept to the rows of one of its inputs, picked as it runs -- a flow's Dynamic Input (Alteryx's;
        docs/DATACUBE_FLOW_MODE_2026_10_24.md M8), as its settings' Keep the rows of sets it (headless pageGesture:
        dynamic-input.ts pickCommands): ``parameter`` a String parameter of the page (``page.parameter``) whose value is
        the name of the input kept -- each input read under its grid's title, else its source's name; ``True``, a new
        parameter (``source``, else ``source2``, ...) whose choices are its inputs' names, the first its value; ``None``,
        every input's rows again. Its value set (``page.set_parameter``), the grid reads the other input's rows. Refused in
        DataCube's words: a grid that is no Union, two inputs of one name, a parameter the page lacks or of another type."""
        _ = self._view
        page = self._page
        said = json.loads(page._session.headless().call('pageGesture', 'flow.pick', _json.dumps(
            {'page': page._doc, 'grid': self._id, 'parameter': parameter})))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        with page._batch():
            page._apply_page([_plain(c) for c in said['commands']], 'a pick')
            page._current(self._view['cube'], 3)
        return self

    def rejoin(self, how: Mapping[str, Any]) -> Grid:
        """Its join made again as ``how`` says (page.join's ``how``, read strictly) -- a flow's Join or Union box set
        again, as its Join window's Apply sets it (page-commands.ts setJoin; docs/DATACUBE_FLOW_MODE_2026_10_24.md M2) --
        its inputs as they are, the grid then opened over them as it is now. Refused in DataCube's words: a grid that is
        no join, the join it is already."""
        _ = self._view
        with self._page._batch():
            self._page._apply_page([_commands.page_set_join(grid=self._id, how=dict(how))], 'a join')
            self._page._current(self._view['cube'], 3)
        return self

    @property
    def input(self) -> Grid | None:
        """The grid of its page it reads live (a flow's box after another), or None: it reads a source of its own, or a
        copy (frozen). A join reads two: see ``inputs``."""
        inputs = self.inputs
        if len(inputs) > 1:
            raise ValueError('this grid is a join of two inputs: see inputs')
        return next(iter(inputs.values()), None)

    @property
    def inputs(self) -> dict[str, Grid]:
        """The grids of its page it reads live, by side -- ``'input'``, a join's ``'left'`` and ``'right'``, a Union's
        ``'more1'``, ``'more2'``, ... after them (cube-document.ts inputSides) -- each one a live link of its source
        (live-inputs.ts liveGrids); a side read from a copy is left out."""
        source = self._cube['source']
        given = {'rows': {'input': source.get('input')},
                 'join': {'left': source.get('left'), 'right': source.get('right'),
                          **{f'more{k}': i for k, i in enumerate(source.get('more', []), start=1)}}}.get(source['_type'], {})
        return {side: self._page._tile(i['grid'], Grid) for side, i in given.items() if 'grid' in i}

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

    def _output(self) -> str:
        """Its rows as a flow's Output writes them (``flow.run``): CSV text by the tab's own writer (headless gridOutput,
        flow-output.ts) -- in its sorts' order, the columns it hands on under their names, each value as it is. A pivot
        its rows decide refused in DataCube's words."""
        engine, frames = self._headless()
        with frames.reading():
            said = _json.loads(engine.call('gridOutput', self._spec(engine, frames)))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return said['csv']

    def _write_table(self, output: Mapping[str, Any]) -> int:
        """Its rows written into its table Output's table (``flow.run``), by DataCube's one table writer (headless
        gridWriteTable, table-output.ts): its rows' query asked in a read of the frames (typed by the database), then the
        write run in none, the frames held still and the engine's turn taken -- one transaction. The rows written."""
        engine, frames = self._headless()
        with frames.reading():
            spec = self._spec(engine, frames)
            asked = _json.loads(engine.call('gridOutputQuery', spec))
        if 'problems' in asked:
            raise ValueError('; '.join(asked['problems']))
        leaves = _cube_leaves(self._page._session, self._cube, self._page._doc, self._view['cube'], opened=True)
        # a cached box's copy is held by this session's engine, apart from the frames the write runs over: refused in
        # words, never written from rows it cannot read (the roadmap audit of 2026-10-26)
        copies = [leaf['name'] for leaf in leaves if leaf['_type'] == 'copy']
        if copies:
            raise ValueError(f"a table output writes from frames' rows: this box reads a cached box's copy ({', '.join(copies)}), which this "
                             'session\'s engine holds -- write the table from a box before the cache, or take the cache off')
        others = [leaf['name'] for leaf in leaves if leaf['_type'] != 'frame']
        if others:
            raise ValueError(f"a table output writes from frames' rows: this box reads {', '.join(others)}")
        # each frame in its own package, as its model declares it (frames.py: a frame's model is its own)
        tables = [{'table': name, 'pkg': name} for name in dict.fromkeys(leaf['name'] for leaf in leaves)]
        with engine.turn(), frames.holding():
            said = _json.loads(engine.call('gridWriteTable', spec, _json.dumps(output), asked['query'],
                                           _json.dumps({'tables': tables, 'held': frames.owned()})))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return int(said['rows'])

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

    def pin_pivot(self, values: Sequence[Any] | None) -> Grid:
        """Its pivot's values pinned -- a flow's Cross Tab with its columns fixed (FL2; the gesture ``pivot.pin``, the
        rule its settings' Load values asks): its columns those values', whatever its rows hold, so a box after it reads
        them (each value's column named by it: ``'EMEA'``, or ``'EMEA_notional'`` when it spreads several measures);
        ``None``, its values its rows' each time it runs. Refused in DataCube's words: a pivot on no column, or on more
        than one, an empty value, a value twice."""
        return self._gesture('pivot.pin', {'values': None if values is None else list(values)})

    def add_pivot_values(self, values: Sequence[Any]) -> Grid:
        """The values ``values`` its pin lacks added to its columns, after those it has, in the order given -- a Cross
        Tab's Add to the cast (FL8; the gesture ``pivot.addValues``): a value its rows hold that no column of its has.
        Refused in DataCube's words: its columns not fixed, or a column for each of them already."""
        return self._gesture('pivot.addValues', {'values': list(values)})

    def load_pivot_values(self) -> Grid:
        """Its pivot's values pinned as its rows hold them now, asked of the database as its pivot asks them -- a
        Cross Tab's Load values (DataCube's headless gridPivotValues, then ``pin_pivot``)."""
        engine, frames = self._headless()
        with frames.reading():
            said = json.loads(engine.call('gridPivotValues', self._spec(engine, frames)))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return self.pin_pivot(said['values'])

    def add_pivot(self, column: str) -> Grid:
        """Pivoted on ``column`` too, last, as the menu's Add Horizontal Pivot adds it (the gesture
        ``pivot.addHorizontal``); pivoted on already, nothing changes."""
        return self._gesture('pivot.addHorizontal', {'column': column})

    def remove_pivot(self, column: str) -> Grid:
        """Pivoted on ``column`` no more, as the menu's Remove Horizontal Pivot and a drag out of the Columns zone take
        it off (the gesture ``pivot.removeHorizontal``); not pivoted on, nothing changes."""
        return self._gesture('pivot.removeHorizontal', {'column': column})

    def data_bars(self, column: str, on: bool = True) -> Grid:
        """Data bars in ``column``'s cells -- or, ``on=False``, none -- each from the zero line to its value against the
        column's range at its depth, as the header menu's Conditional Formatting ▸ Data Bars draws them (the gesture
        ``format.dataBar``; DataCube's conditional.ts). On a pivoted column's measure, as a heatmap is."""
        return self._gesture('format.dataBar', {'column': column, 'on': on})

    def color_scale(self, column: str, low: str, high: str, mid: str | None = None) -> Grid:
        """A colour scale on ``column``: ``low`` at its smallest value, ``high`` at its largest, ``mid`` (``'#rrggbb'``)
        halfway -- Excel's three-colour scale -- as Conditional Formatting ▸ Color Scales sets one (``format.colorScale``;
        the heatmap's colours)."""
        scale = {'from': low, 'to': high, **({'mid': mid} if mid is not None else {})}
        return self._gesture('format.colorScale', {'column': column, 'scale': scale})

    def icon_set(self, column: str, set: str | None = 'arrows', reverse: bool = False) -> Grid:
        """An icon before each of ``column``'s values by the third of its range it falls in -- ``set``: ``'arrows'``,
        ``'trafficLights'``, ``'symbols'`` or ``'flags'``; ``None`` takes them off; ``reverse`` the high icon for the low
        values (a cost's) -- as Conditional Formatting ▸ Icon Sets (``format.iconSet``)."""
        return self._gesture('format.iconSet', {'column': column, 'set': set, **({'reverse': True} if reverse else {})})

    def highlight(self, column: str, when: dict, style: str | dict = 'lightRed') -> Grid:
        """A rule on ``column``: its cells drawn in ``style`` where ``when`` holds -- ``{'op': 'greaterThan', 'value':
        100}``, ``{'op': 'between', 'from': 1, 'to': 9}``, ``{'op': 'contains', 'text': 'EM'}``, ``{'op': 'top', 'count':
        10}``, ``{'op': 'aboveAverage'}``, ``{'op': 'blank'}`` ... -- the first rule that holds drawing; ``style`` one of
        Excel's (``'lightRed'``, ``'yellow'``, ``'green'``, ``'redText'``, ``'bold'``) or ``{'foreground', 'background',
        'bold'}`` (``format.addRule``; DataCube's conditional.ts judges it, in its words)."""
        return self._gesture('format.addRule', {'column': column, 'rule': {'when': when, 'style': style}})

    def format_code(self, column: str, code: str | None) -> Grid:
        """``column``'s values written by an Excel number format code -- ``'#,##0.0,,"M"'``, ``'0.0%'``,
        ``'$#,##0;[Red]($#,##0)'``, ``'dd mmm yyyy'`` -- its sections (positive; negative; zero; text) and colours as
        Excel's; ``None`` takes it off. Judged by DataCube's format-code.ts, in its words (the gesture ``format.code``);
        the Excel export writes the code itself."""
        return self._gesture('format.code', {'column': column, 'code': code})

    def clear_formats(self, column: str) -> Grid:
        """``column``'s conditional formats taken off -- its colour scale, data bars, icons and rules -- as Conditional
        Formatting ▸ Clear Formatting from Column (``format.clear``)."""
        return self._gesture('format.clear', {'column': column})

    def heatmap(self, column: str, on: bool = True) -> Grid:
        """A heatmap on ``column`` -- or, ``on=False``, off -- as the menu's Conditional Formatting > Heatmap and
        Remove Heatmap paint it (the gestures ``heatmap.add`` and ``heatmap.remove``): on a pivoted
        column's measure, so every value's column under it has it; a pivot total's its own. The heatmap it has already, or none to take off, changes nothing."""
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
        panel offers for its type; a measure's only) and an aggregate of two's second column, ``weight`` (a weighted
        average's weight; ``corr``'s and ``covarSample``'s other number; the number ``maxBy`` and ``minBy`` order by; the
        column ``distinctCount`` counts) -- None clears either, as the panel's "none" does. What is not given is left as
        it is."""
        change = {**({'kind': kind} if kind is not None else {}),
                  **({'aggregate': aggregate} if aggregate is not _KEEP else {}),
                  **({'weight': weight} if weight is not _KEEP else {})}
        return self._apply([_commands.grid_column(name=name, change=change)])

    def measure(self, column: str, fn: str, name: str | None = None, *, weight: str | None = None) -> Grid:
        """``column`` aggregated by ``fn`` when the grid groups (one Aggregation offers for its type, and none is assumed:
        ``column(name, kind='measure')`` makes a measure at the one DataCube gives its type; an aggregate of two --
        ``wavg``, ``corr``, ``covarSample``, ``covarPopulation``, ``maxBy``, ``minBy`` -- takes its second column as
        ``weight``, and only they and ``distinctCount`` do): ``column``'s own Aggregation, as Column Properties sets it -- replacing the one it
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

    def filter_values(self, column: str, values: Any) -> Grid:
        """The values of ``column`` kept, as the Filter Values window keeps the ticked ones (Excel's autofilter; DataCube's
        one rule, value-filter.ts): ``values`` a list of them, ``None`` among them the blanks, in place of the
        column's own conditions in the filter -- the other columns' kept as they are; ``None`` for every value, the
        column's own conditions taken off. Each value as JSON carries it, tagged where it has no form (a date)."""
        return self._gesture('filter.values', {'column': column, 'values': None if values is None else _tagged(list(values))})

    def filter_date_range(self, column: str, range: str) -> Grid:
        """``column``'s dates in ``range``, relative to when the query runs (the menu's Filter ▸ Date Range; DataCube's
        date-ranges.ts): ``'today'``, ``'lastMonth'``, ``'yearToDate'``, ``'last30Days'``... -- from its start to its end,
        in place of the column's own conditions. The dates are the database's to compute: a page saved with "last
        month" shows last month whenever it is opened."""
        return self._gesture('filter.dateRange', {'column': column, 'range': range})

    def date_filter(self, column: str, range: str | None = None, *, start: str | None = None, end: str | None = None) -> Grid:
        """Its filter made ``column``'s dates in ``range`` (``filter_date_range``'s ranges, relative to when the query
        runs) or from ``start`` to ``end``, both kept, each a day as ``'2024-06-30'`` -- alone, in place of every condition
        it had: a flow's Date Filter box (DataCube's grid-gestures.ts ``filter.dates``, its window's rule). Both or
        neither, a day that is none, or a first day after the last, refused in DataCube's words."""
        given = {'column': column, **({'range': range} if range is not None else {}),
                 **({'from': start} if start is not None else {}), **({'to': end} if end is not None else {})}
        return self._gesture('filter.dates', given)

    def top(self, n: int | None, by: str | None = None, *, bottom: bool = False) -> Grid:
        """The top ``n`` rows by ``by`` -- a grouped grid's groups at each level, each parent's own -- the largest first,
        or the smallest with ``bottom`` (the menu's Top 10 and Bottom 10; DataCube's snapshot.ts TopN): ties kept, the
        rest left out, the database ranking them. ``None`` takes it off."""
        if n is None:
            return self._apply([_commands.grid_top_n(top_n=None)])
        return self._gesture('filter.topN', {'column': by, 'n': n, **({'bottom': True} if bottom else {})})

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
        command). Its charts are not copied, as the screen copies none. A flow's box is duplicated as its Duplicate does it
        (headless pageGesture flow.duplicate: page-gestures.ts duplicateCommands): reading what it reads, after the box it
        reads first."""
        page = self._page
        if self.sheet.flow:
            said = json.loads(page._session.headless().call('pageGesture', 'flow.duplicate', _json.dumps(
                {'page': page._doc, 'tile': self._id, 'title': title})))
            if 'problems' in said:
                raise ValueError('; '.join(said['problems']))
            applied = page._apply_page([_plain(c) for c in said['commands']], 'a title')
            return page._tile(applied[0]['id'], Grid)
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
        """Every group closed, the expand level's too, as the menu's View > Collapse All closes them."""
        return self._apply([_commands.grid_collapse_all()])

    def expand_all(self) -> Grid:
        """Every group above the detail rows open, as the menu's View > Expand All opens them (an ``expandAll`` command)."""
        return self._apply([_commands.grid_expand_all()])

    def tree(self, *open: Sequence[str | None], totals: bool = False, expand_to: int | None = None,
             closed: Sequence[Sequence[str | None]] = ()) -> Grid:
        """The open groups set whole, in the saved tree's own shape (a ``tree`` command; docs/DATACUBE_LIVE_EDITING_2026_10_24.md
        W1): each path in ``open`` a group open (its keys from the top group down, as ``expand`` takes them), ``totals``
        whether the grand total shows, ``expand_to`` the level the groups open to (0 or none: by hand only), ``closed`` the
        groups closed under that level. What puts a collapsed tree back -- an undo of Collapse All on the screen -- which
        ``expand`` and ``collapse`` cannot: Collapse All forgets the level and the closes."""
        return self._apply([_commands.grid_tree(
            open=[list(p) for p in open], show_totals=totals,
            **({} if expand_to is None else {'expand_to': expand_to}),
            **({} if len(closed) == 0 else {'closed': [list(p) for p in closed]}))])

    def calculate(self, name: str, expression: str, *, kind: str | None = None, level: str = 'row',
                  before: str | None = None) -> Grid:
        """A calculated column computed per row, in Pure over the row ``$x`` (``'$x.qty * 2'``; a column that can be
        empty, as a dataframe's can, ``->toOne()`` first for arithmetic), added as the grid's own column editor adds one
        (DataCube's code, headless: gridColumn): its name as DataCube takes one, ``kind`` ``'measure'`` or
        ``'dimension'`` as declared (not given, a measure, as the editor starts a new column -- summed when grouped if it
        is a number, its unique value if not), and the grid with it compiled as DataCube would run it -- reading any of
        the grid's columns before it, a window column's too. ``level='group'``: computed after the grouping instead, over
        each group's row (the editor's Group level: no kind), reading the grid's measures as each level aggregates them.
        A mistake fails at this line, in DataCube's and the compiler's words. ``before``: the calculated column of
        its level the new one goes before (left out: last of its level, as the editor adds one)."""
        _expression(expression)
        return self._column({'name': name, 'expression': f'x|{expression}', 'kind': kind, 'level': level,
                             **({} if before is None else {'before': before})})

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

    def model_measure(self, measure: str) -> Grid:
        """A measure the grid's model declares -- a Pure function of the class's instances stereotyped ``BI.measure``
        (``meta::legend::bi::BI``, or the project's own profile named ``BI``: its store refuses a ``meta::`` path),
        ``grossNotional(t: Trade[*]): Float[1] { $t.notional->sum() }`` -- by its name
        or its path, added as the header menu's Add Model Measure adds it (DataCube's model-measures.ts, headless
        gridModelMeasure): each aggregate over the instances a calculated column with its Aggregation, the rest -- its
        arithmetic, the measures it calls -- a group-level column over them; its format from its ``BI.format`` tag. The
        database aggregates it per group as it does every measure. Refused in DataCube's words, naming the part that does
        not take apart so."""
        self._page._current(self._view['cube'], 4)
        engine, frames = self._headless()
        with frames.reading():
            said = json.loads(engine.call('gridModelMeasure', self._spec(engine, frames), _json.dumps({'measure': measure})))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return self._kept(said, 3)

    def group_values(self, column: str, groups: dict, other: str | None = None, *, name: str | None = None) -> Grid:
        """A column gathering ``column``'s values by hand into named groups -- ``{'Macro': ['FX', 'Rates'], 'Equity':
        ['EQ']}``, ``None`` among a group's values the blanks -- the rest named ``other`` or, on a text column, left as
        themselves (Tableau's groups; the header menu's Group Values; DataCube's snapshot.ts ValueGroups, its expression
        built at query time): a dimension the grid groups, filters and charts by. Named ``<column>_group`` unless ``name``
        says. Refused in DataCube's words: a value in two groups, a number column's rest unnamed, a name taken."""
        made = {'of': column, 'groups': [{'name': k, 'values': list(v)} for k, v in groups.items()],
                **({'other': other} if other is not None else {})}
        return self._gesture('column.calculate', {'level': 'row', 'column': {'name': name or f'{column}_group', 'groups': made,
                                                                             'kind': 'dimension'}})

    def related(self, path: str, aggregate: str | None = None, *, name: str | None = None) -> Grid:
        """A related class's property as a column of a grid over a class (DataCube's class tree: class-tree.ts, headless
        gridRelated) -- ``path`` from the class, dotted (``'customer.region'``), each hop a class-typed property or an
        association's end -- as the header menu's Add Related Column adds it: through a to-many end its values aggregated
        per instance (``aggregate``: ``'count'``, ``'sum'``, ``'average'``, ``'min'`` or ``'max'``; a class at the end
        counted), so a total over the grid counts each instance once. Named after its path (``customer_region``,
        ``trades_notional_sum``) unless ``name`` says; a measure when it aggregates, else a dimension. The database's join,
        planned through the class's mapping. Refused in DataCube's words: a path the model has not, a to-many end not
        aggregated, a grid over no class."""
        given = {'path': path.split('.'), **({'aggregate': aggregate} if aggregate is not None else {}),
                 **({'name': name} if name is not None else {})}
        self._page._current(self._view['cube'], 4)
        engine, frames = self._headless()
        with frames.reading():
            said = json.loads(engine.call('gridRelated', self._spec(engine, frames), _json.dumps(given)))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return self._kept(said, 3)

    def measure_at(self, name: str, of: str, at: str = 'parent') -> Grid:
        """A group-level column: the measure ``of`` as each group's parent has it (``at='parent'``; a top group's parent
        is the grand total) or as the grand total has it (``at='all'``) -- the same aggregate over the coarser group's
        rows, so an average's is the parent's average -- the column editor's Parent or Grand Total (DataCube's code,
        headless: gridColumn). A measure divided by it is an exact share, as ``show_as`` builds one."""
        return self._column({'name': name, 'mode': 'grain', 'level': 'group', 'grain': {'of': of, 'at': at}})

    def grain(self, column: str, grain: str) -> Grid:
        """The date ``column`` taken to its period (``'day'``, ``'week'``, ``'month'``, ``'quarter'``, ``'year'``), a new
        dimension, as the header menu's By Month adds one (DataCube's code: grid-presets.ts presetCommands) -- named
        ``<column>_<grain>``, or that with ``_2``... where the grid has a column of the name."""
        return self._preset({'kind': 'grain', 'column': column, 'grain': grain})

    def show_as(self, column: str, kind: str) -> Grid:
        """The measure ``column`` shown as ``kind`` along the grid's row groups, as the header menu's Show As adds it
        (Excel's Show Values As; grid-presets.ts presetCommands): ``'percentOfTotal'`` (of the grand total, at every
        depth), ``'percentOfParent'`` (of the parent row), ``'runningTotal'``, ``'percentRunningTotal'``,
        ``'difference'``, ``'percentChange'``, ``'movingAverage'``, ``'rank'`` (largest first) or ``'rankAscending'`` --
        group-level columns named ``<column>_<kind>``, the ones it reads hidden, a share or a change shown as a
        percentage. A share divides by the measure at that grain: the same aggregate over the coarser group's rows."""
        return self._preset({'kind': 'showAs', 'column': column, 'showAs': kind})

    def bins(self, column: str, *, width: float | None = None, count: int | None = None) -> Grid:
        """The number ``column`` taken to its bin -- ``width`` wide, or ``count`` bins over its range -- a new dimension
        (and its bin's upper edge), the columns a chart's bins are (calc-presets.ts binColumns, by grid-presets.ts
        presetCommands), named ``<column>_bins``. The screen's Bins opens the column editor on the same expression."""
        return self._preset({'kind': 'bins', 'column': column, 'bins': {
            **({'width': width} if width is not None else {}), **({'count': count} if count is not None else {})}})

    def split(self, column: str, delimiter: str, parts: int | None = None, *, name: str | None = None, rows: bool = False) -> Grid:
        """The text ``column`` split on ``delimiter`` into its first ``parts`` parts, a new column each -- a flow's Text
        To Columns (DataCube's code: text-tool-columns.ts textToolCommands) -- named ``<name>_1`` ... ``<name>_<parts>``
        (``name`` the column's own by default); a value with fewer parts has its last columns empty. ``rows``: into rows
        instead -- one column, ``name``, each row once for each of its parts (an explode)."""
        return self._preset({'kind': 'split', 'column': column, 'delimiter': delimiter,
                             **({'parts': parts} if parts is not None else {}), 'name': column if name is None else name,
                             **({'rows': True} if rows else {})})

    def regex(self, column: str, pattern: str, mode: str = 'match', *, replacement: str | None = None,
              name: str | None = None) -> Grid:
        """A regular expression over the text ``column``, a new column -- a flow's RegEx: ``'match'``, whether its value
        matches (true or false); ``'replace'``, its value with each match replaced by ``replacement``; ``'extract'``,
        the first part it matches (empty where none does); ``'tokenize'``, each part it matches a row of its own (a
        value with no match, no row: Alteryx's Tokenize). Named ``name``, else ``<column>_matches``,
        ``<column>_replaced``, ``<column>_extracted`` or ``<column>_token``, as its window names it (text-tool-columns.ts
        regexName)."""
        return self._preset({'kind': 'regex', 'column': column, 'pattern': pattern, 'mode': mode,
                             **({'replacement': replacement} if replacement is not None else {}),
                             **({'name': name} if name is not None else {})})

    def cleanse(self, columns: Sequence[str], *, blanks: bool = False, punctuation: bool = False, spaces: bool = False,
                trim: bool = True, case: str | None = None, in_place: bool | None = None) -> Grid:
        """The text ``columns`` tidied, a ``<column>_clean`` each -- a flow's Data Cleansing: empty values made blank
        (``blanks``), punctuation taken out, repeated spaces made one (``spaces``), the ends trimmed (``trim``), the
        letters made ``'upper'`` or ``'lower'`` case. ``in_place``: each handed on by its own name, the column itself
        hidden (Alteryx's in place: the box after it reads ``who``, cleansed) -- None, in place on a flow's box, as its
        window starts, and not on a board's grid."""
        return self._preset({'kind': 'cleanse', 'columns': list(columns), 'blanks': blanks, 'punctuation': punctuation,
                             'spaces': spaces, 'trim': trim, **({'letterCase': case} if case is not None else {}),
                             **({'inPlace': True} if self._in_place(in_place) else {})})

    def _in_place(self, given: bool | None) -> bool:
        """A tool's in place, given or -- None -- as its window starts (ui/text-tool-form.ts, impute-form.ts,
        multi-field-form.ts): on a flow's box, in place; on a board's grid, beside its column (the review of 2026-10-26)."""
        return self.sheet.flow if given is None else given

    def unique(self, keys: Sequence[str], *, by: Any = None) -> Grid:
        """A row of each key -- the columns ``keys`` -- its first in the order ``by`` (a column, rising, or ``(column,
        'asc' | 'desc')``; the first key's own by default): a flow's Unique (DataCube's code: unique.ts uniqueCommands),
        a row number per key, a filter keeping the first, the number hidden. Its duplicates -- every row after a key's
        first -- are its False port: ``other.relink(this, port='false')``."""
        return self._preset({'kind': 'unique', 'keys': list(keys),
                             **({'by': _sort_given(by, 'an order')} if by is not None else {})})

    def date_part(self, column: str, part: str, *, name: str | None = None) -> Grid:
        """A part of the date ``column`` -- ``'year'``, ``'month'`` (its number), ``'day'`` (of the month), ``'week'`` (of
        the year), ``'hour'`` -- a new column of whole numbers, named ``<column>_<part>`` (or ``name``): a flow's
        DateTime (DataCube's code: date-tools.ts dateToolCommands)."""
        return self._preset({'kind': 'datePart', 'column': column, 'part': part, **({'name': name} if name else {})})

    def parse_date(self, column: str, format: str, *, name: str | None = None) -> Grid:
        """The text ``column`` read as dates by ``format`` -- the engine's tokens, ``'yyyy-MM-dd'``, ``'dd/MM/yyyy'``,
        ``'yyyy-MM-dd HH:mm:ss'`` (a time of day in it: date-times) -- a new column named ``<column>_date`` (or
        ``name``): a flow's DateTime (DataCube's code: date-tools.ts dateToolCommands). A value the format cannot read
        refuses the grid's rows, in the database's words: filter such text out first."""
        # its name and whether it reads a time of day: the window's rule (date-tools.ts dateToolName, formatHasTime)
        return self._preset({'kind': 'dateParse', 'column': column, 'format': format, **({'name': name} if name else {})})

    def format_date(self, column: str, format: str, *, name: str | None = None) -> Grid:
        """The date ``column`` written as text by ``format`` -- the engine's tokens (``yyyy``, ``MM``, ``dd``, ``HH``,
        ``mm``, ``ss``: ``'dd/MM/yyyy'``, ``'yyyyMMdd'``), translated to the database's strftime -- a new column named ``<column>_text`` (or ``name``): a
        flow's DateTime (DataCube's code: date-tools.ts dateToolCommands). An empty date, an empty text."""
        return self._preset({'kind': 'dateFormat', 'column': column, 'format': format, **({'name': name} if name else {})})

    def impute(self, columns: Sequence[str], *, fill: float | str = 'average', in_place: bool | None = None) -> Grid:
        """The number ``columns``' empty values filled, a ``<column>_filled`` each -- a flow's Imputation (DataCube's code:
        impute.ts imputeCommands): with ``fill``, a number, or the column's own ``'average'``, ``'median'``,
        ``'smallest'`` or ``'largest'`` across every row, which the database computes (a window column, hidden). A column that is no
        number, or a whole-number column filled with a fraction, refused in DataCube's words. ``in_place``: each filled
        column handed on by its own name, the column itself hidden, as Alteryx's Imputation does -- None, in place on a
        flow's box, as its window starts, and not on a board's grid."""
        return self._preset({'kind': 'impute', 'columns': list(columns), 'with': fill,
                             **({'inPlace': True} if self._in_place(in_place) else {})})

    def sample(self, mode: str | None, n: int | None = None, by: str | None = None, *, groups: Sequence[str] = (),
               ranges: str | None = None, seed: int = 1, value: Any = None) -> Grid:
        """Its rows sampled -- a flow's Sample (DataCube's code: row-sample.ts sampleCommands), in the order of the column
        ``by``: ``mode`` ``'first'`` or ``'last'`` ``n`` rows (its smallest or largest: of all the rows, the grid's Top N,
        which keeps rows that tie; of each group of ``groups``, a row number per group), ``'skip'`` -- every row after
        the first ``n`` -- ``'every'``, one row of every ``n``, or ``'records'``, the rows at the places ``ranges`` says
        (``'1-10, 20, 30+'``: Alteryx's Select Records); either of all the rows or of each group -- or ``'random'``, about
        ``n`` percent of all the rows, the same for the same ``seed`` (the database's hash of each row's number and the
        seed) -- or ``'oversample'``, every row whose ``by`` is ``value`` and as many of the others (chosen by that hash) as
        make them ``n`` percent of the rows (Alteryx's Oversample Field). The database numbers the rows (a window column,
        hidden). ``None``: every row again, its sample taken off."""
        if mode is None:
            return self._preset({'kind': 'sample', 'mode': None})
        if mode == 'random':
            return self._preset({'kind': 'sample', 'mode': mode, 'n': n, 'seed': seed})
        if mode == 'oversample':
            return self._preset({'kind': 'sample', 'mode': mode, 'n': n, 'by': by, 'value': value, 'seed': seed})
        if mode == 'records':
            return self._preset({'kind': 'sample', 'mode': mode, 'ranges': ranges, 'by': by, 'groups': list(groups)})
        return self._preset({'kind': 'sample', 'mode': mode, 'n': n, 'by': by, 'groups': list(groups)})

    def transpose(self, columns: Sequence[str], *, keys: Sequence[str] = (), name: str = 'Name', value: str = 'Value') -> Grid:
        """Its ``columns`` made rows -- a flow's Transpose (DataCube's code: transpose.ts transposeCommands): each row once
        for each of them, a ``name`` column saying which and a ``value`` column holding its value, the ``keys`` beside
        them and every other column left out (hidden). The columns hold one kind of value -- numbers, text, dates --
        else it is refused in DataCube's words: make them text first."""
        return self._preset({'kind': 'transpose', 'columns': list(columns), 'keys': list(keys), 'name': name, 'value': value})

    def select(self, keep: Sequence[str], *, rename: Mapping[str, str] | None = None,
               retype: Mapping[str, str] | None = None, order: Sequence[str] | None = None) -> Grid:
        """The columns it hands on, ``keep``, the names it hands some on by, ``rename`` (``{'region': 'area'}``), and the
        types it makes some, ``retype`` (``{'code': 'Integer'}``: ``'String'``, ``'Integer'``, ``'Float'``, ``'Decimal'``,
        ``'Boolean'`` or ``'Date'``, as its column can be made) -- a flow's Select (DataCube's code: select-columns.ts
        selectCommands): every other column hidden, each renamed given its display name, which a box of a flow hands on
        as the relation's ``rename`` (the box after it reads ``area``), each retyped a column of its conversion handed on
        by its name. ``order``: the order it hands them on in, by their own names (its column order; those not named
        after them), a retyped column in its place. Two columns handed on by one name, or a type a column cannot be made,
        refused in DataCube's words."""
        return self._preset({'kind': 'selectColumns', 'keep': list(keep), 'rename': dict(rename or {}),
                             **({'retype': dict(retype)} if retype else {}), **({'order': list(order)} if order is not None else {})})

    def summarize(self, group: Sequence[str], aggregate: Mapping[str, str | tuple[str, str]] | None = None) -> Grid:
        """A row of each ``group`` of its rows, ``aggregate``'s columns beside it, each by its aggregate (``{'notional':
        'average', 'trade_id': 'count'}``: ``'sum'``, ``'average'``, ``'count'``, ``'min'``, ``'max'``, ``'median'``,
        ``'distinctCount'``, ``'joinStrings'``, ... as its column is offered them; an aggregate of two columns with its
        second, ``{'px': ('wavg', 'qty')}``: a weighted average, its weight) -- a flow's Summarize (DataCube's code:
        summarize.ts summarizeCommands): its grouping, each column's kind and Aggregation, every other column left out.
        The database groups and aggregates."""
        given = {c: ({'fn': a[0], 'by': a[1]} if isinstance(a, tuple) else a) for c, a in (aggregate or {}).items()}
        return self._preset({'kind': 'summarize', 'group': list(group), 'aggregate': given})

    def count_records(self, name: str = 'Count') -> Grid:
        """How many rows it has -- one row, one column, ``name`` -- a flow's Count Records (DataCube's code:
        count-records.ts countRecordsCommands): a column of one value grouped by and hidden, a 1 for each row summed,
        every other column hidden, so the box after it reads the count alone. The database counts."""
        return self._preset({'kind': 'countRecords', 'name': name})

    def test(self, checks: Sequence[Mapping[str, Any]]) -> Grid:
        """Checks of its rows -- a flow's Test (DataCube's code: checks.ts checkCommands): each
        ``{'kind': 'notEmpty' | 'unique', 'column': c}``, ``{'kind': 'between', 'column': c, 'min': lo, 'max': hi}``
        (a bound left out: none that side) or ``{'kind': 'rows', 'min': lo, 'max': hi}`` (its count of rows, whole
        numbers, a smallest 1 at least) -- its one row then counting, by the database, the rows that fail each:
        ``<column>_empty``, ``<column>_repeated``, ``<column>_outside``, beside ``Rows`` (``Rows_outside`` 1 when its
        count is outside its bounds), every column it read hidden. A check of a column it has not, a bound of a column
        that is no number, or bounds the wrong way round, refused in DataCube's words. Set again, its checks replace the
        ones it had."""
        return self._preset({'kind': 'test', 'checks': [dict(c) for c in checks]})

    def generate_rows(self, name: str, start: int | str | None = None, end: int | str | None = None, *, step: int = 1,
                      days: tuple[str, str] | None = None) -> Grid:
        """Each row repeated once for each whole number from ``start`` to ``end`` (both kept; each a number, or the name of
        a whole-number column of the row), by ``step`` -- or, ``days=(from_column, to_column)``, once for each day
        between two of its date columns -- a new column ``name`` the number or the day: a flow's Generate Rows
        (DataCube's code: generate-rows.ts; Alteryx's Generate Rows), the database's ``range`` exploded, a row whose
        bound is empty repeated no time. Refused in DataCube's words: a bound that is no whole number, numbers the
        wrong way round, a step below 1, a days' column that is no date, a name taken."""
        if days is not None:
            return self._preset({'kind': 'generateRows', 'name': name, 'days': {'from': days[0], 'to': days[1]}})
        bound = lambda b: {'column': b} if isinstance(b, str) else b
        return self._preset({'kind': 'generateRows', 'name': name, 'from': bound(start), 'to': bound(end),
                             **({'step': step} if step != 1 else {})})

    def binning(self, columns: Sequence[str], bins: int) -> Grid:
        """Number ``columns`` each put into ``bins`` bins of equal width over its own range -- a flow's Binning (DataCube's
        code: binning.ts binningCommands; Alteryx's Multi-Field Binning, its equal intervals): the grid's own bins preset
        (``grid.bins``), the database finding each column's smallest and largest across every row and placing each
        value, a column of each value's bin beside it (its lower edge: ``notional_bins``), its range and upper edge
        hidden. Refused in DataCube's words: no column, one that is no number, a count of bins no whole number from 1."""
        return self._preset({'kind': 'binning', 'columns': list(columns), 'bins': bins})

    def custom_filter(self, condition: str) -> Grid:
        """Its rows those a condition written in Pure is true for -- a flow's Custom Filter (DataCube's code:
        custom-filter.ts customFilterCommands; Alteryx's Filter, its Custom filter): ``condition`` over ``$x``, the row
        (``"$x.notional > 1000 && $x.region == 'EMEA'"``), parsed and typed by the compiler, its column (``CustomFilter``,
        hidden) and the grid's filter keeping the rows it is true for, in place of every condition it had; the rows it
        leaves out its False (``relink(..., port='false')``). Refused in the compiler's words, or DataCube's: a
        condition that is no Boolean."""
        _expression(condition)
        return self._preset({'kind': 'customFilter', 'expression': condition})

    def dynamic_replace(self, rules: Grid | Sequence[Mapping[str, Any]], *, column: str = 'Column', condition: str = 'Condition',
                        value: str = 'Value', expressions: bool = False) -> Grid:
        """Its values replaced by rules -- a flow's Dynamic Replace (DataCube's code: dynamic-replace.ts; Alteryx's Dynamic
        Replace): each rule the column it replaces, a condition in Pure over ``$x``, the row (``"$x.region == 'EMEA'"``),
        and the value put there; of a column's rules the first whose condition holds, its own value where none does.
        ``rules``: a grid whose rows are the rules, read as it hands them on (the database reading them; ``column``,
        ``condition`` and ``value`` name its columns holding each part), or the rules as ``{'column', 'condition',
        'value'}`` mappings. ``expressions``: each value Pure over ``$x`` too, not a value of its column's type. The rules
        are pinned, as a Cross Tab's values are: read now, each parsed by the compiler, written in as a column for each
        column they name (``<column>_replaced``, handed on in its place); called again, read again, a column no rule names
        now shown as it is. Refused in the compiler's words, the rule named, or DataCube's: a column the grid has not, a
        value of another type than its column's, more than 200 rules."""
        if isinstance(rules, Grid):
            engine, frames = rules._headless()
            with frames.reading():
                said = _json.loads(engine.call('gridRules', rules._spec(engine, frames),
                                               _json.dumps({'column': column, 'condition': condition, 'value': value})))
            if 'problems' in said:
                raise ValueError('; '.join(said['problems']))
            given = said['rules']
        else:
            given = [dict(r) for r in rules]
        return self._preset({'kind': 'dynamicReplace', 'rules': given, 'expressions': expressions})

    def use_macro(self, name: str) -> Grid:
        """Its rows macro ``name``'s of its input's -- a flow's standard macro used (DataCube's code: macros.ts; the palette's
        Macro): a Pure box of the macro's function, judged and typed over its input's rows as ``pure`` judges one, named
        after the macro. Refused in DataCube's words or the compiler's: no macro of that name, a grid over no other grid's
        rows."""
        macro = next((m for m in self._page._doc.get('macros', []) if m['name'] == name), None)
        if macro is None:
            raise ValueError(f"'{name}' is not a macro of the page")
        engine, frames = self._headless()
        with frames.reading():
            said = _json.loads(engine.call('gridPure', self._spec(engine, frames), self._id, _json.dumps({'function': macro['pure']})))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        page = self._page
        with page._batch():
            page._apply_page([_plain(c) for c in said['commands']] + [_commands.page_rename_tile(tile=self._id, title=name)], 'a macro')
            page._current(self._view['cube'], 3)
        return self

    def import_pure(self, pure: str) -> list[Grid]:
        """Its Pure taken apart into boxes -- a flow's Import Pure (FL12; DataCube's code: pure-import.ts, the Pure window's
        Import as Boxes): each step of ``pure`` (``"$input->filter(...)->extend(...)->groupBy(...)"``) a Pure box of its
        own, this box its first, each after it over the one before, made as ``sheet.grid(input=...)`` makes a box. The
        boxes, in order. Refused in the compiler's words or DataCube's, as ``pure`` refuses."""
        _expression(pure)
        engine, _frames = self._headless()
        said = _json.loads(engine.call('pureImportSteps', pure))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        steps: list[str] = said['steps']
        boxes = [self.pure(steps[0])]
        for step in steps[1:]:
            boxes.append(self.sheet.grid(input=boxes[-1]).pure(step))
        if len(boxes) > 1:
            # what read this box reads the last from now, its Output, cache, blocking and message with its rows
            # (page-gestures.ts importedCommands, the canvas's rule)
            page = self._page
            moved = json.loads(page._session.headless().call('pageGesture', 'flow.imported', _json.dumps(
                {'page': page._doc, 'grid': self._id, 'made': [b.id for b in boxes[1:]]})))
            if 'problems' in moved:
                raise ValueError('; '.join(moved['problems']))
            if moved['commands']:
                page._apply_page([_plain(c) for c in moved['commands']], 'an Import Pure')
        return boxes

    def pure(self, pure: str | None) -> Grid:
        """Its rows a function of its input's, written in Pure -- a flow's Pure box (docs/DATACUBE_FLOW_MODE_2026_10_24.md
        FL12; DataCube's code: pure-box.ts, its page's setPure): an expression over ``$input``, its input's rows
        (``"$input->filter(x|$x.notional > 1000000)->extend(~fee:x|$x.notional * 0.001)"``), or a lambda of one parameter as
        written (``"rows|$rows->limit(5)"``) -- whatever no other tool says. The compiler parses it and types its rows as a
        relation before the page sets it, and the grid opens over them: a box after it reads its rows, its columns as the
        compiler types them. ``None``: its input's rows as they are again. Refused in the compiler's words, or DataCube's:
        a grid that reads no other grid's rows, a function of more than one parameter, one that never reads its input."""
        _ = self._view
        if pure is not None:
            _expression(pure)
        engine, frames = self._headless()
        with frames.reading():
            said = _json.loads(engine.call('gridPure', self._spec(engine, frames), self._id, _json.dumps({'pure': pure})))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        page = self._page
        with page._batch():
            page._apply_page([_plain(c) for c in said['commands']], 'a Pure box')
            page._current(self._view['cube'], 3)
        return self

    def multi_field(self, columns: Sequence[str], expression: str, *, suffix: str = '_new', in_place: bool | None = None) -> Grid:
        """One ``expression`` over each of ``columns`` -- a flow's Multi-Field Formula (DataCube's code:
        multi-field-columns.ts multiFieldCommands): Pure over ``$v``, each column's value in turn (``'$v->toOne()->trim()'``, ``'$v->toOne() *
        1.1'``; ``$x`` the row, for its other columns), a new column of each named ``<column><suffix>``. The compiler
        parses and types each, and refuses one in its words: ``trim`` over a number column, say. ``in_place``: each new
        column handed on by its column's name, the column itself hidden (Alteryx's "change existing fields") -- None, in
        place on a flow's box, as its window starts, and not on a board's grid."""
        _expression(expression)
        return self._preset({'kind': 'multiField', 'columns': list(columns), 'expression': expression, 'suffix': suffix,
                             **({'inPlace': True} if self._in_place(in_place) else {})})

    def dimension(self, name: str) -> Grid:
        """Grouped by the named dimension ``name``'s first level (its configuration's ``dimensions``: a name and its
        columns, coarsest first), as Properties' Dimensions tab's Group Rows by uses one (the gesture
        ``view.dimension``: dimensions.ts useDimension)."""
        return self._gesture('view.dimension', {'name': name, 'move': 'use'})

    def drill(self, name: str) -> Grid:
        """One level deeper into the named dimension ``name`` (dimensions.ts drillDown): its next column grouped by."""
        return self._gesture('view.dimension', {'name': name, 'move': 'down'})

    def drill_up(self, name: str) -> Grid:
        """One level back out of the named dimension ``name`` (dimensions.ts drillUp); out of its first, not grouped."""
        return self._gesture('view.dimension', {'name': name, 'move': 'up'})

    def edit_calculated(self, name: str, expression: str, *, rename: str | None = None, kind: str | None = None,
                        before: str | None = None) -> Grid:
        """The calculated column ``name`` computed per row by ``expression`` instead, as its Edit Column window's OK
        changes it (an ``editCalculated`` command, built headless as the editor builds it: gridColumn): ``rename`` its
        new name -- what read it (the groups, the pivot, the sorts, the filter, the columns computed from it, its
        settings) reads it by that -- and ``kind`` as for ``calculate``. In its place among the grid's columns, or
        ``before`` the calculated column named."""
        return self._calculated(name if rename is None else rename, expression, kind, name, before)

    def remove_calculated(self, name: str) -> Grid:
        """The calculated column ``name`` taken out, as Edit Column's Delete takes it (a ``removeCalculated`` command):
        refused, in the compiler's words, while another column reads it."""
        return self._apply([_commands.grid_remove_calculated(name=name)])

    def _calculated(self, name: str, expression: str, kind: str | None, replacing: str | None,
                    before: str | None = None) -> Grid:
        _expression(expression)
        return self._column({'name': name, 'expression': f'x|{expression}', 'kind': kind, 'replacing': replacing,
                             **({} if before is None else {'before': before})})

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
        grouping rather than following the grid's -- its state 'pinned' (``Chart.state``; 'frozen' keeps the grid's
        filter too) -- and stays when the grid is removed, as in DataCube; unless told, a
        chart told its ``x``, ``y`` or ``split`` keeps them, as a hand on them in DataCube's options pins the chart,
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

    def timeseries(self, *series: Any, title: str | None = None, **slots: Any) -> Timeseries:
        """A timeseries tile of this grid alone, under it (``Page.timeseries`` with this grid its one source): its date
        and instrument columns DataCube's to choose (``date=``, ``key=`` say them), ``series`` each an expression
        (``'close[AAPL]'``, ``'volatility(close[AAPL], 63)'``); none: DataCube's first series of it."""
        told = {k: slots.pop(k) for k in ('date', 'key', 'alias') if k in slots}
        return self._page.timeseries({'grid': self, **told}, series, title=title, **slots)

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
        made = _json.loads(engine.call('gridChart', _cube_spec(session, page._cube(cube), frames, engine, page._snapped(cube),
                                                               page._doc),
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


def _with_library(base: Mapping[str, Any], said: Mapping[str, Any]) -> Mapping[str, Any]:
    """A query's base with DataCube's Pure library beside its model, where DataCube says the query calls it
    (headless ``library``: datacube/src/pure-library.ts, ``datacube::ts::mean`` and the rest), which the engine plans
    it over after the model -- else the base as it is."""
    library = said.get('library')
    return base if library is None else {**base, 'library': library}


def _plan_request(tree: Any, base: Mapping[str, Any]) -> dict[str, Any]:
    """A query's plan request, as the engine's plan and plan-and-run take it: the query, its base's model and runtime,
    and what its base says beside them (a project's own rows, DataCube's Pure library)."""
    return {'lambda': _json.dumps(tree), 'model': base['model'], 'runtime': base['runtime'],
            **({'project': True} if base.get('project') is True else {}),
            **({'library': base['library']} if isinstance(base.get('library'), str) else {})}


def _engine_rows(session: Any, tree: Any, base: Mapping[str, Any], over: Any) -> Any:
    """A query's rows planned and run by the session's engine where they are (its plan-and-run: the frames' database as
    the read ``over`` sees them, D15; a warehouse; a saved query's project's own, as its ``base`` from DataCube says:
    F28), as Arrow typed by the plan -- every grid's rows, by one plan-and-run (D16). Refused in the planner's or the
    database's words."""
    from . import compiler
    status, kind, body = session.engine().answer_over('/plan-and-run.json', json.dumps(_plan_request(tree, base)), over)
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


def _cube_leaves(session: Any, cube: Mapping[str, Any], page: Mapping[str, Any] | None = None,
                 id: str | None = None, opened: bool = False) -> list[dict[str, str]]:
    """A cube's leaves as DataCube says what a cube reads (headless cubeLeaves: cube-document.ts leavesOf), in its order:
    each ``{"_type", "name"}`` -- a frame's name its table in the frames' database. ``page`` and ``id``, the page it is on
    and its id there: a grid over another grid's rows reads what that grid reads (DataCube's rule, given the page)."""
    return json.loads(session.headless().call('cubeLeaves', _json.dumps(cube), None if page is None else _json.dumps(page),
                                              id, opened))


def _cube_frames(session: Any, cube: Mapping[str, Any], page: Mapping[str, Any] | None = None,
                 id: str | None = None) -> list[str]:
    """The frames a cube reads, as DataCube says (its leaves), each once, in its order."""
    return list(dict.fromkeys(leaf['name'] for leaf in _cube_leaves(session, cube, page, id) if leaf['_type'] == 'frame'))


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
    """A chart of a grid: its mark, what it plots, its options; following its grid unless pinned or frozen (``state``)."""

    _kind = 'chart'

    @property
    def state(self) -> str:
        """Its state, as its pill says it: ``'dynamic'`` (it follows its grid: its grouping, its filter, the page's
        clicks), ``'pinned'`` (its grouping its own; the grid's filter followed) or ``'frozen'`` (its grouping and the
        grid's filter as they were when it was frozen, kept with it) -- read off the chart as DataCube writes it."""
        spec = self._view['spec']
        return 'frozen' if 'kept' in spec else 'pinned' if spec.get('frozen') else 'dynamic'

    @state.setter
    def state(self, state: str) -> None:
        """The chart put in a state as its pill puts it (chart-spec.ts chartState, through editChart): dynamic, regrouped
        the way its grid is now; pinned, its grouping kept; frozen, the grid's filter as it is now kept too. Refused in
        DataCube's words: a state that is none, or a detached chart (its grid removed) told to follow."""
        view = self._view
        self._page._apply_page([_commands.page_set_chart(chart=self._id,
                                                         spec=_made(self._page, view['cube'], {'state': state}, view['spec']))])

    def grid(self) -> Grid:
        """Its grid, as DataCube's Show Grid shows it (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D7; DataCube's code,
        headless chartOwnGrid: chart-spec.ts showGridWay and chartGrid, the screen's rule): the grid it reads, when that
        grid holds what it draws (it follows it; or it draws it as shown, a treemap; or, pinned, it takes nothing of its
        grouping, a scatter, a map, a gauge); else -- its grouping or its filter its own, or its grid removed -- a grid of
        its own query, made beside it, which it follows from then on, drawn as it was: grouped as the chart groups it (its
        column across and its split; a scatter's rows as they are, its plotted columns shown; a map by its places; a
        gauge's one figure the grid's total), its values the grid's Column Properties, its selection, and on a grid over
        a class its page filter, taken off as ``select(None)`` would (followGrid's own rule, the screen's Show Grid's
        too)."""
        view = self._view
        page, cube = self._page, view['cube']
        page._current(cube, 3)
        session = page._session
        engine, frames = session.headless(), session.frames
        with frames.reading():
            said = json.loads(engine.call('chartOwnGrid', _cube_spec(session, page._cube(cube), frames, engine,
                                                                     page._snapped(cube), page._doc),
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
class Text(Tile):
    """A text tile: a page's title, a note beside a chart -- its words in GitHub's Markdown, as DataCube's page-text.ts
    reads it (``# `` a heading, ``**bold**``, ``*italic*``, ``~~struck~~``, ``- `` or ``1. `` a list, ``- [ ] `` a task,
    ``> `` a quote, fenced code, a table, ``---`` a rule, ``[words](https://...)`` a link), drawn as text, never as
    HTML -- and the files it shows beside them (``files``: Markdown, text, CSV, JSON), pointed to, never copied."""

    _kind = 'text'

    @property
    def text(self) -> str:
        return self._view['text']

    @text.setter
    def text(self, text: str) -> None:
        _ = self._view
        # by the command the tile's editor sends (page-commands.ts setText), judged by DataCube in its words: blank, too
        # long, not text
        self._page._apply_page([_commands.page_set_text(tile=self._id, text=text)])

    def place(self, x: int, y: int, *, width: int | None = None, height: int | None = None,
              folded: bool | None = None) -> Text:
        """Its note moved to (``x``, ``y``) on its flow's canvas, as a comment is dragged there (page-commands.ts
        placeBox; docs/DATACUBE_FLOW_MODE_2026_10_24.md M7) -- a container's boxes with it, by as much, one step.
        Given a ``width`` and a ``height``, it is a CONTAINER --
        Alteryx's Tool Container: a frame round the boxes placed inside it, moved with it on the canvas -- ``folded`` to
        its head or not; without, a container keeps its frame. A text tile on a board is placed by its layout, and a
        frame is at least 200 by 100: refused in DataCube's words."""
        _ = self._view
        if (width is None) != (height is None):
            raise ValueError('a container is a width and a height, both')
        frame: dict[str, Any] | None = None
        if width is not None:
            frame = {'w': width, 'h': height, **({'collapsed': True} if folded else {})}
        elif folded is not None:
            raise ValueError('folded is a container\'s: give its width and height too')
        # a container's boxes moved with it, as the canvas's drag moves them (headless pageGesture: flow.ts containerMoves)
        page = self._page
        said = json.loads(page._session.headless().call('pageGesture', 'flow.place', _json.dumps(
            {'page': page._doc, 'tile': self._id, 'x': x, 'y': y, **({'frame': frame} if frame else {})})))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        page._apply_page([_plain(c) for c in said['commands']])
        return self

    def unframe(self) -> Text:
        """A container a comment again: its frame taken off, its place kept (placeBox with no frame)."""
        sheet = next(s for s in self._page.to_dict()['sheets'] if any(b['tile'] == self._id for b in s['layout'].get('boxes', [])))
        box = next(b for b in sheet['layout']['boxes'] if b['tile'] == self._id)
        self._page._apply_page([_commands.page_place_box(tile=self._id, x=box['x'], y=box['y'], frame=None)])
        return self

    @property
    def files(self) -> list[dict[str, Any]]:
        """The files this text box shows beside its words, each its ``name``, ``size`` and ``sha256``: a page points to
        its files, never copies them (DataCube's text-files.ts) -- the browser finds each where it was picked, or asks
        for it again by its name."""
        return [dict(f) for f in self._view.get('files', [])]

    @files.setter
    def files(self, files: Sequence[str | Path | Mapping[str, Any]]) -> None:
        """Sets the files this text box shows: each a path, read here to fingerprint it, or a file as ``files`` gives
        one -- by the command the screen sends when files are opened or dropped on it (page-commands.ts setTextFiles),
        judged by DataCube in its words: a kind a text box does not show (Markdown, text, CSV, JSON), too many, the same
        file twice."""
        _ = self._view
        # one path is no list: its letters would be read as paths (the 2026-10-25 audit)
        if isinstance(files, (str, Path, Mapping)):
            raise TypeError("a text box's files are a list: [path] for one")
        given: list[dict[str, Any]] = []
        for f in files:
            if isinstance(f, Mapping):
                # judged as given: a file missing one of the three, or carrying another, refused in DataCube's words
                given.append(dict(f))
                continue
            path = Path(f)
            if path.is_dir():
                raise ValueError(f'{path} is a folder: a text box shows files')
            # fingerprinted a piece at a time: a big file is never held whole (DataCube refuses it by its size)
            digest = hashlib.sha256()
            with path.open('rb') as handle:
                for piece in iter(lambda: handle.read(1 << 20), b''):
                    digest.update(piece)
            given.append({'name': path.name, 'size': path.stat().st_size, 'sha256': digest.hexdigest()})
        self._page._apply_page([_commands.page_set_text_files(tile=self._id, files=given)])


def _series_told(expr: str, name: str | None, split: str | None, pane: int | None, axis: str | None,
                 style: Mapping[str, Any] | None) -> dict[str, Any]:
    """A series as Python's plot and edit tell it: its expression's text (read by DataCube's parser) and the slots given."""
    told = {'expr': expr, 'name': name, 'split': split, 'pane': pane, 'axis': axis, 'style': dict(style) if style is not None else None}
    return {k: v for k, v in told.items() if v is not None}


def _timeseries_sources(sources: Any) -> list[dict[str, Any]]:
    """A timeseries' sources as ``Page.timeseries`` takes them -- a grid; a list of grids; or by alias, each a grid or a
    mapping of its grid and what DataCube's defaults would otherwise choose (``date``, ``key``) -- as DataCube is told them
    (headless timeseriesMade: each grid by its tile, the rest DataCube's to judge)."""
    def one(alias: str | None, given: Any) -> dict[str, Any]:
        if isinstance(given, Grid):
            told: dict[str, Any] = {'grid': given._id}
        elif isinstance(given, Mapping):
            told = {k: (v._id if k == 'grid' and isinstance(v, Grid) else v) for k, v in given.items()}
        else:
            raise ValueError(f'a timeseries reads grids: {given!r} is not one')
        return told if alias is None else {'alias': alias, **told}
    # one source told as a mapping: its grid and its own slots alone (``{'grid': g, 'date': 'day'}``) -- a mapping with any
    # other key is by alias, each value a grid or a source (the review's A10: an alias named ``grid`` misread it)
    single = isinstance(sources, Mapping) and 'grid' in sources and set(sources) <= {'grid', 'alias', 'date', 'key'} \
        and not any(isinstance(v, (Grid, Mapping)) for k, v in sources.items() if k != 'grid')
    if isinstance(sources, (Grid, Mapping)) and not single:
        if isinstance(sources, Grid):
            return [one(None, sources)]
        return [one(alias, given) for alias, given in sources.items()]
    if isinstance(sources, Mapping):
        return [one(None, sources)]
    return [one(None, g) for g in sources]


@_operations
class Timeseries(Tile):
    """A timeseries tile -- DataCube's timeseries viewer (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md): series of any
    number of the page's grids along one time axis, each an expression in gs-quant's vocabulary, PlotTool Pro's --
    ``returns(px:close[SPX], 1, log)``, ``volatility($A, 63)``, ``correlation($A, $B, 126)``, ``$A / $B`` -- read by
    DataCube's own parser; every figure the database's. Its spec is DataCube's page document's (``spec``); each change
    is the command its viewer sends (page-commands.ts setTimeseries), judged by DataCube in its words."""

    _kind = 'timeseries'

    @property
    def spec(self) -> dict[str, Any]:
        """Its spec as the page keeps it (a copy): its sources, series, frequency, range, panes, annotations and view. Read
        only: every change is one of the tile's changes below, by its one rule (the second audit's A-S2: a spec set whole
        went past the rule, a series re-pointed at another grid without a word)."""
        return copy.deepcopy(self._view['spec'])

    @property
    def series(self) -> list[dict[str, Any]]:
        """Its series, each its id (``A``), its expression as DataCube keeps it, and how it is drawn."""
        return copy.deepcopy(self._view['spec']['series'])

    @property
    def state(self) -> str:
        """Its state, as its header's button says it, in a chart's words: ``'dynamic'`` (it follows its grids' filters and
        the page's clicks) or ``'frozen'`` (each grid's filter as it was when it was frozen, kept with it) -- read off the
        tile as DataCube writes it (timeseries-spec.ts tsStateOf). No ``'pinned'``: its grouping is always its own."""
        return 'frozen' if 'kept' in self._view['spec']['sources'][0] else 'dynamic'

    @state.setter
    def state(self, state: str) -> None:
        """The tile put in a state as its button puts it (timeseries-edit.ts editTimeseries): frozen, each source's grid's
        filter as it is now kept; dynamic, following them again. Refused in DataCube's words: a state that is none."""
        self._edit({'kind': 'state', 'state': state})

    # Every change below is one of the tile's changes, made by its one rule -- timeseries-edit.ts editTimeseries, which
    # its viewer's editors ask too (headless timeseriesEdit) -- and judged as a tile Python makes is: no rule of Python's
    # own (the review's A2, docs/TIMESERIES_REVIEW_2026_10_25.md).

    def plot(self, expr: str, *, name: str | None = None, split: str | None = None, pane: int | None = None,
             axis: str | None = None, style: Mapping[str, Any] | None = None) -> Timeseries:
        """A series more, after its others: ``expr`` in DataCube's expression language, its id the next letter (``$B``
        reads it), a line per value of ``split`` (a text column of the grid it reads first), drawn in ``pane`` (0 the
        main one) on ``axis`` as ``style`` says -- what the viewer's + Series adds."""
        return self._edit({'kind': 'plot', 'series': _series_told(expr, name, split, pane, axis, style)})

    def edit(self, id: str, expr: str, *, name: str | None = None, split: str | None = None, pane: int | None = None,
             axis: str | None = None, style: Mapping[str, Any] | None = None) -> Timeseries:
        """Series ``id`` written again, in place -- what the viewer's series editor's OK makes: its expression and how it is
        drawn, each as told (None: none), hidden if it was. Refused when it has no series ``id``."""
        return self._edit({'kind': 'plot', 'replace': True, 'series': {'id': id, **_series_told(expr, name, split, pane, axis, style)}})

    def unplot(self, id: str) -> Timeseries:
        """Series ``id`` taken off (refused in DataCube's words while another reads it)."""
        return self._edit({'kind': 'unplot', 'id': id})

    def hide(self, id: str) -> Timeseries:
        """Series ``id`` left out of the drawing, its legend entry kept (its legend chip, clicked)."""
        return self._edit({'kind': 'hide', 'id': id, 'hidden': True})

    def show(self, id: str) -> Timeseries:
        """Series ``id`` drawn again."""
        return self._edit({'kind': 'hide', 'id': id, 'hidden': False})

    def options(self, **slots: Any) -> Timeseries:
        """Its other slots told -- ``grain`` (``'day'`` ... ``'year'``), ``align`` (how two series' dates combine:
        ``'intersect'``, the default, ``'step'``, ``'time'``, ``'nan'``, ``'zero'``), ``range`` (``{'last': '1y'}``,
        ``{'from': ..., 'to': ...}``), ``view`` (``{'compare': 'rebase', 'stats': True, 'subtitle': ..., 'note': ...}``),
        ``panes`` (``[{'log': True, 'invert': True, 'min': 0, 'max': 5}]``), ``annotations`` -- each replacing what it
        had, None taking it off: the words DataCube's reader takes."""
        return self._edit({'kind': 'slots', 'slots': slots})

    def annotate(self, annotation: Mapping[str, Any]) -> Timeseries:
        """An annotation more: ``{'kind': 'level', 'value': 70, 'pane': 1, 'label': 'overbought'}``, ``{'kind': 'event',
        'date': ..., 'label': ...}``, ``{'kind': 'period', 'from': ..., 'to': ..., 'label': ...}`` or marks from a grid's
        rows -- ``{'kind': 'marks', 'source': 'tr', 'where': [{'value': 'AAPL'}, {'column': 'side', 'value': 'BUY'}],
        'at': 'price', 'text': 'quantity', 'shape': 'up', 'label': 'Bought'}``, each row its source's matches pick at its
        date, on its ``at`` column's value (absent: a line at its date) -- Settings' Add. Its marks: ``marks``. And a
        person's own drawings, as the screen's Draw puts them: a line from a point to a point -- ``{'kind': 'line',
        'from': {'date': '2024-01-02', 'value': 180}, 'to': {'date': '2024-06-28', 'value': 210}, 'pane': 0, 'label':
        'Trend', 'color': 1}`` -- or a note pinned at one, ``{'kind': 'note', 'at': {...}, 'text': 'Bought here'}``, each
        point a date and a value as its pane draws it (no figure of the database's; left out under a compare)."""
        return self._edit({'kind': 'annotate', 'annotation': dict(annotation)})

    def unannotate(self, index: int) -> Timeseries:
        """Its annotation at ``index`` (0 its first) taken off."""
        return self._edit({'kind': 'unannotate', 'index': index})

    def source(self, alias: str, *, rename: str | None = None, date: str | None = None, key: Any = _KEEP) -> Timeseries:
        """Source ``alias`` changed, as Settings changes it: called ``rename`` (every expression naming it follows), read
        along ``date``, its instruments named by ``key`` (None: none)."""
        told: dict[str, Any] = {'kind': 'source', 'alias': alias}
        if rename is not None:
            told['rename'] = rename
        if date is not None:
            told['date'] = date
        if key is not _KEEP:
            told['key'] = key
        return self._edit(told)

    def read(self, grid: Grid) -> Timeseries:
        """Grid ``grid`` read as a source more, its alias, date and instrument column DataCube's rule's (Settings' Read
        another grid)."""
        if not isinstance(grid, Grid):
            raise ValueError(f'a timeseries reads grids: {grid!r} is not one')
        return self._edit({'kind': 'read', 'grid': grid._id})

    def unread(self, alias: str) -> Timeseries:
        """Source ``alias`` no longer read (refused while a series reads it, or when it is the only one)."""
        return self._edit({'kind': 'unread', 'alias': alias})

    def frame(self, series: str | None = None, *, start: str | None = None, end: str | None = None) -> Any:
        """Series ``series`` (None: its first) as a pandas DataFrame, every figure the database's: its query written by
        DataCube over the tables its grids read -- the viewer's own (headless timeseriesQuery: timeseries-query.ts), a
        series of several grids over one model of their tables -- planned and run by the engine's plan-and-run where the
        rows are (D16). Indexed by date, oldest first; a column per line: the series' name (else its id), or each value
        of its split (``SPX``, ``NDX``); a band's ``lower`` and ``upper``; candles' ``open``, ``high``, ``low`` and
        ``close``. ``start`` and ``end`` (ISO days): the dates it
        keeps -- kept by the database after every value is computed over every date (a window's first values have their
        history), and an index's base, a compare's, its first. Neither: every date, an index's base the first date the
        screen shows (the tile's range), as the screen draws it."""
        return self._framed(self._drawn(series, start, end))[0]

    def _drawn(self, series: str | None, start: str | None, end: str | None) -> dict[str, Any]:
        """What is asked of series ``series`` over ``start`` to ``end`` (``_asked``): those dates kept; neither, every
        date, its base the first date the screen shows -- the tile's range, by its screen's rule (``_window``) -- as the
        screen draws the tile (the Phase 2 review's 3: Python's base had been its first date)."""
        anchor = self._window(None, None).get('from') if start is None and end is None else None
        return self._asked(series, start, end, anchor)

    def _framed(self, asked: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
        """A series' frame (``frame``) as ``asked``, and its answer's words."""
        import pandas as pd
        engine = self._page._session.headless()
        rows, said = self._rows(asked)
        # what its rows say a reader should know -- several rows a date, no frequency to annualize by -- said as the
        # screen says them, by its rule (headless timeseriesNotes; the second audit's P-3: the frame dropped them)
        named = said['columns']
        told = {k: [None if pd.isna(v) else v for v in rows[named[k]].tolist()]
                for k in ('split', 'rows', 'perYear') if named[k] in rows.columns}
        if 'rows' in told or 'perYear' in told:
            notes = _json.loads(engine.call('timeseriesNotes', _json.dumps({**asked, 'columns': told})))
            if 'problems' in notes:
                raise ValueError('; '.join(notes['problems']))
            for note in notes['notes']:
                warnings.warn(note, stacklevel=2)
        # its dates as the database kept them (the window's alone), each a timestamp, as pandas indexes a series
        rows = rows.assign(**{named['date']: pd.to_datetime(rows[named['date']])})
        if said['band']:
            out = rows.set_index(named['date'])[[named['lower'], named['upper']]].rename(columns={named['lower']: 'lower', named['upper']: 'upper'})
        elif said['candles']:
            four = {named['open']: 'open', named['high']: 'high', named['low']: 'low', named['value']: 'close'}
            if said['split'] is not None:
                # a candle per instrument: its four prices, each a column per instrument
                out = rows.rename(columns=four).pivot(index=named['date'], columns=named['split'], values=list(four.values()))
                out.columns.names = [None, said['split']]
            else:
                out = rows.set_index(named['date'])[list(four)].rename(columns=four)
        elif said['split'] is not None:
            out = rows.pivot(index=named['date'], columns=named['split'], values=named['value'])
            out.columns.name = said['split']
        else:
            out = rows.set_index(named['date'])[[named['value']]].rename(columns={named['value']: said['name'] or said['series']})
        out.index.name = 'date'
        return out.sort_index(), said

    def _asked(self, series: str | None, start: str | None, end: str | None, anchor: str | None = None) -> dict[str, Any]:
        """What is asked of series ``series`` (None: its first, by DataCube's rule -- headless seriesOf -- which its answer
        names: the review's §9 A, a first chosen here had been Python's own rule) over the dates ``start`` to ``end``;
        ``anchor``, the first date shown where every date is kept (an index's base)."""
        window = {k: v for k, v in (('from', start), ('to', end)) if v is not None}
        return {'page': self._page._doc, 'tile': self._id, **({'series': series} if series is not None else {}), 'window': window,
                **({'anchor': anchor} if anchor is not None else {})}

    def _query(self, asked: dict[str, Any]) -> tuple[Any, Mapping[str, Any], dict[str, Any]]:
        """One of the tile's queries, its base and its answer's words: asked of DataCube headless (timeseriesQuery: a
        series' points, or with ``table`` one of the tables its screen's statistics panel shows) -- of one grid, as the
        grid is read (its copy, snapped; a class's mapping; the page's filters on it: headless gridTimeseries), of several
        over one model of their tables. Called within a read of the frames (Frames.reading)."""
        from . import compiler
        page = self._page
        session = page._session
        engine, frames = session.headless(), session.frames
        said = _json.loads(engine.call('timeseriesQuery', _json.dumps({
            **asked, 'host': frames.hosted(), 'copies': session.held_copies()})))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        if 'grid' not in said:
            return said['query'], _with_library(said, said), said
        # a series of one grid, read as the grid is: over its base as every read of it is (its copy, snapped; a class's
        # mapping; the page's filters on it) -- headless gridTimeseries, the tab's path through the grid's runner (the
        # review's P1: read live over the grid's leaves, beside its every other read)
        cube = said['grid']
        spec = _cube_spec(session, page._cube(cube), frames, engine, page._snapped(cube), page._doc, cube)
        said = _json.loads(engine.call('gridTimeseries', spec, _json.dumps(asked)))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return compiler.parse(said['query']), _with_library(spec, said), said

    def _rows(self, asked: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
        """The rows of one of the tile's queries (``_query``) as a pandas DataFrame, and its answer's words: planned and
        run by the engine's plan-and-run where the rows are (D16)."""
        from .frames import to_pandas
        session = self._page._session
        with session.frames.reading() as over:
            tree, base, said = self._query(asked)
            rows = to_pandas(_engine_rows(session, tree, base, over))
        return rows, said

    def query(self, series: str | None = None, *, start: str | None = None, end: str | None = None) -> dict[str, str]:
        """Series ``series``' query (None: its first), as ``frame`` reads it over ``start`` to ``end`` -- neither, as the
        screen draws it, its base the first date shown. ``pure``, the query DataCube wrote, as the compiler prints it;
        ``sql``, the SQL its plan runs where its rows are. Planned, nothing run (the engine's dry run, plan.json).
        Refused in the planner's words. Every question the tile asks, as its screen's Query window lists them:
        ``queries``."""
        return self._planned(self._drawn(series, start, end))

    def _planned(self, asked: dict[str, Any]) -> dict[str, str]:
        """One of the tile's queries (``_query``), its ``pure`` as the compiler prints it and its ``sql`` as its plan runs
        it where its rows are: planned, nothing run (the engine's dry run, plan.json)."""
        from . import compiler
        session = self._page._session
        with session.frames.reading():
            tree, base, _said = self._query(asked)
            answer = session.engine().answer('POST', '/plan.json', '', json.dumps(_plan_request(tree, base)))
        said = json.loads(answer.body)
        if not (isinstance(said, dict) and isinstance(said.get('plan'), str)):
            raise ValueError(answer.body.decode('utf-8', errors='replace'))
        return {'pure': compiler.print_tree(tree), 'sql': compiler.read_planned(said['plan']).sql}

    def code(self, language: str = 'python') -> str:
        """The program that builds this tile on a page of your own, in ``language`` (``'python'``: a page, a grid over
        each source's rows opened as its grid was -- a file, a remote file, a warehouse table, a class; else a DataFrame
        of yours -- the grid's ``timeseries``, and each formula's Pure printed; ``'typescript'``: its spec over your
        cubes and the page command the screen's Create ▸ Timeseries sends, applied by DataCube's one function) -- the
        screen's Code, by its one rule (headless timeseriesCode: timeseries-code.ts tileCode). Run, it builds this tile
        again: it names no tile, grid or sheet of this page."""
        page = self._page
        said = _json.loads(page._session.headless().call('timeseriesCode', _json.dumps(page._doc), self._id, language))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return said['code']

    def queries(self, *, start: str | None = None, end: str | None = None) -> list[dict[str, Any]]:
        """Every question the tile asks the database, as its screen's Query window lists them, by its one rule (headless
        timeseriesAsks: timeseries-query.ts tileAsks) -- to draw the chart, each source's bounds, each series' points and
        its view's; for its statistics panel, each line's statistics, each level's drawdowns, each pair's correlation
        and each level's calendar of returns. Each a dict: ``what`` (in words), ``serves`` (``chart``, ``statistics``,
        ``drawdowns``, ``correlations``, ``calendar``), ``kind``, its ``series``, ``with`` or ``source``, and ``pure``,
        the query DataCube wrote -- each by the one function the screen writes it by (askQuery) -- as the compiler
        prints it, and ``sql``, the SQL its plan runs where its rows are; one refused has ``refused`` instead, in
        DataCube's or the planner's words. Over ``start`` to ``end`` (ISO days, either open); neither, the dates the
        screen shows (the tile's range). Planned, nothing run."""
        page = self._page
        said = _json.loads(page._session.headless().call('timeseriesAsks', _json.dumps(page._doc), self._id))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        window = self._window(start, end)
        out: list[dict[str, Any]] = []
        for ask in said['asks']:
            told: dict[str, Any] = {'page': page._doc, 'tile': self._id}
            if ask['kind'] == 'bounds':
                told.update({'table': 'bounds', 'source': ask['source']})
            elif ask['kind'] == 'marks':
                told.update({'table': 'marks', 'marks': ask['marks']})
            else:
                told.update({'series': ask['series'], 'window': window, 'drawn': True,
                             **({'table': ask['kind']} if ask['kind'] != 'series' else {}),
                             **({'with': ask['with']} if 'with' in ask else {}),
                             **({'projection': ask['projection']} if 'projection' in ask else {})})
            row = {k: ask[k] for k in ('what', 'serves', 'kind', 'series', 'with', 'source', 'marks', 'projection') if k in ask}
            try:
                row.update(self._planned(told))
            except ValueError as e:
                row['refused'] = str(e)
            out.append(row)
        return out

    def table(self, *, start: str | None = None, end: str | None = None) -> Any:
        """Every line side by side, a row per date over ``start`` to ``end`` (ISO days, either open; neither, the tile's
        range, as its screen shows them) -- the screen's Data tab's: a pandas DataFrame indexed by date, oldest first,
        a column per line shown (none hidden, no condition: a condition is shaded, no figures of its own) named as the
        screen names it (a series' name, else its expression; a split's each line by its value beside it; a band's
        ``lower`` and ``upper``, a candle's four prices after it -- headless timeseriesLineNames), each its ``frame`` over
        those dates (an index's base their first); none where a line has no point that date. Each frame the database's,
        put side by side by their dates (nothing computed). (The Phase 2 review's 3: every date had been read, its base
        the first, hidden series and conditions among the columns, and the columns named by their ids.)"""
        import pandas as pd
        window = self._window(start, end)
        engine = self._page._session.headless()
        doc = _json.dumps(self._page._doc)
        frames = []
        for s in self._view['spec']['series']:
            if s.get('hidden'):
                continue
            f, said = self._framed(self._asked(s['id'], window.get('from'), window.get('to')))
            if said['shade']:
                continue

            def names(splits: list[Any] | None, series: str = s['id']) -> list[str]:
                got = _json.loads(engine.call('timeseriesLineNames', doc, self._id, series, _json.dumps(splits)))
                if 'problems' in got:
                    raise ValueError('; '.join(got['problems']))
                return got['names']

            plain = (lambda v: v.item() if hasattr(v, 'item') else v)
            if isinstance(f.columns, pd.MultiIndex):
                # a candle per instrument: each line's four prices after its name
                f.columns = [f'{n} {c[0]}' for n, c in zip(names([plain(c[1]) for c in f.columns]), f.columns)]
            elif said['split'] is not None:
                f.columns = names([plain(c) for c in f.columns])
            else:
                [own] = names(None)
                f.columns = [own] if not (said['band'] or said['candles']) else [f'{own} {c}' for c in f.columns]
            frames.append(f)
        if not frames:
            raise ValueError(f'{self!r} shows no line')
        return pd.concat(frames, axis=1).sort_index()

    def _tables(self) -> dict[str, Any]:
        """The series each of the tile's tables reads, by the rule its screen's tabs read (headless timeseriesTables)."""
        said = _json.loads(self._page._session.headless().call('timeseriesTables', _json.dumps(self._page._doc), self._id))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return said

    def _line(self, said: dict[str, Any], split: Any, name: str = 'name', series: str = '') -> str:
        """A line of a table, named: its series' name (else its id), and its split value after it -- every line of
        every series apart (two series split by the same column have lines of the same values; two of one name each its
        id after it)."""
        own = said[name] or series
        named = [s.get('name') or s['id'] for s in self._view['spec']['series']]
        if named.count(own) > 1:
            own = f'{own} ({series})'
        return own if split is None else f'{own} {split}'

    def _window(self, start: str | None, end: str | None) -> dict[str, Any]:
        """The dates a table reads: ``start`` and ``end`` as given (either open); neither, the tile's range, as its
        screen's tables read it (headless timeseriesWindow: timeseries-query.ts rangeWindow, the viewer's own rule) --
        a range of the last of a period from its sources' latest date, each source's bounds the database's."""
        if start is not None or end is not None:
            return {k: v for k, v in (('from', start), ('to', end)) if v is not None}
        import pandas as pd
        page = self._page
        engine = page._session.headless()
        said = _json.loads(engine.call('timeseriesWindow', _json.dumps(page._doc), self._id, 'null'))
        if 'bounds' in said:
            told: dict[str, dict[str, Any]] = {}
            for alias in said['bounds']:
                rows, _ = self._rows({'page': page._doc, 'tile': self._id, 'table': 'bounds', 'source': alias})
                told[alias] = {c: (None if len(rows) == 0 or pd.isna(rows[c].iloc[0]) else
                                   rows[c].iloc[0].isoformat() if hasattr(rows[c].iloc[0], 'isoformat') else str(rows[c].iloc[0]))
                               for c in rows.columns}
            said = _json.loads(engine.call('timeseriesWindow', _json.dumps(page._doc), self._id, _json.dumps(told)))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return said['window']

    def statistics(self, *, start: str | None = None, end: str | None = None) -> Any:
        """Each line's statistics over the dates ``start`` to ``end`` (ISO days, either open; neither, the tile's
        range, as its screen shows them), every figure the database's -- the screen's Summary and Risk tabs'
        (timeseries-query.ts statsQuery: first, last, change, extremes, mean, deviation; of a level its volatility,
        drawdown, growth, Sharpe, Sortino, Calmar, skew, kurtosis, up periods, best, worst, value at risk and expected
        shortfall): a pandas DataFrame, a row per line, a column per statistic (none where a line has none: a return has
        no volatility). A band and a condition have none. Why a level's are blank -- at or below zero, no frequency,
        under a year -- is said as a warning, in the screen's words (headless timeseriesStatNotes)."""
        import pandas as pd
        window = self._window(start, end)
        out = []
        blank = []
        for series in self._tables()['statistics']:
            rows, said = self._rows({'page': self._page._doc, 'tile': self._id, 'series': series, 'window': window, 'table': 'statistics'})
            named, why = said['columns'], said['why']
            value = (lambda r, c: None if c not in rows.columns or pd.isna(r[c]) else r[c])
            for _, r in rows.iterrows():
                split = r[named['split']] if named['split'] in rows.columns else None
                line = {'line': self._line(said, split, series=series)}
                line.update({stat: value(r, col) for stat, col in named.items() if stat != 'split'})
                out.append(line)
                if said['level']:
                    figure = (lambda v: None if v is None else float(v))
                    blank.append({'name': line['line'], **{k: figure(value(r, c)) for k, c in (('low', why['levelLow']), ('perYear', why['perYear']),
                                                                                                 ('cagr', named['cagr'])) if c in rows.columns}})
        if blank:
            notes = _json.loads(self._page._session.headless().call('timeseriesStatNotes', _json.dumps({'lines': blank})))
            if 'problems' in notes:
                raise ValueError('; '.join(notes['problems']))
            for note in notes['notes']:
                warnings.warn(note, stacklevel=2)
        return pd.DataFrame(out).set_index('line') if out else pd.DataFrame()

    def drawdowns(self, *, start: str | None = None, end: str | None = None) -> Any:
        """Each level line's deepest drawdowns over the dates ``start`` to ``end``, every date and figure the
        database's -- the screen's Drawdowns tab's (timeseries-query.ts drawdownsQuery): a pandas DataFrame, a row per
        drawdown, deepest first per line -- its line, its rank, its peak's date, its trough's and its recovery's (none:
        not yet), its depth (-0.2 for 20%), the days down and the days back. Neither ``start`` nor ``end``: the tile's
        range, as its screen shows them."""
        import pandas as pd
        window = self._window(start, end)
        out = []
        for series in self._tables()['drawdowns']:
            rows, said = self._rows({'page': self._page._doc, 'tile': self._id, 'series': series, 'window': window, 'table': 'drawdowns'})
            named = said['columns']
            for _, r in rows.iterrows():
                split = r[named['split']] if named['split'] in rows.columns else None
                out.append({'line': self._line(said, split, series=series),
                            **{k: (None if pd.isna(r[named[k]]) else r[named[k]]) for k in ('rank', 'peak', 'trough', 'recovered', 'depth', 'decline', 'recovery')}})
        return pd.DataFrame(out)

    def marks(self) -> Any:
        """Every mark the tile draws from a grid's rows -- its ``marks`` annotations: a trade at its price, an earnings
        day -- each row the database's, as its screen marks them (timeseries-query.ts marksQuery: the grid's filter as the
        tile reads it, the marks' matches, each at its date as a line's points are): a pandas DataFrame, a row per mark,
        each set latest first -- its marks' ``name``, its ``date``, its ``value`` (where they are drawn at one) and its
        ``text`` (where they say one). Past the most a set draws, its latest, said as the screen says it."""
        import pandas as pd
        out = []
        for i, a in enumerate(self._view['spec'].get('annotations', [])):
            if a.get('kind') != 'marks':
                continue
            rows, said = self._rows({'page': self._page._doc, 'tile': self._id, 'table': 'marks', 'marks': i})
            named = said['columns']
            if len(rows) > said['limit']:
                rows = rows.head(said['limit'])
                warnings.warn(said['cut'], stacklevel=2)
            for _, r in rows.iterrows():
                read = lambda k: None if named[k] is None or pd.isna(r[named[k]]) else r[named[k]]
                out.append({'name': said['name'], 'date': pd.Timestamp(r[named['date']]), 'value': read('value'), 'text': read('text')})
        return pd.DataFrame(out, columns=['name', 'date', 'value', 'text'])

    def projections(self) -> Any:
        """Every trend the tile projects -- its ``projection`` annotations -- each the database's, as its screen draws it
        (timeseries-query.ts projectionQuery: the series' line by least squares over its last ``window`` observations, on
        its time, and the fit at the first of them, at the last and at the date ``ahead`` past it): a pandas DataFrame, a
        row per projection -- its ``name``, its ``series``, the dates ``start``, ``last`` and ``end`` and the fit at each
        (``start_value``, ``last_value``, ``end_value``), its ``slope`` a day, and ``n``, the observations it is fit over;
        each figure none where it has no fit (under two observations of different times). Under a compare, refused in
        DataCube's words, as the screen leaves them out."""
        import pandas as pd
        out = []
        # over the tile's range, as its screen draws it: a series read from the first date shown (an index, a change)
        # fit on its values from there, as its line is
        window = None
        for i, a in enumerate(self._view['spec'].get('annotations', [])):
            if a.get('kind') != 'projection':
                continue
            window = self._window(None, None) if window is None else window
            rows, said = self._rows({'page': self._page._doc, 'tile': self._id, 'series': a['series'], 'table': 'projection', 'projection': i,
                                     'window': window})
            named = said['columns']
            for _, r in rows.iterrows():
                read = lambda k: None if pd.isna(r[named[k]]) else r[named[k]]
                day = lambda k: None if pd.isna(r[named[k]]) else pd.Timestamp(r[named[k]])
                out.append({'name': said['name'], 'series': a['series'], 'start': day('from'), 'start_value': read('fromValue'), 'last': day('last'),
                            'last_value': read('lastValue'), 'end': day('end'), 'end_value': read('endValue'), 'slope': read('slope'), 'n': read('n')})
        return pd.DataFrame(out, columns=['name', 'series', 'start', 'start_value', 'last', 'last_value', 'end', 'end_value', 'slope', 'n'])

    def project(self, series: str, *, window: int | None = None, ahead: str | None = None, label: str | None = None) -> Timeseries:
        """Series ``series``' trend projected, as Settings' Project adds it (an ``annotate`` of the tile's one rule): its
        line fit over its last ``window`` observations (absent, 63) and drawn ``ahead`` past its last date (a calendar
        period, ``3m``; absent, 3m) -- every figure the database's (``projections``)."""
        told: dict[str, Any] = {'kind': 'projection', 'series': series}
        if window is not None:
            told['window'] = window
        if ahead is not None:
            told['ahead'] = ahead
        if label is not None:
            told['label'] = label
        return self.annotate(told)

    def correlations(self, *, start: str | None = None, end: str | None = None) -> Any:
        """Each pair of lines' correlation over the dates ``start`` to ``end``, each the database's -- the screen's
        Correlations tab's (timeseries-query.ts correlationsQuery: a level's simple returns, anything else as it is,
        over the dates both have): a pandas DataFrame, a matrix of the lines -- each line with itself the database's too
        (1, or none where it has under two values or does not vary). Neither ``start`` nor ``end``: the tile's range, as
        its screen shows them."""
        import pandas as pd
        window = self._window(start, end)
        cells: dict[tuple[str, str], Any] = {}
        lines: list[str] = []
        for a, b in self._tables()['correlations']:
            rows, said = self._rows({'page': self._page._doc, 'tile': self._id, 'series': a, 'with': b, 'window': window, 'table': 'correlations'})
            named = said['columns']
            for _, r in rows.iterrows():
                la = self._line(said, r[named['a']] if named['a'] in rows.columns else None, series=a)
                lb = self._line(said, r[named['b']] if named['b'] in rows.columns else None, 'withName', series=b)
                v = None if pd.isna(r[named['corr']]) else r[named['corr']]
                cells[(la, lb)] = cells[(lb, la)] = v
                for x in (la, lb):
                    if x not in lines:
                        lines.append(x)
        return pd.DataFrame([[cells.get((x, y)) for y in lines] for x in lines], index=lines, columns=lines)

    # -- its views (the next phases' 3, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11): each the screen's own query --

    def _view_of(self, kind: str, series: str | None) -> str:
        """The series view ``kind`` draws: ``series`` as given, else the one the screen's draws (the view's ``of``, else
        the tile's first it can draw: headless timeseriesTables' ``views``, timeseries-query.ts viewSeries)."""
        if series is not None:
            return series
        picked = self._tables()['views'].get(kind)
        if picked is None:
            words = {'one': 'a series of one line', 'curve': 'a series split by a column (its tenors)'}
            raise ValueError(f'{self!r} has no {words[kind]} to draw')
        return picked

    def scatter(self, x: str | None = None, y: str | None = None, *, start: str | None = None, end: str | None = None) -> Any:
        """Series ``y`` against series ``x`` over the dates ``start`` to ``end`` (neither, the tile's range), a point a
        date both have shown -- the screen's Scatter (timeseries-query.ts scatterQuery): a pandas DataFrame indexed by
        date, oldest first, its columns ``x`` and ``y`` named as the screen's axes are (a series' name, else its
        expression); and in its ``attrs['regression']`` y's
        ordinary least squares on x over those dates -- ``slope``, ``intercept``, ``r2``, ``corr`` and ``n`` -- every
        figure the database's (none of the fit where x does not vary, or under two dates). Neither ``x`` nor ``y``: the
        two the screen's draws (the view's ``of``, else the tile's first two series of one line)."""
        import pandas as pd
        if x is None or y is None:
            pair = self._tables()['views'].get('scatter')
            if pair is None and (x is None or y is None):
                raise ValueError(f'{self!r} has no two series of one line to draw one against the other')
            x, y = x or pair[0], y or pair[1]
        rows, said = self._rows({'page': self._page._doc, 'tile': self._id, 'series': x, 'with': y, 'window': self._window(start, end), 'table': 'scatter'})
        named = said['columns']
        xs, ys = said['name'], said['withName']
        # two of one name (a series against itself) kept apart (the Phase 3 review's 11: one column of both)
        if xs == ys:
            xs, ys = f'{xs} (x)', f'{ys} (y)'
        out = pd.DataFrame({xs: rows[named['x']].tolist(), ys: rows[named['y']].tolist()},
                           index=pd.Index(rows[named['date']].tolist(), name='date'))
        first = (lambda c: None if len(rows) == 0 or pd.isna(rows[named[c]].iloc[0]) else float(rows[named[c]].iloc[0]))
        out.attrs['regression'] = {k: first(k) for k in ('slope', 'intercept', 'r2', 'corr')}
        out.attrs['regression']['n'] = 0 if len(rows) == 0 else int(rows[named['n']].iloc[0])
        return out

    def seasonality(self, series: str | None = None, *, years: int | None = None) -> Any:
        """Series ``series``' last ``years`` years before its latest (None: the view's, else 5) and its latest, each
        along its calendar -- the screen's Seasonality (timeseries-query.ts seasonalityQuery): a pandas DataFrame, a row
        per date, its ``year``, its ``place`` in the year by the tile's frequency (a day of the year; a week, month or
        quarter number) and its ``value``; and at its place the ``low``, ``high`` and ``mean`` of the years before the
        latest -- every figure the database's. The tile's compare takes each year from its own start (rebase 100, or its
        % change). ``df.pivot(index='place', columns='year', values='value')`` lays the years side by side."""
        import pandas as pd
        one = self._view_of('one', series)
        rows, said = self._rows({'page': self._page._doc, 'tile': self._id, 'series': one, 'table': 'seasonality',
                                 **({'years': years} if years is not None else {})})
        named = said['columns']
        keep = ['date', 'year', 'place', 'value', 'low', 'high', 'mean']
        out = pd.DataFrame({k: rows[named[k]].tolist() for k in keep})
        for k in ('year', 'place'):
            out[k] = out[k].astype('int64')
        out.attrs['latest'] = None if len(rows) == 0 else int(rows[named['latest']].iloc[0])
        return out

    def histogram(self, series: str | None = None, *, bins: int | None = None, start: str | None = None, end: str | None = None) -> Any:
        """Series ``series``' values over the dates ``start`` to ``end`` (neither, the tile's range) in ``bins`` bins of
        one width from their least to their most (None: the view's, else 30) -- the screen's Histogram
        (timeseries-query.ts histogramQuery): a pandas DataFrame, a row per bin a value fell in, its ``low``, ``middle``
        and ``high`` edges, its ``count``, and ``normal``: the count a normal of the values' mean and sample deviation
        would give it; and in its ``attrs`` the values' ``p5`` (5th percentile), ``mean``, ``sd`` and ``n`` -- every
        figure the database's."""
        import pandas as pd
        one = self._view_of('one', series)
        rows, said = self._rows({'page': self._page._doc, 'tile': self._id, 'series': one, 'window': self._window(start, end), 'table': 'histogram',
                                 **({'bins': bins} if bins is not None else {})})
        named = said['columns']
        out = pd.DataFrame({k: rows[named[k]].tolist() for k in ('low', 'middle', 'high', 'count', 'normal')})
        out['count'] = out['count'].astype('int64')
        first = (lambda c: None if len(rows) == 0 or pd.isna(rows[named[c]].iloc[0]) else float(rows[named[c]].iloc[0]))
        out.attrs.update({k: first(k) for k in ('p5', 'mean', 'sd')})
        out.attrs['n'] = 0 if len(rows) == 0 else int(rows[named['n']].iloc[0])
        return out

    def curve(self, series: str | None = None, *, dates: list[str] | None = None) -> Any:
        """Split series ``series``' lines across its key -- its tenors -- as of each of ``dates`` (None: the view's, else
        ``['latest', '1m', '1y']``: the latest date any line has, a period back from it, or a day) -- the screen's Curve
        (timeseries-query.ts curveQuery): a pandas DataFrame, a row per line in the screen's order (tenors by their span,
        3M before 2Y: headless timeseriesCurveOrder), a column per date asked, each its line's last value at or before
        it -- the database's; in its ``attrs['dates']`` each date asked as the database found it, and in its
        ``attrs['line_dates']`` each line's own day there (an earlier one: its value is of that day, as the screen's
        curve marks it; none: no value at or before it)."""
        import pandas as pd
        one = self._view_of('curve', series)
        rows, said = self._rows({'page': self._page._doc, 'tile': self._id, 'series': one, 'table': 'curve',
                                 **({'dates': list(dates)} if dates is not None else {})})
        named = said['columns']
        keys = [str(k) for k in rows[named['split']].tolist()]
        order = _json.loads(self._page._session.headless().call('timeseriesCurveOrder', _json.dumps(keys)))
        if 'problems' in order:
            raise ValueError('; '.join(order['problems']))
        at = order['order']
        out = pd.DataFrame({d['told']: [rows[d['value']].iloc[i] for i in at] for d in named['dates']},
                           index=pd.Index([keys[i] for i in at], name=said['split']))
        out.attrs['dates'] = {d['told']: (None if len(rows) == 0 or pd.isna(rows[d['asked']].iloc[0]) else str(rows[d['asked']].iloc[0])[:10])
                              for d in named['dates']}
        out.attrs['line_dates'] = {d['told']: {keys[i]: (None if pd.isna(rows[d['date']].iloc[i]) else str(rows[d['date']].iloc[i])[:10]) for i in at}
                                   for d in named['dates']}
        return out

    def calendar(self, series: str | None = None) -> Any:
        """Level series ``series``' return each month and each year over every date it has (None: the screen's
        Calendar tab's first level) -- the fund sheet's table (timeseries-query.ts calendarQuery): a pandas DataFrame, a
        row per year (latest first, as the screen lists them; a split's lines each apart, a level of the index ``line``),
        a column per month (``Jan`` ... ``Dec``) and the ``Year``'s -- each a simple return, the month before's last level
        to its own (the year before's to its own), the database's; none where there is no level the month (or the year)
        before -- the first, and after one missing -- or the level before is at or below zero. Read at least monthly: a
        quarterly or yearly tile's is refused."""
        import pandas as pd
        if series is None:
            # the first level shown, as the screen's tab picks it
            hidden = {s['id'] for s in self._view['spec']['series'] if s.get('hidden')}
            levels = [id for id in self._tables()['views']['calendar'] if id not in hidden]
            if not levels:
                raise ValueError(f'{self!r} has no level: a return, a difference or a deviation has no calendar of returns')
            series = levels[0]
        rows, said = self._rows({'page': self._page._doc, 'tile': self._id, 'series': series, 'table': 'calendar'})
        named = said['columns']
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        split = said['split'] is not None and named['split'] in rows.columns
        table: dict[tuple[Any, int], dict[str, Any]] = {}
        for _, r in rows.iterrows():
            key = (r[named['split']] if split else None, int(r[named['year']]))
            row = table.setdefault(key, {m: None for m in months} | {'Year': None})
            value = (lambda c: None if pd.isna(r[c]) else float(r[c]))
            row[months[int(r[named['month']]) - 1]] = value(named['return'])
            row['Year'] = value(named['yearReturn'])
        keys = sorted(table, key=lambda k: (str(k[0]), -k[1]))
        index = pd.MultiIndex.from_tuples(keys, names=['line', 'year']) if split else pd.Index([k[1] for k in keys], name='year')
        return pd.DataFrame([table[k] for k in keys], index=index, columns=[*months, 'Year'])

    def swap(self, instrument: Any, to: Any, *, source: str | None = None) -> Timeseries:
        """Instrument ``instrument`` read as ``to`` in every series that reads it (``close[SPX]``: a match on its source's
        instrument column) -- of source ``source`` alone when given -- a name that says it saying the other: the screen's
        Swap, by the tile's one rule (timeseries-edit.ts). An instrument column of numbers takes them as numbers
        (``swap(10, 20)``); the spaces round a name are not of it. Refused where no series reads it."""
        return self._edit({'kind': 'swap', 'from': str(instrument), 'to': str(to), **({'alias': source} if source is not None else {})})

    def shade(self, column: str, *, source: str | None = None, where: Sequence[Mapping[str, Any]] | None = None,
              name: str | None = None) -> Timeseries:
        """A 0/1 column shaded where it is not 0, in one step (FRED's recession bars): a condition series of it -- of source
        ``source`` (None: the first), its rows ``where`` picks (``[{'value': 'US'}]``, ``[{'column': 'region', 'value':
        'EU'}]``) -- named ``name`` (None: the column), shaded across every pane where it holds, its runs the database's:
        the screen's Settings' Shade, by the tile's one rule (timeseries-edit.ts). Refused, in its words, on a column that
        is no number."""
        return self._edit({'kind': 'shade', 'column': column, **({'source': source} if source is not None else {}),
                           **({'where': [dict(m) for m in where]} if where is not None else {}),
                           **({'name': name} if name is not None else {})})

    def _edit(self, change: dict[str, Any]) -> Timeseries:
        """``change`` (a timeseries-edit.ts TsChange) made by the tile's one rule, headless, and told as setTimeseries."""
        _ = self._view
        page = self._page
        said = json.loads(page._session.headless().call('timeseriesEdit', _json.dumps(page._doc), self._id, _json.dumps(change)))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        page._apply_page([_commands.page_set_timeseries(tile=self._id, spec=said['spec'])])
        return self

    def __repr__(self) -> str:
        return f'<Timeseries {self._id}>'


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
        """Its tiles in reading order, a stack's in its tabs' order (a flow's boxes, in the order they were added)."""
        return [self._page._tile_of(t) for t in self._page._tiles(self._doc['layout'])]

    @property
    def flow(self) -> bool:
        """Whether it is a flow sheet (``page.flow()``): its boxes on a canvas, each a grid, placed by ``box.place``."""
        return self._doc['layout']['kind'] == 'flow'

    def tidy(self) -> Sheet:
        """Its boxes placed by the flow's own rule, as the canvas's Tidy Up places them (headless pageGesture:
        page-gestures.ts tidyCommands; docs/DATACUBE_FLOW_MODE_2026_10_24.md M7): each a column after the boxes it reads,
        level with them -- one layout, or nothing when they are there already. A board's tiles are laid out in bands:
        refused in DataCube's words."""
        page = self._page
        said = json.loads(page._session.headless().call('pageGesture', 'flow.tidy', _json.dumps(
            {'page': page._doc, 'sheet': self.id})))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        commands = [_plain(c) for c in said['commands']]
        if commands:
            page._apply_page(commands, 'a tidy')
        return self

    def run(self, folder: str | Path) -> list[Path | str]:
        """The flow run as the canvas's Run runs it, its Outputs written (docs/DATACUBE_FLOW_MODE_2026_10_24.md M6): each
        box that writes a file (``grid.output(name)``) writes every one of its rows -- in its sorts' order, the columns it
        hands on under the names it hands them on by, those the box after it reads -- to ``folder``/its name, as CSV, by
        the tab's own writer (headless gridOutput: the same file, byte for byte); each that writes a table
        (``grid.output_table``) writes them into it, in one transaction (FL13). What was written, in the order of the
        flow's boxes: each file's path, each table as ``'table <name>'``. A Test that stops a run
        (``grid.stops_run()``), failing, writes none: refused, its failing checks said (FL8's refuse). A board has no
        Outputs: refused.

        IN STAGES (M8, the canvas's Run's rule: headless flowRun): the boxes after one that blocks
        (``grid.block_until_done()``) run, and their Outputs are written, once every Output before them is -- a stopping
        Test judged with its stage, so a failing one after a block stops what is after it alone. A Message
        (``grid.message``) says its words: a warning warned (``warnings.warn``), an error stopping the run as a failing
        Test does. Each run is kept in ``runs``, refused or not."""
        return self._run(Path(folder), dry=False)['written']

    def dry_run(self) -> dict[str, Any]:
        """The flow run as the canvas's Dry Run runs it (docs/DATACUBE_FLOW_MODE_2026_10_24.md M6): every box's rows counted
        as the canvas counts them (its view's, the row cap's total when it cut them), the boxes that stop a run judged,
        nothing written -- ``{'rows': {box: n}, 'would_write': [names], 'stopped': [words], 'messages': [said]}``,
        ``would_write`` the files a run would write (none from the stage on that a stopping box fails, each said in
        ``stopped``), ``messages`` each Message's ``{'box', 'level', 'said'}`` (M8). A board has no Outputs: refused."""
        return self._run(None, dry=True)

    @property
    def runs(self) -> list[dict[str, Any]]:
        """THE RUN HISTORY (F3's, this process's; M8): each run and dry run of this flow made here, the last last --
        ``{'at', 'seconds', 'dry', 'written', 'messages', 'stopped'}``, ``written`` what it wrote (a dry run's, what it
        would), ``stopped`` the words of what stopped it (none, it ran through). A copy."""
        return copy.deepcopy(self._page._flow_runs.get(self._id, []))

    def _plan(self, counts: Mapping[str, int]) -> dict[str, Any]:
        """The flow's stages and what its Messages say of ``counts`` (each box's rows), by DataCube's rule (headless
        flowRun: the canvas's Run's)."""
        said = json.loads(self._page._session.headless().call('flowRun', _json.dumps(self._page._doc), self._id, json.dumps(dict(counts))))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return said

    def _run(self, out: Path | None, dry: bool) -> dict[str, Any]:
        """A run (``out``, the folder its files go in) or a dry run, stage by stage (``_plan``): each stage's boxes counted
        -- every box in a dry run, a Message's in a run -- its stopping boxes and Messages judged with every earlier
        stage's, then its Outputs written (or, dry, named) unless one stops it. Kept in the page's run history."""
        layout = self._doc['layout']
        if layout.get('kind') != 'flow':
            raise ValueError(f'{self!r} is a board: a flow runs its Outputs')
        import time
        at = datetime.datetime.now(datetime.timezone.utc)
        started = time.perf_counter()
        boxes = {b['tile']: b for b in layout['boxes']}
        # a cached box copied again where its copy is of what it is no more
        self._page._refresh_caches()
        counts: dict[str, int] = {}
        judged: list[str] = []
        written: list[Path | str] = []
        stopped: list[str] = []
        messages: list[dict[str, Any]] = []
        refused: BaseException | None = None
        try:
            for stage in self._plan({})['stages']:
                for tile in stage:
                    if dry or boxes[tile].get('message') is not None:
                        view = self._page._tile(tile, Grid).view()
                        counts[tile] = view.total_rows if view.total_rows is not None else len(view.rows)
                judged += stage
                said = [m for m in self._plan(counts)['messages'] if m['box'] in judged]
                # each Message's warning warned once, as its stage is judged (its box's, never its place in the list)
                for m in said if not dry else []:
                    if m['level'] == 'warning' and m['box'] in stage:
                        warnings.warn(f"{m['box']}: {m['said']}", stacklevel=4)
                messages = said
                # a Test that stops a run, failing (grid.stops_run; FL8's refuse), or a Message's error, in this stage or
                # before: no Output written from here on, said in their words
                stopped = self._stopped(judged) + [f"{m['box']} said {m['said']}" for m in messages if m['level'] == 'error']
                if stopped and not dry:
                    before = f" (written before it: {', '.join(str(w) for w in written)})" if written else ''
                    raise ValueError('; '.join(stopped) + ': no Output written' + before)
                # a dry run counts every box all the same, naming no Output from a stop on
                if stopped:
                    continue
                if not dry:
                    assert out is not None
                    out.mkdir(parents=True, exist_ok=True)
                for tile in stage:
                    target = boxes[tile].get('output')
                    if target is None:
                        continue
                    if dry:
                        written.append(target['name'] if target['format'] == 'csv' else f"table {target['name']}")
                    elif target['format'] == 'table':
                        self._page._tile(tile, Grid)._write_table(target)
                        written.append(f"table {target['name']}")
                    else:
                        path = out / target['name']
                        # the text as the tab writes it, its CRLFs and its BOM as they are
                        path.write_text(self._page._tile(tile, Grid)._output(), encoding='utf-8', newline='')
                        written.append(path)
        except BaseException as e:
            refused = e
            raise
        finally:
            self._page._flow_runs.setdefault(self._id, []).append({
                'at': at, 'seconds': round(time.perf_counter() - started, 3), 'dry': dry, 'written': list(written),
                'messages': copy.deepcopy(messages), 'stopped': stopped or ([str(refused)] if refused is not None else []),
            })
            del self._page._flow_runs[self._id][:-_RUN_HISTORY]
        return {'rows': counts, 'would_write': written, 'stopped': stopped, 'messages': messages} if dry else {'written': written}

    def _stopped(self, tiles: Sequence[str] | None = None) -> list[str]:
        """The boxes set to stop a run (``grid.stops_run()``) that fail -- a Test's checks, a fixed Cross Tab's values with
        no column -- each in its words, of ``tiles`` (None: every box): what stops a run and a dry run alike (FL8's
        refuse)."""
        stopping = [(box['tile'], self._page._tile(box['tile'], Grid).failing) for box in self._doc['layout']['boxes']
                    if box.get('stops') and (tiles is None or box['tile'] in tiles)]
        return [f"{tile} stops the run: " + ', '.join(c['says'] for c in f) for tile, f in stopping if f]

    def grid(self, frame: Any = None, **given: Any) -> Grid:
        """A grid on this sheet, as ``page.grid(..., sheet=this)`` adds one: on a flow, a box -- after ``input``, the box
        it reads, when it is one of this flow's."""
        if 'sheet' in given:
            raise ValueError("a sheet's grid is on that sheet: it takes no sheet=")
        return self._page.grid(frame, sheet=self, **given)

    def text(self, text: str, *, title: str | None = None) -> Text:
        """A text tile on this sheet, a band of its own at its bottom (``title``: its own; None, numbered), as the
        menus' Create ▸ Text Box adds one (page-commands.ts addText) -- its words judged by DataCube, in its words. On a
        flow, a comment: a note on its canvas, placed as a box is (``tile.place(x, y)`` moves it)."""
        _ = self._doc
        applied = self._page._apply_page([_commands.page_add_text(sheet=self._id, text=text, title=title, columns=None)])
        return self._page._tile(applied[0]['id'], Text)

    def timeseries(self, sources: Any, series: Sequence[Any] = (), *, title: str | None = None, near: Tile | None = None,
                   **slots: Any) -> Timeseries:
        """A timeseries tile on this sheet -- under ``near`` (a tile of this sheet), else a band of its own at its bottom
        -- reading ``sources`` (a grid, grids, or grids by alias: ``{'px': prices, 'rates': rates}``; each grid's date
        and instrument columns DataCube's to choose unless told: ``{'grid': rates, 'date': 'asof', 'key': 'tenor'}``),
        plotting ``series`` (each an expression, ``'volatility(px:close[SPX], 63)'``, or a mapping of one and how it is
        drawn), with its other ``slots`` (``grain``, ``range``, ``view``, ``panes``, ``annotations``) -- as Create ▸
        Timeseries adds one (page-commands.ts addTimeseries), every word judged by DataCube."""
        _ = self._doc
        page = self._page
        # no series: DataCube's first series of its first grid (its price-like value, a line per instrument)
        given = {'sources': _timeseries_sources(sources), **({'series': list(series)} if series else {}),
                 **{k: v for k, v in slots.items() if v is not None}}
        said = json.loads(page._session.headless().call('timeseriesMade', _json.dumps(page._doc), _json.dumps(given)))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        if near is not None:
            _ = near._view
        applied = page._apply_page([_commands.page_add_timeseries(
            sheet=self._id, near=near._id if near is not None else None, spec=said['spec'], title=title, columns=None)])
        return page._tile(applied[0]['id'], Timeseries)

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
        # the page, started as DataCube starts a page (headless pageNew: page-document.ts newPage, the tab's New Page's
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
        # each flow's runs this process made, by its sheet's id (flow.runs: the run history, F3's, this process's)
        self._flow_runs: dict[str, list[dict[str, Any]]] = {}
        # each cached box's copy, by its id: what it was made of when copied (Grid._chain_key)
        self._cache_keys: dict[str, str] = {}
        # what changed it since it was last served, as the commands it was (a page's, a grid's with its id) -- what a page
        # open on it applies rather than opening it again (docs/DATACUBE_LIVE_COMMANDS_2026_10_18.md); None once it was
        # changed by no command, which the open page follows by opening it again
        self._unserved: list[dict[str, Any]] | None = []
        # each handle given, by its kind and id: the same object each time it is asked for
        self._handles: dict[tuple[type[Any], str], Any] = {}
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
        keeper has it (every id an edit makes is its editor's own, so no handle is renamed:
        docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md). One that no longer
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
                                                 sorted(self._ids | set(reads['ids']))), False)
            else:
                # admitted as a tab's write is, by the frames this host serves (Frames.unadmitted: the S7 audit's Y-10)
                settled = keeper.settle(self._key_name, self._version, self._unserved, self._session.frames.unadmitted)
        except Refused as refused:
            self._discard()
            raise ValueError(str(refused)) from None
        self._dirty = False
        self._unserved = []
        if settled.combined:
            # the keeper's page, the open page's write and this change on top of it
            self._version = 0
            self._pull()
        else:
            self._version = settled.version

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

    @property
    def texts(self) -> list[Text]:
        return [self._tile(v['id'], Text) for v in self._doc['views'] if v['kind'] == 'text']

    def text(self, text: str, *, title: str | None = None, sheet: Sheet | None = None) -> Text:
        """A text tile -- a title, a note -- on ``sheet`` (None: the first), a band of its own at its bottom
        (``Sheet.text``)."""
        return (sheet if sheet is not None else self.sheets[0]).text(text, title=title)

    @property
    def timeseries_tiles(self) -> list[Timeseries]:
        return [self._tile(v['id'], Timeseries) for v in self._doc['views'] if v['kind'] == 'timeseries']

    def timeseries(self, sources: Any, series: Sequence[Any] = (), *, title: str | None = None,
                   sheet: Sheet | None = None, **slots: Any) -> Timeseries:
        """A timeseries tile reading ``sources`` and plotting ``series`` (``Sheet.timeseries``): on ``sheet``, a band of
        its own at its bottom; None, under the first grid it reads, on that grid's sheet."""
        if sheet is not None:
            return sheet.timeseries(sources, series, title=title, **slots)
        told = _timeseries_sources(sources)
        first = next((self._tile(t['grid'], Grid) for t in told if 'grid' in t), None)
        if first is None:
            return self.sheets[0].timeseries(sources, series, title=title, **slots)
        return first.sheet.timeseries(sources, series, title=title, near=first, **slots)

    # -- building ------------------------------------------------------------------------------------------------------

    def grid(self, frame: Any = None, *, file: str | Path | None = None, remote: str | None = None,
             cls: str | None = None, project: str | None = None, revision: str | None = None, query: str | None = None,
             warehouse: str | None = None, table: str | None = None, catalog: str | None = None,
             input: Grid | None = None, name: str | None = None, rows: Sequence[str] = (), columns: Sequence[str] = (),
             measures: Mapping[str, str] | Sequence[tuple[str, str]] | None = None, filter: Any = None,
             sort: Sequence[tuple[str, str]] = (), dimensions: Sequence[str] = (), title: str | None = None,
             sheet: Sheet | None = None, snap: bool = False, port: str | None = None) -> Grid:
        """A grid over ``frame`` (a pandas or polars DataFrame, an Arrow table, or a function returning one), served
        under ``name`` (``frame``, ``frame_2``, ... unless given) -- or over a ``file`` (its path; ``name`` the file's
        name, its path's unless given), read in as the tab reads a file it is given (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md
        X4: once for its bytes), or a ``remote`` file (its URL), mounted as the tab mounts one (once for its URL) -- in a band of its own at
        the bottom of ``sheet`` (the first unless given) -- or over what the tab adds a grid over beside them (X4), read
        where this host reads it (DataCube's code, headless sourceCube: host-tables.ts leafOnHost) and titled as the tab
        titles it: a class (``cls``, its name or its path), of the page's model at its pin unless ``revision`` names
        another, or of another ``project`` by its path at a ``revision`` (class-source.ts classAt, the tab's Add a Grid's
        rule); a saved query of this user's store, by its id (``query``); a warehouse table (``warehouse``, its address;
        ``table``, ``schema.table``; ``catalog``), read under the session this host holds for that warehouse -- or over
        another grid's rows, ``input`` (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL20: a flow's box after another), read live:
        an edit of ``input`` reaches this grid, which DataCube makes (headless rowsCube) on ``input``'s sheet unless
        ``sheet`` is given -- ``port``, one of ``input``'s ports read instead (a Filter's ``'false'``, a Join's ``'left'``
        or ``'right'``), as the canvas adds a box on one. Each query
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
        if input is not None and input._page is not self:
            raise ValueError(f'{input!r} is another page\'s')
        if title is not None:
            title = _given_name(self._session, 'tile', title, 'a title')
        session = self._session
        if sum(b is not None for b in (frame, file, remote, cls, query, warehouse, input)) != 1:
            raise ValueError('a grid is over a frame, a file (file=path), a remote file (remote=url), a class (cls=...), a '
                             'saved query (query=id), a warehouse table (warehouse=url, table=...) or another grid\'s rows '
                             '(input=grid): one of them')
        if remote is not None and name is not None:
            raise ValueError("a remote file is named after its URL: it takes no name")
        if (project is not None or revision is not None) and cls is None:
            raise ValueError("project= and revision= say where a class is read: they go with cls=")
        if (table is not None or catalog is not None) and warehouse is None:
            raise ValueError("table= and catalog= name a warehouse table: they go with warehouse=")
        if name is not None and (cls is not None or query is not None or warehouse is not None):
            raise ValueError('a class, a saved query and a warehouse table are named as DataCube names them: they take no '
                             'name')
        if name is not None and input is not None:
            raise ValueError("another grid's rows are named as DataCube names them (as the grid it reads is called): they "
                             'take no name')
        if port is not None and input is None:
            raise ValueError("port= is one of another grid's ports (a Filter's 'false', a Join's 'left' or 'right'): it goes "
                             'with input=')
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
            if input is not None:
                # over another grid's rows: its cube and the command that adds it are DataCube's (headless rowsCube), over
                # what this host serves, on that grid's sheet unless one is named
                engine, frames = session.headless(), session.frames
                with frames.reading():
                    made = _json.loads(engine.call('rowsCube', _json.dumps({
                        'page': self._doc, 'input': input.id, 'host': frames.hosted(),
                        'sheet': None if sheet is None else sheet.id, 'title': title,
                        **({'port': port} if port is not None else {})})))
                if 'problems' in made:
                    raise ValueError('; '.join(made['problems']))
                for note in made['notes']:
                    warnings.warn(note, stacklevel=2)
                with self._batch(tell=False):
                    applied = self._apply_page([_plain(made['command'])], 'a title')
            else:
                cube = (self._source_cube(given, len(self._doc['cubes']) + 1) if given is not None
                        else self._frame_cube(served, len(self._doc['cubes']) + 1)[0] if read is None
                        else self._leaf_cube(read, len(self._doc['cubes']) + 1))
                # on its sheet, as the screen adds a grid (page-commands.ts addGrid): its id after every one held, a band
                # at the bottom, told nothing yet
                with self._batch(tell=False):
                    applied = self._apply_page([_commands.page_add_grid(document=cube, sheet=target.id, near=None,
                                                                        title=title, columns=None)], 'a title')
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
             sheet: Sheet | None = None, snap: bool = False, live: bool | None = None) -> Grid:
        """A grid of ``mine`` joined with ``theirs`` (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17), as the tab's Join…
        makes one: each grid's rows at their finest level, as each is now, combined as ``how`` says -- DataCube's own
        join (join-how.ts), read strictly: ``{'form': 'join', 'kind': 'inner' | 'left' | 'right' | 'full', 'on':
        {'rows': [{'left': 'customer_id', 'op': '==', 'right': 'id'}]}, 'prefix': 'their_', 'keep': None}`` (``keep``,
        the columns of theirs to keep, every one when None; ``'once': True``, each row of mine with the first of theirs
        of its key at most, the condition rows of ``==`` alone), ``{'form': 'semi' | 'anti', 'on': ...}``, ``{'form':
        'asOf', 'match': {...}, 'on': ..., 'prefix', 'keep'}`` or ``{'form': 'append'}``. Written by DataCube (headless
        joinCube) over the frames the two read, judged by the compiler -- a refusal in its words, the page as it was --
        and added beside ``mine`` on its sheet -- or at the bottom of ``sheet``, given -- by the page command every grid is
        added by (DataCube's placement, the tab's Join…'s). ``live``: each input read live, the grid as it is now, so an
        edit of either reaches the join (a flow's Join box: docs/DATACUBE_FLOW_MODE_2026_10_24.md M2, FL6) -- None, live
        when ``mine`` is a box of a flow; else each input kept by value, the join not following later edits of the grids
        joined. ``snap``: the join read once, by its grid's Snap (``grid.snap()``); refused, the page is as it was."""
        for g in (mine, theirs):
            if g._page is not self:
                raise ValueError(f'{g!r} is another page\'s')
        if sheet is not None and sheet._page is not self:
            raise ValueError(f'{sheet!r} is another page\'s')
        if sheet is not None:
            _ = sheet._doc
        if title is not None:
            title = _given_name(self._session, 'tile', title, 'a title')
        if live is None:
            live = mine.sheet.flow
        session = self._session
        engine, frames = session.headless(), session.frames
        # the tables its leaves read, each grid read as the page has it, under the name it shows, and the join's grid placed
        # beside the first, are DataCube's (headless joinCube: host-tables.ts leafTables, joinInputOf): Python hands in
        # what it serves
        with frames.reading():
            made = _json.loads(engine.call('joinCube', _json.dumps({
                'page': self._doc, 'mine': mine.id, 'theirs': theirs.id, 'how': how, 'host': frames.hosted(),
                'sheet': None if sheet is None else sheet.id, 'title': title, 'live': live})))
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

    def union(self, *grids: Grid, title: str | None = None, sheet: Sheet | None = None, live: bool | None = None) -> Grid:
        """The rows of every grid, one after another -- a flow's Union (Alteryx's, of any number), the Join window's
        Append: ``join`` of the first two with ``{'form': 'append'}``, each grid after them one more input in turn
        (page-commands.ts setJoin's ``more``), columns matched by name -- one a grid lacks empty in its rows. ``live`` as
        ``join``'s: when not, each input after the first two frozen too (a copy of what it is now)."""
        if len(grids) < 2:
            raise ValueError(f'a union reads two grids or more, not {len(grids)}')
        for g in grids:
            if g._page is not self:
                raise ValueError(f'{g!r} is another page\'s')
        if live is None:
            live = grids[0].sheet.flow
        grid = self.join(grids[0], grids[1], {'form': 'append'}, title=title, sheet=sheet, live=live)
        if len(grids) > 2:
            more = [{'grid': g.id} for g in grids[2:]]
            with self._batch():
                self._apply_page([_commands.page_set_join(grid=grid.id, how={'form': 'append'}, more=more)], 'a union')
                if not live:
                    self._apply_page([_commands.page_set_input(grid=grid.id, side=f'more{k}', input=None)
                                      for k in range(1, len(more) + 1)], 'a freeze')
                self._current(grid._view['cube'], 3)
        return grid

    def append_fields(self, mine: Grid, theirs: Grid, *, prefix: str = 'their_', keep: Sequence[str] | None = None,
                      title: str | None = None, sheet: Sheet | None = None, live: bool | None = None) -> Grid:
        """Each row of ``mine`` beside every row of ``theirs`` -- a flow's Append Fields (Alteryx's), the Join window's
        Every pair: ``join`` with no condition, matching rows only (join-how.ts isEveryPair), their names that mine has
        given ``prefix``, ``keep`` the columns of theirs kept (every one when None). A small table's values on each row of
        a big one; its rows are the two counts multiplied."""
        return self.join(mine, theirs, {'form': 'join', 'kind': 'inner', 'on': {'rows': []}, 'prefix': prefix,
                                        'keep': None if keep is None else list(keep)}, title=title, sheet=sheet, live=live)

    def fuzzy_match(self, mine: Grid, theirs: Grid, on: tuple[str, str], *, within: int | None = None, kind: str = 'inner',
                    score: Any = _KEEP, prefix: str = 'their_', title: str | None = None, sheet: Sheet | None = None,
                    live: bool | None = None) -> Grid:
        """``mine`` joined with ``theirs`` where their text is alike -- a flow's Fuzzy Match (Alteryx's), the Join window's
        similar comparison: ``on`` is my column and theirs, ``within`` how many edits apart they may be (a letter added,
        taken away or changed is one: the database's ``levenshteinDistance``; None, the Join window's own, join-how.ts
        SIMILAR_WITHIN), ``kind`` the join's (``'inner'``: the pairs alone; ``'left'``: every row of mine). ``score``: a
        column of this name holding each pair's likeness, 0 to 100 (100 less its edits apart for each 100 letters of the
        longer, rounded: the database's), as the window's Fuzzy Match starts with -- matching rows only, so ``'score'``
        when ``kind`` is ``'inner'`` and none otherwise unless given; None, none. A score asked of another kind is refused
        in DataCube's words, never dropped."""
        left, right = on
        row = {'left': left, 'op': '~', 'right': right, **({'within': within} if within is not None else {})}
        scored = ('score' if kind == 'inner' else None) if score is _KEEP else score
        return self.join(mine, theirs, {'form': 'join', 'kind': kind, 'on': {'rows': [row]}, 'prefix': prefix, 'keep': None,
                                        **({'score': scored} if scored is not None else {})},
                         title=title, sheet=sheet, live=live)

    def find_replace(self, mine: Grid, theirs: Grid, column: str, find: str, replace: str, *, prefix: str = 'their_',
                     title: str | None = None, sheet: Sheet | None = None, live: bool | None = None) -> Grid:
        """Each row's ``column`` of ``mine`` found among ``theirs``' ``find`` and replaced by their ``replace`` -- a flow's
        Find Replace (Alteryx's): every row of mine joined to theirs on ``column`` equal to ``find``, their ``replace``
        alone brought over (page.join), then ``column`` handed on as their value where one was found, its own where none
        was, theirs left out (DataCube's code: find-replace.ts findReplaceCommands). A ``find`` theirs repeats matches its
        first row only (in an order of their other columns, rising): each row of mine stays once (the join's ``once``). The
        database joins and chooses."""
        joined = self.join(mine, theirs, {'form': 'join', 'kind': 'left', 'on': {'rows': [{'left': column, 'op': '==', 'right': find}]},
                                          'prefix': prefix, 'keep': [replace], 'once': True}, title=title, sheet=sheet, live=live)
        # their column as the join brings it out: the last, prefixed where a column of mine has its name
        brought = joined._cube['query']['columns'][-1]['name']
        return joined._preset({'kind': 'findReplace', 'column': column, 'with': brought})

    def sheet(self, name: str | None = None) -> Sheet:
        """A new sheet after the last (``name``: its own; None, named after its first grid)."""
        # as the screen adds one -- what is not text refused by addSheet's reader, in DataCube's words (page-commands.ts addSheet): its id after every one the page has held
        applied = self._apply_page([_commands.page_add_sheet(name=name)], "a sheet's name")
        return self._sheet(applied[0]['id'])

    def flow(self, name: str | None = None) -> Sheet:
        """A new flow sheet after the last (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL4, FL20; ``name``: its own; None,
        "Flow n"): a canvas of boxes, each a grid that does one step -- ``flow.grid(...)`` its first, ``flow.grid(input=box)``
        each after another, ``box.place(x, y)`` where it sits -- as the screen's Create ▸ Flow adds one."""
        # as the screen adds one (page-commands.ts addFlow): its id after every one the page has held
        applied = self._apply_page([_commands.page_add_flow(name=name)], "a sheet's name")
        return self._sheet(applied[0]['id'])

    def stack(self, *tiles: Tile) -> None:
        """``tiles`` in one place, as tabs, one shown at a time, where the first one is: all on one sheet. Stacked as
        DataCube stacks a tile dropped on another's middle (bands.ts stackTiles): the first one's place keeps its own
        tabs first, the others follow in the order given, and a tile stacked already brings its stack's others after
        them."""
        strays = [t for t in tiles if not isinstance(t, Tile)]
        if strays:
            raise ValueError(f'a stack is of tiles (grids, charts and texts), not {", ".join(repr(t) for t in strays)}')
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

    def parameter(self, name: str, type: str, value: Any, *, choices: Sequence[Any] | None = None,
                  label: str | None = None) -> None:
        """A parameter of the page -- an analytic app's input (DataCube's code: page-parameters.ts; Alteryx's interface
        tools): a value any Pure on the page reads as ``$name`` -- a Custom Filter's condition (``"$x.notional > $min"``),
        a Formula, a Pure box, a Dynamic Replace's rules -- bound into each query that reads it where the query is sent,
        as the page's filters are put on it; ``type`` one of ``String``, ``Integer``, ``Float``, ``Decimal``,
        ``Boolean``, ``StrictDate`` (``'2024-01-31'``), ``DateTime``; ``choices``, a pick-list's values (its value one
        of them); ``label``, what its input says. Set (a ``setParameter`` command): the one of its name replaced, else
        added. Refused in DataCube's words: a name Pure cannot read, a value of another type, none of its choices."""
        parameter: dict[str, Any] = {'name': name, 'type': type, 'value': _parameter_value(type, value)}
        if choices is not None:
            parameter['choices'] = [_parameter_value(type, c) for c in choices]
        if label is not None:
            parameter['label'] = label
        self._apply_page([_commands.page_set_parameter(parameter=parameter)])
        self._refresh_caches()

    def set_parameter(self, name: str, value: Any) -> None:
        """Parameter ``name``'s value changed, as its input on the page's parameters bar changes it (a ``setParameter``
        command of it whole, its value ``value``): every grid reads it so from now. Refused as ``parameter`` is."""
        found = next((p for p in self._doc.get('parameters', []) if p['name'] == name), None)
        if found is None:
            raise ValueError(f"'{name}' is not a parameter of the page")
        self._apply_page([_commands.page_set_parameter(parameter={**found, 'value': _parameter_value(found['type'], value)})])
        self._refresh_caches()

    @property
    def parameters(self) -> dict[str, Any]:
        """Its parameters' values, by name, in the page's order."""
        return {p['name']: Decimal(p['value']) if is_decimal(p['type']) and isinstance(p['value'], str) else copy.deepcopy(p['value'])
                for p in self._doc.get('parameters', [])}

    def remove_parameter(self, name: str) -> None:
        """Parameter ``name`` taken off (a ``removeParameter`` command), as the x on its input takes it: a Pure that
        still reads it is refused by the compiler, in its words."""
        self._apply_page([_commands.page_remove_parameter(name=name)])
        self._refresh_caches()

    def _refresh_caches(self) -> None:
        """Each cached box whose copy is of what it is no more (Grid._chain_key: an input's edit, a parameter's value)
        copied again, those it reads first -- as the canvas lets such a copy go and copies the box again (FL10, the
        roadmap audit of 2026-10-26)."""
        cached = [self._tile(b['tile'], Grid) for s in self._doc['sheets'] if s['layout'].get('kind') == 'flow'
                  for b in s['layout']['boxes'] if b.get('cache')]
        for grid in sorted(cached, key=lambda g: len(json.loads(g._chain_key()))):
            if grid.snapped is not None and self._cache_keys.get(grid.id) != grid._chain_key():
                grid.snap()
                self._cache_keys[grid.id] = grid._chain_key()

    def macro(self, name: str, boxes: Sequence[Grid]) -> None:
        """A standard macro (DataCube's code: macros.ts; a box's menu, Save as Macro): ``boxes``, each over the one before
        it, their steps saved as one Pure function of the first one's input's rows -- each box's own steps read off its
        query as the screen's grid reads them (headless gridFunction), composed (macroComposed) -- kept on the page as
        ``name`` (a ``setMacro`` command; a macro of that name replaced). Used after any box by ``grid.use_macro(name)``;
        the page's parameters it reads stay ``$name``. Refused in DataCube's words: a box over no other box's rows, a box
        that reads none of the one before it."""
        boxes = list(boxes)
        if not boxes:
            raise ValueError('a macro is made of one box at least')
        for before, after in zip(boxes, boxes[1:]):
            source = next(c['cube']['source'] for c in self._doc['cubes'] if c['id'] == after._view['cube'])
            if source.get('_type') != 'rows' or source.get('input', {}).get('grid') != before._id:
                raise ValueError(f"{after._id} reads none of {before._id}'s rows: each box of a macro reads the one before it")
            # a box's own steps are its rows as it hands them on: one reading another port (a Filter's False, a Join's L
            # or R) reads rows no step of it says (the roadmap audit of 2026-10-26: its True rows were taken)
            if source['input'].get('port') is not None:
                raise ValueError(f"{after._id} reads {before._id}'s {source['input']['port']} rows: a macro's boxes each read the rows the one before hands on")
        steps = []
        for box in boxes:
            engine, frames = box._headless()
            with frames.reading():
                said = _json.loads(engine.call('gridFunction', box._spec(engine, frames)))
            if 'problems' in said:
                raise ValueError('; '.join(said['problems']))
            steps.append(said['pure'])
        composed = _json.loads(self._session.headless().call('macroComposed', _json.dumps(steps)))
        if 'problems' in composed:
            raise ValueError('; '.join(composed['problems']))
        self._apply_page([_commands.page_set_macro(name=name, pure=composed['pure'])])

    @property
    def macros(self) -> list[str]:
        """Its macros' names, in the page's order."""
        return [m['name'] for m in self._doc.get('macros', [])]

    def remove_macro(self, name: str) -> None:
        """Macro ``name`` taken off (a ``setMacro`` command of no function): the boxes using it keep their own Pure."""
        self._apply_page([_commands.page_set_macro(name=name, pure=None)])

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
        leaves = [leaf for c in doc['cubes'] for leaf in _cube_leaves(session, c['cube'], doc, c['id'])]
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
        self._changed()
        return applied

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
        return self._tile(id, {'grid': Grid, 'chart': Chart, 'text': Text, 'timeseries': Timeseries}[view['kind']])

    def _sheet(self, id: str) -> Sheet:
        return self._tile(id, Sheet)

    def _update(self, name: str, frame: Any) -> None:
        """Frame ``name`` served again over ``frame``: each grid's cube over it reopened over the frame as the compiler
        types it now (``_reopened``); the page served again when one changed."""
        session = self._session
        session.register(frame, name)
        # every cube reading the frame reopened (a join of it among them), whichever changed: their commands written once
        reading = [c['id'] for c in self._doc['cubes'] if name in _cube_frames(self._session, c['cube'], self._doc, c['id'])]
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
        said = self._opened_cube(held, given or {}, leave_out, id)
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
                     leave_out: bool = False, id: str | None = None) -> tuple[list[str], bool, dict[str, Any]] | str:
        """A saved cube as DataCube opens it over the frames as they are now -- a frame's, a join's of frames (headless
        gridOpened, by the rule of the page's ``rebuilt`` command: cube-document.ts cubeOpenedAgain) -- what DataCube
        says of what changed, whether the page takes it (cubeTaken), and the source as it is now, which the page's
        ``rebuilt`` command carries -- or, ``leave_out`` and a source a page leaves out, DataCube's note of it."""
        engine, frames = self._session.headless(), self._session.frames
        with frames.reading():
            # its page and its id there: a grid over another grid's rows opens over that grid as it is now (DataCube's rule)
            made = json.loads(engine.call('gridOpened', _json.dumps(saved), frames.hosted(), self.name,
                                          len(self._doc['cubes']), dict(given), leave_out, _json.dumps(self._doc), id))
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
