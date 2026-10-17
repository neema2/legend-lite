"""A DataCube page built in Python: grids over frames, their charts, sheets, layouts and stacks -- everything the page does
in DataCube, said in code (docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md).

    import legend_lite as ll
    page = ll.Page('Q3 review')
    trades = page.grid(trades_df, name='trades', rows=['region', 'desk'], columns=['year'],
                       measures={'notional': 'sum'}, filter=[('region', 'notEqual', 'APAC')])
    trades.calculate('margin', '$x.pnl->toOne() / $x.notional->toOne()')   # Pure, checked by the compiler here
    by_desk = trades.chart('bar', x='desk', y=[('notional', 'sum')], split='region')
    summary = page.sheet('Summary')
    summary.add(by_desk)
    ll.show(page)                       # under the cell in a notebook, else a browser tab; live from then on
    page.read()                         # the page as it is open now, its changes made in DataCube taken in
    page.save()                         # into this user's saved pages, as DataCube's Save keeps it (Open… lists it)
    page.save('q3.page.json')           # DataCube's own page document (Export > Page File writes the same)
    again = ll.Page.load('q3.page.json', frames={'trades': trades_df})

A page IS its document: DataCube's page document (version 4; datacube/src/page-document.ts), the one copy of it. Its
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

import datetime
import json
import math
import numbers
import re
import uuid
import warnings
from collections.abc import Mapping, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

from . import _json
from ._types import is_boolean, is_decimal, is_float, is_integer
from .frames import LIVE, SNAPPED

# DataCube's own vocabularies, as its sources spell them (datacube/src/snapshot.ts, chart-spec.ts): a name outside
# them is refused here, at the line that names it. datacube/test/python-vocabulary.test.ts reads each against
# DataCube's source.
AGGREGATES = frozenset({
    'sum', 'count', 'average', 'min', 'max', 'median', 'stdDevSample', 'stdDevPopulation', 'varianceSample',
    'variancePopulation', 'joinStrings', 'wavg', 'unique',
})
MARKS = frozenset({'bar', 'line', 'area', 'scatter', 'pie', 'heatmap', 'treemap', 'kpi',
                   # batch 3 (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3)
                   'lollipop', 'dumbbell', 'range', 'waterfall', 'pareto', 'funnel', 'rose', 'polar', 'radar',
                   'parallel', 'sankey', 'chord', 'stream', 'calendar', 'gauge', 'sunburst', 'tree', 'matrix'})
# the chart spec's version 2 slots (chart-spec.ts, docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.1): a measure's own mark
# and axis, what it is shown as, a date's grain, a layer's kind and statistic
CHART_VERSION = 2
MEASURE_MARKS = frozenset({'bar', 'line', 'area'})
AXES = frozenset({'left', 'right'})
SHOW_AS = frozenset({'runningTotal', 'percentOfTotal', 'difference', 'percentChange', 'movingAverage', 'rank'})
GRAINS = frozenset({'day', 'week', 'month', 'quarter', 'year'})
LAYERS = frozenset({'line', 'stat', 'band', 'threshold', 'extremes'})
STATS = frozenset({'average', 'min', 'max'})
DIRECTIONS = frozenset({'asc', 'desc'})

PAGE_KIND = 'datacube.page'
# 4: the page's model, where its classes are (DataCube's Create a Class…); a page of Python's has none yet
PAGE_VERSION = 4
_KEEP: Any = object()


def _given_name(session: Any, part: str, name: Any, what: str) -> str | None:
    """A name a page, a tile or a sheet is given, by DataCube's one rule (page-names.ts givenName, asked headless:
    nameGiven, as the screen's renames ask it): its text trimmed, a page's or a tile's blank refused in DataCube's words,
    a sheet's blank none of its own (None). Python only refuses what is not text."""
    if not isinstance(name, str):
        raise ValueError(f'{what} is a name, not {name!r}')
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


def _number(id: str) -> int:
    match = re.search(r'-(\d+)$', id)
    return int(match.group(1)) if match else 0


# -- filters ----------------------------------------------------------------------------------------------------------

def _value(value: Any) -> Any:
    """A filter's value as DataCube keeps one (snapshot.ts, FilterValue): text, a number, a boolean, a date relative to
    when the query runs (``{'relative': 'today' | 'now'}``) or a variant column's JSON (``{'json': text}``). A date and a
    Decimal go as their exact text, which DataCube writes as the column's type writes it (``'2024-01-31'``, ``'12.30'``)."""
    if isinstance(value, (bool, str)):
        return value
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, Decimal) and value.is_finite():
        return str(value)
    if isinstance(value, numbers.Real) and not isinstance(value, Decimal) and math.isfinite(value):
        return float(value)
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, Mapping) and ((set(value) == {'relative'} and value['relative'] in ('today', 'now'))
                                       or (set(value) == {'json'} and isinstance(value['json'], str))):
        return dict(value)
    raise ValueError(f'a filter takes text, a number, a boolean, a date, {{"relative": "today" | "now"}} or '
                     f'{{"json": text}}, not {value!r} (a timestamp as its text: "2024-01-31 10:30:00")')


def _given_filter(node: Any) -> dict[str, Any]:
    """A filter as Python takes one -- a condition ``(column, operator[, operand])``, a group ``('and' | 'or',
    [nodes])`` or ``('not', node)``, a list being all of them -- in the shape of the filter editor's tree, each
    operand's values as DataCube keeps them (``_value``). What an operator takes, and whether a column takes it,
    DataCube's code says (gridFilter)."""
    if isinstance(node, list):
        node = ('and', node)
    if not isinstance(node, tuple) or len(node) < 2:
        raise ValueError(f'a filter is a condition (column, operator, value), a group ("and", [...]) or ("not", ...), '
                         f'not {node!r}')
    head = node[0]
    if head in ('and', 'or') and len(node) == 2 and isinstance(node[1], (list, tuple)):
        return {'kind': head, 'children': [_given_filter(child) for child in node[1]]}
    if head == 'not' and len(node) == 2:
        return {'kind': 'not', 'child': _given_filter(node[1])}
    if len(node) > 3:
        raise ValueError(f'a condition is (column, operator) or (column, operator, value), not {node!r}')
    condition: dict[str, Any] = {'kind': 'condition', 'column': node[0], 'operator': node[1]}
    if len(node) == 3:
        given = node[2]
        if isinstance(given, (set, frozenset)):
            raise ValueError('a filter takes a list of values, in their order (a list or a tuple), not a set')
        condition['operand'] = [_value(v) for v in given] if isinstance(given, (list, tuple)) else _value(given)
    return condition


# -- a sheet's bands, read: DataCube edits them (datacube/src/layout/bands.ts, asked headless: Page._layout) ---------

def _is_leaf(node: dict[str, Any]) -> bool:
    return 'tile' in node or 'stack' in node


def _leaf_tiles(node: dict[str, Any]) -> list[str]:
    return [node['tile']] if 'tile' in node else list(node['stack'])


def _leaves(layout: dict[str, Any]) -> list[dict[str, Any]]:
    """A layout's places in reading order: each a tile or a stack."""
    out: list[dict[str, Any]] = []

    def walk(node: dict[str, Any]) -> None:
        if _is_leaf(node):
            out.append(node)
        else:
            for part in node['parts']:
                walk(part['node'])
    for band in layout['bands']:
        walk(band['node'])
    return out


def _tiles(layout: dict[str, Any]) -> list[str]:
    return [t for leaf in _leaves(layout) for t in _leaf_tiles(leaf)]


# -- the handles ------------------------------------------------------------------------------------------------------

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
        view = self._view
        if title is None:
            if self._kind == 'chart':
                raise ValueError('a chart has a title')
            view.pop('title', None)
        else:
            view['title'] = _given_name(self._page._session, 'tile', title, 'a title')
        self._page._changed()

    @property
    def sheet(self) -> Sheet:
        """The sheet it is on."""
        _ = self._view
        return next(self._page._sheet(s['id']) for s in self._page._doc['sheets'] if self._id in _tiles(s['layout']))

    def move_to(self, sheet: Sheet) -> None:
        """Onto another sheet of its page, in a band of its own at the bottom (its charts stay where they are)."""
        sheet.add(self)


class Grid(Tile):
    """A grid over a frame: its groupings (``group``), pivot (``pivot``), measures, filter, sorts and calculated
    columns, as the grid's panels set them; its configuration (``configure``); its charts (``chart``)."""

    _kind = 'grid'

    @property
    def _cube(self) -> dict[str, Any]:
        return self._page._cube(self._view['cube'])

    @property
    def _query(self) -> dict[str, Any]:
        return self._cube['query']

    @property
    def frame(self) -> str:
        """The name of the frame it is over."""
        return self._cube['source']['name']

    def _columns(self) -> set[str]:
        """Its columns: the frame's, as the compiler types it now, and its calculated ones."""
        return {c.name for c in self._page._session.frames[self.frame].columns} | \
            {d['name'] for d in self._query.get('derived', [])}

    def _check(self, names: Sequence[str], what: str) -> list[str]:
        columns = self._columns()
        unknown = [n for n in names if n not in columns]
        if unknown:
            raise ValueError(f'{what} names {", ".join(map(repr, unknown))}, not a column of {self.frame!r} '
                             f'({", ".join(sorted(columns))})')
        return list(names)

    def _set(self, key: str, value: Any) -> Grid:
        self._query[key] = value
        self._page._changed()
        return self

    # -- headless: DataCube's own code, in Python (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §4) ------------

    def _headless(self) -> tuple[Any, Any]:
        """The session's headless engine, got first (the session's lock, taken and let go), and the frames: a caller then
        holds the frames still (the frames' lock) -- the order a frame's registering takes the two, so neither waits on
        the other."""
        session = self._page._session
        return session.headless(), session.frames

    def _spec(self, frames: Any) -> dict[str, Any]:
        """This grid's cube and its frame's model, as the bundle's grid functions take them (the frames held still)."""
        return _cube_spec(self._cube, frames)

    def view(self) -> GridView:
        """What the grid shows, as DataCube computes it: its columns and rows, their groups and totals, its pivot, and
        what its row cap cut -- by DataCube's own code, run headless in V8 (the `datacube` extra), as the grid on screen
        runs it: its configuration folded into its query, its columns laid out as the screen lays them out."""
        import json

        from ._headless import cells
        engine, frames = self._headless()
        with frames.serving():
            # DataCube's cells are exact as it gives them (an Integer or a Float a number, a Decimal its text): read as
            # JSON reads them, an integer past 2^53 and a non-finite float from their tags
            return GridView(cells(json.loads(engine.call('gridView', self._spec(frames)))))

    @property
    def relation(self) -> str:
        """Its rows at their finest level, as Pure: filtered and extended by its calculated columns, grouped by its
        innermost group when it is grouped (a flat grid's rows as they are) -- the relation DataCube's Create a Class
        makes a class of."""
        engine, frames = self._headless()
        with frames.serving():
            return engine.call('gridRelation', self._spec(frames))

    def to_pandas(self) -> Any:
        """Every row of ``relation``, as a pandas DataFrame: DataCube says the query, Python plans it over its frame's
        model and its own database answers it, typed by the plan (``Table.execute``), the rows coming to Python from
        there (never through DataCube's engine)."""
        from .frames import to_pandas
        engine, frames = self._headless()
        with frames.serving():
            return to_pandas(frames[self.frame].execute(engine.call('gridRelation', self._spec(frames))))

    def _class(self, name: str | None = None, columns: Mapping[str, Any] | None = None,
               documentation: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
        """Its rows as a class's, as the window asks for them: DataCube's own request (headless ``gridClass``: its rows
        at their finest level, its table, its first rows as the window writes them into the project -- planned and run
        on Python's database, as every headless grid's are -- and the names offered), with the frames held still.
        Answers the writer's class action and what DataCube said; a grid whose rows cannot be a class refused in
        DataCube's words (``ValueError``)."""
        engine, frames = self._headless()
        with frames.serving():
            asked = _json.loads(engine.call('gridClass', self._spec(frames), {'source': self.frame, 'page': self._page.name}))
        if 'refused' in asked:
            raise ValueError(f'{self.frame!r} cannot be a class: {asked["refused"]}')
        cls = {'query': asked['query'], 'source': asked['source'], 'name': name or asked['names']['className'],
               **({'documentation': documentation} if documentation else {}),
               **({'columns': _column_choices(columns)} if columns else {}),
               'where': {'from': asked['table']['database'], 'as': 'Python',
                         'samples': {asked['table']['table']: asked['sample']}}}
        return cls, asked

    def export(self, kind: str = 'csv', to: str | Path | None = None) -> str | bytes | Path:
        """The grid as its Export menu exports it, by DataCube's own code (the screen's core, run headless): what it
        shows, with its formats, appearance and heatmaps -- ``'csv'`` (raw values), ``'excel'`` (a workbook, the tree
        as outlines), ``'html'``, ``'text'`` (formatted, to paste) or ``'pdf'``. Answers the file's content (text, or
        bytes for a workbook or a PDF), or, given ``to``, writes it there and answers the path."""
        import base64
        if kind not in ('csv', 'excel', 'html', 'text', 'pdf'):
            raise ValueError(f"a grid exports as 'csv', 'excel', 'html', 'text' or 'pdf', not {kind!r}")
        engine, frames = self._headless()
        with frames.serving():
            said = _json.loads(engine.call('gridExport', self._spec(frames), kind))
        content: str | bytes = said['content'] if said['text'] else base64.b64decode(said['content'])
        if to is None:
            return content
        path = Path(to)
        # the bytes as the export wrote them: CSV's own CRLF, no newline translated on any system
        path.write_bytes(content.encode('utf-8') if isinstance(content, str) else content)
        return path

    def create_class(self, name: str | None = None, *, package: str | None = None,
                     columns: Mapping[str, Any] | None = None, documentation: str | None = None,
                     project: str | None = None, sdlc: Any = None, writer: Any = None) -> str:
        """Its rows at their finest level (``relation``) made a class of the page's model, as DataCube's Create a Class…
        makes it (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B4): with everything it needs to run (its relation function,
        mapping, the frames' store, connection and runtime -- the place ``Python`` -- and its first rows as sample
        data), into the page's project in this user's model home, made with the page's first class. ``name``: the
        class's (suggested from the frame's name, as the window suggests it); ``package``: the page's (suggested from
        the page's name before its first class). ``columns``: by column, ``'property'`` (the default), ``'key'`` (kept
        for an association to join on, not shown) or ``'omit'``, or ``{'as', 'name', 'documentation'}``. ``project``:
        the name of the project made with the page's first class (the page's name unless given, as the window offers
        it). ``sdlc`` and ``writer``: another SDLC or writer than this user's (legend_lite.model). Answers the class's
        path."""
        from . import model as models
        cls, asked = self._class(name, columns, documentation)
        made = self._page._into(package or asked['names']['package'], sdlc, writer,
                                lambda s, w, at, made: models.create_class(s, w, *at, cls, made=made), project)
        path = next((c['class'] for c in made.answer.get('classes', [])), None)
        self._page._keep(self._view['cube'], asked['table']['table'], path)
        return path or cls['name']

    def group(self, *rows: str) -> Grid:
        """Grouped by ``rows``, outermost first (the Rows zone); none, not grouped."""
        return self._set('rows', self._check(rows, 'group'))

    def pivot(self, *columns: str) -> Grid:
        """Pivoted on ``columns`` (the Columns zone); none, not pivoted."""
        return self._set('pivotOn', self._check(columns, 'pivot'))

    def measure(self, column: str, fn: str = 'sum', name: str | None = None, *, weight: str | None = None) -> Grid:
        """A measure: ``column`` aggregated by ``fn`` (one of AGGREGATES; ``wavg`` takes a ``weight`` column), named
        ``name`` (the column's own unless given: two measures of one column need names of their own)."""
        if fn not in AGGREGATES:
            raise ValueError(f'{fn!r} is not one of DataCube\'s aggregates: {", ".join(sorted(AGGREGATES))}')
        # a count counts rows: its column is not read (snapshot.ts, Measure)
        if fn != 'count':
            self._check([column], 'a measure')
        if (fn == 'wavg') != (weight is not None):
            raise ValueError('a weighted average (wavg) takes a weight column, and only it does')
        if name is not None and (not isinstance(name, str) or not name.strip()):
            # DataCube offers no name entry for a measure (a person never types one): only the argument's own check
            raise ValueError(f"a measure's name is a name, not {name!r}")
        named = name or column
        measures = self._query.get('measures', [])
        if any(m['name'] == named for m in measures):
            raise ValueError(f'the grid has a measure named {named!r} already: give this one a name of its own (name=...)')
        measure = {'name': named, 'column': column, 'fn': fn,
                   **({'weight': self._check([weight], 'a weight')[0]} if weight is not None else {})}
        return self._set('measures', [*measures, measure])

    def filter(self, condition: Any) -> Grid:
        """Its filter: a condition ``(column, operator[, value])``, a list of them (all of them), a group
        ``('and' | 'or', [...])`` or ``('not', ...)`` -- the filter editor's own conditions, set as it sets them
        (DataCube's code, headless: gridFilter): each operator's value as it takes one (none, one, a list, another
        column), one a column of its type is offered, the grid with it compiled, in the editor's normal form (a group of
        one is that one, an empty one none: ``[]`` takes the filter off, as ``None`` does); ``None`` for none."""
        if condition is not None:
            engine, frames = self._headless()
            with frames.serving():
                said = json.loads(engine.call('gridFilter', self._spec(frames), _json.dumps(_given_filter(condition))))
            if 'problems' in said:
                raise ValueError(f'{"; ".join(said["problems"])} -- in Python a condition is (column, operator) or '
                                 '(column, operator, value), its value a list where the operator takes several')
            if said['filter'] is not None:
                return self._set('filter', said['filter'])
        self._query.pop('filter', None)
        self._page._changed()
        return self

    def sort(self, column: str, direction: str = 'asc') -> Grid:
        """A sort, after those it has: ``column`` ``'asc'`` or ``'desc'``."""
        if direction not in DIRECTIONS:
            raise ValueError(f"a sort's direction is 'asc' or 'desc', not {direction!r}")
        sort = {'column': self._check([column], 'a sort')[0], 'direction': direction}
        return self._set('sorts', [*self._query.get('sorts', []), sort])

    def calculate(self, name: str, expression: str, *, kind: str | None = None) -> Grid:
        """A calculated column computed per row, in Pure over the row ``$x`` (``'$x.qty * 2'``; a column that can be
        empty, as a dataframe's can, ``->toOne()`` first for arithmetic), added as the grid's own column editor adds one
        (DataCube's code, headless: gridCalculate): its name as DataCube takes one, ``kind`` ``'measure'`` or
        ``'dimension'`` as declared (not given, a measure, as the editor starts a new column -- summed when grouped if it
        is a number, its unique value if not), and the grid with it compiled as DataCube would run it -- reading any of
        the grid's columns before it, a window column's too.
        A mistake fails at this line, in DataCube's and the compiler's words."""
        if not isinstance(name, str) or not isinstance(expression, str):
            raise ValueError(f'a calculated column is a name and an expression, not {name!r} and {expression!r}')
        engine, frames = self._headless()
        with frames.serving():
            said = json.loads(engine.call('gridCalculate', self._spec(frames), name, f'x|{expression}', kind))
        if 'problems' in said:
            raise ValueError('; '.join(said['problems']))
        return self._set('derived', [*self._query.get('derived', []), _plain(_json.loads(said['derived']))])

    def configure(self, settings: Mapping[str, Any]) -> Grid:
        """Any of the grid's configuration (its Properties), in its saved document's own keys (``reportTitle``,
        ``showRootAggregation``, ``columns``: each column's formats and colours, ...)."""
        self._cube.setdefault('configuration', {}).update(_copied(settings))
        self._page._changed()
        return self

    def chart(self, mark: str, *, x: Any = _KEEP, y: Sequence[Any] | None = None,
              split: Any = _KEEP, title: str | None = None, frozen: bool | None = None,
              options: Mapping[str, Any] | None = None, grain: str | None = None,
              bins: Mapping[str, float] | None = None, tooltip: Sequence[Any] | None = None,
              layers: Sequence[Mapping[str, Any]] | None = None, label: Any = None, size: Any = None,
              facet: Mapping[str, str] | None = None) -> Chart:
        """A chart of this grid, beside it: ``mark`` (one of MARKS), its category ``x``, what it plots ``y`` --
        ``[(column, aggregate), ...]``, the measure DataCube's own new chart would plot unless given -- and a ``split``
        (one series per value), each DataCube's own new chart's unless given (None: none); ``frozen`` keeps its own
        grouping rather than following the grid's (and stays when the grid is removed, as in DataCube) -- unless told, a
        chart told its ``x``, ``y`` or ``split`` keeps them, as a hand on them in DataCube's options freezes the chart,
        and one told none of them follows the grid (told them and ``frozen=False``, one the grid would replace is
        refused); ``options``: the chart's Options, in its spec's own keys (one left out is its default).

        The spec's version 2 slots, each in its own keys: a measure of ``y`` may be a dict -- ``column``, ``fn`` (an
        aggregate, or ``'value'``: a group-level column of the grid, plotted as it is), its own ``mark`` (MEASURE_MARKS),
        ``axis`` (AXES) and ``showAs`` (``{'kind': one of SHOW_AS}``); ``grain`` takes a date across to its period
        (GRAINS); ``bins`` a number across to its bins (``{'width': w}`` or ``{'count': n}``); ``label`` a measure
        written on each mark instead of its own value; ``tooltip`` further measures a hover lists; ``layers`` reference
        lines, bands, thresholds and pinned extremes (each a dict, its ``kind`` one of LAYERS); ``size`` a scatter's
        third column, each point sized by it (a bubble); ``facet`` small multiples of bars, lines or areas, a chart per
        value of ``{'column': ...}`` and/or ``{'row': ...}``.

        DataCube makes the chart -- its own new chart, changed as its options form changes one (a date across runs in time
        order) -- and judges it as it will draw it, by its own rules run in V8 (the datacube extra): what it would refuse
        to draw is refused here, in its words, and nothing is kept."""
        given = {**_given(mark, x, y, split, frozen), **({'options': _carried(options)} if options is not None else {}),
                 **_slots(grain, bins, tooltip, layers, label, size, facet)}
        # made as DataCube makes a new chart (what is not given is its own default), and judged as it will draw it: what
        # DataCube would refuse to draw is refused here, in its words, before the chart is kept
        spec = _made(self._page, self._view['cube'], given)
        named = None if title is None else _given_name(self._page._session, 'tile', title, 'a title')
        id = self._page._next('chart')
        self._page._doc['views'].append({'id': id, 'kind': 'chart', 'cube': self._view['cube'],
                                         'title': named or f'Chart {_number(id)}', 'spec': spec})
        self._page._place(id, self.sheet, near=self._id)
        return self._page._tile(id, Chart)

    @property
    def charts(self) -> list[Chart]:
        """The charts of this grid, on any sheet."""
        cube = self._view['cube']
        return [c for c in self._page.charts if c._view['cube'] == cube]

    def update(self, frame: Any, mode: str | None = None) -> None:
        """Another frame for this grid (Live or Snapped as before, unless ``mode`` says): a page showing it queries it
        again, and -- its columns changed -- opens again over them."""
        _ = self._view
        self._page._update(self.frame, frame, mode)

    def __repr__(self) -> str:
        return f'<Grid {self._id}>'


def _given(mark: str, x: Any, y: Sequence[Any] | None, split: Any, frozen: bool | None) -> dict[str, Any]:
    """What a chart was told, in its spec's own keys (chart-spec.ts, version 2): DataCube makes the chart of it and judges
    it (_made), so nothing here is a rule of the chart's -- only Python's arguments turned into the spec's keys (an x or
    a split told None is null: none; frozen told False is null: the chart taken live)."""
    return {'version': CHART_VERSION, 'mark': mark, **({'x': x} if x is not _KEEP else {}),
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
    session = page._session
    engine, frames = session.headless(), session.frames
    with frames.serving():
        made = _json.loads(engine.call('gridChart', _cube_spec(page._cube(cube), frames), _json.dumps(given),
                                       None if before is None else _json.dumps(before)))
    if made['problems']:
        raise ValueError(' '.join(made['problems']))
    return made['spec']


def _cube_spec(cube: Mapping[str, Any], frames: Any) -> dict[str, Any]:
    """A cube document and its frame's model, as the bundle's grid functions take them (the frames held still)."""
    table = frames[cube['source']['name']]
    return {'cube': _json.dumps(cube), 'model': table.model, 'runtime': table.runtime, 'source': table.accessor}


def _slots(grain: str | None, bins: Mapping[str, float] | None, tooltip: Sequence[Any] | None,
           layers: Sequence[Mapping[str, Any]] | None, label: Any = None, size: Any = None,
           facet: Mapping[str, str] | None = None) -> dict[str, Any]:
    """The spec's version 2 slots a chart was given, in the spec's own keys and as given: DataCube's reader and rules
    judge each (_made), in their words."""
    given = {'size': None if size is None else _measure(size), 'facet': facet, 'grain': grain, 'bins': bins,
             'label': None if label is None else _measure(label),
             'tooltip': None if tooltip is None else _measures(tooltip), 'layers': layers}
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
        # text (DataCube's exact decimals) a Decimal, a Float a float even when JSON wrote it as a whole number (10.0 is
        # written 10)
        def typed(v: Any, type: str) -> Any:
            if is_decimal(type) and isinstance(v, str):
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


class Chart(Tile):
    """A chart of a grid: its mark, what it plots, its options; following its grid unless frozen."""

    _kind = 'chart'

    @property
    def grid(self) -> Grid | None:
        """Its grid; None when the grid was removed and the chart, frozen, stayed (DataCube's detached chart)."""
        cube = self._view['cube']
        return next((g for g in self._page.grids if g._view['cube'] == cube), None)

    def plot(self, mark: str | None = None, *, x: Any = _KEEP, y: Any = _KEEP, split: Any = _KEEP,
             frozen: bool | None = None) -> Chart:
        """What it plots changed, as DataCube's options form changes it (chart-spec.ts editChart): its ``mark``, ``x``
        (None: none), ``y``, ``split`` (None: none) or ``frozen`` (False: following its grid); what is not given stays, a
        new column across takes its own order and a grain or bins go with the old one, and a live chart told its ``x``,
        ``y`` or ``split`` freezes unless told ``frozen``. DataCube makes the edit and judges it (_made): a detached chart
        against its cube, which stays."""
        view = self._view
        given = {**({'mark': mark} if mark is not None else {}), **({'x': x} if x is not _KEEP else {}),
                 **({'y': _measures(y)} if y is not _KEEP else {}),
                 **({'split': split} if split is not _KEEP else {}),
                 **({'frozen': True if frozen else None} if frozen is not None else {})}
        view['spec'] = _made(self._page, view['cube'], given, view['spec'])
        self._page._changed()
        return self

    def options(self, settings: Mapping[str, Any]) -> Chart:
        """The chart's Options (``orientation``, ``stack``, ``sort``, ``limit``, ``labels``, ``legend``), in its spec's own
        keys; one left out is the chart's default. DataCube makes the edit and judges it (_made)."""
        view = self._view
        view['spec'] = _made(self._page, view['cube'], {'options': _carried(settings)}, view['spec'])
        self._page._changed()
        return self

    def __repr__(self) -> str:
        return f'<Chart {self._id}>'


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
        sheet = self._doc
        own = None if name is None else _given_name(self._page._session, 'sheet', name, "a sheet's name")
        if own is None:
            sheet.pop('name', None)
        else:
            sheet['name'] = own
        self._page._changed()

    @property
    def tiles(self) -> list[Tile]:
        """Its tiles in reading order, a stack's in its tabs' order."""
        return [self._page._tile_of(t) for t in _tiles(self._doc['layout'])]

    def add(self, *tiles: Tile) -> Sheet:
        """Puts ``tiles`` on this sheet, each in a band of its own at the bottom (from the sheet it was on)."""
        _ = self._doc
        for tile in tiles:
            if tile._page is not self._page:
                raise ValueError(f'{tile!r} is another page\'s')
            _ = tile._view
        with self._page._batch():
            for tile in tiles:
                self._page._unplace(tile._id)
                self._page._place(tile._id, self)
        return self

    def layout(self, *bands: Sequence[Tile]) -> Sheet:
        """Its places laid out in bands, top to bottom, each band's places side by side and alike, as the layout picker
        lays them out: ``sheet.layout([grid, chart], [other])``. Every tile on the sheet is in one band; a stack is named
        by any of its tiles. (``layout_from`` takes any layout DataCube draws: splits, sizes, heights.)"""
        leaves = {t: leaf for leaf in _leaves(self._doc['layout']) for t in _leaf_tiles(leaf)}
        chosen: list[list[dict[str, Any]]] = []
        used: set[str] = set()
        for band in bands:
            if isinstance(band, Tile) or not band:
                raise ValueError('a band is a list of tiles, one at least: sheet.layout([grid, chart], [other])')
            row = []
            for tile in band:
                if isinstance(tile, Tile):
                    _ = tile._view
                leaf = leaves.get(tile._id) if isinstance(tile, Tile) else None
                if leaf is None:
                    raise ValueError(f'{tile!r} is not on the sheet {self._id}')
                if used & set(_leaf_tiles(leaf)):
                    raise ValueError(f'{tile!r} (or a tile stacked with it) is in the layout twice')
                used.update(_leaf_tiles(leaf))
                row.append(leaf)
            chosen.append(row)
        if used != set(leaves):
            raise ValueError(f'every tile on the sheet is in the layout: {", ".join(sorted(set(leaves) - used))} not')
        if not chosen:
            return self  # a sheet with no tiles, laid out in no bands: as it is (no shape names no bands)
        # a band of each list's places side by side: DataCube's rows preset, the bands' counts and the places in order
        return self._arranged('rows:' + '-'.join(str(len(row)) for row in chosen),
                              [tile._id for band in bands for tile in band])

    def arrange(self, shape: str) -> Sheet:
        """Its places arranged as one of DataCube's layout picker's shapes, in reading order (a stack is one place):
        ``'side-by-side'`` (one band), ``'stacked'`` (a band each), ``'rows:2-1'`` (bands of 2 then 1),
        ``'columns:1-2'``, ``'focus-left'`` (the first half the page, the rest beside it) and the picker's others, as
        DataCube names them."""
        if not isinstance(shape, str):
            raise ValueError(f"a shape is the layout picker's name ('side-by-side', 'rows:2-1', ...), not {shape!r}")
        return self._arranged(shape, None)

    def _arranged(self, preset: str, order: list[str] | None) -> Sheet:
        """Its places arranged as DataCube arranges them (bands.ts arrange, asked headless), in ``order`` (their tiles;
        None: reading order)."""
        said = json.loads(self._page._session.headless().call(
            'layoutArrange', _json.dumps(self._doc['layout']), preset, None if order is None else _json.dumps(order)))
        if 'problems' in said:
            raise ValueError(f'{"; ".join(said["problems"])} (a shape as the layout picker names it, or '
                             f'sheet.layout(...) band by band)')
        self._doc['layout'] = said['layout']
        self._page._changed()
        return self

    def layout_from(self, layout: Mapping[str, Any]) -> Sheet:
        """Its layout in the page document's own words -- ``{"kind": "bands", "fit": ..., "bands": [{"height": ...,
        "node": ...}]}``, each node a ``{"tile": id}``, a ``{"stack": [ids]}`` or a ``{"split": "row" | "column",
        "parts": [{"node": ..., "size": share}]}`` -- as ``page.to_dict()`` shows a sheet's. Every tile on the sheet is in
        it once, and only they: the page with it is read as DataCube reads one, and refused in its words."""
        _ = self._doc  # a sheet no longer on its page refused, as every sheet call refuses one
        page = self._page
        doc = _copied(page._doc)
        next(s for s in doc['sheets'] if s['id'] == self._id)['layout'] = _copied(layout)
        page._doc = _read_page(doc, page._session)
        page._changed()
        return self

    def __repr__(self) -> str:
        return f'<Sheet {self._id}>'


# -- the page ---------------------------------------------------------------------------------------------------------

class Page:
    """A DataCube page: its grids over frames (``grid``), their charts, its sheets (``sheet``), stacks (``stack``) and
    layouts -- shown with ``ll.show(page)``, live from then on; saved as DataCube's page document (``save``,
    ``to_json``, ``to_dict``) and loaded again (``load``); read back as it is open (``read``)."""

    def __init__(self, name: str = 'Untitled page') -> None:
        from .datacube import _current
        if name is None:
            raise ValueError('a page has a name')
        self._session = _current()
        name = _given_name(self._session, 'page', name, "a page's name")
        # the page: its one copy
        self._doc: dict[str, Any] = {'kind': PAGE_KIND, 'version': PAGE_VERSION, 'name': name, 'cubes': [], 'views': [],
                                     'sheets': [{'id': 'sheet-1', 'layout': {'kind': 'bands', 'fit': True, 'bands': []}}]}
        # each handle given, by its kind and id: the same object each time it is asked for
        self._handles: dict[tuple[type[Any], str], Any] = {}
        # every id the page has held: none is given out again, so no handle meets a new tile under its old one's id
        self._ids: set[str] = {'sheet-1'}
        # where it is shown (datacube.show_page): told each time it changes
        self._shown: list[Any] = []
        self._quiet = 0
        # the saved page it was last saved as (``save()``): its id, or None
        self._saved: str | None = None

    # -- the page's model (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B4): its classes, into a project ----------------------

    @property
    def model(self) -> dict[str, Any] | None:
        """Where the page's classes are, as the page document keeps it (DataCube's ``PageModelRef``): its project,
        workspace and package, and each grid's table and class by its cube; None before its first class."""
        ref = self._doc.get('model')
        return _copied(ref) if ref else None

    def _names(self) -> dict[str, Any]:
        """The page's model as DataCube's windows name it before its first class (headless ``pageModelNames``): its
        package, its data space, its project to make and DataCube's workspace in it."""
        return _json.loads(self._session.headless().call('pageModelNames', self.name))

    def _into(self, package: str, sdlc: Any, writer: Any, call: Any, project: str | None = None) -> Any:
        """A model action into the page's project (this user's model home, unless ``sdlc`` and ``writer`` say otherwise):
        the page's own, or one made as DataCube makes a page's (``pageModelNames``: in its group, named after the page,
        or ``project``), the page's from the moment it is made."""
        from . import model as models
        ref = self._doc.get('model')
        if ref:
            at = (ref['project'], ref['workspace'], ref['package'])
        else:
            names = self._names()
            made_project = names['project']
            at = (models.NewProject(project or made_project['name'], made_project['group'], made_project['description']),
                  names['workspace'], package)

        def made(project_id: str) -> None:
            self._doc['model'] = {'project': project_id, 'workspace': at[1], 'package': at[2], 'tables': {}}
            self._changed()
        return call(sdlc if sdlc is not None else models.local_sdlc(),
                    writer if writer is not None else models.local_writer(), at, made)

    def _keep(self, cube: str, table: str, cls: str | None) -> None:
        """The page's model now keeps ``table`` and ``cls`` for the grid ``cube`` (page-model.ts ``keepTable``,
        ``keepClass``)."""
        ref = self._doc['model']
        ref.setdefault('tables', {})[cube] = table
        if cls is not None:
            ref.setdefault('classes', {})[cube] = cls
        self._changed()

    def create_association(self, a: Any, b: Any, *, name: str | None = None, sdlc: Any = None,
                           writer: Any = None) -> str:
        """An association between two of the page's classes, as DataCube's Create an Association… makes it: each end
        a class and what it is joined on -- ``(class, on)``, or ``{'class', 'on', 'name', 'multiplicity', 'unique'}``
        with the name the other class reaches it by and how many of it one of the other has -- preset, as the window
        presets it, from whether its key is unique over its class's rows when its class was made from one of this page's
        grids. Answers the association's path."""
        from . import model as models
        ref = self._doc.get('model')
        if not ref:
            raise ValueError("an association relates two of the page's classes: make them first (grid.create_class, or "
                             'page.create_data_space for the whole page)')
        ends = [_end(a), _end(b)]

        def made_with_facts(s: Any, w: Any, at: Any, m: Any) -> Any:
            self._facts(s, w, at, ends)
            return models.create_association(s, w, *at, {**({'name': name} if name else {}), 'ends': ends}, made=m)
        made = self._into(ref['package'], sdlc, writer, made_with_facts)
        return next((x['association'] for x in made.answer.get('associations', [])), name or 'the association')

    def _facts(self, sdlc: Any, writer: Any, at: Any, ends: list[dict[str, Any]]) -> None:
        """Each end that says neither how many nor whether its key is unique given the fact, as Create an Association…
        presets it (model-actions.ts ``unique``): the class's key read through its relation function over the project
        (the writer's ``joinable`` names the function and the column), DataCube's query for a value held twice
        (headless ``classDuplicate``) planned over the project's model through its place's runtime and run on Python's
        database -- no row, and the key is unique. An end whose facts cannot be read (the project's tables not Python's)
        is left for the writer to refuse in words, as the window leaves it for the person."""
        from . import model as models
        open_ends = [e for e in ends if 'unique' not in e and 'multiplicity' not in e]
        if not open_ends:
            return
        text, _ = models.project_state(sdlc, at[0], at[1])
        joinable = {j['class']: j for j in writer.create_association(text, at[2], None).get('joinable', [])}
        frames, headless = self._session.frames, self._session.headless()
        for end in open_ends:
            j = joinable.get(end['class'], {})
            column = next((k.get('column') for k in j.get('keys', []) if k['name'] == end['on']), None)
            if j.get('function') is None or column is None:
                continue
            query, runtime = headless.call('classDuplicate', j['function'], column), f'{at[2]}::runtime::Python'
            # planned as the page's own grids are, by Python's own compiler over the model it holds, and run there
            with frames.serving():
                held = frames.model_for(text)
                if held is None:
                    continue
                end['unique'] = frames.run(held.plan(_json.loads(query), runtime)).num_rows == 0

    def create_data_space(self, name: str | None = None, *, title: str | None = None, description: str | None = None,
                          associations: Sequence[Any] = (), project: str | None = None, sdlc: Any = None,
                          writer: Any = None) -> str:
        """The whole page in one go, as DataCube's Create a Data Space… makes it: each grid not yet a class made one
        (named as the window suggests; a grid whose rows cannot be one left out, as the window leaves it), the
        associations given (as ``create_association`` takes them: pairs of ends, or ``{'name', 'ends'}``) made, and a
        data space over the page's classes -- one revision, the project made with it when the page has none (named
        ``project``, else after the page). ``name``: the data space's (suggested from the page's name). Answers the data
        space's path."""
        from . import model as models
        ref = self._doc.get('model')
        names = self._names()
        taken = (ref or {}).get('classes', {})
        classes, kept = [], []
        for grid in self.grids:
            cube = grid._view['cube']
            if cube in taken:
                continue
            try:
                cls, asked = grid._class()
            except ValueError:
                continue
            classes.append(cls)
            kept.append((cube, asked['table']['table'], cls['name']))
        package = ref['package'] if ref else names['package']
        data_space = {'name': name or names['dataSpace'], **({'title': title} if title else {}),
                      **({'description': description} if description else {})}
        joined = [x if isinstance(x, Mapping) else {'ends': [_end(x[0]), _end(x[1])]} for x in associations]
        made = self._into(package, sdlc, writer, lambda s, w, at, m: models.create_data_space(
            s, w, *at, data_space, classes, [{**j, 'ends': [_end(e) for e in j['ends']]} for j in joined], made=m),
            project)
        made_classes = [c['class'] for c in made.answer.get('classes', [])]
        for cube, table, class_name in kept:
            path = next((c for c in made_classes if c.rsplit('::', 1)[-1] == class_name), None)
            self._keep(cube, table, path)
        return made.answer.get('dataSpace') or data_space['name']

    @property
    def name(self) -> str:
        return self._doc['name']

    @name.setter
    def name(self, name: str) -> None:
        if name is None:
            raise ValueError('a page has a name')
        name = _given_name(self._session, 'page', name, "a page's name")
        self._doc['name'] = name
        for cube in self._doc['cubes']:
            cube['cube']['name'] = name
        self._changed()

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

    def grid(self, frame: Any, *, name: str | None = None, rows: Sequence[str] = (), columns: Sequence[str] = (),
             measures: Mapping[str, str] | Sequence[tuple[str, str]] | None = None, filter: Any = None,
             sort: Sequence[tuple[str, str]] = (), title: str | None = None, sheet: Sheet | None = None,
             mode: str = LIVE) -> Grid:
        """A grid over ``frame`` (a pandas or polars DataFrame, an Arrow table, or a function returning one), served
        under ``name`` (``frame``, ``frame_2``, ... unless given), in a band of its own at the bottom of ``sheet`` (the
        first unless given). A name served already is this grid's when it serves this same frame, the same way (two
        grids over one frame), and refused when it serves another: ``grid.update(frame)`` replaces a frame, every grid
        over it showing the new one. ``rows``, ``columns``, ``measures`` (``{column: aggregate}`` or ``[(column,
        aggregate)]``), ``filter`` and ``sort``: as ``group``, ``pivot``, ``measure``, ``filter`` and ``sort`` say -- all
        checked before the grid is on the page, so a mistake leaves the page and the frames as they were. ``mode``:
        Live (each query reads the frame as it is then) or ``'snapped'``."""
        if mode not in (LIVE, SNAPPED):
            raise ValueError(f'mode is {LIVE!r} or {SNAPPED!r}, not {mode!r}')
        if sheet is not None and sheet._page is not self:
            raise ValueError(f'{sheet!r} is another page\'s')
        target = self.sheets[0] if sheet is None else sheet
        _ = target._doc
        if title is not None:
            title = _given_name(self._session, 'tile', title, 'a title')
        id = self._next('grid')
        session = self._session
        if name is not None and name in session.frames:
            table = session.frames[name]
            if table.given is not frame or table.mode != mode:
                serves = 'another frame' if table.given is not frame else f'this frame {table.mode}'
                raise ValueError(f'{name!r} serves {serves} already: grid.update(frame) replaces a frame, every grid over '
                                 f'it showing the new one; or give this grid a name of its own')
            served, new = table.name, False
        else:
            served, new = session.register(frame, name, mode), True
        try:
            # its cube as DataCube makes a grid's over a frame: capped at the row limit when it is not the page's only grid
            cube, _ = self._frame_cube(served, len(self._doc['cubes']) + 1)
            self._doc['cubes'].append({'id': id, 'cube': cube})
            self._doc['views'].append({'id': id, 'kind': 'grid', 'cube': id, **({'title': title} if title else {})})
            grid = self._tile(id, Grid)
            # set before it is placed, nothing told meanwhile
            with self._batch(tell=False):
                grid.group(*rows)
                grid.pivot(*columns)
                for column, fn in (measures.items() if isinstance(measures, Mapping) else measures or ()):
                    grid.measure(column, fn)
                if filter is not None:
                    grid.filter(filter)
                for column, direction in sort:
                    grid.sort(column, direction)
        except BaseException:
            # the grid taken off again, and a frame served for it served no more
            self._doc['cubes'] = [c for c in self._doc['cubes'] if c['id'] != id]
            self._doc['views'] = [v for v in self._doc['views'] if v['id'] != id]
            self._forget({id})
            if new:
                session.unregister(served)
            raise
        self._place(id, target)
        return grid

    def sheet(self, name: str | None = None) -> Sheet:
        """A new sheet after the last (``name``: its own; None, named after its first grid)."""
        if name is not None:
            name = _given_name(self._session, 'sheet', name, "a sheet's name")
        id = self._next('sheet')
        self._doc['sheets'].append({'id': id, **({'name': name} if name else {}),
                                    'layout': {'kind': 'bands', 'fit': True, 'bands': []}})
        self._ids.add(id)
        self._changed()
        return self._sheet(id)

    def stack(self, *tiles: Tile) -> None:
        """``tiles`` in one place, as tabs, one shown at a time, where the first one is: all on one sheet. Stacked as
        DataCube stacks a tile dropped on another's middle (bands.ts stackTiles): the first one's place keeps its own
        tabs first, the others follow in the order given, and a tile stacked already brings its stack's others after
        them."""
        strays = [t for t in tiles if not isinstance(t, Tile)]
        if strays:
            raise ValueError(f'a stack is of tiles (grids and charts), not {", ".join(repr(t) for t in strays)}')
        if len({t._id for t in tiles}) < 2:
            raise ValueError('a stack is two tiles or more')
        if any(t._page is not self for t in tiles):
            raise ValueError('the tiles of a stack are of this page')
        sheet = tiles[0].sheet
        if any(t.sheet is not sheet for t in tiles):
            raise ValueError('the tiles of a stack are on one sheet (put them there first: sheet.add(tile))')
        sheet._doc['layout'] = self._layout('layoutStack', sheet._doc['layout'], _json.dumps([t._id for t in tiles]))
        self._changed()

    def remove(self, tile: Tile) -> None:
        """A tile off the page, its neighbours closing over its place: a grid with its charts, but a frozen one, which
        stays without it (as DataCube keeps one); a chart."""
        if tile._page is not self:
            raise ValueError(f'{tile!r} is another page\'s')
        _ = tile._view
        gone = {tile._id} | ({c._id for c in tile.charts if not c._view['spec'].get('frozen')} if isinstance(tile, Grid) else set())
        views = [v for v in self._doc['views'] if v['id'] not in gone]
        # a cube stays while a view shows it (a frozen chart's, its grid gone)
        cubes = [c for c in self._doc['cubes'] if any(v['cube'] == c['id'] for v in views)]
        if not cubes and self._shown:
            raise ValueError('a page that is shown has a grid at least (DataCube shows no empty page): add another '
                             'first, or close it')
        # its model's tables and classes for the cubes it still holds, as DataCube writes a page (its keptFor, headless):
        # a grid removed takes its table's name and its class with it, or DataCube would never open the page again --
        # asked before anything changes, so a refusal leaves the page as it was
        model = None if 'model' not in self._doc else _json.loads(self._session.headless().call(
            'pageModelKept', _json.dumps(self._doc['model']), _json.dumps([c['id'] for c in cubes])))
        self._doc['views'], self._doc['cubes'] = views, cubes
        if model is not None:
            self._doc['model'] = model
        for id in gone:
            self._unplace(id)
        self._forget(gone)
        self._changed()

    # -- the document --------------------------------------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """The page as DataCube's page document (version 4), a copy: what is done to it is not done to the page. A
        calculated column's numbers are exact (``Decimal``), as the compiler writes them: ``to_json`` and ``save`` write
        them so. It is the page's document as Python holds it; ``to_json`` and ``save`` write it as DataCube writes it
        (its reader's, headless: a chart's options filled, a key DataCube does not know left out)."""
        return _copied(self._doc)

    def to_json(self, indent: int | None = 2) -> str:
        """The page document as JSON text, its numbers exact, as DataCube writes it (what ``save`` writes): refused in
        DataCube's words when DataCube would not open it (a page with no grid)."""
        return _json.dumps(self._written(), indent=indent)

    def save(self, path: str | Path | None = None, *, name: str | None = None) -> str | None:
        """Saved. With no ``path``, as DataCube's Save keeps a page: into this user's saved pages (``ll.home()/pages``,
        what Open… lists in DataCube shown from Python), by legend-lite's own saved-page rules (the native library's,
        the page's and the server's: docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B6) -- over the copy this page was last
        saved as, else as a new one -- named ``name`` (the page's own when not given); its id is returned. With a
        ``path``, the page document as a file instead (as DataCube's Export > Page File writes it)."""
        if path is not None:
            if name is not None:
                raise ValueError("a page saved as a file takes no name: its document carries the page's own")
            Path(path).write_text(self.to_json() + '\n', 'utf-8', newline='')
            return None
        from ._home import THIS_USER, pages_folder, store_call
        named = _given_name(self._session, 'page', name if name is not None else self.name, "a saved page's name")
        id = self._saved if self._saved is not None else str(uuid.uuid4())
        record = _json.dumps({'id': id, 'name': named, 'content': self._written()}, indent=None)
        status, said = (store_call('lite_saved_cubes', pages_folder(), 'PUT', f'/{id}', '', record, THIS_USER)
                        if self._saved is not None else (404, ''))
        if status == 404:
            # never saved, or its saved copy deleted since (in DataCube's Open…): saved as a new one under its id
            status, said = store_call('lite_saved_cubes', pages_folder(), 'POST', '', '', record, THIS_USER)
        if status != 200:
            try:
                said = json.loads(said).get('message', said)
            except (ValueError, AttributeError):
                pass  # not the store's {"message"}: said as it came
            raise ValueError(f'the page was not saved: {said}')
        self._saved = id
        return id

    @classmethod
    def load(cls, document: str | Path | Mapping[str, Any], frames: Mapping[str, Any], *, mode: str = LIVE) -> Page:
        """A page document (a file, or its dict) as a page again, its grids over ``frames`` by their frame names (each
        cube's ``{"_type": "frame", "name": ...}``), served ``mode``: the document as DataCube reads it (any version it
        reads, brought to its own; refused in its words when DataCube would not open it)."""
        from .datacube import _current
        # read on the session the page will be made on (Page() takes the current one)
        doc = _read_page(_copied(document) if isinstance(document, Mapping) else Path(document).read_text('utf-8'),
                         _current())
        wanted = sorted({c['cube']['source']['name'] for c in doc['cubes']})
        missing = [n for n in wanted if n not in frames]
        if missing:
            raise ValueError(f'the page reads the frames {", ".join(missing)}: give them, frames={{name: frame}}')
        page = cls(doc['name'])
        for name in wanted:
            page._session.register(frames[name], name, mode)
        page._doc = doc
        page._ids |= _ids(doc)
        return page

    # -- shown ---------------------------------------------------------------------------------------------------------

    def read(self) -> Page:
        """The page as it is open now: the changes made in DataCube since Python last changed it (a sheet arranged, a
        chart added, a grid regrouped) taken in -- the open page's document is this page's from now on, its handles
        still good (a tile closed there is gone here too) -- and it returns itself. Not open, or the open page has said
        nothing since: as it is. (A change made in Python replaces what was changed in DataCube meanwhile: read first, to
        keep it.)"""
        said = self._session.engine().read_page(self._key) if self._shown else None
        if said is not None:
            doc = _read_page(said, self._session)
            unserved = sorted({c['cube']['source']['name'] for c in doc['cubes']} - set(self._served()))
            if unserved:
                raise ValueError(f'the open page reads the frames {", ".join(unserved)}, which this session does not serve')
            # taken quietly: the open page is this already; a handle of a tile it no longer has, gone
            self._forget(_ids(self._doc) - _ids(doc))
            self._doc = doc
            self._ids |= _ids(doc)
        return self

    @property
    def _key(self) -> str:
        """Its name on the engine (page.json?page=): this page object's own."""
        return f'page-{id(self):x}'

    def _showable(self) -> None:
        """Refused when it cannot be shown: a page DataCube would not open (one with no grid), in DataCube's words."""
        self._written()

    def _written(self) -> dict[str, Any]:
        """The page as DataCube reads it, to be written, saved or shown: refused in DataCube's words -- a page with no
        grid yet with Python's own call to make one after them, as a filter's refusals say Python's call form."""
        try:
            return _read_page(self._doc, self._session)
        except ValueError as refused:
            if self._doc['cubes']:
                raise
            raise ValueError(f'{refused} -- in Python, page.grid(frame) adds one') from None

    def _changed(self) -> None:
        """Something on the page changed: where it is shown, it is served again (and opened again there)."""
        if self._quiet or not self._shown:
            return
        version = self._session.engine().serve_page(self._key, self._doc)
        for view in list(self._shown):
            view._page_moved(version)

    class _Batch:
        def __init__(self, page: Page, tell: bool) -> None:
            self.page = page
            self.tell = tell

        def __enter__(self) -> None:
            self.page._quiet += 1

        def __exit__(self, kind: Any, *_: Any) -> None:
            self.page._quiet -= 1
            # told once, when it went through; a change that raised tells nothing
            if kind is None and self.tell:
                self.page._changed()

    def _batch(self, tell: bool = True) -> Page._Batch:
        """Several changes, told as one (``tell=False``: not told, what changed not being on the page yet)."""
        return Page._Batch(self, tell)

    # -- the document's parts ------------------------------------------------------------------------------------------

    def _next(self, kind: str) -> str:
        """A new id of ``kind`` (``grid-3``): after every one the page has held, so never one a handle had."""
        return f'{kind}-{max((_number(t) for t in self._ids if t.startswith(kind + "-")), default=0) + 1}'

    def _forget(self, ids: set[str]) -> None:
        """The handles of tiles and sheets gone, refused from now on."""
        for key in [k for k in self._handles if k[1] in ids]:
            self._handles.pop(key)._dead = True

    def _frames(self) -> set[str]:
        """The frames its cubes read, a frozen chart's whose grid is gone included."""
        return {c['cube']['source']['name'] for c in self._doc['cubes']}

    def _cube(self, id: str) -> dict[str, Any]:
        return next(c['cube'] for c in self._doc['cubes'] if c['id'] == id)

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

    def _place(self, id: str, sheet: Sheet, near: str | None = None) -> None:
        """A place for tile ``id`` on ``sheet``, as DataCube places a new tile (bands.ts add): beside ``near`` while its
        band has room, otherwise in a band of its own below it; with no ``near``, a band at the bottom."""
        self._ids.add(id)
        sheet._doc['layout'] = self._layout('layoutAdd', sheet._doc['layout'], id, near)
        self._changed()

    def _unplace(self, id: str) -> None:
        """Tile ``id`` off the sheet it is on, as DataCube takes one off (bands.ts remove): its place closed by its
        neighbours, a band it emptied gone."""
        for sheet in self._doc['sheets']:
            if id in _tiles(sheet['layout']):
                sheet['layout'] = self._layout('layoutRemove', sheet['layout'], id)

    def _layout(self, call: str, layout: dict[str, Any], *args: Any) -> dict[str, Any]:
        """A sheet's layout edited by DataCube's own rules (datacube/src/layout/bands.ts), asked headless (``call``: its
        layoutAdd, layoutRemove or layoutStack): the layout as a page saves it."""
        return json.loads(self._session.headless().call(call, _json.dumps(layout), *args))

    def _update(self, name: str, frame: Any, mode: str | None) -> None:
        """Frame ``name`` served again over ``frame``: each grid's cube over it rebuilt over the frame as the compiler
        types it now, as DataCube reopens a saved cube over its source (``_frame_cube``: a new column hidden, a gone one
        left out of everything that read it, the open groups dropped when the grouping changed -- each said as a
        warning, in DataCube's words); the page served again when one changed."""
        session = self._session
        session.register(frame, name, session.frames[name].mode if mode is None else mode)
        moved = False
        for c in (c for c in self._doc['cubes'] if c['cube']['source']['name'] == name):
            cube, notes = self._frame_cube(name, len(self._doc['cubes']), c['cube'])
            # the frame's columns as they were: its cube stands as it is (a page opened over them rebuilds the same)
            if cube['source'] == c['cube']['source']:
                continue
            for note in notes:
                warnings.warn(f'{name}: {note}', stacklevel=3)
            c['cube'] = cube
            moved = True
        if moved:
            self._changed()

    def _frame_cube(self, frame: str, grids_on_page: int,
                    saved: dict[str, Any] | None = None) -> tuple[dict[str, Any], list[str]]:
        """A grid's cube over ``frame`` as DataCube makes one (cube-document.ts rebuilt, asked headless: gridCube): a
        new grid's (its rows as they are; capped at the row limit when ``grids_on_page``, the page's grids counting
        it, are more than one: DataCube's gridCapped), or ``saved`` rebuilt over the frame as it is now; and what
        DataCube says of what changed."""
        engine, frames = self._session.headless(), self._session.frames
        with frames.serving():
            table = frames[frame]
            spec = {'model': table.model, 'runtime': table.runtime, 'source': table.accessor, 'frame': frame}
            made = json.loads(engine.call(
                'gridCube', spec, None if saved is None else _json.dumps(saved), self.name, frame, grids_on_page))
        return _plain(_json.loads(made['cube'])), list(made['notes'])

    def __repr__(self) -> str:
        return f'<Page {self.name!r}: {len(self.grids)} grids, {len(self.charts)} charts, {len(self.sheets)} sheets>'


def _ids(doc: dict[str, Any]) -> set[str]:
    """The ids a page document gives its cubes, views and sheets."""
    return {x['id'] for part in ('cubes', 'views', 'sheets') for x in doc[part]}


def _read_page(doc: Any, session: Any) -> dict[str, Any]:
    """``doc`` (a page document, or its JSON text as it is) as DataCube reads a page document and would write it again
    (page-document.ts readPage and pageToJson, asked headless, on ``session``'s engine: the page's own): of any version
    DataCube reads, migrated to its own, every rule of its reader held, and refused in its words otherwise -- Python
    keeps no second copy of them. Then what a page in Python adds: each cube over a frame."""
    said = json.loads(session.headless().call('pageRead', doc if isinstance(doc, str) else _json.dumps(doc)))
    if 'problems' in said:
        raise ValueError('; '.join(said['problems']))
    page: dict[str, Any] = _plain(_json.loads(said['page']))
    for c in page['cubes']:
        source = c['cube']['source']
        if source.get('_type') != 'frame' or not isinstance(source.get('name'), str):
            raise ValueError(f'the cube {c["id"]} reads no frame (a page in Python is over frames)')
    return page


def _column_choices(columns: Mapping[str, Any]) -> dict[str, dict[str, str]]:
    """A person's choices for the class's columns, as the window gives them (model-changes' ``ColumnChoice``)."""
    out: dict[str, dict[str, str]] = {}
    for column, choice in columns.items():
        if isinstance(choice, str):
            choice = {'as': choice}
        if choice.get('as', 'property') not in ('property', 'key', 'omit'):
            raise ValueError(f"a column is a 'property', a 'key' or 'omit', not {choice.get('as')!r} ({column!r})")
        out[column] = {k: v for k, v in choice.items() if k in ('as', 'name', 'documentation')}
    return out


def _end(end: Any) -> dict[str, Any]:
    """An association's end as the writer takes it (model-changes' ``AssociationEnd``): ``(class, on)`` or a mapping."""
    if isinstance(end, Mapping):
        if not isinstance(end.get('class'), str) or not isinstance(end.get('on'), str):
            raise ValueError(f"an association's end names its class and what it is joined on: {dict(end)!r}")
        return {k: end[k] for k in ('class', 'on', 'name', 'multiplicity', 'unique') if k in end}
    cls, on = end
    return {'class': cls, 'on': on}
