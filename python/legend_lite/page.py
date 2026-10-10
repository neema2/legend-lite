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
    page.save('q3.page.json')           # DataCube's own page document (Export > Page File writes the same)
    again = ll.Page.load('q3.page.json', frames={'trades': trades_df})

A page is written as DataCube's page document (version 3; datacube/src/page-document.ts) and read by DataCube's own
reader, so whatever a page can hold, Python says in the page's own words: the typed methods here cover what a page is
usually built from, and ``configure`` (a grid's configuration) and ``options`` (a chart's) take the document's own keys
for the rest. Each grid is a cube over its frame by the frame's name (``{"_type": "frame"}``), served by this process's
engine, as ``show(df)`` serves one.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from . import compiler
from .frames import LIVE, SNAPPED

# DataCube's own vocabularies, as its sources spell them (datacube/src/snapshot.ts, chart-spec.ts): a name outside them is
# refused here, at the line that names it. tests/test_page.py reads each against DataCube's source.
FILTER_OPERATORS = frozenset({
    'equal', 'notEqual', 'lessThan', 'lessThanEqual', 'greaterThan', 'greaterThanEqual', 'isEmpty', 'isNotEmpty',
    'contains', 'notContains', 'startsWith', 'notStartsWith', 'endsWith', 'notEndsWith', 'in', 'notIn',
    'equalCaseInsensitive', 'notEqualCaseInsensitive', 'containsCaseInsensitive', 'startsWithCaseInsensitive',
    'endsWithCaseInsensitive', 'inCaseInsensitive', 'notInCaseInsensitive', 'equalColumn', 'equalCaseInsensitiveColumn',
    'notEqualColumn', 'notEqualCaseInsensitiveColumn', 'lessThanColumn', 'lessThanEqualColumn', 'greaterThanColumn',
    'greaterThanEqualColumn',
})
AGGREGATES = frozenset({
    'sum', 'count', 'average', 'min', 'max', 'median', 'stdDevSample', 'stdDevPopulation', 'varianceSample',
    'variancePopulation', 'joinStrings', 'wavg', 'unique',
})
MARKS = frozenset({'bar', 'line', 'area', 'scatter', 'pie', 'heatmap', 'treemap'})
DIRECTIONS = frozenset({'asc', 'desc'})
# the operators that take no value, and those that compare to another column (FilterCondition's rightColumn)
_NO_VALUE = frozenset({'isEmpty', 'isNotEmpty'})
_COLUMN = frozenset(op for op in FILTER_OPERATORS if op.endswith('Column'))

PAGE_KIND = 'datacube.page'
PAGE_VERSION = 3
CUBE_KIND = 'datacube.cube'
CUBE_VERSION = 1
# a sheet's layout before it is arranged: up to this many tiles side by side, in bands of their own after
_PER_BAND = 2
_PLAIN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _pure_name(name: str) -> str:
    """A column name in a column spec: as it is when plain, quoted otherwise."""
    return name if _PLAIN.match(name) else "'" + name.replace('\\', '\\\\').replace("'", "\\'") + "'"


def _filter(node: Any, columns: set[str]) -> dict[str, Any]:
    """A filter as the filter editor writes it: a condition ``(column, operator[, value])``, a group ``('and' | 'or',
    [nodes])``, or ``('not', node)``; checked against the frame's columns and DataCube's operators."""
    if isinstance(node, list):
        node = ('and', node)
    if not isinstance(node, tuple) or not node:
        raise ValueError(f'a filter is a condition (column, operator, value), a group ("and", [...]) or ("not", ...), '
                         f'not {node!r}')
    head = node[0]
    if head in ('and', 'or') and len(node) == 2 and isinstance(node[1], (list, tuple)):
        return {'kind': head, 'children': [_filter(child, columns) for child in node[1]]}
    if head == 'not' and len(node) == 2:
        return {'kind': 'not', 'child': _filter(node[1], columns)}
    column, operator, *rest = node
    if column not in columns:
        raise ValueError(f'the filter names {column!r}, not a column of the grid ({", ".join(sorted(columns))})')
    if operator not in FILTER_OPERATORS:
        raise ValueError(f'{operator!r} is not one of DataCube\'s filter operators: {", ".join(sorted(FILTER_OPERATORS))}')
    condition: dict[str, Any] = {'kind': 'condition', 'column': column, 'operator': operator}
    if operator in _NO_VALUE:
        if rest:
            raise ValueError(f'{operator!r} takes no value')
        return condition
    if len(rest) != 1:
        raise ValueError(f'{operator!r} takes one value: (column, operator, value)')
    if operator in _COLUMN:
        if rest[0] not in columns:
            raise ValueError(f'{operator!r} compares to another column: {rest[0]!r} is not one of the grid\'s')
        condition['rightColumn'] = rest[0]
    else:
        condition['value'] = list(rest[0]) if isinstance(rest[0], (list, tuple, set)) else rest[0]
    return condition


class Tile:
    """A tile on a page: a grid or a chart. ``title``: what its header says (a grid's own title when unset)."""

    _prefix = 'tile'

    def __init__(self, page: Page, id: str, title: str | None) -> None:
        self.page = page
        self.id = id
        self._title = title

    @property
    def title(self) -> str | None:
        return self._title

    @title.setter
    def title(self, title: str | None) -> None:
        self._title = title
        self.page._changed()

    @property
    def sheet(self) -> Sheet:
        """The sheet it is on."""
        return next(s for s in self.page.sheets if any(self.id in place for place in s._places))

    def move_to(self, sheet: Sheet) -> None:
        """Onto another sheet of its page, at its end (its charts stay where they are, following it)."""
        sheet.add(self)


class Grid(Tile):
    """A grid over a frame: its groupings (``rows``), pivot (``columns``), measures, filter, sorts and calculated
    columns, as the grid's panels set them; its charts (``chart``)."""

    def __init__(self, page: Page, id: str, frame: str, title: str | None) -> None:
        super().__init__(page, id, title)
        self.frame = frame
        self._rows: list[str] = []
        self._pivot: list[str] = []
        self._measures: list[dict[str, Any]] = []
        self._filter: dict[str, Any] | None = None
        self._sorts: list[dict[str, Any]] = []
        self._derived: list[dict[str, Any]] = []
        self._configuration: dict[str, Any] = {'reportTitle': frame}
        # fields of its saved cube this module does not write, read from a page loaded: written back as they were
        self._query_extra: dict[str, Any] = {}
        self._cube_extra: dict[str, Any] = {}
        self._tree: dict[str, Any] = {'open': [], 'showTotals': False}

    @property
    def _table(self) -> Any:
        return self.page._session.frames[self.frame]

    @property
    def _columns(self) -> set[str]:
        return {c.name for c in self._table.columns} | {d['name'] for d in self._derived}

    def _named(self, names: Iterable[str], what: str) -> list[str]:
        names = list(names)
        unknown = [n for n in names if n not in self._columns]
        if unknown:
            raise ValueError(f'{what} names {", ".join(map(repr, unknown))}, not a column of {self.frame!r} '
                             f'({", ".join(sorted(self._columns))})')
        return names

    def group(self, *rows: str) -> Grid:
        """Grouped by ``rows``, outermost first (the Rows zone)."""
        self._rows = self._named(rows, 'group')
        self.page._changed()
        return self

    def pivot(self, *columns: str) -> Grid:
        """Pivoted on ``columns`` (the Columns zone)."""
        self._pivot = self._named(columns, 'pivot')
        self.page._changed()
        return self

    def measure(self, column: str, fn: str = 'sum', name: str | None = None, *, weight: str | None = None) -> Grid:
        """A measure: ``column`` aggregated by ``fn`` (one of AGGREGATES; ``wavg`` takes a ``weight`` column)."""
        self._named([column] if fn != 'count' else [], 'a measure')
        if fn not in AGGREGATES:
            raise ValueError(f'{fn!r} is not one of DataCube\'s aggregates: {", ".join(sorted(AGGREGATES))}')
        if (fn == 'wavg') != (weight is not None):
            raise ValueError('a weighted average (wavg) takes a weight column, and only it does')
        measure: dict[str, Any] = {'name': name or column, 'column': column, 'fn': fn}
        if weight is not None:
            measure['weight'] = self._named([weight], 'a weight')[0]
        self._measures.append(measure)
        self.page._changed()
        return self

    def filter(self, condition: Any) -> Grid:
        """Its filter: a condition ``(column, operator[, value])``, a list of them (all of them), a group
        ``('and' | 'or', [...])`` or ``('not', ...)`` -- the filter editor's own conditions; ``None`` for none."""
        self._filter = None if condition is None else _filter(condition, self._columns)
        self.page._changed()
        return self

    def sort(self, column: str, direction: str = 'asc') -> Grid:
        """A sort, after those it has: ``column`` ``'asc'`` or ``'desc'``."""
        if direction not in DIRECTIONS:
            raise ValueError(f"a sort's direction is 'asc' or 'desc', not {direction!r}")
        self._sorts.append({'column': self._named([column], 'a sort')[0], 'direction': direction})
        self.page._changed()
        return self

    def calculate(self, name: str, expression: str) -> Grid:
        """A calculated column computed per row, in Pure over the row ``$x`` (``'$x.qty * 2'``; a column that can be
        empty, as a dataframe's can, ``->toOne()`` first for arithmetic), as the grid's own editor takes it. The compiler
        checks it here, against the frame: a mistake fails at this line, in the compiler's words."""
        if name in self._columns:
            raise ValueError(f'{name!r} is a column of the grid already')
        table = self._table
        # typed as the grid would run it: the frame's rows extended by it
        compiler.relation_type(table.model, compiler.parse(f'|{table.accessor}->extend(~{_pure_name(name)}:x|{expression})'))
        self._derived.append({'name': name, 'lambda': compiler.parse(f'x|{expression}')})
        self.page._changed()
        return self

    def configure(self, settings: Mapping[str, Any]) -> Grid:
        """Any of the grid's configuration (its Properties), in its saved document's own keys (``reportTitle``,
        ``showRootAggregation``, ``columns``: each column's formats and colours, ...)."""
        self._configuration.update(settings)
        self.page._changed()
        return self

    def chart(self, mark: str, *, x: str | None = None, y: Sequence[tuple[str, str]] | None = None,
              split: str | None = None, title: str | None = None, frozen: bool = False,
              options: Mapping[str, Any] | None = None) -> Chart:
        """A chart of this grid, beside it: ``mark`` (one of MARKS), its category ``x``, what it plots ``y`` --
        ``[(column, aggregate), ...]`` -- and a ``split`` (one series per value); ``frozen`` keeps its own grouping
        rather than following the grid's; ``options``: the chart's Options, in its spec's own keys."""
        if mark not in MARKS:
            raise ValueError(f'{mark!r} is not one of DataCube\'s charts: {", ".join(sorted(MARKS))}')
        y = list(y if y is not None else [(m['column'], m['fn']) for m in self._measures[:1]])
        if not y:
            raise ValueError('a chart plots something: give y=[(column, aggregate)] (or a measure to the grid)')
        spec: dict[str, Any] = {'version': 1, 'mark': mark, 'y': []}
        for column, fn in y:
            if fn not in AGGREGATES:
                raise ValueError(f'{fn!r} is not one of DataCube\'s aggregates')
            spec['y'].append({'column': self._named([column], 'a chart')[0], 'fn': fn})
        if x is not None:
            spec['x'] = self._named([x], 'a chart')[0]
        if split is not None:
            spec['split'] = self._named([split], 'a chart')[0]
        if frozen:
            spec['frozen'] = True
        chart = Chart(self.page, self.page._next('chart'), self, title or f'Chart {self.page._count("chart")}', spec,
                      dict(options or {}))
        self.page._add(chart, near=self)
        return chart

    def update(self, frame: Any, mode: str | None = None) -> None:
        """Another frame for this grid (Live or Snapped as before, unless ``mode`` says): a page showing it queries it
        again (over its new columns if they changed)."""
        session = self.page._session
        session.register(frame, self.frame, session.frames[self.frame].mode if mode is None else mode)
        # a page showing it queries it again (its frame's version moves)
        session.engine().changed(self.frame)

    def _cube(self, name: str) -> dict[str, Any]:
        """The grid as its saved cube (cube-document.ts): its frame by name, its query, its configuration."""
        table = self._table
        columns = [{'name': c.name, 'type': c.type} for c in table.columns]
        query = {**self._query_extra, 'columns': columns, 'derived': self._derived, 'rows': self._rows,
                 'pivotOn': self._pivot, 'measures': self._measures, 'sorts': self._sorts}
        if self._filter is not None:
            query['filter'] = self._filter
        return {**self._cube_extra, 'kind': CUBE_KIND, 'version': CUBE_VERSION, 'name': name,
                'source': {'_type': 'frame', 'name': self.frame, 'columns': columns}, 'query': query,
                'configuration': self._configuration, 'tree': self._tree}

    def __repr__(self) -> str:
        return f'<Grid {self.id} over {self.frame!r}>'


class Chart(Tile):
    """A chart of a grid: its mark, what it plots, its options; following its grid unless frozen."""

    def __init__(self, page: Page, id: str, grid: Grid, title: str, spec: dict[str, Any], options: dict[str, Any]) -> None:
        super().__init__(page, id, title)
        self.grid = grid
        self._spec = spec
        self._options = options

    def options(self, settings: Mapping[str, Any]) -> Chart:
        """The chart's Options (``orientation``, ``stack``, ``sort``, ``limit``, ``labels``, ``legend``), in its spec's own
        keys; one left out is the chart's default."""
        self._options.update(settings)
        self.page._changed()
        return self

    def _view(self) -> dict[str, Any]:
        return {'id': self.id, 'kind': 'chart', 'cube': self.grid.id, 'title': self._title,
                'spec': {**self._spec, 'options': self._options}}

    def __repr__(self) -> str:
        return f'<Chart {self.id} of {self.grid.id}>'


class Sheet:
    """A sheet of a page: a whole screen of tiles, in places -- each a tile, or a stack of tiles shown one at a time --
    laid out in bands (``layout``, ``arrange``)."""

    def __init__(self, page: Page, id: str, name: str | None) -> None:
        self.page = page
        self.id = id
        self._name = name
        # its places in reading order, each the tiles in it (more than one: a stack)
        self._places: list[list[str]] = []
        # its bands: each a list of indexes into the places, side by side; None, laid out as they come
        self._bands: list[list[int]] | None = None
        # a layout read from a page loaded, kept as it was until the sheet's tiles change
        self._kept: dict[str, Any] | None = None

    @property
    def name(self) -> str | None:
        """Its own name; None: named after its first grid, as DataCube names a sheet."""
        return self._name

    @name.setter
    def name(self, name: str | None) -> None:
        self._name = name
        self.page._changed()

    @property
    def tiles(self) -> list[Tile]:
        return [self.page._tiles[t] for place in self._places for t in place]

    def add(self, *tiles: Tile) -> Sheet:
        """Puts ``tiles`` on this sheet, each at its end (from the sheet it was on)."""
        for tile in tiles:
            self.page._place(tile, self)
        self.page._changed()
        return self

    def layout(self, *bands: Sequence[Tile]) -> Sheet:
        """Its tiles laid out in bands, top to bottom, each band's tiles side by side: ``sheet.layout([grid, chart],
        [other])``. Every tile on the sheet is in one band; a stack is named by any of its tiles."""
        chosen: list[list[int]] = []
        for band in bands:
            if not band:
                raise ValueError('a band holds a tile at least')
            row: list[int] = []
            for tile in band:
                at = next((i for i, place in enumerate(self._places) if tile.id in place), None)
                if at is None:
                    raise ValueError(f'{tile!r} is not on the sheet {self.id}')
                if at in row or any(at in b for b in chosen):
                    raise ValueError(f'{tile!r} (or a tile stacked with it) is in the layout twice')
                row.append(at)
            chosen.append(row)
        if sorted(i for band in chosen for i in band) != list(range(len(self._places))):
            raise ValueError('every tile on the sheet is in the layout once')
        self._bands = chosen
        self._kept = None
        self.page._changed()
        return self

    def arrange(self, shape: str) -> Sheet:
        """Its places arranged as one of the layout picker's simplest shapes: ``'side-by-side'`` (one band) or
        ``'stacked'`` (a band each)."""
        n = len(self._places)
        if shape == 'side-by-side':
            self._bands = [list(range(n))]
        elif shape == 'stacked':
            self._bands = [[i] for i in range(n)]
        else:
            raise ValueError(f"a shape is 'side-by-side' or 'stacked' (or sheet.layout(...) band by band), not {shape!r}")
        self._kept = None
        self.page._changed()
        return self

    def _layout(self) -> dict[str, Any]:
        """Its bands as the page document has them: each band a row of its places with even shares, a screenful in all."""
        if self._kept is not None:
            return self._kept
        bands = self._bands if self._bands is not None else [
            list(range(i, min(i + _PER_BAND, len(self._places)))) for i in range(0, len(self._places), _PER_BAND)]
        leaf = lambda place: {'tile': place[0]} if len(place) == 1 else {'stack': list(place)}  # noqa: E731
        out = []
        for band in bands:
            places = [self._places[i] for i in band]
            node = leaf(places[0]) if len(places) == 1 else {
                'split': 'row', 'parts': [{'node': leaf(p), 'size': 1 / len(places)} for p in places]}
            out.append({'height': 1 / len(bands), 'node': node})
        return {'kind': 'bands', 'fit': True, 'bands': out}

    def _document(self) -> dict[str, Any]:
        return {'id': self.id, **({'name': self._name} if self._name is not None else {}), 'layout': self._layout()}

    def __repr__(self) -> str:
        return f'<Sheet {self.id}{f" {self._name!r}" if self._name else ""}: {len(self.tiles)} tiles>'


class Page:
    """A DataCube page: its grids over frames (``grid``), their charts, its sheets (``sheet``), stacks (``stack``) and
    layouts -- shown with ``ll.show(page)``, live from then on; saved as DataCube's page document (``save``,
    ``to_dict``) and loaded again (``load``)."""

    def __init__(self, name: str = 'Untitled page') -> None:
        from .datacube import _current
        self._session = _current()
        self.name = name
        self._tiles: dict[str, Tile] = {}
        self._counts: dict[str, int] = {}
        self.sheets: list[Sheet] = [Sheet(self, 'sheet-1', None)]
        self._counts['sheet'] = 1
        # fields of a page loaded this module does not write, written back as they were
        self._extra: dict[str, Any] = {}
        # where it is shown (datacube.show_page): told each time it changes
        self._shown: list[Any] = []
        self._quiet = 0

    # -- building ----------------------------------------------------------------------------------------------------

    def grid(self, frame: Any, *, name: str | None = None, rows: Sequence[str] = (), columns: Sequence[str] = (),
             measures: Mapping[str, str] | Sequence[tuple[str, str]] | None = None, filter: Any = None,
             sort: Sequence[tuple[str, str]] = (), title: str | None = None, sheet: Sheet | None = None,
             mode: str = LIVE) -> Grid:
        """A grid over ``frame`` (a pandas or polars DataFrame, an Arrow table, or a function returning one), served
        under ``name`` (``frame``, ``frame_2``, ... unless given; a frame already served by that name is replaced), on
        ``sheet`` (the first unless given). ``rows``, ``columns``, ``measures`` (``{column: aggregate}`` or
        ``[(column, aggregate)]``), ``filter`` and ``sort``: as ``group``, ``pivot``, ``measure``, ``filter`` and
        ``sort`` say. ``mode``: Live (each query reads the frame as it is then) or ``'snapped'``."""
        if mode not in (LIVE, SNAPPED):
            raise ValueError(f'mode is {LIVE!r} or {SNAPPED!r}, not {mode!r}')
        served = self._session.register(frame, name, mode)
        grid = Grid(self, self._next('grid'), served, title)
        with self._batch():
            self._add(grid, sheet=sheet)
            if rows:
                grid.group(*rows)
            if columns:
                grid.pivot(*columns)
            for column, fn in (measures.items() if isinstance(measures, Mapping) else measures or ()):
                grid.measure(column, fn)
            if filter is not None:
                grid.filter(filter)
            for column, direction in sort:
                grid.sort(column, direction)
        return grid

    def sheet(self, name: str | None = None) -> Sheet:
        """A new sheet after the last (``name``: its own; None, named after its first grid)."""
        sheet = Sheet(self, self._next('sheet'), name)
        self.sheets.append(sheet)
        self._changed()
        return sheet

    def stack(self, *tiles: Tile) -> None:
        """``tiles`` in one place, as tabs, one shown at a time (where the first one is): all on one sheet."""
        if len(tiles) < 2:
            raise ValueError('a stack is two tiles or more')
        sheet = tiles[0].sheet
        if any(t.sheet is not sheet for t in tiles):
            raise ValueError('the tiles of a stack are on one sheet (move them there first: sheet.add(tile))')
        # in the order given, where the first one is (a tile stacked already brings its stack's others after)
        ids = list(dict.fromkeys([t.id for t in tiles] + [o for t in tiles for place in sheet._places
                                                          if t.id in place for o in place]))
        first = next(i for i, place in enumerate(sheet._places) if ids[0] in place)
        rest = [[t for t in place if t not in ids] for place in sheet._places]
        rest[first] = ids
        sheet._places = [place for place in rest if place]
        sheet._bands = None
        sheet._kept = None
        self._changed()

    @property
    def grids(self) -> list[Grid]:
        return [t for t in self._tiles.values() if isinstance(t, Grid)]

    @property
    def charts(self) -> list[Chart]:
        return [t for t in self._tiles.values() if isinstance(t, Chart)]

    # -- the document --------------------------------------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """The page as DataCube's page document (version 3): one cube per grid, a view per tile, its sheets."""
        views = [{'id': g.id, 'kind': 'grid', 'cube': g.id, **({'title': g.title} if g.title else {})} for g in self.grids]
        views += [c._view() for c in self.charts]
        return {**self._extra, 'kind': PAGE_KIND, 'version': PAGE_VERSION, 'name': self.name,
                'cubes': [{'id': g.id, 'cube': g._cube(self.name)} for g in self.grids], 'views': views,
                'sheets': [s._document() for s in self.sheets]}

    def save(self, path: str | Path) -> None:
        """The page document, as a file (as DataCube's Export > Page File writes it)."""
        Path(path).write_text(json.dumps(self.to_dict(), indent=2), 'utf-8')

    @classmethod
    def load(cls, document: str | Path | Mapping[str, Any], frames: Mapping[str, Any], *, mode: str = LIVE) -> Page:
        """A page document (a file, or its dict) as a page again, its grids over ``frames`` by their frame names (each
        cube's ``{"_type": "frame", "name": ...}``). What this module does not model is kept as it was."""
        doc = dict(document) if isinstance(document, Mapping) else json.loads(Path(document).read_text('utf-8'))
        if doc.get('kind') != PAGE_KIND or doc.get('version') != PAGE_VERSION:
            raise ValueError(f'not a DataCube page document of version {PAGE_VERSION} '
                             f'(kind {doc.get("kind")!r}, version {doc.get("version")!r})')
        page = cls(doc['name'])
        with page._batch():
            page._extra = {k: v for k, v in doc.items() if k not in ('kind', 'version', 'name', 'cubes', 'views', 'sheets')}
            cubes = {c['id']: c['cube'] for c in doc['cubes']}
            views = {v['id']: v for v in doc['views']}
            missing = sorted({c['source']['name'] for c in cubes.values() if c['source']['_type'] == 'frame'} - set(frames))
            if missing:
                raise ValueError(f'the page reads the frames {", ".join(missing)}: give them, frames={{name: frame}}')
            page.sheets = []
            for saved in doc['sheets']:
                sheet = Sheet(page, saved['id'], saved.get('name'))
                sheet._kept = saved['layout']
                page.sheets.append(sheet)
                page._counts['sheet'] = max(page._counts.get('sheet', 0), _number(saved['id']))
                for place in _places(saved['layout']['bands']):
                    sheet._places.append(place)
                    for tile in place:
                        view = views[tile]
                        if view['kind'] == 'grid':
                            page._tiles[tile] = page._loaded_grid(tile, cubes[view['cube']], view.get('title'), frames, mode)
            for view in doc['views']:
                if view['kind'] == 'chart':
                    if not isinstance(page._tiles.get(view['cube']), Grid):
                        raise ValueError(f'the chart {view["id"]} reads a grid that was removed from the page (a detached '
                                         f'chart): Python does not load one yet')
                    spec = dict(view['spec'])
                    options = dict(spec.pop('options', {}))
                    page._tiles[view['id']] = Chart(page, view['id'], page._tiles[view['cube']], view['title'], spec, options)
                    page._counts['chart'] = max(page._counts.get('chart', 0), _number(view['id']))
        return page

    def _loaded_grid(self, id: str, cube: dict[str, Any], title: str | None, frames: Mapping[str, Any], mode: str) -> Grid:
        source = cube['source']
        served = self._session.register(frames[source['name']], source['name'], mode)
        grid = Grid(self, id, served, title)
        query = dict(cube['query'])
        grid._rows = list(query.pop('rows'))
        grid._pivot = list(query.pop('pivotOn'))
        grid._measures = list(query.pop('measures'))
        grid._sorts = list(query.pop('sorts'))
        grid._derived = list(query.pop('derived', []))
        grid._filter = query.pop('filter', None)
        query.pop('columns', None)
        grid._query_extra = query
        grid._configuration = dict(cube.get('configuration', {}))
        grid._tree = dict(cube.get('tree', {'open': [], 'showTotals': False}))
        grid._cube_extra = {k: v for k, v in cube.items()
                            if k not in ('kind', 'version', 'name', 'source', 'query', 'configuration', 'tree')}
        self._counts['grid'] = max(self._counts.get('grid', 0), _number(id))
        return grid

    # -- shown ---------------------------------------------------------------------------------------------------------

    def read(self) -> Page:
        """The page as it is open now, its changes made in DataCube since included (a sheet arranged, a chart added),
        as a page of these frames: this page itself when it is not open, or the open page has said nothing."""
        document = self._session.engine().read_page(self._key) if self._shown else None
        if document is None:
            return self
        frames = {g.frame: self._session.frames[g.frame]._read for g in self.grids}
        return Page.load(document, frames)

    @property
    def _key(self) -> str:
        """Its name on the engine (page.json?page=): this page object's own."""
        return f'page-{id(self):x}'

    def _changed(self) -> None:
        """Something on the page changed: where it is shown, it is served again (and opened again there)."""
        if self._quiet or not self._shown:
            return
        version = self._session.engine().serve_page(self._key, self.to_dict())
        for view in list(self._shown):
            view._page_moved(version)

    class _Batch:
        def __init__(self, page: Page) -> None:
            self.page = page

        def __enter__(self) -> None:
            self.page._quiet += 1

        def __exit__(self, *_: Any) -> None:
            self.page._quiet -= 1
            self.page._changed()

    def _batch(self) -> Page._Batch:
        """Several changes, told as one."""
        return Page._Batch(self)

    # -- its tiles -----------------------------------------------------------------------------------------------------

    def _next(self, kind: str) -> str:
        n = self._counts.get(kind, 0) + 1
        while f'{kind}-{n}' in self._tiles or (kind == 'sheet' and any(s.id == f'sheet-{n}' for s in self.sheets)):
            n += 1
        self._counts[kind] = n
        return f'{kind}-{n}'

    def _count(self, kind: str) -> int:
        return self._counts.get(kind, 0)

    def _add(self, tile: Tile, *, near: Tile | None = None, sheet: Sheet | None = None) -> None:
        """A new tile on ``sheet`` (``near``'s, else the first), after ``near``'s place, else at the end."""
        self._tiles[tile.id] = tile
        target = sheet if sheet is not None else near.sheet if near is not None else self.sheets[0]
        if target.page is not self:
            raise ValueError('the sheet is another page\'s')
        at = next((i for i, place in enumerate(target._places) if near is not None and near.id in place), None)
        target._places.insert(len(target._places) if at is None else at + 1, [tile.id])
        target._bands = None
        target._kept = None
        self._changed()

    def _place(self, tile: Tile, sheet: Sheet) -> None:
        """``tile`` onto ``sheet``, at its end, out of the place it had (a stack it leaves stays stacked)."""
        if tile.page is not self or sheet.page is not self:
            raise ValueError('the tile and the sheet are of one page')
        for s in self.sheets:
            places = [[t for t in place if t != tile.id] for place in s._places]
            if places != s._places:
                s._places = [p for p in places if p]
                s._bands = None
                s._kept = None
        sheet._places.append([tile.id])
        sheet._bands = None
        sheet._kept = None

    def __repr__(self) -> str:
        return f'<Page {self.name!r}: {len(self.grids)} grids, {len(self.charts)} charts, {len(self.sheets)} sheets>'


def _number(id: str) -> int:
    match = re.search(r'-(\d+)$', id)
    return int(match.group(1)) if match else 0


def _places(bands: list[dict[str, Any]]) -> list[list[str]]:
    """A saved layout's places in reading order: each a tile, or a stack's tiles."""
    out: list[list[str]] = []

    def walk(node: dict[str, Any]) -> None:
        if 'tile' in node:
            out.append([node['tile']])
        elif 'stack' in node:
            out.append(list(node['stack']))
        else:
            for part in node['parts']:
                walk(part['node'])
    for band in bands:
        walk(band['node'])
    return out
