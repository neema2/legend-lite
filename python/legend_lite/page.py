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
    page.save('q3.page.json')           # DataCube's own page document (Export > Page File writes the same)
    again = ll.Page.load('q3.page.json', frames={'trades': trades_df})

A page IS its document: DataCube's page document (version 3; datacube/src/page-document.ts), the one copy of it. Its
grids, charts and sheets are handles onto it -- each its id -- whose methods edit it in place, each edit checked at the
line that makes it (a column the frame has, a calculated column the compiler types, a word DataCube's vocabulary has).
So whatever DataCube writes in a page is kept as it is, read back from where it is open by taking the open page's
document (``read``), and the document's own words reach what no method names: ``configure`` (a grid's configuration),
``options`` (a chart's), ``layout_from`` (a sheet's bands). Each grid's cube is over its frame by the frame's name
(``{"_type": "frame"}``), served by this process's engine, as ``show(df)`` serves one. A tile is placed and taken off as
DataCube's layout places it (datacube/src/layout/bands.ts): a grid in a band of its own at the bottom, a chart beside its
grid (four to a band at most), a tile taken off leaving its neighbours closing over its place.
"""

from __future__ import annotations

import datetime
import math
import numbers
import re
from collections.abc import Mapping, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

from . import _json, compiler
from .frames import LIVE, SNAPPED

# DataCube's own vocabularies, as its sources spell them (datacube/src/snapshot.ts, chart-spec.ts, layout/bands.ts): a
# name outside them is refused here, at the line that names it. datacube/test/python-vocabulary.test.ts reads each
# against DataCube's source.
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
# a band added at the bottom of a sheet, in screenfuls; the places a band holds side by side, at most; the bands the
# layout picker fits to a screen, at most
BAND_HEIGHT = 0.5
MAX_COLUMNS = 4
SCREEN_BANDS = 3

PAGE_KIND = 'datacube.page'
PAGE_VERSION = 3
CUBE_KIND = 'datacube.cube'
CUBE_VERSION = 1
# the operators that take no value, those that take a list of them, and those that compare to another column
_NO_VALUE = frozenset({'isEmpty', 'isNotEmpty'})
_LIST = frozenset({'in', 'notIn', 'inCaseInsensitive', 'notInCaseInsensitive'})
_COLUMN = frozenset(op for op in FILTER_OPERATORS if op.endswith('Column'))
_PLAIN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_KEEP: Any = object()


def _named(name: Any, what: str) -> Any:
    """A name given (a title, a sheet's, a page's): text that is not blank, or None."""
    if name is not None and (not isinstance(name, str) or not name.strip()):
        raise ValueError(f'{what} is a name, not {name!r}')
    return name


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
    return _plain(_json.loads(_json.dumps(dict(value))))


def _number(id: str) -> int:
    match = re.search(r'-(\d+)$', id)
    return int(match.group(1)) if match else 0


# -- filters ----------------------------------------------------------------------------------------------------------

def _value(operator: str, value: Any) -> Any:
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
    raise ValueError(f'{operator!r} takes text, a number, a boolean, a date, {{"relative": "today" | "now"}} or '
                     f'{{"json": text}}, not {value!r} (a timestamp as its text: "2024-01-31 10:30:00")')


def _filter(node: Any, columns: set[str]) -> dict[str, Any]:
    """A filter as the filter editor writes it: a condition ``(column, operator[, value])``, a group ``('and' | 'or',
    [nodes])``, or ``('not', node)``; a list is all of them. Checked against the grid's columns and DataCube's operators,
    each value against its operator."""
    if isinstance(node, list):
        node = ('and', node)
    if not isinstance(node, tuple) or len(node) < 2:
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
        raise ValueError(f'{operator!r} takes one value: (column, {operator!r}, value)')
    given = rest[0]
    if operator in _COLUMN:
        if given not in columns:
            raise ValueError(f'{operator!r} compares to another column: {given!r} is not one of the grid\'s')
        condition['rightColumn'] = given
    elif operator in _LIST:
        if not isinstance(given, (list, tuple)):
            raise ValueError(f'{operator!r} takes a list of values, in their order: (column, {operator!r}, [...])')
        condition['value'] = [_value(operator, v) for v in given]
    else:
        if isinstance(given, (list, tuple, set, frozenset)):
            raise ValueError(f'{operator!r} takes one value, not several (several are for "in")')
        condition['value'] = _value(operator, given)
    return condition


# -- a sheet's bands, edited as datacube/src/layout/bands.ts edits them: shares summing to 1, no split of one part nor
# one inside a split of its own direction, a stack of two tiles or more, every tile once ------------------------------

def _is_leaf(node: dict[str, Any]) -> bool:
    return 'tile' in node or 'stack' in node


def _leaf_tiles(node: dict[str, Any]) -> list[str]:
    return [node['tile']] if 'tile' in node else list(node['stack'])


def _split(direction: str, nodes: list[dict[str, Any]], sizes: list[Any] | None = None) -> dict[str, Any]:
    """A split of ``nodes`` in ``direction``, their shares ``sizes`` (even unless given); one node is itself. A part
    splitting the same way is folded in, its parts taking its share between them."""
    if len(nodes) == 1:
        return nodes[0]
    shares = sizes if sizes is not None else [1 / len(nodes)] * len(nodes)
    parts: list[dict[str, Any]] = []
    for node, share in zip(nodes, shares):
        if not _is_leaf(node) and node['split'] == direction:
            parts += [{'node': p['node'], 'size': p['size'] * share} for p in node['parts']]
        else:
            parts.append({'node': node, 'size': share})
    total = sum(p['size'] for p in parts)
    return {'split': direction, 'parts': [{'node': p['node'], 'size': p['size'] / total} for p in parts]}


def _without(node: dict[str, Any], tile: str) -> dict[str, Any] | None:
    """``node`` without ``tile`` (None when nothing is left), its neighbours taking its share; out of a stack, the stack
    keeps its place, a stack left with one tile being that tile. (A stack's tile in front is what a page shows, not what
    it saves -- bands.ts, asSaved -- so a stack here has none to keep.)"""
    if _is_leaf(node):
        if tile not in _leaf_tiles(node):
            return node
        rest = [t for t in _leaf_tiles(node) if t != tile]
        return None if not rest else {'tile': rest[0]} if len(rest) == 1 else {'stack': rest}
    kept = [(inner, p['size']) for p in node['parts'] if (inner := _without(p['node'], tile)) is not None]
    if not kept:
        return None
    return kept[0][0] if len(kept) == 1 else _split(node['split'], [n for n, _ in kept], [s for _, s in kept])


def _replaced(node: dict[str, Any], tile: str, by: dict[str, Any]) -> dict[str, Any]:
    """``node`` with the place holding ``tile`` (it, or its stack) replaced by ``by``."""
    if _is_leaf(node):
        return by if tile in _leaf_tiles(node) else node
    return _split(node['split'], [_replaced(p['node'], tile, by) for p in node['parts']], [p['size'] for p in node['parts']])


def _contains(node: dict[str, Any], tile: str) -> bool:
    return tile in _leaf_tiles(node) if _is_leaf(node) else any(_contains(p['node'], tile) for p in node['parts'])


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


def _problems(layout: Any) -> list[str]:
    """What keeps ``layout`` from being one DataCube draws (as page-document.ts and bands.ts read it), or nothing."""
    if not isinstance(layout, dict) or layout.get('kind') != 'bands' or not isinstance(layout.get('fit'), bool) \
            or not isinstance(layout.get('bands'), list):
        return ['it is not a layout of bands ({"kind": "bands", "fit": True | False, "bands": [...]})']
    out: list[str] = []
    seen: set[str] = set()

    def share(v: Any) -> bool:
        return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and v > 0

    def walk(node: Any, parent: str | None, where: str) -> None:
        if isinstance(node, dict) and isinstance(node.get('tile'), str):
            tiles = [node['tile']]
        elif isinstance(node, dict) and isinstance(node.get('stack'), list) and len(node['stack']) >= 2 \
                and all(isinstance(t, str) for t in node['stack']):
            tiles = node['stack']
        elif isinstance(node, dict) and node.get('split') in ('row', 'column') and isinstance(node.get('parts'), list) \
                and len(node['parts']) >= 2 and all(isinstance(p, dict) and share(p.get('size')) for p in node['parts']):
            if node['split'] == parent:
                out.append(f'{where}: a {parent} inside a {parent}')
            if abs(sum(p['size'] for p in node['parts']) - 1) > 1e-9:
                out.append(f'{where}: shares not summing to 1')
            for i, part in enumerate(node['parts']):
                walk(part.get('node'), node['split'], f'{where}.{i}')
            return
        else:
            out.append(f'{where} is not a tile, a stack of two or more, or a split of two parts or more')
            return
        out.extend(f'{t} is in the layout twice' for t in tiles if t in seen)
        seen.update(tiles)
    for i, band in enumerate(layout['bands']):
        if not isinstance(band, dict) or not share(band.get('height')):
            out.append(f'band {i} has no height above 0')
        else:
            walk(band.get('node'), None, f'band {i}')
    return out


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
        if _named(title, 'a title') is None:
            if self._kind == 'chart':
                raise ValueError('a chart has a title')
            view.pop('title', None)
        else:
            view['title'] = title
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
        named = _named(name, "a measure's name") or column
        measures = self._query.get('measures', [])
        if any(m['name'] == named for m in measures):
            raise ValueError(f'the grid has a measure named {named!r} already: give this one a name of its own (name=...)')
        measure = {'name': named, 'column': column, 'fn': fn,
                   **({'weight': self._check([weight], 'a weight')[0]} if weight is not None else {})}
        return self._set('measures', [*measures, measure])

    def filter(self, condition: Any) -> Grid:
        """Its filter: a condition ``(column, operator[, value])``, a list of them (all of them), a group
        ``('and' | 'or', [...])`` or ``('not', ...)`` -- the filter editor's own conditions; ``None`` for none."""
        if condition is not None:
            return self._set('filter', _filter(condition, self._columns()))
        self._query.pop('filter', None)
        self._page._changed()
        return self

    def sort(self, column: str, direction: str = 'asc') -> Grid:
        """A sort, after those it has: ``column`` ``'asc'`` or ``'desc'``."""
        if direction not in DIRECTIONS:
            raise ValueError(f"a sort's direction is 'asc' or 'desc', not {direction!r}")
        sort = {'column': self._check([column], 'a sort')[0], 'direction': direction}
        return self._set('sorts', [*self._query.get('sorts', []), sort])

    def calculate(self, name: str, expression: str) -> Grid:
        """A calculated column computed per row, in Pure over the row ``$x`` (``'$x.qty * 2'``; a column that can be
        empty, as a dataframe's can, ``->toOne()`` first for arithmetic), as the grid's own editor takes it. It can read
        the grid's calculated columns before it (a window column made in DataCube excepted: Python does not type one
        yet). The compiler checks it here, against the frame and those columns: a mistake fails at this line."""
        if not isinstance(name, str) or not name:
            raise ValueError(f'a calculated column has a name, not {name!r}')
        if name in self._columns():
            raise ValueError(f'{name!r} is a column of the grid already')
        table = self._page._session.frames[self.frame]
        derived = self._query.get('derived', [])
        # typed as the grid runs it (query.ts, extendDerived): the frame's rows extended by its calculated columns in
        # order, then by it
        before = ''.join(_step(d) for d in derived if 'lambda' in d and 'window' not in d and 'childAggregate' not in d)
        compiler.relation_type(table.model, compiler.parse(
            f'|{table.accessor}{before}->extend(~{_pure_name(name)}:x|{expression})'))
        return self._set('derived', [*derived, {'name': name, 'lambda': compiler.parse(f'x|{expression}')}])

    def configure(self, settings: Mapping[str, Any]) -> Grid:
        """Any of the grid's configuration (its Properties), in its saved document's own keys (``reportTitle``,
        ``showRootAggregation``, ``columns``: each column's formats and colours, ...)."""
        self._cube.setdefault('configuration', {}).update(_copied(settings))
        self._page._changed()
        return self

    def chart(self, mark: str, *, x: str | None = None, y: Sequence[tuple[str, str]] | None = None,
              split: str | None = None, title: str | None = None, frozen: bool = False,
              options: Mapping[str, Any] | None = None) -> Chart:
        """A chart of this grid, beside it: ``mark`` (one of MARKS), its category ``x``, what it plots ``y`` --
        ``[(column, aggregate), ...]``, the grid's first measure unless given -- and a ``split`` (one series per value);
        ``frozen`` keeps its own grouping rather than following the grid's (and stays when the grid is removed, as in
        DataCube); ``options``: the chart's Options, in its spec's own keys (one left out is its default)."""
        spec = {**_spec(self, mark, x, y, split, frozen), 'options': _copied(options or {})}
        named = _named(title, 'a title')
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


def _pure_name(name: str) -> str:
    """A column name in a column spec: as it is when plain, quoted otherwise."""
    return name if _PLAIN.match(name) else "'" + name.replace('\\', '\\\\').replace("'", "\\'") + "'"


def _step(derived: dict[str, Any]) -> str:
    """A calculated column as the grid's query takes it, as Pure text: ``->extend(~name:x|...)``, or an exploded one's
    ``->lateral(x|<collection>->flatten(~name))``."""
    lambda_ = derived['lambda']
    if not derived.get('unnest'):
        return f'->extend(~{_pure_name(derived["name"])}:{compiler.print_tree(lambda_, "STANDARD")})'
    # flatten's own tree, from the compiler's parse of it, around the column's collection
    flatten = compiler.parse(f'x|$x->flatten(~{_pure_name(derived["name"])})')['body'][0]
    flatten['parameters'][0] = lambda_['body'][0]
    return f'->lateral({compiler.print_tree({**lambda_, "body": [flatten]}, "STANDARD")})'


def _spec(grid: Grid, mark: str, x: str | None, y: Sequence[tuple[str, str]] | None, split: str | None,
          frozen: bool) -> dict[str, Any]:
    """What a chart of ``grid`` plots (chart-spec.ts, version 1), checked against DataCube's charts and aggregates and
    the grid's columns."""
    if mark not in MARKS:
        raise ValueError(f'{mark!r} is not one of DataCube\'s charts: {", ".join(sorted(MARKS))}')
    # the grid's first measure a chart can plot (a weighted average needs its weight, which a chart's y has not)
    measures = grid._query.get('measures', [])
    columns = grid._columns()
    plotted = list(y if y is not None else [(m['column'], m['fn']) for m in measures
                                            if m['fn'] != 'wavg' and m['column'] in columns][:1])
    if not plotted:
        raise ValueError('a chart plots something: give y=[(column, aggregate)] (or the grid a measure)')
    if not all(isinstance(p, tuple) and len(p) == 2 for p in plotted):
        raise ValueError(f'a chart plots (column, aggregate) pairs, not {plotted!r}')
    for _, fn in plotted:
        if fn not in AGGREGATES or fn == 'wavg':
            raise ValueError(f'{fn!r} is not an aggregate a chart plots: {", ".join(sorted(AGGREGATES - {"wavg"}))}')
    named = grid._check([c for c, _ in plotted] + [n for n in (x, split) if n is not None], 'a chart')
    return {'version': 1, 'mark': mark, 'y': [{'column': c, 'fn': fn} for c, (_, fn) in zip(named, plotted)],
            **({'x': x} if x is not None else {}), **({'split': split} if split is not None else {}),
            **({'frozen': True} if frozen else {})}


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
        """What it plots changed, as the chart's editor changes it: its ``mark``, ``x``, ``y``, ``split`` (None: none) or
        ``frozen``; what is not given stays."""
        view = self._view
        grid = self.grid
        if grid is None:
            raise ValueError(f'{self!r} has no grid (it was removed): its columns cannot be checked')
        was = view['spec']
        spec = _spec(grid, mark if mark is not None else was['mark'], was.get('x') if x is _KEEP else x,
                     [(p['column'], p['fn']) for p in was['y']] if y is _KEEP else y,
                     was.get('split') if split is _KEEP else split, bool(was.get('frozen')) if frozen is None else frozen)
        # what the spec holds besides (its options, a newer writer's fields) stays
        view['spec'] = {**{k: v for k, v in was.items() if k not in ('mark', 'x', 'y', 'split', 'frozen')}, **spec}
        self._page._changed()
        return self

    def options(self, settings: Mapping[str, Any]) -> Chart:
        """The chart's Options (``orientation``, ``stack``, ``sort``, ``limit``, ``labels``, ``legend``), in its spec's own
        keys; one left out is the chart's default."""
        self._view['spec'].setdefault('options', {}).update(_copied(settings))
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
        if _named(name, "a sheet's name") is None:
            sheet.pop('name', None)
        else:
            sheet['name'] = name
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
        return self._laid([{'height': _band_height(len(chosen)), 'node': _split('row', row)} for row in chosen])

    def arrange(self, shape: str) -> Sheet:
        """Its places arranged as one of the layout picker's simplest shapes, in reading order: ``'side-by-side'`` (one
        band) or ``'stacked'`` (a band each)."""
        leaves = _leaves(self._doc['layout'])
        if shape == 'side-by-side':
            return self._laid([{'height': 1, 'node': _split('row', leaves)}] if leaves else [])
        if shape == 'stacked':
            return self._laid([{'height': _band_height(len(leaves)), 'node': leaf} for leaf in leaves])
        raise ValueError(f"a shape is 'side-by-side' or 'stacked' (or sheet.layout(...) band by band), not {shape!r}")

    def layout_from(self, layout: Mapping[str, Any]) -> Sheet:
        """Its layout in the page document's own words -- ``{"kind": "bands", "fit": ..., "bands": [{"height": ...,
        "node": ...}]}``, each node a ``{"tile": id}``, a ``{"stack": [ids]}`` or a ``{"split": "row" | "column",
        "parts": [{"node": ..., "size": share}]}`` -- as ``page.to_dict()`` shows a sheet's. Every tile on the sheet is in
        it once, and only they."""
        sheet = self._doc
        given = _copied(layout)
        wrong = _problems(given)
        if wrong:
            raise ValueError(f'not a layout DataCube draws: {"; ".join(wrong)}')
        on, named = set(_tiles(sheet['layout'])), set(_tiles(given))
        if on != named:
            raise ValueError(f'a sheet\'s layout holds its tiles, each once: {", ".join(sorted(on - named)) or "none"} '
                             f'left out, {", ".join(sorted(named - on)) or "none"} not on the sheet')
        sheet['layout'] = given
        self._page._changed()
        return self

    def _laid(self, bands: list[dict[str, Any]]) -> Sheet:
        layout = self._doc['layout']
        layout['bands'] = bands
        self._page._changed()
        return self

    def __repr__(self) -> str:
        return f'<Sheet {self._id}>'


def _band_height(count: int) -> float:
    """A band's height as the layout picker makes its bands: up to SCREEN_BANDS to a screen."""
    return 1 / min(max(count, 1), SCREEN_BANDS)


# -- the page ---------------------------------------------------------------------------------------------------------

class Page:
    """A DataCube page: its grids over frames (``grid``), their charts, its sheets (``sheet``), stacks (``stack``) and
    layouts -- shown with ``ll.show(page)``, live from then on; saved as DataCube's page document (``save``,
    ``to_json``, ``to_dict``) and loaded again (``load``); read back as it is open (``read``)."""

    def __init__(self, name: str = 'Untitled page') -> None:
        from .datacube import _current
        if _named(name, "a page's name") is None:
            raise ValueError('a page has a name')
        self._session = _current()
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

    @property
    def name(self) -> str:
        return self._doc['name']

    @name.setter
    def name(self, name: str) -> None:
        if _named(name, "a page's name") is None:
            raise ValueError('a page has a name')
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
        _named(title, 'a title')
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
            columns_now = [{'name': c.name, 'type': c.type} for c in self._session.frames[served].columns]
            self._doc['cubes'].append({'id': id, 'cube': {
                'kind': CUBE_KIND, 'version': CUBE_VERSION, 'name': self.name,
                'source': {'_type': 'frame', 'name': served, 'columns': columns_now},
                'query': {'columns': [dict(c) for c in columns_now], 'derived': [], 'rows': [], 'pivotOn': [],
                          'measures': [], 'sorts': []},
                'configuration': {'reportTitle': served}, 'tree': {'open': [], 'showTotals': False}}})
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
        _named(name, "a sheet's name")
        id = self._next('sheet')
        self._doc['sheets'].append({'id': id, **({'name': name} if name else {}),
                                    'layout': {'kind': 'bands', 'fit': True, 'bands': []}})
        self._ids.add(id)
        self._changed()
        return self._sheet(id)

    def stack(self, *tiles: Tile) -> None:
        """``tiles`` in one place, as tabs, one shown at a time, where the first one is: all on one sheet. A tile
        stacked already brings its stack's others, after those given."""
        if len({t._id for t in tiles}) < 2:
            raise ValueError('a stack is two tiles or more')
        if any(t._page is not self for t in tiles):
            raise ValueError('the tiles of a stack are of this page')
        sheet = tiles[0].sheet
        if any(t.sheet is not sheet for t in tiles):
            raise ValueError('the tiles of a stack are on one sheet (put them there first: sheet.add(tile))')
        layout = sheet._doc['layout']
        leaves = {t: leaf for leaf in _leaves(layout) for t in _leaf_tiles(leaf)}
        ids = list(dict.fromkeys([t._id for t in tiles] + [o for t in tiles for o in _leaf_tiles(leaves[t._id])]))
        bands = layout['bands']
        for other in ids[1:]:
            bands = _bands_without(bands, other)
        layout['bands'] = [{**b, 'node': _replaced(b['node'], ids[0], {'stack': ids})} for b in bands]
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
        self._doc['views'], self._doc['cubes'] = views, cubes
        for id in gone:
            self._unplace(id)
        self._forget(gone)
        self._changed()

    # -- the document --------------------------------------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """The page as DataCube's page document (version 3), a copy: what is done to it is not done to the page. A
        calculated column's numbers are exact (``Decimal``), as the compiler writes them: ``to_json`` and ``save`` write
        them so."""
        return _copied(self._doc)

    def to_json(self, indent: int | None = 2) -> str:
        """The page document as JSON text, its numbers exact (what ``save`` writes)."""
        return _json.dumps(self._doc, indent=indent)

    def save(self, path: str | Path) -> None:
        """The page document, as a file (as DataCube's Export > Page File writes it)."""
        Path(path).write_text(self.to_json() + '\n', 'utf-8', newline='')

    @classmethod
    def load(cls, document: str | Path | Mapping[str, Any], frames: Mapping[str, Any], *, mode: str = LIVE) -> Page:
        """A page document (a file, or its dict) as a page again, its grids over ``frames`` by their frame names (each
        cube's ``{"_type": "frame", "name": ...}``), served ``mode``; the document kept as it is."""
        doc = _copied(document) if isinstance(document, Mapping) else _plain(_json.loads(Path(document).read_text('utf-8')))
        _check(doc)
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
            doc = _plain(said)
            _check(doc)
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
        """Refused when it cannot be shown: DataCube shows a page with a grid at least."""
        if not self._doc['cubes']:
            raise ValueError('a page shows a grid at least: page.grid(frame) first')

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
        """A place for tile ``id`` on ``sheet``, as DataCube places a new tile (bands.ts, add): beside ``near`` while its
        band is a row of fewer than MAX_COLUMNS places, otherwise in a band of its own below it; with no ``near``, a band
        at the bottom."""
        self._ids.add(id)
        layout = sheet._doc['layout']
        bands = list(layout['bands'])
        at = next((i for i, b in enumerate(bands) if near is not None and _contains(b['node'], near)), -1)
        if at < 0:
            bands.append({'height': BAND_HEIGHT, 'node': {'tile': id}})
        else:
            node = bands[at]['node']
            across = not _is_leaf(node) and node['split'] == 'row'
            if (len(node['parts']) if across else 1) < MAX_COLUMNS:
                nodes = [p['node'] for p in node['parts']] if across else [node]
                bands[at] = {**bands[at], 'node': _split('row', [*nodes, {'tile': id}])}
            else:
                bands.insert(at + 1, {'height': BAND_HEIGHT, 'node': {'tile': id}})
        layout['bands'] = bands
        self._changed()

    def _unplace(self, id: str) -> None:
        """Tile ``id`` off the sheet it is on, its place closed by its neighbours, a band it emptied gone."""
        for sheet in self._doc['sheets']:
            sheet['layout']['bands'] = _bands_without(sheet['layout']['bands'], id)

    def _update(self, name: str, frame: Any, mode: str | None) -> None:
        """Frame ``name`` served again over ``frame``: its grids' columns, as the compiler types the frame now, in their
        cubes (each column's settings kept by its name); the page served again when they changed."""
        session = self._session
        session.register(frame, name, session.frames[name].mode if mode is None else mode)
        now = [{'name': c.name, 'type': c.type} for c in session.frames[name].columns]
        moved = False
        for cube in (c['cube'] for c in self._doc['cubes'] if c['cube']['source']['name'] == name):
            if cube['source'].get('columns') != now:
                kept = {c['name']: c for c in cube['query'].get('columns', [])}
                cube['source']['columns'] = [dict(c) for c in now]
                cube['query']['columns'] = [{**kept.get(c['name'], {}), **c} for c in now]
                moved = True
        if moved:
            self._changed()

    def __repr__(self) -> str:
        return f'<Page {self.name!r}: {len(self.grids)} grids, {len(self.charts)} charts, {len(self.sheets)} sheets>'


def _bands_without(bands: list[dict[str, Any]], tile: str) -> list[dict[str, Any]]:
    """``bands`` without ``tile``, a band it emptied gone (bands.ts, remove)."""
    return [{**b, 'node': n} for b in bands if (n := _without(b['node'], tile)) is not None]


def _ids(doc: dict[str, Any]) -> set[str]:
    """The ids a page document gives its cubes, views and sheets."""
    return {x['id'] for part in ('cubes', 'views', 'sheets') for x in doc[part]}


def _check(doc: Any) -> None:
    """Refused, saying why, unless ``doc`` is a page document Python can work on: DataCube's (version 3), each cube over a
    frame, each id once, each view of one of its cubes and on one sheet, each layout of the shape Python edits. What else
    DataCube's reader asks of a page (page-document.ts: a chart's mark, a sheet's name), it asks when the page is shown:
    Python keeps no second copy of its rules."""
    def need(ok: bool, why: str) -> None:
        if not ok:
            raise ValueError(f'not a page Python reads: {why}')

    def each(part: str, ok: Any) -> list[dict[str, Any]]:
        items = doc.get(part)
        need(isinstance(items, list) and all(isinstance(x, dict) and isinstance(x.get('id'), str) and ok(x)
                                             for x in items), f'its {part} are not what a page holds')
        need(len({x['id'] for x in items}) == len(items), f'two of its {part} share an id')
        return items
    need(isinstance(doc, dict) and doc.get('kind') == PAGE_KIND, f'it is not a DataCube page ({PAGE_KIND})')
    need(doc.get('version') == PAGE_VERSION,
         f'it is of version {doc.get("version")!r}, not {PAGE_VERSION} (open it in DataCube and export it again)')
    need(isinstance(doc.get('name'), str), 'it has no name')
    cubes = each('cubes', lambda c: isinstance(c.get('cube'), dict) and isinstance(c['cube'].get('query'), dict))
    need(bool(cubes), 'it has no cube')
    for c in cubes:
        source = c['cube'].get('source')
        need(isinstance(source, dict) and source.get('_type') == 'frame' and isinstance(source.get('name'), str),
             f'the cube {c["id"]} reads no frame (a page in Python is over frames)')
    ids = {c['id'] for c in cubes}
    views = each('views', lambda v: v.get('kind') in ('grid', 'chart') and isinstance(v.get('cube'), str)
                 and v['cube'] in ids and (v['kind'] == 'grid' or isinstance(v.get('spec'), dict)))
    sheets = each('sheets', lambda s: True)
    need(bool(sheets), 'it has no sheet')
    placed: list[str] = []
    for s in sheets:
        wrong = _problems(s.get('layout'))
        need(not wrong, f'the sheet {s["id"]} cannot be laid out: {"; ".join(wrong)}')
        placed += _tiles(s['layout'])
    need(sorted(placed) == sorted(v['id'] for v in views), 'every view has its place, on one sheet')
