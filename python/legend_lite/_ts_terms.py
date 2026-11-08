"""THE #TS BUILDER'S TERMS IN PYTHON (B3's Python twin, docs/TS_DECISIONS_2026_11_07.md §4, the builders).

A series written as a gs-quant notebook writes it -- ``volatility(column('close', 'SPX'), 22)``, ``spx / ndx - 1``,
``spx > moving_average(spx, 50)`` -- each term a gs-quant call as told, never computed here: DataCube's builder makes it
the tile's tree and writes its #TS text (datacube/src/timeseries-builder-wire.ts, the TypeScript builder's own
functions), and core reads that text back, the one reader. What gs-quant spells otherwise, or DataCube computes
otherwise, the TypeScript builder's ledger says (datacube/test/timeseries-builder.test.ts); its words are generated
(_ts_words.py, from the same catalog as TypeScript's). Python's operators are gs-quant's (pandas'), and its methods too
(``x.add(y)``, ``x.radd(5)``)."""

from __future__ import annotations

from enum import Enum
from typing import Any

__all__ = ['Term', 'ColumnTerm', 'Window', 'column', 'series', 'log', 'exp', 'sqrt', 'abs_', 'power', 'not_']


class _Default:
    """A parameter's default, shown as gs-quant writes it, and never told: the word's own default is DataCube's to take."""

    __slots__ = ('_text',)

    def __init__(self, text: str) -> None:
        self._text = text

    def __repr__(self) -> str:
        return self._text


class Window:
    """gs-quant's Window (gs_quant.timeseries.helper.Window): a size -- observations, or a calendar period ('1m', '1y')
    -- and a ramp, the size unless told; ``Window(None, 0)`` every observation to date."""

    __slots__ = ('w', 'r')

    def __init__(self, w: int | str | None = None, r: int | str | None = None) -> None:
        self.w = w
        self.r = w if r is None else r

    def __repr__(self) -> str:
        return f'Window({self.w!r}, {self.r!r})'


def _wire(v: Any) -> Any:
    """``v`` as DataCube's builder is told it (timeseries-builder-wire.ts): a term, a window, an enumeration's value."""
    if isinstance(v, Term):
        return v._wire
    if isinstance(v, Window):
        return {'term': 'window', 'w': v.w, 'r': v.r}
    if isinstance(v, Enum):
        return v.value
    if isinstance(v, (list, tuple)):
        return [_wire(x) for x in v]
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    raise TypeError(f'{v!r} is no argument of a series: a series, a number, a word, a Window, or a list of them')


class Term:
    """A SERIES WRITTEN IN CODE: a column, another series, a word of them, or their arithmetic -- what a timeseries plots
    (Timeseries.plot), its #TS text DataCube's to write."""

    __slots__ = ('_wire',)
    __hash__ = None  # type: ignore[assignment]  -- == is a series of comparisons, as pandas' is

    def __init__(self, wire: dict[str, Any]) -> None:
        self._wire = wire

    def _method(self, name: str, other: Any) -> Term:
        return Term({'term': 'method', 'name': name, 'x': self._wire, 'y': _wire(other)})

    # pandas' methods, gs-quant's arithmetic
    def add(self, other: Any) -> Term:
        return self._method('add', other)

    def sub(self, other: Any) -> Term:
        return self._method('sub', other)

    def mul(self, other: Any) -> Term:
        return self._method('mul', other)

    def div(self, other: Any) -> Term:
        return self._method('div', other)

    def radd(self, other: Any) -> Term:
        return self._method('radd', other)

    def rsub(self, other: Any) -> Term:
        return self._method('rsub', other)

    def rmul(self, other: Any) -> Term:
        return self._method('rmul', other)

    def rdiv(self, other: Any) -> Term:
        return self._method('rdiv', other)

    def gt(self, other: Any) -> Term:
        return self._method('gt', other)

    def ge(self, other: Any) -> Term:
        return self._method('ge', other)

    def lt(self, other: Any) -> Term:
        return self._method('lt', other)

    def le(self, other: Any) -> Term:
        return self._method('le', other)

    def eq(self, other: Any) -> Term:  # type: ignore[override]
        return self._method('eq', other)

    def ne(self, other: Any) -> Term:  # type: ignore[override]
        return self._method('ne', other)

    # Python's operators, as pandas' series take them
    __add__ = add
    __sub__ = sub
    __mul__ = mul
    __truediv__ = div
    __radd__ = radd
    __rsub__ = rsub
    __rmul__ = rmul
    __rtruediv__ = rdiv
    __gt__ = gt
    __ge__ = ge
    __lt__ = lt
    __le__ = le
    __eq__ = eq  # type: ignore[assignment]
    __ne__ = ne  # type: ignore[assignment]

    def __and__(self, other: Any) -> Term:
        return self._method('and', other)

    def __or__(self, other: Any) -> Term:
        return self._method('or', other)

    def __invert__(self) -> Term:
        return not_(self)

    def __neg__(self) -> Term:
        return self.rsub(0)

    def __bool__(self) -> bool:
        raise TypeError('a series is no truth value: combine conditions with &, | and ~')

    def __repr__(self) -> str:
        return f'<Term {self._wire!r}>'


class ColumnTerm(Term):
    """A column of a source along its date."""

    __slots__ = ()

    def period(self, fn: str) -> ColumnTerm:
        """Each period's ``fn`` of its rows under the tile's frequency (``last`` unless told; ``sum``, ``average``,
        ``first``, ``min``, ``max``)."""
        return ColumnTerm({**self._wire, 'period': fn})


def column(name: str, *picks: Any, **matches: Any) -> ColumnTerm:
    """A column of the tile's source (``column('px.close')``: a source by its alias), its rows of one instrument
    (``column('close', 'AAPL')``) or of a column's value (``column('close', venue='NYSE')``)."""
    told = [*picks, *([dict(matches)] if matches else [])]
    return ColumnTerm({'term': 'column', 'name': name, **({'picks': told} if told else {})})


def series(id: str) -> Term:
    """Another series of the tile, by its id (``series('A')``)."""
    return Term({'term': 'series', 'id': id})


def _scalar(name: str, x: Term, *args: float) -> Term:
    return Term({'term': 'scalar', 'name': name, 'x': _wire(x), **({'args': list(args)} if args else {})})


def log(x: Term) -> Term:
    """gs-quant's algebra.log: the natural log (the language's ln)."""
    return _scalar('log', x)


def exp(x: Term) -> Term:
    """gs-quant's algebra.exp: e to the x."""
    return _scalar('exp', x)


def sqrt(x: Term) -> Term:
    """gs-quant's algebra.sqrt: the square root."""
    return _scalar('sqrt', x)


def abs_(x: Term) -> Term:
    """gs-quant's algebra.abs_: the absolute value."""
    return _scalar('abs_', x)


def power(x: Term, y: float = 1) -> Term:
    """gs-quant's algebra.power: x to the power y (gs-quant's y 1)."""
    return _scalar('power', x, y)


def not_(series: Term) -> Term:
    """gs-quant's algebra.not_: a condition turned over."""
    return _scalar('not_', series)


def _word(name: str, lines: list[Any], **told: Any) -> Term:
    """Word ``name`` (the builder's, timeseries-builder-wire.ts) of ``lines``, each parameter told by name -- a default
    left out, DataCube's to take."""
    kw = {k: _wire(v) for k, v in told.items() if not isinstance(v, _Default)}
    return Term({'term': 'word', 'name': name, 'lines': [_wire(x) for x in lines], 'kw': kw})
