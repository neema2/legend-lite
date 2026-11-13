"""THE INTERPOLATION BASIS: values interpolated piecewise-linearly between the knots of a series, the non-linear basis
functions of them, and an expression's expansion into basis terms over those functions.

    from legend_lite.scalar_parallel import basis as b
    axis = b.PiecewiseLinear('S', [1.0, 2.0, 5.0])      # a series' knots, by coordinate
    y = axis.lerp(3.0)                                  # the value at 3.0: a LERP node over knots 1 and 2
    z = axis.exp_neg(3.0, t=3.0)                        # exp(-y(3.0) * 3.0): an EXP_NEG_XT node
    poly = b.expand(2.0 * z * y - z + 1.0)              # {(y, z): 2.0, (z,): -1.0, (): 1.0}

A KNOT is a variable of the compute tree (``Knot``, named ``series[index]``). A BASIS NODE is one function of the value
interpolated between two knots -- ``x = x0 + w (x1 - x0)`` -- and a parameter ``t``: ``exp(-x t)``, ``x`` or
``1 / (1 + x t)`` (``OPS``), as compute_basis::cbsEvalNode computes it in Pure. Its derivative by a knot is a constant
times the node itself (or its square), so the derivative of anything this module expands expands too: the basis is
closed under ``diff``.

``expand`` decomposes any expression into additive terms over shared ``BasisNode``s and general non-linear basis factors
(with no static limit on polynomial degree or parameter count), which ``legend_lite.scalar_parallel.basis_pure`` compiles
dynamically into Pure tables and functions. Standard library only.
"""

from __future__ import annotations

import bisect
import math
from dataclasses import dataclass
from typing import Iterable, Sequence

from .expr import (
    ONE,
    ZERO,
    Add,
    Const,
    Div,
    Exp,
    Expr,
    Mul,
    Neg,
    Var,
    _pure_float,
    add,
    diff,
    evaluate,
    is_constant,
    mul,
    postorder,
    variables,
)

# the basis functions, as compute_basis::cbsEvalNode names them
EXP_NEG_XT = 'EXP_NEG_XT'   # exp(-x t)
LERP = 'LERP'               # x
RECIP_1PXT = 'RECIP_1PXT'   # 1 / (1 + x t)
CONST = 'CONST'             # the offset alone
OPS = (EXP_NEG_XT, LERP, RECIP_1PXT, CONST)


class BasisError(ValueError):
    """An expression the basis cannot hold: a monomial of degree three or more, a division by a value of the knots, or
    a non-linear function of the knots that is not a basis node."""


class Knot(Var):
    """The ``index``-th knot of ``series``: a variable named ``series[index]``."""
    __slots__ = ()

    def __new__(cls, series: str, index: int) -> 'Knot':
        return Expr.__new__(cls, series, int(index))  # type: ignore[return-value]

    @property
    def series(self) -> str:
        return self.key[1]

    @property
    def index(self) -> int:
        return self.key[2]

    @property
    def name(self) -> str:
        return knot_name(self.series, self.index)


def knot_name(series: str, index: int) -> str:
    """The name a knot's value is given by in an environment."""
    return f'{series}[{index}]'


def knot(series: str, index: int) -> Knot:
    return Knot(series, index)


@dataclass(frozen=True)
class Interp:
    """Where a coordinate falls among a series' knots: ``x = x[k0] + weight (x[k1] - x[k0])``."""

    series: str
    k0: int
    k1: int
    weight: float


class BasisNode(Expr):
    """A LEVEL 1 BASIS NODE: ``op`` of the value interpolated between knots ``k0`` and ``k1`` of ``series`` and the
    parameter ``t``, plus ``offset``."""
    __slots__ = ()

    def __new__(cls, op: str, series: str, k0: int, k1: int, weight: float, t: float, offset: float = 0.0) -> 'BasisNode':
        if op not in OPS:
            raise ValueError(f'a basis node is one of {OPS}, not {op!r}')
        if not 0.0 <= weight <= 1.0:
            raise ValueError(f'a basis node interpolates with a weight in [0, 1], not {weight}')
        # one knot read one way: a weight of 0 reads k0, of 1 reads k1
        if weight == 0.0:
            k1 = k0
        elif weight == 1.0:
            k0, weight = k1, 0.0
        return Expr.__new__(cls, op, series, int(k0), int(k1), float(weight), float(t), float(offset))  # type: ignore

    op = property(lambda self: self.key[1])
    series = property(lambda self: self.key[2])
    k0 = property(lambda self: self.key[3])
    k1 = property(lambda self: self.key[4])
    weight = property(lambda self: self.key[5])
    t = property(lambda self: self.key[6])
    offset = property(lambda self: self.key[7])

    def knots(self) -> tuple[Knot, Knot]:
        return Knot(self.series, self.k0), Knot(self.series, self.k1)

    def reads(self):
        return () if self.op == CONST else self.knots()

    def value_of(self, x0: float, x1: float) -> float:
        """The node's value given its two knots' values: compute_basis::cbsEvalNode, in Python."""
        x = x0 + self.weight * (x1 - x0)
        if self.op == EXP_NEG_XT:
            return math.exp(-1.0 * x * self.t) + self.offset
        if self.op == LERP:
            return x + self.offset
        if self.op == RECIP_1PXT:
            return 1.0 / (1.0 + x * self.t) + self.offset
        return self.offset

    def _value(self, memo, env):
        if self.op == CONST:
            return self.offset
        k0, k1 = self.knots()
        return self.value_of(env[k0.name], env[k1.name])

    def _bare(self) -> 'BasisNode':
        return BasisNode(self.op, self.series, self.k0, self.k1, self.weight, self.t, 0.0)

    def _derivative(self, d, wrt):
        """In the basis: a constant times the node (exp), a constant (lerp), a constant times the node squared."""
        if self.op == CONST or not isinstance(wrt, Knot) or wrt.series != self.series:
            return ZERO
        dx = (1.0 - self.weight if wrt.index == self.k0 else 0.0) + (self.weight if wrt.index == self.k1 else 0.0)
        if dx == 0.0:
            return ZERO
        if self.op == EXP_NEG_XT:
            return mul(Const(-self.t * dx), self._bare())
        if self.op == LERP:
            return Const(dx)
        bare = self._bare()
        return mul(Const(-self.t * dx), mul(bare, bare))

    def _show(self, parts):
        where = f'{self.series}[{self.k0}]' if self.k0 == self.k1 else \
            f'{self.series}[{self.k0}..{self.k1}]@{self.weight:g}'
        return f'{self.op}({where}, t={self.t:g})' + (f'+{self.offset:g}' if self.offset else '')

    def _pure(self, parts, resolve):
        if self.op == CONST:
            return _pure_float(self.offset)
        k0, k1 = self.knots()
        x0 = resolve(k0)
        x = x0 if self.weight == 0.0 else f'({x0} + ({_pure_float(self.weight)} * ({resolve(k1)} - {x0})))'
        if self.op == EXP_NEG_XT:
            core = f'exp((-1.0 * {x}) * {_pure_float(self.t)})'
        elif self.op == LERP:
            core = x
        else:
            core = f'(1.0 / (1.0 + ({x} * {_pure_float(self.t)})))'
        return core if self.offset == 0.0 else f'({core} + {_pure_float(self.offset)})'


def node(op: str, series: str, k0: int, k1: int, weight: float, t: float = 0.0, offset: float = 0.0) -> BasisNode:
    return BasisNode(op, series, k0, k1, weight, t, offset)


# the constant monomial's node: valued 1 (its offset), reading no knot
UNIT = BasisNode(CONST, '', 0, 0, 0.0, 0.0, 1.0)


class SubMeasure(Expr):
    """A staged scalar sub-measure (such as a multi-cashflow swap annuity, floating-leg PV, or multi-knot surface
    volatility) reduced via SQL ``GROUP BY`` at stage ``stage - 1`` and read as a single slot input at stage ``>= stage``."""

    __slots__ = ()

    def __new__(cls, sub_id: str, stage: int, target: Expr) -> 'SubMeasure':
        return Expr.__new__(cls, str(sub_id), int(stage), target)  # type: ignore[return-value]

    sub_id = property(lambda self: self.key[1])
    stage = property(lambda self: self.key[2])
    target = property(lambda self: self.key[3])

    def reads(self):
        return tuple(variables(self.target))

    def _value(self, memo, env):
        return evaluate(self.target, env)

    def _derivative(self, d, wrt):
        return diff(self.target, wrt)

    def _show(self, parts):
        return f'SubMeasure({self.sub_id}@S{self.stage})'

    def _pure(self, parts, resolve):
        return f'${self.sub_id}'


class PiecewiseLinear:
    """A series' knots at increasing coordinates, a value between them interpolated linearly and held flat beyond the
    first and the last: where a coordinate falls (``at``), and the basis nodes of the value there."""

    def __init__(self, series: str, coords: Sequence[float]) -> None:
        if list(coords) != sorted(coords) or len(set(coords)) != len(coords):
            raise ValueError('knot coordinates are strictly increasing')
        self.series = series
        self.coords = [float(c) for c in coords]

    def knot(self, index: int) -> Knot:
        return Knot(self.series, index)

    @property
    def knots(self) -> list[Knot]:
        return [self.knot(i) for i in range(len(self.coords))]

    def at(self, coord: float) -> Interp:
        cs = self.coords
        if coord <= cs[0]:
            return Interp(self.series, 0, 0, 0.0)
        if coord >= cs[-1]:
            last = len(cs) - 1
            return Interp(self.series, last, last, 0.0)
        k1 = bisect.bisect_right(cs, coord)
        k0 = k1 - 1
        if coord == cs[k0]:
            # on a knot: that knot alone
            return Interp(self.series, k0, k0, 0.0)
        return Interp(self.series, k0, k1, (coord - cs[k0]) / (cs[k1] - cs[k0]))

    def lerp(self, coord: float) -> BasisNode:
        """The value at ``coord``."""
        i = self.at(coord)
        return BasisNode(LERP, self.series, i.k0, i.k1, i.weight, 0.0)

    def exp_neg(self, coord: float, t: float) -> Expr:
        """``exp(-x t)`` of the value x at ``coord`` (exactly 1 at ``t == 0``)."""
        if t == 0.0:
            return ONE
        i = self.at(coord)
        return BasisNode(EXP_NEG_XT, self.series, i.k0, i.k1, i.weight, t)

    def recip_1pxt(self, coord: float, t: float) -> Expr:
        """``1 / (1 + x t)`` of the value x at ``coord``."""
        if t == 0.0:
            return ONE
        i = self.at(coord)
        return BasisNode(RECIP_1PXT, self.series, i.k0, i.k1, i.weight, t)


# ---------------------------------------------------------------------------------------------- expansion

Monomial = tuple  # (), (factor_1,), (factor_1, factor_2, ...), ordered deterministically
Polynomial = dict  # Monomial -> coefficient


def _factor_sort_key(n: Expr) -> tuple:
    if isinstance(n, BasisNode):
        return (0, n.key)
    if isinstance(n, SubMeasure):
        return (1, n.stage, n.sub_id)
    return (2, repr(n))


def _combine_monomial(nodes: Iterable[Expr]) -> Monomial:
    """Orders basis factors and combines any EXP_NEG_XT BasisNodes that share the same interpolation point
    (exp(-x t1) * exp(-x t2) = exp(-x (t1 + t2)); exp(0) = 1)."""
    exp_t: dict[tuple[str, int, int, float], float] = {}
    others: list[Expr] = []
    for n in nodes:
        if isinstance(n, BasisNode) and n.op == EXP_NEG_XT and n.offset == 0.0:
            loc = (n.series, n.k0, n.k1, n.weight)
            exp_t[loc] = exp_t.get(loc, 0.0) + n.t
        else:
            others.append(n)
    for (series, k0, k1, weight), t_sum in exp_t.items():
        if abs(t_sum) > 1e-15:
            others.append(BasisNode(EXP_NEG_XT, series, k0, k1, weight, round(t_sum, 14), 0.0))
    return tuple(sorted(others, key=_factor_sort_key))


_ordered = _combine_monomial


def _mul_polys(pa: Polynomial, pb: Polynomial, context: Expr, max_degree: int | None = None) -> Polynomial:
    p: Polynomial = {}
    for ma, ca in pa.items():
        for mb, cb in pb.items():
            m = _combine_monomial(ma + mb)
            if max_degree is not None and len(m) > max_degree:
                raise BasisError(f'a monomial of degree {len(m)} (at most {max_degree}): {context!r}'[:300])
            p[m] = p.get(m, 0.0) + ca * cb
    return p


def expand(
    root: Expr | float,
    polys: dict[Expr, Polynomial] | None = None,
    constant: dict[Expr, bool] | None = None,
    *,
    max_degree: int | None = None,
    strict: bool = False,
) -> Polynomial:
    """Decomposes ``root`` into a ``Polynomial`` (mapping each ``Monomial`` tuple of ``BasisNode`` / non-linear
    ``Expr`` factors to its scalar coefficient).

    - Bare ``Knot``s (and ``Var``s) become ``LERP`` ``BasisNode``s; sub-expressions reading no variable fold to scalar
      constants.
    - Sums, negations, products of any degree, divisions by constants, divisions by ``EXP_NEG_XT`` monomials
      (``1 / exp(-x t) = exp(-x (-t))``), and exponentials of linear ``LERP`` combinations
      (``exp(c0 + sum c_i x_i) = exp(c0) * prod EXP_NEG_XT(x_i, -c_i)``) expand into pure ``BasisNode`` monomials.
    - When ``strict=False`` (default), general divisions and non-polynomial operations (``log``, ``sqrt``, ``cnd``,
      ``exp`` of products, etc.) are retained as dynamic basis factors rather than refused.
    """
    from .expr import lift
    root = lift(root)
    const_memo = constant if constant is not None else {}
    poly_memo = polys if polys is not None else {}
    for e in postorder(root, poly_memo):
        if is_constant(e, const_memo):
            v = evaluate(e, {})
            poly_memo[e] = {(): v} if v != 0.0 else {}
        elif isinstance(e, Knot):
            poly_memo[e] = {(BasisNode(LERP, e.series, e.index, e.index, 0.0, 0.0),): 1.0}
        elif isinstance(e, Var):
            poly_memo[e] = {(BasisNode(LERP, e.name, 0, 0, 0.0, 0.0),): 1.0}
        elif isinstance(e, (BasisNode, SubMeasure)):
            poly_memo[e] = {(e,): 1.0}
        elif isinstance(e, Add):
            p = dict(poly_memo[e.a])
            for m, c in poly_memo[e.b].items():
                p[m] = p.get(m, 0.0) + c
            poly_memo[e] = p
        elif isinstance(e, Neg):
            poly_memo[e] = {m: -c for m, c in poly_memo[e.a].items()}
        elif isinstance(e, Mul):
            poly_memo[e] = _mul_polys(poly_memo[e.a], poly_memo[e.b], e, max_degree=max_degree)
        elif isinstance(e, Div) and is_constant(e.b, const_memo):
            k = 1.0 / evaluate(e.b, {})
            poly_memo[e] = {m: c * k for m, c in poly_memo[e.a].items()}
        elif isinstance(e, Div):
            denom = {m: c for m, c in poly_memo[e.b].items() if abs(c) > 1e-18}
            if len(denom) == 1:
                mb, cb = next(iter(denom.items()))
                if all(isinstance(n, BasisNode) and n.op == EXP_NEG_XT and n.offset == 0.0 for n in mb):
                    inv_m = _combine_monomial(
                        BasisNode(EXP_NEG_XT, n.series, n.k0, n.k1, n.weight, -n.t, 0.0) for n in mb
                    )
                    poly_memo[e] = _mul_polys(poly_memo[e.a], {inv_m: 1.0 / cb}, e, max_degree=max_degree)
                    continue
                if not strict:
                    inv_factors = _combine_monomial(Div(ONE, f) for f in mb)
                    poly_memo[e] = _mul_polys(poly_memo[e.a], {inv_factors: 1.0 / cb}, e, max_degree=max_degree)
                    continue
            if not strict:
                poly_memo[e] = _mul_polys(poly_memo[e.a], {(Div(ONE, e.b),): 1.0}, e, max_degree=max_degree)
                continue
            raise BasisError(f'{type(e).__name__} of the knots is not in the basis (make it a basis node): {e!r}'[:300])
        elif isinstance(e, Exp):
            arg_poly = {m: c for m, c in poly_memo[e.a].items() if abs(c) > 1e-18}
            if arg_poly and all(
                len(m) == 0 or (len(m) == 1 and isinstance(m[0], BasisNode) and m[0].op == LERP and m[0].offset == 0.0)
                for m in arg_poly
            ):
                scale = 1.0
                exp_nodes: list[Expr] = []
                for m, c in arg_poly.items():
                    if not m:
                        scale *= math.exp(c)
                    else:
                        n = m[0]
                        exp_nodes.append(BasisNode(EXP_NEG_XT, n.series, n.k0, n.k1, n.weight, -c, 0.0))
                comb = _combine_monomial(exp_nodes)
                if max_degree is not None and len(comb) > max_degree:
                    raise BasisError(f'a monomial of degree {len(comb)} (at most {max_degree}): {e!r}'[:300])
                poly_memo[e] = {comb: scale}
                continue
            if not strict:
                poly_memo[e] = {(e,): 1.0}
                continue
            raise BasisError(f'{type(e).__name__} of the knots is not in the basis (make it a basis node): {e!r}'[:300])
        elif not strict:
            poly_memo[e] = {(e,): 1.0}
        else:
            raise BasisError(f'{type(e).__name__} of the knots is not in the basis (make it a basis node): {e!r}'[:300])
    return {m: c for m, c in poly_memo[root].items() if abs(c) > 1e-18}
