"""THE COMPUTE TREE: ordinary Python arithmetic traced into an expression DAG that can be walked, evaluated and
differentiated exactly.

    from legend_lite.scalar_parallel import expr as e
    x, y = e.Var('x'), e.Var('y')
    f = 3.0 * x * y + e.exp(x) / y          # a traced value: nothing is computed yet
    e.evaluate(f, {'x': 0.5, 'y': 2.0})
    df_dx = e.diff(f, x)                    # an expression too: exact, symbolic

The tree is HASH-CONSED: two structurally equal expressions are one object, so a value written twice is one node and
sharing in the DAG is identity. Constants fold as the tree is built. Walks are iterative (children before parents),
so a long sum does not recurse.

Every node kind says how it evaluates and differentiates itself (``_value``, ``_derivative``), so a module can add a
kind -- ``legend_lite.scalar_parallel.basis`` adds basis functions over interpolated knots -- and evaluate, diff and the walks
take it as their own. Nothing here knows of curves, knots or SQL. Standard library only.
"""

from __future__ import annotations

import math
import re
from typing import TYPE_CHECKING, Any, Callable, Mapping, Sequence

if TYPE_CHECKING:
    from .. import compiler

_INTERNED: dict[tuple, 'Expr'] = {}


def _pure_float(v: float) -> str:
    mag = abs(float(v))
    s = f'{mag:.16g}'
    if '.' not in s and 'e' not in s and 'E' not in s:
        s += '.0'
    return f'(-1.0 * {s})' if v < 0.0 else s


class Expr:
    """A traced value. Arithmetic on it builds the tree; Python numbers taking part become constants."""

    __slots__ = ('key', '__weakref__')

    def __new__(cls, *key: Any) -> 'Expr':
        full = (cls.__name__,) + key
        found = _INTERNED.get(full)
        if found is not None:
            return found
        made = object.__new__(cls)
        made.key = full
        _INTERNED[full] = made
        return made

    # ---- the tree
    def children(self) -> tuple['Expr', ...]:
        """The expressions this one is computed from."""
        return ()

    def reads(self) -> tuple['Var', ...]:
        """The variables this node reads directly (a Var itself; a kind that reads some without children)."""
        return ()

    # ---- what each kind says of itself
    def _value(self, memo: Mapping['Expr', float], env: Mapping[str, float]) -> float:
        raise NotImplementedError(type(self).__name__)

    def _derivative(self, d: Mapping['Expr', 'Expr'], wrt: 'Var') -> 'Expr':
        raise NotImplementedError(type(self).__name__)

    def _show(self, parts: list[str]) -> str:
        return f"{type(self).__name__}({', '.join(parts)})"

    def _pure(self, parts: list[str], resolve: Callable[['Var'], str]) -> str:
        raise NotImplementedError(type(self).__name__)

    # ---- arithmetic
    def __add__(self, o: Any) -> 'Expr':
        return add(self, lift(o))

    def __radd__(self, o: Any) -> 'Expr':
        return add(lift(o), self)

    def __sub__(self, o: Any) -> 'Expr':
        return add(self, neg(lift(o)))

    def __rsub__(self, o: Any) -> 'Expr':
        return add(lift(o), neg(self))

    def __mul__(self, o: Any) -> 'Expr':
        return mul(self, lift(o))

    def __rmul__(self, o: Any) -> 'Expr':
        return mul(lift(o), self)

    def __truediv__(self, o: Any) -> 'Expr':
        return div(self, lift(o))

    def __rtruediv__(self, o: Any) -> 'Expr':
        return div(lift(o), self)

    def __neg__(self) -> 'Expr':
        return neg(self)

    def __abs__(self) -> 'Expr':
        return abs_(self)

    def __repr__(self) -> str:
        return show(self)


class Const(Expr):
    __slots__ = ()

    @property
    def value(self) -> float:
        return self.key[1]

    def _value(self, memo, env):
        return self.value

    def _derivative(self, d, wrt):
        return ZERO

    def _show(self, parts):
        return repr(self.value)

    def _pure(self, parts, resolve):
        return _pure_float(self.value)


class Var(Expr):
    """A named input, valued by the environment an evaluation is given (``env[name]``)."""
    __slots__ = ()

    @property
    def name(self) -> str:
        return self.key[1]

    def reads(self):
        return (self,)

    def _value(self, memo, env):
        return env[self.name]

    def _derivative(self, d, wrt):
        return ONE if self is wrt else ZERO

    def _show(self, parts):
        return self.name

    def _pure(self, parts, resolve):
        return resolve(self)


class _Binary(Expr):
    __slots__ = ()

    @property
    def a(self) -> Expr:
        return self.key[1]

    @property
    def b(self) -> Expr:
        return self.key[2]

    def children(self):
        return (self.key[1], self.key[2])


class Add(_Binary):
    __slots__ = ()

    def _value(self, memo, env):
        return memo[self.a] + memo[self.b]

    def _derivative(self, d, wrt):
        return add(d[self.a], d[self.b])

    def _show(self, parts):
        return f'({parts[0]} + {parts[1]})'

    def _pure(self, parts, resolve):
        return f'({parts[0]} + {parts[1]})'


class Mul(_Binary):
    __slots__ = ()

    def _value(self, memo, env):
        return memo[self.a] * memo[self.b]

    def _derivative(self, d, wrt):
        return add(mul(d[self.a], self.b), mul(self.a, d[self.b]))

    def _show(self, parts):
        return f'({parts[0]} * {parts[1]})'

    def _pure(self, parts, resolve):
        return f'({parts[0]} * {parts[1]})'


class Div(_Binary):
    __slots__ = ()

    def _value(self, memo, env):
        return memo[self.a] / memo[self.b]

    def _derivative(self, d, wrt):
        # (a/b)' = a'/b - a b' / b^2
        return add(div(d[self.a], self.b), neg(div(mul(self.a, d[self.b]), mul(self.b, self.b))))

    def _show(self, parts):
        return f'({parts[0]} / {parts[1]})'

    def _pure(self, parts, resolve):
        return f'({parts[0]} / {parts[1]})'


class _Unary(Expr):
    __slots__ = ()

    @property
    def a(self) -> Expr:
        return self.key[1]

    def children(self):
        return (self.key[1],)


class Neg(_Unary):
    __slots__ = ()

    def _value(self, memo, env):
        return -memo[self.a]

    def _derivative(self, d, wrt):
        return neg(d[self.a])

    def _show(self, parts):
        return f'-{parts[0]}'

    def _pure(self, parts, resolve):
        return f'(-1.0 * {parts[0]})'


class Exp(_Unary):
    __slots__ = ()

    def _value(self, memo, env):
        return math.exp(memo[self.a])

    def _derivative(self, d, wrt):
        return mul(self, d[self.a])

    def _show(self, parts):
        return f'exp({parts[0]})'

    def _pure(self, parts, resolve):
        return f'exp({parts[0]})'


class Log(_Unary):
    __slots__ = ()

    def _value(self, memo, env):
        return math.log(memo[self.a])

    def _derivative(self, d, wrt):
        return div(d[self.a], self.a)

    def _show(self, parts):
        return f'log({parts[0]})'

    def _pure(self, parts, resolve):
        return f'log({parts[0]})'


class Sqrt(_Unary):
    __slots__ = ()

    def _value(self, memo, env):
        return math.sqrt(memo[self.a])

    def _derivative(self, d, wrt):
        return div(d[self.a], mul(Const(2.0), self))

    def _show(self, parts):
        return f'sqrt({parts[0]})'

    def _pure(self, parts, resolve):
        return f'sqrt({parts[0]})'


class Abs(_Unary):
    __slots__ = ()

    def _value(self, memo, env):
        return abs(memo[self.a])

    def _derivative(self, d, wrt):
        if d[self.a] is ZERO:
            return ZERO
        return mul(div(self.a, self), d[self.a])

    def _show(self, parts):
        return f'abs({parts[0]})'

    def _pure(self, parts, resolve):
        return f'abs({parts[0]})'


_TWO_OVER_SQRT_PI = 2.0 / math.sqrt(math.pi)


class Erf(_Unary):
    """Gaussian error function ``erf(a) = (2 / sqrt(pi)) * integral_0^a exp(-t^2) dt``, lowered to
    ``meta::legend::lite::erf`` in Pure with exact Gaussian derivative."""
    __slots__ = ()

    def _value(self, memo, env):
        return math.erf(memo[self.a])

    def _derivative(self, d, wrt):
        da = d[self.a]
        if da is ZERO:
            return ZERO
        return mul(mul(Const(_TWO_OVER_SQRT_PI), exp(neg(mul(self.a, self.a)))), da)

    def _show(self, parts):
        return f'erf({parts[0]})'

    def _pure(self, parts, resolve):
        return f'erf({parts[0]})'


ZERO = Const(0.0)
ONE = Const(1.0)


# ---------------------------------------------------------------------------------------------- building

def lift(x: Any) -> Expr:
    """An expression as it is; a Python number as a constant."""
    if isinstance(x, Expr):
        return x
    if isinstance(x, bool) or not isinstance(x, (int, float)):
        raise TypeError(f'not a number or an expression: {x!r}')
    return Const(float(x))


def add(a: Expr, b: Expr) -> Expr:
    if isinstance(a, Const) and isinstance(b, Const):
        return Const(a.value + b.value)
    if a is ZERO:
        return b
    if b is ZERO:
        return a
    return Add(a, b)


def neg(a: Expr) -> Expr:
    if isinstance(a, Const):
        return Const(-a.value)
    if isinstance(a, Neg):
        return a.a
    return Neg(a)


def mul(a: Expr, b: Expr) -> Expr:
    if isinstance(a, Const) and isinstance(b, Const):
        return Const(a.value * b.value)
    if a is ZERO or b is ZERO:
        return ZERO
    if a is ONE:
        return b
    if b is ONE:
        return a
    return Mul(a, b)


def div(a: Expr, b: Expr) -> Expr:
    if isinstance(b, Const):
        return mul(a, Const(1.0 / b.value))
    if a is ZERO:
        return ZERO
    return Div(a, b)


def exp(a: Any) -> float | Expr:
    if not isinstance(a, Expr):
        return math.exp(float(a))
    return Const(math.exp(a.value)) if isinstance(a, Const) else Exp(a)


def log(a: Any) -> float | Expr:
    if not isinstance(a, Expr):
        return math.log(float(a))
    return Const(math.log(a.value)) if isinstance(a, Const) else Log(a)


def sqrt(a: Any) -> float | Expr:
    if not isinstance(a, Expr):
        return math.sqrt(float(a))
    return Const(math.sqrt(a.value)) if isinstance(a, Const) else Sqrt(a)


def abs_(a: Any) -> float | Expr:
    if not isinstance(a, Expr):
        return abs(float(a))
    return Const(abs(a.value)) if isinstance(a, Const) else Abs(a)


def erf(a: Any) -> float | Expr:
    """Gaussian error function ``erf(a)``: returns a ``float`` on numbers and an ``Erf`` node on ``Expr``."""
    if not isinstance(a, Expr):
        return math.erf(float(a))
    return Const(math.erf(a.value)) if isinstance(a, Const) else Erf(a)


# ---------------------------------------------------------------------------------------------- walking

def postorder(root: Expr | float, skip: Any = None) -> list[Expr]:
    """Each node of the DAG once, children before parents (skipping subtrees already in ``skip``)."""
    root = lift(root)
    if skip is not None and root in skip:
        return []
    out: list[Expr] = []
    seen: set[int] = set()
    stack: list[tuple[Expr, bool]] = [(root, False)]
    while stack:
        node, done = stack.pop()
        if done:
            out.append(node)
            continue
        if id(node) in seen or (skip is not None and node in skip):
            continue
        seen.add(id(node))
        stack.append((node, True))
        for c in node.children():
            if id(c) not in seen and (skip is None or c not in skip):
                stack.append((c, False))
    return out


def evaluate(root: Expr | float, env: Mapping[str, float], cache: dict[Expr, float] | None = None) -> float:
    """``root``'s value, each variable valued by ``env`` (by name); ``cache`` may be shared by calls under one ``env``,
    so a node shared by many expressions is computed once."""
    if not isinstance(root, Expr):
        return float(root)
    memo = cache if cache is not None else {}
    for node in postorder(root, memo):
        memo[node] = node._value(memo, env)
    return memo[root]


def diff(root: Expr | float, wrt: Var, memo: dict[Expr, Expr] | None = None) -> Expr:
    """The exact derivative of ``root`` by ``wrt``, as an expression (each node differentiated once)."""
    if not isinstance(root, Expr):
        return ZERO
    d = memo if memo is not None else {}
    for node in postorder(root, d):
        d[node] = node._derivative(d, wrt)
    return d[root]


def variables(root: Expr | float) -> list[Var]:
    """The variables ``root`` reads, sorted by name."""
    if not isinstance(root, Expr):
        return []
    found: set[Var] = set()
    for node in postorder(root):
        found.update(node.reads())
    return sorted(found, key=lambda v: v.name)


def is_constant(root: Expr | float, memo: dict[Expr, bool] | None = None) -> bool:
    """Whether ``root`` reads no variable (its value is the same under any environment)."""
    if not isinstance(root, Expr):
        return True
    m = memo if memo is not None else {}
    for node in postorder(root, m):
        m[node] = not node.reads() and all(m[c] for c in node.children())
    return m[root]


def show(root: Expr | float) -> str:
    """``root`` as text, for reading."""
    root = lift(root)
    text: dict[Expr, str] = {}
    for node in postorder(root):
        text[node] = node._show([text[c] for c in node.children()])
    return text[root]


def _clean_ident(name: str) -> str:
    ident = re.sub(r'[^A-Za-z0-9_]', '_', name).strip('_')
    if not ident or ident[0].isdigit():
        return f'v_{ident}'
    return ident


def to_pure(
    root: Expr | float,
    *,
    params: Sequence[str] | None = None,
    row_param: str | None = None,
    row_to_one: bool = True,
    var_map: Mapping[str, str] | None = None,
    as_lambda: bool = True,
) -> str:
    """Renders ``root`` (or its symbolic derivative ``diff(root, x)``) directly as a Pure expression or lambda string.

    - With ``row_param='x'`` and ``as_lambda=False``, emits a Pure row expression ``$x.col->toOne() + ...`` suitable
      for ``grid.calculate(name, expr.to_pure(f, row_param='x', as_lambda=False))``.
    - With ``as_lambda=True`` (default), emits a complete Pure lambda (e.g. ``{x, y | ...}`` or ``x|$x.col->toOne()``)
      that ``compiler.parse`` compiles into a protocol ``compiler.Tree``.
    """
    root = lift(root)
    suffix = '->toOne()' if (row_param is not None and row_to_one) else ''

    def resolve(v: Var) -> str:
        if var_map is not None and v.name in var_map:
            mapped = var_map[v.name]
            if mapped.startswith('$') or row_param is None:
                return mapped
            return f'${row_param}.{mapped}{suffix}'
        ident = _clean_ident(v.name)
        if row_param is not None:
            return f'${row_param}.{ident}{suffix}'
        return f'${ident}'

    text: dict[Expr, str] = {}
    for node in postorder(root):
        text[node] = node._pure([text[c] for c in node.children()], resolve)
    body = text[root]
    if not as_lambda:
        return body
    if row_param is not None:
        return f'{row_param}|{body}'
    if params is not None:
        p_list = list(params)
    else:
        p_list = []
        if var_map is None:
            for v in variables(root):
                ident = _clean_ident(v.name)
                if ident not in p_list:
                    p_list.append(ident)
    if not p_list:
        return f'|{body}'
    if len(p_list) == 1:
        return f'{p_list[0]}|{body}'
    return f"{{{', '.join(p_list)} | {body}}}"


def _prune_for_diff(root: Expr | float, wrt_names: set[str]) -> Expr:
    """Drops additive terms (across ``Add``, ``Neg``, and products/quotients with ``wrt``-independent factors) that do
    not depend on ``wrt_names``, preserving the exact function value on all ``wrt``-dependent terms for Pure ``diff``."""
    root = lift(root)
    dep: dict[Expr, bool] = {}
    pruned: dict[Expr, Expr] = {}
    for node in postorder(root):
        has_w = any(v.name in wrt_names for v in node.reads()) or any(dep[c] for c in node.children())
        dep[node] = has_w
        if not has_w:
            pruned[node] = ZERO
        elif isinstance(node, Add):
            pruned[node] = add(pruned[node.a], pruned[node.b])
        elif isinstance(node, Neg):
            pruned[node] = neg(pruned[node.a])
        elif isinstance(node, Mul):
            if not dep[node.a]:
                pruned[node] = mul(node.a, pruned[node.b])
            elif not dep[node.b]:
                pruned[node] = mul(pruned[node.a], node.b)
            else:
                pruned[node] = node
        elif isinstance(node, Div) and not dep[node.b]:
            pruned[node] = div(pruned[node.a], node.b)
        else:
            pruned[node] = node
    return pruned[root]


def to_pure_diff(
    root: Expr | float,
    wrt: Var | str | Sequence[Var | str],
    *,
    params: Sequence[str] | None = None,
    row_param: str | None = None,
    row_to_one: bool = True,
    var_map: Mapping[str, str] | None = None,
    as_lambda: bool = True,
    scale: float = 1.0,
) -> str:
    """Renders ``root`` differentiated by ``wrt`` using Pure's native ``meta::legend::lite::diff`` rather than
    differentiating in Python."""
    wrt_seq: list[Var | str] = [wrt] if isinstance(wrt, (Var, str)) else list(wrt)
    if not wrt_seq:
        raise ValueError('to_pure_diff requires at least one differentiation variable')
    wrt_names = {w.name if isinstance(w, Var) else str(w) for w in wrt_seq}
    active_root = _prune_for_diff(root, wrt_names)

    def _build_nested(idx: int, cur_map: dict[str, str]) -> str:
        w = wrt_seq[idx]
        w_name = w.name if isinstance(w, Var) else str(w)
        w_var = w if isinstance(w, Var) else Var(w_name)
        arg_expr = to_pure(
            w_var,
            row_param=row_param,
            row_to_one=row_to_one,
            var_map=cur_map,
            as_lambda=False,
        )
        d_param = f'_d{idx}_{_clean_ident(w_name)}'
        next_map = dict(cur_map)
        next_map[w_name] = f'${d_param}'
        if idx + 1 < len(wrt_seq):
            inner_body = _build_nested(idx + 1, next_map)
        else:
            inner_body = to_pure(
                active_root,
                row_param=row_param,
                row_to_one=row_to_one,
                var_map=next_map,
                as_lambda=False,
            )
        return f'diff({{{d_param} | {inner_body}}}, {arg_expr})'

    base_map = dict(var_map) if var_map is not None else {}
    diff_call = _build_nested(0, base_map)
    if scale != 1.0:
        diff_call = f'({_pure_float(scale)} * {diff_call})'
    if not as_lambda:
        return diff_call
    if row_param is not None:
        return f'{row_param}|{diff_call}'
    if params is not None:
        p_list = list(params)
    else:
        p_list = []
        if var_map is None:
            for v in variables(root):
                ident = _clean_ident(v.name)
                if ident not in p_list:
                    p_list.append(ident)
    if not p_list:
        return f'|{diff_call}'
    if len(p_list) == 1:
        return f'{p_list[0]}|{diff_call}'
    return f"{{{', '.join(p_list)} | {diff_call}}}"


def to_tree(
    root: Expr,
    *,
    params: Sequence[str] | None = None,
    row_param: str | None = None,
    row_to_one: bool = True,
    var_map: Mapping[str, str] | None = None,
) -> 'compiler.Tree':
    """Compiles ``root`` into a Legend protocol ``compiler.Tree`` via ``compiler.parse(to_pure(root, ...))``."""
    from .. import compiler
    return compiler.parse(
        to_pure(
            root,
            params=params,
            row_param=row_param,
            row_to_one=row_to_one,
            var_map=var_map,
            as_lambda=True,
        )
    )


