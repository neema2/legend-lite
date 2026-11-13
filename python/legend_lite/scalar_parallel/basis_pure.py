"""THE BASIS IN PURE: measures expanded over the interpolation basis, hierarchical sub-measures, and dynamic formula
templates, compiled directly into a dynamically generated Pure model and relational schema -- planned for any database
legend-lite renders, run on DuckDB, ``Frames``, or ``legend_lite.warehouse``.

    from legend_lite.scalar_parallel import basis as b, basis_pure as p
    book = p.BasisBook()
    book.add('E1', 'G1', 'VALUE', '*', 2.0 * axis.exp_neg(3.0, 3.0) - 1.0)
    tables = book.tables([p.Scenario('BASE', {'S': [('S:1', 1.0, 0.03), ('S:2', 2.0, 0.04), ('S:5', 5.0, 0.05)]})])
    with p.ComputeBasis(book) as cb:
        con = cb.connect(tables)
        cb.run(con, p.measures_query())            # valued in SQL from the knots
        cb.plan(p.measures_query(), 'Snowflake').sql

The tables, by level:
  - Level 0 (``CBS_KNOT``): each series' knot coordinates and values per scenario.
  - Level 1 (``CBS_SHARED_NODE``): each shared interpolation basis node once per scenario.
  - Level 2 (``CBS_BASIS_TERM``): each sub-measure's (stage ``0 .. max_stage - 1``) and entity measure's
    (stage ``max_stage``) expanded terms, with dynamically sized node slots (``NODE_A_ID .. NODE_<K>_ID``) and parameter
    columns (``PARAM_1 .. PARAM_<M>``) plus dynamically synthesized Pure functions (and Pure ``diff()`` derivatives) for
    non-polynomial basis formulas (such as Black-Scholes / Black-76 option and swaption payoffs).
  - Level 3 (``CBS_JACOBIAN_ENTRY``): a Jacobian matrix ``d factor / d input`` for chain-rule risk aggregation.

Zero payoff or node evaluation runs on the Python host when building tables (``TENET_CHARTER.md`` C2.1: "the database
computes; the host carries"). No static ``.pure`` files are needed: ``render_pure_model`` synthesizes the Pure classes,
functions, relational store, view, and mappings directly from ``BasisSchema``.
"""

from __future__ import annotations

import functools
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence, Union

from .. import _host, compiler
from .._extra import require_datacube
from .basis import (
    CONST,
    EXP_NEG_XT,
    LERP,
    RECIP_1PXT,
    UNIT,
    BasisNode,
    Knot,
    Polynomial,
    SubMeasure,
    _combine_monomial,
    _factor_sort_key,
    expand,
    knot_name,
)
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
    add,
    diff,
    evaluate,
    is_constant,
    lift,
    mul,
    neg,
    postorder,
)

SlotNode = Union[BasisNode, SubMeasure]
SUB_MEASURE_CODE = '__SUB__'
SCHEMA_META_KEY = b'legend.basis_schema'


def _slot_tag(i: int) -> str:
    """Tag for the ``i``-th node slot on ``CBS_BASIS_TERM``: ``'A'..'Z'`` for ``0..25``, ``'N26'..`` beyond."""
    return chr(ord('A') + i) if i < 26 else f'N{i}'


def _is_slot_node(e: Expr) -> bool:
    return isinstance(e, (BasisNode, SubMeasure))


def _as_slot_node(e: Expr) -> SlotNode | None:
    if isinstance(e, (BasisNode, SubMeasure)):
        return e
    if hasattr(e, 'series') and hasattr(e, 'index'):
        return BasisNode(LERP, e.series, e.index, e.index, 0.0, 0.0)
    if isinstance(e, Var):
        return BasisNode(LERP, e.name, 0, 0, 0.0, 0.0)
    return None


def _is_pure_poly(poly: Polynomial) -> bool:
    return all(all(_is_slot_node(f) for f in m) for m in poly)


def _poly_stage(poly: Polynomial) -> int:
    return max((f.stage for m in poly for f in m if isinstance(f, SubMeasure)), default=0)


def _poly_to_expr(poly: Polynomial) -> Expr:
    acc: Expr = ZERO
    for m, c in sorted(poly.items(), key=lambda mc: tuple(_factor_sort_key(n) for n in mc[0])):
        if abs(c) <= 1e-18:
            continue
        term: Expr = Const(c)
        for f in m:
            term = mul(term, f)
        acc = add(acc, term)
    return acc


@dataclass(frozen=True)
class FormulaSpec:
    """A dynamically extracted non-polynomial basis formula compiled into ``compute_basis::cbsFormula_<op>``."""

    op: str
    signature: str
    pure_body: str
    num_nodes: int
    num_params: int
    base_op: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            'op': self.op,
            'signature': self.signature,
            'pure_body': self.pure_body,
            'num_nodes': self.num_nodes,
            'num_params': self.num_params,
            'base_op': self.base_op,
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> FormulaSpec:
        return cls(
            op=str(d['op']),
            signature=str(d['signature']),
            pure_body=str(d['pure_body']),
            num_nodes=int(d['num_nodes']),
            num_params=int(d['num_params']),
            base_op=str(d['base_op']) if d.get('base_op') is not None else None,
        )


class _PreextractedFormula(Expr):
    """Wraps a pre-extracted ``FormulaSpec`` (such as a Pure-level ``diff(...)`` derivative of a base formula)
    together with its slot inputs (``BasisNode`` or ``SubMeasure``) and scalar parameters."""

    __slots__ = ()

    @property
    def spec(self) -> FormulaSpec:
        return self.key[1]

    @property
    def nodes(self) -> tuple[SlotNode, ...]:
        return self.key[2]

    @property
    def params(self) -> tuple[float, ...]:
        return self.key[3]

    @property
    def base_expr(self) -> Expr:
        return self.key[4]

    @property
    def slot_idx(self) -> int:
        return self.key[5]

    @property
    def node_op(self) -> str:
        return self.key[6]

    def _value(self, memo, env):
        # Evaluated only if explicitly called in Python debug/oracle tests; never called when building SQL tables.
        dummy = Var('__pure_diff_slot__')
        base_slots = self.nodes[:-1] if self.node_op == 'SUB_CHAIN' else self.nodes
        slot_memo: dict[Expr, Expr] = {n: (ONE if i == self.slot_idx else ZERO) for i, n in enumerate(base_slots)}
        df_dslot = diff(self.base_expr, dummy, slot_memo)
        target = base_slots[self.slot_idx]
        if self.node_op == EXP_NEG_XT and isinstance(target, BasisNode):
            py_expr = mul(target._bare(), df_dslot)
        elif self.node_op == RECIP_1PXT and isinstance(target, BasisNode):
            bare = target._bare()
            py_expr = mul(mul(bare, bare), df_dslot)
        elif self.node_op == 'SUB_CHAIN':
            py_expr = mul(self.nodes[-1], df_dslot)
        else:
            py_expr = df_dslot
        return evaluate(py_expr, env, dict(memo))

    def _show(self, parts):
        return f'PureFormula({self.spec.op})'


@dataclass(frozen=True)
class BasisSchema:
    """Dynamic schema configuration for a ``BasisBook`` and its synthesized Pure model."""

    num_nodes: int = 3
    num_params: int = 0
    formulas: tuple[FormulaSpec, ...] = ()
    max_stage: int = 0
    stage_nodes: tuple[int, ...] = ()
    stage_slot_kinds: tuple[tuple[str, ...], ...] = ()
    primal_stage_nodes: tuple[int, ...] = ()
    diff_stage_nodes: tuple[int, ...] = ()
    primal_nodes: int = 3
    primal_ops: tuple[str, ...] | None = None
    diff_ops: tuple[str, ...] | None = None

    @property
    def slot_tags(self) -> tuple[str, ...]:
        return tuple(_slot_tag(i) for i in range(max(3, self.num_nodes)))

    @property
    def primal_formulas(self) -> tuple[FormulaSpec, ...]:
        if self.primal_ops is not None:
            wanted = set(self.primal_ops)
            return tuple(f for f in self.formulas if f.op in wanted)
        return tuple(f for f in self.formulas if f.base_op is None)

    @property
    def diff_formulas(self) -> tuple[FormulaSpec, ...]:
        if self.diff_ops is not None:
            wanted = set(self.diff_ops)
            return tuple(f for f in self.formulas if f.op in wanted)
        return tuple(f for f in self.formulas if f.base_op is not None)

    def to_json(self) -> bytes:
        payload: dict[str, Any] = {
            'num_nodes': self.num_nodes,
            'num_params': self.num_params,
            'formulas': [f.to_dict() for f in self.formulas],
            'max_stage': self.max_stage,
            'stage_nodes': list(self.stage_nodes),
            'stage_slot_kinds': [list(k) for k in self.stage_slot_kinds],
            'primal_stage_nodes': list(self.primal_stage_nodes),
            'diff_stage_nodes': list(self.diff_stage_nodes),
            'primal_nodes': self.primal_nodes,
        }
        if self.primal_ops is not None:
            payload['primal_ops'] = list(self.primal_ops)
        if self.diff_ops is not None:
            payload['diff_ops'] = list(self.diff_ops)
        return json.dumps(payload, separators=(',', ':')).encode('utf-8')

    @classmethod
    def from_json(cls, raw: bytes | str) -> BasisSchema:
        d = json.loads(raw.decode('utf-8') if isinstance(raw, bytes) else raw)
        p_ops = d.get('primal_ops')
        d_ops = d.get('diff_ops')
        return cls(
            num_nodes=int(d.get('num_nodes', 3)),
            num_params=int(d.get('num_params', 0)),
            formulas=tuple(FormulaSpec.from_dict(f) for f in d.get('formulas', ())),
            max_stage=int(d.get('max_stage', 0)),
            stage_nodes=tuple(int(x) for x in d.get('stage_nodes', ())),
            stage_slot_kinds=tuple(tuple(str(s) for s in k) for k in d.get('stage_slot_kinds', ())),
            primal_stage_nodes=tuple(int(x) for x in d.get('primal_stage_nodes', ())),
            diff_stage_nodes=tuple(int(x) for x in d.get('diff_stage_nodes', ())),
            primal_nodes=int(d.get('primal_nodes', d.get('num_nodes', 3))),
            primal_ops=tuple(str(x) for x in p_ops) if p_ops is not None else None,
            diff_ops=tuple(str(x) for x in d_ops) if d_ops is not None else None,
        )


DEFAULT_SCHEMA = BasisSchema()


@dataclass
class BasisContext:
    """Scoped context holding registered formula templates and the active ``BasisSchema``.

    Passed explicitly between ``BasisBook``, ``ComputeBasis``, and query builders so concurrent books or
    sessions remain completely isolated without mutable module-level globals.
    """

    schema: BasisSchema = DEFAULT_SCHEMA
    formulas_by_sig: dict[str, FormulaSpec] = field(default_factory=dict, repr=False)
    formulas_by_op: dict[str, FormulaSpec] = field(default_factory=dict, repr=False)

    def register_formula(
        self,
        signature: str,
        pure_body: str,
        num_nodes: int,
        num_params: int,
        base_op: str | None = None,
    ) -> FormulaSpec:
        found = self.formulas_by_sig.get(signature)
        if found is not None:
            return found
        op = f'F{len(self.formulas_by_sig) + 1:03d}'
        spec = FormulaSpec(
            op=op,
            signature=signature,
            pure_body=pure_body,
            num_nodes=num_nodes,
            num_params=num_params,
            base_op=base_op,
        )
        self.formulas_by_sig[signature] = spec
        self.formulas_by_op[op] = spec
        return spec

    def register_formula_diff(self, base_spec: FormulaSpec, slot_idx: int, node_op: str) -> FormulaSpec:
        """Synthesizes a Pure formula function that differentiates ``compute_basis::cbsFormula_<base_spec.op>``
        with respect to its ``slot_idx``-th slot input using Pure's native ``meta::legend::lite::diff``.

        When ``node_op == 'SUB_CHAIN'``, an extra slot ``val<N>`` carries ``d(sub_measure) / d(knot)`` and multiplies
        the Pure ``diff(...)`` partial derivative via the multivariate chain rule.
        """
        tag = _slot_tag(slot_idx)
        sig = f'PURE_DIFF({base_spec.signature},slot={slot_idx},op={node_op})'
        call_args = [
            '$_dv' if i == slot_idx else f'$val{_slot_tag(i)}' for i in range(base_spec.num_nodes)
        ] + [f'$param{j}' for j in range(1, base_spec.num_params + 1)]
        if not call_args:
            call_args = ['$_dv']
        diff_call = f"diff({{_dv | compute_basis::cbsFormula_{base_spec.op}({', '.join(call_args)})}}, $val{tag})"
        out_nodes = base_spec.num_nodes
        if node_op == EXP_NEG_XT:
            body = f'   $val{tag} * {diff_call}'
        elif node_op == RECIP_1PXT:
            body = f'   ($val{tag} * $val{tag}) * {diff_call}'
        elif node_op == 'SUB_CHAIN':
            chain_tag = _slot_tag(base_spec.num_nodes)
            out_nodes = base_spec.num_nodes + 1
            body = f'   $val{chain_tag} * {diff_call}'
        else:
            body = f'   {diff_call}'
        return self.register_formula(sig, body, out_nodes, base_spec.num_params, base_op=base_spec.op)

    def extract_formula(
        self,
        factors: Sequence[Expr],
    ) -> tuple[FormulaSpec, tuple[SlotNode, ...], tuple[float, ...], Expr]:
        """Fingerprints a non-polynomial product of ``factors`` into a reusable ``FormulaSpec``, its deduplicated
        slot inputs (``BasisNode`` or ``SubMeasure``), its scalar ``Const`` parameters, and the combined ``Expr``."""
        if len(factors) == 1 and isinstance(factors[0], _PreextractedFormula):
            pre = factors[0]
            self.formulas_by_sig.setdefault(pre.spec.signature, pre.spec)
            self.formulas_by_op.setdefault(pre.spec.op, pre.spec)
            return pre.spec, pre.nodes, pre.params, pre.base_expr

        term_expr: Expr = ONE
        for f in factors:
            term_expr = f if term_expr is ONE else mul(term_expr, f)

        nodes: list[SlotNode] = []
        node_idx: dict[SlotNode, int] = {}
        params: list[float] = []

        order: list[Expr] = []
        seen: set[int] = set()
        ref_counts: dict[int, int] = {}
        stack: list[tuple[Expr, bool]] = [(term_expr, False)]
        while stack:
            cur, done = stack.pop()
            if _as_slot_node(cur) is not None or isinstance(cur, Const):
                continue
            if done:
                order.append(cur)
                continue
            if id(cur) in seen:
                continue
            seen.add(id(cur))
            stack.append((cur, True))
            for ch in cur.children():
                if _as_slot_node(ch) is None and not isinstance(ch, Const):
                    ref_counts[id(ch)] = ref_counts.get(id(ch), 0) + 1
                    if id(ch) not in seen:
                        stack.append((ch, False))

        sig_memo: dict[int, str] = {}
        pure_memo: dict[int, str] = {}
        let_lines: list[str] = []
        let_count = 0

        def _child_rep(ch: Expr) -> tuple[str, str]:
            sn = _as_slot_node(ch)
            if sn is not None:
                idx = node_idx.get(sn)
                if idx is None:
                    idx = len(nodes)
                    nodes.append(sn)
                    node_idx[sn] = idx
                tag = _slot_tag(idx)
                return f'N{idx}', f'$val{tag}'
            if isinstance(ch, Const):
                params.append(float(ch.value))
                p_idx = len(params)
                return f'P{p_idx}', f'$param{p_idx}'
            return sig_memo[id(ch)], pure_memo[id(ch)]

        sn_root = _as_slot_node(term_expr)
        if sn_root is not None or isinstance(term_expr, Const):
            root_sig, root_pure = _child_rep(term_expr)
        else:
            for cur in order:
                c_reps = [_child_rep(ch) for ch in cur.children()]
                c_sigs = [s for s, _ in c_reps]
                c_pures = [p for _, p in c_reps]
                sig = f"{type(cur).__name__.upper()}({','.join(c_sigs)})"
                p_expr = cur._pure(c_pures, lambda v: v.name)

                sig_memo[id(cur)] = sig
                if cur is not term_expr and ref_counts.get(id(cur), 0) > 1:
                    let_count += 1
                    v_name = f'v{let_count}'
                    let_lines.append(f'   let {v_name} = {p_expr};')
                    pure_memo[id(cur)] = f'${v_name}'
                else:
                    pure_memo[id(cur)] = p_expr
            root_sig = sig_memo[id(term_expr)]
            root_pure = pure_memo[id(term_expr)]

        body = '\n'.join([*let_lines, f'   {root_pure};']) if let_lines else f'   {root_pure}'
        spec = self.register_formula(root_sig, body, len(nodes), len(params))
        return spec, tuple(nodes), tuple(params), term_expr


@dataclass(frozen=True)
class Scenario:
    """Each series' knots, in order, as ``(knot_id, coord, value)``, under one scenario."""

    scenario_id: str
    series: Mapping[str, Sequence[tuple[str, float, float]]]
    unit: str | None = None

    def env(self) -> dict[str, float]:
        """The knots' values by name: the environment ``expr.evaluate`` takes."""
        return {knot_name(s, i): v for s, knots in self.series.items() for i, (_k, _c, v) in enumerate(knots)}


@dataclass(frozen=True)
class JacobianEntry:
    """d ``factor_id`` / d ``input_id`` under a scenario (``input_coord``: where the input sits, for reading)."""

    scenario_id: str
    factor_id: str
    input_id: str
    weight: float
    input_coord: float = 0.0


@dataclass(frozen=True)
class _TermSpec:
    """Prepared specification for a single basis term row in ``CBS_BASIS_TERM``."""

    seq: int
    op: str
    coeff: float
    node_ids: tuple[str, ...]
    params: tuple[float, ...]
    shift: float


@dataclass
class BasisBook:
    """Measures expanded over the basis, hierarchical sub-measures, and dynamic formula templates, written as the
    ``compute_basis`` tables."""

    measures: list[tuple[str, str, str, str, Polynomial]] = field(default_factory=list)
    ctx: BasisContext = field(default_factory=BasisContext, repr=False)
    _ids: dict[BasisNode, str] = field(default_factory=dict)
    _polys: dict[Expr, Polynomial] = field(default_factory=dict, repr=False)
    _constant: dict[Expr, bool] = field(default_factory=dict, repr=False)
    _max_nodes: int = 3
    _max_primal_nodes: int = 3
    _max_params: int = 0
    _formulas: dict[str, FormulaSpec] = field(default_factory=dict, repr=False)
    _primal_ops: set[str] = field(default_factory=set, repr=False)
    _diff_ops: set[str] = field(default_factory=set, repr=False)
    _sub_measures: list[tuple[str, int, Polynomial]] = field(default_factory=list, repr=False)
    _sub_by_id: dict[str, tuple[int, Polynomial]] = field(default_factory=dict, repr=False)
    _sub_by_key: dict[tuple, SubMeasure] = field(default_factory=dict, repr=False)
    _factored_cache: dict[Expr, Expr] = field(default_factory=dict, repr=False)
    _orig_pure: dict[Expr, bool] = field(default_factory=dict, repr=False)
    _clean_polys: dict[Expr, Polynomial] = field(default_factory=dict, repr=False)
    _sub_diff_cache: dict[tuple[str, Var], Polynomial] = field(default_factory=dict, repr=False)
    _registered_measures: int = field(default=0, repr=False)
    _registered_subs: int = field(default=0, repr=False)
    _schema_cache: tuple[int, int, BasisSchema] | None = field(default=None, repr=False)

    def node_id(self, n: BasisNode) -> str:
        found = self._ids.get(n)
        if found is None:
            found = self._ids[n] = f'N{len(self._ids):06d}'
        return found

    def slot_id(self, n: SlotNode) -> str:
        if isinstance(n, SubMeasure):
            return n.sub_id
        return self.node_id(n)

    def _register_poly_slots(self, poly: Polynomial, *, role: str = 'sub') -> None:
        for m in poly:
            if all(_is_slot_node(n) for n in m):
                if len(m) > self._max_nodes:
                    self._max_nodes = len(m)
                if role == 'primal' and len(m) > self._max_primal_nodes:
                    self._max_primal_nodes = len(m)
                for n in m:
                    if isinstance(n, BasisNode):
                        self.node_id(n)
            else:
                spec, slots, params, _ = self.ctx.extract_formula(m)
                for n in slots:
                    if isinstance(n, BasisNode):
                        self.node_id(n)
                if len(slots) > self._max_nodes:
                    self._max_nodes = len(slots)
                if role == 'primal' and len(slots) > self._max_primal_nodes:
                    self._max_primal_nodes = len(slots)
                if len(params) > self._max_params:
                    self._max_params = len(params)
                self._formulas[spec.op] = spec
                if role == 'primal':
                    self._primal_ops.add(spec.op)
                elif role == 'diff':
                    self._diff_ops.add(spec.op)
                if spec.base_op and spec.base_op in self.ctx.formulas_by_op:
                    self._formulas[spec.base_op] = self.ctx.formulas_by_op[spec.base_op]

    def _register_sub_poly(self, poly: Polynomial, raw_expr: Expr | None = None) -> tuple[float, SubMeasure]:
        """Normalizes a pure stage-polynomial ``poly`` up to scalar scale and registers (or reuses) a ``SubMeasure``."""
        clean = {m: c for m, c in poly.items() if abs(c) > 1e-18}
        ordered_items = sorted(clean.items(), key=lambda mc: tuple(_factor_sort_key(n) for n in mc[0]))
        scale = ordered_items[0][1]
        inv_scale = 1.0 / scale
        unit_poly: Polynomial = {m: c * inv_scale for m, c in ordered_items}
        poly_key = tuple((m, round(unit_poly[m], 14)) for m, _ in ordered_items)
        found = self._sub_by_key.get(poly_key)
        if found is not None:
            return scale, found

        term_stage = _poly_stage(unit_poly)
        self._register_poly_slots(unit_poly)
        target_unit = mul(Const(inv_scale), raw_expr) if raw_expr is not None else _poly_to_expr(unit_poly)
        sub_id = f'S{len(self._sub_measures):06d}'
        sub = SubMeasure(sub_id, term_stage + 1, target_unit)
        was_synced = self._registered_subs == len(self._sub_measures)
        self._sub_measures.append((sub_id, term_stage, unit_poly))
        if was_synced:
            self._registered_subs += 1
        self._schema_cache = None
        self._sub_by_id[sub_id] = (term_stage, unit_poly)
        self._sub_by_key[poly_key] = sub
        return scale, sub

    def _expand_cached(self, root: Expr) -> Polynomial:
        found = self._clean_polys.get(root)
        if found is None:
            found = self._clean_polys[root] = expand(root, self._polys, self._constant)
        return found

    def _factor_expr(self, root: Expr | float) -> Expr:
        """Automatically factors multi-term additive basis polynomials (``len(poly) >= 2``) that feed non-polynomial
        operations (such as ``IRSwaption``'s swap annuity, float-leg PV, and 2D bilinear vol feeding ``black_formula``)
        into compact reusable ``SubMeasure``s."""
        root_e = lift(root)
        cached = self._factored_cache.get(root_e)
        if cached is not None:
            return cached

        orig_pure = self._orig_pure
        for n in postorder(root_e, orig_pure):
            if is_constant(n, self._constant) or isinstance(n, (Var, BasisNode, SubMeasure)):
                orig_pure[n] = True
            elif isinstance(n, Neg):
                orig_pure[n] = orig_pure[n.a]
            elif isinstance(n, (Add, Mul)):
                orig_pure[n] = orig_pure[n.a] and orig_pure[n.b]
            elif isinstance(n, Div):
                if not orig_pure[n.a]:
                    orig_pure[n] = False
                elif is_constant(n.b, self._constant):
                    orig_pure[n] = True
                elif orig_pure[n.b]:
                    b_poly = self._expand_cached(n.b)
                    if len(b_poly) == 1:
                        mb = next(iter(b_poly))
                        orig_pure[n] = all(
                            isinstance(f, BasisNode) and f.op == EXP_NEG_XT and f.offset == 0.0 for f in mb
                        )
                    else:
                        orig_pure[n] = False
                else:
                    orig_pure[n] = False
            elif isinstance(n, Exp):
                if is_constant(n.a, self._constant):
                    orig_pure[n] = True
                elif orig_pure[n.a]:
                    a_poly = self._expand_cached(n.a)
                    orig_pure[n] = bool(a_poly) and all(
                        len(m) == 0
                        or (len(m) == 1 and isinstance(m[0], BasisNode) and m[0].op == LERP and m[0].offset == 0.0)
                        for m in a_poly
                    )
                else:
                    orig_pure[n] = False
            else:
                orig_pure[n] = False

        # Fast path: pure polynomials (e.g. 1-curve / 2-curve swaps) need zero sub-measure staging
        if orig_pure[root_e]:
            self._factored_cache[root_e] = root_e
            return root_e

        nodes_order = postorder(root_e)
        parent_counts: dict[Expr, int] = {}
        for n in nodes_order:
            for ch in n.children():
                parent_counts[ch] = parent_counts.get(ch, 0) + 1

        def _maybe_wrap_poly_expr(cand: Expr) -> Expr:
            p = self._expand_cached(cand)
            if not _is_pure_poly(p):
                return cand
            if len(p) >= 2:
                sc, sub = self._register_sub_poly(p)
                return mul(Const(sc), sub)
            return _poly_to_expr(p)

        def _rebuild(node: Expr, new_children: list[Expr]) -> Expr:
            if not new_children:
                return node
            if all(nc is oc for nc, oc in zip(new_children, node.children())):
                return node
            if isinstance(node, Add):
                return add(new_children[0], new_children[1])
            if isinstance(node, Mul):
                return mul(new_children[0], new_children[1])
            if isinstance(node, Div):
                return Div(new_children[0], new_children[1])
            if isinstance(node, Neg):
                return neg(new_children[0])
            return type(node)(*new_children)

        rw: dict[Expr, Expr] = {}
        for n in nodes_order:
            kids = n.children()
            if not kids:
                rw[n] = n
                continue
            if orig_pure[n]:
                cur = _rebuild(n, [rw[c] for c in kids])
                # Shared multi-term polynomial feeding >= 2 branches (e.g. 2D vol in vol*sqrt(T) and 0.5*vol^2*T,
                # or swap annuity in both forward swap rate and Black discount factor)
                if parent_counts.get(n, 0) >= 2:
                    cur = _maybe_wrap_poly_expr(cur)
                rw[n] = cur
            else:
                wrapped_kids = [
                    _maybe_wrap_poly_expr(rw[c]) if orig_pure[c] else rw[c]
                    for c in kids
                ]
                rw[n] = _rebuild(n, wrapped_kids)

        out = rw[root_e]
        self._factored_cache[root_e] = out
        return out

    def _diff_basis_node_scalar(self, bn: BasisNode, wrt: Knot | Var) -> float:
        if bn.op == CONST:
            return 0.0
        if isinstance(wrt, Knot):
            if bn.series != wrt.series:
                return 0.0
            return (1.0 - bn.weight if wrt.index == bn.k0 else 0.0) + (
                bn.weight if wrt.index == bn.k1 else 0.0
            )
        if bn.series != wrt.name or bn.k0 != 0:
            return 0.0
        return 1.0

    def _diff_sub_measure(self, sub: SubMeasure, wrt: Knot | Var) -> Polynomial:
        key = (sub.sub_id, wrt)
        found = self._sub_diff_cache.get(key)
        if found is not None:
            return found
        _stage, unit_poly = self._sub_by_id[sub.sub_id]
        res = self._diff_poly_pure(unit_poly, wrt, 1.0)
        self._sub_diff_cache[key] = res
        return res

    def _diff_poly_pure(self, primal_poly: Polynomial, wrt: Knot | Var, scale: float) -> Polynomial:
        diff_poly: Polynomial = {}
        for m, c in primal_poly.items():
            if not m:
                continue
            if all(_is_slot_node(n) for n in m):
                for idx, sn in enumerate(m):
                    rest = m[:idx] + m[idx + 1:]
                    if isinstance(sn, BasisNode):
                        dx = self._diff_basis_node_scalar(sn, wrt)
                        if dx == 0.0:
                            continue
                        if sn.op == LERP:
                            dm = _combine_monomial(rest)
                            dc = c * dx * scale
                        elif sn.op == EXP_NEG_XT:
                            dm = _combine_monomial((*rest, sn._bare()))
                            dc = c * (-sn.t * dx) * scale
                        else:
                            bare = sn._bare()
                            dm = _combine_monomial((*rest, bare, bare))
                            dc = c * (-sn.t * dx) * scale
                        diff_poly[dm] = diff_poly.get(dm, 0.0) + dc
                    elif isinstance(sn, SubMeasure):
                        d_sub = self._diff_sub_measure(sn, wrt)
                        if not d_sub:
                            continue
                        if len(d_sub) == 1:
                            dm_sub, dc_sub = next(iter(d_sub.items()))
                            dm = _combine_monomial((*rest, *dm_sub))
                            diff_poly[dm] = diff_poly.get(dm, 0.0) + c * dc_sub * scale
                        else:
                            d_sc, d_sm = self._register_sub_poly(d_sub)
                            dm = _combine_monomial((*rest, d_sm))
                            diff_poly[dm] = diff_poly.get(dm, 0.0) + c * d_sc * scale
            else:
                f_spec, f_slots, f_params, f_expr = self.ctx.extract_formula(m)
                self._formulas[f_spec.op] = f_spec
                for idx, sn in enumerate(f_slots):
                    if isinstance(sn, BasisNode):
                        dx = self._diff_basis_node_scalar(sn, wrt)
                        if dx == 0.0:
                            continue
                        node_mult = dx if sn.op == LERP else (-sn.t * dx)
                        d_spec = self.ctx.register_formula_diff(f_spec, idx, sn.op)
                        self._formulas[d_spec.op] = d_spec
                        pre = _PreextractedFormula(d_spec, f_slots, f_params, f_expr, idx, sn.op)
                        for s_in in f_slots:
                            if isinstance(s_in, BasisNode):
                                self.node_id(s_in)
                        if len(f_slots) > self._max_nodes:
                            self._max_nodes = len(f_slots)
                        if len(f_params) > self._max_params:
                            self._max_params = len(f_params)
                        dm = (pre,)
                        diff_poly[dm] = diff_poly.get(dm, 0.0) + c * node_mult * scale
                    elif isinstance(sn, SubMeasure):
                        d_sub = self._diff_sub_measure(sn, wrt)
                        if not d_sub:
                            continue
                        if len(d_sub) == 1 and () in d_sub:
                            dc_const = d_sub[()]
                            d_spec = self.ctx.register_formula_diff(f_spec, idx, LERP)
                            self._formulas[d_spec.op] = d_spec
                            pre = _PreextractedFormula(d_spec, f_slots, f_params, f_expr, idx, LERP)
                            ext_slots = f_slots
                            total_mult = dc_const
                        else:
                            if len(d_sub) == 1:
                                dm_sub, dc_sub = next(iter(d_sub.items()))
                                if len(dm_sub) == 1 and _is_slot_node(dm_sub[0]):
                                    chain_slot: SlotNode = dm_sub[0]  # type: ignore[assignment]
                                    total_mult = dc_sub
                                else:
                                    total_mult, chain_slot = self._register_sub_poly(d_sub)
                            else:
                                total_mult, chain_slot = self._register_sub_poly(d_sub)
                            d_spec = self.ctx.register_formula_diff(f_spec, idx, 'SUB_CHAIN')
                            self._formulas[d_spec.op] = d_spec
                            ext_slots = (*f_slots, chain_slot)
                            pre = _PreextractedFormula(d_spec, ext_slots, f_params, f_expr, idx, 'SUB_CHAIN')
                        for s_in in ext_slots:
                            if isinstance(s_in, BasisNode):
                                self.node_id(s_in)
                        if len(ext_slots) > self._max_nodes:
                            self._max_nodes = len(ext_slots)
                        if len(f_params) > self._max_params:
                            self._max_params = len(f_params)
                        dm = (pre,)
                        diff_poly[dm] = diff_poly.get(dm, 0.0) + c * total_mult * scale

        return {m: c for m, c in diff_poly.items() if abs(c) > 1e-18}

    def schema(self) -> BasisSchema:
        """The dynamic ``BasisSchema`` required by the measures and sub-measures added to this book."""
        cache_key = (len(self.measures), len(self._sub_measures))
        if self._schema_cache is not None and self._schema_cache[:2] == cache_key:
            return self._schema_cache[2]

        if self._registered_subs < len(self._sub_measures):
            for _sub_id, _st, s_poly in self._sub_measures[self._registered_subs:]:
                self._register_poly_slots(s_poly, role='sub')
            self._registered_subs = len(self._sub_measures)
        if self._registered_measures < len(self.measures):
            for _entity, _group, _measure, factor, poly in self.measures[self._registered_measures:]:
                self._register_poly_slots(poly, role='primal' if factor == '*' else 'diff')
            self._registered_measures = len(self.measures)

        max_stage = max((st + 1 for _sid, st, _p in self._sub_measures), default=0)
        num_nodes = max(3, self._max_nodes)
        primal_nodes = max(3, min(num_nodes, self._max_primal_nodes))
        formulas = tuple(sorted(self._formulas.values(), key=lambda f: f.op))
        p_ops = tuple(sorted(self._primal_ops)) if formulas else None
        d_ops = tuple(sorted(self._diff_ops)) if formulas else None

        if max_stage == 0:
            sch = BasisSchema(
                num_nodes=num_nodes,
                num_params=self._max_params,
                formulas=formulas,
                primal_nodes=primal_nodes,
                primal_ops=p_ops,
                diff_ops=d_ops,
            )
            self._schema_cache = (*cache_key, sch)
            self.ctx.schema = sch
            return sch

        # Compute per-stage active slot counts and slot kinds ('N', 'S', or 'B') to minimize SQL joins
        stage_polys: list[list[Polynomial]] = [[] for _ in range(max_stage + 1)]
        for _sid, st, s_poly in self._sub_measures:
            stage_polys[st].append(s_poly)
        for _e, _g, _m, _f, poly in self.measures:
            stage_polys[max_stage].append(poly)

        def _poly_slot_width(poly: Polynomial) -> int:
            w = 1
            for m in poly:
                if not m:
                    continue
                if all(_is_slot_node(n) for n in m):
                    w = max(w, len(m))
                else:
                    _, slots, _, _ = self.ctx.extract_formula(m)
                    w = max(w, len(slots) or 1)
            return w

        s_nodes: list[int] = []
        s_kinds: list[tuple[str, ...]] = []
        for st in range(max_stage + 1):
            max_k = 1
            has_n = [False] * num_nodes
            has_s = [False] * num_nodes
            for poly in stage_polys[st]:
                if not poly:
                    has_n[0] = True
                for m in poly:
                    if not m:
                        has_n[0] = True
                    elif all(_is_slot_node(n) for n in m):
                        max_k = max(max_k, len(m))
                        for idx, n in enumerate(m):
                            if isinstance(n, SubMeasure):
                                has_s[idx] = True
                            else:
                                has_n[idx] = True
                    else:
                        _, slots, _, _ = self.ctx.extract_formula(m)
                        if not slots:
                            has_n[0] = True
                        else:
                            max_k = max(max_k, len(slots))
                            for idx, n in enumerate(slots):
                                if isinstance(n, SubMeasure):
                                    has_s[idx] = True
                                else:
                                    has_n[idx] = True
            s_nodes.append(max_k)
            kinds = []
            for idx in range(max_k):
                if st == 0:
                    kinds.append('N')
                elif has_n[idx] and has_s[idx]:
                    kinds.append('B')
                elif has_s[idx]:
                    kinds.append('S')
                else:
                    kinds.append('N')
            s_kinds.append(tuple(kinds))

        primal_top_k = max(
            (_poly_slot_width(poly) for _e, _g, _m, f, poly in self.measures if f == '*'),
            default=1,
        )
        diff_top_k = max(
            (_poly_slot_width(poly) for _e, _g, _m, f, poly in self.measures if f != '*'),
            default=1,
        )
        p_stage_nodes = (*s_nodes[:-1], primal_top_k)
        d_stage_nodes = (*s_nodes[:-1], diff_top_k)

        sch = BasisSchema(
            num_nodes=num_nodes,
            num_params=self._max_params,
            formulas=formulas,
            max_stage=max_stage,
            stage_nodes=tuple(s_nodes),
            stage_slot_kinds=tuple(s_kinds),
            primal_stage_nodes=p_stage_nodes,
            diff_stage_nodes=d_stage_nodes,
            primal_nodes=primal_nodes,
            primal_ops=p_ops,
            diff_ops=d_ops,
        )
        self._schema_cache = (*cache_key, sch)
        self.ctx.schema = sch
        return sch

    def add(self, entity_id: str, group_id: str, measure_code: str, factor_id: str, value: Expr) -> Polynomial:
        """One measure of one entity (``factor_id``: what it is a derivative by, ``'*'`` for none), factored and expanded."""
        factored = self._factor_expr(value)
        poly = self._expand_cached(factored)
        self._register_poly_slots(poly, role='primal' if factor_id == '*' else 'diff')
        was_synced = self._registered_measures == len(self.measures)
        self.measures.append((entity_id, group_id, measure_code, factor_id, poly))
        if was_synced:
            self._registered_measures += 1
        self._schema_cache = None
        return poly

    def add_pure_diff(
        self,
        entity_id: str,
        group_id: str,
        measure_code: str,
        factor_id: str,
        value: Expr,
        wrt: Knot | Var,
        *,
        scale: float = 1.0,
    ) -> Polynomial:
        """Adds the derivative of ``value`` with respect to ``wrt`` (scaled by ``scale``) using the hierarchical
        chain rule: ``BasisNode`` polynomials differentiate in the basis, ``SubMeasure`` slots chain through their
        SQL-aggregated derivatives, and non-polynomial formulas ``compute_basis::cbsFormula_<op>`` differentiate in Pure
        via ``meta::legend::lite::diff``."""
        factored = self._factor_expr(value)
        primal_poly = self._expand_cached(factored)
        diff_poly = self._diff_poly_pure(primal_poly, wrt, scale)
        if diff_poly:
            self._register_poly_slots(diff_poly, role='diff')
            was_synced = self._registered_measures == len(self.measures)
            self.measures.append((entity_id, group_id, measure_code, factor_id, diff_poly))
            if was_synced:
                self._registered_measures += 1
            self._schema_cache = None
        return diff_poly

    @property
    def nodes(self) -> list[BasisNode]:
        return list(self._ids)

    @property
    def sub_measures(self) -> list[tuple[str, int, Polynomial]]:
        return list(self._sub_measures)

    def term_count(self) -> int:
        sub_terms = sum(len(poly) or 1 for _sid, _st, poly in self._sub_measures)
        top_terms = sum(len(poly) or 1 for *_x, poly in self.measures)
        return sub_terms + top_terms

    def _poly_to_specs(self, poly: Polynomial, unit: str) -> list[_TermSpec]:
        monomials = (
            sorted(poly.items(), key=lambda mc: tuple(_factor_sort_key(n) for n in mc[0]))
            or [((), 0.0)]
        )
        specs: list[_TermSpec] = []
        for seq, (m, c) in enumerate(monomials):
            if not m:
                specs.append(_TermSpec(seq, CONST, c, (unit,), (), 1.0))
            elif all(_is_slot_node(n) for n in m):
                nids = tuple(self.slot_id(n) for n in m)
                op = 'LINEAR' if len(nids) == 1 else 'PROD'
                specs.append(_TermSpec(seq, op, c, nids, (), 0.0))
            else:
                f_spec, f_slots, f_params, _ = self.ctx.extract_formula(m)
                nids = tuple(self.slot_id(n) for n in f_slots) or (unit,)
                specs.append(_TermSpec(seq, f_spec.op, c, nids, f_params, 0.0))
        return specs

    def _prepare_measures(self) -> tuple[BasisSchema, list[tuple[int, str, str, str, str, list[_TermSpec]]]]:
        unit = self.node_id(UNIT)
        sch = self.schema()
        prepared: list[tuple[int, str, str, str, str, list[_TermSpec]]] = []
        for sub_id, st, s_poly in self._sub_measures:
            prepared.append((st, sub_id, SUB_MEASURE_CODE, SUB_MEASURE_CODE, '*', self._poly_to_specs(s_poly, unit)))
        top_stage = sch.max_stage
        for entity, group, measure, factor, poly in self.measures:
            prepared.append((top_stage, entity, group, measure, factor, self._poly_to_specs(poly, unit)))
        return sch, prepared

    def market_rows(
        self,
        scenarios: Sequence[Scenario],
        jacobian: Sequence[JacobianEntry] = (),
    ) -> dict[str, list[dict]]:
        """The market-dependent tables' rows (``CBS_KNOT`` and ``CBS_JACOBIAN_ENTRY``), plain Python."""
        knots = [
            {
                'SCENARIO_ID': s.scenario_id,
                'SERIES_ID': series,
                'KNOT_INDEX': i,
                'KNOT_ID': kid,
                'KNOT_COORD': float(coord),
                'KNOT_VALUE': float(v),
                'UNIT': s.unit,
            }
            for s in scenarios
            for series, ks in s.series.items()
            for i, (kid, coord, v) in enumerate(ks)
        ]
        jac = [
            {
                'SCENARIO_ID': j.scenario_id,
                'FACTOR_ID': j.factor_id,
                'INPUT_ID': j.input_id,
                'JACOBIAN_WEIGHT': j.weight,
                'INPUT_COORD': j.input_coord,
            }
            for j in jacobian
        ]
        return {'CBS_KNOT': knots, 'CBS_JACOBIAN_ENTRY': jac}

    def market_tables(
        self,
        scenarios: Sequence[Scenario],
        jacobian: Sequence[JacobianEntry] = (),
    ) -> dict[str, Any]:
        """The market-dependent tables (``CBS_KNOT`` and ``CBS_JACOBIAN_ENTRY``) as pyarrow Tables."""
        require_datacube()
        import pyarrow as pa
        sch = self.schema()
        rows = self.market_rows(scenarios, jacobian)
        return {name: pa.Table.from_pylist(rows[name], schema=_schema(name, sch)) for name in rows}

    def static_rows(self, scenario_ids: Sequence[str] = ('BASE',)) -> dict[str, list[dict]]:
        """The portfolio-structure tables' rows (``CBS_SHARED_NODE`` and ``CBS_BASIS_TERM``) for ``scenario_ids``,
        independent of market knot values (zero host math evaluation, ``TENET_CHARTER.md`` C2.1)."""
        sch, prepared = self._prepare_measures()
        tags = sch.slot_tags
        n_params = sch.num_params
        has_stage = sch.max_stage > 0
        nodes, terms = [], []
        for sid in scenario_ids:
            for n, nid in self._ids.items():
                nodes.append({
                    'SCENARIO_ID': sid,
                    'NODE_ID': nid,
                    'SERIES_ID': n.series,
                    'OP_CODE': n.op,
                    'KNOT_0': n.k0,
                    'KNOT_1': n.k1,
                    'WEIGHT_1': n.weight,
                    'PARAM_T': n.t,
                    'CONST_OFFSET': n.offset,
                })
            for st, entity, group, measure, factor, specs in prepared:
                for sp in specs:
                    row: dict[str, Any] = {
                        'SCENARIO_ID': sid,
                        'ENTITY_ID': entity,
                        'MEASURE_CODE': measure,
                        'FACTOR_ID': factor,
                        'TERM_SEQ': sp.seq,
                        'GROUP_ID': group,
                    }
                    if has_stage:
                        row['TERM_STAGE'] = st
                    row['TERM_OP'] = sp.op
                    row['COEFF'] = sp.coeff
                    for idx, tag in enumerate(tags):
                        row[f'NODE_{tag}_ID'] = sp.node_ids[idx] if idx < len(sp.node_ids) else None
                    for j in range(1, n_params + 1):
                        row[f'PARAM_{j}'] = sp.params[j - 1] if j - 1 < len(sp.params) else 0.0
                    row['SHIFT_CONST'] = sp.shift
                    row['TERM_VALUE'] = 0.0
                    terms.append(row)
        return {'CBS_SHARED_NODE': nodes, 'CBS_BASIS_TERM': terms}

    def static_tables(self, scenario_ids: Sequence[str] = ('BASE',)) -> dict[str, Any]:
        """The portfolio-structure tables (``CBS_SHARED_NODE`` and ``CBS_BASIS_TERM``) as pyarrow Tables."""
        require_datacube()
        import pyarrow as pa
        sch, prepared = self._prepare_measures()
        tags = sch.slot_tags
        n_params = sch.num_params
        has_stage = sch.max_stage > 0

        node_ids, series_ids, op_codes, k0s, k1s, weights, param_ts, offsets = [], [], [], [], [], [], [], []
        for n, nid in self._ids.items():
            node_ids.append(nid)
            series_ids.append(n.series)
            op_codes.append(n.op)
            k0s.append(n.k0)
            k1s.append(n.k1)
            weights.append(n.weight)
            param_ts.append(n.t)
            offsets.append(n.offset)
        n_nodes = len(node_ids)

        stages, entities, measures, factors, seqs, groups, ops, coeffs, shifts = [], [], [], [], [], [], [], [], []
        slot_cols: list[list[str | None]] = [[] for _ in tags]
        param_cols: list[list[float]] = [[] for _ in range(n_params)]
        for st, entity, group, measure, factor, specs in prepared:
            for sp in specs:
                stages.append(st)
                entities.append(entity)
                measures.append(measure)
                factors.append(factor)
                seqs.append(sp.seq)
                groups.append(group)
                ops.append(sp.op)
                coeffs.append(sp.coeff)
                for idx in range(len(tags)):
                    slot_cols[idx].append(sp.node_ids[idx] if idx < len(sp.node_ids) else None)
                for j in range(n_params):
                    param_cols[j].append(sp.params[j] if j < len(sp.params) else 0.0)
                shifts.append(sp.shift)
        n_terms = len(entities)
        term_zeros = [0.0] * n_terms

        node_schema = _schema('CBS_SHARED_NODE', sch)
        raw_term_schema = _schema('CBS_BASIS_TERM', sch)
        term_schema = raw_term_schema.with_metadata({
            **(raw_term_schema.metadata or {}),
            SCHEMA_META_KEY: sch.to_json(),
        })
        node_tables = [
            pa.Table.from_pydict(
                {
                    'SCENARIO_ID': [sid] * n_nodes,
                    'NODE_ID': node_ids,
                    'SERIES_ID': series_ids,
                    'OP_CODE': op_codes,
                    'KNOT_0': k0s,
                    'KNOT_1': k1s,
                    'WEIGHT_1': weights,
                    'PARAM_T': param_ts,
                    'CONST_OFFSET': offsets,
                },
                schema=node_schema,
            )
            for sid in scenario_ids
        ]
        term_tables = []
        for sid in scenario_ids:
            col_dict: dict[str, Any] = {
                'SCENARIO_ID': [sid] * n_terms,
                'ENTITY_ID': entities,
                'MEASURE_CODE': measures,
                'FACTOR_ID': factors,
                'TERM_SEQ': seqs,
                'GROUP_ID': groups,
            }
            if has_stage:
                col_dict['TERM_STAGE'] = stages
            col_dict['TERM_OP'] = ops
            col_dict['COEFF'] = coeffs
            for idx, tag in enumerate(tags):
                col_dict[f'NODE_{tag}_ID'] = slot_cols[idx]
            for j in range(n_params):
                col_dict[f'PARAM_{j + 1}'] = param_cols[j]
            col_dict['SHIFT_CONST'] = shifts
            col_dict['TERM_VALUE'] = term_zeros
            term_tables.append(pa.Table.from_pydict(col_dict, schema=term_schema))
        return {
            'CBS_SHARED_NODE': (
                pa.concat_tables(node_tables) if node_tables else pa.Table.from_pylist([], schema=node_schema)
            ),
            'CBS_BASIS_TERM': (
                pa.concat_tables(term_tables) if term_tables else pa.Table.from_pylist([], schema=term_schema)
            ),
        }

    def rows(self, scenarios: Sequence[Scenario], jacobian: Sequence[JacobianEntry] = ()) -> dict[str, list[dict]]:
        """The four tables' rows, plain Python, carrying inputs and basis topology without host math evaluation."""
        mkt = self.market_rows(scenarios, jacobian)
        stc = self.static_rows([s.scenario_id for s in scenarios])
        return {
            'CBS_KNOT': mkt['CBS_KNOT'],
            'CBS_SHARED_NODE': stc['CBS_SHARED_NODE'],
            'CBS_BASIS_TERM': stc['CBS_BASIS_TERM'],
            'CBS_JACOBIAN_ENTRY': mkt['CBS_JACOBIAN_ENTRY'],
        }

    def tables(self, scenarios: Sequence[Scenario], jacobian: Sequence[JacobianEntry] = ()) -> dict[str, Any]:
        """The four tables as pyarrow Tables, typed as the dynamic ``compute_basis::Store`` declares them."""
        mkt = self.market_tables(scenarios, jacobian)
        stc = self.static_tables([s.scenario_id for s in scenarios])
        return {
            'CBS_KNOT': mkt['CBS_KNOT'],
            'CBS_SHARED_NODE': stc['CBS_SHARED_NODE'],
            'CBS_BASIS_TERM': stc['CBS_BASIS_TERM'],
            'CBS_JACOBIAN_ENTRY': mkt['CBS_JACOBIAN_ENTRY'],
        }

    def frames(
        self,
        scenarios: Sequence[Scenario],
        jacobian: Sequence[JacobianEntry] = (),
        frames: Any = None,
        *,
        materialize_stored: bool = True,
    ) -> Any:
        """Registers the four ``compute_basis`` tables into a ``Frames`` instance (or a ``ll.Page``'s session frames, or
        creates a new ``Frames`` when ``frames`` is None) so ``Frames.model_for`` and ``ll.Page`` / ``page.flow()`` can
        query them directly. When ``materialize_stored=True``, ``TERM_VALUE`` is populated in DuckDB SQL via Pure."""
        with ComputeBasis(self) as cb:
            return cb.frames(self.tables(scenarios, jacobian), frames, materialize_stored=materialize_stored)


def unwrap_frames(frames_or_page: Any) -> Any:
    """Resolves either an ``ll.Frames`` or an ``ll.Page`` (via ``page._session.frames``) to its underlying ``Frames``."""
    if frames_or_page is None:
        return None
    if hasattr(frames_or_page, '_session') and hasattr(frames_or_page._session, 'frames'):
        return frames_or_page._session.frames
    return frames_or_page


# ---------------------------------------------------------------------------------------------- dynamic Pure synthesis


SchemaSource = Union['BasisBook', 'BasisSchema', 'BasisContext', 'ComputeBasis', None]


def _resolve_schema(book_or_schema: SchemaSource = None) -> BasisSchema:
    if isinstance(book_or_schema, BasisSchema):
        return book_or_schema
    if isinstance(book_or_schema, BasisBook):
        return book_or_schema.schema()
    if isinstance(book_or_schema, BasisContext):
        return book_or_schema.schema
    if isinstance(book_or_schema, ComputeBasis):
        return book_or_schema.schema
    return DEFAULT_SCHEMA


@functools.cache
def render_pure_model(schema: BasisSchema = DEFAULT_SCHEMA) -> str:
    """Synthesizes the complete ``compute_basis`` Pure model (``###Pure``, ``###Relational``, ``###Mapping``) for
    ``schema``."""
    tags = schema.slot_tags
    n_params = schema.num_params
    has_stage = schema.max_stage > 0

    # 1. Dynamic formula functions
    formula_funcs: list[str] = []
    for f in schema.formulas:
        f_args = [f'val{_slot_tag(i)}: Float[1]' for i in range(f.num_nodes)] + [
            f'param{j}: Float[1]' for j in range(1, f.num_params + 1)
        ]
        if not f_args:
            f_args = ['valA: Float[1]']
        formula_funcs.append(
            f"function compute_basis::cbsFormula_{f.op}({', '.join(f_args)}): Float[1]\n{{\n{f.pure_body}\n}}"
        )
    formula_block = ('\n\n'.join(formula_funcs) + '\n\n') if formula_funcs else ''

    # 2. cbsEvalPolyTerm, cbsEvalPrimalTerm, cbsEvalDiffTerm, and cbsEvalTerm signatures and bodies
    poly_term_args = (
        ['termOp: String[1]', 'coeff: Float[1]']
        + [f'val{t}: Float[1]' for t in tags]
        + ['shiftConst: Float[1]']
    )
    eval_term_args = (
        ['termOp: String[1]', 'coeff: Float[1]']
        + [f'val{t}: Float[1]' for t in tags]
        + [f'param{j}: Float[1]' for j in range(1, n_params + 1)]
        + ['shiftConst: Float[1]']
    )
    prod_expr = f'$val{tags[-1]}'
    for t in reversed(tags[:-1]):
        prod_expr = f'($val{t} * {prod_expr})'

    def _formula_branch(formulas: Sequence[FormulaSpec]) -> str:
        fb = '$coeff * $shiftConst'
        for f in reversed(formulas):
            call_args = [f'$val{_slot_tag(i)}' for i in range(f.num_nodes)] + [
                f'$param{j}' for j in range(1, f.num_params + 1)
            ]
            if not call_args:
                call_args = ['$valA']
            call = f"compute_basis::cbsFormula_{f.op}({', '.join(call_args)})"
            fb = f"if($termOp == '{f.op}',\n               | $coeff * {call},\n               | {fb}\n            )"
        return fb

    fallback = _formula_branch(schema.formulas)
    split_eval_funcs: list[str] = []
    if schema.primal_formulas:
        p_fb = _formula_branch(schema.primal_formulas)
        split_eval_funcs.append(
            f"""function compute_basis::cbsEvalPrimalTerm({', '.join(eval_term_args)}): Float[1]
{{
   if($termOp == 'LINEAR',
      | $coeff * ($valA + $shiftConst),
      | if($termOp == 'PROD',
         | $coeff * {prod_expr},
         | {p_fb}
      )
   )
}}"""
        )
    if schema.diff_formulas:
        d_fb = _formula_branch(schema.diff_formulas)
        split_eval_funcs.append(
            f"""function compute_basis::cbsEvalDiffTerm({', '.join(eval_term_args)}): Float[1]
{{
   if($termOp == 'LINEAR',
      | $coeff * ($valA + $shiftConst),
      | if($termOp == 'PROD',
         | $coeff * {prod_expr},
         | {d_fb}
      )
   )
}}"""
        )
    split_eval_block = ('\n\n'.join(split_eval_funcs) + '\n\n') if split_eval_funcs else ''

    # 3. CbsBasisTerm class properties and methods
    stage_prop = '\n   termStage: Integer[1];' if has_stage else ''
    opt_node_id_props = '\n'.join(f'   node{t}Id: String[0..1];' for t in tags[1:])
    param_props = '\n'.join(f'   param{j}: Float[1];' for j in range(1, n_params + 1))
    param_props_block = ('\n' + param_props) if param_props else ''
    opt_node_assocs = '\n'.join(f'   node{t}: compute_basis::CbsSharedNode[0..1];' for t in tags[1:])

    eval_live_args = (
        ['$this.termOp', '$this.coeff', '$this.nodeA.evaluatedValue()->toOne()']
        + [f'if($this.node{t}->isEmpty(), |1.0, |$this.node{t}.evaluatedValue()->toOne())' for t in tags[1:]]
        + [f'$this.param{j}' for j in range(1, n_params + 1)]
        + ['$this.shiftConst']
    )

    # 4. Relational table CBS_BASIS_TERM and Joins
    stage_col = '\n      TERM_STAGE SMALLINT NOT NULL,' if has_stage else ''
    opt_node_cols = '\n'.join(f'      NODE_{t}_ID VARCHAR(48),' for t in tags[1:])
    param_cols = '\n'.join(f'      PARAM_{j} DOUBLE NOT NULL,' for j in range(1, n_params + 1))
    param_cols_block = ('\n' + param_cols) if param_cols else ''
    term_node_joins = '\n'.join(
        f'   Join Cbs_TermNode{t}(CBS_BASIS_TERM.SCENARIO_ID = CBS_SHARED_NODE.SCENARIO_ID '
        f'and CBS_BASIS_TERM.NODE_{t}_ID = CBS_SHARED_NODE.NODE_ID)'
        for t in tags
    )
    view_filter = '\n      ~filter CbsTopStageTermRows' if has_stage else ''
    exp_main_filter = '\n         ~filter [compute_basis::Store]CbsTopStageTermRows' if has_stage else ''
    top_stage_filter_def = (
        f'\n\n   Filter CbsTopStageTermRows(CBS_BASIS_TERM.TERM_STAGE = {schema.max_stage})' if has_stage else ''
    )

    # 5. Mapping for CbsBasisTerm
    map_stage = '\n      termStage: [compute_basis::Store]CBS_BASIS_TERM.TERM_STAGE,' if has_stage else ''
    map_node_ids = '\n'.join(
        f'      node{t}Id: [compute_basis::Store]CBS_BASIS_TERM.NODE_{t}_ID,' for t in tags
    )
    map_params = '\n'.join(
        f'      param{j}: [compute_basis::Store]CBS_BASIS_TERM.PARAM_{j},' for j in range(1, n_params + 1)
    )
    map_params_block = ('\n' + map_params) if map_params else ''
    map_node_assocs = '\n'.join(
        f'      node{t}[cbsSharedNode]: [compute_basis::Store]@Cbs_TermNode{t},' for t in tags
    )
    map_node_assocs = map_node_assocs.rstrip(',')

    return f"""###Pure
function compute_basis::cbsLerp(x0: Float[1], x1: Float[1], w1: Float[1]): Float[1]
{{
   $x0 + ($w1 * ($x1 - $x0))
}}

function compute_basis::cbsEvalNode(opCode: String[1], x0: Float[1], x1: Float[1], w1: Float[1], paramT: Float[1], constOffset: Float[1]): Float[1]
{{
   let x = compute_basis::cbsLerp($x0, $x1, $w1);
   if($opCode == 'EXP_NEG_XT',
      | exp(-1.0 * $x * $paramT) + $constOffset,
      | if($opCode == 'LERP',
         | $x + $constOffset,
         | if($opCode == 'RECIP_1PXT',
            | (1.0 / (1.0 + ($x * $paramT))) + $constOffset,
            | $constOffset
         )
      )
   );
}}

{formula_block}function compute_basis::cbsEvalPolyTerm({', '.join(poly_term_args)}): Float[1]
{{
   if($termOp == 'LINEAR',
      | $coeff * ($valA + $shiftConst),
      | if($termOp == 'PROD',
         | $coeff * {prod_expr},
         | $coeff * $shiftConst
      )
   )
}}

{split_eval_block}function compute_basis::cbsEvalTerm({', '.join(eval_term_args)}): Float[1]
{{
   if($termOp == 'LINEAR',
      | $coeff * ($valA + $shiftConst),
      | if($termOp == 'PROD',
         | $coeff * {prod_expr},
         | {fallback}
      )
   )
}}

function compute_basis::cbsChainRuleTerm(byFactor: Float[1], jacobianWeight: Float[1]): Float[1]
{{
   $byFactor * $jacobianWeight
}}

Class compute_basis::CbsKnot
{{
   scenarioId: String[1];
   seriesId: String[1];
   knotIndex: Integer[1];
   knotId: String[1];
   knotCoord: Float[1];
   knotValue: Float[1];
   unit: String[0..1];
}}

Class compute_basis::CbsSharedNode
[
   weightInUnitInterval: (($this.weight1 >= 0.0) && ($this.weight1 <= 1.0))
]
{{
   scenarioId: String[1];
   nodeId: String[1];
   seriesId: String[1];
   opCode: String[1];
   knot0Index: Integer[1];
   knot1Index: Integer[1];
   weight1: Float[1];
   paramT: Float[1];
   constOffset: Float[1];

   knot0: compute_basis::CbsKnot[0..1];
   knot1: compute_basis::CbsKnot[0..1];

   evaluatedValue()
   {{
      compute_basis::cbsEvalNode(
         $this.opCode,
         $this.knot0.knotValue->toOne(),
         $this.knot1.knotValue->toOne(),
         $this.weight1,
         $this.paramT,
         $this.constOffset
      )
   }} : Float[1];
}}

Class compute_basis::CbsBasisTerm
{{
   scenarioId: String[1];
   entityId: String[1];
   measureCode: String[1];
   factorId: String[1];
   termSeq: Integer[1];
   groupId: String[1];{stage_prop}
   termOp: String[1];
   coeff: Float[1];
   nodeAId: String[1];
{opt_node_id_props}{param_props_block}
   shiftConst: Float[1];
   termValue: Float[1];

   nodeA: compute_basis::CbsSharedNode[0..1];
{opt_node_assocs}

   evaluatedTerm()
   {{
      compute_basis::cbsEvalTerm(
         {', '.join(eval_live_args)}
      )
   }} : Float[1];
}}

Class compute_basis::CbsJacobianEntry
{{
   scenarioId: String[1];
   factorId: String[1];
   inputId: String[1];
   jacobianWeight: Float[1];
   inputCoord: Float[1];
}}

Class compute_basis::CbsMeasureExposure
{{
   scenarioId: String[1];
   groupId: String[1];
   entityId: String[1];
   measureCode: String[1];
   factorId: String[1];
   termValue: Float[1];
}}

Class compute_basis::CbsEntityMeasure
{{
   scenarioId: String[1];
   groupId: String[1];
   entityId: String[1];
   measureCode: String[1];
   factorId: String[1];
   totalValue: Float[1];
   termCount: Integer[1];
}}

###Relational
Database compute_basis::Store
(
   Table CBS_KNOT
   (
      SCENARIO_ID VARCHAR(32) PRIMARY KEY,
      SERIES_ID VARCHAR(32) PRIMARY KEY,
      KNOT_INDEX SMALLINT PRIMARY KEY,
      KNOT_ID VARCHAR(48) NOT NULL,
      KNOT_COORD DOUBLE NOT NULL,
      KNOT_VALUE DOUBLE NOT NULL,
      UNIT VARCHAR(16)
   )

   Table CBS_SHARED_NODE
   (
      SCENARIO_ID VARCHAR(32) PRIMARY KEY,
      NODE_ID VARCHAR(48) PRIMARY KEY,
      SERIES_ID VARCHAR(32) NOT NULL,
      OP_CODE VARCHAR(16) NOT NULL,
      KNOT_0 SMALLINT NOT NULL,
      KNOT_1 SMALLINT NOT NULL,
      WEIGHT_1 DOUBLE NOT NULL,
      PARAM_T DOUBLE NOT NULL,
      CONST_OFFSET DOUBLE NOT NULL
   )

   Table CBS_BASIS_TERM
   (
      SCENARIO_ID VARCHAR(32) PRIMARY KEY,
      ENTITY_ID VARCHAR(48) PRIMARY KEY,
      MEASURE_CODE VARCHAR(32) PRIMARY KEY,
      FACTOR_ID VARCHAR(48) PRIMARY KEY,
      TERM_SEQ SMALLINT PRIMARY KEY,
      GROUP_ID VARCHAR(32) NOT NULL,{stage_col}
      TERM_OP VARCHAR(16) NOT NULL,
      COEFF DOUBLE NOT NULL,
      NODE_A_ID VARCHAR(48) NOT NULL,
{opt_node_cols}{param_cols_block}
      SHIFT_CONST DOUBLE NOT NULL,
      TERM_VALUE DOUBLE NOT NULL
   )

   Table CBS_JACOBIAN_ENTRY
   (
      SCENARIO_ID VARCHAR(32) PRIMARY KEY,
      FACTOR_ID VARCHAR(48) PRIMARY KEY,
      INPUT_ID VARCHAR(48) PRIMARY KEY,
      JACOBIAN_WEIGHT DOUBLE NOT NULL,
      INPUT_COORD DOUBLE NOT NULL
   )

   View CBS_ENTITY_MEASURE
   ({view_filter}
      ~groupBy
      (
         CBS_BASIS_TERM.SCENARIO_ID,
         CBS_BASIS_TERM.GROUP_ID,
         CBS_BASIS_TERM.ENTITY_ID,
         CBS_BASIS_TERM.MEASURE_CODE,
         CBS_BASIS_TERM.FACTOR_ID
      )
      SCENARIO_ID: CBS_BASIS_TERM.SCENARIO_ID PRIMARY KEY,
      GROUP_ID: CBS_BASIS_TERM.GROUP_ID PRIMARY KEY,
      ENTITY_ID: CBS_BASIS_TERM.ENTITY_ID PRIMARY KEY,
      MEASURE_CODE: CBS_BASIS_TERM.MEASURE_CODE PRIMARY KEY,
      FACTOR_ID: CBS_BASIS_TERM.FACTOR_ID PRIMARY KEY,
      TOTAL_VALUE: sum(CBS_BASIS_TERM.TERM_VALUE),
      TERM_COUNT: count(CBS_BASIS_TERM.TERM_SEQ)
   )

   Join Cbs_NodeKnot0(CBS_SHARED_NODE.SCENARIO_ID = CBS_KNOT.SCENARIO_ID and CBS_SHARED_NODE.SERIES_ID = CBS_KNOT.SERIES_ID and CBS_SHARED_NODE.KNOT_0 = CBS_KNOT.KNOT_INDEX)
   Join Cbs_NodeKnot1(CBS_SHARED_NODE.SCENARIO_ID = CBS_KNOT.SCENARIO_ID and CBS_SHARED_NODE.SERIES_ID = CBS_KNOT.SERIES_ID and CBS_SHARED_NODE.KNOT_1 = CBS_KNOT.KNOT_INDEX)
{term_node_joins}{top_stage_filter_def}
)

###Mapping
Mapping compute_basis::Mapping
(
   compute_basis::CbsKnot[cbsKnot]: Relational
   {{
      ~primaryKey
      (
         [compute_basis::Store]CBS_KNOT.SCENARIO_ID,
         [compute_basis::Store]CBS_KNOT.SERIES_ID,
         [compute_basis::Store]CBS_KNOT.KNOT_INDEX
      )
      ~mainTable [compute_basis::Store]CBS_KNOT
      scenarioId: [compute_basis::Store]CBS_KNOT.SCENARIO_ID,
      seriesId: [compute_basis::Store]CBS_KNOT.SERIES_ID,
      knotIndex: [compute_basis::Store]CBS_KNOT.KNOT_INDEX,
      knotId: [compute_basis::Store]CBS_KNOT.KNOT_ID,
      knotCoord: [compute_basis::Store]CBS_KNOT.KNOT_COORD,
      knotValue: [compute_basis::Store]CBS_KNOT.KNOT_VALUE,
      unit: [compute_basis::Store]CBS_KNOT.UNIT
   }}

   compute_basis::CbsSharedNode[cbsSharedNode]: Relational
   {{
      ~primaryKey
      (
         [compute_basis::Store]CBS_SHARED_NODE.SCENARIO_ID,
         [compute_basis::Store]CBS_SHARED_NODE.NODE_ID
      )
      ~mainTable [compute_basis::Store]CBS_SHARED_NODE
      scenarioId: [compute_basis::Store]CBS_SHARED_NODE.SCENARIO_ID,
      nodeId: [compute_basis::Store]CBS_SHARED_NODE.NODE_ID,
      seriesId: [compute_basis::Store]CBS_SHARED_NODE.SERIES_ID,
      opCode: [compute_basis::Store]CBS_SHARED_NODE.OP_CODE,
      knot0Index: [compute_basis::Store]CBS_SHARED_NODE.KNOT_0,
      knot1Index: [compute_basis::Store]CBS_SHARED_NODE.KNOT_1,
      weight1: [compute_basis::Store]CBS_SHARED_NODE.WEIGHT_1,
      paramT: [compute_basis::Store]CBS_SHARED_NODE.PARAM_T,
      constOffset: [compute_basis::Store]CBS_SHARED_NODE.CONST_OFFSET,
      knot0[cbsKnot]: [compute_basis::Store]@Cbs_NodeKnot0,
      knot1[cbsKnot]: [compute_basis::Store]@Cbs_NodeKnot1
   }}

   compute_basis::CbsBasisTerm[cbsBasisTerm]: Relational
   {{
      ~primaryKey
      (
         [compute_basis::Store]CBS_BASIS_TERM.SCENARIO_ID,
         [compute_basis::Store]CBS_BASIS_TERM.ENTITY_ID,
         [compute_basis::Store]CBS_BASIS_TERM.MEASURE_CODE,
         [compute_basis::Store]CBS_BASIS_TERM.FACTOR_ID,
         [compute_basis::Store]CBS_BASIS_TERM.TERM_SEQ
      )
      ~mainTable [compute_basis::Store]CBS_BASIS_TERM
      scenarioId: [compute_basis::Store]CBS_BASIS_TERM.SCENARIO_ID,
      entityId: [compute_basis::Store]CBS_BASIS_TERM.ENTITY_ID,
      measureCode: [compute_basis::Store]CBS_BASIS_TERM.MEASURE_CODE,
      factorId: [compute_basis::Store]CBS_BASIS_TERM.FACTOR_ID,
      termSeq: [compute_basis::Store]CBS_BASIS_TERM.TERM_SEQ,
      groupId: [compute_basis::Store]CBS_BASIS_TERM.GROUP_ID,{map_stage}
      termOp: [compute_basis::Store]CBS_BASIS_TERM.TERM_OP,
      coeff: [compute_basis::Store]CBS_BASIS_TERM.COEFF,
{map_node_ids}{map_params_block}
      shiftConst: [compute_basis::Store]CBS_BASIS_TERM.SHIFT_CONST,
      termValue: [compute_basis::Store]CBS_BASIS_TERM.TERM_VALUE,
{map_node_assocs}
   }}

   compute_basis::CbsJacobianEntry[cbsJacobianEntry]: Relational
   {{
      ~primaryKey
      (
         [compute_basis::Store]CBS_JACOBIAN_ENTRY.SCENARIO_ID,
         [compute_basis::Store]CBS_JACOBIAN_ENTRY.FACTOR_ID,
         [compute_basis::Store]CBS_JACOBIAN_ENTRY.INPUT_ID
      )
      ~mainTable [compute_basis::Store]CBS_JACOBIAN_ENTRY
      scenarioId: [compute_basis::Store]CBS_JACOBIAN_ENTRY.SCENARIO_ID,
      factorId: [compute_basis::Store]CBS_JACOBIAN_ENTRY.FACTOR_ID,
      inputId: [compute_basis::Store]CBS_JACOBIAN_ENTRY.INPUT_ID,
      jacobianWeight: [compute_basis::Store]CBS_JACOBIAN_ENTRY.JACOBIAN_WEIGHT,
      inputCoord: [compute_basis::Store]CBS_JACOBIAN_ENTRY.INPUT_COORD
   }}

   compute_basis::CbsEntityMeasure[cbsEntityMeasure]: Relational
   {{
      ~primaryKey
      (
         [compute_basis::Store]CBS_ENTITY_MEASURE.SCENARIO_ID,
         [compute_basis::Store]CBS_ENTITY_MEASURE.GROUP_ID,
         [compute_basis::Store]CBS_ENTITY_MEASURE.ENTITY_ID,
         [compute_basis::Store]CBS_ENTITY_MEASURE.MEASURE_CODE,
         [compute_basis::Store]CBS_ENTITY_MEASURE.FACTOR_ID
      )
      ~mainTable [compute_basis::Store]CBS_ENTITY_MEASURE
      scenarioId: [compute_basis::Store]CBS_ENTITY_MEASURE.SCENARIO_ID,
      groupId: [compute_basis::Store]CBS_ENTITY_MEASURE.GROUP_ID,
      entityId: [compute_basis::Store]CBS_ENTITY_MEASURE.ENTITY_ID,
      measureCode: [compute_basis::Store]CBS_ENTITY_MEASURE.MEASURE_CODE,
      factorId: [compute_basis::Store]CBS_ENTITY_MEASURE.FACTOR_ID,
      totalValue: [compute_basis::Store]CBS_ENTITY_MEASURE.TOTAL_VALUE,
      termCount: [compute_basis::Store]CBS_ENTITY_MEASURE.TERM_COUNT
   }}

   compute_basis::CbsMeasureExposure[cbsMeasureExposure]: AggregationAware
   {{
      Views:
      [
         (
            ~modelOperation:
            {{
               ~canAggregate true,
               ~groupByFunctions
               (
                  $this.scenarioId,
                  $this.groupId,
                  $this.entityId,
                  $this.measureCode,
                  $this.factorId
               ),
               ~aggregateValues
               (
                  ( ~mapFn: $this.termValue, ~aggregateFn: $mapped->sum() )
               )
            }},
            ~aggregateMapping: Relational
            {{
               ~primaryKey
               (
                  [compute_basis::Store]CBS_ENTITY_MEASURE.SCENARIO_ID,
                  [compute_basis::Store]CBS_ENTITY_MEASURE.GROUP_ID,
                  [compute_basis::Store]CBS_ENTITY_MEASURE.ENTITY_ID,
                  [compute_basis::Store]CBS_ENTITY_MEASURE.MEASURE_CODE,
                  [compute_basis::Store]CBS_ENTITY_MEASURE.FACTOR_ID
               )
               ~mainTable [compute_basis::Store]CBS_ENTITY_MEASURE
               scenarioId: [compute_basis::Store]CBS_ENTITY_MEASURE.SCENARIO_ID,
               groupId: [compute_basis::Store]CBS_ENTITY_MEASURE.GROUP_ID,
               entityId: [compute_basis::Store]CBS_ENTITY_MEASURE.ENTITY_ID,
               measureCode: [compute_basis::Store]CBS_ENTITY_MEASURE.MEASURE_CODE,
               factorId: [compute_basis::Store]CBS_ENTITY_MEASURE.FACTOR_ID,
               termValue: [compute_basis::Store]CBS_ENTITY_MEASURE.TOTAL_VALUE
            }}
         )
      ],
      ~mainMapping: Relational
      {{{exp_main_filter}
         ~primaryKey
         (
            [compute_basis::Store]CBS_BASIS_TERM.SCENARIO_ID,
            [compute_basis::Store]CBS_BASIS_TERM.ENTITY_ID,
            [compute_basis::Store]CBS_BASIS_TERM.MEASURE_CODE,
            [compute_basis::Store]CBS_BASIS_TERM.FACTOR_ID,
            [compute_basis::Store]CBS_BASIS_TERM.TERM_SEQ
         )
         ~mainTable [compute_basis::Store]CBS_BASIS_TERM
         scenarioId: [compute_basis::Store]CBS_BASIS_TERM.SCENARIO_ID,
         groupId: [compute_basis::Store]CBS_BASIS_TERM.GROUP_ID,
         entityId: [compute_basis::Store]CBS_BASIS_TERM.ENTITY_ID,
         measureCode: [compute_basis::Store]CBS_BASIS_TERM.MEASURE_CODE,
         factorId: [compute_basis::Store]CBS_BASIS_TERM.FACTOR_ID,
         termValue: [compute_basis::Store]CBS_BASIS_TERM.TERM_VALUE
      }}
   }}
)
"""


@functools.cache
def _store_schemas_for(schema: BasisSchema = DEFAULT_SCHEMA) -> dict[str, Any]:
    require_datacube()
    import pyarrow as pa
    elements = compiler.model_elements(render_pure_model(schema))
    type_map = {
        'varchar': pa.string(),
        'char': pa.string(),
        'smallint': pa.int32(),
        'integer': pa.int32(),
        'bigint': pa.int64(),
        'double': pa.float64(),
        'float': pa.float64(),
    }
    schemas: dict[str, Any] = {}
    for element in elements:
        if element.get('_type') != 'relational':
            continue
        for rel_schema in element.get('schemas', []):
            for table in rel_schema.get('tables', []):
                fields = []
                for col in table.get('columns', []):
                    col_type = col.get('type', {})
                    kind = str(col_type.get('_type', '')).lower()
                    if kind not in type_map:
                        raise ValueError(
                            f"unsupported relational column type {col_type!r} on {table['name']}.{col['name']}"
                        )
                    fields.append((col['name'], type_map[kind]))
                schemas[table['name']] = pa.schema(fields)
    return schemas


def store_schemas(book_or_schema: BasisBook | BasisSchema | None = None) -> dict[str, Any]:
    """The PyArrow schemas of ``compute_basis::Store``'s tables, derived directly from the synthesized Pure model via
    ``compiler.model_elements``."""
    return dict(_store_schemas_for(_resolve_schema(book_or_schema)))


def _schema(name: str, book_or_schema: BasisBook | BasisSchema | None = None) -> Any:
    return _store_schemas_for(_resolve_schema(book_or_schema))[name]


# ---------------------------------------------------------------------------------------------- the Pure queries


def _nodes(prefix: str, scenario: str | None = None) -> str:
    rel = 'compute_basis::CbsSharedNode.all()'
    if scenario:
        rel += f'->filter(x|$x.scenarioId == {_quoted(scenario)})'
    return (
        f"{rel}->project(~[{prefix}Scenario: x|$x.scenarioId, {prefix}Node: "
        f"x|$x.nodeId, {prefix}Value: x|$x.evaluatedValue()])"
    )


def _quoted(text: str) -> str:
    return "'" + text.replace('\\', '\\\\').replace("'", "\\'") + "'"


def _stage_terms_relation(
    sch: BasisSchema,
    stage_idx: int | None,
    scenario: str | None = None,
    *,
    include_seq: bool = False,
    eval_mode: str = 'all',
    measure_code: str | None = None,
) -> str:
    tags = sch.slot_tags
    n_params = sch.num_params
    is_top = stage_idx is None or stage_idx == sch.max_stage

    if is_top:
        if eval_mode == 'primal':
            active_slots = (
                sch.primal_stage_nodes[stage_idx]
                if (stage_idx is not None and stage_idx < len(sch.primal_stage_nodes))
                else sch.primal_nodes
            )
        elif eval_mode == 'diff':
            active_slots = (
                sch.diff_stage_nodes[stage_idx]
                if (stage_idx is not None and stage_idx < len(sch.diff_stage_nodes))
                else len(tags)
            )
        else:
            active_slots = (
                sch.stage_nodes[stage_idx]
                if (stage_idx is not None and stage_idx < len(sch.stage_nodes))
                else len(tags)
            )
    else:
        active_slots = (
            sch.stage_nodes[stage_idx]
            if (stage_idx is not None and stage_idx < len(sch.stage_nodes))
            else len(tags)
        )

    if stage_idx is None:
        slot_kinds = ('N',) * active_slots
    else:
        slot_kinds = (
            sch.stage_slot_kinds[stage_idx]
            if stage_idx < len(sch.stage_slot_kinds)
            else (('N',) * active_slots if stage_idx == 0 else ('B',) * active_slots)
        )

    def _sub_values(st: int, prefix: str) -> str:
        inner = _stage_terms_relation(sch, st, scenario, include_seq=False, eval_mode='all')
        return (
            inner
            + '->groupBy(~[scenarioId, entityId], ~[subVal: x|$x.termValue : y|$y->sum()])'
            + f'->project(~[{prefix}Scenario: x|$x.scenarioId, {prefix}Node: x|$x.entityId, {prefix}Value: x|$x.subVal])'
        )

    def _slot_lookup(slot_i: int, prefix: str) -> str:
        if stage_idx is None or stage_idx == 0:
            return _nodes(prefix, scenario)
        kind = slot_kinds[slot_i] if slot_i < len(slot_kinds) else 'B'
        if kind == 'N':
            return _nodes(prefix, scenario)
        subs = [_sub_values(st, prefix) for st in range(stage_idx)]
        sub_union = subs[0]
        for s_rel in subs[1:]:
            sub_union = f'{sub_union}->concatenate({s_rel})'
        if kind == 'S':
            return sub_union
        return f'{_nodes(prefix, scenario)}->concatenate({sub_union})'

    if stage_idx == 0 and sch.max_stage > 0:
        eval_fn = 'cbsEvalPolyTerm'
    elif eval_mode == 'primal':
        eval_fn = 'cbsEvalPolyTerm' if not sch.primal_formulas else 'cbsEvalPrimalTerm'
    elif eval_mode == 'diff':
        eval_fn = 'cbsEvalPolyTerm' if not sch.diff_formulas else 'cbsEvalDiffTerm'
    else:
        eval_fn = 'cbsEvalPolyTerm' if not sch.formulas else 'cbsEvalTerm'

    use_poly_only = eval_fn == 'cbsEvalPolyTerm'

    proj_cols = ['scenarioId: x|$x.scenarioId', 'groupId: x|$x.groupId', 'entityId: x|$x.entityId',
                 'measureCode: x|$x.measureCode', 'factorId: x|$x.factorId']
    if include_seq:
        proj_cols.append('termSeq: x|$x.termSeq')
    if stage_idx is not None:
        proj_cols.append('termStage: x|$x.termStage')
    proj_cols.extend(['termOp: x|$x.termOp', 'coeff: x|$x.coeff'])
    proj_cols.extend(f'node{t}Id: x|$x.node{t}Id' for t in tags[: (len(tags) if include_seq else active_slots)])
    if include_seq or not use_poly_only:
        proj_cols.extend(f'param{j}: x|$x.param{j}' for j in range(1, n_params + 1))
    proj_cols.append('shiftConst: x|$x.shiftConst')

    preds: list[str] = []
    if stage_idx is not None:
        preds.append(f'($x.termStage == {stage_idx})')
    if scenario:
        preds.append(f'($x.scenarioId == {_quoted(scenario)})')
    if measure_code is not None:
        preds.append(f'($x.measureCode == {_quoted(measure_code)})')

    terms = f"compute_basis::CbsBasisTerm.all()->project(~[{', '.join(proj_cols)}])"
    if preds:
        terms += f"->filter(x|{' && '.join(preds)})"

    pfx_base = f's{stage_idx}_' if stage_idx is not None else ''
    p0 = f'{pfx_base}a'
    joins = [
        f"->join({_slot_lookup(0, p0)}, meta::pure::functions::relation::JoinKind.INNER, "
        f"{{l, r | ($l.scenarioId == $r.{p0}Scenario) && ($l.nodeAId == $r.{p0}Node)}})"
    ]
    for idx in range(1, active_slots):
        t = tags[idx]
        p = f'{pfx_base}{t.lower()}'
        joins.append(
            f"->join({_slot_lookup(idx, p)}, meta::pure::functions::relation::JoinKind.LEFT, "
            f"{{l, r | ($l.scenarioId == $r.{p}Scenario) && ($l.node{t}Id == $r.{p}Node)}})"
        )

    eval_args = [
        '$x.termOp->toOne()',
        '$x.coeff->toOne()',
        f'$x.{p0}Value->toOne()',
    ]
    for idx in range(1, len(tags)):
        if idx < active_slots:
            p = f'{pfx_base}{tags[idx].lower()}'
            eval_args.append(f'if($x.{p}Value->isEmpty(), |1.0, |$x.{p}Value->toOne())')
        else:
            eval_args.append('1.0')
    if not use_poly_only:
        for j in range(1, n_params + 1):
            eval_args.append(f'$x.param{j}->toOne()')
    eval_args.append('$x.shiftConst->toOne()')

    return (
        terms
        + ''.join(joins)
        + f"->extend(~[termValue: x|compute_basis::{eval_fn}({', '.join(eval_args)})])"
    )


def terms_relation(
    scenario: str | None = None,
    book_or_schema: SchemaSource = None,
    *,
    eval_mode: str = 'all',
    measure_code: str | None = None,
) -> str:
    """Every top-level entity term valued in SQL from the knots (a relation's Pure text, no leading ``|``): each
    ``BasisNode`` and each intermediate ``SubMeasure`` stage evaluated in SQL, joined by key, and combined by
    ``compute_basis::cbsEvalTerm`` as ``termValue``."""
    sch = _resolve_schema(book_or_schema)
    stage_idx = None if sch.max_stage == 0 else sch.max_stage
    return _stage_terms_relation(
        sch, stage_idx, scenario, include_seq=False, eval_mode=eval_mode, measure_code=measure_code
    )


def _materialize_terms_query(book_or_schema: SchemaSource = None) -> str:
    """Pure query evaluating ``CBS_BASIS_TERM`` rows (including ``TERM_VALUE``) purely in SQL so ``CBS_ENTITY_MEASURE``
    and ``AggregationAware`` views can be materialized without host Python evaluation."""
    sch = _resolve_schema(book_or_schema)
    tags = sch.slot_tags
    n_params = sch.num_params

    def _proj_store_cols(rel: str, has_stage: bool) -> str:
        cols = [
            'SCENARIO_ID: x|$x.scenarioId->toOne()',
            'ENTITY_ID: x|$x.entityId->toOne()',
            'MEASURE_CODE: x|$x.measureCode->toOne()',
            'FACTOR_ID: x|$x.factorId->toOne()',
            'TERM_SEQ: x|$x.termSeq->toOne()',
            'GROUP_ID: x|$x.groupId->toOne()',
        ]
        if has_stage:
            cols.append('TERM_STAGE: x|$x.termStage->toOne()')
        cols.extend([
            'TERM_OP: x|$x.termOp->toOne()',
            'COEFF: x|$x.coeff->toOne()',
            'NODE_A_ID: x|$x.nodeAId->toOne()',
        ])
        for t in tags[1:]:
            cols.append(f'NODE_{t}_ID: x|$x.node{t}Id')
        for j in range(1, n_params + 1):
            cols.append(f'PARAM_{j}: x|$x.param{j}->toOne()')
        cols.extend([
            'SHIFT_CONST: x|$x.shiftConst->toOne()',
            'TERM_VALUE: x|$x.termValue->toOne()',
        ])
        return f"{rel}->project(~[{', '.join(cols)}])"

    if sch.max_stage == 0:
        return '|' + _proj_store_cols(_stage_terms_relation(sch, None, None, include_seq=True), False)

    stage_rels = [
        _proj_store_cols(_stage_terms_relation(sch, st, None, include_seq=True), True)
        for st in range(sch.max_stage + 1)
    ]
    combined = stage_rels[0]
    for sr in stage_rels[1:]:
        combined = f'{combined}->concatenate({sr})'
    return '|' + combined


def measures_query(
    scenario: str | None = None,
    by_entity: bool = True,
    measure_code: str | None = None,
    book_or_schema: SchemaSource = None,
    *,
    eval_mode: str | None = None,
) -> str:
    """Each entity's measures (or, ``by_entity=False``, each group's), valued from the knots: columns scenarioId,
    groupId, [entityId,] measureCode, factorId, value."""
    keys = (
        'scenarioId, groupId, entityId, measureCode, factorId'
        if by_entity
        else 'scenarioId, groupId, measureCode, factorId'
    )
    actual_eval_mode = eval_mode if eval_mode is not None else ('primal' if measure_code == 'NPV' else 'all')
    rel = terms_relation(
        scenario, book_or_schema=book_or_schema, eval_mode=actual_eval_mode, measure_code=measure_code
    )
    return '|' + rel + f'->groupBy(~[{keys}], ~[value: x|$x.termValue : y|$y->sum()])'


def chained_query(
    measure_code: str,
    scenario: str | None = None,
    book_or_schema: SchemaSource = None,
    *,
    eval_mode: str = 'diff',
) -> str:
    """Each group's ``measure_code`` by factor carried through the Jacobian into a measure by input
    (``compute_basis::cbsChainRuleTerm``, summed): columns scenarioId, groupId, inputId, chainedValue."""
    by_factor = (
        terms_relation(
            scenario, book_or_schema=book_or_schema, eval_mode=eval_mode, measure_code=measure_code
        )
        + '->groupBy(~[scenarioId, groupId, factorId], ~[byFactor: x|$x.termValue : y|$y->sum()])'
    )
    jac = (
        'compute_basis::CbsJacobianEntry.all()->project(~[jScenario: x|$x.scenarioId, jFactor: x|$x.factorId, '
        'inputId: x|$x.inputId, jWeight: x|$x.jacobianWeight])'
    )
    return (
        '|'
        + by_factor
        + f'->join({jac}, meta::pure::functions::relation::JoinKind.INNER, '
        '{l, r | ($l.scenarioId == $r.jScenario) && ($l.factorId == $r.jFactor)})'
        + '->extend(~[chained: x|compute_basis::cbsChainRuleTerm($x.byFactor->toOne(), $x.jWeight->toOne())])'
        + '->groupBy(~[scenarioId, groupId, inputId], ~[chainedValue: x|$x.chained : y|$y->sum()])'
    )


def value_and_risk_query(
    value_measure: str = 'NPV',
    risk_measure: str = 'PILLAR_DELTA',
    scenario: str | None = None,
    by_entity: bool = True,
    book_or_schema: SchemaSource = None,
) -> str:
    """Evaluates both primal value (via ``eval_mode='primal'``) and derivative risk (via ``eval_mode='diff'``) in a single
    query via ``->concatenate()``: columns scenarioId, groupId, [entityId,] measureCode, factorId, value."""
    keys = (
        'scenarioId, groupId, entityId, measureCode, factorId'
        if by_entity
        else 'scenarioId, groupId, measureCode, factorId'
    )
    val_rel = (
        terms_relation(
            scenario, book_or_schema=book_or_schema, eval_mode='primal', measure_code=value_measure
        )
        + f'->groupBy(~[{keys}], ~[value: x|$x.termValue : y|$y->sum()])'
    )
    risk_rel = (
        terms_relation(
            scenario, book_or_schema=book_or_schema, eval_mode='diff', measure_code=risk_measure
        )
        + f'->groupBy(~[{keys}], ~[value: x|$x.termValue : y|$y->sum()])'
    )
    return '|' + val_rel + f'->concatenate({risk_rel})'


def value_and_chained_query(
    value_measure: str = 'NPV',
    risk_measure: str = 'PILLAR_DELTA',
    scenario: str | None = None,
    book_or_schema: SchemaSource = None,
) -> str:
    """Evaluates entity-level primal value (resultKind: 'VALUE', itemId: entityId) and group-level chained quote risk
    (resultKind: 'CHAINED', itemId: inputId) in a single DuckDB SQL query execution:
    columns resultKind, scenarioId, groupId, itemId, value."""
    val_rel = (
        terms_relation(
            scenario, book_or_schema=book_or_schema, eval_mode='primal', measure_code=value_measure
        )
        + '->groupBy(~[scenarioId, groupId, entityId], ~[val: x|$x.termValue : y|$y->sum()])'
        + "->project(~[resultKind: x|'VALUE', scenarioId: x|$x.scenarioId, groupId: x|$x.groupId, itemId: x|$x.entityId, value: x|$x.val])"
    )
    by_factor = (
        terms_relation(
            scenario, book_or_schema=book_or_schema, eval_mode='diff', measure_code=risk_measure
        )
        + '->groupBy(~[scenarioId, groupId, factorId], ~[byFactor: x|$x.termValue : y|$y->sum()])'
    )
    jac = (
        'compute_basis::CbsJacobianEntry.all()->project(~[jScenario: x|$x.scenarioId, jFactor: x|$x.factorId, '
        'inputId: x|$x.inputId, jWeight: x|$x.jacobianWeight])'
    )
    chained_rel = (
        by_factor
        + f'->join({jac}, meta::pure::functions::relation::JoinKind.INNER, '
        '{l, r | ($l.scenarioId == $r.jScenario) && ($l.factorId == $r.jFactor)})'
        + '->extend(~[chained: x|compute_basis::cbsChainRuleTerm($x.byFactor->toOne(), $x.jWeight->toOne())])'
        + '->groupBy(~[scenarioId, groupId, inputId], ~[tot: x|$x.chained : y|$y->sum()])'
        + "->project(~[resultKind: x|'CHAINED', scenarioId: x|$x.scenarioId, groupId: x|$x.groupId, itemId: x|$x.inputId, value: x|$x.tot])"
    )
    return '|' + val_rel + f'->concatenate({chained_rel})'


def stored_measures_query() -> str:
    """The measures as the View ``CBS_ENTITY_MEASURE`` rolls up the terms' stored values (``TERM_VALUE``)."""
    return (
        '|compute_basis::CbsEntityMeasure.all()->project(~[scenarioId: x|$x.scenarioId, groupId: x|$x.groupId, '
        'entityId: x|$x.entityId, measureCode: x|$x.measureCode, factorId: x|$x.factorId, value: x|$x.totalValue, '
        'termCount: x|$x.termCount])'
    )


def exposure_query(
    scenario: str | None = None,
    by_entity: bool = True,
    measure_code: str | None = None,
) -> str:
    """Each entity's (or group's) measures queried through ``compute_basis::CbsMeasureExposure``
    (``AggregationAware``), which the compiler routes to the store View ``CBS_ENTITY_MEASURE``."""
    key_names = (
        ['scenarioId', 'groupId', 'entityId', 'measureCode', 'factorId']
        if by_entity
        else ['scenarioId', 'groupId', 'measureCode', 'factorId']
    )
    key_fns = ', '.join(f'x|$x.{k}' for k in key_names)
    col_names = ', '.join(_quoted(k) for k in [*key_names, 'value'])
    rel = (
        f'compute_basis::CbsMeasureExposure.all()->groupBy([{key_fns}], '
        f'[agg(x|$x.termValue, y|$y->sum())], [{col_names}])'
    )
    preds = []
    if scenario is not None:
        preds.append(f'($x.scenarioId == {_quoted(scenario)})')
    if measure_code is not None:
        preds.append(f'($x.measureCode == {_quoted(measure_code)})')
    if preds:
        rel += f"->filter(x|{' && '.join(preds)})"
    return '|' + rel


def query_tree(query: str | compiler.Tree) -> compiler.Tree:
    """A Pure query as a protocol ``compiler.Tree`` (parsed when given as text, returned as-is when already a tree)."""
    return compiler.parse(query) if isinstance(query, str) else query


@functools.cache
def _replace_table_statements(name: str, database: str = 'DuckDB') -> tuple[str, ...]:
    """The planner's host-operation statements that replace ``name`` from ``__cbs_load``, cached per table name."""
    op = {
        'op': 'relation',
        'kind': 'table',
        'target': _host.name(name),
        'replace': True,
        'from': {'relation': _host.name('__cbs_load')},
    }
    return tuple(s for group in compiler.host_operations(database, [op]) for s in group)


# ---------------------------------------------------------------------------------------------- planning and running

_CONNECTION = '''
###Connection
RelationalDatabaseConnection compute_basis::Conn
{{
    type: {database};
    specification: DuckDB {{ }};
    auth: Test;
}}

###Runtime
Runtime compute_basis::RT
{{
    mappings: [compute_basis::Mapping];
    connections: [ compute_basis::Store: [ c1: compute_basis::Conn ] ];
}}
'''

RUNTIME = 'compute_basis::RT'

# the databases a plan is shown for by default (each one legend-lite renders)
DATABASES = ('DuckDB', 'Postgres', 'Snowflake', 'BigQuery', 'Databricks', 'Trino', 'SQLite', 'ClickHouse')


def _schema_from_tables(tables: Mapping[str, Any]) -> BasisSchema | None:
    term_tbl = tables.get('CBS_BASIS_TERM')
    if term_tbl is None or not hasattr(term_tbl, 'column_names'):
        return None

    # Item 3: Check explicit metadata first in O(1)
    tbl_schema = getattr(term_tbl, 'schema', None)
    if tbl_schema is not None and getattr(tbl_schema, 'metadata', None):
        meta = tbl_schema.metadata
        if SCHEMA_META_KEY in meta:
            return BasisSchema.from_json(meta[SCHEMA_META_KEY])

    col_names = list(term_tbl.column_names)
    node_cols = [c for c in col_names if c.startswith('NODE_') and c.endswith('_ID')]
    param_cols = [c for c in col_names if c.startswith('PARAM_')]
    num_nodes = max(3, len(node_cols))
    num_params = len(param_cols)
    if 'TERM_STAGE' not in col_names:
        return BasisSchema(num_nodes=num_nodes, num_params=num_params)
    if len(term_tbl) == 0:
        return BasisSchema(num_nodes=num_nodes, num_params=num_params, max_stage=1)
    stages = term_tbl['TERM_STAGE'].to_pylist()
    max_stage = max(stages) if stages else 0
    return BasisSchema(
        num_nodes=num_nodes,
        num_params=num_params,
        max_stage=max_stage,
        stage_nodes=tuple([len(node_cols)] * (max_stage + 1)),
        stage_slot_kinds=tuple([('B',) * len(node_cols)] * (max_stage + 1)),
    )


class ComputeBasis:
    """The dynamic ``compute_basis`` model with a connection and runtime: planned for any database legend-lite renders,
    run on DuckDB, ``Frames``, or ``legend_lite.warehouse``. Each ``(BasisSchema, database)`` model is compiled once and
    held until ``close``."""

    def __init__(self, book_or_schema: SchemaSource = None) -> None:
        if isinstance(book_or_schema, BasisBook):
            self.ctx: BasisContext = book_or_schema.ctx
            self._explicit_schema: BasisSchema | None = book_or_schema.schema()
            self._book_ref: BasisBook | None = book_or_schema
        elif isinstance(book_or_schema, BasisContext):
            self.ctx = book_or_schema
            self._explicit_schema = book_or_schema.schema
            self._book_ref = None
        elif isinstance(book_or_schema, BasisSchema):
            self.ctx = BasisContext(schema=book_or_schema)
            self._explicit_schema = book_or_schema
            self._book_ref = None
        elif isinstance(book_or_schema, ComputeBasis):
            self.ctx = book_or_schema.ctx
            self._explicit_schema = book_or_schema.schema
            self._book_ref = getattr(book_or_schema, '_book_ref', None)
        else:
            self.ctx = BasisContext()
            self._explicit_schema = None
            self._book_ref = None

        self._connected_schema: BasisSchema | None = None
        self._held: dict[tuple[BasisSchema, str], compiler.HeldModel] = {}
        self._plans: dict[tuple[BasisSchema, str, str], compiler.Plan] = {}
        self._frame_holders: dict[tuple[int, str], list[Any]] = {}

    @property
    def schema(self) -> BasisSchema:
        if self._explicit_schema is not None:
            return self._explicit_schema
        if self._connected_schema is not None:
            return self._connected_schema
        if self._book_ref is not None:
            return self._book_ref.schema()
        return self.ctx.schema

    @property
    def source(self) -> str:
        return render_pure_model(self.schema)

    def model(self, database: str = 'DuckDB', schema: BasisSchema | None = None) -> str:
        sch = schema if schema is not None else self.schema
        return render_pure_model(sch) + _CONNECTION.format(database=database)

    def held(self, database: str = 'DuckDB', schema: BasisSchema | None = None) -> compiler.HeldModel:
        sch = schema if schema is not None else self.schema
        key = (sch, database)
        found = self._held.get(key)
        if found is None:
            found = self._held[key] = compiler.HeldModel(self.model(database, sch))
        return found

    def plan(
        self,
        query: str | compiler.Tree,
        database: str = 'DuckDB',
        schema: BasisSchema | None = None,
    ) -> compiler.Plan:
        """A query (Pure lambda text or a protocol ``compiler.Tree``) planned for ``database``: its SQL and columns."""
        sch = schema if schema is not None else self.schema
        if isinstance(query, str):
            key = (sch, database, query)
            found = self._plans.get(key)
            if found is None:
                found = self._plans[key] = self.held(database, sch).plan(compiler.parse(query), RUNTIME)
            return found
        key = (sch, database, compiler.print_tree(query, 'STANDARD'))
        found = self._plans.get(key)
        if found is None:
            found = self._plans[key] = self.held(database, sch).plan(query, RUNTIME)
        return found

    def relation_type(
        self,
        query: str | compiler.Tree,
        database: str = 'DuckDB',
        schema: BasisSchema | None = None,
    ) -> tuple[compiler.Column, ...]:
        """The columns ``query`` returns, typed against the held model without generating SQL."""
        return self.held(database, schema).relation_type(query_tree(query))

    def measures_query(
        self,
        scenario: str | None = None,
        by_entity: bool = True,
        measure_code: str | None = None,
        *,
        eval_mode: str | None = None,
    ) -> str:
        return measures_query(
            scenario=scenario,
            by_entity=by_entity,
            measure_code=measure_code,
            book_or_schema=self,
            eval_mode=eval_mode,
        )

    def chained_query(
        self,
        measure_code: str,
        scenario: str | None = None,
        *,
        eval_mode: str = 'diff',
    ) -> str:
        return chained_query(
            measure_code=measure_code,
            scenario=scenario,
            book_or_schema=self,
            eval_mode=eval_mode,
        )

    def value_and_risk_query(
        self,
        value_measure: str = 'NPV',
        risk_measure: str = 'PILLAR_DELTA',
        scenario: str | None = None,
        by_entity: bool = True,
    ) -> str:
        return value_and_risk_query(
            value_measure=value_measure,
            risk_measure=risk_measure,
            scenario=scenario,
            by_entity=by_entity,
            book_or_schema=self,
        )

    def value_and_chained_query(
        self,
        value_measure: str = 'NPV',
        risk_measure: str = 'PILLAR_DELTA',
        scenario: str | None = None,
    ) -> str:
        return value_and_chained_query(
            value_measure=value_measure,
            risk_measure=risk_measure,
            scenario=scenario,
            book_or_schema=self,
        )

    def terms_relation(
        self,
        scenario: str | None = None,
        *,
        eval_mode: str = 'all',
        measure_code: str | None = None,
    ) -> str:
        return terms_relation(
            scenario=scenario,
            book_or_schema=self,
            eval_mode=eval_mode,
            measure_code=measure_code,
        )

    def store_schemas(self) -> dict[str, Any]:
        return store_schemas(self)

    def materialize_stored(self, connection: Any) -> Any:
        """Computes ``CBS_BASIS_TERM.TERM_VALUE`` in DuckDB SQL via Pure so ``CBS_ENTITY_MEASURE`` / ``AggregationAware``
        views reflect current ``CBS_KNOT`` values without any host-side math evaluation."""
        require_datacube()
        import pyarrow as pa
        sch = self.schema
        evaluated = self.run(connection, _materialize_terms_query(sch))
        raw_term_schema = _schema('CBS_BASIS_TERM', sch)
        term_schema = raw_term_schema.with_metadata({
            **(raw_term_schema.metadata or {}),
            SCHEMA_META_KEY: sch.to_json(),
        })
        casted = evaluated.cast(term_schema) if evaluated.schema != term_schema else pa.Table.from_arrays(
            evaluated.columns, schema=term_schema
        )
        return self.update(connection, {'CBS_BASIS_TERM': casted})

    def _put_frame(self, frames_obj: Any, name: str, table: Any) -> Any:
        import pyarrow as pa
        key = name.lower()
        existing = getattr(frames_obj, '_tables', {}).get(key)
        arrow = table if isinstance(table, pa.Table) else None
        if (
            existing is not None
            and not getattr(existing, '_closed', True)
            and arrow is not None
            and getattr(existing, '_schema', None) == arrow.schema
        ):
            frames_obj.connection.register(existing._registered, arrow)
            existing._arrow = arrow
            existing.given = table
            existing._read = table if callable(table) else (lambda: table)
            return existing
        return frames_obj.register(name, table)

    def connect(
        self,
        tables: Mapping[str, Any],
        connection: Any = None,
        *,
        materialize_stored: bool = True,
    ) -> Any:
        """A DuckDB connection (or ``connection``, including ``Frames``) set up as the planner's SQL expects, the tables
        loaded. When ``CBS_KNOT`` and ``CBS_BASIS_TERM`` are both supplied and ``materialize_stored=True``, populates
        ``TERM_VALUE`` in SQL so stored/AggregationAware views work alongside live queries."""
        require_datacube()
        inferred = _schema_from_tables(tables)
        if inferred is not None:
            self._connected_schema = inferred
            self.ctx.schema = inferred
        from ..frames import Frames
        if isinstance(connection, Frames):
            con: Any = connection
        else:
            import duckdb
            con = connection if connection is not None else duckdb.connect()
            for statement in compiler.session_setup('DuckDB'):
                con.execute(statement)
        self.update(con, tables, materialize_stored=False)
        if (
            materialize_stored
            and 'CBS_KNOT' in tables
            and 'CBS_BASIS_TERM' in tables
            and len(tables['CBS_KNOT']) > 0
            and len(tables['CBS_BASIS_TERM']) > 0
        ):
            self.materialize_stored(con)
        return con

    def update(
        self,
        connection: Any,
        tables: Mapping[str, Any],
        *,
        materialize_stored: bool = False,
    ) -> Any:
        """Replaces ``tables`` (such as ``CBS_KNOT`` and ``CBS_JACOBIAN_ENTRY`` on a market tick) in ``connection``."""
        require_datacube()
        inferred = _schema_from_tables(tables)
        if inferred is not None:
            self._connected_schema = inferred
            self.ctx.schema = inferred
        from ..frames import Frames
        if isinstance(connection, Frames):
            for name, table in tables.items():
                self._put_frame(connection, name, table)
        else:
            for name, table in tables.items():
                connection.register('__cbs_load', table)
                try:
                    for statement in _replace_table_statements(name):
                        connection.execute(statement)
                finally:
                    connection.unregister('__cbs_load')
        if materialize_stored and 'CBS_KNOT' in tables and len(tables['CBS_KNOT']) > 0:
            self.materialize_stored(connection)
        return connection

    def frames(
        self,
        tables: Mapping[str, Any],
        frames: Any = None,
        *,
        materialize_stored: bool = True,
    ) -> Any:
        """Registers ``tables`` into a ``Frames`` instance (or a ``ll.Page``'s session frames, or creates a new
        ``Frames``) so ``Frames.model_for(self.model())`` admits the multi-table ``compute_basis`` model and
        ``ll.Page`` / ``page.flow()`` can query the tables directly."""
        require_datacube()
        inferred = _schema_from_tables(tables)
        if inferred is not None:
            self._connected_schema = inferred
            self.ctx.schema = inferred
        from ..frames import Frames
        target = unwrap_frames(frames) if frames is not None else Frames()
        for name, table in tables.items():
            self._put_frame(target, name, table)
        if (
            materialize_stored
            and 'CBS_KNOT' in tables
            and 'CBS_BASIS_TERM' in tables
            and len(tables['CBS_KNOT']) > 0
            and len(tables['CBS_BASIS_TERM']) > 0
        ):
            self.materialize_stored(target)
        return target

    def run(self, connection: Any, query: str | compiler.Tree | compiler.Plan, database: str = 'DuckDB') -> Any:
        """A query planned for ``database`` and run on ``connection`` (a DuckDB connection, a ``Frames`` instance, or a
        ``warehouse.Warehouse`` / ``warehouse.Session``): a plan-typed ``pyarrow.Table`` with ``b'legend.type'`` column
        metadata."""
        require_datacube()
        from .. import compiler, warehouse
        if isinstance(connection, (warehouse.Warehouse, warehouse.Session)):
            return self.run_warehouse(connection, query, database=database)
        from ..frames import Frames, typed_by_plan
        planned = query if isinstance(query, compiler.Plan) else self.plan(query, database)
        if isinstance(connection, Frames):
            return connection.run(planned)
        return typed_by_plan(connection.execute(planned.sql).to_arrow_table(), planned)

    def run_warehouse(
        self,
        warehouse_or_session: Any,
        query: str | compiler.Tree | compiler.Plan,
        database: str = 'DuckDB',
        *,
        catalog: str = 'main',
        on_receipt: Callable[[dict[str, Any]], None] | None = None,
    ) -> Any:
        """Plans ``query`` for ``database`` and executes its SQL on a ``legend_lite.warehouse.Warehouse`` (or
        ``warehouse.Session``), returning a plan-typed ``pyarrow.Table``."""
        require_datacube()
        import pyarrow as pa
        import pyarrow.ipc
        from .. import compiler, warehouse
        from ..frames import typed_by_plan
        wh = (
            warehouse.Warehouse(warehouse_or_session, catalog=catalog)
            if isinstance(warehouse_or_session, warehouse.Session)
            else warehouse_or_session
        )
        planned = query if isinstance(query, compiler.Plan) else self.plan(query, database)
        tables = [pyarrow.ipc.open_stream(c).read_all() for c in wh.arrow_chunks(planned.sql, on_receipt=on_receipt)]
        if not tables:
            raise pa.ArrowInvalid('the warehouse answered no chunk, not even its columns')
        rows = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        return typed_by_plan(rows, planned)

    def close(self) -> None:
        self._plans.clear()
        for held in self._held.values():
            held.close()
        self._held.clear()
        self._frame_holders.clear()

    def __enter__(self) -> 'ComputeBasis':
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
