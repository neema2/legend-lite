"""Fitting single and multi-curve zero curves to par swap quotes, with exact symbolic Jacobians, quote-bump refits,
and skinny vectorized 2-iteration 100%-SQL Newton-Schulz / Halley calibration and adjoint risk blocks."""

from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from legend_lite._extra import require_datacube
from legend_lite.scalar_parallel import basis, expr

if TYPE_CHECKING:
    from legend_lite.scalar_parallel import basis_pure as pure

from ..instruments.base import Instrument
from ..instruments.ir_swap_fixed_float import IRSwapFixedFloat
from ..instruments.ir_swap_fixed_floatapprox import IRSwapFixedFloatApprox
from .ir_curve_yield import Pillar, ZeroCurve

DEFAULT_NEWTON_STEPS = 2
_SQL_STAGE_BARRIER = '->limit(1000000)'


def _is_lower_matrix(a: Sequence[Sequence[float]]) -> bool:
    n = len(a)
    for i in range(n):
        row = a[i]
        for k in range(i + 1, n):
            if row[k] != 0.0:
                return False
    return True


def _lu_factor(a: Sequence[Sequence[float]]) -> tuple[list[list[float]], list[int]]:
    """In-place PA = LU factorization with partial pivoting in O(N^3)."""
    n = len(a)
    lu = [list(row) for row in a]
    piv = list(range(n))
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(lu[r][c]))
        if p != c:
            lu[c], lu[p] = lu[p], lu[c]
            piv[c], piv[p] = piv[p], piv[c]
        diag = lu[c][c]
        for r in range(c + 1, n):
            f = lu[r][c] / diag
            lu[r][c] = f
            row_r = lu[r]
            row_c = lu[c]
            for k in range(c + 1, n):
                row_r[k] -= f * row_c[k]
    return lu, piv


def _lu_solve(lu: list[list[float]], piv: list[int], b: Sequence[float]) -> list[float]:
    n = len(b)
    y = [b[piv[i]] for i in range(n)]
    for r in range(1, n):
        row_r = lu[r]
        acc = y[r]
        for k in range(r):
            acc -= row_r[k] * y[k]
        y[r] = acc
    x = [0.0] * n
    for r in range(n - 1, -1, -1):
        row_r = lu[r]
        acc = y[r]
        for k in range(r + 1, n):
            acc -= row_r[k] * x[k]
        x[r] = acc / row_r[r]
    return x


def solve(a: Sequence[Sequence[float]], b: Sequence[float]) -> list[float]:
    """Solves ``A x = b`` via O(nnz) forward substitution when lower-triangular, else O(N^3) PA = LU."""
    n = len(b)
    if _is_lower_matrix(a):
        x = [0.0] * n
        for i in range(n):
            row = a[i]
            acc = b[i]
            for k in range(i):
                v = row[k]
                if v != 0.0:
                    acc -= v * x[k]
            x[i] = acc / row[i]
        return x
    lu, piv = _lu_factor(a)
    return _lu_solve(lu, piv, b)


def inverse(a: Sequence[Sequence[float]]) -> list[list[float]]:
    """Inverts ``A`` via sparse O(N * nnz) forward substitution when lower-triangular, else single PA = LU in O(N^3)."""
    n = len(a)
    if _is_lower_matrix(a):
        inv = [[0.0] * n for _ in range(n)]
        nz_off = [[(k, a[i][k]) for k in range(i) if a[i][k] != 0.0] for i in range(n)]
        for i in range(n):
            inv_ii = 1.0 / a[i][i]
            inv[i][i] = inv_ii
            offs = nz_off[i]
            if offs:
                row_inv = inv[i]
                for j in range(i):
                    s = 0.0
                    for k, aik in offs:
                        if k >= j:
                            s += aik * inv[k][j]
                    if s != 0.0:
                        row_inv[j] = -s * inv_ii
        return inv
    lu, piv = _lu_factor(a)
    cols = [_lu_solve(lu, piv, [1.0 if i == j else 0.0 for i in range(n)]) for j in range(n)]
    return [[cols[j][i] for j in range(n)] for i in range(n)]


@dataclass(frozen=True)
class QuoteSpec:
    """Metadata for one calibration market quote."""

    quote_id: str
    curve_id: str
    pillar: Pillar
    quote: float
    par_expr: expr.Expr


@dataclass(frozen=True)
class FactorSpec:
    """Metadata for one fitted curve pillar factor."""

    factor_id: str
    curve: ZeroCurve
    index: int
    pillar: Pillar
    knot: expr.Var

    @property
    def series_id(self) -> str:
        return self.curve.curve_id


@dataclass(frozen=True)
class Fit:
    """A fitted curve (or multi-curve system): the pillars' zero rates, d pillar_k / d quote_j (``dr_dq[k][j]``),
    the Newton steps taken, per-curve rate lookup, and optional volatility surface grids."""

    rates: list[float]
    dr_dq: list[list[float]]
    iterations: int
    rates_by_curve: dict[str, list[float]] = field(default_factory=dict)
    vols_by_surface: dict[str, list[list[float]]] = field(default_factory=dict)

    def rates_for(self, curve_id: str) -> list[float]:
        """The fitted pillar zero rates for ``curve_id``."""
        return list(self.rates_by_curve.get(curve_id, self.rates))

    def env(
        self,
        curves: Sequence[ZeroCurve],
        vol_surfaces: Sequence[object] = (),
    ) -> dict[str, float]:
        out: dict[str, float] = {}
        for c in curves:
            r = self.rates_by_curve.get(c.curve_id, self.rates)
            out.update(c.env(r))
        for vs in vol_surfaces:
            sid = getattr(vs, 'surface_id', None)
            vols = self.vols_by_surface.get(sid) if sid is not None else None
            if hasattr(vs, 'env'):
                out.update(vs.env(vols))  # type: ignore[attr-defined]
        return out


class CurveFitter:
    """Newton on a curve's pillar rates so a par swap to each pillar's tenor prices at that pillar's quote. The Jacobian
    d par / d pillar is the symbolic derivative of each par rate, evaluated; its inverse is d pillar / d quote."""

    def __init__(self, curve: ZeroCurve, quotes: Sequence[float], quote_prefix: str = 'PAR') -> None:
        if len(quotes) != len(curve.pillars):
            raise ValueError('one quote per pillar')
        self.curve = curve
        self.curves = [curve]
        self.quotes = list(quotes)
        self.quote_prefix = quote_prefix
        self.instruments = [
            IRSwapFixedFloatApprox(
                symbol=self.quote_id(k),
                notional=1.0,
                fixed_rate=0.0,
                tenor_years=p.tenor,
                start=0.0,
                book='CALIBRATION',
                curve=curve,
            )
            for k, p in enumerate(curve.pillars)
        ]
        self.pars = [s.par_rate(curve) for s in self.instruments]
        self.factors = [
            FactorSpec(curve.factor_id(k), curve, k, p, curve.pillar(k))
            for k, p in enumerate(curve.pillars)
        ]
        self.quote_specs = [
            QuoteSpec(self.quote_id(j), curve.curve_id, p, self.quotes[j], self.pars[j])
            for j, p in enumerate(curve.pillars)
        ]
        par_vars = [expr.variables(par) for par in self.pars]
        self.jacobian = [
            [expr.diff(par, f.knot) if f.knot in pvars else expr.ZERO for f in self.factors]
            for par, pvars in zip(self.pars, par_vars)
        ]

    def quote_id(self, j: int) -> str:
        """How the j-th market quote is named as an input."""
        return f'{self.quote_prefix}_{self.curve.pillars[j].label}'

    def quote_index(self, quote_id_or_index: str | int) -> int:
        if isinstance(quote_id_or_index, int):
            return quote_id_or_index
        for j, q in enumerate(self.quote_specs):
            if q.quote_id == quote_id_or_index:
                return j
        raise KeyError(f'Unknown quote {quote_id_or_index!r}')

    def with_bumped_quote(self, quote_id_or_index: str | int, delta: float) -> CurveFitter:
        """Returns a new ``CurveFitter`` with the specified input benchmark quote bumped by ``delta``."""
        j = self.quote_index(quote_id_or_index)
        bumped = list(self.quotes)
        bumped[j] += delta
        return CurveFitter(self.curve, bumped, quote_prefix=self.quote_prefix)

    def fit_quotes(
        self,
        quotes: Sequence[float] | None = None,
        *,
        tol: float = 1e-14,
        max_iter: int = 30,
        steps: int | None = None,
        compute_jacobian: bool = True,
        initial_rates: Sequence[float] | None = None,
        halley: bool = False,
    ) -> Fit:
        """Fits the curve to ``quotes`` (defaults to ``self.quotes``) using the pre-built symbolic par & Jacobian DAGs.

        When ``steps`` is given (e.g. ``steps=2``), executes a fixed number of Newton iterations matching the SQL fitter.
        When ``halley=True``, Step >= 2 applies a midpoint-secant Halley correction exploiting the Jacobian shift
        ``J(r_k) - J(r_{k-1})`` learned across consecutive steps.
        """
        target_quotes = list(self.quotes if quotes is None else quotes)
        rates = list(target_quotes if initial_rates is None else initial_rates)
        limit = max_iter if steps is None else int(steps)
        stop_tol = tol if steps is None else -1.0
        active_cols = [[k for k, d in enumerate(row) if d is not expr.ZERO] for row in self.jacobian]
        n = len(rates)
        prev_jac: list[list[float]] | None = None
        prev_step: list[float] | None = None
        iterations = 0
        for iterations in range(1, limit + 1):
            env = self.curve.env(rates)
            cache: dict = {}
            residual = [q - expr.evaluate(p, env, cache) for p, q in zip(self.pars, target_quotes)]
            jac = [[0.0] * n for _ in range(n)]
            for i, cols in enumerate(active_cols):
                row_expr = self.jacobian[i]
                row_val = jac[i]
                for k in cols:
                    row_val[k] = expr.evaluate(row_expr[k], env, cache)
            step_n = solve(jac, residual)
            if halley and prev_jac is not None and prev_step is not None:
                step_0 = solve(prev_jac, residual)
                step = []
                for ps, sn, s0 in zip(prev_step, step_n, step_0):
                    al = max(-0.25, min(0.25, (ps * sn) / (ps * ps + 1e-24)))
                    step.append(sn + 0.5 * al * (sn - s0))
            else:
                step = step_n
            rates = [r + s for r, s in zip(rates, step)]
            prev_jac = jac
            prev_step = step
            if stop_tol >= 0.0 and max(abs(x) for x in residual) < stop_tol:
                break
        dr_dq: list[list[float]] = []
        if compute_jacobian:
            env = self.curve.env(rates)
            cache = {}
            jac = [[0.0] * n for _ in range(n)]
            for i, cols in enumerate(active_cols):
                row_expr = self.jacobian[i]
                row_val = jac[i]
                for k in cols:
                    row_val[k] = expr.evaluate(row_expr[k], env, cache)
            dr_dq = inverse(jac)
        return Fit(rates, dr_dq, iterations, {self.curve.curve_id: rates})

    def fit(self, tol: float = 1e-14, max_iter: int = 30, *, steps: int | None = None, halley: bool = False) -> Fit:
        return self.fit_quotes(self.quotes, tol=tol, max_iter=max_iter, steps=steps, compute_jacobian=True, halley=halley)

    def seed_curve_state_pure(self, prefix: str = '', **kwargs: Any) -> str:
        return seed_curve_state_pure(self, prefix=prefix, **kwargs)

    def newton_step_pure(self, prefix: str = '', **kwargs: Any) -> str:
        return newton_step_pure(self, prefix=prefix, **kwargs)

    def inverse_jacobian_pure(self, prefix: str = '', **kwargs: Any) -> str:
        return inverse_jacobian_pure(self, prefix=prefix, **kwargs)

    def sql_fitter(
        self,
        cb: pure.ComputeBasis | None = None,
        *,
        steps: int = DEFAULT_NEWTON_STEPS,
        halley: bool = False,
    ) -> PureBasisCurveFitter:
        """Returns a ``PureBasisCurveFitter`` configured to fit this curve 100% in DuckDB SQL."""
        return PureBasisCurveFitter(self, cb=cb, steps=steps, halley=halley)


class MultiCurveFitter:
    """Joint multi-curve Newton fitter for a discount curve (calibrated to OIS par quotes via ``IRSwapFixedFloatApprox``)
    and a projection curve (calibrated to two-curve IRS par quotes via ``IRSwapFixedFloat`` discounted on the discount
    curve).

    Computes the joint cross-curve inverse Jacobian ``d pillar_k / d quote_j`` across all pillars and quotes of both
    curves, capturing both direct sensitivity and the cross-curve discounting effect of OIS quotes on projection pillars.
    """

    def __init__(
        self,
        discount_curve: ZeroCurve,
        discount_quotes: Sequence[float],
        projection_curve: ZeroCurve,
        projection_quotes: Sequence[float],
        discount_prefix: str = 'OIS',
        projection_prefix: str = 'IRS',
    ) -> None:
        if len(discount_quotes) != len(discount_curve.pillars):
            raise ValueError('one discount quote per discount_curve pillar')
        if len(projection_quotes) != len(projection_curve.pillars):
            raise ValueError('one projection quote per projection_curve pillar')
        self.discount_curve = discount_curve
        self.projection_curve = projection_curve
        self.curves = [discount_curve, projection_curve]
        self.discount_quotes = list(discount_quotes)
        self.projection_quotes = list(projection_quotes)
        self.quotes = self.discount_quotes + self.projection_quotes
        self.discount_prefix = discount_prefix
        self.projection_prefix = projection_prefix

        self.discount_instruments = [
            IRSwapFixedFloatApprox(
                symbol=self.discount_quote_id(k),
                notional=1.0,
                fixed_rate=0.0,
                tenor_years=p.tenor,
                start=0.0,
                book='CALIBRATION',
                curve=discount_curve,
            )
            for k, p in enumerate(discount_curve.pillars)
        ]
        self.projection_instruments = [
            IRSwapFixedFloat(
                symbol=self.projection_quote_id(k),
                notional=1.0,
                fixed_rate=0.0,
                tenor_years=p.tenor,
                start=0.0,
                book='CALIBRATION',
                discount_curve=discount_curve,
                projection_curve=projection_curve,
            )
            for k, p in enumerate(projection_curve.pillars)
        ]
        self.instruments = list(self.discount_instruments) + list(self.projection_instruments)

        self.factors = [
            FactorSpec(discount_curve.factor_id(k), discount_curve, k, p, discount_curve.pillar(k))
            for k, p in enumerate(discount_curve.pillars)
        ] + [
            FactorSpec(projection_curve.factor_id(k), projection_curve, k, p, projection_curve.pillar(k))
            for k, p in enumerate(projection_curve.pillars)
        ]

        disc_pars = [s.par_rate(discount_curve) for s in self.discount_instruments]
        proj_pars = [s.par_rate(discount_curve, projection_curve) for s in self.projection_instruments]
        self.pars = disc_pars + proj_pars

        self.quote_specs = [
            QuoteSpec(self.discount_quote_id(j), discount_curve.curve_id, p, self.discount_quotes[j], disc_pars[j])
            for j, p in enumerate(discount_curve.pillars)
        ] + [
            QuoteSpec(self.projection_quote_id(j), projection_curve.curve_id, p, self.projection_quotes[j], proj_pars[j])
            for j, p in enumerate(projection_curve.pillars)
        ]

        par_vars = [expr.variables(par) for par in self.pars]
        self.jacobian = [
            [expr.diff(par, f.knot) if f.knot in pvars else expr.ZERO for f in self.factors]
            for par, pvars in zip(self.pars, par_vars)
        ]

    def discount_quote_id(self, j: int) -> str:
        return f'{self.discount_prefix}_{self.discount_curve.pillars[j].label}'

    def projection_quote_id(self, j: int) -> str:
        return f'{self.projection_prefix}_{self.projection_curve.pillars[j].label}'

    def quote_id(self, j: int) -> str:
        return self.quote_specs[j].quote_id

    def quote_index(self, quote_id_or_index: str | int) -> int:
        if isinstance(quote_id_or_index, int):
            return quote_id_or_index
        for j, q in enumerate(self.quote_specs):
            if q.quote_id == quote_id_or_index:
                return j
        raise KeyError(f'Unknown quote {quote_id_or_index!r}')

    def with_bumped_quote(self, quote_id_or_index: str | int, delta: float) -> MultiCurveFitter:
        """Returns a new ``MultiCurveFitter`` with the specified input benchmark quote bumped by ``delta``."""
        j = self.quote_index(quote_id_or_index)
        n_disc = len(self.discount_curve.pillars)
        disc_q = list(self.discount_quotes)
        proj_q = list(self.projection_quotes)
        if j < n_disc:
            disc_q[j] += delta
        else:
            proj_q[j - n_disc] += delta
        return MultiCurveFitter(
            self.discount_curve,
            disc_q,
            self.projection_curve,
            proj_q,
            discount_prefix=self.discount_prefix,
            projection_prefix=self.projection_prefix,
        )

    def fit_quotes(
        self,
        quotes: Sequence[float] | None = None,
        *,
        tol: float = 1e-14,
        max_iter: int = 30,
        steps: int | None = None,
        compute_jacobian: bool = True,
        initial_rates: Sequence[float] | None = None,
        halley: bool = False,
    ) -> Fit:
        """Fits both curves simultaneously to ``quotes`` (defaults to ``self.quotes``)."""
        n_disc = len(self.discount_curve.pillars)
        target_quotes = list(self.quotes if quotes is None else quotes)
        rates = list(target_quotes if initial_rates is None else initial_rates)
        limit = max_iter if steps is None else int(steps)
        stop_tol = tol if steps is None else -1.0
        active_cols = [[k for k, d in enumerate(row) if d is not expr.ZERO] for row in self.jacobian]
        n = len(rates)
        prev_jac: list[list[float]] | None = None
        prev_step: list[float] | None = None
        iterations = 0
        for iterations in range(1, limit + 1):
            env = {
                **self.discount_curve.env(rates[:n_disc]),
                **self.projection_curve.env(rates[n_disc:]),
            }
            cache: dict = {}
            residual = [q - expr.evaluate(p, env, cache) for p, q in zip(self.pars, target_quotes)]
            jac = [[0.0] * n for _ in range(n)]
            for i, cols in enumerate(active_cols):
                row_expr = self.jacobian[i]
                row_val = jac[i]
                for k in cols:
                    row_val[k] = expr.evaluate(row_expr[k], env, cache)
            step_n = solve(jac, residual)
            if halley and prev_jac is not None and prev_step is not None:
                step_0 = solve(prev_jac, residual)
                step = []
                for ps, sn, s0 in zip(prev_step, step_n, step_0):
                    al = max(-0.25, min(0.25, (ps * sn) / (ps * ps + 1e-24)))
                    step.append(sn + 0.5 * al * (sn - s0))
            else:
                step = step_n
            rates = [r + s for r, s in zip(rates, step)]
            prev_jac = jac
            prev_step = step
            if stop_tol >= 0.0 and max(abs(x) for x in residual) < stop_tol:
                break
        dr_dq: list[list[float]] = []
        if compute_jacobian:
            env = {
                **self.discount_curve.env(rates[:n_disc]),
                **self.projection_curve.env(rates[n_disc:]),
            }
            cache = {}
            jac = [[0.0] * n for _ in range(n)]
            for i, cols in enumerate(active_cols):
                row_expr = self.jacobian[i]
                row_val = jac[i]
                for k in cols:
                    row_val[k] = expr.evaluate(row_expr[k], env, cache)
            dr_dq = inverse(jac)
        by_curve = {
            self.discount_curve.curve_id: rates[:n_disc],
            self.projection_curve.curve_id: rates[n_disc:],
        }
        return Fit(rates, dr_dq, iterations, by_curve)

    def fit(self, tol: float = 1e-14, max_iter: int = 30, *, steps: int | None = None, halley: bool = False) -> Fit:
        return self.fit_quotes(self.quotes, tol=tol, max_iter=max_iter, steps=steps, compute_jacobian=True, halley=halley)

    def seed_curve_state_pure(self, prefix: str = '', **kwargs: Any) -> str:
        return seed_curve_state_pure(self, prefix=prefix, **kwargs)

    def newton_step_pure(self, prefix: str = '', **kwargs: Any) -> str:
        return newton_step_pure(self, prefix=prefix, **kwargs)

    def inverse_jacobian_pure(self, prefix: str = '', **kwargs: Any) -> str:
        return inverse_jacobian_pure(self, prefix=prefix, **kwargs)

    def sql_fitter(
        self,
        cb: pure.ComputeBasis | None = None,
        *,
        steps: int = DEFAULT_NEWTON_STEPS,
        halley: bool = False,
    ) -> PureBasisCurveFitter:
        """Returns a ``PureBasisCurveFitter`` configured to fit this curve system 100% in DuckDB SQL."""
        return PureBasisCurveFitter(self, cb=cb, steps=steps, halley=halley)


def _jacobian_topology(
    fitter: CurveFitter | MultiCurveFitter,
) -> tuple[bool, list[list[int]], list[list[int]], list[int], list[list[int]]]:
    """Analyzes the symbolic Jacobian sparsity of ``fitter``.

    Returns ``(is_lower_triangular, active_jac, off_deps, levels, reachable)``:
      - ``active_jac[i]``: column indices ``k`` where ``d par_i / d r_k`` is not identically zero.
      - ``off_deps[i]``: strict lower-triangular column dependencies ``k < i`` of row ``i``.
      - ``levels[i]``: topological forward-substitution depth of row ``i`` (when lower-triangular).
      - ``reachable[i]``: sorted column indices ``m <= i`` where ``(J^-1)_{i, m}`` can be non-zero.
    """
    n = len(fitter.factors)
    active_jac = [
        [k for k in range(n) if fitter.jacobian[i][k] is not expr.ZERO]
        for i in range(n)
    ]
    is_lower = all(
        fitter.jacobian[i][k] is expr.ZERO
        for i in range(n)
        for k in range(i + 1, n)
    )
    if not is_lower:
        full = list(range(n))
        return False, active_jac, [full for _ in range(n)], list(range(n)), [full for _ in range(n)]

    off_deps = [[k for k in active_jac[i] if k < i] for i in range(n)]
    levels = [0] * n
    reach_sets: list[set[int]] = [set() for _ in range(n)]
    for i in range(n):
        if off_deps[i]:
            levels[i] = 1 + max(levels[k] for k in off_deps[i])
        reach_sets[i] = {i} | {m for k in off_deps[i] for m in reach_sets[k]}
    reachable = [sorted(s) for s in reach_sets]
    return True, active_jac, off_deps, levels, reachable


def seed_curve_state_pure(
    fitter: CurveFitter | MultiCurveFitter,
    prefix: str = '',
    *,
    rate_expr: str | None = None,
    scenario_col: str = 'scenario_id',
    index_col: str = 'quote_index',
    rate_col: str = 'quote_rate',
) -> str:
    """Pure relation step pivoting normalized quote rows into 1 wide curve-state row per scenario:
    ``(scenario_id, q_0..q_{N-1}, r_0..r_{N-1}, max_step_bps)`` seeded at ``r_i = q_i``."""
    n = len(fitter.factors)
    val = rate_expr if rate_expr is not None else f'$x.{rate_col}->toOne()'
    ext_v = ', '.join(
        f'q_{j}_v: x|if($x.{index_col} == {j}, |{val}, |0.0)'
        for j in range(n)
    )
    aggs = ', '.join(
        f'q_{j}: x|$x.q_{j}_v:y|$y->plus()'
        for j in range(n)
    )
    init_r = [f'r_{j}: x|$x.q_{j}->toOne()' for j in range(n)] + ['max_step_bps: x|0.0']
    return (
        f'{prefix}->extend(~[{ext_v}])'
        f'->groupBy(~[{scenario_col}], ~[{aggs}])'
        f"->extend(~[{', '.join(init_r)}]){_SQL_STAGE_BARRIER}"
    )


def _gauss_jordan_pure(
    n: int,
    active_jac: Sequence[Sequence[int]],
    rhs_cols: Sequence[tuple[str, Sequence[str]]],
    passthrough_cols: Sequence[str],
    scenario_col: str,
) -> tuple[str, list[str]]:
    """Unrolled N-pivot Gauss-Jordan elimination in Pure for general non-triangular square systems ``J * X = B``."""
    init_cols: list[str] = []
    for i in range(n):
        active_set = set(active_jac[i])
        for k in range(n):
            init_cols.append(
                f'a_{i}_{k}: x|$x.j_{i}_{k}->toOne()' if k in active_set else f'a_{i}_{k}: x|0.0'
            )
    for m_idx, (_, col_exprs) in enumerate(rhs_cols):
        for i in range(n):
            init_cols.append(f'b_{i}_{m_idx}: x|{col_exprs[i]}')
    stages = f"->extend(~[{', '.join(init_cols)}]){_SQL_STAGE_BARRIER}"
    m_count = len(rhs_cols)
    for c in range(n):
        proj = [f'{scenario_col}: x|$x.{scenario_col}'] + [f'{col}: x|$x.{col}->toOne()' for col in passthrough_cols]
        piv = f'$x.a_{c}_{c}->toOne()'
        for k in range(c + 1, n):
            proj.append(f'a_{c}_{k}: x|($x.a_{c}_{k}->toOne() / {piv})')
            for r in range(n):
                if r != c:
                    proj.append(
                        f'a_{r}_{k}: x|($x.a_{r}_{k}->toOne() - ($x.a_{r}_{c}->toOne() / {piv}) * $x.a_{c}_{k}->toOne())'
                    )
        for m_idx in range(m_count):
            proj.append(f'b_{c}_{m_idx}: x|($x.b_{c}_{m_idx}->toOne() / {piv})')
            for r in range(n):
                if r != c:
                    proj.append(
                        f'b_{r}_{m_idx}: x|($x.b_{r}_{m_idx}->toOne() - ($x.a_{r}_{c}->toOne() / {piv}) * $x.b_{c}_{m_idx}->toOne())'
                    )
        stages += f"->project(~[{', '.join(proj)}]){_SQL_STAGE_BARRIER}"
    out_names = [f'b_{i}_{m_idx}' for i in range(n) for m_idx in range(m_count)]
    return stages, out_names


def _dag_stages_pure(
    expr_items: Sequence[tuple[str, expr.Expr]],
    var_map: Mapping[str, str],
    *,
    keep_cols: Sequence[str] | None = None,
    scenario_col: str | None = None,
    barrier: str = _SQL_STAGE_BARRIER,
) -> str:
    """Renders hash-consed scalar micro-expressions (``expr.Expr``) into compact staged Pure blocks.

    Stages shared transcendental/basis nodes (e.g. ``exp(-r * t)`` discount factors) and large shared
    subtrees (e.g. multi-coupon annuity and floating-leg sums) into ``u_k`` columns, and immediately drops
    ``u_k`` via ``project`` when ``keep_cols`` is provided so downstream relational typechecking stays narrow.
    """
    counts: dict[expr.Expr, int] = {}
    order: list[expr.Expr] = []
    visited: set[expr.Expr] = set()

    def visit(node: expr.Expr) -> None:
        counts[node] = counts.get(node, 0) + 1
        if node in visited:
            return
        visited.add(node)
        for ch in node.children():
            visit(ch)
        order.append(node)

    for _, root in expr_items:
        visit(root)

    resolve = lambda v: var_map[v.name]
    u_map: dict[expr.Expr, str] = {}
    u_levels: dict[expr.Expr, int] = {}
    rendered: dict[expr.Expr, str] = {}
    stages: dict[int, list[str]] = {}

    for node in order:
        ch_strs = [f'$x.{u_map[c]}->toOne()' if c in u_map else rendered[c] for c in node.children()]
        rhs = node._pure(ch_strs, resolve)
        max_child_lvl = max((u_levels[c] for c in node.children()), default=-1)
        is_transcendental = isinstance(node, (expr.Exp, expr.Log, expr.Sqrt, expr.Erf)) or (
            isinstance(node, basis.BasisNode) and node.op != basis.LERP
        )
        should_stage = (
            not isinstance(node, (expr.Const, expr.Var))
            and counts[node] >= 2
            and (is_transcendental or (counts[node] - 1) * len(rhs) >= 28)
        )
        if should_stage:
            lvl = 1 + max_child_lvl
            uid = f'u_{len(u_map)}'
            u_map[node] = uid
            u_levels[node] = lvl
            stages.setdefault(lvl, []).append(f'{uid}: x|{rhs}')
        else:
            rendered[node] = rhs
            u_levels[node] = max_child_lvl

    sorted_lvls = sorted(stages)
    parts = [
        f"->extend(~[{', '.join(stages[lvl])}]){barrier if (idx % 3 == 2 or idx == len(sorted_lvls) - 1) else ''}"
        for idx, lvl in enumerate(sorted_lvls)
    ]
    out_cols = [
        f"{name}: x|{f'$x.{u_map[root]}->toOne()' if root in u_map else rendered[root]}"
        for name, root in expr_items
    ]
    if keep_cols is not None:
        kept = ([f'{scenario_col}: x|$x.{scenario_col}'] if scenario_col is not None else []) + [
            f'{col}: x|$x.{col}->toOne()' for col in keep_cols
        ]
        parts.append(f"->project(~[{', '.join(kept + out_cols)}]){barrier}")
    else:
        parts.append(f"->extend(~[{', '.join(out_cols)}]){barrier}")
    return ''.join(parts)


def newton_step_pure(
    fitter: CurveFitter | MultiCurveFitter,
    prefix: str = '',
    *,
    scenario_col: str = 'scenario_id',
    use_pure_diff: bool = False,
) -> str:
    """Pure relation step executing one exact SQL Newton iteration ``r <- r + J(r)^-1 * (q - par(r))``.

    By default (``use_pure_diff=False``), factors the hash-consed scalar micro-expression DAG across all par rates
    and symbolic partial derivatives ``fitter.jacobian`` into compact staged Pure columns; when ``use_pure_diff=True``,
    emits Pure's native ``diff()`` (``expr.to_pure_diff``) for ``SymbolicDiff`` + ``ProjectionCse``.
    """
    n = len(fitter.factors)
    var_map = {f.knot.name: f'$x.r_{i}->toOne()' for i, f in enumerate(fitter.factors)}
    is_lower, active_jac, off_deps, levels, _ = _jacobian_topology(fitter)
    keep_state = [f'q_{i}' for i in range(n)] + [f'r_{i}' for i in range(n)]

    if use_pure_diff:
        eval_cols: list[str] = []
        for i in range(n):
            eval_cols.append(f'p_{i}: x|{expr.to_pure(fitter.pars[i], var_map=var_map, as_lambda=False)}')
            for k in active_jac[i]:
                eval_cols.append(
                    f'j_{i}_{k}: x|{expr.to_pure_diff(fitter.pars[i], fitter.factors[k].knot, var_map=var_map, as_lambda=False)}'
                )
        out = f"{prefix}->extend(~[{', '.join(eval_cols)}]){_SQL_STAGE_BARRIER}"
    else:
        expr_items: list[tuple[str, expr.Expr]] = [(f'p_{i}', fitter.pars[i]) for i in range(n)]
        for i in range(n):
            for k in active_jac[i]:
                expr_items.append((f'j_{i}_{k}', fitter.jacobian[i][k]))
        out = prefix + _dag_stages_pure(expr_items, var_map, keep_cols=keep_state, scenario_col=scenario_col)

    if is_lower:
        max_lvl = max(levels, default=0)
        for lvl in range(max_lvl + 1):
            step_cols: list[str] = []
            for i in range(n):
                if levels[i] != lvl:
                    continue
                rhs = f'($x.q_{i}->toOne() - $x.p_{i}->toOne())'
                if off_deps[i]:
                    sub = ' + '.join(f'($x.j_{i}_{k}->toOne() * $x.s_{k}->toOne())' for k in off_deps[i])
                    num = f'({rhs} - ({sub}))'
                else:
                    num = rhs
                step_cols.append(f's_{i}: x|({num} / $x.j_{i}_{i}->toOne())')
            if step_cols:
                out += f"->extend(~[{', '.join(step_cols)}])"
                if lvl == 2 and lvl < max_lvl:
                    out += _SQL_STAGE_BARRIER
        s_refs = [f'$x.s_{i}->toOne()' for i in range(n)]
    else:
        rhs = [('rhs', [f'($x.q_{i}->toOne() - $x.p_{i}->toOne())' for i in range(n)])]
        gj_stages, _ = _gauss_jordan_pure(n, active_jac, rhs, keep_state, scenario_col)
        out += gj_stages
        s_refs = [f'$x.b_{i}_0->toOne()' for i in range(n)]

    max_expr = f'({s_refs[0]})->abs()'
    for i in range(1, n):
        max_expr = f'({max_expr})->max(({s_refs[i]})->abs())'
    proj_cols = [f'{scenario_col}: x|$x.{scenario_col}']
    proj_cols += [f'q_{i}: x|$x.q_{i}->toOne()' for i in range(n)]
    proj_cols += [f'r_{i}: x|($x.r_{i}->toOne() + {s_refs[i]})' for i in range(n)]
    proj_cols.append(f'max_step_bps: x|({max_expr} * 10000.0)')
    return f"{out}->project(~[{', '.join(proj_cols)}]){_SQL_STAGE_BARRIER}"


def inverse_jacobian_pure(
    fitter: CurveFitter | MultiCurveFitter,
    prefix: str = '',
    *,
    scenario_col: str = 'scenario_id',
    include_rates: bool = True,
    use_pure_diff: bool = False,
) -> str:
    """Pure relation step computing the inverse Jacobian ``W = J(r)^-1 = d(pillar)/d(quote)`` 100% in SQL."""
    n = len(fitter.factors)
    var_map = {f.knot.name: f'$x.r_{i}->toOne()' for i, f in enumerate(fitter.factors)}
    is_lower, active_jac, off_deps, levels, reachable = _jacobian_topology(fitter)
    keep_rates = [f'r_{i}' for i in range(n)] if include_rates else []

    if use_pure_diff:
        eval_cols = [
            f'j_{i}_{k}: x|{expr.to_pure_diff(fitter.pars[i], fitter.factors[k].knot, var_map=var_map, as_lambda=False)}'
            for i in range(n)
            for k in active_jac[i]
        ]
        out = f"{prefix}->extend(~[{', '.join(eval_cols)}]){_SQL_STAGE_BARRIER}"
    else:
        expr_items = [
            (f'j_{i}_{k}', fitter.jacobian[i][k])
            for i in range(n)
            for k in active_jac[i]
        ]
        out = prefix + _dag_stages_pure(expr_items, var_map, keep_cols=keep_rates, scenario_col=scenario_col)

    proj_cols = [f'{scenario_col}: x|$x.{scenario_col}']
    if include_rates:
        proj_cols += [f'r_{i}: x|$x.r_{i}->toOne()' for i in range(n)]

    if is_lower:
        max_lvl = max(levels, default=0)
        for lvl in range(max_lvl + 1):
            w_cols: list[str] = []
            for i in range(n):
                if levels[i] != lvl:
                    continue
                for m in reachable[i]:
                    if m == i:
                        w_val = f'(1.0 / $x.j_{i}_{i}->toOne())'
                    else:
                        ks = [k for k in off_deps[i] if m in reachable[k]]
                        terms = ' + '.join(f'($x.j_{i}_{k}->toOne() * $x.w_{k}_{m}->toOne())' for k in ks)
                        w_val = f'((-1.0 * ({terms})) / $x.j_{i}_{i}->toOne())'
                    w_cols.append(f'w_{i}_{m}: x|{w_val}')
            if w_cols:
                out += f"->extend(~[{', '.join(w_cols)}])"
                if lvl == 2 and lvl < max_lvl:
                    out += _SQL_STAGE_BARRIER
        for i in range(n):
            for m in reachable[i]:
                proj_cols.append(f'w_{i}_{m}: x|$x.w_{i}_{m}->toOne()')
    else:
        rhs = [(f'e_{m}', ['1.0' if i == m else '0.0' for i in range(n)]) for m in range(n)]
        gj_stages, _ = _gauss_jordan_pure(n, active_jac, rhs, keep_rates, scenario_col)
        out += gj_stages
        for i in range(n):
            for m in range(n):
                proj_cols.append(f'w_{i}_{m}: x|$x.b_{i}_{m}->toOne()')

    return f"{out}->project(~[{', '.join(proj_cols)}])"


def _pure_index_mux(index_expr: str, choices: Sequence[tuple[int, str]], default: str = '0.0') -> str:
    """Builds a Pure ``if(idx == k, |val_k, |...)`` selector expression."""
    acc = default
    for idx, val in reversed(choices):
        acc = f'if({index_expr} == {idx}, |{val}, |{acc})'
    return acc


def fitted_pillars_pure(
    fitter: CurveFitter | MultiCurveFitter,
    prefix: str = '$input',
    *,
    scenario_col: str = 'scenario_id',
    index_col: str = 'factor_idx',
) -> str:
    """Pure relation step unpivoting a cross-joined ``(pillar_meta x converged_state)`` relation into diagnostic
    pillar rows with fitted zero rates, discount factors, repriced par rates, and calibration residuals in SQL."""
    n = len(fitter.factors)
    var_map = {f.knot.name: f'$x.r_{i}->toOne()' for i, f in enumerate(fitter.factors)}
    par_stages = _dag_stages_pure([(f'par_{k}', fitter.pars[k]) for k in range(n)], var_map)
    idx_ref = f'$x.{index_col}'
    q_mux = _pure_index_mux(idx_ref, [(k, f'$x.q_{k}->toOne()') for k in range(n)])
    r_mux = _pure_index_mux(idx_ref, [(k, f'$x.r_{k}->toOne()') for k in range(n)])
    p_mux = _pure_index_mux(idx_ref, [(k, f'$x.par_{k}->toOne()') for k in range(n)])
    return (
        f'{prefix}{par_stages}->extend(~['
        f'market_quote_rate: x|{q_mux}, '
        f'fitted_zero_rate: x|{r_mux}, '
        f'repriced_par_rate: x|{p_mux}'
        f']){_SQL_STAGE_BARRIER}->project(~['
        f'{scenario_col}: x|$x.{scenario_col}, '
        f'factor_idx: x|$x.{index_col}->toOne(), '
        f'factor_id: x|$x.factor_id->toOne(), '
        f'series_id: x|$x.series_id->toOne(), '
        f'knot_index: x|$x.knot_index->toOne(), '
        f'curve_id: x|$x.curve_id->toOne(), '
        f'pillar: x|$x.pillar->toOne(), '
        f'tenor_years: x|$x.tenor_years->toOne(), '
        f'quote_id: x|$x.quote_id->toOne(), '
        f'market_quote_bps: x|($x.market_quote_rate->toOne() * 10000.0), '
        f'fitted_zero_rate: x|$x.fitted_zero_rate->toOne(), '
        f'fitted_zero_rate_bps: x|($x.fitted_zero_rate->toOne() * 10000.0), '
        f'zero_vs_par_spread_bps: x|(($x.fitted_zero_rate->toOne() - $x.market_quote_rate->toOne()) * 10000.0), '
        f'discount_factor: x|(-1.0 * $x.fitted_zero_rate->toOne() * $x.tenor_years->toOne())->exp(), '
        f'repriced_par_bps: x|($x.repriced_par_rate->toOne() * 10000.0), '
        f'fit_error_bps: x|(($x.repriced_par_rate->toOne() - $x.market_quote_rate->toOne()) * 10000.0)'
        f'])'
    )


def unpivot_jacobian_pure(
    fitter: CurveFitter | MultiCurveFitter,
    prefix: str = '$input',
    *,
    scenario_col: str = 'scenario_id',
    factor_idx_col: str = 'factor_idx',
    quote_idx_col: str = 'quote_idx',
) -> str:
    """Pure relation step unpivoting ``(jacobian_pairs x inverse_jacobian_state)`` into ``d_pillar_d_quote`` rows."""
    n = len(fitter.factors)
    _, _, _, _, reachable = _jacobian_topology(fitter)
    row_exprs: list[tuple[int, str]] = []
    q_ref = f'$x.{quote_idx_col}'
    for i in range(n):
        col_choices = [(m, f'$x.w_{i}_{m}->toOne()') for m in reachable[i]]
        if col_choices:
            row_exprs.append((i, _pure_index_mux(q_ref, col_choices)))
    w_mux = _pure_index_mux(f'$x.{factor_idx_col}', row_exprs)
    return (
        f'{prefix}->project(~['
        f'{scenario_col}: x|$x.{scenario_col}, '
        f'factor_id: x|$x.factor_id->toOne(), '
        f'input_id: x|$x.input_id->toOne(), '
        f'input_coord: x|$x.input_coord->toOne(), '
        f'pillar_curve: x|$x.pillar_curve->toOne(), '
        f'pillar: x|$x.pillar->toOne(), '
        f'pillar_tenor: x|$x.pillar_tenor->toOne(), '
        f'quote_curve: x|$x.quote_curve->toOne(), '
        f'quote_id: x|$x.quote_id->toOne(), '
        f'quote_tenor: x|$x.quote_tenor->toOne(), '
        f'd_pillar_d_quote: x|{w_mux}'
        f'])'
    )


@dataclass
class CalibrationParInstrument(Instrument):
    """An instrument whose ``npv()`` expression tree is its par rate, allowing ``FirstOrderAnalyticRisk``
    and ``BasisBook`` to differentiate and evaluate calibration par rates and ``d(par)/d(pillar)``
    sensitivities using the exact same Pure ``diff()`` and DuckDB SQL pipeline as the portfolio risk engine."""

    swap: Instrument
    discount_curve: ZeroCurve
    projection_curve: ZeroCurve | None = None
    book: str = 'CALIBRATION'

    @property
    def trade_id(self) -> str:
        return self.swap.trade_id

    def curves(self, default_curves: Sequence[ZeroCurve] = ()) -> list[ZeroCurve]:
        if self.projection_curve is not None and self.projection_curve.curve_id != self.discount_curve.curve_id:
            return [self.discount_curve, self.projection_curve]
        return [self.discount_curve]

    def npv(self, *curves: ZeroCurve) -> expr.Expr:
        if self.projection_curve is not None:
            return self.swap.par_rate(self.discount_curve, self.projection_curve)
        return self.swap.par_rate(self.discount_curve)


_SKINNY_OP_CODES: dict[str, int] = {
    basis.CONST: 0,
    basis.LERP: 1,
    basis.EXP_NEG_XT: 2,
    basis.RECIP_1PXT: 3,
}
_SKINNY_MAX_DEGREE = 4
_SKINNY_KERNEL_CACHE: dict[str, dict[str, Any]] = {}


@dataclass(frozen=True)
class CalibrationBasisTables:
    """Scenario-independent skinny Arrow tables encoding a fitter's par-rate numerators, denominators,
    partial derivatives, and active Jacobian sparsity coordinates."""

    cal_shared_node: Any
    cal_poly_slot: Any
    cal_jac_coords: Any
    is_lower: bool
    max_level: int
    step1_schulz_iters: int
    reachable: list[list[int]]


def _diff_basis_poly_by_factor(
    poly: basis.Polynomial,
    factor_lookup: Mapping[tuple[str, int], int],
) -> dict[int, basis.Polynomial]:
    """Differentiates an expanded ``BasisNode`` polynomial simultaneously wrt all active global factors in one pass."""
    out: dict[int, dict[tuple, float]] = {}
    for m, coeff in poly.items():
        if abs(coeff) <= 1e-18 or not m:
            continue
        for pos, factor in enumerate(m):
            if not isinstance(factor, basis.BasisNode) or factor.op == basis.CONST:
                continue
            if factor.k0 == factor.k1:
                g0 = factor_lookup.get((factor.series, factor.k0))
                deps = [(g0, 1.0)] if g0 is not None else []
            else:
                deps = []
                g0 = factor_lookup.get((factor.series, factor.k0))
                if g0 is not None and factor.weight < 1.0:
                    deps.append((g0, 1.0 - factor.weight))
                g1 = factor_lookup.get((factor.series, factor.k1))
                if g1 is not None and factor.weight > 0.0:
                    deps.append((g1, factor.weight))
            if not deps:
                continue
            prefix_m = m[:pos]
            suffix_m = m[pos + 1:]
            if factor.op == basis.LERP:
                repl: tuple[basis.BasisNode, ...] = ()
                scale = 1.0
            elif factor.op == basis.EXP_NEG_XT:
                repl = (factor._bare(),)
                scale = -factor.t
            else:
                bare = factor._bare()
                repl = (bare, bare)
                scale = -factor.t
            dm = basis._combine_monomial(prefix_m + repl + suffix_m)
            for g_idx, w_dx in deps:
                dc = coeff * scale * w_dx
                if abs(dc) > 1e-18:
                    p_g = out.setdefault(g_idx, {})
                    p_g[dm] = p_g.get(dm, 0.0) + dc
    return out


def build_calibration_basis_tables(fitter: CurveFitter | MultiCurveFitter) -> CalibrationBasisTables:
    """Expands ``fitter.pars`` and all active partial derivatives into fixed-width skinny Arrow tables."""
    require_datacube()
    import pyarrow as pa

    n = len(fitter.factors)
    if n == 0:
        raise ValueError('fitter must have at least one calibration factor')
    is_lower, active_jac, _, levels, reachable = _jacobian_topology(fitter)
    max_lvl = max(levels, default=0) if is_lower else n
    if is_lower:
        step1_iters = max(0, math.ceil(math.log2(max(2, max_lvl + 1))) - 1)
    else:
        step1_iters = 7

    factor_lookup = {(f.curve.curve_id, f.index): idx for idx, f in enumerate(fitter.factors)}
    node_ids: dict[basis.BasisNode, int] = {}
    sn_id: list[int] = []
    sn_op: list[int] = []
    sn_k0: list[int] = []
    sn_k1: list[int] = []
    sn_w1: list[float] = []
    sn_pt: list[float] = []
    sn_off: list[float] = []

    def _intern_node(nd: basis.BasisNode) -> int:
        found = node_ids.get(nd)
        if found is not None:
            return found
        nid = len(sn_id)
        node_ids[nd] = nid
        sn_id.append(nid)
        sn_op.append(_SKINNY_OP_CODES[nd.op])
        if nd.op == basis.CONST:
            sn_k0.append(0)
            sn_k1.append(0)
        else:
            sn_k0.append(factor_lookup[(nd.series, nd.k0)])
            sn_k1.append(factor_lookup[(nd.series, nd.k1)])
        sn_w1.append(float(nd.weight))
        sn_pt.append(float(nd.t))
        sn_off.append(float(nd.offset))
        return nid

    unit_nid = _intern_node(basis.UNIT)

    ps_q: list[int] = []
    ps_role: list[int] = []
    ps_col: list[int] = []
    ps_seq: list[int] = []
    ps_deg: list[int] = []
    ps_coeff: list[float] = []
    ps_slot: list[int] = []
    ps_nid: list[int] = []

    def _emit_poly(q_idx: int, role: int, col_k: int, poly: basis.Polynomial, *, ensure_row: bool) -> None:
        items = [(m, float(c)) for m, c in poly.items() if abs(c) > 1e-18]
        items.sort(key=lambda mc: tuple(basis._factor_sort_key(f) for f in mc[0]))
        if not items:
            if ensure_row:
                ps_q.append(q_idx)
                ps_role.append(role)
                ps_col.append(col_k)
                ps_seq.append(0)
                ps_deg.append(1)
                ps_coeff.append(0.0)
                ps_slot.append(0)
                ps_nid.append(unit_nid)
            return
        for seq_idx, (monom, c_val) in enumerate(items):
            if not monom:
                slot_nids = [unit_nid]
            else:
                if len(monom) > _SKINNY_MAX_DEGREE or not all(isinstance(f, basis.BasisNode) for f in monom):
                    raise basis.BasisError(
                        f'calibration par polynomial term exceeds skinny BasisNode degree {_SKINNY_MAX_DEGREE}: {monom!r}'
                    )
                slot_nids = [_intern_node(f) for f in monom]
            deg = len(slot_nids)
            for s_idx, nid in enumerate(slot_nids):
                ps_q.append(q_idx)
                ps_role.append(role)
                ps_col.append(col_k)
                ps_seq.append(seq_idx)
                ps_deg.append(deg)
                ps_coeff.append(c_val)
                ps_slot.append(s_idx)
                ps_nid.append(nid)

    poly_memo: dict[expr.Expr, basis.Polynomial] = {}
    const_memo: dict[expr.Expr, bool] = {}
    jc_row: list[int] = []
    jc_col: list[int] = []

    for i, par in enumerate(fitter.pars):
        if isinstance(par, expr.Div):
            num_expr, den_expr = par.a, par.b
        else:
            num_expr, den_expr = par, expr.ONE
        p_num = basis.expand(num_expr, poly_memo, const_memo)
        p_den = basis.expand(den_expr, poly_memo, const_memo)
        _emit_poly(i, 0, -1, p_num, ensure_row=True)
        _emit_poly(i, 1, -1, p_den, ensure_row=True)

        d_num_map = _diff_basis_poly_by_factor(p_num, factor_lookup)
        d_den_map = _diff_basis_poly_by_factor(p_den, factor_lookup)
        active_set = set(active_jac[i])
        if i not in active_set:
            active_set.add(i)
        for k in sorted(active_set):
            jc_row.append(i)
            jc_col.append(k)
            if k in d_num_map:
                _emit_poly(i, 2, k, d_num_map[k], ensure_row=False)
            if k in d_den_map:
                _emit_poly(i, 3, k, d_den_map[k], ensure_row=False)

    cal_shared_node = pa.table({
        'node_id': pa.array(sn_id, type=pa.int64()),
        'op_code': pa.array(sn_op, type=pa.int64()),
        'k0': pa.array(sn_k0, type=pa.int64()),
        'k1': pa.array(sn_k1, type=pa.int64()),
        'weight_1': pa.array(sn_w1, type=pa.float64()),
        'param_t': pa.array(sn_pt, type=pa.float64()),
        'const_offset': pa.array(sn_off, type=pa.float64()),
    })
    cal_poly_slot = pa.table({
        'quote_idx': pa.array(ps_q, type=pa.int64()),
        'role': pa.array(ps_role, type=pa.int64()),
        'col_k': pa.array(ps_col, type=pa.int64()),
        'term_seq': pa.array(ps_seq, type=pa.int64()),
        'term_degree': pa.array(ps_deg, type=pa.int64()),
        'coeff': pa.array(ps_coeff, type=pa.float64()),
        'slot_idx': pa.array(ps_slot, type=pa.int64()),
        'node_id': pa.array(ps_nid, type=pa.int64()),
    })
    cal_jac_coords = pa.table({
        'row_i': pa.array(jc_row, type=pa.int64()),
        'col_k': pa.array(jc_col, type=pa.int64()),
    })
    return CalibrationBasisTables(
        cal_shared_node=cal_shared_node,
        cal_poly_slot=cal_poly_slot,
        cal_jac_coords=cal_jac_coords,
        is_lower=is_lower,
        max_level=max_lvl,
        step1_schulz_iters=step1_iters,
        reachable=reachable,
    )


def _skinny_pure_join(
    lhs: str,
    rhs: str,
    pairs: Sequence[tuple[str, str]],
    rhs_keep: Sequence[str | tuple[str, str]],
    kind: str = 'INNER',
    *,
    tag: str = 'j',
) -> str:
    """Builds a Pure ``->join(...)`` renaming right-hand join keys and payload columns cleanly."""
    key_tag = f'{tag}_{len(lhs)}'
    proj = [f'__rk_{key_tag}_{i}: x|$x.{r_col}' for i, (_l, r_col) in enumerate(pairs)]
    for item in rhs_keep:
        if isinstance(item, tuple):
            alias, src = item
            proj.append(f'{alias}: x|$x.{src}')
        else:
            proj.append(f'{item}: x|$x.{item}')
    cond = ' && '.join(f'($l.{l_col} == $r.__rk_{key_tag}_{i})' for i, (l_col, _r) in enumerate(pairs))
    rhs_proj = f"{rhs}->project(~[{', '.join(proj)}])"
    return f'{lhs}->join({rhs_proj}, meta::pure::functions::relation::JoinKind.{kind}, {{l, r | {cond}}})'


def skinny_seed_knots_pure(
    quotes_acc: str,
    *,
    quote_idx_col: str = 'quote_idx',
    quote_rate_col: str = 'quote_rate',
    shift_rate_expr: str | None = None,
) -> str:
    """Pure relation seeding skinny ``(scenario_id, factor_idx, r_val, s1_val)`` directly from par quotes."""
    q_expr = f'$x.{quote_rate_col}->toOne()'
    if shift_rate_expr is not None:
        q_expr = f'({q_expr} + {shift_rate_expr})'
    return (
        f'{quotes_acc}->project(~['
        f'scenario_id: x|$x.scenario_id, '
        f'factor_idx: x|$x.{quote_idx_col}->toOne(), '
        f'r_val: x|{q_expr}, '
        f's1_val: x|0.0'
        f'])'
    )


def skinny_eval_nodes_pure(nodes_acc: str, knots_acc: str) -> str:
    """Pure relation evaluating all shared calibration ``BasisNode``s across all scenarios via two skinny joins."""
    j0 = _skinny_pure_join(
        nodes_acc,
        knots_acc,
        [('k0', 'factor_idx')],
        ['scenario_id', ('r0', 'r_val')],
    )
    j1 = _skinny_pure_join(
        j0,
        knots_acc,
        [('scenario_id', 'scenario_id'), ('k1', 'factor_idx')],
        [('r1', 'r_val')],
    )
    return (
        f'{j1}->extend(~['
        f'x_val: x|(((1.0 - $x.weight_1->toOne()) * $x.r0->toOne()) + ($x.weight_1->toOne() * $x.r1->toOne()))'
        f']){_SQL_STAGE_BARRIER}->project(~['
        f'scenario_id: x|$x.scenario_id, '
        f'node_id: x|$x.node_id->toOne(), '
        f'node_val: x|if($x.op_code == 0, |$x.const_offset->toOne(), '
        f'|if($x.op_code == 1, |($x.x_val->toOne() + $x.const_offset->toOne()), '
        f'|if($x.op_code == 2, |((-1.0 * $x.x_val->toOne() * $x.param_t->toOne())->exp() + $x.const_offset->toOne()), '
        f'|(1.0 / (1.0 + ($x.x_val->toOne() * $x.param_t->toOne()))) + $x.const_offset->toOne())))'
        f'])'
    )


def skinny_eval_polys_pure(nodes_acc: str, slots_acc: str, knots_acc: str) -> str:
    """Pure relation evaluating all calibration par numerators, denominators, and active partial derivatives in SQL."""
    nodes_rel = skinny_eval_nodes_pure(nodes_acc, knots_acc)
    joined = _skinny_pure_join(
        slots_acc,
        nodes_rel,
        [('node_id', 'node_id')],
        ['scenario_id', 'node_val'],
    )
    return (
        f'{joined}->extend(~['
        f's0: x|if($x.slot_idx == 0, |$x.node_val->toOne(), |0.0), '
        f's1: x|if($x.slot_idx == 1, |$x.node_val->toOne(), |0.0), '
        f's2: x|if($x.slot_idx == 2, |$x.node_val->toOne(), |0.0), '
        f's3: x|if($x.slot_idx == 3, |$x.node_val->toOne(), |0.0)'
        f']){_SQL_STAGE_BARRIER}->groupBy('
        f'~[scenario_id, quote_idx, role, col_k, term_seq, term_degree, coeff], '
        f'~['
        f'va: x|$x.s0 : y|$y->plus(), '
        f'vb: x|$x.s1 : y|$y->plus(), '
        f'vc: x|$x.s2 : y|$y->plus(), '
        f'vd: x|$x.s3 : y|$y->plus()'
        f']){_SQL_STAGE_BARRIER}->extend(~['
        f'term_val: x|if($x.term_degree == 1, |($x.coeff->toOne() * $x.va->toOne()), '
        f'|if($x.term_degree == 2, |(($x.coeff->toOne() * $x.va->toOne()) * $x.vb->toOne()), '
        f'|if($x.term_degree == 3, |((($x.coeff->toOne() * $x.va->toOne()) * $x.vb->toOne()) * $x.vc->toOne()), '
        f'|(((($x.coeff->toOne() * $x.va->toOne()) * $x.vb->toOne()) * $x.vc->toOne()) * $x.vd->toOne()))))'
        f']){_SQL_STAGE_BARRIER}->groupBy('
        f'~[scenario_id, quote_idx, role, col_k], '
        f'~[poly_val: x|$x.term_val : y|$y->plus()]'
        f')'
    )


def skinny_residual_pure(
    quotes_acc: str,
    polys_acc: str,
    *,
    quote_idx_col: str = 'quote_idx',
    quote_rate_col: str = 'quote_rate',
) -> str:
    """Pure relation computing par rates ``F_i / A_i`` and calibration residuals ``q_i - par_i`` per quote."""
    num_rel = f'{polys_acc}->filter(x|$x.role == 0)'
    den_rel = f'{polys_acc}->filter(x|$x.role == 1)'
    j_num = _skinny_pure_join(
        quotes_acc,
        num_rel,
        [('scenario_id', 'scenario_id'), (quote_idx_col, 'quote_idx')],
        [('num_val', 'poly_val')],
    )
    j_den = _skinny_pure_join(
        j_num,
        den_rel,
        [('scenario_id', 'scenario_id'), (quote_idx_col, 'quote_idx')],
        [('den_val', 'poly_val')],
    )
    return (
        f'{j_den}->extend(~['
        f'par_calc: x|($x.num_val->toOne() / $x.den_val->toOne())'
        f']){_SQL_STAGE_BARRIER}->project(~['
        f'scenario_id: x|$x.scenario_id, '
        f'quote_idx: x|$x.{quote_idx_col}->toOne(), '
        f'par_val: x|$x.par_calc->toOne(), '
        f'residual: x|($x.{quote_rate_col}->toOne() - $x.par_calc->toOne())'
        f'])'
    )


def skinny_jacobian_pure(coords_acc: str, polys_acc: str) -> str:
    """Pure relation assembling sparse Jacobian entries ``J_{i,k} = (F'_{i,k} A_i - F_i A'_{i,k}) / A_i^2``."""
    num_rel = f'{polys_acc}->filter(x|$x.role == 0)'
    den_rel = f'{polys_acc}->filter(x|$x.role == 1)'
    dnum_rel = f'{polys_acc}->filter(x|$x.role == 2)'
    dden_rel = f'{polys_acc}->filter(x|$x.role == 3)'

    j1 = _skinny_pure_join(coords_acc, num_rel, [('row_i', 'quote_idx')], ['scenario_id', ('num_val', 'poly_val')])
    j2 = _skinny_pure_join(
        j1,
        den_rel,
        [('scenario_id', 'scenario_id'), ('row_i', 'quote_idx')],
        [('den_val', 'poly_val')],
    )
    j3 = _skinny_pure_join(
        j2,
        dnum_rel,
        [('scenario_id', 'scenario_id'), ('row_i', 'quote_idx'), ('col_k', 'col_k')],
        [('dnum_raw', 'poly_val')],
        kind='LEFT',
    )
    j4 = _skinny_pure_join(
        j3,
        dden_rel,
        [('scenario_id', 'scenario_id'), ('row_i', 'quote_idx'), ('col_k', 'col_k')],
        [('dden_raw', 'poly_val')],
        kind='LEFT',
    )
    return (
        f'{j4}->extend(~['
        f'dnum_v: x|if($x.dnum_raw->isEmpty(), |0.0, |$x.dnum_raw->toOne()), '
        f'dden_v: x|if($x.dden_raw->isEmpty(), |0.0, |$x.dden_raw->toOne())'
        f']){_SQL_STAGE_BARRIER}->project(~['
        f'scenario_id: x|$x.scenario_id, '
        f'row_i: x|$x.row_i->toOne(), '
        f'col_k: x|$x.col_k->toOne(), '
        f'j_val: x|((($x.dnum_v->toOne() * $x.den_val->toOne()) - ($x.num_val->toOne() * $x.dden_v->toOne())) '
        f'/ ($x.den_val->toOne() * $x.den_val->toOne()))'
        f'])'
    )


def skinny_init_w0_pure(jac_acc: str) -> str:
    """Pure relation initializing 1st-order Neumann inverse ``W_0 = (I + L) D^-1`` from sparse ``J``."""
    diag_rel = f'{jac_acc}->filter(x|$x.row_i == $x.col_k)'
    j1 = _skinny_pure_join(
        jac_acc,
        diag_rel,
        [('scenario_id', 'scenario_id'), ('row_i', 'row_i')],
        [('j_ii', 'j_val')],
    )
    j2 = _skinny_pure_join(
        j1,
        diag_rel,
        [('scenario_id', 'scenario_id'), ('col_k', 'row_i')],
        [('j_kk', 'j_val')],
    )
    return (
        f'{j2}->project(~['
        f'scenario_id: x|$x.scenario_id, '
        f'row_i: x|$x.row_i->toOne(), '
        f'col_j: x|$x.col_k->toOne(), '
        f'w_val: x|if($x.row_i->toOne() == $x.col_k->toOne(), '
        f'|(1.0 / $x.j_ii->toOne()), '
        f'|((-1.0 * $x.j_val->toOne()) / ($x.j_ii->toOne() * $x.j_kk->toOne())))'
        f'])'
    )


def skinny_schulz_step_pure(w_acc: str, jac_acc: str) -> str:
    """Pure relation executing one sparse Newton-Schulz / pointer-doubling step ``W_next = W * (2I - J * W)``."""
    jw_join = _skinny_pure_join(
        jac_acc,
        w_acc,
        [('scenario_id', 'scenario_id'), ('col_k', 'row_i')],
        [('jw_col', 'col_j'), ('w_rhs', 'w_val')],
    )
    m_rel = (
        f'{jw_join}->extend(~[prod: x|($x.j_val->toOne() * $x.w_rhs->toOne())])'
        f'{_SQL_STAGE_BARRIER}->groupBy(~[scenario_id, row_i, jw_col], ~[jw_val: x|$x.prod : y|$y->plus()])'
        f'{_SQL_STAGE_BARRIER}->project(~['
        f'scenario_id: x|$x.scenario_id, '
        f'm_row: x|$x.row_i->toOne(), '
        f'm_col: x|$x.jw_col->toOne(), '
        f'm_val: x|if($x.row_i->toOne() == $x.jw_col->toOne(), |(2.0 - $x.jw_val->toOne()), |(0.0 - $x.jw_val->toOne()))'
        f'])'
    )
    wm_join = _skinny_pure_join(
        w_acc,
        m_rel,
        [('scenario_id', 'scenario_id'), ('col_j', 'm_row')],
        [('out_col', 'm_col'), 'm_val'],
    )
    return (
        f'{wm_join}->extend(~[wprod: x|($x.w_val->toOne() * $x.m_val->toOne())])'
        f'{_SQL_STAGE_BARRIER}->groupBy(~[scenario_id, row_i, out_col], ~[w_next: x|$x.wprod : y|$y->plus()])'
        f'{_SQL_STAGE_BARRIER}->project(~['
        f'scenario_id: x|$x.scenario_id, '
        f'row_i: x|$x.row_i->toOne(), '
        f'col_j: x|$x.out_col->toOne(), '
        f'w_val: x|$x.w_next->toOne()'
        f'])'
    )


def _skinny_matvec_pure(w_acc: str, res_acc: str, out_col: str) -> str:
    joined = _skinny_pure_join(
        w_acc,
        res_acc,
        [('scenario_id', 'scenario_id'), ('col_j', 'quote_idx')],
        [('res_v', 'residual')],
    )
    return (
        f'{joined}->extend(~[term: x|($x.w_val->toOne() * $x.res_v->toOne())])'
        f'{_SQL_STAGE_BARRIER}->groupBy(~[scenario_id, row_i], ~[{out_col}: x|$x.term : y|$y->plus()])'
    )


def skinny_newton_update_pure(knots_acc: str, w_acc: str, res_acc: str, *, save_s1: bool = False) -> str:
    """Pure relation updating ``r_next = r + W * residual`` (optionally storing ``s1_val = W * residual``)."""
    step_rel = _skinny_matvec_pure(w_acc, res_acc, 'step_n')
    joined = _skinny_pure_join(
        knots_acc,
        step_rel,
        [('scenario_id', 'scenario_id'), ('factor_idx', 'row_i')],
        ['step_n'],
    )
    s1_expr = '$x.step_n->toOne()' if save_s1 else '$x.s1_val->toOne()'
    return (
        f'{joined}->project(~['
        f'scenario_id: x|$x.scenario_id, '
        f'factor_idx: x|$x.factor_idx->toOne(), '
        f'r_val: x|($x.r_val->toOne() + $x.step_n->toOne()), '
        f's1_val: x|{s1_expr}'
        f'])'
    )


def skinny_halley_update_pure(knots_acc: str, w0_acc: str, w1_acc: str, res_acc: str) -> str:
    """Pure relation applying Step >= 2 componentwise midpoint-secant Halley curvature boost using ``W_1 - W_0``."""
    step1_rel = _skinny_matvec_pure(w1_acc, res_acc, 'step_n')
    step0_rel = _skinny_matvec_pure(w0_acc, res_acc, 'step_0')
    j1 = _skinny_pure_join(
        knots_acc,
        step1_rel,
        [('scenario_id', 'scenario_id'), ('factor_idx', 'row_i')],
        ['step_n'],
    )
    j2 = _skinny_pure_join(
        j1,
        step0_rel,
        [('scenario_id', 'scenario_id'), ('factor_idx', 'row_i')],
        ['step_0'],
    )
    return (
        f'{j2}->extend(~['
        f'al_raw: x|(($x.s1_val->toOne() * $x.step_n->toOne()) / (($x.s1_val->toOne() * $x.s1_val->toOne()) + 1.0e-24))'
        f']){_SQL_STAGE_BARRIER}->extend(~['
        f'al_c: x|if($x.al_raw->toOne() > 0.25, |0.25, |if($x.al_raw->toOne() < -0.25, |-0.25, |$x.al_raw->toOne()))'
        f']){_SQL_STAGE_BARRIER}->extend(~['
        f'step_h: x|($x.step_n->toOne() + ((0.5 * $x.al_c->toOne()) * ($x.step_n->toOne() - $x.step_0->toOne())))'
        f']){_SQL_STAGE_BARRIER}->project(~['
        f'scenario_id: x|$x.scenario_id, '
        f'factor_idx: x|$x.factor_idx->toOne(), '
        f'r_val: x|($x.r_val->toOne() + $x.step_h->toOne()), '
        f's1_val: x|$x.step_h->toOne()'
        f'])'
    )


def skinny_adjoint_risk_pure(deltas_acc: str, w1_acc: str, jac2_acc: str) -> str:
    """Pure relation computing vector-only adjoint par-quote sensitivities ``(2 W_1^T - W_1^T J_2^T W_1^T) delta``
    without forming ``J(r_2)^-1``."""
    lam0_join = _skinny_pure_join(
        deltas_acc,
        w1_acc,
        [('scenario_id', 'scenario_id'), ('factor_idx', 'row_i')],
        [('quote_idx', 'col_j'), 'w_val'],
    )
    lam0_rel = (
        f'{lam0_join}->extend(~[term0: x|($x.delta_val->toOne() * $x.w_val->toOne())])'
        f'{_SQL_STAGE_BARRIER}->groupBy(~[scenario_id, group_id, quote_idx], ~[lam0: x|$x.term0 : y|$y->plus()])'
    )
    u_join = _skinny_pure_join(
        lam0_rel,
        jac2_acc,
        [('scenario_id', 'scenario_id'), ('quote_idx', 'row_i')],
        ['col_k', 'j_val'],
    )
    u_rel = (
        f'{u_join}->extend(~[uterm: x|($x.lam0->toOne() * $x.j_val->toOne())])'
        f'{_SQL_STAGE_BARRIER}->groupBy(~[scenario_id, group_id, col_k], ~[u_val: x|$x.uterm : y|$y->plus()])'
    )
    corr_join = _skinny_pure_join(
        u_rel,
        w1_acc,
        [('scenario_id', 'scenario_id'), ('col_k', 'row_i')],
        [('c_quote_idx', 'col_j'), 'w_val'],
    )
    corr_rel = (
        f'{corr_join}->extend(~[cterm: x|($x.u_val->toOne() * $x.w_val->toOne())])'
        f'{_SQL_STAGE_BARRIER}->groupBy(~[scenario_id, group_id, c_quote_idx], ~[corr_val: x|$x.cterm : y|$y->plus()])'
    )
    final_join = _skinny_pure_join(
        lam0_rel,
        corr_rel,
        [('scenario_id', 'scenario_id'), ('group_id', 'group_id'), ('quote_idx', 'c_quote_idx')],
        ['corr_val'],
        kind='LEFT',
    )
    return (
        f'{final_join}->extend(~['
        f'corr_v: x|if($x.corr_val->isEmpty(), |0.0, |$x.corr_val->toOne())'
        f']){_SQL_STAGE_BARRIER}->project(~['
        f'scenario_id: x|$x.scenario_id, '
        f'group_id: x|$x.group_id, '
        f'quote_idx: x|$x.quote_idx->toOne(), '
        f'quote_dv01: x|((2.0 * $x.lam0->toOne()) - $x.corr_v->toOne())'
        f'])'
    )


def _skinny_adjoint_refine_pure(lam_acc: str, deltas_acc: str, w1_acc: str, jac2_acc: str) -> str:
    """One iterative adjoint refinement pass ``lam_next = lam + W_1^T (delta - J_2^T lam)`` on dense ``deltas_acc``."""
    u_join = _skinny_pure_join(
        lam_acc,
        jac2_acc,
        [('scenario_id', 'scenario_id'), ('quote_idx', 'row_i')],
        ['col_k', 'j_val'],
    )
    u_rel = (
        f'{u_join}->extend(~[uterm: x|($x.quote_dv01->toOne() * $x.j_val->toOne())])'
        f'{_SQL_STAGE_BARRIER}->groupBy(~[scenario_id, group_id, col_k], ~[u_val: x|$x.uterm : y|$y->plus()])'
    )
    res_join = _skinny_pure_join(
        deltas_acc,
        u_rel,
        [('scenario_id', 'scenario_id'), ('group_id', 'group_id'), ('factor_idx', 'col_k')],
        ['u_val'],
        kind='LEFT',
    )
    res_rel = (
        f'{res_join}->extend(~['
        f'u_v: x|if($x.u_val->isEmpty(), |0.0, |$x.u_val->toOne())'
        f']){_SQL_STAGE_BARRIER}->project(~['
        f'scenario_id: x|$x.scenario_id, '
        f'group_id: x|$x.group_id, '
        f'factor_idx: x|$x.factor_idx->toOne(), '
        f'adj_res: x|($x.delta_val->toOne() - $x.u_v->toOne())'
        f'])'
    )
    corr_join = _skinny_pure_join(
        res_rel,
        w1_acc,
        [('scenario_id', 'scenario_id'), ('factor_idx', 'row_i')],
        [('c_quote_idx', 'col_j'), 'w_val'],
    )
    corr_rel = (
        f'{corr_join}->extend(~[cterm: x|($x.adj_res->toOne() * $x.w_val->toOne())])'
        f'{_SQL_STAGE_BARRIER}->groupBy(~[scenario_id, group_id, c_quote_idx], ~[d_lam: x|$x.cterm : y|$y->plus()])'
    )
    final_join = _skinny_pure_join(
        lam_acc,
        corr_rel,
        [('scenario_id', 'scenario_id'), ('group_id', 'group_id'), ('quote_idx', 'c_quote_idx')],
        ['d_lam'],
        kind='LEFT',
    )
    return (
        f'{final_join}->extend(~['
        f'dl_v: x|if($x.d_lam->isEmpty(), |0.0, |$x.d_lam->toOne())'
        f']){_SQL_STAGE_BARRIER}->project(~['
        f'scenario_id: x|$x.scenario_id, '
        f'group_id: x|$x.group_id, '
        f'quote_idx: x|$x.quote_idx->toOne(), '
        f'quote_dv01: x|($x.quote_dv01->toOne() + $x.dl_v->toOne())'
        f'])'
    )


def _get_skinny_kernel_plans(dialect: str = 'DuckDB') -> dict[str, Any]:
    """Compiles and caches all skinny Pure relational calibration kernels once per SQL dialect."""
    cached = _SKINNY_KERNEL_CACHE.get(dialect)
    if cached is not None:
        return cached

    require_datacube()
    import pyarrow as pa
    from legend_lite import compiler
    from legend_lite.frames import Frames

    dummy_tables: dict[str, pa.Table] = {
        'cal_shared_node': pa.table({
            'node_id': pa.array([0], type=pa.int64()),
            'op_code': pa.array([0], type=pa.int64()),
            'k0': pa.array([0], type=pa.int64()),
            'k1': pa.array([0], type=pa.int64()),
            'weight_1': pa.array([0.0], type=pa.float64()),
            'param_t': pa.array([0.0], type=pa.float64()),
            'const_offset': pa.array([1.0], type=pa.float64()),
        }),
        'cal_poly_slot': pa.table({
            'quote_idx': pa.array([0], type=pa.int64()),
            'role': pa.array([0], type=pa.int64()),
            'col_k': pa.array([-1], type=pa.int64()),
            'term_seq': pa.array([0], type=pa.int64()),
            'term_degree': pa.array([1], type=pa.int64()),
            'coeff': pa.array([1.0], type=pa.float64()),
            'slot_idx': pa.array([0], type=pa.int64()),
            'node_id': pa.array([0], type=pa.int64()),
        }),
        'cal_jac_coords': pa.table({
            'row_i': pa.array([0], type=pa.int64()),
            'col_k': pa.array([0], type=pa.int64()),
        }),
        'cal_quotes': pa.table({
            'scenario_id': pa.array(['BASE'], type=pa.string()),
            'quote_idx': pa.array([0], type=pa.int64()),
            'quote_rate': pa.array([0.03], type=pa.float64()),
        }),
        'cal_knots': pa.table({
            'scenario_id': pa.array(['BASE'], type=pa.string()),
            'factor_idx': pa.array([0], type=pa.int64()),
            'r_val': pa.array([0.03], type=pa.float64()),
            's1_val': pa.array([0.0], type=pa.float64()),
        }),
        'cal_polys': pa.table({
            'scenario_id': pa.array(['BASE'], type=pa.string()),
            'quote_idx': pa.array([0], type=pa.int64()),
            'role': pa.array([0], type=pa.int64()),
            'col_k': pa.array([-1], type=pa.int64()),
            'poly_val': pa.array([1.0], type=pa.float64()),
        }),
        'cal_res': pa.table({
            'scenario_id': pa.array(['BASE'], type=pa.string()),
            'quote_idx': pa.array([0], type=pa.int64()),
            'par_val': pa.array([0.03], type=pa.float64()),
            'residual': pa.array([0.0], type=pa.float64()),
        }),
        'cal_jac': pa.table({
            'scenario_id': pa.array(['BASE'], type=pa.string()),
            'row_i': pa.array([0], type=pa.int64()),
            'col_k': pa.array([0], type=pa.int64()),
            'j_val': pa.array([1.0], type=pa.float64()),
        }),
        'cal_w': pa.table({
            'scenario_id': pa.array(['BASE'], type=pa.string()),
            'row_i': pa.array([0], type=pa.int64()),
            'col_j': pa.array([0], type=pa.int64()),
            'w_val': pa.array([1.0], type=pa.float64()),
        }),
        'cal_w0': pa.table({
            'scenario_id': pa.array(['BASE'], type=pa.string()),
            'row_i': pa.array([0], type=pa.int64()),
            'col_j': pa.array([0], type=pa.int64()),
            'w_val': pa.array([1.0], type=pa.float64()),
        }),
        'cal_deltas': pa.table({
            'scenario_id': pa.array(['BASE'], type=pa.string()),
            'group_id': pa.array(['BOOK'], type=pa.string()),
            'factor_idx': pa.array([0], type=pa.int64()),
            'delta_val': pa.array([0.0], type=pa.float64()),
        }),
        'cal_lam': pa.table({
            'scenario_id': pa.array(['BASE'], type=pa.string()),
            'group_id': pa.array(['BOOK'], type=pa.string()),
            'quote_idx': pa.array([0], type=pa.int64()),
            'quote_dv01': pa.array([0.0], type=pa.float64()),
        }),
    }

    fr = Frames()
    try:
        names = list(dummy_tables.keys())
        cat_entries: list[dict[str, Any]] = []
        for t_name, tbl in dummy_tables.items():
            fr.register(t_name, tbl)
            rows = fr.connection.execute(compiler.catalog_columns_sql('main', t_name)).fetchall()
            cat_entries.append({
                'table': t_name,
                'pkg': t_name,
                'convertible': True,
                'columns': [
                    dict(zip(('name', 'dataType', 'logicalType', 'precision', 'scale', 'notNull'), r))
                    for r in rows
                ],
            })
        spec = compiler.table_model({'databaseType': 'DuckDB', 'tables': cat_entries})
    finally:
        fr.close()

    acc = {t_name: t_info['accessor'] for t_name, t_info in zip(names, spec['tables'])}
    pure_queries: dict[str, str] = {
        'seed_knots': '|' + skinny_seed_knots_pure(acc['cal_quotes']),
        'eval_polys': '|' + skinny_eval_polys_pure(acc['cal_shared_node'], acc['cal_poly_slot'], acc['cal_knots']),
        'residual': '|' + skinny_residual_pure(acc['cal_quotes'], acc['cal_polys']),
        'jacobian': '|' + skinny_jacobian_pure(acc['cal_jac_coords'], acc['cal_polys']),
        'init_w0': '|' + skinny_init_w0_pure(acc['cal_jac']),
        'schulz_step': '|' + skinny_schulz_step_pure(acc['cal_w'], acc['cal_jac']),
        'newton_update': '|' + skinny_newton_update_pure(acc['cal_knots'], acc['cal_w'], acc['cal_res'], save_s1=False),
        'newton_update_s1': '|' + skinny_newton_update_pure(acc['cal_knots'], acc['cal_w'], acc['cal_res'], save_s1=True),
        'halley_update': '|' + skinny_halley_update_pure(acc['cal_knots'], acc['cal_w0'], acc['cal_w'], acc['cal_res']),
        'adjoint_risk': '|' + skinny_adjoint_risk_pure(acc['cal_deltas'], acc['cal_w'], acc['cal_jac']),
        'adjoint_refine': '|' + _skinny_adjoint_refine_pure(
            acc['cal_lam'], acc['cal_deltas'], acc['cal_w'], acc['cal_jac'],
        ),
        'read_knots': '|' + acc['cal_knots'],
        'read_w': '|' + acc['cal_w'],
        'read_lam': '|' + acc['cal_lam'],
    }

    model_src = spec['model'] if dialect == 'DuckDB' else spec['model'].replace('type: DuckDB;', f'type: {dialect};')
    plans: dict[str, Any] = {'pure_queries': pure_queries}
    with compiler.HeldModel(model_src) as held:
        for k_name, q_text in pure_queries.items():
            plans[k_name] = held.plan(compiler.parse(q_text), spec['runtime'])

    if dialect == 'DuckDB':
        from legend_lite import _host

        target_map = {
            'seed_knots': 'cal_knots',
            'eval_polys': 'cal_polys',
            'residual': 'cal_res',
            'jacobian': 'cal_jac',
            'init_w0': 'cal_w',
            'schulz_step': 'cal_w',
            'newton_update': 'cal_knots',
            'newton_update_s1': 'cal_knots',
            'halley_update': 'cal_knots',
            'adjoint_risk': 'cal_lam',
            'adjoint_refine': 'cal_lam',
        }
        mat_stmts: dict[str, tuple[str, ...]] = {}
        for k_name, target_tbl in target_map.items():
            mat_stmts[k_name] = tuple(_host.statements({
                'op': 'relation',
                'kind': 'table',
                'replace': True,
                'target': _host.name(target_tbl),
                'from': {'query': plans[k_name].sql},
            }))
        mat_stmts['copy_w0'] = tuple(_host.statements({
            'op': 'relation',
            'kind': 'table',
            'replace': True,
            'target': _host.name('cal_w0'),
            'from': {'relation': _host.name('cal_w')},
        }))
        plans['mat_stmts'] = mat_stmts

    _SKINNY_KERNEL_CACHE[dialect] = plans
    return plans


class PureBasisCurveFitter:
    """Skinny vectorized 2-iteration (configurable ``steps``) 100%-SQL curve fitter, SQL Jacobian inverter,
    and vector-only SQL adjoint quote-risk engine.

    Architecture:
      1. **Skinny Relational Calibration Basis**: Expands all calibration par numerators ``F_i``, denominators ``A_i``,
         and active partial derivatives into fixed-width Arrow tables (``cal_shared_node``, ``cal_poly_slot``,
         ``cal_jac_coords``) with schema completely independent of pillar count ``N`` (scaling cleanly past ``N = 200``
         coupled OIS + IRS pillars with zero wide-column bloat).
      2. **Sparse Newton-Schulz / Pointer-Doubling Linear Solve & Inversion**: Initializes ``W_0 = (I + L) D^-1`` and
         iterates ``W_next = W * (2I - J * W)`` in ``O(log L_max)`` skinny SQL joins at Step 1, then reuses Step 1's
         ``W_0`` to converge ``W_1 = J(r_1)^-1`` in just 3 quadratic-refinement stages at Step >= 2.
      3. **Optional Halley Midpoint Curvature Boost (``halley=True``)**: Reuses ``W_1 - W_0`` along the step trajectory
         for cubic-like 2-step accuracy on large shocks with zero extra derivative evaluations.
      4. **Vector-Only Adjoint Quote Risk (``fit_and_adjoint_risk_sql``)**: Computes ``(J(r_2)^-1)^T delta`` using
         only skinny matrix-vector SQL joins seeded from ``W_1^T``, avoiding forming ``J(r_2)^-1`` explicitly.
    """

    def __init__(
        self,
        fitter: CurveFitter | MultiCurveFitter,
        cb: pure.ComputeBasis | None = None,
        *,
        steps: int = DEFAULT_NEWTON_STEPS,
        halley: bool = False,
    ) -> None:
        require_datacube()
        from legend_lite.frames import Frames

        if steps < 1:
            raise ValueError('steps must be >= 1')
        self.fitter = fitter
        self.curves = list(fitter.curves)
        self.factors = list(fitter.factors)
        self.quotes = list(fitter.quotes)
        self.quote_specs = list(fitter.quote_specs)
        self.steps = int(steps)
        self.halley = bool(halley)
        self._cb = cb
        self._n = len(self.factors)
        self._factor_idx_by_id = {f.factor_id: idx for idx, f in enumerate(self.factors)}

        self.basis_tables = build_calibration_basis_tables(fitter)
        self._reachable = self.basis_tables.reachable
        self._kernels = _get_skinny_kernel_plans('DuckDB')

        self._frames = Frames()
        con = self._frames.connection
        con.register('cal_shared_node', self.basis_tables.cal_shared_node)
        con.register('cal_poly_slot', self.basis_tables.cal_poly_slot)
        con.register('cal_jac_coords', self.basis_tables.cal_jac_coords)
        self._warm_knots: Any = None
        self._warm_w: Any = None
        self._warm_sids: tuple[str, ...] = ()

    def pure_query(self, *, steps: int | None = None, compute_jacobian: bool = True, halley: bool | None = None) -> str:
        """Returns a human-readable Pure summary of the skinny relational calibration pipeline stages."""
        k = self.steps if steps is None else int(steps)
        use_halley = self.halley if halley is None else bool(halley)
        pq = self._kernels['pure_queries']
        parts = [
            f"// Skinny calibration basis (N={self._n}, step1_schulz={self.basis_tables.step1_schulz_iters}, "
            f"steps={k}, halley={use_halley})",
            pq['seed_knots'],
            pq['eval_polys'],
            pq['residual'],
            pq['jacobian'],
            pq['init_w0'],
            pq['schulz_step'],
            pq['halley_update' if (use_halley and k >= 2) else 'newton_update'],
        ]
        if not compute_jacobian:
            parts.append(pq['adjoint_risk'])
        return '\n'.join(parts)

    def plan_sql(
        self,
        dialect: str = 'DuckDB',
        *,
        steps: int | None = None,
        compute_jacobian: bool = True,
        halley: bool | None = None,
    ) -> str:
        """Renders the compiled skinny SQL kernels for ``dialect`` across all 8 ``legend-lite`` SQL dialects."""
        k = self.steps if steps is None else int(steps)
        use_halley = self.halley if halley is None else bool(halley)
        plans = _get_skinny_kernel_plans(dialect)
        stage_names = ['seed_knots', 'eval_polys', 'residual', 'jacobian', 'init_w0', 'schulz_step', 'newton_update_s1']
        if k >= 2:
            stage_names.append('halley_update' if use_halley else 'newton_update')
        if not compute_jacobian:
            stage_names.append('adjoint_risk')
        return '\n\n'.join(f'-- [{name}]\n{plans[name].sql}' for name in stage_names)

    def _run_calibration_tables(
        self,
        scenario_quotes: Mapping[str, Sequence[float]],
        *,
        steps: int,
        compute_jacobian: bool,
        use_halley: bool,
        warm_start: bool,
        initial_rates: Mapping[str, Sequence[float]] | None = None,
    ) -> tuple[list[str], Any, Any]:
        import pyarrow as pa

        sids_order = list(scenario_quotes.keys())
        sids: list[str] = []
        q_idxs: list[int] = []
        q_rates: list[float] = []
        for sid, qs in scenario_quotes.items():
            if len(qs) != self._n:
                raise ValueError(f'expected {self._n} quotes for scenario {sid!r}, got {len(qs)}')
            sids.extend([sid] * self._n)
            q_idxs.extend(range(self._n))
            q_rates.extend(float(v) for v in qs)

        con = self._frames.connection
        run = self._frames.run
        kplans = self._kernels
        mat = kplans['mat_stmts']

        def _exec_mat(stage_key: str) -> None:
            for stmt in mat[stage_key]:
                con.execute(stmt)

        can_warm = warm_start and initial_rates is None and self._warm_sids == tuple(sids_order)
        if initial_rates is not None:
            init_rates: list[float] = []
            for sid in sids_order:
                r0 = initial_rates[sid]
                if len(r0) != self._n:
                    raise ValueError(f'expected {self._n} initial_rates for scenario {sid!r}, got {len(r0)}')
                init_rates.extend(float(v) for v in r0)
            seed_tbl = pa.table({
                'scenario_id': pa.array(sids, type=pa.string()),
                'quote_idx': pa.array(q_idxs, type=pa.int64()),
                'quote_rate': pa.array(init_rates, type=pa.float64()),
            })
            con.register('cal_quotes', seed_tbl)
            _exec_mat('seed_knots')

        quotes_tbl = pa.table({
            'scenario_id': pa.array(sids, type=pa.string()),
            'quote_idx': pa.array(q_idxs, type=pa.int64()),
            'quote_rate': pa.array(q_rates, type=pa.float64()),
        })
        con.register('cal_quotes', quotes_tbl)

        if not can_warm and initial_rates is None:
            _exec_mat('seed_knots')

        has_w_prev = False
        for step_idx in range(1, steps + 1):
            _exec_mat('eval_polys')
            _exec_mat('residual')
            _exec_mat('jacobian')

            if step_idx == 1 and not can_warm:
                _exec_mat('init_w0')
                n_schulz = self.basis_tables.step1_schulz_iters
            else:
                # Seeded from previous step's (or warm tick's) inverse Jacobian: 4 quadratic-doubling stages -> <1e-15
                n_schulz = 4

            for _ in range(n_schulz):
                _exec_mat('schulz_step')

            if use_halley and step_idx >= 2 and has_w_prev:
                _exec_mat('halley_update')
            else:
                upd_key = 'newton_update_s1' if (use_halley or step_idx == 1) else 'newton_update'
                _exec_mat(upd_key)

            if use_halley and step_idx < steps:
                _exec_mat('copy_w0')
                has_w_prev = True

        if compute_jacobian:
            _exec_mat('eval_polys')
            _exec_mat('jacobian')
            for _ in range(2):
                _exec_mat('schulz_step')

        knots_tbl = run(kplans['read_knots'])
        w_tbl = run(kplans['read_w']) if compute_jacobian else None
        self._warm_sids = tuple(sids_order)
        return sids_order, knots_tbl, w_tbl

    def _assemble_fits(
        self,
        sids_order: Sequence[str],
        knots_tbl: Any,
        w_tbl: Any | None,
        steps: int,
        compute_jacobian: bool,
    ) -> dict[str, Fit]:
        rates_by_sid: dict[str, list[float]] = {sid: [0.0] * self._n for sid in sids_order}
        k_cols = knots_tbl.to_pydict()
        for sid, f_idx, r_val in zip(k_cols['scenario_id'], k_cols['factor_idx'], k_cols['r_val']):
            rates_by_sid[str(sid)][int(f_idx)] = float(r_val)

        jac_by_sid: dict[str, list[list[float]]] = {}
        if compute_jacobian and w_tbl is not None:
            for sid in sids_order:
                jac_by_sid[sid] = [[0.0] * self._n for _ in range(self._n)]
            w_cols = w_tbl.to_pydict()
            for sid, r_i, c_j, w_v in zip(w_cols['scenario_id'], w_cols['row_i'], w_cols['col_j'], w_cols['w_val']):
                jac_by_sid[str(sid)][int(r_i)][int(c_j)] = float(w_v)

        out: dict[str, Fit] = {}
        for sid in sids_order:
            rates = rates_by_sid[sid]
            dr_dq = jac_by_sid.get(sid, [])
            by_curve: dict[str, list[float]] = {}
            if isinstance(self.fitter, MultiCurveFitter):
                n_d = len(self.fitter.discount_curve.pillars)
                by_curve[self.fitter.discount_curve.curve_id] = rates[:n_d]
                by_curve[self.fitter.projection_curve.curve_id] = rates[n_d:]
            else:
                by_curve[self.curves[0].curve_id] = rates
            out[sid] = Fit(rates, dr_dq, steps, by_curve)
        return out

    def fit_scenarios_sql(
        self,
        scenario_quotes: Mapping[str, Sequence[float]],
        *,
        steps: int | None = None,
        compute_jacobian: bool = False,
        halley: bool | None = None,
        warm_start: bool = False,
        initial_rates: Mapping[str, Sequence[float]] | None = None,
    ) -> dict[str, Fit]:
        """Calibrates all scenarios in ``scenario_quotes`` simultaneously using skinny vectorized DuckDB SQL joins."""
        k = self.steps if steps is None else int(steps)
        use_halley = self.halley if halley is None else bool(halley)
        sids_order, knots_tbl, w_tbl = self._run_calibration_tables(
            scenario_quotes,
            steps=k,
            compute_jacobian=compute_jacobian,
            use_halley=use_halley,
            warm_start=warm_start,
            initial_rates=initial_rates,
        )
        return self._assemble_fits(sids_order, knots_tbl, w_tbl, k, compute_jacobian)

    def fit_and_adjoint_risk_sql(
        self,
        scenario_quotes: Mapping[str, Sequence[float]] | Sequence[float],
        pillar_deltas: Mapping[tuple[str, str, str], float] | Mapping[tuple[str, str], float],
        *,
        steps: int | None = None,
        halley: bool | None = None,
        warm_start: bool = False,
    ) -> tuple[Any, Any]:
        """Calibrates ``scenario_quotes`` and computes par-quote sensitivities ``(J(r)^-1)^T delta`` in SQL
        using vector-only adjoint joins seeded from ``W_1^T``, without forming the full ``J(r_2)^-1`` matrix.

        ``scenario_quotes`` may be either a ``{scenario_id: quotes}`` mapping (returning ``(fits_by_scen,
        {(scen, book, quote_id): dv01})``) or a single-scenario ``quotes`` sequence (returning ``(fit,
        {(book, quote_id): dv01})``).
        """
        import pyarrow as pa

        single_mode = not isinstance(scenario_quotes, Mapping)
        scen_map: Mapping[str, Sequence[float]] = (
            {'BASE': list(scenario_quotes)} if single_mode else scenario_quotes  # type: ignore[arg-type]
        )

        k = self.steps if steps is None else int(steps)
        use_halley = self.halley if halley is None else bool(halley)
        sids_order, knots_tbl, _w1_tbl = self._run_calibration_tables(
            scen_map,
            steps=k,
            compute_jacobian=False,
            use_halley=use_halley,
            warm_start=warm_start,
        )
        fits = self._assemble_fits(sids_order, knots_tbl, None, k, False)
        if not pillar_deltas:
            return (fits['BASE'], {}) if single_mode else (fits, {})

        con = self._frames.connection
        run = self._frames.run
        kplans = self._kernels
        mat = kplans['mat_stmts']

        def _exec_mat(stage_key: str) -> None:
            for stmt in mat[stage_key]:
                con.execute(stmt)

        # Evaluate sparse J_2 = J(r_2) at converged knots (O(nnz(J)) rows, no matrix-matrix W_2 inversion)
        _exec_mat('eval_polys')
        _exec_mat('jacobian')

        default_sid = sids_order[0] if sids_order else 'BASE'
        dense_deltas: dict[tuple[str, str], list[float]] = {}
        for key, val in pillar_deltas.items():
            if len(key) == 2:
                sid, book, fid = default_sid, str(key[0]), str(key[1])
            else:
                sid, book, fid = str(key[0]), str(key[1]), str(key[2])
            f_idx = self._factor_idx_by_id.get(fid)
            if f_idx is None:
                continue
            vec = dense_deltas.get((sid, book))
            if vec is None:
                vec = [0.0] * self._n
                dense_deltas[(sid, book)] = vec
            vec[f_idx] += float(val)

        d_sid: list[str] = []
        d_grp: list[str] = []
        d_idx: list[int] = []
        d_val: list[float] = []
        for (sid, book), vec in dense_deltas.items():
            d_sid.extend([sid] * self._n)
            d_grp.extend([book] * self._n)
            d_idx.extend(range(self._n))
            d_val.extend(vec)

        deltas_tbl = pa.table({
            'scenario_id': pa.array(d_sid, type=pa.string()),
            'group_id': pa.array(d_grp, type=pa.string()),
            'factor_idx': pa.array(d_idx, type=pa.int64()),
            'delta_val': pa.array(d_val, type=pa.float64()),
        })
        con.register('cal_deltas', deltas_tbl)

        _exec_mat('adjoint_risk')
        _exec_mat('adjoint_refine')
        _exec_mat('adjoint_refine')
        lam_tbl = run(kplans['read_lam'])

        l_cols = lam_tbl.to_pydict()
        quote_ids = [qs.quote_id for qs in self.quote_specs]
        quote_risk_out: dict[tuple[str, str, str], float] = {}
        for sid, grp, q_idx, dv01 in zip(
            l_cols['scenario_id'], l_cols['group_id'], l_cols['quote_idx'], l_cols['quote_dv01']
        ):
            quote_risk_out[(str(sid), str(grp), quote_ids[int(q_idx)])] = float(dv01)
        if single_mode:
            flat_out = {(grp, qid): dv01 for (_sid, grp, qid), dv01 in quote_risk_out.items()}
            return fits['BASE'], flat_out
        return fits, quote_risk_out

    def fit_quotes(
        self,
        quotes: Sequence[float] | None = None,
        *,
        tol: float = 1e-14,
        max_iter: int = 30,
        steps: int | None = None,
        compute_jacobian: bool = True,
        halley: bool | None = None,
        warm_start: bool = False,
        initial_rates: Sequence[float] | None = None,
    ) -> Fit:
        """Calibrates ``quotes`` (defaults to ``self.quotes``) in DuckDB SQL using ``steps`` (default 2)
        skinny Newton-Schulz (or Halley) iterations and optional sparse SQL Jacobian inversion."""
        del tol, max_iter
        target_quotes = list(self.quotes if quotes is None else quotes)
        init_map = {'BASE': list(initial_rates)} if initial_rates is not None else None
        return self.fit_scenarios_sql(
            {'BASE': target_quotes},
            steps=steps,
            compute_jacobian=compute_jacobian,
            halley=halley,
            warm_start=warm_start,
            initial_rates=init_map,
        )['BASE']

    def fit(self, *, steps: int | None = None, halley: bool | None = None) -> Fit:
        return self.fit_quotes(self.quotes, steps=steps, compute_jacobian=True, halley=halley)

    def close(self) -> None:
        self._warm_knots = None
        self._warm_w = None
        self._frames.close()

    def __enter__(self) -> PureBasisCurveFitter:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

