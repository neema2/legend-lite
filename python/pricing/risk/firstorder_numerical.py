"""First-order numerical (Bump, Refit & Reprice) risk calculator ``FirstOrderNumericalRisk``.

For each input benchmark par quote ``q_j`` (or each zero-curve pillar ``r_k``):
  1. Bumps the input quote ``q_j -> q_j + delta_q`` (forward difference) or ``q_j +/- delta_q / 2`` (central difference).
  2. Refits the yield curve(s) via ``fitter.fit_quotes(bumped_quotes)`` to obtain the perturbed zero-rate pillars.
  3. Reprices every instrument in the portfolio under the refitted curves (using either ``PythonEngineExpr`` locally in
     Python or ``PureBasisEngine`` in DuckDB SQL) and divides the NPV change by the bump size in basis points.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Sequence

from legend_lite.scalar_parallel import basis_pure as pure, expr

from ..instruments.base import Instrument
from ..instruments.portfolio import Portfolio
from ..marketmodels.ir_curve_fitter import FactorSpec, Fit
from ..marketmodels.ir_curve_yield import ZeroCurve
from .base import BP, FitterType, FirstOrderRiskBase, scenario

if TYPE_CHECKING:
    from ..engines.base import ExecutionEngine


class FirstOrderNumericalRisk(FirstOrderRiskBase):
    """Calculates first-order sensitivities by bumping each input market quote, refitting the zero curves, and
    repricing the portfolio (or bumping each zero-rate pillar directly and repricing)."""

    def __init__(
        self,
        instrument: Instrument | Portfolio | Sequence[Instrument],
        curves: ZeroCurve | Sequence[ZeroCurve] = (),
        factors: Sequence[FactorSpec] | None = None,
        regex: str | None = None,
        name: str = 'FirstOrderNumericalRisk',
        bump_bps: float = 1.0,
    ) -> None:
        super().__init__(instrument, curves=curves, factors=factors, regex=regex, name=name)
        self.bump_bps = float(bump_bps)
        self.bump_size = self.bump_bps * BP

    def refit_quote_bumps(
        self,
        fitter: FitterType,
        *,
        method: str = 'forward',
        quotes: Sequence[float] | None = None,
    ) -> dict[str, Fit]:
        """Refits the curve(s) for each bumped input benchmark quote.

        Returns a dict mapping scenario IDs (``f'BUMP_Q_{quote_id}'`` for ``method='forward'``, or
        ``f'BUMP_Q_{quote_id}_UP'`` and ``f'BUMP_Q_{quote_id}_DN'`` for ``method='central'``) to their refitted ``Fit``.
        """
        pattern = re.compile(self.regex) if self.regex else None
        base_quotes = list(fitter.quotes if quotes is None else quotes)
        fits: dict[str, Fit] = {}
        for j, q_spec in enumerate(fitter.quote_specs):
            if pattern and not pattern.search(q_spec.quote_id):
                continue
            if method == 'central':
                up_q = list(base_quotes)
                dn_q = list(base_quotes)
                up_q[j] += 0.5 * self.bump_size
                dn_q[j] -= 0.5 * self.bump_size
                fits[f'BUMP_Q_{q_spec.quote_id}_UP'] = fitter.fit_quotes(up_q, compute_jacobian=False)
                fits[f'BUMP_Q_{q_spec.quote_id}_DN'] = fitter.fit_quotes(dn_q, compute_jacobian=False)
            else:
                up_q = list(base_quotes)
                up_q[j] += self.bump_size
                fits[f'BUMP_Q_{q_spec.quote_id}'] = fitter.fit_quotes(up_q, compute_jacobian=False)
        return fits

    def quote_scenarios(
        self,
        fitter: FitterType,
        bump_fits: dict[str, Fit] | None = None,
        *,
        method: str = 'forward',
        unit: str | None = 'USD',
    ) -> tuple[dict[str, Fit], list[pure.Scenario]]:
        """Builds ``pure.Scenario`` objects for each refitted quote bump so ``PureBasisEngine`` can value all bumped
        curves in a single SQL query."""
        fits = bump_fits if bump_fits is not None else self.refit_quote_bumps(fitter, method=method)
        scens = [
            scenario(fitter.curves, sid, f, 0.0, unit, vol_surfaces=self.vol_surfaces)
            for sid, f in fits.items()
        ]
        return fits, scens

    def instrument_quote_risk(
        self,
        fitter: FitterType,
        base_fit: Fit | None = None,
        *,
        engine: ExecutionEngine | None = None,
        method: str = 'forward',
        bump_fits: dict[str, Fit] | None = None,
    ) -> tuple[dict[tuple[str, str, str], float], dict[str, Fit]]:
        """Per-trade quote DV01 ``{(book, trade_id, quote_id): dv01}`` via naive quote bump, curve refit, and reprice."""
        fit0 = base_fit if base_fit is not None else fitter.fit()
        fits = bump_fits if bump_fits is not None else self.refit_quote_bumps(fitter, method=method)
        curves = list(fitter.curves)

        if engine is not None:
            scen_map = {'BASE': fit0, **fits}
            npvs_by_scen = engine.scenario_npvs(self.portfolio, curves, scen_map)
        else:
            npv_exprs = self.portfolio.trade_npv_exprs(curves)
            npvs_by_scen = {}
            for sid, f in {'BASE': fit0, **fits}.items():
                env = f.env(curves, self.vol_surfaces)
                cache: dict = {}
                npvs_by_scen[sid] = {key: expr.evaluate(e, env, cache) for key, e in npv_exprs.items()}

        pattern = re.compile(self.regex) if self.regex else None
        out: dict[tuple[str, str, str], float] = {}
        for q_spec in fitter.quote_specs:
            qid = q_spec.quote_id
            if pattern and not pattern.search(qid):
                continue
            for t in self.trades:
                key = (t.book, t.trade_id)
                if method == 'central':
                    up_val = npvs_by_scen[f'BUMP_Q_{qid}_UP'][key]
                    dn_val = npvs_by_scen[f'BUMP_Q_{qid}_DN'][key]
                    dv01 = (up_val - dn_val) / self.bump_bps
                else:
                    base_val = npvs_by_scen['BASE'][key]
                    up_val = npvs_by_scen[f'BUMP_Q_{qid}'][key]
                    dv01 = (up_val - base_val) / self.bump_bps
                out[(t.book, t.trade_id, qid)] = dv01
        return out, fits

    def quote_risk(
        self,
        fitter: FitterType,
        base_fit: Fit | None = None,
        *,
        engine: ExecutionEngine | None = None,
        method: str = 'forward',
        bump_fits: dict[str, Fit] | None = None,
    ) -> tuple[dict[tuple[str, str], float], dict[str, Fit]]:
        """Book-aggregated quote DV01 ``{(book, quote_id): dv01}`` via naive quote bump, curve refit, and reprice."""
        inst_risk, fits = self.instrument_quote_risk(
            fitter, base_fit, engine=engine, method=method, bump_fits=bump_fits
        )
        book_risk: dict[tuple[str, str], float] = {}
        for (book, _trade_id, qid), val in inst_risk.items():
            key = (book, qid)
            book_risk[key] = book_risk.get(key, 0.0) + val
        return book_risk, fits

    def pillar_risk(
        self,
        base_fit: Fit,
        *,
        engine: ExecutionEngine | None = None,
        method: str = 'central',
    ) -> dict[tuple[str, str, str], float]:
        """Direct zero-rate pillar bump-and-reprice ``{(book, trade_id, factor_id): delta_per_bp}`` (bumping one pillar
        rate at a time without refitting quotes)."""
        curves = self.curves
        factors = self.factors()
        base_env = base_fit.env(curves, self.vol_surfaces)
        npv_exprs = self.portfolio.trade_npv_exprs(curves)
        base_cache: dict = {}
        base_npvs = {key: expr.evaluate(e, base_env, base_cache) for key, e in npv_exprs.items()}

        out: dict[tuple[str, str, str], float] = {}
        for f in factors:
            knot_name = f.knot.name
            if method == 'central':
                env_up = {**base_env, knot_name: base_env[knot_name] + 0.5 * self.bump_size}
                env_dn = {**base_env, knot_name: base_env[knot_name] - 0.5 * self.bump_size}
                c_up: dict = {}
                c_dn: dict = {}
                for (book, trade_id), e in npv_exprs.items():
                    v = (expr.evaluate(e, env_up, c_up) - expr.evaluate(e, env_dn, c_dn)) / self.bump_bps
                    if abs(v) > 1e-12:
                        out[(book, trade_id, f.factor_id)] = v
            else:
                env_up = {**base_env, knot_name: base_env[knot_name] + self.bump_size}
                c_up = {}
                for (book, trade_id), e in npv_exprs.items():
                    v = (expr.evaluate(e, env_up, c_up) - base_npvs[(book, trade_id)]) / self.bump_bps
                    if abs(v) > 1e-12:
                        out[(book, trade_id, f.factor_id)] = v
        return out


__all__ = ['FirstOrderNumericalRisk']
