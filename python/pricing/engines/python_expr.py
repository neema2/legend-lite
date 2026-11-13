"""Local Python execution engine evaluating ``legend_lite.scalar_parallel.expr.Expr`` DAGs directly in Python."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from legend_lite.scalar_parallel import basis_pure as pure, expr

from ..instruments.base import Instrument
from ..instruments.portfolio import Portfolio
from ..marketmodels.ir_curve_fitter import FactorSpec, Fit
from ..marketmodels.ir_curve_yield import ZeroCurve
from ..marketmodels.swaption_vol_surface import SwaptionVolSurface
from ..risk.base import BP, FitterType, as_portfolio, scenario
from ..risk.firstorder_analytic import FirstOrderAnalyticRisk
from ..risk.firstorder_numerical import FirstOrderNumericalRisk
from .base import ExecutionEngine, MarketSession, TickResult

if TYPE_CHECKING:
    from ..risk import RiskRun


def is_bump_and_grind(risk_method: Any) -> bool:
    if risk_method is None:
        return False
    if isinstance(risk_method, str):
        return risk_method.lower().strip() in ('numerical', 'firstorder_numerical', 'bump', 'bump_and_grind', 'fd')
    if isinstance(risk_method, type) and issubclass(risk_method, FirstOrderNumericalRisk):
        return True
    return isinstance(risk_method, FirstOrderNumericalRisk)


class PythonMarketSession(MarketSession):
    """Stateful market-tick session for ``PythonEngineExpr``."""

    def __init__(
        self,
        portfolio: Portfolio,
        fitter: FitterType,
        risk_method: Any = None,
        *,
        bump_method: str = 'forward',
        bump_bps: float = 1.0,
    ) -> None:
        self.portfolio = portfolio
        self.fitter = fitter
        self.curves = list(fitter.curves)
        self.vol_surfaces = self.portfolio.vol_surfaces()
        self.use_bump = is_bump_and_grind(risk_method)
        self.risk_method_name = 'bump_and_grind' if self.use_bump else 'analytic'
        self.bump_method = bump_method
        self.bump_bps = float(bump_bps)
        self.tick_count = 0
        self._npv_exprs: dict[tuple[str, str], expr.Expr] = {}
        self._pillar_exprs_by_trade: dict[tuple[str, str], list[tuple[str, expr.Expr]]] = {}
        self._bg: FirstOrderNumericalRisk | None = None

    def tick(self, quotes: Sequence[float] | None = None) -> TickResult:
        is_first = self.tick_count == 0
        t_start = time.perf_counter()

        if is_first:
            t_b0 = time.perf_counter()
            self._npv_exprs = self.portfolio.trade_npv_exprs(self.curves)
            if self.use_bump:
                self._bg = FirstOrderNumericalRisk(
                    self.portfolio,
                    curves=self.curves,
                    factors=self.fitter.factors,
                    bump_bps=self.bump_bps,
                )
            else:
                analytic = FirstOrderAnalyticRisk(
                    self.portfolio,
                    curves=self.curves,
                    factors=self.fitter.factors,
                )
                by_trade: dict[tuple[str, str], list[tuple[str, expr.Expr]]] = {}
                for (b_id, t_id, f_id), d_expr in analytic.trade_pillar_exprs(scale=BP).items():
                    by_trade.setdefault((b_id, t_id), []).append((f_id, d_expr))
                self._pillar_exprs_by_trade = by_trade
            build_ms = (time.perf_counter() - t_b0) * 1000.0
        else:
            build_ms = 0.0

        if self.use_bump:
            assert self._bg is not None
            t_f0 = time.perf_counter()
            fit = self.fitter.fit_quotes(quotes, compute_jacobian=False)
            bump_fits = self._bg.refit_quote_bumps(
                self.fitter,
                method=self.bump_method,
                quotes=quotes,
            )
            fit_ms = (time.perf_counter() - t_f0) * 1000.0

            t_e0 = time.perf_counter()
            base_env = fit.env(self.curves, self.vol_surfaces)
            npv = {k: expr.evaluate(e, base_env) for k, e in self._npv_exprs.items()}
            scen_npvs: dict[str, dict[tuple[str, str], float]] = {'BASE': npv}
            for sid, b_fit in bump_fits.items():
                b_env = b_fit.env(self.curves, self.vol_surfaces)
                scen_npvs[sid] = {k: expr.evaluate(e, b_env) for k, e in self._npv_exprs.items()}
            quote_risk: dict[tuple[str, str], float] = {}
            for q_spec in self.fitter.quote_specs:
                qid = q_spec.quote_id
                for t in self.portfolio.trades:
                    key = (t.book, t.trade_id)
                    if self.bump_method == 'central':
                        dv01 = (
                            scen_npvs[f'BUMP_Q_{qid}_UP'][key] - scen_npvs[f'BUMP_Q_{qid}_DN'][key]
                        ) / self.bump_bps
                    else:
                        dv01 = (scen_npvs[f'BUMP_Q_{qid}'][key] - npv[key]) / self.bump_bps
                    b_key = (t.book, qid)
                    quote_risk[b_key] = quote_risk.get(b_key, 0.0) + dv01
            eval_ms = (time.perf_counter() - t_e0) * 1000.0
        else:
            t_f0 = time.perf_counter()
            fit = self.fitter.fit_quotes(quotes, compute_jacobian=True)
            fit_ms = (time.perf_counter() - t_f0) * 1000.0

            t_e0 = time.perf_counter()
            env = fit.env(self.curves, self.vol_surfaces)
            npv = {}
            deltas: dict[tuple[str, str, str], float] = {}
            for key, e in self._npv_exprs.items():
                trade_cache: dict = {}
                npv[key] = expr.evaluate(e, env, trade_cache)
                b_id, t_id = key
                for f_id, d_expr in self._pillar_exprs_by_trade.get(key, ()):
                    deltas[(b_id, t_id, f_id)] = expr.evaluate(d_expr, env, trade_cache)
            quote_risk = FirstOrderAnalyticRisk.chain_quote_risk(deltas, self.fitter, fit)
            eval_ms = (time.perf_counter() - t_e0) * 1000.0

        total_ms = (time.perf_counter() - t_start) * 1000.0
        idx = self.tick_count
        self.tick_count += 1
        return TickResult(
            tick_index=idx,
            is_first_tick=is_first,
            engine_name=PythonEngineExpr.engine_name,
            risk_method_name=self.risk_method_name,
            trade_count=len(self.portfolio.trades),
            shared_nodes=0,
            basis_terms=0,
            build_ms=build_ms,
            fit_ms=fit_ms,
            eval_ms=eval_ms,
            total_ms=total_ms,
            fit=fit,
            npv=npv,
            quote_risk=quote_risk,
        )


class PythonEngineExpr(ExecutionEngine):
    """Native Python execution engine using symbolic ``expr.Expr`` DAG evaluation with shared-node caching."""

    engine_name = 'python'

    @staticmethod
    def _resolve_env(
        fit_or_env: Fit | Mapping[str, float] | None,
        curves: Sequence[ZeroCurve],
        vol_surfaces: Sequence[SwaptionVolSurface] = (),
    ) -> dict[str, float]:
        if fit_or_env is None:
            raise ValueError('fit or environment mapping is required')
        if isinstance(fit_or_env, Fit):
            return fit_or_env.env(curves, vol_surfaces)
        out: dict[str, float] = {}
        for vs in vol_surfaces:
            if vs.atm_vols is not None:
                out.update(vs.env())
        out.update(fit_or_env)
        return out

    def npvs(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        curves: ZeroCurve | Sequence[ZeroCurve] = (),
        fit: Fit | Mapping[str, float] | None = None,
    ) -> dict[tuple[str, str], float]:
        port = as_portfolio(portfolio, curves)
        curve_list = port.curves()
        env = self._resolve_env(fit, curve_list, port.vol_surfaces())
        cache: dict = {}
        return {
            (t.book, t.trade_id): expr.evaluate(t.npv_on(curve_list), env, cache)
            for t in port.trades
        }

    def scenario_npvs(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        curves: ZeroCurve | Sequence[ZeroCurve],
        scenario_fits: Mapping[str, Fit],
    ) -> dict[str, dict[tuple[str, str], float]]:
        port = as_portfolio(portfolio, curves)
        curve_list = port.curves()
        vol_surfaces = port.vol_surfaces()
        npv_exprs = port.trade_npv_exprs(curve_list)
        out: dict[str, dict[tuple[str, str], float]] = {}
        for sid, f in scenario_fits.items():
            env = f.env(curve_list, vol_surfaces)
            cache: dict = {}
            out[sid] = {key: expr.evaluate(e, env, cache) for key, e in npv_exprs.items()}
        return out

    def pillar_risk(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        curves: ZeroCurve | Sequence[ZeroCurve] = (),
        fit: Fit | Mapping[str, float] | None = None,
        factors: Sequence[FactorSpec] | None = None,
    ) -> dict[tuple[str, str, str], float]:
        port = as_portfolio(portfolio, curves)
        curve_list = port.curves()
        env = self._resolve_env(fit, curve_list, port.vol_surfaces())
        calc = FirstOrderAnalyticRisk(port, curves=curve_list, factors=factors)
        cache: dict = {}
        return {
            key: expr.evaluate(d_expr, env, cache)
            for key, d_expr in calc.trade_pillar_exprs(scale=BP).items()
        }

    def instrument_risk(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        curves: ZeroCurve | Sequence[ZeroCurve] = (),
        fit: Fit | Mapping[str, float] | None = None,
        risk_method: Any = None,
        regex: str | None = None,
        **kwargs: Any,
    ) -> dict[str, dict[str, float]]:
        port = as_portfolio(portfolio, curves)
        curve_list = port.curves()
        env = self._resolve_env(fit, curve_list, port.vol_surfaces())
        method = risk_method or FirstOrderAnalyticRisk
        if isinstance(method, type) and issubclass(method, FirstOrderNumericalRisk):
            if not isinstance(fit, Fit):
                raise ValueError('FirstOrderNumericalRisk requires a Fit object')
            calc = method(port, curves=curve_list, regex=regex, **kwargs)
            p_risk = calc.pillar_risk(fit, engine=self)
            res: dict[str, dict[str, float]] = {t.trade_id: {} for t in port.trades}
            for (_b, t_id, f_id), val in p_risk.items():
                res.setdefault(t_id, {})[f_id] = val
            return res
        calc_a = FirstOrderAnalyticRisk(port, curves=curve_list, regex=regex)
        expr_map = calc_a.instrument_risk(scale=kwargs.get('scale', BP))
        cache: dict = {}
        return {
            t_id: {f_id: expr.evaluate(e, env, cache) for f_id, e in row.items()}
            for t_id, row in expr_map.items()
        }

    def total_risk(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        curves: ZeroCurve | Sequence[ZeroCurve] = (),
        fit: Fit | Mapping[str, float] | None = None,
        risk_method: Any = None,
        regex: str | None = None,
        **kwargs: Any,
    ) -> dict[str, float]:
        per_inst = self.instrument_risk(portfolio, curves, fit, risk_method=risk_method, regex=regex, **kwargs)
        totals: dict[str, float] = {}
        for row in per_inst.values():
            for f_id, val in row.items():
                totals[f_id] = totals.get(f_id, 0.0) + val
        return totals

    def quote_risk(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        fitter: FitterType,
        fit: Fit | None = None,
        risk_method: Any = None,
        **kwargs: Any,
    ) -> dict[tuple[str, str], float]:
        fit0 = fit if fit is not None else fitter.fit()
        port = as_portfolio(portfolio, fitter.curves)
        method = risk_method or FirstOrderAnalyticRisk
        if isinstance(method, type) and issubclass(method, FirstOrderNumericalRisk):
            calc_bg = method(port, curves=fitter.curves, factors=fitter.factors)
            q_risk, _ = calc_bg.quote_risk(fitter, fit0, engine=self, **kwargs)
            return q_risk
        deltas = self.pillar_risk(port, fitter.curves, fit0, factors=fitter.factors)
        return FirstOrderAnalyticRisk.chain_quote_risk(deltas, fitter, fit0)

    def run(
        self,
        curves: ZeroCurve | Sequence[ZeroCurve],
        fitter: FitterType,
        trades: Sequence[Instrument],
        bumps: Sequence[tuple[str, float]] = (),
        unit: str | None = 'USD',
        *,
        include_bump_and_grind: bool = True,
        bump_method: str = 'forward',
    ) -> RiskRun:
        from ..risk import RiskRun

        fit = fitter.fit()
        curve_list = list(fitter.curves) if hasattr(fitter, 'curves') else (
            [curves] if isinstance(curves, ZeroCurve) else list(curves)
        )
        port = as_portfolio(trades, curve_list)
        vol_surfaces = port.vol_surfaces()
        all_factors = list(fitter.factors)
        seen_fids = {f.factor_id for f in all_factors}
        for f in port.factors(curve_list):
            if f.factor_id not in seen_fids:
                seen_fids.add(f.factor_id)
                all_factors.append(f)
        analytic = FirstOrderAnalyticRisk(port, curves=curve_list, factors=all_factors)
        book = analytic.populate_book(pure.BasisBook(), scale=BP)

        scenarios = [scenario(curve_list, 'BASE', fit, 0.0, unit, vol_surfaces=vol_surfaces)] + [
            scenario(curve_list, sid, fit, shift, unit, vol_surfaces=vol_surfaces) for sid, shift in bumps
        ]
        factors = fitter.factors
        quotes = fitter.quote_specs
        jacobian = [
            pure.JacobianEntry(
                s.scenario_id,
                factors[k].factor_id,
                quotes[j].quote_id,
                fit.dr_dq[k][j],
                quotes[j].pillar.tenor,
            )
            for s in scenarios
            for k in range(len(factors))
            for j in range(len(quotes))
            if abs(fit.dr_dq[k][j]) > 1e-18
        ]

        env = fit.env(curve_list, vol_surfaces)
        cache: dict = {}
        npv_map: dict[tuple[str, str], float] = {}
        notional_map: dict[tuple[str, str], float] = {}
        for t in port.trades:
            key = (t.book, t.trade_id)
            notional_map[key] = t.notional
            npv_map[key] = expr.evaluate(t.npv_on(curve_list), env, cache)

        delta_map = {
            key: expr.evaluate(d_expr, env, cache)
            for key, d_expr in analytic.trade_pillar_exprs(scale=BP).items()
        }
        quote_risk_map = FirstOrderAnalyticRisk.chain_quote_risk(delta_map, fitter, fit)

        bump_quote_risk_map: dict[tuple[str, str], float] = {}
        bump_fits: dict[str, Fit] = {}
        if include_bump_and_grind:
            bg = FirstOrderNumericalRisk(port, curves=curve_list, factors=factors)
            bump_quote_risk_map, bump_fits = bg.quote_risk(fitter, fit, engine=self, method=bump_method)
            for sid, b_fit in bump_fits.items():
                scenarios.append(scenario(curve_list, sid, b_fit, 0.0, unit, vol_surfaces=vol_surfaces))

        return RiskRun(
            book=book,
            scenarios=scenarios,
            jacobian=jacobian,
            fit=fit,
            npv=npv_map,
            delta=delta_map,
            quote_risk=quote_risk_map,
            notional=notional_map,
            bump_quote_risk=bump_quote_risk_map,
            bump_fits=bump_fits,
            engine_name=self.engine_name,
        )

    def session(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        fitter: FitterType,
        risk_method: Any = None,
        *,
        bump_method: str = 'forward',
        bump_bps: float = 1.0,
    ) -> PythonMarketSession:
        port = as_portfolio(portfolio, fitter.curves)
        return PythonMarketSession(
            port,
            fitter,
            risk_method=risk_method,
            bump_method=bump_method,
            bump_bps=bump_bps,
        )

