"""First-order analytic risk via symbolic differentiation (``expr.diff``) and Jacobian chain rule."""

from __future__ import annotations

from typing import Mapping, Sequence

from legend_lite.scalar_parallel import basis_pure as pure, expr

from ..instruments.base import Instrument
from ..instruments.portfolio import Portfolio
from ..marketmodels.ir_curve_fitter import FactorSpec, Fit
from ..marketmodels.ir_curve_yield import ZeroCurve
from .base import BP, FitterType, FirstOrderRiskBase, NPV, PILLAR_DELTA


class FirstOrderAnalyticRisk(FirstOrderRiskBase):
    """Calculates first-order sensitivities (Zero-Rate Pillar Deltas and Par-Quote DV01s) via symbolic differentiation.

    Moves analytic differentiation off the individual instruments into a shared risk definition:
      1. Differentiates each instrument's symbolic ``npv()`` expression tree with respect to every curve pillar knot
         ``f.knot`` using ``expr.diff(npv, f.knot)``.
      2. Chains those pillar deltas through the curve fitter's inverse Jacobian ``d pillar_k / d quote_j``
         (``fit.dr_dq[k][j]``) to obtain benchmark par-quote risk without refitting curves or repricing trades.
    """

    def __init__(
        self,
        instrument: Instrument | Portfolio | Sequence[Instrument],
        curves: ZeroCurve | Sequence[ZeroCurve] = (),
        factors: Sequence[FactorSpec] | None = None,
        regex: str | None = None,
        name: str = 'AnalyticRisk',
    ) -> None:
        super().__init__(instrument, curves=curves, factors=factors, regex=regex, name=name)
        self._memo: dict[tuple[int, str], expr.Expr] = {}
        self._knot_memos: dict[str, dict[expr.Expr, expr.Expr]] = {}

    def diff_expr(self, target: expr.Expr, knot: expr.Var) -> expr.Expr:
        key = (id(target), knot.name)
        if key not in self._memo:
            k_memo = self._knot_memos.setdefault(knot.name, {})
            self._memo[key] = expr.diff(target, knot, k_memo)
        return self._memo[key]

    def risk_ladder(self, instrument: Instrument | None = None, *, scale: float = 1.0) -> dict[str, expr.Expr]:
        """Symbolic pillar derivative map ``{factor_id: d(npv)/d(pillar) * scale}`` for a single instrument."""
        inst = instrument or (self.trades[0] if len(self.trades) == 1 else self.portfolio)
        n = inst.npv_on(self.curves)
        out: dict[str, expr.Expr] = {}
        for f in self.factors():
            d = self.diff_expr(n, f.knot)
            out[f.factor_id] = d * scale if (scale != 1.0 and d is not expr.ZERO) else d
        return out

    def total_risk(self, *, scale: float = 1.0) -> dict[str, expr.Expr]:
        """Portfolio-level symbolic pillar derivatives ``{factor_id: d(total_npv)/d(pillar) * scale}``."""
        return self.risk_ladder(self.portfolio, scale=scale)

    def instrument_risk(self, *, scale: float = 1.0) -> dict[str, dict[str, expr.Expr]]:
        """Per-instrument symbolic pillar derivatives ``{trade_id: {factor_id: d(npv)/d(pillar) * scale}}``."""
        factors = self.factors()
        defaults = self.curves
        res: dict[str, dict[str, expr.Expr]] = {}
        for t in self.trades:
            n = t.npv_on(defaults)
            row: dict[str, expr.Expr] = {}
            for f in factors:
                d = self.diff_expr(n, f.knot)
                row[f.factor_id] = d * scale if (scale != 1.0 and d is not expr.ZERO) else d
            res[t.trade_id] = row
        return res

    def trade_pillar_exprs(self, *, scale: float = BP) -> dict[tuple[str, str, str], expr.Expr]:
        """Non-zero symbolic pillar delta expressions keyed by ``(book, trade_id, factor_id)``."""
        factors = self.factors()
        defaults = self.curves
        out: dict[tuple[str, str, str], expr.Expr] = {}
        for t in self.trades:
            n = t.npv_on(defaults)
            for f in factors:
                d = self.diff_expr(n, f.knot) * scale
                if d is not expr.ZERO:
                    out[(t.book, t.trade_id, f.factor_id)] = d
        return out

    def populate_book(
        self,
        book: pure.BasisBook | None = None,
        *,
        scale: float = BP,
    ) -> pure.BasisBook:
        """Expands every trade's ``NPV`` and non-zero ``PILLAR_DELTA`` sensitivities into a ``BasisBook``,
        differentiating non-polynomial formulas and hierarchical sub-measures in Pure via ``meta::legend::lite::diff``."""
        target_book = book if book is not None else pure.BasisBook()
        factors = self.factors()
        defaults = self.curves
        for t in self.trades:
            n = t.npv_on(defaults)
            target_book.add(t.trade_id, t.book, NPV, '*', n)
            for f in factors:
                target_book.add_pure_diff(
                    t.trade_id, t.book, PILLAR_DELTA, f.factor_id, n, f.knot, scale=scale
                )
        return target_book

    def evaluate(self, ctx_or_fit: Mapping[str, float] | Fit, *, scale: float = 1.0) -> dict[str, float]:
        """Evaluates ``total_risk(scale=scale)`` in Python against ``ctx_or_fit``."""
        env = (
            ctx_or_fit.env(self.curves, self.vol_surfaces)
            if isinstance(ctx_or_fit, Fit)
            else dict(ctx_or_fit)
        )
        cache: dict = {}
        return {f_id: expr.evaluate(e, env, cache) for f_id, e in self.total_risk(scale=scale).items()}

    @staticmethod
    def chain_quote_risk(
        pillar_deltas: Mapping[tuple[str, str, str], float],
        fitter: FitterType,
        fit: Fit,
    ) -> dict[tuple[str, str], float]:
        """Chains ``(book, trade_id, factor_id)`` pillar deltas through ``fit.dr_dq`` to ``(book, quote_id)`` DV01s."""
        quote_risk: dict[tuple[str, str], float] = {}
        for (book, _trade_id, qid), val in FirstOrderAnalyticRisk.instrument_quote_risk(
            pillar_deltas, fitter, fit
        ).items():
            key = (book, qid)
            quote_risk[key] = quote_risk.get(key, 0.0) + val
        return quote_risk

    @staticmethod
    def instrument_quote_risk(
        pillar_deltas: Mapping[tuple[str, str, str], float],
        fitter: FitterType,
        fit: Fit,
    ) -> dict[tuple[str, str, str], float]:
        """Chains ``(book, trade_id, factor_id)`` pillar deltas through ``fit.dr_dq`` to per-trade
        ``(book, trade_id, quote_id)`` DV01s."""
        factor_idx = {f.factor_id: k for k, f in enumerate(fitter.factors)}
        quotes = fitter.quote_specs
        out: dict[tuple[str, str, str], float] = {}
        for (book, trade_id, factor_id), delta_val in pillar_deltas.items():
            k = factor_idx.get(factor_id)
            if k is None:
                continue
            for j, q_spec in enumerate(quotes):
                w = fit.dr_dq[k][j]
                if abs(w) > 1e-18:
                    key = (book, trade_id, q_spec.quote_id)
                    out[key] = out.get(key, 0.0) + delta_val * w
        return out

