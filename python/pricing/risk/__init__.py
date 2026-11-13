"""Shared risk definitions for swap portfolios across single and multi-curve market models:
  - ``FirstOrderAnalyticRisk``: symbolic differentiation (``expr.diff``) with respect to zero-rate pillars chained
    through the curve fitter's inverse Jacobian ``d pillar / d quote``.
  - ``FirstOrderNumericalRisk``: numerical finite-difference bump of each input benchmark quote with curve refitting
    (``fitter.fit_quotes``) and portfolio repricing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Sequence

from legend_lite.scalar_parallel import basis_pure as pure

from ..instruments.base import Instrument
from ..marketmodels.ir_curve_fitter import Fit
from ..marketmodels.ir_curve_yield import ZeroCurve
from .base import (
    BP,
    FitterType,
    FirstOrderRiskBase,
    NPV,
    PILLAR_DELTA,
    SwapInstrument,
    as_portfolio,
    quote_risk_query,
    scenario,
    swap_npv,
)
from .firstorder_numerical import FirstOrderNumericalRisk
from .firstorder_analytic import FirstOrderAnalyticRisk

if TYPE_CHECKING:
    from ..engines.base import ExecutionEngine


@dataclass
class RiskRun:
    """The basis book and scenarios to write, and reference values:
      - ``npv`` by ``(book, trade)``
      - ``delta`` (pillar delta per 1bp) by ``(book, trade, factor)``
      - ``quote_risk`` (analytic par-quote DV01 via Jacobian chain rule) by ``(book, quote)``
      - ``bump_quote_risk`` (naive bump-refit-reprice par-quote DV01) by ``(book, quote)``
      - ``bump_fits`` (refitted ``Fit`` per bumped input quote)
    """

    book: pure.BasisBook
    scenarios: list[pure.Scenario]
    jacobian: list[pure.JacobianEntry]
    fit: Fit
    npv: dict[tuple[str, str], float] = field(default_factory=dict)
    delta: dict[tuple[str, str, str], float] = field(default_factory=dict)
    quote_risk: dict[tuple[str, str], float] = field(default_factory=dict)
    notional: dict[tuple[str, str], float] = field(default_factory=dict)
    bump_quote_risk: dict[tuple[str, str], float] = field(default_factory=dict)
    bump_fits: dict[str, Fit] = field(default_factory=dict)
    engine_name: str = 'pure_duckdb'

    @property
    def total_bump_newton_steps(self) -> int:
        """Total Newton steps used across all naive per-quote bump-and-refit solves."""
        return sum(f.iterations for f in self.bump_fits.values())


def risk_run(
    curves: ZeroCurve | Sequence[ZeroCurve],
    fitter: FitterType,
    trades: Sequence[Instrument],
    bumps: Sequence[tuple[str, float]] = (),
    unit: str | None = 'USD',
    *,
    engine: str | ExecutionEngine = 'pure_duckdb',
    include_bump_and_grind: bool = True,
    bump_method: str = 'forward',
) -> RiskRun:
    """Fits the curves and evaluates the portfolio's NPVs, zero-rate pillar deltas, analytic par-quote risk, and
    naive bump-refit-reprice par-quote risk using the chosen ``engine`` (``'python'`` or ``'pure_duckdb'``)."""
    from ..engines import resolve_engine

    eng = resolve_engine(engine)
    return eng.run(
        curves,
        fitter,
        trades,
        bumps,
        unit,
        include_bump_and_grind=include_bump_and_grind,
        bump_method=bump_method,
    )


def resolve_risk_method(method: str | type[FirstOrderRiskBase] | None = None) -> type[FirstOrderRiskBase]:
    """Resolves a risk method class or name (``'analytic'`` -> ``FirstOrderAnalyticRisk``,
    ``'numerical'`` / ``'bump_and_grind'`` -> ``FirstOrderNumericalRisk``)."""
    if method is None:
        return FirstOrderAnalyticRisk
    if isinstance(method, type) and issubclass(method, FirstOrderRiskBase):
        return method
    if isinstance(method, str):
        key = method.lower().strip()
        if key in ('analytic', 'firstorder_analytic', 'symbolic', 'jacobian'):
            return FirstOrderAnalyticRisk
        if key in ('numerical', 'firstorder_numerical', 'bump', 'bump_and_grind', 'fd'):
            return FirstOrderNumericalRisk
    raise ValueError(f"Unknown risk method {method!r}; expected 'analytic' or 'numerical'")


__all__ = [
    'BP',
    'NPV',
    'PILLAR_DELTA',
    'SwapInstrument',
    'FitterType',
    'FirstOrderRiskBase',
    'FirstOrderAnalyticRisk',
    'FirstOrderNumericalRisk',
    'RiskRun',
    'risk_run',
    'resolve_risk_method',
    'scenario',
    'quote_risk_query',
    'as_portfolio',
    'swap_npv',
]

