"""Shared base classes and constants for first-order risk calculators (analytic symbolic differentiation vs. naive
bump-and-grind quote refit & reprice)."""

from __future__ import annotations

import re
from typing import Sequence, Union

from legend_lite.scalar_parallel import basis_pure as pure, expr

from ..instruments.base import Instrument
from ..instruments.ir_swap_fixed_float import IRSwapFixedFloat
from ..instruments.ir_swap_fixed_floatapprox import IRSwapFixedFloatApprox
from ..instruments.portfolio import Portfolio
from ..marketmodels.ir_curve_fitter import CurveFitter, FactorSpec, Fit, MultiCurveFitter
from ..marketmodels.ir_curve_yield import ZeroCurve
from ..marketmodels.swaption_vol_surface import SwaptionVolSurface

SwapInstrument = Union[IRSwapFixedFloatApprox, IRSwapFixedFloat, Instrument]
FitterType = Union[CurveFitter, MultiCurveFitter]

NPV = 'NPV'
PILLAR_DELTA = 'PILLAR_DELTA'   # d NPV / d pillar zero rate, per basis point
BP = 1e-4


def as_portfolio(
    instrument_or_trades: Instrument | Portfolio | Sequence[Instrument],
    curves: ZeroCurve | Sequence[ZeroCurve] = (),
) -> Portfolio:
    """Normalizes a single ``Instrument``, a ``Portfolio``, or a sequence of trades into a ``Portfolio``."""
    curve_list = [curves] if isinstance(curves, ZeroCurve) else list(curves)
    if isinstance(instrument_or_trades, Portfolio):
        if curve_list and not instrument_or_trades._default_curves:
            return Portfolio(instrument_or_trades.trades, curve_list, book=instrument_or_trades.book)
        return instrument_or_trades
    if isinstance(instrument_or_trades, Instrument):
        return Portfolio([instrument_or_trades], curve_list, book=instrument_or_trades.book)
    return Portfolio(list(instrument_or_trades), curve_list)


def swap_npv(swap: Instrument, curves: Sequence[ZeroCurve] = ()) -> expr.Expr:
    """Resolves a swap or instrument's ``npv`` expression tree over ``curves``."""
    return swap.npv_on(curves)


def scenario(
    curves: ZeroCurve | Sequence[ZeroCurve],
    scenario_id: str,
    rates_or_fit: Sequence[float] | Fit,
    shift: float = 0.0,
    unit: str | None = None,
    *,
    vol_surfaces: Sequence[SwaptionVolSurface] = (),
    vol_shift: float = 0.0,
) -> pure.Scenario:
    """Builds a ``pure.Scenario`` for ``CBS_KNOT`` from a ``Fit`` or rate vector, optional parallel ``shift``, and any
    2D ``SwaptionVolSurface`` knot series."""
    curve_list = [curves] if isinstance(curves, ZeroCurve) else list(curves)
    knots_by_series: dict[str, list[tuple[str, float, float]]] = {}
    for c in curve_list:
        if isinstance(rates_or_fit, Fit):
            rates = rates_or_fit.rates_by_curve.get(c.curve_id, rates_or_fit.rates)
        else:
            rates = rates_or_fit
        knots_by_series[c.curve_id] = [
            (c.factor_id(k), p.tenor, r + shift)
            for k, (p, r) in enumerate(zip(c.pillars, rates))
        ]
    for vs in vol_surfaces:
        vols = rates_or_fit.vols_by_surface.get(vs.surface_id) if isinstance(rates_or_fit, Fit) else None
        knots_by_series.update(vs.scenario_series(vols, shift=vol_shift))
    return pure.Scenario(scenario_id, knots_by_series, unit)


def quote_risk_query(
    scenario_id: str | None = None,
    book_or_schema: pure.BasisBook | pure.BasisSchema | None = None,
) -> str:
    """Each book's risk to each par quote, per basis point: its pillar deltas carried through d pillar / d quote."""
    return pure.chained_query(PILLAR_DELTA, scenario_id, book_or_schema=book_or_schema)


class FirstOrderRiskBase:
    """Base class for first-order risk calculators (Analytic Symbolic Risk and Naive Bump & Grind Risk)."""

    def __init__(
        self,
        instrument: Instrument | Portfolio | Sequence[Instrument],
        curves: ZeroCurve | Sequence[ZeroCurve] = (),
        factors: Sequence[FactorSpec] | None = None,
        regex: str | None = None,
        name: str = 'Risk',
    ) -> None:
        self.portfolio = as_portfolio(instrument, curves)
        self.instrument = self.portfolio if not isinstance(instrument, Instrument) else instrument
        self._explicit_factors = list(factors) if factors is not None else None
        self.regex = regex
        self.name = name

    @property
    def trades(self) -> list[Instrument]:
        return self.portfolio.trades

    @property
    def curves(self) -> list[ZeroCurve]:
        return self.portfolio.curves()

    @property
    def vol_surfaces(self) -> list[SwaptionVolSurface]:
        return self.portfolio.vol_surfaces()

    def factors(self) -> list[FactorSpec]:
        """Discovers and optionally regex-filters the relevant curve pillar factors."""
        all_factors = self._explicit_factors if self._explicit_factors is not None else self.portfolio.factors()
        if not self.regex:
            return all_factors
        pattern = re.compile(self.regex)
        return [f for f in all_factors if pattern.search(f.factor_id) or pattern.search(f.knot.name)]

    def _pillars(self) -> list[str]:
        return [f.factor_id for f in self.factors()]

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} name={self.name!r} regex={self.regex!r} trades={len(self.trades)}>'

