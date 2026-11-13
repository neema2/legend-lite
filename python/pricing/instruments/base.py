"""Base class for financial instruments valued as ``legend_lite.scalar_parallel.expr.Expr`` trees."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Sequence

from legend_lite.scalar_parallel import expr

if TYPE_CHECKING:
    from ..marketmodels.ir_curve_fitter import FactorSpec
    from ..marketmodels.ir_curve_yield import ZeroCurve
    from ..marketmodels.swaption_vol_surface import SwaptionVolSurface


class Instrument(ABC):
    """Base class for financial instruments.

    Instruments define their cashflow and valuation formulas (``npv()``, ``par_rate()``, ``annuity()``, ``dv01()``) as
    ``expr.Expr`` trees over ``ZeroCurve``s. Risk definitions (analytic symbolic differentiation vs. naive bump-and-grind
    quote refit & reprice) live in ``pricing.risk`` and execution strategies (local Python vs. Pure/SQL/DuckDB) live in
    ``pricing.engines``.
    """

    symbol: str = ''
    book: str = 'RATES_NY'
    notional: float = 0.0

    @property
    def trade_id(self) -> str:
        return self.symbol

    @property
    def swap_type(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def npv(self, *curves: ZeroCurve) -> expr.Expr:
        """The Net Present Value expression tree of the instrument."""

    @abstractmethod
    def curves(self, default_curves: Sequence[ZeroCurve] = ()) -> list[ZeroCurve]:
        """The ordered unique ``ZeroCurve``s referenced by this instrument."""

    def vol_surfaces(self) -> list[SwaptionVolSurface]:
        """The ordered unique ``SwaptionVolSurface``s referenced by this instrument."""
        return []

    def npv_on(self, default_curves: Sequence[ZeroCurve] = ()) -> expr.Expr:
        """Evaluates ``self.npv(...)`` resolving any unbound curves from ``default_curves``."""
        resolved = self.curves(default_curves)
        return self.npv(*resolved)

    def factors(self, default_curves: Sequence[ZeroCurve] = ()) -> list[FactorSpec]:
        """All curve pillar and volatility surface knot factors referenced by this instrument."""
        from ..marketmodels.ir_curve_fitter import FactorSpec

        out: list[FactorSpec] = []
        seen: set[str] = set()
        for c in self.curves(default_curves):
            if c.curve_id in seen:
                continue
            seen.add(c.curve_id)
            for k, p in enumerate(c.pillars):
                out.append(FactorSpec(c.factor_id(k), c, k, p, c.pillar(k)))
        seen_vs: set[str] = set()
        for vs in self.vol_surfaces():
            if vs.surface_id in seen_vs:
                continue
            seen_vs.add(vs.surface_id)
            out.extend(vs.factors())
        return out

    @property
    def pillar_names(self) -> list[str]:
        """Factor IDs of all curve pillars referenced by this instrument."""
        return [f.factor_id for f in self.factors()]

