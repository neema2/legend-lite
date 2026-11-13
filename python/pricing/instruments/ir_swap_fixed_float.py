"""Explicit floating-leg interest-rate swap priced across separate discount and projection curves."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Sequence

from legend_lite.scalar_parallel import expr

from .base import Instrument
from .ir_scheduling import day_count_fraction, payment_dates, reset_dates

if TYPE_CHECKING:
    from ..marketmodels.ir_curve_yield import ZeroCurve

BP = 1e-4


@dataclass
class IRSwapFixedFloat(Instrument):
    """IRS with explicit floating coupon cashflows projected off ``projection_curve`` and discounted on
    ``discount_curve``.

    For each coupon period ``[start_i, end_i]``:
      - ``rate_i = projection_curve.fwd(start_i, end_i) + float_spread``
      - ``df_i   = discount_curve.df(end_i)``
      - ``pv_i   = rate_i * df_i * notional * dt_i``

    In the interpolation basis, ``projection_curve.fwd(start_i, end_i) * discount_curve.df(end_i)`` expands into a
    degree-3 product of shared exponential basis nodes:
      ``exp(-start_i * r_proj(start_i)) * exp(+end_i * r_proj(end_i)) * exp(-end_i * r_disc(end_i))``
    so both the NPV and all its symbolic derivatives with respect to every discount and projection pillar stay closed in
    the basis and compile to compact SQL without exploding expression trees.
    """

    symbol: str = ''
    notional: float = 0.0
    fixed_rate: float = 0.0
    float_spread: float = 0.0
    tenor_years: float = 0.0
    start: float = 0.0
    book: str = 'RATES_NY'
    side: str = 'RECEIVER'   # 'RECEIVER' (receive fixed, pay float) or 'PAYER' (pay fixed, receive float)
    currency: str = 'USD'
    collateral_currency: str = 'USD'
    discount_curve: ZeroCurve | None = field(default=None, repr=False)
    projection_curve: ZeroCurve | None = field(default=None, repr=False)
    is_target: bool = False

    @property
    def trade_id(self) -> str:
        return self.symbol

    @property
    def years(self) -> int:
        return int(round(self.tenor_years))

    @property
    def receive_fixed(self) -> bool:
        return self.side.upper() == 'RECEIVER'

    @property
    def swap_type(self) -> str:
        return 'FixedFloat (2-Curve)'

    def _resolve_curves(
        self,
        discount_curve: ZeroCurve | None = None,
        projection_curve: ZeroCurve | None = None,
    ) -> tuple[ZeroCurve, ZeroCurve]:
        disc = discount_curve if discount_curve is not None else self.discount_curve
        proj = projection_curve if projection_curve is not None else (self.projection_curve or disc)
        if disc is None or proj is None:
            raise ValueError(f'discount_curve and projection_curve required for {self.symbol}')
        return disc, proj

    def curves(self, default_curves: Sequence[ZeroCurve] = ()) -> list[ZeroCurve]:
        disc = self.discount_curve or (default_curves[0] if default_curves else None)
        proj = self.projection_curve or (
            default_curves[1] if len(default_curves) > 1 else disc
        )
        if disc is None or proj is None:
            raise ValueError(f'discount_curve and projection_curve required for {self.symbol}')
        return [disc, proj] if disc.curve_id != proj.curve_id else [disc]

    def npv_on(self, default_curves: Sequence[ZeroCurve] = ()) -> expr.Expr:
        disc = self.discount_curve or (default_curves[0] if default_curves else None)
        proj = self.projection_curve or (
            default_curves[1] if len(default_curves) > 1 else disc
        )
        return self.npv(disc, proj)

    def target_dates(self) -> list[float]:
        return payment_dates(self.tenor_years, self.start)

    def reset_dates(self) -> list[float]:
        return reset_dates(self.tenor_years, self.start)

    def _safe_dt(self, t1: float, t2: float) -> float:
        return day_count_fraction(t1, t2)

    def annuity(self, discount_curve: ZeroCurve | None = None) -> float | expr.Expr:
        disc, _ = self._resolve_curves(discount_curve, discount_curve)
        a: float | expr.Expr = 0.0
        for r_t, p_t in zip(self.reset_dates(), self.target_dates()):
            a = a + disc.df(p_t) * self._safe_dt(r_t, p_t)
        return a

    def dv01(self, discount_curve: ZeroCurve | None = None) -> float | expr.Expr:
        """Sum of payment periods * discount(T) * notional * 0.0001."""
        return self.annuity(discount_curve) * (self.notional * BP)

    def fixed_leg_pv(self, discount_curve: ZeroCurve | None = None) -> float | expr.Expr:
        """PV of fixed leg = dv01 * fixed_rate * 10000.0."""
        return self.annuity(discount_curve) * (self.notional * self.fixed_rate)

    def float_leg_pv(
        self,
        discount_curve: ZeroCurve | None = None,
        projection_curve: ZeroCurve | None = None,
    ) -> float | expr.Expr:
        """Explicit floating PV for multi-curve pricing: sum(rate_i * df_i * notional * dt_i)."""
        disc, proj = self._resolve_curves(discount_curve, projection_curve)
        pv: float | expr.Expr = 0.0
        for start, end in zip(self.reset_dates(), self.target_dates()):
            dt = self._safe_dt(start, end)
            rate = proj.fwd(start, end) + self.float_spread
            df = disc.df(end)
            pv = pv + rate * df * (self.notional * dt)
        return pv

    def npv(
        self,
        discount_curve: ZeroCurve | None = None,
        projection_curve: ZeroCurve | None = None,
    ) -> float | expr.Expr:
        """NPV: RECEIVER = fixed - float, PAYER = float - fixed."""
        diff = self.fixed_leg_pv(discount_curve) - self.float_leg_pv(discount_curve, projection_curve)
        return diff if self.receive_fixed else -diff

    def par_rate(
        self,
        discount_curve: ZeroCurve | None = None,
        projection_curve: ZeroCurve | None = None,
    ) -> float | expr.Expr:
        """Par rate: the fixed_rate at which NPV = 0."""
        disc, proj = self._resolve_curves(discount_curve, projection_curve)
        return self.float_leg_pv(disc, proj) / (self.annuity(disc) * self.notional)
