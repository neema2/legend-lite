"""Single-curve interest-rate swap using the telescoping floating-leg approximation N * (D(start) - D(end))."""

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
class IRSwapFixedFloatApprox(Instrument):
    """IRS with telescoping float leg: ``notional * (df(start) - df(end))``.

    Used for single-curve OIS swaps where the projection and discount curves are identical.
    """

    symbol: str = ''
    notional: float = 0.0
    fixed_rate: float = 0.0
    tenor_years: float = 0.0
    start: float = 0.0
    book: str = 'RATES_NY'
    side: str = 'RECEIVER'   # 'RECEIVER' (receive fixed, pay float) or 'PAYER' (pay fixed, receive float)
    currency: str = 'USD'
    collateral_currency: str = 'USD'
    curve: ZeroCurve | None = field(default=None, repr=False)
    is_target: bool = False

    def __post_init__(self) -> None:
        if self.currency != self.collateral_currency:
            raise ValueError(
                f'IRSwapFixedFloatApprox cannot have differing currency ({self.currency}) and '
                f'collateral_currency ({self.collateral_currency}). Please use IRSwapFixedFloat instead.'
            )

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
    def discount_curve(self) -> ZeroCurve | None:
        return self.curve

    @property
    def projection_curve(self) -> ZeroCurve | None:
        return self.curve

    @property
    def swap_type(self) -> str:
        return 'FixedFloatApprox (1-Curve)'

    def _resolve_curve(self, curve: ZeroCurve | None = None) -> ZeroCurve:
        c = curve if curve is not None else self.curve
        if c is None:
            raise ValueError(f'No curve provided for {self.symbol}')
        return c

    def curves(self, default_curves: Sequence[ZeroCurve] = ()) -> list[ZeroCurve]:
        c = self.curve or (default_curves[0] if default_curves else None)
        if c is None:
            raise ValueError(f'No curve provided for {self.symbol}')
        return [c]

    def coupon_payment_dates(self) -> list[float]:
        return payment_dates(self.tenor_years, self.start)

    def target_dates(self) -> list[float]:
        return payment_dates(self.tenor_years, self.start)

    def reset_dates(self) -> list[float]:
        return reset_dates(self.tenor_years, self.start)

    def annuity(self, curve: ZeroCurve | None = None) -> float | expr.Expr:
        c = self._resolve_curve(curve)
        a: float | expr.Expr = 0.0
        for r_t, p_t in zip(self.reset_dates(), self.coupon_payment_dates()):
            a = a + c.df(p_t) * day_count_fraction(r_t, p_t)
        return a

    def dv01(self, curve: ZeroCurve | None = None) -> float | expr.Expr:
        """DV01 = sum(notional * dt_i * df(t_i) * 0.0001)."""
        return self.annuity(curve) * (self.notional * BP)

    def fixed_leg_pv(self, curve: ZeroCurve | None = None) -> float | expr.Expr:
        """PV of fixed leg = dv01 * fixed_rate * 10000.0."""
        return self.annuity(curve) * (self.notional * self.fixed_rate)

    def float_leg(self, curve: ZeroCurve | None = None) -> float | expr.Expr:
        c = self._resolve_curve(curve)
        return c.df(self.start) - c.df(self.start + self.tenor_years)

    def float_leg_pv(self, curve: ZeroCurve | None = None) -> float | expr.Expr:
        """PV of floating leg = notional * (df(start) - df(end))."""
        return self.notional * self.float_leg(curve)

    def npv(self, curve: ZeroCurve | None = None, *_extra: ZeroCurve) -> float | expr.Expr:
        """NPV: RECEIVER = fixed - float, PAYER = float - fixed."""
        diff = self.fixed_leg_pv(curve) - self.float_leg_pv(curve)
        return diff if self.receive_fixed else -diff

    def par_rate(self, curve: ZeroCurve | None = None) -> float | expr.Expr:
        """Par rate: the fixed_rate at which NPV = 0."""
        return self.float_leg(curve) / self.annuity(curve)
