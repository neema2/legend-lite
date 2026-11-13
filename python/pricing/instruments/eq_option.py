"""European single-stock / index option (Black-Scholes-Merton) on ``legend_lite.scalar_parallel``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from legend_lite.scalar_parallel import expr

from ..marketmodels.ir_curve_yield import ZeroCurve
from ..maths.black import black_scholes_formula
from .base import Instrument


@dataclass
class EquityOption(Instrument):
    """European single-stock or equity-index option priced via Black-Scholes-Merton over a discount ``ZeroCurve`` and
    a dividend-yield ``ZeroCurve`` (with ``spot_price`` and ``volatility`` as scalars or traced ``expr.Expr`` /
    ``Knot`` inputs)."""

    symbol: str = ''
    book: str = 'EQ_DERIV'
    is_call: bool = True
    spot_price: float | expr.Expr = 100.0
    strike_price: float = 100.0
    tenor_years: float = 1.0
    volatility: float | expr.Expr = 0.20
    notional_shares: float = 1.0
    discount_curve: ZeroCurve | None = None
    dividend_curve: ZeroCurve | None = None

    @property
    def notional(self) -> float:  # type: ignore[override]
        sp = self.spot_price.value if isinstance(self.spot_price, expr.Const) else (
            float(self.spot_price) if isinstance(self.spot_price, (int, float)) else self.strike_price
        )
        return abs(float(self.notional_shares) * sp)

    def _resolve_curves(
        self,
        disc_curve: ZeroCurve | None = None,
        div_curve: ZeroCurve | None = None,
    ) -> tuple[ZeroCurve, ZeroCurve]:
        dc = disc_curve or self.discount_curve
        if dc is None:
            raise ValueError(f'EquityOption {self.symbol!r} requires a discount_curve')
        qc = div_curve or self.dividend_curve or dc
        return dc, qc

    def curves(self, default_curves: Sequence[ZeroCurve] = ()) -> list[ZeroCurve]:
        dc = self.discount_curve or (default_curves[0] if default_curves else None)
        qc = self.dividend_curve or (
            default_curves[1] if len(default_curves) > 1 else dc
        )
        out: list[ZeroCurve] = []
        for c in (dc, qc):
            if c is not None and all(x.curve_id != c.curve_id for x in out):
                out.append(c)
        return out

    def npv(
        self,
        disc_curve: ZeroCurve | None = None,
        div_curve: ZeroCurve | None = None,
        *_extra: Any,
    ) -> float | expr.Expr:
        if self.tenor_years <= 0.0 or (isinstance(self.volatility, (int, float)) and self.volatility <= 0.0):
            return 0.0
        dc, qc = self._resolve_curves(disc_curve, div_curve)
        t = float(self.tenor_years)
        df_r = dc.df(t)
        df_q = qc.df(t)
        r_rate = dc.zero_rate(t)
        q_rate = qc.zero_rate(t)
        unit_price = black_scholes_formula(
            self.is_call,
            self.spot_price,
            self.strike_price,
            t,
            self.volatility,
            df_r,
            df_q,
            r_rate=r_rate,
            q_rate=q_rate,
        )
        return unit_price * self.notional_shares

