"""European interest-rate swaption (``IRSwaption``) priced via Black-76 over an underlying ``IRSwapFixedFloat`` and
2D ``SwaptionVolSurface``."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Sequence

from legend_lite.scalar_parallel import expr

from ..marketmodels.swaption_vol_surface import SwaptionVolSurface
from ..maths.black import black_formula
from .base import Instrument
from .ir_swap_fixed_float import IRSwapFixedFloat

if TYPE_CHECKING:
    from ..marketmodels.ir_curve_fitter import FactorSpec
    from ..marketmodels.ir_curve_yield import ZeroCurve


@dataclass
class IRSwaption(Instrument):
    """European payer or receiver swaption into a forward-starting ``IRSwapFixedFloat``.

    Prices in Black-76 annuity measure:
      - ``swap         = underlying_swap(discount_curve, projection_curve)``
      - ``ann_notional = swap.annuity(discount_curve) * notional``
      - ``fwd_rate     = swap.float_leg_pv(discount_curve, projection_curve) / ann_notional``
      - ``vol          = vol_surface.vol(expiry_years, swap_tenor)``
      - ``npv          = black_formula(is_payer, fwd_rate, strike_rate, expiry_years, vol, ann_notional)``

    Authored as pure domain pricing code with zero SQL or staging annotations:
      - Under numerical curves/surfaces (``.numerical(...)``), evaluates directly to plain ``float`` values.
      - Under symbolic curves/surfaces, ``BasisBook`` automatically factors ``ann_notional``, ``float_leg_pv``, and
        2D bilinear ``vol`` into Stage-0 ``SubMeasure``s and differentiates the Stage-1 Black-76 formula in Pure via
        ``meta::legend::lite::diff``.
    """

    symbol: str = ''
    notional: float = 10_000_000.0
    strike_rate: float = 0.035
    expiry_years: float = 1.0
    swap_tenor: float = 5.0
    is_payer: bool = True
    float_spread: float = 0.0
    book: str = 'RATES_VOL'
    currency: str = 'USD'
    collateral_currency: str = 'USD'
    discount_curve: ZeroCurve | None = field(default=None, repr=False)
    projection_curve: ZeroCurve | None = field(default=None, repr=False)
    vol_surface: SwaptionVolSurface | float | expr.Expr = field(default=0.20, repr=False)

    @property
    def trade_id(self) -> str:
        return self.symbol

    @property
    def side(self) -> str:
        return 'PAYER' if self.is_payer else 'RECEIVER'

    @property
    def receive_fixed(self) -> bool:
        return not self.is_payer

    @property
    def start(self) -> float:
        return float(self.expiry_years)

    @property
    def years(self) -> int:
        return int(round(self.swap_tenor))

    @property
    def fixed_rate(self) -> float:
        return float(self.strike_rate)

    @property
    def swap_type(self) -> str:
        return 'Swaption (Black-76)'

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

    def vol_surfaces(self) -> list[SwaptionVolSurface]:
        if isinstance(self.vol_surface, SwaptionVolSurface) and not self.vol_surface.is_numerical:
            return [self.vol_surface]
        return []

    def underlying_swap(
        self,
        discount_curve: ZeroCurve | None = None,
        projection_curve: ZeroCurve | None = None,
    ) -> IRSwapFixedFloat:
        disc, proj = self._resolve_curves(discount_curve, projection_curve)
        return IRSwapFixedFloat(
            symbol=f'{self.symbol}_SWAP',
            notional=self.notional,
            fixed_rate=self.strike_rate,
            float_spread=self.float_spread,
            tenor_years=self.swap_tenor,
            start=self.expiry_years,
            book=self.book,
            side=self.side,
            currency=self.currency,
            collateral_currency=self.collateral_currency,
            discount_curve=disc,
            projection_curve=proj,
        )

    def annuity(self, discount_curve: ZeroCurve | None = None) -> float | expr.Expr:
        disc, _ = self._resolve_curves(discount_curve, discount_curve)
        return self.underlying_swap(disc, disc).annuity(disc)

    def forward_swap_rate(
        self,
        discount_curve: ZeroCurve | None = None,
        projection_curve: ZeroCurve | None = None,
    ) -> float | expr.Expr:
        disc, proj = self._resolve_curves(discount_curve, projection_curve)
        swap = self.underlying_swap(disc, proj)
        return swap.float_leg_pv(disc, proj) / (swap.annuity(disc) * self.notional)

    def npv_on(self, default_curves: Sequence[ZeroCurve] = ()) -> expr.Expr:
        disc = self.discount_curve or (default_curves[0] if default_curves else None)
        proj = self.projection_curve or (
            default_curves[1] if len(default_curves) > 1 else disc
        )
        return expr.lift(self.npv(disc, proj))

    def npv(
        self,
        discount_curve: ZeroCurve | None = None,
        projection_curve: ZeroCurve | None = None,
        vol_surface: SwaptionVolSurface | float | expr.Expr | None = None,
    ) -> float | expr.Expr:
        disc, proj = self._resolve_curves(discount_curve, projection_curve)
        vs = self.vol_surface if vol_surface is None else vol_surface
        vol = vs.vol(self.expiry_years, self.swap_tenor) if isinstance(vs, SwaptionVolSurface) else vs

        swap = self.underlying_swap(disc, proj)
        ann_notional = swap.annuity(disc) * self.notional
        flt_pv = swap.float_leg_pv(disc, proj)
        fwd_rate = flt_pv / ann_notional
        return black_formula(
            is_call=self.is_payer,
            forward=fwd_rate,
            strike=self.strike_rate,
            time_to_expiry=self.expiry_years,
            vol=vol,
            discount_factor=ann_notional,
        )


__all__ = ['IRSwaption']
