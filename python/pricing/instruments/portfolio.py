"""Named collection of ``Instrument`` expression trees across shared yield curves."""

from __future__ import annotations

from typing import Iterable, Sequence

from legend_lite.scalar_parallel import expr

from ..marketmodels.ir_curve_yield import ZeroCurve
from ..marketmodels.swaption_vol_surface import SwaptionVolSurface
from .base import Instrument


class Portfolio(Instrument):
    """Aggregates multiple ``Instrument``s (such as 1-curve ``IRSwapFixedFloatApprox`` and 2-curve ``IRSwapFixedFloat``)
    across shared discount and projection curves."""

    def __init__(
        self,
        trades: Iterable[Instrument] = (),
        default_curves: Sequence[ZeroCurve] = (),
        book: str = 'PORTFOLIO',
    ) -> None:
        self.symbol = 'PORTFOLIO'
        self.book = book
        self._trades: list[Instrument] = list(trades)
        self._by_id: dict[str, Instrument] = {t.trade_id: t for t in self._trades}
        self._default_curves: list[ZeroCurve] = list(default_curves)

    def add_instrument(self, name_or_inst: str | Instrument, instrument: Instrument | None = None) -> Instrument:
        if isinstance(name_or_inst, Instrument):
            inst = name_or_inst
            name = inst.trade_id
        else:
            if instrument is None:
                raise ValueError('instrument required when name is provided')
            name = name_or_inst
            inst = instrument
        self._trades.append(inst)
        self._by_id[name] = inst
        return inst

    @property
    def trades(self) -> list[Instrument]:
        return list(self._trades)

    @property
    def names(self) -> list[str]:
        return list(self._by_id.keys())

    @property
    def notional(self) -> float:  # type: ignore[override]
        return sum(abs(t.notional) for t in self._trades)

    def curves(self, default_curves: Sequence[ZeroCurve] = ()) -> list[ZeroCurve]:
        defaults = list(default_curves) if default_curves else self._default_curves
        out: list[ZeroCurve] = []
        seen: set[str] = set()
        for c in defaults:
            if c.curve_id not in seen:
                seen.add(c.curve_id)
                out.append(c)
        for t in self._trades:
            for c in t.curves(defaults):
                if c.curve_id not in seen:
                    seen.add(c.curve_id)
                    out.append(c)
        return out

    def vol_surfaces(self) -> list[SwaptionVolSurface]:
        out: list[SwaptionVolSurface] = []
        seen: set[str] = set()
        for t in self._trades:
            for vs in t.vol_surfaces():
                if vs.surface_id not in seen:
                    seen.add(vs.surface_id)
                    out.append(vs)
        return out

    def npv(self, *curves: ZeroCurve) -> expr.Expr:
        defaults = list(curves) if curves else self._default_curves
        total: expr.Expr = expr.ZERO
        for t in self._trades:
            total = total + t.npv_on(defaults)
        return total

    @property
    def npv_exprs(self) -> dict[str, expr.Expr]:
        """Named NPV expressions ``{trade_id: npv_expr}``."""
        return {name: inst.npv_on(self._default_curves) for name, inst in self._by_id.items()}

    def trade_npv_exprs(self, default_curves: Sequence[ZeroCurve] = ()) -> dict[tuple[str, str], expr.Expr]:
        """NPV expressions keyed by ``(book, trade_id)``."""
        defaults = list(default_curves) if default_curves else self._default_curves
        return {(t.book, t.trade_id): t.npv_on(defaults) for t in self._trades}

