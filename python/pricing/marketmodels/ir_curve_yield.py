"""Yield curves on the interpolation basis: zero rates, discount factors and forward rates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from legend_lite.scalar_parallel import basis, expr


@dataclass(frozen=True)
class Pillar:
    label: str
    tenor: float


class ZeroCurve:
    """Continuously compounded zero rates at pillars, linearly interpolated in time and flat beyond the first and the
    last; discount factors ``exp(-r(t) t)``, each a basis node over the two pillars around ``t``; and simple forward
    rates ``(df(start) / df(end) - 1) / (end - start)``.

    When ``rates`` is supplied (or via ``curve.numerical(rates)``), ``zero_rate(t)``, ``df(t)``, and
    ``fwd(start, end)`` return plain ``float``s so instrument pricing methods can be stepped through with plain numbers
    in a debugger.
    """

    def __init__(
        self,
        curve_id: str,
        pillars: Sequence[Pillar],
        currency: str = 'USD',
        rates: Sequence[float] | None = None,
    ) -> None:
        self.curve_id = curve_id
        self.pillars = list(pillars)
        self.currency = currency
        self.axis = basis.PiecewiseLinear(curve_id, [p.tenor for p in pillars])
        self._rates: list[float] | None = [float(r) for r in rates] if rates is not None else None
        self._env_cache: dict[str, float] | None = (
            {self.pillar(k).name: r for k, r in enumerate(self._rates)} if self._rates is not None else None
        )

    @property
    def name(self) -> str:
        return self.curve_id

    def numerical(self, rates: Any) -> 'ZeroCurve':
        """Returns a numerical view of this curve whose ``zero_rate``, ``df``, and ``fwd`` return plain ``float``s."""
        if hasattr(rates, 'rates_by_curve'):
            r_seq = rates.rates_by_curve.get(self.curve_id, rates.rates)
        else:
            r_seq = rates
        return ZeroCurve(self.curve_id, self.pillars, self.currency, rates=r_seq)

    def pillar(self, k: int) -> basis.Knot:
        """The k-th pillar's zero rate: a knot of the curve."""
        return self.axis.knot(k)

    def factor_id(self, k: int) -> str:
        """How the k-th pillar is named as a risk factor."""
        return f'{self.curve_id}:{self.pillars[k].label}'

    def zero_rate(self, t: float) -> float | expr.Expr:
        node = self.axis.lerp(round(float(t), 10))
        return expr.evaluate(node, self._env_cache) if self._env_cache is not None else node

    def interp(self, t: float) -> float | expr.Expr:
        return self.zero_rate(t)

    def df(self, t: float) -> float | expr.Expr:
        """The discount factor to time ``t`` (years)."""
        t_clean = round(float(t), 10)
        node = self.axis.exp_neg(t_clean, t_clean)
        return expr.evaluate(node, self._env_cache) if self._env_cache is not None else node

    def fwd(self, start: float, end: float) -> float | expr.Expr:
        """Simple forward rate over ``[start, end]``: ``(df(start) / df(end) - 1) / (end - start)``."""
        dt = round(float(end - start), 10)
        if dt <= 0.0:
            return 0.0 if self._env_cache is not None else expr.ZERO
        return (self.df(start) / self.df(end) - 1.0) / dt

    def env(self, rates: Sequence[float]) -> dict[str, float]:
        """The pillars' rates as the environment an expression over this curve is evaluated in."""
        return {self.pillar(k).name: r for k, r in enumerate(rates)}

