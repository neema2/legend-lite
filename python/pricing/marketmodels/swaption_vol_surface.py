"""2D expiry x swap-tenor ATM volatility surface (``SwaptionVolSurface``) on ``legend_lite.scalar_parallel.basis``.

Represents each expiry row ``expiries[i]`` as a 1D ``basis.PiecewiseLinear`` series over ``tenors`` and interpolates
linearly across bounding expiry rows. Every 2D bilinear volatility lookup ``surface.vol(expiry, tenor)`` over up to
four surface knots lowers directly into ``CBS_KNOT`` and ``CBS_SHARED_NODE`` without modifying the 2-knot ``BasisNode``
primitive:
  - On an expiry grid row (``w_exp == 0``): 1 ``BasisNode(LERP)`` over the 2 bounding tenor knots.
  - Between expiry grid rows (``0 < w_exp < 1``): ``(1 - w_exp) * row_i0.lerp(tenor) + w_exp * row_i1.lerp(tenor)``,
    which ``BasisBook`` automatically factors into a shared Stage-0 ``SubMeasure`` when feeding ``black_formula``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from legend_lite.scalar_parallel import basis, expr

from .ir_curve_yield import Pillar

if TYPE_CHECKING:
    from .ir_curve_fitter import FactorSpec


def _tenor_tag(years: float) -> str:
    months = round(years * 12.0)
    if abs(years * 12.0 - months) < 1e-9:
        if months % 12 == 0:
            return f'{months // 12}Y'
        return f'{months}M'
    return f'{years:g}Y'


class SwaptionVolSurface:
    """2D ATM swaption volatility surface ``sigma(expiry, swap_tenor)`` with bilinear interpolation and flat clamping.

    Dual-mode execution (matching ``ZeroCurve``):
      - Symbolic mode (default): ``vol(expiry, tenor)`` returns a traced ``expr.Expr`` over surface knot variables so
        both Python ``expr.diff`` and SQL / Pure ``diff()`` compute exact 4-knot bilinear Vegas per surface knot.
      - Numerical debug mode (``surface.numerical(...)``): ``vol(expiry, tenor)`` returns plain Python ``float`` values
        for interactive step-through debugging.
    """

    def __init__(
        self,
        surface_id: str,
        expiries: Sequence[float],
        tenors: Sequence[float],
        atm_vols: Sequence[Sequence[float]] | None = None,
        *,
        _is_numerical: bool = False,
    ) -> None:
        if not expiries or not tenors:
            raise ValueError('expiries and tenors must be non-empty')
        self.surface_id = surface_id
        self.expiries = [float(x) for x in expiries]
        self.tenors = [float(x) for x in tenors]
        self._expiry_axis = basis.PiecewiseLinear(f'{surface_id}:EXP', self.expiries)
        self._rows = [
            basis.PiecewiseLinear(self.row_series_id(i), self.tenors)
            for i in range(len(self.expiries))
        ]
        self.atm_vols: list[list[float]] | None = None
        if atm_vols is not None:
            if len(atm_vols) != len(self.expiries) or any(len(r) != len(self.tenors) for r in atm_vols):
                raise ValueError(
                    f'atm_vols shape must be ({len(self.expiries)}, {len(self.tenors)}) for surface {surface_id!r}'
                )
            self.atm_vols = [[float(v) for v in row] for row in atm_vols]
        self._is_numerical = bool(_is_numerical)
        if self._is_numerical and self.atm_vols is None:
            raise ValueError('numerical SwaptionVolSurface requires atm_vols')

    @property
    def is_numerical(self) -> bool:
        return self._is_numerical

    def row_series_id(self, expiry_idx: int) -> str:
        return f'{self.surface_id}:E{expiry_idx:02d}'

    def expiry_label(self, expiry_idx: int) -> str:
        return _tenor_tag(self.expiries[expiry_idx])

    def tenor_label(self, tenor_idx: int) -> str:
        return _tenor_tag(self.tenors[tenor_idx])

    def factor_id(self, expiry_idx: int, tenor_idx: int) -> str:
        return f'{self.surface_id}:{self.expiry_label(expiry_idx)}x{self.tenor_label(tenor_idx)}'

    def knot(self, expiry_idx: int, tenor_idx: int) -> basis.Knot:
        return self._rows[expiry_idx].knot(tenor_idx)

    def factors(self) -> list[FactorSpec]:
        """All ``len(expiries) * len(tenors)`` surface knot ``FactorSpec``s in row-major ``(expiry, tenor)`` order."""
        from .ir_curve_fitter import FactorSpec

        n_ten = len(self.tenors)
        return [
            FactorSpec(
                factor_id=self.factor_id(i, j),
                curve=self,  # type: ignore[arg-type]
                index=i * n_ten + j,
                pillar=Pillar(f'{self.expiry_label(i)}x{self.tenor_label(j)}', self.expiries[i]),
                knot=self.knot(i, j),
            )
            for i in range(len(self.expiries))
            for j in range(n_ten)
        ]

    def numerical(self, atm_vols: Sequence[Sequence[float]] | None = None) -> SwaptionVolSurface:
        """Returns a numerical copy of this surface whose ``vol(expiry, tenor)`` evaluates directly to ``float``."""
        vols = atm_vols if atm_vols is not None else self.atm_vols
        if vols is None:
            raise ValueError(f'atm_vols required to create numerical surface for {self.surface_id!r}')
        return SwaptionVolSurface(self.surface_id, self.expiries, self.tenors, vols, _is_numerical=True)

    def _resolve_vols(self, atm_vols: Sequence[Sequence[float]] | None = None) -> list[list[float]]:
        vols = atm_vols if atm_vols is not None else self.atm_vols
        if vols is None:
            raise ValueError(f'atm_vols not bound on SwaptionVolSurface {self.surface_id!r}')
        if len(vols) != len(self.expiries) or any(len(r) != len(self.tenors) for r in vols):
            raise ValueError(
                f'atm_vols shape must be ({len(self.expiries)}, {len(self.tenors)}) for surface {self.surface_id!r}'
            )
        return [[float(v) for v in row] for row in vols]

    def vol(self, expiry: float, tenor: float) -> float | expr.Expr:
        """Bilinearly interpolated ATM volatility at ``(expiry, tenor)`` with flat boundary clamping."""
        e_loc = self._expiry_axis.at(float(expiry))
        if self._is_numerical:
            assert self.atm_vols is not None
            t_loc = self._rows[0].at(float(tenor))
            r0 = self.atm_vols[e_loc.k0]
            v0 = r0[t_loc.k0] + t_loc.weight * (r0[t_loc.k1] - r0[t_loc.k0])
            if e_loc.weight == 0.0:
                return v0
            r1 = self.atm_vols[e_loc.k1]
            v1 = r1[t_loc.k0] + t_loc.weight * (r1[t_loc.k1] - r1[t_loc.k0])
            return (1.0 - e_loc.weight) * v0 + e_loc.weight * v1

        v0_expr = self._rows[e_loc.k0].lerp(float(tenor))
        if e_loc.weight == 0.0:
            return v0_expr
        v1_expr = self._rows[e_loc.k1].lerp(float(tenor))
        return (1.0 - e_loc.weight) * v0_expr + e_loc.weight * v1_expr

    def env(self, atm_vols: Sequence[Sequence[float]] | None = None, *, shift: float = 0.0) -> dict[str, float]:
        """Knot environment ``{knot_name: vol}`` for ``expr.evaluate``."""
        vols = self._resolve_vols(atm_vols)
        return {
            basis.knot_name(self.row_series_id(i), j): vols[i][j] + shift
            for i in range(len(self.expiries))
            for j in range(len(self.tenors))
        }

    def scenario_series(
        self,
        atm_vols: Sequence[Sequence[float]] | None = None,
        *,
        shift: float = 0.0,
    ) -> dict[str, list[tuple[str, float, float]]]:
        """Series mapping ``{row_series_id: [(factor_id, tenor, vol), ...]}`` for ``pure.Scenario`` / ``CBS_KNOT``."""
        vols = self._resolve_vols(atm_vols)
        return {
            self.row_series_id(i): [
                (self.factor_id(i, j), self.tenors[j], vols[i][j] + shift)
                for j in range(len(self.tenors))
            ]
            for i in range(len(self.expiries))
        }

    def __repr__(self) -> str:
        mode = 'numerical' if self._is_numerical else 'symbolic'
        return (
            f'<SwaptionVolSurface {self.surface_id!r} {len(self.expiries)}x{len(self.tenors)} mode={mode}>'
        )


__all__ = ['SwaptionVolSurface']

