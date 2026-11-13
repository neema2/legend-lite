"""Base class for pricing and risk execution engines (local Python vs. Pure / SQL / DuckDB)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from ..instruments.base import Instrument
from ..instruments.portfolio import Portfolio
from ..marketmodels.ir_curve_fitter import FactorSpec, Fit
from ..marketmodels.ir_curve_yield import ZeroCurve

if TYPE_CHECKING:
    from ..risk import FitterType, RiskRun


@dataclass(frozen=True)
class TickResult:
    """Result and sub-stage timing (in milliseconds) of a single market tick on a ``MarketSession``."""

    tick_index: int
    is_first_tick: bool
    engine_name: str
    risk_method_name: str
    trade_count: int
    shared_nodes: int
    basis_terms: int
    build_ms: float
    fit_ms: float
    eval_ms: float
    total_ms: float
    fit: Fit
    npv: dict[tuple[str, str], float]
    quote_risk: dict[tuple[str, str], float]

    @property
    def total_npv(self) -> float:
        return sum(self.npv.values())


class MarketSession(ABC):
    """Stateful market-tick session for a fixed portfolio and curve fitter.

    On the **1st market tick** (``tick_index == 0``), builds the portfolio ``Expr`` DAGs (and for ``PureBasisEngine``,
    expands ``BasisBook``, compiles Pure queries to SQL, and loads ``CBS_SHARED_NODE`` and ``CBS_BASIS_TERM`` into
    DuckDB), fits the initial market quotes, and evaluates NPV + par-quote DV01.

    On **later market updates** (``tick_index >= 1``), reuses the already-built portfolio DAGs / ``BasisBook`` tables
    and compiled SQL queries (`build_ms == 0.0`), only refitting the curve knots to the new market quotes (`fit_ms`)
    and updating ``CBS_KNOT`` / ``CBS_JACOBIAN_ENTRY`` or evaluating the cached ``Expr`` trees (`eval_ms`).
    """

    tick_count: int = 0

    @abstractmethod
    def tick(self, quotes: Sequence[float] | None = None) -> TickResult:
        """Evaluates one market tick (cold setup + eval on the 1st tick; warm update + eval on subsequent ticks)."""

    def close(self) -> None:
        """Releases any held native / database resources."""

    def __enter__(self) -> MarketSession:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class ExecutionEngine(ABC):
    """Base class for all pricing and risk execution strategies.

    Separates *what* financial instruments and risk measures are defined (``pricing.instruments``, ``pricing.risk``)
    from *how* their expression DAGs are evaluated:
      - ``PythonEngineExpr``: evaluates ``legend_lite.scalar_parallel.expr.Expr`` trees locally in Python with node caching.
      - ``PureBasisEngine`` (``DuckDBBasisEngine``): expands ``Expr`` trees into the piecewise interpolation basis
        (``legend_lite.scalar_parallel.basis``), generates Pure queries (``legend_lite.scalar_parallel.basis_pure``), compiles them to
        SQL for any target dialect, and executes them in DuckDB.
    """

    engine_name: str = 'base'

    @abstractmethod
    def npvs(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        curves: ZeroCurve | Sequence[ZeroCurve] = (),
        fit: Fit | Mapping[str, float] | None = None,
    ) -> dict[tuple[str, str], float]:
        """Evaluates all instrument NPVs keyed by ``(book, trade_id)``."""

    @abstractmethod
    def scenario_npvs(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        curves: ZeroCurve | Sequence[ZeroCurve],
        scenario_fits: Mapping[str, Fit],
    ) -> dict[str, dict[tuple[str, str], float]]:
        """Evaluates instrument NPVs across multiple ``Fit`` scenarios ``{scenario_id: {(book, trade_id): npv}}``."""

    @abstractmethod
    def pillar_risk(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        curves: ZeroCurve | Sequence[ZeroCurve] = (),
        fit: Fit | Mapping[str, float] | None = None,
        factors: Sequence[FactorSpec] | None = None,
    ) -> dict[tuple[str, str, str], float]:
        """Evaluates per-instrument zero-rate pillar deltas ``{(book, trade_id, factor_id): delta_per_bp}``."""

    @abstractmethod
    def quote_risk(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        fitter: FitterType,
        fit: Fit | None = None,
        risk_method: Any = None,
        **kwargs: Any,
    ) -> dict[tuple[str, str], float]:
        """Evaluates book-level benchmark par-quote DV01 ``{(book, quote_id): dv01}`` using ``risk_method``
        (defaults to ``FirstOrderAnalyticRisk``; pass ``FirstOrderNumericalRisk`` for per-quote bump, refit & reprice)."""

    @abstractmethod
    def run(
        self,
        curves: ZeroCurve | Sequence[ZeroCurve],
        fitter: FitterType,
        trades: Sequence[Instrument],
        bumps: Sequence[tuple[str, float]] = (),
        unit: str | None = 'USD',
        *,
        include_bump_and_grind: bool = True,
        bump_method: str = 'forward',
    ) -> RiskRun:
        """Executes a full curve fit, portfolio valuation, analytic risk, and optional naive bump-and-grind risk run."""

    @abstractmethod
    def session(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        fitter: FitterType,
        risk_method: Any = None,
        *,
        bump_method: str = 'forward',
        bump_bps: float = 1.0,
    ) -> MarketSession:
        """Creates a stateful ``MarketSession`` for measuring 1st market tick (cold setup + eval) vs. later market
        updates (warm curve refit + eval) under the chosen ``risk_method``."""

