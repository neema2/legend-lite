"""Interest-rate swap curve fitting, pricing and risk on ``legend_lite.scalar_parallel``.

Organized into:
  - ``pricing.instruments``: ``Instrument``, ``Portfolio``, ``IRSwapFixedFloatApprox`` (single-curve telescoping float
    leg), ``IRSwapFixedFloat`` (two-curve explicit floating coupon cashflows), and ``ir_scheduling``.
  - ``pricing.marketmodels``: ``ZeroCurve``, ``Pillar``, ``CurveFitter``, and ``MultiCurveFitter`` (simultaneous
    discount + projection curve calibration and cross-curve symbolic Jacobian).
  - ``pricing.risk``: ``FirstOrderAnalyticRisk`` (symbolic differentiation + Jacobian chain rule) vs.
    ``FirstOrderNumericalRisk`` (numerical bump of each input market quote with curve refit and portfolio reprice).
  - ``pricing.engines``: ``PythonEngineExpr`` (local Python ``expr.Expr`` evaluation) vs. ``PureBasisEngine`` /
    ``DuckDBBasisEngine`` (Pure -> SQL -> DuckDB basis evaluation).
  - ``pricing.demo``: DataCube ``ll.Page`` dashboard builder (``pricing.demo.dashboard``), CLI demo
    (``pricing.demo.irs_pricing``), and interactive workbench (``pricing.demo.irs_workbench``).
"""

import importlib

from . import engines, instruments, marketmodels, maths, risk
from .engines import DuckDBBasisEngine, ExecutionEngine, PureBasisEngine, PythonEngineExpr, resolve_engine
from .instruments import EquityOption, IRSwapFixedFloat, IRSwapFixedFloatApprox, IRSwaption, Instrument, Portfolio
from .marketmodels import (
    CurveFitter,
    FactorSpec,
    Fit,
    MultiCurveFitter,
    Pillar,
    QuoteSpec,
    SwaptionVolSurface,
    ZeroCurve,
    inverse,
    solve,
)
from .maths import black_formula, black_scholes_formula, cnd, norm_pdf, phi_approx
from .risk import (
    BP,
    FirstOrderAnalyticRisk,
    FirstOrderNumericalRisk,
    FirstOrderRiskBase,
    NPV,
    PILLAR_DELTA,
    RiskRun,
    quote_risk_query,
    risk_run,
    scenario,
)

__all__ = [
    'Pillar',
    'ZeroCurve',
    'SwaptionVolSurface',
    'Instrument',
    'Portfolio',
    'EquityOption',
    'IRSwapFixedFloat',
    'IRSwapFixedFloatApprox',
    'IRSwaption',
    'black_formula',
    'black_scholes_formula',
    'cnd',
    'phi_approx',
    'norm_pdf',
    'maths',
    'Fit',
    'FactorSpec',
    'QuoteSpec',
    'CurveFitter',
    'MultiCurveFitter',
    'solve',
    'inverse',
    'ExecutionEngine',
    'PythonEngineExpr',
    'PureBasisEngine',
    'DuckDBBasisEngine',
    'resolve_engine',
    'FirstOrderRiskBase',
    'FirstOrderAnalyticRisk',
    'FirstOrderNumericalRisk',
    'NPV',
    'PILLAR_DELTA',
    'BP',
    'RiskRun',
    'scenario',
    'quote_risk_query',
    'risk_run',
    'irs_frames',
    'irs_page',
    'dashboard',
    'demo',
    'engines',
    'instruments',
    'marketmodels',
    'risk',
]


def __getattr__(name: str):
    if name == 'demo':
        return importlib.import_module('.demo', __name__)
    if name in ('dashboard', 'irs_frames', 'irs_page'):
        mod = importlib.import_module('.demo', __name__)
        return getattr(mod, name)
    raise AttributeError(f"module 'pricing' has no attribute {name!r}")

