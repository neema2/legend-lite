# Stage B: Pricing Domain, Shared Risk & Execution Engines (`pricing/`)

This document describes the structure of [`python/pricing/`](../), adapting the `py-flow` domain split (`maths`, `instruments`, `marketmodels`, `risk`, `engines`, `demo`) to `legend-lite`.

---

## 1. Directory Structure

```text
python/pricing/
├── __init__.py
├── maths/
│   ├── __init__.py                    # black_formula, black_scholes_formula, cnd, norm_pdf
│   ├── cnd.py                         # cnd() & norm_pdf() built directly on expr.erf (zero custom Expr classes)
│   └── black.py                       # black_formula(), black_scholes_formula() (float or Expr)
├── instruments/
│   ├── __init__.py
│   ├── base.py                        # Instrument ABC (.npv(), .curves(), .vol_surfaces(), .factors())
│   ├── portfolio.py                   # Portfolio aggregator (.npv(), .trade_npv_exprs(), .vol_surfaces())
│   ├── ir_scheduling.py               # payment_dates, reset_dates, day_count_fraction
│   ├── ir_swap_fixed_floatapprox.py   # IRSwapFixedFloatApprox (1-curve OIS telescoping float leg)
│   ├── ir_swap_fixed_float.py         # IRSwapFixedFloat (2-curve explicit floating cashflows)
│   ├── ir_swaption.py                 # IRSwaption (European payer/receiver swaption via Black-76)
│   └── eq_option.py                   # EquityOption (European call/put via Black-Scholes-Merton)
├── marketmodels/
│   ├── __init__.py
│   ├── ir_curve_yield.py              # Pillar, ZeroCurve (.zero_rate(t), .df(t), .fwd(t1, t2), .numerical(rates))
│   ├── ir_curve_fitter.py             # CurveFitter, MultiCurveFitter & skinny 2-step SQL PureBasisCurveFitter (sparse Schulz + Halley + adjoint risk)
│   └── swaption_vol_surface.py        # SwaptionVolSurface (2D bilinear expiry x tenor vol surface + .numerical())
├── risk/
│   ├── __init__.py                    # RiskRun, risk_run(), resolve_risk_method()
│   ├── base.py                        # FirstOrderRiskBase, scenario(), quote_risk_query()
│   ├── firstorder_analytic.py         # FirstOrderAnalyticRisk (Pure diff / expr.diff + Jacobian chain rule)
│   └── firstorder_numerical.py        # FirstOrderNumericalRisk (quote/pillar bump, refit & reprice)
├── engines/
│   ├── __init__.py                    # resolve_engine()
│   ├── base.py                        # ExecutionEngine ABC, MarketSession, TickResult
│   ├── python_expr.py                 # PythonEngineExpr (local Python Expr DAG oracle & debugger)
│   └── pure_basis.py                  # PureBasisEngine / DuckDBBasisEngine (Pure -> SQL -> DuckDB / ll.Frames)
└── demo/
    ├── __init__.py
    ├── dashboard.py                   # irs_frames(), irs_page() (5-sheet Flow + Board DataCube ll.Page)
    ├── irs_pricing.py                 # CLI portfolio & multi-dialect SQL demo (//python:irs_demo)
    ├── irs_scaling.py                 # Cold vs warm market-tick scaling benchmark (//python:irs_scaling)
    ├── irs_workbench.py               # Interactive user workbench & widget (//python:irs_workbench)
    └── irs_workbench.ipynb            # Interactive Jupyter notebook
```

---

## 2. Key Architectural Decisions

### A. Orthogonal Risk Operators (`pricing/risk/`)
Instruments (`IRSwapFixedFloatApprox`, `IRSwapFixedFloat`, `IRSwaption`, `EquityOption`, `Portfolio`) define **only** their payoff and cashflow math (`npv()`, `par_rate()`, `annuity()`, `forward_swap_rate()`).
All sensitivity calculations live in `pricing/risk/` under [`FirstOrderRiskBase`](../risk/base.py):
- **[`FirstOrderAnalyticRisk`](../risk/firstorder_analytic.py)**: Populates `BasisBook` with `NPV` and Pure-differentiated `PILLAR_DELTA` sensitivities (`BasisBook.add_pure_diff`) across all referenced curves and 2D volatility surfaces, and chains curve pillar deltas through the fitter's inverse Jacobian $\frac{\partial r_k}{\partial q_j}$ (`fit.dr_dq`) — requiring **only 1 curve fit**.
- **[`FirstOrderNumericalRisk`](../risk/firstorder_numerical.py)**: Bumps each input benchmark par quote $q_j$ by $+1\text{bp}$ (forward) or $\pm 0.5\text{bp}$ (central), calls `fitter.fit_quotes(bumped_quotes)` to refit both curves, and reprices the portfolio across all bumped scenarios.

### B. Genuine Database Execution vs. Host Oracle (`pricing/engines/`)
- **[`PureBasisEngine`](../engines/pure_basis.py)** (`DuckDBBasisEngine`): Executes `npvs()`, `pillar_risk()`, `quote_risk()`, `run()`, and stateful `session()` ticks **100% in DuckDB SQL** via Pure (`ComputeBasis`), plans SQL across all 8 `legend-lite` dialects, and binds directly onto a caller's `ll.Frames` or `ll.Page` (`PureBasisEngine(frames=page)`).
- **[`PythonEngineExpr`](../engines/python_expr.py)**: Evaluates `expr.Expr` DAGs in Python with shared-node memoization (`expr.evaluate(e, env, cache)`) as a unit-debugging and differential-conformance oracle.
