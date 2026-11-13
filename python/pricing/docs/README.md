# `python/pricing` on `legend-lite`: Architecture & Roadmap

This folder documents the quantitative pricing, calibration, and risk framework built on top of [`legend_lite.scalar_parallel`](../../legend_lite/scalar_parallel/__init__.py), DataCube's [`page.flow()`](../../legend_lite/page.py) relational compute graph, Pure's native symbolic differentiation (`meta::legend::lite::diff`), portable `erf` lowering (`ErfMath.java`), and the dynamically synthesized `compute_basis` Pure model.

---

## Staged Architecture Overview

| Stage | Document | Scope | Status |
| :--- | :--- | :--- | :--- |
| **Aims & Benchmarks** | [`PRICING_AIMS.md`](PRICING_AIMS.md) | North-star aims, **Two-Level Compute Graph (`page.flow()` macro blocks + `expr.Expr` scalar micro-expressions)**, empirical lessons, and scaling results (1–1,000 swaps & 1–500 swaptions). | **Phases 1–5 Implemented** |
| **Stage A** | [`STAGE_A_Traceable_Compute_and_Basis.md`](STAGE_A_Traceable_Compute_and_Basis.md) | Domain-free scalar micro-expression DAG (`expr.py`, stdlib-only), piecewise basis & `SubMeasure` expansion (`basis.py`), and dynamic Pure/SQL model synthesis (`basis_pure.py`) in `legend_lite/scalar_parallel/` (`:legend_lite` / `[datacube]`). | **Implemented** |
| **Stage B** | [`STAGE_B_Pricing_Domain_Risk_and_Engines.md`](STAGE_B_Pricing_Domain_Risk_and_Engines.md) | `pricing/instruments` (1-curve & 2-curve IRS, `EquityOption`, `IRSwaption`), `pricing/marketmodels` (`ZeroCurve`, skinny relational 2-iteration 100%-SQL `PureBasisCurveFitter` with sparse Newton-Schulz + Halley + adjoint risk, `SwaptionVolSurface`), `pricing/risk`, and `pricing/engines`. | **Implemented** |
| **Stage C** | [`STAGE_C_Notebooks_Workbench_and_Dashboards.md`](STAGE_C_Notebooks_Workbench_and_Dashboards.md) | 5-sheet Flow + Board DataCube dashboard (`pricing/demo/dashboard.py`), CLI demo (`irs_pricing.py`), interactive workbench (`irs_workbench.py`), scaling benchmark (`irs_scaling.py`), and Jupyter notebook. | **Implemented** |
| **Owners' Review** | [`LEGEND_LITE_AUTHORS_REVIEW.md`](LEGEND_LITE_AUTHORS_REVIEW.md) | Post-Phase-5 skeptical review from the `legend-lite` owners' viewpoint: zero-`ast.parse` audit, `page.flow()` vs `expr.Expr` two-level compute graph, 100%-SQL 2-step fitter, wheel boundaries, and remaining critiques. | **Updated Review** |

---

## Detailed Architecture Guides

1. **[`architecture/compute_expr_and_basis_pure.md`](architecture/compute_expr_and_basis_pure.md)** — Why scalar micro-expressions on single IEEE-754 doubles (`legend_lite.scalar_parallel.expr`) are essential inside macro `page.flow()` blocks, Pure `erf` & `diff`, arbitrary-degree basis expansion, hierarchical `SubMeasure` factoring (`legend_lite.scalar_parallel.basis`), and dynamic Pure-to-SQL compilation across 8 SQL dialects (`legend_lite.scalar_parallel.basis_pure`).
2. **[`architecture/instruments_swaps.md`](architecture/instruments_swaps.md)** — `Instrument`, `Portfolio`, single-curve `IRSwapFixedFloatApprox`, two-curve `IRSwapFixedFloat`, European `EquityOption`, and physically-settled `IRSwaption` over a 2D `SwaptionVolSurface`.
3. **[`architecture/marketmodel_curves.md`](architecture/marketmodel_curves.md)** — `ZeroCurve`, `Pillar`, `SwaptionVolSurface`, Python reference fitters (`CurveFitter`, `MultiCurveFitter`), and the **skinny relational 2-iteration 100%-SQL curve fitter** (`PureBasisCurveFitter`: `CalibrationBasisTables`, sparse Newton-Schulz $O(\log L_{\max})$ doubling, Step-2 Halley & inverse reuse, and vector-only adjoint quote risk scaling past $N = 200$ coupled pillars).
4. **[`architecture/risk_and_engines.md`](architecture/risk_and_engines.md)** — Shared risk definitions (`FirstOrderAnalyticRisk` vs. `FirstOrderNumericalRisk`) and execution engines (`PureBasisEngine` compiling Pure to SQL across 8 dialects & executing 100% in SQL on DuckDB / `ll.Frames`, vs. `PythonEngineExpr` standalone Python oracle).

---

## Summary of What Is Produced vs. Planned (TODO)

### Produced & Verified
- [x] **Core Pure `diff`, `erf` & Zero-Allocation Fast-Path `ProjectionCse`** (`core/`): `meta::legend::lite::diff` (`SymbolicDiff.java`), `meta::legend::lite::erf` (`ErfMath.java` Cody 1969 IEEE-754 1-ULP + native DuckDB `erf` list-lambda), and automatic reference-counted `ProjectionCse.java` (`__pcN.__cM`, zero-allocation fast-path precheck for ordinary BI queries).
- [x] **Clean Wheel & Package Boundaries** (`python/BUILD.bazel`, `legend_lite/scalar_parallel/`): Entire `legend_lite/scalar_parallel/` package is globbed into `:legend_lite` (`[datacube]`), keeping `:compiler` (`legend-lite` base wheel) untouched. `expr.py` and `basis.py` remain stdlib-only, with zero `ast.parse` anywhere in Python.
- [x] **Two-Level Compute Graph (`page.flow()` Macro Blocks + `expr.Expr` Scalar Micro-Expressions)**: Macro relational DataCube Flow stages orchestrate tables, joins, scenario shocks, and aggregations, while hash-consed scalar micro-expressions on single doubles (`expr.Expr`) provide step-debuggable IEEE-754 math and emit staged Pure formulas (`_dag_stages_pure`, `to_pure`, `to_pure_diff`, `BasisBook`) inside each Flow block.
- [x] **Skinny Relational 2-Iteration 100%-SQL Curve Fitter & Adjoint Risk** (`pricing/marketmodels/ir_curve_fitter.py`): `PureBasisCurveFitter` (`fitter.sql_fitter()`) factors calibration swaps into fixed-schema `CalibrationBasisTables` (`0 ms` cached compile), calibrates coupled OIS + 3M curves via sparse Newton-Schulz $O(\log L_{\max})$ doubling + Step-2 componentwise Halley boost + warm inverse reuse (`104.4 ms` cold, `71.1 ms` warm at $N = 200$, `max |dr| = 9.71e-16`), and computes vector-only adjoint par-quote DV01s (`123.1 ms` at $N = 200$, `~4e-15` rel) or full SQL $J^{-1}$ (`127.5 ms`).
- [x] **5-Sheet Flow + Board Dashboard & 100%-SQL Engines** (`pricing/demo/dashboard.py`, `pricing/engines/pure_basis.py`): Sheet 0 (`0. Pricing & Risk Flow`) executes all 12 stages of curve fitting, Jacobian inversion, basis evaluation, swap valuation, pillar risk, and chained par-quote DV01 in SQL, feeding Sheets 1–4 directly with live parameter reactivity (`parallel_shift_bps`, `steepener_2s10s_bps`).

### Planned Roadmap (TODO)
- [ ] **Timeseries Replay & Bitemporal P&L (`pricing/demo/`)**: Multi-date curve & vol replay (`SCENARIO_ID = obs_date`) wired into `page.timeseries(...)` and bitemporal `MilestoningStrategy.BITEMPORAL` clean-vs-new-trade P&L attribution.
- [ ] **Dates, Additional Instruments & Higher-Order Risk**: Holiday calendars & day-count conventions (`pricing/dates/`), `IRSwapFixedOIS`, `IRSwapFloatFloat`, `IRSwapXccyOIS`, and `SecondOrderAnalyticRisk` (Gamma / Vanna / Volga via nested Pure `diff(diff(...))`).
