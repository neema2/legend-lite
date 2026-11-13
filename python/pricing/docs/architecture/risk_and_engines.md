# Shared Risk Definitions & Execution Engines (`pricing/risk/`, `pricing/engines/`)

This document details the unification of **Analytic Symbolic Risk** (`FirstOrderAnalyticRisk`) and **First-Order Numerical Risk** (`FirstOrderNumericalRisk`) across **`PureBasisEngine`** (Pure $\to$ SQL $\to$ DuckDB / `ll.Frames`) and **`PythonEngineExpr`** (local Python oracle).

---

## 1. Two Risk Definitions, One Interface (`pricing/risk/`)

Both risk calculators inherit from [`FirstOrderRiskBase`](../../risk/base.py), which discovers the portfolio's instruments, curves, 2D volatility surfaces, and pillar/surface factors (`FactorSpec`).

| Feature | [`FirstOrderAnalyticRisk`](../../risk/firstorder_analytic.py) | [`FirstOrderNumericalRisk`](../../risk/firstorder_numerical.py) |
| :--- | :--- | :--- |
| **Zero-Rate / Surface Factor Delta ($\frac{\partial \text{NPV}}{\partial f_k}$)** | Exact Pure symbolic differentiation (`BasisBook.add_pure_diff` via `meta::legend::lite::diff`) | Direct finite-difference factor bump $f_k \pm \frac{1}{2}\text{bp}$ & reprice |
| **Par-Quote DV01 ($\frac{\partial \text{NPV}}{\partial q_j}$)** | SQL Jacobian chain rule: $\sum_k \frac{\partial \text{NPV}}{\partial r_k} \frac{\partial r_k}{\partial q_j}$ | Bump $q_j \to q_j + 1\text{bp}$ (or $\pm 0.5\text{bp}$), **refit curves**, and **reprice portfolio** |
| **Curve Fits Required (12 Quotes)** | **1** Newton fit (4 iterations total) | **1 + 12 = 13** Newton fits (forward) or **1 + 24 = 25** (central) |
| **Discretization Error** | Zero (`< 1e-10` vs Python exact tangent) | $O(\Delta q)$ forward (~$1\text{ \$/bp}$ convexity on \$25M); $O(\Delta q^2)$ central ($< 0.01\text{ \$/bp}$) |

---

## 2. Two Execution Engines (`pricing/engines/`)

Both engines implement [`ExecutionEngine`](../../engines/base.py):

- **[`PureBasisEngine`](../../engines/pure_basis.py)** (`DuckDBBasisEngine`):
  - Executes `npvs()`, `pillar_risk()`, `quote_risk()`, `run()`, and `session()` ticks **100% in DuckDB SQL** via `ComputeBasis` (zero `PythonEngineExpr` delegation or host math evaluation).
  - For **`FirstOrderAnalyticRisk`**, expands `NPV` and Pure-differentiated `PILLAR_DELTA` terms into `CBS_SHARED_NODE` and `CBS_BASIS_TERM`, writes `fit.dr_dq` into `CBS_JACOBIAN_ENTRY`, and runs `pure.measures_query()` + `quote_risk_query('BASE')` in SQL.
  - For **`FirstOrderNumericalRisk`**, compiles the portfolio's `NPV` `BasisBook` once (aggregating bump scenarios at book grain in `PureBasisMarketSession`) and evaluates `'BASE'` plus all refitted `BUMP_Q_<quote_id>` curve states in a **single vectorized SQL query**.
  - Compiles Pure queries across all **8 `legend-lite` SQL dialects** (`plan_sql(dialect)`, `plan_all_dialects()`) and binds directly onto a caller-provided `ll.Frames` or `ll.Page` (`PureBasisEngine(frames=page)`).
- **[`PythonEngineExpr`](../../engines/python_expr.py)**:
  - Evaluates `expr.Expr` DAGs locally in Python with `id(node)` memoization as a fast single-trade debugger and differential test oracle.

---

## 3. Stateful Market Sessions & Empirical Scaling (`pricing/demo/irs_scaling.py`)

`engine.session(portfolio, fitter, risk_method=..., frames=...)` separates **Cold-Start Compilation** (1st tick: `BasisBook` expansion, Pure $\to$ SQL compilation, static `CBS_SHARED_NODE` / `CBS_BASIS_TERM` registration) from **Warm Market Ticks** (updating only `CBS_KNOT` and `CBS_JACOBIAN_ENTRY` and re-running the cached SQL plans):

- **1,000 Two-Curve Swaps (`bazel run //python:irs_scaling`)**: `3.5 KB` SQL; warm Analytic tick **`26.2 ms`** in SQL vs. `135.0 ms` in Python (**`5.2x` faster**); warm Bump & Grind tick **`53.5 ms`** in SQL vs. `445.8 ms` in Python (**`8.3x` faster**).
- **500 Multi-Tenor `IRSwaption`s + 2D `SwaptionVolSurface`**: `162 KB` SQL (`<= 4` node slots, 4–6 base formulas across 1Y–30Y tenors); warm Bump & Grind eval **`60.7 ms`** in SQL vs. `261.1 ms` in Python (**`4.3x` faster**).
