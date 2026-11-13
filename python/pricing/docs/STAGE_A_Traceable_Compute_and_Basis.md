# Stage A: Traceable Computation & Staged Piecewise Basis in Pure (`legend_lite.scalar_parallel`)

This document describes the domain-free mathematical foundation in [`legend_lite/scalar_parallel/`](../../legend_lite/scalar_parallel/) and its compiler counterparts in [`core/`](../../../core/).

---

## 1. Why Execution Tracing + Staged Basis Decomposition?

Two structural constraints govern scaling quantitative pricing and analytic risk to SQL databases in `legend-lite`:
1. **Unrolled Portfolio Expressions Explode**: A single 10Y quarterly two-curve swap has 40 floating coupons, and a 30Y swaption has 120 underlying coupons feeding a non-linear Black-76 formula. Emitting $N$ portfolio trades as inline SQL expressions produces megabytes of SQL text (`> 3 MB`), whereas representing trades as normalized basis rows keeps swap SQL constant at **`3.5 KB`** and multi-tenor swaption SQL bounded at **`65–162 KB`**.
2. **Zero Handwritten SQL & Zero Host Value Precomputation**: `//python:host_sql_test` forbids spelling SQL strings in Python, and [`docs/TENET_CHARTER.md`](../../../docs/TENET_CHARTER.md) (Clause C2.1) requires the database—not the Python host—to evaluate mathematical values.

**The Solution** combines two `core/` compiler capabilities with three `legend_lite/scalar_parallel/` modules:
- **In `core/`**:
  - [`SymbolicDiff.java`](../../../core/src/main/java/com/legend/compiler/spec/SymbolicDiff.java): Compile-time symbolic differentiation (`meta::legend::lite::diff`) in Phase G½ after `UserCallInliner` $\beta$-reduction.
  - [`ErfMath.java`](../../../core/src/main/java/com/legend/sql/dialect/ErfMath.java): Portable IEEE-754 double-precision `meta::legend::lite::erf` across all 8 SQL dialects (native `erf` where supported; Cody 1969 1-ULP rational minimax elsewhere).
  - *Note on SQL Projection CSE (tested & deferred from this PR)*: We also prototyped and tested an automatic guard-safe `ProjectionCse` pass in `AnsiSqlRenderer` (lifting repeated transcendentals and `erf` arguments into inner `SqlSelect` stages `__pc0, __pc1, ...` while respecting `CASE`/`AND`/`OR`/`COALESCE` guard barriers). Measured impact: **0 bytes** difference on IRS swaps and negligible runtime difference (`compute_test` `1.5s` vs `2.4s`, `pricing_test` `23.0s` vs `23.0s`), while on 2D Black-76 swaptions (`erf` + `diff`) it keeps Postgres inline `ErfMath` SQL under `600 KB` (vs `~638 KB` without it). Deferred from this PR to keep `core/` minimal and avoid altering non-pricing SQL paths; can be reintroduced later behind an opt-in or unified with [`CommonSubexpressions.java`](../../../core/src/main/java/com/legend/sql/dialect/CommonSubexpressions.java).
- **In `legend_lite/scalar_parallel/`** *(globbed into `:legend_lite` `[datacube]` in `python/BUILD.bazel`)*:
  1. **[`expr.py`](../../legend_lite/scalar_parallel/expr.py)** *(stdlib-only)*: Lightweight traceable scalar micro-expression DAG on single IEEE-754 doubles (`Const`, `Var`, `Add`, `Mul`, `Div`, `Exp`, `Log`, `Sqrt`, `Abs`, `Erf`) with zero `ast.parse` magic, dual numerical/symbolic math helpers (`exp`, `log`, `sqrt`, `abs_`, `erf`), Pure emitters (`to_pure`, `to_pure_diff`), exact differentiation (`expr.diff`), and cached evaluation (`expr.evaluate`).
  2. **[`basis.py`](../../legend_lite/scalar_parallel/basis.py)** *(stdlib-only)*: Arbitrary-degree polynomial expansion (`expand`) over shared piecewise interpolation nodes (`LERP`, `EXP_NEG_XT`, `RECIP_1PXT`, `CONST`) and staged scalar `SubMeasure`s.
  3. **[`basis_pure.py`](../../legend_lite/scalar_parallel/basis_pure.py)** *(`pyarrow`/`duckdb`)*: Automatic hierarchical sub-measure factoring (`BasisBook._factor_expr`), Pure-native derivative synthesis (`BasisBook.add_pure_diff`), pure-topology Arrow table emission (zero host math evaluation), and dynamic Pure model/query synthesis (`render_pure_model(schema)`).

---

## 1B. Scalar Micro-Expressions (`expr.Expr`) Inside Macro `page.flow()` Blocks

DataCube's `page.flow()` provides visual relational blocks (`Grid`, `Join`, `GroupBy`) over tables, whereas `expr.Expr` provides hash-consed scalar micro-expressions over single `float`s:
- **Author & Step-Debug on Plain Doubles**: Quants call `swap.npv(curve.numerical(rates))` or `black_formula(...)` to step through every intermediate float in a standard Python debugger without DuckDB, Arrow, or JVM dependencies, and with zero `ast.parse` source rewriting.
- **Inner Scalar Engine Within Flow Blocks**: Inside each `page.flow()` Pure block (such as `BasisBook` or `CalibrationBasisTables`), hash-consed `expr.Expr` + `BasisNode` trees factor both portfolio payoffs (`BasisBook`) and calibration par swaps (`CalibrationBasisTables`) into skinny fixed-schema relational tables, compute exact symbolic partial derivatives, and emit compact Pure formulas without turning a 120-coupon payoff into 120 UI table boxes.

---

## 2. Two-Curve Swaps, Options & Hierarchical Swaptions in the Basis

> [!NOTE]
> **Exact Math vs. Taylor/Polynomial Approximations**:
> The staged basis is an **exact coordinate representation** of common subexpressions, **not** an approximating Taylor or polynomial series. Every price, Greek, and sensitivity evaluated in DuckDB or Python is exact analytical closed-form mathematics ($\sim 10^{-15}$ machine epsilon agreement). While automated approximations to a predefined spanning basis (e.g. Chebyshev polynomials or polynomial chaos) could be developed in future research for path-dependent payoffs, that is explicitly **not** the current aim or use case.

### A. Two-Curve Swaps (`max_stage = 0`, Pure Polynomials)
For a two-curve swap (`IRSwapFixedFloat`), each floating coupon on $[t_{i-1}, t_i]$ has present value:

$$\text{PV}_i = N \left(\frac{D_{\text{proj}}(t_{i-1})}{D_{\text{proj}}(t_i)} - 1 + s \cdot \Delta t_i\right) D_{\text{disc}}(t_i)$$

Because $1 / D_{\text{proj}}(t_i) = \exp(+t_i r_{\text{proj}}(t_i))$ is an `EXP_NEG_XT` node with negated parameter $-t_i$, every cross-curve floating coupon expands into a **degree-3 monomial** (`TERM_OP = 'PROD'`) of three shared `EXP_NEG_XT` nodes, and its derivative with respect to any pillar knot stays in the exact same degree-$\le 3$ basis. When `projection_curve == discount_curve`, `basis.expand()` automatically combines $\exp(+t_i r(t_i)) \cdot \exp(-t_i r(t_i)) = 1$, telescoping the explicit floating leg to $N(D(t_0) - D(t_n))$.

### B. Composite Non-Linear Payoffs (`max_stage >= 1`, `SubMeasure` + Pure `diff()`)
When `BasisBook._factor_expr` encounters multi-term additive polynomials (`len(poly) >= 2`, such as a 120-coupon swap annuity $A$, floating-leg PV $V_{\text{flt}}$, or 4-knot bilinear volatility $\sigma$) feeding a non-polynomial operation (`Div`, `Log`, `Sqrt`, `Erf`):
1. **Stage 0 (`TERM_STAGE = 0`)**: Registers deduplicated unit-normalized `SubMeasure` rows (`S000000`, `S000001`, ...) evaluated in SQL via `cbsEvalPolyTerm` and `GROUP BY`.
2. **Stage 1 (`TERM_STAGE = 1`)**: Extracts a compact $\le 4$-slot `FormulaSpec` (`compute_basis::cbsFormula_F001`, ...) reading the aggregated sub-measures as scalar slots, and synthesizes Pure `diff({_dv | compute_basis::cbsFormula_F001(...)}, ...)` functions (`SUB_CHAIN`) that multiply partial derivatives by Stage-0 sub-measure derivatives in SQL.

---

## 3. Verification

- [`python/tests/test_compute.py`](../../tests/test_compute.py) (`bazel test //python:compute_test`): Verifies `expr.diff`, `erf`, `basis.expand`, Pure `diff()`, `ProjectionCse` (`__pc0`), zero host math evaluation (`'NODE_VALUE' not in row`), hierarchical `SubMeasure` factoring across 12/28/60-period sums, and `ComputeBasis` across all 8 SQL dialects.
- [`python/tests/test_pricing.py`](../../tests/test_pricing.py) (`bazel test //python:pricing_test`): Verifies 1-curve swaps, 2-curve swaps, `EquityOption`, and `IRSwaption` + `SwaptionVolSurface` across Python and DuckDB SQL.
