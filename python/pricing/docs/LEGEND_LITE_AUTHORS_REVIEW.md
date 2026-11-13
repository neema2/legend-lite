# Architectural Notes & Open Concerns (`legend-lite` Maintainers' View)

This note summarizes the design boundaries and remaining open concerns for [`legend_lite/scalar_parallel/`](../../legend_lite/scalar_parallel/), [`python/pricing/`](../), and the compiler additions in [`core/`](../../../core/), written for reviewers approaching the PR fresh.

---

## 1. Design Boundaries (Summary)

- **No `ast.parse`**: Zero `import ast` or `ast.parse` in `legend_lite/` or `pricing/`. Scalar formulas are traced via Python operator overloading on [`expr.Expr`](../../legend_lite/scalar_parallel/expr.py), and all Pure text parsing stays in `core/` (`compiler.parse`).
- **Two-Level Compute Split (`irs_frames()` vs. `irs_page()`)**:
  - `irs_frames()` executes 100% of curve fitting (`PureBasisCurveFitter`: 2-iteration sparse Newton-Schulz + Step-2 Halley + SQL $J^{-1}$), shared basis evaluation (`PureBasisEngine`), swap valuation, pillar risk, and quote risk in DuckDB SQL, producing 6 canonical tables (`market_quotes`, `curve_profile`, `fitter_jacobian`, `swap_portfolio`, `pillar_risk`, `quote_risk`).
  - `irs_page()` builds a 10-box connected `page.flow()` graph over those 6 tables (with live `$parallel_shift_bps` valuation and live `pillar_risk × fitter_jacobian` chain-rule DV01), avoiding ~48 intermediate fitter/basis UI boxes.
- **Package Isolation**: `legend_lite/scalar_parallel/` is globbed into `:legend_lite` (`[datacube]`) in [`python/BUILD.bazel`](../../BUILD.bazel), leaving the minimal `:compiler` wheel untouched.

---

## 2. Open Concerns & Trade-Offs

### 1. Non-DuckDB Inline `ErfMath` SQL Size (Deferred `ProjectionCse`)
- **Concern**: On dialects without a native `erf` builtin (e.g. Postgres), [`ErfMath.Expansion`](../../../core/src/main/java/com/legend/sql/dialect/ErfMath.java) expands `erf(x)` into a rational/polynomial approximation inline. For 2D Black-76 swaptions (`erf` + `diff`), this produces ~638 KB of Postgres SQL (DuckDB uses native `list_transform` `erf` and is unaffected; IRS swaps have 0 bytes difference).
- **Trade-off**: We prototyped and tested an automatic guard-safe `ProjectionCse` pass (`< 600 KB` on Postgres) and deferred it from this PR to keep `core/` minimal and leave all non-pricing SQL paths untouched. It can be added later as an opt-in pass or unified with [`CommonSubexpressions.java`](../../../core/src/main/java/com/legend/sql/dialect/CommonSubexpressions.java).

### 2. Dashboard `page.flow()` Live Scope vs. `IRSWorkbench`
- **Concern**: Because `irs_page()` consumes the 6 SQL-computed `irs_frames()` tables rather than putting the 2-step fitter inside `page.flow()`, changing `$parallel_shift_bps` on the Flow canvas updates NPVs via first-order `analytic_dv01_sum` rather than re-running a full non-linear curve refit inside the browser canvas.
- **Trade-off**: Putting all ~48 intermediate fitter/basis boxes into `page.flow()` caused ~4,457 V8 `py_mini_racer` + ~4,338 `libcompiler.so` calls (~50s load time). Full quote-shock recalibration and portfolio edits are handled cleanly by [`IRSWorkbench`](../demo/irs_workbench.py) (`irs_frames()` + `grid.update(...)`).

### 3. Pure Text Emission vs. Direct V1 Protocol Trees
- **Concern**: [`basis_pure.py`](../../legend_lite/scalar_parallel/basis_pure.py) and [`ir_curve_fitter.py`](../marketmodels/ir_curve_fitter.py) emit Pure query strings compiled once via `compiler.parse` + cached `HeldModel`s.
- **Trade-off**: Warm ticks bypass parsing via cached `CompiledPlan`s (~30 ms for 1,000 swaps). If cold-start compilation for very large curves ($N > 50$) becomes a bottleneck, emitting V1 protocol trees directly would skip lexer/parser overhead.

### 4. Dual-Path Parity Discipline (`SymbolicDiff.java` $\leftrightarrow$ `expr.py`)
- **Concern**: Math primitives in [`SymbolicDiff.java`](../../../core/src/main/java/com/legend/compiler/spec/SymbolicDiff.java) (`exp`, `log`, `sqrt`, `erf`, `sin`, `cos`, `abs`, `sign`, `min`, `max`, `pow`) are mirrored in stdlib [`expr.py`](../../legend_lite/scalar_parallel/expr.py) so the Python debug path matches SQL to $< 10^{-12}$. Any new primitive added to one must be added to both and gated in `//python:compute_test`.

### 5. DuckDB Warm-Tick Staging (`__cbs_load`) & Basis Node Vocabulary
- **Concern**: `ComputeBasis.update` refreshes `CBS_KNOT` and `CBS_JACOBIAN_ENTRY` via `DELETE` + `INSERT INTO ... SELECT * FROM __cbs_load` (~2–3 ms/tick), and `CBS_SHARED_NODE` covers `LERP`, `EXP_NEG_XT`, and `RECIP_1PXT` (plus hierarchical `SubMeasure`s). Sub-millisecond tick rates or exotic models (e.g. cubic splines, SABR) would require Arrow replacement-scan binding or additional basis ops.
