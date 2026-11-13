# Quantitative Pricing & Risk on `legend-lite`: Aims, Lessons & Architecture

This document synthesizes:
1. **What we are achieving** across [`python/pricing/`](../), [`legend_lite/scalar_parallel/`](../../legend_lite/scalar_parallel/), and the Pure compiler (`com.legend.compiler.spec` / `com.legend.sql.dialect`).
2. **What we learned empirically** from building and benchmarking both the dynamic staged basis engine (`BasisBook` + `SubMeasure`) and compile-time Pure differentiation (`diff`) + automatic guard-safe SQL projection CSE (`ProjectionCse`).
3. **Measured scaling results** across 1–1,000 two-curve swaps and 1–500 European swaptions (`IRSwaption` + 2D `SwaptionVolSurface`), together with the Phase 4 roadmap.

---

## 1. North-Star Aims

We want a quantitative pricing, calibration, and risk system on `legend-lite` that satisfies **four simultaneous goals**:

### Aim 1 — Clean, Idiomatic, Step-Debuggable Quant Authoring
- **Domain code reads like mathematics, not SQL plumbing**: Instrument payoffs ([`IRSwapFixedFloatApprox`](../instruments/ir_swap_fixed_floatapprox.py), [`IRSwapFixedFloat`](../instruments/ir_swap_fixed_float.py), [`IRSwaption`](../instruments/ir_swaption.py), [`EquityOption`](../instruments/eq_option.py)), quantitative functions ([`black_formula`](../maths/black.py), [`cnd`](../maths/cnd.py)), and market models ([`ZeroCurve`](../marketmodels/ir_curve_yield.py), [`SwaptionVolSurface`](../marketmodels/swaption_vol_surface.py)) are written once as clean, composable Python math—with zero custom `Expr` subclasses in domain code, zero derivative formulas, zero SQL strings, and zero manual staging annotations.
- **Plain-float step debugging**: Every payoff, curve, and volatility surface method can be stepped through line-by-line in a standard debugger with plain numbers (`curve.numerical(rates)`, `surface.numerical(vols)`).
- **Exact Mathematics, Not Taylor Series or Approximating Polynomials**:
  - The framework does **not** perform Taylor series expansions or polynomial curve fitting to approximate payoffs or Greeks. All valuations, Jacobians, and sensitivities evaluated in Python or compiled into DuckDB SQL are **exact analytical closed-form mathematics** ($\sim 10^{-15}$ machine epsilon agreement).
  - In `legend-lite`, the term **"basis"** refers to an **exact common-subexpression coordinate basis** (shared piecewise exponential curve nodes, factored Stage-0 multilinear aggregates, and symbolically differentiated Pure formulas) that allows millions of trades to evaluate in compact relational SQL without expression explosion.
  - *Future note on approximations*: Automated functional approximations (e.g. projection onto a predefined spanning basis set like Chebyshev or polynomial chaos) could in principle be developed in the future to map complex path-dependent or numerical payoffs into shared basis functions, but that is explicitly **not** the current aim or use case.
- **Risk as an orthogonal operator**: Analytic sensitivities ($\partial V / \partial r_k$ chained through calibration inverse Jacobians $\partial r_k / \partial q_j$) live outside instrument definitions in [`pricing/risk/`](../risk/), verified side-by-side against independent bump-refit-reprice numerical risk ([`FirstOrderNumericalRisk`](../risk/firstorder_numerical.py)).

### Aim 2 — Strict `legend-lite` Tenet Alignment ("The Database Computes; The Host Carries")
Per [`docs/TENET_CHARTER.md`](../../../docs/TENET_CHARTER.md) and [`AGENTS.md`](../../../AGENTS.md):
- **Zero handwritten SQL in host code** (enforced by `//python:host_sql_test`) and **zero `ast.parse` source-inspection magic**.
- **Clean Wheel Isolation (`python/BUILD.bazel`)**: The entire `legend_lite/scalar_parallel/` package (`expr.py`, `basis.py`, `basis_pure.py`) lives in `:legend_lite` (`legend-lite[datacube]`) via a single folder glob, keeping `:compiler` (the minimal base wheel) completely untouched.
- **100%-SQL Calibration, Valuation & Risk (World 2) + Standalone Python Oracle (World 1)**:
  - [`PureBasisCurveFitter`](../marketmodels/ir_curve_fitter.py) (`fitter.sql_fitter()`) factors calibration swaps into fixed-width [`CalibrationBasisTables`](../marketmodels/ir_curve_fitter.py) and executes a **skinny relational 2-iteration sparse Newton-Schulz solver** with Step-2 Halley curvature boost, warm inverse reuse, and vector-only adjoint quote risk inside DuckDB/SQL (scaling past $N = 200$ coupled pillars in `104.4 ms` cold / `71.1 ms` warm with `max |dr| = 9.71e-16`), alongside 100%-SQL portfolio valuation, sensitivity evaluation, and Jacobian chain-rule aggregation across all **8 `legend-lite` dialects** (`newton_step_pure` and `inverse_jacobian_pure` remain as small-$N$ scalar inspection helpers).
  - Local numerical evaluation ([`CurveFitter.fit`](../marketmodels/ir_curve_fitter.py), [`PythonEngineExpr`](../engines/python_expr.py), `.numerical()`) remains completely standalone (zero DuckDB/Arrow/JVM dependency) as a step-debugging tool and differential test oracle.
  - **Zero host-side math evaluation or post-processing** in data tables handed to or returned from SQL (`TENET_CHARTER.md` Clauses C2.1 & C2.3).

---

## 1B. Why Scalar Micro-Expressions (`expr.Expr`) Form the Inner Engine of Macro `page.flow()` Blocks

A central architectural insight of `python/pricing` on `legend-lite` is that quantitative finance requires **two complementary levels of compute graph**—neither of which can cleanly replace the other:

| Dimension | Outer Macro Graph: DataCube `page.flow()` (`Grid`, `Join`, `GroupBy`) | Inner Micro Graph: Scalar `expr.Expr` on Single IEEE-754 Doubles |
| :--- | :--- | :--- |
| **Unit of data** | Relational tables / Arrow frames (`flow_quotes`, `cbs_shared_node`, `cbs_basis_term`, `swap_portfolio`) | Single IEEE-754 `float` scalar (`r_k`, discount factor $D(t)$, forward rate $F$, Black-76 $d_1, d_2$) |
| **Operations** | `filter`, `join`, `groupBy`, `pivot`, `sort`, `.cache()`, `.snap()`, interactive `$parameter` binding | `+`, `-`, `*`, `/`, `exp`, `log`, `sqrt`, `abs_`, `erf`, exact `diff(f, x)`, `basis.expand(f)` |
| **How authored** | Declarative table pipeline (`flow.grid(...)`, `page.join(...)`, `.pure(...)`) | Idiomatic Python math over coupon schedules, knot spans, and closed-form option formulas (zero `ast.parse`) |
| **How debugged** | Inspect intermediate table rows at each numbered box on Sheet `0. Pricing & Risk Flow` | Set a standard `pdb` / IDE breakpoint inside `npv()` or `black_formula()` with `curve.numerical(rates)` and inspect plain Python `float`s |
| **Role in SQL compilation** | Defines the relational query skeleton (`SELECT ... JOIN ... GROUP BY ...`) | Deduplicates shared scalar math via hash-consing (`_dag_stages_pure`), differentiates symbolically, and emits the compact Pure scalar formulas **inside** each Flow block |

Why can't macro `Grid` boxes replace `expr.Expr`?
1. **A single 30Y swap or swaption contains hundreds of scalar operations per row**: Expressing 120 quarterly coupon discount factors, piecewise knot weights, $\ln(F/K)$, $\text{erf}(d_1/\sqrt{2})$, and 12 coupled Newton Jacobian entries as hundreds of separate relational `Grid` boxes on a UI canvas would create an unreadable spaghetti graph and massive query overhead.
2. **Quants debug formulas on single numbers, not tables**: When verifying day-count fractions, telescoping float legs, or Black-76 Vega edge cases, a developer wants `swap.npv(curve.numerical(rates))` to execute standard `float` arithmetic line-by-line in a debugger without spinning up DuckDB or V8.
3. **Hash-consed micro-expressions keep Flow Pure blocks small and fast**: Inside a single Flow block (such as `3. Iter 1: SQL Newton Step` or `7. Fitted Inverse Jacobian`), `_dag_stages_pure` walks the hash-consed `expr.Expr` DAG across all 12 par-swap equations and their 48 active partial derivatives, hoisting shared discount factors and annuity legs into staged Pure `extend` columns (`u_0 .. u_M`)—shrinking generated Pure text by **3x–4x** before DataCube compiles the Flow block to SQL.

### Aim 3 — $O(1)$ Bounded Compiled SQL Size & Fast Warm Market Ticks
- **Compiled SQL size stays bounded as portfolio trade count $N$ grows**: Whether pricing 10 trades or 1,000 swaps / 500 swaptions, compiled SQL remains compact (`3.5 KB` for swaps; `65–162 KB` for multi-tenor swaptions + 2D vol surfaces).
- **Static contract structure vs. ticking market state**:
  - *Cold start (when trades change)*: expand `BasisBook`, compile Pure queries to SQL, and load static trade/topology tables (`CBS_SHARED_NODE`, `CBS_BASIS_TERM`) once.
  - *Warm tick (when market quotes move)*: update only `CBS_KNOT` and `CBS_JACOBIAN_ENTRY` and execute the pre-compiled SQL plans.
- **Automatic work-sharing**: Shared curve/surface evaluations and non-linear subexpressions (`EXP`, `LOG`, `SQRT`, `ERF` arguments) are hoisted automatically into inner `SqlSelect` projection layers (`__pc0, __pc1, ...`) by [`ProjectionCse.java`](../../../core/src/main/java/com/legend/sql/dialect/ProjectionCse.java).

### Aim 4 — Native DataCube, `Frames`, `Flow`, Timeseries & Bitemporal Integration
- **Shared reactive tables**: [`PureBasisEngine`](../engines/pure_basis.py) and [`PureBasisMarketSession`](../engines/pure_basis.py) bind directly onto [`ll.Frames`](../../legend_lite/frames.py) and [`ll.Page`](../../legend_lite/page.py) (`frames=...`), feeding [`pricing/demo/dashboard.py`](../demo/dashboard.py) grids and charts.
- **Timeseries & bitemporal P&L**: Multi-date `SCENARIO_ID = obs_date` runs plug into DataCube's `page.timeseries(...)` DSL and Legend's bitemporal milestoning (`BUS_FROM`/`BUS_THRU`, `PROCESSING_IN`/`PROCESSING_OUT`).

---

## 2. Five Empirical Lessons That Shaped the Architecture

1. **Trades-as-Rows ($O(1)$ SQL) vs. Trades-as-Expressions ($O(N)$ SQL)**:
   - Unrolling $N$ portfolio trades into a single scalar Pure expression scales SQL text as $O(N_{\text{trades}} \times N_{\text{coupons}})$ (`> 3 MB` at 500 swaps). Representing trades as relational rows in `CBS_SHARED_NODE` + `CBS_BASIS_TERM` keeps swap SQL constant at **`3.5 KB`** across 1 to 1,000+ swaps.
2. **Automatic Guard-Safe Compiler CSE (`ProjectionCse.java`) Beats Manual `split()`**:
   - Manual `split()` collided with `meta::pure::functions::string::split`, leaked physical SQL staging into domain code, and hoisted guarded expressions (`log(x)`, `sqrt(x)`) out of `CASE WHEN` branches causing domain errors on inactive rows.
   - Replacing `split()` with automatic reference-counted DAG CSE in [`ProjectionCse.java`](../../../core/src/main/java/com/legend/sql/dialect/ProjectionCse.java) — treating `CASE`/`AND`/`OR`/`COALESCE` as hoisting barriers for general expressions while safely staging guarded `ERF` arguments under mirrored `CASE` guards — cut swaption `quote_risk` SQL from **`6.18 MB` to `65.8–162.0 KB` (38x–95x smaller)** with zero manual annotations.
3. **First-Class `erf` (`ErfMath.java` + `SymbolicDiff.java`) Eliminates Rational-Polynomial Drift**:
   - Registering `meta::legend::lite::erf` with exact Gaussian derivative $\frac{d}{dx}\text{erf}(x) = \frac{2}{\sqrt{\pi}}e^{-x^2}$ in `SymbolicDiff.java` and Cody (1969) IEEE-754 1-ULP SQL lowering in `ErfMath.java` improved option/swaption Greeks from `~1e-4` polynomial-slope error to **`< 1e-10`**, allowing [`cnd.py`](../maths/cnd.py) to drop its custom `Cnd` AST class altogether (`cnd(x) = 0.5 * (1 + expr.erf(x / sqrt(2)))`).
4. **Pure Topology Tables (`TENET_CHARTER.md` C2.1)**:
   - Evicting `NODE_VALUE` and host-side `TERM_VALUE` precomputation from `BasisBook.rows()` / `tables()` and making `PureBasisEngine.run()` execute genuinely in DuckDB SQL eliminated redundant cold-start Python evaluation and aligned strictly with `TENET_CHARTER.md` C2.1.
5. **Hierarchical `SubMeasure` Staging Solves Composite Non-Linear Payoffs (`IRSwaption` + 2D `SwaptionVolSurface`)**:
   - A European swaption applies `black_formula` to coupon-sum aggregates ($A = \sum_i \alpha_i D_{\text{OIS}}(t_i)$, $V_{\text{flt}} = \sum_j \text{flt}_j$, $F = V_{\text{flt}} / A$) and a 4-knot bilinear volatility $\sigma = \sum_{a,b} w_{ab}\sigma_{ab}$. Neither flat cashflow summation nor a single wide row works alone.
   - `BasisBook._factor_expr` automatically factors multi-term additive polynomials (`len(poly) >= 2`) feeding non-polynomial operators into Stage-0 `SubMeasure`s (`TERM_STAGE = 0`, evaluated with lightweight `cbsEvalPolyTerm`), then evaluates compact $\le 4$-slot Black-76 formulas and Pure `diff()` multivariate chain-rule terms (`SUB_CHAIN`) at `TERM_STAGE = 1`.
6. **Exact Canonical Coordinates vs. Approximating Series**:
   - The engine uses the computer algebra terms `Polynomial` and `expand()` because it maintains an internal dictionary mapping monomial tuples to coefficients (`dict[tuple[Node, ...], float]`) to collect and cancel algebraic subexpressions (e.g. $e^{a} e^{b} = e^{a+b}$, or telescoping identical float legs to $D(t_0) - D(t_n)$).
   - This represents **exact closed-form mathematics**, not a Taylor or polynomial series approximation. Future work could investigate automated approximations onto a predefined spanning basis for path-dependent or numerical payoffs, but current production models operate with zero approximation error.

---

## 3. Implemented Multi-Stage Architecture

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│ WORLD 1: Host Calibration & Authoring (Python)                               │
│  • MultiCurveFitter / Surface ingress (knots + calibration inverse Jacobian) │
│  • Domain authoring: Swap, EquityOption, IRSwaption, SwaptionVolSurface      │
│  • Plain-float step debugging (.numerical()) & differential test oracle      │
└─────────────────────────────────┬────────────────────────────────────────────┘
                                  │ Arrow tables via ComputeBasis / ll.Frames
                                  ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ WORLD 2: Database Execution via Pure -> 8 SQL Dialects (legend-lite)         │
│                                                                              │
│  Layer 0: Market Knots (CBS_KNOT: 1D curves, 2D vol surfaces, spot inputs)   │
│     │                                                                        │
│     ▼                                                                        │
│  Layer 1: Shared Basis Nodes (CBS_SHARED_NODE: LERP, EXP_NEG_XT, RECIP_1PXT) │
│     │     + Automatic Guard-Safe SQL Projection CSE (ProjectionCse.java)     │
│     ▼                                                                        │
│  Layer 2: Stage-0 Polynomial Sub-Measures (TERM_STAGE = 0, cbsEvalPolyTerm)  │
│     │     • Swaps evaluate directly without staging (max_stage = 0)          │
│     │     • Swaptions reduce (Annuity, FloatPV, BilinearVol) & dS_r/dknot    │
│     ▼                                                                        │
│  Layer 3: Top-Stage Non-Linear Formulas + Pure diff() Chain Rule             │
│     │     • Compact <= 4-slot cbsFormula_<op> + SUB_CHAIN derivatives        │
│     ▼                                                                        │
│  Layer 4: Calibration Jacobian Chain Rule (CBS_JACOBIAN_ENTRY -> Quote Risk) │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Implementation Status & Roadmap

- [x] **Phase 1 — Core Pure `diff`, `erf` & Guard-Safe `ProjectionCse` (`core/`)**: Implemented & verified across all 8 dialects (`SymbolicDiff.java`, `ErfMath.java`, `DuckDbSpellings.java`, `ProjectionCse.java`, `SymbolicDiffTest.java`).
- [x] **Phase 2 — Hierarchical `SubMeasure` Factoring, `IRSwaption`, 2D `SwaptionVolSurface` & Clean `BasisBook` (`python/`)**: Implemented & verified (`//python:compute_test`, `//python:pricing_test`).
- [x] **Phase 3 — Native `ll.Frames` / `ll.Page` Binding & Codebase Simplification (`python/pricing/`)**: Implemented & verified (`PureBasisEngine(..., frames=page)`, `PureBasisEngine.run()` in SQL, `NODE_VALUE` & custom `Cnd` removed).
- [x] **Phase 4 — Architectural Hardening & Alignment with `legend-lite` Review**:
  1. *Explicit Context Architecture*: Replaced all ambient mutable globals (`_ACTIVE_SCHEMA`, `_ACTIVE_BOOK`, `_OPEN_CBS`, `_FORMULA_BY_SIG`, `_FORMULA_BY_OP`) with a clean `BasisContext` dataclass, keeping `python/BUILD.bazel` unchanged.
  2. *Split Value vs. Risk & Native DuckDB ERF*: Native single-evaluation list-lambda `erf(x)` in `DuckDbSpellings.java` (eliminating 15 nested `ProjectionCse` subselects); partitioned `cbsEvalPrimalTerm` and `cbsEvalDiffTerm` in Pure; `value_and_chained_query` halving roundtrips by computing both entity NPVs and chained quote risk in a single SQL execution.
  3. *Explicit Schema Metadata*: Serialized `BasisSchema` into `b'legend.basis_schema'` Arrow table metadata on `CBS_BASIS_TERM`, replacing cell-by-cell `'S'` prefix scanning with $O(1)$ schema recovery.
  4. *Live `Frames` In-Place Updates*: `_put_frame` updates Arrow tables and regenerates `_read` callable in-place with zero view dropping or `HeldModel` recompilation.
  5. *Unified Database & Python Calibration*: `CalibrationParInstrument(Instrument)` and `PureBasisCurveFitter` compile calibration swaps into skinny fixed-schema [`CalibrationBasisTables`](../marketmodels/ir_curve_fitter.py) and solve coupled curves + adjoint quote risk in SQL via sparse Newton-Schulz $O(\log L_{\max})$ doubling and Step-2 Halley curvature, scaling cleanly past $N = 200$ pillars while keeping the pure-Python debug path intact.
- [ ] **Phase 5 — Timeseries Replay & Bitemporal P&L Attribution**:
  - Multi-date historical curve & vol replay (`SCENARIO_ID = obs_date`) wired into DataCube's `page.timeseries(...)` and bitemporal `BUS_FROM`/`PROCESSING_IN` clean-vs-new-trade P&L attribution.

---

## 5. Empirical Scaling & SQL-Size Results

### A. Two-Curve Interest Rate Swaps (`bazel run //python:irs_scaling`, 12 Quotes, OIS + 3M)

| Portfolio Size | Shared Nodes | Basis Terms | DuckDB SQL Size | Cold Build (`Pure/SQL`) | Warm Tick (`Pure/SQL` Analytic) | Warm Tick (`Python` Analytic) | Warm Tick (`Pure/SQL` Bump&Grind) | Warm Tick (`Python` Bump&Grind) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **1 swap** | 25 | 251 | `3.5 KB` | `66.1 ms` | `20.2 ms` | `5.8 ms` | `25.8 ms` | `13.4 ms` |
| **10 swaps** | 206 | 2,008 | `3.5 KB` | `38.6 ms` | `20.5 ms` | `6.8 ms` | `28.5 ms` | `18.3 ms` |
| **100 swaps** | 853 | 19,205 | `3.5 KB` | `97.9 ms` | `21.2 ms` | `18.1 ms` | `31.0 ms` | `61.1 ms` |
| **1,000 swaps** | 1,622 | 195,947 | `3.5 KB` | `698.0 ms` | **`26.2 ms`** (`5.2x` faster) | `135.0 ms` | **`53.5 ms`** (`8.3x` faster) | `445.8 ms` |

### B. European Swaptions (`IRSwaption` + 2D `SwaptionVolSurface`, 1Y–30Y Underlying Two-Curve Swaps)

| Portfolio Size | Shared Nodes | Sub-Measures | Formulas (Base + Diff) | DuckDB `quote_risk` SQL Size | Warm Eval (`Pure/SQL` Analytic) | Warm Eval (`Python` Analytic) | Warm Eval (`Pure/SQL` Bump&Grind) | Warm Eval (`Python` Bump&Grind) | Max SQL vs Py Diff |
| ---: | ---: | ---: | :---: | ---: | ---: | ---: | ---: | ---: | :---: |
| **1 swaption** | 40 | 5 | `3 + 9 = 12` | `65.8 KB` *(was `6.18 MB`)* | `102.7 ms` | `0.6 ms` | `29.6 ms` | `1.1 ms` | `7.28e-12` |
| **10 swaptions** | 378 | 50 | `6 + 18 = 24` | `112.7 KB` | `191.2 ms` | `2.7 ms` | `31.1 ms` | `6.8 ms` | `1.46e-10` |
| **100 swaptions** | 1,648 | 462 | `6 + 18 = 24` | `112.7 KB` | `199.7 ms` | `19.6 ms` | `38.0 ms` (`1.6x` faster) | `59.4 ms` | `2.62e-10` |
| **500 swaptions** | 2,165 | 1,683 | `6 + 18 = 24` | `162.0 KB` | `225.5 ms` | `92.8 ms` | **`60.7 ms`** (`4.3x` faster) | `261.1 ms` | `3.49e-10` |
