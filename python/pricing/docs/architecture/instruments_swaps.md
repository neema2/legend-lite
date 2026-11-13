# Financial Products & Portfolio (`pricing/instruments/`, `pricing/maths/`)

This document outlines the design of financial instruments, quantitative math formulas, and the [`Portfolio`](../../instruments/portfolio.py) aggregator in [`pricing/instruments/`](../../instruments/) and [`pricing/maths/`](../../maths/).

---

## 1. Instrument Base Class (`pricing/instruments/base.py`)

All financial products inherit from [`Instrument`](../../instruments/base.py):
- **`npv(*curves) -> expr.Expr | float`**: Returns the symbolic Net Present Value expression tree when given symbolic `ZeroCurve` / `SwaptionVolSurface` models, or a plain `float` when evaluated on numerical models (`curve.numerical(rates)`, `surface.numerical(vols)`).
- **`curves(default_curves=()) -> list[ZeroCurve]`** & **`vol_surfaces() -> list[SwaptionVolSurface]`**: Discover all curves and 2D volatility surfaces referenced by the instrument.
- **`factors(default_curves=()) -> list[FactorSpec]`**: Automatically discovers all curve pillar and volatility surface knot factors referenced by the instrument.

> [!IMPORTANT]
> Instruments do **not** implement risk differentiation methods. All sensitivity logic (Pure/symbolic analytic differentiation vs. numerical bump, refit & reprice) is decoupled into [`pricing/risk/`](../../risk/).

---

## 2. Quantitative Math Primitives (`pricing/maths/`)

- **[`pricing/maths/cnd.py`](../../maths/cnd.py)**:
  - `cnd(x)` (`phi_approx`): Cumulative standard normal distribution $\Phi(x) = \frac{1}{2}\bigl(1 + \text{erf}(x / \sqrt{2})\bigr)$, built directly on `legend_lite.scalar_parallel.expr.erf` (`meta::legend::lite::erf` in Pure) with zero custom `Expr` subclasses. Folds negated arguments `cnd(-d)` into `d * (-1/sqrt(2))` so call/put and payer/receiver options share identical formula templates in SQL.
  - `norm_pdf(x)`: Standard normal density $\phi(x) = \frac{1}{\sqrt{2\pi}} e^{-x^2 / 2}$.
- **[`pricing/maths/black.py`](../../maths/black.py)**:
  - `black_formula(is_call, forward, strike, time_to_expiry, vol, discount_factor=1.0)` and `black_scholes_formula(...)`.
  - Written with zero `expr.lift()` casts: works identically for plain-`float` step debugging and symbolic `expr.Expr` tracing.

---

## 3. Implemented Product Catalog

### A. `IRSwapFixedFloatApprox` (Single-Curve OIS Approximation)
Defined in [`ir_swap_fixed_floatapprox.py`](../../instruments/ir_swap_fixed_floatapprox.py).
- **Fixed Leg PV**: $\sum_{i=1}^n N \cdot r_{\text{fixed}} \cdot \Delta t_i \cdot D(t_i)$
- **Floating Leg PV (Telescoping Identity)**: $N \bigl(D(t_{\text{start}}) - D(t_{\text{end}})\bigr)$

### B. `IRSwapFixedFloat` (Two-Curve Explicit Cashflow Swap)
Defined in [`ir_swap_fixed_float.py`](../../instruments/ir_swap_fixed_float.py).
- Projects floating coupons off `projection_curve` (`USD-SOFR-3M`) and discounts on `discount_curve` (`USD-SOFR-OIS`), expanding into degree-3 `EXP_NEG_XT` monomials (`TERM_OP = 'PROD'`). Algebraically telescopes to `IRSwapFixedFloatApprox` inside `basis.expand()` when `discount_curve == projection_curve` and `float_spread == 0`.

### C. `EquityOption` (European Call / Put via Black-Scholes-Merton)
Defined in [`eq_option.py`](../../instruments/eq_option.py).
- Prices European equity calls and puts discounted on `discount_curve` with continuous dividend curve `dividend_curve`, evaluating both NPV and exact Pure `diff()` curve deltas and par-quote DV01s in SQL to machine precision.

### D. `IRSwaption` (Physically-Settled European Swaption via Black-76)
Defined in [`ir_swaption.py`](../../instruments/ir_swaption.py).
- Composes an underlying two-curve [`IRSwapFixedFloat`](../../instruments/ir_swap_fixed_float.py) ($A = \text{annuity}$, $V_{\text{flt}} = \text{float\_leg\_pv}$, $F = V_{\text{flt}} / A$) with a 2D bilinear [`SwaptionVolSurface`](../../marketmodels/swaption_vol_surface.py) $\sigma(T_e, T_{\text{swap}})$ inside [`black_formula`](../../maths/black.py).
- `BasisBook._factor_expr` automatically stages $A$, $V_{\text{flt}}$, and $\sigma$ as Stage-0 `SubMeasure`s (`TERM_STAGE = 0`), keeping the Stage-1 Black-76 formula narrow ($\le 4$ slots) across arbitrary underlying swap tenors (1Y to 30Y, 120 coupons) and propagating two-curve Delta, par-quote DV01, and 2D Vol Surface Vega in SQL.
