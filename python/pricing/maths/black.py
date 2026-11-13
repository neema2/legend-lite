"""Black-76 and Black-Scholes-Merton option pricing formulas supporting both numerical ``float`` execution (for natural
debugging) and symbolic ``legend_lite.scalar_parallel.expr.Expr`` tracing."""

from __future__ import annotations

from legend_lite.scalar_parallel import expr

from .cnd import phi_approx


def black_formula(
    is_call: bool,
    forward: float | expr.Expr,
    strike: float | expr.Expr,
    time_to_expiry: float | expr.Expr,
    vol: float | expr.Expr,
    discount_factor: float | expr.Expr = 1.0,
) -> float | expr.Expr:
    """Standard Black-76 formula for European options on forwards or forward swap rates."""
    if (
        isinstance(time_to_expiry, (int, float))
        and isinstance(vol, (int, float))
        and isinstance(forward, (int, float))
        and (time_to_expiry <= 0.0 or vol <= 0.0 or forward <= 0.0)
    ):
        intrinsic = max(forward - strike, 0.0) if is_call else max(strike - forward, 0.0)
        return intrinsic * discount_factor

    std_dev = vol * expr.sqrt(time_to_expiry)
    d1 = (expr.log(forward / strike) + 0.5 * vol * vol * time_to_expiry) / std_dev
    d2 = d1 - std_dev
    if is_call:
        return discount_factor * (forward * phi_approx(d1) - strike * phi_approx(d2))
    return discount_factor * (strike * phi_approx(-d2) - forward * phi_approx(-d1))


def black_scholes_formula(
    is_call: bool,
    spot: float | expr.Expr,
    strike: float | expr.Expr,
    time_to_expiry: float | expr.Expr,
    vol: float | expr.Expr,
    df_r: float | expr.Expr,
    df_q: float | expr.Expr,
    *,
    r_rate: float | expr.Expr | None = None,
    q_rate: float | expr.Expr | None = None,
) -> float | expr.Expr:
    """Black-Scholes-Merton formula with continuous dividend yield (or foreign discount curve).

    Uses ``expr.log``, ``expr.sqrt``, and ``phi_approx`` (``cnd``) directly on ``spot``, ``strike``,
    ``time_to_expiry``, ``vol``, ``df_r``, and ``df_q`` without casting inputs via ``expr.lift()``:
      - When called with plain ``float``s, every intermediate variable (``r``, ``q``, ``std_dev``, ``d1``, ``d2``) and
        the returned price is a plain ``float`` in the debugger.
      - When any input is an ``expr.Expr``, standard Python arithmetic and ``expr.*`` / ``phi_approx`` functions trace
        the symbolic expression tree automatically.
    """
    if (
        isinstance(time_to_expiry, (int, float))
        and isinstance(vol, (int, float))
        and isinstance(spot, (int, float))
        and (time_to_expiry <= 0.0 or vol <= 0.0 or spot <= 0.0)
    ):
        intrinsic = max(spot - strike, 0.0) if is_call else max(strike - spot, 0.0)
        return intrinsic * df_r

    if isinstance(df_r, (int, float)) and df_r == 0.0:
        return 0.0

    r = r_rate if r_rate is not None else (-expr.log(df_r) / time_to_expiry)
    q = q_rate if q_rate is not None else (-expr.log(df_q) / time_to_expiry)
    std_dev = vol * expr.sqrt(time_to_expiry)
    d1 = (expr.log(spot / strike) + (r - q + 0.5 * vol * vol) * time_to_expiry) / std_dev
    d2 = d1 - std_dev
    if is_call:
        return spot * df_q * phi_approx(d1) - strike * df_r * phi_approx(d2)
    return strike * df_r * phi_approx(-d2) - spot * df_q * phi_approx(-d1)

