"""Cumulative standard normal distribution ``cnd`` (``phi_approx``) and standard normal density ``norm_pdf``.

Expressed directly in terms of ``legend_lite.scalar_parallel.expr.erf`` (``math.erf`` on plain ``float``s and
``meta::legend::lite::erf`` in Pure/SQL), requiring zero custom ``Expr`` subclasses in domain code:
  1. Works identically on plain Python ``float``s (for step-debugging) and traced ``expr.Expr`` inputs.
  2. Achieves full IEEE-754 double precision (``<= 2.22e-16``) in both Python and SQL.
  3. Differentiates symbolically in both Python (``expr.diff``) and Pure (``meta::legend::lite::diff``) to the exact
     Gaussian probability density ``exp(-0.5 * x^2) / sqrt(2 * pi)``.
"""

from __future__ import annotations

import math

from legend_lite.scalar_parallel import expr

_INV_SQRT2 = 1.0 / math.sqrt(2.0)
_SQRT_2PI = math.sqrt(2.0 * math.pi)


def norm_pdf(x: float | expr.Expr) -> float | expr.Expr:
    """Standard normal probability density function: ``exp(-0.5 * x^2) / sqrt(2 * pi)``."""
    return expr.exp(-0.5 * x * x) / _SQRT_2PI


def cnd(x: float | expr.Expr) -> float | expr.Expr:
    """Cumulative standard normal distribution ``Phi(x) = 0.5 * (1 + erf(x / sqrt(2)))``."""
    if isinstance(x, expr.Neg):
        return 0.5 * (1.0 + expr.erf(x.a * (-_INV_SQRT2)))
    return 0.5 * (1.0 + expr.erf(x * _INV_SQRT2))


phi_approx = cnd

__all__ = ['cnd', 'phi_approx', 'norm_pdf']
