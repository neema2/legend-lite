"""Correctly rounded transcendental functions: the same double on every platform (Bazel workplan P2-01).

Python's `math` calls the platform's C library, and those disagree in the last bit (glibc's cbrt is not correctly
rounded; macOS's is), so an expectation computed with `math.cbrt` would differ by platform and the corpus's
generated files with it. Each function here computes in decimal at 60 significant digits and rounds ONCE to the
nearest double (float(Decimal) is correctly rounded), which gives the correctly rounded result everywhere: the
double nearest the true value. (A true value within 1e-60 of a halfway point could still round the other way; no
double input comes that close for these functions in practice.)

sqrt and fmod need nothing here: IEEE 754 makes them exact (sqrt) or correctly rounded already.
"""
from __future__ import annotations

from decimal import Decimal, localcontext

_PREC = 60


def _d(x: float) -> Decimal:
    return Decimal(x)  # exact: every double is a finite decimal


def _pi() -> Decimal:
    # Machin-free: the decimal module's own recipe (docs), at the current context's precision
    with localcontext() as ctx:
        ctx.prec += 2
        three = Decimal(3)
        lasts, t, s, n, na, d, da = 0, three, 3, 1, 0, 0, 24
        while s != lasts:
            lasts = s
            n, na = n + na, na + 8
            d, da = d + da, da + 32
            t = (t * n) / d
            s += t
    return +s


def _sin_d(x: Decimal) -> Decimal:
    # reduce to [-pi, pi], then the Taylor series (converges fast there at this precision)
    with localcontext() as ctx:
        ctx.prec = _PREC + 20
        pi = _pi()
        x = x.remainder_near(2 * pi)
        i, lasts, s, fact, num, sign = 1, 0, x, 1, x, 1
        while s != lasts:
            lasts = s
            i += 2
            fact *= i * (i - 1)
            num *= x * x
            sign *= -1
            s += num / fact * sign
    return s


def _cos_d(x: Decimal) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = _PREC + 20
        pi = _pi()
        x = x.remainder_near(2 * pi)
        i, lasts, s, fact, num, sign = 0, 0, Decimal(1), 1, Decimal(1), 1
        while s != lasts:
            lasts = s
            i += 2
            fact *= i * (i - 1)
            num *= x * x
            sign *= -1
            s += num / fact * sign
    return s


def _atan_d(x: Decimal) -> Decimal:
    # halve the argument twice (atan x = 2 atan(x / (1 + sqrt(1 + x^2)))), then the series
    with localcontext() as ctx:
        ctx.prec = _PREC + 20
        if x == 0:
            return Decimal(0)
        y = x
        for _ in range(3):
            y = y / (1 + (1 + y * y).sqrt())
        lasts, s, term, n, y2 = 0, y, y, 1, y * y
        while s != lasts:
            lasts = s
            term *= -y2
            n += 2
            s += term / n
        return s * 8


def _round(f) -> float:
    def g(*args: float) -> float:
        with localcontext() as ctx:
            ctx.prec = _PREC
            return float(+f(*[_d(a) for a in args]))
    return g


exp = _round(lambda x: x.exp())
log = _round(lambda x: x.ln())
log10 = _round(lambda x: x.log10())
cbrt = _round(lambda x: (x.copy_abs().ln() / 3).exp().copy_sign(x) if x != 0 else x)
sin = _round(_sin_d)
cos = _round(_cos_d)
tan = _round(lambda x: _sin_d(x) / _cos_d(x))
cot = _round(lambda x: _cos_d(x) / _sin_d(x))
sinh = _round(lambda x: (x.exp() - (-x).exp()) / 2)
cosh = _round(lambda x: (x.exp() + (-x).exp()) / 2)
tanh = _round(lambda x: (x.exp() - (-x).exp()) / (x.exp() + (-x).exp()))
atan = _round(_atan_d)
asin = _round(lambda x: _atan_d(x / (1 - x * x).sqrt()) if abs(x) < 1 else (_pi() / 2).copy_sign(x))
acos = _round(lambda x: _pi() / 2 - (_atan_d(x / (1 - x * x).sqrt()) if abs(x) < 1 else (_pi() / 2).copy_sign(x)))


def atan2(y: float, x: float) -> float:
    with localcontext() as ctx:
        ctx.prec = _PREC
        dy, dx, pi = _d(y), _d(x), _pi()
        if dx > 0:
            r = _atan_d(dy / dx)
        elif dx < 0:
            r = _atan_d(dy / dx) + (pi if dy >= 0 else -pi)
        else:
            r = (pi / 2).copy_sign(dy) if dy != 0 else Decimal(0)
        return float(+r)


def pow(a: float, b: float) -> float:
    """a ** b, correctly rounded where it is real; an integral exponent stays exact as Python computes it."""
    if b == int(b) and abs(b) < 64:
        return a ** int(b) if not (a == 0 and b < 0) else a ** b
    with localcontext() as ctx:
        ctx.prec = _PREC
        return float(_d(a) ** _d(b))
