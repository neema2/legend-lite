"""Minimal fractional-year date schedule helpers for interest-rate swaps."""

from __future__ import annotations


def rack_dates(tenor_years: float, start: float = 0.0) -> list[float]:
    """Complete annual coupon schedule rack from ``start`` to ``start + tenor_years``, walking back from maturity."""
    if tenor_years < 0:
        raise ValueError(f'tenor_years must be >= 0, got {tenor_years}')
    dates: list[float] = []
    end = round(float(start + tenor_years), 10)
    start_f = round(float(start), 10)
    t = end
    while t > start_f + 1e-9:
        dates.append(round(t, 10))
        t -= 1.0
    dates.append(start_f)
    return sorted(dates)


def payment_dates(tenor_years: float, start: float = 0.0) -> list[float]:
    """Coupon payment dates = schedule rack minus the initial start date."""
    return rack_dates(tenor_years, start)[1:]


def reset_dates(tenor_years: float, start: float = 0.0) -> list[float]:
    """Floating reset dates = schedule rack minus the final maturity date."""
    return rack_dates(tenor_years, start)[:-1]


def day_count_fraction(t1: float, t2: float) -> float:
    """Year fraction between two decimal-year tenors."""
    return round(float(t2 - t1), 10)

