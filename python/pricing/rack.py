"""SOD schedule + holiday expansion, external curve fitting, and Pure pricing bridge.

Keeps schedule/holiday rules and multivariable curve calibration in Python, feeding two
independent domain tables (PR_CURVE_KNOT and PR_CASHFLOW) into projects/pricing.
Note: This slice passes pre-expanded year fractions (knot_t, pay_t) to Pure; real StrictDate/DateTime
domain columns, G10/DB holiday calendars, and seasoned OIS fixings arrive in a follow-up PR.
"""

from __future__ import annotations

import datetime as dt
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import pyarrow as pa
import legend_lite as ll


VALID_INTERP_METHODS = frozenset({"LOG_LINEAR_DF", "LINEAR_ZERO", "LINEAR_DF"})


def _require_finite(name: str, val: float) -> float:
    if isinstance(val, bool) or not isinstance(val, (int, float)) or not math.isfinite(val):
        raise ValueError(f"{name} must be a finite float, got {val!r}")
    return float(val)


def _resolve_pricing_dir(projects_dir: Path | None) -> Path:
    if projects_dir is not None:
        return projects_dir
    env_files = os.environ.get("PRICING_PROJECT_FILES")
    if env_files:
        from python.runfiles import runfiles

        rf = runfiles.Create()
        if rf is not None:
            for rel in env_files.split():
                if rel.endswith("/model.pure"):
                    resolved = rf.Rlocation(rel, source_repo="")
                    if resolved and Path(resolved).is_file():
                        return Path(resolved).parent
        raise FileNotFoundError(f"Cannot resolve PRICING_PROJECT_FILES={env_files!r} via Bazel runfiles")
    cand = Path(__file__).resolve().parents[2] / "projects" / "pricing"
    if (cand / "model.pure").is_file():
        return cand
    raise FileNotFoundError("Cannot locate projects/pricing; pass projects_dir=... or set PRICING_PROJECT_FILES")


def discount_curve_for_csa(ccy: str, csa: str) -> str:
    """Return the canonical collateralized discount curve ID for cashflow ``ccy`` under ``csa`` collateral."""
    if not ccy or not csa:
        raise ValueError(f"ccy ({ccy!r}) and csa ({csa!r}) must be non-empty")
    return f"{ccy}_OIS" if ccy == csa else f"{ccy}_DISC_{csa}"


@dataclass(frozen=True)
class CurveKnot:
    scenario_id: str
    curve_id: str
    knot_idx: int
    knot_t: float
    zero_rate: float
    interp_method: str


@dataclass(frozen=True)
class CashflowRow:
    trade_id: str
    leg_id: str
    cf_idx: int
    instrument_type: str
    ccy: str
    csa: str
    discount_curve_id: str
    proj_curve_id: str
    accrual_start_t: float
    accrual_end_t: float
    pay_t: float
    dcf: float
    notional: float
    fixed_rate: float
    spread: float
    is_float: bool
    pay_receive: float


@dataclass(frozen=True)
class SwapSpec:
    trade_id: str
    instrument_type: str  # "IRS" or "OIS"
    ccy: str
    discount_curve_id: str
    proj_curve_id: str
    start_date: dt.date
    maturity_months: int
    freq_months: int
    notional: float
    fixed_rate: float
    spread: float = 0.0
    pay_fixed: bool = True
    day_count: str = "ACT/365F"
    csa: str = ""


@dataclass(frozen=True)
class FraSpec:
    trade_id: str
    ccy: str
    discount_curve_id: str
    proj_curve_id: str
    start_date: dt.date
    end_date: dt.date
    notional: float
    fixed_rate: float
    pay_fixed: bool = True
    day_count: str = "ACT/360"
    csa: str = ""


@dataclass(frozen=True)
class XccyBasisSwapSpec:
    """Float-for-float cross-currency basis swap with initial and final notional exchanges.

    Leg 1 (foreign, e.g. EUR) pays/receives float(leg1_proj) + leg1_spread, discounted on
    leg1_discount_curve_id (or ``discount_curve_for_csa(ccy1, csa)`` when empty).
    Leg 2 (reporting currency, e.g. USD) pays/receives flat float(leg2_proj), discounted on
    leg2_discount_curve_id (or ``discount_curve_for_csa(ccy2, csa)`` when empty).
    """

    trade_id: str
    ccy1: str
    ccy2: str
    leg1_discount_curve_id: str
    leg1_proj_curve_id: str
    leg2_discount_curve_id: str
    leg2_proj_curve_id: str
    start_date: dt.date
    maturity_months: int
    freq_months: int
    notional_ccy1: float
    fx_spot_ccy2_per_ccy1: float
    leg1_spread: float
    pay_leg1: bool = True
    day_count1: str = "ACT/360"
    day_count2: str = "ACT/360"
    csa: str = ""


def _add_months(d: dt.date, months: int) -> dt.date:
    month0 = d.month - 1 + months
    year = d.year + month0 // 12
    month = month0 % 12 + 1
    for day in (d.day, 30, 29, 28):
        try:
            return dt.date(year, month, day)
        except ValueError:
            continue
    raise ValueError(f"Invalid date {year}-{month}-{d.day}")


def is_business_day(d: dt.date, holidays: set[dt.date]) -> bool:
    return d.weekday() < 5 and d not in holidays


def adjust_date(
    d: dt.date,
    holidays: set[dt.date],
    convention: str = "MOD_FOLLOWING",
) -> dt.date:
    """Adjust a schedule date for weekends and dynamic holiday calendars."""
    if is_business_day(d, holidays):
        return d
    if convention == "FOLLOWING":
        cur = d
        while not is_business_day(cur, holidays):
            cur += dt.timedelta(days=1)
        return cur
    if convention == "MOD_FOLLOWING":
        cur = d
        while not is_business_day(cur, holidays):
            cur += dt.timedelta(days=1)
        if cur.month == d.month:
            return cur
        cur = d
        while not is_business_day(cur, holidays):
            cur -= dt.timedelta(days=1)
        return cur
    raise ValueError(f"Unsupported convention: {convention}")


def imm_date(year: int, month: int) -> dt.date:
    """Return the 3rd Wednesday (IMM date) of the given year and month."""
    first = dt.date(year, month, 1)
    offset = (2 - first.weekday()) % 7  # Wednesday == 2
    return first + dt.timedelta(days=offset + 14)


def next_imm_dates(valuation_date: dt.date, count: int) -> list[dt.date]:
    """Return the next ``count`` quarterly IMM dates (Mar/Jun/Sep/Dec) strictly after ``valuation_date``."""
    if count <= 0:
        raise ValueError(f"count ({count}) must be positive")
    out: list[dt.date] = []
    y = valuation_date.year
    q_months = (3, 6, 9, 12)
    while len(out) < count:
        for qm in q_months:
            if (y, qm) >= (valuation_date.year, valuation_date.month):
                d = imm_date(y, qm)
                if d > valuation_date:
                    out.append(d)
                    if len(out) == count:
                        break
        y += 1
    return out


def year_fraction(d1: dt.date, d2: dt.date, basis: str = "ACT/365F") -> float:
    days = (d2 - d1).days
    if basis == "ACT/365F":
        return days / 365.0
    if basis == "ACT/360":
        return days / 360.0
    if basis == "30/360":
        d1_d = min(d1.day, 30)
        d2_d = 30 if (d2.day == 31 and d1_d == 30) else d2.day
        return ((d2.year - d1.year) * 360 + (d2.month - d1.month) * 30 + (d2_d - d1_d)) / 360.0
    raise ValueError(f"Unsupported day count basis: {basis}")


def _build_month_schedule(maturity_months: int, freq_months: int) -> list[int]:
    """Roll backward from maturity_months so non-divisible tenors form a front stub."""
    if maturity_months <= 0 or freq_months <= 0:
        raise ValueError(
            f"maturity_months ({maturity_months}) and freq_months ({freq_months}) must be positive"
        )
    months: list[int] = []
    m = maturity_months
    while m > 0:
        months.append(m)
        m -= freq_months
    months.append(0)
    months.reverse()
    return months


def expand_swap_to_cashflows(
    spec: SwapSpec,
    valuation_date: dt.date,
    holidays: set[dt.date],
) -> list[CashflowRow]:
    """Expand a swap at SOD with front-stub support, holiday adjustment, CSA discounting, and past-coupon filtering."""
    _require_finite("SwapSpec.notional", spec.notional)
    _require_finite("SwapSpec.fixed_rate", spec.fixed_rate)
    _require_finite("SwapSpec.spread", spec.spread)
    resolved_csa = spec.csa or spec.ccy
    disc_curve = spec.discount_curve_id or discount_curve_for_csa(spec.ccy, resolved_csa)
    proj_curve = spec.proj_curve_id or disc_curve
    month_offsets = _build_month_schedule(spec.maturity_months, spec.freq_months)
    unadj_dates = [_add_months(spec.start_date, m) for m in month_offsets]
    adj_dates = [adjust_date(d, holidays, "MOD_FOLLOWING") for d in unadj_dates]

    fixed_sign = -1.0 if spec.pay_fixed else 1.0
    float_sign = -fixed_sign
    rows: list[CashflowRow] = []

    for i in range(len(adj_dates) - 1):
        s_d, e_d = adj_dates[i], adj_dates[i + 1]
        if e_d <= valuation_date:
            continue
        t_start = year_fraction(valuation_date, s_d, "ACT/365F")
        t_end = year_fraction(valuation_date, e_d, "ACT/365F")
        if t_start < 0.0:
            raise ValueError(
                f"Mid-period seasoned swap coupon [{s_d}, {e_d}] on {spec.trade_id!r} requires historical fixings "
                "(supported in real-date schedule PR)"
            )
        dcf = year_fraction(s_d, e_d, spec.day_count)
        rows.append(CashflowRow(
            trade_id=spec.trade_id,
            leg_id="FIXED",
            cf_idx=i,
            instrument_type=spec.instrument_type,
            ccy=spec.ccy,
            csa=resolved_csa,
            discount_curve_id=disc_curve,
            proj_curve_id=proj_curve,
            accrual_start_t=t_start,
            accrual_end_t=t_end,
            pay_t=t_end,
            dcf=dcf,
            notional=spec.notional,
            fixed_rate=spec.fixed_rate,
            spread=0.0,
            is_float=False,
            pay_receive=fixed_sign,
        ))
        rows.append(CashflowRow(
            trade_id=spec.trade_id,
            leg_id="FLOAT",
            cf_idx=i,
            instrument_type=spec.instrument_type,
            ccy=spec.ccy,
            csa=resolved_csa,
            discount_curve_id=disc_curve,
            proj_curve_id=proj_curve,
            accrual_start_t=t_start,
            accrual_end_t=t_end,
            pay_t=t_end,
            dcf=dcf,
            notional=spec.notional,
            fixed_rate=0.0,
            spread=spec.spread,
            is_float=True,
            pay_receive=float_sign,
        ))
    return rows


def expand_fra_to_cashflows(
    spec: FraSpec,
    valuation_date: dt.date,
    holidays: set[dt.date],
) -> list[CashflowRow]:
    """Expand an in-arrears FRA (single-period forward swaplet, pay_t = t_end) into PR_CASHFLOW, dropping settled FRAs."""
    _require_finite("FraSpec.notional", spec.notional)
    _require_finite("FraSpec.fixed_rate", spec.fixed_rate)
    if spec.end_date <= spec.start_date:
        raise ValueError(f"FRA end_date ({spec.end_date}) must be after start_date ({spec.start_date})")
    s_d = adjust_date(spec.start_date, holidays, "MOD_FOLLOWING")
    e_d = adjust_date(spec.end_date, holidays, "MOD_FOLLOWING")
    if e_d <= s_d:
        raise ValueError(f"Adjusted FRA end_date ({e_d}) must be after adjusted start_date ({s_d})")
    if e_d <= valuation_date:
        return []
    resolved_csa = spec.csa or spec.ccy
    disc_curve = spec.discount_curve_id or discount_curve_for_csa(spec.ccy, resolved_csa)
    proj_curve = spec.proj_curve_id or disc_curve
    t_start = year_fraction(valuation_date, s_d, "ACT/365F")
    t_end = year_fraction(valuation_date, e_d, "ACT/365F")
    if t_start < 0.0:
        raise ValueError(
            f"Mid-period seasoned FRA [{s_d}, {e_d}] on {spec.trade_id!r} requires historical fixings"
        )
    dcf = year_fraction(s_d, e_d, spec.day_count)
    fixed_sign = -1.0 if spec.pay_fixed else 1.0
    float_sign = -fixed_sign
    return [
        CashflowRow(
            trade_id=spec.trade_id,
            leg_id="FIXED",
            cf_idx=0,
            instrument_type="FRA",
            ccy=spec.ccy,
            csa=resolved_csa,
            discount_curve_id=disc_curve,
            proj_curve_id=proj_curve,
            accrual_start_t=t_start,
            accrual_end_t=t_end,
            pay_t=t_end,
            dcf=dcf,
            notional=spec.notional,
            fixed_rate=spec.fixed_rate,
            spread=0.0,
            is_float=False,
            pay_receive=fixed_sign,
        ),
        CashflowRow(
            trade_id=spec.trade_id,
            leg_id="FLOAT",
            cf_idx=0,
            instrument_type="FRA",
            ccy=spec.ccy,
            csa=resolved_csa,
            discount_curve_id=disc_curve,
            proj_curve_id=proj_curve,
            accrual_start_t=t_start,
            accrual_end_t=t_end,
            pay_t=t_end,
            dcf=dcf,
            notional=spec.notional,
            fixed_rate=0.0,
            spread=0.0,
            is_float=True,
            pay_receive=float_sign,
        ),
    ]


def expand_xccy_basis_swap_to_cashflows(
    spec: XccyBasisSwapSpec,
    valuation_date: dt.date,
    holidays: set[dt.date],
    in_reporting_ccy: bool = True,
) -> list[CashflowRow]:
    """Expand a cross-currency basis swap (coupons + initial/final notional exchanges) into PR_CASHFLOW.

    When ``in_reporting_ccy=True`` (default), Leg 1 amounts are converted into ``spec.ccy2`` at
    ``fx_spot_ccy2_per_ccy1`` and every row carries ``ccy=spec.ccy2`` so the ``currency`` column
    honestly matches the cashflow units. When ``in_reporting_ccy=False``, each leg keeps its
    native notional and native ``ccy`` (``spec.ccy1`` vs ``spec.ccy2``) for per-``(ccy, csa)`` PV reporting.
    """
    _require_finite("XccyBasisSwapSpec.notional_ccy1", spec.notional_ccy1)
    _require_finite("XccyBasisSwapSpec.fx_spot_ccy2_per_ccy1", spec.fx_spot_ccy2_per_ccy1)
    _require_finite("XccyBasisSwapSpec.leg1_spread", spec.leg1_spread)
    if spec.fx_spot_ccy2_per_ccy1 <= 0.0:
        raise ValueError(f"fx_spot_ccy2_per_ccy1 must be positive, got {spec.fx_spot_ccy2_per_ccy1}")
    resolved_csa = spec.csa or spec.ccy2
    leg1_disc = spec.leg1_discount_curve_id or discount_curve_for_csa(spec.ccy1, resolved_csa)
    leg2_disc = spec.leg2_discount_curve_id or discount_curve_for_csa(spec.ccy2, resolved_csa)
    leg1_proj = spec.leg1_proj_curve_id or f"{spec.ccy1}_OIS"
    leg2_proj = spec.leg2_proj_curve_id or f"{spec.ccy2}_OIS"

    month_offsets = _build_month_schedule(spec.maturity_months, spec.freq_months)
    unadj_dates = [_add_months(spec.start_date, m) for m in month_offsets]
    adj_dates = [adjust_date(d, holidays, "MOD_FOLLOWING") for d in unadj_dates]

    notional_ccy2 = spec.notional_ccy1 * spec.fx_spot_ccy2_per_ccy1
    leg1_notional = notional_ccy2 if in_reporting_ccy else spec.notional_ccy1
    leg1_ccy = spec.ccy2 if in_reporting_ccy else spec.ccy1
    leg2_notional = notional_ccy2
    leg2_ccy = spec.ccy2

    leg1_sign = -1.0 if spec.pay_leg1 else 1.0
    leg2_sign = -leg1_sign
    rows: list[CashflowRow] = []

    t0 = year_fraction(valuation_date, adj_dates[0], "ACT/365F")
    if t0 > 0.0:
        rows.append(CashflowRow(
            trade_id=spec.trade_id, leg_id=f"{spec.ccy1}_NX", cf_idx=0,
            instrument_type="XCCY", ccy=leg1_ccy, csa=resolved_csa,
            discount_curve_id=leg1_disc, proj_curve_id=leg1_proj,
            accrual_start_t=t0, accrual_end_t=t0, pay_t=t0, dcf=1.0,
            notional=leg1_notional, fixed_rate=1.0, spread=0.0, is_float=False,
            pay_receive=-leg1_sign,
        ))
        rows.append(CashflowRow(
            trade_id=spec.trade_id, leg_id=f"{spec.ccy2}_NX", cf_idx=0,
            instrument_type="XCCY", ccy=leg2_ccy, csa=resolved_csa,
            discount_curve_id=leg2_disc, proj_curve_id=leg2_proj,
            accrual_start_t=t0, accrual_end_t=t0, pay_t=t0, dcf=1.0,
            notional=leg2_notional, fixed_rate=1.0, spread=0.0, is_float=False,
            pay_receive=-leg2_sign,
        ))

    for i in range(len(adj_dates) - 1):
        s_d, e_d = adj_dates[i], adj_dates[i + 1]
        if e_d <= valuation_date:
            continue
        t_start = year_fraction(valuation_date, s_d, "ACT/365F")
        t_end = year_fraction(valuation_date, e_d, "ACT/365F")
        if t_start < 0.0:
            raise ValueError(
                f"Mid-period seasoned XCCY float coupon [{s_d}, {e_d}] on {spec.trade_id!r} requires historical fixings"
            )
        dcf1 = year_fraction(s_d, e_d, spec.day_count1)
        dcf2 = year_fraction(s_d, e_d, spec.day_count2)
        rows.append(CashflowRow(
            trade_id=spec.trade_id, leg_id=f"{spec.ccy1}_FLOAT", cf_idx=i,
            instrument_type="XCCY", ccy=leg1_ccy, csa=resolved_csa,
            discount_curve_id=leg1_disc, proj_curve_id=leg1_proj,
            accrual_start_t=t_start, accrual_end_t=t_end, pay_t=t_end, dcf=dcf1,
            notional=leg1_notional, fixed_rate=0.0, spread=spec.leg1_spread, is_float=True,
            pay_receive=leg1_sign,
        ))
        rows.append(CashflowRow(
            trade_id=spec.trade_id, leg_id=f"{spec.ccy2}_FLOAT", cf_idx=i,
            instrument_type="XCCY", ccy=leg2_ccy, csa=resolved_csa,
            discount_curve_id=leg2_disc, proj_curve_id=leg2_proj,
            accrual_start_t=t_start, accrual_end_t=t_end, pay_t=t_end, dcf=dcf2,
            notional=leg2_notional, fixed_rate=0.0, spread=0.0, is_float=True,
            pay_receive=leg2_sign,
        ))

    t_mat = year_fraction(valuation_date, adj_dates[-1], "ACT/365F")
    if t_mat > 0.0:
        rows.append(CashflowRow(
            trade_id=spec.trade_id, leg_id=f"{spec.ccy1}_NX", cf_idx=1,
            instrument_type="XCCY", ccy=leg1_ccy, csa=resolved_csa,
            discount_curve_id=leg1_disc, proj_curve_id=leg1_proj,
            accrual_start_t=t_mat, accrual_end_t=t_mat, pay_t=t_mat, dcf=1.0,
            notional=leg1_notional, fixed_rate=1.0, spread=0.0, is_float=False,
            pay_receive=leg1_sign,
        ))
        rows.append(CashflowRow(
            trade_id=spec.trade_id, leg_id=f"{spec.ccy2}_NX", cf_idx=1,
            instrument_type="XCCY", ccy=leg2_ccy, csa=resolved_csa,
            discount_curve_id=leg2_disc, proj_curve_id=leg2_proj,
            accrual_start_t=t_mat, accrual_end_t=t_mat, pay_t=t_mat, dcf=1.0,
            notional=leg2_notional, fixed_rate=1.0, spread=0.0, is_float=False,
            pay_receive=leg2_sign,
        ))
    return rows


def _ref_df(knots: Sequence[tuple[float, float]], method: str, t: float) -> float:
    _require_finite("t", t)
    if method not in VALID_INTERP_METHODS:
        raise ValueError(f"Unsupported interp_method: {method!r}")
    if t <= 0.0:
        return 1.0
    if t <= knots[0][0]:
        return math.exp(-knots[0][1] * t)
    if t >= knots[-1][0]:
        return math.exp(-knots[-1][1] * t)
    for i in range(len(knots) - 1):
        t0, r0 = knots[i]
        t1, r1 = knots[i + 1]
        if t0 <= t <= t1:
            w = (t - t0) / (t1 - t0) if t1 > t0 else 0.0
            if method == "LOG_LINEAR_DF":
                return math.exp(-((1.0 - w) * (r0 * t0) + w * (r1 * t1)))
            if method == "LINEAR_DF":
                return (1.0 - w) * math.exp(-r0 * t0) + w * math.exp(-r1 * t1)
            r = (1.0 - w) * r0 + w * r1
            return math.exp(-r * t)
    raise RuntimeError(f"Unbracketed t={t}")


def price_cashflows_python(
    cashflows: Sequence[CashflowRow],
    knots: Sequence[CurveKnot],
) -> dict[tuple[str, str, str, str], float]:
    """Host float pricing evaluator & calibration kernel: returns ``{(scenario_id, trade_id, ccy, csa): npv}``."""
    by_scen_curve: dict[tuple[str, str], tuple[str, list[tuple[float, float]]]] = {}
    scenarios: set[str] = set()
    for k in sorted(knots, key=lambda x: (x.scenario_id, x.curve_id, x.knot_t, x.knot_idx)):
        _require_finite("CurveKnot.knot_t", k.knot_t)
        _require_finite("CurveKnot.zero_rate", k.zero_rate)
        if k.interp_method not in VALID_INTERP_METHODS:
            raise ValueError(f"Unsupported interp_method: {k.interp_method!r}")
        if k.knot_t <= 0.0:
            raise ValueError(f"knot_t must be positive, got {k.knot_t}")
        scenarios.add(k.scenario_id)
        key = (k.scenario_id, k.curve_id)
        if key not in by_scen_curve:
            by_scen_curve[key] = (k.interp_method, [])
        else:
            prev_method, pts = by_scen_curve[key]
            if k.interp_method != prev_method:
                raise ValueError(f"Mixed interp_method on curve {key}: {prev_method!r} vs {k.interp_method!r}")
            if pts and k.knot_t <= pts[-1][0]:
                raise ValueError(f"Duplicate or non-increasing knot_t {k.knot_t} on curve {key}")
        by_scen_curve[key][1].append((k.knot_t, k.zero_rate))

    for cf in cashflows:
        _require_finite("CashflowRow.pay_t", cf.pay_t)
        if cf.pay_t > 0.0:
            _require_finite("CashflowRow.accrual_start_t", cf.accrual_start_t)
            _require_finite("CashflowRow.accrual_end_t", cf.accrual_end_t)
            _require_finite("CashflowRow.dcf", cf.dcf)
            _require_finite("CashflowRow.notional", cf.notional)
            _require_finite("CashflowRow.fixed_rate", cf.fixed_rate)
            _require_finite("CashflowRow.spread", cf.spread)
            _require_finite("CashflowRow.pay_receive", cf.pay_receive)
            if cf.pay_receive not in (-1.0, 1.0):
                raise ValueError(f"pay_receive must be -1.0 or +1.0 on trade {cf.trade_id}: {cf.pay_receive}")
            if cf.dcf <= 0.0:
                raise ValueError(f"Non-positive dcf on trade {cf.trade_id}: {cf.dcf}")
            if cf.is_float and (cf.accrual_start_t < 0.0 or cf.accrual_end_t <= cf.accrual_start_t):
                raise ValueError(f"Invalid or mid-period float accrual [{cf.accrual_start_t}, {cf.accrual_end_t}] on trade {cf.trade_id}")

    totals: dict[tuple[str, str, str, str], float] = {}
    for scen in scenarios:
        for cf in cashflows:
            if cf.pay_t <= 0.0:
                continue
            d_key = (scen, cf.discount_curve_id)
            if d_key not in by_scen_curve:
                raise KeyError(f"Missing discount curve {d_key}")
            d_method, d_pts = by_scen_curve[d_key]
            df_pay = _ref_df(d_pts, d_method, cf.pay_t)
            if cf.is_float:
                proj_id = cf.proj_curve_id if cf.proj_curve_id else cf.discount_curve_id
                p_key = (scen, proj_id)
                if p_key not in by_scen_curve:
                    raise KeyError(f"Missing projection curve {p_key}")
                p_method, p_pts = by_scen_curve[p_key]
                df_s = _ref_df(p_pts, p_method, cf.accrual_start_t)
                df_e = _ref_df(p_pts, p_method, cf.accrual_end_t)
                rate = ((df_s / df_e) - 1.0) / cf.dcf + cf.spread
            else:
                rate = cf.fixed_rate
            pv = cf.pay_receive * cf.notional * cf.dcf * rate * df_pay
            tkey = (scen, cf.trade_id, cf.ccy, cf.csa)
            totals[tkey] = totals.get(tkey, 0.0) + pv
    return totals


def fit_zero_curve(
    benchmark_swaps: Sequence[SwapSpec],
    valuation_date: dt.date,
    holidays: set[dt.date],
    curve_id: str,
    interp_method: str = "LOG_LINEAR_DF",
    max_iter: int = 20,
    tol: float = 1e-12,
) -> list[CurveKnot]:
    """Simultaneous multivariable Gauss-Newton curve fitter in host Python floats."""
    if interp_method not in VALID_INTERP_METHODS:
        raise ValueError(f"Unsupported interp_method: {interp_method!r}")
    if not benchmark_swaps:
        raise ValueError("benchmark_swaps must be non-empty")
    if isinstance(max_iter, bool) or max_iter < 1:
        raise ValueError(f"max_iter must be >= 1, got {max_iter!r}")
    _require_finite("tol", tol)
    if tol <= 0.0 or tol > 1e-3:
        raise ValueError(f"tol must be in (0, 1e-3], got {tol}")
    for sw in benchmark_swaps:
        if _require_finite("SwapSpec.notional", sw.notional) == 0.0:
            raise ValueError(f"Benchmark swap {sw.trade_id!r} notional must be non-zero")
        _require_finite("SwapSpec.fixed_rate", sw.fixed_rate)
        _require_finite("SwapSpec.spread", sw.spread)

    cfs_by_swap = [expand_swap_to_cashflows(sw, valuation_date, holidays) for sw in benchmark_swaps]
    if any(not cfs for cfs in cfs_by_swap):
        raise ValueError("All benchmark_swaps must have at least one future cashflow")
    knot_ts = [max(cf.pay_t for cf in cfs) for cfs in cfs_by_swap]
    if len(set(knot_ts)) != len(knot_ts) or knot_ts != sorted(knot_ts):
        raise ValueError(f"Benchmark pillar maturities must be strictly increasing, got {knot_ts}")
    rates = [float(sw.fixed_rate) for sw in benchmark_swaps]
    n = len(knot_ts)

    def residuals(r_vec: list[float]) -> list[float]:
        knots = [
            CurveKnot("BASE", curve_id, idx, knot_ts[idx], r_vec[idx], interp_method)
            for idx in range(n)
        ]
        res: list[float] = []
        for idx, cfs in enumerate(cfs_by_swap):
            npv = price_cashflows_python(cfs, knots)[("BASE", cfs[0].trade_id, cfs[0].ccy, cfs[0].csa)]
            val = npv / benchmark_swaps[idx].notional
            if not math.isfinite(val):
                raise RuntimeError(f"Non-finite residual while fitting curve {curve_id!r}: {val!r}")
            res.append(val)
        return res

    eps = 6e-6
    converged = False
    for _ in range(max_iter):
        f0 = residuals(rates)
        if all(math.isfinite(v) and abs(v) < tol for v in f0):
            converged = True
            break
        jac = [[0.0] * n for _ in range(n)]
        for j in range(n):
            rp = list(rates)
            rm = list(rates)
            rp[j] += eps
            rm[j] -= eps
            fp = residuals(rp)
            fm = residuals(rm)
            for i in range(n):
                jac[i][j] = (fp[i] - fm[i]) / (2.0 * eps)
        aug = [jac[i][:] + [-f0[i]] for i in range(n)]
        for col in range(n):
            piv = max(range(col, n), key=lambda r: abs(aug[r][col]))
            aug[col], aug[piv] = aug[piv], aug[col]
            pv = aug[col][col]
            if not math.isfinite(pv) or abs(pv) < 1e-15:
                raise RuntimeError(f"Singular Jacobian while fitting curve {curve_id!r} at column {col}")
            for r in range(col + 1, n):
                factor = aug[r][col] / pv
                for c in range(col, n + 1):
                    aug[r][c] -= factor * aug[col][c]
        delta = [0.0] * n
        for i in range(n - 1, -1, -1):
            s = aug[i][n] - sum(aug[i][c] * delta[c] for c in range(i + 1, n))
            d_val = s / aug[i][i]
            if not math.isfinite(d_val):
                raise RuntimeError(f"Non-finite Newton step while fitting curve {curve_id!r}")
            delta[i] = d_val
        rates = [rates[i] + delta[i] for i in range(n)]

    if not converged:
        f_end = residuals(rates)
        if not all(math.isfinite(v) and abs(v) < tol for v in f_end):
            final_err = max((abs(v) if math.isfinite(v) else float("inf")) for v in f_end)
            raise RuntimeError(
                f"fit_zero_curve({curve_id!r}) did not converge within {max_iter} iterations (max residual={final_err:.3e} >= {tol:.1e})"
            )

    return [
        CurveKnot("BASE", curve_id, idx, knot_ts[idx], rates[idx], interp_method)
        for idx in range(n)
    ]


def fit_curve_from_cashflows(
    benchmark_cashflows: Sequence[Sequence[CashflowRow]],
    curve_id: str,
    interp_method: str = "LOG_LINEAR_DF",
    fixed_knots: Sequence[CurveKnot] = (),
    initial_rate: float = 0.03,
    max_iter: int = 25,
    tol: float = 1e-12,
) -> list[CurveKnot]:
    """Fit ``curve_id`` from pre-expanded benchmark instruments (IMM FRAs, OIS swaps, or XCCY basis swaps),
    holding any upstream ``fixed_knots`` constant.
    """
    if interp_method not in VALID_INTERP_METHODS:
        raise ValueError(f"Unsupported interp_method: {interp_method!r}")
    if not benchmark_cashflows:
        raise ValueError("benchmark_cashflows must be non-empty")
    if isinstance(max_iter, bool) or max_iter < 1:
        raise ValueError(f"max_iter must be >= 1, got {max_iter!r}")
    _require_finite("initial_rate", initial_rate)
    _require_finite("tol", tol)
    if tol <= 0.0 or tol > 1e-3:
        raise ValueError(f"tol must be in (0, 1e-3], got {tol}")
    for k in fixed_knots:
        _require_finite("CurveKnot.knot_t", k.knot_t)
        _require_finite("CurveKnot.zero_rate", k.zero_rate)
    for cfs in benchmark_cashflows:
        for cf in cfs:
            _require_finite("CashflowRow.pay_t", cf.pay_t)
            _require_finite("CashflowRow.dcf", cf.dcf)
            _require_finite("CashflowRow.notional", cf.notional)
            _require_finite("CashflowRow.fixed_rate", cf.fixed_rate)
            _require_finite("CashflowRow.spread", cf.spread)
    if any(not [cf for cf in cfs if cf.pay_t > 0.0] for cfs in benchmark_cashflows):
        raise ValueError("Each benchmark instrument in benchmark_cashflows must have at least one future cashflow")
    knot_ts = [max(cf.pay_t for cf in cfs if cf.pay_t > 0.0) for cfs in benchmark_cashflows]
    if len(set(knot_ts)) != len(knot_ts) or knot_ts != sorted(knot_ts):
        raise ValueError(f"Benchmark pillar maturities must be strictly increasing, got {knot_ts}")
    n = len(benchmark_cashflows)
    rates = [float(initial_rate)] * n
    fixed_list = list(fixed_knots)
    bm_keys: list[tuple[str, str, str]] = []
    scales: list[float] = []
    for cfs in benchmark_cashflows:
        active = [cf for cf in cfs if cf.pay_t > 0.0]
        buckets = {(cf.trade_id, cf.ccy, cf.csa) for cf in active}
        if len(buckets) != 1:
            raise ValueError(f"Benchmark cashflows must share a single (trade_id, ccy, csa) bucket, got {sorted(buckets)}")
        bm_keys.append(next(iter(buckets)))
        scale = max(abs(cf.notional) for cf in active)
        if scale <= 0.0:
            raise ValueError(f"Benchmark cashflows for {bm_keys[-1][0]!r} must have non-zero notional")
        scales.append(scale)

    def residuals(r_vec: list[float]) -> list[float]:
        knots = fixed_list + [
            CurveKnot("BASE", curve_id, idx, knot_ts[idx], r_vec[idx], interp_method)
            for idx in range(n)
        ]
        res: list[float] = []
        for idx, cfs in enumerate(benchmark_cashflows):
            npv = price_cashflows_python(cfs, knots)[("BASE", *bm_keys[idx])]
            val = npv / scales[idx]
            if not math.isfinite(val):
                raise RuntimeError(f"Non-finite residual while fitting curve {curve_id!r}: {val!r}")
            res.append(val)
        return res

    eps = 6e-6
    converged = False
    for _ in range(max_iter):
        f0 = residuals(rates)
        if all(math.isfinite(v) and abs(v) < tol for v in f0):
            converged = True
            break
        jac = [[0.0] * n for _ in range(n)]
        for j in range(n):
            rp = list(rates)
            rm = list(rates)
            rp[j] += eps
            rm[j] -= eps
            fp = residuals(rp)
            fm = residuals(rm)
            for i in range(n):
                jac[i][j] = (fp[i] - fm[i]) / (2.0 * eps)
        aug = [jac[i][:] + [-f0[i]] for i in range(n)]
        for col in range(n):
            piv = max(range(col, n), key=lambda r: abs(aug[r][col]))
            aug[col], aug[piv] = aug[piv], aug[col]
            pv = aug[col][col]
            if not math.isfinite(pv) or abs(pv) < 1e-15:
                raise RuntimeError(f"Singular Jacobian while fitting curve {curve_id!r} at column {col}")
            for r in range(col + 1, n):
                factor = aug[r][col] / pv
                for c in range(col, n + 1):
                    aug[r][c] -= factor * aug[col][c]
        delta = [0.0] * n
        for i in range(n - 1, -1, -1):
            s = aug[i][n] - sum(aug[i][c] * delta[c] for c in range(i + 1, n))
            d_val = s / aug[i][i]
            if not math.isfinite(d_val):
                raise RuntimeError(f"Non-finite Newton step while fitting curve {curve_id!r}")
            delta[i] = d_val
        rates = [rates[i] + delta[i] for i in range(n)]

    if not converged:
        f_end = residuals(rates)
        if not all(math.isfinite(v) and abs(v) < tol for v in f_end):
            final_err = max((abs(v) if math.isfinite(v) else float("inf")) for v in f_end)
            raise RuntimeError(
                f"fit_curve_from_cashflows({curve_id!r}) did not converge within {max_iter} iterations (max residual={final_err:.3e} >= {tol:.1e})"
            )

    return [
        CurveKnot("BASE", curve_id, idx, knot_ts[idx], rates[idx], interp_method)
        for idx in range(n)
    ]


def build_curve_scenarios(
    base_knots: Sequence[CurveKnot],
    bump_bp: float = 1.0,
) -> list[CurveKnot]:
    """Generate BASE + central +/- bump_bp scenarios for each pillar knot."""
    _require_finite("bump_bp", bump_bp)
    bump = bump_bp * 1e-4
    out = list(base_knots)
    for k in base_knots:
        for sign, tag in ((1.0, "UP"), (-1.0, "DN")):
            scen_id = f"BUMP_{k.curve_id}_{k.knot_idx}_{tag}"
            for row in base_knots:
                rate = row.zero_rate + (sign * bump if row.knot_idx == k.knot_idx and row.curve_id == k.curve_id else 0.0)
                out.append(CurveKnot(scen_id, row.curve_id, row.knot_idx, row.knot_t, rate, row.interp_method))
    return out


def compute_bump_dv01_python(
    npvs: dict[tuple[str, str, str, str], float],
    base_knots: Sequence[CurveKnot],
) -> dict[tuple[str, str, str, str, int], float]:
    """Compute central 1bp DV01 per ``(trade_id, ccy, csa, curve_id, knot_idx)`` from scenario NPVs."""
    buckets = sorted({(t, ccy, csa) for _, t, ccy, csa in npvs.keys()})
    dv01s: dict[tuple[str, str, str, str, int], float] = {}
    for trade, ccy, csa in buckets:
        for k in base_knots:
            up = npvs[(f"BUMP_{k.curve_id}_{k.knot_idx}_UP", trade, ccy, csa)]
            dn = npvs[(f"BUMP_{k.curve_id}_{k.knot_idx}_DN", trade, ccy, csa)]
            dv01s[(trade, ccy, csa, k.curve_id, k.knot_idx)] = 0.5 * (up - dn)
    return dv01s


def _tables_to_arrow(
    knots: Sequence[CurveKnot],
    cashflows: Sequence[CashflowRow],
) -> tuple[pa.Table, pa.Table]:
    knot_tbl = pa.table({
        "SCENARIO_ID": pa.array([k.scenario_id for k in knots], type=pa.string()),
        "CURVE_ID": pa.array([k.curve_id for k in knots], type=pa.string()),
        "KNOT_IDX": pa.array([k.knot_idx for k in knots], type=pa.int16()),
        "KNOT_T": pa.array([k.knot_t for k in knots], type=pa.float64()),
        "ZERO_RATE": pa.array([k.zero_rate for k in knots], type=pa.float64()),
        "INTERP_METHOD": pa.array([k.interp_method for k in knots], type=pa.string()),
    })
    cf_tbl = pa.table({
        "TRADE_ID": pa.array([c.trade_id for c in cashflows], type=pa.string()),
        "LEG_ID": pa.array([c.leg_id for c in cashflows], type=pa.string()),
        "CF_IDX": pa.array([c.cf_idx for c in cashflows], type=pa.int16()),
        "INSTRUMENT_TYPE": pa.array([c.instrument_type for c in cashflows], type=pa.string()),
        "CCY": pa.array([c.ccy for c in cashflows], type=pa.string()),
        "CSA": pa.array([c.csa for c in cashflows], type=pa.string()),
        "DISCOUNT_CURVE_ID": pa.array([c.discount_curve_id for c in cashflows], type=pa.string()),
        "PROJ_CURVE_ID": pa.array([(c.proj_curve_id if c.proj_curve_id else None) for c in cashflows], type=pa.string()),
        "ACCRUAL_START_T": pa.array([c.accrual_start_t for c in cashflows], type=pa.float64()),
        "ACCRUAL_END_T": pa.array([c.accrual_end_t for c in cashflows], type=pa.float64()),
        "PAY_T": pa.array([c.pay_t for c in cashflows], type=pa.float64()),
        "DCF": pa.array([c.dcf for c in cashflows], type=pa.float64()),
        "NOTIONAL": pa.array([c.notional for c in cashflows], type=pa.float64()),
        "FIXED_RATE": pa.array([c.fixed_rate for c in cashflows], type=pa.float64()),
        "SPREAD": pa.array([c.spread for c in cashflows], type=pa.float64()),
        "IS_FLOAT": pa.array([c.is_float for c in cashflows], type=pa.bool_()),
        "PAY_RECEIVE": pa.array([c.pay_receive for c in cashflows], type=pa.float64()),
    })
    return knot_tbl, cf_tbl


def _load_pricing_pure_sources(projects_dir: Path) -> str:
    model = (projects_dir / "model.pure").read_text(encoding="utf-8")
    store = (projects_dir / "store.pure").read_text(encoding="utf-8")
    mapping = (projects_dir / "mapping.pure").read_text(encoding="utf-8")
    runtime = """
###Connection
RelationalDatabaseConnection pricing::DuckDbConn
{
   type: DuckDB;
   specification: DuckDB { };
   auth: Test;
}
###Runtime
Runtime pricing::DuckDbRuntime
{
   mappings: [ pricing::Mapping ];
   connections: [ pricing::Store: [ environment: pricing::DuckDbConn ] ];
}
"""
    return "\n".join((model, store, mapping, runtime))


def run_pure_discount_factors_duckdb(
    knots: Sequence[CurveKnot],
    projects_dir: Path | None = None,
) -> dict[tuple[str, str, float], float]:
    """Evaluate ``pricing::curveDiscountFactors`` at all knot pillar times in DuckDB via ``ll.Frames``."""
    resolved_dir = _resolve_pricing_dir(projects_dir)
    knot_tbl, cf_tbl = _tables_to_arrow(knots, [])

    frames = ll.Frames()
    try:
        frames.register("PR_CURVE_KNOT", knot_tbl)
        frames.register("PR_CASHFLOW", cf_tbl)
        model_text = _load_pricing_pure_sources(resolved_dir)
        with ll.HeldModel(model_text) as held:
            query = (
                "pricing::PrCurveKnot.all()"
                "->project(~[scenarioId: k | $k.scenarioId, curveId: k | $k.curveId, t: k | $k.knotT])"
                "->pricing::curveDiscountFactors()"
                "->sort([~scenarioId->ascending(), ~curveId->ascending(), ~t->ascending()])"
            )
            plan = held.plan(ll.parse(query), "pricing::DuckDbRuntime")
            with frames.serving():
                out = frames.run(plan)
    finally:
        frames.close()

    return {
        (row["scenarioId"], row["curveId"], float(row["t"])): float(row["df"])
        for row in out.to_pylist()
    }


def run_pure_pricing_duckdb(
    knots: Sequence[CurveKnot],
    cashflows: Sequence[CashflowRow],
    projects_dir: Path | None = None,
) -> dict[tuple[str, str, str, str], float]:
    """Execute ``projects/pricing::cashflowPvs()`` grouped by ``(scenarioId, tradeId, currency, csa)`` via ``ll.Frames``."""
    resolved_dir = _resolve_pricing_dir(projects_dir)
    knot_tbl, cf_tbl = _tables_to_arrow(knots, cashflows)

    frames = ll.Frames()
    try:
        frames.register("PR_CURVE_KNOT", knot_tbl)
        frames.register("PR_CASHFLOW", cf_tbl)
        model_text = _load_pricing_pure_sources(resolved_dir)
        with ll.HeldModel(model_text) as held:
            query = (
                "pricing::cashflowPvs()"
                "->groupBy(~[scenarioId, tradeId, currency, csa], ~[npv: x | $x.pv: y | $y->sum()])"
                "->sort([~scenarioId->ascending(), ~tradeId->ascending(), ~currency->ascending(), ~csa->ascending()])"
            )
            plan = held.plan(ll.parse(query), "pricing::DuckDbRuntime")
            with frames.serving():
                out = frames.run(plan)
    finally:
        frames.close()

    return {
        (row["scenarioId"], row["tradeId"], row["currency"], row["csa"]): float(row["npv"])
        for row in out.to_pylist()
    }
