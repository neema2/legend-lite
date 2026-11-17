"""Parity, independent analytical, and SOD-expansion tests for python/pricing + projects/pricing."""

import dataclasses
import datetime as dt
import math
import os
import time
import unittest
from pathlib import Path

from pricing.rack import (
    CashflowRow,
    CurveKnot,
    FraSpec,
    SwapSpec,
    XccyBasisSwapSpec,
    build_curve_scenarios,
    compute_bump_dv01_python,
    discount_curve_for_csa,
    expand_fra_to_cashflows,
    expand_swap_to_cashflows,
    expand_xccy_basis_swap_to_cashflows,
    fit_curve_from_cashflows,
    fit_zero_curve,
    imm_date,
    next_imm_dates,
    price_cashflows_python,
    run_pure_discount_factors_duckdb,
    run_pure_pricing_duckdb,
    year_fraction,
)


def _find_pricing_dir() -> Path:
    """Resolve projects/pricing strictly via Bazel runfiles (PRICING_PROJECT_FILES)."""
    env_files = os.environ.get("PRICING_PROJECT_FILES")
    if not env_files:
        raise FileNotFoundError("PRICING_PROJECT_FILES must be set by Bazel runfiles")
    from python.runfiles import runfiles

    rf = runfiles.Create()
    if rf is None:
        raise FileNotFoundError("Bazel runfiles unavailable")
    for rel in env_files.split():
        if rel.endswith("/model.pure"):
            resolved = rf.Rlocation(rel, source_repo="")
            if resolved and Path(resolved).is_file():
                return Path(resolved).parent
    raise FileNotFoundError(f"Cannot locate projects/pricing/model.pure in {env_files!r}")


class PricingRackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.val_date = dt.date(2026, 1, 2)
        self.base_holidays = {dt.date(2026, 1, 1), dt.date(2026, 12, 25)}
        self.benchmarks = [
            SwapSpec("BM_6M", "OIS", "USD", "USD_OIS", "USD_OIS", self.val_date, 6, 6, 1_000_000.0, 0.0410),
            SwapSpec("BM_1Y", "OIS", "USD", "USD_OIS", "USD_OIS", self.val_date, 12, 6, 1_000_000.0, 0.0430),
            SwapSpec("BM_2Y", "OIS", "USD", "USD_OIS", "USD_OIS", self.val_date, 24, 6, 1_000_000.0, 0.0460),
            SwapSpec("BM_5Y", "OIS", "USD", "USD_OIS", "USD_OIS", self.val_date, 60, 12, 1_000_000.0, 0.0490),
        ]

    def test_sod_holiday_change_updates_cashflow_rack_and_pv(self) -> None:
        knots = fit_zero_curve(self.benchmarks, self.val_date, self.base_holidays, "USD_OIS", "LOG_LINEAR_DF")
        swap = SwapSpec("SW_TEST", "IRS", "USD", "USD_OIS", "USD_OIS", self.val_date, 12, 6, 10_000_000.0, 0.0430)

        cfs_no_extra = expand_swap_to_cashflows(swap, self.val_date, self.base_holidays)
        extra_holidays = self.base_holidays | {dt.date(2026, 7, 2)}
        cfs_with_holiday = expand_swap_to_cashflows(swap, self.val_date, extra_holidays)

        self.assertNotEqual(cfs_no_extra[0].pay_t, cfs_with_holiday[0].pay_t)
        pv_a = price_cashflows_python(cfs_no_extra, knots)[("BASE", "SW_TEST", "USD", "USD")]
        pv_b = price_cashflows_python(cfs_with_holiday, knots)[("BASE", "SW_TEST", "USD", "USD")]
        self.assertNotAlmostEqual(pv_a, pv_b, places=2)

    def test_benchmarks_reprice_to_par_in_pure_and_exact_knot_dfs(self) -> None:
        pricing_dir = _find_pricing_dir()
        bm_cfs = []
        for bm in self.benchmarks:
            bm_cfs.extend(expand_swap_to_cashflows(bm, self.val_date, self.base_holidays))

        for method in ("LOG_LINEAR_DF", "LINEAR_ZERO", "LINEAR_DF"):
            knots = fit_zero_curve(self.benchmarks, self.val_date, self.base_holidays, "USD_OIS", method)
            # 1) Exact discount factors at every knot: P(0, t_k) == exp(-r_k * t_k) through Pure
            pure_dfs = run_pure_discount_factors_duckdb(knots, projects_dir=pricing_dir)
            for k in knots:
                self.assertAlmostEqual(
                    pure_dfs[("BASE", "USD_OIS", k.knot_t)],
                    math.exp(-k.zero_rate * k.knot_t),
                    delta=1e-14,
                    msg=f"{method} knot {k.knot_idx}",
                )
            # 2) Benchmark swaps reprice to zero through Pure (not only Python)
            pure_npvs = run_pure_pricing_duckdb(knots, bm_cfs, projects_dir=pricing_dir)
            for bm in self.benchmarks:
                self.assertAlmostEqual(
                    pure_npvs[("BASE", bm.trade_id, "USD", "USD")], 0.0, delta=1e-6, msg=f"{method} {bm.trade_id}"
                )

    def test_front_stub_seasoned_swap_dual_curve_and_refusals(self) -> None:
        pricing_dir = _find_pricing_dir()
        # 1) Non-divisible 18M annual swap generates 6M front stub + 12M period (4 cashflow rows, not truncated to 12M)
        stub_swap = SwapSpec("SW_18M_STUB", "IRS", "USD", "USD_OIS", "USD_OIS", self.val_date, 18, 12, 5_000_000.0, 0.044)
        stub_cfs = expand_swap_to_cashflows(stub_swap, self.val_date, self.base_holidays)
        self.assertEqual(len(stub_cfs), 4)
        self.assertAlmostEqual(stub_cfs[0].dcf, year_fraction(self.val_date, dt.date(2026, 7, 2), "ACT/365F"), places=12)
        self.assertGreater(stub_cfs[-1].pay_t, 1.45)

        # 2) Seasoned swap at a clean coupon boundary (started 12M ago): paid coupons (pay_t <= 0) are excluded;
        #    mid-period seasoned float coupons (t_start < 0 < pay_t) are explicitly refused until OIS fixings PR
        seasoned = SwapSpec(
            "SW_SEASONED", "IRS", "USD", "USD_OIS", "USD_OIS",
            dt.date(2025, 1, 2), 24, 6, 1_000_000.0, 0.045,
        )
        seasoned_cfs = expand_swap_to_cashflows(seasoned, self.val_date, self.base_holidays)
        self.assertTrue(all(cf.pay_t > 0.0 for cf in seasoned_cfs))
        self.assertEqual(len(seasoned_cfs), 4)  # 2 remaining semi-annual coupons x 2 legs

        with self.assertRaises(ValueError):
            expand_swap_to_cashflows(
                SwapSpec("SW_MID", "IRS", "USD", "USD_OIS", "USD_OIS", dt.date(2025, 10, 2), 12, 6, 1_000_000.0, 0.045),
                self.val_date, self.base_holidays,
            )
        with self.assertRaises(ValueError):
            expand_fra_to_cashflows(
                FraSpec("FRA_MID", "USD", "USD_OIS", "USD_OIS", dt.date(2025, 11, 2), dt.date(2026, 2, 2), 1_000_000.0, 0.045),
                self.val_date, self.base_holidays,
            )

        # Also verify Pure ignores a raw past CashflowRow (pay_t <= 0) and empty proj_curve_id="" falls back to discountCurveId
        ois_knots = [
            CurveKnot("BASE", "USD_OIS", 0, 0.5, 0.040, "LOG_LINEAR_DF"),
            CurveKnot("BASE", "USD_OIS", 1, 1.5, 0.045, "LOG_LINEAR_DF"),
        ]
        proj_knots = [
            CurveKnot("BASE", "USD_3M", 0, 0.5, 0.043, "LOG_LINEAR_DF"),
            CurveKnot("BASE", "USD_3M", 1, 1.5, 0.048, "LOG_LINEAR_DF"),
        ]
        blank_proj_cf = [
            CashflowRow("T_BLANK", "FLOAT", 0, "OIS", "USD", "USD", "USD_OIS", "", 0.5, 1.5, 1.5, 1.0, 1_000_000.0, 0.0, 0.0, True, 1.0)
        ]
        self.assertAlmostEqual(
            run_pure_pricing_duckdb(ois_knots, blank_proj_cf, projects_dir=pricing_dir)[("BASE", "T_BLANK", "USD", "USD")],
            price_cashflows_python(blank_proj_cf, ois_knots)[("BASE", "T_BLANK", "USD", "USD")],
            delta=1e-9,
        )

        # 3) Dual-curve trade (USD_OIS discount != USD_3M projection) + CSA collateral discounting segregation
        self.assertEqual(discount_curve_for_csa("USD", "USD"), "USD_OIS")
        self.assertEqual(discount_curve_for_csa("USD", "EUR"), "USD_DISC_EUR")
        dual_cfs = [
            CashflowRow("DUAL_1", "FIXED", 0, "IRS", "USD", "USD", "USD_OIS", "USD_3M", -1.0, -0.5, -0.5, 0.5, 1_000_000.0, 0.045, 0.0, False, -1.0),
            CashflowRow("DUAL_1", "FIXED", 1, "IRS", "USD", "USD", "USD_OIS", "USD_3M", 0.5, 1.5, 1.5, 1.0, 1_000_000.0, 0.045, 0.0, False, -1.0),
            CashflowRow("DUAL_1", "FLOAT", 1, "IRS", "USD", "USD", "USD_OIS", "USD_3M", 0.5, 1.5, 1.5, 1.0, 1_000_000.0, 0.0, 0.0025, True, 1.0),
        ]
        pure_dual = run_pure_pricing_duckdb(ois_knots + proj_knots, dual_cfs, projects_dir=pricing_dir)[("BASE", "DUAL_1", "USD", "USD")]
        # Independent closed-form check at knot times 0.5 and 1.5:
        df_ois_15 = math.exp(-0.045 * 1.5)
        fwd_3m = (math.exp(-0.043 * 0.5) / math.exp(-0.048 * 1.5) - 1.0) / 1.0 + 0.0025
        expected_dual = 1_000_000.0 * 1.0 * (fwd_3m - 0.045) * df_ois_15
        self.assertAlmostEqual(pure_dual, expected_dual, delta=1e-8)

        eur_csa_cfs = expand_swap_to_cashflows(
            SwapSpec("IRS_EUR_CSA", "IRS", "USD", "", "USD_3M", self.val_date, 12, 6, 1_000_000.0, 0.045, csa="EUR"),
            self.val_date, self.base_holidays,
        )
        self.assertEqual({(cf.ccy, cf.csa, cf.discount_curve_id) for cf in eur_csa_cfs}, {("USD", "EUR", "USD_DISC_EUR")})

        # 4) Scenario missing USD_3M projection curve fails loudly in Pure
        with self.assertRaises(Exception):
            run_pure_pricing_duckdb(ois_knots, dual_cfs, projects_dir=pricing_dir)

        # 5) Whole-curve validation: duplicate knot_t, mixed interp_method, or NaN rate at t=2.0 refuses even a clean-segment cashflow at t=0.75!
        clean_seg_cf = [CashflowRow("T1", "FIXED", 0, "IRS", "USD", "USD", "USD_OIS", "USD_OIS", 0.0, 0.75, 0.75, 0.75, 1_000.0, 0.04, 0.0, False, 1.0)]
        for bad_curve in (
            [CurveKnot("BASE", "USD_OIS", 0, 1.0, 0.040, "LOGLINEAR")],
            [
                CurveKnot("BASE", "USD_OIS", 0, 0.5, 0.040, "LOG_LINEAR_DF"),
                CurveKnot("BASE", "USD_OIS", 1, 2.0, 0.045, "LOG_LINEAR_DF"),
                CurveKnot("BASE", "USD_OIS", 2, 2.0, 0.050, "LOG_LINEAR_DF"),
            ],
            [
                CurveKnot("BASE", "USD_OIS", 0, 0.5, 0.040, "LOG_LINEAR_DF"),
                CurveKnot("BASE", "USD_OIS", 1, 1.5, 0.045, "LOG_LINEAR_DF"),
                CurveKnot("BASE", "USD_OIS", 2, 2.5, 0.050, "LINEAR_ZERO"),
            ],
            [
                CurveKnot("BASE", "USD_OIS", 0, 0.5, 0.040, "LOG_LINEAR_DF"),
                CurveKnot("BASE", "USD_OIS", 1, 2.0, float("nan"), "LOG_LINEAR_DF"),
            ],
        ):
            with self.assertRaises(ValueError):
                price_cashflows_python(clean_seg_cf, bad_curve)
            with self.assertRaises(Exception):
                run_pure_pricing_duckdb(bad_curve, clean_seg_cf, projects_dir=pricing_dir)

        for bad_cf in (
            [CashflowRow("T1", "FLOAT", 0, "IRS", "USD", "USD", "USD_OIS", "USD_OIS", 0.0, 1.0, 1.0, 0.0, 1_000.0, 0.0, 0.0, True, 1.0)],
            [CashflowRow("T1", "FLOAT", 0, "IRS", "USD", "USD", "USD_OIS", "USD_OIS", -0.25, 0.25, 0.25, 0.5, 1_000.0, 0.0, 0.0, True, 1.0)],
            [CashflowRow("T1", "FIXED", 0, "IRS", "USD", "USD", "USD_OIS", "USD_OIS", 0.0, 1.0, 1.0, 1.0, float("nan"), 0.04, 0.0, False, 1.0)],
            [CashflowRow("T1", "FIXED", 0, "IRS", "USD", "USD", "USD_OIS", "USD_OIS", 0.0, 1.0, 1.0, 1.0, 1_000.0, 0.04, 0.0, False, 2.0)],
        ):
            with self.assertRaises(ValueError):
                price_cashflows_python(bad_cf, ois_knots)
            with self.assertRaises(Exception):
                run_pure_pricing_duckdb(ois_knots, bad_cf, projects_dir=pricing_dir)

        # 6) NaN / Inf / invalid-tol / max_iter guards in fit_zero_curve (never returns NaN as converged)
        for bad_val in (float("nan"), float("inf"), float("-inf")):
            bms_bad_last = list(self.benchmarks[:-1]) + [dataclasses.replace(self.benchmarks[-1], fixed_rate=bad_val)]
            bms_bad_first = [dataclasses.replace(self.benchmarks[0], fixed_rate=bad_val)] + list(self.benchmarks[1:])
            with self.assertRaises(ValueError):
                fit_zero_curve(bms_bad_last, self.val_date, self.base_holidays, "USD_OIS")
            with self.assertRaises(ValueError):
                fit_zero_curve(bms_bad_first, self.val_date, self.base_holidays, "USD_OIS")
        with self.assertRaises(ValueError):
            fit_zero_curve(self.benchmarks, self.val_date, self.base_holidays, "USD_OIS", tol=0.0)
        with self.assertRaises(ValueError):
            fit_zero_curve(self.benchmarks, self.val_date, self.base_holidays, "USD_OIS", tol=1.0)
        with self.assertRaises(ValueError):
            fit_zero_curve(self.benchmarks, self.val_date, self.base_holidays, "USD_OIS", max_iter=0)
        with self.assertRaises(RuntimeError):
            fit_zero_curve(self.benchmarks, self.val_date, self.base_holidays, max_iter=1, tol=1e-15, curve_id="USD_OIS")
        with self.assertRaises(ValueError):
            expand_fra_to_cashflows(
                FraSpec("BAD_FRA", "USD", "USD_OIS", "USD_OIS", dt.date(2026, 6, 17), dt.date(2026, 3, 18), 1_000_000.0, 0.04),
                self.val_date, self.base_holidays,
            )
        with self.assertRaises(ValueError):
            fit_zero_curve([self.benchmarks[1], self.benchmarks[0]], self.val_date, self.base_holidays, "USD_OIS")

    def test_pure_pricer_duckdb_parity_and_bump_dv01_across_interpolations(self) -> None:
        pricing_dir = _find_pricing_dir()

        cashflows = []
        for i in range(50):
            sw = SwapSpec(
                f"IRS_{i:03d}", "IRS", "USD", "USD_OIS", "USD_OIS",
                self.val_date, 12 * ((i % 5) + 1), 6,
                1_000_000.0 * ((i % 4) + 1),
                0.040 + 0.0002 * (i % 15),
                pay_fixed=(i % 2 == 0),
            )
            cashflows.extend(expand_swap_to_cashflows(sw, self.val_date, self.base_holidays))
        for i in range(30):
            sw = SwapSpec(
                f"OIS_{i:03d}", "OIS", "USD", "USD_OIS", "USD_OIS",
                self.val_date, 12 * ((i % 4) + 1), 12,
                2_000_000.0,
                0.042 + 0.0003 * (i % 10),
                pay_fixed=(i % 2 == 1),
            )
            cashflows.extend(expand_swap_to_cashflows(sw, self.val_date, self.base_holidays))
        for i in range(20):
            s_d = dt.date(2026, 4, 2) + dt.timedelta(days=30 * (i % 6))
            e_d = s_d + dt.timedelta(days=180)
            fra = FraSpec(
                f"FRA_{i:03d}", "USD", "USD_OIS", "USD_OIS",
                s_d, e_d, 5_000_000.0, 0.043 + 0.0002 * i,
                pay_fixed=(i % 2 == 0),
            )
            cashflows.extend(expand_fra_to_cashflows(fra, self.val_date, self.base_holidays))

        for method in ("LOG_LINEAR_DF", "LINEAR_ZERO", "LINEAR_DF"):
            base_knots = fit_zero_curve(self.benchmarks, self.val_date, self.base_holidays, "USD_OIS", method)
            scen_knots = build_curve_scenarios(base_knots, bump_bp=1.0)

            py_npvs = price_cashflows_python(cashflows, scen_knots)
            t0 = time.perf_counter()
            sql_npvs = run_pure_pricing_duckdb(scen_knots, cashflows, projects_dir=pricing_dir)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0

            self.assertEqual(set(py_npvs.keys()), set(sql_npvs.keys()))
            self.assertEqual(len(sql_npvs), 900)

            max_npv_diff = max(abs(py_npvs[k] - sql_npvs[k]) for k in py_npvs)
            self.assertLess(max_npv_diff, 1e-6, f"{method} max NPV diff={max_npv_diff}")

            py_dv01 = compute_bump_dv01_python(py_npvs, base_knots)
            sql_dv01 = compute_bump_dv01_python(sql_npvs, base_knots)
            max_dv01_diff = max(abs(py_dv01[k] - sql_dv01[k]) for k in py_dv01)
            self.assertLess(max_dv01_diff, 1e-7, f"{method} max DV01 diff={max_dv01_diff}")
            print(
                f"[pricing_test] method={method} trades=100 scenarios=9 "
                f"rows={len(sql_npvs)} duckdb_ms={elapsed_ms:.1f} "
                f"max_npv_diff={max_npv_diff:.2e} max_dv01_diff={max_dv01_diff:.2e}"
            )

    def test_imm_calendar_and_hybrid_imm_swap_curve_fit_in_pure(self) -> None:
        pricing_dir = _find_pricing_dir()
        # Exact 2026 quarterly IMM dates (3rd Wednesday of Mar, Jun, Sep, Dec)
        self.assertEqual(
            next_imm_dates(self.val_date, 4),
            [dt.date(2026, 3, 18), dt.date(2026, 6, 17), dt.date(2026, 9, 16), dt.date(2026, 12, 16)],
        )
        # Strictly after valuation_date when standing on an IMM date
        self.assertEqual(imm_date(2026, 3), dt.date(2026, 3, 18))
        self.assertEqual(next_imm_dates(dt.date(2026, 3, 18), 1), [dt.date(2026, 6, 17)])

        ois_knots = fit_zero_curve(self.benchmarks, self.val_date, self.base_holidays, "USD_OIS", "LOG_LINEAR_DF")
        imms = next_imm_dates(self.val_date, 4)
        hybrid_bms = [
            expand_fra_to_cashflows(FraSpec("IMM_FRA_1", "USD", "USD_OIS", "USD_3M", imms[0], imms[1], 5_000_000.0, 0.0435), self.val_date, self.base_holidays),
            expand_fra_to_cashflows(FraSpec("IMM_FRA_2", "USD", "USD_OIS", "USD_3M", imms[1], imms[2], 5_000_000.0, 0.0445), self.val_date, self.base_holidays),
            expand_fra_to_cashflows(FraSpec("IMM_FRA_3", "USD", "USD_OIS", "USD_3M", imms[2], imms[3], 5_000_000.0, 0.0455), self.val_date, self.base_holidays),
            expand_swap_to_cashflows(SwapSpec("IRS_3M_2Y", "IRS", "USD", "USD_OIS", "USD_3M", self.val_date, 24, 6, 5_000_000.0, 0.0475), self.val_date, self.base_holidays),
            expand_swap_to_cashflows(SwapSpec("IRS_3M_5Y", "IRS", "USD", "USD_OIS", "USD_3M", self.val_date, 60, 6, 5_000_000.0, 0.0505), self.val_date, self.base_holidays),
        ]
        usd_3m_knots = fit_curve_from_cashflows(hybrid_bms, "USD_3M", "LOG_LINEAR_DF", fixed_knots=ois_knots, initial_rate=0.045)
        all_cfs = [cf for bm in hybrid_bms for cf in bm]
        pure_npvs = run_pure_pricing_duckdb(ois_knots + usd_3m_knots, all_cfs, projects_dir=pricing_dir)
        for bm in hybrid_bms:
            self.assertAlmostEqual(pure_npvs[("BASE", bm[0].trade_id, "USD", "USD")], 0.0, delta=1e-5)

    def test_xccy_basis_swap_negative_rates_notional_exchanges_and_csa_labels(self) -> None:
        pricing_dir = _find_pricing_dir()
        usd_ois = fit_zero_curve(self.benchmarks, self.val_date, self.base_holidays, "USD_OIS", "LOG_LINEAR_DF")
        eur_bms = [
            SwapSpec("EUR_6M", "OIS", "EUR", "EUR_OIS", "EUR_OIS", self.val_date, 6, 6, 1_000_000.0, -0.015),
            SwapSpec("EUR_2Y", "OIS", "EUR", "EUR_OIS", "EUR_OIS", self.val_date, 24, 6, 1_000_000.0, -0.008),
            SwapSpec("EUR_5Y", "OIS", "EUR", "EUR_OIS", "EUR_OIS", self.val_date, 60, 12, 1_000_000.0, 0.002),
        ]
        eur_ois = fit_zero_curve(eur_bms, self.val_date, self.base_holidays, "EUR_OIS", "LOG_LINEAR_DF")

        spot = 1.10
        # Blank leg discount curves auto-resolve from CSA ('USD' -> EUR_DISC_USD / USD_OIS)
        xccy_bms = [
            expand_xccy_basis_swap_to_cashflows(
                XccyBasisSwapSpec(f"XCCY_{m}M", "EUR", "USD", "", "EUR_OIS", "", "USD_OIS", self.val_date, m, 6, 1_000_000.0, spot, sp, csa="USD"),
                self.val_date, self.base_holidays,
            )
            for m, sp in [(12, -0.0012), (24, -0.0015), (60, -0.0018)]
        ]
        eur_disc_usd = fit_curve_from_cashflows(
            xccy_bms, "EUR_DISC_USD", "LOG_LINEAR_DF", fixed_knots=usd_ois + eur_ois, initial_rate=-0.01
        )
        all_knots = usd_ois + eur_ois + eur_disc_usd

        # 1) All XCCY basis benchmarks reprice to par (0.0) through Pure
        bm_flat = [cf for bm in xccy_bms for cf in bm]
        pure_bm = run_pure_pricing_duckdb(all_knots, bm_flat, projects_dir=pricing_dir)
        for bm in xccy_bms:
            self.assertAlmostEqual(pure_bm[("BASE", bm[0].trade_id, "USD", "USD")], 0.0, delta=1e-5)

        # 2) Forward-starting XCCY swap includes initial (t0 > 0) & final (t_mat > 0) notional exchanges,
        #    and reporting-currency vs native-currency modes agree under standard (scen, trade, ccy, csa) grain
        fwd_spec = XccyBasisSwapSpec(
            "XCCY_FWD", "EUR", "USD", "", "EUR_OIS", "", "USD_OIS",
            dt.date(2026, 4, 2), 24, 6, 2_000_000.0, spot, -0.0010, csa="USD",
        )
        rep_cfs = expand_xccy_basis_swap_to_cashflows(fwd_spec, self.val_date, self.base_holidays, in_reporting_ccy=True)
        nat_cfs = expand_xccy_basis_swap_to_cashflows(fwd_spec, self.val_date, self.base_holidays, in_reporting_ccy=False)
        self.assertEqual({(cf.ccy, cf.csa) for cf in rep_cfs}, {("USD", "USD")})
        self.assertEqual({(cf.ccy, cf.csa) for cf in nat_cfs}, {("EUR", "USD"), ("USD", "USD")})
        self.assertEqual(len(rep_cfs), 12)

        rep_npvs = run_pure_pricing_duckdb(all_knots, rep_cfs, projects_dir=pricing_dir)
        nat_npvs = run_pure_pricing_duckdb(all_knots, nat_cfs, projects_dir=pricing_dir)
        nat_py = price_cashflows_python(nat_cfs, all_knots)
        self.assertEqual(set(rep_npvs.keys()), {("BASE", "XCCY_FWD", "USD", "USD")})
        self.assertEqual(set(nat_npvs.keys()), {("BASE", "XCCY_FWD", "EUR", "USD"), ("BASE", "XCCY_FWD", "USD", "USD")})
        for k in nat_npvs:
            self.assertAlmostEqual(nat_npvs[k], nat_py[k], delta=1e-8)
        combined_usd = spot * nat_npvs[("BASE", "XCCY_FWD", "EUR", "USD")] + nat_npvs[("BASE", "XCCY_FWD", "USD", "USD")]
        self.assertAlmostEqual(rep_npvs[("BASE", "XCCY_FWD", "USD", "USD")], combined_usd, delta=1e-8)

        # Verify EUR-CSA XCCY basis swap auto-resolves leg1 -> EUR_OIS and leg2 -> USD_DISC_EUR
        eur_csa_x = expand_xccy_basis_swap_to_cashflows(
            XccyBasisSwapSpec("XCCY_EUR_CSA", "EUR", "USD", "", "EUR_OIS", "", "USD_OIS", self.val_date, 24, 6, 1_000_000.0, spot, -0.0015, csa="EUR"),
            self.val_date, self.base_holidays,
        )
        self.assertEqual({cf.discount_curve_id for cf in eur_csa_x if cf.leg_id.startswith("EUR")}, {"EUR_OIS"})
        self.assertEqual({cf.discount_curve_id for cf in eur_csa_x if cf.leg_id.startswith("USD")}, {"USD_DISC_EUR"})

        # 3) Seasoned XCCY swap started 12M ago drops past initial notional exchange (t0 < 0) and paid coupons
        seasoned_xccy = XccyBasisSwapSpec(
            "XCCY_SEAS", "EUR", "USD", "EUR_DISC_USD", "EUR_OIS", "USD_OIS", "USD_OIS",
            dt.date(2025, 1, 2), 24, 6, 1_000_000.0, spot, -0.0015,
        )
        seas_cfs = expand_xccy_basis_swap_to_cashflows(seasoned_xccy, self.val_date, self.base_holidays)
        self.assertTrue(all(cf.pay_t > 0.0 for cf in seas_cfs))
        nx_rows = [cf for cf in seas_cfs if cf.leg_id.endswith("_NX")]
        self.assertEqual(len(nx_rows), 2)

        # 4) Independent closed-form telescoping identity (no calibration): when each leg discounts on its own
        #    projection curve and spread == 0, -N*P(0,t0) + sum N*(P(0,t_{i-1}) - P(0,t_i)) + N*P(0,t_n) == 0 on BOTH legs!
        tele_spec = XccyBasisSwapSpec(
            "XCCY_TELE", "EUR", "USD", "EUR_OIS", "EUR_OIS", "USD_OIS", "USD_OIS",
            dt.date(2026, 4, 2), 18, 12, 3_000_000.0, spot, 0.0,
        )
        tele_nat_cfs = expand_xccy_basis_swap_to_cashflows(tele_spec, self.val_date, self.base_holidays, in_reporting_ccy=False)
        tele_npvs = run_pure_pricing_duckdb(usd_ois + eur_ois, tele_nat_cfs, projects_dir=pricing_dir)
        self.assertAlmostEqual(tele_npvs[("BASE", "XCCY_TELE", "EUR", "USD")], 0.0, delta=1e-9)
        self.assertAlmostEqual(tele_npvs[("BASE", "XCCY_TELE", "USD", "USD")], 0.0, delta=1e-9)

        # 5) Curve fitter & XCCY refusals: mid-period float coupon, NaN/Inf inputs, max_iter=0, and non-convergence
        with self.assertRaises(ValueError):
            expand_xccy_basis_swap_to_cashflows(
                XccyBasisSwapSpec(
                    "XCCY_MID", "EUR", "USD", "EUR_DISC_USD", "EUR_OIS", "USD_OIS", "USD_OIS",
                    dt.date(2025, 9, 2), 24, 6, 1_000_000.0, spot, -0.0015,
                ),
                self.val_date, self.base_holidays,
            )
        with self.assertRaises(ValueError):
            fit_curve_from_cashflows([nat_cfs], "EUR_DISC_USD", fixed_knots=usd_ois + eur_ois)
        with self.assertRaises(ValueError):
            fit_curve_from_cashflows(xccy_bms, "EUR_DISC_USD", fixed_knots=usd_ois + eur_ois, max_iter=0)
        with self.assertRaises(ValueError):
            fit_curve_from_cashflows(xccy_bms, "EUR_DISC_USD", fixed_knots=usd_ois + eur_ois, tol=0.0)
        for bad_val in (float("nan"), float("inf"), float("-inf")):
            with self.assertRaises(ValueError):
                fit_curve_from_cashflows(xccy_bms, "EUR_DISC_USD", fixed_knots=usd_ois + eur_ois, initial_rate=bad_val)
            bad_bms = list(xccy_bms[:-1]) + [[dataclasses.replace(xccy_bms[-1][0], spread=bad_val)] + list(xccy_bms[-1][1:])]
            with self.assertRaises(ValueError):
                fit_curve_from_cashflows(bad_bms, "EUR_DISC_USD", fixed_knots=usd_ois + eur_ois)
        with self.assertRaises(RuntimeError):
            fit_curve_from_cashflows(xccy_bms, "EUR_DISC_USD", fixed_knots=usd_ois + eur_ois, initial_rate=0.02, max_iter=1, tol=1e-15)


if __name__ == "__main__":
    unittest.main()
