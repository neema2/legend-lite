"""Parity, independent analytical, and SOD-expansion tests for python/pricing + projects/pricing."""

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
    build_curve_scenarios,
    compute_bump_dv01_python,
    discount_curve_for_csa,
    expand_fra_to_cashflows,
    expand_swap_to_cashflows,
    fit_zero_curve,
    price_cashflows_python,
    run_pure_discount_factors_duckdb,
    run_pure_pricing_duckdb,
    year_fraction,
)


def _find_pricing_dir() -> Path:
    env_files = os.environ.get("PRICING_PROJECT_FILES")
    if env_files:
        try:
            from python.runfiles import runfiles

            rf = runfiles.Create()
            if rf is not None:
                for rel in env_files.split():
                    if rel.endswith("/model.pure"):
                        resolved = rf.Rlocation(rel, source_repo="")
                        if resolved and Path(resolved).is_file():
                            return Path(resolved).parent
        except ImportError:
            pass

    here = Path(__file__).resolve()
    for parent in here.parents:
        cand = parent / "projects" / "pricing"
        if (cand / "model.pure").is_file():
            return cand
    raise FileNotFoundError("Cannot locate projects/pricing")


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

        # 2) Seasoned swap started 12M ago: paid coupons (pay_t <= 0) are excluded; remaining 12M coupons are kept
        seasoned = SwapSpec(
            "SW_SEASONED", "IRS", "USD", "USD_OIS", "USD_OIS",
            dt.date(2025, 1, 2), 24, 6, 1_000_000.0, 0.045,
        )
        seasoned_cfs = expand_swap_to_cashflows(seasoned, self.val_date, self.base_holidays)
        self.assertTrue(all(cf.pay_t > 0.0 for cf in seasoned_cfs))
        self.assertEqual(len(seasoned_cfs), 4)  # 2 remaining semi-annual coupons x 2 legs

        # Also verify Pure ignores a raw past CashflowRow (pay_t <= 0)
        ois_knots = [
            CurveKnot("BASE", "USD_OIS", 0, 0.5, 0.040, "LOG_LINEAR_DF"),
            CurveKnot("BASE", "USD_OIS", 1, 1.5, 0.045, "LOG_LINEAR_DF"),
        ]
        proj_knots = [
            CurveKnot("BASE", "USD_3M", 0, 0.5, 0.043, "LOG_LINEAR_DF"),
            CurveKnot("BASE", "USD_3M", 1, 1.5, 0.048, "LOG_LINEAR_DF"),
        ]
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

        # 5) Unknown interpolation method, duplicate knot_t, mixed interp_method, and non-positive dcf fail loudly in Pure
        bad_knots = [CurveKnot("BASE", "USD_OIS", 0, 1.0, 0.040, "LOGLINEAR")]
        simple_cf = [CashflowRow("T1", "FIXED", 0, "IRS", "USD", "USD", "USD_OIS", "USD_OIS", 0.0, 1.0, 1.0, 1.0, 1_000.0, 0.04, 0.0, False, 1.0)]
        with self.assertRaises(Exception):
            run_pure_pricing_duckdb(bad_knots, simple_cf, projects_dir=pricing_dir)

        dup_knots = [
            CurveKnot("BASE", "USD_OIS", 0, 1.0, 0.040, "LOG_LINEAR_DF"),
            CurveKnot("BASE", "USD_OIS", 1, 1.0, 0.045, "LOG_LINEAR_DF"),
        ]
        with self.assertRaises(Exception):
            run_pure_pricing_duckdb(dup_knots, simple_cf, projects_dir=pricing_dir)

        mixed_knots = [
            CurveKnot("BASE", "USD_OIS", 0, 0.5, 0.040, "LOG_LINEAR_DF"),
            CurveKnot("BASE", "USD_OIS", 1, 1.5, 0.045, "LINEAR_ZERO"),
        ]
        with self.assertRaises(Exception):
            run_pure_pricing_duckdb(mixed_knots, simple_cf, projects_dir=pricing_dir)

        bad_dcf_cf = [CashflowRow("T1", "FLOAT", 0, "IRS", "USD", "USD", "USD_OIS", "USD_OIS", 0.0, 1.0, 1.0, 0.0, 1_000.0, 0.0, 0.0, True, 1.0)]
        with self.assertRaises(Exception):
            run_pure_pricing_duckdb(ois_knots, bad_dcf_cf, projects_dir=pricing_dir)

        # 6) Python schedule & calibration guards: inverted FRA dates, non-increasing pillars, and unconverged max_iter
        with self.assertRaises(ValueError):
            expand_fra_to_cashflows(
                FraSpec("BAD_FRA", "USD", "USD_OIS", "USD_OIS", dt.date(2026, 6, 17), dt.date(2026, 3, 18), 1_000_000.0, 0.04),
                self.val_date, self.base_holidays,
            )
        with self.assertRaises(ValueError):
            fit_zero_curve([self.benchmarks[1], self.benchmarks[0]], self.val_date, self.base_holidays, "USD_OIS")
        with self.assertRaises(RuntimeError):
            fit_zero_curve(self.benchmarks, self.val_date, self.base_holidays, "USD_OIS", max_iter=0)

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


if __name__ == "__main__":
    unittest.main()
