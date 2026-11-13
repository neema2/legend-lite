"""python/pricing on legend_lite.scalar_parallel: fitted discount and projection zero curves, single-curve
(``IRSwapFixedFloatApprox``) and two-curve explicit-cashflow (``IRSwapFixedFloat``) swap portfolios, shared risk
definitions (``FirstOrderAnalyticRisk`` vs ``FirstOrderNumericalRisk``) and execution engines (``PythonEngineExpr`` vs
``PureBasisEngine`` on DuckDB)."""

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import legend_lite as ll  # noqa: E402
from legend_lite.scalar_parallel import basis, basis_pure as pure, expr  # noqa: E402
from legend_lite.page import Grid  # noqa: E402
from pricing import risk  # noqa: E402
from pricing.demo import irs_pricing as demo, irs_scaling  # noqa: E402
from pricing.demo.dashboard import irs_frames, irs_page  # noqa: E402
from pricing.demo.irs_workbench import IRSWorkbench  # noqa: E402
from pricing.engines import DuckDBBasisEngine, PureBasisEngine, PythonEngineExpr, resolve_engine  # noqa: E402
from pricing.instruments import (  # noqa: E402
    EquityOption,
    IRSwapFixedFloat,
    IRSwapFixedFloatApprox,
    IRSwaption,
    Portfolio,
)
from pricing.marketmodels import (  # noqa: E402
    CurveFitter,
    MultiCurveFitter,
    SwaptionVolSurface,
    ZeroCurve,
    inverse,
)
from pricing.risk import FirstOrderAnalyticRisk, FirstOrderNumericalRisk  # noqa: E402


class SingleAndMultiCurveFitting(unittest.TestCase):
    def setUp(self):
        self.disc_curve = ZeroCurve(demo.DISC_CURVE, demo.PILLARS)
        self.proj_curve = ZeroCurve(demo.PROJ_CURVE, demo.PILLARS)
        self.single_fitter = CurveFitter(self.disc_curve, demo.OIS_QUOTES)
        self.single_fit = self.single_fitter.fit()
        self.multi_fitter = MultiCurveFitter(self.disc_curve, demo.OIS_QUOTES, self.proj_curve, demo.IRS_QUOTES)
        self.multi_fit = self.multi_fitter.fit()

    def test_the_calibration_swaps_reprice_at_their_quotes(self):
        env = self.multi_fit.env([self.disc_curve, self.proj_curve])
        for q_spec in self.multi_fitter.quote_specs:
            self.assertAlmostEqual(expr.evaluate(q_spec.par_expr, env), q_spec.quote, places=14)
        self.assertLessEqual(self.multi_fit.iterations, 8)

    def test_pure_basis_curve_fitter_matches_python_fitter_on_single_and_multi_curve(self):
        from pricing.marketmodels import DEFAULT_NEWTON_STEPS, Pillar, newton_step_pure

        # Single curve calibration in 2 hardwired SQL Newton iterations + SQL Jacobian inversion
        py_single_2 = self.single_fitter.fit(steps=DEFAULT_NEWTON_STEPS)
        with self.single_fitter.sql_fitter() as sql_single:
            sql_fit = sql_single.fit_quotes(self.single_fitter.quotes)
            self.assertEqual(sql_fit.iterations, DEFAULT_NEWTON_STEPS)
            for r_py2, r_py, r_sql in zip(py_single_2.rates, self.single_fit.rates, sql_fit.rates):
                self.assertAlmostEqual(r_py2, r_sql, delta=1e-14)
                self.assertAlmostEqual(r_py, r_sql, delta=1e-12)
            for row_py2, row_py, row_sql in zip(py_single_2.dr_dq, self.single_fit.dr_dq, sql_fit.dr_dq):
                for v_py2, val_py, val_sql in zip(row_py2, row_py, row_sql):
                    self.assertAlmostEqual(v_py2, val_sql, delta=1e-13)
                    self.assertAlmostEqual(val_py, val_sql, delta=1e-11)

            # Verify Pure-native diff() emission option as well as multi-dialect SQL generation
            self.assertIn('diff(', newton_step_pure(self.single_fitter, use_pure_diff=True))
            snow_sql = sql_single.plan_sql('Snowflake')
            self.assertIn('SELECT', snow_sql.upper())

        # Multi-curve simultaneous calibration + multi-scenario vectorized SQL fit
        py_multi_2 = self.multi_fitter.fit(steps=DEFAULT_NEWTON_STEPS)
        bumped_q = [q + risk.BP for q in self.multi_fitter.quotes]
        py_bumped_2 = self.multi_fitter.fit_quotes(bumped_q, steps=DEFAULT_NEWTON_STEPS)
        with self.multi_fitter.sql_fitter() as sql_multi:
            sql_mfit = sql_multi.fit_quotes(self.multi_fitter.quotes)
            self.assertEqual(sql_mfit.iterations, DEFAULT_NEWTON_STEPS)
            for r_py2, r_py, r_sql in zip(py_multi_2.rates, self.multi_fit.rates, sql_mfit.rates):
                self.assertAlmostEqual(r_py2, r_sql, delta=1e-14)
                self.assertAlmostEqual(r_py, r_sql, delta=1e-12)
            for row_py2, row_py, row_sql in zip(py_multi_2.dr_dq, self.multi_fit.dr_dq, sql_mfit.dr_dq):
                for v_py2, val_py, val_sql in zip(row_py2, row_py, row_sql):
                    self.assertAlmostEqual(v_py2, val_sql, delta=1e-13)
                    self.assertAlmostEqual(val_py, val_sql, delta=1e-11)

            scen_fits = sql_multi.fit_scenarios_sql(
                {'BASE': self.multi_fitter.quotes, 'UP_1BP': bumped_q},
                compute_jacobian=True,
            )
            for r_exp, r_got in zip(py_bumped_2.rates, scen_fits['UP_1BP'].rates):
                self.assertAlmostEqual(r_exp, r_got, delta=1e-14)

        # Non-triangular Jacobian fallback (sparse Newton-Schulz refinement) when pillars have coupled coupons
        coupled_curve = ZeroCurve('COUPLED', [Pillar('1Y', 1.0), Pillar('1.5Y', 1.5), Pillar('3Y', 3.0)])
        coupled_fitter = CurveFitter(coupled_curve, [0.030, 0.032, 0.035])
        py_coupled = coupled_fitter.fit(steps=2)
        with coupled_fitter.sql_fitter(steps=2) as sql_coupled:
            got_coupled = sql_coupled.fit()
            for r_exp, r_got in zip(py_coupled.rates, got_coupled.rates):
                self.assertAlmostEqual(r_exp, r_got, delta=1e-13)
            for row_exp, row_got in zip(py_coupled.dr_dq, got_coupled.dr_dq):
                for v_exp, v_got in zip(row_exp, row_got):
                    self.assertAlmostEqual(v_exp, v_got, delta=1e-12)

    def test_skinny_sql_fitter_scales_to_200_coupled_parameters_and_halley_adjoint(self):
        """Verifies that the skinny relational Pure->SQL curve fitter scales cleanly to 200 coupled parameters
        (100 OIS pillars + 100 2-curve IRS pillars up to 25Y), accelerates +200 bp stress shocks via Step-2
        Halley curvature reuse, supports warm-tick seeding, and computes exact vector-only adjoint par-quote DV01."""
        from pricing.marketmodels import Pillar

        n_per_curve = 100
        tenors = [0.25 * (i + 1) for i in range(n_per_curve)]
        pillars = [Pillar(f'T{i + 1:03d}', t) for i, t in enumerate(tenors)]
        disc200 = ZeroCurve('USD-OIS-100', pillars)
        proj200 = ZeroCurve('USD-3M-100', pillars)
        ois_q = [0.030 + 0.008 * (1.0 - 1.0 / (1.0 + 0.25 * t)) for t in tenors]
        irs_q = [q + 0.0015 + 0.0005 * (t / 25.0) for q, t in zip(ois_q, tenors)]

        mf200 = MultiCurveFitter(disc200, ois_q, proj200, irs_q)
        self.assertEqual(len(mf200.factors), 200)
        py2 = mf200.fit(steps=2)

        with mf200.sql_fitter(steps=2) as sf200:
            # 1. Cold 2-step SQL fit + 200x200 coupled SQL inverse Jacobian
            sql2 = sf200.fit()
            for r_py, r_sql in zip(py2.rates, sql2.rates):
                self.assertAlmostEqual(r_py, r_sql, delta=1e-13)
            max_dw = max(
                abs(v_py - v_sql)
                for row_py, row_sql in zip(py2.dr_dq, sql2.dr_dq)
                for v_py, v_sql in zip(row_py, row_sql)
            )
            self.assertLess(max_dw, 1e-10)

            # 2. Warm-start tick (+1 bp parallel shift seeded from previous fit)
            tick_q = [q + risk.BP for q in mf200.quotes]
            py_warm = mf200.fit_quotes(tick_q, steps=2, initial_rates=sql2.rates, compute_jacobian=False)
            sql_warm = sf200.fit_quotes(tick_q, compute_jacobian=False, warm_start=True)
            for r_py, r_sql in zip(py_warm.rates, sql_warm.rates):
                self.assertAlmostEqual(r_py, r_sql, delta=1e-13)

            # 3. +200 bp stress shock from pre-shock curve: Step-2 Halley curvature reuse vs plain 2-step Newton
            shock_q = [q + 0.0200 for q in mf200.quotes]
            py_conv = mf200.fit_quotes(shock_q, compute_jacobian=False)
            py_halley = mf200.fit_quotes(
                shock_q, steps=2, initial_rates=sql2.rates, compute_jacobian=False, halley=True
            )
            sql_newton = sf200.fit_quotes(
                shock_q, initial_rates=sql2.rates, compute_jacobian=False, halley=False
            )
            sql_halley = sf200.fit_quotes(
                shock_q, initial_rates=sql2.rates, compute_jacobian=False, halley=True
            )
            for r_py, r_sql in zip(py_halley.rates, sql_halley.rates):
                self.assertAlmostEqual(r_py, r_sql, delta=1e-12)
            err_newton = max(abs(a - b) for a, b in zip(sql_newton.rates, py_conv.rates))
            err_halley = max(abs(a - b) for a, b in zip(sql_halley.rates, py_conv.rates))
            self.assertLess(err_halley, 5e-8)
            self.assertLess(err_halley * 1.8, err_newton)

            # 4. Vector-only adjoint par-quote DV01 without forming explicit 200x200 W_2
            s_a = IRSwapFixedFloat(
                symbol='SW200_A', book='BK_A', notional=10e6, fixed_rate=0.036,
                tenor_years=20.0, start=0.5, discount_curve=disc200, projection_curve=proj200,
            )
            s_b = IRSwapFixedFloat(
                symbol='SW200_B', book='BK_B', notional=25e6, fixed_rate=0.038,
                tenor_years=24.0, start=1.0, side='PAYER', discount_curve=disc200, projection_curve=proj200,
            )
            env2 = sql2.env([disc200, proj200])
            factors200 = mf200.factors
            quote_specs200 = mf200.quote_specs
            book_deltas: dict[tuple[str, str], float] = {}
            for sw in (s_a, s_b):
                lad = FirstOrderAnalyticRisk(sw, curves=[disc200, proj200], factors=factors200).risk_ladder()
                for f in factors200:
                    d_val = expr.evaluate(lad[f.factor_id], env2) * risk.BP
                    if abs(d_val) > 0.0:
                        book_deltas[(sw.book, f.factor_id)] = d_val

            fit_adj, adj_qrisk = sf200.fit_and_adjoint_risk_sql(mf200.quotes, book_deltas)
            for r_a, r_b in zip(sql2.rates, fit_adj.rates):
                self.assertAlmostEqual(r_a, r_b, delta=1e-14)
            for bk in ('BK_A', 'BK_B'):
                d_vec = [book_deltas.get((bk, f.factor_id), 0.0) for f in factors200]
                for j, qs in enumerate(quote_specs200):
                    ref_q = sum(d_vec[k] * sql2.dr_dq[k][j] for k in range(len(factors200)))
                    self.assertAlmostEqual(adj_qrisk[(bk, qs.quote_id)], ref_q, delta=1e-7)

    def test_d_pillar_d_quote_inverts_d_par_d_pillar(self):
        env = self.multi_fit.env([self.disc_curve, self.proj_curve])
        jac = [[expr.evaluate(d, env) for d in row] for row in self.multi_fitter.jacobian]
        back = inverse(self.multi_fit.dr_dq)
        for i, row in enumerate(jac):
            for j, v in enumerate(row):
                self.assertAlmostEqual(back[i][j], v, places=10)

    def test_a_par_swap_is_risky_to_its_own_quote_alone_across_both_curves(self):
        env = self.multi_fit.env([self.disc_curve, self.proj_curve])
        factors = self.multi_fitter.factors
        quotes = self.multi_fitter.quote_specs
        n_f = len(factors)
        n_q = len(quotes)
        cal_swaps = demo.par_swaps(self.disc_curve, demo.OIS_QUOTES, self.proj_curve, demo.IRS_QUOTES)
        for j, s in enumerate(cal_swaps):
            analytic = FirstOrderAnalyticRisk(s, curves=[self.disc_curve, self.proj_curve], factors=factors)
            ladder = analytic.risk_ladder()
            delta = [expr.evaluate(ladder[f.factor_id], env) for f in factors]
            by_quote = [sum(delta[k] * self.multi_fit.dr_dq[k][i] for k in range(n_f)) for i in range(n_q)]
            for i, x in enumerate(by_quote):
                if i == j:
                    annuity = expr.evaluate(s.annuity(self.disc_curve), env)
                    self.assertAlmostEqual(x, -s.notional * annuity, delta=1e-4)
                else:
                    self.assertAlmostEqual(x, 0.0, delta=1e-4)

    def test_explicit_float_leg_telescopes_in_the_basis_when_curves_coincide(self):
        approx = IRSwapFixedFloatApprox(
            symbol='S_APPROX', notional=10e6, fixed_rate=0.035, tenor_years=5.0, start=0.25, curve=self.disc_curve
        )
        explicit = IRSwapFixedFloat(
            symbol='S_EXPLICIT', notional=10e6, fixed_rate=0.035, tenor_years=5.0, start=0.25,
            discount_curve=self.disc_curve, projection_curve=self.disc_curve
        )
        # On a single curve, 1/D(t_i) * D(t_i) folds to 1 in the basis and intermediate coupons cancel identically
        self.assertEqual(basis.expand(approx.npv()), basis.expand(explicit.npv()))

    def test_single_and_two_curve_swap_npvs_and_deltas_are_in_the_basis(self):
        s1 = IRSwapFixedFloatApprox(
            symbol='T1', book='B', notional=1e6, fixed_rate=0.03, tenor_years=5.0, start=0.25, curve=self.disc_curve
        )
        book = pure.BasisBook()
        poly1 = book.add('T1', 'B', risk.NPV, '*', s1.npv(self.disc_curve))
        self.assertEqual(len(poly1), 5 + 1)

        s2 = IRSwapFixedFloat(
            symbol='T2', book='B', notional=1e6, fixed_rate=0.035, float_spread=0.001,
            tenor_years=5.0, start=0.25, discount_curve=self.disc_curve, projection_curve=self.proj_curve
        )
        analytic = FirstOrderAnalyticRisk([s1, s2], curves=[self.disc_curve, self.proj_curve])
        analytic.populate_book(book)
        self.assertGreater(book.term_count(), 10)


class EnginesAndRiskMethods(unittest.TestCase):
    """Tests ``PythonEngineExpr`` vs ``PureBasisEngine`` and ``FirstOrderAnalyticRisk`` vs ``FirstOrderNumericalRisk``."""

    @classmethod
    def setUpClass(cls):
        cls.disc_curve = ZeroCurve(demo.DISC_CURVE, demo.PILLARS)
        cls.proj_curve = ZeroCurve(demo.PROJ_CURVE, demo.PILLARS)
        cls.curves = [cls.disc_curve, cls.proj_curve]
        cls.fitter = MultiCurveFitter(cls.disc_curve, demo.OIS_QUOTES, cls.proj_curve, demo.IRS_QUOTES)
        cls.fit = cls.fitter.fit()
        cls.trades = demo.portfolio(12, cls.disc_curve, cls.proj_curve)
        cls.portfolio = Portfolio(cls.trades, cls.curves)
        cls.cb = pure.ComputeBasis()
        cls.py_engine = PythonEngineExpr()
        cls.pure_engine = PureBasisEngine(cls.cb)

    @classmethod
    def tearDownClass(cls):
        cls.cb.close()

    def test_instruments_delegate_analytic_risk_to_shared_risk_calculator(self):
        for t in self.trades:
            self.assertFalse(hasattr(t, 'risk_ladder'))
        calc = FirstOrderAnalyticRisk(self.portfolio, curves=self.curves, factors=self.fitter.factors)
        inst_risk = calc.instrument_risk(scale=risk.BP)
        self.assertEqual(set(inst_risk), {t.trade_id for t in self.trades})

    def test_python_engine_and_pure_basis_engine_agree_on_npvs_and_analytic_risk(self):
        py_npvs = self.py_engine.npvs(self.portfolio, self.curves, self.fit)
        sql_npvs = self.pure_engine.npvs(self.portfolio, self.curves, self.fit)
        for key, py_val in py_npvs.items():
            self.assertAlmostEqual(sql_npvs[key], py_val, delta=1e-7)

        py_deltas = self.py_engine.pillar_risk(self.portfolio, self.curves, self.fit, self.fitter.factors)
        sql_deltas = self.pure_engine.pillar_risk(self.portfolio, self.curves, self.fit, self.fitter.factors)
        for key, py_val in py_deltas.items():
            self.assertAlmostEqual(sql_deltas[key], py_val, delta=1e-7)

        py_q = self.py_engine.quote_risk(self.portfolio, self.fitter, self.fit, risk_method=FirstOrderAnalyticRisk)
        sql_q = self.pure_engine.quote_risk(self.portfolio, self.fitter, self.fit, risk_method=FirstOrderAnalyticRisk)
        for key, py_val in py_q.items():
            self.assertAlmostEqual(sql_q[key], py_val, delta=1e-6)

    def test_naive_bump_and_grind_refit_reprice_matches_analytic_chain_rule_on_both_engines(self):
        analytic_q = self.py_engine.quote_risk(
            self.portfolio, self.fitter, self.fit, risk_method=FirstOrderAnalyticRisk
        )
        # Central-difference naive bump, curve refit & reprice in Python
        bg_central_py = self.py_engine.quote_risk(
            self.portfolio, self.fitter, self.fit, risk_method=FirstOrderNumericalRisk, method='central'
        )
        # Forward-difference naive bump, curve refit & reprice in Pure/DuckDB SQL
        bg_fwd_sql = self.pure_engine.quote_risk(
            self.portfolio, self.fitter, self.fit, risk_method=FirstOrderNumericalRisk, method='forward'
        )
        bg_fwd_py = self.py_engine.quote_risk(
            self.portfolio, self.fitter, self.fit, risk_method=FirstOrderNumericalRisk, method='forward'
        )
        for key, a_val in analytic_q.items():
            # Python and SQL engines agree to machine precision on the refitted quote-bump scenarios
            self.assertAlmostEqual(bg_fwd_sql[key], bg_fwd_py[key], delta=1e-6)
            # Central difference bump-and-refit matches exact analytic Jacobian chain rule within < 0.05 $/bp
            self.assertAlmostEqual(bg_central_py[key], a_val, delta=max(0.05, 1e-5 * abs(a_val)))
            # Forward 1bp bump-and-refit matches analytic within 1bp convexity (~0.5% of DV01)
            self.assertAlmostEqual(bg_fwd_sql[key], a_val, delta=max(10.0, 5e-3 * abs(a_val)))

    def test_pure_basis_engine_exposes_pure_queries_and_multi_dialect_sql(self):
        self.assertIsInstance(resolve_engine('python'), PythonEngineExpr)
        self.assertIsInstance(resolve_engine('pure_duckdb'), PureBasisEngine)
        self.assertIs(DuckDBBasisEngine, PureBasisEngine)
        queries = self.pure_engine.pure_queries('BASE')
        self.assertEqual(set(queries), {'measures_by_trade', 'measures_by_book', 'quote_risk'})
        sql_duckdb = self.pure_engine.plan_sql('duckdb')
        for q_name, sql in sql_duckdb.items():
            self.assertIn('SELECT', sql.upper(), msg=q_name)
        by_db = self.pure_engine.plan_all_dialects()
        self.assertEqual(set(by_db), set(pure.DATABASES))

    def test_equity_option_black_scholes_npv_and_analytic_risk_in_python_and_sql(self):
        """Prices and differentiates European Call and Put EquityOptions (Black-Scholes-Merton with continuous
        dividend curve) in both PythonEngineExpr and PureBasisEngine (dynamic Pure -> DuckDB SQL), and verifies
        put-call parity and exact SQL vs Python agreement."""
        call_opt = EquityOption(
            symbol='AAPL_CALL_2Y5',
            book='EQ_OPT',
            is_call=True,
            spot_price=185.0,
            strike_price=190.0,
            tenor_years=2.5,
            volatility=0.24,
            notional_shares=10_000.0,
            discount_curve=self.disc_curve,
            dividend_curve=self.proj_curve,
        )
        put_opt = EquityOption(
            symbol='AAPL_PUT_2Y5',
            book='EQ_OPT',
            is_call=False,
            spot_price=185.0,
            strike_price=190.0,
            tenor_years=2.5,
            volatility=0.24,
            notional_shares=10_000.0,
            discount_curve=self.disc_curve,
            dividend_curve=self.proj_curve,
        )
        opt_port = Portfolio([call_opt, put_opt], self.curves, book='EQ_OPT')

        py_npvs = self.py_engine.npvs(opt_port, self.curves, self.fit)
        sql_npvs = self.pure_engine.npvs(opt_port, self.curves, self.fit)
        for key, py_val in py_npvs.items():
            self.assertAlmostEqual(sql_npvs[key], py_val, delta=1e-7)

        # Put-Call parity: C - P = N * (S * df_q(T) - K * df_r(T))
        env = self.fit.env(self.curves)
        df_r = expr.evaluate(self.disc_curve.df(2.5), env)
        df_q = expr.evaluate(self.proj_curve.df(2.5), env)
        parity_rhs = 10_000.0 * (185.0 * df_q - 190.0 * df_r)
        self.assertAlmostEqual(
            sql_npvs[('EQ_OPT', 'AAPL_CALL_2Y5')] - sql_npvs[('EQ_OPT', 'AAPL_PUT_2Y5')],
            parity_rhs,
            delta=1e-6,
        )

        # Numerical debug mode: calling black_scholes_formula or instrument.npv on numerical curves returns plain floats
        from pricing.maths.cnd import cnd, norm_pdf
        x_var = expr.Var('x')
        for x_val in (-1.2, 0.0, 0.5, 2.1):
            self.assertAlmostEqual(expr.evaluate(expr.diff(cnd(x_var), x_var), {'x': x_val}), norm_pdf(x_val), places=15)
        self.assertIsInstance(cnd(0.5), float)
        self.assertIsInstance(norm_pdf(0.5), float)
        num_disc = self.disc_curve.numerical(self.fit.rates_for(self.disc_curve.curve_id))
        num_proj = self.proj_curve.numerical(self.fit.rates_for(self.proj_curve.curve_id))
        num_call = call_opt.npv(num_disc, num_proj)
        self.assertIsInstance(num_call, float)
        self.assertAlmostEqual(num_call, py_npvs[('EQ_OPT', 'AAPL_CALL_2Y5')], delta=1e-9)

        # Analytic pillar deltas and par-quote DV01s of the options match between Python and SQL
        py_deltas = self.py_engine.pillar_risk(opt_port, self.curves, self.fit, self.fitter.factors)
        sql_deltas = self.pure_engine.pillar_risk(opt_port, self.curves, self.fit, self.fitter.factors)
        for key, py_val in py_deltas.items():
            self.assertAlmostEqual(sql_deltas[key], py_val, delta=1e-7)

        py_q = self.py_engine.quote_risk(opt_port, self.fitter, self.fit, risk_method=FirstOrderAnalyticRisk)
        sql_q = self.pure_engine.quote_risk(opt_port, self.fitter, self.fit, risk_method=FirstOrderAnalyticRisk)
        for key, py_val in py_q.items():
            self.assertAlmostEqual(sql_q[key], py_val, delta=1e-7)

        # Also verify two-layer Pure-native diff (populate_book): Layer-1 BasisNodes + Layer-2 Pure diff()
        analytic = FirstOrderAnalyticRisk(opt_port, curves=self.curves, factors=self.fitter.factors)
        pd_book = analytic.populate_book(pure.BasisBook(), scale=risk.BP)
        self.assertIn('diff({_dv | compute_basis::cbsFormula_', pure.render_pure_model(pd_book.schema()))
        with pure.ComputeBasis(pd_book) as pd_cb:
            con = pd_cb.connect(pd_book.tables([risk.scenario(self.curves, 'BASE', self.fit, 0.0, 'USD')], ()))
            pd_rows = pd_cb.run(con, pure.measures_query('BASE', by_entity=True, book_or_schema=pd_book)).to_pylist()
            pd_deltas = {
                (r['groupId'], r['entityId'], r['factorId']): r['value']
                for r in pd_rows
                if r['measureCode'] == risk.PILLAR_DELTA
            }
            for key, py_val in py_deltas.items():
                self.assertAlmostEqual(pd_deltas[key], py_val, delta=1e-10)

    def test_automatic_projection_cse_and_pure_diff_curve_risk(self):
        """Prices a two-curve swap (IRSwapFixedFloat) and computes its pillar deltas and chained par-quote DV01s
        via Pure's native diff() and automatic projection CSE (__pc0..__pc5) with zero manual split() annotations."""
        import duckdb
        from legend_lite import compiler

        swap = IRSwapFixedFloat(
            symbol='IRS_2Y5',
            book='RATES_NY',
            notional=10_000_000.0,
            fixed_rate=0.036,
            float_spread=0.001,
            tenor_years=2.5,
            start=0.0,
            discount_curve=self.disc_curve,
            projection_curve=self.proj_curve,
        )
        analytic = FirstOrderAnalyticRisk(swap, curves=self.curves, factors=self.fitter.factors)
        n_expr = swap.npv_on(self.curves)
        var_map = {
            f.knot.name: f'$r.{expr._clean_ident(f.knot.name)}'
            for f in self.fitter.factors
        }
        proj_cols = [f"npv: r|{expr.to_pure(n_expr, var_map=var_map, as_lambda=False)}"]
        for f in self.fitter.factors:
            if analytic.diff_expr(n_expr, f.knot) is not expr.ZERO:
                col_id = f'delta_{expr._clean_ident(f.factor_id)}'
                d_pure = expr.to_pure_diff(
                    n_expr, f.knot, var_map=var_map, scale=risk.BP, as_lambda=False
                )
                proj_cols.append(f'{col_id}: r|{d_pure}')
        pure_q = f"|curve_risk::CurveScenario.all()->project(~[{', '.join(proj_cols)}])"
        self.assertIn('diff(', pure_q)
        self.assertNotIn('split(', pure_q)

        env = self.fit.env(self.curves)
        prop_names = [expr._clean_ident(f.knot.name) for f in self.fitter.factors]
        pure_props = '\n    '.join(f'{p}: Float[1];' for p in prop_names)
        sql_cols = ',\n        '.join(f'{p.upper()} DOUBLE' for p in prop_names)
        map_cols = ',\n        '.join(f'{p}: SCEN.{p.upper()}' for p in prop_names)

        model_src = (
            f"###Pure\n"
            f"Class curve_risk::CurveScenario {{\n"
            f"    {pure_props}\n"
            f"}}\n"
            f"###Relational\n"
            f"Database curve_risk::DB (\n"
            f"    Table SCEN (\n"
            f"        {sql_cols}\n"
            f"    )\n"
            f")\n"
            f"###Mapping\n"
            f"Mapping curve_risk::M (\n"
            f"    *curve_risk::CurveScenario: Relational {{\n"
            f"        ~mainTable [curve_risk::DB] SCEN\n"
            f"        {map_cols}\n"
            f"    }}\n"
            f")\n"
            f"###Connection\n"
            f"RelationalDatabaseConnection curve_risk::Conn {{ store: curve_risk::DB; type: DuckDB; specification: DuckDB {{ }}; auth: Test; }}\n"
            f"###Runtime\n"
            f"Runtime curve_risk::RT {{ mappings: [curve_risk::M]; connections: [ curve_risk::DB: [ c0: curve_risk::Conn ] ]; }}\n"
        )

        with compiler.HeldModel(model_src) as held:
            plan = held.plan(compiler.parse(pure_q), 'curve_risk::RT')
            self.assertIn('SELECT', plan.sql.upper())

            con = duckdb.connect(':memory:')
            col_defs = ', '.join(f'{p.upper()} DOUBLE' for p in prop_names)
            vals = ', '.join(repr(env[f.knot.name]) for f in self.fitter.factors)
            con.execute(f'CREATE TABLE SCEN ({col_defs})')
            con.execute(f'INSERT INTO SCEN VALUES ({vals})')
            row = con.execute(plan.sql).fetchone()
            got = dict(zip([c.name for c in plan.columns], row))

            py_npv = expr.evaluate(n_expr, env)
            self.assertAlmostEqual(got['npv'], py_npv, delta=1e-9)

            py_ladder = analytic.evaluate(self.fit, scale=risk.BP)
            sql_pillar_deltas: dict[tuple[str, str, str], float] = {}
            for f in self.fitter.factors:
                col_id = f'delta_{expr._clean_ident(f.factor_id)}'
                if col_id in got:
                    self.assertAlmostEqual(got[col_id], py_ladder[f.factor_id], delta=1e-10)
                    sql_pillar_deltas[(swap.book, swap.trade_id, f.factor_id)] = got[col_id]
                else:
                    self.assertAlmostEqual(py_ladder[f.factor_id], 0.0, delta=1e-12)

            sql_quote_dv01 = FirstOrderAnalyticRisk.chain_quote_risk(sql_pillar_deltas, self.fitter, self.fit)
            py_quote_dv01 = self.py_engine.quote_risk(swap, self.fitter, self.fit, risk_method=FirstOrderAnalyticRisk)
            for key, py_qval in py_quote_dv01.items():
                self.assertAlmostEqual(sql_quote_dv01[key], py_qval, delta=1e-9)

    def test_ir_swaption_portfolio_with_2d_vol_surface_pure_diff_and_frames_session(self):
        """End-to-end verification of multi-stage composite payoffs (IRSwaption + 2D SwaptionVolSurface):
          - Numerical debug mode (plain floats) vs symbolic mode
          - Payer-receiver swaption parity in SQL
          - Hierarchical SubMeasure factoring (max_stage == 1, num_nodes <= 4 even for 30Y / 120-coupon swaption)
          - PureBasisEngine (SQL + Pure diff() + SUB_CHAIN) vs PythonEngineExpr vs central-difference FirstOrderNumericalRisk
            across NPV, OIS + 3M curve pillar deltas, 2D vol surface knot vegas, and chained par-quote DV01s
          - Planning across all 8 SQL dialects + live ll.Page / ll.Frames PureBasisMarketSession updates."""
        vol_surface = SwaptionVolSurface(
            'USD-SWAPTION-VOL',
            expiries=[1.0, 2.0, 5.0, 10.0],
            tenors=[2.0, 5.0, 10.0, 30.0],
            atm_vols=[
                [0.24, 0.22, 0.20, 0.18],
                [0.23, 0.21, 0.19, 0.175],
                [0.21, 0.195, 0.18, 0.165],
                [0.19, 0.18, 0.17, 0.155],
            ],
        )
        swaptions = [
            IRSwaption(
                symbol='SWPN_1Y2Y_PAY',
                book='RATES_VOL',
                notional=15_000_000.0,
                strike_rate=0.036,
                expiry_years=1.0,
                swap_tenor=2.0,
                is_payer=True,
                discount_curve=self.disc_curve,
                projection_curve=self.proj_curve,
                vol_surface=vol_surface,
            ),
            IRSwaption(
                symbol='SWPN_2Y5Y_REC',
                book='RATES_VOL',
                notional=12_000_000.0,
                strike_rate=0.038,
                expiry_years=2.0,
                swap_tenor=5.0,
                is_payer=False,
                discount_curve=self.disc_curve,
                projection_curve=self.proj_curve,
                vol_surface=vol_surface,
            ),
            # Off-grid in BOTH expiry (3Y in [2Y, 5Y]) and swap tenor (7Y in [5Y, 10Y]) -> hits 4 bilinear vol knots!
            IRSwaption(
                symbol='SWPN_3Y7Y_PAY',
                book='RATES_VOL',
                notional=10_000_000.0,
                strike_rate=0.0375,
                expiry_years=3.0,
                swap_tenor=7.0,
                is_payer=True,
                float_spread=0.0005,
                discount_curve=self.disc_curve,
                projection_curve=self.proj_curve,
                vol_surface=vol_surface,
            ),
            IRSwaption(
                symbol='SWPN_3Y7Y_REC',
                book='RATES_VOL',
                notional=10_000_000.0,
                strike_rate=0.0375,
                expiry_years=3.0,
                swap_tenor=7.0,
                is_payer=False,
                float_spread=0.0005,
                discount_curve=self.disc_curve,
                projection_curve=self.proj_curve,
                vol_surface=vol_surface,
            ),
            # 30Y swap tenor (120 quarterly coupons) feeding log() and erf()
            IRSwaption(
                symbol='SWPN_5Y30Y_PAY',
                book='RATES_VOL',
                notional=8_000_000.0,
                strike_rate=0.039,
                expiry_years=5.0,
                swap_tenor=30.0,
                is_payer=True,
                discount_curve=self.disc_curve,
                projection_curve=self.proj_curve,
                vol_surface=vol_surface,
            ),
        ]
        swpn_port = Portfolio(swaptions, self.curves, book='RATES_VOL')

        # 1. Numerical debug mode returns plain Python floats matching symbolic evaluation to < 1e-12
        num_disc = self.disc_curve.numerical(self.fit.rates_for(self.disc_curve.curve_id))
        num_proj = self.proj_curve.numerical(self.fit.rates_for(self.proj_curve.curve_id))
        num_vol = vol_surface.numerical()
        env = self.fit.env(self.curves, [vol_surface])
        for swpn in swaptions:
            num_val = swpn.npv(num_disc, num_proj, num_vol)
            self.assertIsInstance(num_val, float)
            sym_val = expr.evaluate(swpn.npv_on(self.curves), env)
            self.assertAlmostEqual(num_val, sym_val, delta=max(1e-8, 1e-11 * abs(sym_val)))

        # 2. Hierarchical SubMeasure factoring keeps slot width <= 4 and O(1) base formulas across all tenors & book sizes
        analytic = FirstOrderAnalyticRisk(swpn_port, curves=self.curves)
        book = analytic.populate_book(pure.BasisBook(), scale=risk.BP)
        sch = book.schema()
        self.assertEqual(sch.max_stage, 1)
        self.assertLessEqual(sch.num_nodes, 4)
        base_formulas = [f for f in sch.formulas if f.base_op is None]
        self.assertLessEqual(len(base_formulas), 10)

        # Scaling from 20 to 60 heterogeneous swaptions across tenors up to 30Y saturates at <= 12 base formulas & <= 4 slots
        def _make_swaptions(count: int):
            return list(swaptions) + [
                IRSwaption(
                    symbol=f'SWPN_EXTRA_{i}',
                    book='RATES_VOL',
                    notional=5_000_000.0 + 1_000_000.0 * i,
                    strike_rate=0.032 + 0.0005 * i,
                    expiry_years=1.0 + 0.5 * (i % 9),
                    swap_tenor=float(2 + 2 * (i % 15)),
                    is_payer=(i % 2 == 0),
                    discount_curve=self.disc_curve,
                    projection_curve=self.proj_curve,
                    vol_surface=vol_surface,
                )
                for i in range(count)
            ]

        book_20 = FirstOrderAnalyticRisk(_make_swaptions(15), curves=self.curves).populate_book(
            pure.BasisBook(), scale=risk.BP
        )
        book_60 = FirstOrderAnalyticRisk(_make_swaptions(55), curves=self.curves).populate_book(
            pure.BasisBook(), scale=risk.BP
        )
        sch_20, sch_60 = book_20.schema(), book_60.schema()
        self.assertLessEqual(sch_60.num_nodes, 4)
        base_20 = [f for f in sch_20.formulas if f.base_op is None]
        base_60 = [f for f in sch_60.formulas if f.base_op is None]
        self.assertLessEqual(len(base_60), 12)
        self.assertEqual(len(base_60), len(base_20))

        # Zero host math evaluation in rows() (TENET_CHARTER.md C2.1)
        scen0 = risk.scenario(self.curves, 'BASE', self.fit, 0.0, 'USD', vol_surfaces=[vol_surface])
        raw_rows = book.rows([scen0])
        self.assertTrue(all('NODE_VALUE' not in r for r in raw_rows['CBS_SHARED_NODE']))
        self.assertTrue(all(r['TERM_VALUE'] == 0.0 for r in raw_rows['CBS_BASIS_TERM']))

        with pure.ComputeBasis(book) as swpn_cb:
            for db in pure.DATABASES:
                sql = swpn_cb.plan(pure.measures_query(book_or_schema=book), db).sql
                self.assertIn('SELECT', sql.upper(), db)
                # Without ProjectionCse, Postgres inline ErfMath expansion on 2D Black-76 is ~638 KB (vs <600 KB with CSE)
                limit = 250_000 if db == 'DuckDB' else 700_000
                self.assertLess(len(sql), limit, f'Expected compact swaption SQL on {db}, got {len(sql)} bytes')

        # 3. PureBasisEngine (DuckDB SQL + Pure diff()) vs PythonEngineExpr vs central-difference FirstOrderNumericalRisk
        sql_npvs = self.pure_engine.npvs(swpn_port, self.curves, self.fit)
        py_npvs = self.py_engine.npvs(swpn_port, self.curves, self.fit)
        self.assertEqual(set(sql_npvs), set(py_npvs))
        for k, py_v in py_npvs.items():
            self.assertAlmostEqual(sql_npvs[k], py_v, delta=max(1e-8, 1e-11 * abs(py_v)))

        # Payer - Receiver Swaption Parity: Payer(K) - Receiver(K) == Payer Forward Swap(K)
        fwd_swap = swaptions[2].underlying_swap(self.disc_curve, self.proj_curve)
        fwd_swap_npv = expr.evaluate(fwd_swap.npv_on(self.curves), env)
        self.assertAlmostEqual(
            sql_npvs[('RATES_VOL', 'SWPN_3Y7Y_PAY')] - sql_npvs[('RATES_VOL', 'SWPN_3Y7Y_REC')],
            fwd_swap_npv,
            delta=max(1e-7, 1e-11 * abs(fwd_swap_npv)),
        )

        # Full factor sensitivities (12 curve pillars + 16 2D vol surface knots)
        all_factors = swpn_port.factors(self.curves)
        self.assertEqual(len(all_factors), len(self.fitter.factors) + 16)
        sql_risk = self.pure_engine.pillar_risk(swpn_port, self.curves, self.fit, all_factors)
        py_risk = self.py_engine.pillar_risk(swpn_port, self.curves, self.fit, all_factors)
        fd_calc = FirstOrderNumericalRisk(swpn_port, curves=self.curves, factors=all_factors, bump_bps=0.01)
        fd_risk = fd_calc.pillar_risk(self.fit, method='central')

        # Off-grid 3Y7Y swaption must have non-zero Vegas on all 4 bounding surface knots (2Yx5Y, 2Yx10Y, 5Yx5Y, 5Yx10Y)
        expected_3y7y_knots = {
            'USD-SWAPTION-VOL:2Yx5Y',
            'USD-SWAPTION-VOL:2Yx10Y',
            'USD-SWAPTION-VOL:5Yx5Y',
            'USD-SWAPTION-VOL:5Yx10Y',
        }
        got_3y7y_vol_knots = {
            fid for (_b, tid, fid) in sql_risk
            if tid == 'SWPN_3Y7Y_PAY' and fid.startswith('USD-SWAPTION-VOL:')
        }
        self.assertEqual(got_3y7y_vol_knots, expected_3y7y_knots)

        self.assertEqual(set(sql_risk), set(py_risk))
        for k, py_d in py_risk.items():
            self.assertAlmostEqual(sql_risk[k], py_d, delta=max(1e-8, 1e-10 * abs(py_d)), msg=str(k))
            self.assertAlmostEqual(sql_risk[k], fd_risk.get(k, 0.0), delta=max(1e-2, 1e-5 * abs(py_d)), msg=str(k))

        # Chained par-quote DV01s in SQL vs Python vs central-difference quote refit & reprice
        sql_q_risk = self.pure_engine.quote_risk(swpn_port, self.fitter, self.fit)
        py_q_risk = self.py_engine.quote_risk(swpn_port, self.fitter, self.fit)
        fd_q_risk = self.pure_engine.quote_risk(
            swpn_port, self.fitter, self.fit, risk_method=FirstOrderNumericalRisk, method='central'
        )
        for k, py_q in py_q_risk.items():
            self.assertAlmostEqual(sql_q_risk[k], py_q, delta=max(1e-7, 1e-10 * abs(py_q)))
            self.assertAlmostEqual(sql_q_risk[k], fd_q_risk[k], delta=max(0.5, 5e-4 * abs(py_q)))

        # 4. Phase 3: PureBasisMarketSession bound directly onto ll.Page / ll.Frames with SQL curve fitting & in-place updates
        page = ll.Page('Swaption Live Session')
        with PureBasisEngine().session(swpn_port, self.fitter, frames=page, use_sql_fitter=True) as sess:
            t0 = sess.tick()
            self.assertTrue(t0.is_first_tick)
            grid = page.sheets[0].grid(
                sess.frames['session_npv'].given, name='session_npv', title='Live Swaption NPVs'
            )
            self.assertEqual(len(grid.view().rows), len(swaptions))

            bumped_quotes = [q + risk.BP for q in self.fitter.quotes]
            t1 = sess.tick(bumped_quotes)
            self.assertFalse(t1.is_first_tick)
            self.assertEqual(t1.build_ms, 0.0)
            live_rows = {
                r['trade_id']: r['npv']
                for r in sess.frames['session_npv'].given.to_pylist()
            }
            for (_b, tid), val in t1.npv.items():
                self.assertAlmostEqual(live_rows[tid], val, delta=1e-12)
            self.assertIn('session_quote_risk', sess.frames)
            self.assertEqual(len(sess.frames['session_quote_risk'].given), len(t1.quote_risk))



class InPure(unittest.TestCase):
    """The demo's two-curve portfolio (1-curve OIS + 2-curve IRS), written as a compute-basis book and valued on DuckDB."""

    @classmethod
    def setUpClass(cls):
        cls.disc_curve = ZeroCurve(demo.DISC_CURVE, demo.PILLARS)
        cls.proj_curve = ZeroCurve(demo.PROJ_CURVE, demo.PILLARS)
        cls.curves = [cls.disc_curve, cls.proj_curve]
        cls.fitter = MultiCurveFitter(cls.disc_curve, demo.OIS_QUOTES, cls.proj_curve, demo.IRS_QUOTES)
        cls.trades = demo.portfolio(1, cls.disc_curve, cls.proj_curve)
        cls.result = risk.risk_run(cls.curves, cls.fitter, cls.trades, [(demo.BUMP, risk.BP)], engine='python')
        cls.cb = pure.ComputeBasis()
        cls.con = cls.cb.connect(cls.result.book.tables(cls.result.scenarios, cls.result.jacobian))

    @classmethod
    def tearDownClass(cls):
        cls.cb.close()

    def values(self, by_entity=True):
        keys = ('scenarioId', 'groupId', 'entityId', 'measureCode', 'factorId') if by_entity else \
            ('scenarioId', 'groupId', 'measureCode', 'factorId')
        return {tuple(r[k] for k in keys): r['value']
                for r in self.cb.run(self.con, pure.measures_query(by_entity=by_entity)).to_pylist()}

    def test_npvs_and_deltas_in_sql_are_pythons(self):
        v = self.values()
        for (bk, t), x in self.result.npv.items():
            self.assertLess(abs(v[('BASE', bk, t, risk.NPV, '*')] - x) / self.result.notional[(bk, t)], 1e-12)
        for (bk, t, f), x in self.result.delta.items():
            self.assertLess(abs(v[('BASE', bk, t, risk.PILLAR_DELTA, f)] - x) / self.result.notional[(bk, t)], 1e-12)

    def test_a_parallel_bump_in_sql_is_the_sum_of_the_symbolic_deltas(self):
        v = self.values(by_entity=False)
        for bk in {k[1] for k in v}:
            moved = v[(demo.BUMP, bk, risk.NPV, '*')] - v[('BASE', bk, risk.NPV, '*')]
            delta = sum(x for (s, g, m, _f), x in v.items() if s == 'BASE' and g == bk and m == risk.PILLAR_DELTA)
            self.assertAlmostEqual(moved, delta, delta=2e-3 * abs(delta) + 1.0)

    def test_quote_risk_in_sql_is_the_chain_rule_and_reconciles_with_bump_and_grind(self):
        rows = self.cb.run(self.con, risk.quote_risk_query('BASE')).to_pylist()
        self.assertEqual({(r['groupId'], r['inputId']) for r in rows}, set(self.result.quote_risk))
        for r in rows:
            key = (r['groupId'], r['inputId'])
            self.assertAlmostEqual(r['chainedValue'], self.result.quote_risk[key], delta=1e-6)
            self.assertAlmostEqual(
                r['chainedValue'],
                self.result.bump_quote_risk[key],
                delta=max(15.0, 5e-3 * abs(r['chainedValue'])),
            )

    def test_the_demo_runs_clean(self):
        self.assertTrue(demo.run(20, engine='pure_duckdb'))
        self.assertTrue(demo.run(20, engine='python'))

    def test_the_dashboard_page_builds_views_and_saves(self):
        import warnings
        from pricing.demo.dashboard import CANONICAL_FRAME_NAMES, LEAF_FRAME_NAMES

        frames = irs_frames(self.curves, self.fitter, self.trades, self.result, self.cb)
        self.assertTrue(set(CANONICAL_FRAME_NAMES).issubset(frames))
        self.assertTrue(set(LEAF_FRAME_NAMES).issubset(frames))
        self.assertEqual(frames['market_quotes'].num_rows, 2 * len(demo.PILLARS))
        self.assertEqual(frames['swap_portfolio'].num_rows, len(self.trades))
        self.assertIn('bump_reprice_dv01', frames['quote_risk'].column_names)
        self.assertIn('analytic_vs_bump_diff', frames['quote_risk'].column_names)

        page = irs_page(frames, curve_id=f'{demo.DISC_CURVE} & {demo.PROJ_CURVE}',
                        fitter=self.fitter,
                        iterations=self.result.fit.iterations,
                        shared_nodes=len(self.result.book.nodes), basis_terms=self.result.book.term_count())
        self.assertEqual([s.name for s in page.sheets], [
            '0. Pricing & Risk Flow',
            '1. Market & Curve Fit',
            '2. Products & Output Prices',
            '3. Output Risk Ladders',
            '4. Diagnostic Curve Jacobian',
        ])
        self.assertTrue(page.sheets[0].flow)
        board_grids = [t for s in page.sheets[1:] for t in s.tiles if isinstance(t, Grid)]
        self.assertEqual(len(board_grids), 7)
        self.assertEqual(len(page.charts), 6)
        for grid in board_grids:
            self.assertGreater(len(grid.view().rows), 0)

        # Verify live Flow parameter reactivity: +1bp parallel quote shock recalibrates both curves & revalues in SQL
        trades_grid = next(t for t in page.sheets[2].tiles if isinstance(t, Grid) and 'Trade-Level' in (t.title or ''))
        v0 = trades_grid.view()
        cols0 = [c['name'] for c in v0.columns]
        base_by_tid = {dict(zip(cols0, r))['trade_id']: dict(zip(cols0, r)) for r in v0.rows}
        page.set_parameter('parallel_shift_bps', 1.0)
        v1 = trades_grid.view()
        cols1 = [c['name'] for c in v1.columns]
        shocked_by_tid = {dict(zip(cols1, r))['trade_id']: dict(zip(cols1, r)) for r in v1.rows}
        sample_tid = next(tid for tid, row in base_by_tid.items() if row['book'] != 'CALIBRATION')
        moved = shocked_by_tid[sample_tid]['sql_npv'] - base_by_tid[sample_tid]['sql_npv']
        # A +1bp parallel par-quote shock moves swap NPV in the same direction & order of magnitude as 1bp zero-rate DV01
        self.assertAlmostEqual(moved, base_by_tid[sample_tid]['analytic_dv01_sum'],
                               delta=max(100.0, 0.15 * abs(base_by_tid[sample_tid]['analytic_dv01_sum'])))
        page.set_parameter('parallel_shift_bps', 0.0)

        tmp = Path(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR'))) / 'irs.page.json'
        page.save(tmp)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            reloaded = ll.Page.load(tmp, frames=frames)
        self.assertEqual(len(reloaded.grids), len(page.grids))
        self.assertEqual(len(reloaded.charts), 6)

    def test_the_interactive_workbench_updates_prices_risk_and_page_live(self):
        wb = IRSWorkbench(
            ois_quotes=[300.0, 320.0, 335.0, 355.0, 368.0, 380.0],
            irs_quotes=[315.0, 338.0, 355.0, 378.0, 392.0, 405.0],
        )
        try:
            page = wb.build_page()
            frames0 = wb.price_swap(
                symbol='USER_SWAP_5Y',
                notional=20_000_000.0,
                fixed_rate_bps=360.0,
                float_spread_bps=5.0,
                start=0.5,
                tenor_years=5.0,
                side='RECEIVER',
                two_curve=True,
                verbose=False,
            )
            npv0 = frames0['swap_portfolio'].to_pylist()[0]['sql_npv']
            q_row_5y = next(
                r for r in frames0['quote_risk'].to_pylist() if 'IRS_5Y' in r['quote_id']
            )
            q_risk_5y = q_row_5y['sql_quote_dv01']
            bump_risk_5y = q_row_5y['bump_reprice_dv01']
            # Bumping IRS_5Y by +1bp moves the SQL NPV by identically bump_reprice_dv01 (~q_risk_5y)
            frames1 = wb.bump_quote('IRS_5Y', 1.0, verbose=False)
            npv1 = frames1['swap_portfolio'].to_pylist()[0]['sql_npv']
            self.assertAlmostEqual(npv1 - npv0, bump_risk_5y, delta=1e-3)
            self.assertAlmostEqual(npv1 - npv0, q_risk_5y, delta=15.0)
            trades_grid = next(
                t for t in page.sheets[2].tiles if isinstance(t, Grid) and 'Trade-Level' in (t.title or '')
            )
            view1 = trades_grid.view()
            self.assertEqual(len(view1.rows), 1)
            row1 = dict(zip([c['name'] for c in view1.columns], view1.rows[0]))
            self.assertAlmostEqual(row1['sql_npv'], npv1, delta=1e-4)
        finally:
            wb.close()

    def test_market_sessions_and_scaling_benchmark_across_first_and_warm_ticks(self):
        summaries = irs_scaling.run_scaling_benchmark(sizes=(1, 10), n_ticks=3)
        self.assertEqual(len(summaries), 8)  # 2 sizes x 4 routes
        for s in summaries:
            self.assertEqual(s.first_tick.tick_index, 0)
            self.assertTrue(s.first_tick.is_first_tick)
            self.assertGreater(s.first_tick.build_ms, 0.0)
            self.assertEqual(s.last_tick.tick_index, 2)
            self.assertFalse(s.last_tick.is_first_tick)
            self.assertEqual(s.last_tick.build_ms, 0.0)
            self.assertLess(s.max_npv_diff_vs_ref, 1e-6)
            if s.risk_method_name == 'analytic':
                self.assertLess(s.max_risk_diff_vs_ref, 1e-7)
            else:
                # Forward 1bp bump-and-grind vs analytic DV01 agrees within second-order convexity
                self.assertLess(s.max_risk_diff_vs_ref, 25.0)
        report = irs_scaling.format_scaling_report(summaries)
        self.assertIn('1ST MARKET TICK (COLD) vs LATER MARKET UPDATES (WARM)', report)
        page = irs_scaling.scaling_page(summaries)
        self.assertEqual(len(page.grids), 2)
        self.assertEqual(len(page.charts), 2)

    def test_datacube_flow_and_grid_calculate_over_compute_basis_frames_and_expr_to_pure(self):
        page = ll.Page('Compute Basis Flow')
        tables = self.result.book.tables(self.result.scenarios, self.result.jacobian)
        frames = self.cb.frames(tables, frames=page)
        self.assertIsNotNone(frames.model_for(self.cb.model()))
        flow = page.flow('Knot Flow')
        r_var = expr.Var('KNOT_VALUE')
        t_var = expr.Var('KNOT_COORD')
        df_expr = expr.exp(-r_var * t_var)
        ddf_dr_expr = expr.diff(df_expr, r_var)
        box = (
            flow.grid(tables['CBS_KNOT'], name='CBS_KNOT', title='Knots')
            .custom_filter("$x.SCENARIO_ID == 'BASE'")
            .calculate('discount_factor', expr.to_pure(df_expr, row_param='x', as_lambda=False))
            .calculate('d_df_d_rate', expr.to_pure(ddf_dr_expr, row_param='x', as_lambda=False))
        )
        view = box.view()
        col_names = [c['name'] for c in view.columns]
        rows = [dict(zip(col_names, r)) for r in view.rows]
        self.assertEqual(len(rows), 2 * len(demo.PILLARS))
        for row in rows:
            env = {'KNOT_VALUE': row['KNOT_VALUE'], 'KNOT_COORD': row['KNOT_COORD']}
            self.assertAlmostEqual(row['discount_factor'], expr.evaluate(df_expr, env), places=12)
            self.assertAlmostEqual(row['d_df_d_rate'], expr.evaluate(ddf_dr_expr, env), places=12)


if __name__ == '__main__':
    unittest.main()
