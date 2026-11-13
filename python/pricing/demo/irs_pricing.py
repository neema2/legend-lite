"""Fit discount and projection yield curves, price a swap portfolio containing both single-curve
(``IRSwapFixedFloatApprox``) and two-curve explicit-cashflow (``IRSwapFixedFloat``) swaps, and compute its risk using:
  - Engine choice: ``PythonEngineExpr`` (local Python ``expr.Expr`` evaluation) vs. ``PureBasisEngine`` (Pure -> SQL
    across all 8 dialects -> DuckDB execution).
  - Risk method choice: ``FirstOrderAnalyticRisk`` (symbolic pillar differentiation + Jacobian chain rule) vs.
    ``FirstOrderNumericalRisk`` (naive per-quote bump, curve refit, and portfolio reprice).

    bazel run //python:irs_demo -- [--swaps 500] [--engine {pure_duckdb,python}] [--sql] [--page irs.page.json] [--show]

Or from a shell under ``python/``:

    python -m pricing.demo.irs_pricing [--swaps 500] [--engine {pure_duckdb,python}] [--sql] [--page irs.page.json] [--show]
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import random
import sys
import time


def _bootstrap_env() -> None:
    """Resolves Bazel runfiles when launched via ``bazel run``, or falls back to ``bazel-bin`` under the repo root."""
    py_root = str(Path(__file__).resolve().parents[2])
    if py_root not in sys.path:
        sys.path.insert(0, py_root)
    try:
        from python.runfiles import runfiles
        r = runfiles.Create()
    except Exception:
        r = None
    if r is not None:
        for var in ('LEGEND_LITE_LIBRARY', 'LEGEND_LITE_SITE', 'LEGEND_LITE_DATACUBE',
                    'LEGEND_LITE_DUCKDB_EXTENSIONS'):
            val = os.environ.get(var)
            if not val:
                continue
            parts = val.split()
            resolved = [r.Rlocation(p) for p in parts]
            if all(p and os.path.exists(p) for p in resolved):
                os.environ[var] = os.pathsep.join(resolved)
    repo = Path(__file__).resolve().parents[3]
    defaults = {
        'LEGEND_LITE_LIBRARY': repo / 'bazel-bin' / 'native' / 'libcompiler.so',
        'LEGEND_LITE_DATACUBE': repo / 'bazel-bin' / 'datacube' / 'headless' / 'legend-lite-headless.js',
        'LEGEND_LITE_SITE': repo / 'bazel-bin' / 'datacube' / 'dist',
    }
    for var, candidate in defaults.items():
        if var not in os.environ and candidate.exists():
            os.environ[var] = str(candidate)


_bootstrap_env()

import legend_lite as ll  # noqa: E402
from legend_lite.scalar_parallel import basis_pure as pure  # noqa: E402
from pricing.demo.dashboard import irs_frames, irs_page  # noqa: E402
from pricing.engines import PureBasisEngine, PythonEngineExpr, resolve_engine  # noqa: E402
from pricing.instruments import IRSwapFixedFloat, IRSwapFixedFloatApprox  # noqa: E402
from pricing.marketmodels import MultiCurveFitter, Pillar, ZeroCurve  # noqa: E402
from pricing.risk import (  # noqa: E402
    BP,
    FirstOrderAnalyticRisk,
    FirstOrderNumericalRisk,
    NPV,
    PILLAR_DELTA,
    SwapInstrument,
    quote_risk_query,
    risk_run,
)

DISC_CURVE = 'USD-SOFR-OIS'
PROJ_CURVE = 'USD-SOFR-3M'
CURVE = DISC_CURVE
PILLARS = [
    Pillar('1Y', 1.0),
    Pillar('2Y', 2.0),
    Pillar('3Y', 3.0),
    Pillar('5Y', 5.0),
    Pillar('7Y', 7.0),
    Pillar('10Y', 10.0),
]
OIS_QUOTES = [0.0300, 0.0320, 0.0335, 0.0355, 0.0368, 0.0380]   # OIS discount curve par swap rates
IRS_QUOTES = [0.0315, 0.0338, 0.0355, 0.0378, 0.0392, 0.0405]   # 3M projection curve par swap rates (discounted on OIS)
QUOTES = OIS_QUOTES
BOOKS = ('RATES_NY', 'RATES_LDN', 'RATES_TKY')
BUMP = 'PARALLEL_UP_1BP'


def portfolio(
    n: int,
    disc_curve: ZeroCurve | None = None,
    proj_curve: ZeroCurve | None = None,
    seed: int = 7,
) -> list[SwapInstrument]:
    """``n`` random swaps across three books: each swap's maturity is chosen randomly on a ``1/255``-year business-day
    grid between 1Y and 10Y (marching backwards in 1.0-year steps to ``start = 0.0`` via ``ir_scheduling.rack_dates``),
    using the full two-curve ``IRSwapFixedFloat`` when ``proj_curve`` is provided."""
    rnd = random.Random(seed)
    trades: list[SwapInstrument] = []
    for i in range(n):
        book = rnd.choice(BOOKS)
        notional = float(rnd.randint(1, 50)) * 1e6
        fixed_rate = round(rnd.uniform(0.02, 0.05), 5)
        end_days = rnd.randint(255, 10 * 255)
        tenor_years = round(end_days / 255.0, 10)
        side = 'RECEIVER' if rnd.random() < 0.5 else 'PAYER'
        if proj_curve is not None:
            spread = round(rnd.choice((0.0, 0.0005, 0.0010, -0.0005)), 6)
            trades.append(
                IRSwapFixedFloat(
                    symbol=f'IRS{i:06d}',
                    notional=notional,
                    fixed_rate=fixed_rate,
                    float_spread=spread,
                    tenor_years=tenor_years,
                    start=0.0,
                    book=book,
                    side=side,
                    discount_curve=disc_curve,
                    projection_curve=proj_curve,
                )
            )
        else:
            trades.append(
                IRSwapFixedFloatApprox(
                    symbol=f'OIS{i:06d}',
                    notional=notional,
                    fixed_rate=fixed_rate,
                    tenor_years=tenor_years,
                    start=0.0,
                    book=book,
                    side=side,
                    curve=disc_curve,
                )
            )
    return trades


def par_swaps(
    disc_curve: ZeroCurve,
    ois_quotes: list[float],
    proj_curve: ZeroCurve | None = None,
    irs_quotes: list[float] | None = None,
    notional: float = 10e6,
) -> list[SwapInstrument]:
    """The calibration swaps booked at their market quotes: single-curve OIS swaps on ``disc_curve`` and (when
    ``proj_curve`` is given) two-curve IRS swaps on ``(disc_curve, proj_curve)``."""
    prefix = 'OIS' if proj_curve is not None else 'PAR'
    swaps: list[SwapInstrument] = [
        IRSwapFixedFloatApprox(
            symbol=f'{prefix}_{p.label}',
            notional=notional,
            fixed_rate=q,
            tenor_years=p.tenor,
            start=0.0,
            book='CALIBRATION',
            curve=disc_curve,
        )
        for p, q in zip(disc_curve.pillars, ois_quotes)
    ]
    if proj_curve is not None and irs_quotes is not None:
        swaps.extend(
            IRSwapFixedFloat(
                symbol=f'IRS_{p.label}',
                notional=notional,
                fixed_rate=q,
                tenor_years=p.tenor,
                start=0.0,
                book='CALIBRATION',
                discount_curve=disc_curve,
                projection_curve=proj_curve,
            )
            for p, q in zip(proj_curve.pillars, irs_quotes)
        )
    return swaps


def run(
    n_swaps: int,
    show_sql: bool = False,
    page_out: str | None = None,
    show_page: bool = False,
    engine: str = 'pure_duckdb',
) -> bool:
    t0 = time.perf_counter()
    disc_curve = ZeroCurve(DISC_CURVE, PILLARS)
    proj_curve = ZeroCurve(PROJ_CURVE, PILLARS)
    curves = [disc_curve, proj_curve]
    fitter = MultiCurveFitter(disc_curve, OIS_QUOTES, proj_curve, IRS_QUOTES)
    with fitter.sql_fitter() as sql_fitter:
        sql_fit = sql_fitter.fit()
    trades = par_swaps(disc_curve, OIS_QUOTES, proj_curve, IRS_QUOTES) + portfolio(n_swaps, disc_curve, proj_curve)
    eng = resolve_engine(engine)
    r = risk_run(curves, fitter, trades, [(BUMP, BP)], 'USD', engine=eng, include_bump_and_grind=True)
    t1 = time.perf_counter()
    fit_diff = max(abs(a - b) for a, b in zip(sql_fit.rates, r.fit.rates))
    jac_diff = max(abs(a - b) for row_a, row_b in zip(sql_fit.dr_dq, r.fit.dr_dq) for a, b in zip(row_a, row_b))
    print(f'2 curves ({DISC_CURVE}, {PROJ_CURVE}) fitted in {sql_fit.iterations} SQL Newton steps '
          f'(vs {r.fit.iterations} Python ref steps: max |dr|={fit_diff:.2e}, max |d(J^-1)|={jac_diff:.2e}; '
          f'+{r.total_bump_newton_steps} steps for {len(r.bump_fits)} naive quote-bump refits); '
          f'engine={r.engine_name}; {len(r.npv)} trades (1-curve OIS + 2-curve IRS); '
          f'{len(r.book.nodes)} shared nodes; {r.book.term_count()} basis terms per scenario ({t1 - t0:.2f}s)')

    with pure.ComputeBasis() as cb:
        con = cb.connect(r.book.tables(r.scenarios, r.jacobian))
        for db in pure.DATABASES:
            sql = cb.plan(pure.measures_query(), db).sql
            print(f'  {db:<11} measures: {len(sql):>6} characters of SQL')
            if show_sql:
                print(sql, '\n')

        v = {(x['scenarioId'], x['groupId'], x['entityId'], x['measureCode'], x['factorId']): x['value']
             for x in cb.run(con, pure.measures_query()).to_pylist()}
        t2 = time.perf_counter()
        py_ref = r if r.engine_name == 'python' else PythonEngineExpr().run(
            curves, fitter, trades, include_bump_and_grind=False
        )
        # per unit of notional: an NPV near zero is the difference of two legs of the notional's size
        worst = max([abs(v[('BASE', b, t, NPV, '*')] - x) / r.notional[(b, t)] for (b, t), x in py_ref.npv.items()]
                    + [abs(v[('BASE', b, t, PILLAR_DELTA, f)] - x) / r.notional[(b, t)]
                       for (b, t, f), x in py_ref.delta.items()])
        print(f'NPVs and pillar deltas in SQL against Python: worst difference per unit notional {worst:.2e} '
              f'({t2 - t1:.2f}s)')

        # the symbolic deltas against a finite difference the database computes (the parallel 1bp scenario across both curves)
        fd_worst = 0.0
        for (b, t) in r.npv:
            moved = v[(BUMP, b, t, NPV, '*')] - v[('BASE', b, t, NPV, '*')]
            analytic = sum(v.get(('BASE', b, t, PILLAR_DELTA, f.factor_id), 0.0) for f in fitter.factors)
            fd_worst = max(fd_worst, abs(moved - analytic) / max(1.0, abs(analytic)))
        print(f'parallel 1bp bump in SQL against the sum of symbolic deltas: worst relative difference {fd_worst:.2e}')

        risk = cb.run(con, quote_risk_query('BASE')).to_pylist()
        rworst = max(abs(x['chainedValue'] - py_ref.quote_risk[(x['groupId'], x['inputId'])]) / max(1.0, abs(x['chainedValue']))
                     for x in risk)
        print(f'par-quote risk (FirstOrderAnalyticRisk in SQL vs Python): worst relative difference {rworst:.2e}')

        # Compare FirstOrderAnalyticRisk (Jacobian chain rule) against FirstOrderNumericalRisk (naive per-quote bump, refit & reprice)
        book_scale = max(1000.0, max((abs(v) for v in r.quote_risk.values()), default=1000.0))
        bg_worst = max(
            abs(r.quote_risk[k] - r.bump_quote_risk[k]) / book_scale
            for k in r.quote_risk
        )
        print(f'par-quote risk (FirstOrderAnalyticRisk vs FirstOrderNumericalRisk refit & reprice): '
              f'worst relative difference {bg_worst:.2e}')

        frames = irs_frames(curves, fitter, trades, r, cb)

    # Print diagnostic summary tables
    print('\n1. Input Market Quotes & Diagnostic Multi-Curve Fit:')
    print(f"  {'Curve':<14} {'Quote ID':<10} {'Tenor':>6} {'Mkt Par (bps)':>14} {'Zero Rate (bps)':>16} "
          f"{'Discount Factor':>16} {'Fit Error (bps)':>16}")
    for row in frames['market_quotes'].to_pylist():
        print(f"  {row['curve_id']:<14} {row['quote_id']:<10} {row['tenor_years']:>5.1f}Y "
              f"{row['market_quote_bps']:>14.2f} {row['fitted_zero_rate_bps']:>16.2f} "
              f"{row['discount_factor']:>16.6f} {row['fit_error_bps']:>16.2e}")

    quotes = [q.quote_id for q in fitter.quote_specs]
    table = {(x['groupId'], x['inputId']): x['chainedValue'] for x in risk}
    print('\n2. Output Book Risk to Each Input Market Par Quote, per 1bp (FirstOrderAnalyticRisk vs FirstOrderNumericalRisk):')
    print(f"  {'book (method)':<22}" + ''.join(f'{q:>10}' for q in quotes))
    for b in sorted({x['groupId'] for x in risk}):
        print(f"  {b + ' (Analytic)':<22}" + ''.join(f'{table.get((b, q), 0.0):>10,.0f}' for q in quotes))
        print(f"  {b + ' (Bump&Refit)':<22}" + ''.join(f'{r.bump_quote_risk.get((b, q), 0.0):>10,.0f}' for q in quotes))

    if page_out is not None or show_page:
        page = irs_page(frames, curve_id=f'{DISC_CURVE} & {PROJ_CURVE}', fitter=fitter,
                        iterations=r.fit.iterations,
                        shared_nodes=len(r.book.nodes), basis_terms=r.book.term_count())
        print(f'\nDataCube Page built: {page.name!r} ({len(page.sheets)} sheets, {len(page.grids)} grids, '
              f'{len(page.charts)} charts)')
        for sheet in page.sheets:
            tiles = [f'{t._kind}:{t.title or t.id}' for t in sheet.tiles]
            print(f'  - {sheet.name}: {", ".join(tiles)}')
        if page_out is not None:
            page.save(page_out)
            print(f'saved DataCube page document to {page_out}')
        if show_page:
            tab = ll.show(page)
            tab._session.opened = True

    return fit_diff < 1e-12 and jac_diff < 1e-11 and worst < 1e-12 and rworst < 1e-9 and fd_worst < 1e-3 and bg_worst < 2e-2


def main(argv: list[str] | None = None) -> int:
    args = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    args.add_argument('--swaps', type=int, default=500, help='number of random swaps in the portfolio')
    args.add_argument('--engine', choices=('pure_duckdb', 'python'), default='pure_duckdb',
                      help='execution engine: pure_duckdb (Pure -> SQL -> DuckDB) or python (local Expr DAG)')
    args.add_argument('--sql', action='store_true', help="print the measures' SQL for each database")
    args.add_argument('--page', metavar='PATH', help='save the DataCube dashboard (.page.json) to PATH')
    args.add_argument('--show', action='store_true', help='open the interactive DataCube dashboard in a browser')
    o = args.parse_args(argv)
    ok = run(o.swaps, o.sql, o.page, o.show, engine=o.engine)
    print('\nOK' if ok else '\nMISMATCH')
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
