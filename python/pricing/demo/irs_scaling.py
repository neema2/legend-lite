"""IRS Pricing & Risk Timing and Scaling Demo: Python ``Expr`` vs. Pure -> SQL -> DuckDB across Portfolio Sizes and
Market Ticks.

Compares the four execution routes formed by pairing:
  - **Execution Engine**:
      1. ``PythonEngineExpr`` (``python``): evaluates symbolic ``legend_lite.scalar_parallel.expr.Expr`` DAGs locally in Python.
      2. ``PureBasisEngine`` (``pure_duckdb``): expands ``Expr`` trees into the piecewise interpolation basis
         (``CBS_SHARED_NODE``, ``CBS_BASIS_TERM``), compiles Pure queries to SQL, and executes them in DuckDB.
  - **Risk Method**:
      1. ``FirstOrderAnalyticRisk`` (``analytic``): exact symbolic differentiation ``dV/dr_k`` + single curve fit with
         analytic inverse Jacobian ``dr_k/dq_j`` chained via ``dV/dq_j = sum_k (dV/dr_k) * (dr_k/dq_j)``.
      2. ``FirstOrderNumericalRisk`` (``bump_and_grind``): naive 1bp bump of each of the 12 benchmark quotes (6 OIS + 6 IRS),
         full multi-curve Newton refit per quote, and portfolio reprice.

Measures two distinct operational phases across portfolio sizes (default: **1 swap**, **10 swaps**, **100 swaps**):
  - **1st Market Tick (Cold Start)**: builds instrument & derivative ``Expr`` DAGs, expands ``BasisBook`` polynomials,
    plans Pure queries to DuckDB SQL, loads static portfolio tables (``CBS_SHARED_NODE``, ``CBS_BASIS_TERM``), fits the
    initial market quotes, and evaluates portfolio NPVs and par-quote DV01 ladders.
  - **Later Market Updates (Warm Ticks)**: when benchmark OIS/IRS quotes tick, the portfolio structure
    (``CBS_SHARED_NODE``, ``CBS_BASIS_TERM``, symbolic ``Expr`` DAGs, and compiled SQL plans) is unchanged and reused!
    Only the curve fitter re-solves (``fitter.fit_quotes(new_quotes)``) and updates ``CBS_KNOT`` and
    ``CBS_JACOBIAN_ENTRY`` (or evaluates cached ``Expr`` trees in Python).

Run from the repository root::

    bazel run //python:irs_scaling
    bazel run //python:irs_scaling -- --sizes 1 10 100 --ticks 5
    bazel run //python:irs_scaling -- --engine pure_duckdb --risk analytic --sizes 1 10 100 250
    bazel run //python:irs_scaling -- --show
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import random
from typing import Any, Sequence

from legend_lite.scalar_parallel import basis_pure as pure
from pricing.demo.irs_pricing import (
    DISC_CURVE,
    IRS_QUOTES,
    OIS_QUOTES,
    PILLARS,
    PROJ_CURVE,
    _bootstrap_env,
)
from pricing.engines import PureBasisEngine, PythonEngineExpr, TickResult, resolve_engine
from pricing.instruments import IRSwapFixedFloat, IRSwapFixedFloatApprox, Instrument, Portfolio
from pricing.marketmodels import MultiCurveFitter, Pillar, ZeroCurve
from pricing.risk import BP, NPV, FirstOrderAnalyticRisk, FirstOrderNumericalRisk, quote_risk_query, resolve_risk_method

_bootstrap_env()

DISCOUNT_CURVE = ZeroCurve(DISC_CURVE, PILLARS)
PROJECTION_CURVE = ZeroCurve(PROJ_CURVE, PILLARS)


ROUTES: tuple[tuple[str, str, str], ...] = (
    ('python', 'analytic', 'Python Expr + Analytic Risk (Symbolic DAG + Jacobian)'),
    ('python', 'bump_and_grind', 'Python Expr + Bump & Grind (12 Quote Refits & Reprice)'),
    ('pure_duckdb', 'analytic', 'Pure/DuckDB SQL + Python Fitter + Analytic Risk (Basis SQL + SQL Chain Rule)'),
    ('pure_duckdb', 'bump_and_grind', 'Pure/DuckDB SQL + Bump & Grind (Basis SQL x 13 Scenarios)'),
)

SQL_FITTER_ROUTES: tuple[tuple[str, str, str], ...] = (
    ('pure_duckdb_sql_fit', 'analytic', 'Pure/DuckDB SQL + Skinny SQL Fitter + Analytic Risk'),
)


@dataclass(frozen=True)
class RouteScalingSummary:
    """Timing and reconciliation summary for one ``(portfolio_size, engine, risk_method)`` route across market ticks."""

    trade_count: int
    engine_name: str
    risk_method_name: str
    route_label: str
    shared_nodes: int
    basis_terms: int
    first_build_ms: float
    first_fit_ms: float
    first_eval_ms: float
    first_total_ms: float
    warm_ticks: int
    warm_fit_ms: float
    warm_eval_ms: float
    warm_total_ms: float
    speedup_vs_first: float
    first_tick: TickResult
    last_tick: TickResult
    max_npv_diff_vs_ref: float = 0.0
    max_risk_diff_vs_ref: float = 0.0


BUSINESS_DAYS_PER_YEAR = 255


def make_scaling_portfolio(
    n_trades: int,
    discount_curve: ZeroCurve = DISCOUNT_CURVE,
    projection_curve: ZeroCurve = PROJECTION_CURVE,
    seed: int = 42,
) -> Portfolio:
    """Generates a deterministic portfolio of ``n_trades`` full two-curve ``IRSwapFixedFloat`` swaps.

    Each swap's maturity is chosen randomly on a ``1/255``-year business-day grid between 1Y (255 days) and 10Y
    (2,550 days), and ``ir_scheduling.rack_dates`` marches backwards in 1.0-year steps from that maturity date down to
    ``start = 0.0`` (producing a front stub plus annual periods at ``(end_days - 255*k) / 255``). This creates a rich
    variety of interpolated discount factors on ``discount_curve`` and forward rate fixings on ``projection_curve``.
    """
    rng = random.Random(seed)
    books = ('RATES_NY', 'RATES_LDN', 'RATES_TKY')
    trades: list[Instrument] = []
    for i in range(n_trades):
        book = books[i % len(books)]
        end_days = rng.randint(BUSINESS_DAYS_PER_YEAR, 10 * BUSINESS_DAYS_PER_YEAR)
        tenor_years = round(end_days / float(BUSINESS_DAYS_PER_YEAR), 10)
        side = 'RECEIVER' if rng.random() < 0.5 else 'PAYER'
        notional = float(rng.randint(1, 50)) * 1_000_000.0
        fixed_rate = round(rng.uniform(0.025, 0.048), 6)
        float_spread = round(rng.choice((0.0, 0.0002, 0.0005, -0.0002)), 6)
        trades.append(
            IRSwapFixedFloat(
                symbol=f'IRS_{i + 1:04d}',
                notional=notional,
                fixed_rate=fixed_rate,
                float_spread=float_spread,
                tenor_years=tenor_years,
                start=0.0,
                book=book,
                side=side,
                discount_curve=discount_curve,
                projection_curve=projection_curve,
            )
        )
    return Portfolio(trades=trades, default_curves=(discount_curve, projection_curve))


def make_market_ticks(
    n_ticks: int = 5,
    base_ois: Sequence[float] = OIS_QUOTES,
    base_irs: Sequence[float] = IRS_QUOTES,
) -> list[list[float]]:
    """Generates ``n_ticks`` deterministic market quote vectors (6 OIS + 6 IRS quotes per tick):
      - Tick 0: base market quotes (Cold Start / 1st Market Tick)
      - Ticks 1..n_ticks-1: realistic parallel + slope + curvature market updates (Warm Ticks)
    """
    base = list(base_ois) + list(base_irs)
    ticks = [base]
    n_q = len(base_ois)
    for t in range(1, max(1, n_ticks)):
        parallel = ((t % 3) - 1) * 0.00015 + t * 0.00005
        steepener = (1.0 if t % 2 == 1 else -0.8) * 0.00004
        ois_t = [q + parallel + steepener * (j - 2.5) for j, q in enumerate(base_ois)]
        irs_t = [q + parallel * 1.05 + steepener * (j - 2.5) for j, q in enumerate(base_irs)]
        ticks.append(ois_t + irs_t)
    return ticks[: max(1, n_ticks)]


def run_scaling_benchmark(
    sizes: Sequence[int] = (1, 10, 100, 1000),
    n_ticks: int = 5,
    routes: Sequence[tuple[str, str, str]] = ROUTES,
    bump_method: str = 'forward',
) -> list[RouteScalingSummary]:
    """Runs the 1st-tick vs. warm-tick scaling benchmark across ``sizes`` and ``routes``."""
    fitter = MultiCurveFitter(DISCOUNT_CURVE, OIS_QUOTES, PROJECTION_CURVE, IRS_QUOTES)
    ticks = make_market_ticks(n_ticks)
    summaries: list[RouteScalingSummary] = []

    # Hold one shared ComputeBasis instance so native compiler startup is amortized across sizes, while each session
    # plans its queries and builds its own DuckDB tables on Tick 1.
    with pure.ComputeBasis() as shared_cb:
        # Warm up native compiler library once before timed runs so 1-swap 1st-tick measures actual query planning
        # rather than one-time shared library initialization.
        shared_cb.plan(pure.measures_query('BASE', by_entity=True, measure_code=NPV))
        shared_cb.plan(quote_risk_query('BASE'))
        shared_cb._plans.clear()

        for n_trades in sizes:
            portfolio = make_scaling_portfolio(n_trades)
            ref_first: TickResult | None = None
            ref_last: TickResult | None = None

            for engine_key, risk_key, label in routes:
                risk_cls = resolve_risk_method(risk_key)
                session_kwargs: dict[str, Any] = {'risk_method': risk_cls, 'bump_method': bump_method}
                if engine_key == 'python':
                    engine = PythonEngineExpr()
                else:
                    # Fresh ComputeBasis plan cache per session so 1st-tick includes Pure-to-SQL query planning,
                    # reusing the already-loaded HeldModel.
                    shared_cb._plans.clear()
                    engine = PureBasisEngine(cb=shared_cb)
                    if engine_key == 'pure_duckdb_sql_fit':
                        session_kwargs['use_sql_fitter'] = True

                with engine.session(portfolio, fitter, **session_kwargs) as sess:
                    tick_results = [sess.tick(q) for q in ticks]

                first = tick_results[0]
                warm = tick_results[1:] if len(tick_results) > 1 else [first]
                warm_count = len(tick_results) - 1
                warm_fit = sum(r.fit_ms for r in warm) / len(warm)
                warm_eval = sum(r.eval_ms for r in warm) / len(warm)
                warm_total = sum(r.total_ms for r in warm) / len(warm)
                speedup = first.total_ms / warm_total if warm_total > 0 else 1.0

                if ref_first is None:
                    ref_first = first
                    ref_last = tick_results[-1]

                assert ref_last is not None
                last = tick_results[-1]
                max_npv_diff = max(
                    (abs(last.npv[k] - ref_last.npv[k]) for k in ref_last.npv),
                    default=0.0,
                )
                max_risk_diff = max(
                    (abs(last.quote_risk[k] - ref_last.quote_risk[k]) for k in ref_last.quote_risk),
                    default=0.0,
                )

                summaries.append(
                    RouteScalingSummary(
                        trade_count=n_trades,
                        engine_name=engine_key,
                        risk_method_name=risk_key,
                        route_label=label,
                        shared_nodes=first.shared_nodes,
                        basis_terms=first.basis_terms,
                        first_build_ms=first.build_ms,
                        first_fit_ms=first.fit_ms,
                        first_eval_ms=first.eval_ms,
                        first_total_ms=first.total_ms,
                        warm_ticks=warm_count,
                        warm_fit_ms=warm_fit,
                        warm_eval_ms=warm_eval,
                        warm_total_ms=warm_total,
                        speedup_vs_first=speedup,
                        first_tick=first,
                        last_tick=last,
                        max_npv_diff_vs_ref=max_npv_diff,
                        max_risk_diff_vs_ref=max_risk_diff,
                    )
                )

    return summaries


def scaling_frames(summaries: Sequence[RouteScalingSummary]) -> dict[str, Any]:
    """Builds Arrow tables from ``RouteScalingSummary`` results for DataCube visualization."""
    import pyarrow as pa

    summary_rows = []
    phase_rows = []
    for s in summaries:
        route_short = f'{s.engine_name} + {s.risk_method_name}'
        summary_rows.append(
            {
                'TRADES': s.trade_count,
                'SIZE_LABEL': f'{s.trade_count} Swap' + ('s' if s.trade_count != 1 else ''),
                'ENGINE': s.engine_name,
                'RISK_METHOD': s.risk_method_name,
                'ROUTE': route_short,
                'SHARED_NODES': s.shared_nodes,
                'BASIS_TERMS': s.basis_terms,
                'FIRST_BUILD_MS': round(s.first_build_ms, 2),
                'FIRST_FIT_MS': round(s.first_fit_ms, 2),
                'FIRST_EVAL_MS': round(s.first_eval_ms, 2),
                'FIRST_TOTAL_MS': round(s.first_total_ms, 2),
                'WARM_FIT_MS': round(s.warm_fit_ms, 2),
                'WARM_EVAL_MS': round(s.warm_eval_ms, 2),
                'WARM_TOTAL_MS': round(s.warm_total_ms, 2),
                'WARM_SPEEDUP_X': round(s.speedup_vs_first, 2),
                'MAX_NPV_DIFF': round(s.max_npv_diff_vs_ref, 6),
                'MAX_DV01_DIFF': round(s.max_risk_diff_vs_ref, 4),
            }
        )
        for tick_label, total_ms, eval_ms in (
            ('1st Market Tick (Cold)', s.first_total_ms, s.first_eval_ms),
            ('Later Market Tick (Warm)', s.warm_total_ms, s.warm_eval_ms),
        ):
            phase_rows.append(
                {
                    'TRADES': s.trade_count,
                    'SIZE_LABEL': f'{s.trade_count:3d} Swaps' if s.trade_count > 1 else '  1 Swap',
                    'ROUTE': route_short,
                    'ENGINE': s.engine_name,
                    'RISK_METHOD': s.risk_method_name,
                    'TICK_PHASE': tick_label,
                    'TOTAL_MS': round(total_ms, 2),
                    'EVAL_MS': round(eval_ms, 2),
                }
            )
    return {
        'scaling_summary': pa.Table.from_pylist(summary_rows),
        'scaling_phases': pa.Table.from_pylist(phase_rows),
    }


def scaling_page(summaries: Sequence[RouteScalingSummary]) -> Any:
    """Builds a DataCube ``ll.Page`` comparing 1st market tick vs. warm market updates across portfolio sizes and
    execution routes."""
    import legend_lite as ll

    frames = scaling_frames(summaries)
    page = ll.Page('IRS Pricing & Risk Scaling: Python vs Pure/DuckDB SQL')
    sheet = page.sheets[0]
    sheet.name = '1. Scaling & Market Tick Latencies'

    summary_grid = sheet.grid(
        frames['scaling_summary'],
        name='scaling_summary',
        title='1st Market Tick (Cold) vs Later Market Updates (Warm) by Portfolio Size & Route (ms)',
        sort=[('TRADES', 'asc'), ('ENGINE', 'asc'), ('RISK_METHOD', 'asc')],
    )
    summary_grid.data_bars('FIRST_TOTAL_MS')
    summary_grid.data_bars('WARM_TOTAL_MS')
    summary_grid.chart(
        'bar',
        x='SIZE_LABEL',
        y=[('FIRST_TOTAL_MS', 'sum'), ('WARM_TOTAL_MS', 'sum')],
        split='ROUTE',
        title='1st Market Tick vs Warm Market Update Total Latency (ms)',
    )

    phases_grid = sheet.grid(
        frames['scaling_phases'],
        name='scaling_phases',
        title='Tick Phase Breakdown: Total vs Engine Evaluation Latency (ms)',
        sort=[('TRADES', 'asc'), ('ROUTE', 'asc')],
    )
    phases_grid.chart(
        'bar',
        x='SIZE_LABEL',
        y=[('EVAL_MS', 'sum')],
        split='ROUTE',
        title='Engine Evaluation Latency Only (Excluding Curve Fit, ms)',
    )
    return page


def format_scaling_report(summaries: Sequence[RouteScalingSummary]) -> str:
    """Formats a human-readable console report comparing 1st Market Tick vs. Later Market Updates across portfolio sizes."""
    lines: list[str] = []
    lines.append('=' * 124)
    lines.append(
        'IRS PRICING & RISK SCALING BENCHMARK: 1ST MARKET TICK (COLD) vs LATER MARKET UPDATES (WARM)'
    )
    lines.append('=' * 124)
    header = (
        f"{'Swaps':>5}  {'Engine':<11}  {'Risk Method':<14}  {'Nodes':>6}  {'Terms':>7}  "
        f"{'1st:Build':>9}  {'1st:Fit':>8}  {'1st:Eval':>8}  {'1st:Total':>9}  "
        f"{'Warm:Fit':>8}  {'Warm:Eval':>9}  {'Warm:Tot':>8}  {'Speedup':>7}"
    )
    lines.append(header)
    lines.append('-' * 124)

    current_size: int | None = None
    for s in summaries:
        if current_size is not None and s.trade_count != current_size:
            lines.append('-' * 124)
        current_size = s.trade_count
        lines.append(
            f"{s.trade_count:>5d}  {s.engine_name:<19}  {s.risk_method_name:<14}  "
            f"{s.shared_nodes:>6d}  {s.basis_terms:>7d}  "
            f"{s.first_build_ms:>8.2f}ms  {s.first_fit_ms:>7.2f}ms  {s.first_eval_ms:>7.2f}ms  "
            f"{s.first_total_ms:>8.2f}ms  "
            f"{s.warm_fit_ms:>7.2f}ms  {s.warm_eval_ms:>8.2f}ms  {s.warm_total_ms:>7.2f}ms  "
            f"{s.speedup_vs_first:>6.2f}x"
        )
    lines.append('=' * 132)

    # Reconciliation & Warm-Eval Scaling Analysis
    lines.append('\nRECONCILIATION & SCALING HIGHLIGHTS:')
    by_size: dict[int, dict[tuple[str, str], RouteScalingSummary]] = {}
    for s in summaries:
        by_size.setdefault(s.trade_count, {})[(s.engine_name, s.risk_method_name)] = s

    for n_trades, group in by_size.items():
        py_an = group.get(('python', 'analytic'))
        db_an = group.get(('pure_duckdb', 'analytic'))
        db_sql = group.get(('pure_duckdb_sql_fit', 'analytic'))
        py_bg = group.get(('python', 'bump_and_grind'))
        db_bg = group.get(('pure_duckdb', 'bump_and_grind'))
        parts = [f'  * {n_trades:>4d} swap(s):']
        if py_an and db_an:
            an_speedup = py_an.warm_eval_ms / db_an.warm_eval_ms if db_an.warm_eval_ms > 0 else 1.0
            parts.append(
                f'Analytic NPV diff={db_an.max_npv_diff_vs_ref:.2e}, DV01 diff={db_an.max_risk_diff_vs_ref:.2e} | '
                f'Analytic Warm Eval: Python={py_an.warm_eval_ms:.2f}ms vs DuckDB={db_an.warm_eval_ms:.2f}ms '
                f'({an_speedup:.2f}x) |'
            )
        if db_sql:
            parts.append(
                f'SQL Fitter: Warm Fit={db_sql.warm_fit_ms:.2f}ms, Warm Eval={db_sql.warm_eval_ms:.2f}ms, '
                f'NPV diff={db_sql.max_npv_diff_vs_ref:.2e}, DV01 diff={db_sql.max_risk_diff_vs_ref:.2e} |'
            )
        if py_bg and db_bg:
            bg_speedup = py_bg.warm_eval_ms / db_bg.warm_eval_ms if db_bg.warm_eval_ms > 0 else 1.0
            parts.append(
                f'Bump&Grind Warm Eval: Python={py_bg.warm_eval_ms:.2f}ms vs DuckDB={db_bg.warm_eval_ms:.2f}ms '
                f'({bg_speedup:.2f}x)'
            )
        lines.append(' '.join(parts))

    return '\n'.join(lines)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--sizes',
        type=int,
        nargs='+',
        default=[1, 10, 100, 1000],
        help='Portfolio sizes (number of swaps) to benchmark (default: 1 10 100 1000)',
    )
    parser.add_argument(
        '--ticks',
        type=int,
        default=5,
        help='Total market ticks to simulate (1 cold tick + N-1 warm ticks, default: 5)',
    )
    parser.add_argument(
        '--engine',
        choices=('all', 'python', 'pure_duckdb'),
        default='all',
        help='Filter to a single execution engine or compare all (default: all)',
    )
    parser.add_argument(
        '--risk',
        choices=('all', 'analytic', 'bump_and_grind'),
        default='all',
        help='Filter to a single risk method or compare all (default: all)',
    )
    parser.add_argument(
        '--bump-method',
        choices=('forward', 'central'),
        default='forward',
        help='Finite-difference scheme for FirstOrderNumericalRisk (default: forward)',
    )
    parser.add_argument(
        '--sql-fitter',
        choices=('off', 'on', 'compare'),
        default='off',
        help='Include skinny SQL curve fitter route: off (default), on (SQL fitter only), or compare (all 3)',
    )
    parser.add_argument(
        '--show',
        action='store_true',
        help='Open the interactive DataCube scaling dashboard in a browser tab',
    )
    parser.add_argument(
        '--save',
        metavar='PATH',
        help='Write the DataCube scaling page JSON to PATH',
    )
    args = parser.parse_args(argv)

    base_routes = list(ROUTES)
    if args.sql_fitter == 'on':
        base_routes = list(SQL_FITTER_ROUTES)
    elif args.sql_fitter == 'compare':
        base_routes = [ROUTES[0], ROUTES[2], SQL_FITTER_ROUTES[0]]

    selected_routes = [
        r
        for r in base_routes
        if (args.engine == 'all' or r[0].startswith(args.engine)) and (args.risk == 'all' or r[1] == args.risk)
    ]
    summaries = run_scaling_benchmark(
        sizes=args.sizes,
        n_ticks=args.ticks,
        routes=selected_routes,
        bump_method=args.bump_method,
    )
    print(format_scaling_report(summaries))

    if args.save or args.show:
        page = scaling_page(summaries)
        if args.save:
            page.save(args.save)
            print(f'\nsaved DataCube scaling page -> {args.save}')
        if args.show:
            import legend_lite as ll

            cube = ll.show(page, open_browser=True)
            print(f'serving {cube.url}  (Ctrl-C to stop)')
            try:
                import signal

                signal.pause()
            except (KeyboardInterrupt, AttributeError):
                cube.close()


if __name__ == '__main__':
    main()
