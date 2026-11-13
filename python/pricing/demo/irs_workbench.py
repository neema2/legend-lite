"""Interactive Swap Pricing & Risk Workbench on ``legend_lite.scalar_parallel`` and DataCube.

Allows a user (in a Jupyter/Marimo notebook, an interactive REPL, or from the CLI) to:
  1. Input a specific set of benchmark OIS & IRS quotes and one or more swaps to price (``IRSwapFixedFloat`` or
     ``IRSwapFixedFloatApprox``).
  2. Choose the **Execution Engine** (``PureBasisEngine`` producing Pure & SQL and executing in DuckDB vs.
     ``PythonEngineExpr`` evaluating ``expr.Expr`` trees locally in Python).
  3. Compare **Shared Risk Definitions**: ``FirstOrderAnalyticRisk`` (symbolic pillar differentiation + Jacobian chain
     rule) vs. ``FirstOrderNumericalRisk`` (naive bump of each input quote with curve refit and portfolio reprice).
  4. Live-update an open DataCube ``ll.Page`` whenever quotes, swap terms, or engine choices change.

Run from the terminal:

    bazel run //python:irs_workbench -- --notional 25000000 --fixed-bps 365 --spread-bps 5 --start 0.5 --tenor 5 --show

Or drop into an interactive Python session connected to a live DataCube browser tab:

    bazel run //python:irs_workbench -- --repl --show
"""

from __future__ import annotations

import argparse
import code
from typing import Any, Mapping, Sequence
import sys

from pricing.demo.irs_pricing import (
    BUMP,
    DISC_CURVE,
    IRS_QUOTES,
    OIS_QUOTES,
    PILLARS,
    PROJ_CURVE,
    _bootstrap_env,
)

_bootstrap_env()

import pyarrow as pa  # noqa: E402

import legend_lite as ll  # noqa: E402
from legend_lite.scalar_parallel import basis_pure as pure  # noqa: E402
from pricing.demo.dashboard import irs_frames, irs_page  # noqa: E402
from pricing.engines import ExecutionEngine, PureBasisEngine, PythonEngineExpr, resolve_engine  # noqa: E402
from pricing.instruments import IRSwapFixedFloat, IRSwapFixedFloatApprox  # noqa: E402
from pricing.marketmodels import MultiCurveFitter, Pillar, ZeroCurve  # noqa: E402
from pricing.risk import (  # noqa: E402
    BP,
    FirstOrderAnalyticRisk,
    FirstOrderNumericalRisk,
    RiskRun,
    SwapInstrument,
    risk_run,
)


def _to_rate(val: float) -> float:
    """Accepts either a decimal rate (0.0365) or basis points (365.0) if |val| >= 1.0."""
    return val * 1e-4 if abs(val) >= 1.0 else float(val)


class IRSWorkbench:
    """Interactive workbench holding user-supplied benchmark quotes, swap(s), and engine choice (``PureBasisEngine`` vs.
    ``PythonEngineExpr``), evaluating both ``FirstOrderAnalyticRisk`` and ``FirstOrderNumericalRisk`` and live-updating an open
    DataCube ``ll.Page``."""

    def __init__(
        self,
        pillars: Sequence[Pillar] = PILLARS,
        ois_quotes: Sequence[float] = OIS_QUOTES,
        irs_quotes: Sequence[float] = IRS_QUOTES,
        swaps: Sequence[SwapInstrument] | None = None,
        include_calibration_book: bool = False,
        engine: str | ExecutionEngine = 'pure_duckdb',
    ) -> None:
        self.pillars = list(pillars)
        self.disc_curve = ZeroCurve(DISC_CURVE, self.pillars)
        self.proj_curve = ZeroCurve(PROJ_CURVE, self.pillars)
        self.curves = [self.disc_curve, self.proj_curve]
        self.ois_quotes = [_to_rate(q) for q in ois_quotes]
        self.irs_quotes = [_to_rate(q) for q in irs_quotes]
        self.include_calibration_book = include_calibration_book
        self._cb = pure.ComputeBasis()
        self.engine: ExecutionEngine = (
            PureBasisEngine(self._cb)
            if resolve_engine(engine).engine_name == 'pure_duckdb'
            else PythonEngineExpr()
        )
        self.swaps: list[SwapInstrument] = list(swaps) if swaps is not None else [
            IRSwapFixedFloat(
                symbol='DEAL_5Y_FWD',
                book='CLIENT_DESK',
                notional=25_000_000.0,
                fixed_rate=0.0365,
                float_spread=0.0005,
                start=0.5,
                tenor_years=5.0,
                side='RECEIVER',
                discount_curve=self.disc_curve,
                projection_curve=self.proj_curve,
            )
        ]
        self.fitter: MultiCurveFitter
        self.run: RiskRun
        self.frames: dict[str, pa.Table] = {}
        self.page: ll.Page | None = None
        self.view: Any = None
        self.recalculate(verbose=False)

    def close(self) -> None:
        self._cb.close()

    def set_engine(self, engine: str | ExecutionEngine, *, verbose: bool = True) -> dict[str, pa.Table]:
        """Switches the workbench between ``'pure_duckdb'`` (``PureBasisEngine``) and ``'python'``
        (``PythonEngineExpr``) and recalculates."""
        resolved = resolve_engine(engine)
        self.engine = PureBasisEngine(self._cb) if resolved.engine_name == 'pure_duckdb' else PythonEngineExpr()
        return self.recalculate(verbose=verbose)

    def pure_queries(self) -> dict[str, str]:
        """Returns the Pure query strings used by ``PureBasisEngine``."""
        return PureBasisEngine.pure_queries('BASE')

    def plan_sql(self, dialect: str = 'duckdb') -> dict[str, str]:
        """Compiles the Pure basis queries to SQL for ``dialect`` (e.g. ``'duckdb'``, ``'postgres'``, ``'snowflake'``,
        ``'bigquery'``, ``'databricks'``, ``'trino'``, ``'sqlite'``, ``'clickhouse'``)."""
        return PureBasisEngine(self._cb).plan_sql(dialect=dialect, scenario_id='BASE')

    def _all_trades(self) -> list[SwapInstrument]:
        trades = list(self.swaps)
        if self.include_calibration_book:
            from pricing.demo.irs_pricing import par_swaps
            trades = par_swaps(self.disc_curve, self.ois_quotes, self.proj_curve, self.irs_quotes) + trades
        return trades

    def recalculate(self, *, verbose: bool = True) -> dict[str, pa.Table]:
        """Fits both curves to the current benchmark quotes, evaluates prices and risk (both ``FirstOrderAnalyticRisk``
        and ``FirstOrderNumericalRisk``), and updates any open DataCube ``ll.Page`` in place."""
        trades = self._all_trades()
        self.fitter = MultiCurveFitter(self.disc_curve, self.ois_quotes, self.proj_curve, self.irs_quotes)
        self.run = risk_run(
            self.curves,
            self.fitter,
            trades,
            [(BUMP, BP)],
            'USD',
            engine=self.engine,
            include_bump_and_grind=True,
        )
        self.frames = irs_frames(self.curves, self.fitter, trades, self.run, self._cb)
        if self.page is not None:
            session = self.page._session
            seen_frames: set[str] = set()
            for grid in self.page.grids:
                if (
                    not grid.inputs
                    and len(grid.frames) == 1
                    and grid.frame in self.frames
                    and grid.frame not in seen_frames
                ):
                    fname = grid.frame
                    seen_frames.add(fname)
                    new_tbl = self.frames[fname]
                    if fname in session.frames and getattr(session.frames[fname].given, 'schema', None) == new_tbl.schema:
                        session.register(new_tbl, fname)
                    else:
                        grid.update(new_tbl)
            cached = [g for g in self.page.grids if g.cached]
            for g in sorted(cached, key=lambda box: len(box._chain_key())):
                g.snap()
                self.page._cache_keys[g.id] = g._chain_key()
        if verbose:
            self.print_report()
        return self.frames

    def set_quotes(
        self,
        ois_quotes: Sequence[float] | Mapping[str, float] | None = None,
        irs_quotes: Sequence[float] | Mapping[str, float] | None = None,
        *,
        verbose: bool = True,
    ) -> dict[str, pa.Table]:
        """Replaces the OIS and/or IRS benchmark quotes (accepts decimal 0.0350 or bps 350.0) and recalculates."""
        label_idx = {p.label.upper(): i for i, p in enumerate(self.pillars)}
        if ois_quotes is not None:
            if isinstance(ois_quotes, Mapping):
                for k, v in ois_quotes.items():
                    lbl = k.upper().removeprefix('OIS_')
                    self.ois_quotes[label_idx[lbl]] = _to_rate(v)
            else:
                self.ois_quotes = [_to_rate(q) for q in ois_quotes]
        if irs_quotes is not None:
            if isinstance(irs_quotes, Mapping):
                for k, v in irs_quotes.items():
                    lbl = k.upper().removeprefix('IRS_')
                    self.irs_quotes[label_idx[lbl]] = _to_rate(v)
            else:
                self.irs_quotes = [_to_rate(q) for q in irs_quotes]
        return self.recalculate(verbose=verbose)

    def set_quote(self, quote_id: str, rate_or_bps: float, *, verbose: bool = True) -> dict[str, pa.Table]:
        """Sets a single benchmark quote such as ``'OIS_5Y'`` or ``'IRS_5Y'`` (in bps like ``385.0`` or decimal
        ``0.0385``) and recalculates."""
        key = quote_id.upper()
        label_idx = {p.label.upper(): i for i, p in enumerate(self.pillars)}
        if key.startswith('OIS_'):
            self.ois_quotes[label_idx[key.removeprefix('OIS_')]] = _to_rate(rate_or_bps)
        elif key.startswith('IRS_'):
            self.irs_quotes[label_idx[key.removeprefix('IRS_')]] = _to_rate(rate_or_bps)
        else:
            raise ValueError(f"Unknown quote_id {quote_id!r}; expected 'OIS_<tenor>' or 'IRS_<tenor>'")
        return self.recalculate(verbose=verbose)

    def bump_quote(self, quote_id: str, bump_bps: float, *, verbose: bool = True) -> dict[str, pa.Table]:
        """Bumps a single benchmark quote (e.g. ``'IRS_5Y'``) by ``bump_bps`` basis points and recalculates."""
        key = quote_id.upper()
        label_idx = {p.label.upper(): i for i, p in enumerate(self.pillars)}
        if key.startswith('OIS_'):
            idx = label_idx[key.removeprefix('OIS_')]
            self.ois_quotes[idx] = round(self.ois_quotes[idx] + bump_bps * BP, 10)
        elif key.startswith('IRS_'):
            idx = label_idx[key.removeprefix('IRS_')]
            self.irs_quotes[idx] = round(self.irs_quotes[idx] + bump_bps * BP, 10)
        else:
            raise ValueError(f"Unknown quote_id {quote_id!r}; expected 'OIS_<tenor>' or 'IRS_<tenor>'")
        return self.recalculate(verbose=verbose)

    def reset_quotes(self, *, verbose: bool = True) -> dict[str, pa.Table]:
        """Restores the default OIS and IRS benchmark quotes."""
        self.ois_quotes = list(OIS_QUOTES)
        self.irs_quotes = list(IRS_QUOTES)
        return self.recalculate(verbose=verbose)

    def make_swap(
        self,
        symbol: str = 'DEAL_1',
        *,
        book: str = 'CLIENT_DESK',
        notional: float = 25_000_000.0,
        fixed_rate_bps: float = 365.0,
        float_spread_bps: float = 5.0,
        start: float = 0.5,
        tenor_years: float = 5.0,
        side: str = 'RECEIVER',
        two_curve: bool = True,
    ) -> SwapInstrument:
        """Creates either a 2-curve ``IRSwapFixedFloat`` or a 1-curve ``IRSwapFixedFloatApprox``."""
        fixed_rate = _to_rate(fixed_rate_bps)
        spread = float_spread_bps * BP if abs(float_spread_bps) >= 0.01 else float(float_spread_bps)
        if two_curve:
            return IRSwapFixedFloat(
                symbol=symbol,
                book=book,
                notional=float(notional),
                fixed_rate=fixed_rate,
                float_spread=spread,
                start=float(start),
                tenor_years=float(tenor_years),
                side=side.upper(),
                discount_curve=self.disc_curve,
                projection_curve=self.proj_curve,
            )
        return IRSwapFixedFloatApprox(
            symbol=symbol,
            book=book,
            notional=float(notional),
            fixed_rate=fixed_rate,
            start=float(start),
            tenor_years=float(tenor_years),
            side=side.upper(),
            curve=self.disc_curve,
        )

    def price_swap(
        self,
        symbol: str = 'DEAL_1',
        *,
        book: str = 'CLIENT_DESK',
        notional: float = 25_000_000.0,
        fixed_rate_bps: float = 365.0,
        float_spread_bps: float = 5.0,
        start: float = 0.5,
        tenor_years: float = 5.0,
        side: str = 'RECEIVER',
        two_curve: bool = True,
        verbose: bool = True,
    ) -> dict[str, pa.Table]:
        """Replaces the active swap with the specified parameters, prices it and computes its risk."""
        self.swaps = [
            self.make_swap(
                symbol=symbol,
                book=book,
                notional=notional,
                fixed_rate_bps=fixed_rate_bps,
                float_spread_bps=float_spread_bps,
                start=start,
                tenor_years=tenor_years,
                side=side,
                two_curve=two_curve,
            )
        ]
        return self.recalculate(verbose=verbose)

    def add_swap(self, swap: SwapInstrument | None = None, *, verbose: bool = True, **kwargs: Any) -> dict[str, pa.Table]:
        """Adds another swap to the workbench portfolio and recalculates."""
        if swap is None:
            symbol = kwargs.pop('symbol', f'DEAL_{len(self.swaps) + 1}')
            swap = self.make_swap(symbol=symbol, **kwargs)
        self.swaps.append(swap)
        return self.recalculate(verbose=verbose)

    def print_report(self) -> None:
        """Prints a formatted summary of the input benchmark quotes, swap valuation, pillar deltas, and Analytic vs.
        Bump & Grind quote risk."""
        print('=' * 104)
        print(f'IRS WORKBENCH [{self.engine.engine_name}]: {DISC_CURVE} (Discount) & {PROJ_CURVE} (Projection) | '
              f'Base fit: {self.run.fit.iterations} Newton steps (+{self.run.total_bump_newton_steps} steps for '
              f'{len(self.run.bump_fits)} bump&refit quotes) | '
              f'{len(self.run.book.nodes)} basis nodes, {self.run.book.term_count()} terms')
        print('=' * 104)

        print('\n1. Input Benchmark Quotes & Fitted Zero Curves:')
        print(f"  {'Curve':<14} {'Quote ID':<10} {'Tenor':>6} {'Mkt Par (bps)':>14} {'Zero Rate (bps)':>16} "
              f"{'Discount Factor':>16} {'Fit Error (bps)':>16}")
        for row in self.frames['market_quotes'].to_pylist():
            print(f"  {row['curve_id']:<14} {row['quote_id']:<10} {row['tenor_years']:>5.1f}Y "
                  f"{row['market_quote_bps']:>14.2f} {row['fitted_zero_rate_bps']:>16.2f} "
                  f"{row['discount_factor']:>16.6f} {row['fit_error_bps']:>16.2e}")

        print('\n2. Input Swap(s) & Output Valuation (Pure/SQL Basis vs Local Python):')
        print(f"  {'Trade ID':<14} {'Swap Type':<26} {'Side':<14} {'Notional':>14} {'Fixed (bps)':>12} "
              f"{'Spread (bps)':>12} {'Window':>12} {'SQL NPV ($)':>15} {'Python NPV ($)':>15} {'Sum DV01 ($)':>13}")
        for row in self.frames['swap_portfolio'].to_pylist():
            if row['book'] == 'CALIBRATION':
                continue
            win = f"{row['start_years']:.2f}Y->{row['end_years']:.2f}Y"
            print(f"  {row['trade_id']:<14} {row['swap_type']:<26} {row['direction']:<14} "
                  f"{row['notional']:>14,.0f} {row['fixed_rate_bps']:>12.2f} {row['float_spread_bps']:>12.2f} "
                  f"{win:>12} {row['sql_npv']:>15,.2f} {row['python_npv']:>15,.2f} "
                  f"{row['analytic_dv01_sum']:>13,.2f}")

        print('\n3. Output Zero-Rate Pillar Deltas (dNPV / dZeroRate, $/bp):')
        for row in self.frames['pillar_risk'].to_pylist():
            if row['book'] == 'CALIBRATION':
                continue
            print(f"  {row['trade_id']:<14} {row['pillar']:<26} SQL Delta: {row['sql_delta']:>12,.2f} $/bp "
                  f"| Python Delta: {row['python_delta']:>12,.2f} $/bp (diff: {row['sql_py_diff']:>+.2e})")

        print('\n4. Output Par-Quote Risk Ladder: FirstOrderAnalyticRisk vs FirstOrderNumericalRisk ($/bp):')
        quotes = [q.quote_id for q in self.fitter.quote_specs]
        q_sql: dict[tuple[str, str], float] = {}
        q_py: dict[tuple[str, str], float] = {}
        q_bg: dict[tuple[str, str], float] = {}
        for row in self.frames['quote_risk'].to_pylist():
            q_id = row['quote_id'].split('(')[-1].rstrip(')')
            q_sql[(row['book'], q_id)] = row['sql_quote_dv01']
            q_py[(row['book'], q_id)] = row['python_quote_dv01']
            q_bg[(row['book'], q_id)] = row['bump_reprice_dv01']
        books = sorted({row['book'] for row in self.frames['quote_risk'].to_pylist() if row['book'] != 'CALIBRATION'})
        print(f"  {'Book / Risk Method':<26}" + ''.join(f'{q:>10}' for q in quotes))
        for b in books:
            print(f"  {b + ' (SQL Analytic)':<26}" + ''.join(f'{q_sql.get((b, q), 0.0):>10,.1f}' for q in quotes))
            print(f"  {b + ' (Py Analytic)':<26}" + ''.join(f'{q_py.get((b, q), 0.0):>10,.1f}' for q in quotes))
            print(f"  {b + ' (Bump & Refit)':<26}" + ''.join(f'{q_bg.get((b, q), 0.0):>10,.1f}' for q in quotes))
        print()

    def build_page(self) -> ll.Page:
        """Builds (or returns) the interactive 5-sheet Flow + Board DataCube ``ll.Page`` bound to this workbench."""
        if self.page is None:
            self.page = irs_page(
                self.frames,
                curve_id=f'{DISC_CURVE} & {PROJ_CURVE}',
                fitter=self.fitter,
                iterations=self.run.fit.iterations,
                shared_nodes=len(self.run.book.nodes),
                basis_terms=self.run.book.term_count(),
            )
        return self.page

    def show(self, *, browser: bool = True, inline: bool | None = None) -> Any:
        """Shows the workbench's DataCube ``ll.Page`` (under the cell in a notebook, or in a browser tab)."""
        page = self.build_page()
        self.view = ll.show(page, browser=browser, inline=inline)
        return self.view

    def widget(self) -> Any:
        """In a Jupyter notebook with ``ipywidgets`` and ``anywidget`` installed, returns an interactive control panel
        (swap parameters + engine choice + OIS/IRS benchmark quote inputs) stacked above the live DataCube ``PageCube``
        widget."""
        import ipywidgets as widgets
        from legend_lite.notebook import PageCube

        s0 = self.swaps[0]
        is_two_curve = isinstance(s0, IRSwapFixedFloat)

        w_engine = widgets.Dropdown(
            options=[('Pure -> SQL -> DuckDB (PureBasisEngine)', 'pure_duckdb'),
                     ('Local Python Expr (PythonEngineExpr)', 'python')],
            value=self.engine.engine_name,
            description='Engine:',
        )
        w_type = widgets.Dropdown(
            options=[('2-Curve Explicit (IRSwapFixedFloat)', True), ('1-Curve Approx (IRSwapFixedFloatApprox)', False)],
            value=is_two_curve,
            description='Model:',
        )
        w_side = widgets.Dropdown(
            options=['RECEIVER', 'PAYER'],
            value='RECEIVER' if s0.receive_fixed else 'PAYER',
            description='Side:',
        )
        w_notional = widgets.FloatText(value=s0.notional / 1e6, description='Notional ($M):', step=5.0)
        w_fixed = widgets.FloatSlider(
            value=round(s0.fixed_rate * 1e4, 2), min=50.0, max=700.0, step=1.0, description='Fixed (bps):'
        )
        w_spread = widgets.FloatSlider(
            value=round(getattr(s0, 'float_spread', 0.0) * 1e4, 2),
            min=-100.0,
            max=200.0,
            step=1.0,
            description='Spread (bps):',
        )
        w_start = widgets.FloatSlider(value=s0.start, min=0.0, max=5.0, step=0.25, description='Start (Y):')
        w_tenor = widgets.IntSlider(value=s0.years, min=1, max=10, step=1, description='Tenor (Y):')

        ois_boxes = [
            widgets.FloatText(value=round(q * 1e4, 2), description=f'OIS {p.label}:', step=1.0,
                              layout=widgets.Layout(width='155px'))
            for p, q in zip(self.pillars, self.ois_quotes)
        ]
        irs_boxes = [
            widgets.FloatText(value=round(q * 1e4, 2), description=f'IRS {p.label}:', step=1.0,
                              layout=widgets.Layout(width='155px'))
            for p, q in zip(self.pillars, self.irs_quotes)
        ]
        status = widgets.HTML()

        page = self.build_page()
        cube = PageCube(page)

        def _refresh(*_: Any) -> None:
            self.set_engine(w_engine.value, verbose=False)
            self.ois_quotes = [b.value * BP for b in ois_boxes]
            self.irs_quotes = [b.value * BP for b in irs_boxes]
            self.price_swap(
                symbol=s0.trade_id,
                book=s0.book,
                notional=w_notional.value * 1e6,
                fixed_rate_bps=w_fixed.value,
                float_spread_bps=w_spread.value,
                start=w_start.value,
                tenor_years=float(w_tenor.value),
                side=w_side.value,
                two_curve=w_type.value,
                verbose=False,
            )
            row = self.frames['swap_portfolio'].to_pylist()[0]
            status.value = (
                f"<b>{row['trade_id']}</b> ({row['swap_type']}, {row['direction']}, engine=<code>{self.engine.engine_name}</code>) &mdash; "
                f"<b>SQL NPV:</b> ${row['sql_npv']:,.2f} &nbsp;|&nbsp; "
                f"<b>Python NPV:</b> ${row['python_npv']:,.2f} &nbsp;|&nbsp; "
                f"<b>Analytic DV01:</b> ${row['analytic_dv01_sum']:,.2f}/bp &nbsp;|&nbsp; "
                f"<b>1bp Bump:</b> ${row['bump_1bp_change']:,.2f}"
            )

        for w in [w_engine, w_type, w_side, w_notional, w_fixed, w_spread, w_start, w_tenor, *ois_boxes, *irs_boxes]:
            w.observe(_refresh, names='value')
        _refresh()

        return widgets.VBox([
            widgets.HTML('<h4>1. Swap Contract & Execution Engine Inputs</h4>'),
            widgets.HBox([w_engine, w_type, w_side, w_notional]),
            widgets.HBox([w_fixed, w_spread, w_start, w_tenor]),
            widgets.HTML('<h4>2. Benchmark Par Swap Quotes (bps) &mdash; Discount (OIS) & Projection (IRS 3M)</h4>'),
            widgets.HBox(ois_boxes),
            widgets.HBox(irs_boxes),
            status,
            cube,
        ])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--symbol', default='DEAL_5Y_FWD', help='swap trade ID')
    parser.add_argument('--book', default='CLIENT_DESK', help='book name')
    parser.add_argument('--notional', type=float, default=25_000_000.0, help='swap notional ($)')
    parser.add_argument('--fixed-bps', type=float, default=365.0, help='fixed coupon rate in bps')
    parser.add_argument('--spread-bps', type=float, default=5.0, help='floating leg spread in bps (2-curve swap)')
    parser.add_argument('--start', type=float, default=0.5, help='forward start in years (0.0 for spot)')
    parser.add_argument('--tenor', type=float, default=5.0, help='swap tenor in years')
    parser.add_argument('--side', choices=('RECEIVER', 'PAYER'), default='RECEIVER', help='RECEIVER or PAYER')
    parser.add_argument('--approx', action='store_true', help='use 1-curve IRSwapFixedFloatApprox instead of 2-curve')
    parser.add_argument('--engine', choices=('pure_duckdb', 'python'), default='pure_duckdb',
                        help='execution engine: pure_duckdb (Pure -> SQL -> DuckDB) or python (local Expr DAG)')
    parser.add_argument('--ois-bps', type=float, nargs=6, metavar=('1Y', '2Y', '3Y', '5Y', '7Y', '10Y'),
                        help='custom OIS discount curve par quotes in bps')
    parser.add_argument('--irs-bps', type=float, nargs=6, metavar=('1Y', '2Y', '3Y', '5Y', '7Y', '10Y'),
                        help='custom IRS 3M projection curve par quotes in bps')
    parser.add_argument('--show', action='store_true', help='open the live DataCube dashboard in a browser')
    parser.add_argument('--repl', action='store_true',
                        help='open an interactive Python prompt with `wb` (IRSWorkbench) ready for live edits')
    args = parser.parse_args(argv)

    wb = IRSWorkbench(
        ois_quotes=args.ois_bps if args.ois_bps is not None else OIS_QUOTES,
        irs_quotes=args.irs_bps if args.irs_bps is not None else IRS_QUOTES,
        engine=args.engine,
    )
    wb.price_swap(
        symbol=args.symbol,
        book=args.book,
        notional=args.notional,
        fixed_rate_bps=args.fixed_bps,
        float_spread_bps=args.spread_bps,
        start=args.start,
        tenor_years=args.tenor,
        side=args.side,
        two_curve=not args.approx,
        verbose=True,
    )

    if args.show:
        tab = wb.show()
        tab._session.opened = True

    if args.repl:
        banner = (
            '\nInteractive IRS Workbench (`wb`) is ready:\n'
            "  wb.bump_quote('IRS_5Y', +10.0)                      # bump 5Y IRS quote by +10 bps\n"
            "  wb.set_quote('OIS_2Y', 310.0)                       # set 2Y OIS quote to 310 bps\n"
            "  wb.set_engine('python')                             # switch to local Python Expr engine\n"
            "  wb.set_engine('pure_duckdb')                        # switch to Pure -> SQL -> DuckDB engine\n"
            "  wb.plan_sql('snowflake')['measures_by_trade']       # inspect compiled SQL for Snowflake\n"
            '  wb.price_swap(notional=50e6, fixed_rate_bps=380, tenor_years=7, start=0.25)\n'
            '  wb.reset_quotes()\n'
        )
        code.interact(banner=banner, local={'wb': wb, 'll': ll}, exitmsg='')

    return 0


if __name__ == '__main__':
    sys.exit(main())
