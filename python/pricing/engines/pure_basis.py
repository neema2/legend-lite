"""Pure / SQL / DuckDB basis execution engine (``legend_lite.scalar_parallel.basis`` + ``legend_lite.scalar_parallel.basis_pure``).

Compiles instrument ``expr.Expr`` trees and their symbolic derivatives into a ``pure.BasisBook``
(``CBS_SHARED_NODE``, ``CBS_BASIS_TERM``, ``CBS_KNOT``, ``CBS_JACOBIAN_ENTRY``), plans Pure queries to SQL across
any database dialect supported by ``legend-lite``, and executes them on DuckDB.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from legend_lite.scalar_parallel import basis_pure as pure

from ..instruments.base import Instrument
from ..instruments.portfolio import Portfolio
from ..marketmodels.ir_curve_fitter import FactorSpec, Fit
from ..marketmodels.ir_curve_yield import ZeroCurve
from ..risk.base import BP, FitterType, NPV, PILLAR_DELTA, as_portfolio, quote_risk_query, scenario
from ..risk.firstorder_analytic import FirstOrderAnalyticRisk
from ..risk.firstorder_numerical import FirstOrderNumericalRisk
from .base import ExecutionEngine, MarketSession, TickResult
from .python_expr import PythonEngineExpr, is_bump_and_grind

if TYPE_CHECKING:
    from ..risk import RiskRun


class PureBasisMarketSession(MarketSession):
    """Stateful market-tick session for ``PureBasisEngine`` (Pure -> SQL -> DuckDB / ``ll.Frames``).

    On the 1st market tick, expands the portfolio into ``CBS_SHARED_NODE`` and ``CBS_BASIS_TERM``, compiles Pure
    queries to DuckDB SQL, and loads the static trade tables into DuckDB (or a bound ``ll.Frames`` / ``ll.Page``) once.
    On subsequent market ticks, only ``CBS_KNOT`` and ``CBS_JACOBIAN_ENTRY`` are updated and the pre-compiled SQL
    queries are executed.
    """

    def __init__(
        self,
        portfolio: Portfolio,
        fitter: FitterType,
        risk_method: Any = None,
        *,
        cb: pure.ComputeBasis | None = None,
        frames: Any = None,
        bump_method: str = 'forward',
        bump_bps: float = 1.0,
        use_sql_fitter: bool = False,
    ) -> None:
        self.portfolio = portfolio
        self.fitter = fitter
        self.curves = list(fitter.curves)
        self.vol_surfaces = self.portfolio.vol_surfaces()
        self.use_bump = is_bump_and_grind(risk_method)
        self.risk_method_name = 'bump_and_grind' if self.use_bump else 'analytic'
        self.bump_method = bump_method
        self.bump_bps = float(bump_bps)
        self.use_sql_fitter = bool(use_sql_fitter)
        self.tick_count = 0
        self._owns_cb = cb is None
        self._cb = cb if cb is not None else pure.ComputeBasis()
        self._frames = pure.unwrap_frames(frames)
        self._con: Any = None
        self._book = pure.BasisBook()
        self._bg: FirstOrderNumericalRisk | None = None
        self._npv_query: str = ''
        self._risk_query: str = ''
        self._value_and_chained_query: str = ''
        self._value_and_chained_plan: Any = None
        self._session_npv_plan: Any = None
        self._session_quote_risk_plan: Any = None
        self._sql_fitter: Any = None

    @property
    def frames(self) -> Any:
        """The bound ``ll.Frames`` instance (when created with ``frames=...``), or ``None``."""
        return self._frames

    def tick(self, quotes: Sequence[float] | None = None) -> TickResult:
        is_first = self.tick_count == 0
        t_start = time.perf_counter()

        if is_first:
            t_b0 = time.perf_counter()
            self._book = pure.BasisBook()
            if self.use_sql_fitter and hasattr(self.fitter, 'sql_fitter'):
                self._sql_fitter = self.fitter.sql_fitter(cb=self._cb)
            if self.use_bump:
                import pyarrow as pa

                self._bg = FirstOrderNumericalRisk(
                    self.portfolio,
                    curves=self.curves,
                    factors=self.fitter.factors,
                    bump_bps=self.bump_bps,
                )
                book_polys: dict[str, dict[tuple, float]] = {}
                for t in self.portfolio.trades:
                    poly = self._book.add(t.trade_id, t.book, NPV, '*', t.npv_on(self.curves))
                    bp = book_polys.setdefault(t.book, {})
                    for m, c in poly.items():
                        bp[m] = bp.get(m, 0.0) + c
                bump_book = pure.BasisBook(
                    _ids=dict(self._book._ids),
                    _max_nodes=self._book._max_nodes,
                    _max_params=self._book._max_params,
                    _formulas=dict(self._book._formulas),
                    _sub_measures=list(self._book._sub_measures),
                    _sub_by_id=dict(self._book._sub_by_id),
                    _sub_by_key=dict(self._book._sub_by_key),
                )
                for b_id, bp in sorted(book_polys.items()):
                    bump_book.measures.append(
                        (b_id, b_id, NPV, '*', {m: c for m, c in bp.items() if abs(c) > 1e-18})
                    )
                bump_scen_ids = []
                for q_spec in self.fitter.quote_specs:
                    if self.bump_method == 'central':
                        bump_scen_ids.append(f'BUMP_Q_{q_spec.quote_id}_UP')
                        bump_scen_ids.append(f'BUMP_Q_{q_spec.quote_id}_DN')
                    else:
                        bump_scen_ids.append(f'BUMP_Q_{q_spec.quote_id}')
                base_static = self._book.static_tables(['BASE'])
                bump_static = bump_book.static_tables(bump_scen_ids)
                static_tables = {
                    'CBS_SHARED_NODE': pa.concat_tables(
                        [base_static['CBS_SHARED_NODE'], bump_static['CBS_SHARED_NODE']]
                    ),
                    'CBS_BASIS_TERM': pa.concat_tables(
                        [base_static['CBS_BASIS_TERM'], bump_static['CBS_BASIS_TERM']]
                    ),
                }
                bump_stencil: list[pure.JacobianEntry] = []
                inv_span = 1.0 / self.bump_bps
                for q_spec in self.fitter.quote_specs:
                    qid = q_spec.quote_id
                    tenor = q_spec.pillar.tenor
                    if self.bump_method == 'central':
                        bump_stencil.append(pure.JacobianEntry(f'BUMP_Q_{qid}_UP', '*', qid, inv_span, tenor))
                        bump_stencil.append(pure.JacobianEntry(f'BUMP_Q_{qid}_DN', '*', qid, -inv_span, tenor))
                    else:
                        bump_stencil.append(pure.JacobianEntry(f'BUMP_Q_{qid}', '*', qid, inv_span, tenor))
                        bump_stencil.append(pure.JacobianEntry('BASE', '*', qid, -inv_span, tenor))
                stencil_mkt = self._book.market_tables((), bump_stencil)
                static_tables['CBS_JACOBIAN_ENTRY'] = stencil_mkt['CBS_JACOBIAN_ENTRY']
                self._con = self._cb.connect(
                    static_tables, connection=self._frames, materialize_stored=False
                )
                self._session_npv_plan = self._cb.plan(
                    pure.measures_query('BASE', by_entity=True, measure_code=NPV, book_or_schema=self._cb.schema)
                    + '->project(~[book: x|$x.groupId, trade_id: x|$x.entityId, npv: x|$x.value->toOne()])'
                    + '->sort([~book->ascending(), ~trade_id->ascending()])'
                )
                book_npv_rel = (
                    pure.terms_relation(book_or_schema=self._cb.schema, eval_mode='primal', measure_code=NPV)
                    + '->groupBy(~[scenarioId, groupId], ~[bookVal: x|$x.termValue : y|$y->sum()])'
                )
                stencil_rel = (
                    'compute_basis::CbsJacobianEntry.all()->project(~['
                    'jScen: x|$x.scenarioId, quote_id: x|$x.inputId, w: x|$x.jacobianWeight])'
                )
                self._session_quote_risk_plan = self._cb.plan(
                    '|'
                    + book_npv_rel
                    + f'->join({stencil_rel}, JoinKind.INNER, {{a, b | $a.scenarioId == $b.jScen}})'
                    + '->extend(~[termDv01: x|($x.bookVal->toOne() * $x.w->toOne())])'
                    + '->groupBy(~[groupId, quote_id], ~[dv01: x|$x.termDv01 : y|$y->sum()])'
                    + '->project(~[book: x|$x.groupId, quote_id: x|$x.quote_id, dv01: x|$x.dv01->toOne()])'
                    + '->sort([~book->ascending(), ~quote_id->ascending()])'
                )
            else:
                analytic = FirstOrderAnalyticRisk(
                    self.portfolio,
                    curves=self.curves,
                    factors=self.fitter.factors,
                )
                analytic.populate_book(self._book, scale=BP)
                self._con = self._cb.connect(
                    self._book.static_tables(['BASE']),
                    connection=self._frames,
                    materialize_stored=False,
                )
                self._value_and_chained_query = self._cb.value_and_chained_query(
                    NPV, PILLAR_DELTA, 'BASE'
                )
                self._value_and_chained_plan = self._cb.plan(self._value_and_chained_query)
                if self._frames is not None:
                    self._session_npv_plan = self._cb.plan(
                        pure.measures_query('BASE', by_entity=True, measure_code=NPV, book_or_schema=self._cb.schema)
                        + '->project(~[book: x|$x.groupId, trade_id: x|$x.entityId, npv: x|$x.value->toOne()])'
                        + '->sort([~book->ascending(), ~trade_id->ascending()])'
                    )
                    self._session_quote_risk_plan = self._cb.plan(
                        quote_risk_query('BASE', book_or_schema=self._cb.schema)
                        + '->project(~[book: x|$x.groupId, quote_id: x|$x.inputId, dv01: x|$x.chainedValue->toOne()])'
                        + '->sort([~book->ascending(), ~quote_id->ascending()])'
                    )
            build_ms = (time.perf_counter() - t_b0) * 1000.0
        else:
            build_ms = 0.0

        if self.use_bump:
            assert self._bg is not None
            t_f0 = time.perf_counter()
            if self._sql_fitter is not None:
                base_q = list(self.fitter.quotes if quotes is None else quotes)
                delta = self.bump_bps * BP
                scen_quotes: dict[str, list[float]] = {'BASE': base_q}
                for j, q_spec in enumerate(self.fitter.quote_specs):
                    qid = q_spec.quote_id
                    if self.bump_method == 'central':
                        q_up = list(base_q)
                        q_dn = list(base_q)
                        q_up[j] += 0.5 * delta
                        q_dn[j] -= 0.5 * delta
                        scen_quotes[f'BUMP_Q_{qid}_UP'] = q_up
                        scen_quotes[f'BUMP_Q_{qid}_DN'] = q_dn
                    else:
                        q_up = list(base_q)
                        q_up[j] += delta
                        scen_quotes[f'BUMP_Q_{qid}'] = q_up
                all_fits = self._sql_fitter.fit_scenarios_sql(scen_quotes, compute_jacobian=False)
                fit = all_fits.pop('BASE')
                bump_fits = all_fits
            else:
                fit = self.fitter.fit_quotes(quotes, compute_jacobian=False)
                bump_fits = self._bg.refit_quote_bumps(
                    self.fitter,
                    method=self.bump_method,
                    quotes=quotes,
                )
            fit_ms = (time.perf_counter() - t_f0) * 1000.0

            t_e0 = time.perf_counter()
            scens = [scenario(self.curves, 'BASE', fit, 0.0, 'USD', vol_surfaces=self.vol_surfaces)] + [
                scenario(self.curves, sid, b_fit, 0.0, 'USD', vol_surfaces=self.vol_surfaces)
                for sid, b_fit in bump_fits.items()
            ]
            mkt = self._book.market_tables(scens, ())
            self._cb.update(self._con, {'CBS_KNOT': mkt['CBS_KNOT']}, materialize_stored=False)
            npv_tbl = self._cb.run(self._con, self._session_npv_plan)
            qr_tbl = self._cb.run(self._con, self._session_quote_risk_plan)
            npv_cols = npv_tbl.to_pydict()
            qr_cols = qr_tbl.to_pydict()
            npv = dict(zip(zip(npv_cols['book'], npv_cols['trade_id']), npv_cols['npv']))
            quote_risk = dict(zip(zip(qr_cols['book'], qr_cols['quote_id']), qr_cols['dv01']))
            if self._frames is not None:
                self._cb._put_frame(self._frames, 'session_npv', npv_tbl)
                self._cb._put_frame(self._frames, 'session_quote_risk', qr_tbl)
            eval_ms = (time.perf_counter() - t_e0) * 1000.0
        else:
            t_f0 = time.perf_counter()
            if self._sql_fitter is not None:
                fit = self._sql_fitter.fit_quotes(quotes, compute_jacobian=True)
            else:
                fit = self.fitter.fit_quotes(quotes, compute_jacobian=True)
            fit_ms = (time.perf_counter() - t_f0) * 1000.0

            t_e0 = time.perf_counter()
            scens = [scenario(self.curves, 'BASE', fit, 0.0, 'USD', vol_surfaces=self.vol_surfaces)]
            factors = self.fitter.factors
            q_specs = self.fitter.quote_specs
            jacobian = [
                pure.JacobianEntry(
                    'BASE',
                    factors[k].factor_id,
                    q_specs[j].quote_id,
                    fit.dr_dq[k][j],
                    q_specs[j].pillar.tenor,
                )
                for k in range(len(factors))
                for j in range(len(q_specs))
                if abs(fit.dr_dq[k][j]) > 1e-18
            ]
            self._cb.update(
                self._con, self._book.market_tables(scens, jacobian), materialize_stored=False
            )
            rows = self._cb.run(self._con, self._value_and_chained_plan).to_pylist()
            npv = {}
            quote_risk = {}
            for r in rows:
                k = r['resultKind']
                gid = r['groupId']
                item_id = r['itemId']
                val = r['value']
                if k == 'VALUE':
                    npv[(gid, item_id)] = val
                elif k == 'CHAINED':
                    quote_risk[(gid, item_id)] = val
            if self._frames is not None:
                self._cb._put_frame(
                    self._frames,
                    'session_npv',
                    self._cb.run(self._con, self._session_npv_plan),
                )
                self._cb._put_frame(
                    self._frames,
                    'session_quote_risk',
                    self._cb.run(self._con, self._session_quote_risk_plan),
                )
            eval_ms = (time.perf_counter() - t_e0) * 1000.0

        total_ms = (time.perf_counter() - t_start) * 1000.0
        idx = self.tick_count
        self.tick_count += 1
        return TickResult(
            tick_index=idx,
            is_first_tick=is_first,
            engine_name=PureBasisEngine.engine_name,
            risk_method_name=self.risk_method_name,
            trade_count=len(self.portfolio.trades),
            shared_nodes=len(self._book.nodes),
            basis_terms=self._book.term_count(),
            build_ms=build_ms,
            fit_ms=fit_ms,
            eval_ms=eval_ms,
            total_ms=total_ms,
            fit=fit,
            npv=npv,
            quote_risk=quote_risk,
        )

    def close(self) -> None:
        if self._sql_fitter is not None:
            self._sql_fitter.close()
            self._sql_fitter = None
        if self._con is not None and self._frames is None:
            self._con.close()
        self._con = None
        if self._owns_cb:
            self._cb.close()


class PureBasisEngine(ExecutionEngine):
    """Executes pricing, analytic risk, and scenario bump-and-grind risk in Pure -> SQL -> DuckDB / ``ll.Frames``."""

    engine_name = 'pure_duckdb'

    def __init__(self, cb: pure.ComputeBasis | None = None, frames: Any = None) -> None:
        self._external_cb = cb
        self._frames = pure.unwrap_frames(frames)

    @staticmethod
    def pure_queries(scenario_id: str | None = 'BASE') -> dict[str, str]:
        """Returns the Pure query strings used to evaluate trade measures, book measures, and chained quote risk."""
        return {
            'measures_by_trade': pure.measures_query(by_entity=True),
            'measures_by_book': pure.measures_query(by_entity=False),
            'quote_risk': quote_risk_query(scenario_id),
        }

    @staticmethod
    def _normalize_dialect(dialect: str) -> str:
        by_lower = {db.lower(): db for db in pure.DATABASES}
        return by_lower.get(dialect.lower().strip(), dialect)

    def plan_sql(
        self,
        dialect: str = 'DuckDB',
        scenario_id: str | None = 'BASE',
    ) -> dict[str, str]:
        """Compiles the Pure basis queries into SQL for ``dialect`` (case-insensitive: ``'DuckDB'``, ``'Postgres'``,
        ``'Snowflake'``, ``'BigQuery'``, ``'Databricks'``, ``'Trino'``, ``'SQLite'``, ``'ClickHouse'``)."""
        db = self._normalize_dialect(dialect)
        queries = self.pure_queries(scenario_id)
        if self._external_cb is not None:
            return {name: self._external_cb.plan(q, db).sql for name, q in queries.items()}
        with pure.ComputeBasis() as cb:
            return {name: cb.plan(q, db).sql for name, q in queries.items()}

    def plan_all_dialects(self, query: str | None = None) -> dict[str, str]:
        """Compiles ``query`` (defaults to ``pure.measures_query()``) across all 8 SQL dialects supported by
        ``legend-lite``."""
        target = query if query is not None else pure.measures_query()
        if self._external_cb is not None:
            return {db: self._external_cb.plan(target, db).sql for db in pure.DATABASES}
        with pure.ComputeBasis() as cb:
            return {db: cb.plan(target, db).sql for db in pure.DATABASES}

    def _with_cb(self, fn: Any) -> Any:
        if self._external_cb is not None:
            return fn(self._external_cb)
        with pure.ComputeBasis() as cb:
            return fn(cb)

    def npvs(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        curves: ZeroCurve | Sequence[ZeroCurve] = (),
        fit: Fit | Mapping[str, float] | None = None,
    ) -> dict[tuple[str, str], float]:
        if not isinstance(fit, Fit):
            return PythonEngineExpr().npvs(portfolio, curves, fit)
        return self.scenario_npvs(portfolio, curves, {'BASE': fit})['BASE']

    def scenario_npvs(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        curves: ZeroCurve | Sequence[ZeroCurve],
        scenario_fits: Mapping[str, Fit],
    ) -> dict[str, dict[tuple[str, str], float]]:
        port = as_portfolio(portfolio, curves)
        curve_list = port.curves()
        vol_surfaces = port.vol_surfaces()
        book = pure.BasisBook()
        for t in port.trades:
            book.add(t.trade_id, t.book, NPV, '*', t.npv_on(curve_list))
        scens = [
            scenario(curve_list, sid, f, 0.0, 'USD', vol_surfaces=vol_surfaces)
            for sid, f in scenario_fits.items()
        ]

        def _exec(cb: pure.ComputeBasis) -> dict[str, dict[tuple[str, str], float]]:
            con = cb.connect(
                book.tables(scens, ()), connection=self._frames, materialize_stored=False
            )
            rows = cb.run(con, pure.measures_query(by_entity=True, book_or_schema=book)).to_pylist()
            out: dict[str, dict[tuple[str, str], float]] = {sid: {} for sid in scenario_fits}
            for r in rows:
                if r['measureCode'] == NPV:
                    out.setdefault(r['scenarioId'], {})[(r['groupId'], r['entityId'])] = r['value']
            return out

        return self._with_cb(_exec)

    def pillar_risk(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        curves: ZeroCurve | Sequence[ZeroCurve] = (),
        fit: Fit | Mapping[str, float] | None = None,
        factors: Sequence[FactorSpec] | None = None,
    ) -> dict[tuple[str, str, str], float]:
        if not isinstance(fit, Fit):
            return PythonEngineExpr().pillar_risk(portfolio, curves, fit, factors)
        port = as_portfolio(portfolio, curves)
        curve_list = port.curves()
        vol_surfaces = port.vol_surfaces()
        analytic = FirstOrderAnalyticRisk(port, curves=curve_list, factors=factors)
        book = analytic.populate_book(pure.BasisBook(), scale=BP)
        scens = [scenario(curve_list, 'BASE', fit, 0.0, 'USD', vol_surfaces=vol_surfaces)]

        def _exec(cb: pure.ComputeBasis) -> dict[tuple[str, str, str], float]:
            con = cb.connect(
                book.tables(scens, ()), connection=self._frames, materialize_stored=False
            )
            rows = cb.run(con, pure.measures_query('BASE', by_entity=True, book_or_schema=book)).to_pylist()
            return {
                (r['groupId'], r['entityId'], r['factorId']): r['value']
                for r in rows
                if r['scenarioId'] == 'BASE' and r['measureCode'] == PILLAR_DELTA
            }

        return self._with_cb(_exec)

    def quote_risk(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        fitter: FitterType,
        fit: Fit | None = None,
        risk_method: Any = None,
        **kwargs: Any,
    ) -> dict[tuple[str, str], float]:
        fit0 = fit if fit is not None else fitter.fit()
        port = as_portfolio(portfolio, fitter.curves)
        method = risk_method or FirstOrderAnalyticRisk
        if isinstance(method, type) and issubclass(method, FirstOrderNumericalRisk):
            calc_bg = method(port, curves=fitter.curves, factors=fitter.factors)
            q_risk, _ = calc_bg.quote_risk(fitter, fit0, engine=self, **kwargs)
            return q_risk

        curve_list = list(fitter.curves)
        vol_surfaces = port.vol_surfaces()
        analytic = FirstOrderAnalyticRisk(port, curves=curve_list, factors=fitter.factors)
        book = analytic.populate_book(pure.BasisBook(), scale=BP)
        scens = [scenario(curve_list, 'BASE', fit0, 0.0, 'USD', vol_surfaces=vol_surfaces)]
        factors = fitter.factors
        quotes = fitter.quote_specs
        jacobian = [
            pure.JacobianEntry(
                'BASE',
                factors[k].factor_id,
                quotes[j].quote_id,
                fit0.dr_dq[k][j],
                quotes[j].pillar.tenor,
            )
            for k in range(len(factors))
            for j in range(len(quotes))
            if abs(fit0.dr_dq[k][j]) > 1e-18
        ]

        def _exec(cb: pure.ComputeBasis) -> dict[tuple[str, str], float]:
            con = cb.connect(
                book.tables(scens, jacobian), connection=self._frames, materialize_stored=False
            )
            rows = cb.run(con, quote_risk_query('BASE', book_or_schema=book)).to_pylist()
            return {(r['groupId'], r['inputId']): r['chainedValue'] for r in rows}

        return self._with_cb(_exec)

    def run(
        self,
        curves: ZeroCurve | Sequence[ZeroCurve],
        fitter: FitterType,
        trades: Sequence[Instrument],
        bumps: Sequence[tuple[str, float]] = (),
        unit: str | None = 'USD',
        *,
        include_bump_and_grind: bool = True,
        bump_method: str = 'forward',
    ) -> RiskRun:
        """Fits the curves, populates the ``BasisBook`` with symbolic ``NPV`` and ``PILLAR_DELTA`` measures, and
        evaluates NPVs, pillar deltas, chained par-quote risk, and scenario bump-and-grind risk in DuckDB SQL."""
        from ..risk import RiskRun

        fit = fitter.fit()
        curve_list = list(fitter.curves) if hasattr(fitter, 'curves') else (
            [curves] if isinstance(curves, ZeroCurve) else list(curves)
        )
        port = as_portfolio(trades, curve_list)
        vol_surfaces = port.vol_surfaces()
        all_factors = list(fitter.factors)
        seen_fids = {f.factor_id for f in all_factors}
        for f in port.factors(curve_list):
            if f.factor_id not in seen_fids:
                seen_fids.add(f.factor_id)
                all_factors.append(f)
        analytic = FirstOrderAnalyticRisk(port, curves=curve_list, factors=all_factors)
        book = analytic.populate_book(pure.BasisBook(), scale=BP)

        base_and_parallel = [scenario(curve_list, 'BASE', fit, 0.0, unit, vol_surfaces=vol_surfaces)] + [
            scenario(curve_list, sid, fit, shift, unit, vol_surfaces=vol_surfaces) for sid, shift in bumps
        ]
        factors = fitter.factors
        quotes = fitter.quote_specs
        jacobian = [
            pure.JacobianEntry(
                s.scenario_id,
                factors[k].factor_id,
                quotes[j].quote_id,
                fit.dr_dq[k][j],
                quotes[j].pillar.tenor,
            )
            for s in base_and_parallel
            for k in range(len(factors))
            for j in range(len(quotes))
            if abs(fit.dr_dq[k][j]) > 1e-18
        ]

        scenarios = list(base_and_parallel)
        bump_fits: dict[str, Fit] = {}
        bg: FirstOrderNumericalRisk | None = None
        if include_bump_and_grind:
            bg = FirstOrderNumericalRisk(port, curves=curve_list, factors=factors)
            bump_fits = bg.refit_quote_bumps(fitter, method=bump_method)
            for sid, b_fit in bump_fits.items():
                scenarios.append(scenario(curve_list, sid, b_fit, 0.0, unit, vol_surfaces=vol_surfaces))

        notional_map = {(t.book, t.trade_id): t.notional for t in port.trades}
        books = sorted({t.book for t in port.trades})

        def _exec(cb: pure.ComputeBasis) -> tuple[
            dict[tuple[str, str], float],
            dict[tuple[str, str, str], float],
            dict[tuple[str, str], float],
            dict[tuple[str, str], float],
        ]:
            con = cb.connect(
                book.tables(scenarios, jacobian), connection=self._frames, materialize_stored=False
            )
            m_rows = cb.run(con, pure.measures_query(by_entity=True, book_or_schema=book)).to_pylist()
            q_rows = cb.run(con, quote_risk_query('BASE', book_or_schema=book)).to_pylist()

            npv_out: dict[tuple[str, str], float] = {}
            delta_out: dict[tuple[str, str, str], float] = {}
            scen_book_npv: dict[tuple[str, str], float] = {}
            for row in m_rows:
                sid = row['scenarioId']
                gid = row['groupId']
                eid = row['entityId']
                mcode = row['measureCode']
                val = row['value']
                if mcode == NPV:
                    scen_book_npv[(sid, gid)] = scen_book_npv.get((sid, gid), 0.0) + val
                    if sid == 'BASE':
                        npv_out[(gid, eid)] = val
                elif sid == 'BASE' and mcode == PILLAR_DELTA:
                    delta_out[(gid, eid, row['factorId'])] = val

            q_out = {(r['groupId'], r['inputId']): r['chainedValue'] for r in q_rows}
            bump_q_out: dict[tuple[str, str], float] = {}
            if bg is not None:
                for q_spec in quotes:
                    qid = q_spec.quote_id
                    for b_id in books:
                        if bump_method == 'central':
                            dv01 = (
                                scen_book_npv[(f'BUMP_Q_{qid}_UP', b_id)]
                                - scen_book_npv[(f'BUMP_Q_{qid}_DN', b_id)]
                            ) / bg.bump_bps
                        else:
                            dv01 = (
                                scen_book_npv[(f'BUMP_Q_{qid}', b_id)] - scen_book_npv[('BASE', b_id)]
                            ) / bg.bump_bps
                        bump_q_out[(b_id, qid)] = dv01
            return npv_out, delta_out, q_out, bump_q_out

        npv_map, delta_map, quote_risk_map, bump_quote_risk_map = self._with_cb(_exec)
        return RiskRun(
            book=book,
            scenarios=scenarios,
            jacobian=jacobian,
            fit=fit,
            npv=npv_map,
            delta=delta_map,
            quote_risk=quote_risk_map,
            notional=notional_map,
            bump_quote_risk=bump_quote_risk_map,
            bump_fits=bump_fits,
            engine_name=self.engine_name,
        )

    def session(
        self,
        portfolio: Instrument | Portfolio | Sequence[Instrument],
        fitter: FitterType,
        risk_method: Any = None,
        *,
        frames: Any = None,
        bump_method: str = 'forward',
        bump_bps: float = 1.0,
        use_sql_fitter: bool = False,
    ) -> PureBasisMarketSession:
        port = as_portfolio(portfolio, fitter.curves)
        return PureBasisMarketSession(
            port,
            fitter,
            risk_method=risk_method,
            cb=self._external_cb,
            frames=frames if frames is not None else self._frames,
            bump_method=bump_method,
            bump_bps=bump_bps,
            use_sql_fitter=use_sql_fitter,
        )


DuckDBBasisEngine = PureBasisEngine

