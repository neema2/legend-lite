"""A DataCube Flow + Board dashboard (``legend_lite.Page``) for the interest-rate swap curve fitting, pricing, and
risk engine.

Combines skinny relational ``page.flow()`` blocks with compact hash-consed scalar micro-expressions (``expr.Expr``):
  - **Sheet 0 (``0. Pricing & Risk Flow``)**: end-to-end 100%-SQL skinny vectorized dataflow graph (zero wide-column
    ``r_0..r_{N-1}`` or ``w_{i}_{m}`` unrolling — fixed schema independent of pillar count ``N``):
      1. Market Par Quotes
      2. Iter 0: Shocked Quotes & Skinny Seed (``parallel_shift_bps``, ``steepener_2s10s_bps`` parameters)
      3. Iter 1: Skinny SQL Newton-Schulz Step (``O(log L_max)`` sparse pointer-doubling linear solve)
      4. Iter 2: Skinny SQL Newton-Schulz Step (reusing Step 1 ``W_0`` inverse for quadratic convergence ``< 1e-15``)
      5. Fitted Curve Pillars
      6. Interpolated Curve Profile (0.25Y tenor grid via single normalized stencil join)
      7. Fitted Inverse Jacobian ``dr/dq`` (sparse SQL Newton-Schulz refinement at ``r_2``)
      8. Evaluated Shared Nodes (``LERP``, ``EXP_NEG_XT``, ``RECIP_1PXT`` via skinny knot joins)
      9. Evaluated Basis Terms (single-join normalized polynomial evaluation)
      10. Portfolio Swap Valuation (SQL vs Python + live 1bp parallel bump & convexity check)
      11. Analytic Pillar Risk (zero-rate pillar deltas ``dNPV/dr``)
      12. Chained Par-Quote DV01 (``FirstOrderAnalyticRisk`` SQL chain rule vs ``FirstOrderNumericalRisk``)
  - **Sheets 1--4 (Board Sheets)**: formatted pivot grids, heatmaps, data bars, and interactive charts reading
    directly from the terminal Flow blocks.
"""

from __future__ import annotations

import warnings
from typing import Any, Mapping, Sequence

import pyarrow as pa

import legend_lite as ll
from legend_lite import compiler, frames as _frames_mod
from legend_lite.scalar_parallel import basis, basis_pure as pure

from ..engines.python_expr import PythonEngineExpr
from ..instruments.ir_swap_fixed_float import IRSwapFixedFloat
from ..marketmodels.ir_curve_fitter import (
    CurveFitter,
    MultiCurveFitter,
    _skinny_pure_join,
    build_calibration_basis_tables,
)
from ..marketmodels.ir_curve_yield import Pillar, ZeroCurve
from ..risk import FitterType, RiskRun, SwapInstrument

CANONICAL_FRAME_NAMES = (
    'market_quotes',
    'curve_profile',
    'fitter_jacobian',
    'swap_portfolio',
    'pillar_risk',
    'quote_risk',
)

LEAF_FRAME_NAMES = (
    'flow_quotes',
    'flow_cal_nodes',
    'flow_cal_slots',
    'flow_cal_reach',
    'flow_curve_grid',
    'flow_jac_pairs',
    'cbs_shared_node',
    'cbs_basis_term',
    'flow_swap_trades',
    'flow_pillar_ref',
    'flow_quote_ref',
)

_STAGE_BARRIER = '->limit(1000000)'


def _col_cfg(*, code: str | None = None, data_bar: bool = False, heatmap: bool = False) -> dict[str, Any]:
    """Builds a single-pass DataCube ``grid.configure({'columns': ...})`` entry."""
    cfg: dict[str, Any] = {}
    if code is not None:
        cfg['format'] = {'code': code, 'kind': 'auto'}
    if data_bar:
        cfg['dataBar'] = {'color': '#9fc3ea'}
    if heatmap:
        cfg['heatmap'] = {'from': '#ffffff', 'to': '#ff8a65'}
    return cfg


# --------------------------------------------------------------------------- Skinny Pure blocks shared by SQL & Flow


def _skinny_shocked_seed_pure(prefix: str = '$input', *, parameterized: bool = True) -> str:
    """Stage 2 Pure expression: applies optional curve shocks and seeds skinny ``(scenario_id, factor_idx, r_val)``."""
    if parameterized:
        q_expr = (
            '($x.quote_rate->toOne() + ($parallel_shift_bps * 0.0001) '
            '+ ($steepener_2s10s_bps * 0.0001 * (($x.tenor_years->toOne() - 2.0) / 8.0)))'
        )
    else:
        q_expr = '$x.quote_rate->toOne()'
    return (
        f'{prefix}->extend(~[shocked_q: x|{q_expr}]){_STAGE_BARRIER}->project(~['
        f'scenario_id: x|$x.scenario_id, '
        f'quote_idx: x|$x.quote_index->toOne(), '
        f'factor_idx: x|$x.factor_idx->toOne(), '
        f'factor_id: x|$x.factor_id->toOne(), '
        f'series_id: x|$x.series_id->toOne(), '
        f'knot_index: x|$x.knot_index->toOne(), '
        f'curve_id: x|$x.curve_id->toOne(), '
        f'quote_id: x|$x.quote_id->toOne(), '
        f'pillar: x|$x.pillar->toOne(), '
        f'tenor_years: x|$x.tenor_years->toOne(), '
        f'shocked_quote: x|$x.shocked_q->toOne(), '
        f'r_val: x|$x.shocked_q->toOne()'
        f'])'
    )


def _eval_cal_nodes_pure(prefix: str = '$input', *, scen_col: str = 'scenario_id') -> str:
    """Evaluates normalized calibration basis nodes from a single join with knot rates."""
    return (
        f'{prefix}->extend(~[wt_r: x|($x.interp_w->toOne() * $x.r_val->toOne())])'
        f'{_STAGE_BARRIER}->groupBy(~[{scen_col}, node_id, op_code, param_t, const_offset], ~[x_val: x|$x.wt_r : y|$y->plus()])'
        f'{_STAGE_BARRIER}->project(~['
        f'scenario_id: x|$x.{scen_col}, '
        f'node_id: x|$x.node_id->toOne(), '
        f'node_val: x|if($x.op_code == 0, |$x.const_offset->toOne(), '
        f'|if($x.op_code == 1, |($x.x_val->toOne() + $x.const_offset->toOne()), '
        f'|if($x.op_code == 2, |((-1.0 * $x.x_val->toOne() * $x.param_t->toOne())->exp() + $x.const_offset->toOne()), '
        f'|(1.0 / (1.0 + ($x.x_val->toOne() * $x.param_t->toOne()))) + $x.const_offset->toOne())))'
        f'])'
    )


def _eval_cal_polys_pure(prefix: str = '$input', *, scen_col: str = 'scenario_id') -> str:
    """Evaluates calibration par numerators, denominators, and partial derivatives from evaluated nodes."""
    return (
        f'{prefix}->extend(~['
        f's0: x|if($x.slot_idx == 0, |$x.node_val->toOne(), |0.0), '
        f's1: x|if($x.slot_idx == 1, |$x.node_val->toOne(), |0.0), '
        f's2: x|if($x.slot_idx == 2, |$x.node_val->toOne(), |0.0), '
        f's3: x|if($x.slot_idx == 3, |$x.node_val->toOne(), |0.0)'
        f']){_STAGE_BARRIER}->groupBy('
        f'~[{scen_col}, quote_idx, role, col_k, term_seq, term_degree, coeff], '
        f'~['
        f'va: x|$x.s0 : y|$y->plus(), '
        f'vb: x|$x.s1 : y|$y->plus(), '
        f'vc: x|$x.s2 : y|$y->plus(), '
        f'vd: x|$x.s3 : y|$y->plus()'
        f']){_STAGE_BARRIER}->extend(~['
        f'term_val: x|if($x.term_degree == 1, |($x.coeff->toOne() * $x.va->toOne()), '
        f'|if($x.term_degree == 2, |(($x.coeff->toOne() * $x.va->toOne()) * $x.vb->toOne()), '
        f'|if($x.term_degree == 3, |((($x.coeff->toOne() * $x.va->toOne()) * $x.vb->toOne()) * $x.vc->toOne()), '
        f'|(((($x.coeff->toOne() * $x.va->toOne()) * $x.vb->toOne()) * $x.vc->toOne()) * $x.vd->toOne()))))'
        f']){_STAGE_BARRIER}->groupBy('
        f'~[{scen_col}, quote_idx, role, col_k], '
        f'~[poly_val: x|$x.term_val : y|$y->plus()]'
        f'){_STAGE_BARRIER}->project(~['
        f'scenario_id: x|$x.{scen_col}, '
        f'quote_idx: x|$x.quote_idx->toOne(), '
        f'role: x|$x.role->toOne(), '
        f'poly_col_k: x|$x.col_k->toOne(), '
        f'poly_val: x|$x.poly_val->toOne()'
        f'])'
    )


def _assemble_reach_jac_pure(prefix: str = '$input', *, scen_col: str = 'scenario_id') -> str:
    """Computes ``par_val = F / A`` and sparse reachable Jacobian entries ``J_{i,j} = (F' A - F A') / A^2`` in one join."""
    return (
        f'{prefix}->filter(x|($x.poly_col_k == -1) || ($x.poly_col_k == $x.col_k))'
        f'{_STAGE_BARRIER}->extend(~['
        f'nv: x|if($x.role == 0, |$x.poly_val->toOne(), |0.0), '
        f'dv: x|if($x.role == 1, |$x.poly_val->toOne(), |0.0), '
        f'dnv: x|if($x.role == 2, |$x.poly_val->toOne(), |0.0), '
        f'ddv: x|if($x.role == 3, |$x.poly_val->toOne(), |0.0)'
        f']){_STAGE_BARRIER}->groupBy(~[{scen_col}, row_i, col_k], ~['
        f'num_val: x|$x.nv : y|$y->plus(), '
        f'den_val: x|$x.dv : y|$y->plus(), '
        f'dnum_val: x|$x.dnv : y|$y->plus(), '
        f'dden_val: x|$x.ddv : y|$y->plus()'
        f']){_STAGE_BARRIER}->project(~['
        f'scenario_id: x|$x.{scen_col}, '
        f'row_i: x|$x.row_i->toOne(), '
        f'col_j: x|$x.col_k->toOne(), '
        f'par_val: x|($x.num_val->toOne() / $x.den_val->toOne()), '
        f'j_val: x|((($x.dnum_val->toOne() * $x.den_val->toOne()) - ($x.num_val->toOne() * $x.dden_val->toOne())) '
        f'/ ($x.den_val->toOne() * $x.den_val->toOne()))'
        f'])'
    )


def _skinny_pillars_output_pure(
    prefix: str = '$input',
    *,
    rate_col: str = 'kn_r_val',
    par_col: str = 'par_val',
) -> str:
    """Stage 5 Pure expression: formats fitted curve pillars, discount factors, repriced par rates & residuals in SQL."""
    return (
        f'{prefix}->sort([~factor_idx->ascending()])->project(~['
        f'curve_id: x|$x.curve_id->toOne(), '
        f'quote_id: x|$x.quote_id->toOne(), '
        f'pillar: x|$x.pillar->toOne(), '
        f'tenor_years: x|$x.tenor_years->toOne(), '
        f'market_quote_bps: x|($x.shocked_quote->toOne() * 10000.0), '
        f'fitted_zero_rate_bps: x|($x.{rate_col}->toOne() * 10000.0), '
        f'zero_vs_par_spread_bps: x|(($x.{rate_col}->toOne() - $x.shocked_quote->toOne()) * 10000.0), '
        f'discount_factor: x|(-1.0 * $x.{rate_col}->toOne() * $x.tenor_years->toOne())->exp(), '
        f'repriced_par_bps: x|($x.{par_col}->toOne() * 10000.0), '
        f'fit_error_bps: x|(($x.{par_col}->toOne() - $x.shocked_quote->toOne()) * 10000.0)'
        f'])'
    )


def _skinny_curve_profile_pure(prefix: str = '$input', *, rate_col: str = 'r_val') -> str:
    """Stage 6 Pure expression: interpolates 0.25Y zero rates, 3M forwards, and discount factors from 1 skinny join."""
    return (
        f'{prefix}->extend(~['
        f'wt_r: x|($x.weight->toOne() * $x.{rate_col}->toOne())'
        f']){_STAGE_BARRIER}->extend(~['
        f'zt_part: x|if($x.z_role == 0, |$x.wt_r->toOne(), |0.0), '
        f'zp_part: x|if($x.z_role == 1, |$x.wt_r->toOne(), |0.0)'
        f']){_STAGE_BARRIER}->groupBy('
        f'~[scenario_id, grid_idx, curve_id, tenor_label, tenor_years, prev_tenor, point_type], '
        f'~['
        f'z_t: x|$x.zt_part : y|$y->plus(), '
        f'z_prev: x|$x.zp_part : y|$y->plus()'
        f']){_STAGE_BARRIER}->extend(~['
        f'df: x|(-1.0 * $x.z_t->toOne() * $x.tenor_years->toOne())->exp(), '
        f'df_prev: x|if($x.prev_tenor->toOne() <= 0.0, |1.0, |(-1.0 * $x.z_prev->toOne() * $x.prev_tenor->toOne())->exp())'
        f']){_STAGE_BARRIER}->sort([~grid_idx->ascending()])->project(~['
        f'curve_id: x|$x.curve_id->toOne(), '
        f'tenor_label: x|$x.tenor_label->toOne(), '
        f'tenor_years: x|$x.tenor_years->toOne(), '
        f'point_type: x|$x.point_type->toOne(), '
        f'zero_rate_bps: x|($x.z_t->toOne() * 10000.0), '
        f'forward_1y_bps: x|(((($x.df_prev->toOne() / $x.df->toOne()) - 1.0) / 0.25) * 10000.0), '
        f'discount_factor: x|$x.df->toOne()'
        f'])'
    )


def _skinny_jacobian_output_pure(prefix: str = '$input', *, w_col: str = 'w_val') -> str:
    """Stage 7 Pure expression: formats skinny ``d_pillar_d_quote`` in deterministic ``(factor_idx, quote_idx)`` order."""
    return (
        f'{prefix}->sort([~factor_idx->ascending(), ~quote_idx->ascending()])->project(~['
        f'scenario_id: x|$x.scenario_id, '
        f'factor_id: x|$x.factor_id->toOne(), '
        f'input_id: x|$x.input_id->toOne(), '
        f'input_coord: x|$x.input_coord->toOne(), '
        f'pillar_curve: x|$x.pillar_curve->toOne(), '
        f'pillar: x|$x.pillar->toOne(), '
        f'pillar_tenor: x|$x.pillar_tenor->toOne(), '
        f'quote_curve: x|$x.quote_curve->toOne(), '
        f'quote_id: x|$x.quote_id->toOne(), '
        f'quote_tenor: x|$x.quote_tenor->toOne(), '
        f'd_pillar_d_quote: x|if($x.{w_col}->isEmpty(), |0.0, |$x.{w_col}->toOne())'
        f'])'
    )


def _skinny_shared_nodes_pure(prefix: str = '$input') -> str:
    """Stage 8 Pure expression: evaluates normalized shared basis nodes at fitted zero rates (and +1bp parallel-shifted zero rates)."""
    return (
        f'{prefix}->extend(~[wt_r: x|($x.INTERP_W->toOne() * $x.r_val->toOne())])'
        f'{_STAGE_BARRIER}->groupBy(~[SCENARIO_ID, NODE_ID, SERIES_ID, OP_CODE, PARAM_T, CONST_OFFSET], ~[lerp_v: x|$x.wt_r : y|$y->plus()])'
        f'{_STAGE_BARRIER}->extend(~[lerp_b: x|($x.lerp_v->toOne() + 0.0001)])'
        f'{_STAGE_BARRIER}->project(~['
        f'SCENARIO_ID: x|$x.SCENARIO_ID, '
        f'NODE_ID: x|$x.NODE_ID->toOne(), '
        f'SERIES_ID: x|$x.SERIES_ID->toOne(), '
        f'OP_CODE: x|$x.OP_CODE->toOne(), '
        f'PARAM_T: x|$x.PARAM_T->toOne(), '
        f"node_value: x|if($x.OP_CODE == 'CONST', |$x.CONST_OFFSET->toOne(), "
        f"|if($x.OP_CODE == 'LERP', |($x.lerp_v->toOne() + $x.CONST_OFFSET->toOne()), "
        f"|if($x.OP_CODE == 'EXP_NEG_XT', |((-1.0 * $x.lerp_v->toOne() * $x.PARAM_T->toOne())->exp() + $x.CONST_OFFSET->toOne()), "
        f"|(1.0 / (1.0 + ($x.lerp_v->toOne() * $x.PARAM_T->toOne()))) + $x.CONST_OFFSET->toOne()))), "
        f"bump_node_value: x|if($x.OP_CODE == 'CONST', |$x.CONST_OFFSET->toOne(), "
        f"|if($x.OP_CODE == 'LERP', |($x.lerp_b->toOne() + $x.CONST_OFFSET->toOne()), "
        f"|if($x.OP_CODE == 'EXP_NEG_XT', |((-1.0 * $x.lerp_b->toOne() * $x.PARAM_T->toOne())->exp() + $x.CONST_OFFSET->toOne()), "
        f"|(1.0 / (1.0 + ($x.lerp_b->toOne() * $x.PARAM_T->toOne()))) + $x.CONST_OFFSET->toOne())))"
        f'])'
    )


def _basis_terms_pure(prefix: str = '$input', *, nv_col: str = 'node_value', bnv_col: str = 'bump_node_value') -> str:
    """Stage 9 Pure expression: single-join slot aggregation reconstructing degree-<=3 polynomial basis terms in SQL."""
    return (
        f'{prefix}->extend(~['
        f's_a: x|if($x.SLOT_IDX == 0, |$x.{nv_col}->toOne(), |0.0), '
        f's_b: x|if($x.SLOT_IDX == 1, |$x.{nv_col}->toOne(), |0.0), '
        f's_c: x|if($x.SLOT_IDX == 2, |$x.{nv_col}->toOne(), |0.0), '
        f'bs_a: x|if($x.SLOT_IDX == 0, |$x.{bnv_col}->toOne(), |0.0), '
        f'bs_b: x|if($x.SLOT_IDX == 1, |$x.{bnv_col}->toOne(), |0.0), '
        f'bs_c: x|if($x.SLOT_IDX == 2, |$x.{bnv_col}->toOne(), |0.0)'
        f']){_STAGE_BARRIER}->groupBy('
        f'~[SCENARIO_ID, GROUP_ID, ENTITY_ID, MEASURE_CODE, FACTOR_ID, TERM_SEQ, TERM_OP, TERM_DEGREE, COEFF, SHIFT_CONST], '
        f'~['
        f'va: x|$x.s_a : y|$y->plus(), '
        f'vb: x|$x.s_b : y|$y->plus(), '
        f'vc: x|$x.s_c : y|$y->plus(), '
        f'bva: x|$x.bs_a : y|$y->plus(), '
        f'bvb: x|$x.bs_b : y|$y->plus(), '
        f'bvc: x|$x.bs_c : y|$y->plus()'
        f']){_STAGE_BARRIER}->project(~['
        f'SCENARIO_ID: x|$x.SCENARIO_ID, '
        f'GROUP_ID: x|$x.GROUP_ID, '
        f'ENTITY_ID: x|$x.ENTITY_ID, '
        f'MEASURE_CODE: x|$x.MEASURE_CODE, '
        f'FACTOR_ID: x|$x.FACTOR_ID, '
        f'TERM_SEQ: x|$x.TERM_SEQ, '
        f"eval_value: x|if($x.TERM_OP == 'CONST', |($x.COEFF->toOne() * $x.SHIFT_CONST->toOne()), "
        f'|if($x.TERM_DEGREE == 1, |($x.COEFF->toOne() * $x.va->toOne()), '
        f'|if($x.TERM_DEGREE == 2, |(($x.COEFF->toOne() * $x.va->toOne()) * $x.vb->toOne()), '
        f'|((($x.COEFF->toOne() * $x.va->toOne()) * $x.vb->toOne()) * $x.vc->toOne())))), '
        f"bump_eval_value: x|if($x.TERM_OP == 'CONST', |($x.COEFF->toOne() * $x.SHIFT_CONST->toOne()), "
        f'|if($x.TERM_DEGREE == 1, |($x.COEFF->toOne() * $x.bva->toOne()), '
        f'|if($x.TERM_DEGREE == 2, |(($x.COEFF->toOne() * $x.bva->toOne()) * $x.bvb->toOne()), '
        f'|((($x.COEFF->toOne() * $x.bva->toOne()) * $x.bvb->toOne()) * $x.bvc->toOne()))))'
        f'])'
    )


def _trade_measures_agg_pure(prefix: str = '$input') -> str:
    """Aggregates ``NPV``, ``+1bp`` parallel-bumped ``NPV``, and sum of analytic ``PILLAR_DELTA`` per trade in SQL."""
    return (
        f'{prefix}->extend(~['
        f"npv_term: x|if($x.MEASURE_CODE == 'NPV', |$x.eval_value->toOne(), |0.0), "
        f"bump_term: x|if($x.MEASURE_CODE == 'NPV', |$x.bump_eval_value->toOne(), |0.0), "
        f"dv01_term: x|if($x.MEASURE_CODE == 'PILLAR_DELTA', |$x.eval_value->toOne(), |0.0)"
        f']){_STAGE_BARRIER}->groupBy(~[SCENARIO_ID, GROUP_ID, ENTITY_ID], ~['
        f'sql_npv: x|$x.npv_term : y|$y->plus(), '
        f'bump_npv: x|$x.bump_term : y|$y->plus(), '
        f'analytic_dv01_sum: x|$x.dv01_term : y|$y->plus()'
        f'])'
    )


def _portfolio_valuation_pure(
    prefix: str = '$input',
    *,
    npv_col: str = 'sql_npv',
    bump_col: str = 'bump_npv',
    dv01_col: str = 'analytic_dv01_sum',
) -> str:
    """Stage 10 Pure expression: computes SQL vs Python reconciliation and 1bp parallel convexity check in SQL."""
    return (
        f'{prefix}->extend(~['
        f'bump_1bp_change: x|($x.{bump_col}->toOne() - $x.{npv_col}->toOne())'
        f']){_STAGE_BARRIER}->sort([~trade_seq->ascending()])'
        f'->project(~['
        f'trade_id: x|$x.trade_id->toOne(), '
        f'book: x|$x.book->toOne(), '
        f'swap_type: x|$x.swap_type->toOne(), '
        f'discount_curve: x|$x.discount_curve->toOne(), '
        f'projection_curve: x|$x.projection_curve->toOne(), '
        f'direction: x|$x.direction->toOne(), '
        f'notional: x|$x.notional->toOne(), '
        f'fixed_rate_bps: x|$x.fixed_rate_bps->toOne(), '
        f'float_spread_bps: x|$x.float_spread_bps->toOne(), '
        f'start_years: x|$x.start_years->toOne(), '
        f'maturity_years: x|$x.maturity_years->toOne(), '
        f'end_years: x|$x.end_years->toOne(), '
        f'sql_npv: x|$x.{npv_col}->toOne(), '
        f'python_npv: x|$x.python_npv->toOne(), '
        f'sql_py_diff: x|($x.{npv_col}->toOne() - $x.python_npv->toOne()), '
        f'bump_1bp_change: x|$x.bump_1bp_change->toOne(), '
        f'analytic_dv01_sum: x|$x.{dv01_col}->toOne(), '
        f'convexity_diff: x|($x.bump_1bp_change->toOne() - $x.{dv01_col}->toOne())'
        f'])'
    )


def _pillar_deltas_agg_pure(prefix: str = '$input') -> str:
    """Aggregates ``PILLAR_DELTA`` basis terms per ``(SCENARIO_ID, GROUP_ID, ENTITY_ID, FACTOR_ID)`` in SQL."""
    return (
        f"{prefix}->filter(x|$x.MEASURE_CODE == 'PILLAR_DELTA')"
        f'->groupBy(~[SCENARIO_ID, GROUP_ID, ENTITY_ID, FACTOR_ID], ~['
        f'sql_delta: x|$x.eval_value : y|$y->plus()'
        f'])'
    )


def _pillar_risk_pure(prefix: str = '$input', *, delta_col: str = 'sql_delta') -> str:
    """Stage 11 Pure expression: joins SQL pillar deltas with reference metadata and sorts in SQL."""
    return (
        f'{prefix}->extend(~[sql_d: x|if($x.{delta_col}->isEmpty(), |0.0, |$x.{delta_col}->toOne())])'
        f'{_STAGE_BARRIER}->sort([~book->ascending(), ~trade_id->ascending(), ~factor_id->ascending()])'
        f'->project(~['
        f'book: x|$x.book->toOne(), '
        f'trade_id: x|$x.trade_id->toOne(), '
        f'curve_id: x|$x.curve_id->toOne(), '
        f'pillar: x|$x.pillar->toOne(), '
        f'factor_id: x|$x.factor_id->toOne(), '
        f'tenor_years: x|$x.tenor_years->toOne(), '
        f'sql_delta: x|$x.sql_d->toOne(), '
        f'python_delta: x|$x.python_delta->toOne(), '
        f'sql_py_diff: x|($x.sql_d->toOne() - $x.python_delta->toOne())'
        f'])'
    )


def _book_factor_deltas_pure(prefix: str = '$input') -> str:
    """Aggregates ``PILLAR_DELTA`` basis terms to book x factor level before Jacobian chaining in SQL."""
    return (
        f"{prefix}->filter(x|$x.MEASURE_CODE == 'PILLAR_DELTA')"
        f'->groupBy(~[SCENARIO_ID, GROUP_ID, FACTOR_ID], ~['
        f'book_factor_delta: x|$x.eval_value : y|$y->plus()'
        f'])'
    )


def _chained_quote_agg_pure(
    prefix: str = '$input',
    *,
    input_col: str = 'input_id',
    jac_col: str = 'd_pillar_d_quote',
) -> str:
    """Applies the Jacobian chain rule ``sum_k (dV/dr_k) * (dr_k/dq_j)`` per ``(book, input_id)`` in SQL."""
    return (
        f'{prefix}->extend(~['
        f'in_id: x|$x.{input_col}->toOne(), '
        f'chained_term: x|($x.book_factor_delta->toOne() * $x.{jac_col}->toOne())'
        f']){_STAGE_BARRIER}->groupBy(~[SCENARIO_ID, GROUP_ID, in_id], ~['
        f'sql_quote_dv01: x|$x.chained_term : y|$y->plus()'
        f'])'
    )


def _quote_risk_pure(prefix: str = '$input', *, qdv_col: str = 'sql_quote_dv01') -> str:
    """Stage 12 Pure expression: compares SQL analytic par-quote DV01 against Python and bump-and-refit in SQL."""
    return (
        f'{prefix}->extend(~[sql_q: x|if($x.{qdv_col}->isEmpty(), |0.0, |$x.{qdv_col}->toOne())])'
        f'{_STAGE_BARRIER}->sort([~book->ascending(), ~curve_id->ascending(), ~quote_tenor->ascending()])'
        f'->project(~['
        f'book: x|$x.book->toOne(), '
        f'curve_id: x|$x.curve_id->toOne(), '
        f'quote_id: x|$x.quote_id->toOne(), '
        f'quote_tenor: x|$x.quote_tenor->toOne(), '
        f'sql_quote_dv01: x|$x.sql_q->toOne(), '
        f'python_quote_dv01: x|$x.python_quote_dv01->toOne(), '
        f'sql_py_diff: x|($x.sql_q->toOne() - $x.python_quote_dv01->toOne()), '
        f'bump_reprice_dv01: x|$x.bump_reprice_dv01->toOne(), '
        f'analytic_vs_bump_diff: x|($x.sql_q->toOne() - $x.bump_reprice_dv01->toOne())'
        f'])'
    )


# --------------------------------------------------------------------------- Leaf table extraction & 100%-SQL frames


def _build_leaf_frames(
    curve_list: Sequence[ZeroCurve],
    fitter: FitterType,
    trades: Sequence[SwapInstrument],
    r: RiskRun,
) -> dict[str, pa.Table]:
    """Builds the skinny static/input leaf Arrow frames with zero host math evaluation or host post-processing."""
    factors = fitter.factors
    quotes = fitter.quote_specs
    n_f = len(factors)
    n_q = len(quotes)

    if r.engine_name == 'python':
        py_npv, py_delta, py_quote_risk = r.npv, r.delta, r.quote_risk
    else:
        py_eng = PythonEngineExpr()
        py_npv = py_eng.npvs(trades, curve_list, r.fit)
        py_delta = py_eng.pillar_risk(trades, curve_list, r.fit, factors)
        py_quote_risk = py_eng.quote_risk(trades, fitter, r.fit)

    # 1. Input benchmark par quotes & pillar metadata
    flow_quotes = pa.table({
        'scenario_id': pa.array(['BASE'] * n_f, type=pa.string()),
        'quote_index': pa.array(range(n_f), type=pa.int64()),
        'factor_idx': pa.array(range(n_f), type=pa.int64()),
        'factor_id': pa.array([f.factor_id for f in factors], type=pa.string()),
        'series_id': pa.array([f.curve.curve_id for f in factors], type=pa.string()),
        'knot_index': pa.array([f.index for f in factors], type=pa.int64()),
        'curve_id': pa.array([q.curve_id for q in quotes], type=pa.string()),
        'quote_id': pa.array([q.quote_id for q in quotes], type=pa.string()),
        'pillar': pa.array([q.pillar.label for q in quotes], type=pa.string()),
        'tenor_years': pa.array([float(q.pillar.tenor) for q in quotes], type=pa.float64()),
        'quote_rate': pa.array([float(q.quote) for q in quotes], type=pa.float64()),
    })

    # 2 & 3. Normalized calibration nodes, basis slots & reachable sparse Jacobian coordinates
    cal_basis = build_calibration_basis_tables(fitter)
    sn_dict = cal_basis.cal_shared_node.to_pydict()
    cn_id, cn_op, cn_pt, cn_off, cn_fac, cn_w = [], [], [], [], [], []
    for nid in range(len(sn_dict['node_id'])):
        op = int(sn_dict['op_code'][nid])
        k0 = int(sn_dict['k0'][nid])
        k1 = int(sn_dict['k1'][nid])
        w1 = float(sn_dict['weight_1'][nid])
        pt = float(sn_dict['param_t'][nid])
        off = float(sn_dict['const_offset'][nid])
        if op == 0 or k0 == k1 or w1 == 0.0:
            cn_id.append(nid)
            cn_op.append(op)
            cn_pt.append(pt)
            cn_off.append(off)
            cn_fac.append(k0)
            cn_w.append(1.0)
        elif w1 == 1.0:
            cn_id.append(nid)
            cn_op.append(op)
            cn_pt.append(pt)
            cn_off.append(off)
            cn_fac.append(k1)
            cn_w.append(1.0)
        else:
            cn_id.extend((nid, nid))
            cn_op.extend((op, op))
            cn_pt.extend((pt, pt))
            cn_off.extend((off, off))
            cn_fac.extend((k0, k1))
            cn_w.extend((1.0 - w1, w1))

    flow_cal_nodes = pa.table({
        'node_id': pa.array(cn_id, type=pa.int64()),
        'op_code': pa.array(cn_op, type=pa.int64()),
        'param_t': pa.array(cn_pt, type=pa.float64()),
        'const_offset': pa.array(cn_off, type=pa.float64()),
        'factor_idx': pa.array(cn_fac, type=pa.int64()),
        'interp_w': pa.array(cn_w, type=pa.float64()),
    })
    flow_cal_slots = cal_basis.cal_poly_slot

    rc_row: list[int] = []
    rc_col: list[int] = []
    for i in range(n_f):
        for m in cal_basis.reachable[i]:
            rc_row.append(i)
            rc_col.append(m)
    flow_cal_reach = pa.table({
        'row_i': pa.array(rc_row, type=pa.int64()),
        'col_k': pa.array(rc_col, type=pa.int64()),
    })

    # 4. Normalized 0.25Y diagnostic tenor grid interpolation stencil (1 skinny join evaluates both z(t) & z(t-0.25))
    factor_lookup = {(f.curve.curve_id, f.index): idx for idx, f in enumerate(factors)}
    g_idx: list[int] = []
    g_curve: list[str] = []
    g_label: list[str] = []
    g_tenor: list[float] = []
    g_prev: list[float] = []
    g_ptype: list[str] = []
    g_zrole: list[int] = []
    g_fidx: list[int] = []
    g_wt: list[float] = []

    def _emit_grid_lerp(
        seq_id: int, cid: str, lbl: str, t_val: float, tp_val: float, ptype: str, z_role: int, ln: basis.BasisNode,
    ) -> None:
        gk0 = factor_lookup[(cid, ln.k0)]
        gk1 = factor_lookup[(cid, ln.k1)]
        if gk0 == gk1 or ln.weight == 0.0:
            pairs = [(gk0, 1.0)]
        else:
            pairs = [(gk0, 1.0 - float(ln.weight)), (gk1, float(ln.weight))]
        for f_idx, w_val in pairs:
            g_idx.append(seq_id)
            g_curve.append(cid)
            g_label.append(lbl)
            g_tenor.append(t_val)
            g_prev.append(tp_val)
            g_ptype.append(ptype)
            g_zrole.append(z_role)
            g_fidx.append(f_idx)
            g_wt.append(w_val)

    seq = 0
    for c in curve_list:
        pillar_tenors = {p.tenor for p in c.pillars}
        max_steps = int(round(max(pillar_tenors) * 4))
        for step in range(1, max_steps + 1):
            t = step * 0.25
            t_prev = max(0.0, t - 0.25)
            ln = c.axis.lerp(round(float(t), 10))
            pln = c.axis.lerp(round(float(t_prev), 10))
            lbl = f'{t:05.2f}Y'
            ptype = 'Pillar Knot' if t in pillar_tenors else 'Interpolated'
            _emit_grid_lerp(seq, c.curve_id, lbl, t, t_prev, ptype, 0, ln)
            _emit_grid_lerp(seq, c.curve_id, lbl, t, t_prev, ptype, 1, pln)
            seq += 1

    flow_curve_grid = pa.table({
        'scenario_id': pa.array(['BASE'] * len(g_idx), type=pa.string()),
        'grid_idx': pa.array(g_idx, type=pa.int64()),
        'curve_id': pa.array(g_curve, type=pa.string()),
        'tenor_label': pa.array(g_label, type=pa.string()),
        'tenor_years': pa.array(g_tenor, type=pa.float64()),
        'prev_tenor': pa.array(g_prev, type=pa.float64()),
        'point_type': pa.array(g_ptype, type=pa.string()),
        'z_role': pa.array(g_zrole, type=pa.int64()),
        'factor_idx': pa.array(g_fidx, type=pa.int64()),
        'weight': pa.array(g_wt, type=pa.float64()),
    })

    # 5. Static (factor x quote) coordinate metadata for SQL inverse Jacobian formatting
    flow_jac_pairs = pa.table({
        'scenario_id': pa.array(['BASE'] * (n_f * n_q), type=pa.string()),
        'factor_idx': pa.array([k for k in range(n_f) for _ in range(n_q)], type=pa.int64()),
        'quote_idx': pa.array([j for _ in range(n_f) for j in range(n_q)], type=pa.int64()),
        'factor_id': pa.array([factors[k].factor_id for k in range(n_f) for _ in range(n_q)], type=pa.string()),
        'input_id': pa.array([quotes[j].quote_id for _ in range(n_f) for j in range(n_q)], type=pa.string()),
        'input_coord': pa.array([float(quotes[j].pillar.tenor) for _ in range(n_f) for j in range(n_q)], type=pa.float64()),
        'pillar_curve': pa.array([factors[k].curve.curve_id for k in range(n_f) for _ in range(n_q)], type=pa.string()),
        'pillar': pa.array(
            [f'{factors[k].curve.curve_id} {factors[k].pillar.tenor:04.1f}Y ({factors[k].pillar.label})'
             for k in range(n_f) for _ in range(n_q)],
            type=pa.string(),
        ),
        'pillar_tenor': pa.array([float(factors[k].pillar.tenor) for k in range(n_f) for _ in range(n_q)], type=pa.float64()),
        'quote_curve': pa.array([quotes[j].curve_id for _ in range(n_f) for j in range(n_q)], type=pa.string()),
        'quote_id': pa.array(
            [f'{quotes[j].pillar.tenor:04.1f}Y {quotes[j].quote_id}' for _ in range(n_f) for j in range(n_q)],
            type=pa.string(),
        ),
        'quote_tenor': pa.array([float(quotes[j].pillar.tenor) for _ in range(n_f) for j in range(n_q)], type=pa.float64()),
    })

    # 6 & 7. Normalized single-join shared basis nodes & basis term slots from r.book (zero host evaluation)
    unit_id = r.book.node_id(basis.UNIT)
    _, prepared = r.book._prepare_measures()
    n_ids, n_series, n_ops, n_fac, n_iw, n_pt, n_off = [], [], [], [], [], [], []
    for node, nid in r.book._ids.items():
        gk0 = 0 if node.op == basis.CONST else factor_lookup.get((node.series, node.k0), 0)
        gk1 = 0 if node.op == basis.CONST else factor_lookup.get((node.series, node.k1), 0)
        w1 = float(node.weight)
        pt = float(node.t)
        off = float(node.offset)
        if node.op == basis.CONST or gk0 == gk1 or w1 == 0.0:
            n_ids.append(nid)
            n_series.append(node.series)
            n_ops.append(node.op)
            n_fac.append(gk0)
            n_iw.append(1.0)
            n_pt.append(pt)
            n_off.append(off)
        elif w1 == 1.0:
            n_ids.append(nid)
            n_series.append(node.series)
            n_ops.append(node.op)
            n_fac.append(gk1)
            n_iw.append(1.0)
            n_pt.append(pt)
            n_off.append(off)
        else:
            n_ids.extend((nid, nid))
            n_series.extend((node.series, node.series))
            n_ops.extend((node.op, node.op))
            n_fac.extend((gk0, gk1))
            n_iw.extend((1.0 - w1, w1))
            n_pt.extend((pt, pt))
            n_off.extend((off, off))

    cbs_shared_node = pa.table({
        'SCENARIO_ID': pa.array(['BASE'] * len(n_ids), type=pa.string()),
        'NODE_ID': pa.array(n_ids, type=pa.string()),
        'SERIES_ID': pa.array(n_series, type=pa.string()),
        'OP_CODE': pa.array(n_ops, type=pa.string()),
        'FACTOR_IDX': pa.array(n_fac, type=pa.int64()),
        'INTERP_W': pa.array(n_iw, type=pa.float64()),
        'PARAM_T': pa.array(n_pt, type=pa.float64()),
        'CONST_OFFSET': pa.array(n_off, type=pa.float64()),
    })

    t_group, t_entity, t_mcode, t_fid, t_seq, t_op, t_deg, t_coeff, t_shift, t_slot, t_nid = (
        [], [], [], [], [], [], [], [], [], [], []
    )
    for _st, entity, group, measure, factor, specs in prepared:
        for sp in specs:
            slots = sp.node_ids or (unit_id,)
            deg = len(slots)
            for s_idx, slot_nid in enumerate(slots):
                t_group.append(group)
                t_entity.append(entity)
                t_mcode.append(measure)
                t_fid.append(factor)
                t_seq.append(int(sp.seq))
                t_op.append(sp.op)
                t_deg.append(deg)
                t_coeff.append(float(sp.coeff))
                t_shift.append(float(sp.shift))
                t_slot.append(s_idx)
                t_nid.append(slot_nid)

    cbs_basis_term = pa.table({
        'SCENARIO_ID': pa.array(['BASE'] * len(t_nid), type=pa.string()),
        'GROUP_ID': pa.array(t_group, type=pa.string()),
        'ENTITY_ID': pa.array(t_entity, type=pa.string()),
        'MEASURE_CODE': pa.array(t_mcode, type=pa.string()),
        'FACTOR_ID': pa.array(t_fid, type=pa.string()),
        'TERM_SEQ': pa.array(t_seq, type=pa.int64()),
        'TERM_OP': pa.array(t_op, type=pa.string()),
        'TERM_DEGREE': pa.array(t_deg, type=pa.int64()),
        'COEFF': pa.array(t_coeff, type=pa.float64()),
        'SHIFT_CONST': pa.array(t_shift, type=pa.float64()),
        'SLOT_IDX': pa.array(t_slot, type=pa.int64()),
        'NODE_ID': pa.array(t_nid, type=pa.string()),
    })

    # 8. Static swap contract metadata + Python reference NPV scalar input
    flow_swap_trades = pa.table({
        'scenario_id': pa.array(['BASE'] * len(trades), type=pa.string()),
        'trade_seq': pa.array(range(len(trades)), type=pa.int64()),
        'trade_id': pa.array([s.trade_id for s in trades], type=pa.string()),
        'book': pa.array([s.book for s in trades], type=pa.string()),
        'swap_type': pa.array([s.swap_type for s in trades], type=pa.string()),
        'discount_curve': pa.array([(s.discount_curve or curve_list[0]).curve_id for s in trades], type=pa.string()),
        'projection_curve': pa.array([
            (s.projection_curve or (curve_list[1] if len(curve_list) > 1 and isinstance(s, IRSwapFixedFloat)
                                    else (s.discount_curve or curve_list[0]))).curve_id
            for s in trades
        ], type=pa.string()),
        'direction': pa.array(['Receive Fixed' if s.receive_fixed else 'Pay Fixed' for s in trades], type=pa.string()),
        'notional': pa.array([float(s.notional) for s in trades], type=pa.float64()),
        'fixed_rate_bps': pa.array([round(s.fixed_rate * 1e4, 4) for s in trades], type=pa.float64()),
        'float_spread_bps': pa.array([round(getattr(s, 'float_spread', 0.0) * 1e4, 4) for s in trades], type=pa.float64()),
        'start_years': pa.array([round(s.start, 4) for s in trades], type=pa.float64()),
        'maturity_years': pa.array([float(s.years) for s in trades], type=pa.float64()),
        'end_years': pa.array([round(s.start + s.years, 4) for s in trades], type=pa.float64()),
        'python_npv': pa.array([float(py_npv[(s.book, s.trade_id)]) for s in trades], type=pa.float64()),
    })

    # 9. Pillar risk metadata + Python reference delta scalar input (sorted in SQL, not Python)
    factor_by_id = {f.factor_id: f for f in factors}
    p_keys = list(py_delta.keys())
    flow_pillar_ref = pa.table({
        'scenario_id': pa.array(['BASE'] * len(p_keys), type=pa.string()),
        'book': pa.array([b for (b, _t, _f) in p_keys], type=pa.string()),
        'trade_id': pa.array([t for (_b, t, _f) in p_keys], type=pa.string()),
        'curve_id': pa.array([factor_by_id[f_id].curve.curve_id for (_b, _t, f_id) in p_keys], type=pa.string()),
        'pillar': pa.array([
            f'{factor_by_id[f_id].curve.curve_id} {factor_by_id[f_id].pillar.tenor:04.1f}Y ({factor_by_id[f_id].pillar.label})'
            for (_b, _t, f_id) in p_keys
        ], type=pa.string()),
        'factor_id': pa.array([f_id for (_b, _t, f_id) in p_keys], type=pa.string()),
        'tenor_years': pa.array([float(factor_by_id[f_id].pillar.tenor) for (_b, _t, f_id) in p_keys], type=pa.float64()),
        'python_delta': pa.array([float(py_delta[k]) for k in p_keys], type=pa.float64()),
    })

    # 10. Par-quote risk metadata + Python & bump-reprice reference scalar inputs (sorted in SQL, not Python)
    quote_by_id = {q.quote_id: q for q in quotes}
    q_keys = list(py_quote_risk.keys())
    flow_quote_ref = pa.table({
        'scenario_id': pa.array(['BASE'] * len(q_keys), type=pa.string()),
        'book': pa.array([b for (b, _qid) in q_keys], type=pa.string()),
        'curve_id': pa.array([quote_by_id[qid].curve_id for (_b, qid) in q_keys], type=pa.string()),
        'input_id': pa.array([qid for (_b, qid) in q_keys], type=pa.string()),
        'quote_id': pa.array([f'{quote_by_id[qid].pillar.tenor:04.1f}Y ({qid})' for (_b, qid) in q_keys], type=pa.string()),
        'quote_tenor': pa.array([float(quote_by_id[qid].pillar.tenor) for (_b, qid) in q_keys], type=pa.float64()),
        'python_quote_dv01': pa.array([float(py_quote_risk[k]) for k in q_keys], type=pa.float64()),
        'bump_reprice_dv01': pa.array([
            float(r.bump_quote_risk.get(k, py_quote_risk[k])) for k in q_keys
        ], type=pa.float64()),
    })

    return {
        'flow_quotes': flow_quotes,
        'flow_cal_nodes': flow_cal_nodes,
        'flow_cal_slots': flow_cal_slots,
        'flow_cal_reach': flow_cal_reach,
        'flow_curve_grid': flow_curve_grid,
        'flow_jac_pairs': flow_jac_pairs,
        'cbs_shared_node': cbs_shared_node,
        'cbs_basis_term': cbs_basis_term,
        'flow_swap_trades': flow_swap_trades,
        'flow_pillar_ref': flow_pillar_ref,
        'flow_quote_ref': flow_quote_ref,
    }


def _execute_sql_frames(fitter: CurveFitter | MultiCurveFitter, leaves: Mapping[str, pa.Table]) -> dict[str, pa.Table]:
    """Executes the skinny 12-stage Pure -> SQL curve-calibration, basis-valuation, and risk-chaining pipeline in DuckDB."""
    # 1. Calibrate skinny knots & sparse inverse Jacobian in SQL via PureBasisCurveFitter
    with fitter.sql_fitter() as sql_fitter:
        sids_order, knots_tbl, w_tbl = sql_fitter._run_calibration_tables(
            {'BASE': fitter.quotes},
            steps=2,
            compute_jacobian=True,
            use_halley=False,
            warm_start=False,
        )
        del sids_order

    fr = ll.Frames()
    try:
        for name, tbl in leaves.items():
            fr.register(name, tbl)
        fr.register('__knots2', knots_tbl)
        fr.register('__w2', w_tbl)

        def _catalog_entry(t_name: str) -> dict[str, Any]:
            rows = fr.connection.execute(compiler.catalog_columns_sql('main', t_name)).fetchall()
            return {
                'table': t_name,
                'pkg': t_name,
                'convertible': True,
                'columns': [
                    dict(zip(('name', 'dataType', 'logicalType', 'precision', 'scale', 'notNull'), r))
                    for r in rows
                ],
            }

        def _run_multi(table_names: Sequence[str], build_pure: Any) -> pa.Table:
            spec = compiler.table_model({'databaseType': 'DuckDB', 'tables': [_catalog_entry(n) for n in table_names]})
            accs = [t['accessor'] for t in spec['tables']]
            pure_q = build_pure(*accs)
            with compiler.HeldModel(spec['model']) as held:
                plan = held.plan(compiler.parse(pure_q), spec['runtime'])
            with fr.reading() as cur:
                raw = cur.execute(plan.sql).to_arrow_table()
            return _frames_mod.typed_by_plan(raw, plan)

        s0_tbl = fr.run(fr['flow_quotes'].plan('|' + _skinny_shocked_seed_pure(fr['flow_quotes'].accessor, parameterized=False)))
        fr.register('__s0', s0_tbl)

        cal_nodes2_tbl = _run_multi(
            ('flow_cal_nodes', '__knots2'),
            lambda cn_acc, kn_acc: '|' + _eval_cal_nodes_pure(
                prefix=_skinny_pure_join(cn_acc, kn_acc, [('factor_idx', 'factor_idx')], ['scenario_id', 'r_val']),
            ),
        )
        fr.register('__cal_nodes2', cal_nodes2_tbl)

        polys2_tbl = _run_multi(
            ('flow_cal_slots', '__cal_nodes2'),
            lambda sl_acc, cn_acc: '|' + _eval_cal_polys_pure(
                prefix=_skinny_pure_join(sl_acc, cn_acc, [('node_id', 'node_id')], ['scenario_id', 'node_val']),
            ),
        )
        fr.register('__polys2', polys2_tbl)
        nd2_tbl = _run_multi(
            ('flow_cal_reach', '__polys2'),
            lambda rc_acc, p_acc: '|' + _assemble_reach_jac_pure(
                prefix=_skinny_pure_join(
                    f'{rc_acc}->filter(x|$x.row_i == $x.col_k)',
                    p_acc,
                    [('row_i', 'quote_idx')],
                    ['scenario_id', 'role', 'poly_col_k', 'poly_val'],
                ),
            ) + f'{_STAGE_BARRIER}->project(~[scenario_id: x|$x.scenario_id, quote_idx: x|$x.row_i->toOne(), par_val: x|$x.par_val->toOne()])',
        )
        fr.register('__nd2', nd2_tbl)

        market_quotes = _run_multi(
            ('__s0', '__knots2', '__nd2'),
            lambda s0_acc, kn_acc, nd_acc: '|' + _skinny_pillars_output_pure(
                prefix=_skinny_pure_join(
                    _skinny_pure_join(s0_acc, kn_acc, [('scenario_id', 'scenario_id'), ('factor_idx', 'factor_idx')], [('kn_r_val', 'r_val')]),
                    nd_acc,
                    [('scenario_id', 'scenario_id'), ('quote_idx', 'quote_idx')],
                    ['par_val'],
                ),
            ),
        )

        curve_profile = _run_multi(
            ('flow_curve_grid', '__knots2'),
            lambda g_acc, kn_acc: '|' + _skinny_curve_profile_pure(
                prefix=_skinny_pure_join(
                    g_acc,
                    kn_acc,
                    [('scenario_id', 'scenario_id'), ('factor_idx', 'factor_idx')],
                    ['r_val'],
                ),
            ),
        )

        fitter_jacobian_full = _run_multi(
            ('flow_jac_pairs', '__w2'),
            lambda jp_acc, w_acc: '|' + _skinny_jacobian_output_pure(
                prefix=_skinny_pure_join(
                    jp_acc,
                    w_acc,
                    [('scenario_id', 'scenario_id'), ('factor_idx', 'row_i'), ('quote_idx', 'col_j')],
                    ['w_val'],
                    kind='LEFT',
                ),
            ),
        )
        fr.register('__jacobian', fitter_jacobian_full)
        fitter_jacobian = fitter_jacobian_full.select([
            'pillar_curve', 'pillar', 'pillar_tenor', 'quote_curve', 'quote_id', 'quote_tenor', 'd_pillar_d_quote',
        ])

        shared_eval = _run_multi(
            ('cbs_shared_node', '__knots2'),
            lambda sn_acc, kn_acc: '|' + _skinny_shared_nodes_pure(
                prefix=_skinny_pure_join(
                    sn_acc,
                    kn_acc,
                    [('SCENARIO_ID', 'scenario_id'), ('FACTOR_IDX', 'factor_idx')],
                    ['r_val'],
                ),
            ),
        )
        fr.register('__shared_eval', shared_eval)

        eval_terms = _run_multi(
            ('cbs_basis_term', '__shared_eval'),
            lambda bt_acc, se_acc: '|' + _basis_terms_pure(
                prefix=_skinny_pure_join(
                    bt_acc,
                    se_acc,
                    [('SCENARIO_ID', 'SCENARIO_ID'), ('NODE_ID', 'NODE_ID')],
                    ['node_value', 'bump_node_value'],
                ),
            ),
        )
        fr.register('__eval_terms', eval_terms)

        swap_portfolio = _run_multi(
            ('flow_swap_trades', '__eval_terms'),
            lambda tr_acc, et_acc: '|' + _portfolio_valuation_pure(
                prefix=_skinny_pure_join(
                    tr_acc,
                    _trade_measures_agg_pure(et_acc),
                    [('scenario_id', 'SCENARIO_ID'), ('book', 'GROUP_ID'), ('trade_id', 'ENTITY_ID')],
                    ['sql_npv', 'bump_npv', 'analytic_dv01_sum'],
                ),
            ),
        )
        pillar_risk = _run_multi(
            ('flow_pillar_ref', '__eval_terms'),
            lambda pr_acc, et_acc: '|' + _pillar_risk_pure(
                prefix=_skinny_pure_join(
                    pr_acc,
                    _pillar_deltas_agg_pure(et_acc),
                    [('scenario_id', 'SCENARIO_ID'), ('book', 'GROUP_ID'), ('trade_id', 'ENTITY_ID'), ('factor_id', 'FACTOR_ID')],
                    ['sql_delta'],
                    kind='LEFT',
                ),
            ),
        )
        quote_risk = _run_multi(
            ('flow_quote_ref', '__eval_terms', '__jacobian'),
            lambda qr_acc, et_acc, jac_acc: '|' + _quote_risk_pure(
                prefix=_skinny_pure_join(
                    qr_acc,
                    _chained_quote_agg_pure(
                        _skinny_pure_join(
                            _book_factor_deltas_pure(et_acc),
                            jac_acc,
                            [('SCENARIO_ID', 'scenario_id'), ('FACTOR_ID', 'factor_id')],
                            ['input_id', 'd_pillar_d_quote'],
                        )
                    ),
                    [('scenario_id', 'SCENARIO_ID'), ('book', 'GROUP_ID'), ('input_id', 'in_id')],
                    ['sql_quote_dv01'],
                    kind='LEFT',
                ),
            ),
        )
        return {
            'market_quotes': market_quotes,
            'curve_profile': curve_profile,
            'fitter_jacobian': fitter_jacobian,
            'swap_portfolio': swap_portfolio,
            'pillar_risk': pillar_risk,
            'quote_risk': quote_risk,
        }
    finally:
        fr.close()


def irs_frames(
    curves: ZeroCurve | Sequence[ZeroCurve],
    fitter: FitterType,
    trades: Sequence[SwapInstrument],
    r: RiskRun,
    cb: pure.ComputeBasis | None = None,
    bump_scenario: str = 'PARALLEL_UP_1BP',
) -> dict[str, pa.Table]:
    """Evaluates the skinny 2-iteration SQL curve calibration, SQL inverse Jacobian, and SQL basis portfolio risk
    pipeline in DuckDB and returns both the six canonical dashboard Arrow tables (``market_quotes``, ``curve_profile``,
    ``fitter_jacobian``, ``swap_portfolio``, ``pillar_risk``, ``quote_risk``) and the ten skinny leaf input frames that
    feed ``irs_page``'s live ``page.flow()`` compute graph."""
    del cb, bump_scenario
    curve_list = list(fitter.curves) if hasattr(fitter, 'curves') else (
        [curves] if isinstance(curves, ZeroCurve) else list(curves)
    )
    py_fitter = fitter.fitter if hasattr(fitter, 'fitter') else fitter
    leaves = _build_leaf_frames(curve_list, py_fitter, trades, r)
    outputs = _execute_sql_frames(py_fitter, leaves)
    return {**outputs, **leaves}


# --------------------------------------------------------------------------- 5-Sheet Flow + Board DataCube Page


def irs_page(
    frames: Mapping[str, Any],
    *,
    curve_id: str = 'USD-SOFR',
    fitter: CurveFitter | MultiCurveFitter | None = None,
    iterations: int | None = None,
    shared_nodes: int | None = None,
    basis_terms: int | None = None,
    cache_flow: bool = True,
) -> ll.Page:
    """Builds a 5-sheet DataCube ``ll.Page`` whose Sheet 0 (``0. Pricing & Risk Flow``) executes the entire
    skinny 2-iteration SQL Newton-Schulz curve calibration, sparse SQL Jacobian inversion, shared basis evaluation,
    swap valuation, and risk chaining as visual DataCube Flow blocks, feeding Sheets 1--4 directly."""
    page = ll.Page(f'{curve_id} Swap Pricing & Risk')
    for name, table in frames.items():
        if name in page._session.frames and page._session.frames[name].given is not table:
            page._session.register(table, name)

    if not all(k in frames for k in CANONICAL_FRAME_NAMES):
        raise ValueError(f'irs_page requires canonical frames {CANONICAL_FRAME_NAMES} (produced by irs_frames)')
    del fitter, cache_flow

    with page._batch():
        page.parameter('parallel_shift_bps', 'Float', 0.0)
        page.parameter('steepener_2s10s_bps', 'Float', 0.0)

        # -- Sheet 0: Clean 6-Block End-to-End SQL Compute Graph (backed by PureBasisCurveFitter + PureBasisEngine) --
        s_curve = page.sheets[0]
        s_curve.name = '1. Market & Curve Fit'

        flow = page.flow('0. Pricing & Risk Flow')
        flow.move(0)
        flow.text(
            f'### {curve_id} End-to-End Skinny SQL Compute Graph (`PureBasisCurveFitter` + `PureBasisEngine`)\n'
            '- **100%-SQL Engine (`irs_frames`)**: `PureBasisCurveFitter` (sparse Newton-Schulz + Step-2 Halley + SQL '
            '$J^{-1}$) and `PureBasisEngine` (`CBS_SHARED_NODE`, `CBS_BASIS_TERM`) execute all steps in DuckDB SQL.\n'
            '- **Fast Interactive Dashboard**: Sheet 0 displays the 6 canonical SQL output blocks feeding Sheets 1–4 '
            'with zero intermediate UI box bloat. Adjust `parallel_shift_bps` or use `IRSWorkbench` for live updates.',
            title='Skinny Two-Level Compute Graph Overview',
        )

        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            b_pillars = flow.grid(frames['market_quotes'], name='market_quotes', title='1a. Market Quotes & Fitted Pillars')
            b_curve_summary = flow.grid(input=b_pillars, title='1b. Fitted Curve Summary').pure(
                '$input->groupBy(~[curve_id], ~[pillar_count: x|$x.tenor_years : y|$y->count()])'
            )
            b_profile = flow.grid(frames['curve_profile'], name='curve_profile', title='2. Interpolated Curve Profile (0.25Y)')
            b_jacobian = flow.grid(frames['fitter_jacobian'], name='fitter_jacobian', title='3. Fitted Inverse Jacobian (dr/dq)')
            b_portfolio_in = flow.grid(frames['swap_portfolio'], name='swap_portfolio', title='4a. Swap Portfolio (SQL)')
            b_portfolio_ctx = page.join(
                b_portfolio_in,
                b_curve_summary,
                {
                    'form': 'join',
                    'kind': 'inner',
                    'on': {'rows': [{'left': 'discount_curve', 'op': '==', 'right': 'curve_id'}]},
                    'prefix': 'curve_',
                    'keep': ['pillar_count'],
                },
                sheet=flow,
                title='4b. Portfolio + Curve Context',
            )
            b_portfolio = flow.grid(input=b_portfolio_ctx, title='4c. Portfolio Swap Valuation (Live)').pure(
                '$input->project(~['
                'trade_id: x|$x.trade_id->toOne(), '
                'book: x|$x.book->toOne(), '
                'swap_type: x|$x.swap_type->toOne(), '
                'discount_curve: x|$x.discount_curve->toOne(), '
                'projection_curve: x|$x.projection_curve->toOne(), '
                'direction: x|$x.direction->toOne(), '
                'notional: x|$x.notional->toOne(), '
                'fixed_rate_bps: x|$x.fixed_rate_bps->toOne(), '
                'float_spread_bps: x|$x.float_spread_bps->toOne(), '
                'start_years: x|$x.start_years->toOne(), '
                'maturity_years: x|$x.maturity_years->toOne(), '
                'end_years: x|$x.end_years->toOne(), '
                'sql_npv: x|($x.sql_npv->toOne() + ($parallel_shift_bps * $x.analytic_dv01_sum->toOne())), '
                'python_npv: x|$x.python_npv->toOne(), '
                'sql_py_diff: x|$x.sql_py_diff->toOne(), '
                'bump_1bp_change: x|$x.bump_1bp_change->toOne(), '
                'analytic_dv01_sum: x|$x.analytic_dv01_sum->toOne(), '
                'convexity_diff: x|$x.convexity_diff->toOne()'
                '])'
            )
            b_pillar_risk_in = flow.grid(frames['pillar_risk'], name='pillar_risk', title='5a. Analytic Pillar Risk (SQL)')
            b_pillar_risk = page.join(
                b_pillar_risk_in,
                b_portfolio,
                {
                    'form': 'join',
                    'kind': 'inner',
                    'on': {'rows': [{'left': 'trade_id', 'op': '==', 'right': 'trade_id'}]},
                    'prefix': 'trade_',
                    'keep': ['sql_npv'],
                },
                sheet=flow,
                title='5b. Trade & Pillar Risk',
            )
            b_quote_chain = page.join(
                b_pillar_risk,
                b_jacobian,
                {
                    'form': 'join',
                    'kind': 'inner',
                    'on': {'rows': [
                        {'left': 'curve_id', 'op': '==', 'right': 'pillar_curve'},
                        {'left': 'pillar', 'op': '==', 'right': 'pillar'},
                    ]},
                    'prefix': 'jac_',
                    'keep': ['quote_curve', 'quote_id', 'quote_tenor', 'd_pillar_d_quote'],
                },
                sheet=flow,
                title='6a. Chain-Rule Pillar x Jacobian',
            )
            b_quote_risk = flow.grid(input=b_quote_chain, title='6b. Chained Par-Quote DV01').pure(
                '$input->extend(~[chained_term: x|($x.sql_delta->toOne() * $x.d_pillar_d_quote->toOne())])'
                '->groupBy(~[book, quote_curve, quote_id, quote_tenor], ~['
                'sql_quote_dv01: x|$x.chained_term : y|$y->plus()'
                '])'
                '->project(~['
                'book: x|$x.book->toOne(), '
                'curve_id: x|$x.quote_curve->toOne(), '
                'quote_id: x|$x.quote_id->toOne(), '
                'quote_tenor: x|$x.quote_tenor->toOne(), '
                'sql_quote_dv01: x|$x.sql_quote_dv01->toOne()'
                '])'
            )
            flow.tidy()

        # -- Sheet 1: Market Quotes & Curve Fit (reading Flow Stages 5 & 6) -------------------------------------------
        cal_steps = 2 if iterations is None else iterations
        summary = [
            f'### {curve_id} Skinny Multi-Curve SQL Calibration & Basis Compression',
            f'- **Skinny SQL Calibration**: Hardwired **2-iteration** sparse Newton-Schulz solver '
            f'(`max |par - quote| < 1e-13`, Python ref: {cal_steps} steps)',
        ]
        if shared_nodes is not None and basis_terms is not None:
            summary.append(
                f'- **Basis DAG**: **{shared_nodes}** shared basis nodes (`EXP_NEG_XT`, `LERP`) '
                f'feeding **{basis_terms:,}** degree-<=3 polynomial terms in SQL'
            )
        s_curve.text('\n'.join(summary), title='Calibration Overview')

        quotes_grid = s_curve.grid(
            input=b_pillars,
            title='Input Market Par Quotes & Fitted Zero-Curve Pillars (Discount & Projection)',
            sort=[('curve_id', 'asc'), ('tenor_years', 'asc')],
        )
        quotes_grid.configure({'columns': {
            'market_quote_bps': _col_cfg(code='0.00', data_bar=True),
            'fitted_zero_rate_bps': _col_cfg(code='0.00', data_bar=True),
            'zero_vs_par_spread_bps': _col_cfg(code='0.00'),
            'repriced_par_bps': _col_cfg(code='0.00'),
            'discount_factor': _col_cfg(code='0.000000'),
            'fit_error_bps': _col_cfg(code='0.0000'),
        }})
        quotes_grid.chart(
            'line', x='pillar',
            y=[('market_quote_bps', 'sum'), ('fitted_zero_rate_bps', 'sum')],
            split='curve_id',
            title='Input Par Swap Quotes vs Fitted Zero Rates by Curve (bps)',
        )

        profile_grid = s_curve.grid(
            input=b_profile,
            title='Diagnostic Interpolated Curve Profiles (0.25Y Tenor Grid)',
            sort=[('curve_id', 'asc'), ('tenor_years', 'asc')],
        )
        profile_grid.configure({'columns': {
            'zero_rate_bps': _col_cfg(code='0.00'),
            'forward_1y_bps': _col_cfg(code='0.00'),
            'discount_factor': _col_cfg(code='0.000000', data_bar=True),
        }})
        profile_grid.chart(
            'line', x='tenor_label',
            y=[('zero_rate_bps', 'sum')],
            split='curve_id',
            title='Piecewise-Linear Interpolated Zero Curves (bps)',
        )

        # -- Sheet 2: Input Products & Output Prices (reading Flow Stage 10) ------------------------------------------
        s_prices = page.sheet('2. Products & Output Prices')
        book_grid = s_prices.grid(
            input=b_portfolio,
            title='Output Prices by Book, Swap Type & Direction (SQL vs Python & 1bp Parallel Bump)',
            rows=['book', 'swap_type', 'direction'],
            measures={
                'notional': 'sum',
                'sql_npv': 'sum',
                'python_npv': 'sum',
                'sql_py_diff': 'sum',
                'bump_1bp_change': 'sum',
                'analytic_dv01_sum': 'sum',
                'convexity_diff': 'sum',
            },
            sort=[('book', 'asc')],
        )
        book_grid.expand_all()
        book_grid.configure({'columns': {
            'notional': _col_cfg(code='#,##0'),
            'sql_npv': _col_cfg(code='#,##0.00', data_bar=True),
            'python_npv': _col_cfg(code='#,##0.00'),
            'bump_1bp_change': _col_cfg(code='#,##0.00'),
            'analytic_dv01_sum': _col_cfg(code='#,##0.00', data_bar=True),
            'convexity_diff': _col_cfg(code='#,##0.00'),
        }})
        book_grid.chart(
            'bar', x='book',
            y=[('sql_npv', 'sum')],
            split='swap_type',
            title='Output Price (SQL NPV) by Book & Swap Type ($)',
        )

        trades_grid = s_prices.grid(
            input=b_portfolio,
            title='Input Swap Products (1-Curve Approx & 2-Curve Explicit Float) & Trade-Level Valuation',
            sort=[('notional', 'desc')],
        )
        trades_grid.configure({'columns': {
            'notional': _col_cfg(code='#,##0'),
            'fixed_rate_bps': _col_cfg(code='0.00'),
            'float_spread_bps': _col_cfg(code='0.00'),
            'sql_npv': _col_cfg(code='#,##0.00', data_bar=True),
            'python_npv': _col_cfg(code='#,##0.00'),
            'bump_1bp_change': _col_cfg(code='#,##0.00'),
            'analytic_dv01_sum': _col_cfg(code='#,##0.00'),
            'convexity_diff': _col_cfg(code='#,##0.00'),
        }})

        # -- Sheet 3: Output Risk Ladders (reading Flow Stages 12 & 11) -----------------------------------------------
        s_risk = page.sheet('3. Output Risk Ladders')
        q_grid = s_risk.grid(
            input=b_quote_risk,
            title='Output Par-Quote Risk Ladder across Discount & Projection Quotes (SQL Chain Rule vs Bump & Refit, $/bp)',
            rows=['book'],
            columns=['quote_id'],
            measures={'sql_quote_dv01': 'sum'},
            sort=[('book', 'asc')],
        )
        q_grid.configure({'columns': {'sql_quote_dv01': _col_cfg(code='#,##0.0', heatmap=True)}})
        q_grid.chart(
            'bar', x='quote_id',
            y=[('sql_quote_dv01', 'sum')],
            split='book',
            title='Output Par-Quote DV01 by Quote & Book ($/bp)',
        )

        p_grid = s_risk.grid(
            input=b_pillar_risk,
            title='Output Zero-Rate Pillar Deltas across Discount & Projection Curves ($/bp)',
            rows=['book'],
            columns=['pillar'],
            measures={'sql_delta': 'sum'},
            sort=[('book', 'asc')],
        )
        p_grid.configure({'columns': {'sql_delta': _col_cfg(code='#,##0.0', heatmap=True)}})
        p_grid.chart(
            'bar', x='pillar',
            y=[('sql_delta', 'sum')],
            split='book',
            title='Output Zero-Rate Pillar Delta by Curve Pillar & Book ($/bp)',
        )

        # -- Sheet 4: Diagnostic Curve Fitter Jacobian (reading Flow Stage 7) -----------------------------------------
        s_jac = page.sheet('4. Diagnostic Curve Jacobian')
        jac_grid = s_jac.grid(
            input=b_jacobian,
            title='Diagnostic Fitter Inverse Jacobian (d Pillar Zero Rate / d Market Par Quote)',
            rows=['pillar'],
            columns=['quote_id'],
            measures={'d_pillar_d_quote': 'sum'},
            sort=[('pillar', 'asc')],
        )
        jac_grid.configure({'columns': {'d_pillar_d_quote': _col_cfg(code='0.0000', heatmap=True)}})
        jac_grid.chart(
            'bar', x='pillar',
            y=[('d_pillar_d_quote', 'sum')],
            split='quote_id',
            title='Fitter Jacobian Coupling (d Pillar / d Quote)',
        )

    return page
