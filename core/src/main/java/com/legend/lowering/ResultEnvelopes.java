// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;

import java.util.List;

/**
 * legend-engine's RESULT ENVELOPE, A LOWERING FACET OF THE PLAN STYLE (Phase 6's B5, 2026-11-17, the orchestrator's
 * ruling (A) on tdsBuilder): an {@code executeLegendQuery}'s value for a relation, class or serialized root is
 * legend-engine's JSON result ({@code builder}, {@code activities}, {@code result} or {@code objects} or
 * {@code values}), so its keys, tags and skeleton are the compatibility edge's; the product writes a query's rows by
 * its result writers and refuses each by name AT LOWERING ({@code JsonEmission}). The Lowerer asks the planner's style
 * for this facet only when it meets one, carrying the call's own refusal. A hook of signatures only: its parts are the
 * product's (names, types' simple names, cells, the activity's SQL), its envelope the edge's.
 */
@com.legend.base.OpenLine("B5") public interface ResultEnvelopes {

    /** A relation root's envelope: its columns' {@code names} and their types' simple {@code typeNames}, each row's
     *  {@code cells} (in column order, read over the rows' alias), the activity's SQL or null. */
    SqlExpr tds(List<String> names, List<String> typeNames, List<SqlExpr> cells,
            @com.legend.base.Nullable String activitySql);

    /** A class root's envelope: its {@code objects} (the graph emission's JSON array), the activity's SQL or null. */
    SqlExpr classes(SqlExpr objects, @com.legend.base.Nullable String activitySql);

    /** A serialized root's envelope around its JSON text {@code value}. */
    SqlExpr values(SqlExpr value);
}
