// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;

/**
 * A GRAPH ENVELOPE'S EXPLICIT ORDER carried into its array aggregate. A sorted graph fetch lowers to
 * {@code json_group_array(obj) FROM (SELECT ... ORDER BY k) t}; SQL keeps no subquery's order through an aggregate, and
 * a plan that decorrelates a nested subquery (a hash join) scrambles it: the GraphQL hardening fuzzer met
 * {@code Customer(orderBy: [{ id: ASC }]) { referredAggregate { ... } }} answering ids 1, 2, 3, 5, 4 on DuckDB
 * (2026-10-20). The ordered subselect gains {@code ROW_NUMBER() OVER (ORDER BY k)} -- the same keys, directions and
 * null placements, in the same scope -- and the aggregate orders by it, as the relation wire hoists its order
 * ({@code Render.hoistOrder}). SQL-level only: the typed schema never sees the column.
 */
final class GraphSortOrdinal {

    static final String COLUMN = "g_sort_ord__";

    private GraphSortOrdinal() {
    }

    /** {@code envelope} over its ordered subselect, numbered; null when the envelope reads no ordered subselect (the
     *  order then is the aggregate's own keys', or none) or the subselect's rows would change by a new column
     *  ({@code DISTINCT}). */
    static @com.legend.base.Nullable SqlSelect inject(SqlSelect envelope) {
        SqlSource numbered = numbered(envelope.from());
        return numbered == null ? null : envelope.withFrom(numbered);
    }

    /** {@code from} with its ordered subselect numbered: the source itself, or a join's LEFT side when the join keeps
     *  every left row unpadded -- a decorrelated per-object subquery ({@code $this.lines->count()} as a LEFT JOIN to
     *  a keyed group) joins beside the ordered roots, and the roots' order is still theirs (the fuzzer's
     *  {@code Order(orderBy: [{ id: ASC }]) { lineCount }}, 2026-10-20). */
    private static @com.legend.base.Nullable SqlSource numbered(SqlSource from) {
        if (from instanceof SqlSource.Join j && !j.kind().padsLeft()) {
            SqlSource left = numbered(j.left());
            return left == null ? null : new SqlSource.Join(left, j.right(), j.kind(), j.on());
        }
        if (!(from instanceof SqlSource.Subselect ss) || !(ss.inner() instanceof SqlSelect inner)
                || inner.orderBy().isEmpty() || inner.distinct()) {
            return null;
        }
        List<SqlSelect.SortKey> keys = new ArrayList<>();
        for (SqlSelect.SortKey k : inner.orderBy()) {
            SqlExpr e = k.expr();
            if (k.outputName() != null) {
                // a key named by its projection: the window reads the projection's expression (a window's ORDER BY
                // sees the select's sources, not its aliases)
                for (SqlSelect.Projection p : inner.projections()) {
                    if (k.outputName().equals(p.outputName()) && !(p.expr() instanceof SqlExpr.Star)) {
                        e = p.expr();
                    }
                }
            }
            keys.add(new SqlSelect.SortKey(e, k.ascending(), k.nullOrder(), null));
        }
        SqlExpr rn = new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()), List.of(), keys,
                null);
        OutputCol out = new OutputCol(COLUMN, SqlType.Scalar.BIGINT, false);
        List<SqlSelect.Projection> ps = new ArrayList<>();
        List<OutputCol> outs = new ArrayList<>();
        if (inner.projections().isEmpty()) {
            // a star frame: every source column, then the ordinal
            ps.add(new SqlSelect.Projection(new SqlExpr.Star(null), null, null));
            outs.addAll(inner.outputs());
        } else {
            ps.addAll(inner.projections());
            outs.addAll(inner.outputs());
        }
        ps.add(new SqlSelect.Projection(rn, COLUMN, out));
        outs.add(out);
        SqlSelect numbered = new SqlSelect(ps, false, inner.from(), inner.where(), inner.groupBy(), inner.having(),
                inner.qualify(), inner.orderBy(), inner.limit(), inner.offset(), outs);
        return new SqlSource.Subselect(numbered, ss.alias(), ss.frameName());
    }
}
