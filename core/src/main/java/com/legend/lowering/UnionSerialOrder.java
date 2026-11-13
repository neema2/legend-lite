// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlUnion;

import java.util.ArrayList;
import java.util.List;

/**
 * UNION-MEMBER serial order for the graph envelope (engine contract: union
 * members serialize in BRANCH DECLARATION order — the engine's stitch is
 * serial per member; {@code json_group_array} inherits scan order). A
 * {@code UNION ALL} under pass-through selects gains a per-branch ordinal
 * projection, NEGATED because the ordered-agg renders {@code DESC} (the
 * witness TRUE-first contract). SQL-level only — the typed schema never
 * sees the column.
 */
final class UnionSerialOrder {

    static final String COLUMN = "u_serial_ord__";

    private UnionSerialOrder() {
    }

    /** {@code base} with a per-branch NEGATED ordinal column added to the
     * first {@code UNION ALL} found under pass-through (star) selects;
     * null when no such union surfaces (joins, explicit projections,
     * distinct/grouped pass-throughs — injection bails silently and the
     * array keeps scan order). */
    static @com.legend.base.Nullable SqlSelect inject(SqlSelect base) {
        SqlSource f2 = intoSource(base.from());
        return f2 == null ? null : base.withFrom(f2);
    }

    /** Whether a {@code UNION ALL} surfaces in {@code q} under pass-through (star) selects, as {@link #inject} finds
     *  one: its members' order is the order a graph envelope over it serializes them in. A relation read more than
     *  once is written at each read where this holds (Lowerer.sharedRead, 2026-11-06): a common table expression's
     *  rows carry no order, and an envelope over a read of one finds no union to order by (CI on #81:
     *  testUnionWithQualifiedProperty's employees in another order on each runner). */
    static boolean surfaces(SqlQuery q) {
        if (q instanceof SqlUnion u) {
            return u.all();
        }
        return q instanceof SqlSelect s && s.projections().isEmpty() && s.groupBy().isEmpty() && !s.distinct()
                && s.from() instanceof SqlSource.Subselect ss && surfaces(ss.inner());
    }

    private static @com.legend.base.Nullable SqlSource intoSource(SqlSource src) {
        if (!(src instanceof SqlSource.Subselect ss)) {
            return null;    // tables/joins: no serial member spine to order
        }
        SqlQuery rewritten = intoQuery(ss.inner());
        return rewritten == null ? null
                : new SqlSource.Subselect(rewritten, ss.alias(), ss.frameName());
    }

    private static @com.legend.base.Nullable SqlQuery intoQuery(SqlQuery q) {
        if (q instanceof SqlUnion u && u.all()) {
            List<SqlQuery> branches = new ArrayList<>(u.branches().size());
            for (int i = 0; i < u.branches().size(); i++) {
                if (!(u.branches().get(i) instanceof SqlSelect bs)) {
                    return null;    // nested unions: bail
                }
                OutputCol ord = new OutputCol(COLUMN,
                        PureSql.type(Type.Primitive.INTEGER), false);
                SqlSelect.Projection p = new SqlSelect.Projection(
                        new SqlExpr.IntLit(-i), COLUMN, ord);
                if (bs.projections().isEmpty()) {
                    // a star branch (a shared member read by name, 2026-11-06: SELECT * FROM __let<n>) keeps its row
                    // explicitly beside its ordinal, SELECT *, -i, as the scan-order key writes a leg's
                    List<OutputCol> outs = new ArrayList<>(bs.outputs());
                    outs.add(ord);
                    branches.add(new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Star(null), null, null), p),
                            bs.distinct(), bs.from(), bs.where(), bs.groupBy(), bs.having(), bs.qualify(),
                            bs.orderBy(), bs.limit(), bs.offset(), List.copyOf(outs)));
                    continue;
                }
                List<SqlSelect.Projection> ps = new ArrayList<>(bs.projections());
                ps.add(p);
                branches.add(bs.withProjections(ps));
            }
            List<OutputCol> uOuts = new ArrayList<>(u.outputs());
            uOuts.add(new OutputCol(COLUMN,
                    PureSql.type(Type.Primitive.INTEGER), false));
            return new SqlUnion(branches, true, uOuts);
        }
        if (q instanceof SqlSelect s && s.projections().isEmpty()
                && s.groupBy().isEmpty() && !s.distinct()) {
            // star pass-through: the ordinal flows up unchanged (adding a
            // source column under DISTINCT or GROUP BY would change their
            // semantics — those bail above)
            SqlSource f2 = intoSource(s.from());
            return f2 == null ? null : s.withFrom(f2);
        }
        return null;
    }
}
