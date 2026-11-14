// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.ArrayList;
import java.util.List;

/**
 * The check a window with a computed frame offset is written inside ({@code CASE WHEN <bad> THEN <raise> ELSE <window>
 * END}): its offsets are the database's to work out, so the database checks them too, as upstream checks the numbers
 * it computed (legend-engine {@code pureToSQLQuery.pure}'s {@code processWindowFrame}). Without it two of the frames an
 * expression can make answer quietly (measured 2026-10-31, docs/TS_LIBRARY_DEFINES_THE_LANGUAGE_2026_10_31.md §3a): a
 * start after the end is an empty window on every database but ClickHouse, and a negative offset an empty window on
 * DuckDB. The database computes every value; this only writes the shape.
 */
final class FrameGuard {

    private FrameGuard() {
    }

    /** {@code w} inside the check of its computed offsets, or {@code w} itself where its frame has none (or is
     *  already inside its check). */
    static SqlExpr guarded(SqlExpr.WindowCall w) {
        SqlExpr.WindowCall.Frame f = w.frame();
        if (f == null || f.checked() || !f.computed()) {
            return w;
        }
        List<SqlExpr.Case.When> whens = new ArrayList<>(2);
        SqlExpr lo = signed(f.from());
        SqlExpr hi = signed(f.to());
        if (lo != null && hi != null) {
            whens.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.GREATER, lo, hi),
                    raise(com.legend.sql.RaiseKind.WINDOW_FRAME_INVERTED)));
        }
        SqlExpr bad = null;
        for (SqlExpr o : f.offsets()) {
            SqlExpr b = SqlExpr.Call.of(SqlFn.OR, SqlExpr.Call.of(SqlFn.IS_NULL, o),
                    SqlExpr.Call.of(SqlFn.LESS, o, new SqlExpr.IntLit(0)));
            bad = bad == null ? b : SqlExpr.Call.of(SqlFn.OR, bad, b);
        }
        if (bad != null) {
            whens.add(new SqlExpr.Case.When(bad, raise(com.legend.sql.RaiseKind.WINDOW_FRAME_OFFSET_INVALID)));
        }
        return new SqlExpr.Case(whens, new SqlExpr.WindowCall(w.fn(), w.partitionBy(), w.orderBy(), f.asChecked(),
                w.type()));
    }

    /** A bound's signed place (before the current row negative): null for an unbounded side, which every other bound
     *  follows or precedes. */
    private static @com.legend.base.Nullable SqlExpr signed(SqlExpr.WindowCall.Frame.Bound b) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.ComputedPreceding c -> SqlExpr.Call.of(SqlFn.NEGATE, c.offset());
            case SqlExpr.WindowCall.Frame.Bound.ComputedFollowing c -> c.offset();
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> literal(p.n(), true);
            case SqlExpr.WindowCall.Frame.Bound.Following p -> literal(p.n(), false);
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow c -> new SqlExpr.IntLit(0);
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding u -> null;
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing u -> null;
            // an interval frame keeps literal bounds (Frames.intervalBound): never beside a computed one
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding p -> throw new IllegalStateException(
                    "an interval frame bound beside a computed one");
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing p -> throw new IllegalStateException(
                    "an interval frame bound beside a computed one");
        };
    }

    private static SqlExpr literal(Number n, boolean negative) {
        return switch (n) {
            case Long l -> new SqlExpr.IntLit(negative ? -l : l);
            case Integer i -> new SqlExpr.IntLit(negative ? -(long) i : i);
            case java.math.BigDecimal d -> new SqlExpr.DecimalLit(negative ? d.negate() : d);
            default -> new SqlExpr.FloatLit(negative ? -n.doubleValue() : n.doubleValue());
        };
    }

    /** The raise of {@code kind} (phase D's D1.1, merged with #81 2026-11-14): the product writes the kind, the edge
     *  its sentence -- legend-pure's for a start after the end, lite's own for a bad offset (S170). */
    private static SqlExpr raise(com.legend.sql.RaiseKind kind) {
        return SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.RaiseText(kind, List.of()));
    }
}
