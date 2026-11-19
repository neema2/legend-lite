// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DialectCapability;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * EXPLOSION MARKERS (2026-10-11, docs/CLOUD_BACKENDS_PCT_2026_10_11.md): for a database with no set-returning
 * projection (Snowflake, BigQuery), every list explosion becomes a MARKER -- a FROM-less select whose projections are
 * each one list's {@code UNNEST} -- placed in a FROM, where the dialect writes it over the positions of its longest
 * list (Snowflake a {@code FLATTEN}, BigQuery an {@code UNNEST(GENERATE_ARRAY(..))}) and reads each exploded column as
 * its list's element at the position. These passes place the markers; the predicates name them.
 */
final class ExplosionMarkers {

    private ExplosionMarkers() {
    }

    /**
     * {@code (SELECT * FROM (marker) AS t0) AS t1} is the marker itself, as {@code t1}:
     * Snowflake refuses a correlated lateral subquery ("Unsupported subquery type cannot be evaluated", measured), and
     * a FLATTEN joins laterally only bare.
     */
    static final class StarOverExplosionUnwrapped extends com.legend.sql.SqlRewriter {
        @Override
        protected SqlSource source(SqlSource s) {
            if (s instanceof SqlSource.Subselect outer && outer.inner() instanceof SqlSelect star && plainStar(star)
                    && star.from() instanceof SqlSource.Subselect inner && isMarker(inner.inner())) {
                return new SqlSource.Subselect(inner.inner(), outer.alias(), outer.frameName());
            }
            return s;
        }

        private static boolean plainStar(SqlSelect s) {
            return s.projections().isEmpty() && !s.distinct() && s.where() == null && s.groupBy().isEmpty()
                    && s.having() == null && s.qualify() == null && s.orderBy().isEmpty() && s.limit() == null
                    && s.offset() == null;
        }
    }

    /**
     * The read of an exploded column, as its slot wants it. Its element's type when known; else the slot's when the
     * slot is scalar or the list is NULL (an exploded NULL list has no element type, and its untyped read is a
     * VARIANT column under the label -- harmless to cast, it has no row), never a composite slot over a real list
     * (a class value's slot holds the class's name, measured: "Failed to cast variant value"). A JSON element under
     * a VARCHAR slot is its JSON text: DuckDB's JSON travels as VARCHAR, the label the plan carries.
     */
    static SqlExpr explodedRead(String alias, SqlSelect.Projection p, @com.legend.base.Nullable SqlType slot) {
        String name = markerColumn(p);
        SqlType element = p.expr().type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null;
        SqlExpr list = ((SqlExpr.Call) p.expr()).args().get(0);
        SqlType type = element != null ? element
                : slot instanceof SqlType.Scalar || nullList(list) ? slot : null;
        SqlExpr read = type == null ? SqlExpr.Column.derived(alias, name)
                : SqlExpr.Column.of(alias, name, type, true, com.legend.sql.OutputCol.Origin.DERIVED);
        return type == SqlType.Scalar.JSON && slot == SqlType.Scalar.VARCHAR
                ? new SqlExpr.Cast(read, SqlType.Scalar.VARCHAR, SqlExpr.Reason.DIALECT) : read;
    }

    /** A list's element, min or max over the NULL list: NULL. */
    static boolean nullOverNullList(SqlExpr e) {
        if (e instanceof SqlExpr.Cast c && !(c.value() instanceof SqlExpr.NullLit)) {
            return nullOverNullList(c.value());
        }
        return e instanceof SqlExpr.Call c && (c.fn() == SqlFn.LIST_MIN || c.fn() == SqlFn.LIST_MAX
                || c.fn() == SqlFn.LIST_GET) && nullList(c.args().get(0))
                || e instanceof SqlExpr.ReduceCollection rc && (rc.reducer() == SqlAgg.Fn.MIN
                        || rc.reducer() == SqlAgg.Fn.MAX) && nullList(rc.collection());
    }

    static boolean nullList(SqlExpr e) {
        return e instanceof SqlExpr.NullLit || e instanceof SqlExpr.CompactList cl && nullList(cl.list());
    }

    /** A select that only explodes lists: every projection a one-list UNNEST, FROM nothing, no clause. */
    static boolean isMarker(SqlQuery q) {
        return q instanceof SqlSelect s && s.from() instanceof SqlSource.Dual && !s.projections().isEmpty()
                && s.projections().stream().allMatch(p -> isUnnest(p.expr()))
                && s.where() == null && s.groupBy().isEmpty() && s.having() == null && s.qualify() == null
                && s.orderBy().isEmpty() && s.limit() == null && s.offset() == null && !s.distinct();
    }

    static boolean isUnnest(SqlExpr e) {
        return e instanceof SqlExpr.Call c && c.fn() == SqlFn.UNNEST && c.args().size() == 1;
    }

    static String markerColumn(SqlSelect.Projection p) {
        String name = p.alias() != null ? p.alias() : p.out() != null ? p.out().name() : null;
        if (name == null) {
            throw new DialectCapability("an UNNEST without a column name reached the renderer");
        }
        return name;
    }

    /**
     * A LATERAL explosion of a carried list ({@code CROSS/LEFT JOIN LATERAL (SELECT UNNEST(c.lst)
     * AS elem FROM (SELECT <list> AS lst) AS c)}, Fold.lateralElem) as a MARKER -- {@code (SELECT UNNEST(<list>) AS
     * elem)} -- which the writer spells as a lateral FLATTEN: Snowflake refuses a correlated subquery over FLATTEN
     * ("Unsupported subquery type", measured), so the carry is inlined and nothing is nested.
     */
    static final class LateralUnnestAsMarker extends com.legend.sql.SqlRewriter {
        @Override
        protected SqlSource source(SqlSource s) {
            if (!(s instanceof SqlSource.Join j) || !(j.kind() == SqlSource.Join.Kind.CROSS_LATERAL
                    || j.kind() == SqlSource.Join.Kind.LEFT_LATERAL)
                    || !(j.right() instanceof SqlSource.Subselect sub) || !(sub.inner() instanceof SqlSelect inner)
                    || isMarker(inner)) {
                return s;
            }
            SqlSelect marker = inlined(inner);
            return marker == null ? s : new SqlSource.Join(j.left(), new SqlSource.Subselect(marker, sub.alias(),
                    sub.frameName()), j.kind(), j.on());
        }

        /** {@code inner} with its carry's lists inlined, or null when it is not an explosion of a carry. */
        private static @com.legend.base.Nullable SqlSelect inlined(SqlSelect inner) {
            if (!(inner.from() instanceof SqlSource.Subselect carry) || !(carry.inner() instanceof SqlSelect c)
                    || !(c.from() instanceof SqlSource.Dual) || c.where() != null || !c.groupBy().isEmpty()
                    || inner.where() != null || !inner.groupBy().isEmpty() || inner.having() != null
                    || inner.qualify() != null || !inner.orderBy().isEmpty() || inner.limit() != null
                    || inner.offset() != null || inner.distinct() || inner.projections().isEmpty()) {
                return null;
            }
            Map<String, SqlExpr> carried = new LinkedHashMap<>();
            for (SqlSelect.Projection p : c.projections()) {
                String name = p.outputName();
                if (name == null) {
                    return null;
                }
                carried.put(name, p.expr());
            }
            List<SqlSelect.Projection> ps = new ArrayList<>();
            for (SqlSelect.Projection p : inner.projections()) {
                if (!isUnnest(p.expr()) || !(((SqlExpr.Call) p.expr()).args().get(0) instanceof SqlExpr.Column col)
                        || !carry.alias().equals(col.table()) || !carried.containsKey(col.name())) {
                    return null;
                }
                ps.add(new SqlSelect.Projection(SqlExpr.Call.of(SqlFn.UNNEST, carried.get(col.name())), p.alias(),
                        p.out()));
            }
            return new SqlSelect(ps, false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null,
                    List.of());
        }
    }

    /**
     * A select-list UNNEST over a FROM (DuckDB explodes it in place: {@code SELECT k, UNNEST(xs)
     * AS x FROM t}) as a lateral MARKER joined to the FROM, the projection reading its element: Snowflake explodes
     * only in FROM. Several UNNESTs explode in parallel (one marker). A select that groups, or whose list holds an
     * aggregate, is left as it is: the writer refuses it by name. A bare marker is the writer's.
     */
    static final class SelectListUnnestAsLateral extends com.legend.sql.SqlRewriter {
        private int next;

        @Override
        protected SqlQuery select(SqlSelect s) {
            if (isMarker(s) || s.projections().stream().noneMatch(p -> isUnnest(p.expr()))
                    || !s.groupBy().isEmpty() || s.having() != null || s.qualify() != null) {
                return s;
            }
            for (SqlSelect.Projection p : s.projections()) {
                if (isUnnest(p.expr()) && aggregates(((SqlExpr.Call) p.expr()).args().get(0))) {
                    return s;
                }
            }
            String alias = "__unnest" + (next++);
            List<SqlSelect.Projection> unnests = new ArrayList<>();
            List<SqlSelect.Projection> ps = new ArrayList<>();
            for (SqlSelect.Projection p : s.projections()) {
                if (!isUnnest(p.expr())) {
                    ps.add(p);
                    continue;
                }
                String name = markerColumn(p);
                unnests.add(new SqlSelect.Projection(p.expr(), name, p.out()));
                ps.add(new SqlSelect.Projection(explodedRead(alias, p, p.out() != null ? p.out().type() : null),
                        p.alias(), p.out()));
            }
            SqlSource marker = new SqlSource.Subselect(new SqlSelect(unnests, false, new SqlSource.Dual(), null,
                    List.of(), null, null, List.of(), null, null, List.of()), alias, null);
            SqlSource from = s.from() instanceof SqlSource.Dual ? marker
                    : new SqlSource.Join(s.from(), marker, SqlSource.Join.Kind.CROSS_LATERAL, null);
            return new SqlSelect(ps, s.distinct(), from, s.where(), s.groupBy(), s.having(), s.qualify(), s.orderBy(),
                    s.limit(), s.offset(), s.outputs());
        }

        private static boolean aggregates(SqlExpr e) {
            if (e instanceof SqlAgg || e instanceof SqlExpr.WindowCall) {
                return true;
            }
            for (SqlExpr k : e.children()) {
                if (aggregates(k)) {
                    return true;
                }
            }
            return false;
        }
    }
}
