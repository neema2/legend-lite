// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;
import com.legend.sql.TypeFact;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * H2-family structural pass (2026-10-20, the parity phase): a lateral join exploding a CONSTANT list -- a literal
 * list, or none -- is no lateral at all: the list's rows, one select per element under the join's alias, joined
 * ON TRUE (a LEFT join keeps its row where the list is empty, as the lateral did). H2 2.1 has no LATERAL (probed:
 * {@code Function "LATERAL" not found}) and no struct value its rows could carry, so an element that is a struct
 * literal is its fields, each a column ({@code elem__f}), and each read of a field ({@code struct_extract(t.elem,
 * 'f')}) in the select over the join reads that column. The shape the lowering writes for a literal instance's
 * many-valued property under {@code project} (testSimpleProject, testSimpleProjectList, testSimpleProjectWithEmpty,
 * testFilterPostProject: the engine passes them on H2). A list that reads a row is left to {@link
 * LateralExplodeToUnion}; a struct element read whole, or elements of differing fields, are left as they are and
 * refused where they are written.
 */
final class ConstantLateralsAsRows extends SqlRewriter {

    @Override
    protected SqlQuery select(SqlSelect s) {
        if (!(s.from() instanceof SqlSource.Join)) {
            return s;
        }
        Map<String, Explosion> explosions = new LinkedHashMap<>();
        SqlSource from = rewriteJoins(s.from(), explosions);
        if (explosions.isEmpty()) {
            return s;
        }
        // every read of an exploded element must be a field read
        List<SqlExpr> roots = new ArrayList<>();
        s.projections().forEach(p -> roots.add(p.expr()));
        if (s.where() != null) {
            roots.add(s.where());
        }
        roots.addAll(s.groupBy());
        if (s.having() != null) {
            roots.add(s.having());
        }
        s.orderBy().forEach(k -> roots.add(k.expr()));
        for (SqlExpr r : roots) {
            if (!readsFieldsOnly(r, explosions)) {
                return s;
            }
        }
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (SqlSelect.Projection p : s.projections()) {
            SqlExpr x = fieldsAsColumns(p.expr(), explosions);
            ps.add(x == p.expr() ? p : new SqlSelect.Projection(x, p.alias(), p.out()));
        }
        List<SqlSelect.SortKey> ob = new ArrayList<>();
        for (SqlSelect.SortKey k : s.orderBy()) {
            SqlExpr x = fieldsAsColumns(k.expr(), explosions);
            ob.add(x == k.expr() ? k : new SqlSelect.SortKey(x, k.ascending(), k.nullOrder(), k.outputName()));
        }
        List<SqlExpr> g = s.groupBy().stream().map(x -> fieldsAsColumns(x, explosions)).toList();
        return new SqlSelect(ps, s.distinct(), from,
                s.where() == null ? null : fieldsAsColumns(s.where(), explosions), g,
                s.having() == null ? null : fieldsAsColumns(s.having(), explosions), s.qualify(), ob, s.limit(),
                s.offset(), s.outputs());
    }

    /** One exploded constant list: its alias, its element's name, and the element's struct fields (empty for a
     *  scalar element, read as the element's own column). */
    private record Explosion(String alias, String element, List<String> fields) {
    }

    /** {@code src} with each lateral over a constant list as its rows, recorded in {@code out}; any other source as
     *  it is. */
    private static SqlSource rewriteJoins(SqlSource src, Map<String, Explosion> out) {
        if (!(src instanceof SqlSource.Join j)) {
            return src;
        }
        SqlSource left = rewriteJoins(j.left(), out);
        if ((j.kind() == SqlSource.Join.Kind.LEFT_LATERAL || j.kind() == SqlSource.Join.Kind.CROSS_LATERAL)
                && j.right() instanceof SqlSource.Subselect sub) {
            SqlSource rows = rowsOf(sub, out);
            if (rows != null) {
                return new SqlSource.Join(left, rows, j.kind() == SqlSource.Join.Kind.LEFT_LATERAL
                        ? SqlSource.Join.Kind.LEFT : SqlSource.Join.Kind.INNER, new SqlExpr.BoolLit(true));
            }
        }
        return left == j.left() ? j : new SqlSource.Join(left, j.right(), j.kind(), j.on());
    }

    /** The lateral {@code sub}'s rows when it explodes a constant list -- {@code SELECT UNNEST(lst) AS elem FROM
     *  (SELECT <list> AS lst)} or {@code SELECT UNNEST(<list>) AS elem} -- else null. */
    private static @com.legend.base.Nullable SqlSource rowsOf(SqlSource.Subselect sub, Map<String, Explosion> out) {
        // the list the carrier strategies already wrote as its rows: a union of FROM-less selects of one element each
        List<SqlSelect> rowSelects = constantRows(sub.inner());
        if (rowSelects != null) {
            SqlSelect.Projection p0 = rowSelects.get(0).projections().get(0);
            List<SqlExpr> elements = rowSelects.stream().map(r -> r.projections().get(0).expr()).toList();
            return rows(sub, p0, elements, out);
        }
        if (!(sub.inner() instanceof SqlSelect u) || u.projections().size() != 1 || u.where() != null
                || !u.groupBy().isEmpty() || u.having() != null || !u.orderBy().isEmpty()) {
            return null;
        }
        SqlSelect.Projection p = u.projections().get(0);
        if (!(unwrap(p.expr()) instanceof SqlExpr.Call c) || c.fn() != com.legend.sql.SqlFn.UNNEST
                || c.args().size() != 1) {
            return null;
        }
        SqlExpr list = unwrap(c.args().get(0));
        if (list instanceof SqlExpr.Column lc && u.from() instanceof SqlSource.Subselect inner
                && inner.inner() instanceof SqlSelect l && l.projections().size() == 1
                && lc.name().equals(l.projections().get(0).alias()) && l.from() instanceof SqlSource.Dual) {
            list = unwrap(l.projections().get(0).expr());
        } else if (!(u.from() instanceof SqlSource.Dual)) {
            return null;
        }
        List<SqlExpr> elements;
        if (list instanceof SqlExpr.ArrayLit al) {
            elements = al.elements();
        } else if (list instanceof SqlExpr.NullLit) {
            elements = List.of();
        } else {
            return null;
        }
        return rows(sub, p, elements, out);
    }

    /** The FROM-less selects of one constant column each that {@code q} is (a union of them, or one), or null. */
    private static @com.legend.base.Nullable List<SqlSelect> constantRows(SqlQuery q) {
        List<SqlQuery> branches = q instanceof SqlUnion un && un.all() ? un.branches() : List.of(q);
        List<SqlSelect> out = new ArrayList<>();
        for (SqlQuery b : branches) {
            if (!(b instanceof SqlSelect r) || !(r.from() instanceof SqlSource.Dual) || r.projections().size() != 1
                    || r.where() != null || !r.groupBy().isEmpty() || !r.orderBy().isEmpty()
                    || r.projections().get(0).expr() instanceof SqlExpr.Call c && c.fn() == com.legend.sql.SqlFn.UNNEST
                    || readsColumn(r.projections().get(0).expr())) {
                return null;
            }
            out.add(r);
        }
        return out;
    }

    /** The lateral {@code sub}'s rows for its constant {@code elements}, the projection {@code p} naming the element,
     *  or null where an element has no form here. */
    private static @com.legend.base.Nullable SqlSource rows(SqlSource.Subselect sub, SqlSelect.Projection p,
            List<SqlExpr> elements, Map<String, Explosion> out) {
        String element = p.alias() == null ? "elem" : p.alias();
        for (SqlExpr e : elements) {
            if (readsColumn(e)) {
                return null;
            }
        }
        // the element's fields: each struct literal's, the same in each; a NULL list's from its declared slot
        List<SqlType.Struct.Field> fields = null;
        boolean structs = !elements.isEmpty() && elements.stream().allMatch(SqlExpr.StructLit.class::isInstance);
        if (structs) {
            SqlExpr.StructLit first = (SqlExpr.StructLit) elements.get(0);
            fields = new ArrayList<>();
            for (SqlExpr.StructLit.Field f : first.fields()) {
                SqlType t = f.value().type() instanceof TypeFact.Typed ty ? ty.type() : f.declared();
                if (t == null) {
                    return null;   // a field of no type: no column to carry it
                }
                fields.add(new SqlType.Struct.Field(f.name(), t));
            }
            for (SqlExpr e : elements) {
                List<String> names = ((SqlExpr.StructLit) e).fields().stream().map(SqlExpr.StructLit.Field::name)
                        .toList();
                if (!names.equals(fields.stream().map(SqlType.Struct.Field::name).toList())) {
                    return null;
                }
            }
        } else if (elements.isEmpty() && p.out() != null && p.out().type() instanceof SqlType.Struct st) {
            fields = st.fields();
        } else if (elements.stream().anyMatch(SqlExpr.StructLit.class::isInstance)) {
            return null;
        }
        List<SqlQuery> branches = new ArrayList<>();
        List<OutputCol> outs = new ArrayList<>();
        if (fields != null) {
            for (SqlType.Struct.Field f : fields) {
                outs.add(new OutputCol(element + "__" + f.name(), f.type(), true));
            }
        } else {
            SqlType t = p.out() != null ? p.out().type() : null;
            if (t == null) {
                return null;
            }
            outs.add(new OutputCol(element, t, true));
        }
        for (SqlExpr e : elements) {
            List<SqlSelect.Projection> ps = new ArrayList<>();
            if (fields != null) {
                List<SqlExpr.StructLit.Field> fs = ((SqlExpr.StructLit) e).fields();
                for (int i = 0; i < fs.size(); i++) {
                    ps.add(new SqlSelect.Projection(typed(fs.get(i).value(), outs.get(i).type()),
                            outs.get(i).name(), outs.get(i)));
                }
            } else {
                ps.add(new SqlSelect.Projection(typed(e, outs.get(0).type()), element, outs.get(0)));
            }
            branches.add(new SqlSelect(ps, false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null,
                    null, List.of()));
        }
        if (branches.isEmpty()) {
            // the empty list: one row of NULLs that no row passes
            List<SqlSelect.Projection> ps = new ArrayList<>();
            for (OutputCol oc : outs) {
                ps.add(new SqlSelect.Projection(new SqlExpr.Cast(new SqlExpr.NullLit(), oc.type()), oc.name(), oc));
            }
            branches.add(new SqlSelect(ps, false, new SqlSource.Dual(), new SqlExpr.BoolLit(false), List.of(), null,
                    null, List.of(), null, null, List.of()));
        }
        SqlQuery body = branches.size() == 1 ? branches.get(0) : new SqlUnion(branches, true, outs);
        out.put(sub.alias(), new Explosion(sub.alias(), element,
                fields == null ? List.of() : fields.stream().map(SqlType.Struct.Field::name).toList()));
        return new SqlSource.Subselect(body, sub.alias(), sub.frameName());
    }

    /** A NULL element typed by its column (a UNION's first branch types the column). */
    private static SqlExpr typed(SqlExpr v, SqlType t) {
        return v instanceof SqlExpr.NullLit ? new SqlExpr.Cast(v, t) : v;
    }

    private static boolean readsColumn(SqlExpr e) {
        if (e instanceof SqlExpr.Column || e instanceof SqlExpr.ScalarSubquery || e instanceof SqlExpr.Exists) {
            return true;
        }
        for (SqlExpr c : e.children()) {
            if (readsColumn(c)) {
                return true;
            }
        }
        return false;
    }

    private static SqlExpr unwrap(SqlExpr e) {
        while (true) {
            if (e instanceof SqlExpr.CompactList cl) {
                e = cl.list();
            } else if (e instanceof SqlExpr.Cast c) {
                e = c.value();
            } else {
                return e;
            }
        }
    }

    /** Whether every read of an exploded struct element under {@code e} reads one of its fields. */
    private static boolean readsFieldsOnly(SqlExpr e, Map<String, Explosion> ex) {
        if (e instanceof SqlExpr.StructGet g && element(g.source(), ex) instanceof Explosion x && !x.fields().isEmpty()) {
            return x.fields().contains(g.field());
        }
        if (element(e, ex) instanceof Explosion x && !x.fields().isEmpty()) {
            return false;
        }
        for (SqlExpr c : e.children()) {
            if (!readsFieldsOnly(c, ex)) {
                return false;
            }
        }
        return true;
    }

    private static @com.legend.base.Nullable Explosion element(SqlExpr e, Map<String, Explosion> ex) {
        if (e instanceof SqlExpr.Column c && c.table() != null) {
            Explosion x = ex.get(c.table());
            return x != null && x.element().equals(c.name()) ? x : null;
        }
        return null;
    }

    private static SqlExpr fieldsAsColumns(SqlExpr e, Map<String, Explosion> ex) {
        if (e instanceof SqlExpr.StructGet g && element(g.source(), ex) instanceof Explosion x
                && !x.fields().isEmpty()) {
            return SqlExpr.Column.derived(x.alias(), x.element() + "__" + g.field());
        }
        return e.children().isEmpty() ? e : e.mapChildren(c -> fieldsAsColumns(c, ex));
    }
}
