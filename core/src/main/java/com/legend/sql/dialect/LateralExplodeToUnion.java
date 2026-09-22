package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;
import java.util.ArrayList;
import java.util.List;

/**
 * H2-family structural pass: a per-row LITERAL collection exploded through
 * a correlated lateral join — {@code LEFT JOIN LATERAL (SELECT UNNEST(
 * list_concat([a], [b])) AS elem FROM (...)) AS t1 ON TRUE} — has no
 * spelling on H2 2.1 (no LATERAL; probed 2026-09-03: {@code from t,
 * unnest(array[t.id])} cannot see {@code t}). The engine's own shape is
 * the DECORRELATED union keyed by the base row identity, joined once:
 * {@code LEFT OUTER JOIN (SELECT a AS elem, base._ROWID_ AS __rid FROM
 * <base> UNION ALL SELECT b, base._ROWID_ FROM <base>) AS t1 ON t1.__rid
 * = base._ROWID_}. Null-dropping singleton wrappers ({@code CASE WHEN e IS
 * NULL THEN [] ELSE [e] END}) become the branch's {@code WHERE e IS NOT
 * NULL}. A non-literal list (a real per-row array) is left alone and
 * walls at the renderer as before.
 */
final class LateralExplodeToUnion extends SqlRewriter {
    @Override
    protected SqlQuery select(SqlSelect s) {
        if (!(s.from() instanceof SqlSource.Join j)
                || (j.kind() != SqlSource.Join.Kind.LEFT_LATERAL
                        && j.kind() != SqlSource.Join.Kind.CROSS_LATERAL)
                || !(j.right() instanceof SqlSource.Subselect sub)
                || !(sub.inner() instanceof SqlSelect u)
                || u.projections().size() != 1
                || u.where() != null || !u.groupBy().isEmpty()) {
            return s;
        }
        SqlSelect.Projection p = u.projections().get(0);
        SqlExpr un = unwrap(p.expr());
        if (!(un instanceof SqlExpr.Call c) || c.fn() != SqlFn.UNNEST
                || c.args().size() != 1) {
            return s;
        }
        SqlExpr list = unwrap(c.args().get(0));
        // the list may be a column of the lateral's own inner select
        if (list instanceof SqlExpr.Column lc
                && u.from() instanceof SqlSource.Subselect inner
                && inner.inner() instanceof SqlSelect l
                && l.projections().size() == 1
                && lc.name().equals(l.projections().get(0).alias())) {
            list = unwrap(l.projections().get(0).expr());
        }
        List<Element> elements = new ArrayList<>();
        if (!elementsOf(list, elements) || elements.isEmpty()) {
            return s;
        }
        String base = baseAlias(elements);
        if (base == null) {
            return s;
        }
        String elemAlias = p.alias() == null ? "elem" : p.alias();
        OutputCol elemOut = p.out() != null ? p.out()
                : new OutputCol(elemAlias, SqlType.Scalar.LITERAL, true);
        OutputCol ridOut = new OutputCol("__rid", SqlType.Scalar.BIGINT, false);
        List<SqlQuery> branches = new ArrayList<>();
        for (Element e : elements) {
            branches.add(new SqlSelect(
                    List.of(new SqlSelect.Projection(e.value(), elemAlias, elemOut),
                            new SqlSelect.Projection(
                                    new SqlExpr.RowOrder(base),
                                    "__rid", ridOut)),
                    false, j.left(),
                    e.notNull() ? SqlExpr.Call.of(SqlFn.IS_NOT_NULL, e.value())
                            : null,
                    List.of(), null, null, List.of(), null, null,
                    List.of(elemOut, ridOut)));
        }
        SqlQuery body = branches.size() == 1 ? branches.get(0)
                : new SqlUnion(branches, true, List.of(elemOut, ridOut));
        SqlSource.Join decorrelated = new SqlSource.Join(j.left(),
                new SqlSource.Subselect(body, sub.alias(), sub.frameName()),
                j.kind() == SqlSource.Join.Kind.CROSS_LATERAL
                        ? SqlSource.Join.Kind.INNER : SqlSource.Join.Kind.LEFT,
                SqlExpr.Call.of(SqlFn.EQUAL,
                        SqlExpr.Column.derived(sub.alias(), "__rid"),
                        new SqlExpr.RowOrder(base)));
        return s.withFrom(decorrelated);
    }

    private record Element(SqlExpr value, boolean notNull) {
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

    /** The literal elements of a list expression, or false when the
     * list is not literal (a stored array). */
    private static boolean elementsOf(SqlExpr list, List<Element> out) {
        SqlExpr e = unwrap(list);
        if (e instanceof SqlExpr.ArrayLit al) {
            for (SqlExpr x : al.elements()) {
                out.add(new Element(x, false));
            }
            return true;
        }
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.LIST_CONCAT) {
            for (SqlExpr a : c.args()) {
                if (!elementsOf(a, out)) {
                    return false;
                }
            }
            return true;
        }
        // CASE WHEN x IS NULL THEN [] ELSE [x] END — the null-dropping
        // singleton
        if (e instanceof SqlExpr.Case cs && cs.whens().size() == 1
                && cs.whens().get(0).condition() instanceof SqlExpr.Call cond
                && cond.fn() == SqlFn.IS_NULL && cond.args().size() == 1
                && unwrap(cs.whens().get(0).then()) instanceof SqlExpr.ArrayLit empty
                && empty.elements().isEmpty()
                && cs.otherwise() != null
                && unwrap(cs.otherwise()) instanceof SqlExpr.ArrayLit one
                && one.elements().size() == 1) {
            out.add(new Element(one.elements().get(0), true));
            return true;
        }
        return false;
    }

    /** The base alias the elements correlate to (the first table-
     * qualified column read); null = no correlation to key on. */
    private static @com.legend.base.Nullable String baseAlias(List<Element> elements) {
        for (Element e : elements) {
            String a = firstTable(e.value());
            if (a != null) {
                return a;
            }
        }
        return null;
    }

    private static @com.legend.base.Nullable String firstTable(SqlExpr e) {
        if (e instanceof SqlExpr.Column c && c.table() != null) {
            return c.table();
        }
        if (e instanceof SqlExpr.Call c) {
            for (SqlExpr a : c.args()) {
                String t = firstTable(a);
                if (t != null) {
                    return t;
                }
            }
        }
        if (e instanceof SqlExpr.Cast c) {
            return firstTable(c.value());
        }
        if (e instanceof SqlExpr.Case cs) {
            for (var w : cs.whens()) {
                String t = firstTable(w.condition());
                if (t == null) {
                    t = firstTable(w.then());
                }
                if (t != null) {
                    return t;
                }
            }
            return cs.otherwise() == null ? null : firstTable(cs.otherwise());
        }
        return null;
    }
}
