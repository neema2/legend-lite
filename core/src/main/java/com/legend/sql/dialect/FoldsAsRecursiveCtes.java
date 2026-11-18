// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlUnion;
import com.legend.sql.SqlWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static com.legend.sql.dialect.ExplosionMarkers.nullList;

/**
 * A fold that reads nothing of the row it is in becomes a recursive common table expression at the top of the
 * statement, for a database with no lambdas whose {@code WITH RECURSIVE} is top-level only (BigQuery, measured: a
 * recursive CTE inside a subquery is refused). Pure's fold {@code (x, acc)} over {@code xs} from {@code init}:
 * <pre>
 *   __foldN AS (SELECT 0 AS __i, init AS __acc
 *               UNION ALL
 *               SELECT __foldN.__i + 1, body[x := xs[__i + 1], acc := __foldN.__acc]
 *               FROM __foldN WHERE __foldN.__i &lt; length(xs))
 * </pre>
 * read in its place as the last row's accumulator, {@code (SELECT __acc FROM __foldN ORDER BY __i DESC LIMIT 1)}. A
 * fold over the NULL list is its init (no element to apply the body to). DuckDB's {@code list_reduce(xs, (acc, x))},
 * a fold whose first element is its init, likewise from 1 (an empty list reduces to NULL, as on SQLite; DuckDB
 * raises). A fold inside another is hoisted first, and the outer one reads it as a table.
 *
 * <p>Closed means: every column it names is one of its own lambdas' parameters (or a field of one), no aggregate,
 * window or row-dependent read, and no subquery but the reads this pass made. A fold that reads its row stays where it
 * is, and the dialect walls it.
 */
final class FoldsAsRecursiveCtes extends com.legend.sql.SqlRewriter {

    private static final String I = "__i";
    private static final String ACC = "__acc";

    private final List<SqlWith.Cte> ctes = new ArrayList<>();
    private final Set<SqlExpr> reads = Collections.newSetFromMap(new IdentityHashMap<>());

    @Override
    public SqlQuery rewriteRoot(SqlQuery q) {
        ctes.clear();
        reads.clear();
        SqlQuery out = rewrite(q);
        return ctes.isEmpty() ? out : SqlWith.prepend(List.copyOf(ctes), out, true);
    }

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (e instanceof SqlExpr.FoldCall f && f.lambda().params().size() == 2 && closed(f, Set.of())) {
            if (nullList(f.source())) {
                return f.init();
            }
            SqlExpr.Lambda l = f.lambda();
            return hoist(f.source(), new SqlExpr.IntLit(0), f.init(), l.body(), l.params().get(0), l.params().get(1));
        }
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.LIST_REDUCE && c.args().size() == 2
                && c.args().get(1) instanceof SqlExpr.Lambda l && l.params().size() == 2 && closed(c, Set.of())) {
            SqlExpr xs = c.args().get(0);
            return hoist(xs, new SqlExpr.IntLit(1), SqlExpr.Call.of(SqlFn.LIST_GET, xs, new SqlExpr.IntLit(1)),
                    l.body(), l.params().get(1), l.params().get(0));
        }
        return e;
    }

    /** The recursive expression stepping {@code body} over {@code xs} from offset {@code from} with {@code init}, and
     *  the read of its last accumulator. */
    private SqlExpr hoist(SqlExpr xs, SqlExpr from, SqlExpr init, SqlExpr body, String x, String acc) {
        String name = "__fold" + ctes.size();
        SqlSelect anchor = select(List.of(new SqlSelect.Projection(from, I, null),
                new SqlSelect.Projection(init, ACC, null)), new SqlSource.Dual(), null, List.of(), null);
        List<OutputCol> outs = anchor.outputs();
        SqlSource self = new SqlSource.Cte(name, name, outs);
        SqlExpr i = SqlExpr.Column.of(name, outs, I);
        SqlExpr element = SqlExpr.Call.of(SqlFn.LIST_GET, xs, SqlExpr.Call.of(SqlFn.PLUS, i, new SqlExpr.IntLit(1)));
        SqlExpr step = substitute(substitute(body, x, element), acc, SqlExpr.Column.of(name, outs, ACC));
        SqlSelect next = select(List.of(
                new SqlSelect.Projection(SqlExpr.Call.of(SqlFn.PLUS, i, new SqlExpr.IntLit(1)), I, null),
                new SqlSelect.Projection(step, ACC, null)), self,
                SqlExpr.Call.of(SqlFn.LESS, i, SqlExpr.Call.of(SqlFn.LIST_LENGTH, xs)), List.of(), null);
        ctes.add(new SqlWith.Cte(name, new SqlUnion(List.of(anchor, next), true, outs)));
        SqlExpr last = new SqlExpr.ScalarSubquery(select(List.of(new SqlSelect.Projection(
                SqlExpr.Column.of(name, outs, ACC), ACC, null)), self, null,
                List.of(SqlSelect.SortKey.desc(i)), 1L));
        reads.add(last);
        return last;
    }

    private static SqlSelect select(List<SqlSelect.Projection> ps, SqlSource from,
            SqlExpr.@com.legend.base.Nullable Call where, List<SqlSelect.SortKey> order,
            @com.legend.base.Nullable Long limit) {
        return new SqlSelect(ps, false, from, where, List.of(), null, null, order, limit, null, List.of());
    }

    /** Whether {@code e} names nothing but {@code bound} lambda parameters (and the reads this pass made). */
    private boolean closed(SqlExpr e, Set<String> bound) {
        return switch (e) {
            case SqlExpr.Column c -> c.table() == null ? bound.contains(c.name()) : bound.contains(c.table());
            case SqlExpr.Lambda l -> {
                Set<String> inner = new java.util.HashSet<>(bound);
                inner.addAll(l.params());
                yield closed(l.body(), inner);
            }
            case SqlExpr.ScalarSubquery s -> reads.contains(s);
            case SqlExpr.Exists x -> false;
            case SqlExpr.InSubquery x -> false;
            case SqlExpr.Quantified x -> false;
            case SqlExpr.RowOrder x -> false;
            case SqlExpr.StoredRead x -> false;
            case SqlExpr.Star x -> false;
            case SqlExpr.StarExcept x -> false;
            case SqlExpr.WindowCall x -> false;
            case com.legend.sql.SqlAgg.Reducer x -> false;
            case SqlExpr.FoldCall f -> closed(f.source(), bound) && closed(f.init(), bound)
                    && closed(f.lambda(), bound);
            default -> e.children().stream().allMatch(k -> closed(k, bound));
        };
    }

    /** {@code e} with the parameter {@code name} read as {@code with}: a bare read is {@code with}; a read qualified by
     *  it ({@code p.first}, a field of a struct element) is that field of {@code with}. A lambda rebinding the name
     *  shadows it. */
    private static SqlExpr substitute(SqlExpr e, String name, SqlExpr with) {
        if (e instanceof SqlExpr.Column c) {
            if (c.table() == null && c.name().equals(name)) {
                return with;
            }
            return name.equals(c.table()) ? SqlExpr.StructGet.of(with, c.name()) : e;
        }
        if (e instanceof SqlExpr.Lambda l && l.params().contains(name)) {
            return e;
        }
        List<SqlExpr> kids = e.children();
        if (kids.isEmpty()) {
            return e;
        }
        List<SqlExpr> mapped = new ArrayList<>(kids.size());
        boolean changed = false;
        for (SqlExpr k : kids) {
            SqlExpr m = substitute(k, name, with);
            changed |= m != k;
            mapped.add(m);
        }
        return changed ? e.withChildren(mapped) : e;
    }
}
