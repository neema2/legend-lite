// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;
import com.legend.sql.SqlWith;
import com.legend.sql.TypeFact;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The dialect DELIVERS the platform's type facts on its wire (the {@link AggregatesDeliverDouble} doctrine),
 * here at the statement's boundary. Postgres keeps no precision on a COMPUTED numeric (its typmod is
 * gone: {@code 1.5 * 2} reports numeric with precision 0, probed on 16.15, 2026-10-02) and has no
 * 128-bit integer ({@code sum(bigint)} is a bare numeric), so a root column the platform types
 * DECIMAL(p,s) or HUGEINT arrives as an unsized numeric. Each such root column is cast to its type
 * -- NUMERIC(p,s), and HUGEINT's NUMERIC(38) -- which a subquery, a CTE and a UNION ALL of the same
 * types carry through (probed). The values are unchanged: Postgres's numeric arithmetic keeps
 * DuckDB's scales; the cast states them.
 *
 * <p>Root-only: a typmod inside the statement is never read. A star at the root carries its
 * source's columns as they are, and a table's own column is read as declared.
 */
final class RootNumericTypes extends SqlRewriter {

    /** The SQL type of a Decimal no precision is declared for (the lowering's {@code PureSql.type(DECIMAL)}). */
    private static final SqlType UNBOUNDED_DECIMAL = new SqlType.Decimal(38, 18);

    @Override
    public SqlQuery rewriteRoot(SqlQuery q) {
        return switch (q) {
            case SqlSelect s -> typed(s);
            case SqlUnion u -> {
                List<SqlQuery> bs = new ArrayList<>();
                u.branches().forEach(b -> bs.add(rewriteRoot(b)));
                yield new SqlUnion(bs, u.all(), u.outputs());
            }
            case SqlWith w -> SqlWith.prepend(w.ctes(), rewriteRoot(w.body()), w.recursive());
        };
    }

    private static SqlSelect typed(SqlSelect s) {
        Map<String, SqlType> labels = new HashMap<>();
        s.outputs().forEach(o -> labels.put(o.name(), o.type()));
        List<SqlSelect.Projection> ps = new ArrayList<>();
        Map<SqlExpr, SqlExpr> cast = new HashMap<>();
        for (SqlSelect.Projection p : s.projections()) {
            String name = p.alias() != null ? p.alias() : p.outputName();
            SqlType label = name != null ? labels.get(name) : null;
            SqlType t = numeric(p.expr(), label != null ? label
                    : p.expr().type() instanceof TypeFact.Typed f ? f.type() : null);
            if (t == null) {
                ps.add(p);
            } else {
                SqlExpr c = new SqlExpr.Cast(p.expr(), t, false);
                ps.add(new SqlSelect.Projection(c, name, p.out()));
                cast.put(p.expr(), c);
            }
        }
        if (cast.isEmpty()) {
            return s;
        }
        // a DISTINCT select orders by its select list's expressions only (Postgres: "for SELECT DISTINCT, ORDER BY
        // expressions must appear in select list"): a key that was a cast column's expression is the cast one now.
        // The cast changes no value, so no order either (found by the SQL front end's randomized check, 2026-10-12)
        List<SqlSelect.SortKey> order = s.orderBy();
        if (s.distinct()) {
            order = new ArrayList<>();
            for (SqlSelect.SortKey k : s.orderBy()) {
                SqlExpr c = cast.get(k.expr());
                order.add(c == null ? k : new SqlSelect.SortKey(c, k.ascending(), k.nullOrder(), k.outputName()));
            }
        }
        return new SqlSelect(ps, s.distinct(), s.from(), s.where(), s.groupBy(), s.having(), s.qualify(), order,
                s.limit(), s.offset(), s.outputs());
    }

    /** The type a root column is cast to: its label's DECIMAL(p,s) or HUGEINT, unless already cast to
     *  it. The label is the select's own output of that name -- the type the statement claims, which
     *  can be wider than its expression's (a folded integer sum, {@code 15 + 13 + 2}, is labeled
     *  HUGEINT, as every sum is); an unnamed column's is its expression's type. */
    private static @com.legend.base.Nullable SqlType numeric(SqlExpr e, @com.legend.base.Nullable SqlType label) {
        if (!(label instanceof SqlType.Decimal || label == SqlType.Scalar.HUGEINT)) {
            return null;
        }
        // an unbounded Decimal (the platform's carrier, DECIMAL(38,18): the compiler's Type.PrecisionDecimal
        // .DEFAULT_DECIMAL, the engine's carrier precision, which the typer also refines a Decimal to -- so a declared
        // Decimal(38,18) is that same type by design, not a value read as a guess) states no scale of its own: the value keeps the
        // scale Postgres's numeric arithmetic gives it, as Pure's BigDecimal keeps it (a Decimal parameter bound as
        // 2.50 answers 2.50, not 2.500000000000000000; 2026-10-27, the result boundary program's phase 3, found when
        // the typed rows became the answer and the root became the statement's own)
        if (label.equals(UNBOUNDED_DECIMAL)) {
            return null;
        }
        // a table's own column is held as its store declares it (the StoredReads premise), precision and all
        boolean asDeclared = e instanceof SqlExpr.Cast c && c.target().equals(label)
                || e instanceof SqlExpr.Column col && col.origin() != com.legend.sql.OutputCol.Origin.DERIVED;
        return asDeclared ? null : label;
    }
}
