// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

/** Read-only MIR probes shared by scalar lowering rules. */
final class SqlProbes {

    private SqlProbes() {
    }

    /**
     * Star + plain-column renames, nothing else — the shape a prefixed join
     * produces. Such a select adds no row semantics; it can host further
     * joins with its renames carried forward (the Lowerer's join site).
     */
    static boolean isRenameOnlySelect(SqlSelect s) {
        if (s.projections().isEmpty() || s.distinct()
                || s.where() != null || !s.groupBy().isEmpty() || s.having() != null
                || s.qualify() != null || !s.orderBy().isEmpty()
                || s.limit() != null || s.offset() != null) {
            return false;
        }
        if (!(s.from() instanceof SqlSource.Join || s.from() instanceof SqlSource.Table)) {
            return false;
        }
        for (SqlSelect.Projection p : s.projections()) {
            if (!(p.expr() instanceof SqlExpr.Star || p.expr() instanceof SqlExpr.Column)) {
                return false;
            }
        }
        return true;
    }

    /** Whether the expression tree carries a scalar subquery or exists
     * (walked via the shared MIR rewriter; probe-only). */
    static boolean containsSubquery(SqlExpr e) {
        boolean[] hit = {false};
        var probe = new SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr x) {
                if (x instanceof SqlExpr.ScalarSubquery
                        || x instanceof SqlExpr.Exists
                        || x instanceof SqlExpr.InSubquery
                        || x instanceof SqlExpr.Quantified) {
                    hit[0] = true;
                }
                return x;
            }

            void scan(SqlExpr x) {
                rewriteExpr(x);
            }
        };
        probe.scan(e);
        return hit[0];
    }
    /** Whether the expression tree carries an aggregate that reads no column ({@code MIN(-1)}, {@code COUNT(*)}):
     *  substituted into a subquery, no database can tell whose rows it aggregates (DuckDB: "Aggregate with only
     *  constant parameters has to be bound in the root subquery"; Postgres binds it to the subquery's own rows). The IQ
     *  scale program, 2026-10-22: its fuzzer's grouped derived tables read by a correlated subquery. */
    static boolean aggregatesNoColumn(SqlExpr e) {
        boolean[] hit = {false};
        var probe = new SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr x) {
                if (x instanceof com.legend.sql.SqlAgg.Reducer r
                        && r.args().stream().noneMatch(SqlProbes::readsColumn)) {
                    hit[0] = true;
                }
                return x;
            }

            void scan(SqlExpr x) {
                rewriteExpr(x);
            }
        };
        probe.scan(e);
        return hit[0];
    }

    /** Whether the expression tree reads a column. */
    static boolean readsColumn(SqlExpr e) {
        boolean[] hit = {false};
        var probe = new SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr x) {
                if (x instanceof SqlExpr.Column) {
                    hit[0] = true;
                }
                return x;
            }

            void scan(SqlExpr x) {
                rewriteExpr(x);
            }
        };
        probe.scan(e);
        return hit[0];
    }

    /** MERGE BY NAME (engine tds.pure {@code join(TDS, TDS, JoinType, keys)}):
     * when a join's sides SHARE output names, the explicit merged
     * projection — the outer-preserved side's copy of a shared column
     * survives (RIGHT keeps the right's, every other kind the left's) and
     * the other side's copy is dropped; a star would be ambiguous. Empty
     * when the sides are disjoint: the star frame stands. */
    static java.util.Optional<java.util.List<SqlSelect.Projection>> mergedByName(
            SqlSource.Join source, java.util.List<OutputCol> contract) {
        java.util.Set<String> leftNames = new java.util.LinkedHashSet<>();
        source.left().outputs().forEach(c -> leftNames.add(c.name()));
        java.util.Set<String> rightNames = new java.util.LinkedHashSet<>();
        source.right().outputs().forEach(c -> rightNames.add(c.name()));
        if (java.util.Collections.disjoint(leftNames, rightNames)) {
            return java.util.Optional.empty();
        }
        boolean rightKeeps = source.kind() == SqlSource.Join.Kind.RIGHT;
        java.util.List<SqlSelect.Projection> ps = new java.util.ArrayList<>();
        for (OutputCol c : source.left().outputs()) {
            if (rightKeeps && rightNames.contains(c.name())) {
                continue;
            }
            SqlExpr.Column col = sideColumn(source.left(), c.name());
            ps.add(new SqlSelect.Projection(source.kind().padsLeft() ? col.asNullable() : col,
                    c.name(), Fold.named(contract, c.name())));
        }
        for (OutputCol c : source.right().outputs()) {
            if (!rightKeeps && leftNames.contains(c.name())) {
                continue;
            }
            SqlExpr.Column col = sideColumn(source.right(), c.name());
            ps.add(new SqlSelect.Projection(source.kind().padsRight() ? col.asNullable() : col,
                    c.name(), Fold.named(contract, c.name())));
        }
        return java.util.Optional.of(ps);
    }

    private static SqlExpr.Column sideColumn(SqlSource side, String name) {
        return side instanceof SqlSource.Join tree
                ? java.util.Objects.requireNonNull(Fold.sourceColumn(tree, name), name)
                : SqlExpr.Column.of(side.alias(), side.outputs(), name);
    }
}
