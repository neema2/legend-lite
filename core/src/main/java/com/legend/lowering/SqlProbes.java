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

    /** Whether a subquery of {@code predicate} holds one of {@code grouped}'s aggregates that read no column: a HAVING
     *  fold carried it there, where no database can tell whose rows it aggregates (DuckDB: "Aggregate with only constant
     *  parameters has to be bound in the root subquery"; Postgres binds it to the subquery's own rows; the IQ scale
     *  program, 2026-10-22: its fuzzer's grouped derived tables read by a correlated subquery). A subquery reading only
     *  group keys keeps its HAVING (an independent review, 2026-10-23); one with an aggregate of its own equal to such a
     *  projection isolates too, which costs a subselect, never an answer. */
    static boolean carriesAggregateOfNoColumn(SqlExpr predicate, SqlSelect grouped) {
        java.util.Set<SqlExpr> carried = new java.util.HashSet<>();
        for (SqlSelect.Projection p : grouped.projections()) {
            aggregatesOfNoColumn(p.expr(), carried);
        }
        if (carried.isEmpty()) {
            return false;
        }
        boolean[] hit = {false};
        var subqueries = new SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr x) {
                com.legend.sql.SqlQuery sub = switch (x) {
                    case SqlExpr.ScalarSubquery q -> q.subquery();
                    case SqlExpr.Exists q -> q.subquery();
                    case SqlExpr.InSubquery q -> q.subquery();
                    case SqlExpr.Quantified q -> q.subquery();
                    default -> null;
                };
                if (sub != null && !hit[0]) {
                    var inside = new SqlRewriter() {
                        @Override
                        protected SqlExpr expr(SqlExpr y) {
                            hit[0] |= carried.contains(y);
                            return y;
                        }
                    };
                    inside.rewrite(sub);
                }
                return x;
            }

            void scan(SqlExpr x) {
                rewriteExpr(x);
            }
        };
        subqueries.scan(predicate);
        return hit[0];
    }

    /** The aggregates of {@code e} that read no column ({@code MIN(-1)}, {@code COUNT(*)}), into {@code out}. */
    private static void aggregatesOfNoColumn(SqlExpr e, java.util.Set<SqlExpr> out) {
        var probe = new SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr x) {
                if (x instanceof com.legend.sql.SqlAgg.Reducer r
                        && r.args().stream().noneMatch(SqlProbes::readsColumn)) {
                    out.add(x);
                }
                return x;
            }

            void scan(SqlExpr x) {
                rewriteExpr(x);
            }
        };
        probe.scan(e);
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
