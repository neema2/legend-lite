package com.legend.sql.dialect;

import com.legend.sql.DialectCapability;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Structural capability pass (remediation T3.2 step 3): a backend without
 * a QUALIFY clause filters window results by WRAPPING the select &mdash;
 * {@code SELECT * FROM (inner) AS qualify_src WHERE <qualify>}. Formerly
 * the renderer's {@code select} method rewrote this on the fly; now it is
 * MIR&rarr;MIR, and the writer never sees a QUALIFY it cannot spell.
 *
 * <p>BY OUTPUTS (2026-10-01 W5.5/P1 Postgres dialect, its first production user):
 * the plain wrap moves the predicate verbatim, so a window call (the reason a
 * predicate is a QUALIFY) lands in the outer WHERE, where no database allows one,
 * still naming the inner select's aliases; and the inner ORDER BY/LIMIT run BEFORE
 * the filter. The by-outputs form addresses everything through
 * {@code qualify_src}: a predicate or sort operand that IS a projection reads that
 * output, any other window call or column becomes a hidden inner projection (the
 * outer select lists the declared outputs, so it never leaks), and ORDER BY,
 * LIMIT and OFFSET move outside, after the filter. The plain form stays the base
 * renderer's (SQLite) until that dialect adopts this one: its output is unchanged.
 */
final class QualifyToSubselect extends SqlRewriter {

    private static final String ALIAS = "qualify_src";

    private final boolean byOutputs;

    QualifyToSubselect() {
        this(false);
    }

    QualifyToSubselect(boolean byOutputs) {
        this.byOutputs = byOutputs;
    }

    @Override
    protected SqlQuery select(SqlSelect s) {
        if (s.qualify() == null) {
            return s;
        }
        if (byOutputs) {
            return byOutputs(s);
        }
        SqlSelect inner = s.withQualify(null);
        return new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlExpr.Star(null),
                        null, null)),
                false,
                new SqlSource.Subselect(inner, "qualify_src", null),
                s.qualify(), List.of(), null, null,
                List.of(), null, null, s.outputs());
    }

    private static SqlQuery byOutputs(SqlSelect s) {
        SqlExpr qualify = java.util.Objects.requireNonNull(s.qualify());
        if (hasSubquery(qualify) || s.orderBy().stream().anyMatch(k -> hasSubquery(k.expr()))) {
            throw new DialectCapability("a QUALIFY or sort holding a subquery cannot be"
                    + " re-addressed through its wrapping select");
        }
        List<SqlSelect.Projection> projections = new ArrayList<>(s.projections().isEmpty()
                ? List.of(new SqlSelect.Projection(new SqlExpr.Star(null), null, null))
                : s.projections());
        List<SqlSelect.Projection> hidden = new ArrayList<>();
        Function<SqlExpr, SqlExpr> readOutputs = e -> readThroughOutputs(e, projections, hidden);
        SqlExpr where = readOutputs.apply(qualify);
        List<SqlSelect.SortKey> order = s.orderBy().stream()
                .map(k -> new SqlSelect.SortKey(readOutputs.apply(k.expr()), k.ascending(),
                        k.nullOrder(), k.outputName()))
                .toList();
        if (!hidden.isEmpty() && s.distinct()) {
            throw new DialectCapability("a DISTINCT select's QUALIFY or sort reads a value"
                    + " it does not project; a hidden column would change what is distinct");
        }
        List<SqlSelect.Projection> all = new ArrayList<>(projections);
        all.addAll(hidden);
        SqlSelect inner = new SqlSelect(all, s.distinct(), s.from(), s.where(), s.groupBy(),
                s.having(), null, List.of(), null, null, s.outputs());
        // the declared outputs, by name: a hidden column never leaves (a star when
        // nothing is hidden — the outputs are the inner select's own)
        List<SqlSelect.Projection> outer = hidden.isEmpty()
                ? List.of(new SqlSelect.Projection(new SqlExpr.Star(null), null, null))
                : s.outputs().stream().map(o -> new SqlSelect.Projection(
                        SqlExpr.Column.derived(ALIAS, o.name()), null, o)).toList();
        return new SqlSelect(outer, false, new SqlSource.Subselect(inner, ALIAS, null),
                where, List.of(), null, null, order, s.limit(), s.offset(), s.outputs());
    }

    /** {@code e} re-addressed through {@code qualify_src}: whole window calls first
     * (so their own column operands stay inner), then the columns left over. */
    private static SqlExpr readThroughOutputs(SqlExpr e, List<SqlSelect.Projection> projections,
            List<SqlSelect.Projection> hidden) {
        SqlExpr windowsRead = new Walk() {
            @Override
            protected SqlExpr expr(SqlExpr x) {
                return x instanceof SqlExpr.WindowCall ? output(x, projections, hidden) : x;
            }
        }.run(e);
        return new Walk() {
            @Override
            protected SqlExpr expr(SqlExpr x) {
                return x instanceof SqlExpr.Column c && !ALIAS.equals(c.table())
                        ? output(x, projections, hidden) : x;
            }
        }.run(windowsRead);
    }

    /** The output that projects {@code x}, else a new hidden projection of it. */
    private static SqlExpr output(SqlExpr x, List<SqlSelect.Projection> projections,
            List<SqlSelect.Projection> hidden) {
        for (SqlSelect.Projection p : projections) {
            String name = p.alias() != null ? p.alias()
                    : p.out() != null ? p.out().name() : p.outputName();
            if (name != null && same(p.expr(), x)) {
                return SqlExpr.Column.derived(ALIAS, name);
            }
        }
        for (SqlSelect.Projection p : hidden) {
            if (same(p.expr(), x)) {
                return SqlExpr.Column.derived(ALIAS, java.util.Objects.requireNonNull(p.alias()));
            }
        }
        String name = "__qualify" + hidden.size();
        hidden.add(new SqlSelect.Projection(x, name, null));
        return SqlExpr.Column.derived(ALIAS, name);
    }

    /** Structural equality; a column is the same column whatever type fact it carries. */
    private static boolean same(SqlExpr a, SqlExpr b) {
        if (a instanceof SqlExpr.Column ca && b instanceof SqlExpr.Column cb) {
            return java.util.Objects.equals(ca.table(), cb.table()) && ca.name().equals(cb.name());
        }
        return a.equals(b);
    }

    private static boolean hasSubquery(SqlExpr e) {
        boolean[] found = {false};
        new Walk() {
            @Override
            protected SqlExpr expr(SqlExpr x) {
                if (x instanceof SqlExpr.Exists || x instanceof SqlExpr.ScalarSubquery
                        || x instanceof SqlExpr.InSubquery || x instanceof SqlExpr.Quantified) {
                    found[0] = true;
                }
                return x;
            }
        }.run(e);
        return found[0];
    }

    /** An expression walk with the rewriter's own traversal (post-children hooks). */
    private abstract static class Walk extends SqlRewriter {
        final SqlExpr run(SqlExpr e) {
            return rewriteExpr(e);
        }
    }
}
