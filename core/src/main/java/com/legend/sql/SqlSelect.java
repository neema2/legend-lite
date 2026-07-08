package com.legend.sql;

import java.util.List;

/**
 * THE query node (PHASE_HIJ_LOWERING.md): one record with every clause slot,
 * mirroring real legend's {@code SelectSQLQuery}. The fold policy extends a
 * single {@code SqlSelect} through a run of compatible relational ops via the
 * {@code with*} copiers; a fresh nesting level exists only as an explicit
 * {@link SqlSource.Subselect}. Empty {@link #projections} means {@code SELECT *}.
 */
public record SqlSelect(List<Projection> projections, boolean distinct, SqlSource from,
                        SqlExpr where, List<SqlExpr> groupBy, SqlExpr having, SqlExpr qualify,
                        List<SortKey> orderBy, Long limit, Long offset, List<OutputCol> outputs)
        implements SqlQuery {

    /** {@code SELECT * FROM source} with every other clause empty. */
    public static SqlSelect starOf(SqlSource from) {
        return new SqlSelect(List.of(), false, from, null, List.of(), null, null,
                List.of(), null, null, from.outputs());
    }

    public record Projection(SqlExpr expr, String alias) {

        /**
         * The projected OUTPUT name: the alias, else the bare column's own
         * name, else null (a computed expression with no alias has no
         * addressable name). THE one implementation of the rule (an audit
         * found it duplicated across Fold and the Lowerer).
         */
        public String outputName() {
            return alias != null ? alias
                    : expr instanceof SqlExpr.Column c ? c.name() : null;
        }

    }

    /** One ORDER BY key; {@code nullOrder} null = dialect default. */
    public record SortKey(SqlExpr expr, boolean ascending, NullOrder nullOrder) {
        public enum NullOrder { NULLS_FIRST, NULLS_LAST }

        public static SortKey asc(SqlExpr e) {
            return new SortKey(e, true, null);
        }

        /** Test-DSL convenience (no production callers; hand-built IR only). */
        public static SortKey desc(SqlExpr e) {
            return new SortKey(e, false, null);
        }
    }

    // ----- clause copiers: the fold policy's fingers -----

    public SqlSelect withProjections(List<Projection> p, List<OutputCol> out) {
        return new SqlSelect(p, distinct, from, where, groupBy, having, qualify, orderBy, limit, offset, out);
    }

    public SqlSelect withDistinct() {
        return new SqlSelect(projections, true, from, where, groupBy, having, qualify, orderBy, limit, offset, outputs);
    }

    public SqlSelect withWhere(SqlExpr w) {
        return new SqlSelect(projections, distinct, from, w, groupBy, having, qualify, orderBy, limit, offset, outputs);
    }

    public SqlSelect withGroupBy(List<SqlExpr> keys) {
        return new SqlSelect(projections, distinct, from, where, keys, having, qualify, orderBy, limit, offset, outputs);
    }

    public SqlSelect withHaving(SqlExpr h) {
        return new SqlSelect(projections, distinct, from, where, groupBy, h, qualify, orderBy, limit, offset, outputs);
    }

    public SqlSelect withQualify(SqlExpr q) {
        return new SqlSelect(projections, distinct, from, where, groupBy, having, q, orderBy, limit, offset, outputs);
    }

    public SqlSelect withOrderBy(List<SortKey> keys) {
        return new SqlSelect(projections, distinct, from, where, groupBy, having, qualify, keys, limit, offset, outputs);
    }

    public SqlSelect withLimit(Long n) {
        return new SqlSelect(projections, distinct, from, where, groupBy, having, qualify, orderBy, n, offset, outputs);
    }

    public SqlSelect withOffset(Long n) {
        return new SqlSelect(projections, distinct, from, where, groupBy, having, qualify, orderBy, limit, n, outputs);
    }
}
