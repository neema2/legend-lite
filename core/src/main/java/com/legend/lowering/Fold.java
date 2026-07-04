package com.legend.lowering;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;

/**
 * THE fold authority (PHASE_HIJ_LOWERING.md): the single owner of the
 * fold-vs-isolate decision. SQL's SELECT evaluates its slots in one fixed
 * order &mdash; {@code FROM → WHERE → GROUP BY → HAVING → window → QUALIFY →
 * SELECT list → DISTINCT → ORDER BY → LIMIT/OFFSET} &mdash; so whether a
 * pipeline op can extend the current select is a property of that order (does
 * the op commute past every already-occupied later slot?), not of the op.
 * Master plangen re-derived this per operator and drifted; this class is the
 * one place the knowledge lives.
 *
 * <p>Each method answers for one op kind. {@code true} = extend the current
 * select; {@code false} = the caller must isolate it as a subselect first.
 */
final class Fold {

    private Fold() {
    }

    /**
     * A filter always has a slot in an UNTRUNCATED select: WHERE normally,
     * HAVING over GROUP BY, QUALIFY over window columns (filtering commutes
     * with sorting, so ORDER BY does not force isolation &mdash; but LIMIT/OFFSET
     * truncate, and filtering does not commute with truncation). DISTINCT is
     * also a boundary: filtering after dedup can only be expressed after the
     * dedup happened.
     */
    static FilterSlot filterSlot(SqlSelect s, boolean referencesWindowColumn) {
        if (s.limit() != null || s.offset() != null || s.distinct()) {
            return FilterSlot.ISOLATE;
        }
        if (referencesWindowColumn) {
            return FilterSlot.QUALIFY;
        }
        if (!s.groupBy().isEmpty()) {
            return FilterSlot.HAVING;
        }
        return FilterSlot.WHERE;
    }

    enum FilterSlot { WHERE, HAVING, QUALIFY, ISOLATE }

    /**
     * Column selection/rename/projection narrows or re-labels the SELECT list.
     * It folds unless the select already DEDUPed (narrowing after DISTINCT
     * changes semantics) or truncated (projection commutes with LIMIT, but a
     * narrowed list may drop a column ORDER BY needs only under DISTINCT
     * &mdash; plain ORDER BY may reference source columns in the same select).
     */
    static boolean projectionFolds(SqlSelect s) {
        return !s.distinct() && s.limit() == null && s.offset() == null;
    }

    /**
     * Narrowing WITH dedup ({@code distinct(~cols)}): DISTINCT requires every
     * ORDER BY key to survive into the projected set (SQL rejects ordering a
     * deduped result by a dropped column). Full-row dedup trivially satisfies
     * this — which is why {@code sort→distinct()} stays flat: whole-row dedup
     * commutes with reordering.
     */
    static boolean distinctNarrowFolds(SqlSelect s, java.util.List<String> keptColumns) {
        for (SqlSelect.SortKey k : s.orderBy()) {
            if (!(k.expr() instanceof SqlExpr.Column c) || !keptColumns.contains(c.name())) {
                return false;
            }
        }
        return true;
    }

    /** Sort folds iff ORDER BY is free (a second sort re-orders; last wins only via isolation). */
    static boolean sortFolds(SqlSelect s) {
        return s.orderBy().isEmpty();
    }

    /** {@code limit n} folds iff LIMIT is free (limit-of-limit must nest to keep the smaller window). */
    static boolean limitFolds(SqlSelect s) {
        return s.limit() == null;
    }

    /** {@code drop n} folds iff OFFSET AND LIMIT are free (offset after limit shrinks the window). */
    static boolean offsetFolds(SqlSelect s) {
        return s.offset() == null && s.limit() == null;
    }

    /**
     * DISTINCT dedups the projected row; it folds only while nothing
     * order-or-truncation-sensitive is pending (master's rule: groupBy,
     * orderBy, limit all force isolation).
     */
    static boolean distinctFolds(SqlSelect s) {
        return s.groupBy().isEmpty() && s.orderBy().isEmpty()
                && s.limit() == null && s.offset() == null;
    }

    /**
     * Can a predicate/projection reference {@code column} directly in this
     * select, and as WHAT expression? Star select: the column IS a source
     * column. Projected select: the reference substitutes to the projection's
     * expression &mdash; folding stays legal when that expression is a plain
     * column (real legend's restrict/rename flatten); a COMPUTED projection
     * returns null and the caller isolates (recomputing scalars in WHERE is
     * legal but deferred until the corpus pins it).
     */
    static SqlExpr resolveInto(SqlSelect s, String fromAlias, String column) {
        if (s.projections().isEmpty()) {
            return new SqlExpr.Column(fromAlias, column);
        }
        for (SqlSelect.Projection p : s.projections()) {
            String name = p.alias() != null ? p.alias()
                    : p.expr() instanceof SqlExpr.Column c ? c.name() : null;
            if (column.equals(name)) {
                return p.expr() instanceof SqlExpr.Column c ? c : null;
            }
        }
        return null;
    }
}
