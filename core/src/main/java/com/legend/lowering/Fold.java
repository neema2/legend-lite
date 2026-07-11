package com.legend.lowering;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

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

    /**
     * GROUP BY replaces the select's row space; it folds only onto a select
     * with nothing group-sensitive accumulated (no prior grouping/dedup/
     * truncation/order — grouping does not preserve order). QUALIFY and
     * window projections also block: their windows compute over the
     * UNGROUPED rows, which no longer exist once GROUP BY lands.
     */
    static boolean groupByFolds(SqlSelect s) {
        return s.groupBy().isEmpty() && !s.distinct() && s.orderBy().isEmpty()
                && s.limit() == null && s.offset() == null && s.qualify() == null
                && s.projections().stream()
                        .noneMatch(p -> p.expr() instanceof SqlExpr.WindowCall);
    }

    /**
     * extend APPENDS a computed column: row count is untouched, so it commutes
     * with truncation and ordering; only DISTINCT is a boundary (extending a
     * deduped row set would dedup WITH the new column).
     */
    static boolean extendFolds(SqlSelect s) {
        return !s.distinct();
    }

    /**
     * A window column computes over the CURRENT select's row set: WHERE is fine
     * (windows evaluate after it), ORDER BY is fine (independent orderings) —
     * but truncation, dedup, and grouping all change the row set the window
     * must see, so each forces isolation.
     */
    static boolean windowFolds(SqlSelect s) {
        return !s.distinct() && s.limit() == null && s.offset() == null && s.groupBy().isEmpty();
    }

    /** Sort folds iff ORDER BY is free (a second sort re-orders; last wins only via isolation). */
    static boolean sortFolds(SqlSelect s) {
        // LIMIT/OFFSET guard: within ONE select ORDER BY applies BEFORE
        // LIMIT, so folding a sort into an already-limited select would
        // sort-then-limit where the query asked limit-then-sort (audit).
        return s.orderBy().isEmpty() && s.limit() == null && s.offset() == null;
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
    static SqlExpr resolveInto(SqlSelect s, String column) {
        if (s.projections().isEmpty()) {
            return sourceColumn(s.from(), column);
        }
        boolean star = false;
        for (SqlSelect.Projection p : s.projections()) {
            if (p.expr() instanceof SqlExpr.Star) {
                star = true;
                continue;
            }
            String name = p.outputName();
            if (column.equals(name)) {
                return p.expr() instanceof SqlExpr.Column c ? c : null;
            }
        }
        // A star projection (extend's `t0.*, expr AS x`) keeps every source
        // column visible; names not claimed by an explicit projection resolve
        // straight to the source.
        return star ? sourceColumn(s.from(), column) : null;
    }

    /**
     * The qualified column reference for {@code column} within a FROM source.
     * Single-alias sources resolve schema-blind (the alias qualifies any name
     * — Phase G already validated existence). A JOIN resolves by SIDE: the
     * side whose output schema claims the name qualifies it; join outputs are
     * disjoint by Phase-G typing (duplicate columns are a type error; prefix
     * joins rename). Null when no side claims the column.
     */
    static SqlExpr.Column sourceColumn(SqlSource src, String column) {
        return switch (src) {
            case SqlSource.Table t -> claims(t.outputs(), column)
                    ? new SqlExpr.Column(t.alias(), column) : null;
            case SqlSource.Subselect sub -> claims(sub.outputs(), column)
                    ? new SqlExpr.Column(sub.alias(), column) : null;
            case SqlSource.Values v -> claims(v.outputs(), column)
                    ? new SqlExpr.Column(v.alias(), column) : null;
            case SqlSource.SourceUrl u -> claims(u.outputs(), column)
                    ? new SqlExpr.Column(u.alias(), column) : null;
            // Pivot outputs are DYNAMIC (one column per pivoted value) — the
            // static schema cannot enumerate them, so a pivot claims any name.
            case SqlSource.Pivot p -> new SqlExpr.Column(p.alias(), column);
            case SqlSource.Join j -> {
                SqlExpr.Column left = sourceColumn(j.left(), column);
                yield left != null ? left : sourceColumn(j.right(), column);
            }
        };
    }

    /**
     * Schema-AWARE claim check: a source claims only columns its stamped
     * outputs actually contain — load-bearing for CORRELATED scopes, where an
     * unclaimed name must fall through to the enclosing lambda instead of
     * being blindly alias-qualified. The real pipeline stamps outputs on
     * every source; an UNSTAMPED source is a construction bug and fails
     * loudly rather than silently claiming everything.
     */
    private static boolean claims(java.util.List<com.legend.sql.OutputCol> outputs, String column) {
        if (outputs.isEmpty()) {
            throw new IllegalStateException(
                    "source has no stamped output schema — cannot resolve column '"
                            + column + "' (stamp outputs at construction)");
        }
        return outputs.stream().anyMatch(c -> c.name().equals(column));
    }
}
