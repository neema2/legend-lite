package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import java.util.ArrayList;
import java.util.List;
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

    /** The plan-template placeholder KIND for a field type (h2New spells
     * date placeholders with the type keyword). */
    static com.legend.sql.SqlExpr.PlanParam.Kind planKindOf(
            com.legend.compiler.element.type.Type t) {
        if (t == com.legend.compiler.element.type.Type.Primitive.STRING) {
            return com.legend.sql.SqlExpr.PlanParam.Kind.STRING;
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.DATE) {
            return com.legend.sql.SqlExpr.PlanParam.Kind.DATE;
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.STRICT_DATE) {
            return com.legend.sql.SqlExpr.PlanParam.Kind.STRICT_DATE;
        }
        if (t == com.legend.compiler.element.type.Type.Primitive
                .DATE_TIME) {
            return com.legend.sql.SqlExpr.PlanParam.Kind.DATETIME;
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.FLOAT) {
            return com.legend.sql.SqlExpr.PlanParam.Kind.FLOAT;
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.BOOLEAN) {
            // spells like OTHER on H2; DB2-family dialects QUOTE boolean
            // placeholders (the case-expr compares 'true'/'false' strings)
            return com.legend.sql.SqlExpr.PlanParam.Kind.BOOLEAN;
        }
        if (t instanceof com.legend.compiler.element.type.Type.EnumType) {
            // enum values travel as NAME strings — the engine spells the
            // placeholder quoted ('\${yesOrNo}' = 'NO')
            return com.legend.sql.SqlExpr.PlanParam.Kind.ENUM;
        }
        return com.legend.sql.SqlExpr.PlanParam.Kind.OTHER;
    }

    /** An ORDER-SENSITIVE aggregate over a UNION-ALL-backed source with
     * no explicit key carries pure's CONCATENATE order obligation — the
     * union's branches are an ORDERED list. Stamp a branch ordinal into
     * the union and order the aggregate by it
     * ({@code string_agg(x, sep ORDER BY u_ord)}). H2 satisfies the
     * obligation by insertion order; a parallel backend must carry it
     * explicitly. Null when not applicable. */
    record OrderedAgg(com.legend.sql.SqlSelect base,
            com.legend.sql.SqlAgg.Reducer reducer) {
    }

    static @com.legend.Nullable OrderedAgg orderUnionAggregate(com.legend.sql.SqlSelect base,
            com.legend.sql.SqlAgg.Reducer red) {
        if (red.fn() != com.legend.sql.SqlAgg.Fn.STRING_AGG || !red.orderBy().isEmpty()) {
            return null;
        }
        com.legend.sql.SqlSource.Subselect sub = findUnionSub(base.from());
        if (sub == null || !readsAlias(red.args(), sub.alias())) {
            return null;
        }
        var u = (com.legend.sql.SqlUnion) sub.inner();
        java.util.List<com.legend.sql.SqlQuery> bs = new java.util.ArrayList<>();
        int i = 0;
        for (com.legend.sql.SqlQuery b : u.branches()) {
            if (!(b instanceof com.legend.sql.SqlSelect s)
                    || s.projections().stream().anyMatch(p ->
                            "u_ord".equals(p.outputName()))) {
                return null;
            }
            var ps = new java.util.ArrayList<>(s.projections());
            if (ps.isEmpty()) {
                // a star branch keeps its star AND gains the ordinal
                ps.add(new com.legend.sql.SqlSelect.Projection(
                        new com.legend.sql.SqlExpr.Star(null), null, null));
            }
            // label = wire truth (typed-IR census witness): the ordinal IS
            // a BIGINT
            ps.add(new com.legend.sql.SqlSelect.Projection(
                    new com.legend.sql.SqlExpr.IntLit(i++), "u_ord",
                    new com.legend.sql.OutputCol("u_ord",
                            com.legend.sql.SqlType.Scalar.BIGINT, false)));
            bs.add(s.withProjections(ps));
        }
        var nu = new com.legend.sql.SqlUnion(bs, true, widen(u.outputs()));
        var repl = new com.legend.sql.SqlSource.Subselect(nu, sub.alias(),
                sub.frameName());
        var red2 = new com.legend.sql.SqlAgg.Reducer(red.fn(), red.args(),
                red.distinct(), java.util.List.of(
                        new com.legend.sql.SqlSelect.SortKey(
                                com.legend.sql.SqlExpr.Column.derived(
                                        sub.alias(), "u_ord"), true, null, null)));
        return new OrderedAgg(
                base.withFrom(replaceSub(base.from(), sub, repl)), red2);
    }

    /** The §3b widening (audit: one extra argument silently dropped
     * ordering): the CONCATENATE order obligation applies to a reducer
     * NESTED inside a wrapping expression too — the 3-arg
     * {@code joinStrings('[',',',']')} emits
     * {@code CONCAT(prefix, string_agg, suffix)}, and the old
     * {@code instanceof Reducer} gate never saw through the wrap.
     * First nested match wins (one union source per agg projection). */
    record OrderedAggExpr(com.legend.sql.SqlSelect base,
            com.legend.sql.SqlExpr expr) {
    }

    static @com.legend.Nullable OrderedAggExpr orderUnionAggregateExpr(
            com.legend.sql.SqlSelect base, com.legend.sql.SqlExpr av) {
        if (av instanceof com.legend.sql.SqlAgg.Reducer red) {
            OrderedAgg oa = orderUnionAggregate(base, red);
            return oa == null ? null
                    : new OrderedAggExpr(oa.base(), oa.reducer());
        }
        java.util.List<com.legend.sql.SqlExpr> kids = av.children();
        for (int i = 0; i < kids.size(); i++) {
            OrderedAggExpr k = orderUnionAggregateExpr(base, kids.get(i));
            if (k != null) {
                var nk = new java.util.ArrayList<>(kids);
                nk.set(i, k.expr());
                return new OrderedAggExpr(k.base(), av.withChildren(nk));
            }
        }
        return null;
    }

    private static java.util.List<com.legend.sql.OutputCol> widen(
            java.util.List<com.legend.sql.OutputCol> outs) {
        if (outs == null || outs.isEmpty()) {
            return outs;
        }
        var w = new java.util.ArrayList<>(outs);
        // label = wire truth (typed-IR census witness u_ord := IntLit):
        // the ordinal literal IS a BIGINT
        w.add(new com.legend.sql.OutputCol("u_ord",
                com.legend.sql.SqlType.Scalar.BIGINT, false));
        return w;
    }

    /** The aggregate's VALUE must actually read the union it is ordered
     * by — anchor on the alias, never "the first union found". */
    private static boolean readsAlias(
            java.util.List<com.legend.sql.SqlExpr> args, String alias) {
        var found = new boolean[1];
        for (com.legend.sql.SqlExpr a : args) {
            walkColumns(a, c -> {
                if (alias.equals(c.table())) {
                    found[0] = true;
                }
            });
        }
        return found[0];
    }

    private static void walkColumns(com.legend.sql.SqlExpr e,
            java.util.function.Consumer<com.legend.sql.SqlExpr.Column> f) {
        switch (e) {
            case com.legend.sql.SqlExpr.Column c -> f.accept(c);
            case com.legend.sql.SqlExpr.Call c ->
                    c.args().forEach(x -> walkColumns(x, f));
            case com.legend.sql.SqlExpr.Cast c -> walkColumns(c.value(), f);
            case com.legend.sql.SqlExpr.Group g -> walkColumns(g.inner(), f);
            default -> e.children().forEach(c -> walkColumns(c, f));
        }
    }

    private static com.legend.sql.SqlSource.@com.legend.Nullable Subselect findUnionSub(
            com.legend.sql.SqlSource src) {
        return switch (src) {
            case com.legend.sql.SqlSource.Subselect s
                    when s.inner() instanceof com.legend.sql.SqlUnion u
                            && u.all() -> s;
            case com.legend.sql.SqlSource.Join j -> {
                var l = findUnionSub(j.left());
                yield l != null ? l : findUnionSub(j.right());
            }
            case null, default -> null;
        };
    }

    private static com.legend.sql.SqlSource replaceSub(
            com.legend.sql.SqlSource src,
            com.legend.sql.SqlSource.Subselect target,
            com.legend.sql.SqlSource.Subselect repl) {
        if (src == target) {
            return repl;
        }
        return src instanceof com.legend.sql.SqlSource.Join j
                ? new com.legend.sql.SqlSource.Join(
                        replaceSub(j.left(), target, repl),
                        replaceSub(j.right(), target, repl),
                        j.kind(), j.on())
                : src;
    }

    /** Whether the source reads (possibly through joins) a UNION
     * subselect — the count-of-rows aggregate spelling seam. */
    static boolean unionBacked(com.legend.sql.SqlSource src) {
        return switch (src) {
            case com.legend.sql.SqlSource.Subselect s ->
                    s.inner() instanceof com.legend.sql.SqlUnion;
            case com.legend.sql.SqlSource.Join j ->
                    unionBacked(j.left()) || unionBacked(j.right());
            case null, default -> false;
        };
    }

    /** Combine conjuncts into one AND, FLATTENING same-operator nesting
     * and Group-of-AND operands (the engine's andFilters: same-operator
     * chains never keep their group — a guard group survives only
     * standalone or under or/not). */
    static com.legend.sql.SqlExpr mergeAnd(com.legend.sql.SqlExpr... parts) {
        java.util.List<com.legend.sql.SqlExpr> flat =
                new java.util.ArrayList<>();
        for (com.legend.sql.SqlExpr p2 : parts) {
            flattenInto(p2, flat);
        }
        return flat.size() == 1 ? flat.get(0)
                : new com.legend.sql.SqlExpr.Call(
                        com.legend.sql.SqlFn.AND, flat);
    }

    private static void flattenInto(com.legend.sql.SqlExpr e,
            java.util.List<com.legend.sql.SqlExpr> out) {
        if (e instanceof com.legend.sql.SqlExpr.Group g
                && g.inner() instanceof com.legend.sql.SqlExpr.Call c
                && c.fn() == com.legend.sql.SqlFn.AND) {
            c.args().forEach(x -> flattenInto(x, out));
        } else if (e instanceof com.legend.sql.SqlExpr.Call c2
                && c2.fn() == com.legend.sql.SqlFn.AND) {
            c2.args().forEach(x -> flattenInto(x, out));
        } else {
            out.add(e);
        }
    }

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

    /** Window containment at EXPRESSION depth ({@code rank() + 1} in a
     * projection is as window-carrying as a bare call); children() stops
     * at query boundaries, so subquery-internal windows don't count.
     * NOT consulted by filterSlot: in plain relation composition the
     * engine's relational runtimes fold an ordinary predicate to WHERE
     * even over a window-carrying select (PCT testExtendFilterOutNull
     * passes on H2 AND DuckDB reference adapters — the window sees the
     * FILTERED rows). The opposite behavior is required only at the
     * MAPPING seam, where the engine treats the mapped relation as a
     * non-mergeable view — that isolation is the RESOLVER's decision
     * (windowed ~func pipelines), never a fold rule: ClassSources stamps
     * every window in a class extent's pipeline {@code extentBoundary}
     * and Lowerer.extentBoundary isolates it (corpus
     * testMappingWithWindowColumn was the witness — before the stamp the
     * class filter folded INTO the ~func's window select and John
     * ranked 1st instead of 2nd). */
    static boolean containsWindow(SqlExpr e) {
        return e instanceof SqlExpr.WindowCall
                || e.children().stream().anyMatch(Fold::containsWindow);
    }

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
    static boolean distinctNarrowFolds(SqlSelect s, List<String> keptColumns) {
        for (SqlSelect.SortKey k : s.orderBy()) {
            if (k.expr() instanceof SqlExpr.Column c && keptColumns.contains(c.name())) {
                continue;
            }
            // a key the sort resolved to a kept projection's OWN expression
            // (sort('Firm') over `LEGALNAME as Firm` orders by t.LEGALNAME):
            // the expression survives in the narrowed list (batch 111)
            boolean keptExpr = s.projections().stream().anyMatch(p ->
                    p.outputName() != null && keptColumns.contains(p.outputName())
                    && p.expr().equals(k.expr()));
            if (!keptExpr) {
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
    /**
     * Whether any projection is ROW-MULTIPLYING (a select-list UNNEST —
     * flatten's emission). COUNT(*)-replacement and other whole-row
     * summaries must isolate first: swapping projections out would count
     * the PRE-explosion rows.
     */
    static boolean unnestInProjections(SqlSelect s) {
        return s.projections().stream().anyMatch(p -> containsUnnest(p.expr()));
    }

    private static boolean containsUnnest(SqlExpr e) {
        return e instanceof SqlExpr.Call c
                && (c.fn() == SqlFn.UNNEST
                        || c.args().stream().anyMatch(Fold::containsUnnest));
    }

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

    /** PURE-semantics null placement for a sort key (the
     * pureNullOrder-gated stamp, Sorts). THE TWO-SPEC SPLIT (§7
     * slice-2, SQLTEXT charter landing record): upstream Legend itself
     * diverges — pure-language sorts are NULL-LARGEST (PCT receipts)
     * while the engine's relational sort emits NO NULLS clause and
     * rides its H2 backend's NULLS-LOW default (the corpus goldens'
     * own asserts pin nulls first ascending). Our emission carries
     * each spec explicitly: pure sorts stamp here (null-largest, both
     * directions); engine TDS sorts stay BARE and the execution
     * dialects' bare-key rendering pins the engine placement
     * (AnsiSqlRenderer.sortKey — nulls-low). The old C1.2 story
     * ("connected target places nulls") conflated the two specs. */
    static SqlSelect.SortKey.@com.legend.Nullable NullOrder sortNulls(boolean ascending) {
        // PURE null ordering: null is LARGEST — ASC nulls last, DESC
        // nulls first (witness testRange_..._WithOrderByDESC). BOTH
        // directions stamp EXPLICITLY (§7 slice-2, 2026-09-01): the ASC
        // leg used to ride DuckDB's un-clause default, which happened to
        // coincide — but bare keys now belong to the ENGINE-relational
        // sort semantics (nulls-low, the dialect's bare-key emission),
        // so a pure-semantics sort must carry its whole placement
        // itself. The window ORDER emission already carries the same
        // rule (Lowerer's over() keys).
        return ascending ? SqlSelect.SortKey.NullOrder.NULLS_LAST
                : SqlSelect.SortKey.NullOrder.NULLS_FIRST;
    }

    /** A ≤1-ROW PROOF for a select (C2-i, STAMP_DISCIPLINE_PROGRAM): an
     * explicit LIMIT 0/1, a constant (Dual) source, or a select over an
     * already-proven subselect — projections, WHERE, DISTINCT and
     * GROUP BY never ADD rows; a join source could, so anything else is
     * unprovable. Used to lower a provably-single cell read as a PLAIN
     * scalar subquery instead of the LIST collect (the shape lie the
     * stamp census measured; DB-native scalar-subquery semantics even
     * enforce the row bound at runtime). */
    static boolean provablySingleRow(com.legend.sql.SqlQuery q) {
        if (!(q instanceof SqlSelect s)) {
            return false;
        }
        if (s.limit() != null && s.limit() <= 1) {
            return true;
        }
        return switch (s.from()) {
            case com.legend.sql.SqlSource.Subselect ss ->
                    provablySingleRow(ss.inner());
            case com.legend.sql.SqlSource.Dual ignored -> true;
            default -> false;
        };
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
     * expression &mdash; a plain column (real legend's restrict/rename
     * flatten) or a PURE-SCALAR computed expression (the engine inlines
     * computed projections into consumer threads and merges &mdash; one flat
     * SELECT; testTdsProjectWithEnumToStringEqualityComparison pins it:
     * the enum decode CASE must stay visible to the comparison's
     * source-value inversion). Row-space-dependent shapes (reducers,
     * windows, exists/subqueries, list aggs, UNNEST) return null and the
     * caller isolates.
     */
    static @com.legend.Nullable SqlExpr resolveInto(SqlSelect s, String column) {
        if (column == null) {
            // a COLUMN-LESS read reaching fold resolution was an NPE
            // (dishonest wall) — the producing shape failed to name its
            // column; surface it loudly (audit 24 follow-on).
            throw new IllegalStateException("resolver bug: a column-less"
                    + " read reached fold resolution — the producing shape"
                    + " did not name its column");
        }
        SqlExpr r = resolveIntoExact(s, column);
        if (r == null && s.projections().isEmpty() && s.from()
                instanceof com.legend.sql.SqlSource.RawSql raw) {
            // Phase 1c: an authored-SQL grid's by-name read. A DECLARED
            // schema (the fetchDb catalog grids — §4bZ-U leg 4) stamps
            // the read through the lookup door; a late-bound grid keeps
            // the trust-name behavior (empty outputs -> plain: the
            // database adjudicates unknown names at execution).
            return SqlExpr.Column.of(raw.alias(), raw.outputs(), column);
        }
        if (r == null) {
            // A pivot dynamic column's PURE identity carries quotes
            // ('2011__|__newCol'); its SQL name is the bare inner text —
            // retry unquoted.
            String bare = pivotIdentity(column);
            if (!bare.equals(column)) {
                r = resolveIntoExact(s, bare);
            }
        }
        return r;
    }

    /** A projection expression that may substitute INTO a consumer slot
     * (WHERE / ORDER BY / an outer projection) of the SAME select without
     * changing semantics: pure scalar over the current row. Reducers and
     * windows compute over a ROW SET, exists/subqueries carry their own
     * queries (duplication deferred until pinned), list aggs are reducers
     * in disguise, and UNNEST is row-multiplying — each isolates. */
    private static boolean scalarInlineable(SqlExpr e) {
        if (e instanceof com.legend.sql.SqlAgg.Reducer
                || e instanceof SqlExpr.WindowCall
                || e instanceof SqlExpr.Exists
                || e instanceof SqlExpr.ScalarSubquery
                || e instanceof SqlExpr.OrderedListAgg
                || e instanceof SqlExpr.JsonArrayAgg
                || e instanceof SqlExpr.Star
                || e instanceof SqlExpr.StarExcept
                || e instanceof SqlExpr.Call c && c.fn() == SqlFn.UNNEST) {
            return false;
        }
        for (SqlExpr ch : e.children()) {
            if (!scalarInlineable(ch)) {
                return false;
            }
        }
        return true;
    }

    /** Whether the expression reads ANY column. A sort key resolving to
     * a pure CONSTANT projection must not inline — {@code ORDER BY 'lit'}
     * is a DuckDB binder error (and a no-op ordinal elsewhere); behind
     * the isolation subselect the same key is a plain output column
     * (testLowerProjectColsNotEliminatedWithSort). */
    static boolean referencesColumn(SqlExpr e) {
        if (e instanceof SqlExpr.Column) {
            return true;
        }
        for (SqlExpr ch : e.children()) {
            if (referencesColumn(ch)) {
                return true;
            }
        }
        return false;
    }

    private static String pivotIdentity(String column) {
        return column.length() >= 2 && column.startsWith("'") && column.endsWith("'")
                && column.contains(Type.RelationType.PIVOT_SEPARATOR)
                ? column.substring(1, column.length() - 1)
                : column;
    }

    private static @com.legend.Nullable SqlExpr resolveIntoExact(SqlSelect s, String column) {
        if (s.projections().isEmpty()) {
            // §E3-S: the select's WHERE rides into the read door so
            // pad flips agree with the frame's WHERE≡INNER outputs
            return sourceColumn(s.from(), column,
                    com.legend.sql.SqlTyping.whereNullRejections(s.where()));
        }
        boolean star = false;
        for (SqlSelect.Projection p : s.projections()) {
            if (p.expr() instanceof SqlExpr.Star) {
                star = true;
                continue;
            }
            String name = p.outputName();
            if (column.equals(name)) {
                return p.expr() instanceof SqlExpr.Column c ? c
                        : scalarInlineable(p.expr()) ? p.expr() : null;
            }
        }
        // A star projection (extend's `t0.*, expr AS x`) keeps every source
        // column visible; names not claimed by an explicit projection resolve
        // straight to the source.
        return star ? sourceColumn(s.from(), column,
                com.legend.sql.SqlTyping.whereNullRejections(s.where()))
                : null;
    }

    /**
     * The qualified column reference for {@code column} within a FROM source.
     * Single-alias sources resolve schema-blind (the alias qualifies any name
     * — Phase G already validated existence). A JOIN resolves by SIDE: the
     * side whose output schema claims the name qualifies it; join outputs are
     * disjoint by Phase-G typing (duplicate columns are a type error; prefix
     * joins rename). Null when no side claims the column.
     */
    /** {@link #sourceColumn} pinned to the DRIVING table (the join tree's
     * leftmost leaf — where the serialized class's own columns live): pk
     * spellings collide across joined tables, and the graph determinism
     * keys must never bind a navigation target's same-named column. */
    /** Whether a resolved column reference is PHYSICALLY renderable from
     * this source tree: its alias exists, and a VALUES source actually
     * carries the column (stamped outputs can be wider than the rendered
     * list — the pruned-demand seed shape). Best-effort consumers (the
     * graph pk determinism keys) drop references that fail this. */
    static boolean physicallyRenderable(SqlSource src, SqlExpr.Column c) {
        return switch (src) {
            case SqlSource.Dual d -> false;
            case SqlSource.Join j -> physicallyRenderable(j.left(), c)
                    || physicallyRenderable(j.right(), c);
            case SqlSource.VarSetPlaceholder vp -> false;
            case SqlSource.RawSql raw -> false;
            case SqlSource.Values v -> v.alias().equals(c.table())
                    && v.columns().contains(c.name());
            case SqlSource.Table t -> t.alias().equals(c.table());
            // A tabular function binds an alias exactly as a table
            // does; only its rendering differs.
            case SqlSource.TableFunction f -> f.alias().equals(c.table());
            case SqlSource.Subselect sub -> sub.alias().equals(c.table());
            case SqlSource.SourceUrl u -> u.alias().equals(c.table());
            case SqlSource.Pivot p -> p.alias().equals(c.table());
        };
    }

    /** §E3 M-N2 — JOIN-PAD FRAME AUTHORITY: a joined frame's born
     * outputs weaken the pad side's columns to nullable. The CHECKER
     * cannot do this — {@code join<T,Z>}'s result type is kind-blind
     * (the type parameters never see the JoinKind value), so the
     * upstream column multiplicities carry no pad truth; the SQL
     * frame's constructor is the one owner that holds both the kind
     * and the sides. Names match by the frame's OUTER spelling
     * (prefix renames applied to the padded right side); a name miss
     * merely keeps today's claim (under-weakening, never a lie). */
    /** PAIR-NATIVE minter: the contract slot for ONE typed result
     * column, from the very {@code Type.Column} the builder's loop
     * holds ({@code t} = the Lowerer's dialect-aware SQL type for it)
     * — outputsOf's scalar arm; a ROW-STRUCT column walls. */
    static com.legend.sql.OutputCol slot(Type.Column c,
            com.legend.sql.SqlType t) {
        if (c.type() instanceof Type.RelationType) {
            throw new IllegalStateException("row-struct column '"
                    + c.name() + "' has no single output slot");
        }
        return new com.legend.sql.OutputCol(c.name(), t,
                PureSql.nullable(c.multiplicity()), false);
    }

    /** The DECLARED slot for {@code name} out of a contract output
     * list — the attachment door for builders whose projections don't
     * line up positionally with the schema (star-headed extends,
     * prefix joins). Name-keyed at CONSTRUCTION against the declared
     * schema: spending builder knowledge, not consumption re-derivation.
     * A miss is a builder bug and walls loudly. */
    static com.legend.sql.OutputCol named(List<com.legend.sql.OutputCol> outs, String name) {
        for (com.legend.sql.OutputCol c : outs) {
            if (c.name().equals(name)) {
                return c;
            }
        }
        throw new IllegalStateException("declared output '" + name
                + "' missing from the contract list "
                + outs.stream().map(com.legend.sql.OutputCol::name).toList());
    }

    // stampJoinOrigins + starSideOrigin DELETED (SQL-IR slice 2,
    // ORIGIN_ARCHITECTURE_AUDIT mechanisms #4-#6): join-frame outputs
    // now BUILD from the projection list at construction — a star
    // projection inherits the starred source's outputs verbatim, an
    // explicit projection is the query's own declaration (DERIVED) —
    // so no construct-then-patch pass, name lookup, or kind-guess
    // fallback exists to lose the thread.

    // padJoinOutputs DELETED (slice-2 finish): the pad truth is
    // stamped at the source — SqlSource.Join.outputs() weakens its
    // padded sides — so no frame runs a name-keyed repair pass over
    // schema-asserted outputs.

    static SqlExpr.@com.legend.Nullable Column sourceColumnDriving(SqlSource src, String column) {
        if (src instanceof SqlSource.Join j) {
            SqlExpr.Column c = sourceColumnDriving(j.left(), column);
            // §E3 M-N2: a RIGHT/FULL join pads even the driving side
            return c != null && j.kind().padsLeft() ? c.asNullable() : c;
        }
        // a VALUES source renders exactly its column list — outputs can be
        // stamped wider (the pruned-demand seed shape); trust the physical
        if (src instanceof SqlSource.Values v && !v.columns().contains(column)) {
            return null;
        }
        return sourceColumn(src, column);
    }

    /** The row-wise-egress NULL-DROP (COMPILER_SHORTCUT_AUDIT §5): an
     * optional scalar cell landing NULL is a pure EMPTY and the value
     * collection holds no empties. Applied ONLY at COLLECTION-shape
     * roots (the Executor reads rows directly there); every other
     * consumer either null-skips natively (SQL aggregates) or compacts
     * its LIST carrier ({@code SqlExpr.CompactList}) — a WHERE at inner
     * seams perturbs the un-ORDER-BY'd row order order-sensitive
     * consumers ride (corpus witness:
     * testSubAggregationMultiLevelJoinString).
     *
     * <p>FOLD-IN per the fold policy (the sql package's own doctrine:
     * one SqlSelect extends through compatible clauses; a fresh nesting
     * level only when forced): when the projection select carries no
     * grouping/window/limit machinery, the condition ANDs into ITS
     * where-clause over the cell's own expression — {@code SELECT
     * t1.LASTNAME … WHERE t1.LASTNAME IS NOT NULL}, no wrapper. The
     * subselect wrap survives only for shapes where a WHERE is not
     * clause-equivalent (pre-aggregation vs post-aggregation, window
     * partitions, LIMIT). */
    static SqlSelect cellPresentFiltered(SqlSelect proj, String col,
            String sub) {
        if (proj.groupBy().isEmpty() && proj.having() == null
                && proj.qualify() == null && proj.limit() == null
                && proj.offset() == null && !proj.distinct()
                && proj.projections().size() == 1
                && whereSafe(proj.projections().get(0).expr())) {
            SqlExpr cond = SqlExpr.Call.of(SqlFn.IS_NOT_NULL,
                    proj.projections().get(0).expr());
            return proj.withWhere(proj.where() == null ? cond
                    : SqlExpr.Call.of(SqlFn.AND, proj.where(), cond));
        }
        return SqlSelect.starOf(new SqlSource.Subselect(proj, sub, null))
                .withWhere(SqlExpr.Call.of(SqlFn.IS_NOT_NULL,
                        proj.outputs().stream()
                                .filter(oc -> oc.name().equals(col))
                                .findFirst()
                                .map(oc -> SqlExpr.Column.of(sub, oc))
                                .orElseGet(() ->
                                        SqlExpr.Column.derived(sub, col))));
    }

    /** A projection expression that may be repeated in WHERE: no window
     * calls (partition semantics), no aggregate reducers (illegal in
     * WHERE), no subqueries (double evaluation of a correlated read). */
    private static boolean whereSafe(SqlExpr e) {
        if (e instanceof SqlExpr.WindowCall
                || e instanceof com.legend.sql.SqlAgg.Reducer
                || e instanceof SqlExpr.ScalarSubquery
                || e instanceof SqlExpr.Exists) {
            return false;
        }
        for (SqlExpr c : e.children()) {
            if (!whereSafe(c)) {
                return false;
            }
        }
        return true;
    }

    /** A per-row cell that CAN be empty: {@code [0..1]} stamped. */
    static boolean optionalScalarCell(
            com.legend.compiler.element.type.Multiplicity m) {
        return m instanceof com.legend.compiler.element.type
                        .Multiplicity.Bounded b
                && b.lower() == 0 && b.upper() != null && b.upper() <= 1;
    }

    /** A relation-rooted query at the statement root: a COLLECTION-shaped
     * root (single synthetic map column, optional cell, many stamp — the
     * ResultShape.COLLECTION classification) filters empty cells at
     * egress, because row-wise reads are the ONE carrier SQL does not
     * null-skip for us (audit §5; aggregates skip natively, LIST
     * collects compact via CompactList). Everything else rides through. */
    static SqlSelect collectionRootEgress(SqlSelect rel,
            com.legend.compiler.element.type.Type.RelationType rt,
            boolean many, java.util.function.Supplier<String> alias) {
        boolean mapChannel = rt.columns().size() == 1
                && rt.columns().get(0).name()
                        .startsWith(SqlSelect.SYNTH_MAP_COL);
        if (rt.columns().size() == 1 && many) {
            var col = rt.columns().get(0);
            if (mapChannel && optionalScalarCell(col.multiplicity())) {
                rel = cellPresentFiltered(rel, col.name(), alias.get());
            }
        }
        // the map-binder channel (single synthetic u_map__ column) IS a
        // VALUE COLLECTION (ResultShape's own rule) — the value lane
        // wearing a RelationType, so its wire cells conform. OUTERMOST
        // (after any filter wrap): the conformed carrier label must be
        // the FINAL output label or the wire census reads a label/wire
        // divergence. True TABULAR roots stay the raw lane.
        return mapChannel ? conformValueEgress(rel,
                LiteralSpelling.ValueLane.MAP_CHANNEL) : rel;
    }

    /** {@code SELECT UNNEST(a.col) AS out FROM (src) a} — the ONE
     * select-list row-explosion emission (carrier-purity ratchet): the
     * list always arrives as a LOCAL column of the wrapped source, never
     * as a raw expression (DuckDB rejects a correlated arg directly
     * under select-list UNNEST). */
    static SqlSelect unnestColumn(SqlSource src, String srcAlias,
            String col, String out, com.legend.sql.SqlType elemType) {
        SqlExpr listRef = src.outputs().stream()
                .filter(oc -> oc.name().equals(col)).findFirst()
                .map(oc -> (SqlExpr) SqlExpr.Column.of(srcAlias, oc))
                .orElseGet(() -> src instanceof SqlSource.Table
                        ? SqlExpr.Column.physical(srcAlias, col)
                        : SqlExpr.Column.derived(srcAlias, col));
        return SqlSelect.starOf(src)
                .withProjections(List.of(new SqlSelect.Projection(
                        SqlExpr.Call.of(SqlFn.UNNEST, listRef),
                        out, new OutputCol(out, elemType, true))));
    }

    /** Row explosion of a (possibly CORRELATED) list expression: a
     * one-row inner select carries the expr as local column "lst", the
     * outer select UNNESTs it — column "elem", one row per element. */
    static SqlSource lateralElem(SqlExpr list,
            com.legend.sql.SqlType elemType,
            String carryAlias, String outerAlias) {
        SqlSelect carry = new SqlSelect(
                List.of(new SqlSelect.Projection(list, "lst",
                        new OutputCol("lst",
                                new com.legend.sql.SqlType.Array(elemType),
                                true))),
                false, new SqlSource.Dual(), null, List.of(),
                null, null, List.of(), null, null, List.of());
        return new SqlSource.Subselect(
                unnestColumn(new SqlSource.Subselect(carry, carryAlias, null),
                        carryAlias, "lst", "elem", elemType),
                outerAlias, null);
    }

    /** A funcCol whose body is a SCALAR-STREAM COMBINATION (a
     * concatenate-rooted many-valued lambda) — the one project shape
     * that lowers to a LIST slot (CSV_DIFFERENTIAL mechanism 3).
     * Association to-many NAVIGATIONS type many too but route through
     * the join machinery and already explode as rows — the first draft
     * keyed on the typed multiplicity alone and broke 28 of them. */
    static boolean isManyScalarCol(
            com.legend.compiler.spec.typed.TypedFuncCol c) {
        List<com.legend.compiler.spec.typed.TypedSpec> body = c.fn().body();
        Type.FunctionType ft = c.fn().functionType();
        return ft.result().multiplicity().isMany()
                && Type.schemaView(ft.result().type()) == null
                && body.get(body.size() - 1)
                        instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                && "meta::pure::functions::collection::concatenate"
                        .equals(nc.callee().qualifiedName());
    }

    /** The column-collect fold over relation rows as the MAP it is
     * (Phase 1c; the recognizer is
     * {@code TypedFold.columnCollectBody}): {@code fold({e,a|
     * concatenate(elemExpr, $a)}, [])} = per-row elemExpr collection.
     * Null = not that shape. */
    static com.legend.compiler.spec.typed.@com.legend.Nullable TypedSpec
            columnCollectAsMap(com.legend.compiler.spec.typed.TypedFold f) {
        com.legend.compiler.spec.typed.TypedSpec body = f.columnCollectBody();
        if (body == null) {
            return null;
        }
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        var anyMany = new com.legend.compiler.element.type.ExprType(
                new com.legend.compiler.element.type.Type.ClassType(
                        com.legend.compiler.element.type.PlatformTypes.ANY),
                com.legend.compiler.element.type.Multiplicity.Bounded
                        .ZERO_MANY);
        var lam = new com.legend.compiler.spec.typed.TypedLambda(
                List.of(f.reducer().parameters().get(0)), List.of(body),
                com.legend.compiler.element.type.ExprType.one(
                        new com.legend.compiler.element.type.Type.FunctionType(
                                List.of(new com.legend.compiler.element.type
                                        .Type.Param(
                                                f.source().info().type(), one)),
                                new com.legend.compiler.element.type.Type.Param(
                                        anyMany.type(),
                                        anyMany.multiplicity()))));
        return new com.legend.compiler.spec.typed.TypedMap(
                f.source(), lam, anyMany);
    }

    /** The named column of a relation consumed by a SCALAR read — or,
     * over a LATE-BOUND raw grid (Phase 1c), the trust-name rule
     * ({@code Type.RelationType.trustedColumn}: the stamped source
     * resolves the name in SQL). */
    static com.legend.compiler.element.type.Type.RelationType.Column
            scalarReadColumn(com.legend.compiler.element.type.Type
                    .RelationType prt, String name) {
        return prt.columns().stream()
                .filter(x -> x.name().equals(name)).findFirst()
                .orElseGet(() -> {
                    if (prt.isLateBound()) {
                        return com.legend.compiler.element.type.Type
                                .RelationType.trustedColumn(name);
                    }
                    throw new com.legend.error.NotImplementedException(
                            "relation has no column '" + name
                                    + "' in scalar read");
                });
    }

    static SqlExpr.@com.legend.Nullable Column sourceColumn(SqlSource src, String column) {
        return sourceColumn(src, column, java.util.Set.of());
    }

    /** §E3-S WHERE≡INNER at the READ DOOR: {@code rejected} = the
     * enclosing select's null-rejected column names
     * ({@link com.legend.sql.SqlTyping#whereNullRejections}) — a pad
     * flip is suppressed when the WHERE rejects a column of the padded
     * side (no padded row survives), so read facts and the ctor's
     * frame-output arm agree. Callers without a WHERE in hand pass the
     * empty set and keep the unconditional flip (the safe side). */
    static SqlExpr.@com.legend.Nullable Column sourceColumn(SqlSource src,
            String column, java.util.Set<String> rejected) {
        // A quote-bearing pivot IDENTITY ('2011__|__newCol') strips to its
        // bare SQL name ONLY when the source does not claim the exact name —
        // a genuine column carrying that spelling (its own extend) wins.
        if (src.outputs().isEmpty() || !claims(src.outputs(), column)) {
            column = pivotIdentity(column);
        }
        // M2 leaf stamping (TYPED_SQL_IR.md §2): a claiming source has
        // the OutputCol in hand — the reference carries its declared
        // type. The LATE-BOUND arms (raw grids, pivots) stay UNKNOWN by
        // design: their schema genuinely does not exist at construction.
        return switch (src) {
            case SqlSource.Dual d -> null;
            case SqlSource.Table t -> stamped(t.alias(), t.outputs(), column);
            case SqlSource.TableFunction f ->
                    stamped(f.alias(), f.outputs(), column);
            case SqlSource.VarSetPlaceholder vp -> null;
            // LATE-BOUND grid (P3-2 single-query): an undemanded raw
            // grid skipped the schema probe, so its outputs are empty
            // BY DESIGN — the trust-name rule applies (pivot's
            // claim-any precedent): a by-name read claims its name and
            // the database resolves it. A STAMPED grid keeps the old
            // behavior (never claims — resolution rides its subselect).
            case SqlSource.RawSql raw -> raw.outputs().isEmpty()
                    ? SqlExpr.Column.physical(raw.alias(), column) : null;
            case SqlSource.Subselect sub -> lateBoundGrid(sub)
                    ? SqlExpr.Column.derived(sub.alias(), column)
                    : stamped(sub.alias(), sub.outputs(), column);
            case SqlSource.Values v -> stamped(v.alias(), v.outputs(), column);
            case SqlSource.SourceUrl u ->
                    stamped(u.alias(), u.outputs(), column);
            // Pivot outputs are DYNAMIC (one column per pivoted value) — the
            // static schema cannot enumerate their NAMES, so a pivot claims
            // any name. But every value column's TYPE is compile-time
            // knowable: it is its aggregate TEMPLATE's (§4bZ-U leg 1, the
            // demand-driven stamp — only the name is runtime).
            case SqlSource.Pivot p -> pivotColumn(p, column);
            case SqlSource.Join j -> {
                // §E3 M-N2 — JOIN-PAD PROVENANCE at the read door: a
                // column resolved from a NULL-padded side may be NULL
                // regardless of its DDL (the breach census's
                // outer-join-slot family). ON-clause reads never come
                // through here (the join-condition channel resolves
                // per side BEFORE the pad exists) — milestoning's
                // temporal conditions stay untouched. §E3-S: the flip
                // is suppressed when the enclosing WHERE null-rejects
                // the padded side (WHERE≡INNER — no padded row
                // survives the filter).
                SqlExpr.Column left = sourceColumn(j.left(), column,
                        rejected);
                SqlExpr.Column c = left != null ? left
                        : sourceColumn(j.right(), column, rejected);
                if (c == null) {
                    yield null;
                }
                SqlSource winner = left != null ? j.left() : j.right();
                boolean pad = left != null ? j.kind().padsLeft()
                        : j.kind().padsRight();
                yield pad && !sideRejected(winner, rejected)
                        ? c.asNullable() : c;
            }
        };
    }

    /** Does the WHERE null-reject any column of this side's subtree
     * (its outputs' names — the pad is then vacuous)? */
    private static boolean sideRejected(SqlSource side,
            java.util.Set<String> rejected) {
        if (rejected.isEmpty()) {
            return false;
        }
        for (OutputCol c : side.outputs()) {
            if (rejected.contains(c.name())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Schema-AWARE claim check: a source claims only columns its stamped
     * outputs actually contain — load-bearing for CORRELATED scopes, where an
     * unclaimed name must fall through to the enclosing lambda instead of
     * being blindly alias-qualified. The real pipeline stamps outputs on
     * every source; an UNSTAMPED source is a construction bug and fails
     * loudly rather than silently claiming everything.
     */
    /** P3-2 SINGLE-QUERY: a source is a LATE-BOUND grid frame when its
     * (empty) outputs trace to a raw-SQL leaf that skipped the schema
     * probe. Empty outputs anywhere ELSE remain {@code claims}' loud
     * construction wall — this predicate is the only exemption. */
    private static boolean lateBoundGrid(com.legend.sql.SqlSource src) {
        return switch (src) {
            case com.legend.sql.SqlSource.RawSql raw ->
                    raw.outputs().isEmpty();
            case com.legend.sql.SqlSource.Subselect sub ->
                    sub.outputs().isEmpty()
                            && sub.inner() instanceof
                                    com.legend.sql.SqlSelect ss
                            && lateBoundGrid(ss.from());
            default -> false;
        };
    }

    /** THE DEMAND-DRIVEN PIVOT STAMP (§4bZ-U leg 1): a value column's
     * name is data-derived ({@code <value>__|__<template>}) but its
     * TYPE is its aggregate template's — the lowering-typed slot
     * {@code Pivots.lower} put on the {@link SqlSource.Pivot.Using} —
     * so the read stamps that type and only the NAME stays runtime.
     * Statically declared columns (group keys) stamp from outputs as
     * before; a separator-free miss stays plain (UNKNOWN); a suffix
     * matching no template stays plain too — the census keeps counting
     * it, never a guess. */
    private static SqlExpr.Column pivotColumn(SqlSource.Pivot p,
            String column) {
        for (OutputCol oc : p.outputs()) {
            if (oc.name().equals(column)) {
                return SqlExpr.Column.of(p.alias(), oc);
            }
        }
        int sep = column.lastIndexOf(Type.RelationType.PIVOT_SEPARATOR);
        if (sep >= 0) {
            String template = column.substring(
                    sep + Type.RelationType.PIVOT_SEPARATOR.length());
            for (SqlSource.Pivot.Using u : p.usings()) {
                if (!u.alias().equals(template)) {
                    continue;
                }
                // the stamp speaks THE EMISSION — the Reducer's own
                // stored promotion fact (SUM widens to HUGEINT,
                // decimal SUM to Decimal(38,s) — exactly what grouped
                // aggregates deliver, lane-green on both backends).
                // Using.type stays the MODEL channel (PctTdsWrap's TDS
                // headers); stamping it here instead re-ran the
                // CEILING rule-vs-emission mistake: 3 wire rows
                // diverged DOUBLE <> DECIMAL(38,2) on first measure.
                if (u.agg().type() instanceof
                        com.legend.sql.TypeFact.Typed t) {
                    // the fact transports WHOLE — the engine-compat
                    // tolerance included (§5 traps: provenance flags
                    // ride every rebuild site)
                    return new SqlExpr.Column(p.alias(), column, t,
                            com.legend.sql.OutputCol.Origin.DERIVED);
                }
                if (u.type() != null) {
                    // §E3: model-channel fallback — the aggregate's
                    // own fact is not in hand here, and a reducer
                    // output may be NULL (empty groups); safe side
                    return SqlExpr.Column.of(p.alias(), column,
                            u.type(), true, com.legend.sql.OutputCol.Origin.DERIVED);
                }
            }
        }
        return SqlExpr.Column.derived(p.alias(), column);
    }

    private static boolean claims(List<OutputCol> outputs, String column) {
        if (outputs.isEmpty()) {
            throw new IllegalStateException(
                    "source has no stamped output schema — cannot resolve column '"
                            + column + "' (stamp outputs at construction)");
        }
        return outputs.stream().anyMatch(c -> c.name().equals(column));
    }

    /** The claimed column as a STAMPED reference, or null (name not
     * claimed). Routes through {@link #claims} — ONE owner of the
     * empty-outputs wall. */
    private static SqlExpr.@com.legend.Nullable Column stamped(String alias,
            List<OutputCol> outputs, String column) {
        if (!claims(outputs, column)) {
            return null;
        }
        String want = column;
        return outputs.stream().filter(c -> c.name().equals(want))
                .findFirst().map(oc -> SqlExpr.Column.of(alias, oc))
                .orElse(null);
    }

    /** One-line structural sketch of a join side for the unknown-column
     * wall — table/alias per leaf, projection names for subselects. */
    static String describeSource(SqlSource s) {
        return switch (s) {
            case SqlSource.Join j -> "join(" + describeSource(j.left()) + ", "
                    + describeSource(j.right()) + ")";
            case SqlSource.Subselect sub -> "subselect:" + sub.alias() + "("
                    + (sub.inner() instanceof SqlSelect ss
                            ? ss.projections().stream()
                                    .map(SqlSelect.Projection::alias)
                                    .collect(java.util.stream.Collectors
                                            .joining(","))
                            : "union") + ")";
            default -> s.getClass().getSimpleName() + ":" + s.alias() + "("
                    + s.outputs().stream().map(o -> o.name())
                            .collect(java.util.stream.Collectors.joining(","))
                    + ")";
        };
    }

    /** A DATETIME-family serialize leaf renders the engine's ISO wire
     * form ({@code 2015-08-26T00:00:00.000000000} — 'T' separator +
     * 9-digit nanos; DuckDB's raw json timestamp text is
     * space-separated). Abstract Date formats by the PHYSICAL value's
     * precision, the engine's JDBC value-class dispatch (java.sql.Date
     * -> bare day, Timestamp -> full instant): setup DDL can diverge
     * from the store declaration, so the dispatch must be runtime
     * ({@code typeof}). NULL propagates (CONCAT would swallow it). */
    /** VALUE-LANE wire-cell egress conformance (disagree-9 burn): the
     * finished value-shaped root's cells conform to the engine's own
     * decode for this lane — TIMESTAMP cells at nine subsecond digits
     * (TEMPORAL_TEXT), column-rooted DECIMAL cells scale-canonical
     * (DECIMAL_TEXT) — see {@link LiteralSpelling#wireValueEgress}.
     * Applied ONLY at value roots (scalar / value-collection):
     * TDS/relation roots are the raw lane and keep driver spellings,
     * and the conformance changes the VALUE both production verdict
     * channels observe, so the host lattice and the byte canon stay
     * in agreement. */
    /** Per-lane scope: {@link LiteralSpelling.ValueLane}. */
    static SqlSelect conformValueEgress(SqlSelect s,
            LiteralSpelling.ValueLane lane) {
        java.util.List<SqlSelect.Projection> out = null;
        for (int i = 0; i < s.projections().size(); i++) {
            SqlSelect.Projection p = s.projections().get(i);
            com.legend.sql.OutputCol col = p.out();
            if (col == null) {
                continue;
            }
            SqlExpr.Cast conformed = LiteralSpelling.wireValueEgress(
                    p.expr(), col.type(), lane);
            if (conformed == null) {
                continue;
            }
            if (out == null) {
                out = new java.util.ArrayList<>(s.projections());
            }
            out.set(i, new SqlSelect.Projection(conformed, p.outputName(),
                    new com.legend.sql.OutputCol(col.name(),
                            conformed.target(), col.nullable(),
                            col.tolerated(), col.origin())));
        }
        return out == null ? s : s.withProjections(out);
    }

    static SqlExpr jsonDateWrap(SqlExpr e,
            com.legend.compiler.element.type.Type t) {
        if (t != com.legend.compiler.element.type.Type.Primitive.DATE_TIME
                && t != com.legend.compiler.element.type.Type.Primitive.DATE) {
            return e;
        }
        // ONE nine-digit spelling owner (disagree-9 audit): ISO_NANO —
        // the same convention the value-egress conformance emits
        SqlExpr iso = SqlExpr.Call.of(SqlFn.STRFTIME, e,
                new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_NANO));
        List<SqlExpr.Case.When> arms = new java.util.ArrayList<>();
        arms.add(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.IS_NULL, e), new SqlExpr.NullLit()));
        if (t == com.legend.compiler.element.type.Type.Primitive.DATE) {
            arms.add(new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.EQUAL,
                            SqlExpr.Call.of(SqlFn.TYPEOF, e),
                            new SqlExpr.StringLit("DATE")),
                    SqlExpr.Call.of(SqlFn.STRFTIME, e,
                            new SqlExpr.FormatLit(com.legend.sql.DateFmt.DATE))));
        }
        return new SqlExpr.Case(arms, iso);
    }


    /** The serialize leaf's DECLARED result type (the lambda's
     * FunctionType result — the resolver stamps the MODEL property type
     * there; the body's own info is column-typed). */
    static com.legend.compiler.element.type.Type leafResultType(
            com.legend.compiler.spec.typed.TypedFuncCol leaf) {
        return leaf.fn().functionType().result().type();
    }



    /** A TDS cell's print text (the Lowerer's makeString-over-row-cells
     * arm): booleans ride the semantic node — the reference prints
     * 'true', H2's VARCHAR cast would print 'TRUE'; everything else is
     * the VARCHAR cast. */
    static SqlExpr cellText(com.legend.compiler.element.type.Type t,
            SqlExpr v) {
        return t == com.legend.compiler.element.type.Type.Primitive.BOOLEAN
                ? SqlExpr.Call.of(SqlFn.BOOL_TO_TEXT, v)
                : new SqlExpr.Cast(v, com.legend.sql.SqlType.Scalar.VARCHAR);
    }

    /** The whole-row {@code distinct()} directly under a restrict (not the
     * TDS-union desugar, which keeps its own UNION form), else null. */
    static com.legend.compiler.spec.typed.@com.legend.Nullable TypedDistinct restrictOverWholeRowDistinct(
            com.legend.compiler.spec.typed.TypedSelect sel) {
        if (!(sel.source() instanceof com.legend.compiler.spec.typed.TypedDistinct d)
                || d.source() instanceof com.legend.compiler.spec.typed.TypedConcatenate) {
            return null;
        }
        boolean wholeRow = d.columns().isEmpty()
                || d.columns().equals(com.legend.compiler.element.type.Type.requireRelationSchema(
                        d.source().info().type()).columns().stream()
                        .map(com.legend.compiler.element.type.Type.Column::name).toList());
        // AGGREGATION only: the engine drops a groupBy's unused aggregates
        // under the distinct (tdsRestrict …EleminatesUnnecessaryAggsWith
        // Distinct) but keeps a plain project's columns inside it
        // (…LowerProjectColsNotEliminatedWithDistinct: the const 'Hello'
        // survives in the distinct select) — sorts in between are order only
        com.legend.compiler.spec.typed.TypedSpec below = d.source();
        while (below instanceof com.legend.compiler.spec.typed.TypedSortBy || below instanceof com.legend.compiler.spec.typed.TypedSort) {
            below = below instanceof com.legend.compiler.spec.typed.TypedSortBy sb ? sb.source()
                    : ((com.legend.compiler.spec.typed.TypedSort) below).source();
        }
        return wholeRow && below instanceof com.legend.compiler.spec.typed.TypedGroupBy ? d : null;
    }

    /** All columns resolved against {@code base}, or null if any misses
     * (moved from Lowerer — this is Fold's own resolveInto vocabulary). */
    static @com.legend.Nullable List<SqlSelect.Projection> tryProjectAll(
            SqlSelect base, List<String> columns, List<OutputCol> contract) {
        List<SqlSelect.Projection> ps = new ArrayList<>(columns.size());
        for (String c : columns) {
            SqlExpr e = Fold.resolveInto(base, c);
            if (e == null) {
                // PROJECTION position may inline a COMPUTED projection
                // (window calls included): the caller REPLACES the whole
                // projection list, so this is a narrowing/reorder of the
                // same select — never a recomputation in a filtering
                // position (the restrict-over-window-cols corpus pin;
                // resolveInto's computed-decline serves the WHERE sites).
                for (SqlSelect.Projection p : base.projections()) {
                    if (c.equals(p.outputName())) {
                        e = p.expr();
                        break;
                    }
                }
            }
            if (e == null) {
                return null;
            }
            // self-aliased reads drop the alias — EXCEPT reads of a
            // union frame's outputs, which keep it (tds union goldens:
            // "unionalias_0"."lhs_lastName" as "lhs_lastName")
            boolean unionRead = base.from() instanceof SqlSource.Subselect sub
                    && "unionAlias".equals(sub.frameName())
                    && e instanceof SqlExpr.Column uc
                    && sub.alias().equals(uc.table());
            ps.add(new SqlSelect.Projection(e,
                    !unionRead && e instanceof SqlExpr.Column col
                            && col.name().equals(c) ? null : c,
                    named(contract, c)));
        }
        return ps;
    }
}
