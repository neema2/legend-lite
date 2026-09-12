// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;


import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;

import java.util.List;

/**
 * The relation-level predicate family (EXISTS forms) — size / exists /
 * forAll / isEmpty / isNotEmpty over RELATION-valued arguments (extracted
 * registry; map §2 positional rules, engine processEmpty L4441 and
 * buildExistsPredicate L6278).
 */
final class RelationPredicates {

    private RelationPredicates() {
    }

    /** Rows-level ->toOne() over a relation ($r.values.rows->toOne(),
     * the corpus's single-ROW claim): row-identical to the relation
     * itself. The exactly-one contract is enforced where the value
     * is CONSUMED (the executor's scalar second-row guard, audit
     * 21b F10) — engine toOne throws at the reader, never in SQL.
     * ANY 1-arg toOne in relation position looks through — the
     * POSITION is the contract (a class-typed nav arg arrives
     * here after resolution; the inner dispatch stays loud when
     * the arg is genuinely not relation-lowerable). */
    static boolean isRelationToOne(TypedNativeCall nc) {
        return com.legend.builtin.Pure.isToOneCall(nc.callee().qualifiedName())
                && nc.args().size() == 1;
    }

    /** A relation-position IDENTITY: the to-one look-through above, or
     * the union-scan marker ({@code Pure.Lite.UNION_SCAN} — a structural
     * fact for the resolver, nothing for the database). */
    static boolean isRelationIdentity(TypedNativeCall nc) {
        return isRelationToOne(nc)
                || (com.legend.builtin.NativeFn.LiteDesugar.of(nc.callee().qualifiedName()).orElse(null) == com.legend.builtin.NativeFn.LiteDesugar.UNION_SCAN
                        && nc.args().size() == 1);
    }

    /** Does {@code s} read any column whose alias is not bound in an
     * ENCLOSING scope within it (an OUTER-row correlation)? Each nested
     * select extends the visible-alias set with its own FROM aliases —
     * a subquery reading its own tables is not "correlated" (the first
     * cut missed this and over-counted 204 whole-extent counts as
     * correlated). Lenient on SQL's derived-table visibility rules —
     * we only ask whether the whole tree references anything OUTSIDE
     * itself. */
    static boolean referencesOuter(SqlSelect s) {
        return readsUnbound(s, java.util.Set.of());
    }

    private static boolean readsUnbound(com.legend.sql.SqlQuery q,
            java.util.Set<String> outer) {
        if (q instanceof com.legend.sql.SqlUnion u) {
            return u.branches().stream().anyMatch(b -> readsUnbound(b, outer));
        }
        SqlSelect sel = (SqlSelect) q;
        java.util.Set<String> scope = new java.util.HashSet<>(outer);
        scopeAliases(sel.from(), scope);
        if (sourceUnbound(sel.from(), scope)) {
            return true;
        }
        java.util.List<SqlExpr> roots = new java.util.ArrayList<>();
        sel.projections().forEach(p -> roots.add(p.expr()));
        if (sel.where() != null) {
            roots.add(sel.where());
        }
        roots.addAll(sel.groupBy());
        if (sel.having() != null) {
            roots.add(sel.having());
        }
        sel.orderBy().forEach(k -> roots.add(k.expr()));
        return roots.stream().anyMatch(e -> exprUnbound(e, scope));
    }

    /** The aliases a select's FROM tree binds — the frame scope
     * (§4AD batch-6 tail: GraphAggDecorrelate splits correlation keys
     * against it). */
    static java.util.Set<String> frameAliases(SqlSelect sel) {
        java.util.Set<String> scope = new java.util.HashSet<>();
        scopeAliases(sel.from(), scope);
        return scope;
    }

    /** The aliases THIS select's FROM tree binds (join members' aliases;
     * never descends into subselect inners — those are their own scope). */
    private static void scopeAliases(com.legend.sql.SqlSource src,
            java.util.Set<String> scope) {
        if (src instanceof com.legend.sql.SqlSource.Join j) {
            scopeAliases(j.left(), scope);
            scopeAliases(j.right(), scope);
            return;
        }
        if (src.alias() != null) {
            scope.add(src.alias());
        }
    }

    /** Unbound reads inside the FROM tree: join ON conditions (this
     * scope) and subselect inners (their own nested scope). */
    private static boolean sourceUnbound(com.legend.sql.SqlSource src,
            java.util.Set<String> scope) {
        if (src instanceof com.legend.sql.SqlSource.Join j) {
            return (j.on() != null && exprUnbound(j.on(), scope))
                    || sourceUnbound(j.left(), scope)
                    || sourceUnbound(j.right(), scope);
        }
        if (src instanceof com.legend.sql.SqlSource.Subselect sub) {
            return readsUnbound(sub.inner(), scope);
        }
        return false;
    }

    private static boolean exprUnbound(SqlExpr e,
            java.util.Set<String> scope) {
        if (e instanceof SqlExpr.Column c) {
            return c.table() != null && !scope.contains(c.table());
        }
        if (e instanceof SqlExpr.ScalarSubquery sq) {
            return readsUnbound(sq.subquery(), scope);
        }
        if (e instanceof SqlExpr.Exists ex) {
            return readsUnbound(ex.subquery(), scope);
        }
        if (e instanceof SqlExpr.InSubquery i) {
            return exprUnbound(i.value(), scope) || readsUnbound(i.subquery(), scope);
        }
        if (e instanceof SqlExpr.Quantified q) {
            return exprUnbound(q.value(), scope) || readsUnbound(q.subquery(), scope);
        }
        for (SqlExpr c : e.children()) {
            if (exprUnbound(c, scope)) {
                return true;
            }
        }
        return false;
    }

    /** Whether {@code n} is a relation-level predicate (the Lowerer's scalar
     *  dispatch): a collection native over a RELATION first argument (a bare
     *  variable is a per-element cell, never a subquery), or the quantification
     *  family, whose searched relation is the SECOND argument. */
    static boolean applies(TypedNativeCall n) {
        if (n.args().isEmpty()) {
            return false;
        }
        if (com.legend.builtin.NativeFn.RelationQuantifier.of(n.callee().qualifiedName()).isPresent()) {
            return n.args().size() == 2 && Type.relationValued(n.args().get(
                    n.callee().qualifiedName().endsWith("::exists") ? 0 : 1).info());
        }
        return Type.relationValued(n.args().get(0).info())
                && !(n.args().get(0) instanceof com.legend.compiler.spec.typed.TypedVariable)
                && of(n) != null;
    }

    /** The quantification family (NativeFn.RelationQuantifier), emitted the
     *  engine's way: {@code exists} → EXISTS (SELECT 1 … WHERE p); {@code in} →
     *  value IN (searched); a quantified comparison → value op ANY|ALL (searched)
     *  — each under {@code value IS NOT NULL AND}, and the searched single
     *  column's nulls dropped INSIDE the subquery, which makes the predicate
     *  two-valued, the Pure bodies' answer (processRelationQuantifiedComparison:
     *  "`false and unknown` is false … which is what makes the negation exact").
     *  A subquery whose row set is already fixed (limit / offset / group /
     *  distinct) is isolated first, so the null drop sits outside it
     *  (excludeNullsFromQuantifiedSubSelect). */
    private static Lowerer.RelationPredicate quantifier(
            com.legend.builtin.NativeFn.RelationQuantifier q) {
        return switch (q) {
            case EXISTS -> (lw, call) -> new SqlExpr.Exists(Lowerer.select1(
                    lw.whereLambda(call.args().get(0), call.args().get(1), false)));
            case IN -> searched((v, sub) -> new SqlExpr.InSubquery(v, sub));
            case EQUAL_ANY -> quantified(SqlFn.EQUAL, SqlExpr.Quantifier.ANY);
            case EQUAL_ALL -> quantified(SqlFn.EQUAL, SqlExpr.Quantifier.ALL);
            case GREATER_THAN_ANY -> quantified(SqlFn.GREATER, SqlExpr.Quantifier.ANY);
            case GREATER_THAN_ALL -> quantified(SqlFn.GREATER, SqlExpr.Quantifier.ALL);
            case GREATER_THAN_EQUAL_ANY -> quantified(SqlFn.GREATER_EQUAL, SqlExpr.Quantifier.ANY);
            case GREATER_THAN_EQUAL_ALL -> quantified(SqlFn.GREATER_EQUAL, SqlExpr.Quantifier.ALL);
            case LESS_THAN_ANY -> quantified(SqlFn.LESS, SqlExpr.Quantifier.ANY);
            case LESS_THAN_ALL -> quantified(SqlFn.LESS, SqlExpr.Quantifier.ALL);
            case LESS_THAN_EQUAL_ANY -> quantified(SqlFn.LESS_EQUAL, SqlExpr.Quantifier.ANY);
            case LESS_THAN_EQUAL_ALL -> quantified(SqlFn.LESS_EQUAL, SqlExpr.Quantifier.ALL);
        };
    }

    private static Lowerer.RelationPredicate quantified(SqlFn comparison, SqlExpr.Quantifier quantifier) {
        return searched((v, sub) -> new SqlExpr.Quantified(v, comparison, quantifier, sub));
    }

    private static Lowerer.RelationPredicate searched(
            java.util.function.BiFunction<SqlExpr, com.legend.sql.SqlQuery, SqlExpr> form) {
        return (lw, call) -> {
            SqlExpr value = lw.enclosingScalar(call.args().get(0));
            SqlExpr pred = form.apply(value, searchedColumn(lw, call.args().get(1)));
            return new SqlExpr.Group(SqlExpr.Call.of(SqlFn.AND,
                    SqlExpr.Call.of(SqlFn.IS_NOT_NULL, value), pred));
        };
    }

    /** The searched relation as a single-column subquery with its nulls dropped. */
    private static SqlSelect searchedColumn(Lowerer lw, com.legend.compiler.spec.typed.TypedSpec rel) {
        SqlSelect src = lw.relation(rel);
        boolean rowSetFixed = src.limit() != null || src.offset() != null
                || !src.groupBy().isEmpty() || src.distinct()
                || src.having() != null || src.qualify() != null;
        SqlSelect base = rowSetFixed ? lw.isolate(src) : src;
        if (!(Type.relationSchema(rel.info().type()) instanceof Type.RelationType rt)
                || rt.columns().size() != 1) {
            throw new IllegalStateException("a quantified comparison expects a single-column relation");
        }
        SqlExpr col = base.projections().size() == 1
                && !(base.projections().get(0).expr() instanceof SqlExpr.Star)
                ? base.projections().get(0).expr()
                : java.util.Objects.requireNonNull(
                        Fold.sourceColumn(base.from(), rt.columns().get(0).name()),
                        "searched column not found: " + rt.columns().get(0).name());
        SqlExpr notNull = SqlExpr.Call.of(SqlFn.IS_NOT_NULL, col);
        return base.withWhere(base.where() == null ? notNull
                        : SqlExpr.Call.of(SqlFn.AND, base.where(), notNull))
                .withProjections(List.of(new SqlSelect.Projection(col, null, null)));
    }

    static Lowerer.@com.legend.Nullable RelationPredicate of(TypedNativeCall n) {
        var quantifier = com.legend.builtin.NativeFn.RelationQuantifier.of(n.callee().qualifiedName());
        if (quantifier.isPresent()) {
            return quantifier(quantifier.get());
        }
        // count over a RELATION argument is size (row count) — the graph-
        // leaf sub-aggregation emission rewrites nav-slot reads to their
        // correlated target relation and counts them (H4b)
        if (Lowerer.isFamily(n, "size") || Lowerer.isFamily(n, "count")) {
            // NOTE (audit 22b F1 residual): size over a RELATION value
            // counts ROWS regardless of the value's [1] multiplicity (one
            // relation != one row — a value-mult constant fold here broke
            // three correlated-count pins). rows->toOne()->size() therefore
            // still answers the row count when toOne sits mid-expression;
            // the exactly-one contract is reader-enforced at toOne ROOTS
            // only. Documented residual, not silently folded.
            // COUNT(*) is a zero-key aggregation: a grouped/deduped/truncated
            // source must count from OUTSIDE (COUNT(*) per group is a row per
            // group — a multi-row scalar subquery).
            return (lw, call) -> {
                SqlSelect src = lw.relation(call.args().get(0));
                SqlSelect base = Fold.groupByFolds(src) && !Fold.unnestInProjections(src)
                        ? src : lw.isolate(src);
                // engine processRowCount (pureToSQLQuery.pure:8985): a
                // SINGLE projected column counts NULL-SKIPPING — count(col);
                // anything else is the bare row count.
                List<SqlSelect.Projection> ps = base.projections();
                SqlAgg.Reducer counter = ps.size() == 1
                        && !(ps.get(0).expr() instanceof SqlExpr.Star)
                        ? new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(ps.get(0).expr()),
                                false, java.util.List.of())
                        : SqlAgg.Reducer.of(SqlAgg.Fn.COUNT);
                // §4AD census, batch-6 SPLIT: a genuinely CORRELATED
                // scalar COUNT subquery (outer-row refs — the banned
                // navigation class) vs a WHOLE-EXTENT count (no outer
                // refs — a plain scalar envelope, row- and shape-benign)
                return new SqlExpr.ScalarSubquery(base
                        .withProjections(List.of(new SqlSelect.Projection(
                                counter, null, null))));
            };
        }
        // GENERAL reducer over a single-scalar-column RELATION argument
        // (the graph derived-leaf sub-aggregation: average($this.employees
        // .age) — engine renders a correlated scalar aggregate subquery)
        com.legend.sql.SqlAgg.Fn fam = Aggregates.reducerOrNull(n.callee());
        if (fam != null && fam != com.legend.sql.SqlAgg.Fn.COUNT && fam != com.legend.sql.SqlAgg.Fn.ANY_VALUE
                && n.args().size() == 1
                && Type.relationSchema(n.args().get(0).info().type())
                        instanceof Type.RelationType rt2
                && rt2.columns().size() == 1
                && !(rt2.columns().get(0).type() instanceof Type.ClassType)) {
            return (lw, call) -> {
                SqlSelect src = lw.relation(call.args().get(0));
                SqlSelect base = Fold.groupByFolds(src)
                        && !Fold.unnestInProjections(src)
                        ? src : lw.isolate(src);
                SqlExpr col = base.projections().size() == 1
                        && !(base.projections().get(0).expr()
                                instanceof SqlExpr.Star)
                        ? base.projections().get(0).expr()
                        : Fold.sourceColumn(base.from(),
                                rt2.columns().get(0).name());
                // §4AD census, batch-6 split (see the COUNT arm)
                return new SqlExpr.ScalarSubquery(base.withProjections(
                        List.of(new SqlSelect.Projection(
                                new SqlAgg.Reducer(fam, List.of(col), false,
                                        List.of()), null, null))));
            };
        }
        if ((Lowerer.isFamily(n, "isEmpty") || Lowerer.isFamily(n, "isNotEmpty"))
                && n.args().size() == 1
                && Type.relationSchema(n.args().get(0).info().type())
                        instanceof Type.RelationType rt0) {
            // An OPTIONAL-VALUE read encoded as a relation (single scalar
            // column, [0..1] stamp — the filtered-nav leaf): emptiness is
            // the VALUE's NULL-ness, not row-set existence (engine
            // processIsEmpty scalar arm; a row with a NULL leaf IS empty).
            // Returning null routes through the scalar bridge + IS NULL.
            if (rt0.columns().size() == 1
                    && !(rt0.columns().get(0).type()
                            instanceof Type.ClassType)
                    && n.args().get(0).info().multiplicity()
                            instanceof com.legend.compiler.element.type
                                    .Multiplicity.Bounded b0
                    && b0.isToOne() && b0.lower() == 0) {
                return null;
            }
            // EXISTS over the relation (map §2 rule; engine processEmpty
            // Class-arm L4441) — never a serialized graph or list carrier.
            boolean isNot = Lowerer.isFamily(n, "isNotEmpty");
            return (lw, call) -> {
                SqlExpr ex = new SqlExpr.Exists(lw.relation(call.args().get(0)));
                return isNot ? ex : SqlExpr.Call.of(SqlFn.NOT, ex);
            };
        }
        if (Lowerer.isFamily(n, "exists")) {
            return (lw, call) -> new SqlExpr.Exists(Lowerer.select1(
                    lw.whereLambda(call.args().get(0), call.args().get(1), false)));
        }
        if (Lowerer.isFamily(n, "forAll")) {
            return (lw, call) -> SqlExpr.Call.of(SqlFn.NOT, new SqlExpr.Exists(Lowerer.select1(
                    lw.whereLambda(call.args().get(0), call.args().get(1), true))));
        }
        if (Lowerer.isFamily(n, "isEmpty")) {
            return (lw, call) -> SqlExpr.Call.of(SqlFn.NOT,
                    new SqlExpr.Exists(Lowerer.select1(lw.relation(call.args().get(0)))));
        }
        if (Lowerer.isFamily(n, "isNotEmpty")) {
            return (lw, call) -> new SqlExpr.Exists(Lowerer.select1(lw.relation(call.args().get(0))));
        }
        return null;
    }
}
