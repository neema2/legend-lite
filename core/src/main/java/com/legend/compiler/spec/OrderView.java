// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * THE ORDER VIEW of a side (D3, batch-2 slice 2; moved out of the verdict seam
 * 2026-09-21 — typed-tree navigation, the compiler layer's): SORTED (ends in a
 * sort through order-preserving tails — the engine contract pins the order),
 * INCIDENTAL (bottoms at a store source or an execution-frame read with no sort —
 * SQL arrival order, engine goldens encode H2's), DEFINED (pure values — the
 * language's own order). Nothing is evaluated.
 *
 * <p>WHERE THE RULE DOES NOT KNOW, IT DOES NOT SAY "ORDERED" (the edition program's step 6, its closing batch; the
 * user, 2026-11-27: "maybe okay if sometimes says \"use hasSameElements\" when it doesn't know"). A function or a
 * node the rule does not name is as ordered as the least ordered of what it was applied to ({@link #leastOrdered}):
 * store rows passed through {@code removeDuplicates}, {@code reverse}, {@code tail}, an {@code if} of two reads are
 * in arrival order still. Until then every unnamed function answered DEFINED without reading its input -- loud
 * enough for a test (a strict assert over rows whose order varies fails, and the lists below grew that way), but a
 * query's {@code ==} over such a side was answered quietly, in the order the rows arrived. A function found to be
 * refused wrongly is NAMED below, with its test.
 */
public enum OrderView {
    /** A side's order semantics: SORTED (ends in a sort through
     * order-preserving tails — the engine contract pins the order),
     * INCIDENTAL (bottoms at a store source or an execution-frame
     * read with no sort — SQL arrival order, engine goldens encode
     * H2's), DEFINED (pure values — the language's own order). */
    SORTED, INCIDENTAL, DEFINED;

    public static final java.util.Set<String> SORT_FQNS = java.util.Set.of(
            "meta::pure::functions::collection::sort",
            "meta::pure::functions::collection::sortBy",
            "meta::pure::functions::collection::sortByReversed",
            "meta::pure::functions::relation::sort",
            // the legacy table's sort (named 2026-11-27: until the fallback read its input it answered DEFINED,
            // which a sorted table is)
            "meta::pure::tds::sort");

    /** Natives after which SQL leaves no order (GROUP BY, joins, UNION,
     * pivots): the chain's order is incidental past them. EXACT identities
     * read from the signature catalog ({@link com.legend.builtin.Pure}: one
     * overload names each FQN; audit §4y deleted the simple-name suffix
     * match of the harness's audited list, audit 23 D1). */
    public static final java.util.Set<String> ORDER_DESTROYING = java.util.Set.of(
            com.legend.builtin.Pure.GROUP_BY__X_MANY__FUNCTION_1.qualifiedName(),
            com.legend.builtin.Pure.GROUP_BY__RELATION_1__COL_SPEC_1__AGG_COL_SPEC_1.qualifiedName(),
            com.legend.builtin.Pure.GROUP_BY__K_MANY__FUNCTION_MANY__AGGREGATE_VALUE_MANY__STRING_MANY
                    .qualifiedName(),
            com.legend.builtin.Pure.JOIN__RELATION_1__RELATION_1__JOIN_KIND_1__FUNCTION_1.qualifiedName(),
            // a RELATION's concatenate (SQL's UNION ALL leaves no order); a collection's concatenate and union keep the
            // language's order -- the first's elements, then the second's (union its first occurrences) -- so a side
            // that reads them is compared in order, as upstream compares it (2026-11-04, the asserts-as-plan-nodes
            // program: assertEquals([3, 1, 2], [1, 2]->concatenate([3])) passed as a multiset where upstream fails it)
            com.legend.builtin.Pure.CONCATENATE__RELATION_1__RELATION_1.qualifiedName(),
            com.legend.builtin.Pure.PIVOT__RELATION_1__COL_SPEC_1__AGG_COL_SPEC_1.qualifiedName());
    // (a relation's aggregate with no grouping: left ORDER_DESTROYING 2026-11-24, B3 step 4 -- its result is one row,
    // whose order is defined; PCT's aggregate tests were refused as order-dependent on its strength)

    /** A collection's concatenate and union (2026-11-04, the asserts-as-plan-nodes program): the language's order --
     *  the first's elements, then the second's; union's first occurrences -- over its inputs', each read. */
    public static final java.util.Set<String> ORDER_COMBINING = java.util.Set.of(
            com.legend.builtin.Pure.CONCATENATE__T_MANY__T_MANY.qualifiedName(),
            com.legend.builtin.Pure.UNION__T_MANY__T_MANY.qualifiedName());

    /** Order-preserving native tails, EXACT identities from the catalog. */
    public static final java.util.Set<String> ORDER_PRESERVING = java.util.Set.of(
            com.legend.builtin.Pure.MAP__T_MANY__FUNCTION_1.qualifiedName(),
            com.legend.builtin.Pure.MAP__RELATION_1__FUNCTION_1.qualifiedName(),
            com.legend.builtin.Pure.LIMIT__T_MANY__INTEGER_1.qualifiedName(),
            com.legend.builtin.Pure.LIMIT__RELATION_1__INTEGER_1.qualifiedName(),
            com.legend.builtin.Pure.LIMIT__TDS_1__INTEGER_0_1.qualifiedName(),
            com.legend.builtin.Pure.TAKE__T_MANY__INTEGER_1.qualifiedName(),
            com.legend.builtin.Pure.DROP__T_MANY__INTEGER_1.qualifiedName(),
            com.legend.builtin.Pure.DROP__RELATION_1__INTEGER_1.qualifiedName(),
            com.legend.builtin.Pure.SLICE__T_MANY__INTEGER_1__INTEGER_1.qualifiedName(),
            com.legend.builtin.Pure.SLICE__RELATION_1__INTEGER_1__INTEGER_1.qualifiedName(),
            com.legend.builtin.Pure.ROWS__INTEGER_1__INTEGER_1.qualifiedName(),
            com.legend.builtin.Pure.TO_ONE__T_MANY.qualifiedName(),
            com.legend.builtin.Pure.AT__T_MANY__INTEGER_1.qualifiedName(),
            com.legend.builtin.Pure.MAKE_STRING__ANY_MANY.qualifiedName(),
            com.legend.builtin.Pure.TO_CSV__TDS.qualifiedName(),
            com.legend.builtin.Pure.TO_STRING__RELATION.qualifiedName(),
            com.legend.builtin.Pure.TO_STRING__ANY_1.qualifiedName(),
            com.legend.builtin.Pure.FROM__T_m__MAPPING_1__RUNTIME_1.qualifiedName(),
            com.legend.builtin.Pure.FILTER__T_MANY__FUNCTION_1.qualifiedName(),
            com.legend.builtin.Pure.FILTER__RELATION_1__FUNCTION_1.qualifiedName(),
            com.legend.builtin.Pure.TDS_FILTER__TDS_1__FUNCTION_1.qualifiedName(),
            com.legend.builtin.Pure.SELECT__RELATION_1.qualifiedName(),
            com.legend.builtin.Pure.RENAME__RELATION_1__COL_SPEC_1__COL_SPEC_1.qualifiedName(),
            com.legend.builtin.Pure.PROJECT__RELATION_1__FUNC_COL_SPEC_ARRAY_1.qualifiedName(),
            com.legend.builtin.Pure.PROJECT__K_MANY__FUNCTION_MANY__STRING_MANY.qualifiedName(),
            com.legend.builtin.Pure.DISTINCT__T_MANY.qualifiedName(),
            com.legend.builtin.Pure.DISTINCT__RELATION_1.qualifiedName(),
            // a graph fetch / serialize keeps its root's order
            com.legend.builtin.Pure.GRAPH_FETCH__T_MANY__ROOT_GRAPH_FETCH_TREE_1.qualifiedName(),
            com.legend.builtin.Pure.GRAPH_FETCH_CHECKED__T_MANY__ROOT_GRAPH_FETCH_TREE_1.qualifiedName(),
            com.legend.builtin.Pure.SERIALIZE__T_MANY__ROOT_GRAPH_FETCH_TREE_1.qualifiedName());

    /** An execute() FRAME returns its query's rows in the query's order. */
    public static final java.util.Set<String> EXECUTE_FRAMES = java.util.Set.of(
            com.legend.compiler.element.type.PlatformTypes.EXECUTE,
            com.legend.compiler.element.type.PlatformTypes.EXECUTION_PLAN_EXECUTE);

    public static OrderView of(TypedSpec s0, List<TypedSpec> letPrefix) {
        return of(s0, letPrefix, new java.util.HashSet<>(), null);
    }

    /** The order view WITH the envelope splice: a read of an execute
     * frame ({@code $result.values…}) resolves to the frame's own chain,
     * whose sort the view sees. Without the hook such a read is
     * INCIDENTAL — a bag compare that would hide an ORDER BY (USER
     * 2026-09-18: the bag only without a top-level sort). */
    public static OrderView of(TypedSpec s0, List<TypedSpec> letPrefix,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec, java.util.Set<String>, TypedSpec> hook) {
        return of(s0, letPrefix, new java.util.HashSet<>(), hook);
    }

    private static OrderView of(TypedSpec s, List<TypedSpec> lets, java.util.Set<String> seen,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec, java.util.Set<String>, TypedSpec> hook) {
        if (s instanceof com.legend.compiler.spec.typed.TypedSort
                || s instanceof com.legend.compiler.spec.typed.TypedSortBy) {
            return OrderView.SORTED;
        }
        if (s instanceof TypedNativeCall c) {
            String fqn = c.callee().qualifiedName();
            if (SORT_FQNS.contains(fqn)) {
                return OrderView.SORTED;
            }
            if (ORDER_PRESERVING.contains(fqn) && !c.args().isEmpty()) {
                return of(c.args().get(0), lets, seen, hook);
            }
            // an execute() FRAME returns its query's rows in the query's
            // order: descend into the lambda's tail expression -- the query a
            // let binds as well (let query = {|...}; execute($query, ...),
            // 2026-11-05: the corpus's testCheckedWithCircularConstraints); a
            // query not in sight is the database's rows, its order INCIDENTAL,
            // never the language's
            if (EXECUTE_FRAMES.contains(fqn) && !c.args().isEmpty()) {
                TypedSpec query = c.args().get(0);
                if (query instanceof com.legend.compiler.spec.typed.TypedVariable qv) {
                    for (int i = lets.size() - 1; i >= 0; i--) {
                        if (lets.get(i) instanceof com.legend.compiler.spec.typed.TypedLet l
                                && l.name().equals(qv.name())) {
                            query = l.value();
                            break;
                        }
                    }
                }
                // preval(lambda, ...): the lambda itself, pre-folded at plan time (the review of B3 step 2b's M4,
                // 2026-11-24: tds::sort::testSimpleSortAscWithPreval's sort was out of sight)
                if (query instanceof TypedNativeCall pv
                        && com.legend.builtin.NativeFn.Handle.of(pv.callee().id()).orElse(null)
                                == com.legend.builtin.NativeFn.Handle.PREVAL
                        && !pv.args().isEmpty()) {
                    query = pv.args().get(0);
                }
                if (query instanceof com.legend.compiler.spec.typed.TypedLambda lam && !lam.body().isEmpty()) {
                    return of(lam.body().get(lam.body().size() - 1), lets, seen, hook);
                }
                return OrderView.INCIDENTAL;
            }
            if (ORDER_DESTROYING.contains(fqn)) {
                return OrderView.INCIDENTAL;
            }
            if (ORDER_COMBINING.contains(fqn)) {
                // a collection's concatenate or union: the language's order over its inputs' -- as ordered as the
                // least ordered of them (an input in arrival order leaves the whole in arrival order)
                for (TypedSpec arg : c.args()) {
                    if (of(arg, lets, seen, hook) == OrderView.INCIDENTAL) {
                        return OrderView.INCIDENTAL;
                    }
                }
                return OrderView.DEFINED;
            }
            // a function the rule does not name: as ordered as what it was applied to
            return leastOrdered(c, c.args(), lets, seen, hook);
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedGetAll
                || s instanceof com.legend.compiler.spec.typed
                        .TypedTableReference
                || s instanceof com.legend.compiler.spec.typed
                        .TypedRawSqlRelation
                || s instanceof com.legend.compiler.spec.typed
                        .TypedCatalogGrid
                // a store's view, a recursion (a UNION ALL of its steps) and a join's slot are rows in arrival order
                // as a table's are (named 2026-11-27: each had fallen to DEFINED)
                || s instanceof com.legend.compiler.spec.typed.TypedViewRelation
                || s instanceof com.legend.compiler.spec.typed.TypedRecurse
                || s instanceof com.legend.compiler.spec.typed.TypedJoinSlot) {
            return OrderView.INCIDENTAL;
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedVariable v) {
            if (!seen.add(v.name())) {
                return OrderView.DEFINED;
            }
            for (int i = lets.size() - 1; i >= 0; i--) {
                if (lets.get(i) instanceof
                        com.legend.compiler.spec.typed.TypedLet l
                        && l.name().equals(v.name())) {
                    return of(l.value(), lets, seen, hook);
                }
            }
            // unresolvable binding = an execution frame ($result): with the
            // splice in hand its values read IS the frame's chain (a
            // compiler-minted read, VerdictQueries.valuesRead); without it,
            // a store query by construction
            if (hook != null) {
                TypedSpec read = com.legend.compiler.spec.VerdictQueries.valuesRead(v);
                TypedSpec chain = hook.apply(read, java.util.Set.of());
                if (chain != read) {
                    return of(chain, lets, seen, hook);
                }
            }
            return OrderView.INCIDENTAL;
        }
        // a graph fetch / serialize keeps its ROOT query's order (the
        // engine's graph result is the root SQL's arrival order; a nested
        // property's order is the mapping's, not the chain's)
        // a relation's aggregate with no grouping (TypedAggregate) is one row, whose order is defined (B3 step 4,
        // 2026-11-24: it left the order-destroying nodes below with its native above)
        if (s instanceof com.legend.compiler.spec.typed.TypedAggregate) {
            return OrderView.DEFINED;
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedGraphFetch gf) {
            return of(gf.source(), lets, seen, hook);
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedSerializeGraph sg) {
            return of(sg.source(), lets, seen, hook);
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedSerialize sz) {
            return of(sz.source(), lets, seen, hook);
        }
        // a grouping / join / concatenation / pivot leaves NO order behind
        // in SQL (a GROUP BY, a join, a UNION have none): the chain's
        // order is incidental past them unless a later sort names it
        if (s instanceof com.legend.compiler.spec.typed.TypedGroupBy
                || s instanceof com.legend.compiler.spec.typed.TypedJoin
                || s instanceof com.legend.compiler.spec.typed.TypedAsOfJoin
                || s instanceof com.legend.compiler.spec.typed.TypedConcatenate
                || s instanceof com.legend.compiler.spec.typed.TypedPivot) {
            return OrderView.INCIDENTAL;
        }
        // an extend keeps its source's rows in order
        if (s instanceof com.legend.compiler.spec.typed.TypedExtend
                || s instanceof com.legend.compiler.spec.typed.TypedExtendAgg
                || s instanceof com.legend.compiler.spec.typed.TypedExtendWindow) {
            List<TypedSpec> ch = s.children();
            return ch.isEmpty() ? OrderView.DEFINED
                    : of(ch.get(0), lets, seen, hook);
        }
        // order-preserving wrappers descend to their SOURCE (first
        // child); anything else keeps the language's defined order
        if (s instanceof com.legend.compiler.spec.typed.TypedFilter
                || s instanceof com.legend.compiler.spec.typed.TypedProject
                || s instanceof com.legend.compiler.spec.typed.TypedSelect
                || s instanceof com.legend.compiler.spec.typed.TypedRename
                || s instanceof com.legend.compiler.spec.typed.TypedDistinct
                || s instanceof com.legend.compiler.spec.typed.TypedLimit
                || s instanceof com.legend.compiler.spec.typed.TypedDrop
                || s instanceof com.legend.compiler.spec.typed.TypedSlice
                || s instanceof com.legend.compiler.spec.typed.TypedMap
                || s instanceof com.legend.compiler.spec.typed
                        .TypedPropertyAccess
                || s instanceof com.legend.compiler.spec.typed.TypedCast
                || s instanceof com.legend.compiler.spec.typed.TypedFrom
                || s instanceof com.legend.compiler.spec.typed.TypedNavigate
                || s instanceof com.legend.compiler.spec.typed
                        .TypedMilestonedAccess) {
            List<TypedSpec> ch = s.children();
            if (ch.isEmpty()) {
                return OrderView.DEFINED;
            }
            OrderView source = of(ch.get(0), lets, seen, hook);
            // MANY values read from ONE (a to-many property of the first of a sorted read): their order is the
            // property's own, which a sort of the parents does not give -- in arrival order where the parent came
            // from a sort (a store's, in practice), the language's where it is a value (2026-11-27: it had been
            // SORTED, by the parents' sort)
            return source == OrderView.SORTED && !oneValue(s) && oneValue(ch.get(0)) ? OrderView.INCIDENTAL : source;
        }
        // a node the rule does not name: as ordered as what it was made of
        return leastOrdered(s, s.children(), lets, seen, hook);
    }

    /**
     * The order of a function or node the rule does not name, {@code s}, applied to {@code inputs}: DEFINED where it
     * is one value (one value has no order to compare); else INCIDENTAL where any many-valued input is -- a lambda
     * read as its answer (an {@code if}'s branch, a thunk), its parameters values the walk cannot see -- and DEFINED
     * otherwise. Never SORTED: {@code reverse} of a sorted list is ordered, not in its sort's order.
     */
    private static OrderView leastOrdered(TypedSpec s, List<TypedSpec> inputs, List<TypedSpec> lets,
            java.util.Set<String> seen,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec, java.util.Set<String>, TypedSpec> hook) {
        if (oneValue(s)) {
            return OrderView.DEFINED;
        }
        for (TypedSpec input : inputs) {
            TypedSpec read = input;
            List<TypedSpec> scope = lets;
            if (input instanceof com.legend.compiler.spec.typed.TypedLambda lambda) {
                if (lambda.body().isEmpty()) {
                    continue;
                }
                read = lambda.body().get(lambda.body().size() - 1);
                scope = new java.util.ArrayList<>(lets);
                for (String p : lambda.parameters()) {
                    // a parameter is a value the walk cannot see: read as a literal, never as a frame's rows (the
                    // rule OrderedAsserts.rewrite has for a lambda it walks into)
                    scope.add(new com.legend.compiler.spec.typed.TypedLet(p, new com.legend.compiler.spec.typed
                            .TypedCollection(List.of(), lambda.info(), false, false), lambda.info()));
                }
                scope.addAll(lambda.body().subList(0, lambda.body().size() - 1));
            }
            if (!oneValue(read) && of(read, scope, new java.util.HashSet<>(seen), hook) == OrderView.INCIDENTAL) {
                return OrderView.INCIDENTAL;
            }
        }
        return OrderView.DEFINED;
    }

    /** Whether {@code s} is one value at most: to-one or zero-to-one, and no relation. */
    private static boolean oneValue(TypedSpec s) {
        com.legend.compiler.element.type.ExprType info = s.info();
        return info.multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && b.upper() != null && b.upper() <= 1
                && !com.legend.compiler.element.type.Type.isRelation(info.type());
    }
}
