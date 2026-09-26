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
            "meta::pure::functions::relation::sort");

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
            com.legend.builtin.Pure.CONCATENATE__T_MANY__T_MANY.qualifiedName(),
            com.legend.builtin.Pure.CONCATENATE__RELATION_1__RELATION_1.qualifiedName(),
            com.legend.builtin.Pure.UNION__T_MANY__T_MANY.qualifiedName(),
            com.legend.builtin.Pure.PIVOT__RELATION_1__COL_SPEC_1__AGG_COL_SPEC_1.qualifiedName(),
            com.legend.builtin.Pure.AGGREGATE__RELATION_1__AGG_COL_SPEC_1.qualifiedName());

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
            // order: descend into the lambda's tail expression
            if (EXECUTE_FRAMES.contains(fqn) && !c.args().isEmpty()
                    && c.args().get(0) instanceof com.legend.compiler.spec.typed.TypedLambda lam
                    && !lam.body().isEmpty()) {
                return of(lam.body().get(lam.body().size() - 1), lets, seen, hook);
            }
            if (ORDER_DESTROYING.contains(fqn)) {
                return OrderView.INCIDENTAL;
            }
            return OrderView.DEFINED;
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedGetAll
                || s instanceof com.legend.compiler.spec.typed
                        .TypedTableReference
                || s instanceof com.legend.compiler.spec.typed
                        .TypedRawSqlRelation) {
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
                || s instanceof com.legend.compiler.spec.typed.TypedAggregate
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
            return ch.isEmpty() ? OrderView.DEFINED
                    : of(ch.get(0), lets, seen, hook);
        }
        return OrderView.DEFINED;
    }
}
