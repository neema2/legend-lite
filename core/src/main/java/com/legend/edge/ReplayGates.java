// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * THE REFEREE'S GATES for a verified chain (moved from the product's AssertVerdicts.ReplayGates and
 * VerdictQueries.extentSubset, 2026-11-18, the edge audit's J40: the referee is the edge's, G6), derived ONCE from the
 * typed chain where the appeal is planned (the plan carries each as the appeal's {@code Appeal.Rows} fields): the
 * extent-subset fact, the order view (ends in a sort = order is contract), and the tail-most sort's keys. Typed-tree
 * navigation only -- nothing evaluated.
 */
record ReplayGates(boolean extentSubset, boolean ordered, @com.legend.base.Nullable List<String> sortKeys) {

    static final ReplayGates NONE = new ReplayGates(false, false, null);

    static ReplayGates of(TypedSpec chain, List<TypedSpec> letPrefix) {
        boolean ordered = com.legend.compiler.spec.OrderView.of(chain, letPrefix)
                == com.legend.compiler.spec.OrderView.SORTED;
        return new ReplayGates(extentSubset(chain), ordered,
                ordered ? sortKeys(chain, letPrefix, new java.util.HashSet<>()) : null);
    }

    /** Whether a typed chain is a SUB-COLLECTION of a class extent:
     * getAll through filter/sort/limit/slice/drop/from/first/last/toOne
     * (the walk lane's extentSubset, on the typed tree). */
    private static boolean extentSubset(TypedSpec n) {
        return switch (n) {
            case com.legend.compiler.spec.typed.TypedGetAll g -> true;
            case com.legend.compiler.spec.typed.TypedFilter f ->
                    extentSubset(f.source());
            case com.legend.compiler.spec.typed.TypedSort s ->
                    extentSubset(s.source());
            case com.legend.compiler.spec.typed.TypedSortBy s ->
                    extentSubset(s.source());
            case com.legend.compiler.spec.typed.TypedLimit l ->
                    extentSubset(l.source());
            case com.legend.compiler.spec.typed.TypedSlice l ->
                    extentSubset(l.source());
            case com.legend.compiler.spec.typed.TypedDrop d ->
                    extentSubset(d.source());
            case com.legend.compiler.spec.typed.TypedFrom f ->
                    extentSubset(f.source());
            case com.legend.compiler.spec.typed.TypedNativeCall c when !c.args().isEmpty() -> {
                String q = c.callee().qualifiedName();
                String simple = q.substring(q.lastIndexOf(':') + 1);
                yield switch (simple) {
                    case "first", "last", "toOne", "take", "limit", "drop",
                            "slice" -> extentSubset(c.args().get(0));
                    default -> false;
                };
            }
            default -> false;
        };
    }

    /** The key names of the sort NEAREST THE TAIL (the engine's own
     * last-sort-wins semantics), through the same order-preserving tails
     * OrderView descends; null = underivable (a computed key,
     * a native sort spelling, a value the compared output cannot carry). */
    private static @com.legend.base.Nullable List<String> sortKeys(TypedSpec s,
            List<TypedSpec> lets, java.util.Set<String> seen) {
        if (s instanceof com.legend.compiler.spec.typed.TypedSort so) {
            List<String> keys = new java.util.ArrayList<>();
            for (var k : so.keys()) {
                keys.add(k.column());
            }
            return keys.isEmpty() ? null : keys;
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedSortBy sb) {
            if (sb.keyAlias() != null) {
                return List.of(sb.keyAlias());
            }
            List<TypedSpec> body = sb.key().body();
            return body.size() == 1 && body.get(0)
                    instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                    ? List.of(pa.property()) : null;
        }
        if (s instanceof TypedNativeCall c) {
            String fqn = c.callee().qualifiedName();
            if (com.legend.compiler.spec.OrderView.EXECUTE_FRAMES.contains(fqn) && !c.args().isEmpty()
                    && c.args().get(0) instanceof com.legend.compiler.spec.typed.TypedLambda lam
                    && !lam.body().isEmpty()) {
                return sortKeys(lam.body().get(lam.body().size() - 1), lets, seen);
            }
            return com.legend.compiler.spec.OrderView.ORDER_PRESERVING.contains(fqn)
                    && !c.args().isEmpty() ? sortKeys(c.args().get(0), lets, seen) : null;
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedVariable v) {
            if (!seen.add(v.name())) {
                return null;
            }
            com.legend.compiler.spec.typed.TypedLet l =
                    com.legend.compiler.spec.typed.Lets.binding(lets, v.name());
            return l == null ? null : sortKeys(l.value(), lets, seen);
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedFilter
                || s instanceof com.legend.compiler.spec.typed.TypedProject
                || s instanceof com.legend.compiler.spec.typed.TypedSelect
                || s instanceof com.legend.compiler.spec.typed.TypedRename
                || s instanceof com.legend.compiler.spec.typed.TypedDistinct
                || s instanceof com.legend.compiler.spec.typed.TypedLimit
                || s instanceof com.legend.compiler.spec.typed.TypedDrop
                || s instanceof com.legend.compiler.spec.typed.TypedSlice
                || s instanceof com.legend.compiler.spec.typed.TypedMap
                || s instanceof com.legend.compiler.spec.typed.TypedPropertyAccess
                || s instanceof com.legend.compiler.spec.typed.TypedCast
                || s instanceof com.legend.compiler.spec.typed.TypedFrom
                || s instanceof com.legend.compiler.spec.typed.TypedNavigate
                || s instanceof com.legend.compiler.spec.typed.TypedMilestonedAccess) {
            List<TypedSpec> ch = s.children();
            return ch.isEmpty() ? null : sortKeys(ch.get(0), lets, seen);
        }
        return null;
    }
}
