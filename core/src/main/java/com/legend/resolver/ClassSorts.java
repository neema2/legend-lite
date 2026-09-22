// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;

/**
 * Class-space sorts recognized as the relation sort ({@code sortBy(coll,
 * key)}, {@code sort(key, {x,y|compare})}). Extracted from StoreResolver
 * (file-size guardrail).
 */
final class ClassSorts {

    private ClassSorts() {
    }

    private static final String SORT_BY_FQN = "meta::pure::functions::collection::sortBy";
    private static final String SORT_BY_REV_FQN = "meta::pure::functions::collection::sortByReversed";
    private static final String COMPARE_FQN = "meta::pure::functions::lang::compare";

    /** first()/head() over an object-space chain — LIMIT 1 in disguise. */
    static boolean isFirstLike(TypedNativeCall c) {
        String fqn = c.callee().qualifiedName();
        return c.args().size() == 1
                && (StoreResolver.FIRST_FQN.equals(fqn) || StoreResolver.HEAD_FQN.equals(fqn));
    }

    /**
     * Class-space {@code sort(key, {x,y|compare})}: the comparator must be a
     * BARE compare over the two parameters — its argument order IS the
     * direction ({@code $x->compare($y)} ascending, {@code $y->compare($x)}
     * descending). Anything richer has no relation sort shape.
     */
    static @com.legend.base.Nullable TypedSortBy classSortOf(TypedSpec n) {
        // class-space sortBy(coll, key)/sortByReversed — the 2-arg native
        // spelling of the relation sort (computed keys substitute like any)
        if (n instanceof TypedNativeCall sb && sb.args().size() == 2
                && (SORT_BY_FQN.equals(sb.callee().qualifiedName())
                        || SORT_BY_REV_FQN.equals(sb.callee().qualifiedName()))
                && sb.args().get(1) instanceof TypedLambda key2) {
            return new TypedSortBy(sb.args().get(0), key2,
                    SORT_BY_FQN.equals(sb.callee().qualifiedName()),
                    sb.info());
        }
        if (!(n instanceof TypedNativeCall c) || c.args().size() != 3
                || !StoreResolver.SORT_FQN.equals(c.callee().qualifiedName())
                || !(c.args().get(1) instanceof TypedLambda key)
                || !(c.args().get(2) instanceof TypedLambda cmp)) {
            return null;
        }
        Boolean ascending = comparatorDirection(cmp);
        return ascending == null ? null
                : new TypedSortBy(c.args().get(0), key, ascending, c.info());
    }

    private static @com.legend.base.Nullable Boolean comparatorDirection(TypedLambda cmp) {
        if (cmp.parameters().size() != 2 || cmp.body().size() != 1
                || !(cmp.body().get(0) instanceof TypedNativeCall cc)
                || !COMPARE_FQN.equals(cc.callee().qualifiedName())
                || cc.args().size() != 2
                || !(cc.args().get(0) instanceof TypedVariable a)
                || !(cc.args().get(1) instanceof TypedVariable b)) {
            return null;
        }
        String p0 = cmp.parameters().get(0);
        String p1 = cmp.parameters().get(1);
        if (a.name().equals(p0) && b.name().equals(p1)) {
            return Boolean.TRUE;
        }
        if (a.name().equals(p1) && b.name().equals(p0)) {
            return Boolean.FALSE;
        }
        return null;
    }
}
