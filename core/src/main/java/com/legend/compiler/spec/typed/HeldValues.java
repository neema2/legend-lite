// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import com.legend.builtin.Pure;
import com.legend.model.FunctionId;

/**
 * WHAT A TYPED EXPRESSION'S VALUES ARE VALUES OF (step 6B, slice 5; here since 2026-11-28 so the type-checker and
 * the lowering read ONE list: the lowering consumes typed HIR and nothing of the type-checker's own -- invariant 6h).
 */
public final class HeldValues {

    private HeldValues() {
    }

    /** The natives that answer values of the list they are given and no other: a part of it, or all of it in
     *  another order. */
    private static final java.util.Set<FunctionId> KEEPS_ITS_VALUES = java.util.Collections.unmodifiableSet(
            new java.util.LinkedHashSet<>(FunctionId.all(
                    Pure.AT_COLLECTION_SORT, Pure.AT_COLLECTION_REVERSE, Pure.AT_COLLECTION_TAIL,
                    Pure.AT_COLLECTION_INIT, Pure.AT_COLLECTION_DISTINCT, com.legend.builtin.Comparisons.REMOVE_DUPLICATES)));

    /** The natives that answer one value of the list they are given, as it is. */
    private static final java.util.Set<FunctionId> ONE_OF_A_LIST = java.util.Collections.unmodifiableSet(
            new java.util.LinkedHashSet<>(FunctionId.all(
                    Pure.AT_MULTIPLICITY_TO_ONE, Pure.AT_COLLECTION_FIRST, Pure.AT_COLLECTION_LAST,
                    Pure.AT_COLLECTION_AT, Pure.AT_COLLECTION_HEAD)));

    /**
     * WHAT {@code side}'S VALUES ARE VALUES OF, where it only keeps, reorders or picks among the values it is given:
     * a {@code filter}'s source; the list a {@code sort}, a {@code reverse}, a {@code take}, a {@code drop}, a
     * {@code slice}, a {@code tail}, an {@code init} or a deduplication is given; the list {@code toOne},
     * {@code first}, {@code last}, {@code at} or {@code head} answers one value of. Null for anything else. The ONE
     * list of what a held type is read through: the type-checker reads it for the types a comparison's sides hold
     * ({@code compiler.spec.EqualityClasses.heldCrossing}), and the lowering for the kind a value typed at run time holds
     * ({@code lowering.MixedEncoding.kindThroughUpcasts}).
     */
    public static @com.legend.base.Nullable TypedSpec valuesOf(TypedSpec side) {
        // (the type-checker gives some of these their own node, a list's as a table's: take and limit, drop, slice
        // and sortBy never reach here as a call -- the review of slice 5, finding 2: read by their function ids
        // alone, [1, 2]->cast(@Number)->take(1)->contains(1.0) was still the database's membership. A list's sort
        // and its deduplications are calls; a table's are the nodes, which no side of these types is)
        return switch (side) {
            case TypedFilter o -> o.source();
            case TypedSort o -> o.source();
            case TypedSortBy o -> o.source();
            case TypedDistinct o -> o.source();
            case TypedLimit o -> o.source();
            case TypedDrop o -> o.source();
            case TypedSlice o -> o.source();
            case TypedNativeCall read when !read.args().isEmpty()
                    && (KEEPS_ITS_VALUES.contains(read.callee().id()) || ONE_OF_A_LIST.contains(read.callee().id()))
                    -> read.args().get(0);
            default -> null;
        };
    }
}
