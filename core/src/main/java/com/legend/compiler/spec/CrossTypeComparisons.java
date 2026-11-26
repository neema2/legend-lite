// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedCrossTypeComparison;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.ModelException;

import java.util.ArrayList;
import java.util.List;

/**
 * THE ONE EQUALITY AT THE PLANNER (B3 step 5; the user's ruling of 2026-11-24, "Love it"). Lite's planner refuses a
 * comparison across types ({@link TypedCrossTypeComparison}, the marker lite's comparison setting leaves in the query,
 * {@link LiteCrossTypeRule}) by name before it plans anything, naming the fix. A compatibility door's setting answers
 * the comparison where it is typed and leaves no marker ({@link CrossTypeRule}; the edition program's step 2b,
 * 2026-11-26: until then a door rewrote the markers after typing). The core never chooses between audiences: it
 * refuses whatever marker reaches it. {@link #in} lists the markers a typed body holds.
 */
public final class CrossTypeComparisons {

    private CrossTypeComparisons() {
    }

    /** Refuses the first comparison across types in {@code body}, by name. */
    public static void refuse(List<? extends TypedSpec> body) {
        for (TypedSpec statement : body) {
            TypedCrossTypeComparison first = first(statement);
            if (first != null) {
                throw refusal(first);
            }
        }
    }

    /** The refusal of {@code c}: a compile error naming the two types and the fix. */
    public static ModelException refusal(TypedCrossTypeComparison c) {
        return c.refusal();
    }

    /** Every comparison across types in {@code body}, in order: what a door that rewrites them loosened. */
    public static List<TypedCrossTypeComparison> in(List<? extends TypedSpec> body) {
        List<TypedCrossTypeComparison> out = new ArrayList<>();
        for (TypedSpec statement : body) {
            collect(statement, out);
        }
        return out;
    }

    /** Whether {@code n} holds a comparison across types. */
    static boolean holds(TypedSpec n) {
        return first(n) != null;
    }

    private static @com.legend.base.Nullable TypedCrossTypeComparison first(TypedSpec n) {
        if (n instanceof TypedCrossTypeComparison c) {
            return c;
        }
        for (TypedSpec k : n.children()) {
            TypedCrossTypeComparison f = first(k);
            if (f != null) {
                return f;
            }
        }
        return null;
    }

    private static void collect(TypedSpec n, List<TypedCrossTypeComparison> out) {
        if (n instanceof TypedCrossTypeComparison c) {
            out.add(c);
        }
        for (TypedSpec k : n.children()) {
            collect(k, out);
        }
    }
}
