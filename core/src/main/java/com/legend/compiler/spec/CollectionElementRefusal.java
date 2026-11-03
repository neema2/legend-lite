// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.compiler.spec;

/**
 * A collection literal of more than one value with an element not exactly [1] (a nullable column's cell in
 * {@code $x.n * 2}, the parser's {@code times([$x.n, 2])}), refused in legend-engine's own words
 * ({@code ValueSpecificationBuilder.java:883-896}): {@code Collection element must have a multiplicity [1] -
 * Context:[...], multiplicity:[0..1]}. legend-engine's context is its compiler's pass stack; lite's is the call it was
 * applying, the innermost, named as legend-engine names it ({@code Applying times}), its pass names a register row
 * (S128; docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md D18 C, the user, 2026-11-03).
 */
public final class CollectionElementRefusal extends TypeInferenceException {
    private final String multiplicity;
    private final @com.legend.base.Nullable String applying;

    CollectionElementRefusal(String multiplicity) {
        this(multiplicity, null);
    }

    private CollectionElementRefusal(String multiplicity, @com.legend.base.Nullable String applying) {
        super("Collection element must have a multiplicity [1] - Context:["
                + (applying == null ? "" : "Applying " + applying) + "], multiplicity:" + multiplicity);
        this.multiplicity = multiplicity;
        this.applying = applying;
    }

    /** This refusal in the context of the call it was raised under ({@code function}'s name, its package dropped, as
     *  legend-engine names it); the innermost call's only -- one already in a context keeps it. */
    CollectionElementRefusal applying(String function) {
        if (applying != null) {
            return this;
        }
        int sep = function.lastIndexOf("::");
        return new CollectionElementRefusal(multiplicity, sep < 0 ? function : function.substring(sep + 2));
    }
}
