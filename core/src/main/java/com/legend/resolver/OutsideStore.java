// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

/**
 * WHICH MODEL AND MAPPING CODE THE RESOLVER STAMPS as outside any store query as it brings it into a statement (the
 * edition program's step 2, 2026-11-26; {@code TypedNativeCall.outsideStore}, read by the lowering's edition table).
 * The planner decides it per statement from its door's edition ({@code planner.Edition}); the product stamps none.
 *
 * @param constraints a class's constraints, where a checked graph fetch evaluates them
 * @param transforms  a model-to-model mapping's transforms (its filters and its properties' expressions, and the code
 *                    of the functions they call), and the qualified properties a graph fetch reads off a class such a
 *                    mapping makes
 */
public record OutsideStore(boolean constraints, boolean transforms) {

    /** Nothing stamped: the product's, and every road that plans for no door's edition. */
    public static final OutsideStore NONE = new OutsideStore(false, false);
}
