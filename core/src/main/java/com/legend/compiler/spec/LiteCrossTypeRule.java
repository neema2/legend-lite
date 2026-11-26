// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedCrossTypeComparison;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * LITE'S COMPARISON SETTING ({@link CrossTypeRule}), the type-checker's own unless a door hands it another. In the
 * query a comparison across types is a mistake: it is typed as its marker, which lite's planner refuses by name before
 * it plans anything ({@link CrossTypeComparisons#refuse}), naming the two types and the fix. In a body the query
 * reaches it is still the call, the database's comparison, as before the setting: making it the same compile error
 * there is the edition program's step 4, which waits on the user's ruling of what breaks
 * (docs/records/edition-homework/step4-exposure-2026-11-26.md).
 */
public final class LiteCrossTypeRule implements CrossTypeRule {

    /** Lite's setting. */
    public static final LiteCrossTypeRule INSTANCE = new LiteCrossTypeRule();

    private LiteCrossTypeRule() {
    }

    @Override
    public TypedSpec inQuery(TypedCrossTypeComparison crossing) {
        return crossing;
    }

    @Override
    public boolean readsBodies() {
        return false;
    }

    @Override
    public TypedSpec inBody(TypedCrossTypeComparison crossing) {
        return crossing.comparison();
    }
}
