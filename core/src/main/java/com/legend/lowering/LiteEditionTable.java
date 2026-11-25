// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.FunctionId;
import com.legend.platform.Feature;

import java.util.Set;
import java.util.function.Supplier;

/**
 * THE PRODUCT'S EDITION TABLE ({@link EditionTable}; the edition program's step 2, 2026-11-25): no row and no stamp.
 * Lite means what its own rules say, so every call lowers by its own rule and the planner stamps nothing. Its own
 * class, as each two-form kind's product form is (a kind holds signatures only: {@code PlanStyleSurfaceTest}).
 */
public final class LiteEditionTable implements EditionTable {

    public static final LiteEditionTable INSTANCE = new LiteEditionTable();

    private LiteEditionTable() {
    }

    @Override
    public @com.legend.base.Nullable FunctionId lowersAs(TypedNativeCall call, Set<Feature> features) {
        return null;
    }

    @Override
    public EditionTable on(Supplier<@com.legend.base.Nullable DatabaseType> database) {
        return this;
    }

    @Override
    public boolean stampsOutsideStore() {
        return false;
    }
}
