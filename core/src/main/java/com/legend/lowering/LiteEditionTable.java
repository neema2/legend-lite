// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.model.FunctionId;
import com.legend.platform.Feature;

import java.util.Set;

/**
 * THE PRODUCT'S EDITION TABLE ({@link EditionTable}; the edition program's step 2, 2026-11-25): no row. Lite means what
 * its own rules say, so every call lowers by its own rule.
 */
public final class LiteEditionTable implements EditionTable {

    public static final LiteEditionTable INSTANCE = new LiteEditionTable();

    private LiteEditionTable() {
    }

    @Override
    public @com.legend.base.Nullable FunctionId lowersAs(TypedNativeCall call, Set<Feature> features) {
        return null;
    }
}
