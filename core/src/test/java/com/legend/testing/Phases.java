// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testing;

import com.legend.compiler.ModelBuilder;
import com.legend.compiler.element.PureModelContext;
import com.legend.model.NormalizedModel;
import com.legend.model.ParsedModel;
import com.legend.normalizer.ModelNormalizer;

/**
 * The phase calls a unit test makes on its own fixture, in the product's
 * shape (T4.1 step 2): ONE index built from the resolved elements before
 * Phase E, handed to it, and carried to the Phase-F gate. Tests that
 * normalize and compile in separate steps build a fresh index for the
 * second step from the normalized elements (the gate's add then skips
 * every element it already holds).
 */
public final class Phases {

    private Phases() {}

    /** Phase E over a name-resolved model, on its own fresh index. */
    public static NormalizedModel normalize(ParsedModel resolved) {
        return ModelNormalizer.normalize(resolved, ModelBuilder.from(resolved), null);
    }

    /** Phase F over an already-normalized model (Phase-F unit fixtures). */
    public static PureModelContext context(NormalizedModel normalized) {
        return PureModelContext.from(normalized, ModelBuilder.from(
                new ParsedModel(normalized.elements(), normalized.imports())), com.legend.lowering.PlatformRegistrations.current());
    }
}
