// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.graphql;

import com.legend.error.LegendCompileException;

/**
 * The schema generator's refusal (docs/GRAPHQL_STUDY_2026_10_18.md §5.7, G11): a model the generator cannot map
 * to a schema without renaming, skipping or defaulting, named by element and rule. A model-phase error: the data
 * space has no endpoint until the model changes.
 */
public final class GqlSchemaException extends LegendCompileException {

    public GqlSchemaException(String message, String element) {
        super(Phase.MODEL, message, element);
    }
}
