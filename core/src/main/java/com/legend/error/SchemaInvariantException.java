// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.error;

/**
 * A SCHEMA INVARIANT violation (duplicate/colliding column names — real
 * legend-pure's own errors, never silent). Distinct from a plain
 * {@link TypeInferenceException} so the overload-retry search rethrows
 * it instead of trying the next candidate: an invariant broken under the
 * BEST-matching overload is the program's defect, and a lesser overload
 * silently absorbing it would erase the diagnosis (the
 * extendRejectsExistingColumnName pin).
 */
public class SchemaInvariantException extends TypeInferenceException {

    public SchemaInvariantException(String message) {
        super(message);
    }
}
