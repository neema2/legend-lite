// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedSpec;

/** A helper-wrapped root that takes the verdict route directly
 * (BodyDriver's statement loop): a string-entry read. (Any assert-family
 * root does too since 2026-11-04 -- the asserts-as-plan-nodes program,
 * step 3 -- so the class-value predicate of batch 54 went.) A pure
 * predicate over the typed tree. */
public final class VerdictRoutes {

    private VerdictRoutes() {
    }

    /** Whether the tree reads a string-entry result (the JSON envelope
     * or a JSON tree read over it). */
    public static boolean readsStringEntry(TypedSpec n) {
        if (n instanceof com.legend.compiler.spec.typed.TypedJsonResult
                || n instanceof com.legend.compiler.spec.typed.TypedJsonAccess) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (readsStringEntry(c)) {
                return true;
            }
        }
        return false;
    }

}
