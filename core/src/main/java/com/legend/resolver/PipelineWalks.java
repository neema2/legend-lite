// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.resolver;

import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.Set;

/** Structural walks over a resolved pipeline (extracted from
 * {@link StoreResolver} at the file guardrail). */
final class PipelineWalks {

    private PipelineWalks() {
    }

    static void collectLambdaParams(TypedSpec n, Set<String> out) {
        if (n instanceof TypedLambda l) {
            out.addAll(l.parameters());
        }
        for (TypedSpec c : n.children()) {
            collectLambdaParams(c, out);
        }
    }

    /** Whether the pipeline carries a mapping ~filter anywhere. */
    static boolean containsFilter(TypedSpec pipeline) {
        if (pipeline instanceof TypedFilter) {
            return true;
        }
        for (TypedSpec c : pipeline.children()) {
            if (containsFilter(c)) {
                return true;
            }
        }
        return false;
    }
}
