// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.ClassLayouts;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Inliner-consumed lets prefix the lowering as {@link TypedLet}
 * statements (the classic {@code lower(List)} path records them as
 * letBindings) so a surviving {@code $let} read — a graph-tree
 * qualifier ARG, engine inScopeVars — resolves at the leaf. A let whose
 * value has no scalar lowering (runtime handles, relation frames) is
 * not seeded: the SEEDABILITY PROBE is a trial lowering, which is why
 * this lives in the lowering package (Invariant 7: the executor asks,
 * the lowering answers); reads of an unseeded let keep their existing
 * loud walls.
 */
public final class SeedableLets {

    private SeedableLets() {
    }

    public static List<TypedSpec> withSeedableLetPrefix(List<TypedSpec> body,
            Map<String, TypedSpec> queryLets, ModelContext ctx) {
        List<TypedSpec> out = new ArrayList<>();
        for (var qe : queryLets.entrySet()) {
            var let = new TypedLet(qe.getKey(), qe.getValue(),
                    qe.getValue().info());
            try {
                new Lowerer(t -> ClassLayouts.layoutOf(ctx, t),
                        f -> ctx.findClass(f).isPresent(), ctx.implementations())
                        .lower(List.of(let, qe.getValue()));
                out.add(let);
            } catch (RuntimeException notScalar) {
                // BROAD BY DESIGN (ErrorShapeGuardrail pin, reviewed
                // 2026-08-21): this is a trial-lowering PROBE — "can this
                // let seed?" — and ANY lowering failure (NotImplemented,
                // IllegalState, UnfoldableRef, dialect capability) means
                // the same thing: not seedable. Reads of the unseeded let
                // keep their own loud walls, so nothing is swallowed.
                continue;
            }
        }
        out.addAll(body);
        return out;
    }
}
