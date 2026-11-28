// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.spec.InferenceKernel;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.FunctionId;

import java.util.ArrayList;
import java.util.List;

/**
 * A CALL BUILT AT A COMPATIBILITY DOOR, TYPED AS THE TYPER TYPES IT (D-S2 of the A3 design's review,
 * docs/records/compat-pr-reviews-2026-11-19.md): the callee is the registry's function for one of the native's ids,
 * chosen by the typer's own overload resolution ({@link InferenceKernel#resolveOverload}) over the call's typed
 * arguments, and its type is that resolution's output. Neither a signature nor an {@code ExprType} is assembled here
 * (AGENTS.md invariant 1: the frontend does all typing).
 *
 * <p>Until the edition program's step 2 (2026-11-25) this class also built the substitutes legend-engine's door put in
 * the typed query for {@code substring}, {@code substr}, {@code indexOf} and {@code toOne}; the lowering's edition table
 * ({@link EngineTable}) names those natives now, and nothing rewrites the query for them.
 */
final class EngineSubstitutes {

    private EngineSubstitutes() {
    }

    /** A NEW call of the registry's function for {@code nativeIds} that the typer picks for {@code args}, at
     *  {@code pos}, its type the typer's (B3, 2026-11-24: the test-body door's explicit order-free assert,
     *  {@code UpstreamTestBodies.joinsOrderFree}); neither a signature nor a type assembled here. */
    static TypedNativeCall call(ModelContext ctx, List<FunctionId> nativeIds, List<TypedSpec> args,
            com.legend.protocol.@com.legend.base.Nullable SourceInfo pos) {
        InferenceKernel.Resolution r = resolve(ctx, args, nativeIds);
        return new TypedNativeCall(r.chosen(), args, r.output(), pos);
    }

    /** The typer's choice among the registry's functions for {@code nativeIds}, over {@code args}' types. */
    private static InferenceKernel.Resolution resolve(ModelContext ctx, List<TypedSpec> args,
            List<FunctionId> nativeIds) {
        List<TypedFunction> registry = new ArrayList<>();
        for (FunctionId id : nativeIds) {
            registry.addAll(ctx.findFunctionById(id.qualified()));
        }
        return new InferenceKernel(ctx).resolveOverload(registry, args.stream().map(TypedSpec::info).toList());
    }
}
