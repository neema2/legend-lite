// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.spec.InferenceKernel;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.FunctionId;

import java.util.ArrayList;
import java.util.List;

/**
 * THE {@code engine} REWRITE'S SUBSTITUTES, TYPED AS THE TYPER TYPES THEM (A3 of the compatibility program,
 * 2026-11-19; D-S2 of the design's review, docs/records/compat-pr-reviews-2026-11-19.md). legend-engine sends a
 * store-bound Pure {@code substring} and String {@code indexOf} to the database as its own functions over Pure's
 * arguments as given. Under the door's {@code engine} setting, the rewrite puts a lite-internal native in each such call's
 * place:
 * <ul>
 *   <li>{@code substring} becomes {@code meta::legend::lite::substringAsGiven}, the database's own SUBSTRING
 *       ({@link Pure.Lite#SUBSTRING_AS_GIVEN});</li>
 *   <li>the two-argument String {@code indexOf} becomes {@code meta::legend::lite::sqlPosition}, the database's own
 *       find function ({@link Pure.Lite#SQL_POSITION}).</li>
 * </ul>
 *
 * <p>A substitute's callee is the registry's function for one of its native's ids, chosen by the typer's own overload
 * resolution ({@link InferenceKernel#resolveOverload}) over the call's typed arguments, and its type is that
 * resolution's output. Neither a signature nor an {@code ExprType} is assembled here (AGENTS.md invariant 1: the
 * frontend does all typing). {@link #checked} holds every substitute to that, whoever built it, so a substitute built any
 * other way is refused before it is planned. Which calls are replaced is the rewrite's decision, never this class's: it
 * refuses a call that is not the Pure overload a substitute stands for.
 */
final class EngineSubstitutes {

    private EngineSubstitutes() {
    }

    /** The substitute for Pure's String {@code substring} (its 2- or 3-argument overload): the same typed arguments and
     *  source position, {@code substringAsGiven}'s overload for their arity, its type the typer's. */
    static TypedNativeCall substring(ModelContext ctx, TypedNativeCall call) {
        return substitute(ctx, call, Pure.AT_STRING_SUBSTRING, Pure.AT_LEGEND_LITE_SUBSTRING_AS_GIVEN);
    }

    /** The substitute for Pure's 2-argument String {@code indexOf}: the same typed arguments and source position,
     *  {@code sqlPosition}, its type the typer's. The 3-argument {@code indexOf} has none. */
    static TypedNativeCall indexOf(ModelContext ctx, TypedNativeCall call) {
        return substitute(ctx, call, List.of(FunctionId.of(Pure.INDEX_OF__STRING_1__STRING_1)),
                Pure.AT_LEGEND_LITE_SQL_POSITION);
    }

    /** {@code call} as given, or refused: its callee must be the registry's function, for one of {@code nativeIds},
     *  that the typer picks for its arguments, and its type that function's output. */
    static TypedNativeCall checked(ModelContext ctx, TypedNativeCall call, List<FunctionId> nativeIds) {
        InferenceKernel.Resolution r = resolve(ctx, call.args(), nativeIds);
        if (!call.callee().equals(r.chosen())) {
            throw new IllegalStateException("the engine rewrite's substitute calls " + describe(call.callee())
                    + ", not the registry's " + describe(r.chosen()) + " that the typer picks for its arguments");
        }
        if (!call.info().equals(r.output())) {
            throw new IllegalStateException("the engine rewrite's substitute is typed " + call.info()
                    + ", not " + r.output() + ", the type the typer gives " + describe(r.chosen()));
        }
        return call;
    }

    private static TypedNativeCall substitute(ModelContext ctx, TypedNativeCall call, List<FunctionId> replaces,
            List<FunctionId> nativeIds) {
        if (!replaces.contains(call.callee().id())) {
            throw new IllegalStateException("the engine rewrite substitutes " + replaces + " alone, not "
                    + describe(call.callee()));
        }
        InferenceKernel.Resolution r = resolve(ctx, call.args(), nativeIds);
        return checked(ctx, new TypedNativeCall(r.chosen(), call.args(), r.output(), call.pos()), nativeIds);
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

    private static String describe(TypedFunction f) {
        return f.qualifiedName() + "/" + f.parameters().size() + ":" + f.returnType().typeName()
                + f.returnMultiplicity();
    }
}
