// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;


import com.legend.platform.CoreFn;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTestDataGen;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.ValueSpecification;

import java.util.List;

/**
 * {@code generateTestData(func, mapping, runtime, ...)} — TDG lane S2.
 * A RUNTIME data-extraction native (fetches execute through the
 * database), so no fold happens here: the checker validates the shape
 * (this CoreFn arm owns EVERY engine overload — 8 spellings differing
 * in parameters/exeCtx/hashStrings, testDataGeneration.pure:104-144 —
 * classified structurally by the orchestrator's parser) and emits the
 * protocol-capturing carrier {@link TypedTestDataGen}.
 */
public final class GenerateTestDataChecker {

    private static final String RESULT_FQN =
            "meta::relational::testDataGeneration::TestDataGenResult";

    private GenerateTestDataChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        // bind-once (family E): a let-bound query lambda / mapping ref
        // resolves through the alias channel to the same view an inline
        // call presents (SourceSubst — the mechanism this wall named).
        List<ValueSpecification> args =
                SourceSubst.resolveStructuralArgs(af.parameters(), env);
        if (args.size() < 4
                || !(args.get(0) instanceof LambdaFunction)
                || !(args.get(1) instanceof PackageableElementPtr)) {
            throw new TypeInferenceException(
                    "generateTestData needs its query lambda and mapping"
                            + " reference INLINE at the call site (>=4 args)");
        }
        return new TypedTestDataGen(args, "generate",
                new ExprType(new Type.ClassType(RESULT_FQN),
                        Multiplicity.Bounded.ONE));
    }

    /** {@code planTestDataGeneration(func, mapping, runtime, exeCtx,
     * rowIds..., hashStrings, [dates], ext)} — the TDG PLAN: the same
     * carrier, flavor {@code plan}, typed ExecutionPlan[1]; its
     * planToString prints the engine's MultiResultSequence text. */
    static TypedSpec checkPlan(Typer t, AppliedFunction af, Env env) {
        List<ValueSpecification> args = new java.util.ArrayList<>(
                SourceSubst.resolveStructuralArgs(af.parameters(), env));
        // a let-bound row-identifier list (`let ids = []; ...($ids, ...)`)
        // binds through the alias chase (the carrier captures protocol)
        for (int i = 4; i < args.size(); i++) {
            if (args.get(i) instanceof com.legend.protocol.spec.Variable v) {
                ValueSpecification r = env.resolveAlias(v);
                if (r != v && !(r instanceof com.legend.protocol.spec.Variable)) {
                    // hoisted constructor lets inside the value adopt too
                    args.set(i, env.aliases().isEmpty() ? r
                            : SourceSubst.substitute(r, env.aliases()));
                }
            }
        }
        if (args.size() < 5
                || !(args.get(0) instanceof LambdaFunction)
                || !(args.get(1) instanceof PackageableElementPtr)) {
            throw new TypeInferenceException(
                    "planTestDataGeneration needs its query lambda and"
                            + " mapping reference INLINE at the call site"
                            + " (>=5 args)");
        }
        return new TypedTestDataGen(args, "plan",
                new ExprType(new Type.ClassType(
                        "meta::pure::executionPlan::ExecutionPlan"),
                        Multiplicity.Bounded.ONE));
    }

    /** {@code generateSeedDataString(...)} — same carrier, String[1]. */
    static TypedSpec checkSeed(Typer t, AppliedFunction af, Env env) {
        List<ValueSpecification> args =
                SourceSubst.resolveStructuralArgs(af.parameters(), env);
        if (args.size() < 2
                || !(args.get(0) instanceof LambdaFunction)
                || !(args.get(1) instanceof PackageableElementPtr)) {
            throw new TypeInferenceException(
                    "generateSeedDataString needs its query lambda and"
                            + " mapping reference INLINE at the call site");
        }
        return new TypedTestDataGen(args, "seedString",
                new ExprType(Type.Primitive.STRING,
                        Multiplicity.Bounded.ONE));
    }
}
