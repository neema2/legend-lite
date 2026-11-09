// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.error.TypeInferenceException;

import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.LambdaFunction;

/**
 * {@code mayExecuteAlloyTest(f1, f2)} / {@code mayExecuteLegendTest(f1, f2)}
 * (engine {@code core_functions_unclassified/test.pure:15-19}) &mdash; the
 * test-harness BRANCH natives: {@code f1} runs against a live Alloy/Legend
 * server, {@code f2} is the no-server branch. This platform has no server,
 * so the call IS the fallback thunk's body &mdash; the same branch the
 * engine's own serverless CI takes and the walk splices
 * ({@code EngineTestExecutor.alloyFallback}). The server thunk is
 * deliberately never typed: server-only vocabulary lives there, and typing
 * a branch that can never run would manufacture walls. A let-heavy
 * fallback folds through {@link SourceSubst#inlineLets}; any other shape
 * stays loud.
 */
final class MayExecuteChecker {

    private MayExecuteChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() == 2
                && env.resolveAlias(af.parameters().get(1))
                        instanceof LambdaFunction fb
                && fb.parameters().isEmpty()) {
            LambdaFunction folded = SourceSubst.inlineLets(fb);
            if (folded != null) {
                return t.synth(folded.body().get(0), env);
            }
        }
        throw new TypeInferenceException(af.function() + " needs a zero-arg"
                + " fallback thunk of lets + one expression (the engine's"
                + " no-server branch is this platform's ONLY branch)");
    }
}
