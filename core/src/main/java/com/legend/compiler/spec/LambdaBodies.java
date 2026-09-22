// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import java.util.ArrayList;
import java.util.List;

/** The ONE rule for a multi-statement lambda body (batch 54): lets inline,
 * a leading let substitutes forward, {@code fail(..); v} is the raise typed
 * at the value, {@code assert(c, m); v} is {@code if(c, |v, |fail(m))}. */
final class LambdaBodies {

    /** The assert family as body GUARDS: FQN → how many leading arguments
     * form the condition (the rest is the message). */
    private static final java.util.Map<String, Integer> ASSERT_GUARDS = java.util.Map.of(
            "meta::pure::functions::asserts::assert", 1,
            "meta::pure::functions::asserts::assertTrue", 1,
            "meta::pure::functions::asserts::assertFalse", 1,
            "meta::pure::functions::asserts::assertEquals", 2,
            "meta::pure::functions::asserts::assertNotEquals", 2,
            "meta::pure::functions::asserts::assertEmpty", 1,
            "meta::pure::functions::asserts::assertNotEmpty", 1);

    private LambdaBodies() {
    }

    /** A lambda BODY as one typed expression — the one rule for every
     * body-consuming checker (match arms, if branches): a single
     * expression as is; {@code [let*, final]} by source-level let-inlining
     * ({@link SourceSubst#inlineLets}); {@code [fail(..)+, value]} as the
     * raise typed at the unreachable value ({@link IfChecker#failThenValue}
     * — toPostgresModel's TableFunctionParamPlaceHolder arm); anything else
     * stays loud, never a silently dropped statement. */
    static TypedSpec synthBody(Typer t, LambdaFunction lam, Env scope) {
        if (lam.body().size() == 1) {
            return t.synth(lam.body().get(0), scope);
        }
        LambdaFunction folded = SourceSubst.inlineLets(lam);
        if (folded != null) {
            return t.synth(folded.body().get(0), scope);
        }
        // a LEADING let before a non-let statement (let c = …; assert(..); …):
        // substitute it forward and take the rest through the same rule
        com.legend.protocol.spec.CString leading = SourceSubst.letName(lam.body().get(0));
        if (leading != null && lam.body().size() > 1) {
            ValueSpecification bound = ((AppliedFunction) lam.body().get(0)).parameters().get(1);
            List<ValueSpecification> rest = new ArrayList<>();
            for (ValueSpecification s : lam.body().subList(1, lam.body().size())) {
                rest.add(SourceSubst.substitute(s, java.util.Map.of(leading.value(), bound)));
            }
            return synthBody(t, new LambdaFunction(lam.parameters(), rest), scope);
        }
        TypedSpec asFail = IfChecker.failThenValue(t, lam, scope);
        if (asFail != null) {
            return asFail;
        }
        TypedSpec guarded = assertThenValue(t, lam, scope);
        if (guarded != null) {
            return guarded;
        }
        // a leading EXPRESSION statement whose value is discarded (real pure
        // sequences it: `if(..., |fail(..), |fail(..)); [];`) — TYPED, so
        // its errors surface, then the rest is the value. Its effect is a
        // run-time matter this one-expression view cannot carry: a body
        // reached at the lowering with such a statement is the lowering's
        // to refuse (SYSTEM_PRELUDE_DESIGN §5 — typing asks fit, not run).
        t.synth(lam.body().get(0), scope);
        return synthBody(t, new LambdaFunction(lam.parameters(),
                lam.body().subList(1, lam.body().size())), scope);
    }

    /** {@code assert(cond, msg); rest} is {@code if(cond, |rest, |fail(msg))}
     * — the raise semantics exactly (the database raises when the condition
     * is false; a spelled condition folds the guard away in the unroll —
     * toPostgresModel's converter-registry self-check). Null = not this
     * shape (the first statement is not an assert). */
    private static @com.legend.base.Nullable TypedSpec assertThenValue(Typer t, LambdaFunction lam, Env scope) {
        ValueSpecification first = lam.body().get(0);
        if (!(first instanceof AppliedFunction af) || af.parameters().isEmpty()) {
            return null;
        }
        // RESOLVED dispatch: every candidate the name resolves to is the
        // asserts::assert native (never a spelling compare)
        List<TypedFunction> cands = t.functionCandidates(af);
        if (cands.isEmpty()) {
            return null;
        }
        String fqn = cands.get(0).qualifiedName();
        if (!cands.stream().allMatch(f -> f.qualifiedName().equals(fqn))
                || !ASSERT_GUARDS.containsKey(fqn)) {
            return null;
        }
        // the guard's CONDITION and how many leading arguments it consumes
        // (asserts.pure: each assertX(args, [message]) is assert(cond, message))
        int condArgs = ASSERT_GUARDS.get(fqn);
        if (af.parameters().size() < condArgs) {
            return null;
        }
        List<ValueSpecification> condIn = af.parameters().subList(0, condArgs);
        ValueSpecification condVs = switch (fqn.substring(fqn.lastIndexOf(':') + 1)) {
            case "assert", "assertTrue" -> condIn.get(0);
            case "assertFalse" -> new AppliedFunction("not", condIn);
            case "assertEquals" -> new AppliedFunction("equal", condIn);
            case "assertNotEquals" -> new AppliedFunction("not",
                    List.of(new AppliedFunction("equal", condIn)));
            case "assertEmpty" -> new AppliedFunction("isEmpty", condIn);
            case "assertNotEmpty" -> new AppliedFunction("isNotEmpty", condIn);
            default -> throw new IllegalStateException(fqn);
        };
        TypedSpec cond = t.synth(condVs, scope);
        ExprType str = new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE);
        List<ValueSpecification> msgArgs = af.parameters().subList(condArgs, af.parameters().size());
        TypedSpec message;
        if (msgArgs.isEmpty()) {
            message = new com.legend.compiler.spec.typed.TypedCString("Assert failed", str);
        } else if (msgArgs.get(0) instanceof LambdaFunction thunk
                && thunk.parameters().isEmpty()) {
            message = synthBody(t, thunk, scope);
        } else if (msgArgs.size() == 1) {
            message = t.synth(msgArgs.get(0), scope);
        } else {
            // assert(cond, format, args) — the formatted message
            message = t.synth(new AppliedFunction("format", msgArgs), scope);
        }
        TypedSpec rest = synthBody(t, new LambdaFunction(lam.parameters(),
                lam.body().subList(1, lam.body().size())), scope);
        var fail = t.model().findFunction("meta::pure::functions::asserts::fail").stream()
                .filter(f -> f.parameters().size() == 1).findFirst()
                .orElseThrow(() -> new TypeInferenceException("fail(message) is not registered"));
        TypedSpec raise = new TypedNativeCall(fail, List.of(message), rest.info());
        return new com.legend.compiler.spec.typed.TypedIf(cond, rest,
                java.util.Optional.of(raise), rest.info());
    }

}
