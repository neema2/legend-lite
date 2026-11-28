// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * SPEC-SOUND constant folds over typed nodes — ONE owner, applied at
 * TYPING (the emit seam) and at INLINING (β-substitution creates the
 * literal shapes typing could not see). Real pure evaluates these at
 * compile time; the platform must too, or a statically-dead branch
 * still lowers — assertEquals' size-1 fast path inlines an {@code if}
 * whose dead branch cannot lower over the other physical shape
 * (Phase 4 channel B surfaced this across the essential suite).
 *
 * <ul>
 * <li>{@code size(x)} where x's MULTIPLICITY is exactly [n..n] (a
 * type-level truth) and x is not relation-shaped (a relation is one
 * VALUE; its size is its row count) → the integer literal.</li>
 * <li>{@code eq/equal} over two integer literals → the boolean.</li>
 * <li>{@code and/or} over boolean literals → the boolean.</li>
 * <li>{@code TypedIf} with a literal condition → the taken branch
 * (the dead branch is DROPPED, never lowered).</li>
 * </ul>
 */
public final class NormalizeFolds {

    private NormalizeFolds() {
    }

    /** The literal-{@code if} prune — INLINED-BODY provenance ONLY
     * (engine parity: user-authored query ifs keep BOTH branches in SQL
     * — testIfIncludingQualifiers pins the CASE WHEN TRUE with both
     * joins; the engine's interpreter folds, its plan generator does
     * not. An if arising from an inlined platform function — the
     * assert plumbing — was never SQL in the engine and folds). */
    public static TypedSpec foldInlined(TypedSpec call) {
        if (call instanceof TypedIf ti
                && ti.condition() instanceof TypedCBoolean cb) {
            if (cb.value()) {
                return ti.thenBranch();
            }
            return ti.elseBranch().isPresent() ? ti.elseBranch().get()
                    : new com.legend.compiler.spec.typed.TypedCollection(
                            java.util.List.of(),
                            new ExprType(Type.Primitive.BOOLEAN,
                                    Multiplicity.Bounded.ZERO_ONE));
        }
        return fold(call);
    }

    /** The reflection identity applied at EMISSION (every native call):
     * evaluateAndDeactivate over a LAMBDA LITERAL is the literal (real
     * pure: a lambda evaluates to itself; the generic {@code <T|m>}
     * signature would strip the function carrier and lose
     * {@code .expressionSequence}). */
    public static TypedSpec foldReflection(TypedSpec call) {
        if (call instanceof TypedNativeCall nc
                && "meta::pure::functions::meta::evaluateAndDeactivate"
                        .equals(nc.callee().qualifiedName())
                && nc.args().size() == 1
                && nc.args().get(0) instanceof com.legend.compiler.spec.typed.TypedLambda lam) {
            return lam;
        }
        return call;
    }

    public static TypedSpec fold(TypedSpec call) {
        if (!(call instanceof TypedNativeCall nc)) {
            return call;
        }
        String fqn = com.legend.builtin.Comparisons.name(nc.callee().qualifiedName());
        if (("meta::pure::functions::collection::size".equals(fqn)
                    || "meta::pure::functions::relation::size".equals(fqn))
                && nc.args().size() == 1) {
            TypedSpec arg = nc.args().get(0);
            boolean relationish = Type.relationValued(arg.info())
                    || PlatformTypes.isTdsType(arg.info().type());
            if (!relationish && arg.info().multiplicity()
                        instanceof Multiplicity.Bounded b
                    && b.upper() != null && b.lower() == b.upper()) {
                return new TypedCInteger((long) b.lower(),
                        ExprType.one(Type.Primitive.INTEGER));
            }
        }
        if (("meta::pure::functions::boolean::eq".equals(fqn)
                    || "meta::pure::functions::boolean::equal".equals(fqn))
                && nc.args().size() == 2
                && nc.args().get(0) instanceof TypedCInteger l
                && nc.args().get(1) instanceof TypedCInteger r) {
            // exact (TypedCInteger.big), as LiteralUnroll's compares:
            // longValue() wraps a literal beyond long
            return new TypedCBoolean(
                    l.big().equals(r.big()),
                    ExprType.one(Type.Primitive.BOOLEAN));
        }
        boolean isAnd = "meta::pure::functions::boolean::and".equals(fqn);
        if ((isAnd || "meta::pure::functions::boolean::or".equals(fqn))
                && nc.args().size() == 2
                && nc.args().get(0) instanceof TypedCBoolean bl
                && nc.args().get(1) instanceof TypedCBoolean br) {
            return new TypedCBoolean(
                    isAnd ? bl.value() && br.value()
                            : bl.value() || br.value(),
                    ExprType.one(Type.Primitive.BOOLEAN));
        }
        return call;
    }
}
