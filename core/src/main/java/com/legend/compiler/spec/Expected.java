package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
/**
 * The bidirectional-typing mode passed down into body type-checking
 * (PHASE_G_SPEC_COMPILER.md §2; GHC's {@code Expected}). Either we are
 * <em>checking</em> an expression against a known type ({@link Check} &mdash;
 * e.g. a user function's body against its declared return), or we are
 * <em>inferring</em> ({@link Infer}) and the synthesized type is simply read off
 * the produced node.
 */
public sealed interface Expected permits Expected.Check, Expected.Infer {

    /** Check the expression against this expected type. */
    record Check(ExprType expected) implements Expected { }

    /** Infer (synthesize) the expression's type with no constraint. */
    record Infer() implements Expected { }

    static Expected check(ExprType expected) {
        return new Check(expected);
    }

    static Expected infer() {
        return new Infer();
    }
}
