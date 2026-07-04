package com.legend.compiler.spec;

import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Shared bespoke-path helpers for construct checkers that validate a call
 * against a registered signature <em>parameter by parameter</em> (the
 * prefix-carrying joins, whose output the signature algebra cannot express).
 */
final class Checkers {

    private Checkers() {
    }

    /** Synthesize argument {@code i} and unify it against the signature's parameter {@code i}. */
    static TypedSpec unifiedArg(Typer t, TypedFunction sig, int i, AppliedFunction af,
                                Bindings b, Env env) {
        TypedSpec arg = t.synth(af.parameters().get(i), env);
        t.kernel().unify(sig.parameters().get(i).type(), arg.info().type(), b);
        t.kernel().unifyMult(sig.parameters().get(i).multiplicity(),
                arg.info().multiplicity(), arg.info().type(), b);
        return arg;
    }

    /** Argument {@code i} as a checked string literal. */
    static String stringLiteralArg(Typer t, AppliedFunction af, int i, Env env, String what) {
        if (t.synth(af.parameters().get(i), env) instanceof TypedCString s) {
            return s.value();
        }
        throw new TypeInferenceException(what + " must be a string literal");
    }

    /**
     * The prefixed schema union of two relation values: all left columns, then
     * every right column renamed {@code prefix + name} when {@code renameWhen}
     * accepts it (both joins prefix ALL right columns). Residual collisions
     * (e.g. a prefixed name colliding with a left column) still fail loudly via
     * the relation type's uniqueness invariant.
     */
    static Type.RelationType prefixedUnion(TypedSpec left, TypedSpec right, String prefix,
                                           Predicate<Type.Column> renameWhen) {
        if (!(left.info().type() instanceof Type.RelationType lr)
                || !(right.info().type() instanceof Type.RelationType rr)) {
            throw new TypeInferenceException("both join sides must be relations");
        }
        List<Type.Column> cols = new ArrayList<>(lr.columns());
        for (Type.Column c : rr.columns()) {
            cols.add(renameWhen.test(c)
                    ? new Type.Column(prefix + c.name(), c.type(), c.multiplicity())
                    : c);
        }
        return new Type.RelationType(cols);
    }
}
