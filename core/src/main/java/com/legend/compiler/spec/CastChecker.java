package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTypeRef;
import com.legend.parser.spec.AppliedFunction;

/**
 * Type conversions (engine {@code CastChecker} + {@code TypeConversionChecker}):
 * {@code cast<T|m>(Any[m], type:T[1]):T[m]}, {@code to<T,V>(T[0..1], type:V[0..1]):V[0..1]},
 * {@code toMany<T,V>(T[0..1], type:V[0..1]):V[*]}. All fully generic &mdash; the
 * {@code @Type} argument synthesizes as a prototype value of the target
 * ({@link TypedTypeRef}), the signature's target variable binds from it, and the
 * output type/multiplicity come from {@code resolveOutput}. This class only emits
 * the {@link TypedCast} node lowering dispatches on. ({@code toOne}/{@code toVariant}
 * have no {@code @Type} argument and ride the generic path as plain natives.)
 */
final class CastChecker {

    private CastChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        if (a.args().size() != 2 || !(a.args().get(1) instanceof TypedTypeRef ref)) {
            throw new TypeInferenceException(af.function() + " expects (source, @Type)");
        }
        return new TypedCast(a.args().get(0), ref.target(), a.out());
    }
}
