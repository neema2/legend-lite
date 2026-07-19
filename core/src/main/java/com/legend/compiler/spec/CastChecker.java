package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTypeRef;
import com.legend.model.spec.AppliedFunction;

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
        // cast(@TabularDataSet) over a relation value: the engine's TDS IS
        // the relation carrier — a type ASSERTION, identity by emission;
        // the SOURCE (with its row schema) is the result, so downstream
        // rows.getString('col') keeps the columns. A non-relation source
        // falls through to the plain TypedCast and stays loud downstream.
        if (ref.target() instanceof com.legend.compiler.element.type.Type.GenericType g
                && com.legend.compiler.element.type.PlatformTypes.TABULAR_DATA_SET
                        .equals(g.rawFqn())
                && a.args().get(0).info().type()
                        instanceof com.legend.compiler.element.type.Type.RelationType) {
            return a.args().get(0);
        }
        return new TypedCast(a.args().get(0), ref.target(), a.out());
    }
}
