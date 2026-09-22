package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTypeRef;
import com.legend.protocol.spec.AppliedFunction;

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
                && com.legend.compiler.element.type.Type
                        .isRelation(a.args().get(0).info().type())) {
            return a.args().get(0);
        }
        // cast(@T) over a value whose STATIC class already conforms to T is
        // the identity (real pure: a cast never narrows a value that is
        // already of the type) — the source stands, so a navigation read
        // through it ($c.owner->cast(@Table).name) keeps its property-path
        // shape for the resolver's slot demand. Only cast itself: to/toMany
        // over a Variant DECODE (an array explodes), never the identity.
        if (a.chosen().qualifiedName().equals("meta::pure::functions::lang::cast")
                && ref.target() instanceof com.legend.compiler.element.type.Type.ClassType tc
                && a.args().get(0).info().type()
                        instanceof com.legend.compiler.element.type.Type.ClassType sc
                && !com.legend.compiler.element.type.PlatformTypes.isVariant(sc)
                && (sc.fqn().equals(tc.fqn()) || t.model().isSubtype(sc.fqn(), tc.fqn()))) {
            return a.args().get(0);
        }
        TypedSpec lam = deactivatedLambda(a.args().get(0), ref.target());
        if (lam != null) {
            return lam;
        }
        // a lambda LITERAL cast to a function carrier
        // (cast(lambda, @FunctionDefinition<Any>)) is the lambda: identity
        if (a.args().get(0) instanceof com.legend.compiler.spec.typed.TypedLambda lit
                && ref.target() instanceof com.legend.compiler.element.type.Type.GenericType g2
                && InferenceKernel.FUNCTION_CARRIER_FQNS.contains(g2.rawFqn())) {
            return lit;
        }
        return new TypedCast(a.args().get(0), ref.target(), a.out(), false);
    }

    /** {@code {..}->deactivate()->cast(@InstanceValue).values->at(0)
     * ->cast(@LambdaFunction<..>)} — the reflection round trip real pure
     * makes of a lambda literal (deactivate wraps it in an InstanceValue
     * whose single value is the lambda): the lambda itself. */
    private static @com.legend.base.Nullable TypedSpec deactivatedLambda(
            TypedSpec src, com.legend.compiler.element.type.Type target) {
        if (!(target instanceof com.legend.compiler.element.type.Type.GenericType g
                && InferenceKernel.FUNCTION_CARRIER_FQNS.contains(g.rawFqn()))) {
            return null;
        }
        if (src instanceof com.legend.compiler.spec.typed.TypedNativeCall at
                && at.args().size() == 2
                && "meta::pure::functions::collection::at".equals(at.callee().qualifiedName())
                && at.args().get(1) instanceof com.legend.compiler.spec.typed.TypedCInteger k
                && k.value().longValue() == 0
                && at.args().get(0) instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.property().equals("values")
                && pa.source() instanceof TypedCast iv
                && iv.target() instanceof com.legend.compiler.element.type.Type.ClassType ic
                && ic.fqn().equals("meta::pure::metamodel::valuespecification::InstanceValue")
                && iv.source() instanceof com.legend.compiler.spec.typed.TypedDeactivate d
                && d.inner() instanceof com.legend.compiler.spec.typed.TypedLambda lam) {
            return lam.asQuoted();
        }
        return null;
    }
}
