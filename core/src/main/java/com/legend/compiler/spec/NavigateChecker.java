package com.legend.compiler.spec;

import com.legend.compiler.element.Property;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.ColSpec;
import com.legend.parser.spec.LambdaFunction;

import java.util.List;
import java.util.Optional;

/**
 * {@code navigate} (MAPPING_CLEAN_SHEET.md §3) &mdash; the clean-sheet
 * graph-traversal primitive replacing engine's {@code traverse}/extend-nav
 * variants. Dispatch is by shape: arity 2 = the inline constructor-slot form
 * (fully generic); arity 3 dispatches on the source &mdash; a Relation takes the
 * pre-map (sub-row widening) rule, a class collection the post-map
 * (declared-property fill) rule. Every path validates against its registered
 * signature; only the pre-map {@code Z} binding is bespoke (the design fixes the
 * sub-row column at {@code [1]} per output row, §3.4, which the thunk's
 * {@code T[*]} body multiplicity cannot express).
 */
final class NavigateChecker {

    private NavigateChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() == 2) {
            return inline(t, af, env);
        }
        if (af.parameters().size() != 3) {
            throw new TypeInferenceException(
                    "navigate expects (source, ~alias: Target.all(), {s,t|pred}) or (Target.all(), {t|pred})");
        }
        TypedSpec source = t.synth(af.parameters().get(0), env);
        return source.info().type() instanceof Type.RelationType
                ? preMap(t, af, source, env)
                : postMap(t, af, source, env);
    }

    /** Inline slot form {@code navigate(T.all(), {t|pred})} — fully generic; {@code T[*]}. */
    private static TypedSpec inline(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        if (!(a.args().get(1) instanceof TypedLambda pred)) {
            throw new TypeInferenceException("navigate expects a predicate lambda");
        }
        return new TypedNavigate(a.args().get(0), Optional.empty(), a.args().get(0),
                pred, TypedNavigate.Form.INLINE, a.out());
    }

    /** Pre-map: widen {@code Relation<S>} with a named class-typed sub-row, {@code S + (alias:Target[1])}. */
    private static TypedSpec preMap(Typer t, AppliedFunction af, TypedSpec source, Env env) {
        TypedFunction sig = overload(t, p -> p.type() instanceof Type.GenericType);
        Bindings b = new Bindings();
        t.kernel().unify(sig.parameters().get(0).type(), source.info().type(), b);
        t.kernel().unifyMult(sig.parameters().get(0).multiplicity(),
                source.info().multiplicity(), source.info().type(), b);

        Parts parts = parts(t, sig, af, b, env);
        // Z = (alias : Target[1]) — §3.4: rows multiply, the SUB-ROW COLUMN is to-one.
        b.bindType(schemaVar(sig), new Type.RelationType(List.of(
                new Type.Column(parts.alias(), parts.targetClass(), Multiplicity.Bounded.ONE))));
        TypedLambda pred = (TypedLambda) t.typeLambda(parts.predLam(), sig.parameters().get(2).type(), b, env);

        ExprType out = t.kernel().resolveOutput(sig.returnType(), sig.returnMultiplicity(), b);
        return new TypedNavigate(source, Optional.of(parts.alias()), parts.target(),
                pred, TypedNavigate.Form.PRE_MAP, out);
    }

    /** Post-map: fill a DECLARED property of the class source; the {@code C[*]} passes through. */
    private static TypedSpec postMap(Typer t, AppliedFunction af, TypedSpec source, Env env) {
        if (!(source.info().type() instanceof Type.ClassType ct)) {
            throw new TypeInferenceException("navigate requires a relation or class-collection source, got "
                    + source.info().type().typeName());
        }
        TypedFunction sig = overload(t, p -> p.type() instanceof Type.TypeVar);
        Bindings b = new Bindings();
        t.kernel().unify(sig.parameters().get(0).type(), source.info().type(), b);
        t.kernel().unifyMult(sig.parameters().get(0).multiplicity(),
                source.info().multiplicity(), source.info().type(), b);

        Parts parts = parts(t, sig, af, b, env);
        // The slot must be a DECLARED property whose type accepts the navigated target (§3.3).
        Property prop = t.model().findProperty(ct.fqn(), parts.alias()).orElseThrow(() ->
                new TypeInferenceException("navigate: class " + ct.fqn()
                        + " has no property '" + parts.alias() + "' to fill"));
        if (!t.kernel().accepts(prop.type(), parts.targetClass())) {
            throw new TypeInferenceException("navigate: property '" + parts.alias() + "' is "
                    + prop.type().typeName() + ", not " + parts.targetClass().typeName());
        }
        TypedLambda pred = (TypedLambda) t.typeLambda(parts.predLam(), sig.parameters().get(2).type(), b, env);

        ExprType out = t.kernel().resolveOutput(sig.returnType(), sig.returnMultiplicity(), b);
        return new TypedNavigate(source, Optional.of(parts.alias()), parts.target(),
                pred, TypedNavigate.Form.POST_MAP, out);
    }

    /** The shared middle: the {@code ~alias: Target.all()} colspec — thunk typed, {@code T} bound. */
    private record Parts(String alias, TypedSpec target, Type targetClass, LambdaFunction predLam) {
    }

    private static Parts parts(Typer t, TypedFunction sig, AppliedFunction af, Bindings b, Env env) {
        if (!(af.parameters().get(1) instanceof ColSpec cs) || cs.function1() == null
                || !cs.function1().parameters().isEmpty()
                || !(af.parameters().get(2) instanceof LambdaFunction predLam)) {
            throw new TypeInferenceException(
                    "navigate expects (source, ~alias: Target.all(), {s,t|pred})");
        }
        // Type the target thunk against the signature's {->T[*]} — binds T generically.
        Type.GenericType colspecParam = (Type.GenericType) sig.parameters().get(1).type();
        TypedLambda thunk = (TypedLambda) t.typeLambda(cs.function1(),
                colspecParam.arguments().get(0), b, env);
        Type target = ((Type.FunctionType) thunk.info().type()).result().type();
        if (!(target instanceof Type.ClassType)) {
            throw new TypeInferenceException(
                    "navigate target must be a class extent (Class.all()), got " + target.typeName());
        }
        return new Parts(cs.name(), thunk.body().get(0), target, predLam);
    }

    /** The schema variable {@code Z} of the colspec parameter {@code FuncColSpec<{->T[*]}, Z>}. */
    private static String schemaVar(TypedFunction sig) {
        Type.GenericType g = (Type.GenericType) sig.parameters().get(1).type();
        return ((Type.TypeVar) g.arguments().get(1)).name();
    }

    /** The 3-arity navigate overload whose FIRST parameter matches {@code sourceParam}. */
    private static TypedFunction overload(Typer t, java.util.function.Predicate<com.legend.compiler.element.TypedParameter> sourceParam) {
        return t.model().findFunction(CoreFn.NAVIGATE.parseName()).stream()
                .filter(c -> c.parameters().size() == 3 && sourceParam.test(c.parameters().get(0)))
                .findFirst()
                .orElseThrow(() -> new TypeInferenceException(
                        "no matching navigate overload is registered"));
    }
}
