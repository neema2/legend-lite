package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedFlatten;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.ColSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * Relation {@code flatten(~col)} (engine {@code FlattenChecker}) &mdash; unnests
 * a semi-structured column: the output is the SOURCE schema with the named
 * column widened to {@code Variant}, one row per element (engine-lite's
 * behavior; real Pure's registered {@code flatten<T,Z>(…):Relation<Z=(?:T)>}
 * single-column signature is a documented divergence &mdash; this checker
 * carries the engine behavior, validating what the signature can't express).
 */
final class FlattenChecker {

    private FlattenChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() != 2 || !(af.parameters().get(1) instanceof ColSpec cs)
                || cs.function1() != null) {
            throw new TypeInferenceException("flatten expects (source, ~column)");
        }
        TypedSpec source = t.synth(af.parameters().get(0), env);
        if (!(source.info().type() instanceof Type.RelationType schema)) {
            throw new TypeInferenceException("flatten requires a relation source, got "
                    + source.info().type().typeName());
        }
        // Validate the CALL against the registered native signature — never
        // bypassed (the tableReference lesson: an unexercised registration
        // goes vestigial). flatten<T,Z>(T[*], ColSpec<Z=(?:T)>): the source
        // unifies against parameter 0 and the result multiplicity comes from
        // the signature; the colspec parameter and the OUTPUT SCHEMA are the
        // documented divergences (engine-lite widens in place; real Pure
        // yields the single flattened column).
        var flattenSigs = t.model().findFunction(CoreFn.FLATTEN.parseName());
        if (flattenSigs.isEmpty()) {
            throw new TypeInferenceException("flatten is not registered in the catalog");
        }
        var sig = flattenSigs.get(0);
        t.kernel().unify(sig.parameters().get(0).type(), source.info().type(), new Bindings());
        boolean known = schema.columns().stream().anyMatch(c -> c.name().equals(cs.name()));
        if (!known) {
            throw new TypeInferenceException("unknown column '" + cs.name() + "' in " + schema.typeName());
        }
        List<Type.Column> cols = new ArrayList<>(schema.columns().size());
        for (Type.Column c : schema.columns()) {
            cols.add(c.name().equals(cs.name())
                    ? new Type.Column(c.name(), new Type.ClassType(Pure.VARIANT.qualifiedName()), c.multiplicity())
                    : c);
        }
        return new TypedFlatten(source, cs.name(),
                new ExprType(new Type.RelationType(cols), sig.returnMultiplicity()));
    }
}
