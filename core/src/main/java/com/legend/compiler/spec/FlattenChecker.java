package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedFlatten;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.ColSpec;

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
        // relation::variant::flatten(collection, ~col): a scalar/variant
        // COLLECTION becomes a ONE-COLUMN relation (real flatten.pure:
        // flatten<T,Z>(valueToFlatten:T[*], col:ColSpec<Z=(?:T)>):Relation<Z>).
        // KNOWN GAP (audit round 4): this arm computes the schema directly
        // instead of resolving through the registered VARIANT_FLATTEN
        // signature — the registration's Z=(?:T) colspec form is not yet
        // expressible to checkGeneric. Emission is TypedCollectionRelation.
        if (!(source.info().type() instanceof Type.RelationType)) {
            Type elem = source.info().type();
            var row = new Type.RelationType(List.of(new Type.Column(cs.name(), elem,
                    com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_ONE)));
            return new com.legend.compiler.spec.typed.TypedCollectionRelation(source,
                    cs.name(),
                    new com.legend.compiler.element.type.ExprType(row,
                            com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        }
        Type.RelationType schema = (Type.RelationType) source.info().type();
        // Validate the CALL against the registered native signature — never
        // bypassed (the tableReference lesson). The re-audit showed unifying
        // T[*] against anything is VACUOUS, so the REAL checks are: the
        // registration exists and the call's ARITY matches an overload
        // (serialize's pattern). The colspec parameter and the OUTPUT SCHEMA
        // are the documented divergences (engine-lite widens in place; real
        // Pure yields the single flattened column).
        var sig = t.model().findFunction(CoreFn.FLATTEN.parseName()).stream()
                .filter(f -> f.parameters().size() == af.parameters().size())
                .findFirst()
                .orElseThrow(() -> new TypeInferenceException(
                        "no registered 'flatten' overload accepts "
                                + af.parameters().size() + " argument(s)"));
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
