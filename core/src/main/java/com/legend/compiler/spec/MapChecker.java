package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.spec.AppliedFunction;

/**
 * {@code map} (engine {@code MapChecker}) &mdash; checked generically (the
 * signature's {@code V} binds from the mapper body); this class only emits the
 * construct node.
 */
final class MapChecker {

    private MapChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        com.legend.compiler.element.type.ExprType out = a.out();
        // map over a RELATION value is a ROW map: the result cardinality is
        // the (unknown) row count, never the relation VALUE's multiplicity —
        // a Relation[1] source (the Result.values envelope, audit 19d B2)
        // must not type its row map [0..1].
        if (a.args().get(0).info().type()
                instanceof com.legend.compiler.element.type.Type.RelationType) {
            out = new com.legend.compiler.element.type.ExprType(out.type(),
                    com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_MANY);
        }
        // HIGHER-ORDER map: a function-VALUED argument ($f->map($func) in a
        // user fn body typed generically) has no literal lambda yet — emit
        // the plain call node; UserCallInliner β-reduces or rebuilds the
        // TypedMap once substitution reveals the literal (same family as
        // its eval rule). An un-substituted survivor stays loud at the
        // lowerer's TypedNativeCall wall.
        if (!(a.args().get(1)
                instanceof com.legend.compiler.spec.typed.TypedLambda)) {
            return Typer.emitCall(a.chosen(), a.args(), out);
        }
        return new TypedMap(a.args().get(0), Args.lambda(a, 1), out);
    }
}
