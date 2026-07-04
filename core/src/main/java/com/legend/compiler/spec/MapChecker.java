package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;

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
        return new TypedMap(a.args().get(0), Args.lambda(a, 1), a.out());
    }
}
