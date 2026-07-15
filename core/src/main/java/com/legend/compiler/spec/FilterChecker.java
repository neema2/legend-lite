package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.spec.AppliedFunction;

/**
 * {@code filter} (engine {@code FilterChecker}) &mdash; checked generically (a
 * plain signature+lambda call, relation and collection overloads alike); this
 * class only emits the construct node lowering dispatches on.
 */
final class FilterChecker {

    private FilterChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        return new TypedFilter(a.args().get(0), Args.lambda(a, 1), a.out());
    }
}
