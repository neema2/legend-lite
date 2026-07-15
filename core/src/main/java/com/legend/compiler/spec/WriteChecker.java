package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedWrite;
import com.legend.model.spec.AppliedFunction;

import java.util.Optional;

/**
 * Relation {@code write} (engine {@code WriteChecker}) &mdash; fully generic
 * ({@code write<T>(Relation<T>[1] [, target:Any[1]]):Integer[1]}); the optional
 * destination reference rides the node for the back-end.
 */
final class WriteChecker {

    private WriteChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        Optional<TypedSpec> destination = a.args().size() > 1
                ? Optional.of(a.args().get(1)) : Optional.empty();
        return new TypedWrite(a.args().get(0), destination, a.out());
    }
}
