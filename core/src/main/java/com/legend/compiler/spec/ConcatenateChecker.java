package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;

/**
 * Relation {@code concatenate} (engine {@code ConcatenateChecker}, SQL
 * {@code UNION ALL}) &mdash; fully generic: the shared {@code T} enforces that
 * both sides carry the same schema.
 */
final class ConcatenateChecker {

    private ConcatenateChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        return new TypedConcatenate(a.args().get(0), a.args().get(1), a.out());
    }
}
