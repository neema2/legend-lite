package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;

/**
 * Relation {@code distinct} (engine {@code DistinctChecker}) &mdash; fully
 * generic: whole-row dedup preserves {@code T}; the {@code ~[cols]} form narrows
 * to {@code X⊆T}. Emission reads the columns off the resolved output schema.
 */
final class DistinctChecker {

    private DistinctChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        return new TypedDistinct(a.args().get(0), Args.outputColumns(a), a.out());
    }
}
