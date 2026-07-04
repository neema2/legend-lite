package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedSelect;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;

/**
 * Relation {@code select} (engine {@code SelectChecker}) &mdash; fully generic:
 * the {@code ⊆} constraint validates the names and binds the narrowed {@code Z};
 * emission reads the columns off the resolved output schema.
 */
final class SelectChecker {

    private SelectChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        return new TypedSelect(a.args().get(0), Args.outputColumns(a), a.out());
    }
}
