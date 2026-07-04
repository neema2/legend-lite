package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedAggregate;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;

/**
 * Whole-relation {@code aggregate} (engine {@code AggregateChecker}) &mdash; a
 * groupBy with no keys: checked generically against
 * {@code aggregate<T,K,V,R>(r, AggColSpec(Array)<…,R>):Relation<R>[1]}.
 */
final class AggregateChecker {

    private AggregateChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        return new TypedAggregate(a.args().get(0), Args.aggCols(a.args().get(1)), a.out());
    }
}
