package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedAggregate;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;

/**
 * Whole-relation {@code aggregate} (engine {@code AggregateChecker}) &mdash; a
 * groupBy with no keys: checked generically against
 * {@code aggregate<T,K,V,R>(r, AggColSpec(Array)<…,R>):Relation<R>[1]}.
 */
final class AggregateChecker {

    private AggregateChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        // the group-lambda aggregate form (~c : g | $g->joinStrings(…)): see GroupByChecker
        GroupLambdaAggs.Desugared d = GroupLambdaAggs.rewrite(af, 1);
        if (d != null) {
            af = d.call();
        }
        Application a = t.checkGeneric(af, env);
        java.util.List<com.legend.compiler.spec.typed.TypedAggCol> aggs = d == null
                ? Args.aggCols(a.args().get(1))
                : GroupLambdaAggs.withOrders(t, Args.aggCols(a.args().get(1)), d.orders(),
                        a.args().get(0), env);
        return new TypedAggregate(a.args().get(0), aggs, a.out());
    }
}
