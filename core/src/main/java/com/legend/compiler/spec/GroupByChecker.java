package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedColSpec;
import com.legend.compiler.spec.typed.TypedColSpecArray;
import com.legend.compiler.spec.typed.TypedFuncColSpec;
import com.legend.compiler.spec.typed.TypedFuncColSpecArray;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.element.type.Type;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.CString;
import com.legend.parser.spec.ColSpec;
import com.legend.parser.spec.ColSpecArray;
import com.legend.parser.spec.LambdaFunction;
import com.legend.parser.spec.PureCollection;
import com.legend.parser.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * {@code groupBy} (engine {@code GroupByChecker}) &mdash; checked generically:
 * key columns validate via the {@code ⊆} constraint (binding {@code Z}), each
 * aggregate's map/reduce checks against its {@code AggColSpec} function types
 * (binding {@code R}), and the output schema is {@code resolveOutput(Z+R)} &mdash;
 * keys first, aggregates after. The class-source overload rides the same
 * machinery, its keys carrying extraction lambdas ({@code FuncColSpecArray});
 * the legacy arity-4 TDS form desugars into the modern shape first.
 */
final class GroupByChecker {

    private GroupByChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() == 4) {
            return check(t, legacyToModern(t, af, env), env);   // desugar, then the modern path
        }
        Application a = t.checkGeneric(af, env);
        return new TypedGroupBy(a.args().get(0), groupKeys(a.args().get(1)),
                Args.aggCols(a.args().get(2)), a.out());
    }

    /**
     * Desugar the legacy TDS {@code groupBy(src, [keyFns], [agg(map,agg)…], ['aliases'])}
     * into the modern {@code groupBy(src, ~[keys], ~[alias:map:agg])} (engine
     * {@code rewriteLegacyGroupBy}). Keys become extraction {@code FuncColSpec}s for a
     * class source, bare alias-named colspecs for a relation source (engine's rule);
     * each {@code agg(mapFn, aggFn)} + its alias becomes an aggregate colspec.
     */
    private static AppliedFunction legacyToModern(Typer t, AppliedFunction af, Env env) {
        List<ValueSpecification> ps = af.parameters();
        if (!(ps.get(1) instanceof PureCollection keyFns) || !(ps.get(2) instanceof PureCollection aggs)
                || !(ps.get(3) instanceof PureCollection aliases)) {
            throw new TypeInferenceException(
                    "legacy groupBy expects (source, [keys], [aggs], ['aliases'])");
        }
        int expected = keyFns.values().size() + aggs.values().size();
        if (aliases.values().size() != expected) {
            throw new TypeInferenceException("legacy groupBy expects " + expected + " alias(es) ("
                    + keyFns.values().size() + " keys + " + aggs.values().size()
                    + " aggs), got " + aliases.values().size());
        }
        boolean classSource = t.synth(ps.get(0), env).info().type() instanceof Type.ClassType;

        List<ColSpec> keyCols = new ArrayList<>(keyFns.values().size());
        for (int i = 0; i < keyFns.values().size(); i++) {
            String alias = aliasAt(aliases, i);
            keyCols.add(classSource && keyFns.values().get(i) instanceof LambdaFunction lf
                    ? new ColSpec(alias, lf)
                    : new ColSpec(alias));
        }
        List<ColSpec> aggCols = new ArrayList<>(aggs.values().size());
        for (int i = 0; i < aggs.values().size(); i++) {
            String alias = aliasAt(aliases, keyFns.values().size() + i);
            if (!(aggs.values().get(i) instanceof AppliedFunction aggCall)
                    || !aggCall.function().equals("agg")
                    || aggCall.parameters().size() != 2
                    || !(aggCall.parameters().get(0) instanceof LambdaFunction mapFn)
                    || !(aggCall.parameters().get(1) instanceof LambdaFunction aggFn)) {
                throw new TypeInferenceException(
                        "legacy groupBy aggregate " + i + " must be agg(mapFn, aggFn)");
            }
            aggCols.add(new ColSpec(alias, mapFn, aggFn));
        }
        return new AppliedFunction(af.function(),
                List.of(ps.get(0), new ColSpecArray(keyCols), new ColSpecArray(aggCols)));
    }

    private static String aliasAt(PureCollection aliases, int i) {
        if (aliases.values().get(i) instanceof CString cs) {
            return cs.value();
        }
        throw new TypeInferenceException("legacy groupBy alias " + i + " must be a string literal");
    }

    /** The group keys of a checked colspec argument: bare names, or class-source extraction lambdas. */
    private static List<TypedGroupBy.GroupKey> groupKeys(TypedSpec arg) {
        return switch (arg) {
            case TypedColSpec cs -> List.of(new TypedGroupBy.GroupKey(cs.name(), Optional.empty()));
            case TypedColSpecArray arr -> arr.names().stream()
                    .map(n -> new TypedGroupBy.GroupKey(n, Optional.empty())).toList();
            case TypedFuncColSpec f ->
                    List.of(new TypedGroupBy.GroupKey(f.col().name(), Optional.of(f.col().fn())));
            case TypedFuncColSpecArray fa -> fa.cols().stream()
                    .map(c -> new TypedGroupBy.GroupKey(c.name(), Optional.of(c.fn()))).toList();
            default -> throw new TypeInferenceException("expected group-key column specification(s), got "
                    + arg.getClass().getSimpleName());
        };
    }
}
