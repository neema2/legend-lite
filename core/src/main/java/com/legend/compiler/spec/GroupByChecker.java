package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedColSpec;
import com.legend.compiler.spec.typed.TypedColSpecArray;
import com.legend.compiler.spec.typed.TypedFuncColSpec;
import com.legend.compiler.spec.typed.TypedFuncColSpecArray;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.element.type.Type;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.CInteger;
import com.legend.model.spec.CString;
import com.legend.model.spec.Variable;
import com.legend.model.spec.ColSpec;
import com.legend.model.spec.ColSpecArray;
import com.legend.model.spec.LambdaFunction;
import com.legend.model.spec.PureCollection;
import com.legend.model.spec.ValueSpecification;

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
        if (af.parameters().size() == 3 && isTdsLegacyShape(af)) {
            return check(t, tdsLegacyToModern(af), env);
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
        // scalar spellings wrap: groupBy([keys], agg(...), ['a','b'])
        PureCollection keyFns = asCollection(ps.get(1));
        PureCollection aggs = asCollection(ps.get(2));
        PureCollection aliases = asCollection(ps.get(3));
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

    /**
     * The TDS-era 3-arg spelling: {@code groupBy(['keys'], agg('name', mapFn,
     * aggFn)…)} — string keys, aggregates carrying their OWN names. Distinct
     * from the modern colspec form (which passes ColSpec/ColSpecArray) and
     * from the 4-arg alias-list form.
     */
    private static boolean isTdsLegacyShape(AppliedFunction af) {
        List<ValueSpecification> ps = af.parameters();
        boolean keysOk = ps.get(1) instanceof CString
                || (ps.get(1) instanceof PureCollection c
                        && c.values().stream().allMatch(v -> v instanceof CString));
        return keysOk && aggList(ps.get(2)) != null;
    }

    /** The named-agg calls of the TDS legacy aggs argument, or null if not that shape. */
    private static List<AppliedFunction> aggList(ValueSpecification v) {
        List<ValueSpecification> items = v instanceof PureCollection c ? c.values() : List.of(v);
        List<AppliedFunction> out = new ArrayList<>(items.size());
        for (ValueSpecification item : items) {
            if (item instanceof AppliedFunction call && call.function().equals("agg")
                    && call.parameters().size() == 3
                    && call.parameters().get(0) instanceof CString
                    && call.parameters().get(1) instanceof LambdaFunction
                    && call.parameters().get(2) instanceof LambdaFunction) {
                out.add(call);
            } else {
                return null;
            }
        }
        return out.isEmpty() ? null : out;
    }

    private static AppliedFunction tdsLegacyToModern(AppliedFunction af) {
        List<ValueSpecification> ps = af.parameters();
        List<ValueSpecification> keys = ps.get(1) instanceof PureCollection c
                ? c.values() : List.of(ps.get(1));
        List<ColSpec> keyCols = keys.stream()
                .map(k -> new ColSpec(((CString) k).value())).toList();
        List<ColSpec> aggCols = new ArrayList<>();
        for (AppliedFunction aggCall : aggList(ps.get(2))) {
            String name = ((CString) aggCall.parameters().get(0)).value();
            LambdaFunction mapFn = (LambdaFunction) aggCall.parameters().get(1);
            LambdaFunction aggFn = (LambdaFunction) aggCall.parameters().get(2);
            // the row-count idiom agg('cnt', x|$x, y|$y->count()): an
            // IDENTITY selector over the row maps to the constant 1 —
            // count(1) counts rows, exactly the engine's count(*) emission
            // for the empty-params TDS map. Gated on the aggregator BEING
            // count: max/min/sum over $x would silently aggregate the
            // constant (the engine emits broken SQL and dies loud there).
            if (mapFn.parameters().size() == 1 && mapFn.body().size() == 1
                    && mapFn.body().get(0) instanceof Variable v
                    && v.name().equals(mapFn.parameters().get(0).name())
                    && isCountAgg(aggFn)) {
                mapFn = new LambdaFunction(mapFn.parameters(),
                        List.of(new CInteger(1)));
            }
            aggCols.add(new ColSpec(name, mapFn, aggFn));
        }
        return new AppliedFunction(af.function(), List.of(ps.get(0),
                new ColSpecArray(keyCols), new ColSpecArray(aggCols)));
    }

    /** The aggregator body is a bare {@code $y->count()} over its own param. */
    private static boolean isCountAgg(LambdaFunction aggFn) {
        return aggFn.parameters().size() == 1 && aggFn.body().size() == 1
                && aggFn.body().get(0) instanceof AppliedFunction call
                && call.function().equals("count")
                && call.parameters().size() == 1
                && call.parameters().get(0) instanceof Variable av
                && av.name().equals(aggFn.parameters().get(0).name());
    }

    private static PureCollection asCollection(ValueSpecification v) {
        return v instanceof PureCollection c ? c : new PureCollection(List.of(v));
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
