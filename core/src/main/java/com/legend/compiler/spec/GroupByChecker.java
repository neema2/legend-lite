package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedColSpec;
import com.legend.compiler.spec.typed.TypedColSpecArray;
import com.legend.compiler.spec.typed.TypedFuncColSpec;
import com.legend.compiler.spec.typed.TypedFuncColSpecArray;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.Variable;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;
import com.legend.compiler.spec.typed.TypedAggCol;
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

    /** The TDS-era agg spelling — bare or its exact FQN (the corpus writes
     * {@code meta::pure::tds::agg('count', x|$x, y|$y->count())}). */
    static boolean isAggSpelling(String fn) {
        return fn.equals("agg") || fn.equals("meta::pure::tds::agg");
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() == 4) {
            return check(t, legacyToModern(t, af, env), env);   // desugar, then the modern path
        }
        if (af.parameters().size() == 3 && isTdsLegacyShape(af)) {
            return check(t, tdsLegacyToModern(af), env);
        }
        if (af.parameters().size() == 2 && af.parameters().get(1) instanceof LambdaFunction) {
            // the COLLECTION groupBy (legend-pure collection/map/groupBy.pure:
            // groupBy<X,K>(X[*], {X[1]->K[1]}):Map<K,List<X>>) — a plain
            // native call against its verbatim signature; over a spelled
            // collection the inliner folds it to newMap(pairs)
            Application a = t.checkGeneric(af, env);
            return Typer.emitCall(a.chosen(), a.args(), a.out());
        }
        // the group-lambda aggregate form (~c : g | $g->joinStrings(…)): desugared
        // to map / reduce before the generic check, its sort keys typed after
        GroupLambdaAggs.Desugared d = GroupLambdaAggs.rewrite(af, 2);
        if (d != null) {
            af = d.call();
        }
        Application a = t.checkGeneric(af, env);
        List<TypedAggCol> aggs = d == null ? Args.aggCols(a.args().get(2))
                : GroupLambdaAggs.withOrders(t, Args.aggCols(a.args().get(2)), d.orders(),
                        a.args().get(0), env);
        return new TypedGroupBy(a.args().get(0), groupKeys(a.args().get(1)), aggs, a.out());
    }

    /**
     * {@code groupByWithWindowSubset(set, functions, aggValues, ids,
     * subSelectIds, subAggIds)} (tds.pure:867): the store's rule — engine
     * pureToSQLQuery processObjectGroupByWithWindowSubSet — asserts the id
     * lists (subAggIds disjoint from subSelectIds, subAggIds among the
     * aggregate ids, subSelectIds among ids), picks {@code functions[ids
     * .indexOf(i)]} for each subSelectId and {@code aggValues[ids.indexOf(i)
     * - functions.size()]} for each subAggId, and groups by
     * {@code subSelectIds ++ subAggIds}: the 4-arg legacy groupBy over
     * those subsets. The literal id lists are the store's InstanceValues;
     * a non-literal list walls loud.
     */
    static TypedSpec checkWindowSubset(Typer t, AppliedFunction af, Env env) {
        List<ValueSpecification> ps = af.parameters();
        if (ps.size() != 6) {
            throw new TypeInferenceException("groupByWithWindowSubset expects 6 arguments, got " + ps.size());
        }
        PureCollection functions = asCollection(letBound(ps.get(1), env));
        ValueSpecification aggsRaw = letBound(ps.get(2), env);
        ValueSpecification aggsEx = t.rawSchemaErasedExpansion(aggsRaw);
        PureCollection aggs = asCollection(aggsEx != null ? aggsEx : aggsRaw);
        List<String> allIds = stringList(letBound(ps.get(3), env), "ids");
        List<String> subSelectIds = stringList(letBound(ps.get(4), env), "subSelectIds");
        List<String> subAggIds = stringList(letBound(ps.get(5), env), "subAggIds");
        int nf = functions.values().size();
        for (String i : subAggIds) {
            if (subSelectIds.contains(i)) {
                throw new TypeInferenceException("SubAggIds and Ids should not have an intersection");
            }
            if (!allIds.subList(Math.min(nf, allIds.size()), allIds.size()).contains(i)) {
                throw new TypeInferenceException("SubAggIds must be a subset of ids");
            }
        }
        for (String i : subSelectIds) {
            if (!allIds.contains(i)) {
                throw new TypeInferenceException("Ids and Ids should not have an intersection");
            }
        }
        List<ValueSpecification> newFunctions = new ArrayList<>();
        for (String i : subSelectIds) {
            newFunctions.add(functions.values().get(allIds.indexOf(i)));
        }
        List<ValueSpecification> newAggs = new ArrayList<>();
        for (String i : subAggIds) {
            newAggs.add(aggs.values().get(allIds.indexOf(i) - nf));
        }
        List<ValueSpecification> newIds = new ArrayList<>();
        for (String i : subSelectIds) {
            newIds.add(new CString(i));
        }
        for (String i : subAggIds) {
            newIds.add(new CString(i));
        }
        return check(t, new AppliedFunction("groupBy", List.of(ps.get(0),
                new PureCollection(newFunctions), new PureCollection(newAggs),
                new PureCollection(newIds))), env);
    }

    private static List<String> stringList(ValueSpecification v, String what) {
        List<String> out = new ArrayList<>();
        for (ValueSpecification e : asCollection(v).values()) {
            if (!(e instanceof CString cs)) {
                throw new TypeInferenceException("groupByWithWindowSubset: " + what
                        + " must be a literal string list");
            }
            out.add(cs.value());
        }
        return out;
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
        // an aggregate-spec HELPER CALL (getAggValues():AggregateValue<..>[*])
        // expands raw so its agg(mapFn, aggFn) literals reach the shape check
        // a LET-BOUND aggregate (`let g = agg(x|…, y|…)`) parks at its
        // binding (Typer.deferredLetRhs) and types HERE, against the
        // groupBy that consumes it — the alias chase, per element too
        ValueSpecification aggsRaw = letBound(ps.get(2), env);
        ValueSpecification aggsEx = t.rawSchemaErasedExpansion(aggsRaw);
        PureCollection aggs = asCollection(aggsEx != null ? aggsEx : aggsRaw);
        aggs = new PureCollection(aggs.values().stream()
                .map(v -> letBound(v, env)).toList());
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
            keyCols.add(classSource && ProjectChecker.columnLambda(keyFns.values().get(i)) instanceof LambdaFunction lf
                    ? new ColSpec(alias, lf)
                    : new ColSpec(alias));
        }
        List<ColSpec> aggCols = new ArrayList<>(aggs.values().size());
        for (int i = 0; i < aggs.values().size(); i++) {
            String alias = aliasAt(aliases, keyFns.values().size() + i);
            if (!(aggs.values().get(i) instanceof AppliedFunction aggCall)
                    || !isAggSpelling(aggCall.function())
                    || aggCall.parameters().size() != 2
                    || !(ProjectChecker.columnLambda(aggCall.parameters().get(0)) instanceof LambdaFunction mapFn)
                    || !(ProjectChecker.columnLambda(aggCall.parameters().get(1)) instanceof LambdaFunction aggFn)) {
                throw new TypeInferenceException(
                        "legacy groupBy aggregate " + i + " must be agg(mapFn, aggFn)");
            }
            aggCols.add(new ColSpec(alias, mapFn, aggFn));
        }
        // a RELATION source lands on the modern relation overloads by bare name
        // (same reason as tdsLegacyToModern); a CLASS source lands on the
        // internal-desugar identity — upstream declares no colspec groupBy over
        // instances (batch 5 leg 5d), and a user's bare `groupBy` never reaches it
        return new AppliedFunction(classSource
                ? com.legend.builtin.Pure.Lite.GROUP_BY_OVER_INSTANCES : "groupBy",
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
    private static @com.legend.Nullable List<AppliedFunction> aggList(ValueSpecification v) {
        List<ValueSpecification> items = v instanceof PureCollection c ? c.values() : List.of(v);
        List<AppliedFunction> out = new ArrayList<>(items.size());
        for (ValueSpecification item : items) {
            if (item instanceof AppliedFunction call && isAggSpelling(call.function())
                    && call.parameters().size() == 3
                    && call.parameters().get(0) instanceof CString
                    && ProjectChecker.columnLambda(call.parameters().get(1)) instanceof LambdaFunction
                    && ProjectChecker.columnLambda(call.parameters().get(2)) instanceof LambdaFunction) {
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
        List<AppliedFunction> aggCalls = java.util.Objects
                .requireNonNull(aggList(ps.get(2)), "groupBy agg list");
        for (AppliedFunction aggCall : aggCalls) {
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
        // BARE name: the desugar's whole point is landing in the modern
        // construct — an FQN spelling (meta::pure::tds::groupBy) would
        // resolve against only the FQN-registered overloads, whose keys are
        // FuncColSpecArray, and miss the plain-key relation overloads.
        return new AppliedFunction("groupBy", List.of(ps.get(0),
                new ColSpecArray(keyCols), new ColSpecArray(aggCols)));
    }

    /** The aggregator body is a bare {@code $y->count()} over its own param. */
    private static boolean isCountAgg(LambdaFunction aggFn) {
        return aggFn.parameters().size() == 1 && aggFn.body().size() == 1
                && aggFn.body().get(0) instanceof AppliedFunction call
                && com.legend.compiler.ResolvedNames.names(call, com.legend.compiler.element.type.PlatformTypes.COUNT)
                && call.parameters().size() == 1
                && call.parameters().get(0) instanceof Variable av
                && av.name().equals(aggFn.parameters().get(0).name());
    }

    private static ValueSpecification letBound(ValueSpecification v, Env env) {
        if (v instanceof Variable) {
            ValueSpecification bound = env.resolveAlias(v);
            return bound != null ? bound : v;
        }
        return v;
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
