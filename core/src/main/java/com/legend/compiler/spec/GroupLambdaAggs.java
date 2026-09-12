// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.ResolvedNames;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE GROUP-LAMBDA AGGREGATE FORM (engine core_functions_relation, 4.145.0):
 * a groupBy / aggregate column spec with ONE lambda that takes the GROUP as a
 * relation — {@code ~names : g | $g->joinStrings(~name, ',', ~id->ascending())},
 * {@code ~cnt : g | $g->size()} (upstream's {@code FuncColSpec<{Relation<T>[1]
 * -> Any[0..1]}, R>} overloads). The engine's relational lowering reads these
 * bodies as the aggregate they name; this platform DESUGARS them, before the
 * generic check, into the map / reduce {@code AggColSpec} form the typer and
 * the lowering already own:
 * <ul>
 *   <li>{@code $g->joinStrings(~col, sep[, sorts])} → {@code x | $x.col : y |
 *       $y->joinStrings(sep)}, the sorts riding on as the reducer's ORDER BY
 *       ({@link TypedAggCol.AggOrder}: string_agg(x, sep ORDER BY k));</li>
 *   <li>{@code $g->joinStrings(f, sep[, sorts])} → {@code f : y |
 *       $y->joinStrings(sep)} (the row function IS the map);</li>
 *   <li>{@code $g->size()} → {@code x | 1 : y | $y->count()} — count(1) counts
 *       rows, the engine's count(*) emission (the TDS legacy idiom).</li>
 * </ul>
 * Any other body over the group is refused loudly: the upstream signature admits
 * every {@code Relation<T>[1] -> Any[0..1]} function, the platform implements
 * the two aggregates the engine's own lowering implements.
 */
final class GroupLambdaAggs {

    private GroupLambdaAggs() {
    }

    /** The rewritten call and, per aggregate column, its sort keys. */
    record Desugared(AppliedFunction call, Map<String, List<TypedSort.TypedSortKey>> orders) {
    }

    /** {@code af} with its aggregate argument (index {@code aggIndex}) desugared,
     *  or null when no column spec there is the group-lambda form. */
    static @com.legend.Nullable Desugared rewrite(AppliedFunction af, int aggIndex) {
        if (af.parameters().size() <= aggIndex) {
            return null;
        }
        ValueSpecification arg = af.parameters().get(aggIndex);
        List<ColSpec> specs = arg instanceof ColSpecArray arr ? arr.colSpecs()
                : arg instanceof ColSpec cs ? List.of(cs) : List.of();
        if (specs.stream().noneMatch(GroupLambdaAggs::isGroupLambda)) {
            return null;
        }
        Map<String, List<TypedSort.TypedSortKey>> orders = new LinkedHashMap<>();
        List<ColSpec> out = new ArrayList<>(specs.size());
        for (ColSpec cs : specs) {
            out.add(isGroupLambda(cs) ? desugar(cs, orders) : cs);
        }
        List<ValueSpecification> params = new ArrayList<>(af.parameters());
        params.set(aggIndex, arg instanceof ColSpecArray ? new ColSpecArray(out) : out.get(0));
        return new Desugared(af.withParameters(params), orders);
    }

    private static boolean isGroupLambda(ColSpec cs) {
        return cs.function1() != null && cs.function2() == null
                && cs.function1().parameters().size() == 1;
    }

    private static ColSpec desugar(ColSpec cs, Map<String, List<TypedSort.TypedSortKey>> orders) {
        LambdaFunction lam = java.util.Objects.requireNonNull(cs.function1());
        String g = lam.parameters().get(0).name();
        if (lam.body().size() != 1
                || !(lam.body().get(0) instanceof AppliedFunction call)
                || call.parameters().isEmpty()
                || !(call.parameters().get(0) instanceof Variable v && v.name().equals(g))) {
            throw new TypeInferenceException("~" + cs.name() + ": a group aggregate lambda must be"
                    + " ONE joinStrings(…) or size() call over its group parameter");
        }
        Variable x = new Variable("x", null, null, null);
        Variable y = new Variable("y", null, null, null);
        if (ResolvedNames.names(call, PlatformTypes.RELATION_SIZE) && call.parameters().size() == 1) {
            return new ColSpec(cs.name(),
                    new LambdaFunction(List.of(x), List.of(new CInteger(1)), null),
                    new LambdaFunction(List.of(y), List.of(new AppliedFunction("count", List.of(y))), null));
        }
        if (ResolvedNames.names(call, PlatformTypes.RELATION_JOIN_STRINGS)
                && (call.parameters().size() == 3 || call.parameters().size() == 4)) {
            ValueSpecification value = call.parameters().get(1);
            LambdaFunction map = switch (value) {
                case ColSpec col -> new LambdaFunction(List.of(x),
                        List.of(new AppliedProperty(x, col.name(), null)), null);
                case LambdaFunction f -> f;
                default -> throw new TypeInferenceException("~" + cs.name()
                        + ": joinStrings over the group takes a column (~col) or a row function");
            };
            LambdaFunction reduce = new LambdaFunction(List.of(y), List.of(
                    new AppliedFunction("joinStrings", List.of(y, call.parameters().get(2)))), null);
            if (call.parameters().size() == 4) {
                orders.put(cs.name(), SortChecker.keysFromAst(call.parameters().get(3)));
            }
            return new ColSpec(cs.name(), map, reduce);
        }
        throw new TypeInferenceException("~" + cs.name() + ": a group aggregate lambda must be"
                + " joinStrings(…) or size() over its group — got '" + call.function() + "'");
    }

    /** A ROW lambda over {@code relation}'s rows ({@code x | $x.col}), typed as
     *  {@code {Row[1] -> Any[*]}} by the same machinery as an AggColSpec's map. */
    private static TypedLambda typeRowLambda(Typer t, LambdaFunction lam, TypedSpec relation, Env env) {
        com.legend.compiler.element.type.Type.RelationType row =
                com.legend.compiler.element.type.Type.schemaView(relation.info().type());
        if (row == null) {
            throw new TypeInferenceException("expected a relation, got " + relation.info().type().typeName());
        }
        var f = new com.legend.compiler.element.type.Type.FunctionType(
                List.of(new com.legend.compiler.element.type.Type.Param(row,
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                new com.legend.compiler.element.type.Type.Param(
                        new com.legend.compiler.element.type.Type.ClassType(PlatformTypes.ANY),
                        com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_MANY));
        return (TypedLambda) t.typeLambda(lam, f, new Bindings(), env);
    }

    /** The checked aggregate columns with their desugared sort keys typed as
     *  row lambdas over {@code relation} (the map body's row scope). */
    static List<TypedAggCol> withOrders(Typer t, List<TypedAggCol> aggs,
            Map<String, List<TypedSort.TypedSortKey>> orders, TypedSpec relation, Env env) {
        if (orders.isEmpty()) {
            return aggs;
        }
        List<TypedAggCol> out = new ArrayList<>(aggs.size());
        for (TypedAggCol a : aggs) {
            List<TypedSort.TypedSortKey> keys = orders.get(a.name());
            if (keys == null) {
                out.add(a);
                continue;
            }
            List<TypedAggCol.AggOrder> order = new ArrayList<>(keys.size());
            for (TypedSort.TypedSortKey k : keys) {
                Variable x = new Variable("x", null, null, null);
                TypedLambda key = typeRowLambda(t, new LambdaFunction(List.of(x),
                        List.of(new AppliedProperty(x, k.column(), null)), null), relation, env);
                order.add(new TypedAggCol.AggOrder(key, k.ascending(), k.nullOrder()));
            }
            out.add(new TypedAggCol(a.name(), a.map(), a.reduce(), order));
        }
        return out;
    }
}
