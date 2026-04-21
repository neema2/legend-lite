package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedAggregate;
import com.gs.legend.compiler.typed.TypedAggCall;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Type;

import java.util.*;

/**
 * Type checker for {@code aggregate()} — Relation API only.
 *
 * <p>Supports the 2 official overloads from legend-engine:
 * <pre>
 * aggregate(r:Relation<T>, agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>)      → Relation<R>
 * aggregate(r:Relation<T>, agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>) → Relation<R>
 * </pre>
 *
 * <p>Same as GroupByChecker but without group columns. Output schema = R only.
 * Delegates aggregate column compilation to {@link GroupByChecker#compileAggColSpec}.
 *
 * @see GroupByChecker
 */
public class AggregateChecker extends AbstractChecker {

    public AggregateChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypedSpec check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        NativeFunctionDef def = resolveOverload("aggregate", af.parameters(), source);
        unify(def, source.expressionType());
        List<ValueSpecification> params = af.parameters();

        Type.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException(
                    "aggregate() requires a Relation source with a known schema");
        }

        // Compile each AggColSpec via the shared helper on GroupByChecker —
        // produces a {@link TypedAggCall} directly, no legacy adapter.
        Map<String, Type> resultColumns = new LinkedHashMap<>();
        List<TypedAggCall> aggs = new ArrayList<>();
        var groupByChecker = new GroupByChecker(env);
        for (ColSpec cs : GroupByChecker.extractAggColSpecs(params.get(1))) {
            TypedAggCall agg = groupByChecker.compileTypedAggCall(
                    cs, new Type.Relation(sourceSchema), source, ctx);
            resultColumns.put(agg.alias(),
                    agg.castType().orElse(agg.returnType()));
            aggs.add(agg);
        }

        if (resultColumns.isEmpty()) {
            throw new PureCompileException("aggregate() produced no output columns");
        }

        var outSchema = Type.Schema.withoutPivot(resultColumns); // R only — no group cols
        return new TypedAggregate(source, aggs,
                ExpressionType.one(new Type.Relation(outSchema)));
    }
}
