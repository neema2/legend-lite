package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedAggCall;
import com.gs.legend.compiler.typed.TypedPivot;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Type;

import java.util.*;

/**
 * Type checker for {@code pivot()} — Relation API.
 *
 * <p>Signature-driven: validates via {@code resolveOverload}/{@code unify},
 * then delegates aggregate column compilation to
 * {@link GroupByChecker#compileAggColSpec} — same pattern as
 * {@link AggregateChecker}.
 *
 * <p>Pure syntax:
 * <pre>
 * relation-&gt;pivot(~[pivotCols], ~[aggName : x | $x.col : y | $y-&gt;sum()])
 * </pre>
 *
 * <p>Produces:
 * <ul>
 *   <li>{@link TypeInfo#columnSpecs()} — pivot column names (PlanGenerator reads for PIVOT ON)</li>
 *   <li>{@link TypeInfo#aggColumnSpecs()} — compiled aggregates (same as groupBy)</li>
 * </ul>
 *
 * @see GroupByChecker
 * @see AggregateChecker
 */
public class PivotChecker extends AbstractChecker {

    public PivotChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypedPivot check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        NativeFunctionDef def = resolveOverload("pivot", af.parameters(), source);
        unify(def, source.expressionType());
        List<ValueSpecification> params = af.parameters();

        Type.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException(
                    "pivot() requires a Relation source with a known schema");
        }

        // Pivot columns: validate against source schema.
        List<String> pivotColumns = extractColumnNames(params.get(1));
        for (String col : pivotColumns) {
            if (!sourceSchema.columns().containsKey(col)) {
                throw new PureCompileException(
                        "pivot(): pivot column '" + col + "' not found in source. Available: "
                                + sourceSchema.columns().keySet());
            }
        }

        // Aggregate columns: always the last param, compiled to TypedAggCall.
        int aggParamIdx = params.size() - 1;
        List<ColSpec> aggSpecs = GroupByChecker.extractAggColSpecs(params.get(aggParamIdx));
        var groupByChecker = new GroupByChecker(env);
        List<TypedAggCall> aggs = new ArrayList<>(aggSpecs.size());
        for (ColSpec cs : aggSpecs) {
            aggs.add(groupByChecker.compileTypedAggCall(
                    cs, new Type.Relation(sourceSchema), source, ctx));
        }

        // Group-by columns = source − pivot − value columns. Value columns are
        // derived structurally from fn1's body when it's a direct property access.
        var groupByCols = new LinkedHashMap<>(sourceSchema.columns());
        pivotColumns.forEach(groupByCols::remove);
        for (ColSpec cs : aggSpecs) {
            if (cs.function1() != null && !cs.function1().body().isEmpty()
                    && cs.function1().body().get(0) instanceof AppliedProperty ap) {
                groupByCols.remove(ap.property());
            }
        }

        // Dynamic pivot columns use the compiled return (or cast) types.
        var dynamicCols = aggs.stream()
                .map(a -> new Type.Schema.DynamicPivotColumn(
                        a.alias(), a.castType().orElse(a.returnType())))
                .toList();
        var outSchema = new Type.Schema(groupByCols, dynamicCols);

        return new TypedPivot(source, pivotColumns, aggs, def,
                ExpressionType.one(new Type.Relation(outSchema)));
    }
}
