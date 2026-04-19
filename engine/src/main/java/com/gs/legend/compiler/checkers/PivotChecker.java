package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
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
    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        NativeFunctionDef def = resolveOverload("pivot", af.parameters(), source);
        unify(def, source.expressionType());
        List<ValueSpecification> params = af.parameters();

        Type.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException(
                    "pivot() requires a Relation source with a known schema");
        }

        // --- 1. Pivot columns (param 1): ColSpec or ColSpecArray ---
        List<String> pivotColumns = extractColumnNames(params.get(1));
        for (String col : pivotColumns) {
            if (!sourceSchema.columns().containsKey(col)) {
                throw new PureCompileException(
                        "pivot(): pivot column '" + col + "' not found in source. Available: "
                                + sourceSchema.columns().keySet());
            }
        }

        // --- 2. Aggregate columns (always last param) ---
        int aggParamIdx = params.size() - 1;
        List<ColSpec> aggSpecs = GroupByChecker.extractAggColSpecs(params.get(aggParamIdx));

        var groupByChecker = new GroupByChecker(env);
        List<TypeInfo.AggColumnSpec> aggCols = new ArrayList<>();
        for (ColSpec cs : aggSpecs) {
            TypeInfo.AggColumnSpec acs = groupByChecker.compileAggColSpec(
                    cs, new Type.Relation(sourceSchema), source, ctx);
            aggCols.add(acs);
        }

        // --- 3. Compute group-by columns: source − pivot − value columns ---
        // Value columns are identified from fn1 body (simple property access)
        var groupByCols = new LinkedHashMap<>(sourceSchema.columns());
        pivotColumns.forEach(groupByCols::remove);
        for (ColSpec cs : aggSpecs) {
            if (cs.function1() != null && !cs.function1().body().isEmpty()) {
                ValueSpecification fn1Body = cs.function1().body().get(0);
                if (fn1Body instanceof AppliedProperty ap) {
                    groupByCols.remove(ap.property());
                }
            }
        }

        // --- 4. Build dynamic pivot columns using compiled return types ---
        var dynamicCols = aggCols.stream()
                .map(acs -> {
                    Type returnType = acs.castType() != null
                            ? acs.castType() : acs.returnType();
                    return new Type.Schema.DynamicPivotColumn(
                            acs.alias(), returnType);
                })
                .toList();
        var partialType = new Type.Schema(groupByCols, dynamicCols);

        // Store pivot column names in columnSpecs (same pattern as groupBy group cols)
        List<TypeInfo.ColumnSpec> pivotColSpecs = pivotColumns.stream()
                .map(TypeInfo.ColumnSpec::col)
                .toList();

        return TypeInfo.builder()
                .columnSpecs(pivotColSpecs)
                .aggColumnSpecs(aggCols)
                .expressionType(ExpressionType.one(new Type.Relation(partialType)))
                .build();
    }
}
