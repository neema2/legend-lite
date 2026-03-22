package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

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
    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        NativeFunctionDef def = resolveOverload("aggregate", af.parameters(), source);
        unify(def, source.expressionType()); // validate source matches signature generics
        List<ValueSpecification> params = af.parameters();

        GenericType.Relation.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException(
                    "aggregate() requires a Relation source with a known schema");
        }

        Map<String, GenericType> resultColumns = new LinkedHashMap<>();
        List<TypeInfo.AggColumnSpec> aggCols = new ArrayList<>();

        // --- Aggregate columns (param 1): AggColSpec or AggColSpecArray ---
        var groupByChecker = new GroupByChecker(env);
        List<ColSpec> aggSpecs = GroupByChecker.extractAggColSpecs(params.get(1));
        for (ColSpec cs : aggSpecs) {
            TypeInfo.AggColumnSpec acs = groupByChecker.compileAggColSpec(cs, sourceSchema, source, ctx);
            resultColumns.put(acs.alias(), acs.castType() != null ? acs.castType() : acs.returnType());
            aggCols.add(acs);
        }

        if (resultColumns.isEmpty()) {
            throw new PureCompileException("aggregate() produced no output columns");
        }

        // Output schema = R only (no group columns, unlike groupBy)
        var schema = GenericType.Relation.Schema.withoutPivot(resultColumns);
        return TypeInfo.builder()
                .mapping(source.mapping())
                .aggColumnSpecs(aggCols)
                .expressionType(ExpressionType.many(new GenericType.Relation(schema)))
                .build();
    }
}
