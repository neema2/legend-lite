package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedFlatten;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Primitive;
import com.gs.legend.model.m3.Type;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Signature-driven type checker for {@code flatten()}.
 *
 * <p>Registry signature: {@code flatten<T,Z>(valueToFlatten:T[*], columnWithFlattenedValue:ColSpec<Z=(?:T)>[1]):Relation<Z>[1]}
 *
 * <p>Validates:
 * <ul>
 *   <li>Source type matches signature via unify (binds T)</li>
 *   <li>Source has a relational schema</li>
 *   <li>Flattened column exists in source schema</li>
 *   <li>Output schema: same as source but flattened column changes type to JSON (UNNEST)</li>
 * </ul>
 *
 * <p>Populates columnSpecs for PlanGenerator to emit UNNEST.
 */
public class FlattenChecker extends AbstractChecker {

    public FlattenChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypedSpec check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("flatten", params, source);
        unify(def, source.expressionType());

        // flatten() always names exactly one column (signature enforces the ColSpec arg).
        List<String> names = params.size() < 2
                ? List.of() : extractColumnNames(params.get(1));
        if (names.isEmpty()) {
            throw new PureCompileException(
                    "flatten(): expected a ColSpec argument naming the column to unnest");
        }
        String colName = names.get(0);

        Type.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException(
                    "flatten() requires a relational source with a known schema");
        }
        if (!sourceSchema.columns().containsKey(colName)) {
            throw new PureCompileException(
                    "flatten(): column '" + colName + "' not found in source. Available: "
                            + sourceSchema.columns().keySet());
        }

        // Output schema: source schema with the flattened column widened to Variant.
        Map<String, Type> resultColumns = new LinkedHashMap<>(sourceSchema.columns());
        resultColumns.put(colName, Primitive.VARIANT);
        var outputSchema = new Type.Schema(resultColumns, sourceSchema.dynamicPivotColumns());
        return new TypedFlatten(source, colName,
                ExpressionType.one(new Type.Relation(outputSchema)));
    }
}
