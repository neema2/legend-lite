package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
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

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("flatten", params, source);

        // 1. Bind type variables from signature (T from source)
        var bindings = unify(def, source.expressionType());

        // 2. Extract column name from second param
        String colName = null;
        if (params.size() >= 2) {
            List<String> names = extractColumnNames(params.get(1));
            if (!names.isEmpty()) {
                colName = names.get(0);
            }
        }
        if (colName == null) {
            // No column specified — pass through, use signature output
            return TypeInfo.builder()
                                        .expressionType(resolveOutput(def, bindings, "flatten()"))
                    .build();
        }

        // 3. Validate source has relational schema
        Type.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException("flatten() requires a relational source with a known schema");
        }

        // 4. Validate column exists in source
        if (!sourceSchema.columns().containsKey(colName)) {
            throw new PureCompileException(
                    "flatten(): column '" + colName + "' not found in source. Available: "
                            + sourceSchema.columns().keySet());
        }

        // 5. Output schema: same as source, but flattened column → JSON type
        Map<String, Type> resultColumns = new LinkedHashMap<>(sourceSchema.columns());
        resultColumns.put(colName, Type.Primitive.JSON);

        var flattenRelType = new Type.Schema(
                resultColumns, sourceSchema.dynamicPivotColumns());
        return TypeInfo.builder()
                                .columnSpecs(List.of(TypeInfo.ColumnSpec.col(colName)))
                .expressionType(ExpressionType.one(new Type.Relation(flattenRelType)))
                .build();
    }
}
