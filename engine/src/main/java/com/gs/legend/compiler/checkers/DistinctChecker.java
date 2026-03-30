package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.List;


/**
 * Signature-driven type checker for {@code distinct()}.
 *
 * <p>Overloads:
 * <ul>
 *   <li>{@code distinct<T>(rel:Relation<T>[1]):Relation<T>[1]} — all columns</li>
 *   <li>{@code distinct<X,T>(rel:Relation<T>[1], columns:ColSpecArray<X⊆T>[1]):Relation<X>[1]} — specific columns</li>
 * </ul>
 *
 * <p>Validates:
 * <ul>
 *   <li>Source is relational (via unify against signature)</li>
 *   <li>Selected columns exist in source (⊆ constraint)</li>
 *   <li>No-arg: output from resolveOutput (Relation<T>); with columns: column subset</li>
 * </ul>
 */
public class DistinctChecker extends AbstractChecker {

    public DistinctChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("distinct", params, source);

        // 1. Bind type variables from signature (T from source)
        var bindings = unify(def, source.expressionType());

        // 2. Source must be relational
        GenericType.Relation.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException("distinct() requires a relational source");
        }

        // 3. distinct() with no column arg = output type from signature (Relation<T>[1])
        if (params.size() < 2) {
            return TypeInfo.builder()
                                        .expressionType(resolveOutput(def, bindings, "distinct()"))
                    .build();
        }

        // 4. Extract and validate columns (enforces X⊆T constraint)
        List<String> cols = extractColumnNames(params.get(1));
        if (cols.isEmpty()) {
            return TypeInfo.builder()
                                        .expressionType(resolveOutput(def, bindings, "distinct()"))
                    .build();
        }
        sourceSchema.assertHasColumns(cols);

        // 5. Output schema is the subset (X from Relation<X>)
        GenericType.Relation.Schema outputSchema = sourceSchema.onlyColumns(cols);
        return TypeInfo.builder()
                                .columnSpecs(cols.stream().map(TypeInfo.ColumnSpec::col).toList())
                .expressionType(ExpressionType.one(new GenericType.Relation(outputSchema)))
                .build();
    }
}
