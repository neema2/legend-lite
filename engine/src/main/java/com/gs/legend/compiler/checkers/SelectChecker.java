package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.List;


/**
 * Signature-driven type checker for {@code select()}.
 *
 * <p>Overloads:
 * <ul>
 *   <li>{@code select<T>(r:Relation<T>[1]):Relation<T>[1]} — no-op pass-through</li>
 *   <li>{@code select<T,Z>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1]):Relation<Z>[1]} — single column</li>
 *   <li>{@code select<T,Z>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1]):Relation<Z>[1]} — multi-column</li>
 * </ul>
 *
 * <p>Validates:
 * <ul>
 *   <li>Source is relational (via unify against signature)</li>
 *   <li>All selected columns exist in source (⊆ constraint)</li>
 *   <li>Output schema is the column subset (Z from signature)</li>
 * </ul>
 */
public class SelectChecker extends AbstractChecker {

    public SelectChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("select", params, source);

        // 1. Bind type variables from signature (T from source)
        var bindings = unify(def, source.expressionType());

        // 2. Source must be relational
        GenericType.Relation.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException("select() requires a relational source");
        }

        // 3. select() with no column arg = pass through, output type from signature
        if (params.size() < 2) {
            return TypeInfo.builder()
                                        .expressionType(resolveOutput(def, bindings, "select()"))
                    .build();
        }

        // 4. Extract and validate columns (enforces Z⊆T constraint)
        List<String> cols = extractColumnNames(params.get(1));
        if (cols.isEmpty()) {
            return TypeInfo.builder()
                                        .expressionType(resolveOutput(def, bindings, "select()"))
                    .build();
        }
        sourceSchema.assertHasColumns(cols);

        // 5. Output schema is the subset (Z from Relation<Z>)
        GenericType.Relation.Schema outputSchema = sourceSchema.onlyColumns(cols);
        return TypeInfo.builder()
                                .columnSpecs(cols.stream().map(TypeInfo.ColumnSpec::col).toList())
                .expressionType(ExpressionType.one(new GenericType.Relation(outputSchema)))
                .build();
    }
}
