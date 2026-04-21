package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedDistinct;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Type;

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

    public TypedSpec check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("distinct", params, source);
        var bindings = unify(def, source.expressionType());

        Type.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException("distinct() requires a relational source");
        }

        // No-column overload (or explicit empty column list): dedup on all
        // source columns; output schema = source schema (via resolveOutput).
        List<String> cols = params.size() < 2 ? List.of() : extractColumnNames(params.get(1));
        if (cols.isEmpty()) {
            return new TypedDistinct(source, List.of(),
                    resolveOutput(def, bindings, "distinct()"));
        }

        // Columns overload: enforce X ⊆ T, project to the subset.
        sourceSchema.assertHasColumns(cols);
        Type.Schema outputSchema = sourceSchema.onlyColumns(cols);
        return new TypedDistinct(source, cols,
                ExpressionType.one(new Type.Relation(outputSchema)));
    }
}
