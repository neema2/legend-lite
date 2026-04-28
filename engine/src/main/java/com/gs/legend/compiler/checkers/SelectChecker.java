package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedSelect;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Type;

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

    public TypedSelect check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("select", params, source);
        var bindings = unify(def, source.expressionType());

        Type.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException("select() requires a relational source");
        }

        // Pass-through overload (or empty col list): output = source schema.
        List<String> cols = params.size() < 2 ? List.of() : extractColumnNames(params.get(1));
        if (cols.isEmpty()) {
            return new TypedSelect(source, List.of(), def,
                    resolveOutput(def, bindings, "select()"));
        }

        // Columns overload: Z ⊆ T, output schema = source ↓ Z.
        sourceSchema.assertHasColumns(cols);
        Type.Schema outputSchema = sourceSchema.onlyColumns(cols);
        return new TypedSelect(source, cols, def,
                ExpressionType.one(new Type.Relation(outputSchema)));
    }
}
