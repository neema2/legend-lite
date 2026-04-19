package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.model.m3.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * Signature-driven type checker for {@code rename()}.
 *
 * <p>Two overloads:
 * <ul>
 *   <li>Single: {@code rename<T,Z,K,V>(r:Relation<T>[1], old:ColSpec<Z⊆T>[1], new:ColSpec<V>[1]):Relation<T-Z+V>[1]}</li>
 *   <li>Batch:  {@code rename<T,Z,V>(r:Relation<T>[1], oldCols:ColSpecArray<Z⊆T>[1], newCols:ColSpecArray<V>[1]):Relation<T-Z+V>[1]}</li>
 * </ul>
 *
 * <p>Validates:
 * <ul>
 *   <li>Source is relational (has schema)</li>
 *   <li>Old column(s) exist in source schema</li>
 *   <li>Old and new column counts match (batch rename)</li>
 *   <li>No duplicate new column names</li>
 * </ul>
 *
 * <p>Output schema: source schema with old columns replaced by new column names,
 * preserving types. Populates {@link TypeInfo.ColumnSpec#renamed} pairs for PlanGenerator.
 */
public class RenameChecker extends AbstractChecker {

    public RenameChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("rename", params, source);
        unify(def, source.expressionType()); // validate source matches signature generics

        // 2. Source must be relational
        Type.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException("rename() requires a relational source");
        }

        // 3. Extract old and new column names (single or batch)
        List<String> oldNames = extractColumnNames(params.get(1));
        List<String> newNames = extractColumnNames(params.get(2));

        // 4. Validate old/new count match
        if (oldNames.size() != newNames.size()) {
            throw new PureCompileException(
                    "rename(): old column count (" + oldNames.size()
                            + ") must match new column count (" + newNames.size() + ")");
        }

        // 5. Validate all old columns exist in source
        for (String oldName : oldNames) {
            sourceSchema.assertHasColumn(oldName);
        }

        // 6. Build output schema by applying renames sequentially
        Type.Schema outputSchema = sourceSchema;
        List<TypeInfo.ColumnSpec> colSpecs = new ArrayList<>();
        for (int i = 0; i < oldNames.size(); i++) {
            outputSchema = outputSchema.renameColumn(oldNames.get(i), newNames.get(i));
            colSpecs.add(TypeInfo.ColumnSpec.renamed(oldNames.get(i), newNames.get(i)));
        }

        return TypeInfo.builder()
                .columnSpecs(colSpecs)
                .expressionType(ExpressionType.one(new Type.Relation(outputSchema)))
                .build();
    }
}
