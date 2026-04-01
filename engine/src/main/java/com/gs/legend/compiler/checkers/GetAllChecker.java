package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.PackageableElementPtr;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.model.ModelContext.MappingExpression;
import com.gs.legend.plan.GenericType;

import java.util.List;

/**
 * Checker for {@code getAll()}.
 *
 * <p>Returns ClassType[*] for both relational and M2M mappings.
 * Dispatches on {@link MappingExpression} variant:
 * <ul>
 *   <li>{@link MappingExpression.M2M}: type-checks property expressions with $src as ClassType</li>
 *   <li>{@link MappingExpression.Relational}: type-checks filter with $row as Relation row</li>
 * </ul>
 * No registry access — only ModelContext.findMappingExpression().
 */
public class GetAllChecker extends AbstractChecker {

    public GetAllChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        resolveOverload("getAll", params, null);

        // Extract class name from PackageableElementPtr
        if (!(params.get(0) instanceof PackageableElementPtr(String fullPath))) {
            throw new PureCompileException(
                    "getAll(): first argument must be a class reference, got "
                            + params.get(0).getClass().getSimpleName());
        }
        String className = TypeInfo.simpleName(fullPath);

        // Compile mapping-level expressions (filter, property expressions, etc.)
        env.modelContext().findMappingExpression(className).ifPresent(mapExpr -> {
            switch (mapExpr) {
                case MappingExpression.M2M m2m -> compileM2MExpressions(m2m);
                case MappingExpression.Relational rel -> compileRelationalExpressions(rel);
            }
        });

        // Always return ClassType[*] — same for relational and M2M
        return TypeInfo.builder()
                .expressionType(ExpressionType.many(new GenericType.ClassType(fullPath)))
                .build();
    }

    /**
     * Type-checks M2M property expressions and filter.
     * Recursively compiles source chain if source is also M2M.
     * Uses withLambdaParam("src", ClassType) — $src is a class instance,
     * resolved via compileProperty's existing ClassType path (findClass → findProperty).
     */
    private void compileM2MExpressions(MappingExpression.M2M m2m) {
        // Recursively compile source chain (if source is also M2M)
        env.modelContext().findMappingExpression(m2m.sourceClassName())
                .ifPresent(src -> { if (src instanceof MappingExpression.M2M srcM2M) compileM2MExpressions(srcM2M); });

        // $src is a class instance — use the existing ClassType property resolution path.
        var srcCtx = new TypeChecker.CompilationContext()
                .withLambdaParam("src", new GenericType.ClassType(m2m.sourceClassName()));

        // Compile each property expression + filter
        for (var expr : m2m.propertyExpressions().values()) {
            env.compileExpr(expr, srcCtx);
        }
        if (m2m.filter() != null) {
            env.compileExpr(m2m.filter(), srcCtx);
        }
    }

    /**
     * Compiles relational mapping-level expressions (filter, future: groupBy, primaryKey).
     * These are ValueSpecs produced by MappingNormalizer from RelationalOperation AST.
     * Uses withRelationType to bind $row with the table's column schema.
     */
    private void compileRelationalExpressions(MappingExpression.Relational rel) {
        if (rel.filter() == null) return;

        var rowCtx = new TypeChecker.CompilationContext()
                .withRelationType("row", rel.schema());

        env.compileExpr(rel.filter(), rowCtx);
    }
}
