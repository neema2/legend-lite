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
 * For M2M, type-checks property expressions against the source PureClass
 * via {@link MappingExpression} (no routing info, no registry access).
 * Chain resolution and physical mapping are handled by MappingResolver.
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

        // M2M? Compile expressions against source PureClass
        var mapExpr = env.modelContext().findMappingExpression(className).orElse(null);
        if (mapExpr != null) {
            compileMappingExpressions(mapExpr);
        }

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
    private void compileMappingExpressions(MappingExpression mapExpr) {
        // Recursively compile source chain (if source is also M2M)
        var sourceExpr = env.modelContext().findMappingExpression(mapExpr.sourceClassName()).orElse(null);
        if (sourceExpr != null) {
            compileMappingExpressions(sourceExpr);
        }

        // $src is a class instance — use the existing ClassType property resolution path.
        // compileProperty resolves $src.prop via PureClass.findProperty() + associations.
        var srcCtx = new TypeChecker.CompilationContext()
                .withLambdaParam("src", new GenericType.ClassType(mapExpr.sourceClassName()));

        // Compile each property expression + filter
        for (var expr : mapExpr.propertyExpressions().values()) {
            env.compileExpr(expr, srcCtx);
        }
        if (mapExpr.filter() != null) {
            env.compileExpr(mapExpr.filter(), srcCtx);
        }
    }
}
