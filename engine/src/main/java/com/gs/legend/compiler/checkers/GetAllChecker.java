package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.PackageableElementPtr;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.model.ModelContext.MappingExpression;
import com.gs.legend.plan.GenericType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
                case MappingExpression.M2M m2m -> compileM2MExpressions(className, m2m, new HashSet<>());
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
    private void compileM2MExpressions(String targetClassName, MappingExpression.M2M m2m, Set<String> visited) {
        if (!visited.add(targetClassName)) return;

        // Recursively compile source mapping expressions.
        // If source is M2M, recurse into it. If source is Relational, compile its
        // traverse expressions so join conditions get TypeInfo for PlanGenerator.
        env.modelContext().findMappingExpression(m2m.sourceClassName())
                .ifPresent(src -> {
                    switch (src) {
                        case MappingExpression.M2M srcM2M -> compileM2MExpressions(m2m.sourceClassName(), srcM2M, visited);
                        case MappingExpression.Relational srcRel -> compileRelationalExpressions(srcRel);
                    }
                });

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

        // Recursively compile M2M mappings for nested class-typed properties.
        // E.g., PersonWithAddress has property address:Address[*]. If Address has
        // its own M2M mapping, compile it so PlanGenerator has TypeInfo for its expressions.
        env.modelContext().findClass(targetClassName).ifPresent(pc -> {
            for (var prop : pc.properties()) {
                String propTypeName = prop.genericType().typeName();
                env.modelContext().findMappingExpression(propTypeName).ifPresent(propExpr -> {
                    if (propExpr instanceof MappingExpression.M2M propM2M) {
                        compileM2MExpressions(propTypeName, propM2M, visited);
                    }
                });
            }
        });
    }

    /**
     * Compiles the sourceRelation ValueSpec chain synthesized by MappingNormalizer,
     * then compiles all join traversals (association primitives).
     * Recursively follows associations to compile transitively reachable classes'
     * traversals — needed for multi-hop joins (Person → Firm → Country).
     */
    private void compileRelationalExpressions(MappingExpression.Relational rel) {
        compileRelationalExpressions(rel, new HashSet<>());
    }

    private void compileRelationalExpressions(MappingExpression.Relational rel, Set<String> visited) {
        if (!visited.add(rel.className())) return;

        // sourceRelation already contains association traversals as extend() nodes
        // with fn1=traverse. compileExpr walks the full chain including those.
        var ctx = new TypeChecker.CompilationContext();
        env.compileExpr(rel.sourceRelation(), ctx);

        // Recurse into association target classes to compile their expressions
        for (var nav : env.modelContext().findAllAssociationNavigations(rel.className()).values()) {
            env.modelContext().findMappingExpression(nav.targetClassName())
                    .ifPresent(mapExpr -> {
                        if (mapExpr instanceof MappingExpression.Relational targetRel) {
                            compileRelationalExpressions(targetRel, visited);
                        }
                    });
        }
    }
}
