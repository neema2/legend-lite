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
        // Resolve to FQN — ensures all downstream keys (classPropertyAccesses,
        // associationNavigations, storeClassNames) use consistent FQN keys.
        String fqn = findClass(fullPath).map(c -> c.qualifiedName()).orElse(fullPath);

        // Compile mapping-level expressions (filter, property expressions, etc.)
        env.modelContext().findMappingExpression(fqn).ifPresent(mapExpr -> {
            switch (mapExpr) {
                case MappingExpression.M2M m2m -> compileM2MExpressions(fqn, m2m, new HashSet<>());
                case MappingExpression.Relational rel -> compileRelationalExpressions(rel);
            }
        });

        // Always return ClassType[*] — same for relational and M2M
        return TypeInfo.builder()
                .expressionType(ExpressionType.many(new GenericType.ClassType(fqn)))
                .build();
    }

    /**
     * Compiles the M2M sourceSpec chain — same pattern as relational.
     * The inner getAll("SrcClass") triggers recursive compilation of the source
     * mapping via GetAllChecker.check(), so no manual recursion needed.
     */
    private void compileM2MExpressions(String targetClassName, MappingExpression.M2M m2m, Set<String> visited) {
        if (!visited.add(targetClassName)) return;
        if (m2m.sourceSpec() == null) return;

        var ctx = new TypeChecker.CompilationContext();
        env.compileExpr(m2m.sourceSpec(), ctx);
    }

    /**
     * Compiles the sourceSpec ValueSpec chain synthesized by MappingNormalizer,
     * then compiles all join traversals (association primitives).
     * Recursively follows associations to compile transitively reachable classes'
     * traversals — needed for multi-hop joins (Person → Firm → Country).
     */
    private void compileRelationalExpressions(MappingExpression.Relational rel) {
        compileRelationalExpressions(rel, new HashSet<>());
    }

    private void compileRelationalExpressions(MappingExpression.Relational rel, Set<String> visited) {
        if (!visited.add(rel.className())) return;

        // sourceSpec already contains association traversals as extend() nodes
        // with fn1=traverse. compileExpr walks the full chain including those.
        var ctx = new TypeChecker.CompilationContext();
        env.compileExpr(rel.sourceSpec(), ctx);
        env.markSourceSpecCompiled(rel.className());

        // Target classes' source relations are compiled on-demand by
        // TypeChecker.compileNeededAssociationTargets() (pass 2) — only for
        // associations the query actually navigates.
    }
}
