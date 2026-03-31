package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GraphFetchSpec;

/**
 * Checker for {@code graphFetch(source, #{Tree}#)}.
 *
 * <p>Type-checks:
 * <ol>
 *   <li>Source must be class-based (ClassType)</li>
 *   <li>Root class in tree must match source class</li>
 *   <li>All properties in tree must exist on the target class</li>
 *   <li>Nested properties must be class-typed (not scalars)</li>
 * </ol>
 */
public class GraphFetchChecker extends AbstractChecker {

    public GraphFetchChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        // Compile source (e.g., Person.all())
        TypeInfo sourceInfo = env.compileExpr(af.parameters().get(0), ctx);

        // (1) Source must be class-based (ClassType)
        if (!(sourceInfo.type() instanceof com.gs.legend.plan.GenericType.ClassType classType)) {
            throw new PureCompileException(
                    "graphFetch() requires a class-based source (e.g., Person.all()), "
                            + "but source type is " + sourceInfo.type());
        }

        // Extract GraphFetchTree from ClassInstance parameter
        com.gs.legend.ast.GraphFetchTree tree = null;
        if (af.parameters().size() > 1 && af.parameters().get(1) instanceof ClassInstance ci
                && ci.value() instanceof com.gs.legend.ast.GraphFetchTree gft) {
            tree = gft;
        }
        if (tree == null) {
            throw new PureCompileException("graphFetch() requires a graph fetch tree argument #{...}#");
        }

        // (2) Root class must match source class
        String className = TypeInfo.simpleName(classType.qualifiedName());
        var targetClass = findClass(className)
                .orElseThrow(() -> new PureCompileException(
                        "graphFetch(): class '" + className + "' not found in model"));
        if (!tree.rootClass().equals(targetClass.name())
                && !tree.rootClass().equals(targetClass.qualifiedName())) {
            throw new PureCompileException(
                    "graphFetch tree root class '" + tree.rootClass()
                            + "' does not match source class '" + targetClass.qualifiedName() + "'");
        }

        // (3+4) Validate all properties exist and nested types are correct
        var spec = toGraphFetchSpec(tree, targetClass);

        return TypeInfo.from(sourceInfo)
                .graphFetchSpec(spec)
                .build();
    }

    /**
     * Transforms a parser-level GraphFetchTree into a plan-level GraphFetchSpec.
     * Validates all properties against the target class:
     * - Each property must exist on the class (including inherited)
     * - Nested properties must be class-typed (not scalar/primitive)
     *
     * Package-visible so SerializeChecker can reuse it.
     */
    static GraphFetchSpec toGraphFetchSpec(
            com.gs.legend.ast.GraphFetchTree tree,
            com.gs.legend.model.m3.PureClass targetClass) {
        var properties = tree.properties().stream()
                .map(pf -> {
                    // Validate property exists on the class
                    var propOpt = targetClass.findProperty(pf.name());
                    if (propOpt.isEmpty()) {
                        throw new PureCompileException(
                                "Property '" + pf.name() + "' not found on class '"
                                        + targetClass.qualifiedName() + "'. Available: "
                                        + targetClass.allProperties().stream()
                                                .map(com.gs.legend.model.m3.Property::name)
                                                .toList());
                    }

                    if (pf.isNested()) {
                        // Validate nested property is class-typed
                        var prop = propOpt.get();
                        if (!(prop.genericType() instanceof com.gs.legend.model.m3.PureClass nestedClass)) {
                            throw new PureCompileException(
                                    "Property '" + pf.name() + "' on class '"
                                            + targetClass.qualifiedName()
                                            + "' is not class-typed — cannot nest in graphFetch tree. "
                                            + "Type: " + prop.genericType().typeName());
                        }
                        var nestedSpec = toGraphFetchSpec(pf.subTree(), nestedClass);
                        return GraphFetchSpec.PropertySpec.nested(pf.name(), nestedSpec);
                    }
                    return GraphFetchSpec.PropertySpec.scalar(pf.name());
                })
                .toList();
        return new GraphFetchSpec(tree.rootClass(), properties);
    }
}
