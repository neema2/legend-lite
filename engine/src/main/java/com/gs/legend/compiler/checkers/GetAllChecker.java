package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.PackageableElementPtr;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.List;

/**
 * Checker for {@code getAll()}.
 *
 * <p>Returns {@code ClassType[*]} for both relational and M2M mappings and
 * ensures the class's normalized sourceSpec is type-stamped before
 * {@code PlanGenerator} looks at it. The sourceSpec walk itself is delegated
 * to {@link TypeCheckEnv#compileSourceSpecFor(String)} — the single
 * primitive shared with pass-2 association fan-out and the build-path
 * {@code compileMapping}. Target-class sourceSpecs (for associations the
 * query actually navigates) are still compiled on-demand by
 * {@code TypeChecker.compileNeededAssociationTargets()} (pass 2).
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

        if (!(params.get(0) instanceof PackageableElementPtr(String fullPath))) {
            throw new PureCompileException(
                    "getAll(): first argument must be a class reference, got "
                            + params.get(0).getClass().getSimpleName());
        }
        // Resolve to FQN — ensures all downstream keys (classPropertyAccesses,
        // associationNavigations, storeClassNames) use consistent FQN keys.
        String fqn = findClass(fullPath).map(c -> c.qualifiedName()).orElse(fullPath);

        env.compileSourceSpecFor(fqn);

        return TypeInfo.builder()
                .expressionType(ExpressionType.many(new GenericType.ClassType(fqn)))
                .build();
    }
}
