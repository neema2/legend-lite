package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.PackageableElementPtr;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiled.CompiledFunction;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedGetAll;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Type;

import java.util.List;

/**
 * Checker for {@code getAll()}.
 *
 * <p>Returns {@code ClassType[*]} and ensures the class's synthetic mapping
 * function is compiled and available in the shared function symbol table.
 * Throws if the class has no mapping in the active scope — {@code .all()} on
 * an unmapped class is a compile error, not a silently-empty result.
 *
 * <p>The compiled function is not embedded on the returned {@link TypedGetAll};
 * downstream consumers (MappingResolver, PlanGenerator) resolve it via the
 * shared function table on {@link com.gs.legend.compiled.CompiledExpression}
 * combined with the runtime-specific binding on {@code NormalizedMapping}.
 */
public class GetAllChecker extends AbstractChecker {

    public GetAllChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypedGetAll check(AppliedFunction af, TypedSpec source,
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

        CompiledFunction mappingFn = env.compileMappingFunctionFor(fqn);

        return new TypedGetAll(fqn, mappingFn,
                ExpressionType.many(new Type.ClassType(fqn)));
    }
}
