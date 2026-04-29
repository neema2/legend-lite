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
 * <p>Returns {@code ClassType[*]}. The class's synthetic mapping function is
 * compiled speculatively (when present in scope) and attached to the returned
 * {@link TypedGetAll} as a downstream-convenience artifact.
 *
 * <p>Missing mapping is <strong>not</strong> a type error — type-check is a
 * front-end concern; mapping resolution is a back-end / link-time concern.
 * If the class has no mapping in the active scope, {@link TypedGetAll#mappingFn()}
 * is {@code null} and the precise "no mapping for class X" error fires later
 * at the back-end use site (e.g. {@code SourceLowering}). This matches how a
 * real compiler treats missing symbol definitions: compile-only succeeds,
 * link surfaces the error.
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

        // Probe variant — missing mapping is a back-end / link-time concern,
        // not a type-check failure. SourceLowering surfaces the precise error
        // at the use site if the query is actually lowered.
        CompiledFunction mappingFn = env.tryCompileMappingFunctionFor(fqn).orElse(null);

        return new TypedGetAll(fqn, mappingFn,
                ExpressionType.many(new Type.ClassType(fqn)));
    }
}
