package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Generic strict type checker for all registered builtin functions
 * that don't have a dedicated checker.
 *
 * <p>Replaces the old {@code compileTypePropagating} in TypeChecker with
 * zero-fallback, signature-driven type checking. Uses shared helpers from
 * {@link AbstractChecker}: {@link #resolveOverload}, {@link #unify},
 * {@link #compileLambdaArg}, {@link #resolveAssociationsFromParams},
 * {@link #resolveOutput}.
 *
 * <p>Handles all function shapes: no-arg ({@code now()}), unary ({@code abs()}),
 * lambda-taking ({@code map()}, {@code exists()}), multi-param ({@code if()}).
 */
public class ScalarChecker extends AbstractChecker {

    public ScalarChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        String funcName = simpleName(af.function());

        // 1. Resolve overload — strict structural match, no fallbacks
        NativeFunctionDef def = resolveOverload(funcName, params, source);

        // 2. Unify source to bind type variables
        Map<String, GenericType> bindings = new LinkedHashMap<>();
        if (source != null && !def.params().isEmpty()) {
            bindings = unify(def, source.expressionType());
        }

        // 3. Compile remaining params — signature-driven dispatch
        //    compileLambdaArg binds unbound return TypeVars (e.g., V in map<T,V>)
        //    into bindings, so resolveOutput works for all cases.
        for (int i = (source != null ? 1 : 0); i < params.size(); i++) {
            var param = params.get(i);
            PType.Param sigParam = i < def.params().size() ? def.params().get(i) : null;

            if (sigParam != null && isLambdaParam(sigParam) && param instanceof LambdaFunction lambda) {
                compileLambdaArg(lambda, sigParam, bindings, source, ctx, funcName);
            } else if (param instanceof ClassInstance ci
                    && ("colSpec".equals(ci.type()) || "colSpecArray".equals(ci.type()))) {
                // ColSpec params are column name tokens — no compilation needed
            } else {
                TypeInfo paramInfo = env.compileExpr(param, ctx);
                // Unify param type against signature to bind type vars (e.g., T,U in rowMapper<T,U>)
                if (sigParam != null && paramInfo != null && paramInfo.type() != null) {
                    unifyParam(sigParam, paramInfo.type(), bindings, funcName + "() param " + i);
                }
            }
        }

        // 4. Resolve associations
        Map<String, TypeInfo.AssociationTarget> associations = resolveAssociationsFromParams(params, source);

        // 5. Output type from signature + bindings (V already bound by compileLambdaArg)
        ExpressionType outputType = resolveOutput(def, bindings, funcName + "()");

        // 6. Build TypeInfo
        return TypeInfo.builder()
                .mapping(source != null ? source.mapping() : null)
                .associations(associations)
                .expressionType(outputType)
                .build();
    }
}
