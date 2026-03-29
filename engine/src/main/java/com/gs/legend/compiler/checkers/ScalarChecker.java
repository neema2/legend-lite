package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Generic strict type checker for all registered builtin functions
 * that don't have a dedicated checker.
 *
 * <p>Uses compile-then-resolve: compiles all non-lambda params first,
 * passes their types into overload resolution for precise matching,
 * then compiles lambda params with the resolved signature.
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
        int startIdx = source != null ? 1 : 0;

        // 1. Pre-compile all non-lambda, non-colspec params to get their types.
        //    These expression types (type + multiplicity) feed into overload
        //    resolution for precise matching on both axes.
        Map<Integer, ExpressionType> compiledTypes = new LinkedHashMap<>();
        Map<Integer, TypeInfo> compiledInfos = new LinkedHashMap<>();

        if (source != null && source.type() != null) {
            compiledTypes.put(0, source.expressionType());
        }
        for (int i = startIdx; i < params.size(); i++) {
            var param = params.get(i);
            if (param instanceof LambdaFunction || isColSpec(param)) {
                continue; // lambdas need target-typing; colspecs are structural tokens
            }
            TypeInfo paramInfo = env.compileExpr(param, ctx);
            if (paramInfo != null && paramInfo.type() != null) {
                compiledTypes.put(i, paramInfo.expressionType());
                compiledInfos.put(i, paramInfo);
            }
        }

        // 2. Resolve overload — uses compiled types for structural match + scoring
        NativeFunctionDef def = resolveOverload(funcName, params, source, compiledTypes);

        // 3. Unify source to bind type + mult variables
        var bindings = new Bindings();
        if (source != null && !def.params().isEmpty()) {
            bindings = unify(def, source.expressionType());
        }

        // 4. Process remaining params: unify pre-compiled ones, compile lambdas
        for (int i = startIdx; i < params.size(); i++) {
            var param = params.get(i);
            PType.Param sigParam = i < def.params().size() ? def.params().get(i) : null;

            if (sigParam != null && isLambdaParam(sigParam) && param instanceof LambdaFunction lambda) {
                // Lambdas are compiled AFTER resolution — they need the resolved signature
                compileLambdaArg(lambda, sigParam, bindings, source, ctx, funcName);
            } else if (param instanceof LambdaFunction lambda && sigParam != null) {
                // Lambda passed to a non-Function param (e.g., pair(|expr, |expr) where sig is T[1], U[1]).
                // These are "lazy evaluator" lambdas — {->T} sugar. Compile the body and bind the TypeVar.
                TypeInfo bodyResult = compileLambdaBody(lambda, ctx);
                if (bodyResult.type() != null) {
                    unifyParam(sigParam, bodyResult.type(), bindings, funcName + "() param " + i);
                }
            } else if (isColSpec(param)) {
                // ColSpec params are column name tokens — no compilation needed
            } else {
                // Already compiled in step 1 — just unify against the signature
                TypeInfo paramInfo = compiledInfos.get(i);
                if (sigParam != null && paramInfo != null && paramInfo.type() != null) {
                    unifyParam(sigParam, paramInfo.type(), bindings, funcName + "() param " + i);
                }
            }
        }

        // 5. Resolve associations
        Map<String, TypeInfo.AssociationTarget> associations = resolveAssociationsFromParams(params, source);

        // 6. Output type from signature + bindings
        //    Mult-vars (e.g., m in reverse<T|m>) are auto-bound by unify()
        ExpressionType outputType = resolveOutput(def, bindings, funcName + "()");

        // 7. Build TypeInfo
        return TypeInfo.builder()
                .mapping(source != null ? source.mapping() : null)
                .associations(associations)
                .expressionType(outputType)
                .resolvedFunc(def)
                .build();
    }

    private static boolean isColSpec(ValueSpecification param) {
        return param instanceof ClassInstance ci
                && ("colSpec".equals(ci.type()) || "colSpecArray".equals(ci.type()));
    }
}

