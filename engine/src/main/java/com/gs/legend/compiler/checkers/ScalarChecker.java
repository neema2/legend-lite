package com.gs.legend.compiler.checkers;
import com.gs.legend.model.m3.Type;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedNativeCall;
import com.gs.legend.compiler.typed.TypedSpec;
import java.util.ArrayList;
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
    public TypedNativeCall check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        String funcName = simpleName(af.function());
        int startIdx = source != null ? 1 : 0;

        // Pre-compile non-lambda, non-colspec params so their
        // (type, multiplicity) tuples feed into overload resolution for
        // structural match + scoring. {@code typedArgs[i]} holds the TypedSpec
        // for param[i] once compiled; lambdas and colspecs fill in later.
        Map<Integer, ExpressionType> compiledTypes = new LinkedHashMap<>();
        TypedSpec[] typedArgs = new TypedSpec[params.size()];

        if (source != null) {
            typedArgs[0] = source;
            if (source.type() != null) compiledTypes.put(0, source.expressionType());
        }
        for (int i = startIdx; i < params.size(); i++) {
            var param = params.get(i);
            if (param instanceof LambdaFunction || isColSpec(param)) {
                continue; // lambdas need target-typing post-resolve; colspecs are structural
            }
            TypedSpec argTyped = env.compileExpr(param, ctx);
            typedArgs[i] = argTyped;
            if (argTyped.type() != null) {
                compiledTypes.put(i, argTyped.expressionType());
            }
        }

        // Resolve overload using compiled-arg type info.
        NativeFunctionDef def = resolveOverload(funcName, params, source, compiledTypes);

        // Unify source to bind type + mult variables (e.g. {@code m} in
        // {@code reverse<T|m>}).
        var bindings = new Bindings();
        if (source != null && !def.params().isEmpty()) {
            bindings = unify(def, source.expressionType());
        }

        // Process remaining params: lambdas need target-typing with the resolved
        // signature; pre-compiled scalars unify against their sig param.
        for (int i = startIdx; i < params.size(); i++) {
            var param = params.get(i);
            Type.Parameter sigParam = i < def.params().size() ? def.params().get(i) : null;

            if (sigParam != null && isLambdaParam(sigParam)
                    && param instanceof LambdaFunction lambda) {
                typedArgs[i] = compileLambdaArg(
                        lambda, sigParam, bindings, ctx, funcName, source);
            } else if (param instanceof LambdaFunction lambda && sigParam != null) {
                // Lambda passed to a non-Function param — "lazy evaluator" {->T}
                // sugar (e.g. {@code pair(|expr, |expr)}). Compile the body and
                // bind the sig's TypeVar from the body's result type.
                TypedLambda lambdaTyped = compileLambdaArg(
                        lambda, sigParam, bindings, ctx, funcName, source);
                typedArgs[i] = lambdaTyped;
            } else if (isColSpec(param)) {
                // ColSpec stays AST-shaped — no TypedSpec representation yet.
                // PlanGenerator reads the column name off the AST directly.
            } else if (sigParam != null && typedArgs[i] != null
                    && typedArgs[i].type() != null) {
                unifyParam(sigParam, typedArgs[i].type(), bindings,
                        funcName + "() param " + i);
            }
        }

        // Output type from signature + bindings. Mult-vars auto-bound by unify().
        ExpressionType outputType = resolveOutput(def, bindings, funcName + "()");

        // Collect the TypedSpec args in parameter order, skipping null slots
        // (colspecs). Downstream callers pair {@code def.params()} with
        // {@code args} positionally — the absence of a colspec arg is implied
        // by the signature type at that index.
        List<TypedSpec> args = new ArrayList<>(typedArgs.length);
        for (TypedSpec t : typedArgs) {
            if (t != null) args.add(t);
        }
        return new TypedNativeCall(def, args, outputType);
    }

    private static boolean isColSpec(ValueSpecification param) {
        return param instanceof com.gs.legend.ast.ColumnInstance;
    }
}

