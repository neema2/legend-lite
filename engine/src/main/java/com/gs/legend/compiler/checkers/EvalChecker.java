package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;

import java.util.List;

/**
 * Checker for {@code eval()} — meta-function that inlines function bodies.
 *
 * <p>Handles two patterns:
 * <ul>
 *   <li>{@code eval(functionRef, args...)} — rewrites to normal function call</li>
 *   <li>{@code eval(lambda, args...)} — binds lambda params to args and compiles body</li>
 * </ul>
 *
 * <p>Both cases store the compiled body as {@code inlinedBody} so PlanGenerator
 * processes the resolved expression instead of the original eval call.
 */
public class EvalChecker extends AbstractChecker {

    public EvalChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();

        // eval(functionRef, args...) — rewrite to normal function call
        if (params.size() >= 2 && params.get(0) instanceof PackageableElementPtr(String fullPath)) {
            String lastSegment = fullPath.contains("::")
                    ? fullPath.substring(fullPath.lastIndexOf("::") + 2)
                    : fullPath;
            String funcName = lastSegment.contains("_")
                    ? lastSegment.substring(0, lastSegment.indexOf('_'))
                    : lastSegment;
            // Create resolved function node and compile it via compileExpr dispatch
            List<ValueSpecification> evalArgs = params.subList(1, params.size());
            AppliedFunction resolved = new AppliedFunction(funcName, evalArgs);
            TypeInfo bodyResult = env.compileExpr(resolved, ctx);
            // Store as inlinedBody — PlanGenerator follows this pointer
            return TypeInfo.from(bodyResult).inlinedBody(resolved).build();
        }

        // eval(lambda, args) — compile lambda body and store as inlinedBody
        if (params.size() >= 2
                && params.get(0) instanceof LambdaFunction(List<Variable> parameters, List<ValueSpecification> body)) {
            if (!body.isEmpty()) {
                // Bind each lambda param to its corresponding arg value
                TypeChecker.CompilationContext evalCtx = ctx;
                List<ValueSpecification> args = params.subList(1, params.size());
                for (int i = 0; i < parameters.size() && i < args.size(); i++) {
                    env.compileExpr(args.get(i), ctx);
                    evalCtx = evalCtx.withLetBinding(parameters.get(i).name(), args.get(i));
                }
                ValueSpecification evalBody = body.get(0);
                TypeInfo bodyResult = env.compileExpr(evalBody, evalCtx);
                return TypeInfo.from(bodyResult).inlinedBody(evalBody).build();
            }
        }

        // Fallback: route through ScalarChecker
        TypeInfo evalSource = !params.isEmpty() ? env.compileExpr(params.get(0), ctx) : null;
        return new ScalarChecker(env).check(af, evalSource, ctx);
    }
}
