package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;

import java.util.List;

/**
 * Checker for {@code eval()} — meta-function that applies a function or lambda.
 *
 * <p>Eval is a meta-function: its return type comes from the compiled body,
 * not the signature ({@code Any[*]}). Both branches store the compiled body
 * as {@code inlinedBody} so PlanGenerator processes the resolved expression.
 *
 * <p>Patterns:
 * <ul>
 *   <li>{@code funcRef->eval(args...)} — rewrite to direct function call</li>
 *   <li>{@code eval(lambda, args...)} — bind lambda params, compile body</li>
 *   <li>{@code eval(lambda)} — compile body with no arg bindings</li>
 * </ul>
 */
public class EvalChecker extends AbstractChecker {

    public EvalChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("eval() requires at least one argument");
        }

        return switch (params.get(0)) {
            // ~colName->eval($row) — column accessor applied to a row
            // Rewrite to $row.colName (property access on the row)
            case ClassInstance ci when "colSpec".equals(ci.type()) -> {
                var colSpec = (ColSpec) ci.value();
                if (params.size() < 2) {
                    throw new PureCompileException(
                            "eval() on ~" + colSpec.name() + " requires a row argument");
                }
                // Compile the row argument to ensure it's typed
                env.compileExpr(params.get(1), ctx);
                // Rewrite to a property access: $row.colName
                AppliedProperty propAccess = new AppliedProperty(
                        colSpec.name(), List.of(params.get(1)));
                TypeInfo bodyResult = env.compileExpr(propAccess, ctx);
                yield TypeInfo.from(bodyResult).inlinedBody(propAccess).build();
            }
            // funcRef->eval(args...) — rewrite to direct function call
            case PackageableElementPtr(String fullPath) -> {
                String funcName = stripTypeSignature(simpleName(fullPath));
                List<ValueSpecification> args = params.subList(1, params.size());
                AppliedFunction resolved = new AppliedFunction(funcName, args);
                TypeInfo bodyResult = env.compileExpr(resolved, ctx);
                yield TypeInfo.from(bodyResult).inlinedBody(resolved).build();
            }
            // eval(lambda, args...) or eval(lambda)
            case LambdaFunction lambda -> {
                TypeChecker.CompilationContext evalCtx = params.size() > 1
                        ? bindLambdaArgs(lambda, params.subList(1, params.size()), ctx)
                        : ctx;
                TypeInfo bodyResult = compileLambdaBody(lambda, evalCtx);
                ValueSpecification lastStmt = lambda.body().get(lambda.body().size() - 1);
                yield TypeInfo.from(bodyResult).inlinedBody(lastStmt).build();
            }
            default -> throw new PureCompileException(
                    "eval(): first argument must be a function reference, column spec, or lambda, got "
                            + params.get(0).getClass().getSimpleName());
        };
    }

    /**
     * Binds each lambda parameter to its corresponding eval argument as a
     * let binding, compiling each arg along the way.
     */
    private TypeChecker.CompilationContext bindLambdaArgs(
            LambdaFunction lambda, List<ValueSpecification> args,
            TypeChecker.CompilationContext ctx) {
        TypeChecker.CompilationContext result = ctx;
        int bound = Math.min(lambda.parameters().size(), args.size());
        for (int i = 0; i < bound; i++) {
            env.compileExpr(args.get(i), result);
            result = result.withLetBinding(lambda.parameters().get(i).name(), args.get(i));
        }
        return result;
    }

    /**
     * Strips the Pure type-signature suffix from a function name.
     * E.g., {@code sinh_Number_1__Float_1_} → {@code sinh}.
     *
     * <p>Assumes Pure function names are camelCase (no underscores).
     */
    private static String stripTypeSignature(String name) {
        int idx = name.indexOf('_');
        return idx > 0 ? name.substring(0, idx) : name;
    }
}
