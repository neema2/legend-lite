package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedSpec;

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

    public TypedSpec check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("eval() requires at least one argument");
        }

        return switch (params.get(0)) {
            // {@code ~colName->eval($row)} — rewrite to {@code $row.colName}.
            case ColSpec colSpec -> {
                if (params.size() < 2) {
                    throw new PureCompileException(
                            "eval() on ~" + colSpec.name() + " requires a row argument");
                }
                AppliedProperty propAccess = new AppliedProperty(
                        colSpec.name(), List.of(params.get(1)));
                yield env.compileExpr(propAccess, ctx);
            }
            // {@code funcRef->eval(args...)} — rewrite to a direct call.
            case PackageableElementPtr(String fullPath) -> {
                String funcName = stripTypeSignature(simpleName(fullPath));
                List<ValueSpecification> args = params.subList(1, params.size());
                yield env.compileExpr(new AppliedFunction(funcName, args), ctx);
            }
            // {@code eval(lambda, args...)} or {@code eval(lambda)}.
            case LambdaFunction lambda -> {
                TypeChecker.CompilationContext evalCtx = params.size() > 1
                        ? bindLambdaArgs(lambda, params.subList(1, params.size()), ctx)
                        : ctx;
                yield compileLambdaBody(lambda, evalCtx);
            }
            // {@code $f->eval(args...)} where {@code $f} is a Variable of
            // FunctionType. Occurs during declaration-time body compile of a
            // user function whose signature declares a function-typed
            // parameter (e.g. {@code {Integer[1]->Integer[1]}[1]}). We can't
            // walk a body at this point — the Variable will be replaced with a
            // concrete lambda at each call site by {@code inlineUserFunction}.
            // Type-check using the FunctionType's declared return; the
            // per-call specialization re-walks with the real lambda.
            case Variable var -> {
                TypedSpec varTyped = env.compileExpr(var, ctx);
                if (!(varTyped instanceof com.gs.legend.compiler.typed.TypedVariable typedVar)) {
                    throw new PureCompileException(
                            "eval(): first argument '" + var.name()
                                    + "' did not compile to a TypedVariable (got "
                                    + varTyped.getClass().getSimpleName() + ")");
                }
                if (!(typedVar.type() instanceof com.gs.legend.model.m3.Type.FunctionType ft)) {
                    throw new PureCompileException(
                            "eval(): first argument is a variable '" + var.name()
                                    + "' that does not have a function type (got "
                                    + (typedVar.type() == null ? "null" : typedVar.type().typeName())
                                    + ")");
                }
                // Compile remaining args for type-check side effects.
                java.util.List<TypedSpec> argSpecs = new java.util.ArrayList<>();
                for (int i = 1; i < params.size(); i++) {
                    argSpecs.add(env.compileExpr(params.get(i), ctx));
                }
                ExpressionType outType =
                        ft.returnMult() != null && ft.returnMult().upperBound() != null
                                && ft.returnMult().upperBound() > 1
                                ? ExpressionType.many(ft.returnType())
                                : ExpressionType.one(ft.returnType());
                yield new com.gs.legend.compiler.typed.TypedEval(typedVar, argSpecs, outType);
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
