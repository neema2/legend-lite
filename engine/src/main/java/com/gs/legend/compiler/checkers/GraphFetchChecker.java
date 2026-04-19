package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.model.m3.Type;

import java.util.List;

/**
 * Checker for {@code graphFetch(source, ColSpec|ColSpecArray)}.
 *
 * <p>Compiles fn1 for each ColSpec (property access + association registration).
 * If fn2 exists (nested), extracts nested ColSpecArray and recurses with fn1's
 * result type. PlanGenerator reads fn2 directly from the AST for nesting structure.
 */
public class GraphFetchChecker extends AbstractChecker {

    public GraphFetchChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        TypeInfo sourceInfo = env.compileExpr(af.parameters().get(0), ctx);

        if (!(sourceInfo.type() instanceof Type.ClassType classType)) {
            throw new PureCompileException(
                    "graphFetch() requires a class-based source, but got " + sourceInfo.type());
        }

        if (af.parameters().size() < 2) {
            throw new PureCompileException("graphFetch() requires a column spec argument");
        }

        ColSpecArray colSpecs = extractColSpecs(af.parameters().get(1));
        compileColSpecs(colSpecs, classType, ctx);

        // Preserve source ClassType — graphFetch is a projection, not a type change.
        // The JSON formatting is an execution concern handled by PlanGenerator.
        return TypeInfo.builder()
                .expressionType(sourceInfo.expressionType())
                .build();
    }

    /** Compiles ColSpec fn1 lambdas and recurses into fn2 for nested properties. */
    private void compileColSpecs(ColSpecArray csa, Type.ClassType classType,
                                 TypeChecker.CompilationContext ctx) {
        for (ColSpec cs : csa.colSpecs()) {
            if (cs.function1() == null) {
                throw new PureCompileException(
                        "graphFetch ColSpec '" + cs.name() + "' must have a lambda");
            }
            LambdaFunction fn1 = cs.function1();
            String paramName = fn1.parameters().isEmpty() ? null : fn1.parameters().get(0).name();
            var lambdaCtx = bindLambdaParam(ctx, paramName, classType, null);
            TypeInfo fn1Result = compileLambdaBody(fn1, lambdaCtx);

            // Nested: fn2 wraps a ColSpecArray — recurse with fn1's result type
            if (cs.function2() != null && fn1Result != null
                    && fn1Result.type() instanceof Type.ClassType nestedType) {
                ColSpecArray nested = extractNestedColSpecs(cs);
                if (nested != null) {
                    compileColSpecs(nested, nestedType, ctx);
                }
            }
        }
    }

    /** Extracts the nested ColSpecArray from fn2 (0-param lambda wrapping ClassInstance). */
    private static ColSpecArray extractNestedColSpecs(ColSpec cs) {
        if (cs.function2() == null || cs.function2().body().isEmpty()) return null;
        var body = cs.function2().body().get(0);
        if (body instanceof ClassInstance ci && ci.value() instanceof ColSpecArray csa) return csa;
        return null;
    }

    /** Extracts ColSpecArray from a ClassInstance argument. Same pattern as every other checker. */
    static ColSpecArray extractColSpecs(ValueSpecification specArg) {
        if (specArg instanceof ClassInstance ci) {
            if (ci.value() instanceof ColSpecArray csa) return csa;
            if (ci.value() instanceof ColSpec cs) return new ColSpecArray(List.of(cs));
        }
        throw new PureCompileException(
                "graphFetch() requires a ColSpec or ColSpecArray argument (or #{...}# tree)");
    }
}
