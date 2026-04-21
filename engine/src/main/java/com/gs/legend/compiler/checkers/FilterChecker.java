package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedFilter;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedSpec;

import java.util.List;

/**
 * Signature-driven type checker for {@code filter()}.
 *
 * <p>Two overloads:
 * <ul>
 *   <li>Relational: {@code filter<T>(rel:Relation<T>[1], f:Function<{T[1]->Boolean[1]}>[1]):Relation<T>[1]}</li>
 *   <li>Collection: {@code filter<T>(value:T[*], func:Function<{T[1]->Boolean[1]}>[1]):T[*]}</li>
 * </ul>
 *
 * <p>Fully signature-driven: type variable T is bound via {@link #unify}, lambda param
 * type is resolved via {@link #resolve}, output constructed via {@link #resolveOutput}.
 */
public class FilterChecker extends AbstractChecker {

    public FilterChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypedSpec check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("filter", params, source);

        // Bind type variables from source against the chosen signature.
        var bindings = unify(def, source.expressionType());

        // Compile predicate lambda — helper narrows VS → LambdaFunction and throws
        // on mismatch, so no defensive instanceof check is needed here.
        TypedLambda predicate = compileLambdaArg(
                params.get(1), def.params().get(1), bindings, source, ctx, "filter");

        // Output type from signature's return type + bindings.
        ExpressionType outputType = resolveOutput(def, bindings, "filter()");
        return new TypedFilter(source, predicate, outputType);
    }
}
