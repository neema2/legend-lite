package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.LambdaFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedMap;
import com.gs.legend.compiler.typed.TypedSpec;

import java.util.List;
import java.util.Map;

/**
 * Signature-driven type checker for {@code map()}.
 *
 * <p>Two overloads:
 * <ul>
 *   <li>Collection: {@code map<T,V>(value:T[*], func:Function<{T[1]->V[*]}>):V[*]}</li>
 *   <li>Optional:   {@code map<T,V>(value:T[0..1], func:Function<{T[1]->V[0..1]}>):V[0..1]}</li>
 * </ul>
 *
 * <p>Fully signature-driven: T is bound from source via {@link #unify}, lambda
 * param type resolved via {@link #resolve}, lambda body compiled to determine V,
 * and output type constructed via {@link #resolveOutput}.
 *
 * <p>Unlike {@code filter()} where the output type equals the source type,
 * {@code map()} transforms element type T to V. The lambda body's return type
 * determines V, which is then used to resolve the output multiplicity from the
 * matched signature overload.
 */
public class MapChecker extends AbstractChecker {

    public MapChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypedMap check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("map() requires 2 parameters: source, func");
        }

        // Resolve overload (T[*] vs T[0..1]) using compiled source type.
        // Multiplicity scoring picks the right overload: [0..1] source prefers
        // T[0..1], [*] source prefers T[*].
        NativeFunctionDef def = resolveOverload("map", params, source,
                Map.of(0, source.expressionType()));
        var bindings = unify(def, source.expressionType());

        // The mapper argument is mandated by the grammar to be a lambda — use
        // {@link #compileLambdaArg} which does extract-ft → bind-params → compile
        // → rebind V from body return in one shot.
        if (!(params.get(1) instanceof LambdaFunction lambdaAst)) {
            throw new PureCompileException("map() argument 2 must be a lambda");
        }
        TypedLambda mapper = compileLambdaArg(
                lambdaAst, def.params().get(1), bindings, source, ctx, "map");

        // Output type is V[*] or V[0..1] — V was bound by compileLambdaArg from
        // the body's return type.
        ExpressionType outputType = resolveOutput(def, bindings, "map()");
        return new TypedMap(source, mapper, def, outputType);
    }
}
