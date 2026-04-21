package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedFrom;
import com.gs.legend.compiler.typed.TypedPackageableRef;
import com.gs.legend.compiler.typed.TypedSpec;

import java.util.ArrayList;
import java.util.List;


/**
 * Signature-driven type checker for {@code from()}.
 *
 * <p>{@code from()} is a runtime binding — it tells the engine which
 * Runtime to use for execution. Type-wise it is a passthrough: the
 * output type equals the source type.
 *
 * <p>Overloads:
 * <ul>
 *   <li>{@code from<T>(source:Relation<T>[1], runtime:Any[1]):Relation<T>[1]}</li>
 *   <li>{@code from<T>(source:Relation<T>[1]):Relation<T>[1]}</li>
 *   <li>{@code from<T>(source:T[*], mapping:Any[1], runtime:Any[1]):T[*]} — M2M</li>
 * </ul>
 *
 * <p>Fully signature-driven: T is bound from source via {@link #unify},
 * output type resolved via {@link #resolveOutput}. Mapping is propagated
 * from the source TypeInfo.
 */
public class FromChecker extends AbstractChecker {

    public FromChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypedFrom check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("from", params, source);

        // Compile every non-source arg so downstream has typed nodes, and collect
        // their ExpressionTypes (plus source's) for unification. Every non-source
        // arg of from() is a mapping or runtime FQN reference, produced by
        // TypeChecker.resolvePackageableElement \u2014 statically a TypedPackageableRef.
        List<ExpressionType> actuals = new ArrayList<>(params.size());
        actuals.add(source.expressionType());
        List<TypedPackageableRef> compiled = new ArrayList<>(params.size() - 1);
        for (int i = 1; i < params.size(); i++) {
            TypedSpec arg = env.compileExpr(params.get(i), ctx);
            actuals.add(arg.expressionType());
            if (!(arg instanceof TypedPackageableRef ref)) {
                throw new PureCompileException(
                        "from() argument " + i + " must be a packageable element reference"
                                + " (mapping or runtime FQN), got "
                                + arg.getClass().getSimpleName());
            }
            compiled.add(ref);
        }

        var bindings = unify(def, actuals);
        ExpressionType outputType = resolveOutput(def, bindings, "from()");

        // Three overloads mapped to (mapping, runtime):
        //   from(source)                     → (null, null)
        //   from(source, runtime)            → (null, runtime)
        //   from(source, mapping, runtime)   → (mapping, runtime)  — M2M
        TypedPackageableRef mapping = compiled.size() >= 2 ? compiled.get(0) : null;
        TypedPackageableRef runtime = switch (compiled.size()) {
            case 0 -> null;
            case 1 -> compiled.get(0);
            default -> compiled.get(1);
        };
        return new TypedFrom(source, mapping, runtime, outputType);
    }
}
