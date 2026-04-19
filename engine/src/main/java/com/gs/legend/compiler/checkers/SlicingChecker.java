package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.model.m3.Type;

import java.util.List;
import java.util.Map;

/**
 * Signature-driven type checker for slicing functions:
 * {@code limit}, {@code take}, {@code drop}, {@code slice}, {@code first}, {@code last}.
 *
 * <p>All slicing functions preserve the source type — they only change cardinality.
 * Works on both relations and collections.
 *
 * <p>Canonical signatures:
 * <ul>
 *   <li>Relation: {@code limit<T>(rel:Relation<T>[1], size:Integer[1]):Relation<T>[1]}</li>
 *   <li>Relation: {@code drop<T>(rel:Relation<T>[1], size:Integer[1]):Relation<T>[1]}</li>
 *   <li>Relation: {@code slice<T>(rel:Relation<T>[1], start:Integer[1], stop:Integer[1]):Relation<T>[1]}</li>
 *   <li>Collection: {@code take<T>(set:T[*], count:Integer[1]):T[*]}</li>
 *   <li>Collection: {@code drop<T>(set:T[*], count:Integer[1]):T[*]}</li>
 *   <li>Collection: {@code first<T>(set:T[*]):T[0..1]}</li>
 * </ul>
 */
public class SlicingChecker extends AbstractChecker {

    public SlicingChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        String funcName = simpleName(af.function());

        // Pre-compile non-source arguments so resolveOverload has compiled types
        // for Concrete params (e.g., Integer[1] in limit/take/drop/slice)
        Map<Integer, ExpressionType> compiledTypes = new java.util.HashMap<>();
        for (int i = 1; i < params.size(); i++) {
            TypeInfo argInfo = env.compileExpr(params.get(i), ctx);
            if (argInfo != null && argInfo.type() != null) {
                compiledTypes.put(i, argInfo.expressionType());
            }
        }

        NativeFunctionDef def = resolveOverload(funcName, params, source, compiledTypes);

        // 2. Bind type variables from signature (T from source)
        var bindings = unify(def, source.expressionType());

        // 3. Validate pre-compiled argument types against signature param types
        for (int i = 1; i < params.size() && i < def.params().size(); i++) {
            Type expectedType = resolve(def.params().get(i).type(), bindings,
                    funcName + "() argument " + i);
            ExpressionType actualExpr = compiledTypes.get(i);
            Type actualType = actualExpr != null ? actualExpr.type() : null;
            if (actualType != null && !isAssignable(actualType, expectedType)) {
                throw new PureCompileException(
                        funcName + "() argument " + i + ": expected "
                                + expectedType + ", got " + actualType);
            }
        }

        // 4. Output type from signature (preserves source type, may change multiplicity)
        ExpressionType outputType = resolveOutput(def, bindings, funcName + "()");
        return TypeInfo.builder()
                .expressionType(outputType)
                .build();
    }

    /** Checks if actualType is assignable to expectedType (same type or ANY). */
    private boolean isAssignable(Type actual, Type expected) {
        if (expected == null || actual == null) return true;
        if (expected instanceof Type.Primitive p && "Any".equals(p.name())) return true;
        // Integer assignable to Number
        if (expected instanceof Type.Primitive ep && actual instanceof Type.Primitive ap) {
            return ep.name().equals(ap.name())
                    || "Number".equals(ep.name()) && ("Integer".equals(ap.name()) || "Float".equals(ap.name()));
        }
        return expected.equals(actual);
    }
}
