package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.CInteger;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedSlice;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Primitive;
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

    public TypedSlice check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        String funcName = simpleName(af.function());

        // Pre-compile non-source arguments so resolveOverload has compiled types
        // for Primitive params (e.g., Integer[1] in limit/take/drop/slice).
        Map<Integer, ExpressionType> compiledTypes = new java.util.HashMap<>();
        for (int i = 1; i < params.size(); i++) {
            TypedSpec argTyped = env.compileExpr(params.get(i), ctx);
            if (argTyped != null && argTyped.type() != null) {
                compiledTypes.put(i, argTyped.expressionType());
            }
        }

        NativeFunctionDef def = resolveOverload(funcName, params, source, compiledTypes);
        var bindings = unify(def, source.expressionType());

        // Validate compiled argument types against resolved signature types.
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

        ExpressionType outputType = resolveOutput(def, bindings, funcName + "()");
        long offset = extractOffset(funcName, params);
        long limit  = extractLimit(funcName, params);
        return new TypedSlice(source, offset, limit, outputType);
    }

    /** Offset in rows; 0 unless the op skips rows ({@code drop}/{@code slice}). */
    private static long extractOffset(String funcName, List<ValueSpecification> params) {
        return switch (funcName) {
            case "drop", "slice" -> intArg(params.get(1), funcName);
            default -> 0L;
        };
    }

    /** Limit in rows; {@code -1} = unbounded (used by {@code drop}). */
    private static long extractLimit(String funcName, List<ValueSpecification> params) {
        return switch (funcName) {
            case "limit", "take" -> intArg(params.get(1), funcName);
            case "slice"         -> intArg(params.get(2), funcName)
                                   - intArg(params.get(1), funcName);
            case "first", "head" -> 1L;
            case "drop"          -> -1L;
            default -> throw new PureCompileException(
                    "SlicingChecker: unhandled slicing op '" + funcName + "'");
        };
    }

    private static long intArg(ValueSpecification v, String funcName) {
        if (v instanceof CInteger ci) return ci.value().longValue();
        throw new PureCompileException(
                funcName + "(): expected an Integer literal argument, got "
                        + v.getClass().getSimpleName());
    }

    /**
     * {@code true} if {@code actual} may stand in for {@code expected}. Delegates to the
     * polymorphic {@link Type#isSubtypeOf} which already handles the primitive lattice
     * (Integer/Float ⊆ Number), PrecisionDecimal → DECIMAL, and {@link Primitive#ANY}
     * acceptance uniformly.
     *
     * <p>{@code null} on either side indicates a Compiler bug (missing TypeInfo) — throws
     * rather than silently accepting, per AGENTS.md no-fallback rule.
     */
    private boolean isAssignable(Type actual, Type expected) {
        if (expected == null || actual == null) {
            throw new PureCompileException(
                    "SlicingChecker.isAssignable: null type — Compiler must produce TypeInfo for every argument");
        }
        return actual.isSubtypeOf(expected);
    }
}
