package com.gs.legend.plan.lowering.natives;

import com.gs.legend.compiler.typed.TypedNativeCall;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.List;

/**
 * Lowering rule for one resolved scalar native overload. Receives the typed
 * call node (so bindings can inspect typed arg shape — needed for quirks
 * like {@code get} variant key dispatch and partial-date comparisons), the
 * already-lowered argument {@link SqlExpr}s, and the lowering context.
 *
 * <p>Bindings are registered in {@link NativeBindingTable} keyed by
 * {@link com.gs.legend.compiler.NativeFunctionDef} identity. The checker
 * stamps the resolved {@code NativeFunctionDef} onto every
 * {@link TypedNativeCall}; lowering looks up the binding and emits typed IR.
 */
@FunctionalInterface
public interface NativeBinding {
    SqlExpr emit(TypedNativeCall call, List<SqlExpr> args, LoweringContext ctx);
}
