package com.gs.legend.compiler.typed;

import com.gs.legend.compiled.CompiledFunction;
import com.gs.legend.compiler.ExpressionType;
import java.util.List;

/**
 * Call to a user-defined function with the callee's resolved compiled body.
 *
 * <p>{@code callee} is <strong>name-resolution output</strong>, intrinsic to
 * the typed HIR: computed during overload resolution in {@code compileUserCall}
 * and attached directly on the node. This matches Rust's HIR, where
 * {@code hir::Path} carries {@code res: Res} as a field — resolution is
 * part of HIR construction, not a later analysis pass.
 *
 * <p>Because Pure supports overloading, FQN alone cannot identify a specific
 * overload — so the resolved {@link CompiledFunction} must live on the node,
 * not in an FQN-keyed sidecar table. Identity-memoized inside
 * {@code TypeChecker}, so all call sites of the same overload share one
 * {@link CompiledFunction} instance.
 *
 * @param functionFqn Qualified name of the user function being called.
 * @param args        Typed actual arguments, in source order.
 * @param callee      Resolved compiled body for this call site.
 * @param info        Type + multiplicity of the call's return value.
 */
public record TypedUserCall(
        String functionFqn,
        List<TypedSpec> args,
        CompiledFunction callee,
        ExpressionType info
) implements TypedSpec {}
