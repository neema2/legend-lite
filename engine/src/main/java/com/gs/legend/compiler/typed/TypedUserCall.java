package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import java.util.List;

/**
 * Black-box call to a user-defined function. Carries the callee's FQN and typed
 * argument list; the callee's body is <strong>not</strong> inlined here. The
 * compiled body lives once per {@code PureFunction} on the corresponding
 * {@link com.gs.legend.compiled.CompiledFunction} (keyed by identity), and
 * downstream consumers resolve it by FQN when (if) they need it.
 *
 * <p>This matches how real languages model function calls: a call site references
 * the callee by name + args, and inlining (if any) is a separate lowering pass.
 *
 * @param functionFqn Qualified name of the user function being called.
 * @param args        Typed actual arguments, in source order.
 * @param info        Type + multiplicity of the call's return value.
 */
public record TypedUserCall(
        String functionFqn,
        List<TypedSpec> args,
        ExpressionType info
) implements TypedSpec {}
