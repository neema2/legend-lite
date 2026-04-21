package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import java.util.List;

/**
 * A call to a user-defined function. The call site is preserved (function FQN
 * and typed argument list) together with the already-inlined, fully type-checked
 * {@code body} of the matching overload. Downstream consumers may:
 * <ul>
 *   <li>Walk {@code body} directly (treat as inlined).</li>
 *   <li>Emit a call with {@code args} (no-inline strategy).</li>
 *   <li>Record the call-site dependency via {@code functionFqn}.</li>
 * </ul>
 *
 * @param functionFqn Qualified name of the user function being called.
 * @param args        Typed actual arguments, in source order.
 * @param body        Typed body of the selected overload, fully specialized.
 * @param info        Type + multiplicity of the call's return value.
 */
public record TypedUserCall(
        String functionFqn,
        List<TypedSpec> args,
        TypedSpec body,
        ExpressionType info
) implements TypedSpec {}
