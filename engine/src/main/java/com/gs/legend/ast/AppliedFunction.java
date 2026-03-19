package com.gs.legend.ast;

import java.util.List;

/**
 * Generic function application — THE key AST type.
 *
 * <p>
 * In Pure, {@code $x->foo(y)} is sugar for {@code foo($x, y)}.
 * This single node type represents ALL function calls uniformly:
 * <ul>
 * <li>{@code Person.all()} → AppliedFunction("getAll",
 * [PackageableElementPtr("Person")], false)</li>
 * <li>{@code ->filter({p|$p.age > 21})} → AppliedFunction("filter", [source,
 * lambda], true)</li>
 * <li>{@code ->project([lambdas], ['aliases'])} → AppliedFunction("project",
 * [source, Collection, Collection], true)</li>
 * <li>{@code abs(x)} → AppliedFunction("abs", [x], false)</li>
 * </ul>
 *
 * <p>
 * Following Legend Engine's convention, arrow calls prepend the receiver
 * as parameters[0], while standalone calls only contain the explicit arguments.
 * The {@code hasReceiver} flag makes this implicit convention explicit so the
 * adapter can correctly reconstruct the old IR's source/args split.
 *
 * @param function    The function name — preserved as fully qualified if
 *                    present
 *                    (e.g.,
 *                    "meta::pure::functions::lang::tests::letFn::letWithParam")
 * @param parameters  All parameters — for arrow calls, receiver is first
 * @param hasReceiver true if this is an arrow call (parameters[0] is receiver),
 *                    false for standalone calls
 * @param sourceText  For arrow calls, the receiver expression as raw Pure
 *                    source text
 *                    (used for UDF inlining via textual substitution). Null if
 *                    unavailable.
 * @param argTexts    The argument expressions as raw Pure source text strings
 *                    (used for UDF inlining). Empty list if unavailable.
 */
public record AppliedFunction(
        String function,
        List<ValueSpecification> parameters,
        boolean hasReceiver,
        String sourceText,
        List<String> argTexts) implements ValueSpecification {

    public AppliedFunction {
        parameters = List.copyOf(parameters);
        argTexts = argTexts != null ? List.copyOf(argTexts) : List.of();
    }

    /**
     * Constructor without source text (backward compatible).
     */
    public AppliedFunction(String function, List<ValueSpecification> parameters, boolean hasReceiver) {
        this(function, parameters, hasReceiver, null, List.of());
    }

    /**
     * Convenience constructor for standalone function calls (no receiver).
     */
    public AppliedFunction(String function, List<ValueSpecification> parameters) {
        this(function, parameters, false, null, List.of());
    }

    /**
     * Convenience constructor for zero-parameter functions.
     */
    public AppliedFunction(String function) {
        this(function, List.of(), false, null, List.of());
    }
}
