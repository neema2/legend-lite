package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import com.gs.legend.compiler.NativeFunctionDef;

/**
 * Runtime / mapping binding: {@code source->from(mapping, runtime)}.
 *
 * <p>Type-wise a passthrough — {@code info} equals the source's type — but
 * structurally significant: planners route execution to the named
 * {@code runtime} (and, for M2M, swap the {@code mapping}).
 *
 * @param source   Upstream expression.
 * @param mapping  Mapping reference (e.g. the M2M mapping FQN); {@code null}
 *                 for the plain two-arg {@code from(source, runtime)} form.
 * @param runtime  Runtime reference; may be {@code null} for the one-arg
 *                 {@code from(source)} form where the runtime is implicit.
 * @param info     Output {@link ExpressionType} — same as source's.
 */
public record TypedFrom(
        TypedSpec source,
        TypedPackageableRef mapping,
        TypedPackageableRef runtime,
        NativeFunctionDef def,
        ExpressionType info
) implements TypedNative {}
