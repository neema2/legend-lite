package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/**
 * Structural field extraction from a class instance: the lowered form of
 * {@code $instance.field} or {@code ^Class(...).field}.
 *
 * <p>Produced by TypeChecker when a property access resolves to a struct
 * source (let-bound instance, function result that returns a class, etc.)
 * that was previously handled via the legacy {@code inlinedBody} sidecar
 * pattern. In the typed HIR, inlining becomes structural: the access IS
 * a {@code TypedStructExtract} whose {@code source} is the typed instance
 * expression.
 */
public record TypedStructExtract(
        TypedSpec source,
        String field,
        ExpressionType info
) implements TypedSpec {}
