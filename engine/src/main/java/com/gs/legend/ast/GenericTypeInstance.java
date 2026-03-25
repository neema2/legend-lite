package com.gs.legend.ast;

import com.gs.legend.plan.GenericType;

/**
 * Generic type instance — used for type annotations like {@code @Integer},
 * {@code @String}, or {@code @Relation<(col:Type, ...)>}.
 *
 * <p>
 * In Pure, cast expressions use this: {@code $x->cast(@Integer)}
 *
 * <p>
 * The builder always resolves the type from the ANTLR parse tree and stores
 * it in {@code resolvedType}. Consumers use {@code resolvedType()} directly —
 * no string re-parsing needed.
 *
 * @param fullPath     The type path (e.g., "Integer", "String",
 *                     "my::package::MyClass", "Relation")
 * @param resolvedType Pre-resolved GenericType from the parse tree. Always non-null.
 */
public record GenericTypeInstance(
        String fullPath,
        GenericType resolvedType) implements ValueSpecification {
}
