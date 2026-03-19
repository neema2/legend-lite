package com.gs.legend.ast;

/**
 * Generic type instance — used for type annotations like {@code @Integer} or
 * {@code @String}.
 *
 * <p>
 * In Pure, cast expressions use this: {@code $x->cast(@Integer)}
 *
 * @param fullPath The type path (e.g., "Integer", "String",
 *                 "my::package::MyClass")
 */
public record GenericTypeInstance(
        String fullPath) implements ValueSpecification {
}
