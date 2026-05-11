package com.legend.parser.spec;

import java.util.Objects;

/**
 * A single property binding inside a {@link NewInstance} expression.
 *
 * <p>Source form: {@code key = expression}, used in
 * {@code ^MyClass(name='Alice', age=30)}. Each {@code KeyExpression}
 * carries one such binding; the surrounding {@link NewInstance}
 * carries an ordered list of them in source order.
 *
 * <p>Not a {@link ValueSpecification} variant: a key/value binding is
 * a child component of a new-instance expression, not a standalone
 * expression. Modeling it as its own record (rather than a
 * {@code Map.Entry} or a tuple) gives it a name and a place to attach
 * source-location metadata later without reshaping the AST.
 *
 * @param key         property name as written in source
 * @param expression  the bound value expression
 */
public record KeyExpression(String key, ValueSpecification expression) {
    public KeyExpression {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(expression, "expression");
    }
}
