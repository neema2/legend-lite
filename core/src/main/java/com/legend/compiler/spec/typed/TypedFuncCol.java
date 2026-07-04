package com.legend.compiler.spec.typed;

import java.util.Objects;

/**
 * One mapped column of a {@code ~[alias:x|…]} specification: its output name and
 * its type-checked per-row lambda. A component of {@link TypedFuncColSpec} /
 * {@link TypedFuncColSpecArray} and of the {@link TypedProject}/{@link TypedExtend}
 * constructs that consume them &mdash; not a {@link TypedSpec} itself.
 *
 * @param name the output column name (the colspec alias)
 * @param fn   the type-checked mapping lambda; its body type is the column's type
 */
public record TypedFuncCol(String name, TypedLambda fn) {
    public TypedFuncCol {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(fn, "fn");
    }
}
