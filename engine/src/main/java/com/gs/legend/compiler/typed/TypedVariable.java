package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/**
 * Variable reference: {@code $x}.
 *
 * <p>{@link Role} distinguishes lambda parameters, let-bindings, and function
 * parameters. {@code BindingId} may be added in Commit 5 if scope resolution
 * needs identity beyond name+role.
 */
public record TypedVariable(String name, Role role, ExpressionType info) implements TypedSpec {}
