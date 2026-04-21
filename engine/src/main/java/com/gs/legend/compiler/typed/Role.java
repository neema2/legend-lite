package com.gs.legend.compiler.typed;

/**
 * Classification of a {@link TypedVariable}'s binding site.
 *
 * <p>Used to distinguish lambda parameters from let-bindings from function parameters
 * in scope resolution. {@code BindingId} (unique identity) may be added in Commit 5
 * if name+role lookup proves insufficient.
 */
public enum Role { LAMBDA_PARAM, LET_BINDING, FUNCTION_PARAM }
