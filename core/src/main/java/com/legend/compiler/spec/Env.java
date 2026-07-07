package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The lexical environment for body type-checking: variable name &rarr; its
 * {@link ExprType}. Holds a function's parameters and (later) {@code let}
 * bindings and lambda parameters in scope. Immutable &mdash; {@link #with}
 * returns an extended copy, so nested scopes never mutate their parent.
 */
public final class Env {

    private final Map<String, ExprType> vars;

    private Env(Map<String, ExprType> vars) {
        this.vars = vars;
    }

    public static Env empty() {
        return new Env(Map.of());
    }

    /** Look up a variable's type, empty if it is not in scope. */
    public Optional<ExprType> lookup(String name) {
        return Optional.ofNullable(vars.get(name));
    }

    /** A new environment with {@code name} bound to {@code type} (shadowing any existing binding). */
    public Env with(String name, ExprType type) {
        Map<String, ExprType> next = new LinkedHashMap<>(vars);
        next.put(name, type);
        return new Env(next);
    }
}
