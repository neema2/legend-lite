// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE ONE LET-BINDING LOOKUP (cleanup move 4, 2026-09-22). A body's let prefix is a
 * list of statements; the let IN SCOPE for a name is the last one that binds it — a
 * call frame's parameter let shadows the caller's same-named let. Every reader of a
 * prefix asks here; none walks the list itself.
 */
public final class Lets {

    private Lets() {
    }

    /** The let in scope for {@code name}, or null when the prefix does not bind it. */
    public static @com.legend.base.Nullable TypedLet binding(List<TypedSpec> prefix, String name) {
        for (int i = prefix.size() - 1; i >= 0; i--) {
            if (prefix.get(i) instanceof TypedLet let && let.name().equals(name)) {
                return let;
            }
        }
        return null;
    }

    /** Whether the prefix binds {@code name}. */
    public static boolean binds(List<TypedSpec> prefix, String name) {
        return binding(prefix, name) != null;
    }

    /** {@code e} chased through the lets it is bound to: a variable becomes the value
     * of its let in scope, and a value that is itself a variable chases on DOWN the
     * prefix — below the binding met, never through it (lexical scope: a let's value
     * can only name what came before it). A non-variable, or an unbound variable, is
     * returned as is. */
    public static TypedSpec bound(TypedSpec e, List<TypedSpec> prefix) {
        TypedSpec cur = e;
        int from = prefix.size() - 1;
        while (cur instanceof TypedVariable v) {
            TypedSpec next = null;
            for (int i = from; i >= 0; i--) {
                if (prefix.get(i) instanceof TypedLet let && let.name().equals(v.name())) {
                    next = let.value();
                    from = i - 1;
                    break;
                }
            }
            if (next == null) {
                break;
            }
            cur = next;
        }
        return cur;
    }

    /** A statement's value: a let's value, else the statement itself (a trailing let
     * IS its value, real pure). */
    public static TypedSpec bare(TypedSpec stmt) {
        return stmt instanceof TypedLet let ? let.value() : stmt;
    }

    /** Every binding of the prefix by name, the let in scope winning. */
    public static Map<String, TypedSpec> byName(List<TypedSpec> prefix) {
        Map<String, TypedSpec> out = new LinkedHashMap<>();
        for (TypedSpec s : prefix) {
            if (s instanceof TypedLet let) {
                out.put(let.name(), let.value());
            }
        }
        return out;
    }
}
