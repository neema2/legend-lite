// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.spec.typed.FreeVars;
import com.legend.compiler.spec.typed.TypedFrameRef;
import com.legend.compiler.spec.typed.TypedSourceUrl;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedTds;
import com.legend.compiler.spec.typed.TypedVariable;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 * THE RELATIONS A STATEMENT READS MORE THAN ONCE (2026-11-04, the user: "build it stacked on 83"): a let's value is
 * spliced at every read of its name (the inliner's substitution, UserCallInliner -- a let lives in the database or
 * nowhere), so a relation bound once and read twice is ONE node of the statement's tree, reached twice. Written where
 * each read stands, its SQL is pasted at each, and a let read three times inside a function called three deep is
 * pasted 27 times (the timeseries library's calendar windows: the spec's pairs trade was 23.9 million characters, DuckDB
 * still preparing it after ten minutes). Pure's let binds one value; so does SQL's common table expression.
 *
 * <p>A node is shared when the walk reaches it twice or more by identity, it is a relation, and it reads nothing a
 * statement's head cannot see: no lambda's parameter, no let of an enclosing body (its free variables are the
 * statement's own names -- its query lets and plan parameters, which every part of the statement reads alike). A read
 * of a table, a source URL, a planned frame, a literal grid or a variable is never shared: its SQL is no larger than a
 * reference to it.
 */
final class SharedRelations {

    private SharedRelations() {
    }

    /** The shared relation nodes of {@code root}, by identity; {@code statementNames} the names a statement's head
     *  reads (its query lets, its plan parameters). */
    static Set<TypedSpec> of(TypedSpec root, Set<String> statementNames) {
        Map<TypedSpec, Integer> reads = new IdentityHashMap<>();
        Deque<TypedSpec> todo = new ArrayDeque<>();
        todo.push(root);
        while (!todo.isEmpty()) {
            TypedSpec n = todo.pop();
            Integer before = reads.put(n, reads.getOrDefault(n, 0) + 1);
            if (before == null) {
                for (TypedSpec c : n.children()) {
                    todo.push(c);
                }
            }
        }
        Map<TypedSpec, Set<String>> free = new IdentityHashMap<>();
        Set<TypedSpec> shared = Collections.newSetFromMap(new IdentityHashMap<>());
        reads.forEach((n, count) -> {
            if (count >= 2 && worth(n) && com.legend.compiler.element.type.Type.relationSchema(n.info().type()) != null
                    && statementNames.containsAll(FreeVars.ofShared(n, free))) {
                shared.add(n);
            }
        });
        return shared;
    }

    /** Whether {@code n}'s SQL is larger than a reference to it: a read of a store, a frame, a literal grid or a
     *  variable is not. */
    private static boolean worth(TypedSpec n) {
        return !(n instanceof TypedTableReference || n instanceof TypedSourceUrl || n instanceof TypedFrameRef
                || n instanceof TypedTds || n instanceof TypedVariable);
    }
}
