// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;

import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The CROSS-STORE execution wall (C2.2): a resolved query names every
 * physical table's store, and the runtime maps stores to connections —
 * but execution runs on ONE session connection. When the runtime binds
 * the touched stores to DIFFERENT connections, executing them all on
 * the session connection would silently return rows from the wrong
 * database (the corpus only ever surfaced this as a plan NPE). Multiple
 * Database elements sharing one connection — the ordinary corpus shape
 * — pass untouched; only a genuine multi-connection demand walls.
 */
final class CrossStoreGuard {

    private CrossStoreGuard() {
    }

    static void check(List<TypedSpec> body, ModelContext ctx,
            @com.legend.base.Nullable String runtimeFqn) {
        if (runtimeFqn == null) {
            return;
        }
        var rt = ctx.findRuntime(runtimeFqn);
        if (rt.isEmpty()) {
            return;
        }
        var bindings = rt.get().connectionBindings();
        // store -> bound connection, touched tables only; a store the
        // runtime does not bind rides the session connection (today's
        // single-connection model) and constrains nothing
        var conns = new TreeMap<String, String>();
        for (TypedSpec n : body) {
            collect(n, bindings, conns);
        }
        var distinct = new TreeSet<>(conns.values());
        if (distinct.size() > 1) {
            throw new com.legend.error.NotImplementedException(
                    "query touches stores bound to DIFFERENT connections "
                    + conns + " under runtime '" + runtimeFqn
                    + "' — multi-connection execution is not modeled"
                    + " (one session connection per query)");
        }
    }

    private static void collect(TypedSpec n,
            java.util.Map<String, java.util.List<String>> bindings,
            TreeMap<String, String> conns) {
        if (n instanceof TypedTableReference t) {
            java.util.List<String> bound = bindings.get(t.store());
            if (bound != null) {
                // a store bound to SEVERAL connections is itself the
                // multi-connection shape the distinct-check below refuses,
                // so record each under a keyed name
                for (int i = 0; i < bound.size(); i++) {
                    conns.put(i == 0 ? t.store() : t.store() + "#" + i,
                            bound.get(i));
                }
            }
        }
        for (TypedSpec c : n.children()) {
            collect(c, bindings, conns);
        }
    }
}
