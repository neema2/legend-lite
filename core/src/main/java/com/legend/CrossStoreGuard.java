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
        // a statement executes only after Compiler.executesOn decided its session, which refuses
        // no runtime and an undefined one first (C3b: these were silent passes; measured unreached
        // 2026-10-03 across both corpora, the core tests, PCT and Channel B)
        if (runtimeFqn == null) {
            throw new IllegalStateException("a statement executes with no runtime: Compiler.executesOn"
                    + " refuses that before execution");
        }
        var rt = ctx.findRuntime(runtimeFqn).orElseThrow(() -> new IllegalStateException(
                "runtime '" + runtimeFqn + "' is not defined: Compiler.executesOn refuses that before execution"));
        var bindings = withIncludes(rt.connectionBindings(), ctx);
        // store -> bound connection, touched tables only. A touched store the runtime does not bind is
        // REFUSED by name (it rode the session connection before C3b — legend-engine's
        // connectionByElement at(0); SEMANTICS_REGISTER S27); the platform's own metamodel store is the
        // one exemption: no runtime binds it, the executor routes it to the system database
        var conns = new TreeMap<String, String>();
        for (TypedSpec n : body) {
            collect(n, bindings, conns);
        }
        var unbound = new TreeSet<String>();
        for (TypedSpec n : body) {
            collectUnbound(n, bindings, unbound);
        }
        unbound.remove(com.legend.builtin.SystemMetamodel.STORE_FQN);
        if (!unbound.isEmpty()) {
            throw new com.legend.error.NotImplementedException("the query reads " + unbound + ", which runtime '"
                    + runtimeFqn + "' does not bind to a connection");
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

    /** The runtime's bindings, each also covering every store its database INCLUDES, at any depth: an included
     *  database's tables are part of the including one, reached through its connection (a store bound directly
     *  keeps its own binding too, so a conflicting one is still the multi-connection shape refused above). */
    private static java.util.Map<String, java.util.List<String>> withIncludes(
            java.util.Map<String, java.util.List<String>> bindings, ModelContext ctx) {
        var out = new TreeMap<String, java.util.List<String>>(bindings);
        for (var e : bindings.entrySet()) {
            var work = new java.util.ArrayDeque<String>(java.util.List.of(e.getKey()));
            var seen = new TreeSet<String>();
            while (!work.isEmpty()) {
                String store = work.poll();
                if (!seen.add(store)) {
                    continue;
                }
                ctx.findDatabase(store).ifPresent(db -> db.includes().forEach(inc -> {
                    out.putIfAbsent(inc, e.getValue());
                    work.add(inc);
                }));
            }
        }
        return out;
    }

    private static void collectUnbound(TypedSpec n,
            java.util.Map<String, java.util.List<String>> bindings, TreeSet<String> unbound) {
        if (n instanceof TypedTableReference t && !bindings.containsKey(t.store())) {
            unbound.add(t.store());
        }
        for (TypedSpec c : n.children()) {
            collectUnbound(c, bindings, unbound);
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
