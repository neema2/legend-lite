// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

/**
 * F13 — SYNTHETIC INSTANCE IDENTITY as DATA: each keyless-class
 * construction SITE (a {@code TypedNewInstance}/{@code TypedCopyInstance}
 * evaluation, docs/OBJECT_IDENTITY_2026_10_20.md §3.2) mints ONE
 * deterministic id, carried in the instance struct's {@code __id} field
 * ({@code uuid()} per evaluation would make a let-bound instance unequal
 * to itself): the same let-bound constructor reaches both assert sides
 * as the same site, two textually distinct constructors are two.
 *
 * <p>Scoped to ONE {@link com.legend.StatementExecutor.ExecEnv} (both
 * sides of every verdict in a test share the env, so ids agree across
 * side lowerings by construction). Single-threaded like the env itself.
 */
public final class InstanceIds {

    private final java.util.Map<Object, String> sites = new java.util.HashMap<>();
    private final java.util.IdentityHashMap<Object, String> nodes =
            new java.util.IdentityHashMap<>();

    /** The id of {@code key}, minted on first sight: a construction's site
     * ({@link com.legend.compiler.spec.typed.Evaluation.Site}, a value: each
     * statement's inliner run gives a let's construction the same site), or
     * the node of one the compiler builds after the inliner. The maps'
     * pre-insert size numbers the ids (entries are never removed, so ids
     * stay unique and stable; no mutable counter field). */
    public String idOf(Object key) {
        var ids = key instanceof com.legend.compiler.spec.typed.Evaluation.Site ? sites : nodes;
        String id = ids.get(key);
        if (id == null) {
            id = "i" + (sites.size() + nodes.size() + 1);
            ids.put(key, id);
        }
        return id;
    }
}
