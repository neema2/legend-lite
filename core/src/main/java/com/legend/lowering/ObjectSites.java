// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

/**
 * The ids of one query's objects (docs/OBJECT_IDENTITY_2026_10_20.md §3.2): each construction's site (its evaluation
 * outside its maps, {@link com.legend.compiler.spec.typed.Evaluation.Site}) mints one id, the leading part of every
 * object it builds' {@code __id}. A site is a value: the inliner gives a let's construction one site however many places
 * read it, and two runs over the same statements the same sites. A construction the compiler builds after the inliner
 * keys by its node. One instance per query, shared by every lowering of it.
 */
public final class ObjectSites {

    private final java.util.Map<Object, String> sites = new java.util.HashMap<>();
    private final java.util.IdentityHashMap<Object, String> nodes = new java.util.IdentityHashMap<>();

    /** The id of {@code key}, a site or a node, minted on first sight ({@code o1}, {@code o2}, ...). */
    public String idOf(Object key) {
        var ids = key instanceof com.legend.compiler.spec.typed.Evaluation.Site ? sites : nodes;
        String id = ids.get(key);
        if (id == null) {
            id = "o" + (sites.size() + nodes.size() + 1);
            ids.put(key, id);
        }
        return id;
    }
}
