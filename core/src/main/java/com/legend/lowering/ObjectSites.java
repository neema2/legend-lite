// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

/**
 * The ids of one PROGRAM's objects (docs/OBJECT_IDENTITY_2026_10_20.md §3.2): each construction's site (its evaluation
 * outside its maps, {@link com.legend.compiler.spec.typed.Evaluation.Site}) mints one id, the leading part of every
 * object it builds' {@code __id}. A site is a value: the inliner gives a let's construction one site however many places
 * read it, and two runs over the same statements the same sites. A construction the compiler builds after the inliner
 * keys by its node.
 *
 * <p>One instance per program, shared by every lowering of it (the test-is-the-product plan's B9, R10, 2026-10-31): an
 * object's identity is its evaluation, and one program is the scope in which its objects meet, so the statements of a
 * program name their sites by one counter -- a let's construction read by two statements is one object in both. Until
 * then a product program minted one per statement's lowering, and the test road kept its own copy of this class (the
 * planner's {@code InstanceIds}), one per body. The ids' spelling, {@code o} or the test road's {@code i}, is the
 * minter's ({@link #ObjectSites(String)}): the test road's layouts converge on the product's at the plan's M5.
 */
public final class ObjectSites {

    private final String prefix;
    private final java.util.Map<Object, String> sites = new java.util.HashMap<>();
    private final java.util.IdentityHashMap<Object, String> nodes = new java.util.IdentityHashMap<>();

    /** A program's minter, its ids spelled {@code o1}, {@code o2}, ... */
    public ObjectSites() {
        this("o");
    }

    /** A program's minter, its ids spelled {@code prefix} then the site's number. */
    public ObjectSites(String prefix) {
        this.prefix = prefix;
    }

    /** The id of {@code key}, a site or a node, minted on first sight. The maps' pre-insert size numbers the ids
     *  (entries are never removed, so ids stay unique and stable; no mutable counter field). */
    public String idOf(Object key) {
        var ids = key instanceof com.legend.compiler.spec.typed.Evaluation.Site ? sites : nodes;
        String id = ids.get(key);
        if (id == null) {
            id = prefix + (sites.size() + nodes.size() + 1);
            ids.put(key, id);
        }
        return id;
    }
}
