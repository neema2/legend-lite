// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.ModelContext;

/**
 * The platform callees the resolver SYNTHESIZES calls to (join
 * conditions, membership witnesses, member-thread reads, raises) —
 * looked up by EXACT FQN (audit 23 A3: a user function sharing a simple
 * name never becomes a synthesized callee). Extracted from StoreResolver
 * (file-size guardrail).
 */
final class Callees {

    private final ModelContext ctx;

    Callees(ModelContext ctx) {
        this.ctx = ctx;
    }

    /** The one resolved 2-arg {@code boolean::and} — the ON-form pass's
     * conjunction builder. */
    TypedFunction and() {
        var fns = ctx.findFunction("meta::pure::functions::boolean::and")
                .stream().filter(f -> f.parameters().size() == 2).toList();
        if (fns.size() != 1) {
            throw new IllegalStateException(
                    "resolver bug: expected one 2-arg boolean::and");
        }
        return fns.get(0);
    }

    TypedFunction bool(String n2) {
        return ctx.findFunction("meta::pure::functions::boolean::" + n2).get(0);
    }

    /** {@code coalesce(T[0..1], T[0..1]):T[0..1]} — the member-thread read. */
    TypedFunction coalesce() {
        var fns = ctx.findFunction("meta::pure::functions::flow::coalesce")
                .stream().filter(f -> f.parameters().size() == 2
                        && f.parameters().stream().allMatch(p ->
                                p.multiplicity().equals(
                                        com.legend.compiler.element.type.Multiplicity
                                                .Bounded.ZERO_ONE))).toList();
        if (fns.size() != 1) {
            throw new IllegalStateException(
                    "resolver bug: expected one flow::coalesce(T[0..1], T[0..1])");
        }
        return fns.get(0);
    }

    TypedFunction fail() {
        return ctx.findFunction("meta::pure::functions::asserts::fail")
                .stream().filter(f -> f.parameters().size() == 1)
                .findFirst().orElseThrow(() -> new IllegalStateException(
                        "resolver bug: no fail(String) registration"));
    }

    /** The 2-arg in overload — the objectReferenceIn pk membership. */
    TypedFunction in() {
        return ctx.findFunction("meta::pure::functions::collection::in")
                .stream().filter(f -> f.parameters().size() == 2)
                .findFirst().orElseThrow(() -> new IllegalStateException(
                        "resolver bug: no in registration"));
    }

    /** Any registered equal overload — membership-crossing emission. */
    @com.legend.base.Nullable TypedFunction equal() {
        // exact FQN (audit 23 A3): a user-defined 'equal' must never
        // become the membership callee
        var fns = ctx.findFunction("meta::pure::functions::boolean::equal");
        return fns.isEmpty() ? null : fns.get(0);
    }

    /** Any registered isNotEmpty overload — the lowerer dispatches by
     * family. Never null: a missing registration is a resolver bug and
     * throws. */
    TypedFunction isNotEmpty() {
        var fns = ctx.findFunction(
                "meta::pure::functions::collection::isNotEmpty");
        if (fns.isEmpty()) {
            throw new IllegalStateException("resolver bug: no isNotEmpty registration");
        }
        return fns.get(0);
    }
}
