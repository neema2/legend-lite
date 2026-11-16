// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;
import java.util.List;

/**
 * A LAMBDA'S LEADING STATEMENT WHOSE VALUE IS DISCARDED, kept (C5-J's batch, 3a, the user's ruling of 2026-11-16,
 * register S-batch-6): {@code statement; value} -- the statement typed, so its errors surface at typing, and the body's
 * value the last statement's ({@link #info} its). Pure evaluates the statement, so a raise inside it fails the body;
 * lite plans no effect it cannot carry in one expression, so the lowering refuses this node by name
 * ({@link #refusal}), where until then the statement was typed and dropped -- its raise lost, a false green. It is not a
 * sequence: it evaluates nothing. It refuses at PLANNING, not typing, as the platform refuses what it does not run
 * ("typing asks fit, not run"; a walled native is refused at lowering): a model holding one compiles, and every body of
 * the platform's prelude types at boot (the user's strict pin).
 *
 * @param statement the leading statement, typed
 * @param value     the body's value: the statements after it, typed as one
 * @param named     what the statement calls, by its full path (or its node's kind), for the refusal's words
 * @param info      the body's type: the value's
 */
public record TypedDiscarded(TypedSpec statement, TypedSpec value, String named, ExprType info)
        implements TypedSpec {

    /** The refusal the lowering raises for this node, in the ruled words. */
    public String refusal() {
        return "a statement whose value is discarded (" + named + ") inside a lambda: lite plans no effect it cannot"
                + " carry; bind it with let or make it the lambda's value";
    }

    @Override
    public List<TypedSpec> children() {
        return List.of(statement, value);
    }

    @Override
    public TypedSpec withChildren(List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 2, "TypedDiscarded");
        return new TypedDiscarded(kids.get(0), kids.get(1), named, info);
    }

    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedDiscarded(statement, value, named, info);
    }
}
