// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.TypedQuery;
import com.legend.builtin.NativeFn;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.OrderView;
import com.legend.compiler.spec.typed.Calls;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * H10 SPIKE (claude/compat-judges-spike, never for merge): THE DOOR REWRITE R-S131's TYPED HALF (H6 section 7.5) --
 * {@code assertEquals(e, a)} (each message form) whose actual's order is incidental ({@link OrderView#INCIDENTAL}, a
 * planning fact) becomes {@code assertSameElements(e, a)} with the same arguments: a multiset by its own meaning, so
 * the product's host judge needs no S131 leniency. Built from public entries alone: {@link TypedQuery#body},
 * {@link TypedQuery#context}, {@link ModelContext#findFunction}, {@link OrderView#of}, the {@link TypedNativeCall}
 * record and {@link TypedQuery#withBody}. Not rewritten: two relations (the grid pair's own judgment), an actual of
 * at most one value (a multiset of one is equality; a rendered text's {@code String[1]} is the edge's own kind), and
 * an assert inside a lambda (its order view would read a lambda's parameter).
 */
public final class IncidentalOrderAsserts {

    private IncidentalOrderAsserts() {
    }

    private static final String SAME_ELEMENTS = "meta::pure::functions::asserts::assertSameElements";

    /** How many asserts the last rewrite changed, for the spike's census (one thread per corpus pass). */
    public static int rewritten;

    /** {@code q} with each incidental-order assertEquals rewritten; {@code q} itself where none is. */
    public static TypedQuery rewrite(TypedQuery q) {
        List<TypedSpec> body = q.body();
        ModelContext ctx = q.context();
        List<TypedSpec> out = new ArrayList<>(body.size());
        boolean changed = false;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec s = body.get(i);
            TypedSpec r = rewrite(s, body.subList(0, i), ctx);
            changed |= r != s;
            out.add(r);
        }
        return changed ? q.withBody(out) : q;
    }

    private static TypedSpec rewrite(TypedSpec n, List<TypedSpec> letPrefix, ModelContext ctx) {
        if (n instanceof TypedLambda) {
            return n;
        }
        List<TypedSpec> kids = n.children();
        List<TypedSpec> rebuilt = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = rewrite(k, letPrefix, ctx);
            changed |= r != k;
            rebuilt.add(r);
        }
        TypedSpec node = changed ? n.withChildren(rebuilt) : n;
        if (!(node instanceof TypedNativeCall call)
                || NativeFn.Verdict.of(Calls.calleeIdOf(node)).orElse(null) != NativeFn.Verdict.ASSERT_EQUALS
                || call.args().size() < 2) {
            return node;
        }
        TypedSpec e = call.args().get(0);
        TypedSpec a = call.args().get(1);
        if (Type.isRelation(e.info().type()) && Type.isRelation(a.info().type())) {
            return node;
        }
        if (!a.info().multiplicity().isMany()) {
            return node;
        }
        if (OrderView.of(a, letPrefix) != OrderView.INCIDENTAL) {
            return node;
        }
        TypedFunction same = overload(call.callee(), ctx);
        rewritten++;
        return new TypedNativeCall(same, call.args(), call.info(), call.pos());
    }

    /** assertSameElements' overload of the same parameters as {@code equals}' (Any[*], Any[*], then the message's). */
    private static TypedFunction overload(TypedFunction equals, ModelContext ctx) {
        for (TypedFunction f : ctx.findFunction(SAME_ELEMENTS)) {
            if (f.parameters().size() != equals.parameters().size()) {
                continue;
            }
            boolean same = true;
            for (int i = 0; i < f.parameters().size(); i++) {
                same &= f.parameters().get(i).type().equals(equals.parameters().get(i).type())
                        && f.parameters().get(i).multiplicity().equals(equals.parameters().get(i).multiplicity());
            }
            if (same) {
                return f;
            }
        }
        throw new IllegalStateException("no assertSameElements overload of assertEquals' parameters: " + equals);
    }
}
