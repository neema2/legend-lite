// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.builtin.NativeFn;
import com.legend.compiler.spec.typed.Calls;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * AN ORDER-DEPENDENT COMPARISON OF A RESULT WITH NO DEFINED ORDER (B3 step 4; the user, 2026-11-24: "my ideal state for
 * product is that we don't let platform devs or users write tests that do not have a sort ... But we still need
 * multi-set compare for backwards compatibility"). {@code assertEquals} and {@code assertNotEquals} compare in order; a
 * side whose order the program does not define ({@link OrderView#INCIDENTAL}: a store read or a frame read with no sort,
 * or past a grouping, a join, a union) arrives in whatever order the database chose, so the verdict would be the
 * database's luck. Lite's planner refuses it, naming the two fixes: a sort, or {@code assertSameElements} when the order
 * does not matter. Upstream's such asserts (its judge compared them as multisets) are rewritten to the explicit
 * order-free assert by the compatibility edge's test-body door before planning ({@code edge.UpstreamTestBodies}).
 */
public final class OrderedAsserts {

    private OrderedAsserts() {
    }

    /** Refuses an {@code assertEquals} or {@code assertNotEquals} of a side with no defined order anywhere in
     *  {@code body}'s statements, each read with the statements before it as its lets. */
    public static void refuse(List<? extends TypedSpec> body) {
        List<TypedSpec> lets = new java.util.ArrayList<>();
        for (TypedSpec statement : body) {
            walk(statement, lets);
            lets.add(statement);
        }
    }

    /** Whether {@code side}'s value depends on an order the program does not define: rows -- a relation or a
     *  collection, not one value -- whose order view is {@link OrderView#INCIDENTAL}. One value has no order (a
     *  lambda's own variable reads as "not in sight", which is not a collection). A text that prints or joins such rows
     *  (a relation's toString or toCSV, a joinStrings or makeString) is order-dependent too, and is not refused yet: the
     *  compatibility door has no explicit order-free form for every such text (a join of a list that is not Strings, of
     *  bare table cells, a print the lift does not read), and the corpus's lanes pass them by legend-engine's scan order
     *  (B3 step 4's measurement, 2026-11-24: 30 corpus tests a lane) -- the next sub-step, for the user's word. */
    public static boolean orderDependent(TypedSpec side, List<TypedSpec> lets) {
        return unorderedRows(side, lets);
    }

    /** Whether {@code side} is rows whose order the program does not define: a relation or a collection (not one
     *  value) whose order view is {@link OrderView#INCIDENTAL} -- the compatibility door's row rewrite's own test (a
     *  printed or joined text is the lift's and the join rewrite's). */
    public static boolean unorderedRows(TypedSpec side, List<TypedSpec> lets) {
        return rows(side) && OrderView.of(side, lets) == OrderView.INCIDENTAL;
    }

    private static boolean rows(TypedSpec side) {
        com.legend.compiler.element.type.ExprType info = side.info();
        boolean toOne = info.multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && Integer.valueOf(1).equals(b.upper());
        return !toOne || com.legend.compiler.element.type.Type.isRelation(info.type());
    }

    private static void walk(TypedSpec n, List<TypedSpec> lets) {
        NativeFn.Verdict fn = NativeFn.Verdict.of(Calls.calleeIdOf(n)).orElse(null);
        if (fn == NativeFn.Verdict.ASSERT_EQUALS || fn == NativeFn.Verdict.ASSERT_NOT_EQUALS) {
            List<TypedSpec> args = Calls.argsOf(n);
            if (args.size() >= 2) {
                for (int i = 0; i < 2; i++) {
                    if (orderDependent(args.get(i), lets)) {
                        throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.TYPE,
                                fn.bareName() + " compares in order, and its " + (i == 0 ? "expected" : "actual")
                                        + " side's order is not defined (a store or frame read with no sort, or past a"
                                        + " grouping, a join or a union): the database's arrival order would decide"
                                        + " the verdict -- sort it, or say assertSameElements when the order does not"
                                        + " matter");
                    }
                }
            }
        }
        for (TypedSpec child : n.children()) {
            walk(child, lets);
        }
    }
}
