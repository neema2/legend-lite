// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.TypedQuery;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.spec.CrossTypeRule;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCrossTypeComparison;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * LEGEND-PURE'S COMPARISONS ACROSS TYPES (B3 step 5, the one equality; the user's ruling of 2026-11-24, "Love it":
 * strict typing is lite's alone). In legend-pure two values of different types are never equal ({@code 1 == 1.0} is
 * false, as an enumeration is never its name), so at legend-pure's door an equality across types is {@code false}, a
 * membership across types finds nothing ({@code in} and {@code contains} false, {@code indexOf} -1), and an ordering
 * across types is the database's comparison (legend-pure orders numbers by value, as the database does; a day against
 * a date-time at one instant is the recorded exception: legend-pure orders the day first, the database ties them). The
 * types decide it: nothing is computed from a value. A side answered by its types alone is not evaluated (legend-pure
 * evaluates it, so a side that raises raises there).
 *
 * <p>This is legend-pure's door's comparison setting ({@link CrossTypeRule}; the edition program's step 2b,
 * 2026-11-26): the type-checker asks it where it types a comparison across types. In the query the answer is
 * legend-pure's, above (until step 2b a rewrite of the typed query's markers, after it was typed). In a body the
 * query reaches it is still the call, the database's comparison, as before the setting: legend-pure's answer there
 * is a later step of the program, each test it moves read first.
 */
public final class PureComparisons implements CrossTypeRule {

    /** legend-pure's setting. */
    static final PureComparisons RULE = new PureComparisons();

    private PureComparisons() {
    }

    /** {@code query} typed with each comparison across types written in it answered as legend-pure answers it. The door
     *  asks where it types the query, before anything reads its body ({@code TypedQuery.comparing}). One the door
     *  cannot answer yet (an equality both of whose sides may be empty) is left as its marker while the query is
     *  typed and refused by name here, once it is: the type-checker tries a call's overloads in turn, and a
     *  comparison met inside a candidate it then drops was never the query's (the independent audit of 2026-11-26,
     *  finding 1: a refusal thrown while typing would have ended the search). */
    public static TypedQuery asPures(TypedQuery query) {
        TypedQuery typed = query.comparing(RULE);
        java.util.List<TypedCrossTypeComparison> left = com.legend.compiler.spec.CrossTypeComparisons.in(typed.body());
        if (!left.isEmpty()) {
            throw refusal(left.get(0));
        }
        return typed;
    }

    @Override
    public TypedSpec inQuery(TypedCrossTypeComparison crossing) {
        return answer(crossing);
    }

    @Override
    public boolean readsBodies() {
        return false;
    }

    @Override
    public TypedSpec inBody(TypedCrossTypeComparison crossing) {
        return crossing.comparison();
    }

    private static TypedSpec answer(TypedCrossTypeComparison c) {
        return switch (c.kind()) {
            // equal([], []) is true in legend-pure: only a side that is never empty makes the answer the types' alone
            case EQUALITY -> neverEmpty(c) ? falseOf(c) : c;
            case MEMBERSHIP -> falseOf(c);
            case INDEX -> new TypedCInteger(-1L, c.info());
            // the database's comparison, as before the one equality (B3 step 5a's review, M3, measured 2026-11-25:
            // refusing the orderings of two non-numbers failed four PCT tests the database's comparison passes --
            // testCompareMixedTypes, testDateCompare, testMax_Date, testMin_Date); recorded where it is not Pure's: a
            // day against a date-time at one instant, which legend-pure orders the day first and the database ties
            case ORDERING -> c.comparison();
        };
    }

    private static boolean neverEmpty(TypedCrossTypeComparison c) {
        for (TypedSpec side : c.comparison().children()) {
            if (side.info().multiplicity() instanceof Multiplicity.Bounded b && b.lower() >= 1) {
                return true;
            }
        }
        return false;
    }

    private static TypedSpec falseOf(TypedCrossTypeComparison c) {
        return new TypedCBoolean(false, c.info());
    }

    private static com.legend.error.NotImplementedException refusal(TypedCrossTypeComparison c) {
        return new com.legend.error.NotImplementedException("legend-pure's door: " + c.operator() + " of "
                + c.left().typeName() + " and " + c.right().typeName() + " where both sides may be empty is not"
                + " rewritten yet, so this body is refused (legend-pure's answer is true when both are empty)");
    }
}
