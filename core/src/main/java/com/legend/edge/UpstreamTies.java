// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.base.Nullable;
import com.legend.builtin.Pure;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.lowering.NamedRule;
import com.legend.model.FunctionId;

import java.util.Map;

/**
 * WHERE EQUAL NUMBERS OF DIFFERENT TYPES LAND, AS LEGEND-PURE AND LEGEND-ENGINE ANSWER IT (the edition program's step
 * 6, slice 6.2; the user's rulings of 2026-11-26 ~23:10: lite's rule in lite's mode, upstream's in the other two): the
 * rows both compatibility doors' tables read ({@link PureTable}, {@link EngineTable}), kept in this one place.
 *
 * <p>Lite's own rule places equal numbers by their type (Integer, Float, Decimal). Upstream's is the order they were
 * written in, run on interpreted legend-pure, compiled legend-pure and legend-engine's own Java the same day, all three
 * agreeing on every case (docs/records/edition-homework/upstream-step6/answers.md, groups B and E):
 * <ul>
 *   <li>a sort is stable and compares {@code 3} and {@code 3.0} as equal, so they keep the written order:
 *       {@code [3.0, 3, 1]->sort()} is {@code [1, 3.0, 3]} ({@link NamedRule#SORT_TIES_AS_WRITTEN});</li>
 *   <li>{@code max}, {@code min}, {@code greatest} and {@code least} keep the element the function's own Pure body
 *       keeps ({@link NamedRule#EXTREME_KEEPS_THE_FOLDS_TIE}). A number list's {@code max} and {@code min} are a
 *       left fold: the last written of a tie ({@code [3, 3.0]->max()} is the Float), of several arguments the first
 *       ({@code max(3, 3.0)} is the Integer). {@code greatest}, {@code least} and the generic {@code max} and
 *       {@code min} are an end of that stable sort: the greatest the last written of a tie, the least the first
 *       ({@code [3, 3.0]->least()} is the Integer).</li>
 * </ul>
 * These rows are not "the database's own function": the answer is the same on every database, and the same for a call
 * inside a store query and outside one. So a table asks here before it reads a call's stamp or its database, and no
 * row here is ever refused.
 *
 * <p>Each rule writes something else only for a list that mixes number types; for every other call it writes the
 * call's own rule, unchanged (a list of one number type, a sort of texts, a sort by a key or a comparator).
 */
final class UpstreamTies {

    private UpstreamTies() {
    }

    /** Each function to the rule that lowers it at a compatibility door. The extremes' ids are the number and
     *  generic ones the rule accepts ({@code lowering.Scalars.extremeKeepingTheFoldsTie}); a lookup table, never iterated,
     *  unmodifiable in declaration order all the same. */
    private static final Map<FunctionId, NamedRule> RULES;

    static {
        Map<FunctionId, NamedRule> rules = new java.util.LinkedHashMap<>();
        for (FunctionId sort : Pure.AT_COLLECTION_SORT) {
            rules.put(sort, NamedRule.SORT_TIES_AS_WRITTEN);
        }
        // the date max and min are NOT rows: upstream has no tie among dates to keep (a day is smaller than its own
        // midnight date-time, answers.md group E), and lite refuses that one pair by name at every door, so a date's
        // extreme keeps its own rule here as at lite's doors
        for (FunctionId extreme : FunctionId.all(Pure.AT_MATH_MIN, Pure.AT_COLLECTION_MIN,
                Pure.AT_MATH_MAX, Pure.AT_COLLECTION_MAX, Pure.AT_COLLECTION_GREATEST,
                Pure.AT_COLLECTION_LEAST)) {
            rules.put(extreme, NamedRule.EXTREME_KEEPS_THE_FOLDS_TIE);
        }
        // numbers of different types inside a mixed list: three types never equal, at both doors, as both answered
        // before the product's rule became two families (the user's ruling of 2026-11-27: "legend-pure's and
        // legend-engine's modes keep their own behaviour"). Interpreted legend-pure's answer on every pair
        // (answers.md, group A); compiled legend-pure and legend-engine differ from it on some, as recorded (S127).
        // The optional-needle in is the one-value in under a null guard, so it carries no row of its own
        for (FunctionId compared : FunctionId.all(Pure.AT_BOOLEAN_EQUAL, Pure.AT_BOOLEAN_EQ,
                Pure.AT_COLLECTION_CONTAINS, Pure.AT_COLLECTION_REMOVE_DUPLICATES,
                java.util.List.of(FunctionId.of(Pure.IN__ANY_1__ANY_MANY)))) {
            rules.put(compared, NamedRule.MIXED_NUMBERS_OF_THREE_TYPES);
        }
        RULES = java.util.Collections.unmodifiableMap(rules);
    }

    /** The rule that lowers {@code call} with upstream's ties, or null where the call is none of these functions. */
    static @Nullable NamedRule rule(TypedNativeCall call) {
        return RULES.get(call.callee().id());
    }
}
