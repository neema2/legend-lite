// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

/**
 * HOW NUMBERS OF DIFFERENT TYPES COMPARE where their types are known only when the query runs (a mixed list, a side
 * of a comparison typed {@code Number} or {@code Any}): a rule of the product's, which a comparison takes with it.
 *
 * <p>{@link #TWO_FAMILIES} is the product's own (the user's ruling of 2026-11-27, "Yes I like the rule": exact with
 * exact by value, a Float only with a Float): an Integer and a Decimal are equal by their value, whichever of the two
 * types holds it, and an exact number never equals a Float.
 *
 * <p>{@link #THREE_TYPES} keeps an Integer, a Float and a Decimal three types never equal to one another:
 * interpreted legend-pure's answer on every pair, and what both compatibility doors answered before that ruling. A
 * door's table names it ({@link NamedRule#MIXED_NUMBERS_OF_THREE_TYPES}; the ruling: "legend-pure's and
 * legend-engine's modes keep their own behaviour").
 *
 * <p>The database compares the two values either way; the rule only says whether it may.
 */
public enum NumberFamilies {
    TWO_FAMILIES, THREE_TYPES;

    /** Whether a value of kind {@code a} can equal one of kind {@code b}: the two are of one family
     *  ({@link MixedEncoding#family}), or, under {@link #TWO_FAMILIES}, both are exact numbers. */
    boolean canEqual(String a, String b) {
        return MixedEncoding.family(a).equals(MixedEncoding.family(b))
                || this == TWO_FAMILIES && MixedEncoding.exactNumber(a) && MixedEncoding.exactNumber(b);
    }
}
