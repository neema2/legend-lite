// SPIKE (slice 7 research, 2026-11-28): scratch branch only.
package com.legend.compiler.spec.typed;

/**
 * WHICH COMPARISON A CALL IS (slice 7's spike): chosen by the type-checker where it types the call, from the comparison
 * setting of the code's door ({@code compiler.spec.CrossTypeRule.reading}), carried on the call through every rebuild,
 * and read where the lowering picks the call's rule. Named by what it writes, never by who wants it.
 */
public enum ComparisonReading {

    /** The product's one equality: numbers of two families, a date-time by its instant, two column types never equal
     *  refused, two whole sides planned as one statement, an empty side as Pure answers it. */
    PRODUCT,

    /** Numbers of three types, a date's written precision part of its value, nothing refused for the types its columns
     *  hold; an empty side as Pure answers it. */
    THREE_NUMBER_TYPES,

    /** {@link #THREE_NUMBER_TYPES}; and an equality's empty side is the database's own answer where the call runs
     *  inside a store query (the stamp, {@code TypedNativeCall.outsideStore}), Pure's outside one. */
    THREE_NUMBER_TYPES_EMPTY_SIDE_BY_PLACE
}
