// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

/**
 * The kinds of value a parameter whose SQL type its value decides can take (a Float, a Decimal, a Number, a Date, a
 * DateTime: {@link ValueTyping}), each typed as its literal is: the type is known only when the value is.
 */
public enum ValueKind {
    /** A whole number, a BIGINT (a Number's integer). */
    INTEGER,
    /** A decimal of its own digits -- its precision and scale -- as its literal is typed (a Decimal; a Float's or a
     *  Number's plain digits: the numeric charter's Rule 1, {@link SqlTyping#floatDecimal}). */
    DECIMAL,
    /** A float at an extreme magnitude, written in exponent form (Rule 1): a floating type of its own digits. */
    FLOATING,
    /** A date (a Date's StrictDate value). */
    DATE,
    /** A date-time to the microsecond (a DateTime, a Date's DateTime value). */
    DATE_TIME,
    /** A date-time with digits finer than a microsecond: DuckDB types its literal {@code TIMESTAMP_NS}, Postgres cuts
     *  the finer digits (its literal writer does: Postgres rounds them). */
    DATE_TIME_NANOS
}
