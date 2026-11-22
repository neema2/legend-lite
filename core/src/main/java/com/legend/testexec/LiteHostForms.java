// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

/**
 * THE PRODUCT'S FORM OF {@link HostForms} (GUARD's C.9, the orchestrator's ruling of 2026-11-17: a two-form kind is
 * types only, its product form a class of its own beside it). Lite's form (register S127, the user's ruling of
 * 2026-11-04): two numbers of different kinds are never equal -- the type is part of the value.
 */
public enum LiteHostForms implements HostForms {
    STRICT;

    @Override
    public boolean equalAcrossNumberKinds(Equality.Kind ek, Equality.Kind ak, Object e, Object a) {
        return false;
    }

    /** Lite compares a Decimal by its value (register S139, the user's ruling of 2026-11-05), as {@code ==} does in
     *  the database. */
    @Override
    public boolean equalDecimals(java.math.BigDecimal e, java.math.BigDecimal a) {
        return e.compareTo(a) == 0;
    }

    /** Lite compares two dates by their kind and their instant (register S399, the user's ruling of 2026-11-22: a
     *  year, a year-month, a day and a date-time are kinds never equal across; two date-times at one instant are equal
     *  whatever their written precision, as {@code ==} compares them in the database). The instant is the microsecond
     *  a database's TIMESTAMP holds: a digit past the sixth is not compared, as the database judge's TIMESTAMP holds
     *  none (S399's text, corrected 2026-11-22). */
    @Override
    public boolean equalDates(Object e, Object a) {
        if (e instanceof com.legend.values.PureDateLiteral ed && a instanceof com.legend.values.PureDateLiteral ad) {
            return kind(ed) == kind(ad) && ed.toInstantFloor().truncatedTo(java.time.temporal.ChronoUnit.MICROS)
                    .equals(ad.toInstantFloor().truncatedTo(java.time.temporal.ChronoUnit.MICROS));
        }
        return e.equals(a);
    }

    /** A date's kind under S399: its precision below the hour; every date-time one kind. */
    private static com.legend.values.PureDateLiteral.Precision kind(com.legend.values.PureDateLiteral d) {
        com.legend.values.PureDateLiteral.Precision p = d.precision();
        return p.atLeast(com.legend.values.PureDateLiteral.Precision.HOUR)
                ? com.legend.values.PureDateLiteral.Precision.HOUR : p;
    }

    /** Lite compares a Float exactly (S15/S132, the user's ruling of 2026-11-05: "I like our clean float"). */
    @Override
    public boolean equalFloatsApart(double e, double a) {
        return false;
    }

    /** Lite's words of a caught error: its own message (K10). */
    @Override
    public String caught(RuntimeException error, EdgeHearing hearing) {
        return String.valueOf(error.getMessage());
    }
}
