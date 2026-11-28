// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

/**
 * THE PRODUCT'S FORM OF {@link HostForms} (GUARD's C.9, the orchestrator's ruling of 2026-11-17: a two-form kind is
 * types only, its product form a class of its own beside it): a caught error in lite's words (its Float form went
 * 2026-11-24, B3 step 1b: a Float compared exactly on every route). Its number-kind and Decimal forms went with B3 step 1a, 2026-11-24 (the judge's own rules on every route, in
 * {@code Equality}); its date rule stays here, the judge's on every route, until step 5.
 */
public enum LiteHostForms implements HostForms {
    STRICT;

    /** Lite compares two dates by their kind and their instant (register S399, the user's ruling of 2026-11-22: a
     *  year, a year-month, a day and a date-time are kinds never equal across; two date-times at one instant are equal
     *  whatever their written precision, as {@code ==} compares them in the database). Every digit each carries is
     *  compared (the user's rule of 2026-11-22: nanoseconds compared where they are held, refused by name where the
     *  database cannot read them -- S181 -- never cut). The judge's own rule on every route since B3 step 1a
     *  (2026-11-24): no longer a hook form, it stays here until step 5 (the one equality) rebuilds the judge's rules
     *  (moving it into {@code Equality} would grow that file past its ledger pin). */
    public boolean equalDates(Object e, Object a) {
        if (e instanceof com.legend.values.PureDateLiteral ed && a instanceof com.legend.values.PureDateLiteral ad) {
            return kind(ed) == kind(ad) && ed.toInstantFloor().equals(ad.toInstantFloor());
        }
        return e.equals(a);
    }

    /** A date's kind under S399: its precision below the hour; every date-time one kind. */
    private static com.legend.values.PureDateLiteral.Precision kind(com.legend.values.PureDateLiteral d) {
        com.legend.values.PureDateLiteral.Precision p = d.precision();
        return p.atLeast(com.legend.values.PureDateLiteral.Precision.HOUR)
                ? com.legend.values.PureDateLiteral.Precision.HOUR : p;
    }

    /** Lite's numbers are of two families: an Integer and a Decimal of one value are equal. */
    @Override
    public boolean exactNumbersByValue() {
        return true;
    }

    /** Lite's words of a caught error: its own message (K10). */
    @Override
    public String caught(RuntimeException error, EdgeHearing hearing) {
        return String.valueOf(error.getMessage());
    }
}
