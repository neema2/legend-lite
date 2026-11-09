// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

/**
 * Observer of statement-root assert adjudications (the AssertVerdicts
 * arm). The platform owns the JUDGMENT — this only reports it, so a
 * runner can score (how many asserts a test verified, which one failed)
 * without re-implementing assert semantics. One event per adjudicated
 * assert, in statement order; {@code pass=false} carries the raised
 * detail and precedes the run's failure (first-failure sequencing).
 * OUTPUT ONLY (hole 4, 2026-11-08): the run tells it what happened and
 * asks it for nothing -- no method answers a value, and planning is
 * never handed one.
 */
public interface AssertListener {

    void verdict(String assertName, boolean pass,
            @com.legend.base.Nullable String detail);

    /** A verdict arm DECIDED BY TEXT (Phase 0.6): the rows leg could not
     * be judged — {@code reason} names why (the arm's own vocabulary:
     * {@code foreign-dialect}, {@code rows-underivable}, {@code
     * oracle-declined}, …) — and the byte-equal text is the contract for
     * this assert. Reported BEFORE the text decides, never silently; the
     * runner counts per test and pins ceilings. The platform's fact
     * ledger rides this seam, not a static sink. */
    default void declined(String assertName, String reason) {
    }

    /** The DATABASE judge declined this assert's shape (leg 3.3): the
     * verdict that follows is a failure carrying {@code reason}. Reported
     * BEFORE the verdict, like a decline; the differential ledger keys its
     * UNJUDGED rows on this event, never on message text. */
    default void unjudged(String assertName, String reason) {
    }

    /** The REFEREE judged this assert's rows leg (Phase 0.7): {@code outcome}
     * is the row verdict's name (MATCH / DIVERGED / DECLINED / FAULT). A
     * MATCH is the differential witness — the strength census reads it. */
    default void refereed(String assertName, String outcome) {
    }
}
