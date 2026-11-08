// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.judge;

/**
 * Observer of statement-root assert adjudications (the AssertVerdicts
 * arm). The platform owns the JUDGMENT — this only reports it, so a
 * runner can score (how many asserts a test verified, which one failed)
 * without re-implementing assert semantics. One event per adjudicated
 * assert, in statement order; {@code pass=false} carries the raised
 * detail and precedes the run's failure (first-failure sequencing).
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

    /** A fixture PLANNED AHEAD (zero exemptions, the owner, 2026-11-05; the run-now provision, provideStore, deleted
     *  with the walk, 2026-11-07): the engine's suite runs a package's BeforePackage setups only for that package's
     *  tests, so a golden whose mapping reads another package's store needs that store seeded. The stores an
     *  engine-text assert's rows leg reads are NAMED by its appeal in the test plan (the plan-as-data design's step 8c,
     *  2026-11-08); the harness is asked for each before the body's run ({@link #beforeRun}), and the provision it
     *  answers -- the one setup's program planned then -- runs where the rows leg reads, after the run; true where it
     *  seeded the store. Asked again by name there, it answers the same provision. Null: no fixture to provide, or none
     *  that can be planned ahead. Default: none. */
    default java.util.function.@com.legend.base.Nullable BooleanSupplier fixtureAhead(String storeFqn) {
        return null;
    }

    /** The body is planned and its run is next (the walk deletion's step 3, 2026-11-06): the fixtures of the
     *  {@code stores} its plan's appeals name (step 8c, 2026-11-08) are provided HERE ({@link #fixtureAhead}), each a
     *  run of its own before the body's -- never a run inside it (THE WALL's rule 4) -- and the appeal's own provision
     *  is then a no-op. Default: nothing. */
    default void beforeRun(java.util.Set<String> stores) {
    }
}
