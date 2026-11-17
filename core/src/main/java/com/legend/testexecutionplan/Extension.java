// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

/**
 * THE EDGE'S DATA IN A TEST PLAN (docs/PLAN_AS_DATA_2026_11_07.md 2.7, the user's ruling of 2026-11-08: the edge plans,
 * under the compatibility rules, into a typed extension point): a record of the compatibility edge's, held by the test
 * plan by step ({@link TestPlan#extensionsByStep}: an effect's or a seed's ledger texts) or by verdict
 * ({@link TestPlan#extensionsByVerdict}). The test plan declares it and the edge implements it -- the edge depends on
 * the product, never the reverse (G5); its JSON is the edge's codec's ({@link ExtensionCodec}, found by the
 * {@code ServiceLoader} seam). Under lite's own rules nothing is planned for the edge.
 */
public interface Extension {

    /** A record the run's HEARING reads ({@code testexec.EdgeHearing}), never the ledger's hook ({@link Edge}): held
     *  at the step that sends its statement, whatever edge hook the run was handed (K10b of the edge audit, 2026-11-17:
     *  a raise site's planned words, which a compatibility run heard by the edge's hearing looks up). */
    interface Heard extends Extension {
    }
}
