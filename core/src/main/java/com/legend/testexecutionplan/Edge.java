// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import java.util.List;

/**
 * THE COMPATIBILITY EDGE'S HOOKS (docs/PLAN_AS_DATA_2026_11_07.md 2.7, the design's step 8): handed to a run by its
 * caller ({@code ExecuteOptions.recording}: an edge implies the compatibility rules, ruled 2026-11-08), never found by
 * the product. At planning the test door hands it each effect's and each seed's ledger texts and plans the records it
 * answers into the test plan by the step's index; at run it is told what each such step did, and builds the referee's
 * ledger from the planned texts and the run's events -- the ledger is the run's output, never planned and never read
 * back into a run.
 */
public interface Edge {

    /** An effect step's statements' ledger texts, in order, each with whether it is a query: the records to plan at the
     *  step. */
    List<Extension> effect(List<String> texts, List<Boolean> queries);

    /** A seed step's statements' ledger texts, in order: the records to plan at the step. */
    List<Extension> seed(List<String> texts);

    /** A seed step's statements ran ({@code answeredRows}: whether each answered rows, those before a failure when it
     *  failed), {@code planned} the records planned at it. */
    void seeded(List<Extension> planned, List<Boolean> answeredRows);

    /** An effect step ran, {@code planned} the records planned at it. */
    void effectRan(List<Extension> planned);
}
