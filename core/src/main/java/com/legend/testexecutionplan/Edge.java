// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import java.util.List;

/**
 * THE COMPATIBILITY EDGE'S HOOKS (docs/PLAN_AS_DATA_2026_11_07.md 2.7, the design's step 8): handed to a run by its
 * caller ({@code ExecuteOptions.recording}: an edge implies the compatibility rules, ruled 2026-11-08), never found by
 * the product. At planning the test door hands it each effect's ledger texts and each seed's referee's copy, and plans
 * the records it answers into the test plan by the step's index; at run it is told what each such step did, and
 * builds the referee's ledger from the planned records and the run's events -- the ledger is the run's output, never
 * planned and never read back into a run.
 */
public interface Edge {

    /** Whether this hook records the referee's ledger, so the test door plans its records at each effect and seed step
     *  and the runner tells it each such step ran (census C1, the orchestrator's ruling of 2026-11-17: the hook decides,
     *  never a null): the edge's own recorders do; the product's {@link NoEdge#NONE} does not, and refuses the rest. */
    boolean records();

    /** The rules a run handed this hook judges by (census C1, 2026-11-17: the hook names them, never a flag of the
     *  rules; the user's ruling of 2026-11-08, docs/PLAN_AS_DATA_2026_11_07.md 3a.3: a recorder's run judges by the
     *  compatibility rules): the product's {@link NoEdge#NONE}, lite's. */
    TestPlan.JudgingRules rules();

    /** An effect step's statements' ledger texts, in order, each with whether it is a query: the records to plan at the
     *  step. */
    List<Extension> effect(List<String> texts, List<Boolean> queries);

    /** A seed step's REFEREE'S COPY (B2b, 2026-11-11): the step's load as the replay referee's own database runs it --
     *  {@code steps} one for each of the plan's load's steps, in order, reading {@code content} by digest -- the records
     *  to plan at the step. */
    List<Extension> seed(List<com.legend.executionplan.ExecutionPlan.LoadStep> steps,
            java.util.Map<String, String> content);

    /** A seed step's statements ran ({@code answeredRows}: whether each answered rows, those before a failure when it
     *  failed), {@code planned} the records planned at it. */
    void seeded(List<Extension> planned, List<Boolean> answeredRows);

    /** An effect step ran, {@code planned} the records planned at it. */
    void effectRan(List<Extension> planned);

    /** legend-pure's words of a statement's raise sites, as the planning call's style recorded them ({@code words}):
     *  the records to plan at the step that sends the statement, which the run's hearing reads when an assertError
     *  matches a raise under the compatibility rules (K10 of the edge audit, the orchestrator's ruling (A) of
     *  2026-11-17). The compatibility edge plans them as they are; the product's {@link NoEdge#NONE} refuses by name. */
    List<Extension> siteWords(List<Extension> words);
}
