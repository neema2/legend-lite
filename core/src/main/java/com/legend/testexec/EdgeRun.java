// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Nullable;
import com.legend.testexecutionplan.AssertListener;
import com.legend.testexecutionplan.TestPlan;

/**
 * THE COMPATIBILITY EDGE'S RUN HOOK (census C2, the orchestrator's P2 of 2026-11-17): what a test body's run hands the
 * compatibility appeals and their referee -- the slot the referee held, retyped. A ruled hook (HookRulingTest):
 * signatures only here; every implementation is the edge's ({@code com.legend.edge}), the product's own answer
 * {@link NoEdgeRun} refusing an appeal by name. The caller always passes one: lite's runs pass {@link NoEdgeRun#NONE}.
 */
public interface EdgeRun {

    /** The hearing of {@code plan}'s appeals for one run of it, its reports to {@code callers}; the run's execution id
     *  {@code execution}. A plan naming an appeal this run cannot hear is refused by name. */
    EdgeHearing open(TestPlan plan, @Nullable AssertListener callers, java.util.UUID execution);

    // (wordsAlike -- whether both judges of an isolated run write the same words, census C1 of 2026-11-17 -- deleted
    // with A4 of the compatibility program, stage A ratified by the user 2026-11-19: the host judge is the only judge in compatibility, so only lite's runs
    // run both judges isolated, and both write lite's sentences)
}
