// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Executions;
import com.legend.testexecutionplan.TestPlan;

/**
 * A test plan with the executions its DOOR minted for one run (the execution id as upstream, the user's ruling of
 * 2026-11-12): the run's own id and one per execution the plan declares ({@link TestPlan#executions}: each
 * {@code execute()} of the body), made by the door ({@code com.legend.ExecutionIds}) and handed to the test runner,
 * which binds them and makes none. Data: no logic.
 */
public record MintedPlan(TestPlan plan, Executions executions) {

    public MintedPlan {
        java.util.Objects.requireNonNull(plan, "plan");
        java.util.Objects.requireNonNull(executions, "executions");
        java.util.Map<String, java.util.UUID> declared = executions.declared();
        if (declared == null || !declared.keySet().equals(new java.util.LinkedHashSet<>(plan.executions()))) {
            throw new IllegalArgumentException("a test plan's run is minted one id per execution it declares ("
                    + plan.executions() + "), and was minted " + (declared == null ? "one for the run alone"
                            : declared.keySet()));
        }
    }
}
