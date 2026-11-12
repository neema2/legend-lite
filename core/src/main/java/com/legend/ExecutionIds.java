// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.base.Executions;
import com.legend.testexec.MintedPlan;
import com.legend.testexecutionplan.TestPlan;

/**
 * THE TEST DOOR'S MINT (the execution id as upstream, the user's ruling of 2026-11-12: "this needs to go in C that was
 * the whole point of C"). Upstream mints a fresh random id per execution: a Pure test body's {@code execute()} calls
 * each run under their own ({@code relationalMappingExecution.pure:475-492}, a GUID per query), a plan's run under one
 * ({@code ExecutionState.java:149}). A body's test plan declares one execution per {@code execute()}
 * ({@link TestPlan#executions}), so its door mints the run's id and one for each, here -- the door side, where an
 * execution starts: exec, the test's run and planning make no value ({@code ArchitectureTest.execMakesNoValueOfItsOwn},
 * {@code planningMakesNoValueOfItsOwn}). The test door calls it as its one mint ({@code ArchitectureTest
 * .theDoorsOnlyCompose}): a door's one statement holds no loop, so the loop over the declared executions is here.
 */
public final class ExecutionIds {

    private ExecutionIds() {
    }

    /** {@code plan} with its run's executions minted: the run's id and one fresh random id per execution it declares. */
    public static MintedPlan minted(TestPlan plan) {
        java.util.Map<String, java.util.UUID> declared = new java.util.LinkedHashMap<>();
        for (String execution : plan.executions()) {
            declared.put(execution, java.util.UUID.randomUUID());
        }
        return new MintedPlan(plan, Executions.of(java.util.UUID.randomUUID(), declared));
    }
}
