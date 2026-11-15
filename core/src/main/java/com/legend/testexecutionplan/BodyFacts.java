// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import java.util.Objects;

/**
 * A test body's FACTS, data (C5-J item 2, 2026-11-15: until then the PCT runner read the planner's
 * {@code planner.ProgramFacts}): whether its statements carry effects (store writes, executions, test-data generation),
 * whether its execution contexts seed inline CSV test data (a session-isolation fact), whether it reaches a verdict
 * function, and its shape, one letter per statement ({@code F} a let bound to an execute, {@code L} another let,
 * {@code A} a verdict, {@code X} an assertError, {@code E} an effect, {@code O} anything else). The test planner states
 * them over the body as its caller gave it, BEFORE any rewrite -- a test-data generator's planning, an unread let made
 * the statement it binds, an assert lifted (the C5-J audit's SHOULD 1, 2026-11-15: until then the plan's were stated
 * over the rewritten body, so an unread let read {@code O} in the plan where the door read {@code L}) -- the test plan
 * carries them ({@link TestPlan#facts}), and a runner that chooses the body's session before the body is planned reads
 * the same record from the test door ({@code TestExecution.facts}).
 */
public record BodyFacts(boolean effects, boolean seedsInlineCsv, boolean verdicts, String shape) {

    public BodyFacts {
        Objects.requireNonNull(shape, "shape");
    }
}
