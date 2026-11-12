// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.base.Nullable;
import com.legend.exec.SessionSource;
import com.legend.exec.Sessions;
import com.legend.exec.StatementOrigin;
import com.legend.result.Output;
import com.legend.testexec.SqlReplayReferee;
import com.legend.testexec.TestRunner;
import com.legend.testexecutionplan.AssertListener;
import com.legend.testexecutionplan.TestPlan;
import com.legend.testplan.TestPlanner;

/**
 * THE TEST BODY'S DOOR (the plan-as-data design's slice 5, 2026-11-09; {@code Execution}'s test half until then): a
 * test body planned whole by the test planner ({@link TestPlanner}, which never touches a database), and its test plan
 * run by the test runner ({@link TestRunner}, which plans nothing). The one class that sees both halves
 * ({@code ArchitectureTest.onlyTheDoorSeesBothHalves}); each method is one planning call and one run call, or one of
 * them -- no logic, no state ({@code ArchitectureTest.theDoorsOnlyCompose}). Its own library
 * ({@code //core:test_execution}): the product's door ({@link Execution}) carries none of the test's machinery.
 *
 * <p>EACH CALL IS A RUN, AND EACH {@code execute()} OF ITS BODY AN EXECUTION (the user's rulings of 2026-11-12, "Do the
 * exec id as part of C" and "this needs to go in C that was the whole point of C"): the door mints the run's id and one
 * per execution its plan declares ({@link ExecutionIds#minted}: as upstream mints a GUID per interpreted execute) and
 * hands them to exec, which binds each to the stamps that read it and tells the run's to the listener, and makes no
 * value of its own ({@code ArchitectureTest.execMakesNoValueOfItsOwn}). No caller supplies an id.
 */
public final class TestExecution {

    private TestExecution() {
    }

    /**
     * THE ONE ENTRY FOR A PROGRAM (track B's B2, 2026-10-29): a test body -- lets, effects, asserts, a value -- typed
     * once, planned whole ({@link #planBody}), then run ({@link TestRunner#runOpening}) on the session {@code sessions}
     * opens for the target the plan names (its runtime's), every assert's verdict told to {@code listener} at its turn,
     * the SQL-text referee {@code referee} deciding the appeals held after the run, the last statement's answer, where
     * a caller wants it, streamed to {@code answer} (a format and a stream, or the caller's receiver: data, 2026-11-09). No fixture is provided between the plan and the run: a
     * caller whose appeals read another package's store plans and runs the two halves itself ({@link #planBody},
     * {@link #runBody}), providing them between.
     */
    public static void runProgram(TypedQuery typed, @Nullable String runtimeFqn, SessionSource sessions,
            @Nullable AssertListener listener, @Nullable SqlReplayReferee referee, ExecuteOptions options,
            @Nullable Output answer) {
        TestRunner.runOpening(ExecutionIds.minted(TestPlanner.plan(typed, runtimeFqn, options.planning())), sessions,
                options.stores(), options.edge(), listener, referee, answer);
    }

    /** {@link #runProgram(TypedQuery, String, SessionSource, AssertListener, SqlReplayReferee, ExecuteOptions, Output)}
     *  with no answer wanted: the body's last statement answers nothing to anyone (the corpus's bodies, whose verdicts
     *  are the listener's). */
    public static void runProgram(TypedQuery typed, @Nullable String runtimeFqn, SessionSource sessions,
            @Nullable AssertListener listener, @Nullable SqlReplayReferee referee, ExecuteOptions options) {
        runProgram(typed, runtimeFqn, sessions, listener, referee, options, null);
    }

    /** A test body PLANNED WHOLE, with no session and nothing told to anyone (hole 4, 2026-11-08): run by
     *  {@link #runBody}. A body the program road does not plan is refused by name. */
    public static TestPlan planBody(TypedQuery typed, @Nullable String runtimeFqn, ExecuteOptions options) {
        return TestPlanner.plan(typed, runtimeFqn, options.planning());
    }

    /** {@code plan} RUN on {@code session} (the run half of {@link #runProgram}): its data alone decides what is sent
     *  and judged; each verdict told to {@code listener} at its turn; a held appeal decided after the run by the
     *  {@code referee}; the last statement's answer, where wanted, streamed to {@code answer} (a format and a stream, or the caller's receiver: data, 2026-11-09). */
    public static void runBody(TestPlan plan, Sessions.Session session, @Nullable AssertListener listener,
            @Nullable SqlReplayReferee referee, ExecuteOptions options, @Nullable Output answer) {
        TestRunner.run(ExecutionIds.minted(plan), session, options.stores(), options.edge(), listener, referee, answer,
                StatementOrigin.OTHER);
    }

    /** A fixture's {@code plan} (a package's setup, planned by {@link #planBody}) run on {@code session}, its
     *  statements counted as seeding (the census's SEED). */
    public static void runFixture(TestPlan plan, Sessions.Session session, ExecuteOptions options) {
        TestRunner.run(ExecutionIds.minted(plan), session, options.stores(), options.edge(), null, null, null,
                StatementOrigin.SEED);
    }
}
