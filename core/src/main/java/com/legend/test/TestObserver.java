// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.test;

import com.legend.ExecuteOptions;
import com.legend.testexecutionplan.AssertListener;

/**
 * The seam a caller instruments a {@link PureTestRunner} through. Every
 * method has a no-op default: a user running their own tests attaches
 * nothing; the corpus harness attaches its referee (the replay referee, the
 * H2 mirror), its raw-SQL recorder and its strength census here — and
 * nowhere else. The runner owns the judgment; the observer only watches.
 *
 * <p>The assert events ({@link #verdict}, {@link #declined}, {@link #refereed})
 * are forwarded exactly as the platform reports them, after the runner has
 * recorded them for its own result.
 */
public interface TestObserver extends AssertListener {

    /** A test is about to run (display attribution, never a verdict). */
    default void testStarted(PureTests.TestCase test) {
    }

    /** A package session opened on {@code conn} (a session the runner owns; D68, 2026-10-30: the observer sees
     *  sessions, never connections -- a harness that needs the driver's object asks the session for it); its setups
     *  run next. */
    default void sessionBegan(String pkg, com.legend.exec.Sessions.Session conn) {
    }

    /** The current session is closing. */
    default void sessionEnding() {
    }

    /** The test runs on a PRIVATE workspace (it seeds its own inline data)
     *  rather than the package session; {@code false} restores the session. */
    default void privateWorkspace(boolean on) {
    }

    /** The execution options the body and its setups run with — a recorder,
     *  a test-resource resolver. {@code shared} says the body runs on the
     *  package session (its statements join the session's seed ledger). */
    default ExecuteOptions options(PureTests.TestCase test, boolean shared) {
        return ExecuteOptions.NONE;
    }

    /** The run that hears this test's compatibility appeals (census C2, 2026-11-17): the edge's, or the product's,
     *  which hears none ({@link com.legend.testexec.NoEdgeRun#NONE}). No default (census K6, 2026-11-18, G7): every
     *  observer states its run, so no observer is the product's by omission. */
    com.legend.testexec.JudgeRun edgeRun(PureTests.TestCase test);

    /** H10 SPIKE STAND-IN, A NEW CORE HOOK (never for merge): the typed body as the caller's door rewrites it before
     *  it is planned -- where the phase-3 translator's R-S131 would sit; the corpus has no such door today. */
    default com.legend.TypedQuery door(com.legend.TypedQuery body) {
        return body;
    }

    /** The body is about to execute; {@code effectful} says the platform
     *  found statement effects in it (an attempt a referee may mark). */
    default void bodyStarting(com.legend.exec.Sessions.Session conn, boolean effectful) {
    }

    /** The body executed and its verdicts held — an effectful body's session
     *  state stays, as the engine's run leaves it. */
    default void bodyPassed(com.legend.exec.Sessions.Session conn, boolean effectful) {
    }

    /** The body failed (an exception, a failed assert, a missing verdict) —
     *  an effectful attempt is rolled back. */
    default void bodyFailed(com.legend.exec.Sessions.Session conn, boolean effectful) {
    }

    /** The test finished on a shared session with {@code options}' recorder
     *  holding everything it executed — the caller's seed ledger reads it. */
    default void bodyFinished(PureTests.TestCase test, boolean shared, ExecuteOptions options) {
    }

    @Override
    default void verdict(String assertName, boolean pass, @com.legend.base.Nullable String detail) {
    }

    /** The observer of a caller that attaches no instrument: the product's run, stated (census K6, 2026-11-18). */
    TestObserver NONE = new TestObserver() {
        @Override
        public com.legend.testexec.JudgeRun edgeRun(PureTests.TestCase test) {
            return com.legend.testexec.NoEdgeRun.NONE;
        }
    };
}
