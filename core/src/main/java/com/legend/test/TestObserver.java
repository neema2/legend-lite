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

    /** The audience {@code test}'s body and setups are run for: its options, its run hook and its door together (B3
     *  step 2b, 2026-11-24: until then three methods, options, edgeRun and admitted, chosen apart). No default (census
     *  K6, 2026-11-18, G7): every observer states its audience, so no observer is the product's by omission. */
    Audience audience(PureTests.TestCase test);

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
        public Audience audience(PureTests.TestCase test) {
            return Audience.PRODUCT;
        }
    };
}
