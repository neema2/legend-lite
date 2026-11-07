// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.judge;

/**
 * Whether the assert being judged on this thread carries a CALLER'S MESSAGE (P1.2, the goals of record, 2026-11-07): a
 * compatibility appeal is decided after the body's run, where the message -- a side the body's run reads -- is no longer
 * readable, so an appeal held while a messaged assert is judged is marked, and its failure is refused by name rather
 * than told without the message. Set by the judge around the judgment it hands an appeal; read when the appeal is held.
 */
public final class CallerMessage {

    private CallerMessage() {
    }

    private static final ThreadLocal<Integer> DEPTH = ThreadLocal.withInitial(() -> 0);

    /** A messaged assert's judgment is under way on this thread until the scope closes. */
    public static Scope judging() {
        DEPTH.set(DEPTH.get() + 1);
        return () -> DEPTH.set(DEPTH.get() - 1);
    }

    /** Whether a messaged assert is being judged on this thread. */
    public static boolean present() {
        return DEPTH.get() > 0;
    }

    /** A judgment's scope, closed when it ends. */
    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }
}
