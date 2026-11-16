// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.channelb;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * G10'S GUARD BITES (its planted violators, 2026-11-13): {@code ChannelB.twice} names a body whose two plans differ,
 * and one whose two runs tell different verdicts or end differently; a body that agrees with itself passes.
 */
class ChannelBTwiceTest {

    /** Each call answers the next of {@code answers}. */
    @SafeVarargs
    private static <T> Supplier<T> calls(T... answers) {
        AtomicInteger at = new AtomicInteger();
        return () -> answers[at.getAndIncrement()];
    }

    @Test
    void aBodyThatAgreesWithItselfPasses() {
        assertNull(ChannelB.twice(calls("{\"plan\":1}", "{\"plan\":1}"),
                calls(List.of("a pass", "ended: pass"), List.of("a pass", "ended: pass"))));
    }

    @Test
    void twoPlansOfOneBodyAreNamed() {
        String differs = ChannelB.twice(calls("{\"id\":\"1111\"}", "{\"id\":\"2222\"}"),
                calls(List.of("ended: pass"), List.of("ended: pass")));
        assertTrue(differs != null && differs.startsWith("planned twice, two plans: they part at character 7: 1111"),
                String.valueOf(differs));
    }

    @Test
    void twoRunsTellingDifferentVerdictsAreNamed() {
        String differs = ChannelB.twice(calls("{}", "{}"),
                calls(List.of("a pass", "ended: pass"), List.of("a FAIL expected 1", "ended: pass")));
        assertTrue(differs != null && differs.startsWith("run twice, two answers:"), String.valueOf(differs));
    }

    @Test
    void twoRunsEndingDifferentlyAreNamed() {
        String differs = ChannelB.twice(calls("{}", "{}"),
                calls(List.of("ended: pass"), List.of("ended: SQLException: boom")));
        assertTrue(differs != null && differs.contains("ended: SQLException: boom"), String.valueOf(differs));
    }
}
