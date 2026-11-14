// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testing;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The lanes' floors are LIVE: every key of {@code lanes/<lane>.txt} names an integration test that exists (its class,
 * nested with {@code $}, and its method), so a line cannot outlive its test by a rename or a deletion -- a floor
 * only shrinks when the lane says so, never silently (track T, T-d2, 2026-10-29; the T-d audit's finding 11). A line
 * that pins a test as NOT RUN on the lane carries its date and cause ({@link LaneFloor#NOT_RUN}); every key's class
 * is under the extension. The lanes listed here mirror {@code _CORE_LANES} in core/BUILD.bazel: a lane added there
 * is added here.
 */
class LaneFloorTest {

    private static final List<String> LANES = List.of("sqlite", "h2");

    @Test
    void everyFloorLineNamesATestThatExists() throws Exception {
        List<String> dead = new ArrayList<>();
        int lines = 0;
        for (String lane : LANES) {
            Map<String, String> floor = LaneFloor.floor(lane);
            // an empty floor is a floor (it only shrinks); a missing file is not
            assertTrue(LaneFloorTest.class.getResource("/lanes/" + lane + ".txt") != null,
                    "the " + lane + " lane's floor file is missing");
            for (String key : floor.keySet()) {
                lines++;
                String reason = floor.get(key);
                if (reason.startsWith(LaneFloor.NOT_RUN) && !reason.matches("not run \\d{4}-\\d{2}-\\d{2}[ :].*")) {
                    dead.add(lane + ": " + key + " (a not-run pin without its date and cause: '" + reason + "')");
                }
                String cls = key.substring(0, key.indexOf('#'));
                String method = key.substring(key.indexOf('#') + 1);
                // an invocation's ordinal is not the method's name
                int bracket = method.indexOf('[');
                if (bracket >= 0) {
                    method = method.substring(0, bracket);
                }
                try {
                    Class<?> c = Class.forName("com.legend.integration." + cls);
                    boolean found = false;
                    for (Method m : c.getDeclaredMethods()) {
                        found |= m.getName().equals(method);
                    }
                    if (!found) {
                        dead.add(lane + ": " + key + " (no such method)");
                    }
                    if (!onTheLane(c)) {
                        // a line for a class the extension never sees is inert (the T-d2 audit's finding 9d)
                        dead.add(lane + ": " + key + " (its class is not under LaneFloor: the line is inert)");
                    }
                } catch (ClassNotFoundException e) {
                    dead.add(lane + ": " + key + " (no such class)");
                }
            }
        }
        assertEquals(List.of(), dead, "floor lines naming no test: remove them, with the reason in the commit");
        assertTrue(lines > 0);
    }

    /** Whether {@code c}, a superclass or an enclosing class registers {@link LaneFloor} (JUnit applies an enclosing
     *  class's extensions to its nested classes). */
    private static boolean onTheLane(Class<?> c) {
        for (Class<?> k = c; k != null; k = k.getSuperclass()) {
            org.junit.jupiter.api.extension.ExtendWith w = k.getAnnotation(org.junit.jupiter.api.extension.ExtendWith.class);
            if (w != null && java.util.Arrays.asList(w.value()).contains(LaneFloor.class)) {
                return true;
            }
        }
        return c.getEnclosingClass() != null && onTheLane(c.getEnclosingClass());
    }
}
