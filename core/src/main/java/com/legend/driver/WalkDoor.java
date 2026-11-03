// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.base.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * THE WALK'S DOOR (the walker-deletion program, 2026-11-03, the user's "they cannot hide or come back"): where a lane
 * names the bodies still allowed to walk -- {@code -Dlegend.walk.allowed=classpath:<resource>}, a list of one name a line
 * (a JUnit test as {@code class#method}, a PCT or corpus test by its function's full name; {@code #} comments) -- a
 * body of any other test is refused by name instead of walked, so nothing new reaches the walk while the list burns to
 * zero; the list only shrinks. The running test is named by its runner ({@code legend.test.current}: the JUnit
 * launcher for a method's test, channel B for a PCT test; a test it does not name -- a dynamic test, a lane whose
 * runner sets none -- is refused under a list). {@code -Dlegend.walk.allowed=none} closes the walk to every test: a lane
 * whose list burned to zero holds no list that could grow back (channel B, 2026-11-03). With no property the door is
 * open (a product, a host that holds no list). The door decides nothing about a body the program road runs.
 */
final class WalkDoor {

    private WalkDoor() {
    }

    /** Refuses the walk for the running test when the lane names an allow list that does not name it; {@code why}
     *  says why the body would walk. */
    static void check(String why) {
        String list = System.getProperty("legend.walk.allowed");
        if (list == null) {
            return;
        }
        String current = System.getProperty("legend.test.current");
        if (current != null && !"none".equals(list) && allowed(list).contains(current)) {
            return;
        }
        throw new IllegalStateException("the walk is closed: " + (current == null ? "a body no test names"
                : "the test " + current) + " would walk (" + why + ") and is not on the walk's allow list ("
                + ("none".equals(list) ? "the lane walks nothing" : list) + "): move the body to the program road");
    }

    /** The list read each time a body would walk (no state held: a lane walks few bodies, and the list only
     *  shrinks). */
    private static Set<String> allowed(String list) {
        if (!list.startsWith("classpath:")) {
            throw new IllegalStateException("the walk's allow list names a classpath resource ('classpath:<name>'),"
                    + " not '" + list + "'");
        }
        String name = list.substring("classpath:".length());
        @Nullable InputStream in = WalkDoor.class.getClassLoader().getResourceAsStream(name);
        if (in == null) {
            throw new IllegalStateException("the walk's allow list " + name + " is not on the classpath");
        }
        Set<String> out = new HashSet<>();
        try (in) {
            for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
                String t = line.strip();
                if (!t.isEmpty() && !t.startsWith("#")) {
                    out.add(t);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return java.util.Collections.unmodifiableSet(out);
    }
}
