// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.ladder;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * THE LEAN SQL LADDER (user north star, 2026-09-20 -- docs/LEAN_VERDICT_LADDER_2026_09_20.md): one statement per
 * test body, every {@code let}'s product SQL in it exactly once, unchanged from what the platform emits for a user,
 * and the thinnest assert wrapper around it.
 *
 * <p>We own every rung ({@link LadderRender}'s model: one three-row table, one {@code <<test.Test>>} function per
 * rung, each adding ONE construct to the previous). Each rung's CURRENT emission is pinned in
 * {@code ladder/<rung>.current.sql}, made by Bazel ({@code //core:ladder_report}; {@code //core:update_ladder_test}
 * fails when it moves). Beside it sits the hand-written LEAN target ({@code <rung>.lean.sql}). A rung is CLOSED when
 * current equals lean; until then this prints the distance (chars, subqueries). It reads both as classpath resources
 * and writes nothing.
 */
class LeanSqlLadderTest {

    @Test
    @DisplayName("every rung has its current pin; distance to the lean target reported")
    void ladder() throws IOException {
        List<String> report = new ArrayList<>();
        List<String> missing = new ArrayList<>();
        for (String rung : LadderRender.rungs()) {
            String current = resource(rung + ".current.sql");
            if (current == null) {
                missing.add(rung + ": no current pin (bazel run //core:update_ladder)");
                continue;
            }
            String lean = resource(rung + ".lean.sql");
            String status = lean == null ? "OPEN" : normalize(lean).equals(normalize(current)) ? "CLOSED" : "OPEN";
            report.add(String.format(java.util.Locale.ROOT, "%-24s %-8s statements=%d chars=%5d subqueries=%3d  %s",
                    rung, status, current.split("\n;;\n", -1).length, current.length(),
                    count(current, "(SELECT "), lean == null ? "(no lean target written yet)" : "lean chars=" + lean.length()));
        }
        report.forEach(l -> System.out.println("[ladder] " + l));
        assertTrue(missing.isEmpty(), String.join("\n", missing));
    }

    private static String resource(String name) throws IOException {
        try (InputStream in = LeanSqlLadderTest.class.getResourceAsStream("/ladder/" + name)) {
            return in == null ? null : new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String normalize(String sql) {
        return sql.strip().replaceAll("\\s+", " ");
    }

    private static int count(String s, String needle) {
        int n = 0;
        for (int i = s.indexOf(needle); i >= 0; i = s.indexOf(needle, i + 1)) {
            n++;
        }
        return n;
    }
}
