// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import com.legend.testing.Runfile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE REFERENCE LANE's invariant (execution plan W1.1): every disagreement class in the report
 * ({@link ReferenceLaneReport}, made by {@code //spec:reference_lane_report}) has a reason in
 * {@code reference-lane/reasons.tsv} (kind, spelling or {@code *}, the owning plan item, the reason): reasons are per
 * (kind, spelling) class, or per kind, never per row. The report against its committed golden is
 * {@code //spec:update_reference_lane_test}.
 *
 * <p>Run: {@code bazel test //spec:reference_lane //spec:update_reference_lane_test} (manual; the report's action
 * needs about 8 GB with the reference dump).
 */
@Tag("heavy")   // never in //spec:spec_tests; its own manual lane, //spec:reference_lane
class ReferenceLaneTest {

    record Reason(String kind, String spelling, String owner, String reason) {
    }

    @Test
    void everyDisagreementClassHasAReason() throws IOException {
        List<String> classes = new ArrayList<>();
        boolean in = false;
        for (String line : Files.readAllLines(Runfile.property("reference.report"), StandardCharsets.UTF_8)) {
            if (line.startsWith("== ")) {
                in = line.startsWith("== disagreement classes");
            } else if (in && !line.isBlank()) {
                classes.add(line);
            }
        }
        assertFalse(classes.isEmpty(), "the report lists no disagreement classes: the lane is not looking");
        List<Reason> reasons = reasons();
        List<String> unexplained = new ArrayList<>();
        for (String c : classes) {
            String[] f = c.split("\t", -1);
            if (reasons.stream().noneMatch(r -> r.kind().equals(f[0])
                    && (r.spelling().equals("*") || r.spelling().equals(f[1])))) {
                unexplained.add(f[0] + "\t" + f[1]);
            }
        }
        assertTrue(unexplained.isEmpty(), "disagreement classes with no reason in reference-lane/reasons.tsv:\n  "
                + String.join("\n  ", unexplained.stream().distinct().toList()));
    }

    static List<Reason> reasons() throws IOException {
        List<Reason> out = new ArrayList<>();
        for (String line : resource("reference-lane/reasons.tsv").split("\n")) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] c = line.split("\t", -1);
            if (c.length != 4 || c[2].isBlank() || c[3].isBlank()) {
                throw new IllegalStateException("reasons.tsv: kind, spelling or *, owner, reason: " + line);
            }
            out.add(new Reason(c[0], c[1], c[2], c[3]));
        }
        return out;
    }

    private static String resource(String name) throws IOException {
        try (InputStream in = ReferenceLaneTest.class.getClassLoader().getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException("missing test resource " + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
