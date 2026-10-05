// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PHASE 4 ENTRY GATE (One-Platform Plan, audit addendum #7): a parse
 * census over the PCT TEST FILES specifically — the 1,856/2,110 figure
 * was all reference sources; the test files' own rate is the one that
 * gates channel B. Every {@code .pure} file carrying a
 * {@code <<PCT.test>>} function in the legend-pure and legend-engine
 * checkouts parses through OUR document parser.
 *
 * <p>Census-first discipline: the floor pins the measured rate; a drop
 * is a parser regression against channel B's input set, loudly.
 */
class PctParseCensusTest {

    /** Measured 2026-08-19 at the phase-4 entry gate: 236/236 — every
     * PCT test file parses at LEGEND_PLATFORM after the four m3 surface
     * additions (tagged-value string concat, negative-year date
     * literals, class multiplicity-params, range literals, the (?:?)
     * column wildcard). The floor pins totality. */
    private static final int PARSED_FLOOR = 236;

    @Test
    void pctTestFilesParse() throws IOException {
        List<Path> files = new ArrayList<>();
        collect(Corpus.pureRoot(), files);
        collect(Corpus.engineRoot(), files);
        int parsed = 0;
        List<String> failures = new ArrayList<>();
        for (Path f : files) {
            String text = Files.readString(f);
            try {
                // PCT files are the M3 DIALECT (native declarations,
                // type/mult parameters, PCT stereotypes) — channel B's
                // front door is LEGEND_PLATFORM, the dialect that already
                // parses every native signature at class-load
                com.legend.parser.ElementParser.parse(text,
                        com.legend.parser.Dialect.LEGEND_PLATFORM);
                parsed++;
            } catch (RuntimeException e) {
                String msg = String.valueOf(e.getMessage());
                failures.add(f.getFileName() + ": "
                        + msg.substring(0, Math.min(140, msg.length())));
            }
        }
        System.out.println("[pct-census] files=" + files.size()
                + " parsed=" + parsed + " failed=" + failures.size());
        failures.forEach(f2 -> System.out.println("[pct-census]   FAIL " + f2));
        assertTrue(parsed >= PARSED_FLOOR,
                "PCT test-file parse census fell below the floor: "
                        + parsed + " < " + PARSED_FLOOR);
    }

    private static void collect(Path root, List<Path> out) throws IOException {
        if (!Files.isDirectory(root)) {
            throw new IllegalStateException("PctParseCensusTest root " + root + " is not among its inputs: declare it (Bazel workplan P3-14: a missing root failed silently)");
        }
        try (Stream<Path> walk = Files.walk(root)) {
            for (Path f : walk.filter(p -> p.toString().endsWith(".pure"))
                    .toList()) {
                if (Files.readString(f).contains("<<PCT.test")) {
                    out.add(f);
                }
            }
        }
    }
}
