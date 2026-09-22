// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F1.11 — PCT's best property, pinned (audit §4.3.1, verbatim: "There
 * is no comparison logic in Java anywhere in the PCT module... A 'fix'
 * that moved comparison into Java to dodge the rendering problem would
 * be far worse than what is there"). assertEquals stays in interpreted
 * Pure with both sides in Pure's own value domain; row order is the
 * database's. Zero sort/dedupe/tolerance spellings in this module;
 * stays zero.
 */
class PctDisciplineTest {

    private static final Pattern SITE = Pattern.compile(
            "Collections\\.sort\\(|\\.sorted\\(|\\.distinct\\(\\)"
            + "|\\.sort\\(|Math\\.abs\\(.*<|new TreeSet|new TreeMap"
            + "|Collections\\.max\\(|Collections\\.min\\("
            + "|new PriorityQueue|\\.stream\\(\\)\\.max\\("
            + "|\\.stream\\(\\)\\.min\\(");

    /** The same discipline for the module's PURE sources — the audit's
     * theater #9: the walk visited the directory holding
     * {@code pct_adapter.pure} and excluded it one line later with
     * {@code .endsWith(".java")}, so the ONE guard in the module could
     * not see the file holding finding G. Adapter-side reordering or
     * dedup would dodge a rendering problem exactly the same way. */
    private static final Pattern PURE_SITE = Pattern.compile(
            "->sort\\(|->distinct\\(|->removeDuplicates\\(");

    /** pct_adapter.pure size pin (the eval-ledger convention): the
     * adapter is the CONTRACT surface — growth is new adapter
     * compensation and needs a pin bump with a written justification.
     * Seeded 2026-08-18 (audit finding G resolution: dead widening
     * deleted, exactness relabel documented in place).
     * 320 → 400 (2026-08-27, R1 — ADAPTER_NECESSITY_CENSUS §5b): the
     * semantic dependency collector moved INTO pure (the M3 walk that
     * replaces the Java side's five discovery regexes — deleted in the
     * same commit). The growth is ANTI-compensation: discovery now
     * reads the tree the interpreter holds instead of pattern-guessing
     * printed text; the adapter PAIR shrinks net. (410: the collector's
     * cycle-guard block — differential run 1's 23 stack overflows on
     * cyclic captured-instance graphs, cured by chain-seen breaking.)
     * 410 → 485 (B9 upgrade, user-directed): captured lambda VALUES
     * close by construction (recursive environment resolution) and the
     * capture-safety proof gained its EXECUTABLE WALL (assertClosed —
     * the free-variable postcondition checked on every test). Wall
     * growth; the proof stopped being testimony. */
    private static final int ADAPTER_MAX_LINES = 485;

    @Test
    void noJavaSideComparisonInPct() throws IOException {
        List<String> bad = new ArrayList<>();
        int javaScanned = 0;
        int pureScanned = 0;
        try (Stream<Path> files = Files.walk(com.legend.testing.Repo.module("src"))) {
            for (Path f : files
                    .filter(p -> p.toString().endsWith(".java")
                            || p.toString().endsWith(".pure"))
                    .filter(p -> !p.getFileName().toString()
                            .equals("PctDisciplineTest.java"))
                    .toList()) {
                boolean pure = f.toString().endsWith(".pure");
                if (pure) {
                    pureScanned++;
                } else {
                    javaScanned++;
                }
                String src = Files.readString(f);
                Matcher m = (pure ? PURE_SITE : SITE).matcher(src);
                while (m.find()) {
                    bad.add(f.getFileName() + ": " + m.group());
                }
                if (f.getFileName().toString().equals("pct_adapter.pure")) {
                    long lines = src.lines().count();
                    assertTrue(lines <= ADAPTER_MAX_LINES,
                            "pct_adapter.pure grew to " + lines
                            + " lines (pinned " + ADAPTER_MAX_LINES
                            + ") — new adapter compensation needs a"
                            + " deliberate pin bump with a written"
                            + " justification (audit finding G)");
                }
            }
        }
        // audit item 7: the guard asserts its own coverage
        assertTrue(javaScanned >= 9 && pureScanned >= 3,
                "PctDisciplineTest coverage DROPPED: scanned "
                + javaScanned + " java + " + pureScanned + " pure files"
                + " (floors 9/3) — the walk root or filters rotted");
        assertTrue(bad.isEmpty(),
                "comparison/ordering machinery appeared in the"
                + " PCT module: " + bad + " — comparison stays in"
                + " interpreted Pure (audit §4.3.1); do not dodge a"
                + " rendering problem by comparing or reordering"
                + " elsewhere");
    }
}
