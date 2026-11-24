// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R2 AND R4 OF ITEM 3 (docs/records/compat-assert-records-2026-11-21.md section 6, S5, 2026-11-23): JAVA PRINTS A
 * REPORT, NEVER A VALUE THE PROGRAM READS. Every Java site in the test planner, the test runner and the test plan's
 * library ({@code testplan}, {@code testexec}, {@code testexecutionplan}) that makes a text from a value --
 * {@code String.valueOf}, {@code toString()}, {@code String.format}, {@code formatted} -- in comment- and
 * string-stripped source, counted per file and pinned. The catalog's renderer ({@code error.AssertKind}) is outside
 * these packages; its value printer ({@code testexec.LiteWords}) and the records' builder
 * ({@code testexec.AssertRecords}) are the sites expected here, pinned like the rest. The census SHRINKS ONLY: a count
 * that grows fails (a new site is a value printed in Java -- the database's, or the record's printer's), one that
 * shrinks asks for its pin to move down with a dated note. Measured 2026-11-23 at c2217801: 37 sites in 14 files.
 */
@Tag("census")
class ValueTextCensusTest {

    private static final List<String> PACKAGES = List.of("core/src/main/java/com/legend/testplan",
            "core/src/main/java/com/legend/testexec", "core/src/main/java/com/legend/testexecutionplan");

    private static final Pattern SITE = Pattern.compile(
            "String\\.valueOf\\(|\\.toString\\(\\)|String\\.format\\(|\\.formatted\\("
                    // the audit of 8477c703: the spellings the design's section 6 names beside these -- a number's or a
                    // value's text by its class's toString, a join of texts, a Decimal's plain digits
                    + "|\\b(?:Double|Long|Integer|Float|Boolean|Short|Byte|Character|BigInteger|BigDecimal)\\.toString\\("
                    + "|String\\.join\\(|\\.toPlainString\\(\\)");

    /** The pins: each file's sites (a file absent holds none). Measured 2026-11-23 at c2217801; RE-MEASURED 2026-11-23
     *  (the audit of 8477c703) with the census made to see more -- a correct scan of character literals and the
     *  design's other spellings ({@code Double.toString(x)} and its kin, {@code String.join}, {@code toPlainString()}):
     *  the counts that rose are sites that were there, now seen, not code added; the census shrinks only from here. */
    private static final Map<String, Integer> PINS = Map.ofEntries(
            Map.entry("core/src/main/java/com/legend/testexec/LiteWords.java", 15),
            // 13 -> 9 (2026-11-23, register S401 ruled by the user, "Okay": two whole Variants refused at planning, so
            // the two-Variant record and its joins went)
            Map.entry("core/src/main/java/com/legend/testexec/AssertRecords.java", 9),
            Map.entry("core/src/main/java/com/legend/testexec/CanonicalForm.java", 9),
            Map.entry("core/src/main/java/com/legend/testexec/Equality.java", 8),
            Map.entry("core/src/main/java/com/legend/testplan/JudgePlans.java", 4),
            Map.entry("core/src/main/java/com/legend/testplan/HostJudge.java", 3),
            Map.entry("core/src/main/java/com/legend/testplan/DatabaseAsserts.java", 2),
            Map.entry("core/src/main/java/com/legend/testplan/AssertVerdicts.java", 1),
            Map.entry("core/src/main/java/com/legend/testplan/BodyKinds.java", 1),
            Map.entry("core/src/main/java/com/legend/testplan/DatabaseJudge.java", 1),
            Map.entry("core/src/main/java/com/legend/testplan/JudgmentFacts.java", 1),
            Map.entry("core/src/main/java/com/legend/testplan/TestKinds.java", 1),
            Map.entry("core/src/main/java/com/legend/testexec/AssertRuns.java", 1),
            Map.entry("core/src/main/java/com/legend/testexec/HostJudgments.java", 1),
            Map.entry("core/src/main/java/com/legend/testexec/LiteHostForms.java", 1),
            Map.entry("core/src/main/java/com/legend/testexec/RootLoops.java", 1),
            Map.entry("core/src/main/java/com/legend/testexecutionplan/TestPlanJson.java", 1));

    /** {@code src} with its comments and its string and character literals' contents removed (a literal names no
     *  site): the scan LiteralJoinCensusTest reads by (the audit of 8477c703: a pattern took the char literal
     *  {@code '"'} for a string's start and swallowed code after it). */
    static String stripped(String src) {
        return LiteralJoinCensusTest.stripped(src);
    }

    static Map<String, Integer> measure() throws IOException {
        Map<String, Integer> out = new TreeMap<>();
        for (String pkg : PACKAGES) {
            for (Path f : SourceFiles.in(pkg)) {
                if (!f.toString().endsWith(".java")) {
                    continue;
                }
                Matcher m = SITE.matcher(stripped(Files.readString(f)));
                int n = 0;
                while (m.find()) {
                    n++;
                }
                if (n > 0) {
                    out.put(SourceFiles.path(f), n);
                }
            }
        }
        return out;
    }

    @Test
    void valueTextSitesShrinkOnly() throws IOException {
        Map<String, Integer> measured = measure();
        StringBuilder drift = new StringBuilder();
        for (var e : measured.entrySet()) {
            int pin = PINS.getOrDefault(e.getKey(), 0);
            if (e.getValue() > pin) {
                drift.append("\n  ").append(e.getKey()).append(": ").append(pin).append(" -> ").append(e.getValue())
                        .append(" sites GREW (a value printed in Java: the database's to compute, or the record"
                                + " printer's, LiteWords, to spell)");
            }
        }
        for (var p : PINS.entrySet()) {
            int now = measured.getOrDefault(p.getKey(), 0);
            if (now < p.getValue()) {
                drift.append("\n  ").append(p.getKey()).append(": ").append(p.getValue()).append(" -> ").append(now)
                        .append(" sites shrank: move the pin down, with a dated note");
            }
        }
        assertTrue(drift.length() == 0, "the value-text census moved:" + drift);
    }

    /** The census reads: a planted site in a planted source is counted, a comment and a string literal are not. */
    @Test
    void theCensusCountsCodeAlone() {
        String planted = """
                class X {
                    // String.valueOf(x) in a comment
                    /* x.toString() in a block */
                    String a(Object x) { return "String.format(" + String.valueOf(x) + x.toString(); }
                    char q() { return '"'; }   // a character literal that is a quote, then code the census still reads
                    String b(double d) { return Double.toString(d) + String.join(",", "a"); }
                }
                """;
        Matcher m = SITE.matcher(stripped(planted));
        int n = 0;
        while (m.find()) {
            n++;
        }
        assertTrue(n == 4, "four sites in code -- after a quote's character literal too -- none in the comments or the"
                + " literal: " + n);
    }
}
