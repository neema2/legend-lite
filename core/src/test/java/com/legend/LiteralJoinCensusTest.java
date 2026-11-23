// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * RULE 4 OF ITEM 3, Q-D (item 3's S7, 2026-11-23; the user's Q-D of 2026-11-21 and Q-S7a/Q-S7b of 2026-11-22): JAVA
 * NEVER COMPUTES A VALUE THE PROGRAM COMPARES. The compiler's join of literals ({@code Literals.joined}) serves the
 * ruled carve-outs alone, and every site that takes it is pinned here, per file, exactly:
 * <ul>
 *   <li>an effect's SQL or DDL text, sent as written: {@code TestKinds} (1), {@code BodyClass} (2: the effect's rule
 *   and a let a later effect reads), {@code Seeds} (4: the DDL texts, their source, the effect mapper, the seed
 *   form);</li>
 *   <li>a seed's CSV and an effect's argument read while the body is planned: {@code BodyDriver.evalStringArg};</li>
 *   <li>the test-data generator's load text and a generator comparison's caller message: {@code GeneratorSplice}
 *   (2);</li>
 *   <li>a caller's failure message, a final print: {@code DatabaseJudge.plannedMessage} (1), and the host judge's
 *   message ask, {@code HostSides.Fold.MESSAGE} in {@code HostJudge} (1), which {@code BodyDriver.foldedSide} (1)
 *   joins;</li>
 *   <li>the compatibility edge's SQL-text and plan-text verdicts, a TEMPORARY carve-out until B3 (Q-S7a):
 *   {@code HostSides.Fold.EDGE_TEXT}, at the edge's four doors in {@code EngineTextPlanning} (4).</li>
 * </ul>
 * A compared side takes the program's tokens ({@code Literals.tokens}, {@code HostSides.Fold.TOKENS}) or is planned
 * for the database. A new site grows a count and fails: it is a ruling first. A site gone asks for its pin to move
 * down, with a dated note. {@code Literals.fold} itself is private; the one other join of literals,
 * {@code ContextReading}'s folds of a connection's URL and its test-data setup texts, is counted here too.
 */
@Tag("census")
class LiteralJoinCensusTest {

    private static final Pattern SITE = Pattern.compile(
            "Literals\\.joined\\(|Literals::joined|Fold\\.MESSAGE\\b|Fold\\.EDGE_TEXT\\b"
                    // (the audit of 8477c703) the edge's calls of its fold doors, whose true is EDGE_TEXT; and
                    // ContextReading's own literal folds, a connection's URL and its test-data setup texts
                    + "|EngineTextPlanning\\.(?:ask|evalSide|plannedSide)\\(|\\bfold(?:Raw)?Literal\\(");

    /** The pins, measured 2026-11-23 at item 3's S7 (a file absent holds none). */
    private static final Map<String, Integer> PINS = Map.ofEntries(
            Map.entry("core/src/main/java/com/legend/testplan/TestKinds.java", 1),
            Map.entry("core/src/main/java/com/legend/testplan/BodyClass.java", 2),
            Map.entry("core/src/main/java/com/legend/testplan/Seeds.java", 4),
            Map.entry("core/src/main/java/com/legend/testplan/BodyDriver.java", 2),
            Map.entry("core/src/main/java/com/legend/compiler/spec/GeneratorSplice.java", 2),
            Map.entry("core/src/main/java/com/legend/testplan/DatabaseJudge.java", 1),
            Map.entry("core/src/main/java/com/legend/testplan/HostJudge.java", 1),
            Map.entry("core/src/main/java/com/legend/testplan/EngineTextPlanning.java", 4),
            // (the audit of 8477c703, 2026-11-23) the edge's five calls of the fold doors (four with true, the edge's
            // text sides; one passing the collector's flag on), and ContextReading's literal folds of a connection's
            // URL and its test-data setup SQL and CSV -- seed and setup texts, Q-D's carve-outs (13: the two folds'
            // definitions, their recursion and their calls)
            Map.entry("core/src/main/java/com/legend/edge/SqlTextVerdicts.java", 5),
            Map.entry("core/src/main/java/com/legend/compiler/spec/typed/ContextReading.java", 13));

    /** {@code src} with its comments and its string literals' contents (text blocks included) removed: a scan, never
     *  a backtracking pattern (a pattern's repetition overflows the stack on a long literal). */
    static String stripped(String src) {
        StringBuilder out = new StringBuilder(src.length());
        int i = 0;
        while (i < src.length()) {
            char c = src.charAt(i);
            if (src.startsWith("//", i)) {
                while (i < src.length() && src.charAt(i) != '\n') {
                    i++;
                }
            } else if (src.startsWith("/*", i)) {
                int end = src.indexOf("*/", i + 2);
                i = end < 0 ? src.length() : end + 2;
            } else if (src.startsWith("\"\"\"", i)) {
                int end = src.indexOf("\"\"\"", i + 3);
                out.append("\"\"");
                i = end < 0 ? src.length() : end + 3;
            } else if (c == '"' || c == '\'') {
                int j = i + 1;
                while (j < src.length() && src.charAt(j) != c) {
                    j += src.charAt(j) == '\\' ? 2 : 1;
                }
                out.append(c).append(c);
                i = j + 1;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    static int count(String source) {
        Matcher m = SITE.matcher(stripped(source));
        int n = 0;
        while (m.find()) {
            n++;
        }
        return n;
    }

    @Test
    void theLiteralJoinsServeTheCarveOutsAlone() throws IOException {
        Map<String, Integer> measured = new TreeMap<>();
        for (Path f : SourceFiles.under("core/src/main/java")) {
            if (!f.toString().endsWith(".java")) {
                continue;
            }
            int n = count(Files.readString(f));
            if (n > 0) {
                measured.put(SourceFiles.path(f), n);
            }
        }
        assertEquals(new TreeMap<>(PINS), measured, "the literal joins' sites moved: a new one is a ruling first (the"
                + " user's Q-D: a compared constant is the database's to compute); one gone moves its pin down, dated");
    }

    /** The census reads: a planted site in code is counted, a comment and a string literal are not. */
    @Test
    void theCensusCountsCodeAlone() {
        String planted = """
                class X {
                    // Literals.joined(e, lets) in a comment
                    Object a(Object e) { return "Fold.MESSAGE" + Literals.joined(e, null) + HostSides.Fold.EDGE_TEXT; }
                }
                """;
        assertEquals(2, count(planted), "two sites in code, none in the comment or the literal");
    }
}
