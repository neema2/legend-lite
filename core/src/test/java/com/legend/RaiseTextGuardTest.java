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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A PRODUCT RAISE IS ITS KIND (phase D's D1.1; the orchestrator's guard, 2026-11-12): every raise a product source
 * builds -- {@code SqlFn.ERROR}'s message, a dialect's own raise helper's -- is a {@code SqlExpr.RaiseText}, never free
 * text. The lowering's helpers take one by type ({@code PureSql.raise}, {@code Scalars.guarded}); this scan holds the
 * rest. Exempt BY NAME, each with its reason ({@link #EXEMPT}): the judges' raises (the test road's own verdict and
 * identity messages), the two typed helpers' bodies, and MongoDB's pipeline raises of a constant text, which a family of
 * D1.1 still moves (shrink-only).
 */
@Tag("guardrail")
class RaiseTextGuardTest {

    private static final String MAIN = "core/src/main/java/com/legend/";

    /** A raise built: an ERROR call's message, a dialect helper's raise of a list, MongoDB's raise of a text. */
    static final Pattern RAISE = Pattern.compile(
            "(?:Call\\.of|new\\s+(?:SqlExpr\\.)?Call)\\(\\s*(?:com\\.legend\\.sql\\.)?SqlFn\\.ERROR\\s*,\\s*+"
            + "(?!new\\s+(?:SqlExpr\\.)?RaiseText\\b)"
            + "|\\braise(?:Envelope)?\\((?:\\w+,\\s*)?List\\.of\\("
            + "|\\braiseText\\(\"");

    /** By file: how many raises stand that are not a RaiseText, and why. Shrink-only; a row moves with its date. */
    static final Map<String, Integer> EXEMPT = Map.of(
            // the judges' raises (test road): the verdict row's own message column, the untracked identity's sentence
            "lowering/VerdictSql.java", 1,
            "lowering/CanonicalRenderSql.java", 1,
            // the typed helpers' bodies: their message parameter IS a SqlExpr.RaiseText (PureSql.raise,
            // Scalars.guarded)
            "lowering/PureSql.java", 2,
            "lowering/Scalars.java", 1,
            // MongoDB's pipeline raises of a constant text (lite's own sentences, VALUE_NOT_HELD's family): D1.1 moves
            // them; until then counted here
            "sql/dialect/MongoDb.java", 4);

    @Test
    void everyProductRaiseIsAKind() throws IOException {
        Map<String, Integer> found = new TreeMap<>();
        for (Path f : SourceFiles.under("core/src/main")) {
            String path = SourceFiles.path(f);
            if (!path.endsWith(".java") || path.startsWith(MAIN + "edge/")) {
                continue;
            }
            int n = count(Files.readString(f));
            if (n > 0) {
                found.put(path.startsWith(MAIN) ? path.substring(MAIN.length()) : path, n);
            }
        }
        assertEquals(new TreeMap<>(EXEMPT), found, "a product raise of free text (D1.1: raise a SqlExpr.RaiseText of its"
                + " kind; the edge words it) -- or an exemption that shrank: re-pin it, with the date");
    }

    @Test
    void theScanBitesAPlantedRaise() {
        assertEquals(3, count("""
                SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.StringLit("Cannot divide"));
                writer.piece(raise(List.of(new SqlExpr.StringLit("x"))));
                raiseText("a text");
                SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.RaiseText(RaiseKind.DIVIDE_BY_ZERO, List.of()));
                """));
        assertTrue(count("SqlExpr.Call.of(SqlFn.ERROR,\n        new RaiseText(k, ops))") == 0);
    }

    /** {@code source}'s raises of free text, its comments dropped. */
    static int count(String source) {
        String code = source.replaceAll("(?s)/\\*.*?\\*/", "").replaceAll("//[^\n]*", "");
        Matcher m = RAISE.matcher(code);
        int n = 0;
        while (m.find()) {
            n++;
        }
        return n;
    }
}
