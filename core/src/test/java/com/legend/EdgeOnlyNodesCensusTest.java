// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.sql.SqlFn;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE EDGE-ONLY NODES' CENSUS (the separation census's B8, docs/records/f-separation-census-2026-11-17.md, 2026-11-17):
 * the SqlFn members whose text is legend-pure's or legend-engine's -- the lowering writes the node, the product's
 * renderer refuses it by name or writes its own form, and the compatibility edge spells upstream's text. Two halves:
 * <ol>
 *   <li>WHERE they are written: each member's emitting sites in the product's code (comments and literals stripped,
 *   the SQL layer and the edge left out: the renderers' arms are the second half), pinned exactly both ways. A new
 *   site is a new place a compatibility form enters the product; a site that goes is a re-pin, dated.</li>
 *   <li>WHAT the product writes for each, on every product dialect: a refusal by name (legend-pure's text the product
 *   does not write), or the product's own form, never upstream's text.</li>
 * </ol>
 * A finding is a named, dated entry on the census line, not an allowlist.
 */
@Tag("guardrail")
class EdgeOnlyNodesCensusTest {

    private static final Path MAIN = Path.of("core/src/main/java/com/legend");

    /** The edge-only members (FLOAT_PRINT was renamed FLOAT_SHORTEST_TEXT, a product node, 2026-11-17, census B9). */
    static final List<SqlFn> EDGE_ONLY = List.of(SqlFn.DATETIME_PRINT);

    /** Each member's emitting sites, "file (under core/src/main/java/com/legend)" -> occurrences in code, measured on
     *  d-stage 885328d0 (2026-11-17, the census's B8); a member with no product site has no entry. */
    static final Map<SqlFn, Map<String, Integer>> EMITTERS = Map.of(
            // legend-pure's DateTime print: the judges' (a literal's written text kept), its product rendering the
            // judges' too (CompatSpellingGuardTest's DateTimePrintText JUDGE row) until Phases 4b and 5
            // 2026-11-17 (census B9): a judge's message's DateTime text is the two-form hook DateTimeTexts; the two
            // product emitters left are LiteralSpelling's (the judges' canon, C5, E-P5's step C) and Scalars' (format's
            // %r, refused in the product by census B12's 2b, F's: the user's §10 item 8)
            // LiteralSpelling's gone 2026-11-17 (census C5's step C: the canon is lite's spelling)
            SqlFn.DATETIME_PRINT, Map.of("lowering/Scalars.java", 1));
            // (FORMAT_PADDED: gone from SqlFn 2026-11-17, B8 (i) -- the edge composes it at lowering)
            // (INSTANCE_TEXT: gone from SqlFn 2026-11-17, B8 -- the edge writes it at lowering, Scalars.pureToString)
            // (REPRESENTATION_TEXT: gone from SqlFn 2026-11-17, B8 (i) -- the edge writes it at lowering)
            // (RELATION_TEXT: gone from SqlFn 2026-11-17, B8 -- the edge's RelationTextLowering returns the text
            // itself; the product refuses through the RelationTexts facet)
            // (TEXT_FORMS: gone from SqlFn 2026-11-17, B8 -- a list's text is the two-form hook ListTexts, the
            // product's form ProductListTexts, the edge's PureListTexts; HookRulingTest pins it)
            // (TRACE_TEXT: gone from SqlFn 2026-11-17, B8 (ii) -- the plan style's traceText, HookRulingTest)

    @Test
    void eachEdgeOnlyNodeIsWrittenAtItsPinnedSitesAlone() throws IOException {
        Map<SqlFn, Map<String, Integer>> found = new TreeMap<>();
        List<Path> files;
        try (var walk = Files.walk(MAIN)) {
            files = walk.filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> !MAIN.relativize(p).startsWith("edge") && !MAIN.relativize(p).startsWith("sql"))
                    .sorted().toList();
        }
        GuardCoverage.assertFloor("EdgeOnlyNodesCensusTest", files.size(), 500);
        for (Path f : files) {
            String code = code(Files.readString(f));
            for (SqlFn fn : EDGE_ONLY) {
                Matcher m = Pattern.compile("\\b" + fn.name() + "\\b").matcher(code);
                int n = 0;
                while (m.find()) {
                    n++;
                }
                if (n > 0) {
                    found.computeIfAbsent(fn, k -> new TreeMap<>()).put(MAIN.relativize(f).toString(), n);
                }
            }
        }
        assertEquals(new TreeMap<>(EMITTERS.entrySet().stream().collect(java.util.stream.Collectors.toMap(
                Map.Entry::getKey, e -> new TreeMap<>(e.getValue())))), found,
                "an edge-only node's emitting sites moved: a new site is a compatibility form entering the product "
                        + "(a census line, dated), a gone site a re-pin");
    }

    /** {@code source} with its comments and its string and char literals' contents removed: a linear scan (a regex over a
     *  long file's literals overflows the stack). Text blocks are literals too. */
    static String code(String source) {
        StringBuilder out = new StringBuilder(source.length());
        int i = 0;
        int n = source.length();
        while (i < n) {
            char c = source.charAt(i);
            if (c == '/' && i + 1 < n && source.charAt(i + 1) == '/') {
                while (i < n && source.charAt(i) != '\n') {
                    i++;
                }
            } else if (c == '/' && i + 1 < n && source.charAt(i + 1) == '*') {
                int end = source.indexOf("*/", i + 2);
                i = end < 0 ? n : end + 2;
            } else if (source.startsWith("\"\"\"", i)) {
                int end = source.indexOf("\"\"\"", i + 3);
                i = end < 0 ? n : end + 3;
                out.append("\"\"");
            } else if (c == '"' || c == '\'') {
                i++;
                while (i < n && source.charAt(i) != c) {
                    i += source.charAt(i) == '\\' ? 2 : 1;
                }
                i++;
                out.append(c).append(c);
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    /** The scan reads code alone: a mention in a comment or a literal is no site, a use in code is. */
    @Test
    void theScanCountsCodeNotCommentsOrLiterals() {
        String planted = "// SqlFn.FORMAT_PADDED in a comment\n/* INSTANCE_TEXT */ String s = \"RELATION_TEXT\";\n"
                + "SqlExpr e = SqlExpr.Call.of(SqlFn.TRACE_TEXT, x); char q = '\\'';\n";
        String code = code(planted);
        assertFalse(code.contains("FORMAT_PADDED") || code.contains("INSTANCE_TEXT") || code.contains("RELATION_TEXT"),
                code);
        assertTrue(code.contains("SqlFn.TRACE_TEXT"), code);
    }

    // theProductRefusesLegendPuresTextsByName and theProductWritesItsOwnFormNeverUpstreamsText went 2026-11-17 with
    // RELATION_TEXT and TEXT_FORMS (B8): no dialect meets either now; the product's refusal of a relation's text and
    // its own list text are HookRulingTest's (the RelationTexts facet, the ListTexts two-form kind).
}
