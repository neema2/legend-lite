// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE COMPATIBILITY SPELLINGS IN THE PRODUCT'S RESOURCES (the GUARD stream, step 2's C.8, its resource half; the edge
 * audit of 2026-11-17, section C.8: "scans of resources ..."; found missing by the independent review of C.6, which read
 * classes alone). The product's jars carry text no Java scan reads: the platform's Pure prelude, the builtin tables, the
 * service files. Each resource of {@code :product_jars} is matched against {@link CompatSpellingGuardTest}'s vocabulary
 * -- a {@link CompatSpellingGuardTest#LITERALS literal} as a substring (a {@code ^...$} one as a whole line), a
 * {@link CompatSpellingGuardTest#SYMBOLS symbol} as a whole word -- and every hit is pinned with its count, exact
 * ({@link #RESOURCE_HITS}), with its reason: a new spelling in a resource, or one more of a pinned one, fails.
 */
@Tag("guardrail")
class CompatResourceTest {

    /** Every hit, "jar!/resource | spelling" -> its count, with its reason beside it. Measured 2026-11-17. */
    static final Map<String, Integer> RESOURCE_HITS = Map.ofEntries(
            Map.entry("libbuiltin.jar!/com/legend/builtin/prelude.pure | TDSNull", 5));

    /** Each pin's reason, by the same key. */
    static final Map<String, String> REASONS = Map.ofEntries(
            // four are Pure's class itself (LANGUAGE, C5 step C, C-2): its declaration, isNull's ^TDSNull(), a comment
            // and a cross-reference comment; the fifth is its toString(){'TDSNull'} body -- legend-engine's text of an
            // empty TDS cell (census B10's spelling) in the platform's prelude, a Pure body the platform ships: found by
            // this scan 2026-11-17, reported to the orchestrator for its census line, OPEN until moved or ruled
            Map.entry("libbuiltin.jar!/com/legend/builtin/prelude.pure | TDSNull", "4 LANGUAGE + 1 OPEN (toString body,"
                    + " legend-engine's empty-cell text), found and reported 2026-11-17"));

    /** The hits of {@code resources} (key -> text), "key | spelling" -> count. */
    static Map<String, Integer> hits(Map<String, String> resources) {
        Map<String, Integer> out = new TreeMap<>();
        resources.forEach((key, text) -> {
            for (String lit : CompatSpellingGuardTest.LITERALS) {
                int n = 0;
                if (lit.startsWith("^") && lit.endsWith("$")) {
                    // a whole literal, as the source guard reads it: the whole of a quoted string ('...' in Pure, "..."
                    // elsewhere) or the whole of a table's cell (the review of 139f03aba2, 2026-11-17: never a whole line,
                    // which an indented, quoted literal never is)
                    String whole = lit.substring(1, lit.length() - 1);
                    for (String quoted : List.of("'" + whole + "'", "\"" + whole + "\"")) {
                        for (int at = text.indexOf(quoted); at >= 0; at = text.indexOf(quoted, at + quoted.length())) {
                            n++;
                        }
                    }
                    n += (int) text.lines().flatMap(l -> java.util.Arrays.stream(l.split("\t", -1)))
                            .filter(cell -> cell.equals(whole)).count();
                } else {
                    for (int at = text.indexOf(lit); at >= 0; at = text.indexOf(lit, at + lit.length())) {
                        n++;
                    }
                }
                if (n > 0) {
                    out.put(key + " | " + CompatConstantPoolTest.spelling(lit), n);
                }
            }
            for (String sym : CompatSpellingGuardTest.SYMBOLS) {
                int n = (int) java.util.regex.Pattern.compile("(?<![A-Za-z0-9_])" + java.util.regex.Pattern.quote(sym)
                        + "(?![A-Za-z0-9_])").matcher(text).results().count();
                if (n > 0) {
                    out.merge(key + " | " + sym, n, Integer::sum);
                }
            }
        });
        return out;
    }

    /** What the scan must at least read (the review of 139f03aba2, 2026-11-17: with no hit left, an empty read would pass):
     *  the product jars' resources today number 6, the prelude among them. */
    static final int RESOURCE_FLOOR = 6;

    @Test
    void theResourcesHoldOnlyThePinnedSpellings() {
        Map<String, String> resources = ConstantPools.resourcesOfJars(ConstantPools.jars("legend.product.jars"));
        assertTrue(resources.size() >= RESOURCE_FLOOR && resources.keySet().stream().anyMatch(k -> k.endsWith(
                "!/com/legend/builtin/prelude.pure")), "the scan read the product's resources: " + resources.keySet());
        Map<String, Integer> found = hits(resources);
        assertEquals(new TreeMap<>(RESOURCE_HITS), found, "a compatibility spelling in a product resource that its pin"
                + " does not list (or one more of a pinned one): move it to the edge, or pin it here with its reason; a pin"
                + " gone or fallen is lowered");
        assertEquals(RESOURCE_HITS.keySet(), REASONS.keySet(), "every pin has its reason");
        REASONS.forEach((k, why) -> assertTrue(why.matches(".*\\d{4}-\\d{2}-\\d{2}.*"), k + ": " + why));
    }

    /** A planted resource set: a literal, a whole-literal pattern as a quoted string in Pure and as a table's cell (not as
     *  part of a longer string), a symbol as a word (and not inside a longer word). */
    @Test
    void theScanBites() {
        Map<String, Integer> found = hits(Map.of("planted.jar!/planted.pure",
                "toString(){'TDSNull'}\n    fail('Cannot get ');\nRepr and Representation\n'Cannot get x'\n",
                "planted.jar!/planted.tsv", "a\tCannot get \tb\n"));
        assertEquals(Map.of("planted.jar!/planted.pure | TDSNull", 1, "planted.jar!/planted.pure | Cannot get ", 1,
                "planted.jar!/planted.pure | Repr", 1, "planted.jar!/planted.tsv | Cannot get ", 1), found,
                "a whole literal is a whole quoted string or a whole cell, never part of one");
    }
}
