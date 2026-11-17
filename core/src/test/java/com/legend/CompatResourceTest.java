// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
                    String whole = lit.substring(1, lit.length() - 1);
                    n = (int) text.lines().filter(l -> l.equals(whole)).count();
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

    @Test
    void theResourcesHoldOnlyThePinnedSpellings() {
        Map<String, Integer> found = hits(ConstantPools.resourcesOfJars(ConstantPools.jars("legend.product.jars")));
        assertEquals(new TreeMap<>(RESOURCE_HITS), found, "a compatibility spelling in a product resource that its pin"
                + " does not list (or one more of a pinned one): move it to the edge, or pin it here with its reason; a pin"
                + " gone or fallen is lowered");
        assertEquals(RESOURCE_HITS.keySet(), REASONS.keySet(), "every pin has its reason");
        REASONS.forEach((k, why) -> assertTrue(why.matches(".*\\d{4}-\\d{2}-\\d{2}.*"), k + ": " + why));
    }

    /** A planted resource set: a literal, a whole-line pattern, a symbol as a word (and not inside a longer word). */
    @Test
    void theScanBites() {
        Map<String, Integer> found = hits(Map.of("planted.jar!/planted.pure",
                "toString(){'TDSNull'}\nCannot get \nRepr and Representation\nCannot get x\n"));
        assertEquals(Map.of("planted.jar!/planted.pure | TDSNull", 1, "planted.jar!/planted.pure | Cannot get ", 1,
                "planted.jar!/planted.pure | Repr", 1), found);
    }
}
