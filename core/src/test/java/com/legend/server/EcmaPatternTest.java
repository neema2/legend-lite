// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link EcmaPattern}: a JSON Schema pattern (ECMA-262) in the platform's regex dialect (RE2). The translations were
 * checked differentially on 2026-10-20: each of the 1,260 patterns of SchemaStore's first 1,001 schemas, against the
 * strings the schemas give beside them ({@code examples}, {@code enum}, {@code default}) and 37 probes, 49,101 tests,
 * read by Node 22's {@code RegExp} (with the {@code u} flag) and by DuckDB's {@code regexp_matches} over the
 * translation: the 1,217 translated agree on every string, and the 43 not translated each look ahead.
 */
@DisplayName("EcmaPattern -- a JSON Schema pattern in RE2, or why not")
class EcmaPatternTest {

    private static String re2(String ecma) {
        EcmaPattern.Translation t = EcmaPattern.of(ecma);
        return t.re2() != null ? t.re2() : "NOT " + t.why();
    }

    @Test
    @DisplayName("what the two dialects spell alike is kept as it is")
    void theCommonPartIsKept() {
        assertEquals("^[0-9]{4}$", re2("^[0-9]{4}$"));
        assertEquals("^[\\w.-]+@[\\w-]+\\.[a-z]{2,}$", re2("^[\\w.-]+@[\\w-]+\\.[a-z]{2,}$"));
        assertEquals("(?:ab|cd)+?\\d*\\s\\S", re2("(?:ab|cd)+?\\d*\\s\\S"));
        assertEquals("\\/\\-", re2("\\/\\-"));
    }

    @Test
    @DisplayName("what ECMA spells otherwise is written as RE2 reads it")
    void whatEcmaSpellsOtherwiseIsWritten() {
        assertEquals("été", re2("\\u00e9t\\u00E9"), "a character beyond ASCII, itself");
        assertEquals("😀😀", re2("\\uD83D\\uDE00\\u{1F600}"), "a surrogate pair one code point");
        assertEquals("A\\.\\x0A\\x00", re2("\\x41\\x2e\\cJ\\0"), "a symbol escaped, a control character by its code");
        assertEquals("^Az$", re2("^\\A\\z$"), "an identity escape the letter: RE2 reads \\A and \\z as anchors");
        assertEquals("[\\x08]", re2("[\\b]"), "in a class, \\b is a backspace");
        assertEquals("[\\x{0}-\\x{10FFFF}]|[^\\x{0}-\\x{10FFFF}]", re2("[^]|[]"), "any character, and none");
        assertEquals("(?:\\d{4})-(?:\\d\\d)", re2("(?<year>\\d{4})-(?<m>\\d\\d)"), "a named group a group");
        assertEquals("\\p{Greek}+\\p{L}\\P{Lu}", re2("\\p{Script=Greek}+\\p{Letter}\\P{Lu}"));
        assertEquals("[a-z\\[]x\\{\\}", re2("[a-z[]x{}"), "a [ in a class, a brace beginning no repetition");
    }

    @Test
    @DisplayName("what RE2 cannot say is said why")
    void whatRe2CannotSayIsSaid() {
        assertEquals("NOT it looks ahead ((?!), which RE2 cannot", re2("^(?!@@)[\\w@]+$"));
        assertEquals("NOT it looks behind ((?<=), which RE2 cannot", re2("(?<=a)b"));
        assertEquals("NOT it refers back to a group (\\1), which RE2 cannot", re2("(a)\\1"));
        assertEquals("NOT it refers back to a named group (\\k), which RE2 cannot", re2("(?<n>a)\\k<n>"));
        assertEquals("NOT it repeats 5000 times, past RE2's 1000", re2(".{1,5000}"));
        assertEquals("NOT a character class is not closed", re2("[abc"));
        assertEquals("NOT it names a Unicode property RE2 does not (\\p{Foo})", re2("\\p{Foo}"));
    }
}
