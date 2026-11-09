// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.testexecutionplan.JudgeJson;

import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.TestPlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@code assertJsonStringsEqual}'s texts as the judges read them (register S144): strict JSON in lite's own runs,
 * json-simple 1.1.1's grammar under the compatibility rules. Every compatibility case below is what the 1.1.1 jar
 * itself answered (probed 2026-11-07, GATES of that date): upstream's {@code equalJsonStrings} parses with it.
 */
class JudgeJsonTest {

    private static Object compat(String text) {
        return JudgeJson.read(text, TestPlan.JudgingRules.COMPILED_PURE_COMPAT);
    }

    private static Object lite(String text) {
        return JudgeJson.read(text, TestPlan.JudgingRules.LITE);
    }

    @Test
    @DisplayName("compatibility: what json-simple 1.1.1 reads past -- a trailing unterminated string, missing or extra"
            + " commas and colons, an unknown escape's backslash")
    void compatibilityReadsWhatJsonSimpleReads() {
        assertEquals(List.of(1L), compat("[1]\""));
        assertEquals(List.of(1L), compat("[1]\"abc"));
        assertEquals(List.of(1L), compat("[1] \"x"));
        assertEquals(List.of(1L, 2L), compat("[1 2]"));
        assertEquals(List.of(1L, 2L), compat("[1,,2]"));
        assertEquals(List.of(1L), compat("[,1]"));
        assertEquals(List.of(1L), compat("[1,]"));
        assertEquals(Map.of("a", 1L), compat("{\"a\" 1}"));
        assertEquals(Map.of("a", 1L), compat("{\"a\"::1}"));
        assertEquals(Map.of("a", 1L), compat("{,\"a\":1,}"));
        assertEquals(Map.of("a", 2L), compat("{\"a\":1,\"a\":2}"));
        assertEquals(List.of(Map.of("a", 1L), Map.of("b", 2L)), compat("[{\"a\":1} {\"b\":2}]"));
        assertEquals(List.of(List.of(1L), List.of(2L)), compat("[[1][2]]"));
        assertEquals(List.of("a\\x"), compat("[\"a\\x\"]"));
        assertEquals(List.of("a/b"), compat("[\"a\\/b\"]"));
        assertEquals(List.of("A"), compat("[\"\\u0041\"]"));
        assertEquals(List.of("\\u00"), compat("[\"\\u00\"]"));
        assertEquals(List.of(1L), compat("[01]"));
        assertEquals(List.of(0L), compat("[-0]"));
        assertEquals("top", compat("\"top\""));
        assertEquals(12L, compat("12"));
        assertEquals(Arrays.asList((Object) null), compat("[null]"));
        // the milestoned graph-fetch golden's shape: a complete array, then a stray quote
        assertEquals(List.of(Map.of("id", 3L)), compat("[{\"id\":3}]\""));
        // a \\u escape with digits that are not ASCII hex keeps its backslash (the milestone audit, 2026-11-07)
        assertEquals(List.of("\\u\u0660\u0660\u0664\u0661"), compat("[\"\\u\u0660\u0660\u0664\u0661\"]"));
        assertEquals(List.of("\\u\uFF10\uFF10\uFF14\uFF11"), compat("[\"\\u\uFF10\uFF10\uFF14\uFF11\"]"));
        assertEquals(List.of(Long.MAX_VALUE), compat("[9223372036854775807]"));
    }

    @Test
    @DisplayName("compatibility: what json-simple 1.1.1 refuses -- any other trailing token, a non-string key, an"
            + " unknown character, an unterminated string inside a container, nothing at all")
    void compatibilityRefusesWhatJsonSimpleRefuses() {
        for (String refused : List.of("[1] 2", "[1]]", "[1],", "[1] x", "{\"a\":1}}", "[1]\"abc\"", "[1][2]",
                "[1]\"\"", "{1:2}", "[1.]", "[.5]", "[TRUE]", "[\"abc", "", "   ",
                // the milestone audit's three (2026-11-07): a form feed, a backspace or a vertical tab is no
                // whitespace; an integer past a Long raises
                "[1,\f2]", "\f[1]", "[1,\b2]", "[1,\u000b2]", "[9223372036854775808]", "[-9223372036854775809]",
                "[1e]", "[1.5e]")) {
            assertThrows(IllegalStateException.class, () -> compat(refused), refused);
        }
    }

    @Test
    @DisplayName("lite's own runs: strict JSON -- the texts json-simple reads past are refused")
    void liteReadsStrictJson() {
        assertEquals(List.of(Map.of("a", 1L)), lite("[{\"a\":1}]"));
        for (String refused : List.of("[1]\"", "[1 2]", "[1,]", "{\"a\" 1}", "[{\"a\":1} {\"b\":2}]")) {
            assertThrows(IllegalStateException.class, () -> lite(refused), refused);
        }
    }
}
