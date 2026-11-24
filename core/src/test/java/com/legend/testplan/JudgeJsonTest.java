// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.testexecutionplan.JudgeJson;

import com.legend.executionplan.ExecutionPlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@code assertJsonStringsEqual}'s texts as the judges read them (register S144): strict JSON on every route. Until B3
 * step 2 (2026-11-24) the compatibility rules read them with json-simple 1.1.1's grammar, ported
 * ({@code edge.JsonSimpleGrammar}, its cases pinned here against the 1.1.1 jar, probed 2026-11-07); it is deleted, and the
 * two goldens it read past are cleaned at the edge's test-body door ({@code edge.UpstreamTestBodies}), pinned here.
 */
class JudgeJsonTest {

    private static Object lite(String text) {
        return JudgeJson.STRICT.read(text);
    }

    @Test
    @DisplayName("strict JSON -- the texts json-simple reads past are refused")
    void liteReadsStrictJson() {
        assertEquals(List.of(Map.of("a", 1L)), lite("[{\"a\":1}]"));
        for (String refused : List.of("[1]\"", "[1 2]", "[1,]", "{\"a\" 1}", "[{\"a\":1} {\"b\":2}]")) {
            assertThrows(IllegalStateException.class, () -> lite(refused), refused);
        }
    }

    private static final String MODEL = "###Pure\nClass t::Marker {}\n";

    @Test
    @DisplayName("the compatibility door cleans a golden's stray trailing quote (S144): the edge's run passes, lite's"
            + " own run refuses the text")
    void theDoorCleansTheStrayQuote() throws Exception {
        // the shape of legend-engine's two milestoned graph-fetch goldens: literals joined by +, a stray quote at the end
        String body = "|assertJsonStringsEqual('[{\"id\":2},' + ' {\"id\":3}]\"', '[{\"id\":2}, {\"id\":3}]')";
        String model = com.legend.testbackend.StorelessRuntime.with(MODEL, com.legend.database.Databases.PLATFORM);
        try (java.sql.Connection c = com.legend.testbackend.TestBackends.platform().open()) {
            org.junit.jupiter.api.Assertions.assertTrue(com.legend.testing.EdgeProgram.run(model, body,
                    com.legend.testbackend.StorelessRuntime.RUNTIME, c).passed());
            assertThrows(RuntimeException.class, () -> com.legend.testing.Program.run(model, body,
                    com.legend.testbackend.StorelessRuntime.RUNTIME, c));
            // a trailing empty literal: the quote is dropped from the last piece that holds it (the review of step 2's
            // L1, 2026-11-24: it was trimmed from the empty piece, past its start)
            org.junit.jupiter.api.Assertions.assertTrue(com.legend.testing.EdgeProgram.run(model,
                    "|assertJsonStringsEqual('[1]\"' + '', '[1]')", com.legend.testbackend.StorelessRuntime.RUNTIME, c)
                    .passed());
        }
    }

    @Test
    @DisplayName("the door cleans nothing else: a text strict JSON reads, and one json-simple read past for another"
            + " reason, are left as written (the judge refuses the second)")
    void theDoorCleansNothingElse() {
        com.legend.compiler.element.ModelContext ctx = com.legend.Compiler.compileModel(MODEL);
        for (String body : List.of("|assertJsonStringsEqual('[1]', '[1]')",
                "|assertJsonStringsEqual('\"a\"', '\"a\"')",
                "|assertJsonStringsEqual('[1 2]\"', '[1, 2]')",
                "|assertJsonStringsEqual('[1]' + '\"x', '[1]')")) {
            com.legend.TypedQuery q = com.legend.Compiler.query(ctx, body);
            org.junit.jupiter.api.Assertions.assertSame(q, com.legend.edge.UpstreamTestBodies.admitted(q), body);
        }
    }
}
