// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * THE PROOF that the test runner is product surface (upstream boundary batch
 * 7a): a five-line model with two {@code <<test.Test>>} functions and one
 * {@code <<test.BeforePackage>>} setup, discovered and run through the runner
 * against an in-memory DuckDB — no engine checkout, no corpus, no harness.
 * One pass, one fail with the failed assert named, one excluded-by-mark test
 * reported and not run.
 */
class PureTestRunnerTest {

    private static final String MODEL = """
            function <<test.BeforePackage>> t::setUp(): Boolean[1]
            {
              true;
            }
            function <<test.Test>> t::passes(): Boolean[1]
            {
              assertEquals(2, 1 + 1);
            }
            function <<test.Test>> t::fails(): Boolean[1]
            {
              assertEquals(3, 1 + 1);
            }
            function <<test.Test, test.ToFix>> t::later(): Boolean[1]
            {
              assertEquals(1, 1);
            }
            """;

    @Test
    @DisplayName("a user's model: discovered, ordered, run — one pass, one named failure, one mark reported")
    void discoversAndRunsAUsersTests() throws Exception {
        Compiler.ParsedModule parsed = Compiler.parseSources(
                List.of(new Compiler.ModelSource("t.pure", MODEL)));
        ModelContext ctx = Compiler.buildModule(parsed.model()).context();
        PureTests.Discovery found = PureTests.discover(parsed.model(), Set.of());
        assertEquals(List.of("t::fails", "t::later", "t::passes"),
                found.tests().stream().map(PureTests.TestCase::fqn).toList());
        assertEquals(List.of("t::fails", "t::passes"),
                found.runnable().stream().map(PureTests.TestCase::fqn).toList(),
                "ToFix is REPORTED, and the caller (here: the runnable view) skips it");
        assertEquals(Map.of("t", List.of("t::setUp")), found.setupsByPackage());
        try (PureTestRunner runner = new PureTestRunner(ctx, null,
                () -> DriverManager.getConnection("jdbc:duckdb:"),
                List.of(), found.setupsByPackage(), TestObserver.NONE)) {
            PureTestRunner.Result fails = runner.run(found.runnable().get(0));
            PureTestRunner.Result passes = runner.run(found.runnable().get(1));
            assertEquals(PureTestRunner.Status.PASS, passes.status(), passes.reason());
            assertEquals(1, passes.verdictCount());
            assertEquals(PureTestRunner.Status.FAIL, fails.status());
            // the platform raises the assert's own failure (expected/actual, whole,
            // on one line); that IS the reason — never truncated, never re-derived
            assertTrue(fails.reason().startsWith("AssertFailed: expected: 3 | actual:") && fails.reason().contains("2"),
                    "the failed assert's own message is the reason: " + fails.reason());
            assertEquals(Set.of("t::setUp"), runner.inertSetups(),
                    "a setup with no statement effects is derived inert and never runs");
        }
    }
}
