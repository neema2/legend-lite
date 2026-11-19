// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.rcorpus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.legend.test.PureTestRunner;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * THE EDGE GOLDEN NET's PROOF, the corpus ledger's half (the review of PR #123, 2026-11-19): S0 of the presentation
 * program gave the ledger's non-PASS rows their failure's message ({@link JudgeLedger#record}), and the net pins it.
 * Planted changes through the ledger's own writer: a message change moves a FAIL row; a PASS row carries no message;
 * an id is masked by its first appearance and a value never; a message's tab and line end cannot break the row.
 */
class JudgeLedgerTest {

    private static PureTestRunner.Verdict passed(String name) {
        return new PureTestRunner.Verdict(name, true, null, null, null);
    }

    private static PureTestRunner.Verdict failed(String name) {
        return new PureTestRunner.Verdict(name, false, null, null, null);
    }

    /** The rows {@link JudgeLedger#record} writes for {@code test}, read back. */
    private static List<String> rows(Path dir, String test, String reason, PureTestRunner.Verdict... verdicts)
            throws IOException {
        Path ledger = dir.resolve("ledger-" + System.nanoTime() + ".tsv");
        String before = System.getProperty(JudgeLedger.PROPERTY);
        System.setProperty(JudgeLedger.PROPERTY, ledger.toString());
        try {
            JudgeLedger.record(test, new PureTestRunner.Result(test, PureTestRunner.Status.FAIL, List.of(verdicts),
                    reason));
        } finally {
            if (before == null) {
                System.clearProperty(JudgeLedger.PROPERTY);
            } else {
                System.setProperty(JudgeLedger.PROPERTY, before);
            }
        }
        return Files.readAllLines(ledger, StandardCharsets.UTF_8);
    }

    @Test
    void aFailRowCarriesItsMessageAndAPassRowNone(@TempDir Path dir) throws IOException {
        List<String> rows = rows(dir, "p::testX", "expected 1 but was 2", passed("assertEquals"),
                failed("assertEquals"));
        assertEquals(List.of("p::testX\t0\tassertEquals\tPASS", "p::testX\t1\tassertEquals\tFAIL\texpected 1 but was 2"),
                rows, "the message rides on the row that is not a PASS, and on no other");
    }

    @Test
    void aMessageChangeMovesTheRow(@TempDir Path dir) throws IOException {
        assertNotEquals(rows(dir, "p::testX", "expected 1234567891234567891 but was 2", failed("assertEquals")),
                rows(dir, "p::testX", "expected 1234567891234567892 but was 2", failed("assertEquals")),
                "the net sees a message's number change: a value's digits are never masked");
    }

    @Test
    void anIdIsMaskedByItsFirstAppearance() {
        String a = "3f2504e0-4f89-11d3-9a0c-0305e82c3301";
        String b = "7c9e6679-7425-40de-944b-e07fc1f90ae7";
        assertEquals("run <id1> then <id2> then <id1>", JudgeLedger.message("run " + a + " then " + b + " then " + a));
    }

    @Test
    void aMessagesTabAndLineEndCannotBreakTheRow(@TempDir Path dir) throws IOException {
        List<String> rows = rows(dir, "p::testX", "first\tsecond\nthird", failed("assertEquals"));
        assertEquals(List.of("p::testX\t0\tassertEquals\tFAIL\tfirst second\\nthird"), rows,
                "one row of five fields, the line end escaped");
    }
}
