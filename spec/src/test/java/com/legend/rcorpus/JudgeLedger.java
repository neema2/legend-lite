// SPDX-License-Identifier: Apache-2.0

package com.legend.rcorpus;

import com.legend.test.PureTestRunner;
import com.legend.testing.Runfile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/** THE PER-ASSERT VERDICT LEDGER (leg 3.3, docs/DATABASE_MODE_HOMEWORK_2026_09_18.md §4x):
 * one row per adjudicated assert of every test a corpus lane ran, in statement
 * order — {@code test, ordinal, assert family, verdict} — written by a lane run
 * with {@code -Dlegend.judge.ledger=<path>} and read back by the differential
 * gate, which joins a HOST-mode ledger against a DATABASE-mode ledger on
 * {@code (test, ordinal)}. The verdict vocabulary: {@code PASS}; {@code FAIL};
 * {@code UNJUDGED} (database mode declined the shape — a FAIL whose test reason
 * names it). A test's asserts after its first failure were never adjudicated
 * (first-failure sequencing) and simply have no row. */
final class JudgeLedger {

    private JudgeLedger() {
    }

    static final String PROPERTY = "legend.judge.ledger";


    /** Where this pass writes its ledger: {@code -Dlegend.judge.ledger} when set (the pass names its declared
     *  output); null for none. */
    static Path path() {
        String path = System.getProperty(PROPERTY, "").trim();
        return path.isEmpty() ? null : Path.of(path);
    }

    /** An input file named by {@code -D<property>}: an exec path in a build action (the corpus passes, P2-15), a
     *  runfiles path in a test. */
    static Path input(String property) {
        String value = System.getProperty(property, "").trim();
        if (value.isEmpty()) {
            throw new IllegalStateException("-D" + property + " is not set (spec/corpus.bzl passes it)");
        }
        Path plain = Path.of(value);
        return Files.exists(plain) ? plain : Runfile.of(value);
    }

    /** A corpus pass, a build action (spec/corpus.bzl), passed: its verdict file holds JUnitMain's exit code, and a
     *  failure is reported with the pass's own log, from its failure summary on. {@code pass} names it. */
    static void requirePassed(String pass, String verdictProperty, String logProperty) throws IOException {
        String code = Files.readString(input(verdictProperty), StandardCharsets.UTF_8).trim();
        if (code.equals("0")) {
            return;
        }
        List<String> log = Files.readAllLines(input(logProperty), StandardCharsets.UTF_8);
        int from = Math.max(0, log.size() - 200);
        for (int i = 0; i < log.size(); i++) {
            if (log.get(i).startsWith("Failures (")) {
                from = i;
                break;
            }
        }
        throw new AssertionError("the " + pass + " pass failed (JUnit exit " + code + "); its log from line "
                + (from + 1) + ":\n" + String.join("\n", log.subList(from, Math.min(log.size(), from + 300))));
    }

    /** The host-judge pass passed: the database pass means nothing without it. */
    static void requireHostPassed() throws IOException {
        requirePassed("host-judge", "legend.judge.host.verdict", "legend.judge.host.log");
    }

    /** Append the test's verdict rows when this pass keeps a ledger ({@link #path}). */
    static void record(String test, PureTestRunner.Result r) {
        Path path = path();
        if (path == null) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (PureTestRunner.Verdict v : r.verdicts()) {
            // the UNJUDGED state is the verdict's own fact (the database judge's
            // typed decline), never read from the failure text
            String verdict = v.pass() ? "PASS" : v.unjudgedReason() != null ? "UNJUDGED" : "FAIL";
            sb.append(test).append('\t').append(i++).append('\t').append(v.assertName())
                    .append('\t').append(verdict).append('\n');
        }
        try {
            Files.writeString(path, sb, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException("judge ledger " + path, e);
        }
    }

}
