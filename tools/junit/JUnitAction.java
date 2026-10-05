// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.junit;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * A JUnit run as a BUILD ACTION (Bazel workplan P3-01): one test pass whose output another reads (the corpus's
 * host-judge pass, whose per-assert ledger the database-judge pass joins) becomes a cached build output, instead of a
 * child JVM the test starts.
 *
 * <p>Arguments: {@code <verdict> <log> [<output> ...] --}, then {@link JUnitMain}'s own. The run's output goes to
 * {@code <log>}; its exit code (JUnitMain's: 0 when every selected test passed) to {@code <verdict>}; each
 * {@code <output>} the run did not write is created holding one UNMEASURED line, so a diff against it says why
 * rather than showing a blank file (and re-blessing it would put that line in the tree for review to see). A pass whose tests failed (exit 1) is still a successful
 * action: a fact its consumer reports, as a test failure quoting the log, never a build error with no test result. It is
 * cached like any output, until an input changes ({@code bazel clean} forces a rerun). Anything else (nothing selected,
 * a runner error, a log or verdict it cannot write) fails the action, so a broken run is never cached as a verdict.
 */
public final class JUnitAction {

    private JUnitAction() {}

    public static void main(String[] args) {
        // the heap's live peak, in the log as a test's is, so the action's memory_mb is set from a measurement (P1-21)
        JUnitMain.watchHeap();
        int code = 4;
        try {
            code = run(args);
        } catch (Exception | Error e) {
            e.printStackTrace();
        } finally {
            // exit explicitly, as JUnitMain does: a test's non-daemon thread must not hold the action open
            System.exit(code == 0 || code == 1 ? 0 : 1);
        }
    }

    /** The action's work, in this JVM (RunnerTest and spec's CorpusOne call it): the pass, its log, its verdict, its
     *  outputs; returns JUnitMain's exit code. */
    public static int run(String[] args) throws Exception {
        int split = Arrays.asList(args).indexOf("--");
        if (split < 2) {
            throw new IllegalArgumentException("JUnitAction <verdict> <log> [<output> ...] -- <JUnitMain arguments>");
        }
        Path verdict = Path.of(args[0]);
        Path log = Path.of(args[1]);
        int code = 4;
        PrintStream out = System.out;
        PrintStream err = System.err;
        try (PrintStream to = new PrintStream(Files.newOutputStream(log), true, StandardCharsets.UTF_8)) {
            System.setOut(to);
            System.setErr(to);
            try {
                // no Bazel test protocol here: no XML, no shards, no premature-exit file
                code = JUnitMain.run(Arrays.copyOfRange(args, split + 1, args.length), name -> null);
            } catch (Exception | Error e) {
                e.printStackTrace();
            } finally {
                System.setOut(out);
                System.setErr(err);
            }
        }
        for (int i = 2; i < split; i++) {
            Path p = Path.of(args[i]);
            if (!Files.exists(p)) {
                Files.writeString(p, "UNMEASURED: the pass did not write this file (JUnit exit " + code
                        + "; its log says why)\n", StandardCharsets.UTF_8);
            }
        }
        Files.writeString(verdict, code + "\n", StandardCharsets.UTF_8);
        return code;
    }
}
