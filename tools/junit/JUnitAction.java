// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.junit;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * A JUnit run as a BUILD ACTION (Bazel workplan P3-01): one test pass whose output another lane reads (the corpus's
 * host-judge pass, whose per-assert ledger the database-judge lane joins) becomes a cached build output, instead of a
 * child JVM the test starts.
 *
 * <p>Arguments: {@code <verdict> <log> [<output> ...] --}, then {@link JUnitMain}'s own. The run's output goes to
 * {@code <log>}; its exit code (JUnitMain's: 0 when every selected test passed) to {@code <verdict>}; each
 * {@code <output>} the run did not write is created empty. The action succeeds either way: a failing pass is a fact
 * its consumer reports, as a test failure quoting the log, never a build error with no test result.
 */
public final class JUnitAction {

    private JUnitAction() {}

    public static void main(String[] args) throws Exception {
        run(args);
        // exit explicitly, as JUnitMain does: a test's non-daemon thread must not hold the action open
        System.exit(0);
    }

    /** The action's work, in this JVM (RunnerTest calls it): the pass, its log, its verdict, its outputs. */
    static void run(String[] args) throws Exception {
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
                Files.createFile(p);
            }
        }
        Files.writeString(verdict, code + "\n", StandardCharsets.UTF_8);
    }
}
