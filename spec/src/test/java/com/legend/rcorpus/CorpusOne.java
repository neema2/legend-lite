// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.rcorpus;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * One corpus pass on demand, scoped to a test when one is named (Bazel workplan P2-15: the lane's passes are cached
 * build actions, so a scoped run is a program):
 * {@code bazel run //spec:corpus_one -- <duckdb|h2> <host|database|both> [<test fqn>]} ({@code both}: the agreement
 * gate, the plan path program's step 6, 2026-10-18). It prints the pass's log and exits
 * with JUnit's code. A scoped database pass runs the database judge on its tests alone: no host verdict, no join.
 */
public final class CorpusOne {

    private CorpusOne() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3 || !args[1].matches("host|database|both")) {
            throw new IllegalArgumentException("corpus_one <lane> <host|database|both> [<test fqn>]: the lane a test"
                    + " backend's name (TestBackends.named)");
        }
        // a lane no backend has is refused, listing those there are
        com.legend.testbackend.TestBackends.named(args[0]);
        if (args[1].equals("database") && args.length == 2) {
            throw new IllegalArgumentException("a whole database pass needs the host pass's outputs: bazel test"
                    + " //spec:corpus_" + args[0] + " where the lane has that target (its passes are the actions"
                    + " judge_host_" + args[0] + " and judge_database_" + args[0] + "; spec/BUILD.bazel), else"
                    + " CorpusOn; corpus_one runs the database judge scoped to a test");
        }
        System.setProperty("rcorpus.backend", args[0]);
        System.setProperty("legend.judge.mode", args[1]);
        System.setProperty("legend.exec.engineScanOrder", "true");
        if (args.length == 3) {
            System.setProperty("rcorpus.test", args[2]);
        }
        Path dir = Files.createTempDirectory("corpus-one");
        Path verdict = dir.resolve("verdict.txt");
        Path log = dir.resolve("pass.log");
        int code = com.legend.tools.junit.JUnitAction.run(new String[] {verdict.toString(), log.toString(), "--",
                "--select-class=com.legend.rcorpus.MinimalCorpusTest", "--fail-if-no-tests"});
        System.out.write(Files.readAllBytes(log));
        System.out.println("[corpus_one] " + args[0] + " " + args[1] + (args.length == 3 ? " " + args[2] : "")
                + ": JUnit exit " + code + " (log " + log + ")");
        System.exit(code);
    }
}
