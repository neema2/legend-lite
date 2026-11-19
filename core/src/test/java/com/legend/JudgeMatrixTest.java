// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import com.legend.testing.SourceFiles;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * BOTH WHERE CORE'S TESTS RUN A BODY THROUGH THE LANE'S JUDGE (the PR #118 follow-ups' C3, track A5, 2026-11-19; G2's
 * guard, docs/GOALS_OF_RECORD_2026_11_07.md): a test that runs a body through the lane's judge ({@code testing.Program},
 * whose judge is {@code -Dlegend.test.judge}, or the lanes' floors, {@code LaneFloor}) runs again under the database
 * judge alone and under BOTH -- core's judge matrix, core/BUILD.bazel's {@code _JUDGED_PACKAGES} and the root package's
 * twins -- and the matrix's suite is in the gate. A package whose tests start running bodies through the lane's judge
 * outside the matrix fails here by name, as does a matrix that drops a judge or leaves the gate. (A test that names its
 * own judge, {@code withJudgeMode}, runs that judge on every lane: it is not the lane's.)
 *
 * <p>Read from the build files as text, comments left out ({@link TaggedClassesHaveATargetTest#uncommented}); a text
 * the check expects and does not find fails it by name, never counts as read.
 */
@Tag("guardrail")
class JudgeMatrixTest {

    private static final String TESTS = "core/src/test/java/com/legend/";

    /** A source that runs a body through the lane's judge: it names {@code testing.Program} or {@code LaneFloor}, or
     *  calls one of their methods. */
    private static final Pattern LANES_JUDGE = Pattern.compile(
            "com\\.legend\\.testing\\.(Program|LaneFloor)\\b|(?<![\\w.])(Program|LaneFloor)\\.[a-z]\\w*\\(");

    private static String read(Path p) {
        try {
            return Files.readString(p);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The quoted names of the top-level list {@code name} in {@code build} (comments left out). */
    private static Set<String> list(String build, String name) {
        Matcher m = Pattern.compile("(?m)^" + Pattern.quote(name) + " = \\[([^\\]]*)\\]").matcher(build);
        assertTrue(m.find(), "core/BUILD.bazel's " + name + " is read");
        Set<String> out = new TreeSet<>();
        Matcher item = Pattern.compile("\"([\\w-]+)\"").matcher(m.group(1));
        while (item.find()) {
            out.add(item.group(1));
        }
        assertTrue(!out.isEmpty(), name + " names packages");
        return out;
    }

    /** The test source directory's top-level packages whose tests run a body through the lane's judge ("" for the
     *  root package's own sources). */
    private static Set<String> runningBodies() {
        Set<String> out = new TreeSet<>();
        TreeMap<String, Integer> seen = new TreeMap<>();
        for (Path p : SourceFiles.under("core/src/test/java")) {
            String path = SourceFiles.path(p);
            if (!path.startsWith(TESTS) || !path.endsWith(".java")) {
                continue;
            }
            String rest = path.substring(TESTS.length());
            String pkg = rest.contains("/") ? rest.substring(0, rest.indexOf('/')) : "";
            seen.merge(pkg, 1, Integer::sum);
            if (LANES_JUDGE.matcher(read(p)).find()) {
                out.add(pkg);
            }
        }
        assertTrue(seen.size() > 20 && seen.getOrDefault("integration", 0) > 50,
                "core's test sources are read: " + seen.keySet());
        return out;
    }

    @Test
    void everyPackageRunningABodyThroughTheLanesJudgeIsInTheMatrix() {
        String build = TaggedClassesHaveATargetTest.uncommented(read(Runfile.property("core.build_file")));
        Set<String> judged = list(build, "_JUDGED_PACKAGES");
        Set<String> own = list(build, "_CORE_TEST_PACKAGES");
        Set<String> running = runningBodies();
        assertTrue(running.contains("integration") && running.contains("testplan"), "the probe finds the bodies' "
                + "tests: " + running);
        List<String> outside = new ArrayList<>();
        for (String pkg : running) {
            // a package of no target of its own is the root target's (--exclude-package of each of core's own), whose
            // twins the matrix holds (below)
            if (own.contains(pkg) && !judged.contains(pkg)) {
                outside.add(pkg);
            }
        }
        assertEquals(List.of(), outside, "a package whose tests run a body through the lane's judge is not in"
                + " core/BUILD.bazel's _JUDGED_PACKAGES: add it, so its bodies run under DATABASE and BOTH too");
    }

    @Test
    void theMatrixRunsTheDatabaseJudgeAndBothAndTheGateRunsTheMatrix() {
        String build = TaggedClassesHaveATargetTest.uncommented(read(Runfile.property("core.build_file")));
        String gates = TaggedClassesHaveATargetTest.uncommented(read(Runfile.property("core.gates_build_file")));
        List<String> missing = new ArrayList<>();
        for (String text : List.of(
                // each judged package's twins, the judge their flag
                "name = \"core_tests_\" + p + \"_judge_\" + judge.lower(),",
                "for p in _JUDGED_PACKAGES for judge in [\"DATABASE\", \"BOTH\"]]",
                // the root package's
                "name = \"core_tests_root_judge_\" + judge.lower(),",
                // the suite of them all
                "tests = [\":core_tests_\" + p + \"_judge_\" + judge.lower() for p in _JUDGED_PACKAGES + [\"root\"]")) {
            if (!build.contains(text)) {
                missing.add(text);
            }
        }
        int flags = build.split(Pattern.quote("jvm_flags = [\"-Dlegend.test.judge=\" + judge],"), -1).length - 1;
        if (flags < 2) {
            missing.add("the twins' -Dlegend.test.judge flag (" + flags + " found)");
        }
        int both = build.split(Pattern.quote("for judge in [\"DATABASE\", \"BOTH\"]]"), -1).length - 1;
        if (both < 2) {
            missing.add("the root twins' judges (" + both + " lists of DATABASE and BOTH found)");
        }
        assertEquals(List.of(), missing, "core/BUILD.bazel's judge matrix is not as this guard reads it");
        assertTrue(TaggedClassesHaveATargetTest.gated(gates).contains("//core:core_tests_judges"),
                "the gate runs the judge matrix (//core:core_tests_judges)");
        String program = read(SourceFiles.file("core/src/test/java/com/legend/testing/Program.java"));
        assertTrue(program.contains("System.getProperty(\"legend.test.judge\", \"HOST\")"),
                "testing.Program reads the lane's judge from -Dlegend.test.judge");
    }
}
