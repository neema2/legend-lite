// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Runfile;
import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * EVERY CENSUS AND GUARDRAIL CLASS HAS A TARGET THAT RUNS IT (the orchestrator's priority of 2026-11-17 20:55:
 * PlannerSurfaceTest ran in no Bazel target after the edge split -- tagged census, it compiled into the edge's test
 * library, which //core:census does not carry and :core_edge_tests leaves out by tag; two host sessions ran it by
 * hand). The product's test targets leave both tags out, so a class tagged with one runs only where a target selects
 * it:
 * <ul>
 *   <li>in the product's test library: //core:census or //core:guardrails, each selecting the package by tag;</li>
 *   <li>in the edge's (core/edge_tests.bzl): //core:census_edge, selecting every edge class by tag (so its class is
 *   named {@code *Test}, the edge selector's rule), or //core:guardrails_edge, selecting each class by name;</li>
 *   <li>or any test target naming the class ({@code --select-class=}).</li>
 * </ul>
 * Read from the two build files as text: a class no target selects is named here.
 */
@Tag("guardrail")
class TaggedClassesHaveATargetTest {

    private static final String TEST_ROOT = "core/src/test/java/";

    /** The tags whose classes the product's and the edge's test targets leave out, each with its targets. */
    static final List<String> TAGS = List.of("census", "guardrail");

    private static final Pattern TAG = Pattern.compile("@Tag\\(\"(census|guardrail)\"\\)");

    private static String read(Path p) {
        try {
            return Files.readString(p);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The {@code junit_test} or library block named {@code name} in {@code build}, or "" when there is none. */
    static String block(String build, String name) {
        Matcher m = Pattern.compile("\\n\\w+\\(\\n\\s*name = \"" + Pattern.quote(name) + "\",(.*?)\\n\\)",
                Pattern.DOTALL).matcher(build);
        return m.find() ? m.group(1) : "";
    }

    /** The edge's test sources (core/edge_tests.bzl's EDGE_TEST_SRCS), as "src/test/java/..." paths. */
    static Set<String> edgeSources(String bzl) {
        Matcher list = Pattern.compile("EDGE_TEST_SRCS\\s*=\\s*\\[(.*?)\\n\\]", Pattern.DOTALL).matcher(bzl);
        assertTrue(list.find(), "EDGE_TEST_SRCS is read from core/edge_tests.bzl");
        Set<String> out = new LinkedHashSet<>();
        Matcher m = Pattern.compile("\"(src/test/java/[^\"]+\\.java)\"").matcher(list.group(1));
        while (m.find()) {
            out.add(m.group(1));
        }
        return out;
    }

    /** Whether {@code path} ("src/test/java/...") matches one of the patterns of the library's {@code glob} exclude
     *  list (its first, the sources'; its include pattern is not one). */
    static boolean excluded(String path, String libraryBlock) {
        Matcher list = Pattern.compile("exclude\\s*=\\s*\\[(.*?)\\n\\s*\\]", Pattern.DOTALL).matcher(libraryBlock);
        if (!list.find()) {
            return false;
        }
        Matcher m = Pattern.compile("\"(src/test/java/[^\"]+)\"").matcher(list.group(1));
        while (m.find()) {
            if (path.matches(globRegex(m.group(1)))) {
                return true;
            }
        }
        return false;
    }

    /** A Bazel glob as a regex: {@code **} any path, {@code *} within one segment, every other character itself. */
    static String globRegex(String glob) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            if (c == '*' && i + 1 < glob.length() && glob.charAt(i + 1) == '*') {
                out.append(".*");
                i++;
            } else if (c == '*') {
                out.append("[^/]*");
            } else {
                out.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return out.toString();
    }

    /** Each class of {@code sources} ("core/src/test/java/..." -> text) tagged census or guardrail that no target of
     *  {@code build} selects, "path [tag]: why". */
    static List<String> orphans(Map<String, String> sources, Set<String> edge, String build) {
        List<String> out = new ArrayList<>();
        String library = block(build, "core_tests_lib");
        new TreeMap<>(sources).forEach((path, text) -> {
            if (!path.startsWith(TEST_ROOT) || !path.endsWith(".java")) {
                return;
            }
            String rel = path.substring("core/".length());
            String cls = path.substring(TEST_ROOT.length(), path.length() - ".java".length()).replace('/', '.');
            Set<String> tags = new LinkedHashSet<>();
            Matcher m = TAG.matcher(text);
            while (m.find()) {
                tags.add(m.group(1));
            }
            for (String tag : tags) {
                if (build.contains("\"--select-class=" + cls + "\"")) {
                    continue;
                }
                boolean inEdge = edge.contains(rel);
                String why;
                if (inEdge) {
                    String target = tag.equals("census") ? block(build, "census_edge") : block(build, "guardrails_edge");
                    boolean byTag = target.contains("edge_test_classes()") && target.contains("\"--include-tag=" + tag
                            + "\"") && cls.endsWith("Test");
                    why = byTag ? null : "in the edge's test library, selected by no target (census_edge selects the"
                            + " edge's *Test classes by tag; guardrails_edge names each class)";
                } else {
                    String target = block(build, tag.equals("census") ? "census" : "guardrails");
                    boolean byTag = target.contains("\"--select-package=com.legend\"") && target.contains(
                            "\"--include-tag=" + tag + "\"") && target.contains("\":core_tests_lib\"")
                            && cls.startsWith("com.legend.") && !library.isEmpty() && !excluded(rel, library);
                    why = byTag ? null : "in no library a target selects by tag, and named by no target";
                }
                if (why != null) {
                    out.add(path + " [" + tag + "]: " + why);
                }
            }
        });
        return out;
    }

    @Test
    void everyTaggedClassIsSelectedBySomeTarget() {
        String build = read(Runfile.property("core.build_file"));
        Set<String> edge = edgeSources(read(Runfile.property("core.edge_tests_bzl")));
        Map<String, String> sources = new TreeMap<>();
        for (Path p : SourceFiles.under("core/src/test/java")) {
            String path = SourceFiles.path(p);
            if (path.endsWith(".java")) {
                sources.put(path, read(p));
            }
        }
        long tagged = sources.values().stream().filter(t -> TAG.matcher(t).find()).count();
        assertTrue(tagged > 50 && edge.size() > 50, "the tagged classes and the edge's sources are read: " + tagged
                + ", " + edge.size());
        assertEquals(List.of(), orphans(sources, edge, build), "a census or guardrail class runs in no target: select it"
                + " (census_edge or guardrails_edge for the edge's library, census or guardrails for the product's)");
    }

    /** Planted: an edge census class not named *Test, an edge guardrail class no target names, a product census class
     *  its library leaves out, are each named; the same classes selected are not. */
    @Test
    void theCheckBites() {
        String build = "\njunit_test(\n    name = \"census\",\n    select = [\"--select-package=com.legend\","
                + " \"--include-tag=census\"],\n    runtime_deps = [\":core_tests_lib\"],\n)\n"
                + "\njunit_test(\n    name = \"guardrails\",\n    select = [\"--select-package=com.legend\","
                + " \"--include-tag=guardrail\"],\n    runtime_deps = [\":core_tests_lib\"],\n)\n"
                + "\nlegend_java_library(\n    name = \"core_tests_lib\",\n    srcs = glob(\n        [\"src/test/java/**/*.java\"],"
                + "\n        exclude = [\n            \"src/test/java/com/legend/slow/*.java\",\n        ] + EDGE_TEST_SRCS,\n    ),\n)\n"
                + "\njunit_test(\n    name = \"census_edge\",\n    select = edge_test_classes() +"
                + " [\"--include-tag=census\"],\n)\n"
                + "\njunit_test(\n    name = \"guardrails_edge\",\n    select = [\n"
                + "        \"--select-class=com.legend.EdgeGuardTest\",\n    ],\n)\n"
                + "\njunit_test(\n    name = \"slow_census\",\n    select = [\"--select-class=com.legend.slow.KeptTest\"],\n)\n";
        Set<String> edge = Set.of("src/test/java/com/legend/EdgeCensusCheck.java",
                "src/test/java/com/legend/EdgeCensusTest.java", "src/test/java/com/legend/EdgeGuardTest.java",
                "src/test/java/com/legend/OrphanGuardTest.java");
        String census = "@Tag(\"census\") class X {}";
        String guard = "@Tag(\"guardrail\") class X {}";
        Map<String, String> sources = Map.of(
                TEST_ROOT + "com/legend/EdgeCensusCheck.java", census,
                TEST_ROOT + "com/legend/EdgeCensusTest.java", census,
                TEST_ROOT + "com/legend/EdgeGuardTest.java", guard,
                TEST_ROOT + "com/legend/OrphanGuardTest.java", guard,
                TEST_ROOT + "com/legend/ProductCensusTest.java", census,
                TEST_ROOT + "com/legend/slow/LeftOutTest.java", census,
                TEST_ROOT + "com/legend/slow/KeptTest.java", census,
                TEST_ROOT + "com/legend/Untagged.java", "class U {}");
        List<String> found = orphans(sources, edge, build);
        assertEquals(List.of(
                TEST_ROOT + "com/legend/EdgeCensusCheck.java [census]: in the edge's test library, selected by no target"
                        + " (census_edge selects the edge's *Test classes by tag; guardrails_edge names each class)",
                TEST_ROOT + "com/legend/OrphanGuardTest.java [guardrail]: in the edge's test library, selected by no"
                        + " target (census_edge selects the edge's *Test classes by tag; guardrails_edge names each"
                        + " class)",
                TEST_ROOT + "com/legend/slow/LeftOutTest.java [census]: in no library a target selects by tag, and"
                        + " named by no target"), found);
        assertEquals(List.of(TEST_ROOT + "com/legend/EdgeCensusTest.java [census]: in the edge's test library, selected"
                + " by no target (census_edge selects the edge's *Test classes by tag; guardrails_edge names each"
                + " class)"), orphans(Map.of(TEST_ROOT + "com/legend/EdgeCensusTest.java", census), edge,
                build.replace("name = \"census_edge\"", "name = \"census_edge_gone\"")),
                "with no census_edge target, the edge's census class is an orphan (PlannerSurfaceTest's case)");
    }
}
