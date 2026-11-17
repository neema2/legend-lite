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

    /** JUnitMain's class-name filter when a target names none (the console launcher's standard include pattern). */
    static final Pattern STANDARD_NAMES = Pattern.compile("^(Test.*|.+[.$]Test.*|.*Tests?)$");

    /** A test target of the build file: its name and its text, comment lines left out. */
    record Target(String name, String text) {
        boolean manual() {
            Matcher m = Pattern.compile("tags\\s*=\\s*\\[([^\\]]*)\\]").matcher(text);
            return m.find() && m.group(1).contains("\"manual\"");
        }

        /** Whether the target leaves {@code tag} out: its exclude_tags lists it, or names a list (core's
         *  _CORE_TEST_EXCLUDED_TAGS) that does. */
        boolean drops(String tag, String build) {
            Matcher m = Pattern.compile("exclude_tags\\s*=\\s*(\\[[^\\]]*\\]|\\w+)").matcher(text);
            if (!m.find()) {
                return false;
            }
            String list = m.group(1);
            if (!list.startsWith("[")) {
                Matcher v = Pattern.compile("\\n" + Pattern.quote(list) + "\\s*=\\s*\\[(.*?)\\n\\]", Pattern.DOTALL)
                        .matcher(build);
                list = v.find() ? v.group(1) : "";
            }
            return list.contains("\"" + tag + "\"");
        }

        /** Whether the target's tag filter admits {@code tag}: no --include-tag, or one naming it. */
        boolean admits(String tag) {
            return !text.contains("--include-tag=") || text.contains("\"--include-tag=" + tag + "\"");
        }

        /** Whether one of the target's literal --exclude-classname or --exclude-package arguments drops {@code cls}. */
        boolean excludes(String cls) {
            Matcher m = Pattern.compile("\"--exclude-classname=([^\"]+)\"").matcher(text);
            while (m.find()) {
                if (cls.matches(m.group(1))) {
                    return true;
                }
            }
            Matcher p = Pattern.compile("\"--exclude-package=([^\"]+)\"").matcher(text);
            while (p.find()) {
                if (cls.startsWith(p.group(1) + ".")) {
                    return true;
                }
            }
            return false;
        }
    }

    /** Every simple {@code junit_test} of {@code build}, comment lines left out (a list comprehension's are not
     *  read: a class only one of those selects is named an orphan, loudly). */
    static List<Target> targets(String build) {
        List<Target> out = new ArrayList<>();
        Matcher m = Pattern.compile("\\njunit_test\\(\\n\\s*name = \"([\\w-]+)\",(.*?)\\n\\)", Pattern.DOTALL)
                .matcher(build);
        while (m.find()) {
            StringBuilder text = new StringBuilder();
            for (String line : m.group(2).split("\\n")) {
                if (!line.strip().startsWith("#")) {
                    text.append(line.replaceFirst("\\s#\\s.*$", "")).append('\n');
                }
            }
            out.add(new Target(m.group(1), text.toString()));
        }
        return out;
    }

    /** Whether {@code t} selects {@code cls}: by name, by the edge's selector (an edge class named *Test), or by the
     *  com.legend package over the product's test library (a class it carries). */
    static boolean selects(Target t, String cls, String rel, boolean inEdge, String library) {
        if (t.text().contains("\"--select-class=" + cls + "\"")) {
            return true;
        }
        if (inEdge) {
            return t.text().contains("edge_test_classes()") && cls.endsWith("Test");
        }
        return t.text().contains("\"--select-package=com.legend\"") && t.text().contains("\":core_tests_lib\"")
                && cls.startsWith("com.legend.") && !library.isEmpty() && !excluded(rel, library);
    }

    /** Each class of {@code sources} ("core/src/test/java/..." -> text) tagged census or guardrail that no target of
     *  {@code build} runs in the gate ({@code gates}, gates/BUILD.bazel's text): one that selects it, is not manual, is
     *  listed in the gate, admits and does not drop the tag, excludes neither the class nor its package, and whose
     *  runner's name filter admits the class -- "path [tag]: why". */
    static List<String> orphans(Map<String, String> sources, Set<String> edge, String build, String gates) {
        List<String> out = new ArrayList<>();
        String library = block(build, "core_tests_lib");
        List<Target> targets = targets(build);
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
                boolean inEdge = edge.contains(rel);
                boolean runs = STANDARD_NAMES.matcher(cls).matches() && targets.stream().anyMatch(t -> selects(t, cls,
                        rel, inEdge, library) && !t.manual() && gates.contains("\"//core:" + t.name() + "\"")
                        && t.admits(tag) && !t.drops(tag, build) && !t.excludes(cls));
                if (!runs) {
                    out.add(path + " [" + tag + "]: " + (inEdge ? "in the edge's test library" : "in the product's test"
                            + " library") + ", run by no gate target");
                }
            }
        });
        return out;
    }

    @Test
    void everyTaggedClassIsSelectedBySomeTarget() {
        String build = read(Runfile.property("core.build_file"));
        String gates = read(Runfile.property("core.gates_build_file"));
        Set<String> edge = edgeSources(read(Runfile.property("core.edge_tests_bzl")));
        // the parse reads the real file (a broken exclude-list parse would fail open: the review of e8bd35d19d)
        assertTrue(excluded("src/test/java/com/legend/server/PostgresArmTest.java", block(build, "core_tests_lib")),
                "core_tests_lib's exclude list is read");
        assertTrue(targets(build).stream().anyMatch(t -> t.name().equals("census_edge"))
                && targets(build).stream().anyMatch(t -> t.name().equals("guardrails_edge")), "the targets are read");
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
        assertEquals(List.of(), orphans(sources, edge, build, gates), "a census or guardrail class runs in no gate"
                + " target: select it (census_edge or guardrails_edge for the edge's library, census or guardrails for"
                + " the product's) in a target the gate lists");
    }

    private static String junit(String name, String body) {
        return "\njunit_test(\n    name = \"" + name + "\",\n" + body + "\n)\n";
    }

    /** Planted: each way a class can be selected and still not run is named -- an edge census class not named *Test,
     *  an edge guardrail class no target names, a product class its library excludes, a class named only by a manual
     *  target, by a target the gate does not list, by a target dropping its tag, by a target excluding its name, by a
     *  commented-out line, and a class name the runner's filter skips; the same classes properly selected are not. */
    @Test
    void theCheckBites() {
        String build = "\n_CORE_TEST_EXCLUDED_TAGS = [\n    \"census\",\n    \"guardrail\",\n]\n"
                + junit("census", "    select = [\"--select-package=com.legend\", \"--include-tag=census\"],\n"
                        + "    runtime_deps = [\":core_tests_lib\"],")
                + junit("guardrails", "    select = [\"--select-package=com.legend\", \"--include-tag=guardrail\"],\n"
                        + "    runtime_deps = [\":core_tests_lib\"],")
                + "\nlegend_java_library(\n    name = \"core_tests_lib\",\n    srcs = glob(\n"
                + "        [\"src/test/java/**/*.java\"],\n        exclude = [\n"
                + "            \"src/test/java/com/legend/slow/*.java\",\n        ] + EDGE_TEST_SRCS,\n    ),\n)\n"
                + junit("census_edge", "    select = edge_test_classes() + [\"--include-tag=census\"],\n"
                        + "    tags = [\"edge_test\"],")
                + junit("guardrails_edge", "    select = [\n        \"--select-class=com.legend.EdgeGuardTest\",\n"
                        + "        # \"--select-class=com.legend.CommentedGuardTest\",\n    ],")
                + junit("slow_census", "    select = [\"--select-class=com.legend.slow.KeptTest\"],")
                + junit("probe", "    select = [\"--select-class=com.legend.slow.ManualOnlyTest\"],\n"
                        + "    tags = [\"manual\"],")
                + junit("off_gate", "    select = [\"--select-class=com.legend.slow.OffGateTest\"],")
                + junit("dropping", "    select = [\"--select-class=com.legend.slow.DroppedTest\"],\n"
                        + "    exclude_tags = _CORE_TEST_EXCLUDED_TAGS,")
                + junit("excluding", "    select = [\"--select-class=com.legend.slow.NamedOutTest\","
                        + " \"--exclude-classname=.*NamedOutTest\"],");
        String gates = "\"//core:census\", \"//core:guardrails\", \"//core:census_edge\", \"//core:guardrails_edge\","
                + " \"//core:slow_census\", \"//core:probe\", \"//core:dropping\", \"//core:excluding\"";
        Set<String> edge = Set.of("src/test/java/com/legend/EdgeCensusCheck.java",
                "src/test/java/com/legend/EdgeCensusTest.java", "src/test/java/com/legend/EdgeGuardTest.java",
                "src/test/java/com/legend/OrphanGuardTest.java", "src/test/java/com/legend/CommentedGuardTest.java");
        String census = "@Tag(\"census\") class X {}";
        String guard = "@Tag(\"guardrail\") class X {}";
        Map<String, String> sources = new TreeMap<>();
        for (String c : List.of("EdgeCensusCheck", "EdgeCensusTest", "ProductCensusTest", "slow/LeftOutTest",
                "slow/KeptTest", "slow/ManualOnlyTest", "slow/OffGateTest", "slow/DroppedTest", "slow/NamedOutTest")) {
            sources.put(TEST_ROOT + "com/legend/" + c + ".java", census);
        }
        for (String c : List.of("EdgeGuardTest", "OrphanGuardTest", "CommentedGuardTest", "ProductGuard",
                "ProductGuardTest")) {
            sources.put(TEST_ROOT + "com/legend/" + c + ".java", guard);
        }
        sources.put(TEST_ROOT + "com/legend/Untagged.java", "class U {}");
        assertEquals(List.of(
                TEST_ROOT + "com/legend/CommentedGuardTest.java [guardrail]: in the edge's test library, run by no gate"
                        + " target",
                TEST_ROOT + "com/legend/EdgeCensusCheck.java [census]: in the edge's test library, run by no gate target",
                TEST_ROOT + "com/legend/OrphanGuardTest.java [guardrail]: in the edge's test library, run by no gate"
                        + " target",
                TEST_ROOT + "com/legend/ProductGuard.java [guardrail]: in the product's test library, run by no gate"
                        + " target",
                TEST_ROOT + "com/legend/slow/DroppedTest.java [census]: in the product's test library, run by no gate"
                        + " target",
                TEST_ROOT + "com/legend/slow/LeftOutTest.java [census]: in the product's test library, run by no gate"
                        + " target",
                TEST_ROOT + "com/legend/slow/ManualOnlyTest.java [census]: in the product's test library, run by no gate"
                        + " target",
                TEST_ROOT + "com/legend/slow/NamedOutTest.java [census]: in the product's test library, run by no gate"
                        + " target",
                TEST_ROOT + "com/legend/slow/OffGateTest.java [census]: in the product's test library, run by no gate"
                        + " target"), orphans(sources, edge, build, gates));
        assertEquals(List.of(TEST_ROOT + "com/legend/EdgeCensusTest.java [census]: in the edge's test library, run by no"
                + " gate target"), orphans(Map.of(TEST_ROOT + "com/legend/EdgeCensusTest.java", census), edge, build,
                gates.replace("\"//core:census_edge\"", "")), "with census_edge out of the gate, the edge's census class"
                + " is an orphan (PlannerSurfaceTest's case)");
    }
}
