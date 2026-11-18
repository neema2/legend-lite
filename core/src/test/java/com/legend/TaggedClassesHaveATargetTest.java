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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
 * Read from the build files as text, comments left out. What the gate runs is what gates/BUILD.bazel's {@code local}
 * and {@code lanes} suites reach through the file's own suites ({@code heavy}, run by hand, is no root). Build text the
 * check reads and cannot parse fails it by name, never counts as read (the re-review of 60aa31029b, 2026-11-17: a
 * misread exclude list or tag list would fail open). A class no target runs in the gate is named here.
 */
@Tag("guardrail")
class TaggedClassesHaveATargetTest {

    private static final String TEST_ROOT = "core/src/test/java/";

    private static final Pattern TAG = Pattern.compile("@Tag\\(\"(census|guardrail)\"\\)");

    private static String read(Path p) {
        try {
            return Files.readString(p);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@code text} with its comments left out: from a {@code #} outside a string to its line's end (a {@code #}
     *  inside a quoted argument is the argument's). */
    static String uncommented(String text) {
        StringBuilder out = new StringBuilder();
        char quote = 0;
        boolean comment = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\n') {
                quote = 0;
                comment = false;
                out.append(c);
            } else if (comment) {
                continue;
            } else if (quote != 0) {
                out.append(c);
                if (c == '\\' && i + 1 < text.length() && text.charAt(i + 1) != '\n') {
                    out.append(text.charAt(++i));
                } else if (c == quote) {
                    quote = 0;
                }
            } else if (c == '#') {
                comment = true;
            } else {
                if (c == '"' || c == '\'') {
                    quote = c;
                }
                out.append(c);
            }
        }
        return out.toString();
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

    private static final Pattern LIBRARY_SRCS = Pattern.compile("srcs = glob\\(\\s*\\[\"src/test/java/\\*\\*/\\*\\.java\"\\],"
            + "\\s*exclude = \\[(.*?)\\n\\s*\\]([^\\n]*)\\n\\s*\\),", Pattern.DOTALL);

    private static final Pattern QUOTED_PATTERN = Pattern.compile("\"(src/test/java/[^\"]+)\",?");

    /** The patterns of the product's test library's glob exclude list ({@code libraryBlock}, its block, comments left
     *  out): its sources are {@code glob(["src/test/java/**}{@code /*.java"], exclude = [...] + EDGE_TEST_SRCS)}, each
     *  line of the list one quoted pattern. Other text fails the check by name. */
    static List<String> libraryExcludes(String libraryBlock) {
        Matcher srcs = LIBRARY_SRCS.matcher(libraryBlock);
        if (!srcs.find()) {
            throw new IllegalStateException("core_tests_lib's srcs are not glob([\"src/test/java/**/*.java\"], exclude"
                    + " = [...] + EDGE_TEST_SRCS): the check does not read them");
        }
        if (!srcs.group(2).strip().equals("+ EDGE_TEST_SRCS,")) {
            throw new IllegalStateException("core_tests_lib's exclude list is joined with text the check does not"
                    + " read: " + srcs.group(2).strip());
        }
        List<String> out = new ArrayList<>();
        for (String line : srcs.group(1).split("\\n")) {
            String item = line.strip();
            if (item.isEmpty()) {
                continue;
            }
            Matcher m = QUOTED_PATTERN.matcher(item);
            if (!m.matches()) {
                throw new IllegalStateException("core_tests_lib's exclude list holds text the check does not read: "
                        + item);
            }
            out.add(m.group(1));
        }
        return out;
    }

    /** Whether {@code path} ("src/test/java/...") matches one of the product's test library's exclude patterns. */
    static boolean excluded(String path, String libraryBlock) {
        return libraryExcludes(libraryBlock).stream().anyMatch(g -> path.matches(globRegex(g)));
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

    /** The labels the gate runs: every test gates/BUILD.bazel's {@code local} and {@code lanes} suites reach through
     *  the file's own suites ({@code :name}), read from their {@code tests} lists ({@code gates}, comments left out).
     *  A manual test a reached suite lists runs (Bazel's manual tag keeps a target out of wildcards and out of suites
     *  that do not list it); {@code heavy}, run by hand, is no root; another package's suite is not followed (a
     *  //core target only one of those reaches is named an orphan). A suite reached and not read, or a list item
     *  that is not one quoted label, fails the check by name. */
    static Set<String> gated(String gates) {
        Map<String, List<String>> suites = new TreeMap<>();
        Set<String> filtered = new TreeSet<>();
        Matcher m = Pattern.compile("\\ntest_suite\\(\\n\\s*name = \"(\\w+)\",(.*?)\\n\\)", Pattern.DOTALL)
                .matcher(gates);
        while (m.find()) {
            // (spaces around "=" optional: the third review's NOTE, 2026-11-17 -- "tags=[...]" was not read)
            Matcher tests = Pattern.compile("\\n\\s*tests\\s*=\\s*\\[(.*?)\\]", Pattern.DOTALL).matcher(m.group(2));
            // the attribute's whole value, to the next attribute (the review of 5911e970e1, 2026-11-17: ["manual"] +
            // ["-edge_test"] was read as ["manual"])
            Matcher tags = Pattern.compile("\\n\\s*tags\\s*=\\s*(.*?)(?=\\n\\s*\\w+\\s*=|\\z)", Pattern.DOTALL)
                    .matcher(m.group(2));
            if (tags.find() && !tags.group(1).replaceAll("\\s", "").replaceAll(",$", "").equals("[\"manual\"]")) {
                // a suite's tags filter even the tests it lists (manual alone is no filter): read, it would be unread
                filtered.add(m.group(1));
            }
            List<String> members = new ArrayList<>();
            if (tests.find()) {
                for (String item : tests.group(1).split(",")) {
                    String label = item.strip();
                    if (label.isEmpty()) {
                        continue;
                    }
                    if (!label.matches("\"[^\"]+\"")) {
                        throw new IllegalStateException("gates/BUILD.bazel's suite " + m.group(1) + " lists text the"
                                + " check does not read: " + label);
                    }
                    members.add(label.substring(1, label.length() - 1));
                }
            }
            suites.put(m.group(1), members);
        }
        Set<String> out = new TreeSet<>();
        Set<String> seen = new HashSet<>();
        Deque<String> todo = new ArrayDeque<>(List.of("local", "lanes"));
        while (!todo.isEmpty()) {
            String suite = todo.pop();
            if (!seen.add(suite)) {
                continue;
            }
            List<String> members = suites.get(suite);
            if (members == null || members.isEmpty()) {
                throw new IllegalStateException("gates/BUILD.bazel's suite " + suite + " is reached and not read");
            }
            if (filtered.contains(suite)) {
                throw new IllegalStateException("gates/BUILD.bazel's suite " + suite + " filters its tests by tag, which"
                        + " the check does not read");
            }
            for (String label : members) {
                if (label.startsWith(":")) {
                    todo.push(label.substring(1));
                } else if (label.startsWith("//gates:")) {
                    todo.push(label.substring("//gates:".length()));
                } else {
                    out.add(label);
                }
            }
        }
        return out;
    }

    /** JUnitMain's class-name filter when a target names none (the console launcher's standard include pattern). */
    static final Pattern STANDARD_NAMES = Pattern.compile("^(Test.*|.+[.$]Test.*|.*Tests?)$");

    private static final Pattern TAG_TERM = Pattern.compile("\\[([^\\]]*)\\]|(\\w+)");

    /** The quoted strings of {@code list}, which holds nothing else (a name, a comprehension: the check does not read
     *  it, and fails by name -- the review of 260fdb147f, 2026-11-17). */
    private static List<String> quoted(String target, String list) {
        List<String> out = new ArrayList<>();
        Matcher m = Pattern.compile("\"([^\"]*)\"").matcher(list);
        while (m.find()) {
            out.add(m.group(1));
        }
        String rest = list.replaceAll("\"[^\"]*\"", "").replaceAll("[,\\s]", "");
        if (!rest.isEmpty()) {
            throw new IllegalStateException(target + "'s exclude_tags holds text the check does not read: " + rest);
        }
        return out;
    }

    /** {@code tags}, each a plain tag: JUnit reads a tag filter's value as an expression ({@code census | guardrail}),
     *  which the check does not, so one fails by name. */
    private static List<String> plainTags(String target, List<String> tags) {
        for (String t : tags) {
            if (!t.matches("[\\w.-]+")) {
                throw new IllegalStateException(target + " filters by a tag expression the check does not read: " + t);
            }
        }
        return tags;
    }

    /** The literal values of {@code text}'s {@code "<flag>=..."} arguments, each a whole list item (followed by
     *  {@code ,} or {@code ]}): a computed one -- {@code "<flag>=" + c}, {@code "<flag>=%s" % x},
     *  {@code "<flag>={}".format(p)} -- is not read (the third review, 2026-11-17, S10: only {@code +} was refused), so a
     *  selector the check does not read selects nothing. */
    private static List<String> args(String text, String flag) {
        List<String> out = new ArrayList<>();
        Matcher m = Pattern.compile("\"" + Pattern.quote(flag) + "=([^\"]+)\"(?=\\s*[,\\]])").matcher(text);
        while (m.find()) {
            out.add(m.group(1));
        }
        return out;
    }

    /** The literal values of {@code target}'s {@code "<flag>=..."} arguments, read from its {@code select} alone (the
     *  review of 5911e970e1, 2026-11-17: a selector written in {@code tags} was read as one), for a filter or a selector:
     *  one the check cannot read (a computed argument, a conditional's branch) fails it by name, since a filter misread
     *  fails open. */
    private static List<String> literalArgs(String target, String text, String flag) {
        String select = selectRead(target, text).text();
        List<String> out = args(select, flag);
        int written = select.split(Pattern.quote(flag + "="), -1).length - 1;
        if (written != out.size()) {
            throw new IllegalStateException(target + " has a " + flag + " argument the check does not read (a computed"
                    + " one): write each literally");
        }
        return out;
    }

    /** A target's {@code select} as read: its text, and whether {@code edge_test_classes()} is one of its terms. */
    record Select(String text, boolean edgeClasses) {
    }

    /** {@code target}'s {@code select} (its {@code text}), which holds only what the check reads: terms joined by
     *  {@code +}, each {@code edge_test_classes()} or a list whose every item is a quoted string or writes its flag's
     *  name quoted ({@code "--exclude-classname=" + c for ...}: the flag's count then finds it computed). A name, a
     *  call or an item that writes no flag -- {@code [...] + _EXCLUDES}, {@code [x for x in L]} -- would carry
     *  arguments no read sees (the third review, 2026-11-17, S10), so it fails the check by name. */
    static Select selectRead(String target, String text) {
        // an escape in a string (Starlark's \145, \x65, \u0065) spells what the check reads as other text: refused by
        // name in the target's every string (the review of 5911e970e1, 2026-11-17)
        Matcher escaped = Pattern.compile("\"[^\"\\n]*\\\\").matcher(text);
        if (escaped.find()) {
            throw new IllegalStateException(target + " has an escape in a string the check does not read");
        }
        Matcher m = Pattern.compile("(?<![\\w])select\\s*=\\s*").matcher(text);
        if (!m.find()) {
            return new Select("", false);
        }
        int from = m.end();
        int at = from;
        boolean edgeClasses = false;
        while (true) {
            while (at < text.length() && Character.isWhitespace(text.charAt(at))) {
                at++;
            }
            if (text.startsWith("edge_test_classes()", at)) {
                at += "edge_test_classes()".length();
                edgeClasses = true;
            } else if (at < text.length() && text.charAt(at) == '[') {
                int close = closing(text, at);
                if (close < 0) {
                    throw new IllegalStateException(target + "'s select holds a list the check does not read");
                }
                for (String item : topLevelItems(text.substring(at + 1, close))) {
                    if (!item.matches("\"[^\"]*\"") && !item.matches("(?s).*\"--[\\w-]+=.*")) {
                        throw new IllegalStateException(target + "'s select holds text the check does not read: "
                                + item);
                    }
                }
                at = close + 1;
            } else {
                String term = text.substring(at).split("[\\s,+]", 2)[0];
                throw new IllegalStateException(target + "'s select holds text the check does not read: " + term);
            }
            while (at < text.length() && Character.isWhitespace(text.charAt(at))) {
                at++;
            }
            if (at < text.length() && text.charAt(at) == '+') {
                at++;
                continue;
            }
            if (at >= text.length() || text.charAt(at) == ',') {
                return new Select(text.substring(from, at), edgeClasses);
            }
            throw new IllegalStateException(target + "'s select is followed by text the check does not read");
        }
    }

    /** The index of the bracket closing the one at {@code open} (strings read as text), or -1. */
    private static int closing(String text, int open) {
        int depth = 0;
        for (int i = open; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"') {
                i = text.indexOf('"', i + 1);
                if (i < 0) {
                    return -1;
                }
            } else if (c == '[' || c == '(' || c == '{') {
                depth++;
            } else if ((c == ']' || c == ')' || c == '}') && --depth == 0) {
                return i;
            }
        }
        return -1;
    }

    /** {@code list}'s items, split at its top-level commas (strings read as text), each stripped, blanks left out. */
    private static List<String> topLevelItems(String list) {
        List<String> out = new ArrayList<>();
        int depth = 0;
        int from = 0;
        for (int i = 0; i <= list.length(); i++) {
            char c = i < list.length() ? list.charAt(i) : ',';
            if (c == '"') {
                int end = list.indexOf('"', i + 1);
                i = end < 0 ? list.length() - 1 : end;
            } else if (c == '[' || c == '(' || c == '{') {
                depth++;
            } else if (c == ']' || c == ')' || c == '}') {
                depth--;
            } else if (c == ',' && depth == 0) {
                String item = list.substring(from, i).strip();
                if (!item.isEmpty()) {
                    out.add(item);
                }
                from = i + 1;
            }
        }
        return out;
    }

    /** A test target of the build file: its name and its text, comments left out. */
    record Target(String name, String text) {

        /** The tags the target leaves out: its exclude_tags (lists, and the names of the build file's lists, joined
         *  by {@code +}) and its --exclude-tag arguments. A term or an argument the check does not read fails it. */
        Set<String> dropped(String build) {
            Set<String> out = new TreeSet<>(plainTags(name, literalArgs(name, text, "--exclude-tag")));
            Matcher m = Pattern.compile("(?<![\\w])exclude_tags\\s*=\\s*").matcher(text);
            if (!m.find()) {
                return out;
            }
            int at = m.end();
            while (true) {
                Matcher term = TAG_TERM.matcher(text).region(at, text.length());
                if (!term.lookingAt()) {
                    throw new IllegalStateException(name + "'s exclude_tags holds text the check does not read");
                }
                if (term.group(1) != null) {
                    out.addAll(plainTags(name, quoted(name, term.group(1))));
                } else {
                    Matcher v = Pattern.compile("\\n" + Pattern.quote(term.group(2)) + "\\s*=\\s*\\[(.*?)\\n\\]",
                            Pattern.DOTALL).matcher(build);
                    if (!v.find()) {
                        throw new IllegalStateException(name + "'s exclude_tags names " + term.group(2) + ", a list"
                                + " the check does not read");
                    }
                    out.addAll(plainTags(name, quoted(name, v.group(1))));
                }
                Matcher next = Pattern.compile("\\s*(\\+\\s*|,|$)").matcher(text).region(term.end(), text.length());
                if (!next.lookingAt()) {
                    throw new IllegalStateException(name + "'s exclude_tags is followed by text the check does not"
                            + " read");
                }
                if (!next.group(1).startsWith("+")) {
                    return out;
                }
                at = next.end();
            }
        }

        /** Whether the target's tag filter admits {@code tag}: no --include-tag, or one naming it. */
        boolean admits(String tag) {
            List<String> included = plainTags(name, literalArgs(name, text, "--include-tag"));
            return included.isEmpty() || included.contains(tag);
        }

        /** Whether one of the target's --exclude-classname or --exclude-package arguments drops {@code cls}. */
        boolean excludes(String cls) {
            return literalArgs(name, text, "--exclude-classname").stream().anyMatch(cls::matches)
                    || literalArgs(name, text, "--exclude-package").stream().anyMatch(p -> cls.startsWith(p + "."));
        }

        /** The target, which runs on every platform the gate does: a platform restriction the check does not read
         *  ({@code target_compatible_with}; and {@code compatible_with}, {@code restricted_to},
         *  {@code exec_compatible_with}: the third review's NOTE, 2026-11-17) fails it by name (it may skip the target
         *  where the gate runs). */
        boolean everywhere() {
            Matcher r = Pattern.compile("(?<![\\w])(target_compatible_with|compatible_with|restricted_to"
                    + "|exec_compatible_with)\\s*=").matcher(text);
            if (r.find()) {
                throw new IllegalStateException(name + " has a " + r.group(1) + " the check does not read");
            }
            // a tag the lanes filter by platform (tools/ci/lane-flags.sh drops ci-linux-only off Linux): the review of
            // 5911e970e1's NOTE, 2026-11-17
            if (text.contains("\"ci-linux-only\"")) {
                throw new IllegalStateException(name + " has a ci-linux-only tag the check does not read");
            }
            return true;
        }

        /** Whether the runner's class-name filter admits {@code cls}: the target's --include-classname patterns, or
         *  the standard pattern when it names none. */
        boolean named(String cls) {
            List<String> patterns = literalArgs(name, text, "--include-classname");
            return patterns.isEmpty() ? STANDARD_NAMES.matcher(cls).matches() : patterns.stream().anyMatch(cls::matches);
        }
    }

    /** Every simple {@code junit_test} of {@code build} (comments left out; a list comprehension's are not read: a
     *  class only one of those selects is named an orphan, loudly). */
    static List<Target> targets(String build) {
        List<Target> out = new ArrayList<>();
        Matcher m = Pattern.compile("\\njunit_test\\(\\n\\s*name = \"([\\w-]+)\",(.*?)\\n\\)", Pattern.DOTALL)
                .matcher(build);
        while (m.find()) {
            out.add(new Target(m.group(1), m.group(2)));
        }
        return out;
    }

    /** Whether {@code t} selects {@code cls}: by name, by the edge's selector over the edge's test library (an edge
     *  class named *Test), or by the com.legend package over the product's test library (a class it carries). */
    static boolean selects(Target t, String cls, String rel, boolean inEdge, String library) {
        // read from its select alone, every selector literal (the review of 5911e970e1, 2026-11-17): the edge's selector
        // a term of select, the libraries its deps' labels
        if (literalArgs(t.name(), t.text(), "--select-class").contains(cls)) {
            return true;
        }
        Set<String> deps = deps(t.text());
        if (inEdge) {
            return selectRead(t.name(), t.text()).edgeClasses() && deps.contains(":core_edge_tests_lib")
                    && cls.endsWith("Test");
        }
        return literalArgs(t.name(), t.text(), "--select-package").contains("com.legend")
                && deps.contains(":core_tests_lib") && cls.startsWith("com.legend.") && !excluded(rel, library);
    }

    /** The labels of {@code text}'s {@code deps} and {@code runtime_deps} lists. */
    static Set<String> deps(String text) {
        Set<String> out = new TreeSet<>();
        Matcher m = Pattern.compile("(?<![\\w])(?:runtime_deps|deps)\\s*=\\s*\\[(.*?)\\]", Pattern.DOTALL).matcher(text);
        while (m.find()) {
            Matcher label = Pattern.compile("\"([^\"]*)\"").matcher(m.group(1));
            while (label.find()) {
                out.add(label.group(1));
            }
        }
        return out;
    }

    /** Each class of {@code sources} ("core/src/test/java/..." -> text) tagged census or guardrail that no target of
     *  {@code build} runs in the gate ({@code gates}, gates/BUILD.bazel's text): one that selects it, that the gate
     *  reaches, that admits and does not drop the tag, excludes neither the class nor its package, and whose runner's
     *  name filter admits the class -- "path [tag]: why". */
    static List<String> orphans(Map<String, String> sources, Set<String> edge, String buildText, String gatesText) {
        String build = uncommented(buildText);
        String library = block(build, "core_tests_lib");
        if (library.isEmpty()) {
            throw new IllegalStateException("core_tests_lib is not read from the build file");
        }
        List<Target> targets = targets(build);
        Set<String> gated = gated(uncommented(gatesText));
        List<String> out = new ArrayList<>();
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
            boolean inEdge = edge.contains(rel);
            for (String tag : tags) {
                boolean runs = targets.stream().anyMatch(t -> selects(t, cls, rel, inEdge, library)
                        && gated.contains("//core:" + t.name()) && t.everywhere() && t.admits(tag)
                        && !t.dropped(build).contains(tag) && !t.excludes(cls) && t.named(cls));
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
        // the parse reads the real files (a misread would fail open: the reviews of e8bd35d19d and 60aa31029b)
        String library = block(uncommented(build), "core_tests_lib");
        assertTrue(excluded("src/test/java/com/legend/server/PostgresArmTest.java", library)
                && libraryExcludes(library).size() > 20, "core_tests_lib's exclude list is read");
        Set<String> targets = new TreeSet<>();
        targets(uncommented(build)).forEach(t -> targets.add(t.name()));
        assertTrue(targets.containsAll(List.of("census", "census_edge", "guardrails", "guardrails_edge")),
                "the targets are read: " + targets);
        Set<String> gated = gated(uncommented(gates));
        assertTrue(gated.containsAll(List.of("//core:census", "//core:census_edge", "//core:guardrails",
                "//core:guardrails_edge", "//core:core_tests")) && !gated.contains("//core:seam_benchmark"),
                "the gate's suites are read, heavy's left out: " + gated);
        Map<String, String> sources = new TreeMap<>();
        for (Path p : SourceFiles.under("core/src/test/java")) {
            String path = SourceFiles.path(p);
            if (path.endsWith(".java")) {
                sources.put(path, read(p));
            }
        }
        long tagged = sources.values().stream().filter(t -> TAG.matcher(t).find()).count();
        assertTrue(tagged > 40 && edge.size() > 50, "the tagged classes and the edge's sources are read: " + tagged
                + ", " + edge.size());
        assertEquals(List.of(), orphans(sources, edge, build, gates), "a census or guardrail class runs in no gate"
                + " target: select it (census_edge or guardrails_edge for the edge's library, census or guardrails for"
                + " the product's) in a target the gate's local and lanes suites reach");
    }

    private static String junit(String name, String body) {
        return "\njunit_test(\n    name = \"" + name + "\",\n" + body + "\n)\n";
    }

    private static String suite(String name, String body) {
        return "\ntest_suite(\n    name = \"" + name + "\",\n" + body + "\n)\n";
    }

    private static final String PLANTED_LIBRARY = "\nlegend_java_library(\n    name = \"core_tests_lib\",\n"
            + "    srcs = glob(\n        [\"src/test/java/**/*.java\"],\n        exclude = [\n"
            + "            # the slow tests are their own targets'\n"
            + "            \"src/test/java/com/legend/slow/*.java\",\n        ] + EDGE_TEST_SRCS,\n    ),\n)\n";

    private static final String PLANTED_BUILD = "\n_CORE_TEST_EXCLUDED_TAGS = [\n    \"census\",\n    \"guardrail\",\n]\n"
            + junit("census", "    select = [\"--select-package=com.legend\", \"--include-tag=census\"],\n"
                    + "    runtime_deps = [\":core_tests_lib\"],")
            + junit("guardrails", "    select = [\"--select-package=com.legend\", \"--include-tag=guardrail\"],\n"
                    + "    runtime_deps = [\":core_tests_lib\"],")
            + PLANTED_LIBRARY
            + junit("census_edge", "    select = edge_test_classes() + [\"--include-tag=census\"],\n"
                    + "    tags = [\"edge_test\"],\n    runtime_deps = [\":core_edge_tests_lib\"],")
            + junit("guardrails_edge", "    select = [\n        \"--select-class=com.legend.EdgeGuardTest\",\n"
                    + "        # \"--select-class=com.legend.CommentedGuardTest\",\n    ],")
            + junit("slow_census", "    select = [\"--select-class=com.legend.slow.KeptTest\"],")
            + junit("probe", "    select = [\"--select-class=com.legend.slow.ManualOnlyTest\"],\n"
                    + "    tags = [\"manual\"],")
            + junit("manual_listed", "    select = [\"--select-class=com.legend.slow.ManualListedTest\"],\n"
                    + "    tags = [\"manual\"],")
            + junit("off_gate", "    select = [\"--select-class=com.legend.slow.OffGateTest\"],")
            + junit("heavy_only", "    select = [\"--select-class=com.legend.slow.HeavyOnlyTest\"],")
            + junit("commented_out", "    select = [\"--select-class=com.legend.slow.CommentedOutTest\"],")
            + junit("dropping", "    select = [\"--select-class=com.legend.slow.DroppedTest\"],\n"
                    + "    exclude_tags = _CORE_TEST_EXCLUDED_TAGS,")
            + junit("dropping_joined", "    select = [\"--select-class=com.legend.slow.JoinedDropTest\"],\n"
                    + "    exclude_tags = [\"slow\"] + _CORE_TEST_EXCLUDED_TAGS,")
            + junit("dropping_arg", "    select = [\"--select-class=com.legend.slow.ArgDropTest\","
                    + " \"--exclude-tag=census\"],")
            + junit("excluding", "    select = [\"--select-class=com.legend.slow.NamedOutTest\","
                    + " \"--exclude-classname=.*NamedOutTest\"],")
            + junit("hashed", "    select = [\"--select-class=com.legend.slow.HashedTest\","
                    + " \"--exclude-classname=.*Hashed(Test| # )\"],")
            + junit("named_probe", "    select = [\"--select-class=com.legend.slow.ProbeCheck\","
                    + " \"--include-classname=.*Check\"],");

    private static final String PLANTED_GATES = suite("checks", "    tests = [\n"
            + "        \"//core:census\",\n        \"//core:guardrails\",\n        \"//core:census_edge\",\n"
            + "        \"//core:guardrails_edge\",\n        \"//core:slow_census\",\n        \"//core:manual_listed\",\n"
            + "        # \"//core:commented_out\",\n        \"//core:dropping\",\n        \"//core:dropping_joined\",\n"
            + "        \"//core:dropping_arg\",\n        \"//core:excluding\",\n        \"//core:hashed\",\n"
            + "        \"//core:named_probe\",\n    ],")
            + suite("local", "    tests = [\":checks\"],")
            + suite("lanes", "    tests = [\n        \":checks\",\n        \"//gates:core\",\n    ],")
            + suite("core", "    tests = [\"//core:core_tests\"],")
            + suite("heavy", "    tags = [\"manual\"],\n    tests = [\n        \"//core:probe\",\n"
                    + "        \"//core:heavy_only\",\n    ],");

    /** Planted: each way a class can be selected and still not run in the gate is named -- an edge census class not
     *  named *Test, an edge guardrail class no target names, a product class its library excludes, a class named only
     *  by a manual target in heavy, by a target only heavy lists, by a target no suite lists, by a target listed on a
     *  commented-out line, by targets dropping its tag (a list's name, a joined list, an --exclude-tag argument), by
     *  targets excluding its name (a {@code #} inside the pattern is the pattern's), a commented-out selector, and a
     *  class name the runner's filter skips; the same classes properly selected are not, nor is a manual target a
     *  reached suite lists, nor a class a target's own --include-classname admits. */
    @Test
    void theCheckBites() {
        Set<String> edge = Set.of("src/test/java/com/legend/EdgeCensusCheck.java",
                "src/test/java/com/legend/EdgeCensusTest.java", "src/test/java/com/legend/EdgeGuardTest.java",
                "src/test/java/com/legend/OrphanGuardTest.java", "src/test/java/com/legend/CommentedGuardTest.java");
        String census = "@Tag(\"census\") class X {}";
        String guard = "@Tag(\"guardrail\") class X {}";
        Map<String, String> sources = new TreeMap<>();
        for (String c : List.of("EdgeCensusCheck", "EdgeCensusTest", "ProductCensusTest", "slow/LeftOutTest",
                "slow/KeptTest", "slow/ManualOnlyTest", "slow/ManualListedTest", "slow/OffGateTest",
                "slow/HeavyOnlyTest", "slow/CommentedOutTest", "slow/DroppedTest", "slow/JoinedDropTest",
                "slow/ArgDropTest", "slow/NamedOutTest", "slow/HashedTest", "slow/ProbeCheck")) {
            sources.put(TEST_ROOT + "com/legend/" + c + ".java", census);
        }
        for (String c : List.of("EdgeGuardTest", "OrphanGuardTest", "CommentedGuardTest", "ProductGuard",
                "ProductGuardTest")) {
            sources.put(TEST_ROOT + "com/legend/" + c + ".java", guard);
        }
        sources.put(TEST_ROOT + "com/legend/Untagged.java", "class U {}");
        List<String> expected = new ArrayList<>();
        for (String c : List.of("CommentedGuardTest [guardrail]", "EdgeCensusCheck [census]",
                "OrphanGuardTest [guardrail]")) {
            expected.add(TEST_ROOT + "com/legend/" + c.replace(" [", ".java [") + ": in the edge's test library, run by"
                    + " no gate target");
        }
        for (String c : List.of("ProductGuard [guardrail]", "slow/ArgDropTest [census]", "slow/CommentedOutTest [census]",
                "slow/DroppedTest [census]", "slow/HashedTest [census]", "slow/HeavyOnlyTest [census]",
                "slow/JoinedDropTest [census]", "slow/LeftOutTest [census]", "slow/ManualOnlyTest [census]",
                "slow/NamedOutTest [census]", "slow/OffGateTest [census]")) {
            expected.add(TEST_ROOT + "com/legend/" + c.replace(" [", ".java [") + ": in the product's test library, run"
                    + " by no gate target");
        }
        assertEquals(expected, orphans(sources, edge, PLANTED_BUILD, PLANTED_GATES));
        Map<String, String> edgeCensus = Map.of(TEST_ROOT + "com/legend/EdgeCensusTest.java", census);
        List<String> edgeOrphan = List.of(TEST_ROOT + "com/legend/EdgeCensusTest.java [census]: in the edge's test"
                + " library, run by no gate target");
        assertEquals(edgeOrphan, orphans(edgeCensus, edge, PLANTED_BUILD, PLANTED_GATES.replace(
                "\"//core:census_edge\",", "")), "with census_edge out of the gate, the edge's census class is an orphan"
                + " (PlannerSurfaceTest's case)");
        assertEquals(edgeOrphan, orphans(edgeCensus, edge, PLANTED_BUILD.replace("[\":core_edge_tests_lib\"]", "[]"),
                PLANTED_GATES), "census_edge without the edge's test library runs no edge class");
        // the review of 5911e970e1 (2026-11-17): a selector is read from select alone -- one written in tags selects
        // nothing, and the edge's selector written outside select is no selector
        Map<String, String> kept = Map.of(TEST_ROOT + "com/legend/slow/KeptTest.java", census);
        assertEquals(List.of(TEST_ROOT + "com/legend/slow/KeptTest.java [census]: in the product's test library, run by"
                + " no gate target"), orphans(kept, edge, PLANTED_BUILD.replace(KEPT, "\"--select-class=com.legend.Other\"],"
                + "\n    tags = [\"--select-class=com.legend.slow.KeptTest\"]"), PLANTED_GATES),
                "a selector written in tags selects nothing");
        assertEquals(edgeOrphan, orphans(edgeCensus, edge, PLANTED_BUILD.replace("select = edge_test_classes() + ",
                "tags = edge_test_classes(),\n    select = "), PLANTED_GATES),
                "the edge's selector outside select selects nothing");
    }

    /** slow_census's selector, where the planted unread texts go. */
    private static final String KEPT = "\"--select-class=com.legend.slow.KeptTest\"]";

    /** Planted: build text the check reads and cannot parse fails it by name -- never read as an empty list. */
    @Test
    void unreadBuildTextFailsTheCheck() {
        Map<String, String> sources = Map.of(TEST_ROOT + "com/legend/slow/KeptTest.java", "@Tag(\"census\") class X {}");
        Set<String> edge = Set.of();
        List<String[]> cases = List.of(
                new String[] {PLANTED_BUILD.replace("] + EDGE_TEST_SRCS,", "] + SLOW + EDGE_TEST_SRCS,"), PLANTED_GATES,
                        "core_tests_lib's exclude list is joined with text the check does not read"},
                new String[] {PLANTED_BUILD.replace("\"src/test/java/com/legend/slow/*.java\",",
                        "\"src/test/java/com/legend/slow/*.java\",\n            SLOW_SOURCES,"), PLANTED_GATES,
                        "core_tests_lib's exclude list holds text the check does not read: SLOW_SOURCES,"},
                new String[] {PLANTED_BUILD.replace("[\"src/test/java/**/*.java\"],", "SOURCES,"), PLANTED_GATES,
                        "core_tests_lib's srcs are not glob"},
                new String[] {PLANTED_BUILD.replace("\"--select-class=com.legend.slow.KeptTest\"]",
                        "\"--select-class=com.legend.slow.KeptTest\"] + [\"--include-tag=\" + t for t in TAGS]"),
                        PLANTED_GATES, "slow_census has a --include-tag argument the check does not read"},
                new String[] {PLANTED_BUILD.replace("\"--select-class=com.legend.slow.KeptTest\"],",
                        "\"--select-class=com.legend.slow.KeptTest\"],\n    exclude_tags = SLOW_TAGS,"), PLANTED_GATES,
                        "slow_census's exclude_tags names SLOW_TAGS, a list the check does not read"},
                new String[] {PLANTED_BUILD.replace("\"--select-class=com.legend.slow.KeptTest\"],",
                        "\"--select-class=com.legend.slow.KeptTest\"],\n    exclude_tags = _CORE_TEST_EXCLUDED_TAGS[1:],"),
                        PLANTED_GATES, "slow_census's exclude_tags is followed by text the check does not read"},
                new String[] {PLANTED_BUILD, PLANTED_GATES.replace("tests = [\":checks\"],", "tests = CHECKS,"),
                        "gates/BUILD.bazel's suite local is reached and not read"},
                new String[] {PLANTED_BUILD, PLANTED_GATES.replace("\"//core:named_probe\",", "NAMED,"),
                        "gates/BUILD.bazel's suite checks lists text the check does not read: NAMED"},
                // the review of 260fdb147f (2026-11-17): a partly computed argument, a list holding a name or a
                // comprehension, a tag expression, a suite's tag filter and a platform restriction are unread
                new String[] {PLANTED_BUILD.replace(KEPT, KEPT + " + [\"--exclude-classname=\" + c for c in SLOW]"),
                        PLANTED_GATES, "slow_census has a --exclude-classname argument the check does not read"},
                new String[] {PLANTED_BUILD.replace(KEPT, KEPT + " + [\"--exclude-package=com.legend.\" + p for p in X]"),
                        PLANTED_GATES, "slow_census has a --exclude-package argument the check does not read"},
                new String[] {PLANTED_BUILD.replace(KEPT, KEPT + " + [\"--include-classname=.*\" + x]"),
                        PLANTED_GATES, "slow_census has a --include-classname argument the check does not read"},
                new String[] {PLANTED_BUILD.replace(KEPT + ",", KEPT + ",\n    exclude_tags = [\"slow\", _EXTRA],"),
                        PLANTED_GATES, "slow_census's exclude_tags holds text the check does not read: _EXTRA"},
                new String[] {PLANTED_BUILD.replace(KEPT + ",", KEPT + ",\n    exclude_tags = [t for t in"
                        + " _CORE_TEST_EXCLUDED_TAGS],"), PLANTED_GATES,
                        "slow_census's exclude_tags holds text the check does not read"},
                new String[] {PLANTED_BUILD.replace(KEPT, KEPT.replace("]", ", \"--exclude-tag=census | guardrail\"]")),
                        PLANTED_GATES, "slow_census filters by a tag expression the check does not read"},
                new String[] {PLANTED_BUILD, PLANTED_GATES.replace("name = \"checks\",", "name = \"checks\",\n"
                        + "    tags = [\"-edge_test\"],"),
                        "gates/BUILD.bazel's suite checks filters its tests by tag"},
                new String[] {PLANTED_BUILD.replace(KEPT + ",", KEPT + ",\n    target_compatible_with = ONLY_MAC,"),
                        PLANTED_GATES, "slow_census has a target_compatible_with the check does not read"},
                // the third review (2026-11-17, S10 and its NOTEs): an argument formatted by % or .format, a list or an
                // item joined from outside the target, a comprehension writing no flag, the other platform
                // restrictions, a suite's tags written without spaces
                new String[] {PLANTED_BUILD.replace(KEPT, KEPT.replace("]", ", \"--exclude-package=%s\" % _SLOW]")),
                        PLANTED_GATES, "slow_census has a --exclude-package argument the check does not read"},
                new String[] {PLANTED_BUILD.replace(KEPT, KEPT.replace("]", ", \"--exclude-classname={}\".format(p)]")),
                        PLANTED_GATES, "slow_census has a --exclude-classname argument the check does not read"},
                new String[] {PLANTED_BUILD.replace(KEPT, KEPT + " + _EXCLUDES"), PLANTED_GATES,
                        "slow_census's select holds text the check does not read: _EXCLUDES"},
                new String[] {PLANTED_BUILD.replace(KEPT, KEPT.replace("]", ", _EXTRA]")), PLANTED_GATES,
                        "slow_census's select holds text the check does not read: _EXTRA"},
                new String[] {PLANTED_BUILD.replace(KEPT, KEPT + " + [x for x in _EXCLUDES]"), PLANTED_GATES,
                        "slow_census's select holds text the check does not read: x for x in _EXCLUDES"},
                new String[] {PLANTED_BUILD.replace(KEPT + ",", KEPT + ",\n    compatible_with = ONLY_MAC,"),
                        PLANTED_GATES, "slow_census has a compatible_with the check does not read"},
                new String[] {PLANTED_BUILD.replace(KEPT + ",", KEPT + ",\n    restricted_to = ONLY_MAC,"),
                        PLANTED_GATES, "slow_census has a restricted_to the check does not read"},
                new String[] {PLANTED_BUILD.replace(KEPT + ",", KEPT + ",\n    exec_compatible_with = ONLY_MAC,"),
                        PLANTED_GATES, "slow_census has a exec_compatible_with the check does not read"},
                new String[] {PLANTED_BUILD, PLANTED_GATES.replace("name = \"checks\",", "name = \"checks\",\n"
                        + "    tags=[\"-edge_test\"],"),
                        "gates/BUILD.bazel's suite checks filters its tests by tag"},
                // the review of 5911e970e1 (2026-11-17): an escape in a string, a conditional item, a suite's tags
                // joined, a platform tag
                new String[] {PLANTED_BUILD.replace(KEPT, KEPT.replace("]", ", \"--exclude-classnam\\145=.*KeptTest\"]")),
                        PLANTED_GATES, "slow_census has an escape in a string the check does not read"},
                new String[] {PLANTED_BUILD.replace(KEPT, "\"--select-class=com.legend.Other\" if True else " + KEPT),
                        PLANTED_GATES, "slow_census has a --select-class argument the check does not read"},
                new String[] {PLANTED_BUILD, PLANTED_GATES.replace("name = \"checks\",", "name = \"checks\",\n"
                        + "    tags = [\"manual\"] + [\"-edge_test\"],"),
                        "gates/BUILD.bazel's suite checks filters its tests by tag"},
                new String[] {PLANTED_BUILD.replace(KEPT + ",", KEPT + ",\n    tags = [\"ci-linux-only\"],"),
                        PLANTED_GATES, "slow_census has a ci-linux-only tag the check does not read"});
        for (String[] c : cases) {
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> orphans(sources, edge, c[0], c[1]),
                    c[2]);
            assertTrue(e.getMessage().startsWith(c[2]), e.getMessage());
        }
    }
}
