package com.legend.integration;

import java.nio.file.*;
import java.util.*;

/**
 * Loads the stress corpus for the legend-lite side: the LINKED PROJECTS first (the
 * corpus's mappings include theirs), then every stress file, minus what legend-lite
 * cannot yet build a model from.
 *
 * <p>Both legend-lite tests need the same model, and both need the same exclusions — an
 * unbuildable element does not fail one service, it fails the whole model load, so a
 * single unsupported construct would take the entire legend-lite side of the corpus down
 * with it.
 *
 * <p>Every exclusion is a legend-lite GAP, not a corpus error: legend-engine runs these
 * files. Each carries its reason, {@link LegendLiteGapTest} asserts the gap is still
 * open, and removing one must be a deliberate act.
 */
final class StressCorpus {

    /** The projects the corpus depends on, DEPENDENCIES BEFORE DEPENDENTS: core/stress.bzl's one list (Bazel
     *  workplan P2-02), read from its committed JSON, stress-layout.json beside this class. */
    static final List<String> LINKED_PROJECTS = linkedProjects();

    private static List<String> linkedProjects() {
        try (var in = StressCorpus.class.getResourceAsStream("stress-layout.json")) {
            if (in == null) {
                throw new IllegalStateException("stress-layout.json is not on the classpath (core/stress.bzl writes it)");
            }
            var layout = (com.legend.json.Json.Obj) com.legend.json.Json.parse(
                    new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
            return layout.getArr("linked_projects").items().stream()
                    .map(n -> ((com.legend.json.Json.Str) n).value())
                    .toList();
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** A project's files in SECTION order (model, store, mapping): a file with no
     *  {@code ###} header inherits the section the previous file left open. */
    private static final List<String> SECTION_ORDER = List.of("model.pure", "store.pure",
            "mapping.pure");

    private StressCorpus() {
    }

    /** One file of the corpus: its name (the file name, as a model source names it) and its text. */
    record File(String name, String text) {
    }

    /** Every source file the lite side loads, in load order, read from the classpath (Bazel workplan P3-05, A11:
     *  core_tests_lib carries the stress files, their table of contents stress-index.txt, and the linked projects'
     *  files under projects/), never from a directory. */
    static List<File> files() throws Exception {
        List<File> out = new ArrayList<>();
        for (String project : LINKED_PROJECTS) {
            for (String f : SECTION_ORDER) {
                String text = resource("/projects/" + project + "/" + f, false);
                if (text != null) {
                    out.add(new File(f, text));
                }
            }
        }
        String index = java.util.Objects.requireNonNull(resource("stress-index.txt", true));
        for (String name : index.lines().filter(l -> !l.isBlank()).sorted().toList()) {
            if (StressExclusions.EXCLUDED.containsKey(name)) {
                continue;
            }
            out.add(new File(name, java.util.Objects.requireNonNull(resource("/stress/" + name, true))));
        }
        return out;
    }

    private static String resource(String name, boolean required) throws java.io.IOException {
        try (var in = StressCorpus.class.getResourceAsStream(name)) {
            if (in == null) {
                if (required) {
                    throw new IllegalStateException(name + " is not on the classpath (core_tests_lib's resources)");
                }
                return null;
            }
            return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    static String model() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (File f : files()) {
            sb.append(f.text()).append("\n");
        }
        return sb.toString();
    }

    /** The corpus as SOURCES, the {@code overrides} FIRST: the model builder keeps
     *  the first definition of an element and reports the dropped one, so a
     *  damaged {@code ###Data} element in an override file replaces the seed of
     *  the same name without a corpus file changing (rebuild D23). */
    static List<com.legend.Compiler.ModelSource> sources(List<Path> overrides) throws Exception {
        List<com.legend.Compiler.ModelSource> out = new ArrayList<>();
        for (Path p : overrides) {
            out.add(new com.legend.Compiler.ModelSource(p.toString(), Files.readString(p)));
        }
        for (File f : files()) {
            out.add(new com.legend.Compiler.ModelSource(f.name(), f.text()));
        }
        return out;
    }

    static void reportExclusions() {
        StressExclusions.EXCLUDED.forEach((f, why) ->
                System.out.println("  EXCLUDED " + f + ": " + why));
    }
}
