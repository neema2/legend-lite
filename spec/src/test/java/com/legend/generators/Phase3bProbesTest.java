// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import com.legend.Compiler;
import com.legend.model.FunctionDefinition;
import com.legend.model.FunctionId;
import com.legend.model.PackageableElement;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Phase 3b's homework probes H1 and H2 (docs/build-inventory/program/PHASE_3B_HOMEWORK_2026_10_09.md), over the
 * same module closure the manifest-world census loads ({@link ManifestWorldCensusTest}): manual, in no lane.
 *
 * <p>H1 — the twin cause: for every function the system metamodel declares under an upstream name, each upstream
 * declaration of that name in the closure, with both sides' function id and the parameter-type spellings
 * {@code SystemMetamodel.shadows} compares. Expected: ids equal, spellings differ.
 *
 * <p>H2 — the other versions: every declaration at those names, its id, its file, and whether it twins the system
 * metamodel's version, so each gets a decision (a row, a refusal, or a signature fix).
 */
class Phase3bProbesTest {

    @Test
    @DisplayName("Phase 3b probes H1 and H2: the boot layer's versions against the closure's declarations")
    void twinsAndVersions() throws IOException {
        String target = System.getProperty("manifest.census");
        if (target == null || target.isEmpty()) {
            throw new IllegalStateException("-Dmanifest.census=<module>: run it as //spec:phase3b_probes");
        }
        Path engine = com.legend.testing.ProgramPaths.rootOf("legend.engine.root");
        Path pure = com.legend.testing.ProgramPaths.rootOf("legend.pure.root");
        List<ManifestWorldCensusTest.Module> world =
                ManifestWorldCensusTest.closure(target, ManifestWorldCensusTest.manifests(engine, pure));
        List<Compiler.ModelSource> sources = new ArrayList<>();
        for (ManifestWorldCensusTest.Module m : world) {
            try (Stream<Path> walk = Files.walk(m.root())) {
                for (Path f : walk.filter(p -> p.toString().endsWith(".pure")).sorted().toList()) {
                    sources.add(new Compiler.ModelSource(
                            m.name() + ":" + m.root().relativize(f).toString().replace('\\', '/'),
                            Files.readString(f, StandardCharsets.UTF_8)));
                }
            }
        }
        Compiler.ParsedModule module = Compiler.parseSources(sources, (n, err) -> { },
                com.legend.parser.Dialect.LEGEND_PLATFORM);
        Map<String, String> fileOf = module.model().elementSources();

        // the system metamodel's own functions, by name
        Map<String, List<FunctionDefinition>> system = new LinkedHashMap<>();
        for (PackageableElement e : com.legend.builtin.SystemMetamodel.elements()) {
            if (e instanceof FunctionDefinition f) {
                system.computeIfAbsent(f.qualifiedName(), k -> new ArrayList<>()).add(f);
            }
        }
        // the closure's declarations at those names
        Map<String, List<FunctionDefinition>> upstream = new LinkedHashMap<>();
        for (PackageableElement e : module.model().elements()) {
            if (e instanceof FunctionDefinition f && system.containsKey(f.qualifiedName())) {
                upstream.computeIfAbsent(f.qualifiedName(), k -> new ArrayList<>()).add(f);
            }
        }

        StringBuilder h1 = new StringBuilder("name\tside\tfunction id\tparameter spellings\tsame id as a system version\tsame spellings\tfile\n");
        StringBuilder h2 = new StringBuilder("name\tsystem versions\tupstream versions\tupstream version id\ttwin of a system version\tfile\n");
        int names = 0;
        int twinsById = 0;
        int twinsBySpelling = 0;
        for (Map.Entry<String, List<FunctionDefinition>> en : system.entrySet()) {
            String name = en.getKey();
            List<FunctionDefinition> ours = en.getValue();
            List<FunctionDefinition> theirs = upstream.getOrDefault(name, List.of());
            if (theirs.isEmpty()) {
                continue;   // a name only the system metamodel declares (meta::lite::...)
            }
            names++;
            for (FunctionDefinition s : ours) {
                h1.append(name).append("\tsystem\t").append(FunctionId.of(s).qualified()).append('\t')
                        .append(spellings(s)).append("\t-\t-\tSystemMetamodel\n");
            }
            for (FunctionDefinition u : theirs) {
                FunctionId uid = FunctionId.of(u);
                boolean sameId = ours.stream().anyMatch(s -> FunctionId.of(s).equals(uid));
                boolean sameSpelling = ours.stream().anyMatch(s -> spellings(s).equals(spellings(u)));
                if (sameId) {
                    twinsById++;
                }
                if (sameId && sameSpelling) {
                    twinsBySpelling++;
                }
                h1.append(name).append("\tupstream\t").append(uid.qualified()).append('\t').append(spellings(u))
                        .append('\t').append(sameId).append('\t').append(sameSpelling).append('\t')
                        .append(fileOf.getOrDefault(u.qualifiedName(), "?")).append('\n');
                h2.append(name).append('\t').append(ours.size()).append('\t').append(theirs.size()).append('\t')
                        .append(uid.qualified()).append('\t').append(sameId).append('\t')
                        .append(fileOf.getOrDefault(u.qualifiedName(), "?")).append('\n');
            }
        }
        String out = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (out != null) {
            Files.writeString(Path.of(out, "phase3b-h1-twins.tsv"), h1, StandardCharsets.UTF_8);
            Files.writeString(Path.of(out, "phase3b-h2-versions.tsv"), h2, StandardCharsets.UTF_8);
        }
        System.out.println("[phase3b] names the system metamodel and the closure both declare: " + names
                + "; upstream versions with a system twin by id: " + twinsById
                + "; of those, with the same parameter spellings (so shadows() sees them): " + twinsBySpelling);
        System.out.print(h1);
    }

    /** H4 — item 5b's blast radius: full names with overloads declared in more than one file whose files' import
     * lines differ (the imports are recorded per full name today, so the last file read wins for all of them). */
    @Test
    @DisplayName("Phase 3b probe H4: overloaded names whose overloads come from files with different imports")
    void overloadsAcrossFilesWithDifferentImports() throws IOException {
        String target = System.getProperty("manifest.census");
        Path engine = com.legend.testing.ProgramPaths.rootOf("legend.engine.root");
        Path pure = com.legend.testing.ProgramPaths.rootOf("legend.pure.root");
        List<ManifestWorldCensusTest.Module> world =
                ManifestWorldCensusTest.closure(target, ManifestWorldCensusTest.manifests(engine, pure));
        Map<String, java.util.Set<String>> importsOf = new LinkedHashMap<>();
        List<Compiler.ModelSource> sources = new ArrayList<>();
        java.util.regex.Pattern imp = java.util.regex.Pattern.compile("^\\s*import\\s+([^;]+);", java.util.regex.Pattern.MULTILINE);
        for (ManifestWorldCensusTest.Module m : world) {
            try (Stream<Path> walk = Files.walk(m.root())) {
                for (Path f : walk.filter(q -> q.toString().endsWith(".pure")).sorted().toList()) {
                    String name = m.name() + ":" + m.root().relativize(f).toString().replace('\\', '/');
                    String text = Files.readString(f, StandardCharsets.UTF_8);
                    sources.add(new Compiler.ModelSource(name, text));
                    java.util.Set<String> imps = new java.util.TreeSet<>();
                    java.util.regex.Matcher mm = imp.matcher(text);
                    while (mm.find()) {
                        imps.add(mm.group(1).trim());
                    }
                    importsOf.put(name, imps);
                }
            }
        }
        // each file parsed ALONE: the whole-module parse keeps one file per full name (the bug being measured)
        Map<String, java.util.Set<String>> filesOfName = new LinkedHashMap<>();
        for (Compiler.ModelSource src : sources) {
            Compiler.ParsedModule one = Compiler.parseSources(List.of(src), (n, err) -> { },
                    com.legend.parser.Dialect.LEGEND_PLATFORM);
            for (PackageableElement e : one.model().elements()) {
                if (e instanceof FunctionDefinition f) {
                    filesOfName.computeIfAbsent(f.qualifiedName(), k -> new java.util.TreeSet<>()).add(src.name());
                }
            }
        }
        StringBuilder h4 = new StringBuilder("name\tfiles\tdistinct import sets\n");
        int multiFile = 0;
        int differingImports = 0;
        for (Map.Entry<String, java.util.Set<String>> en : filesOfName.entrySet()) {
            if (en.getValue().size() < 2) {
                continue;
            }
            multiFile++;
            java.util.Set<java.util.Set<String>> sets = new java.util.HashSet<>();
            for (String file : en.getValue()) {
                sets.add(importsOf.getOrDefault(file, java.util.Set.of()));
            }
            if (sets.size() > 1) {
                differingImports++;
                h4.append(en.getKey()).append('\t').append(String.join(" | ", en.getValue())).append('\t')
                        .append(sets.size()).append('\n');
            }
        }
        String out = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (out != null) {
            Files.writeString(Path.of(out, "phase3b-h4-imports.tsv"), h4, StandardCharsets.UTF_8);
        }
        System.out.println("[phase3b] H4: function names declared in more than one file: " + multiFile
                + "; of those, from files with different import lines: " + differingImports);
    }

    /** The parameter-type spellings {@code SystemMetamodel.shadows} compares (its private {@code spelling}). */
    private static String spellings(FunctionDefinition f) {
        return f.parameters().stream().map(p -> spelling(p.type())).collect(Collectors.joining(", ", "(", ")"));
    }

    private static String spelling(com.legend.protocol.TypeExpression t) {
        return switch (t) {
            case com.legend.protocol.TypeExpression.NameRef nr -> nr.name();
            case com.legend.protocol.TypeExpression.Generic g -> g.name() + "<"
                    + g.arguments().stream().map(Phase3bProbesTest::spelling).collect(Collectors.joining(",")) + ">";
            default -> String.valueOf(t);
        };
    }
}
