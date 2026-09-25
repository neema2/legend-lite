// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.testing.Repo;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * THE MANIFEST-WORLD CENSUS (2026-09-25): load a module's world the way
 * upstream does — the module and the dependency closure its
 * {@code <name>.definition.json} declares, every {@code .pure} file of every
 * module, tests included — and type every body once. No hand list of files:
 * the manifests are the loading rule. Each module gets a row: files, parse
 * walls, model walls, bodies typed OK / failed / walled; the failures are
 * bucketed by reason and the top messages listed.
 *
 * <p>A measurement, not a gate: it runs only with
 * {@code -Dmanifest.census=<module>} (e.g. {@code core_relational}) and
 * writes {@code manifest-census-<module>.txt} to the test outputs. It
 * exists to answer one question before the corpus loader is switched to the
 * manifests: how much of the closure the compiler can already carry.
 */
public class ManifestWorldCensusTest {

    /** legend-pure's {@code platform} repository has no definition.json in the
     *  tree (it is declared in Java upstream); its resource root is known. */
    private static final String PLATFORM_ROOT =
            "legend-pure-core/legend-pure-m3-core/src/main/resources/platform";

    private static final Pattern NAME = Pattern.compile("\"name\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern DEPS = Pattern.compile("\"dependencies\"\\s*:\\s*\\[([^\\]]*)\\]", Pattern.DOTALL);
    private static final Pattern DEP = Pattern.compile("\"([^\"]+)\"");

    record Module(String name, Path root, List<String> dependencies) {
    }

    /** Every manifest under the two checkouts, by module name. */
    static Map<String, Module> manifests(Path engine, Path pure) throws IOException {
        Map<String, Module> out = new TreeMap<>();
        for (Path tree : List.of(engine, pure)) {
            try (Stream<Path> walk = Files.walk(tree)) {
                for (Path p : walk.filter(x -> x.getFileName().toString().endsWith(".definition.json")).toList()) {
                    if (p.toString().contains("/target/")) {
                        continue;
                    }
                    String text = Files.readString(p, StandardCharsets.UTF_8);
                    Matcher n = NAME.matcher(text);
                    if (!n.find()) {
                        continue;
                    }
                    List<String> deps = new ArrayList<>();
                    Matcher d = DEPS.matcher(text);
                    if (d.find()) {
                        Matcher each = DEP.matcher(d.group(1));
                        while (each.find()) {
                            deps.add(each.group(1));
                        }
                    }
                    out.putIfAbsent(n.group(1), new Module(n.group(1), p.getParent(), deps));
                }
            }
        }
        out.putIfAbsent("platform", new Module("platform", pure.resolve(PLATFORM_ROOT), List.of()));
        return out;
    }

    /** The module and its dependency closure, dependencies first (a stable order). */
    static List<Module> closure(String name, Map<String, Module> all) {
        LinkedHashSet<String> seen = new LinkedHashSet<>();
        walk(name, all, seen);
        List<Module> out = new ArrayList<>();
        for (String n : seen) {
            Module m = all.get(n);
            if (m == null) {
                throw new IllegalStateException("manifest census: module '" + n + "' has no manifest in the checkouts");
            }
            out.add(m);
        }
        return out;
    }

    private static void walk(String name, Map<String, Module> all, Set<String> seen) {
        if (seen.contains(name)) {
            return;
        }
        Module m = all.get(name);
        if (m != null) {
            for (String d : m.dependencies()) {
                walk(d, all, seen);
            }
        }
        seen.add(name);
    }

    @Test
    @DisplayName("manifest-world census: a module's dependency closure loaded whole, every body typed once")
    void census() throws IOException {
        String target = System.getProperty("manifest.census");
        Assumptions.assumeTrue(target != null && !target.isEmpty(), "-Dmanifest.census=<module> not set");
        Path engine = com.legend.testing.Upstream.engine();
        Path pure = com.legend.testing.Upstream.pure();
        Map<String, Module> all = manifests(engine, pure);
        List<Module> world = closure(target, all);

        // 1. LOAD — every .pure file of every module in the closure, named
        // <module>:<relative path>; parse walls and model walls recorded per module
        long t0 = System.nanoTime();
        List<Compiler.ModelSource> sources = new ArrayList<>();
        Map<String, Integer> filesOf = new LinkedHashMap<>();
        for (Module m : world) {
            int n = 0;
            if (!Files.isDirectory(m.root())) {
                throw new IllegalStateException("manifest census: module root missing: " + m.root());
            }
            try (Stream<Path> walk = Files.walk(m.root())) {
                for (Path f : walk.filter(p -> p.toString().endsWith(".pure")).sorted().toList()) {
                    sources.add(new Compiler.ModelSource(m.name() + ":" + m.root().relativize(f).toString().replace('\\', '/'),
                            Files.readString(f, StandardCharsets.UTF_8)));
                    n++;
                }
            }
            filesOf.put(m.name(), n);
        }
        int fileCount = sources.size();
        long tRead = System.nanoTime() - t0;
        // TIMING (nanoTime, this JVM, warm classes): three full parses, the
        // fastest reported — the loading cost a corpus lane would pay per run
        long tParse = Long.MAX_VALUE;
        for (int i = 0; i < 3; i++) {
            long p0 = System.nanoTime();
            Compiler.parseSources(sources, (n, err) -> { }, com.legend.parser.Dialect.LEGEND_PLATFORM);
            tParse = Math.min(tParse, System.nanoTime() - p0);
        }
        long tLoop0 = System.nanoTime();
        int rounds = 0;
        List<String> loadWalls = new ArrayList<>();
        Map<String, String> elementSources = Map.of();
        ModelContext ctx = null;
        List<String> duplicates = List.of();
        for (int round = 0; round < 4000 && ctx == null; round++) {
            rounds++;
            List<String> parseWalls = new ArrayList<>();
            Compiler.ParsedModule module = Compiler.parseSources(sources,
                    (name, err) -> parseWalls.add(name + ": PARSE " + SpecBodyCensusTest.first(err)),
                    com.legend.parser.Dialect.LEGEND_PLATFORM);
            List<PackageableElement> kept = module.model().elements().stream()
                    .filter(e -> !(e instanceof com.legend.model.NativeFunctionDefinition))
                    .toList();
            ParsedModel pruned = new ParsedModel(kept, module.model().imports(),
                    module.model().source(), module.model().elementOffsets(),
                    module.model().elementImports(), module.model().elementSources(),
                    module.model().unclaimedSections());
            try {
                ctx = Compiler.buildModel(pruned);
                loadWalls.addAll(parseWalls);
                elementSources = module.model().elementSources();
                duplicates = module.duplicateElements();
            } catch (com.legend.error.ModelException e) {
                String el = e.element();
                String src = el == null ? null : module.model().elementSources().get(el);
                if (src == null && el != null && el.indexOf('$') > 0) {
                    // a SYNTHESIZED element (a mapping's per-class piece,
                    // <owner>$class$<Name>) is its owner's: drop the owner's source
                    src = module.model().elementSources().get(el.substring(0, el.indexOf('$')));
                }
                if (src == null) {
                    String simple = el == null ? "" : el.substring(el.lastIndexOf(':') + 1);
                    List<String> near = module.model().elementSources().keySet().stream()
                            .filter(k -> !simple.isEmpty() && k.endsWith(simple)).limit(5).toList();
                    throw new IllegalStateException("manifest census: model wall with no source — element="
                            + el + " message=" + SpecBodyCensusTest.first(e.getMessage())
                            + " nearKeys=" + near + " round=" + round, e);
                }
                final String drop = src;
                sources.removeIf(s -> s.name().equals(drop));
                loadWalls.add(drop + ": MODEL " + SpecBodyCensusTest.first(e.getMessage()));
            }
        }
        if (ctx == null) {
            throw new IllegalStateException("manifest census: the model did not converge");
        }
        long tLoop = System.nanoTime() - tLoop0;
        // the model build ALONE on the converged source set (walls already out):
        // what a loader that remembers its walls pays
        long tModel = Long.MAX_VALUE;
        for (int i = 0; i < 3; i++) {
            long m0 = System.nanoTime();
            Compiler.ParsedModule again = Compiler.parseSources(sources, (n, err) -> { },
                    com.legend.parser.Dialect.LEGEND_PLATFORM);
            List<PackageableElement> keptAgain = again.model().elements().stream()
                    .filter(e -> !(e instanceof com.legend.model.NativeFunctionDefinition)).toList();
            Compiler.buildModel(new ParsedModel(keptAgain, again.model().imports(),
                    again.model().source(), again.model().elementOffsets(),
                    again.model().elementImports(), again.model().elementSources(),
                    again.model().unclaimedSections()));
            tModel = Math.min(tModel, System.nanoTime() - m0);
        }
        String timing = String.format("read %d files %.2fs | parse (best of 3) %.2fs | parse+model on the"
                + " converged set (best of 3) %.2fs | wall-finding loop %d rounds %.1fs",
                fileCount, tRead / 1e9, tParse / 1e9, tModel / 1e9, rounds, tLoop / 1e9);
        System.out.println("[manifest-census] timing: " + timing);
        if (System.getProperty("manifest.census.timing") != null) {
            System.out.println("[manifest-census] " + target + ": modules=" + world.size() + " files=" + fileCount
                    + " loadWalls=" + loadWalls.size() + " (timing only, bodies not typed)");
            return;
        }

        // 2. TYPE — every bodied function, once
        SpecCompiler specs = new SpecCompiler(ctx);
        Map<String, int[]> perModule = new LinkedHashMap<>();   // ok, failed, walled, natives
        for (Module m : world) {
            perModule.put(m.name(), new int[4]);
        }
        Map<String, String> failures = new TreeMap<>();
        Map<String, String> walled = new TreeMap<>();
        Map<String, Integer> byReason = new TreeMap<>();
        Map<String, Integer> byMessage = new TreeMap<>();
        Map<String, Map<String, Integer>> reasonByModule = new TreeMap<>();
        int ok = 0;
        for (String fqn : new java.util.TreeSet<>(ctx.functionFqns())) {
            String src = elementSources.getOrDefault(fqn, "?:?");
            String mod = src.substring(0, Math.max(0, src.indexOf(':')));
            int[] row = perModule.computeIfAbsent(mod, k -> new int[4]);
            List<TypedFunction> overloads;
            try {
                overloads = ctx.findFunction(fqn);
            } catch (RuntimeException e) {
                failures.put(fqn, mod + " | SIGNATURE " + SpecBodyCensusTest.first(e.getMessage()));
                bump(byReason, "signature");
                bump(reasonByModule.computeIfAbsent(mod, k -> new TreeMap<>()), "signature");
                row[1]++;
                continue;
            }
            for (TypedFunction fn : overloads) {
                if (fn.isNative() || fn.body().isEmpty()
                        || ctx.implementations().runsByRule(fn.definition())) {
                    row[3]++;
                    continue;
                }
                String id = fn.qualifiedName() + "(" + fn.parameters().stream()
                        .map(p -> p.type().typeName() + p.multiplicity().text())
                        .collect(java.util.stream.Collectors.joining(",")) + ")";
                try {
                    specs.compile(fn);
                    ok++;
                    row[0]++;
                } catch (StackOverflowError so) {
                    // a compiler bug the census must SURVIVE to report: the
                    // deepest platform frame names the recursion
                    String where = "";
                    for (StackTraceElement f : so.getStackTrace()) {
                        if (f.getClassName().startsWith("com.legend.")) {
                            where = f.getClassName().substring(f.getClassName().lastIndexOf('.') + 1)
                                    + "." + f.getMethodName();
                            break;
                        }
                    }
                    failures.put(id, mod + " | recursion | StackOverflowError @ " + where);
                    bump(byReason, "recursion");
                    bump(reasonByModule.computeIfAbsent(mod, k -> new TreeMap<>()), "recursion");
                    bump(byMessage, "recursion :: StackOverflowError @ " + where);
                    row[1]++;
                } catch (RuntimeException e) {
                    String msg = SpecBodyCensusTest.first(e.getMessage());
                    if (e instanceof com.legend.error.WalledBodyException) {
                        walled.put(id, mod + " | " + msg);
                        row[2]++;
                        continue;
                    }
                    String reason = SpecBodyCensusTest.reasonClass(msg);
                    failures.put(id, mod + " | " + reason + " | " + e.getClass().getSimpleName() + " " + msg);
                    bump(byReason, reason);
                    bump(reasonByModule.computeIfAbsent(mod, k -> new TreeMap<>()), reason);
                    bump(byMessage, reason + " :: " + normalize(msg));
                    row[1]++;
                }
            }
        }

        // 3. REPORT
        List<String> out = new ArrayList<>();
        out.add("# manifest-world census — " + target + " — " + java.time.LocalDate.now());
        out.add("# modules=" + world.size() + " files=" + fileCount + " loadWalls=" + loadWalls.size()
                + " duplicates=" + duplicates.size() + " bodies OK=" + ok + " FAILED=" + failures.size()
                + " WALLED=" + walled.size());
        out.add("# by reason: " + byReason);
        out.add("");
        out.add("## per module: files | parse+model walls | bodies ok | failed | walled | native-or-by-rule | failed by reason");
        for (Module m : world) {
            int[] r = perModule.getOrDefault(m.name(), new int[4]);
            long walls = loadWalls.stream().filter(w -> w.startsWith(m.name() + ":")).count();
            out.add(String.format("%-66s %5d | %4d | %5d | %5d | %4d | %5d | %s", m.name(),
                    filesOf.getOrDefault(m.name(), 0), walls, r[0], r[1], r[2], r[3],
                    reasonByModule.getOrDefault(m.name(), Map.of())));
        }
        out.add("");
        out.add("## top failure messages (normalized)");
        byMessage.entrySet().stream()
                .sorted((a, b) -> b.getValue() - a.getValue())
                .limit(60)
                .forEach(e -> out.add(String.format("%5d  %s", e.getValue(), e.getKey())));
        out.add("");
        out.add("## load walls (" + loadWalls.size() + ")");
        out.addAll(loadWalls);
        out.add("");
        out.add("## duplicates the parser reported (" + duplicates.size() + ")");
        out.addAll(duplicates);
        out.add("");
        out.add("## WALLED bodies (" + walled.size() + ")");
        walled.forEach((k, v) -> out.add("WALLED | " + k + " | " + v));
        out.add("");
        out.add("## typing failures (" + failures.size() + ")");
        failures.forEach((k, v) -> out.add(k + " :: " + v));
        Files.createDirectories(Repo.outDir());
        Files.write(Repo.out("manifest-census-" + target + ".txt"), out);
        System.out.println("[manifest-census] " + target + ": modules=" + world.size() + " files=" + fileCount
                + " loadWalls=" + loadWalls.size() + " ok=" + ok + " failed=" + failures.size()
                + " walled=" + walled.size() + " byReason=" + byReason);
        // SHRINK-ONLY when it runs (charter step 7 / D7, measured 2026-09-25 at
        // engine 4.145.0): the walls and the failing bodies only fall
        if (target.equals("core_relational")) {
            org.junit.jupiter.api.Assertions.assertTrue(loadWalls.size() <= 32,
                    "manifest-world load walls grew: " + loadWalls.size() + " > 32");
            org.junit.jupiter.api.Assertions.assertTrue(failures.size() <= 1447,
                    "manifest-world failing bodies grew: " + failures.size() + " > 1447");
        }
    }

    /** A message with its specifics blanked, so the same kind of failure counts once. */
    private static String normalize(String msg) {
        String m = msg.replaceAll("'[^']*'", "'…'").replaceAll("\\b\\d+\\b", "N");
        return m.length() > 140 ? m.substring(0, 140) : m;
    }

    private static void bump(Map<String, Integer> m, String k) {
        m.merge(k, 1, Integer::sum);
    }
}
