package com.legend.rcorpus;

import com.legend.Compiler;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** THE EAGER COMPILE — a MEASUREMENT, not a gate (USER 2026-09-09: "before
 * we add to gate let's do all the work, then decide"): every body in the
 * corpus's compiled world typed up front through Compiler.compileAllBodies.
 * Run by name only ({@code -Dtest=EagerCorpusCompileProbe}; the Probe suffix
 * keeps it out of surefire's default set). Writes target/eager-corpus.txt
 * (by reason, by package, by source file, every failing body).
 * {@code -Deager.world2=1} adds the second world (the corpus + legend-pure's
 * platform packages whole). Measured 2026-09-09: 9,099 bodies, 1,605 fail
 * (COMPILE_EVERYTHING_HOMEWORK §10). */
class EagerCorpusCompileProbe {
    /** A platform-independent path string: '/' separators always. A Path in a
     *  concatenation converts with the PLATFORM separator, so an id built that
     *  way differs on Windows (census 2026-09-09). */
    private static String slash(java.nio.file.Path p) {
        return p.toString().replace(java.io.File.separatorChar, '/');
    }


    @Test
    void eagerCompileEverything() throws Exception {
        long t0 = System.nanoTime();
        MinimalCorpus corpus = new MinimalCorpus();
        long t1 = System.nanoTime();
        Map<String, String> walls = Compiler.compileAllBodies(corpus.context());
        long t2 = System.nanoTime();
        int total = 0;
        for (String fqn : corpus.context().functionFqns()) {
            try {
                for (var f : corpus.context().findFunction(fqn)) {
                    if (f.body().isPresent()) {
                        total++;
                    }
                }
            } catch (RuntimeException e) {
                total++;
            }
        }
        Map<String, Integer> byReason = new TreeMap<>();
        Map<String, Integer> byPackage = new TreeMap<>();
        for (var e : walls.entrySet()) {
            byReason.merge(reasonClass(e.getValue()), 1, Integer::sum);
            String k = e.getKey();
            String pkg = k.contains("::") ? k.substring(0, k.indexOf("::", k.indexOf("::") + 2)) : k;
            byPackage.merge(pkg, 1, Integer::sum);
        }
        Map<String, Integer> bySource = new TreeMap<>();
        Map<String, Integer> bodiesBySource = new TreeMap<>();
        for (String fqn : corpus.context().functionFqns()) {
            String src = corpus.elementSources().getOrDefault(fqn, "?");
            bodiesBySource.merge(src, 1, Integer::sum);
        }
        for (String k : walls.keySet()) {
            String fqn = k.contains("(") ? k.substring(0, k.indexOf('(')) : k;
            bySource.merge(corpus.elementSources().getOrDefault(fqn, "?"), 1, Integer::sum);
        }
        // THE FAMILIES (COMPILE_EVERYTHING_HOMEWORK §10.5, the last step): a
        // failing NON-TEST body is either the engine's machinery loaded because
        // it shares a source tree with the tests — walled BY FAMILY with its
        // reason, never carried broken — or the RESIDUE: ours to fix, named by
        // file. Test bodies are the roster's (they run; the roster pins them).
        Map<String, Integer> families = new TreeMap<>();
        Map<String, Integer> residueBySource = new TreeMap<>();
        List<String> residue = new ArrayList<>();
        for (var e : walls.entrySet()) {
            String k = e.getKey(); String fqn = k.contains("(") ? k.substring(0, k.indexOf('(')) : k;
            String fam = family(fqn, corpus.elementSources().getOrDefault(fqn, "?"));
            families.merge(fam, 1, Integer::sum);
            if (fam.startsWith("RESIDUE")) {
                residue.add(k + " :: " + e.getValue().replace('\n', ' '));
                residueBySource.merge(corpus.elementSources().getOrDefault(fqn, "?"), 1, Integer::sum);
            }
        }
        List<String> out = new ArrayList<>();
        out.add("# families: " + families);
        out.add("# RESIDUE (non-test, outside the walled families) = " + residue.size()
                + " by source: " + residueBySource.entrySet().stream()
                        .sorted((x, y) -> y.getValue() - x.getValue()).toList());
        Files.createDirectories(Path.of("target"));
        Files.write(Path.of("target/eager-residue.txt"), residue);
        out.add("# by source (failed/total): " + bySource.entrySet().stream()
                .sorted((x, y) -> y.getValue() - x.getValue())
                .map(e -> e.getKey() + "=" + e.getValue() + "/" + bodiesBySource.getOrDefault(e.getKey(), 0))
                .toList());
        out.add("# eager corpus compile — bodies=" + total + " failed=" + walls.size()
                + " build=" + (t1 - t0) / 1_000_000 + "ms typeAll=" + (t2 - t1) / 1_000_000 + "ms");
        out.add("# by reason: " + byReason);
        out.add("# by top package: " + byPackage);
        out.add("");
        walls.forEach((k, v) -> out.add(k + " :: " + v.replace('\n', ' ')));
        Files.createDirectories(Path.of("target"));
        if (!"1".equals(System.getProperty("eager.world2"))) {
            Files.write(Path.of("target/eager-corpus.txt"), out);
            System.out.println(out.get(0));
            System.out.println(out.get(1));
            return;
        }
        // WORLD 2: the corpus + legend-pure's platform packages WHOLE (their
        // bodied FUNCTIONS, which the prelude does not carry) — what closes?
        Path pure = Path.of(System.getProperty("legend.pure.root",
                System.getProperty("user.home") + "/legend/legend-pure"));
        List<Compiler.ModelSource> w2 = new ArrayList<>(corpus.sources());
        for (String r : com.legend.tools.SpecBodyCensusTest.PLATFORM_ROOTS) {
            Path root = pure.resolve(r);
            if (!Files.isDirectory(root)) continue;
            try (var walk = Files.walk(root)) {
                for (Path f : walk.filter(x -> x.toString().endsWith(".pure"))
                        .sorted(java.util.Comparator.comparing(EagerCorpusCompileProbe::slash))
                        .toList()) {
                    w2.add(new Compiler.ModelSource(
                            "platform:" + slash(root.relativize(f)),
                            Files.readString(f)));
                }
            }
        }
        com.legend.compiler.element.ModelContext ctx2 = null;
        List<String> w2walls = new ArrayList<>();
        for (int round = 0; round < 400 && ctx2 == null; round++) {
            List<String> pw = new ArrayList<>();
            Compiler.ParsedModule m2 = Compiler.parseSources(w2, (n, e) -> pw.add(n + ": " + e), com.legend.parser.Dialect.LEGEND_PLATFORM);
            var kept = m2.model().elements().stream().filter(e -> !(e instanceof com.legend.model.NativeFunctionDefinition)).toList();
            var pruned = new com.legend.model.ParsedModel(kept, m2.model().imports(), m2.model().source(), m2.model().elementOffsets(), m2.model().elementImports(), m2.model().elementSources(), m2.model().unclaimedSections());
            try {
                // the corpus's own tolerant build (poison, don't drop)
                Compiler.BuiltModule b2 = Compiler.buildModule(pruned);
                ctx2 = b2.context(); w2walls.addAll(pw);
                w2walls.add("element walls: " + b2.walls().size());
            } catch (com.legend.error.ModelException e) {
                String el = e.element(); String src = el == null ? null : m2.model().elementSources().get(el);
                if (src == null) throw e;
                final String drop = src; w2.removeIf(s -> s.name().equals(drop)); w2walls.add(drop + ": MODEL " + e.getMessage());
            }
        }
        Map<String, String> walls2 = Compiler.compileAllBodies(ctx2);
        Map<String, Integer> byReason2 = new TreeMap<>();
        for (var e : walls2.entrySet()) byReason2.merge(reasonClass(e.getValue()), 1, Integer::sum);
        // failures that closed: keys in world 1 absent in world 2
        long closed = walls.keySet().stream().filter(k -> !walls2.containsKey(k)).count();
        out.add("# WORLD 2 (corpus + legend-pure platform functions): failed=" + walls2.size() + " closed-from-world-1=" + closed + " worldWalls=" + w2walls.size());
        out.add("# WORLD 2 by reason: " + byReason2);
        Map<String, Integer> names2 = new TreeMap<>();
        for (var v : walls2.values()) { var mm = java.util.regex.Pattern.compile("unknown function '([^']+)'").matcher(v); if (mm.find()) { String n = mm.group(1); names2.merge(n.substring(n.lastIndexOf(':') + 1), 1, Integer::sum); } }
        out.add("# WORLD 2 top unknown functions: " + names2.entrySet().stream().sorted((x, y) -> y.getValue() - x.getValue()).limit(25).toList());
        // NEW in world 2: bodies that fail there and did not exist / did not fail in world 1
        Map<String, Integer> newReason = new TreeMap<>(); Map<String, Integer> newTypes = new TreeMap<>(); Map<String, Integer> newMsgs = new TreeMap<>();
        Map<String, Integer> newSrc = new TreeMap<>();
        for (var e : walls2.entrySet()) {
            if (walls.containsKey(e.getKey())) continue;
            String v = e.getValue(); newReason.merge(reasonClass(v), 1, Integer::sum);
            var mt = java.util.regex.Pattern.compile("(?:Unknown type: '|')([^']+)' is not a known|Unknown type: '([^']+)'").matcher(v);
            if (mt.find()) { String n = mt.group(1) != null ? mt.group(1) : mt.group(2); newTypes.merge(n.substring(n.lastIndexOf(':') + 1), 1, Integer::sum); }
            String msg = v.replaceAll("'[^']*'", "'_'"); newMsgs.merge(msg.length() > 110 ? msg.substring(0, 110) : msg, 1, Integer::sum);
            String k = e.getKey(); String fqn = k.contains("(") ? k.substring(0, k.indexOf('(')) : k;
            String pkg = fqn.contains("::") ? fqn.substring(0, Math.min(fqn.length(), fqn.indexOf("::", fqn.indexOf("::", fqn.indexOf("::") + 2) + 2) > 0 ? fqn.indexOf("::", fqn.indexOf("::", fqn.indexOf("::") + 2) + 2) : fqn.length())) : fqn;
            newSrc.merge(pkg, 1, Integer::sum);
        }
        out.add("# WORLD 2 NEW failures by reason: " + newReason);
        out.add("# WORLD 2 NEW by package: " + newSrc.entrySet().stream().sorted((x, y) -> y.getValue() - x.getValue()).limit(14).toList());
        out.add("# WORLD 2 NEW top unknown types: " + newTypes.entrySet().stream().sorted((x, y) -> y.getValue() - x.getValue()).limit(20).toList());
        out.add("# WORLD 2 NEW top messages: " + newMsgs.entrySet().stream().sorted((x, y) -> y.getValue() - x.getValue()).limit(12).toList());
        out.add("# WORLD 2 walls: " + w2walls);
        Files.write(Path.of("target/eager-corpus.txt"), out);
        for (String l : out) if (l.startsWith("# WORLD 2")) System.out.println(l);
    }


    /** The wall FAMILIES — engine machinery the corpus loads only because it
     * shares a source tree with the tests; each with the reason it is the
     * engine's implementation of a concern the platform serves itself or
     * does not serve. A TEST body is the roster's. Anything else is RESIDUE. */
    /** WALLS BY FILE — the corpus source files (core_relational, by path
     * fragment) that are the engine's own machinery, loaded only because they
     * share the tree with the tests; each with the reason it is the engine's
     * implementation of a concern the platform serves itself or does not
     * serve. The list is the receipt; a file leaves it with a witness. */
    static final java.util.LinkedHashMap<String, String> WALLED_FILES = new java.util.LinkedHashMap<>();
    static {
        WALLED_FILES.put("/protocols/pure/", "the engine's JSON protocol serializers, one copy per protocol version — the platform speaks its own protocol");
        WALLED_FILES.put("/pureToSQLQuery/", "the engine's Pure-to-SQL compiler — the platform's compiler is the implementation");
        WALLED_FILES.put("/sqlQueryToString/", "the engine's SQL printer, DDL and dialect tables — the platform's dialects are the implementation");
        WALLED_FILES.put("/sqlDialectTranslation/", "the engine's SQL dialect translation — the platform's dialects");
        WALLED_FILES.put("relationalMappingExecution.pure", "the engine's mapping execution — the platform's resolver is the implementation");
        WALLED_FILES.put("/transform/", "the engine's Pure-to-SQL transform passes — compiler passes on this platform");
        WALLED_FILES.put("/milestoning/milestoning.pure", "the engine's milestoning transformation — the platform's temporal frame (compiler) is the implementation");
        WALLED_FILES.put("/graphFetch/", "the engine's graph-fetch execution machinery — the platform's graph emission is the implementation");
        WALLED_FILES.put("/validation/", "the engine's constraint-validation runners — not served");
        WALLED_FILES.put("/autogeneration/", "relational-to-Pure model autogeneration — not served");
        WALLED_FILES.put("/testDataGeneration/", "the engine's test-data generator — the driver seeds through the platform");
        WALLED_FILES.put("/contract/storeContract.pure", "the engine's store-contract hooks — the platform's own store contract");
        WALLED_FILES.put("/mft/", "the engine's mapping-feature-test harness — the PCT lane's world");
        WALLED_FILES.put("/mutation/", "relational mutation (write) machinery — not served");
        WALLED_FILES.put("/extensions/grammarSerializerExtension.pure", "the engine's grammar serializer extension — the platform prints its own grammar");
        WALLED_FILES.put("/executionPlan/", "the engine's execution-plan machinery — the platform plans itself");
        WALLED_FILES.put("/runtime/", "the engine's runtime and connection machinery (post-processors: a design session pending)");
    }

    static String family(String fqn, String source) {
        if (fqn.matches(".*::tests?::.*")) {
            return "TEST bodies (the roster runs them; not this probe's concern)";
        }
        String path = "/" + source;   // corpus names are relative to core_relational/relational
        for (var w : WALLED_FILES.entrySet()) {
            if (path.contains(w.getKey())) {
                return "WALLED " + w.getKey() + " — " + w.getValue();
            }
        }
        if (fqn.startsWith("meta::protocols::")) {
            return "WALLED /protocols/pure/ — the engine's JSON protocol serializers (attributed by package)";
        }
        return "RESIDUE (ours: a typer gap, a fixture file never admitted, or a file to wall by name)";
    }

    static String reasonClass(String msg) {
        if (msg.contains("unknown function")) return "unknown-function";
        if (msg.contains("has no property")) return "unknown-property";
        if (msg.contains("not a known primitive, class, or enum") || msg.contains("Unknown type")) return "unknown-type";
        if (msg.contains("no overload") || msg.contains("overload")) return "overload";
        if (msg.contains("engine machinery")) return "walled";
        if (msg.contains("not supported yet") || msg.contains("not resolvable")) return "not-implemented";
        return "kernel/other";
    }
}
