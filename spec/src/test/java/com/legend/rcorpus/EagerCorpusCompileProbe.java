package com.legend.rcorpus;

import com.legend.Compiler;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** THE EAGER COMPILE — a MEASUREMENT, not a gate (USER 2026-09-09: "before
 * we add to gate let's do all the work, then decide"): every body in the
 * corpus's compiled world typed up front through Compiler.compileAllBodies.
 * A report action (Bazel workplan P3-17): {@code bazel build //spec:eager_corpus_compile}
 * writes eager-corpus.txt (by reason, by package, by source file, every failing
 * body) and eager-residue.txt. Measured 2026-09-09: 9,099 bodies, 1,605 fail
 * (COMPILE_EVERYTHING_HOMEWORK §10). The second world (the corpus + legend-pure's
 * platform packages whole) left with //spec:eager_corpus_compile_world2; the
 * manifest-world experiments (2026-10-06) measured that question instead. */
public final class EagerCorpusCompileProbe {

    private EagerCorpusCompileProbe() {}

    /** {@code args[0]}: the directory the reports go in (the action's {@code {OUT_DIR}}). */
    public static void main(String[] args) throws Exception {
        java.nio.file.Path outDir = java.nio.file.Path.of(args[0]);
        com.legend.testing.Programs.captureConsole(outDir);
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
        // the side maps are keyed per element, a function's id (Phase 3b item 5b): a wall key is an id too
        for (String fqn : corpus.context().functionFqns()) {
            for (var fn : corpus.context().findFunction(fqn)) {
                String src = fn.definition() == null ? "?" : corpus.elementSources().getOrDefault(
                        com.legend.model.FunctionId.of(fn.definition()).qualified(), "?");
                bodiesBySource.merge(src, 1, Integer::sum);
            }
        }
        for (String k : walls.keySet()) {
            String fqn = k.contains("(") ? k.substring(0, k.indexOf('(')) : k;
            bySource.merge(corpus.elementSources().getOrDefault(k,
                    corpus.elementSources().getOrDefault(fqn, "?")), 1, Integer::sum);
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
            String source = corpus.elementSources().getOrDefault(k, corpus.elementSources().getOrDefault(fqn, "?"));
            String fam = family(fqn, source);
            families.merge(fam, 1, Integer::sum);
            if (fam.startsWith("RESIDUE")) {
                residue.add(k + " :: " + e.getValue().replace('\n', ' '));
                residueBySource.merge(source, 1, Integer::sum);
            }
        }
        List<String> out = new ArrayList<>();
        out.add("# families: " + families);
        out.add("# RESIDUE (non-test, outside the walled families) = " + residue.size()
                + " by source: " + residueBySource.entrySet().stream()
                        .sorted((x, y) -> y.getValue() - x.getValue()).toList());
        Files.write(outDir.resolve("eager-residue.txt"), residue);
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
        Files.write(outDir.resolve("eager-corpus.txt"), out);
        System.out.println(out.get(0));
        System.out.println(out.get(1));
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
