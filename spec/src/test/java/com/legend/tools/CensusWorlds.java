package com.legend.tools;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The census's SECOND pass — COMPILE_EVERYTHING_HOMEWORK §6: a module body
 * types in the world it RUNS in, not at boot. A prelude class from an
 * ENGINE file carries its derived/constraint bodies verbatim; those bodies
 * name that file's own functions and the engine libraries the corpus
 * loads by file (T2: programs enter by file, never by registration). This
 * pass rebuilds the world as boot + the platform packages + the owner's
 * spec file + {@code Corpus.LIBRARY_FILES}, re-types each failing
 * engine-owned row there, and buckets what still fails by the SPEC'S
 * MARKING (§3): B1 a native absent from the registry, B2 a program
 * function whose file is in no world, B3 walled by user decision, B4 a
 * typer/normalizer gap. A measurement only — no arm, no registration.
 */
final class CensusWorlds {
    /** A platform-independent path string: '/' separators always. A Path in a
     *  concatenation converts with the PLATFORM separator, so an id built that
     *  way differs on Windows (census 2026-09-09). */
    private static String slash(java.nio.file.Path p) {
        return p.toString().replace(java.io.File.separatorChar, '/');
    }


    private CensusWorlds() {
    }

    /** The SQL printer's owners — WALLED by user decision 2026-09-08 (the
     * engine's SQL post-processing machinery; ENGINE_MACHINERY_WALLS) until
     * the post-processor design session (homework §8 D1). */
    static final Set<String> WALLED_OWNERS = Set.of(
            "meta::relational::functions::sqlQueryToString::DbConfig",
            "meta::relational::functions::sqlQueryToString::DynaFunctionToSql",
            "meta::relational::functions::sqlstring::SQLResult");

    private static final Pattern UNKNOWN_FUNCTION = Pattern.compile("unknown function '([^']+)'");

    record Row(String id, String owner, String source, String bootMessage,
               String runningMessage, String bucket, String detail) {
    }

    record Report(List<Row> rows, Map<String, Integer> buckets, List<String> worldWalls) {
    }

    /** The owner class of a lifted body id ({@code <owner>$prop$<name>(…)},
     * {@code <owner>$constraint$<name>(…)}); null for anything else. */
    static String ownerOf(String id) {
        int p = id.indexOf("$prop$");
        int c = id.indexOf("$constraint$");
        int cut = p >= 0 ? p : c;
        return cut < 0 ? null : id.substring(0, cut);
    }

    static Report run(List<Compiler.ModelSource> platformSources,
            Map<String, String> bootFailures, Set<String> specNativeNames,
            Path engineRoot) throws IOException {
        Map<String, String> sourceOf = moduleSources();
        java.util.Set<String> nativeNames = new java.util.HashSet<>(specNativeNames);
        nativeNames.addAll(engineNativeNames(engineRoot));
        // the engine files the failing owners come from — one running world
        // over all of them (the corpus's world is one world too)
        Map<String, String> ownerSource = new LinkedHashMap<>();
        for (String id : bootFailures.keySet()) {
            String owner = ownerOf(id);
            String src = owner == null ? null : sourceOf.get(owner);
            if (src != null && src.startsWith("legend-engine/")) {
                ownerSource.put(id, src);
            }
        }
        List<String> worldWalls = new ArrayList<>();
        ModelContext running = ownerSource.isEmpty() ? null
                : runningWorld(platformSources, ownerSource.values(), engineRoot, worldWalls);
        SpecCompiler specs = running == null ? null : new SpecCompiler(running);

        List<Row> rows = new ArrayList<>();
        Map<String, Integer> buckets = new TreeMap<>();
        for (Map.Entry<String, String> e : bootFailures.entrySet()) {
            String id = e.getKey();
            String owner = ownerOf(id);
            String src = ownerSource.get(id);
            String runningMessage = null;
            if (src != null && specs != null) {
                runningMessage = retype(running, specs, id);
            }
            String bucket;
            String detail;
            if (src != null && runningMessage == null) {
                bucket = "TYPED-IN-RUNNING-WORLD";
                detail = src;
            } else {
                String msg = runningMessage != null ? runningMessage : e.getValue();
                Matcher m = UNKNOWN_FUNCTION.matcher(msg);
                String unknown = m.find() ? m.group(1) : null;
                String simple = unknown == null ? null
                        : unknown.substring(unknown.lastIndexOf(':') + 1);
                if (owner != null && WALLED_OWNERS.contains(owner)) {
                    bucket = "B3-WALLED-BY-DECISION";
                    detail = "the SQL printer (user 2026-09-08)";
                } else if (simple != null && nativeNames.contains(simple)) {
                    bucket = "B1-NATIVE-UNREGISTERED";
                    detail = unknown;
                } else if (simple != null) {
                    // the running world HOLDS a function of that name and the
                    // body still cannot see it: the module body's names were
                    // resolved ONCE, at boot, in a world without the file —
                    // "bodies resolve where they run" (closure option B) has
                    // no mechanism yet; the name is frozen bare (FINDING,
                    // homework §6 — a Compiler leg, not a census one)
                    final String want = "::" + simple;
                    boolean present = running != null
                            && running.functionFqns().stream().anyMatch(f -> f.endsWith(want));
                    bucket = present ? "B2b-NAME-FROZEN-AT-BOOT" : "B2-PROGRAM-NOT-LOADED";
                    detail = unknown + (src == null ? " (owner not from an engine file)" : "")
                            + (present ? " — defined in the running world" : "");
                } else {
                    bucket = "B4-TYPER-GAP";
                    detail = SpecBodyCensusTest.first(msg);
                }
            }
            buckets.merge(bucket, 1, Integer::sum);
            rows.add(new Row(id, owner, src, e.getValue(), runningMessage, bucket, detail));
        }
        return new Report(rows, buckets, worldWalls);
    }

    /** Each module declaration's spec file, read from the module's own
     * section headers ({@code // legend-engine/…}, {@code // legend-pure/…}
     * — the generator names every section by its spec path relative to the
     * checkout root, PRELUDE_MODULE_HOMEWORK §9 item 10). */
    static Map<String, String> moduleSources() {
        Map<String, String> out = new LinkedHashMap<>();
        String current = null;
        Pattern decl = Pattern.compile("^(?:Class|Enum)\\s+([A-Za-z0-9_:]+)");
        for (String line : com.legend.builtin.Prelude.source().split("\\n")) {
            if (line.startsWith("// legend-engine/") || line.startsWith("// legend-pure/")) {
                current = line.substring(3).trim();
                continue;
            }
            Matcher m = decl.matcher(line);
            if (m.find() && current != null) {
                out.put(m.group(1), current);
            }
        }
        return out;
    }

    /** The simple names of every {@code native function} the engine
     * checkout declares (main resources only) — the spec's MARKING for an
     * unknown function: native (B1) or program (B2). */
    static java.util.Set<String> engineNativeNames(Path engineRoot) throws IOException {
        java.util.Set<String> out = new java.util.HashSet<>();
        if (!Files.isDirectory(engineRoot)) {
            return out;
        }
        Pattern nat = Pattern.compile("native function\\s+(?:<<[^>]*>>\\s*)?(?:\\{[^}]*\\}\\s*)?[A-Za-z0-9_:]*::([A-Za-z0-9_]+)\\s*[<(]");
        try (java.util.stream.Stream<Path> walk = Files.walk(engineRoot)) {
            // '/' ALWAYS: Path.toString uses the platform separator, so on
            // Windows this filter matches NOTHING and the census silently
            // reports an empty world (Windows CI, 2026-09-09).
            for (Path f : walk.filter(x -> x.toString().endsWith(".pure")
                    && x.toString().replace(java.io.File.separatorChar, '/')
                            .contains("/src/main/resources/")).toList()) {
                Matcher m = nat.matcher(Files.readString(f, StandardCharsets.UTF_8));
                while (m.find()) {
                    out.add(m.group(1));
                }
            }
        }
        return out;
    }

    private static String retype(ModelContext ctx, SpecCompiler specs, String id) {
        String fqn = id.substring(0, id.indexOf('('));
        List<TypedFunction> overloads;
        try {
            overloads = ctx.findFunction(fqn);
        } catch (RuntimeException e) {
            return "SIGNATURE " + SpecBodyCensusTest.first(e.getMessage());
        }
        if (overloads.isEmpty()) {
            return "ABSENT in the running world";
        }
        String last = null;
        for (TypedFunction fn : overloads) {
            try {
                specs.compile(fn);
                return null;
            } catch (RuntimeException e) {
                last = e.getClass().getSimpleName() + " " + SpecBodyCensusTest.first(e.getMessage());
            }
        }
        return last;
    }

    /** boot + platform packages + the owners' engine files + the corpus's
     * library files, built through the front door; a file that does not
     * parse or an element the model refuses is a WALL line, never dropped
     * silently (the census's own bounded loop). */
    private static ModelContext runningWorld(List<Compiler.ModelSource> platformSources,
            java.util.Collection<String> ownerFiles, Path engineRoot, List<String> walls)
            throws IOException {
        List<Compiler.ModelSource> sources = new ArrayList<>(platformSources);
        java.util.LinkedHashSet<Path> files = new java.util.LinkedHashSet<>();
        for (String src : new java.util.LinkedHashSet<>(ownerFiles)) {
            files.add(engineRoot.resolve(src.substring("legend-engine/".length())));
        }
        Path corpusRoot = com.legend.rcorpus.Corpus.ENGINE_ROOT;
        for (Path lib : com.legend.rcorpus.Corpus.LIBRARY_FILES) {
            files.add(engineRoot.resolve(corpusRoot.relativize(lib)));
        }
        for (Path f : files) {
            if (!Files.isRegularFile(f)) {
                walls.add(f + ": MISSING");
                continue;
            }
            // PLATFORM-INDEPENDENT: a Path in a concatenation converts with the
            // platform separator, and sorting Paths is CASE-INSENSITIVE on
            // Windows — both make this id/order differ there (census 2026-09-09)
            sources.add(new Compiler.ModelSource("engine:"
                    + slash(engineRoot.relativize(f)),
                    Files.readString(f, StandardCharsets.UTF_8)));
        }
        for (int round = 0; round < 400; round++) {
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
                ModelContext ctx = Compiler.buildModel(pruned);
                walls.addAll(parseWalls);
                return ctx;
            } catch (com.legend.error.ModelException e) {
                String el = e.element();
                String src = el == null ? null : module.model().elementSources().get(el);
                if (src == null) {
                    throw e;
                }
                final String drop = src;
                sources.removeIf(s -> s.name().equals(drop));
                walls.add(drop + ": MODEL " + SpecBodyCensusTest.first(e.getMessage()));
            }
        }
        throw new IllegalStateException("running world did not converge");
    }
}
