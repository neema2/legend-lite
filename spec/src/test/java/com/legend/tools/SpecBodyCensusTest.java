// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * THE TYPING CENSUS (docs/SYSTEM_PRELUDE_DESIGN_2026_09_08.md §6): load
 * legend-pure's PLATFORM packages as a model — every class, every derived
 * property (lifted to {@code <owner>$prop$<name>}), every Pure-bodied
 * function; the spec's {@code native function} declarations drop (the
 * registry is their definition, channel B's doctrine) — and TYPE every body
 * exactly once. Compiling never executes (§5): a reflective call types
 * against its registered signature. Each failure is one row with its
 * reason; the rows are the typing WORK LIST, which should trend to zero
 * (missing vocabulary, typer gaps). Written to
 * {@code target/spec-body-census.txt}; a summary by reason prints.
 *
 * <p>PINNED shrink-only since batch 151 (22 rows, 6 load walls; 19 since batch 155). It needs
 * the pure checkout ({@code -Dlegend.pure.root}, defaulting to the reference
 * checkout like the prelude generator) and skips without it.
 */
public class SpecBodyCensusTest {
    /** A platform-independent path string: '/' separators always. A Path in a
     *  concatenation converts with the PLATFORM separator, so an id built that
     *  way differs on Windows (census 2026-09-09). */
    private static String slash(java.nio.file.Path p) {
        return p.toString().replace(java.io.File.separatorChar, '/');
    }


    public static final List<String> PLATFORM_ROOTS = List.of(
            "legend-pure-core/legend-pure-m3-core/src/main/resources/platform",
            "legend-pure-core/legend-pure-m3-precisePrimitives/src/main/resources/platform_precise_primitives",
            "legend-pure-dsl/legend-pure-dsl-diagram/legend-pure-m2-dsl-diagram-pure/src/main/resources/platform_dsl_diagram",
            "legend-pure-dsl/legend-pure-dsl-graph/legend-pure-m2-dsl-graph-pure/src/main/resources/platform_dsl_graph",
            "legend-pure-dsl/legend-pure-dsl-mapping/legend-pure-m2-dsl-mapping-pure/src/main/resources/platform_dsl_mapping",
            "legend-pure-dsl/legend-pure-dsl-path/legend-pure-m2-dsl-path-pure/src/main/resources/platform_dsl_path",
            "legend-pure-dsl/legend-pure-dsl-store/legend-pure-m2-dsl-store-pure/src/main/resources/platform_dsl_store",
            "legend-pure-dsl/legend-pure-dsl-tds/legend-pure-m2-dsl-tds-pure/src/main/resources/platform_dsl_tds",
            "legend-pure-store/legend-pure-store-relational/legend-pure-m2-store-relational-pure/src/main/resources/platform_store_relational");

    @Test
    @DisplayName("typing census: every Pure body in legend-pure's platform packages typed once, failures as rows")
    void census() throws IOException {
        // the same default as the prelude generator: the root pom forwards
        // legend.pure.root to every test JVM (-D / LEGEND_PURE_ROOT / the
        // ${user.home} default), and gate 1 passes the resolved roots — so
        // the census RUNS there and its shrink-only pin below is a standing
        // gate; the literal is only the IDE fallback
        Path pure = Path.of(System.getProperty("legend.pure.root",
                System.getProperty("user.home") + "/legend/legend-pure"));
        // PRECHECK ALL NINE ROOTS (upstream boundary batch 2): a missing
        // checkout skips (there is nothing to census); a PRESENT checkout
        // missing any one root FAILS, every miss named — a root that moved
        // upstream shrank the census input and let the shrink-only pins pass
        // easier without a word (the old precheck tested root 0 only, and the
        // walk below `continue`d past the rest).
        Assumptions.assumeTrue(Files.isDirectory(pure), "legend-pure checkout not present at " + pure);
        List<String> missingRoots = new ArrayList<>();
        for (String r : PLATFORM_ROOTS) {
            if (!Files.isDirectory(pure.resolve(r))) {
                missingRoots.add(r);
            }
        }
        org.junit.jupiter.api.Assertions.assertEquals(List.of(), missingRoots,
                "PLATFORM_ROOTS missing under " + pure + " — upstream moved them; fix the path");

        // 1. LOAD — every platform .pure file as a source; files that do not
        // parse and elements the model integrity refuses are recorded, never
        // silently dropped (channel B's wall-collection loop, bounded)
        List<Compiler.ModelSource> sources = new ArrayList<>();
        for (String r : PLATFORM_ROOTS) {
            Path root = pure.resolve(r);
            try (Stream<Path> walk = Files.walk(root)) {
                for (Path f : walk.filter(p -> p.toString().endsWith(".pure")).sorted(java.util.Comparator.comparing(SpecBodyCensusTest::slash)).toList()) {
                    sources.add(new Compiler.ModelSource(
                            r.substring(r.lastIndexOf('/') + 1) + ":"
                                    + slash(root.relativize(f)),
                            Files.readString(f, StandardCharsets.UTF_8)));
                }
            }
        }
        List<String> loadWalls = new ArrayList<>();
        ModelContext ctx = null;
        int fileCount = sources.size();
        // the spec's NATIVE names (simple) — the running-world pass buckets
        // an unknown function by the spec's marking (native vs program)
        java.util.Set<String> specNativeNames = new java.util.HashSet<>();
        for (int round = 0; round < 400 && ctx == null; round++) {
            List<String> parseWalls = new ArrayList<>();
            Compiler.ParsedModule module = Compiler.parseSources(sources,
                    (name, err) -> parseWalls.add(name + ": PARSE " + first(err)),
                    com.legend.parser.Dialect.LEGEND_PLATFORM);
            specNativeNames.clear();
            for (PackageableElement e : module.model().elements()) {
                if (e instanceof com.legend.model.NativeFunctionDefinition n) {
                    String q = n.qualifiedName();
                    specNativeNames.add(q.substring(q.lastIndexOf(':') + 1));
                }
            }
            // the spec's native declarations are the SPEC of natives the
            // registry defines — the registry is the definition; they drop
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
            } catch (com.legend.error.ModelException e) {
                String el = e.element();
                String src = el == null ? null : module.model().elementSources().get(el);
                if (src == null) {
                    throw e;
                }
                final String drop = src;
                sources.removeIf(s -> s.name().equals(drop));
                loadWalls.add(drop + ": MODEL " + first(e.getMessage()));
            }
        }
        if (ctx == null) {
            throw new IllegalStateException("model did not converge");
        }

        // 2. TYPE — every function the model holds (the spec's Pure-bodied
        // functions plus every derived property lifted to <owner>$prop$<name>),
        // each body typed once through the ordinary compile entry
        SpecCompiler specs = new SpecCompiler(ctx);
        List<String> ok = new ArrayList<>();
        Map<String, String> failures = new TreeMap<>();
        Map<String, String> walled = new TreeMap<>();   // WalledBodies: refused by decision, with a reason
        Map<String, Integer> byReason = new TreeMap<>();
        int natives = 0;
        for (String fqn : new java.util.TreeSet<>(ctx.functionFqns())) {
            List<TypedFunction> overloads;
            try {
                overloads = ctx.findFunction(fqn);
            } catch (RuntimeException e) {
                failures.put(fqn, "SIGNATURE " + first(e.getMessage()));
                bump(byReason, "signature");
                continue;
            }
            for (TypedFunction fn : overloads) {
                if (fn.isNative() || fn.body().isEmpty()
                        || com.legend.compiler.element.type.PlatformTypes
                                .isPlatformImplementedDerived(fn.qualifiedName())) {
                    natives++;   // a platform-IMPLEMENTED accessor (the row getters) is Java, like a native
                    continue;
                }
                String id = fn.qualifiedName() + "(" + fn.parameters().stream()
                        .map(p -> p.type().typeName() + p.multiplicity().text())
                        .collect(java.util.stream.Collectors.joining(",")) + ")";
                try {
                    specs.compile(fn);
                    ok.add(id);
                } catch (RuntimeException e) {
                    String msg = first(e.getMessage());
                    if (e instanceof com.legend.error.NotImplementedException
                            && String.valueOf(e.getMessage()).startsWith("walled body '")) {
                        walled.put(id, msg);   // WalledBodies: refused by decision
                        continue;
                    }
                    failures.put(id, e.getClass().getSimpleName() + " " + msg + at(e));
                    bump(byReason, reasonClass(msg));
                }
            }
        }

        // 2b. THE RUNNING WORLD (COMPILE_EVERYTHING_HOMEWORK §6): every
        // failing body of a prelude class from an ENGINE file re-typed in
        // the world it runs in (boot + platform packages + its own spec
        // file + the corpus's library files), then bucketed by the spec's
        // marking. A measurement: no arm, no registration.
        Path engineRoot = Path.of(System.getProperty("legend.engine.root",
                System.getProperty("user.home") + "/legend/legend-engine"));
        CensusWorlds.Report worlds = CensusWorlds.run(sources, failures,
                specNativeNames, engineRoot);

        // 3. REPORT
        List<String> out = new ArrayList<>();
        out.add("# spec body typing census — " + java.time.LocalDate.now());
        out.add("# files=" + fileCount + " loadWalls=" + loadWalls.size()
                + " functions typed OK=" + ok.size() + " FAILED=" + failures.size()
                + " (natives skipped=" + natives + ")");
        out.add("# by reason: " + byReason);
        out.add("");
        out.add("## load walls");
        out.addAll(loadWalls);
        out.add("");
        out.add("## WALLED bodies (WalledBodies.REASONS — by decision, never typed): " + walled.size());
        walled.forEach((k, v) -> out.add("WALLED | " + k + " | " + v));
        out.add("");
        out.add("## typing failures (UNWALLED — must be zero)");
        failures.forEach((k, v) -> out.add(k + " :: " + v));
        out.add("");
        out.add("## running-world pass (COMPILE_EVERYTHING_HOMEWORK §6) — buckets: " + worlds.buckets());
        out.add("## running-world walls: " + worlds.worldWalls());
        for (CensusWorlds.Row r : worlds.rows()) {
            out.add(r.bucket() + " | " + r.id() + " | " + r.detail()
                    + (r.runningMessage() == null ? "" : " | running: " + r.runningMessage()));
        }
        Files.createDirectories(Path.of("target"));
        Files.write(Path.of("target/spec-body-census.txt"), out);
        System.out.println("[spec-census] files=" + fileCount + " loadWalls=" + loadWalls.size()
                + " typedOK=" + ok.size() + " walled=" + walled.size() + " failed(UNWALLED)=" + failures.size()
                + " nativesSkipped=" + natives);
        System.out.println("[spec-census] byReason=" + byReason);
        // THE PIN (SYSTEM_PRELUDE_DESIGN §6: the typing work list trends to
        // ZERO — shrink-only). 22 (batch 151, prelude-as-module phase 1):
        // the 22 boot-body rows of SPEC_BODY_CENSUS §10 — engine-internal
        // derived/constraint bodies that phase 3's demand cut moves out of
        // the prelude, four vocabulary names, one closure-over-bodies row,
        // one typer gap. A new row is a regression to name, never a bump
        // without a written reason; a burned row lowers the number.
        // 22 -> 19 (batch 155, phase 3b-2): three engine-class bodies left the
        // prelude with their classes (the demand cut); the rest of the engine
        // rows stay while their classes are vocabulary (DbConfig by signature,
        // SchemaState by closure) — SPEC_BODY_CENSUS §10.4
        // THE STRICT PIN (batch 173, COMPILE_EVERYTHING_HOMEWORK §11 — USER:
        // "not okay for things not to compile at boot"): every prelude body
        // TYPES at boot, or is on WalledBodies with its reason. Unwalled
        // failures are ZERO; the walled list is shrink-only by count (22
        // rows on 2026-09-09: the census's 18 over 17 FQNs + the four
        // PostProcessor registry properties the inliner already walled;
        // an entry leaves with a witness).
        org.junit.jupiter.api.Assertions.assertTrue(failures.isEmpty(),
                () -> "spec body typing census: " + failures.size()
                        + " UNWALLED boot failures (must be zero) —\n  "
                        + String.join("\n  ", failures.keySet()));
        // 23 -> 22 (batch 5 leg 5c): SQLResult$prop$toSQLString is platform-IMPLEMENTED
        // (the toSQLString routine), counted with the natives, no longer a wall
        org.junit.jupiter.api.Assertions.assertTrue(walled.size() <= 22,
                () -> "spec body census WALLED rows GREW: " + walled.size()
                        + " > 23 (shrink-only; a new wall needs its reason in WalledBodies):\n  "
                        + String.join("\n  ", walled.keySet()));
        org.junit.jupiter.api.Assertions.assertTrue(loadWalls.size() <= 1,
                () -> "spec body census load walls GREW: " + loadWalls);
        System.out.println("[spec-census] runningWorld buckets=" + worlds.buckets()
                + " walls=" + worlds.worldWalls().size());
        // THE BUCKET PINS (COMPILE_EVERYTHING_HOMEWORK §3, §6 — shrink-only,
        // measured batch 168): B1 natives the registry lacks; B2 program
        // functions in no loaded world (the census's world is wrong, or a
        // file no program loads — D2); B3 walled by user decision (the SQL
        // printer, D1); B4 typer/normalizer gaps. TYPED-IN-RUNNING-WORLD
        // rows are closed rows, not failures.
        java.util.Map<String, Integer> pins = java.util.Map.of(
                "B1-NATIVE-UNREGISTERED", 1,
                "B2-PROGRAM-NOT-LOADED", 5,
                "B2b-NAME-FROZEN-AT-BOOT", 5,
                "B3-WALLED-BY-DECISION", 7,
                "B4-TYPER-GAP", 1);
        for (var pin : pins.entrySet()) {
            int n = worlds.buckets().getOrDefault(pin.getKey(), 0);
            org.junit.jupiter.api.Assertions.assertTrue(n <= pin.getValue(),
                    () -> "census bucket " + pin.getKey() + " GREW: " + n + " > "
                            + pin.getValue() + " pinned (shrink-only)");
        }
    }

    /** A coarse reason class for the summary — the rows carry the full text. */
    static String reasonClass(String msg) {
        if (msg.contains("unknown function")) {
            return "unknown-function";
        }
        if (msg.contains("has no property")) {
            return "unknown-property";
        }
        if (msg.contains("Unknown type") || msg.contains("is not a known")) {
            return "unknown-type";
        }
        if (msg.contains("no overload")) {
            return "overload";
        }
        if (msg.contains("type variable") || msg.contains("cannot also bind")
                || msg.contains("no common supertype")) {
            return "kernel";
        }
        if (msg.contains("NormalizeRequired") || msg.contains("cannot inline")) {
            return "normalize";
        }
        return "other";
    }

    /** The first platform frame of a failure — WHERE the typer gave up
     * (a report column; the message alone does not locate a typer bug). */
    private static String at(RuntimeException e) {
        for (StackTraceElement f : e.getStackTrace()) {
            if (f.getClassName().startsWith("com.legend.")) {
                return " @ " + f.getClassName().substring(f.getClassName().lastIndexOf('.') + 1)
                        + "." + f.getMethodName() + ":" + f.getLineNumber();
            }
        }
        return "";
    }

    private static void bump(Map<String, Integer> m, String k) {
        m.merge(k, 1, Integer::sum);
    }

    static String first(String s) {
        if (s == null) {
            return "";
        }
        int nl = s.indexOf('\n');
        String one = nl < 0 ? s : s.substring(0, nl);
        return one.length() > 600 ? one.substring(0, 600) : one;
    }
}
