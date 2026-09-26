// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import com.legend.testing.Repo;
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


    public static final List<String> PLATFORM_ROOTS = UpstreamFiles.PLATFORM_ROOTS;

    /** The core_functions_* failure ceiling — shrink-only to zero. */
    static final int STDLIB_FAILURES_MAX = 6;

    @Test
    @DisplayName("typing census: every Pure body in legend-pure's platform packages typed once, failures as rows")
    void census() throws IOException {
        // the same default as the prelude generator: the root pom forwards
        // legend.pure.root to every test JVM (-D / LEGEND_PURE_ROOT / the
        // ${user.home} default), and gate 1 passes the resolved roots — so
        // the census RUNS there and its shrink-only pin below is a standing
        // gate; the literal is only the IDE fallback
        Path pure = com.legend.testing.Upstream.pure();
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
                            root.getFileName() + ":" + slash(root.relativize(f)),
                            Files.readString(f, StandardCharsets.UTF_8)));
                }
            }
        }
        // THE ENGINE HALF OF THE STANDARD LIBRARY (the five core_functions_*
        // repositories, with the nine roots above upstream's own "core"). A
        // failing body's scope is its DECLARATION's: the signature ids of every
        // function these files declare, generated from each declaration
        // (SignatureMangle.mangle) — membership, never a test on a name
        Path engine = com.legend.testing.Upstream.engine();
        java.util.Set<String> engineHalfIds = new java.util.HashSet<>();
        for (String r : UpstreamFiles.STDLIB_ENGINE_ROOTS) {
            Path root = engine.resolve(r);
            org.junit.jupiter.api.Assertions.assertTrue(Files.isDirectory(root),
                    "STDLIB_ENGINE_ROOTS missing under " + engine + ": " + r);
            // <repository>/src/main/resources: the repository directory names the source
            Path repo = root.getParent().getParent().getParent().getFileName();
            try (Stream<Path> walk = Files.walk(root)) {
                for (Path f : walk.filter(p -> p.toString().endsWith(".pure")).sorted(java.util.Comparator.comparing(SpecBodyCensusTest::slash)).toList()) {
                    String text = Files.readString(f, StandardCharsets.UTF_8);
                    sources.add(new Compiler.ModelSource(repo + ":" + slash(root.relativize(f)), text));
                    for (PackageableElement el : com.legend.parser.ElementParser.parse(text,
                            com.legend.parser.Dialect.LEGEND_PLATFORM).elements()) {
                        if (el instanceof com.legend.model.Function fn) {
                            engineHalfIds.add(com.legend.model.SignatureMangle.mangle(fn));
                        }
                    }
                }
            }
        }
        List<String> loadWalls = new ArrayList<>();
        ModelContext ctx = null;
        int fileCount = sources.size();
        for (int round = 0; round < 400 && ctx == null; round++) {
            List<String> parseWalls = new ArrayList<>();
            Compiler.ParsedModule module = Compiler.parseSources(sources,
                    (name, err) -> parseWalls.add(name + ": PARSE " + first(err)),
                    com.legend.parser.Dialect.LEGEND_PLATFORM);
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
        Map<String, String> stdlibFailures = new TreeMap<>();
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
                        || ctx.implementations().runsByRule(fn.definition())) {
                    natives++;   // a body the platform runs by its own rule (the row getters) is Java, like a native
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
                    if (e instanceof com.legend.error.WalledBodyException) {
                        walled.put(id, msg);   // WalledBodies: refused by decision
                        continue;
                    }
                    // the engine half's rows are their own scope, with their own pin
                    boolean engineHalf = fn.definition() != null && engineHalfIds.contains(
                            com.legend.model.SignatureMangle.mangle(fn.definition()));
                    (engineHalf ? stdlibFailures : failures).put(id,
                            e.getClass().getSimpleName() + " " + msg + at(e));
                    bump(byReason, reasonClass(msg));
                }
            }
        }


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
        out.add("## core_functions_* typing failures (shrink-only to zero): " + stdlibFailures.size());
        stdlibFailures.forEach((k, v) -> out.add(k + " :: " + v));
        out.add("");
        Files.createDirectories(Repo.outDir());
        Files.write(Repo.out("spec-body-census.txt"), out);
        System.out.println("[spec-census] files=" + fileCount + " loadWalls=" + loadWalls.size()
                + " typedOK=" + ok.size() + " walled=" + walled.size() + " failed(UNWALLED)=" + failures.size()
                + " nativesSkipped=" + natives);
        System.out.println("[spec-census] byReason=" + byReason + " stdlibFailed=" + stdlibFailures.size());
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
        // 22 -> 25 at the 4.145.0 bump (batch 8): the engine's SQL printer
        // grew three properties (DbConfig.selectSQLQueryProcessor /
        // .withinGroupProcessor, NullOrderingSupport.processSortItem) —
        // the same PRINTER wall, three more rows, each with its reason
        // 25 -> 26 (feature-flag leg, 2026-09-12): the prelude carries
        // MultiExecutionContext (ExecutionOptionContext's superclass); its
        // allContexts body is the engine's plan-time context flattening — walled
        // with its reason (the platform reads the option context's flags directly)
        org.junit.jupiter.api.Assertions.assertTrue(walled.size() <= 26,
                () -> "spec body census WALLED rows GREW: " + walled.size()
                        + " > 23 (shrink-only; a new wall needs its reason in WalledBodies):\n  "
                        + String.join("\n  ", walled.keySet()));
        // THE STANDARD-LIBRARY PIN (platform architecture study, 2026-09-24):
        // every body in the five core_functions_* repositories — library and PCT
        // test functions alike — types, as the nine platform roots' already do.
        // Shrink-only to zero. 558 at introduction: 548 PCT tests whose own type
        // variables are named like eval's (<T|m>) — the kernel conflated the
        // caller's T with the callee's; SignatureApart renames the callee's
        // parameters apart — plus two signature-id references the resolver
        // cut to the wrong package (function ids now join the name universe).
        // 8 -> 6: relation::eval (@Column<Nil,Z|0..1>'s multiplicity argument
        // dropped in value position — TypeAnnotations) and relation::reduce (a
        // generic caller's own X⊆T handed to sort — the kernel unifies the two
        // symbolic constraints side by side). The 6 left are one cause: calls to
        // upstream overloads the catalog does not declare (CatalogUpstreamDiffTest
        // MISSING) — collection::get<T>(T[*], String[1]) twice, variant
        // to/toMany with a type lookup three times, and wavg(Number[*], Number[*]),
        // which upstream's wavg(RowMapper[*]) body calls. They leave when the
        // declarations come from upstream whole (the untangle, steps 2-4).
        org.junit.jupiter.api.Assertions.assertTrue(stdlibFailures.size() <= STDLIB_FAILURES_MAX,
                () -> "core_functions_* typing failures GREW: " + stdlibFailures.size() + " > "
                        + STDLIB_FAILURES_MAX + " (shrink-only):\n  " + String.join("\n  ", stdlibFailures.keySet()));
        // 1 -> 5 (2026-09-26, execution plan step 2): the model builder judges a
        // duplicate by the declaration IDENTITY (FunctionId) instead of the
        // spelling-based key, and four collisions the spelling hid are refused
        // now: a class's QUALIFIED PROPERTY (EnumerationMapping.toDomainValue,
        // Mapping.enumerationMappingByName, PropertyMappingsImplementation.
        // _propertyMappingsByPropertyName, Database.schema) lifted to a package
        // function has the same identity as the real package function of that
        // name. The reference keeps the two apart because a qualified property
        // is not a package element; ours lifts it. The refusal is the honest
        // state — a call to either name WAS ambiguous under the old key, hidden
        // by `<T>` in one spelling. Step A4 (members bound to properties) removes
        // the lift and the four walls with it; task #43 tracks the pin.
        org.junit.jupiter.api.Assertions.assertTrue(loadWalls.size() <= 5,
                () -> "spec body census load walls GREW: " + loadWalls);
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
