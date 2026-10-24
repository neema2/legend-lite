// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import com.legend.builtin.Pure;
import com.legend.model.Function;
import com.legend.model.FunctionDefinition;
import com.legend.model.NativeFunctionDefinition;
import com.legend.model.PackageableElement;
import com.legend.model.SignatureMangle;
import com.legend.parser.Dialect;
import com.legend.door.ModelFromText;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * THE CATALOG AGAINST UPSTREAM (platform architecture untangle, step 1,
 * 2026-09-24) — a measurement, no verdict yet.
 *
 * <p>The native catalog ({@link Pure}) is a copy of upstream declarations. This
 * census reads EVERY function declaration in the pinned upstream trees with our
 * own parser, identifies each by its engine signature id (generated from the
 * declaration, {@link SignatureMangle#mangle}), and classifies, for every FQN the
 * catalog declares:
 * <ul>
 *   <li>EXACT — a catalog overload whose id upstream also declares;</li>
 *   <li>DIVERGENT — a catalog overload at an FQN upstream declares, but with an
 *       id upstream does not have (our transcription differs);</li>
 *   <li>MISSING — an upstream overload at a catalog FQN the catalog lacks;</li>
 *   <li>NOT_UPSTREAM — a catalog FQN upstream does not declare at all (ours, or
 *       gone upstream).</li>
 * </ul>
 * It also counts the standard library's functions (the nine platform roots and
 * the five core_functions_* repositories) the catalog does not declare at all.
 * Rows go to {@code catalog-upstream-diff.tsv}; a file our parser cannot read is
 * counted and named, never silently skipped.
 */
class CatalogUpstreamDiffTest {

    /** One upstream declaration: its id, where it is, and whether it has a body. */
    private record Decl(String id, String fqn, String file, boolean bodied) {
    }

    @Test
    void census() throws IOException {
        Path pure = com.legend.testing.ProgramPaths.rootOf("legend.pure.root");
        Path engine = com.legend.testing.ProgramPaths.rootOf("legend.engine.root");
        // the pinned trees are declared inputs: absence is an error, never a skip
        org.junit.jupiter.api.Assertions.assertTrue(Files.isDirectory(pure) && Files.isDirectory(engine),
                "pinned upstream trees not present: " + pure + ", " + engine);

        // the standard library's files, by exact path (membership, never a name test)
        Set<Path> stdlibFiles = new LinkedHashSet<>();
        for (String r : UpstreamFiles.PLATFORM_ROOTS) {
            stdlibFiles.addAll(pureFiles(pure.resolve(r)));
        }
        for (String r : UpstreamFiles.STDLIB_ENGINE_ROOTS) {
            stdlibFiles.addAll(pureFiles(engine.resolve(r)));
        }

        // every upstream function declaration, per FQN
        Map<String, List<Decl>> upstream = new HashMap<>();
        Set<String> stdlibFqns = new LinkedHashSet<>();
        List<String> unreadable = new ArrayList<>();
        int files = 0;
        for (Path root : List.of(pure, engine)) {
            for (Path f : pureFiles(root)) {
                files++;
                List<PackageableElement> elements;
                try {
                    elements = ModelFromText.parse(Files.readString(f, StandardCharsets.UTF_8),
                            Dialect.LEGEND_PLATFORM).elements();
                } catch (RuntimeException e) {
                    unreadable.add(root.relativize(f) + "\t" + SpecBodyCensusTest.first(e.getMessage()));
                    continue;
                }
                boolean inStdlib = stdlibFiles.contains(f);
                for (PackageableElement el : elements) {
                    if (el instanceof Function fn) {
                        Decl d = new Decl(SignatureMangle.mangle(fn), fn.qualifiedName(),
                                root.getFileName() + "/" + root.relativize(f), el instanceof FunctionDefinition);
                        upstream.computeIfAbsent(fn.qualifiedName(), k -> new ArrayList<>()).add(d);
                        if (inStdlib) {
                            stdlibFqns.add(fn.qualifiedName());
                        }
                    }
                }
            }
        }

        // the catalog, per FQN
        Map<String, List<NativeFunctionDefinition>> catalog = new LinkedHashMap<>();
        for (NativeFunctionDefinition n : Pure.all()) {
            catalog.computeIfAbsent(n.qualifiedName(), k -> new ArrayList<>()).add(n);
        }

        Map<String, Integer> counts = new LinkedHashMap<>();
        for (String k : List.of("EXACT", "DIVERGENT", "MISSING", "NOT_UPSTREAM")) {
            counts.put(k, 0);
        }
        List<String> rows = new ArrayList<>();
        rows.add("kind\tfqn\tcatalog id\tupstream id\tupstream file\tupstream bodied");
        Set<String> divergentFqns = new LinkedHashSet<>();
        Set<String> missingFqns = new LinkedHashSet<>();
        for (var e : catalog.entrySet()) {
            String fqn = e.getKey();
            List<Decl> ups = upstream.getOrDefault(fqn, List.of());
            Map<String, Decl> upIds = new LinkedHashMap<>();
            for (Decl d : ups) {
                upIds.putIfAbsent(d.id(), d);
            }
            Set<String> catIds = new LinkedHashSet<>();
            for (NativeFunctionDefinition n : e.getValue()) {
                catIds.add(SignatureMangle.mangle(n));
            }
            for (String id : catIds) {
                if (ups.isEmpty()) {
                    counts.merge("NOT_UPSTREAM", 1, Integer::sum);
                    rows.add("NOT_UPSTREAM\t" + fqn + "\t" + id + "\t\t\t");
                } else if (upIds.containsKey(id)) {
                    counts.merge("EXACT", 1, Integer::sum);
                } else {
                    counts.merge("DIVERGENT", 1, Integer::sum);
                    divergentFqns.add(fqn);
                    rows.add("DIVERGENT\t" + fqn + "\t" + id + "\t" + String.join(" | ", upIds.keySet())
                            + "\t" + ups.get(0).file() + "\t");
                }
            }
            for (Decl d : upIds.values()) {
                if (!catIds.contains(d.id())) {
                    counts.merge("MISSING", 1, Integer::sum);
                    missingFqns.add(fqn);
                    rows.add("MISSING\t" + fqn + "\t\t" + d.id() + "\t" + d.file() + "\t" + d.bodied());
                }
            }
        }

        // the standard library beyond the catalog
        int stdlibNotInCatalog = 0;
        int stdlibNotInCatalogBodied = 0;
        for (String fqn : stdlibFqns) {
            if (!catalog.containsKey(fqn)) {
                for (Decl d : upstream.get(fqn)) {
                    stdlibNotInCatalog++;
                    if (d.bodied()) {
                        stdlibNotInCatalogBodied++;
                    }
                }
            }
        }

        List<String> out = new ArrayList<>();
        out.add("# catalog vs pinned upstream — " + java.time.LocalDate.now());
        out.add("# upstream files=" + files + " unreadable=" + unreadable.size()
                + " upstream function FQNs=" + upstream.size() + " catalog FQNs=" + catalog.size()
                + " catalog overloads=" + Pure.all().size());
        out.add("# catalog overloads: " + counts + " | FQNs with a DIVERGENT overload=" + divergentFqns.size()
                + " | FQNs with a MISSING overload=" + missingFqns.size());
        out.add("# stdlib declarations at FQNs the catalog does not declare: " + stdlibNotInCatalog
                + " (bodied " + stdlibNotInCatalogBodied + ", native " + (stdlibNotInCatalog - stdlibNotInCatalogBodied) + ")");
        out.addAll(rows);
        out.add("## unreadable upstream files");
        out.addAll(unreadable);
        Files.createDirectories(com.legend.testing.TestOutputs.dir());
        Files.write(com.legend.testing.TestOutputs.file("catalog-upstream-diff.tsv"), out);
        out.subList(0, 4).forEach(System.out::println);

        // THE CATALOG NEVER DIVERGES: every overload it declares is upstream's
        // own declaration, id for id — the catalog may LACK overloads, never
        // restate one differently
        org.junit.jupiter.api.Assertions.assertEquals(0, counts.get("DIVERGENT"),
                "catalog overloads DIVERGE from upstream's declaration — see catalog-upstream-diff.tsv");
        // ours alone: the meta::legend::lite overloads (Pure.Lite)
        org.junit.jupiter.api.Assertions.assertTrue(counts.get("NOT_UPSTREAM") <= NOT_UPSTREAM_MAX,
                "catalog overloads upstream does not declare GREW: " + counts.get("NOT_UPSTREAM"));
        // upstream overloads at catalog FQNs the catalog lacks — what the
        // FQN-level suppressions hide; they leave as declarations come from
        // upstream whole (the untangle, steps 2-4). SHRINK-ONLY.
        org.junit.jupiter.api.Assertions.assertTrue(counts.get("MISSING") <= MISSING_MAX,
                "upstream overloads the catalog lacks GREW: " + counts.get("MISSING") + " > " + MISSING_MAX);
        org.junit.jupiter.api.Assertions.assertTrue(unreadable.size() <= UNREADABLE_MAX,
                "upstream files our parser cannot read GREW: " + unreadable);
    }

    /** Measured 2026-09-24 at legend-engine 4.145.0 / legend-pure 5.99.0 (43); 47 on 2026-10-16: the Cypher island's
     *  four meta::legend::lite natives (timeOf, atTime, instantIn, localIn -- the time-of-day and the zoned date-time
     *  legs, docs/CYPHER_STUDY_2026_10_15.md), declared by Pure.Lite: ours alone by design, never upstream's. 48 on
     *  2026-10-16 (the user's decision, the SQL islands' inline views): meta::legend::lite::inlineView, a lite-internal
     *  desugar native (NativeCatalogGovernanceTest's INTERNAL_DESUGAR, docs/LITE_INVENTION_CENSUS.md). 54 on
     *  2026-10-17 (the Cypher TCK-to-zero program, docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md; landed by its W2,
     *  W10 and W11 legs without moving this pin, restored by the spec guards' repair): six meta::legend::lite
     *  overloads, ours alone -- elementId's two (a mapped instance's identity, W2), recurse (an unbounded
     *  variable-length pattern's WITH RECURSIVE, W10), isKind, minBy and maxBy (Cypher's orderability over Variants,
     *  W11). The W3 leg's lite random left in the same repair: rand() is upstream's meta::pure::functions::math::random.
     *  56 on 2026-10-18 (the user's decision of that day, the plan's W13 leg 1, docs/CYPHER_STUDY_2026_10_15.md
     *  §12.85): dateTimeOfText and timeOfText, a temporal value in text carriage -- what a nanosecond value leaves
     *  the database as; ours alone, legend-engine carries a date-time in the database's own type. 59 on 2026-10-18 (the
     *  writes design's leg 2, docs/WRITES_DESIGN_2026_10_18.md §4, L1): delete, update and merge, the store writes
     *  beside upstream's write -- lite's user-facing DML over a store table (NativeCatalogGovernanceTest's
     *  LITE_SURFACE); upstream declares no write but write itself. 60 on 2026-10-20 (the writes design's leg 6, §8):
     *  meta::legend::lite::stage, rows computed once into a temporary table of the session for the statements after
     *  (the Cypher island's writes); a lite-internal native, ours alone. 65 on 2026-10-20 (the writes design's leg 5,
     *  §4 L2): writeInstances, mergeInstances, deleteInstances, link and unlink, the class writes over a direct
     *  mapping, which the store resolver writes as L1's statements; ours alone. 66 on 2026-10-22 (merging main into the
     *  Cypher writes branch): main's meta::legend::lite::quotient (57 on 2026-10-18 on main, merging the IQ island's
     *  landing 5), an integer quotient cut toward zero, the database's own integer division -- upstream has none
     *  (legend-pure's divide of integers is a Float). */
    private static final int NOT_UPSTREAM_MAX = 66;
    private static final int MISSING_MAX = 191;
    private static final int UNREADABLE_MAX = 16;

    private static List<Path> pureFiles(Path root) throws IOException {
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(p -> p.toString().endsWith(".pure")).sorted().toList();
        }
    }
}
