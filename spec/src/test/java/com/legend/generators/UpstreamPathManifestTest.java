// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import com.legend.rcorpus.Corpus;
import com.legend.rcorpus.MinimalCorpus;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE UPSTREAM PATH MANIFEST — every legend-engine / legend-pure path this
 * module hardcodes, enumerated FROM THE CONSTANTS THAT USE THEM (never a
 * second list), resolved against the pinned checkouts, and asserted to exist
 * — every miss reported by name (docs/UPSTREAM_BOUNDARY_PROGRAM.md workstream
 * E, batch 2).
 *
 * <p>Why: a moved upstream file used to be a silent {@code continue} in four
 * places (the corpus's SHAPE/LIBRARY walks, the spec census's root loop, the
 * prelude generator's demand scan and platform-function walk), and a starved
 * input passes green — the pins get EASIER. {@code tools/upstream-drift.py}
 * measures the same set by parsing these source files with regexes; this test
 * is the in-gate form, reading the constants themselves.
 *
 * <p>The count is PINNED: a new hardcoded upstream path is a reviewed event
 * (it must be added to whatever it belongs to AND the count moved here with a
 * reason). Paths outside core are covered elsewhere — the ten ChannelB scope
 * roots by {@code ChannelB.run} (pct, gate 9: a missing root throws) and the
 * 33 ledger path keys by {@code CorpusSweepTest}'s staleness assertions
 * (parser-equivalence, gate 8: a key whose source left the corpus is a stale
 * row and fails).
 */
public class UpstreamPathManifestTest {

    /** One hardcoded upstream path: the constant that declares it, the repo,
     *  the resolved path, and whether a file or a directory is expected. */
    record Entry(String site, String repo, Path path, boolean dir) {
    }

    /** 90 = 3 rcorpus roots (RELATIONAL, CORE_PURE, M2M_TESTS) + 6 LIBRARY_FILES
     *  + 64 SHAPE_FILES + 1 ENGINE_IMPLEMENTATION_FILES key + 1 graphFetch domain
     *  + 9 SpecBodyCensusTest.PLATFORM_ROOTS + 3 prelude ENGINE_SPEC_ROOTS
     *  + 1 prelude CORPUS_ROOT + 1 m3.pure + 1 pure checkout root (indexed whole)
     *  + 1 CompileContext.java (91, batch 5 audit).
     *  Measured 2026-09-10 (batch 2). */
    static final int PINNED_COUNT = 91;

    static List<Entry> manifest() {
        Path engine = Corpus.ENGINE_ROOT;
        Path pure = PreludeGeneratorTest.pureRoot();
        List<Entry> out = new ArrayList<>();
        out.add(new Entry("Corpus.RELATIONAL", "engine", Corpus.RELATIONAL, true));
        out.add(new Entry("Corpus.CORE_PURE", "engine", Corpus.CORE_PURE, true));
        out.add(new Entry("Corpus.M2M_TESTS", "engine", Corpus.M2M_TESTS, true));
        for (Path p : Corpus.LIBRARY_FILES) {
            out.add(new Entry("Corpus.LIBRARY_FILES", "engine", p, false));
        }
        for (Path p : Corpus.SHAPE_FILES) {
            out.add(new Entry("Corpus.SHAPE_FILES", "engine", p, false));
        }
        for (String k : MinimalCorpus.engineImplementationFileKeys()) {
            out.add(new Entry("MinimalCorpus.ENGINE_IMPLEMENTATION_FILES", "engine",
                    Corpus.RELATIONAL.resolve(k), false));
        }
        out.add(new Entry("MinimalCorpus.GRAPH_FETCH_DOMAIN", "engine",
                engine.resolve(MinimalCorpus.GRAPH_FETCH_DOMAIN), true));
        for (String r : SpecBodyCensusTest.PLATFORM_ROOTS) {
            out.add(new Entry("SpecBodyCensusTest.PLATFORM_ROOTS", "pure", pure.resolve(r), true));
        }
        for (String r : PreludeGeneratorTest.ENGINE_SPEC_ROOTS) {
            out.add(new Entry("PreludeGeneratorTest.ENGINE_SPEC_ROOTS", "engine", engine.resolve(r), true));
        }
        out.add(new Entry("PreludeGeneratorTest.CORPUS_ROOT", "engine",
                engine.resolve(PreludeGeneratorTest.CORPUS_ROOT), true));
        out.add(new Entry("PreludeGeneratorTest.M3_PURE", "pure", pure.resolve(PreludeGeneratorTest.M3_PURE), false));
        // +1 (batch 5 audit, 2026-09-11): the engine's implicit-import sequence
        out.add(new Entry("CoreImportsParityTest.COMPILE_CONTEXT", "engine",
                engine.resolve(CoreImportsParityTest.COMPILE_CONTEXT), false));
        out.add(new Entry("PreludeGeneratorTest (pure checkout, indexed whole)", "pure", pure, true));
        return out;
    }

    @Test
    @DisplayName("every hardcoded upstream path resolves in the pinned checkouts (misses named)")
    void everyUpstreamPathResolves() {
        Path engine = Corpus.ENGINE_ROOT;
        Path pure = PreludeGeneratorTest.pureRoot();
        // no checkout at all = nothing to check (the gates fail that upstream,
        // tools/oracle-roots.sh); a PRESENT checkout is checked in full
        Assumptions.assumeTrue(Files.isDirectory(engine), "legend-engine checkout not present at " + engine);
        Assumptions.assumeTrue(Files.isDirectory(pure), "legend-pure checkout not present at " + pure);
        List<Entry> all = manifest();
        List<String> missing = new ArrayList<>();
        for (Entry e : all) {
            boolean ok = e.dir() ? Files.isDirectory(e.path()) : Files.isRegularFile(e.path());
            if (!ok) {
                missing.add(e.site() + " [" + e.repo() + "] expected " + (e.dir() ? "dir " : "file ")
                        + e.path());
            }
        }
        System.out.println("[upstream-paths] " + all.size() + " hardcoded paths, " + missing.size() + " missing");
        assertEquals(List.of(), missing,
                "hardcoded upstream paths that do not resolve — upstream moved them, or the pin"
                + " moved under them; fix the constant, never let the input shrink silently");
        assertEquals(PINNED_COUNT, all.size(),
                "the number of hardcoded upstream paths moved — a new (or removed) upstream"
                + " dependency is a reviewed event: re-pin PINNED_COUNT with the arithmetic");
    }
}
