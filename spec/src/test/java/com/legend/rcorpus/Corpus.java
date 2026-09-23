// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.rcorpus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The REAL legend-engine {@code core_relational} test corpus, consumed as
 * data (docs/LEGEND_ENGINE_TEST_PORTING.md): each {@code <<test.Test>>}
 * function is a query + mapping + expected rows (+ golden SQL). This class
 * owns file access and MODEL ASSEMBLY — turning the corpus's shared
 * {@code .pure} sources into the single model string legend-lite
 * compiles: section markers dropped, a Runtime synthesized. (Function
 * bodies are NOT stripped anymore — the platform compiles and executes
 * them; the old strip was retired with the harness-runs-through-the-
 * platform rule. This header previously still advertised it.)
 */
public final class Corpus {

    /** The wire dialect for raw corpus SQL — ONE implementation (core DuckDb). */
    static final com.legend.sql.dialect.DuckDb DIALECT = new com.legend.sql.dialect.DuckDb();

    /**
     * Root of the local legend-engine checkout (the corpus is read in place).
     *
     * <p>Defaults under {@code user.home}, matching
     * {@code parser-equivalence}'s {@code Corpus.engineRoot()}. It previously
     * hard-coded another account's home, which exists and is readable on the
     * build machine — so the plain {@code mvn test} invocation silently read a
     * DIFFERENT checkout than the one the committed scoreboard was generated
     * against, and the sweep (which rewrites docs/RELATIONAL_CORPUS.md in
     * place) reported ~29 phantom missing tests. The runner's own regression
     * gate caught it, but only after the rewrite.
     *
     * <p>Override with {@code -Dlegend.engine.root=...} to point at a
     * different checkout. If the resolved path has no corpus, the sweep skips
     * via {@link #available()} rather than half-running.
     */
    public static final Path ENGINE_ROOT = com.legend.testing.Upstream.engine();

    public static final Path RELATIONAL = ENGINE_ROOT.resolve(com.legend.generators.UpstreamFiles.RELATIONAL);

    /** The PLATFORM M2M test model sources (shared::src/dest classes,
     * createInstances) — the relational corpus's cross-store tests map
     * onto these; they live in engine-core, outside the relational tree. */
    public static final Path M2M_TESTS = ENGINE_ROOT.resolve(
            "legend-engine-core/legend-engine-core-pure/"
            + "legend-engine-pure-code-compiled-core/"
            + "src/main/resources/core/store/m2m/tests");

    /** The engine's core Pure module root (legend-engine-pure-code-compiled-core). */
    public static final Path CORE_PURE = ENGINE_ROOT.resolve(com.legend.generators.UpstreamFiles.CORE_PURE);

    /** Named LIBRARY files admitted to the global module as elements
     * (never setups): each is a PROGRAM library a corpus family imports
     * (docs/WORLD_MAP.md rule 5 — "a loading rule that did not admit the
     * file"). The prelude generator scans the same list for the shapes
     * their signatures name, so the two stay in step. */
    public static final java.util.List<Path> LIBRARY_FILES =
            com.legend.generators.UpstreamFiles.LIBRARY_FILES.stream().map(ENGINE_ROOT::resolve).toList();

    /**
     * SHAPE files (PHASE3_DEMAND_CUT_HOMEWORK D1, USER 2026-09-08: "the most
     * simple thing that makes sense and still sticks to our tenets"): named
     * engine files whose CLASSES AND ENUMS enter the corpus graph and whose
     * functions do not — the corpus needs their shapes (it constructs and
     * reads them); the functions beside them are the engine's own machinery
     * (plan generation, the SQL printer, routing), which this platform
     * implements in Java or walls by name. Before batch 155 these 253 classes
     * rode in the PRELUDE because the corpus named them — T2 says a program's
     * library is graph material, by file. Two lists, two meanings:
     * LIBRARY_FILES are PROGRAMS admitted whole; SHAPE_FILES are declarations.
     */
    public static final java.util.List<Path> SHAPE_FILES =
            com.legend.generators.UpstreamFiles.SHAPE_FILES.stream().map(ENGINE_ROOT::resolve).toList();


    // NOTE (V7 tenet correction 2026-08-28): the corpus lane reads NO
    // legend-pure sources. The assert family is platform-owned registry
    // natives; reference checkouts feed TEST INPUT only (the corpus
    // trees above), never the platform.

    private Corpus() {
    }

    public static boolean available() {
        return Files.isDirectory(RELATIONAL);
    }

    public static String read(String relative) {
        try {
            return Files.readString(RELATIONAL.resolve(relative));
        } catch (IOException e) {
            throw new RuntimeException("corpus file missing: " + relative, e);
        }
    }

    // ===== model assembly =====


    /** Skip a function definition starting at {@code start}; returns the index after its body. */
    private static int skipFunction(String source, int start) {
        int n = source.length();
        int i = start;
        // 1. tagged-value/stereotype blocks between 'function' and the
        //    signature ({doc.doc = '...'} — headers span lines in the
        //    corpus): whole {...} blocks skip; the naive first-'{' read
        //    took the doc block as the body and leaked the rest as
        //    top-level junk ('meta::...' wall family)
        while (i < n) {
            char c = source.charAt(i);
            if (c == '\'') {
                i = skipString(source, i);
                continue;
            }
            if (c == '(') {
                break;
            }
            if (c == '{') {
                i = skipBraces(source, i);
                continue;
            }
            i++;
        }
        // 2. the parameter list (paren-balanced; braces inside generic
        //    types don't count)
        int depth = 0;
        while (i < n) {
            char c = source.charAt(i);
            if (c == '\'') {
                i = skipString(source, i);
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    i++;
                    break;
                }
            }
            i++;
        }
        // 3. the body: the first '{' after the signature that is not a
        //    generic type's (Function<{...}> return types open with '<{')
        while (i < n) {
            char c = source.charAt(i);
            if (c == '\'') {
                i = skipString(source, i);
                continue;
            }
            if (c == '{') {
                int p = i - 1;
                while (p >= 0 && Character.isWhitespace(source.charAt(p))) {
                    p--;
                }
                if (p >= 0 && source.charAt(p) == '<') {
                    i = skipBraces(source, i);
                    continue;
                }
                return skipBraces(source, i);
            }
            i++;
        }
        return n;
    }

    /** Index just past the balanced {@code {...}} block opening at {@code open}. */
    private static int skipBraces(String source, int open) {
        int depth = 0;
        int i = open;
        while (i < source.length()) {
            char c = source.charAt(i);
            if (c == '\'') {
                i = skipString(source, i);
                continue;
            }
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return i + 1;
                }
            }
            i++;
        }
        return source.length();
    }

    /** Index just past a single-quoted pure string starting at {@code i} (handles {@code \'}). */
    static int skipString(String source, int i) {
        int n = source.length();
        i++;   // opening quote
        while (i < n) {
            char c = source.charAt(i);
            if (c == '\\') {
                i += 2;
                continue;
            }
            if (c == '\'') {
                return i + 1;
            }
            i++;
        }
        return n;
    }

    // ===== seed SQL =====



    // ===== table DDL from the store text =====









    // ===== BeforePackage seeds =====

    // ===== test extraction =====



}
