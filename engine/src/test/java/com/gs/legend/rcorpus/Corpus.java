// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.gs.legend.rcorpus;

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
 * {@code .pure} sources into the single model string legend-lite compiles:
 * section markers dropped, function bodies stripped (they are engine-runtime
 * helpers: executeInDb seeds, test bodies), a Runtime synthesized.
 */
public final class Corpus {

    /** The wire dialect for raw corpus SQL — ONE implementation (core DuckDb). */
    static final com.legend.sql.dialect.DuckDb DIALECT = new com.legend.sql.dialect.DuckDb();

    /** Root of the local legend-engine checkout (the corpus is read in place). */
    public static final Path ENGINE_ROOT = Path.of(System.getProperty(
            "legend.engine.root", "/Users/neema/legend/legend-engine"));

    public static final Path RELATIONAL = ENGINE_ROOT.resolve(
            "legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/"
            + "legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/"
            + "src/main/resources/core_relational/relational");

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
