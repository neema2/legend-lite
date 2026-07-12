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

    /**
     * Model-element text of a corpus source: {@code ###...} markers and
     * {@code import} lines dropped (imports are resolved per test file for
     * QUERY names; model-internal names in the corpus's shared files are
     * package-local and resolve via the model parser's own import handling),
     * and every {@code function}/{@code native function} definition removed
     * (string-aware brace matching — bodies carry quoted braces).
     */
    public static String modelElements(String source) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        int n = source.length();
        while (i < n) {
            int lineEnd = source.indexOf('\n', i);
            if (lineEnd < 0) {
                lineEnd = n;
            }
            String line = source.substring(i, lineEnd);
            String trimmed = line.strip();
            if (trimmed.startsWith("###")) {
                i = lineEnd + 1;
                continue;
            }
            if (trimmed.startsWith("function ") || trimmed.startsWith("function<")
                    || trimmed.matches("function\\s+<<.*")
                    || trimmed.startsWith("native function ")) {
                i = skipFunction(source, i);
                continue;
            }
            out.append(line).append('\n');
            i = lineEnd + 1;
        }
        return out.toString();
    }

    /** Skip a function definition starting at {@code start}; returns the index after its body. */
    private static int skipFunction(String source, int start) {
        int n = source.length();
        int i = start;
        // find the opening brace of the BODY (skip constraint blocks? corpus
        // test helpers have none between signature and body in practice —
        // the first top-level '{' after the signature opens the body)
        while (i < n && source.charAt(i) != '{') {
            if (source.charAt(i) == '\'') {
                i = skipString(source, i);
            } else {
                i++;
            }
        }
        int depth = 0;
        while (i < n) {
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
        return n;
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

    private static final Pattern EXECUTE_IN_DB = Pattern.compile(
            "executeInDb\\s*\\(\\s*'");

    /**
     * Every {@code executeInDb('...')} SQL literal in a corpus source, in
     * order, unescaped. These are the seed DDL/DML the engine harness runs
     * against H2 — legend-lite runs them (dialect-translated) on DuckDB.
     */
    public static List<String> seedSql(String source) {
        List<String> out = new ArrayList<>();
        Matcher m = EXECUTE_IN_DB.matcher(source);
        while (m.find()) {
            int strStart = m.end() - 1;
            int strEnd = skipString(source, strStart);
            String lit = source.substring(strStart + 1, strEnd - 1);
            out.add(lit.replace("\\'", "'").replace("\\\\", "\\"));
        }
        return out;
    }

    // ===== table DDL from the store text =====

    public record TableDef(String schema, String name, String columnsText) {
        public String createSql() {
            StringBuilder cols = new StringBuilder();
            // split on top-level commas; strip PRIMARY KEY markers; quote names
            int depth = 0;
            int start = 0;
            List<String> parts = new ArrayList<>();
            for (int i = 0; i < columnsText.length(); i++) {
                char c = columnsText.charAt(i);
                if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    depth--;
                } else if (c == ',' && depth == 0) {
                    parts.add(columnsText.substring(start, i));
                    start = i + 1;
                }
            }
            parts.add(columnsText.substring(start));
            for (String raw : parts) {
                String col = raw.strip().replaceAll("(?i)\\s+PRIMARY\\s+KEY", "")
                        .replaceAll("(?i)\\s+NOT\\s+NULL", "");
                int sp = col.startsWith("\"") ? col.indexOf('"', 1) + 1 : firstSpace(col);
                String name = col.substring(0, sp).strip();
                String type = col.substring(sp).strip();
                if (!name.startsWith("\"")) {
                    name = '"' + name + '"';
                }
                if (cols.length() > 0) {
                    cols.append(", ");
                }
                cols.append(name).append(' ').append(type);
            }
            String qualified = schema.isEmpty() || schema.equals("default")
                    ? name : schema + "." + name;
            return "CREATE TABLE " + qualified + " (" + cols + ")";
        }

        private static int firstSpace(String s) {
            for (int i = 0; i < s.length(); i++) {
                if (Character.isWhitespace(s.charAt(i))) {
                    return i;
                }
            }
            return s.length();
        }
    }

    private static final Pattern TABLE_DEF = Pattern.compile(
            "(?m)^\\s*Table\\s+(\\w+)\\s*\\(");
    private static final Pattern SCHEMA_DEF = Pattern.compile(
            "(?m)^\\s*Schema\\s+(\\w+)\\s*");

    /**
     * Every {@code Table name(...)} definition in a corpus source, with its
     * enclosing {@code Schema} (tracked by brace/paren position). The
     * column text carries the store's own SQL types — the DDL the engine's
     * {@code dropAndCreateTableInDb} would generate.
     */
    public static Map<String, TableDef> tableDefs(String source) {
        Map<String, TableDef> out = new LinkedHashMap<>();
        // schema regions: from 'Schema X (' to its matching close paren
        record Region(int start, int end, String name) { }
        List<Region> regions = new ArrayList<>();
        Matcher sm = SCHEMA_DEF.matcher(source);
        while (sm.find()) {
            int open = source.indexOf('(', sm.end());
            if (open < 0) {
                continue;
            }
            int depth = 0;
            int i = open;
            while (i < source.length()) {
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
                        break;
                    }
                }
                i++;
            }
            regions.add(new Region(open, i, sm.group(1)));
        }
        Matcher m = TABLE_DEF.matcher(source);
        while (m.find()) {
            int open = source.indexOf('(', m.end() - 1);
            int depth = 0;
            int i = open;
            while (i < source.length()) {
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
                        break;
                    }
                }
                i++;
            }
            String cols = source.substring(open + 1, i);
            final int pos = m.start();
            String schema = regions.stream()
                    .filter(r -> r.start() < pos && pos < r.end())
                    .map(Region::name).findFirst().orElse("");
            // first definition wins (dbInc tables re-appear via includes)
            out.putIfAbsent(schema + "." + m.group(1),
                    new TableDef(schema, m.group(1), cols.strip()));
        }
        return out;
    }

    // ===== test extraction =====

    public record TestFn(String fqn, String body, Map<String, String> imports,
                         List<String> wildcardImports) {
    }

    private static final Pattern TEST_FN = Pattern.compile(
            "function\\s+<<[^>]*test\\.Test[^>]*>>\\s+((?:\\w+::)*\\w+)\\s*\\(\\s*\\)\\s*:\\s*Boolean\\s*\\[1\\]\\s*");

    private static final Pattern IMPORT_LINE = Pattern.compile(
            "^import\\s+((?:\\w+::)+)(\\*|\\w+)\\s*;", Pattern.MULTILINE);

    /** All {@code <<test.Test>>} zero-arg Boolean functions in a corpus source. */
    public static List<TestFn> testFunctions(String source) {
        Map<String, String> typeImports = new LinkedHashMap<>();
        List<String> wildcards = new ArrayList<>();
        Matcher im = IMPORT_LINE.matcher(source);
        while (im.find()) {
            String pkg = im.group(1);
            String name = im.group(2);
            if (name.equals("*")) {
                wildcards.add(pkg.substring(0, pkg.length() - 2));
            } else {
                typeImports.put(name, pkg + name);
            }
        }
        List<TestFn> out = new ArrayList<>();
        Matcher m = TEST_FN.matcher(source);
        while (m.find()) {
            int bodyStart = source.indexOf('{', m.end());
            if (bodyStart < 0) {
                continue;
            }
            int end = skipFunction(source, m.start());
            String body = source.substring(bodyStart + 1, end - 1);
            out.add(new TestFn(m.group(1), body, typeImports, wildcards));
        }
        return out;
    }
}
