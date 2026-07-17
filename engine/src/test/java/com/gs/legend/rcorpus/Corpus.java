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

    public record TableDef(String schema, String name, String columnsText) {
        public String createSql() {
            return createSql(Map.of());
        }

        /**
         * {@code seedTypeOverrides}: the RUNTIME truth. The engine executes
         * the harness's raw {@code executeInDb('create table ...')} — where
         * a seed statement declares a column type that differs from the
         * store DDL (milestoning setUp declares {@code from_z DATE} but
         * seeds {@code from_z TIMESTAMP}), the SEED type is what H2 ran
         * with, and the wire reflects it.
         */
        public String createSql(Map<String, String> seedTypeOverrides) {
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
                // a milestoning(business(...)) block is a table DIRECTIVE,
                // not a column — strip the balanced block; the FIRST column
                // follows it in the SAME comma part (no separating comma)
                String stripped = raw.strip();
                if (stripped.startsWith("milestoning")) {
                    int open = stripped.indexOf('(');
                    int d = 0;
                    int k = open;
                    while (k < stripped.length()) {
                        char ch = stripped.charAt(k);
                        if (ch == '(') {
                            d++;
                        } else if (ch == ')') {
                            d--;
                            if (d == 0) {
                                break;
                            }
                        }
                        k++;
                    }
                    raw = stripped.substring(Math.min(k + 1, stripped.length()));
                    if (raw.strip().isEmpty()) {
                        continue;
                    }
                }
                String col = raw.strip().replaceAll("(?i)\\s+PRIMARY\\s+KEY", "")
                        .replaceAll("(?i)\\s+NOT\\s+NULL", "");
                int sp = col.startsWith("\"") ? col.indexOf('"', 1) + 1 : firstSpace(col);
                String name = col.substring(0, sp).strip();
                // H2's FLOAT is an 8-byte double; DuckDB's is REAL —
                // engine-parity means DOUBLE. TYPE position only: a column
                // may literally be NAMED float (audit C1).
                String type = col.substring(sp).strip()
                        .replaceAll("(?i)\\bFLOAT\\b", "DOUBLE");
                String bare = name.startsWith("\"")
                        ? name.substring(1, name.length() - 1) : name;
                String override = seedTypeOverrides.get(bare.toLowerCase());
                // ONLY the temporal-precision mismatch (declared DATE,
                // seeded TIMESTAMP or vice versa) — anything wider trusts
                // the store declaration (the seed text parse is heuristic)
                if (override != null
                        && type.toUpperCase().matches("DATE|TIMESTAMP")
                        && override.toUpperCase().matches("DATE|TIMESTAMP")
                        && !type.equalsIgnoreCase(override)) {
                    type = override.toUpperCase();
                }
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
            // OR REPLACE: a family/file setup legitimately REDEFINES a base
            // table (the engine harness's per-package setUp owns the table);
            // the last definition before the test wins
            return "CREATE OR REPLACE TABLE " + qualified + " (" + cols + ")";
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

    private record SeedTable(String qualified, Map<String, String> types) {}

    /** Column types from the harness's own {@code create table} seed
     * statements, per table (lower-cased names) — the runtime truth the
     * engine executed with. */
    public static Map<String, Map<String, String>> seedColumnTypes(String source) {
        Map<String, Map<String, String>> out = new LinkedHashMap<>();
        for (var e : seedColumnDefs(source).entrySet()) {
            Map<String, String> lower = new LinkedHashMap<>();
            e.getValue().types.forEach((k, v) -> lower.put(k.toLowerCase(), v));
            out.put(e.getKey(), lower);
        }
        return out;
    }

    private static Map<String, SeedTable> seedColumnDefs(String source) {
        Map<String, SeedTable> out = new LinkedHashMap<>();
        Matcher m = Pattern.compile(
                "(?i)create table (?:if not exists )?([A-Za-z0-9_.]+)\\s*\\(([^;]*?)\\)\\s*;")
                .matcher(source);
        while (m.find()) {
            String table = m.group(1);
            String cols = m.group(2);
            Map<String, String> types = new LinkedHashMap<>();
            int depth = 0;
            int start = 0;
            List<String> parts = new ArrayList<>();
            for (int i = 0; i < cols.length(); i++) {
                char c = cols.charAt(i);
                if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    depth--;
                } else if (c == ',' && depth == 0) {
                    parts.add(cols.substring(start, i));
                    start = i + 1;
                }
            }
            parts.add(cols.substring(start));
            boolean ok = !parts.isEmpty();
            for (String raw : parts) {
                String col = raw.strip().replaceAll("(?i)\\s+PRIMARY\\s+KEY", "")
                        .replaceAll("(?i)\\s+NOT\\s+NULL", "");
                int sp = 0;
                while (sp < col.length() && !Character.isWhitespace(col.charAt(sp))) {
                    sp++;
                }
                if (sp == 0 || sp >= col.length()) {
                    ok = false;
                    break;
                }
                types.put(col.substring(0, sp), col.substring(sp).strip());
            }
            if (!ok) {
                continue;   // heuristic parse — skip anything surprising
            }
            String simple = table.contains(".")
                    ? table.substring(table.lastIndexOf('.') + 1) : table;
            out.put(simple.toLowerCase(), new SeedTable(table, types));
        }
        return out;
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
    /** All defs per {@code schema.name} key — one physical table can have
     * SEVERAL declarations across Database blocks in one file (milestoning
     * setUp declares four ProductTables); the runner picks the one the
     * SEEDS actually created. */
    public static Map<String, List<TableDef>> tableDefsAll(String source) {
        Map<String, List<TableDef>> out = new LinkedHashMap<>();
        for (var e : tableDefsList(source)) {
            out.computeIfAbsent(e.schema() + "." + e.name(),
                    k -> new ArrayList<>()).add(e);
        }
        return out;
    }

    private static List<TableDef> tableDefsList(String source) {
        List<TableDef> out = new ArrayList<>();
        Map<String, TableDef> dedup = tableDefsInternal(source, out);
        return out;
    }

    private static Map<String, TableDef> tableDefsInternal(String source,
            List<TableDef> all) {
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
            TableDef def = new TableDef(schema, m.group(1), cols.strip());
            if (all != null) {
                all.add(def);
            }
            // first definition wins (dbInc tables re-appear via includes)
            out.putIfAbsent(schema + "." + m.group(1), def);
        }
        return out;
    }

    // ===== BeforePackage seeds =====

    // ===== test extraction =====



}
