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
        boolean inDiagram = false;
        while (i < n) {
            int lineEnd = source.indexOf('\n', i);
            if (lineEnd < 0) {
                lineEnd = n;
            }
            String line = source.substring(i, lineEnd);
            String trimmed = line.strip();
            if (trimmed.startsWith("###")) {
                // ###Diagram sections are visual metadata, not model — and
                // their bodies (#FFFFCC color literals) don't even lex
                inDiagram = trimmed.equals("###Diagram");
                i = lineEnd + 1;
                continue;
            }
            if (inDiagram) {
                i = lineEnd + 1;
                continue;
            }
            if (trimmed.startsWith("function ") || trimmed.startsWith("function<")
                    || trimmed.matches("function\\s+<<.*")
                    || trimmed.equals("function")
                    || trimmed.startsWith("native function ")) {
                int end = skipFunction(source, i);
                // Relation-mapping provider functions (~func targets —
                // zero-arg, returning Relation<...>) are MODEL, not test
                // machinery: they must survive into the compiled text
                String fnText = source.substring(i, end);
                if (fnText.substring(0, Math.min(fnText.length(), fnText.indexOf('{') < 0
                                ? fnText.length() : fnText.indexOf('{')))
                        .matches("(?s).*\\(\\s*\\)\\s*:\\s*"
                                + "(?:meta::pure::metamodel::relation::)?Relation\\s*<.*")) {
                    out.append(fnText).append('\n');
                }
                i = end;
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

    private static final Pattern EXECUTE_IN_DB = Pattern.compile(
            "executeInDb\\w*\\s*\\(");

    private static final Pattern LET_STRING = Pattern.compile(
            "let\\s+(\\w+)\\s*=\\s*(?=')");

    /**
     * Every {@code executeInDb(...)} SQL argument in a corpus source, in
     * order, unescaped. The argument may be a single literal or a
     * CONCATENATION of literals and {@code let}-bound string variables
     * ({@code executeInDb($s + '(1, ...)')} — the corpus's row-insert
     * idiom); anything unresolvable is skipped (silently absent seeds
     * surface as loud row diffs / seed-failure accounting downstream).
     */
    public static List<String> seedSql(String source) {
        // let-bound string constants resolve IN TEXT ORDER: a binding
        // applies to the executeInDb calls that FOLLOW it until re-bound.
        // (A whole-file last-write-wins map attached one function's rows to
        // another function's insert header — the same variable name `s` is
        // re-bound per setup function across the corpus.)
        List<int[]> letAt = new ArrayList<>();      // [position, matcher index]
        List<String[]> letDefs = new ArrayList<>(); // [name, exprText]
        Matcher lm = LET_STRING.matcher(source);
        while (lm.find()) {
            int start = lm.end();
            int end = exprEnd(source, start);
            letAt.add(new int[]{lm.start(), letDefs.size()});
            letDefs.add(new String[]{lm.group(1), source.substring(start, end)});
        }
        List<String> out = new ArrayList<>();
        Map<String, String> lets = new LinkedHashMap<>();
        int nextLet = 0;
        Matcher m = EXECUTE_IN_DB.matcher(source);
        while (m.find()) {
            while (nextLet < letAt.size() && letAt.get(nextLet)[0] < m.start()) {
                String[] def = letDefs.get(letAt.get(nextLet)[1]);
                String folded = foldConcat(def[1], lets);
                if (folded != null) {
                    lets.put(def[0], folded);
                }
                nextLet++;
            }
            int argStart = m.end();
            int end = exprEnd(source, argStart);
            String folded = foldConcat(source.substring(argStart, end), lets);
            if (folded != null) {
                // dialect at the wire: H2 spells the niladic function with
                // parens, DuckDB without
                out.add(quoteInsertColumns(folded)
                        .replaceAll("(?i)\\bCURRENT_TIMESTAMP\\(\\)", "CURRENT_TIMESTAMP"));
            }
        }
        return out;
    }

    /** Index of the top-level {@code ,} or {@code )} or {@code ;} ending an expression. */
    private static int exprEnd(String source, int start) {
        int depth = 0;
        int i = start;
        while (i < source.length()) {
            char c = source.charAt(i);
            if (c == '\'') {
                i = skipString(source, i);
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                if (depth == 0) {
                    return i;
                }
                depth--;
            } else if ((c == ',' || c == ';') && depth == 0) {
                return i;
            }
            i++;
        }
        return i;
    }

    /**
     * Fold {@code 'lit' + $var + 'lit'} into one unescaped string; null when
     * any part is not a string literal or a known let-bound constant.
     */
    private static String foldConcat(String expr, Map<String, String> lets) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        boolean expectPart = true;
        while (i < expr.length()) {
            char c = expr.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }
            if (expectPart && c == '\'') {
                int end = skipString(expr, i);
                out.append(expr, i + 1, end - 1);
                i = end;
                expectPart = false;
                continue;
            }
            if (expectPart && c == '$') {
                int j = i + 1;
                while (j < expr.length() && Character.isJavaIdentifierPart(expr.charAt(j))) {
                    j++;
                }
                String val = lets.get(expr.substring(i + 1, j));
                if (val == null) {
                    return null;
                }
                out.append(val);
                i = j;
                expectPart = false;
                continue;
            }
            if (!expectPart && c == '+') {
                i++;
                expectPart = true;
                continue;
            }
            return null;   // not a foldable string expression
        }
        return expectPart ? null : unescapePureString(out.toString());
    }

    /** Pure string-literal escapes, decoded LEFT-TO-RIGHT (a replace chain
     * mis-decodes {@code \\n} — escaped backslash then literal n — into a
     * newline: the earlier replace consumes the second backslash). */
    private static String unescapePureString(String s) {
        StringBuilder out = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c != '\\' || i + 1 == s.length()) {
                out.append(c);
                continue;
            }
            char e = s.charAt(++i);
            switch (e) {
                case 'n' -> out.append('\n');
                case 'r' -> out.append('\r');
                case 't' -> out.append('\t');
                case '\'' -> out.append('\'');
                case '\\' -> out.append('\\');
                default -> out.append('\\').append(e);
            }
        }
        return out.toString();
    }

    private static final Pattern INSERT_COLS = Pattern.compile(
            "(?i)^(\\s*insert\\s+into\\s+[\\w.\"]+\\s*\\()([^)]*)(\\))");

    /**
     * Dialect adaptation at the wire boundary: quote every identifier in an
     * INSERT's column list. The corpus seeds use SQL-keyword column names
     * ({@code default, do, else, ...}) unquoted — legal on the engine's H2
     * setup, a syntax error on DuckDB. CREATE TABLE generation already
     * quotes; unquoted INSERTs would fail and leave the table empty.
     */
    private static final Pattern CREATE_HEAD = Pattern.compile(
            "(?i)^\\s*create\\s+table\\s+[\\w.\"]+\\s*\\(");

    static String quoteInsertColumns(String sql) {
        Matcher cm = CREATE_HEAD.matcher(sql);
        if (cm.find()) {
            return quoteCreateColumns(sql, cm.end());
        }
        Matcher m = INSERT_COLS.matcher(sql);
        if (!m.find()) {
            return sql;
        }
        StringBuilder cols = new StringBuilder();
        for (String c : m.group(2).split(",")) {
            if (cols.length() > 0) {
                cols.append(", ");
            }
            String name = c.strip();
            cols.append(name.startsWith("\"") ? name : "\"" + name + "\"");
        }
        return m.group(1) + cols + m.group(3) + sql.substring(m.end(3));
    }

    /**
     * Quote the column NAME of each top-level column definition in a
     * CREATE TABLE literal (constraint entries — PRIMARY KEY(...) etc —
     * pass through). Keyword column names (default, do, else...) are legal
     * on the engine's H2 setup and a syntax error on DuckDB unquoted.
     */
    private static String quoteCreateColumns(String sql, int bodyStart) {
        int depth = 1;
        int end = bodyStart;
        while (end < sql.length() && depth > 0) {
            char c = sql.charAt(end);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            }
            end++;
        }
        String body = sql.substring(bodyStart, end - 1);
        StringBuilder out = new StringBuilder();
        int d = 0;
        int start = 0;
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '(') {
                d++;
            } else if (c == ')') {
                d--;
            } else if (c == ',' && d == 0) {
                parts.add(body.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(body.substring(start));
        for (String part : parts) {
            String col = part.strip();
            if (out.length() > 0) {
                out.append(", ");
            }
            int sp = 0;
            while (sp < col.length() && !Character.isWhitespace(col.charAt(sp))
                    && col.charAt(sp) != '(') {
                sp++;
            }
            String head = col.substring(0, sp);
            if (col.startsWith("\"")
                    || head.matches("(?i)primary|constraint|foreign|unique|check")) {
                out.append(col);
            } else {
                // FLOAT→DOUBLE on the TYPE PART only (H2's FLOAT is an
                // 8-byte double; DuckDB's is REAL) — the whole-statement
                // replace also renamed a column literally named "float"
                // (relationalSetUp testTable, audit C1)
                out.append('\"').append(head).append('\"').append(
                        col.substring(sp).replaceAll("(?i)\\bFLOAT\\b", "DOUBLE"));
            }
        }
        return sql.substring(0, bodyStart) + out + sql.substring(end - 1);
    }

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

    /**
     * CREATE statements rebuilt from the harness's own
     * {@code executeInDb('create table ...')} seeds — the RUNTIME truth
     * the engine executed with (the declared store DDL can disagree in
     * both type AND shape: milestoning setUp declares four different
     * ProductTables across Database blocks; H2 held the seeded one).
     * Keyed by lower-cased simple name; values are DuckDB-ready.
     */
    public static Map<String, String> seedCreateSql(String source) {
        Map<String, String> out = new LinkedHashMap<>();
        for (var e : seedColumnDefs(source).entrySet()) {
            StringBuilder cols = new StringBuilder();
            for (var c : e.getValue().types.entrySet()) {
                if (cols.length() > 0) {
                    cols.append(", ");
                }
                cols.append('"').append(c.getKey()).append("\" ")
                        .append(c.getValue().replaceAll("(?i)\\bFLOAT\\b", "DOUBLE"));
            }
            String create = "CREATE OR REPLACE TABLE " + e.getValue().qualified
                    + " (" + cols + ")";
            if (e.getValue().qualified.contains(".")) {
                create = "CREATE SCHEMA IF NOT EXISTS " + e.getValue().qualified
                        .substring(0, e.getValue().qualified.lastIndexOf('.'))
                        + "; " + create;
            }
            out.put(e.getKey(), create);
        }
        return out;
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

    public static Map<String, TableDef> tableDefs(String source) {
        return tableDefsInternal(source, null);
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

    public record BeforePackage(String pkg, String fqn, boolean callsBaseSetup,
            List<String> sql, String body) {
    }

    private static final Pattern BEFORE_PKG = Pattern.compile(
            "function\\s+<<[^>]*test\\.BeforePackage[^>]*>>\\s+((?:\\w+::)*\\w+)::(\\w+)\\s*\\(");

    /**
     * {@code <<test.BeforePackage>>} setup functions: the engine harness runs
     * them before the package's tests. Each carries its own executeInDb
     * literals and (usually) a call to the shared createTablesAndFillDb.
     */
    public static List<BeforePackage> beforePackages(String source) {
        List<BeforePackage> out = new ArrayList<>();
        Matcher m = BEFORE_PKG.matcher(source);
        while (m.find()) {
            int bodyStart = source.indexOf('{', m.end());
            if (bodyStart < 0) {
                continue;
            }
            int depth = 0;
            int i = bodyStart;
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
                        break;
                    }
                }
                i++;
            }
            String body = source.substring(bodyStart, i);
            // a BeforePackage often just CALLS setup helpers (possibly in
            // SIBLING files — initDatabase() lives next door): the closure
            // expands LAZILY against the whole-run function corpus
            out.add(new BeforePackage(m.group(1),
                    m.group(1) + "::" + m.group(2),
                    body.contains("createTablesAndFillDb"),
                    seedSql(expandCalls(body, source)), body));
        }
        return out;
    }

    /**
     * Seeds of {@code body} expanded against a CROSS-FILE function corpus
     * keyed by FQN. Bare calls resolve to the candidate sharing the LONGEST
     * package prefix with {@code homePkg} (files reuse simple names like
     * createTablesAndFillDb across families — first-wins was wrong seeds).
     */
    public static List<String> expandSeeds(String body, String homePkg,
            Map<String, String> fqnFns) {
        return expandSeeds(body, homePkg, fqnFns, new java.util.HashSet<>(), true);
    }

    /**
     * Run-once emulation across the WHOLE seed replay: {@code seen} carries
     * every function FQN whose body literals are already in the stream
     * (raw shared-file seeds + earlier BeforePackage expansions), so the
     * same setup function never seeds twice — while duplicate statements
     * WITHIN one function body (deliberately repeated inserts feeding
     * distinct() tests) are preserved. {@code includeBody=false} walks the
     * body's CALLS without re-emitting its own literals (a BeforePackage
     * whose defining file already seeded raw).
     */
    public static List<String> expandSeeds(String body, String homePkg,
            Map<String, String> fqnFns, java.util.Set<String> seen,
            boolean includeBody) {
        StringBuilder expanded = new StringBuilder(includeBody ? body : "");
        java.util.ArrayDeque<String> queue = new java.util.ArrayDeque<>();
        queue.add(body);
        while (!queue.isEmpty()) {
            String cur = queue.poll();
            Matcher call = Pattern.compile("((?:[\\w$]+::)*)([\\w$]+)\\s*\\(\\s*\\)").matcher(cur);
            while (call.find()) {
                String qualifier = call.group(1);
                String name = call.group(2);
                String resolved = null;
                if (!qualifier.isEmpty()) {
                    String fqn = qualifier + name;
                    resolved = fqnFns.containsKey(fqn) ? fqn : null;
                } else {
                    int best = -1;
                    int bestLen = Integer.MAX_VALUE;
                    for (String fqn : fqnFns.keySet()) {
                        if (!fqn.endsWith("::" + name)) {
                            continue;
                        }
                        int shared = sharedPrefix(fqn, homePkg);
                        // tie-break on the SHORTEST fqn: the closest package
                        // (fewest segments beyond the shared prefix) wins
                        if (shared > best
                                || shared == best && fqn.length() < bestLen) {
                            best = shared;
                            bestLen = fqn.length();
                            resolved = fqn;
                        }
                    }
                }
                if (resolved != null && seen.add(resolved)) {
                    expanded.append('\n').append(fqnFns.get(resolved));
                    queue.add(fqnFns.get(resolved));
                }
            }
        }
        return seedSql(expanded.toString());
    }

    /**
     * Shared package prefix in WHOLE :: segments — a character-level count
     * let {@code ...::testA} score against {@code ...::testB} on partial
     * segment text, occasionally absorbing a sibling family's same-named
     * helper.
     */
    private static int sharedPrefix(String a, String b) {
        String[] as = a.split("::");
        String[] bs = b.split("::");
        int i = 0;
        while (i < as.length && i < bs.length && as[i].equals(bs[i])) {
            i++;
        }
        return i;
    }

    /** FQN → body of every zero-arg function in {@code source}. */
    public static Map<String, String> functionBodies(String source) {
        Map<String, String> fns = new LinkedHashMap<>();
        Matcher fm = Pattern.compile(
                "(?m)^function\\s+(?:<<[^>]*>>\\s*)?((?:[\\w$]+::)*[\\w$]+)\\s*\\(\\s*\\)").matcher(source);
        while (fm.find()) {
            int bs = source.indexOf('{', fm.end());
            if (bs < 0) {
                continue;
            }
            int depth = 0;
            int i = bs;
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
                        break;
                    }
                }
                i++;
            }
            fns.putIfAbsent(fm.group(1), source.substring(bs, Math.min(i + 1, source.length())));
        }
        return fns;
    }

    /** {@code body} plus the bodies of same-source functions it calls (transitively). */
    private static String expandCalls(String body, String source) {
        Map<String, String> fns = new LinkedHashMap<>();
        Matcher fm = Pattern.compile(
                "(?m)^function\\s+(?:<<[^>]*>>\\s*)?(?:[\\w$]+::)*([\\w$]+)\\s*\\(\\s*\\)").matcher(source);
        while (fm.find()) {
            int bs = source.indexOf('{', fm.end());
            if (bs < 0) {
                continue;
            }
            int depth = 0;
            int i = bs;
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
                        break;
                    }
                }
                i++;
            }
            fns.putIfAbsent(fm.group(1), source.substring(bs, Math.min(i + 1, source.length())));
        }
        StringBuilder expanded = new StringBuilder(body);
        java.util.Set<String> seen = new java.util.HashSet<>();
        java.util.ArrayDeque<String> queue = new java.util.ArrayDeque<>();
        queue.add(body);
        while (!queue.isEmpty()) {
            String cur = queue.poll();
            Matcher call = Pattern.compile("(?:[\\w$]+::)*([\\w$]+)\\s*\\(\\s*\\)").matcher(cur);
            while (call.find()) {
                String callee = call.group(1);
                if (seen.add(callee) && fns.containsKey(callee)) {
                    expanded.append('\n').append(fns.get(callee));
                    queue.add(fns.get(callee));
                }
            }
        }
        return expanded.toString();
    }

    // ===== test extraction =====

    public record TestFn(String fqn, String body, Map<String, String> imports,
                         List<String> wildcardImports) {
    }

    private static final Pattern TEST_FN = Pattern.compile(
            "(?m)^\\s*function\\s+(<<[^>]*test\\.Test[^>]*>>)\\s+((?:\\w+::)*\\w+)\\s*\\(\\s*\\)\\s*:\\s*Boolean\\s*\\[1\\]\\s*");

    private static final Pattern IMPORT_LINE = Pattern.compile(
            "^import\\s+((?:\\w+::)+)(\\*|\\w+)\\s*;", Pattern.MULTILINE);

    /**
     * All {@code <<test.Test>>} zero-arg Boolean functions in a corpus source.
     *
     * <p>Imports are SECTION-scoped (real pure): each {@code ###X} marker
     * opens a fresh import group and a test resolves under ITS section's
     * imports only. File-wide collection cross-contaminated:
     * testUnionPartial.pure section 1 imports {@code partial::*} while later
     * mapping sections import {@code union::*} — merged, 'unionMapping' was
     * ambiguous; the engine resolves it to {@code partial::unionMapping}.
     */
    public static List<TestFn> testFunctions(String source) {
        List<Integer> sectionStarts = new ArrayList<>();
        Matcher sec = Pattern.compile("(?m)^###\\w+").matcher(source);
        while (sec.find()) {
            sectionStarts.add(sec.start());
        }
        int sections = sectionStarts.size() + 1;
        List<Map<String, String>> typeImportsBySec = new ArrayList<>(sections);
        List<List<String>> wildcardsBySec = new ArrayList<>(sections);
        for (int i = 0; i < sections; i++) {
            typeImportsBySec.add(new LinkedHashMap<>());
            wildcardsBySec.add(new ArrayList<>());
        }
        Matcher im = IMPORT_LINE.matcher(source);
        while (im.find()) {
            int s = sectionOf(sectionStarts, im.start());
            String pkg = im.group(1);
            String name = im.group(2);
            if (name.equals("*")) {
                wildcardsBySec.get(s).add(pkg.substring(0, pkg.length() - 2));
            } else {
                typeImportsBySec.get(s).put(name, pkg + name);
            }
        }
        List<TestFn> out = new ArrayList<>();
        Matcher m = TEST_FN.matcher(source);
        while (m.find()) {
            // the engine harness never runs ToFix/Ignore tests — counting
            // them would add PASSes the engine itself doesn't certify
            String stereotypes = m.group(1);
            if (stereotypes.contains("test.ToFix") || stereotypes.contains("test.Ignore")
                    || stereotypes.contains("test.ExcludeAlloy")) {
                continue;
            }
            int bodyStart = source.indexOf('{', m.end());
            if (bodyStart < 0) {
                continue;
            }
            int end = skipFunction(source, m.start());
            // //-comments are STRIPPED from the body (string-aware): a lone
            // apostrophe or '#' in a comment flips string/island parity for
            // every extractor downstream (audit 8 D1 — a live regression
            // swallowed a real execute() span)
            String body = stripLineComments(source.substring(bodyStart + 1, end - 1));
            int s = sectionOf(sectionStarts, m.start());
            out.add(new TestFn(m.group(2), body, typeImportsBySec.get(s),
                    wildcardsBySec.get(s)));
        }
        return out;
    }

    /** Index of the {@code ###}-delimited section containing {@code pos}. */
    private static int sectionOf(List<Integer> sectionStarts, int pos) {
        int s = 0;
        for (int i = 0; i < sectionStarts.size(); i++) {
            if (sectionStarts.get(i) <= pos) {
                s = i + 1;
            }
        }
        return s;
    }

    /** Remove {@code // ...} line comments, respecting string literals. */
    static String stripLineComments(String s) {
        StringBuilder out = new StringBuilder(s.length());
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '\'') {
                int end = skipString(s, i);
                out.append(s, i, end);
                i = end;
                continue;
            }
            if (c == '/' && i + 1 < s.length() && s.charAt(i + 1) == '/') {
                while (i < s.length() && s.charAt(i) != '\n') {
                    i++;
                }
                continue;
            }
            out.append(c);
            i++;
        }
        return out.toString();
    }
}
