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
        // let-bound string constants (values may themselves concatenate)
        Map<String, String> lets = new LinkedHashMap<>();
        Matcher lm = LET_STRING.matcher(source);
        while (lm.find()) {
            int start = lm.end();
            int end = exprEnd(source, start);
            String folded = foldConcat(source.substring(start, end), lets);
            if (folded != null) {
                lets.put(lm.group(1), folded);
            }
        }
        List<String> out = new ArrayList<>();
        Matcher m = EXECUTE_IN_DB.matcher(source);
        while (m.find()) {
            int argStart = m.end();
            int end = exprEnd(source, argStart);
            String folded = foldConcat(source.substring(argStart, end), lets);
            if (folded != null) {
                out.add(quoteInsertColumns(folded));
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
        return expectPart ? null
                : out.toString().replace("\\'", "'").replace("\\\\", "\\");
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
        sql = sql.replaceAll("(?i)\\bFLOAT\\b", "DOUBLE");
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
                out.append('\"').append(head).append('\"').append(col.substring(sp));
            }
        }
        return sql.substring(0, bodyStart) + out + sql.substring(end - 1);
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
                        .replaceAll("(?i)\\s+NOT\\s+NULL", "")
                        // H2's FLOAT is an 8-byte double; DuckDB's is REAL —
                        // engine-parity means DOUBLE
                        .replaceAll("(?i)\\bFLOAT\\b", "DOUBLE");
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

    // ===== BeforePackage seeds =====

    public record BeforePackage(String pkg, boolean callsBaseSetup, List<String> sql,
            String body) {
    }

    private static final Pattern BEFORE_PKG = Pattern.compile(
            "function\\s+<<[^>]*test\\.BeforePackage[^>]*>>\\s+((?:\\w+::)*\\w+)::\\w+\\s*\\(");

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
        StringBuilder expanded = new StringBuilder(body);
        java.util.Set<String> seen = new java.util.HashSet<>();
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

    private static int sharedPrefix(String a, String b) {
        int i = 0;
        while (i < a.length() && i < b.length() && a.charAt(i) == b.charAt(i)) {
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
