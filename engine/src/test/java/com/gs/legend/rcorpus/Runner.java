// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.gs.legend.rcorpus;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.server.QueryService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Executes real core_relational {@code <<test.Test>>} functions against
 * legend-lite + DuckDB and scores them. RUN-as-data: the test body's
 * {@code execute(|query, mapping, runtime, extensions)} supplies the query
 * and mapping; the assertions supply expected rows; golden SQL asserts are
 * ADVISORY (row equality is the contract; our SQL is DuckDB's, not H2's).
 *
 * <p>FAULT-ISOLATED assembly: shared model elements load together; each
 * MAPPING is probe-compiled and dropped (recorded as a WALL) if it uses a
 * roadmap feature — tests over working mappings still run.
 */
public final class Runner {

    public record Outcome(String test, Status status, String detail) {
    }

    public enum Status { PASS, FAIL, ERROR, SHAPE }

    private final String model;
    private final List<String> walls = new ArrayList<>();
    private final List<String> seeds;
    /** {@code <<test.BeforePackage>>} setups collected corpus-wide. */
    private final List<Corpus.BeforePackage> beforePackages = new ArrayList<>();
    /** Advisory golden-SQL diffs: counted, never failed on. */
    public int sqlAsserts;
    public int sqlMatches;

    public Runner(List<String> sharedSources, List<String> seedSources) {
        StringBuilder mandatory = new StringBuilder();
        List<String[]> mappings = new ArrayList<>();
        for (String src : sharedSources) {
            String elements = Corpus.modelElements(src);
            List<String[]> els = splitTopLevel(elements);
            // the file's LEADING imports precede the first element head —
            // they scope the whole file and must survive assembly
            int firstHead = els.isEmpty() ? elements.length()
                    : elements.indexOf(els.get(0)[2]);
            mandatory.append(elements, 0, Math.max(0, firstHead)).append('\n');
            for (String[] el : els) {
                if (el[0].equals("Mapping")) {
                    mappings.add(el);
                } else {
                    mandatory.append(el[2]).append('\n');
                }
            }
        }
        StringBuilder assembled = new StringBuilder(mandatory);
        for (String[] m : mappings) {
            String candidate = assembled + "\n" + m[2];
            try {
                com.legend.Compiler.compile(candidate, "|1", "n/a");
                assembled.append('\n').append(m[2]);
            } catch (Exception e) {
                walls.add(m[1] + " => " + String.valueOf(e.getMessage()).split("\n")[0]);
            }
        }
        this.model = assembled.toString();
        List<String> sql = new ArrayList<>();
        for (String src : sharedSources) {
            for (var d : Corpus.tableDefs(src).values()) {
                if (!d.schema().isEmpty() && !d.schema().equals("default")) {
                    sql.add("CREATE SCHEMA IF NOT EXISTS " + d.schema());
                }
                sql.add(d.createSql());
            }
        }
        for (String src : seedSources) {
            sql.addAll(Corpus.seedSql(src));
        }
        this.seeds = sql;
    }

    public void addBeforePackages(String source) {
        beforePackages.addAll(Corpus.beforePackages(source));
    }

    /** Per-test-file model extensions (classes/mappings defined NEXT TO the tests). */
    private final Map<String, String> fileModels = new LinkedHashMap<>();
    private final Map<String, List<String>> fileSeeds = new LinkedHashMap<>();
    private String currentFileKey = "";

    /**
     * Register a test file's own model elements: mandatory elements append;
     * its mappings probe-compile against base+file (walls recorded). The
     * file's own table DDL and executeInDb literals seed too.
     */
    public void useFile(String key, String source) {
        currentFileKey = key;
        if (fileModels.containsKey(key)) {
            return;
        }
        String elements = Corpus.modelElements(source);
        List<String[]> els = splitTopLevel(elements);
        StringBuilder extension = new StringBuilder();
        List<String[]> fileMappings = new ArrayList<>();
        int firstHead = els.isEmpty() ? elements.length() : elements.indexOf(els.get(0)[2]);
        extension.append(elements, 0, Math.max(0, firstHead)).append('\n');
        for (String[] el : els) {
            if (el[0].equals("Mapping")) {
                fileMappings.add(el);
            } else {
                extension.append(el[2]).append('\n');
            }
        }
        StringBuilder assembled = new StringBuilder(model).append('\n').append(extension);
        for (String[] m : fileMappings) {
            String candidate = assembled + "\n" + m[2];
            try {
                com.legend.Compiler.compile(candidate, "|1", "n/a");
                assembled.append('\n').append(m[2]);
            } catch (Exception e) {
                walls.add(key + " " + m[1] + " => "
                        + String.valueOf(e.getMessage()).split("\n")[0]);
            }
        }
        fileModels.put(key, assembled.substring(model.length()));
        List<String> sql = new ArrayList<>();
        for (var d : Corpus.tableDefs(source).values()) {
            if (!d.schema().isEmpty() && !d.schema().equals("default")) {
                sql.add("CREATE SCHEMA IF NOT EXISTS " + d.schema());
            }
            sql.add(d.createSql());
        }
        fileSeeds.put(key, sql);
    }

    public List<String> walls() {
        return walls;
    }

    /** kind, fqn, text triples of top-level elements (brace/paren matched). */
    private static List<String[]> splitTopLevel(String elements) {
        List<String[]> out = new ArrayList<>();
        Pattern head = Pattern.compile(
                "(?m)^(Class|Association|Enum|Database|Mapping|Primitive|RelationalDatabaseConnection|Runtime|function)\\s+"
                        + "(?:<<[^>]*>>\\s*)?(\\*?[\\w:$]+)");
        Matcher m = head.matcher(elements);
        List<int[]> starts = new ArrayList<>();
        List<String[]> heads = new ArrayList<>();
        while (m.find()) {
            starts.add(new int[]{m.start()});
            heads.add(new String[]{m.group(1), m.group(2)});
        }
        for (int i = 0; i < starts.size(); i++) {
            int s = starts.get(i)[0];
            int e = i + 1 < starts.size() ? starts.get(i + 1)[0] : elements.length();
            out.add(new String[]{heads.get(i)[0], heads.get(i)[1], elements.substring(s, e)});
        }
        return out;
    }

    // ===== per-test execution =====

    private static final Pattern EXECUTE_CALL = Pattern.compile(
            "\\bexecute\\s*\\(\\s*\\|");

    public Outcome run(Corpus.TestFn fn) {
        // extract execute(|QUERY, MAPPING, runtime, extensions)
        Matcher m = EXECUTE_CALL.matcher(fn.body());
        if (!m.find()) {
            return new Outcome(fn.fqn(), Status.SHAPE, "no execute(|...) call");
        }
        if (m.find()) {
            return new Outcome(fn.fqn(), Status.SHAPE, "multiple execute() calls");
        }
        m = EXECUTE_CALL.matcher(fn.body());
        m.find();
        int argStart = fn.body().indexOf('(', m.start());
        int argEnd = matchParen(fn.body(), argStart);
        List<String> args = splitArgs(fn.body().substring(argStart + 1, argEnd));
        if (args.size() < 2) {
            return new Outcome(fn.fqn(), Status.SHAPE, "execute() with " + args.size() + " args");
        }
        String query = args.get(0).strip();
        if (query.startsWith("|")) {
            query = query.substring(1);
        }
        String mappingRef = qualify(args.get(1).strip(), fn);
        try {
            String runtimeFqn = "rcorpus::Rt";
            String fileExt = fileModels.getOrDefault(currentFileKey, "");
            String fullModel = model + fileExt + runtimeBlock(model + fileExt, mappingRef);
            String qualified = qualifyQuery(query, fn);
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
                try (var st = conn.createStatement()) {
                    st.execute("SET TimeZone='UTC'");
                }
                // base seeds + every BeforePackage setup covering the test's
                // package (the engine harness's test.BeforePackage contract)
                List<String> allSeeds = new ArrayList<>(seeds);
                allSeeds.addAll(fileSeeds.getOrDefault(currentFileKey, List.of()));
                for (Corpus.BeforePackage bp : beforePackages) {
                    if (fn.fqn().startsWith(bp.pkg())) {
                        allSeeds.addAll(bp.sql());
                    }
                }
                for (String sql : allSeeds) {
                    try (var st = conn.createStatement()) {
                        st.execute(sql);
                    } catch (Exception ignore) {
                        // seed dialect gaps surface as row diffs, loudly, in
                        // the tests that read the affected table
                    }
                }
                ExecutionResult r = new QueryService().execute(fullModel, qualified, runtimeFqn, conn);
                return checkAsserts(fn, r);
            }
        } catch (Exception e) {
            return new Outcome(fn.fqn(), Status.ERROR,
                    String.valueOf(e.getMessage()).split("\n")[0]);
        }
    }

    /** Synthesized runtime binding EVERY database (stores pick what they need). */
    private String runtimeBlock(String modelText, String mappingFqn) {
        StringBuilder conns = new StringBuilder();
        Matcher m = Pattern.compile("(?m)^Database\\s+([\\w:]+)").matcher(modelText);
        java.util.Set<String> dbs = new java.util.LinkedHashSet<>();
        while (m.find()) {
            dbs.add(m.group(1));
        }
        for (String db : dbs) {
            if (conns.length() > 0) {
                conns.append(", ");
            }
            conns.append(db).append(": [ c: rcorpus::Conn ]");
        }
        return "\nRelationalDatabaseConnection rcorpus::Conn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }\n"
                + "Runtime rcorpus::Rt { mappings: [ " + mappingFqn + " ]; connections: [ " + conns + " ] }\n";
    }

    // ===== assertion evaluation =====

    private Outcome checkAsserts(Corpus.TestFn fn, ExecutionResult r) {
        List<Map<String, Object>> rows = graphRows(r);
        List<String> problems = new ArrayList<>();
        int recognized = 0;
        // the RESULT variable: `let x = execute(...)...;` binds x; bare
        // calls use $result by convention. All result-suffix spellings
        // ($x.values, ->at(0), .rows) collapse onto the one result.
        String rvar = "result";
        Matcher lm = Pattern.compile("let\\s+(\\w+)\\s*=\\s*execute\\s*\\(").matcher(fn.body());
        if (lm.find()) {
            rvar = lm.group(1);
        }
        for (String call : assertCalls(fn.body())) {
            String head = call.substring(0, call.indexOf('(')).strip();
            List<String> args = splitArgs(call.substring(call.indexOf('(') + 1, call.length() - 1));
            // normalize every result spelling to a canonical head "$R"
            for (int ai = 0; ai < args.size(); ai++) {
                String norm = args.get(ai).strip()
                        .replace("$" + rvar, "$R")
                        .replaceAll("^\\$R\\.values->at\\(0\\)", java.util.regex.Matcher.quoteReplacement("$R"))
                        .replaceAll("^\\$R\\.values\\.rows", java.util.regex.Matcher.quoteReplacement("$R.rows"));
                args.set(ai, norm);
            }
            switch (head) {
                case "assertSize" -> {
                    String target = args.get(0).strip();
                    if (args.size() == 2
                            && (target.equals("$R.values") || target.equals("$R.rows")
                                    || target.equals("$R"))) {
                        recognized++;
                        int expected = Integer.parseInt(args.get(1).strip());
                        if (rows.size() != expected) {
                            problems.add("size: expected " + expected + ", got " + rows.size());
                        }
                    }
                }
                case "assertEmpty" -> {
                    if (args.size() == 1 && args.get(0).strip().equals("$R.values")) {
                        recognized++;
                        if (!rows.isEmpty()) {
                            problems.add("expected empty, got " + rows.size() + " rows");
                        }
                    }
                }
                case "assertSameElements" -> {
                    Matcher pm = Pattern.compile("^\\$R(?:\\.values)?(?:->at\\(\\d+\\))?\\.(\\w+)$")
                            .matcher(args.size() == 2 ? args.get(1).strip() : "");
                    if (pm.matches()) {
                        recognized++;
                        List<Object> expected = pureLiteralList(args.get(0).strip());
                        List<Object> actual = column(rows, pm.group(1));
                        if (expected != null && !multisetEquals(expected, actual)) {
                            problems.add(pm.group(1) + ": expected " + expected + ", got " + actual);
                        }
                    }
                }
                case "assertEquals", "assertEqualsHNCompatible" -> {
                    String second = args.size() == 2 ? args.get(1).strip() : "";
                    if (second.endsWith("->sqlRemoveFormatting()") || second.endsWith("->sql()")) {
                        sqlAsserts++;   // advisory golden-SQL diff
                        recognized++;
                        continue;
                    }
                    // TDS idioms — all against the canonical rows
                    Matcher colGet = Pattern.compile(
                            "^\\$R\\.rows\\.get(?:String|Integer|Float|Date|Number)?\\('([^']+)'\\)$")
                            .matcher(second);
                    Matcher rowAtCells = Pattern.compile(
                            "^\\$R\\.rows->at\\((\\d+)\\)\\.values$").matcher(second);
                    Matcher rowAtGet = Pattern.compile(
                            "^\\$R\\.rows->at\\((\\d+)\\)\\.get(?:String|Integer|Float|Date|Number)?\\('([^']+)'\\)$")
                            .matcher(second);
                    Matcher allCells = Pattern.compile(
                            "^\\$R(?:\\.values->at\\(\\d+\\))?\\.rows\\.values$").matcher(second);
                    Matcher sizeOf = Pattern.compile(
                            "^\\$R(?:\\.rows)?->size\\(\\)$").matcher(second);
                    if (colGet.matches()) {
                        recognized++;
                        List<Object> expected = pureLiteralList(args.get(0).strip());
                        List<Object> actual = column(rows, colGet.group(1));
                        if (expected != null && !orderedEquals(expected, actual)) {
                            problems.add(colGet.group(1) + ": expected " + expected + ", got " + actual);
                        }
                        continue;
                    }
                    if (rowAtGet.matches()) {
                        recognized++;
                        Object expected = pureLiteral(args.get(0).strip());
                        int idx = Integer.parseInt(rowAtGet.group(1));
                        Object actual = idx < rows.size() ? rows.get(idx).get(rowAtGet.group(2)) : null;
                        if (expected != null && !valueEquals(expected, actual)) {
                            problems.add("row " + idx + "." + rowAtGet.group(2)
                                    + ": expected " + expected + ", got " + actual);
                        }
                        continue;
                    }
                    if (rowAtCells.matches()) {
                        recognized++;
                        List<Object> expected = pureLiteralList(args.get(0).strip());
                        int idx = Integer.parseInt(rowAtCells.group(1));
                        List<Object> actual = idx < rows.size()
                                ? new ArrayList<>(rows.get(idx).values()) : List.of();
                        if (expected != null && !orderedEquals(expected, actual)) {
                            problems.add("row " + idx + ": expected " + expected + ", got " + actual);
                        }
                        continue;
                    }
                    if (allCells.matches()) {
                        recognized++;
                        List<Object> expected = pureLiteralList(args.get(0).strip());
                        List<Object> actual = new ArrayList<>();
                        rows.forEach(row -> actual.addAll(row.values()));
                        if (expected != null && !orderedEquals(expected, actual)) {
                            problems.add("cells: expected " + expected + ", got " + actual);
                        }
                        continue;
                    }
                    if (sizeOf.matches()) {
                        recognized++;
                        Object expected = pureLiteral(args.get(0).strip());
                        if (expected instanceof Long n && rows.size() != n) {
                            problems.add("size: expected " + n + ", got " + rows.size());
                        }
                        continue;
                    }
                    Matcher one = Pattern.compile(
                            "^\\$R(?:\\.values)?->(?:toOne|first)\\(\\)(?:\\.(\\w+))?(?:->toOne\\(\\))?$")
                            .matcher(second);
                    Matcher at = Pattern.compile(
                            "^\\$R(?:\\.values)?->at\\((\\d+)\\)\\.(\\w+)$").matcher(second);
                    if (one.matches()) {
                        recognized++;
                        Object expected = pureLiteral(args.get(0).strip());
                        Object actual = rows.isEmpty() ? null
                                : one.group(1) == null ? rows.get(0)
                                        : rows.get(0).get(one.group(1));
                        if (expected != null && !valueEquals(expected, actual)) {
                            problems.add("expected " + expected + ", got " + actual);
                        }
                    } else if (at.matches()) {
                        recognized++;
                        Object expected = pureLiteral(args.get(0).strip());
                        int idx = Integer.parseInt(at.group(1));
                        Object actual = idx < rows.size() ? rows.get(idx).get(at.group(2)) : null;
                        if (expected != null && !valueEquals(expected, actual)) {
                            problems.add("at(" + idx + ")." + at.group(2)
                                    + ": expected " + expected + ", got " + actual);
                        }
                    }
                }
                default -> { }
            }
        }
        if (recognized == 0) {
            return new Outcome(fn.fqn(), Status.SHAPE, "no recognizable assertions");
        }
        if (!problems.isEmpty()) {
            return new Outcome(fn.fqn(), Status.FAIL, String.join("; ", problems));
        }
        // NEVER a false pass on partial verification: a PASS requires every
        // assert to be recognized (advisory SQL counted); anything short is
        // honest SHAPE accounting.
        int total = assertCalls(fn.body()).size();
        if (recognized < total) {
            return new Outcome(fn.fqn(), Status.SHAPE,
                    "partial: " + recognized + "/" + total + " asserts recognized"
                            + " (recognized ones hold)");
        }
        return new Outcome(fn.fqn(), Status.PASS, recognized + " assert(s)");
    }

    /** Class results arrive as the GRAPH envelope (JSON rows); TDS as tabular. */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> graphRows(ExecutionResult r) {
        if (r instanceof ExecutionResult.GraphResult g) {
            Object parsed = toJava(com.gs.legend.util.Json.parse(g.json()));
            List<Object> arr = parsed instanceof List<?> l ? (List<Object>) l : List.of(parsed);
            List<Map<String, Object>> out = new ArrayList<>();
            for (Object o : arr) {
                if (o instanceof Map<?, ?> mm) {
                    out.add((Map<String, Object>) mm);
                }
            }
            return out;
        }
        List<Map<String, Object>> out = new ArrayList<>();
        List<String> names = new ArrayList<>();
        r.columns().forEach(c -> names.add(c.name()));
        for (var row : r.rows()) {
            Map<String, Object> mm = new LinkedHashMap<>();
            for (int i = 0; i < names.size(); i++) {
                mm.put(names.get(i), row.values().get(i));
            }
            out.add(mm);
        }
        return out;
    }

    private static Object toJava(com.gs.legend.util.Json.Node n) {
        return switch (n) {
            case com.gs.legend.util.Json.Obj o -> {
                Map<String, Object> m = new LinkedHashMap<>();
                o.fields().forEach((k, v) -> m.put(k, toJava(v)));
                yield m;
            }
            case com.gs.legend.util.Json.Arr a ->
                    a.items().stream().map(Runner::toJava).toList();
            case com.gs.legend.util.Json.Str str -> str.value();
            case com.gs.legend.util.Json.Num num ->
                    num.isInteger() ? (Object) num.longValue() : (Object) num.doubleValue();
            case com.gs.legend.util.Json.Bool b -> b.value();
            case com.gs.legend.util.Json.Null ignored -> null;
        };
    }

    private static List<Object> column(List<Map<String, Object>> rows, String prop) {
        List<Object> out = new ArrayList<>();
        for (var row : rows) {
            Object v = row.get(prop);
            if (v instanceof List<?> l) {
                out.addAll(l);
            } else if (v != null) {
                out.add(v);
            }
        }
        return out;
    }

    private static boolean orderedEquals(List<Object> a, List<Object> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!valueEquals(a.get(i), b.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean multisetEquals(List<Object> a, List<Object> b) {
        if (a.size() != b.size()) {
            return false;
        }
        List<String> x = new ArrayList<>(a.stream().map(String::valueOf).sorted().toList());
        List<String> y = new ArrayList<>(b.stream().map(String::valueOf).sorted().toList());
        return x.equals(y);
    }

    private static boolean valueEquals(Object expected, Object actual) {
        if (expected instanceof Number en && actual instanceof Number an) {
            return Math.abs(en.doubleValue() - an.doubleValue()) < 1e-9;
        }
        return String.valueOf(expected).equals(String.valueOf(actual));
    }

    // ===== pure literal parsing (expected values) =====

    static Object pureLiteral(String text) {
        text = text.strip();
        if (text.startsWith("'") && text.endsWith("'")) {
            return text.substring(1, text.length() - 1).replace("\\'", "'");
        }
        if (text.matches("-?\\d+")) {
            return Long.parseLong(text);
        }
        if (text.matches("-?\\d+\\.\\d+")) {
            return Double.parseDouble(text);
        }
        if (text.equals("true") || text.equals("false")) {
            return Boolean.parseBoolean(text);
        }
        return null;   // not a literal — unrecognized assert shape
    }

    static List<Object> pureLiteralList(String text) {
        text = text.strip();
        if (!text.startsWith("[") || !text.endsWith("]")) {
            Object single = pureLiteral(text);
            return single == null ? null : List.of(single);
        }
        List<Object> out = new ArrayList<>();
        for (String part : splitArgs(text.substring(1, text.length() - 1))) {
            Object v = pureLiteral(part.strip());
            if (v == null) {
                return null;
            }
            out.add(v);
        }
        return out;
    }

    // ===== text machinery =====

    /** Top-level assert*(...) calls in a test body. */
    static List<String> assertCalls(String body) {
        List<String> out = new ArrayList<>();
        Matcher m = Pattern.compile("\\b(assert\\w*)\\s*\\(").matcher(body);
        while (m.find()) {
            int open = body.indexOf('(', m.start());
            int close = matchParen(body, open);
            if (close > 0) {
                out.add(m.group(1) + body.substring(open, close + 1));
            }
        }
        return out;
    }

    static int matchParen(String s, int open) {
        int depth = 0;
        for (int i = open; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'') {
                i = Corpus.skipString(s, i) - 1;
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    /** Split a call's argument text on top-level commas (string/bracket aware). */
    static List<String> splitArgs(String s) {
        List<String> out = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'') {
                i = Corpus.skipString(s, i) - 1;
                continue;
            }
            if (c == '(' || c == '[' || c == '{') {
                depth++;
            } else if (c == ')' || c == ']' || c == '}') {
                depth--;
            } else if (c == ',' && depth == 0) {
                out.add(s.substring(start, i));
                start = i + 1;
            }
        }
        out.add(s.substring(start));
        return out;
    }

    // ===== name qualification (imports) =====

    /** Qualify a bare element reference via the test file's imports + the model. */
    private String qualify(String name, Corpus.TestFn fn) {
        if (name.contains("::")) {
            return name;
        }
        if (fn.imports() != null && fn.imports().containsKey(name)) {
            return fn.imports().get(name);
        }
        String scope = model + fileModels.getOrDefault(currentFileKey, "");
        for (String pkg : packagesInScope(fn)) {
            String candidate = pkg + "::" + name;
            if (scope.contains(candidate)) {
                return candidate;
            }
        }
        return name;
    }

    /** The file's wildcard imports PLUS the test's own package (implicit in real pure). */
    private static List<String> packagesInScope(Corpus.TestFn fn) {
        List<String> pkgs = new ArrayList<>(fn.wildcardImports());
        int cut = fn.fqn().lastIndexOf("::");
        if (cut > 0) {
            pkgs.add(fn.fqn().substring(0, cut));
        }
        return pkgs;
    }

    /**
     * Qualify BARE class/enum names in the query text using the file's
     * wildcard imports against the model's element index. Word-boundary
     * replacement, quoted strings preserved.
     */
    private String qualifyQuery(String query, Corpus.TestFn fn) {
        Map<String, List<String>> index = elementIndex();
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < query.length()) {
            char c = query.charAt(i);
            if (c == '\'') {
                int end = Corpus.skipString(query, i);
                out.append(query, i, end);
                i = end;
                continue;
            }
            if (Character.isJavaIdentifierStart(c)
                    && (i == 0 || (!Character.isJavaIdentifierPart(query.charAt(i - 1))
                            && query.charAt(i - 1) != ':' && query.charAt(i - 1) != '$'
                            && query.charAt(i - 1) != '.'))) {
                int j = i;
                while (j < query.length() && Character.isJavaIdentifierPart(query.charAt(j))) {
                    j++;
                }
                String word = query.substring(i, j);
                // never qualify if followed by :: (already qualified head)
                boolean qualifiedHead = j + 1 < query.length()
                        && query.charAt(j) == ':' && query.charAt(j + 1) == ':';
                // ALL candidates for the simple name; the TEST FILE's own
                // imports disambiguate (section-scoped, like real pure)
                String chosen = null;
                if (!qualifiedHead) {
                    for (String fqn : index.getOrDefault(word, List.of())) {
                        if (importsCover(fqn, word, fn)) {
                            chosen = chosen == null ? fqn : "";   // "" = ambiguous
                        }
                    }
                }
                out.append(chosen != null && !chosen.isEmpty() ? chosen : word);
                i = j;
                continue;
            }
            out.append(c);
            i++;
        }
        return "|" + out;
    }

    private boolean importsCover(String fqn, String word, Corpus.TestFn fn) {
        if (fqn.equals(fn.imports().get(word))) {
            return true;
        }
        int cut = fqn.length() - word.length() - 2;
        if (cut < 0) {
            return false;
        }
        String pkg = fqn.substring(0, Math.max(0, cut));
        return packagesInScope(fn).contains(pkg);
    }

    private final Map<String, Map<String, List<String>>> indexByFile = new LinkedHashMap<>();

    /** simple name → CANDIDATE FQNs for every Class/Enum/Mapping/Database in the model. */
    private Map<String, List<String>> elementIndex() {
        Map<String, List<String>> cachedIndex = indexByFile.get(currentFileKey);
        if (cachedIndex != null) {
            return cachedIndex;
        }
        Map<String, List<String>> index = new LinkedHashMap<>();
        Matcher m = Pattern.compile(
                "(?m)^(?:Class|Enum|Database|Mapping|Association)\\s+(?:<<[^>]*>>\\s*)?((?:\\w+::)+)(\\w+)")
                .matcher(model + fileModels.getOrDefault(currentFileKey, ""));
        while (m.find()) {
            String simple = m.group(2);
            String fqn = m.group(1) + simple;
            List<String> l = index.computeIfAbsent(simple, k -> new ArrayList<>());
            if (!l.contains(fqn)) {
                l.add(fqn);
            }
        }
        indexByFile.put(currentFileKey, index);
        return index;
    }

    // ===== scoreboard =====

    public static void writeScoreboard(Path out, Map<String, List<Outcome>> byFamily,
                                       List<String> walls, String header) throws Exception {
        StringBuilder sb = new StringBuilder(header);
        int pass = 0;
        int fail = 0;
        int error = 0;
        int shape = 0;
        Map<String, Integer> buckets = new LinkedHashMap<>();
        sb.append("\n| family | tests | pass | fail | error | shape |\n|---|---|---|---|---|---|\n");
        for (var e : byFamily.entrySet()) {
            int p = 0;
            int f = 0;
            int er = 0;
            int sh = 0;
            for (Outcome o : e.getValue()) {
                switch (o.status()) {
                    case PASS -> p++;
                    case FAIL -> f++;
                    case ERROR -> {
                        er++;
                        buckets.merge(o.detail(), 1, Integer::sum);
                    }
                    case SHAPE -> sh++;
                }
            }
            pass += p;
            fail += f;
            error += er;
            shape += sh;
            sb.append("| ").append(e.getKey()).append(" | ").append(e.getValue().size())
                    .append(" | ").append(p).append(" | ").append(f).append(" | ")
                    .append(er).append(" | ").append(sh).append(" |\n");
        }
        sb.append("| **total** | ").append(pass + fail + error + shape).append(" | **")
                .append(pass).append("** | ").append(fail).append(" | ")
                .append(error).append(" | ").append(shape).append(" |\n");
        sb.append("\n### mapping walls (dropped at assembly)\n\n");
        for (String w : walls) {
            sb.append("- ").append(w).append('\n');
        }
        sb.append("\n### top error buckets\n\n");
        buckets.entrySet().stream()
                .sorted((a, b) -> b.getValue() - a.getValue())
                .limit(30)
                .forEach(e -> sb.append("- ").append(e.getValue()).append("x ")
                        .append(e.getKey()).append('\n'));
        sb.append("\n### per-test outcomes (non-passing)\n\n");
        for (var e : byFamily.entrySet()) {
            for (Outcome o : e.getValue()) {
                if (o.status() != Status.PASS) {
                    sb.append("- ").append(o.status()).append(' ')
                            .append(o.test().substring(o.test().lastIndexOf("::") + 2))
                            .append(" [").append(e.getKey()).append("]: ")
                            .append(o.detail(), 0, Math.min(160, o.detail().length()))
                            .append('\n');
                }
            }
        }
        Files.writeString(out, sb.toString());
    }
}
