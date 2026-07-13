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
    /** Shared-file table DDL — replayed FIRST, before ANY data. */
    private final List<String> ddlSeeds;
    /** Shared-file executeInDb data literals — replayed after ALL DDL. */
    private final List<String> dataSeeds;
    /**
     * FQNs of every zero-arg function defined in the shared seed files:
     * their body literals already ride {@link #dataSeeds}, so BeforePackage
     * expansion must not run them a SECOND time. Run-once emulation at
     * FUNCTION granularity — the old statement-level dedup also destroyed
     * deliberately repeated inserts feeding distinct() tests (audit A1) and
     * silently swallowed post-REPLACE refills (audit A2).
     */
    private final java.util.Set<String> sharedSeededFns = new java.util.HashSet<>();
    /** {@code <<test.BeforePackage>>} setups collected corpus-wide. */
    private final List<Corpus.BeforePackage> beforePackages = new ArrayList<>();
    /** Advisory golden-SQL diffs: counted, never failed on. */
    public int sqlAsserts;

    /** Element keys (kind::fqn) of the SHARED base model — dedup floor. */
    private final java.util.Set<String> sharedSeen = new java.util.HashSet<>();

    public Runner(List<String> sharedSources, List<String> seedSources) {
        StringBuilder mandatory = new StringBuilder();
        List<String[]> mappings = new ArrayList<>();
        for (String src : sharedSources) {
            for (String[] el : splitSectioned(Corpus.modelElements(src))) {
                sharedSeen.add(el[0] + "::" + el[1]);
                if (el[0].equals("Mapping")) {
                    mappings.add(el);
                } else {
                    mandatory.append(el[2]).append(el[3]).append('\n');
                }
            }
        }
        StringBuilder assembled = new StringBuilder(mandatory);
        for (String[] m : mappings) {
            String candidate = assembled + "\n" + m[2] + m[3];
            try {
                com.legend.Compiler.compile(candidate, "|1", "n/a");
                assembled.append('\n').append(m[2]).append(m[3]);
            } catch (Exception e) {
                walls.add(m[1] + " => " + String.valueOf(e.getMessage()).split("\n")[0]);
            }
        }
        this.model = assembled.toString();
        List<String> ddl = new ArrayList<>();
        for (String src : sharedSources) {
            for (var d : Corpus.tableDefs(src).values()) {
                if (!d.schema().isEmpty() && !d.schema().equals("default")) {
                    ddl.add("CREATE SCHEMA IF NOT EXISTS " + d.schema());
                }
                ddl.add(d.createSql());
            }
        }
        this.ddlSeeds = ddl;
        List<String> data = new ArrayList<>();
        for (String src : seedSources) {
            data.addAll(Corpus.seedSql(src));
            sharedSeededFns.addAll(Corpus.functionBodies(src).keySet());
        }
        this.dataSeeds = data;
    }

    /** Zero-arg setup-function bodies across every scanned file. */
    private final Map<String, String> setupFnBodies = new LinkedHashMap<>();

    public void addBeforePackages(String source) {
        beforePackages.addAll(Corpus.beforePackages(source));
        Corpus.functionBodies(source).forEach(setupFnBodies::putIfAbsent);
    }

    /** Per-test-file model extensions (classes/mappings defined NEXT TO the tests). */
    private final Map<String, String> fileModels = new LinkedHashMap<>();
    private final Map<String, List<String>> fileSeeds = new LinkedHashMap<>();
    private String currentFileKey = "";
    /** Family-level extension: the family's SETUP files (no test functions). */
    private final Map<String, String> familyModels = new LinkedHashMap<>();
    private final Map<String, List<String>> familySeeds = new LinkedHashMap<>();
    private final Map<String, java.util.Set<String>> familySeen = new LinkedHashMap<>();
    private String currentFamilyKey = "";

    /**
     * Register a family's setup files (sources with NO test functions —
     * e.g. advancedRelationalSetUp.pure next to the join tests). Their
     * elements extend the shared model for EVERY test file of the family;
     * their DDL and executeInDb literals seed too.
     */
    public void useFamily(String familyKey, List<String> setupSources) {
        useFamily(familyKey, setupSources, List.of());
    }

    /**
     * {@code modelOnlySources}: sibling TEST files whose ELEMENTS join the
     * family model (cross-file references are normal — the engine compiles
     * the whole module together) but whose SEEDS stay per-file (a sibling's
     * DDL must not reshape tables under another file's test).
     */
    public void useFamily(String familyKey, List<String> setupSources,
            List<String> modelOnlySources) {
        currentFamilyKey = familyKey;
        if (familyModels.containsKey(familyKey)) {
            return;
        }
        StringBuilder ext = new StringBuilder();
        List<String> sql = new ArrayList<>();
        StringBuilder assembled = new StringBuilder(model);
        // pass 1: every family file's NON-mapping elements, deduped by
        // kind::fqn (the real engine compiles the whole module together;
        // cross-file references — embedded's BondDetail association,
        // inheritance's cross-file includes — are normal). A mapping in
        // file A may reference classes from file B, so mappings probe
        // after ALL classes/stores across the family are present.
        List<String[]> mappings = new ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>(sharedSeen);
        familySeen.put(familyKey, seen);
        for (String src : setupSources) {
            StringBuilder part = new StringBuilder();
            for (String[] el : splitSectioned(Corpus.modelElements(src))) {
                if (!seen.add(el[0] + "::" + el[1])) {
                    continue;   // first definition wins
                }
                if (el[0].equals("Mapping")) {
                    mappings.add(el);
                } else {
                    part.append(el[2]).append(el[3]).append('\n');
                }
            }
            assembled.append('\n').append(part);
            ext.append('\n').append(part);
            for (var d : Corpus.tableDefs(src).values()) {
                if (!d.schema().isEmpty() && !d.schema().equals("default")) {
                    sql.add("CREATE SCHEMA IF NOT EXISTS " + d.schema());
                }
                sql.add(d.createSql());
            }
        }
        // pass 1b: model-only sources contribute elements AND their table
        // DDL (empty CREATE OR REPLACE — a sibling's classes are queryable
        // only if their tables exist; the current file's own DDL and the
        // BeforePackage seeds run AFTER and win), but NOT data seeds
        List<String> preSql = new ArrayList<>();
        for (String src : modelOnlySources) {
            StringBuilder part = new StringBuilder();
            for (String[] el : splitSectioned(Corpus.modelElements(src))) {
                if (!seen.add(el[0] + "::" + el[1])) {
                    continue;
                }
                if (el[0].equals("Mapping")) {
                    mappings.add(el);
                } else {
                    part.append(el[2]).append(el[3]).append('\n');
                }
            }
            assembled.append('\n').append(part);
            ext.append('\n').append(part);
            for (var d : Corpus.tableDefs(src).values()) {
                if (!d.schema().isEmpty() && !d.schema().equals("default")) {
                    preSql.add("CREATE SCHEMA IF NOT EXISTS " + d.schema());
                }
                preSql.add(d.createSql());
            }
        }
        sql.addAll(0, preSql);
        // pass 2: probe-compile every family mapping against the full base
        for (String[] m : mappings) {
            String candidate = assembled + "\n" + m[2] + m[3];
            try {
                com.legend.Compiler.compile(candidate, "|1", "n/a");
                assembled.append('\n').append(m[2]).append(m[3]);
                ext.append('\n').append(m[2]).append(m[3]);
            } catch (Exception e) {
                walls.add(familyKey + " " + m[1] + " => "
                        + String.valueOf(e.getMessage()).split("\n")[0]);
            }
        }
        familyModels.put(familyKey, ext.toString());
        familySeeds.put(familyKey, sql);
    }

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
        StringBuilder extension = new StringBuilder();
        List<String[]> fileMappings = new ArrayList<>();
        java.util.Set<String> famSeen = familySeen.getOrDefault(currentFamilyKey, java.util.Set.of());
        for (String[] el : splitSectioned(Corpus.modelElements(source))) {
            if (famSeen.contains(el[0] + "::" + el[1])) {
                continue;   // already carried by the family extension
            }
            if (el[0].equals("Mapping")) {
                fileMappings.add(el);
            } else {
                extension.append(el[2]).append(el[3]).append('\n');
            }
        }
        String base = model + familyModels.getOrDefault(currentFamilyKey, "");
        StringBuilder assembled = new StringBuilder(base).append('\n').append(extension);
        for (String[] m : fileMappings) {
            String candidate = assembled + "\n" + m[2] + m[3];
            try {
                com.legend.Compiler.compile(candidate, "|1", "n/a");
                assembled.append('\n').append(m[2]).append(m[3]);
            } catch (Exception e) {
                walls.add(key + " " + m[1] + " => "
                        + String.valueOf(e.getMessage()).split("\n")[0]);
            }
        }
        fileModels.put(key, assembled.substring(base.length()));
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

    /** Distinct failed seed statements across the whole run (scoreboard-reported). */
    private final java.util.LinkedHashSet<String> seedFailures = new java.util.LinkedHashSet<>();

    public List<String> seedFailures() {
        return new ArrayList<>(seedFailures);
    }

    private static final Pattern IMPORT_LINE = Pattern.compile(
            "(?m)^import\\s+[\\w:]+(?:::\\*)?\\s*;\\s*$");

    /**
     * kind, fqn, importBlock, text quadruples of top-level elements. Each
     * element carries ITS OWN section's imports (the parser's rule: imports
     * open a section scoping the elements that follow; an import after
     * elements opens a NEW section). Assembly REORDERS elements (mappings
     * probe-append last), so every element re-emits its import block —
     * without this, a relocated mapping resolves in whatever section
     * precedes its new position and simple names silently misresolve
     * (join-family Person bound NOTHING because its import block stayed
     * behind at the original position).
     */
    private static List<String[]> splitSectioned(String elements) {
        List<String[]> raw = splitTopLevel(elements);
        List<String[]> out = new ArrayList<>(raw.size());
        // walk import lines and element heads in text order, mirroring the
        // parser's section logic
        List<int[]> importSpans = new ArrayList<>();
        Matcher im = IMPORT_LINE.matcher(elements);
        while (im.find()) {
            importSpans.add(new int[]{im.start(), im.end()});
        }
        int ii = 0;
        StringBuilder current = new StringBuilder();
        boolean sawElement = false;
        for (String[] el : raw) {
            int elStart = elements.indexOf(el[2]);
            while (ii < importSpans.size() && importSpans.get(ii)[0] < elStart) {
                if (sawElement) {
                    current.setLength(0);   // import after elements: new section
                    sawElement = false;
                }
                current.append(elements, importSpans.get(ii)[0], importSpans.get(ii)[1])
                        .append('\n');
                ii++;
            }
            sawElement = true;
            out.add(new String[]{el[0], el[1], current.toString(),
                    IMPORT_LINE.matcher(el[2]).replaceAll("")});
        }
        return out;
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
            "\\bexecute\\s*\\(");

    /** Is offset {@code at} inside a single-quoted string literal of {@code s}? */
    private static boolean insideString(String s, int at) {
        int i = 0;
        while (i < s.length() && i <= at) {
            if (s.charAt(i) == '\'') {
                int end = Corpus.skipString(s, i);
                if (at > i && at < end) {
                    return true;
                }
                i = end;
                continue;
            }
            i++;
        }
        return false;
    }

    /** Index of the LAST top-level {@code ->from(} in the expression, or -1. */
    private static int lastTopLevelFrom(String q) {
        int depth = 0;
        int last = -1;
        int i = 0;
        while (i < q.length()) {
            char c = q.charAt(i);
            if (c == '\'') {
                i = Corpus.skipString(q, i);
                continue;
            }
            if (c == '(' || c == '[' || c == '{') {
                depth++;
            } else if (c == ')' || c == ']' || c == '}') {
                depth--;
            } else if (depth == 0 && c == '-' && q.startsWith("->", i)) {
                int j = i + 2;
                while (j < q.length() && Character.isWhitespace(q.charAt(j))) {
                    j++;
                }
                if (q.startsWith("from", j)) {
                    int k = j + 4;
                    while (k < q.length() && Character.isWhitespace(q.charAt(k))) {
                        k++;
                    }
                    if (k < q.length() && q.charAt(k) == '(') {
                        last = i;
                    }
                }
            }
            i++;
        }
        return last;
    }

    /** One executed query: the let-var it binds, its rows, its body offset. */
    record ResultBinding(String var, List<Map<String, Object>> rows, int pos,
            String query, List<String> colTypes) {
    }

    public Outcome run(Corpus.TestFn fn) {
        // extract EVERY execute(|QUERY, MAPPING, runtime, extensions) —
        // multi-query tests bind each result to its own let-var, and each
        // assert evaluates against the var it references
        List<int[]> spans = new ArrayList<>();
        Matcher m = EXECUTE_CALL.matcher(fn.body());
        while (m.find()) {
            if (insideString(fn.body(), m.start())) {
                continue;   // 'execute(' inside a string literal is data
            }
            int argStart = fn.body().indexOf('(', m.start());
            int argEnd = matchParen(fn.body(), argStart);
            if (argEnd > 0) {
                spans.add(new int[]{m.start(), argStart, argEnd});
            }
        }
        if (spans.isEmpty()) {
            return new Outcome(fn.fqn(), Status.SHAPE, "no execute(|...) call");
        }
        try {
            String runtimeFqn = "rcorpus::Rt";
            String familyExt = familyModels.getOrDefault(currentFamilyKey, "");
            String fileExt = fileModels.getOrDefault(currentFileKey, "");
            String modelText = model + familyExt + fileExt;
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
                try (var st = conn.createStatement()) {
                    st.execute("SET TimeZone='UTC'");
                }
                // ALL DDL first (shared, family, file — a sibling's CREATE
                // OR REPLACE must never wipe already-inserted base rows,
                // audit A2), then data: base literals + every BeforePackage
                // setup covering the test's package (the engine harness's
                // test.BeforePackage contract). Run-once is enforced at
                // FUNCTION granularity via the shared `expanded` set — never
                // by statement-text dedup, which destroyed deliberately
                // repeated inserts (audit A1).
                List<String> allSeeds = new ArrayList<>(ddlSeeds);
                allSeeds.addAll(familySeeds.getOrDefault(currentFamilyKey, List.of()));
                allSeeds.addAll(fileSeeds.getOrDefault(currentFileKey, List.of()));
                allSeeds.addAll(dataSeeds);
                java.util.Set<String> expanded = new java.util.HashSet<>(sharedSeededFns);
                for (Corpus.BeforePackage bp : beforePackages) {
                    if (fn.fqn().startsWith(bp.pkg() + "::")) {
                        // expand against the WHOLE-RUN function corpus —
                        // setup helpers live in sibling files. A bp whose
                        // defining file already seeded raw contributes only
                        // its CALL closure, not its own literals again.
                        boolean includeBody = expanded.add(bp.fqn());
                        allSeeds.addAll(Corpus.expandSeeds(bp.body(), bp.pkg(),
                                setupFnBodies, expanded, includeBody));
                    }
                }
                List<String> failedSeeds = new ArrayList<>();
                for (String sql : allSeeds) {
                    // ONE STATEMENT PER EXECUTE: a multi-statement batch
                    // that fails mid-way reports DuckDB's useless "pending
                    // query result" error (the real cause is swallowed) and
                    // silently drops every statement after the failing one
                    for (String stmt : splitStatements(sql)) {
                        try (var st = conn.createStatement()) {
                            st.execute(stmt);
                        } catch (Exception e) {
                            // a failed seed leaves a table EMPTY, not wrong —
                            // an empty-expectation assert over it would
                            // false-pass. Track and report; checkAsserts
                            // refuses to verify emptiness under failed seeds.
                            String head = stmt.strip().split("\n")[0];
                            failedSeeds.add(head + " => "
                                    + String.valueOf(e.getMessage()).split("\n")[0]);
                        }
                    }
                }
                seedFailures.addAll(failedSeeds);
                List<ResultBinding> bindings = new ArrayList<>();
                String skippedSpan = null;
                for (int[] span : spans) {
                    List<String> args = splitArgs(fn.body().substring(span[1] + 1, span[2]));
                    if (args.size() < 2) {
                        skippedSpan = "execute() with " + args.size() + " args";
                        continue;
                    }
                    // query spellings: |q  {|q}  $letBoundLambda — inline
                    // lets FIRST so the let-bound form resolves, then strip
                    // the wrappers down to the bare expression
                    String query = inlineLets(args.get(0).strip(), fn.body()).strip();
                    if (query.startsWith("(") && query.endsWith(")")
                            && matchParen(query, 0) == query.length() - 1) {
                        query = query.substring(1, query.length() - 1).strip();
                    }
                    if (query.startsWith("{") && query.endsWith("}")) {
                        query = query.substring(1, query.length() - 1).strip();
                    }
                    if (!query.startsWith("|")) {
                        skippedSpan = "execute() query arg is not a lambda: "
                                + query.substring(0, Math.min(40, query.length()));
                        continue;
                    }
                    query = query.substring(1).strip();
                    // a TERMINAL ->from(MAPPING[, runtime]) carries the
                    // mapping IN the query (the ^Mapping(name='') outer arg
                    // is a dummy); extract it and drop the from — the
                    // driver-supplied runtime plays that role here
                    String mappingRef = null;
                    int fi = lastTopLevelFrom(query);
                    if (fi >= 0) {
                        int open = query.indexOf('(', fi);
                        int close = matchParen(query, open);
                        if (close == query.length() - 1) {
                            List<String> fromArgs = splitArgs(
                                    query.substring(open + 1, close));
                            if (!fromArgs.isEmpty()) {
                                mappingRef = qualify(fromArgs.get(0).strip(), fn);
                                query = query.substring(0, fi);
                            }
                        }
                    }
                    if (mappingRef == null) {
                        mappingRef = qualify(args.get(1).strip(), fn);
                    }
                    String fullModel = modelText + runtimeBlock(modelText, mappingRef);
                    String qualified = qualifyQuery(query, fn);
                    // the let-var this result binds ($result by convention)
                    Matcher lm = Pattern.compile("let\\s+(\\w+)\\s*=\\s*$")
                            .matcher(fn.body().substring(
                                    Math.max(0, span[0] - 40), span[0]));
                    String var = lm.find() ? lm.group(1) : "result";
                    ExecutionResult r = new QueryService().execute(
                            fullModel, qualified, runtimeFqn, conn);
                    List<String> colTypes;
                    try {
                        colTypes = r.columns().stream()
                                .map(c -> String.valueOf(c.javaType())).toList();
                    } catch (Exception e) {
                        colTypes = List.of();   // graph results carry no schema
                    }
                    bindings.add(new ResultBinding(var, graphRows(r), span[2],
                            qualified, colTypes));
                }
                if (bindings.isEmpty()) {
                    return new Outcome(fn.fqn(), Status.SHAPE,
                            skippedSpan == null ? "no execute(|...) call" : skippedSpan);
                }
                // intermediate aliases: let tds = $result.values->at(0);
                // asserts against $tds read the SAME result (the ->at(0)
                // head-normalization already equates the spellings)
                Matcher am = Pattern.compile(
                        "let\\s+(\\w+)\\s*=\\s*\\$(\\w+)((?:\\.values)?(?:->at\\(0\\))?)\\s*;")
                        .matcher(fn.body());
                while (am.find()) {
                    String alias = am.group(1);
                    String src = am.group(2);
                    for (ResultBinding b : List.copyOf(bindings)) {
                        if (b.var().equals(src) && b.pos() <= am.start()) {
                            bindings.add(new ResultBinding(alias, b.rows(),
                                    am.start(), b.query(), b.colTypes()));
                        }
                    }
                }
                return checkAsserts(fn, bindings, failedSeeds);
            }
        } catch (Exception e) {
            // flatten, don't truncate — poison reasons ride on later lines
            return new Outcome(fn.fqn(), Status.ERROR,
                    String.valueOf(e.getMessage()).replace("\n", " | "));
        }
    }

    /** Split a SQL blob into single statements on top-level {@code ;} (string-aware). */
    static List<String> splitStatements(String sql) {
        List<String> out = new ArrayList<>();
        int start = 0;
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (c == '\'') {
                i = Corpus.skipString(sql, i);
                continue;
            }
            if (c == ';') {
                String stmt = sql.substring(start, i).strip();
                if (!stmt.isEmpty()) {
                    out.add(stmt);
                }
                start = i + 1;
            }
            i++;
        }
        String tail = sql.substring(start).strip();
        if (!tail.isEmpty()) {
            out.add(tail);
        }
        return out;
    }

    private static final Pattern LET_BINDING = Pattern.compile("let\\s+(\\w+)\\s*=\\s*");

    /**
     * Inline the test body's simple {@code let} bindings into the query
     * text: {@code let type = 'CUSIP'; execute(|...$type...)} — the query
     * lambda references body-level constants the extraction would otherwise
     * drop (unbound {@code $type}). Execute-bound lets never inline; atomic
     * literals inline bare, other expressions parenthesize.
     */
    static String inlineLets(String query, String body) {
        Map<String, String> vals = new LinkedHashMap<>();
        Matcher m = LET_BINDING.matcher(body);
        while (m.find()) {
            int start = m.end();
            int i = start;
            int depth = 0;
            while (i < body.length()) {
                char c = body.charAt(i);
                if (c == '\'') {
                    i = Corpus.skipString(body, i);
                    continue;
                }
                if (c == '(' || c == '[' || c == '{') {
                    depth++;
                } else if (c == ')' || c == ']' || c == '}') {
                    depth--;
                } else if (c == ';' && depth == 0) {
                    break;
                }
                i++;
            }
            String rhs = body.substring(start, Math.min(i, body.length())).strip();
            if (rhs.contains("execute")) {
                continue;
            }
            // inline earlier lets into later RHS text
            for (Map.Entry<String, String> e : vals.entrySet()) {
                rhs = replaceVarOutsideStrings(rhs, e.getKey(), e.getValue());
            }
            // a NEGATIVE literal is not atomic: bare it flips arrow
            // precedence ($v->abs() with v=-5 becoming -5->abs(), audit B4)
            boolean atomic = rhs.matches("'[^']*'") || rhs.matches("\\d+(\\.\\d+)?")
                    || rhs.matches("[\\w:.\\[\\]%]+") || rhs.matches("\\[[^;]*\\]");
            vals.put(m.group(1), atomic ? rhs : "(" + rhs + ")");
        }
        String out = query;
        for (Map.Entry<String, String> e : vals.entrySet()) {
            out = replaceVarOutsideStrings(out, e.getKey(), e.getValue());
        }
        return out;
    }

    /**
     * {@code $var} → replacement, skipping single-quoted string literals —
     * pure does not interpolate, so a '$type' INSIDE a literal is data
     * (audit B4).
     */
    private static String replaceVarOutsideStrings(String text, String var,
            String replacement) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < text.length()) {
            char c = text.charAt(i);
            if (c == '\'') {
                int end = Corpus.skipString(text, i);
                out.append(text, i, end);
                i = end;
                continue;
            }
            if (c == '$' && text.startsWith(var, i + 1)
                    && (i + 1 + var.length() >= text.length()
                            || !Character.isJavaIdentifierPart(
                                    text.charAt(i + 1 + var.length())))) {
                out.append(replacement);
                i += 1 + var.length();
                continue;
            }
            out.append(c);
            i++;
        }
        return out.toString();
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

    private Outcome checkAsserts(Corpus.TestFn fn, List<ResultBinding> bindings,
            List<String> failedSeeds) {
        List<String> problems = new ArrayList<>();
        // recognized = asserts whose SHAPE matched AND whose expected value
        // PARSED (an unparseable expected value is NOT recognized — counting
        // it would score an assert that never ran its comparison).
        // verified = recognized minus advisory golden-SQL asserts.
        int recognized = 0;
        int verified = 0;
        String firstUnrecognized = null;
        int recognizedBefore;
        for (int[] callPos : assertCallSpans(fn.body())) {
            recognizedBefore = recognized;
            String call = fn.body().substring(callPos[0], callPos[1]);
            String head = call.substring(0, call.indexOf('(')).strip();
            List<String> args = splitArgs(call.substring(call.indexOf('(') + 1, call.length() - 1));
            // the RESULT this assert reads: the LATEST prior execute whose
            // let-var the call references ($result by convention)
            ResultBinding bound = null;
            for (int bi = bindings.size() - 1; bi >= 0; bi--) {
                ResultBinding b = bindings.get(bi);
                if (b.pos() > callPos[0]) {
                    continue;
                }
                if (Pattern.compile("\\$" + Pattern.quote(b.var()) + "\\b")
                        .matcher(call).find()) {
                    bound = b;
                    break;
                }
            }
            if (bound == null) {
                continue;   // references no known result — stays unrecognized
            }
            List<Map<String, Object>> rows = bound.rows();
            // normalize every result spelling to a canonical head "$R"
            for (int ai = 0; ai < args.size(); ai++) {
                String norm = args.get(ai).strip()
                        .replace("$" + bound.var(), "$R")
                        .replaceAll("^\\$R\\.values->at\\(0\\)", java.util.regex.Matcher.quoteReplacement("$R"))
                        .replaceAll("^\\$R\\.values\\.rows", java.util.regex.Matcher.quoteReplacement("$R.rows"));
                args.set(ai, norm);
            }
            switch (head) {
                case "assertSize" -> {
                    String target = args.get(0).strip();
                    if (args.size() == 2 && args.get(1).strip().matches("\\d+")
                            && target.matches("\\$R(\\.values)?\\.columns")) {
                        recognized++;
                        verified++;
                        int expected = Integer.parseInt(args.get(1).strip());
                        int actual = rows.isEmpty() ? -1 : rows.get(0).size();
                        if (actual != expected) {
                            problems.add("columns: expected " + expected + ", got "
                                    + (actual < 0 ? "(no rows)" : actual));
                        }
                        continue;
                    }
                    if (args.size() == 2 && args.get(1).strip().matches("\\d+")
                            && (target.equals("$R.values") || target.equals("$R.rows")
                                    || target.equals("$R"))) {
                        int expected = Integer.parseInt(args.get(1).strip());
                        if (expected == 0 && !failedSeeds.isEmpty()) {
                            // expected-zero over a table a failed seed left
                            // empty proves nothing (same rule as assertEmpty)
                            return new Outcome(fn.fqn(), Status.SHAPE,
                                    "assertSize 0 unverifiable: " + failedSeeds.size()
                                            + " seed statement(s) failed: " + failedSeeds.get(0));
                        }
                        recognized++;
                        verified++;
                        if (rows.size() != expected) {
                            problems.add("size: expected " + expected + ", got " + rows.size());
                        }
                    }
                }
                case "assertEmpty" -> {
                    if (args.size() == 1 && args.get(0).strip().equals("$R.values")) {
                        if (!failedSeeds.isEmpty()) {
                            // an empty table proves nothing when a seed failed
                            return new Outcome(fn.fqn(), Status.SHAPE,
                                    "assertEmpty unverifiable: " + failedSeeds.size()
                                            + " seed statement(s) failed: " + failedSeeds.get(0));
                        }
                        recognized++;
                        verified++;
                        if (!rows.isEmpty()) {
                            problems.add("expected empty, got " + rows.size() + " rows");
                        }
                    }
                }
                case "assertSameSQL" -> {
                    if (args.size() == 2 && args.get(1).strip().equals("$R")) {
                        sqlAsserts++;   // advisory golden-SQL: recognized, NOT verified
                        recognized++;
                    }
                }
                case "assertSameElements" -> {
                    if (args.size() == 2 && args.get(0).strip().startsWith("$R")
                            && !args.get(1).strip().startsWith("$R")) {
                        args = List.of(args.get(1), args.get(0));   // actual-first spelling
                    }
                    Matcher getMap = Pattern.compile(
                            "^\\$R(?:\\.values(?:->at\\(\\d+\\))?)?(?:\\.rows)?->map\\(\\s*\\w+\\s*\\|\\s*\\$\\w+\\.get\\w*\\('([^']+)'\\)\\s*\\)$")
                            .matcher(args.size() == 2 ? args.get(1).strip() : "");
                    if (getMap.matches()) {
                        List<Object> expected = pureLiteralList(args.get(0).strip());
                        if (expected != null) {
                            recognized++;
                            verified++;
                            List<Object> actual = column(rows, getMap.group(1));
                            if (!multisetEquals(expected, actual)) {
                                problems.add(getMap.group(1) + ": expected " + expected
                                        + ", got " + actual);
                            }
                        }
                        continue;
                    }
                    Matcher cellAt = Pattern.compile(
                            "^\\$R(?:\\.values->at\\(\\d+\\))?\\.rows->map\\(\\s*\\w+\\s*\\|\\s*\\$\\w+\\.values->at\\((\\d+)\\)\\s*\\)$")
                            .matcher(args.size() == 2 ? args.get(1).strip() : "");
                    if (cellAt.matches()) {
                        List<Object> expected = pureLiteralList(args.get(0).strip());
                        if (expected != null) {
                            recognized++;
                            verified++;
                            int ci = Integer.parseInt(cellAt.group(1));
                            List<Object> actual = new ArrayList<>();
                            for (var row : rows) {
                                List<Object> vals = new ArrayList<>(row.values());
                                actual.add(ci < vals.size() ? vals.get(ci) : null);
                            }
                            if (!multisetEquals(expected, actual)) {
                                problems.add("col[" + ci + "]: expected " + expected
                                        + ", got " + actual);
                            }
                        }
                        continue;
                    }
                    Matcher allCellsSame = Pattern.compile(
                            "^\\$R(?:\\.values(?:->at\\(\\d+\\))?)?\\.rows\\.values$")
                            .matcher(args.size() == 2 ? args.get(1).strip() : "");
                    if (allCellsSame.matches()) {
                        List<Object> expected = pureLiteralList(args.get(0).strip());
                        if (expected != null) {
                            recognized++;
                            verified++;
                            List<Object> actual = new ArrayList<>();
                            rows.forEach(row -> actual.addAll(row.values()));
                            if (!multisetEquals(expected, actual)) {
                                problems.add("cells: expected " + expected + ", got " + actual);
                            }
                        }
                        continue;
                    }
                    Matcher cellsMap = Pattern.compile(
                            "^\\$R(?:\\.values->at\\(\\d+\\))?\\.rows->map\\(\\s*\\w+\\s*\\|\\s*\\$\\w+\\.values\\s*\\)$")
                            .matcher(args.size() == 2 ? args.get(1).strip() : "");
                    if (cellsMap.matches()) {
                        List<Object> expected = pureLiteralList(args.get(0).strip());
                        if (expected != null) {
                            recognized++;
                            verified++;
                            List<Object> actual = new ArrayList<>();
                            rows.forEach(row -> actual.addAll(row.values()));
                            if (!multisetEquals(expected, actual)) {
                                problems.add("cells: expected " + expected + ", got " + actual);
                            }
                        }
                        continue;
                    }
                    if (args.size() == 2 && args.get(1).strip().matches("\\$R(\\.values)?")
                            && !rows.isEmpty() && rows.get(0).size() == 1) {
                        List<Object> expected = pureLiteralList(args.get(0).strip());
                        if (expected != null) {
                            recognized++;
                            verified++;
                            String col = rows.get(0).keySet().iterator().next();
                            List<Object> actual = column(rows, col);
                            if (!multisetEquals(expected, actual)) {
                                problems.add("values: expected " + expected
                                        + ", got " + actual);
                            }
                        }
                        continue;
                    }
                    Matcher pm = Pattern.compile(
                            "^\\$R(?:\\.values)?(?:->at\\((\\d+)\\))?"
                                    + "(?:\\.(\\w+)|->map\\(\\s*\\w+\\s*\\|\\s*\\$\\w+\\.(\\w+)\\s*\\))$")
                            .matcher(args.size() == 2 ? args.get(1).strip() : "");
                    if (pm.matches()) {
                        List<Object> expected = pureLiteralList(args.get(0).strip());
                        if (expected != null) {
                            recognized++;
                            verified++;
                            String prop = pm.group(2) != null ? pm.group(2) : pm.group(3);
                            List<Object> actual = pm.group(1) == null
                                    ? column(rows, prop)
                                    : Integer.parseInt(pm.group(1)) < rows.size()
                                            ? column(rows.subList(Integer.parseInt(pm.group(1)),
                                                    Integer.parseInt(pm.group(1)) + 1), prop)
                                            : List.of();
                            if (!multisetEquals(expected, actual)) {
                                problems.add(prop + ": expected " + expected + ", got " + actual);
                            }
                        }
                    }
                }
                case "assertEquals", "assertEqualsH2Compatible" -> {
                    if (args.size() == 2 && args.get(0).strip().startsWith("$R")
                            && !args.get(1).strip().startsWith("$R")) {
                        args = List.of(args.get(1), args.get(0));   // actual-first spelling
                    }
                    // multi-line arrow chains: collapse whitespace and
                    // tighten '->' so the shape matchers see one line —
                    // OUTSIDE string literals only (a makeString separator's
                    // spaces are data, audit B2)
                    String second = args.size() == 2
                            ? collapseOutsideStrings(args.get(1).strip())
                            : "";
                    // 3-arg H2-compat: (legacySql, h2Sql, $r->sqlRemoveFormatting())
                    if (args.size() == 3
                            && args.get(2).strip().replaceAll("\\s+", "")
                                    .endsWith("->sqlRemoveFormatting()")) {
                        sqlAsserts++;
                        recognized++;
                        continue;
                    }
                    if (second.endsWith("->sqlRemoveFormatting()") || second.endsWith("->sql()")) {
                        sqlAsserts++;   // advisory golden-SQL: recognized, NOT verified
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
                            "^\\$R(?:\\.values->at\\(\\d+\\))?\\.rows(?:\\.values"
                                    + "|->map\\(\\s*\\w+\\s*\\|\\s*\\$\\w+\\.values\\s*\\))$")
                            .matcher(second);
                    Matcher sizeOf = Pattern.compile(
                            "^\\$R(?:\\.values|\\.rows)?(?:\\.rows)?->size\\(\\)$").matcher(second);
                    if (colGet.matches()) {
                        List<Object> expected = pureLiteralList(args.get(0).strip());
                        if (expected != null) {
                            recognized++;
                            verified++;
                            List<Object> actual = column(rows, colGet.group(1));
                            if (!orderedEquals(expected, actual)) {
                                problems.add(colGet.group(1) + ": expected " + expected + ", got " + actual);
                            }
                        }
                        continue;
                    }
                    if (rowAtGet.matches()) {
                        Object expected = pureLiteral(args.get(0).strip());
                        if (expected != null) {
                            recognized++;
                            verified++;
                            int idx = Integer.parseInt(rowAtGet.group(1));
                            Object actual = idx < rows.size() ? rows.get(idx).get(rowAtGet.group(2)) : null;
                            if (!valueEquals(expected, actual)
                                    && !(!sortedQuery(bound.query())
                                            && column(rows, rowAtGet.group(2)).stream()
                                                    .anyMatch(v -> valueEquals(expected, v)))) {
                                problems.add("row " + idx + "." + rowAtGet.group(2)
                                        + ": expected " + expected + ", got " + actual);
                            }
                        }
                        continue;
                    }
                    if (rowAtCells.matches()) {
                        List<Object> expected = pureLiteralList(args.get(0).strip());
                        if (expected != null) {
                            recognized++;
                            verified++;
                            int idx = Integer.parseInt(rowAtCells.group(1));
                            List<Object> actual = idx < rows.size()
                                    ? new ArrayList<>(rows.get(idx).values()) : List.of();
                            // rows->at(N) under an UNSORTED query has no
                            // positional contract (engine order is H2's
                            // incident): membership suffices
                            if (!orderedEquals(expected, actual)
                                    && !(!sortedQuery(bound.query())
                                            && anyRowMatches(rows, expected))) {
                                problems.add("row " + idx + ": expected " + expected + ", got " + actual);
                            }
                        }
                        continue;
                    }
                    if (allCells.matches()) {
                        List<Object> expected = pureLiteralList(args.get(0).strip());
                        if (expected != null) {
                            recognized++;
                            verified++;
                            List<Object> actual = new ArrayList<>();
                            rows.forEach(row -> actual.addAll(row.values()));
                            if (!orderedEquals(expected, actual)) {
                                problems.add("cells: expected " + expected + ", got " + actual);
                            }
                        }
                        continue;
                    }
                    if (sizeOf.matches()) {
                        Object expected = pureLiteral(args.get(0).strip());
                        if (expected instanceof Long n) {
                            recognized++;
                            verified++;
                            if (rows.size() != n) {
                                problems.add("size: expected " + n + ", got " + rows.size());
                            }
                        }
                        continue;
                    }
                    Matcher toCsv = Pattern.compile(
                            "^\\$R(?:\\.values)?(?:->toOne\\(\\))?->toCSV\\(\\)$")
                            .matcher(second);
                    if (toCsv.matches()) {
                        Object expected = pureLiteral(args.get(0).strip());
                        if (expected instanceof String es) {
                            recognized++;
                            verified++;
                            String actual = toCsv(rows);
                            if (!csvEquals(es, actual, bound.query())) {
                                problems.add("toCSV: expected <" + es + ">, got <" + actual + ">");
                            }
                        }
                        continue;
                    }
                    Matcher bare = Pattern.compile(
                            "^\\$R(?:\\.values)?(?:->toOne\\(\\))?$").matcher(second);
                    // #TDS ... # literal expectation (optionally
                    // ->toString() on both sides, optionally an explicit
                    // ->sort(~col->ascending()) on the actual side):
                    // header + full row grid
                    String exp0 = args.get(0).strip();
                    if (exp0.endsWith("->toString()")) {
                        exp0 = exp0.substring(0, exp0.length() - "->toString()".length()).strip();
                    }
                    Matcher tdsAct = Pattern.compile(
                            "^\\$R(?:\\.values)?(?:->toOne\\(\\))?"
                                    + "(?:->sort\\(~(\\w+)->(ascending|descending)\\(\\)\\))?"
                                    + "(?:->toString\\(\\))?$")
                            .matcher(second);
                    if (tdsAct.matches()
                            && pureLiteral(exp0) instanceof TdsExpected te) {
                        recognized++;
                        verified++;
                        List<String> actualCols = rows.isEmpty()
                                ? List.of() : new ArrayList<>(rows.get(0).keySet());
                        List<Map<String, Object>> cmpRows = rows;
                        if (tdsAct.group(1) != null && !rows.isEmpty()) {
                            cmpRows = new ArrayList<>(rows);
                            String key = tdsAct.group(1);
                            boolean desc = "descending".equals(tdsAct.group(2));
                            cmpRows.sort((r1, r2) -> {
                                int c = compareCells(r1.get(key), r2.get(key));
                                return desc ? -c : c;
                            });
                        }
                        if (!rows.isEmpty() && !te.cols().equals(actualCols)) {
                            problems.add("TDS columns: expected " + te.cols()
                                    + ", got " + actualCols);
                        } else if (!tdsRowsEqual(te.rows(), cmpRows,
                                tdsAct.group(1) != null ? "->sort(" : bound.query())) {
                            problems.add("TDS rows: expected " + te.rows() + ", got "
                                    + cmpRows.stream().map(r2 -> new ArrayList<>(r2.values())).toList());
                        }
                        continue;
                    }
                    if (bare.matches() && rows.size() == 1 && rows.get(0).size() == 1
                            && rows.get(0).containsKey("__scalar__")) {
                        Object expected = pureLiteral(args.get(0).strip());
                        if (expected != null) {
                            recognized++;
                            verified++;
                            Object actual = rows.get(0).get("__scalar__");
                            if (!valueEquals(expected, actual)) {
                                problems.add("expected " + expected + ", got " + actual);
                            }
                        }
                        continue;
                    }
                    // a VALUE-COLLECTION result (map over instances lowers to
                    // a single-column relation): bare $R.values against a list
                    if (bare.matches() && !rows.isEmpty() && rows.get(0).size() == 1) {
                        List<Object> expected = pureLiteralList(args.get(0).strip());
                        if (expected != null) {
                            recognized++;
                            verified++;
                            String col = rows.get(0).keySet().iterator().next();
                            List<Object> actual = column(rows, col);
                            if (!orderedEquals(expected, actual)) {
                                problems.add("values: expected " + expected
                                        + ", got " + actual);
                            }
                        }
                        continue;
                    }
                    Matcher mapJoin = Pattern.compile(
                            "^\\$R(?:\\.values)?\\.rows->map\\(\\s*\\w+\\s*\\|\\s*\\$\\w+"
                                    + "\\.get\\w*\\('([^']+)'\\)\\s*\\)(->sort\\(\\))?"
                                    + "->makeString\\('([^']*)'\\)$")
                            .matcher(second);
                    if (mapJoin.matches()) {
                        Object expected = pureLiteral(args.get(0).strip());
                        if (expected instanceof String es) {
                            recognized++;
                            verified++;
                            List<Object> vals = column(rows, mapJoin.group(1));
                            List<String> strs = new ArrayList<>(vals.stream()
                                    .map(String::valueOf).toList());
                            if (mapJoin.group(2) != null) {
                                java.util.Collections.sort(strs);
                            }
                            String actual = String.join(mapJoin.group(3), strs);
                            if (!joinedEquals(es, actual, mapJoin.group(3),
                                    mapJoin.group(2) != null, bound.query())) {
                                problems.add(mapJoin.group(1) + ": expected <" + es
                                        + ">, got <" + actual + ">");
                            }
                        }
                        continue;
                    }
                    Matcher propJoin = Pattern.compile(
                            "^\\$R(?:\\.values)?->map\\(\\s*\\w+\\s*\\|\\s*\\$\\w+\\.(\\w+)\\s*\\)"
                                    + "(->sort\\(\\))?->makeString\\((?:'([^']*)')?\\)$")
                            .matcher(second);
                    if (propJoin.matches()) {
                        Object expRaw = pureLiteral(args.get(0).strip());
                        List<Object> expList = expRaw == null
                                ? pureLiteralList(args.get(0).strip()) : null;
                        Object expected = expRaw != null ? expRaw
                                : expList != null && expList.size() == 1 ? expList.get(0) : null;
                        if (expected instanceof String es) {
                            recognized++;
                            verified++;
                            List<String> strs = new ArrayList<>(
                                    column(rows, propJoin.group(1)).stream()
                                            .map(String::valueOf).toList());
                            if (propJoin.group(2) != null) {
                                java.util.Collections.sort(strs);
                            }
                            String actual = String.join(
                                    propJoin.group(3) == null ? "" : propJoin.group(3), strs);
                            if (!joinedEquals(es, actual,
                                    propJoin.group(3) == null ? "" : propJoin.group(3),
                                    propJoin.group(2) != null, bound.query())) {
                                problems.add(propJoin.group(1) + ": expected <" + es
                                        + ">, got <" + actual + ">");
                            }
                        }
                        continue;
                    }
                    Matcher matrixJoin = Pattern.compile(
                            "^\\$R(?:\\.values(?:->at\\(\\d+\\))?)?\\.rows->map\\(\\s*\\w+\\s*\\|\\s*\\$\\w+\\.values->makeString\\('([^']*)'\\)\\s*\\)"
                                    + "(->sort\\(\\))?->makeString\\('([^']*)'\\)$")
                            .matcher(second);
                    if (matrixJoin.matches()) {
                        Object expected = pureLiteral(args.get(0).strip());
                        if (expected instanceof String es) {
                            recognized++;
                            verified++;
                            List<String> rowStrs = new ArrayList<>();
                            for (var row : rows) {
                                rowStrs.add(row.values().stream()
                                        .map(String::valueOf)
                                        .collect(java.util.stream.Collectors
                                                .joining(matrixJoin.group(1))));
                            }
                            if (matrixJoin.group(2) != null) {
                                java.util.Collections.sort(rowStrs);
                            }
                            String actual = String.join(matrixJoin.group(3), rowStrs);
                            if (!joinedEquals(es, actual, matrixJoin.group(3),
                                    matrixJoin.group(2) != null, bound.query())) {
                                problems.add("rows: expected <" + es + ">, got <"
                                        + actual + ">");
                            }
                        }
                        continue;
                    }
                    Matcher cellsSortJoin = Pattern.compile(
                            "^\\$R(?:\\.values(?:->at\\(\\d+\\))?)?\\.rows\\.values"
                                    + "(->sort\\(\\))?->makeString\\('([^']*)'\\)$")
                            .matcher(second);
                    if (cellsSortJoin.matches()) {
                        Object expected = pureLiteral(args.get(0).strip());
                        if (expected instanceof String es) {
                            recognized++;
                            verified++;
                            List<String> strs = new ArrayList<>();
                            rows.forEach(row -> row.values().forEach(v ->
                                    strs.add(String.valueOf(v))));
                            if (cellsSortJoin.group(1) != null) {
                                java.util.Collections.sort(strs);
                            }
                            String actual = String.join(cellsSortJoin.group(2), strs);
                            if (!joinedEquals(es, actual, cellsSortJoin.group(2),
                                    cellsSortJoin.group(1) != null, bound.query())) {
                                problems.add("cells: expected <" + es + ">, got <"
                                        + actual + ">");
                            }
                        }
                        continue;
                    }
                    Matcher cellsJoin = Pattern.compile(
                            "^\\$R(?:\\.values(?:->at\\(\\d+\\))?)?\\.rows->map\\(\\s*\\w+\\s*\\|\\s*\\$\\w+\\.values\\s*\\)"
                                    + "(->sort\\(\\))?->makeString\\('([^']*)'\\)$")
                            .matcher(second);
                    if (cellsJoin.matches()) {
                        Object expected = pureLiteral(args.get(0).strip());
                        if (expected instanceof String es) {
                            recognized++;
                            verified++;
                            List<String> strs = new ArrayList<>();
                            rows.forEach(row -> row.values().forEach(v ->
                                    strs.add(String.valueOf(v))));
                            if (cellsJoin.group(1) != null) {
                                java.util.Collections.sort(strs);
                            }
                            String actual = String.join(cellsJoin.group(2), strs);
                            if (!joinedEquals(es, actual, cellsJoin.group(2),
                                    cellsJoin.group(1) != null, bound.query())) {
                                problems.add("cells: expected <" + es + ">, got <"
                                        + actual + ">");
                            }
                        }
                        continue;
                    }
                    Matcher colTypes = Pattern.compile(
                            "^\\$R(?:\\.values)?\\.columns\\.type$").matcher(second);
                    if (colTypes.matches()) {
                        // expected is a list of BARE pure type names
                        // ([String, Integer]) — parsed here, never via
                        // pureLiteral (bare identifiers are not literals)
                        String exp = args.get(0).strip();
                        if (exp.matches("\\[\\s*\\w+(\\s*,\\s*\\w+)*\\s*\\]")) {
                            List<Object> expected = new ArrayList<>();
                            for (String part : exp.substring(1, exp.length() - 1).split(",")) {
                                expected.add(part.strip());
                            }
                            recognized++;
                            verified++;
                            List<Object> actual = new ArrayList<>(bound.colTypes());
                            if (!expected.equals(actual)) {
                                problems.add("column types: expected " + expected
                                        + ", got " + actual);
                            }
                        }
                        continue;
                    }
                    Matcher colNames = Pattern.compile(
                            "^\\$R(?:\\.values)?\\.columns\\.name$").matcher(second);
                    if (colNames.matches()) {
                        List<Object> expected = pureLiteralList(args.get(0).strip());
                        if (expected != null) {
                            recognized++;
                            verified++;
                            List<Object> actual = rows.isEmpty() ? List.of()
                                    : new ArrayList<>(rows.get(0).keySet());
                            if (!orderedEquals(expected, actual)) {
                                problems.add("columns: expected " + expected + ", got " + actual);
                            }
                        }
                        continue;
                    }
                    Matcher propCol = Pattern.compile(
                            "^\\$R(?:\\.values)?\\.(\\w+)$").matcher(second);
                    if (propCol.matches()) {
                        List<Object> expected = pureLiteralList(args.get(0).strip());
                        if (expected != null) {
                            recognized++;
                            verified++;
                            List<Object> actual = column(rows, propCol.group(1));
                            if (!orderedEquals(expected, actual)) {
                                problems.add(propCol.group(1) + ": expected " + expected
                                        + ", got " + actual);
                            }
                        }
                        continue;
                    }
                    Matcher one = Pattern.compile(
                            "^\\$R(?:\\.values)?->(?:toOne|first)\\(\\)(?:\\.(\\w+))?(?:->toOne\\(\\))?$")
                            .matcher(second);
                    Matcher at = Pattern.compile(
                            "^\\$R(?:\\.values)?->at\\((\\d+)\\)\\.(\\w+)$").matcher(second);
                    if (one.matches()) {
                        Object expected = pureLiteral(args.get(0).strip());
                        if (expected != null) {
                            recognized++;
                            verified++;
                            Object actual = rows.isEmpty() ? null
                                    : one.group(1) == null ? rows.get(0)
                                            : rows.get(0).get(one.group(1));
                            if (!valueEquals(expected, actual)) {
                                problems.add("expected " + expected + ", got " + actual);
                            }
                        }
                    } else if (at.matches()) {
                        Object expected = pureLiteral(args.get(0).strip());
                        if (expected != null) {
                            recognized++;
                            verified++;
                            int idx = Integer.parseInt(at.group(1));
                            Object actual = idx < rows.size() ? rows.get(idx).get(at.group(2)) : null;
                            if (!valueEquals(expected, actual)
                                    && !(!sortedQuery(bound.query())
                                            && column(rows, at.group(2)).stream()
                                                    .anyMatch(v -> valueEquals(expected, v)))) {
                                problems.add("at(" + idx + ")." + at.group(2)
                                        + ": expected " + expected + ", got " + actual);
                            }
                        }
                    }
                }
                default -> { }
            }
            if (recognized == recognizedBefore && firstUnrecognized == null) {
                firstUnrecognized = call.length() > 120 ? call.substring(0, 120) : call;
            }
        }
        if (recognized == 0) {
            return new Outcome(fn.fqn(), Status.SHAPE, "no recognizable assertions");
        }
        if (!problems.isEmpty()) {
            return new Outcome(fn.fqn(), Status.FAIL, String.join("; ", problems));
        }
        // NEVER a false pass: PASS requires every assert recognized AND at
        // least one non-advisory comparison to have actually run — a test
        // whose asserts are all golden-SQL proves only "executed", which is
        // SHAPE, not PASS.
        int total = assertCalls(fn.body()).size();
        if (recognized < total) {
            return new Outcome(fn.fqn(), Status.SHAPE,
                    "partial: " + recognized + "/" + total + " asserts recognized"
                            + " (recognized ones hold); first unrecognized: "
                            + firstUnrecognized);
        }
        if (verified == 0) {
            return new Outcome(fn.fqn(), Status.SHAPE,
                    "sql-only: " + recognized + " advisory golden-SQL assert(s), no row verification");
        }
        return new Outcome(fn.fqn(), Status.PASS, verified + " assert(s)");
    }

    /** Class results arrive as the GRAPH envelope (JSON rows); TDS as tabular. */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> graphRows(ExecutionResult r) {
        if (r instanceof ExecutionResult.ScalarResult sc) {
            // NULL stays null — coercing to "" let an empty-string
            // expectation pass against a wrongly-NULL scalar (audit A3);
            // ^TDSNull() matches null via NULL_EXPECTED, everything else
            // honestly fails
            return List.of(java.util.Collections.singletonMap("__scalar__", sc.value()));
        }
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

    /**
     * CSV equality honoring the QUERY's order contract: a query with no
     * sort has NO defined row order (the engine expectation encodes H2's
     * incidental order; DuckDB's differs run to run with seed layout), so
     * data rows compare as a MULTISET under an identical header. A sorted
     * query compares exactly.
     */
    /** The query text carries an explicit sort — its row order is a contract. */
    private static boolean sortedQuery(String query) {
        return query != null
                && Pattern.compile("->\\s*(sort|sortBy)\\s*\\(").matcher(query).find();
    }

    /**
     * makeString-join equality under the same order contract as
     * {@link #csvEquals}: an explicit {@code ->sort()} on the assert side
     * or a sorted query compares exactly; otherwise the joined parts
     * compare as a MULTISET (separator-split — engine expectations encode
     * H2's incidental row order).
     */
    private static boolean joinedEquals(String expected, String actual, String sep,
            boolean explicitSort, String query) {
        if (expected.equals(actual)) {
            return true;
        }
        if (explicitSort || sortedQuery(query) || sep.isEmpty()) {
            return false;
        }
        List<String> e = new ArrayList<>(List.of(expected.split(Pattern.quote(sep), -1)));
        for (String part : actual.split(Pattern.quote(sep), -1)) {
            if (!e.remove(part)) {
                return false;
            }
        }
        return e.isEmpty();
    }

    private static boolean csvEquals(String expected, String actual, String query) {
        if (expected.equals(actual)) {
            return true;
        }
        if (sortedQuery(query)) {
            return false;   // ordered contract: exact only
        }
        String[] e = expected.split("\n", -1);
        String[] a = actual.split("\n", -1);
        if (e.length != a.length || e.length == 0 || !e[0].equals(a[0])) {
            return false;
        }
        List<String> pool = new ArrayList<>(List.of(a).subList(1, a.length));
        for (int i = 1; i < e.length; i++) {
            if (!pool.remove(e[i])) {
                return false;
            }
        }
        return true;
    }

    /** TDS toCSV rendering: header row + comma rows, each \n-terminated. */
    private static String toCsv(List<Map<String, Object>> rows) {
        if (rows.isEmpty()) {
            return "";
        }
        StringBuilder out = new StringBuilder();
        out.append(String.join(",", rows.get(0).keySet())).append('\n');
        for (var row : rows) {
            StringBuilder line = new StringBuilder();
            for (Object v : row.values()) {
                if (line.length() > 0) {
                    line.append(',');
                }
                line.append(v == null ? "" : String.valueOf(v));
            }
            out.append(line).append('\n');
        }
        return out.toString();
    }

    /**
     * Collapse whitespace runs to one space and tighten spacing around
     * {@code ->} — OUTSIDE single-quoted string literals only. The old
     * whole-text collapse corrupted separators like {@code makeString(' ')}
     * and any literal containing {@code ->} (audit B2).
     */
    static String collapseOutsideStrings(String s) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '\'') {
                int end = Corpus.skipString(s, i);
                out.append(s, i, end);
                i = end;
                continue;
            }
            if (Character.isWhitespace(c)) {
                int j = i;
                while (j < s.length() && Character.isWhitespace(s.charAt(j))) {
                    j++;
                }
                boolean beforeArrow = j + 1 < s.length()
                        && s.charAt(j) == '-' && s.charAt(j + 1) == '>';
                boolean afterArrow = out.length() >= 2
                        && out.charAt(out.length() - 1) == '>'
                        && out.charAt(out.length() - 2) == '-';
                if (!beforeArrow && !afterArrow && j < s.length()) {
                    out.append(' ');
                }
                i = j;
                continue;
            }
            out.append(c);
            i++;
        }
        return out.toString();
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

    /** Some row's cell list equals {@code expected} (position-free row assert). */
    private static boolean anyRowMatches(List<Map<String, Object>> rows,
            List<Object> expected) {
        for (var row : rows) {
            if (orderedEquals(expected, new ArrayList<>(row.values()))) {
                return true;
            }
        }
        return false;
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

    private static boolean multisetEquals(List<Object> expected, List<Object> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }
        List<Object> pool = new ArrayList<>(actual);
        for (Object e : expected) {
            int hit = -1;
            for (int i = 0; i < pool.size(); i++) {
                if (valueEquals(e, pool.get(i))) {
                    hit = i;
                    break;
                }
            }
            if (hit < 0) {
                return false;
            }
            pool.remove(hit);
        }
        return true;
    }

    private static boolean valueEquals(Object expected, Object actual) {
        if (expected == NULL_EXPECTED) {
            return actual == null;
        }
        if (actual == null) {
            return false;   // null never equals a literal expectation
        }
        if (expected instanceof DateExpected de) {
            // canonical date compare: ISO-ify, drop the zone and
            // insignificant fractional zeros, then trailing midnight
            String a = canonicalDate(String.valueOf(actual));
            String e = canonicalDate(de.iso());
            return a.equals(e) || String.valueOf(actual).equals(de.iso());
        }
        if (expected instanceof EnumExpected ee) {
            return ee.valueName().equals(String.valueOf(actual));
        }
        if (expected instanceof Long el) {
            if (actual instanceof Long al) {
                return el.longValue() == al.longValue();
            }
            if (actual instanceof Integer ai) {
                return el.longValue() == ai.longValue();
            }
            if (actual instanceof java.math.BigInteger bi) {
                return bi.equals(java.math.BigInteger.valueOf(el));
            }
            if (actual instanceof java.math.BigDecimal bd) {
                return bd.compareTo(java.math.BigDecimal.valueOf(el)) == 0;
            }
            if (actual instanceof Double ad) {
                return Math.abs(el.doubleValue() - ad) < 1e-9;
            }
            return false;
        }
        if (expected instanceof Double ed) {
            return actual instanceof Number an
                    && Math.abs(ed - an.doubleValue()) < 1e-9;
        }
        if (expected instanceof Boolean eb) {
            return actual instanceof Boolean ab ? eb.equals(ab) : false;
        }
        // string expectation vs stringified actual — but NEVER across the
        // number/boolean kinds ('25' must not equal integer 25: that
        // collapse masked wrong column typing, audit B5). Date/time actuals
        // legitimately compare through their wire print form.
        return expected instanceof String es
                && !(actual instanceof Number) && !(actual instanceof Boolean)
                && es.equals(String.valueOf(actual));
    }

    // ===== pure literal parsing (expected values) =====

    static Object pureLiteral(String text) {
        text = text.strip();
        // single string literal OR literal-CONCAT chain 'a' + 'b' (toCSV
        // expectations are spelled as concatenated string literals)
        if (text.startsWith("'")) {
            StringBuilder out = new StringBuilder();
            int i = 0;
            while (i < text.length()) {
                if (text.charAt(i) != '\'') {
                    return null;
                }
                int end = Corpus.skipString(text, i);
                out.append(text, i + 1, end - 1);
                i = end;
                while (i < text.length() && Character.isWhitespace(text.charAt(i))) {
                    i++;
                }
                if (i >= text.length()) {
                    return out.toString().replace("\\'", "'").replace("\\n", "\n");
                }
                if (text.charAt(i) != '+') {
                    return null;
                }
                i++;
                while (i < text.length() && Character.isWhitespace(text.charAt(i))) {
                    i++;
                }
            }
            return out.toString().replace("\\'", "'").replace("\\n", "\n");
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
        // ^TDSNull() — the TDS null cell instance literal
        if (text.equals("^TDSNull()")) {
            return NULL_EXPECTED;
        }
        // #TDS <header>\n<rows>...# grid literal
        if (text.startsWith("#TDS") && text.endsWith("#")) {
            String[] lines = text.substring(4, text.length() - 1).strip().split("\n");
            if (lines.length == 0) {
                return null;
            }
            List<String> cols = new ArrayList<>();
            for (String c : lines[0].split(",")) {
                cols.add(c.strip());
            }
            List<List<Object>> rows2 = new ArrayList<>();
            for (int li = 1; li < lines.length; li++) {
                if (lines[li].strip().isEmpty()) {
                    continue;
                }
                List<Object> row = new ArrayList<>();
                for (String cell : lines[li].split(",", -1)) {
                    row.add(tdsCell(cell.strip()));
                }
                rows2.add(row);
            }
            return new TdsExpected(cols, rows2);
        }
        // %date literals — compared through their canonical print form
        if (text.matches("%-?\\d{4}[-\\dT:.+Z]*")) {
            return new DateExpected(text.substring(1));
        }
        // Enum.VALUE references — compared by VALUE NAME (the wire carries
        // enum names)
        Matcher em = Pattern.compile("^((?:\\w+::)*\\w+)\\.(\\w+)$").matcher(text);
        if (em.matches() && !text.contains("(")) {
            return new EnumExpected(em.group(2));
        }
        return null;   // not a literal — unrecognized assert shape
    }

    /** The ^TDSNull() expectation: matches ONLY a null cell. */
    static final Object NULL_EXPECTED = new Object() {
        @Override
        public String toString() {
            return "^TDSNull()";
        }
    };

    /** A {@code #TDS ... #} literal expectation: header + parsed row grid. */
    record TdsExpected(List<String> cols, List<List<Object>> rows) {
    }

    /** One TDS-literal cell: integer/float/boolean/date by shape, null for empty. */
    private static Object tdsCell(String cell) {
        if (cell.isEmpty() || cell.equals("null")) {
            return NULL_EXPECTED;
        }
        if (cell.matches("-?\\d+")) {
            return Long.parseLong(cell);
        }
        if (cell.matches("-?\\d+\\.\\d+")) {
            return Double.parseDouble(cell);
        }
        if (cell.equals("true") || cell.equals("false")) {
            return Boolean.parseBoolean(cell);
        }
        if (cell.matches("\\d{4}-\\d{2}-\\d{2}([T ].*)?")) {
            return new DateExpected(cell.replace(' ', 'T'));
        }
        return cell;
    }

    /** Type-aware cell comparison for assert-side sorts (nulls last). */
    private static int compareCells(Object a, Object b) {
        if (a == null || b == null) {
            return a == null ? (b == null ? 0 : 1) : -1;
        }
        if (a instanceof Number na && b instanceof Number nb) {
            return Double.compare(na.doubleValue(), nb.doubleValue());
        }
        return String.valueOf(a).compareTo(String.valueOf(b));
    }

    /** Row-grid equality under the query's order contract. */
    private static boolean tdsRowsEqual(List<List<Object>> expected,
            List<Map<String, Object>> actual, String query) {
        if (expected.size() != actual.size()) {
            return false;
        }
        if (sortedQuery(query)) {
            for (int i = 0; i < expected.size(); i++) {
                if (!orderedEquals(expected.get(i),
                        new ArrayList<>(actual.get(i).values()))) {
                    return false;
                }
            }
            return true;
        }
        List<List<Object>> pool = new ArrayList<>();
        actual.forEach(r -> pool.add(new ArrayList<>(r.values())));
        for (List<Object> e : expected) {
            int hit = -1;
            for (int i = 0; i < pool.size(); i++) {
                if (orderedEquals(e, pool.get(i))) {
                    hit = i;
                    break;
                }
            }
            if (hit < 0) {
                return false;
            }
            pool.remove(hit);
        }
        return true;
    }

    private static String canonicalDate(String s) {
        // zone suffix and insignificant fractional zeros are PRINT
        // variance; a trailing T00:00 is NOT — stripping it equated a DATE
        // with a midnight DATETIME, masking wrong column typing (audit A4)
        String v = s.replace(' ', 'T')
                .replaceAll("\\+0000$", "").replaceAll("Z$", "");
        if (v.contains(".")) {
            v = v.replaceAll("0+$", "").replaceAll("\\.$", "");
        }
        return v;
    }

    /** A %date expectation: equality via canonical date-print comparison. */
    record DateExpected(String iso) { }

    /** An Enum.VALUE expectation: equality via the value NAME (wire convention). */
    record EnumExpected(String valueName) { }

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
        for (int[] span : assertCallSpans(body)) {
            out.add(body.substring(span[0], span[1]));
        }
        return out;
    }

    /** [start, end) spans of assert*(...) calls, in body order. */
    static List<int[]> assertCallSpans(String body) {
        List<int[]> out = new ArrayList<>();
        Matcher m = Pattern.compile("\\b(assert\\w*)\\s*\\(").matcher(body);
        while (m.find()) {
            int open = body.indexOf('(', m.start());
            int close = matchParen(body, open);
            if (close > 0) {
                out.add(new int[]{m.start(), close + 1});
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
            if (c == '#') {
                int close = s.indexOf('#', i + 1);
                if (close > 0) {
                    i = close;      // #...# island: parens inside are data
                    continue;
                }
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

    /**
     * Split a call's argument text on top-level commas (string/bracket
     * aware; {@code #...#} islands — TDS literals, relation refs, path
     * literals — are opaque: their commas and brackets are data).
     */
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
            if (c == '#') {
                int close = s.indexOf('#', i + 1);
                if (close > 0) {
                    i = close;
                    continue;
                }
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
        String scope = model + familyModels.getOrDefault(currentFamilyKey, "")
                + fileModels.getOrDefault(currentFileKey, "");
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
                .matcher(model + familyModels.getOrDefault(currentFamilyKey, "")
                        + fileModels.getOrDefault(currentFileKey, ""));
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
                    String d = o.detail().replace("\n", "\\n");
                    sb.append("- ").append(o.status()).append(' ')
                            .append(o.test().substring(o.test().lastIndexOf("::") + 2))
                            .append(" [").append(e.getKey()).append("]: ")
                            .append(d, 0, Math.min(300, d.length()))
                            .append('\n');
                }
            }
        }
        Files.writeString(out, sb.toString());
    }
}
