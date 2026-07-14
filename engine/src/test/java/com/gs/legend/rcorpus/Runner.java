// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.gs.legend.rcorpus;


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
            var seedTypes0 = Corpus.seedColumnTypes(src);
            for (var defs : Corpus.tableDefsAll(src).values()) {
                var seed0 = seedTypes0.get(defs.get(0).name().toLowerCase());
                var d = pickBySeed(defs, seed0);
                if (!d.schema().isEmpty() && !d.schema().equals("default")) {
                    ddl.add("CREATE SCHEMA IF NOT EXISTS " + d.schema());
                }
                ddl.add(d.createSql(seed0 == null ? java.util.Map.of() : seed0));
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
        useFamily(familyKey, setupSources, modelOnlySources, null);
    }

    /**
     * {@code parentFamilyKey}: a DEEP subfamily (tests/mapping/union/relation)
     * references its parent family's elements (~func bodies read the parent
     * db) — the engine compiles the module together. The parent's
     * already-vetted model text and DDL prepend; the child's own definitions
     * and seeds run after and win.
     */
    public void useFamily(String familyKey, List<String> setupSources,
            List<String> modelOnlySources, String parentFamilyKey) {
        currentFamilyKey = familyKey;
        if (familyModels.containsKey(familyKey)) {
            return;
        }
        StringBuilder ext = new StringBuilder();
        List<String> sql = new ArrayList<>();
        if (parentFamilyKey != null && familyModels.containsKey(parentFamilyKey)) {
            ext.append(familyModels.get(parentFamilyKey)).append('\n');
            sql.addAll(familySeeds.getOrDefault(parentFamilyKey, List.of()));
        }
        StringBuilder assembled = new StringBuilder(model).append(ext);
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
            var seedTypes1 = Corpus.seedColumnTypes(src);
            for (var defs : Corpus.tableDefsAll(src).values()) {
                var seed1 = seedTypes1.get(defs.get(0).name().toLowerCase());
                var d = pickBySeed(defs, seed1);
                if (!d.schema().isEmpty() && !d.schema().equals("default")) {
                    sql.add("CREATE SCHEMA IF NOT EXISTS " + d.schema());
                }
                sql.add(d.createSql(seed1 == null ? java.util.Map.of() : seed1));
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
            var seedTypes2 = Corpus.seedColumnTypes(src);
            for (var defs : Corpus.tableDefsAll(src).values()) {
                var seed2 = seedTypes2.get(defs.get(0).name().toLowerCase());
                var d = pickBySeed(defs, seed2);
                if (!d.schema().isEmpty() && !d.schema().equals("default")) {
                    preSql.add("CREATE SCHEMA IF NOT EXISTS " + d.schema());
                }
                preSql.add(d.createSql(seed2 == null ? java.util.Map.of() : seed2));
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
        var seedTypes3 = Corpus.seedColumnTypes(source);
        for (var defs : Corpus.tableDefsAll(source).values()) {
            var seed3 = seedTypes3.get(defs.get(0).name().toLowerCase());
            var d = pickBySeed(defs, seed3);
            if (!d.schema().isEmpty() && !d.schema().equals("default")) {
                sql.add("CREATE SCHEMA IF NOT EXISTS " + d.schema());
            }
            sql.add(d.createSql(seed3 == null ? java.util.Map.of() : seed3));
        }
        fileSeeds.put(key, sql);
    }


    /** The declared def the SEEDS actually created: among same-named
     * declarations, the one whose column set best matches the harness's
     * own create-table statement; ties/no-seed keep the first. */
    private static Corpus.TableDef pickBySeed(java.util.List<Corpus.TableDef> defs,
            java.util.Map<String, String> seedCols) {
        if (defs.size() == 1 || seedCols == null || seedCols.isEmpty()) {
            return defs.get(0);
        }
        Corpus.TableDef best = defs.get(0);
        int bestScore = -1;
        for (Corpus.TableDef d : defs) {
            String txt = d.columnsText().toLowerCase();
            int score = 0;
            for (String c : seedCols.keySet()) {
                if (txt.matches("(?s).*\\b" + java.util.regex.Pattern.quote(c) + "\\b.*")) {
                    score++;
                }
            }
            if (score > bestScore) {
                bestScore = score;
                best = d;
            }
        }
        return best;
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



    public Outcome run(Corpus.TestFn fn) {
        // NATIVE test-body execution: core (com.legend.TestBody) compiles
        // and drives the WHOLE body — lets, execute() handles, assert
        // natives — through the ordinary pipeline. The harness's remaining
        // jobs are model assembly, the synthesized Runtime element, seed
        // replay and scoring. The mapping refs are extracted only to build
        // the Runtime; TestBody splices ->from(mapping, rcorpus::Rt) per
        // execute() itself.
        List<String> mappingRefs = executeMappingRefs(fn);
        if (mappingRefs.isEmpty()) {
            return new Outcome(fn.fqn(), Status.SHAPE, "no execute(|...) call");
        }
        try {
            String familyExt = familyModels.getOrDefault(currentFamilyKey, "");
            String fileExt = fileModels.getOrDefault(currentFileKey, "");
            String modelText = model + familyExt + fileExt;
            String fullModel = modelText + runtimeBlock(modelText, mappingRefs);
            com.legend.compiler.element.ModelContext ctx = contextFor(fullModel);
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
                try (var st = conn.createStatement()) {
                    st.execute("SET TimeZone='UTC'");
                }
                List<String> failedSeeds = replaySeeds(fn, conn);
                seedFailures.addAll(failedSeeds);
                com.legend.TestBody.Outcome o = com.legend.TestBody.run(
                        ctx, fn.body(), importScopeOf(fn), "rcorpus::Rt", conn,
                        !failedSeeds.isEmpty(), harnessSetupNames());
                return switch (o) {
                    case com.legend.TestBody.Outcome.Unsupported u ->
                            new Outcome(fn.fqn(), Status.SHAPE, u.reason());
                    case com.legend.TestBody.Outcome.Ran r -> {
                        if (!r.failures().isEmpty()) {
                            yield new Outcome(fn.fqn(), Status.FAIL,
                                    r.failures().get(0));
                        }
                        if (r.verified() == 0 && r.advisory() > 0) {
                            yield new Outcome(fn.fqn(), Status.SHAPE,
                                    "sql-only: " + r.advisory()
                                            + " advisory golden-SQL assert(s),"
                                            + " no row verification");
                        }
                        if (r.verified() == 0) {
                            yield new Outcome(fn.fqn(), Status.SHAPE,
                                    "no verifying assertions");
                        }
                        yield new Outcome(fn.fqn(), Status.PASS,
                                r.verified() + " assert(s)");
                    }
                };
            }
        } catch (Exception e) {
            // flatten, don't truncate — poison reasons ride on later lines
            return new Outcome(fn.fqn(), Status.ERROR,
                    String.valueOf(e.getMessage()).replace("\n", " | "));
        }
    }

    /** Compiled-model cache: one context per assembled model text. */
    private final Map<String, com.legend.compiler.element.ModelContext> ctxCache =
            new LinkedHashMap<>();

    private com.legend.compiler.element.ModelContext contextFor(String fullModel) {
        return ctxCache.computeIfAbsent(fullModel, com.legend.Compiler::compileModel);
    }

    /** Setup-function names (FQN + simple) whose effects the seed replay applied. */
    private java.util.Set<String> harnessSetupNames() {
        java.util.Set<String> out = new java.util.HashSet<>();
        for (String fqn : setupFnBodies.keySet()) {
            out.add(fqn);
            out.add(fqn.substring(fqn.lastIndexOf(':') + 1));
        }
        return out;
    }

    /** The MAPPING argument of every execute() call in the body, qualified. */
    private List<String> executeMappingRefs(Corpus.TestFn fn) {
        List<String> out = new ArrayList<>();
        Matcher m = EXECUTE_CALL.matcher(fn.body());
        while (m.find()) {
            if (insideString(fn.body(), m.start())) {
                continue;
            }
            int argStart = fn.body().indexOf('(', m.start());
            int argEnd = matchParen(fn.body(), argStart);
            if (argEnd < 0) {
                continue;
            }
            List<String> args = splitArgs(fn.body().substring(argStart + 1, argEnd));
            if (args.size() < 2) {
                continue;
            }
            String ref = qualify(args.get(1).strip(), fn);
            if (ref.matches("[\\w:]+") && !out.contains(ref)) {
                out.add(ref);
            }
        }
        // brace-lambda spellings put the mapping IN the query
        // (execute({|Q->from(M, $rt)}, ^Mapping(name=''), ...)) — collect
        // in-query from() mappings too
        Matcher fm = Pattern.compile("->\\s*from\\(\\s*([\\w:]+)")
                .matcher(fn.body());
        while (fm.find()) {
            String ref = qualify(fm.group(1), fn);
            if (ref.matches("[\\w:]+") && !out.contains(ref)) {
                out.add(ref);
            }
        }
        return out;
    }

    /** Seed replay: ALL DDL first, then data (audit A2), one statement per execute. */
    private List<String> replaySeeds(Corpus.TestFn fn, Connection conn) {
        List<String> allSeeds = new ArrayList<>(ddlSeeds);
        allSeeds.addAll(familySeeds.getOrDefault(currentFamilyKey, List.of()));
        allSeeds.addAll(fileSeeds.getOrDefault(currentFileKey, List.of()));
        allSeeds.addAll(dataSeeds);
        java.util.Set<String> expanded = new java.util.HashSet<>(sharedSeededFns);
        for (Corpus.BeforePackage bp : beforePackages) {
            if (fn.fqn().startsWith(bp.pkg() + "::")) {
                boolean includeBody = expanded.add(bp.fqn());
                allSeeds.addAll(Corpus.expandSeeds(bp.body(), bp.pkg(),
                        setupFnBodies, expanded, includeBody));
            }
        }
        List<String> failedSeeds = new ArrayList<>();
        for (String sql : allSeeds) {
            for (String stmt : splitStatements(sql)) {
                try (var st = conn.createStatement()) {
                    st.execute(stmt);
                } catch (Exception e) {
                    String head = stmt.strip().split("\n")[0];
                    failedSeeds.add(head + " => "
                            + String.valueOf(e.getMessage()).split("\n")[0]);
                }
            }
        }
        return failedSeeds;
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




    /** Synthesized runtime binding EVERY database (stores pick what they need). */
    private String runtimeBlock(String modelText, List<String> mappingFqns) {
        String mappingFqn = String.join(", ", mappingFqns);
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


    /** The test file's import sections + the test's own package (implicit). */
    private static com.legend.parser.ImportScope importScopeOf(Corpus.TestFn fn) {
        List<String> wildcards = new ArrayList<>(fn.wildcardImports());
        int cut = fn.fqn().lastIndexOf("::");
        if (cut > 0) {
            wildcards.add(fn.fqn().substring(0, cut));
        }
        return new com.legend.parser.ImportScope(wildcards, fn.imports());
    }














    // ===== pure literal parsing (expected values) =====













    // ===== text machinery =====



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
