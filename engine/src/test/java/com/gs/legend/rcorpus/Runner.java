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
    /** Element keys (kind::fqn) of the SHARED base model — dedup floor. */
    private final java.util.Set<String> sharedSeen = new java.util.HashSet<>();

    // ===== MODULE assembly (Phase B): raw sources through the real
    // parser — the text-extraction path below it is being retired =====
    private final List<com.legend.Compiler.ModelSource> sharedRaw = new ArrayList<>();
    private final Map<String, List<com.legend.Compiler.ModelSource>> familyRaw =
            new LinkedHashMap<>();
    private final Map<String, com.legend.Compiler.ModelSource> fileRaw =
            new LinkedHashMap<>();
    private final Map<String, String> familyParent = new LinkedHashMap<>();
    private final Map<String, com.legend.Compiler.BuiltModule> moduleCache =
            new LinkedHashMap<>();
    private final java.util.Set<String> reportedModuleWalls = new java.util.HashSet<>();

    public Runner(List<String> sharedSources, List<String> seedSources) {
        for (int i = 0; i < sharedSources.size(); i++) {
            sharedRaw.add(new com.legend.Compiler.ModelSource(
                    "shared-" + i + ".pure", sharedSources.get(i)));
        }
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
        List<com.legend.Compiler.ModelSource> raw = new ArrayList<>();
        int rawIx = 0;
        for (String src : setupSources) {
            raw.add(new com.legend.Compiler.ModelSource(
                    familyKey + "/setup-" + rawIx++ + ".pure", src));
        }
        for (String src : modelOnlySources) {
            raw.add(new com.legend.Compiler.ModelSource(
                    familyKey + "/sibling-" + rawIx++ + ".pure", src));
        }
        familyRaw.put(familyKey, raw);
        if (parentFamilyKey != null) {
            familyParent.put(familyKey, parentFamilyKey);
        }
        for (String src : setupSources) {
            Corpus.functionBodies(src).forEach(setupFnBodies::putIfAbsent);
        }
        for (String src : modelOnlySources) {
            Corpus.functionBodies(src).forEach(setupFnBodies::putIfAbsent);
        }
        StringBuilder ext = new StringBuilder();
        List<String> sql = new ArrayList<>();
        if (parentFamilyKey != null && familyModels.containsKey(parentFamilyKey)) {
            ext.append(familyModels.get(parentFamilyKey)).append('\n');
            sql.addAll(familySeeds.getOrDefault(parentFamilyKey, List.of()));
        }
        String prefix = model + ext;
        // pass 1: every family file's NON-mapping elements, deduped by
        // kind::fqn (the real engine compiles the whole module together;
        // cross-file references — embedded's BondDetail association,
        // inheritance's cross-file includes — are normal). A mapping in
        // file A may reference classes from file B, so mappings probe
        // after ALL classes/stores across the family are present.
        List<String[]> mappings = new ArrayList<>();
        List<String[]> baseEls = new ArrayList<>();   // {kind fqn, text}
        java.util.Set<String> seen = new java.util.HashSet<>(sharedSeen);
        familySeen.put(familyKey, seen);
        for (String src : setupSources) {
            for (String[] el : splitSectioned(Corpus.modelElements(src))) {
                if (!seen.add(el[0] + "::" + el[1])) {
                    continue;   // first definition wins
                }
                if (el[0].equals("Mapping")) {
                    mappings.add(el);
                } else {
                    baseEls.add(new String[]{el[0] + " " + el[1],
                            el[2] + el[3] + "\n"});
                }
            }
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
            for (String[] el : splitSectioned(Corpus.modelElements(src))) {
                if (!seen.add(el[0] + "::" + el[1])) {
                    continue;
                }
                if (el[0].equals("Mapping")) {
                    mappings.add(el);
                } else {
                    baseEls.add(new String[]{el[0] + " " + el[1],
                            el[2] + el[3] + "\n"});
                }
            }
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
        // pass 1.5: the assembled BASE itself must compile — a failing base
        // element (engine plan-metamodel classes and other platform-internal
        // constructs) is DROPPED and walled by error-line attribution, so
        // one exotic class no longer darkens its whole family
        // (executionPlan: two ExecutionOption classes hid 109 tests)
        List<int[]> ranges = new ArrayList<>();
        StringBuilder assembled = buildBase(prefix, baseEls, ranges);
        for (int attempt = 0; attempt < 24 && !baseEls.isEmpty(); attempt++) {
            try {
                com.legend.Compiler.compile(assembled.toString(), "|1", "n/a");
                break;
            } catch (Exception e) {
                int line = errorLineOf(e);
                int idx = -1;
                for (int r = 0; r < ranges.size(); r++) {
                    if (line >= ranges.get(r)[0] && line <= ranges.get(r)[1]) {
                        idx = r;
                        break;
                    }
                }
                if (idx < 0) {
                    break;   // unattributable: leave; mapping probes wall as before
                }
                walls.add(familyKey + " " + baseEls.get(idx)[0] + " => "
                        + String.valueOf(e.getMessage()).split("\n")[0]);
                baseEls.remove(idx);
                ranges.clear();
                assembled = buildBase(prefix, baseEls, ranges);
            }
        }
        for (String[] b : baseEls) {
            ext.append('\n').append(b[1]);
        }
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

    /** The base assembly with per-element line ranges (1-based, inclusive). */
    private static StringBuilder buildBase(String prefix, List<String[]> els,
            List<int[]> rangesOut) {
        StringBuilder sb = new StringBuilder(prefix);
        int nl = countNl(prefix);
        for (String[] el : els) {
            sb.append('\n');
            nl++;
            int start = nl + 1;
            sb.append(el[1]);
            nl += countNl(el[1]);
            rangesOut.add(new int[]{start, nl + 1});
        }
        return sb;
    }

    private static int countNl(String s) {
        int c = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == '\n') {
                c++;
            }
        }
        return c;
    }

    /** The 1-based line of a compile error's {@code [line:col]} prefix, or -1. */
    private static int errorLineOf(Throwable e) {
        for (Throwable c = e; c != null; c = c.getCause()) {
            String msg = c.getMessage();
            if (msg != null) {
                Matcher m = Pattern.compile("\\[(\\d+):\\d+\\]").matcher(msg);
                if (m.find()) {
                    return Integer.parseInt(m.group(1));
                }
            }
        }
        return -1;
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
        fileRaw.put(key, new com.legend.Compiler.ModelSource(key, source));
        Corpus.functionBodies(source).forEach(setupFnBodies::putIfAbsent);
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
            // MODULE path (moduleContextFor) is DARK-LAUNCHED: the
            // equivalence sweep showed 699 vs 881 — function bodies now
            // enter the model and integrity walls cascade through helper
            // functions the mappings depend on (the old text path STRIPPED
            // bodies). Flip back after the wall-cascade triage (task #54).
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

    /**
     * MODULE-assembled context (Phase B): the shared + parent + family +
     * test-file RAW sources compile TOGETHER through the real parser —
     * per-file import sections, structured per-element walls, no text
     * extraction. The synthesized Runtime is one more source unit; its
     * connections come from the module's own parsed Database elements.
     */
    private com.legend.compiler.element.ModelContext moduleContextFor() {
        String cacheKey = currentFamilyKey + "|" + currentFileKey;
        com.legend.Compiler.BuiltModule cached = moduleCache.get(cacheKey);
        if (cached != null) {
            return cached.context();
        }
        List<com.legend.Compiler.ModelSource> sources = new ArrayList<>(sharedRaw);
        String parent = familyParent.get(currentFamilyKey);
        if (parent != null && familyRaw.containsKey(parent)) {
            sources.addAll(familyRaw.get(parent));
        }
        sources.addAll(familyRaw.getOrDefault(currentFamilyKey, List.of()));
        com.legend.Compiler.ModelSource file = fileRaw.get(currentFileKey);
        if (file != null) {
            sources.add(file);
        }
        // per-source tolerant PARSE: an unparseable file is a FILE wall,
        // never a family poison
        List<com.legend.Compiler.ModelSource> parseable = new ArrayList<>(sources.size());
        for (com.legend.Compiler.ModelSource src : sources) {
            try {
                com.legend.parser.ElementParser.parse(src.text());
                parseable.add(src);
            } catch (RuntimeException e) {
                wallOnce("file " + src.name() + " => "
                        + String.valueOf(e.getMessage()).split("\n")[0]);
            }
        }
        com.legend.Compiler.ParsedModule pre =
                com.legend.Compiler.parseSources(parseable);
        // the Runtime references every Database the MODULE declares —
        // enumerated from parsed elements, not regex
        StringBuilder conns = new StringBuilder();
        for (com.legend.model.PackageableElement el : pre.model().elements()) {
            if (el instanceof com.legend.model.DatabaseDefinition db) {
                if (conns.length() > 0) {
                    conns.append(", ");
                }
                conns.append(db.qualifiedName()).append(": [ c: rcorpus::Conn ]");
            }
        }
        parseable.add(new com.legend.Compiler.ModelSource("rcorpus-runtime.pure",
                "RelationalDatabaseConnection rcorpus::Conn { type: DuckDB;"
                        + " specification: InMemory { }; auth: NoAuth { }; }\n"
                        + "Runtime rcorpus::Rt { mappings: []; connections: [ "
                        + conns + " ] }\n"));
        com.legend.Compiler.ParsedModule module =
                com.legend.Compiler.parseSources(parseable);
        module.duplicateElements().forEach(d ->
                wallOnce(currentFamilyKey + " duplicate " + d));
        com.legend.Compiler.BuiltModule built =
                com.legend.Compiler.buildModule(module.model());
        built.walls().forEach((fqn, msg) ->
                wallOnce(currentFamilyKey + " " + fqn + " => " + msg));
        moduleCache.put(cacheKey, built);
        return built.context();
    }

    private void wallOnce(String wall) {
        if (reportedModuleWalls.add(wall)) {
            walls.add(wall);
        }
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
        // TEST-BODY setup calls (modelJoin's setupTestData(...)): a
        // statement calling a KNOWN function replays that function's seeds
        // here; TestBody then skips the statement by name
        // (harnessSetupNames covers every known fn). The contains-probe is
        // deliberately coarse — extra seeding is idempotent DDL/inserts.
        for (Map.Entry<String, String> en
                : new ArrayList<>(setupFnBodies.entrySet())) {
            String simple = en.getKey().substring(
                    en.getKey().lastIndexOf(':') + 1);
            // token-boundary + package-scoped (audit 16 F3b): the raw
            // substring probe matched "fillDb(" inside
            // "createTablesAndFillDb(" and pulled same-named setups from
            // FOREIGN families, whose create-table seeds silently replaced
            // the current family's filled tables
            String fnPkg = en.getKey().contains("::")
                    ? en.getKey().substring(0, en.getKey().lastIndexOf("::"))
                    : "";
            boolean inScope = fnPkg.isEmpty()
                    || fn.fqn().startsWith(fnPkg + "::");
            if (inScope && Pattern.compile(
                    "(?<![\\w$])" + Pattern.quote(simple) + "\\s*\\(")
                    .matcher(fn.body()).find()) {
                boolean includeBody = expanded.add(en.getKey());
                String pkg = en.getKey().contains("::")
                        ? en.getKey().substring(0, en.getKey().lastIndexOf("::"))
                        : "";
                allSeeds.addAll(Corpus.expandSeeds(en.getValue(), pkg,
                        setupFnBodies, expanded, includeBody));
            }
        }
        List<String> failedSeeds = new ArrayList<>();
        // resolve dropAndCreate markers (emitted IN CALL ORDER by
        // Corpus.seedSql) to the family's CREATE statements at replay
        // position — the engine's inline drop+create+fill order
        List<String> resolved = new ArrayList<>(allSeeds.size());
        for (String sql : allSeeds) {
            if (sql.startsWith(Corpus.DROP_AND_CREATE_MARKER)) {
                resolved.addAll(familyCreatesOf(
                        sql.substring(Corpus.DROP_AND_CREATE_MARKER.length())));
            } else {
                resolved.add(sql);
            }
        }
        allSeeds = resolved;
        for (String sql : allSeeds) {
            for (String stmt : splitStatements(sql)) {
                // prepare(): DuckDB JDBC masks Statement.execute errors
                try (var st = conn.prepareStatement(stmt)) {
                    st.execute();
                } catch (Exception e) {
                    String head = stmt.strip().split("\n")[0];
                    failedSeeds.add(head + " => "
                            + String.valueOf(e.getMessage()).split("\n")[0]);
                }
            }
        }
        return failedSeeds;
    }

    /**
     * {@code dropAndCreateTableInDb(Db, 'Table', $conn)} re-emission: the
     * engine recreates the table from the STORE's DDL at the call site.
     * Without this, a collide-named table (calendarAggregation's FirmTable
     * vs the shared model's) keeps whatever shape the LAST create in the
     * seed stream gave it — the shared data seeds run after the family
     * DDL and clobber the family shape. Re-emitting the family's own
     * CREATE here restores it right before the setup's inserts.
     */
    /** The family CREATE statements for {@code table} (simple-name match). */
    private List<String> familyCreatesOf(String table) {
        List<String> out = new ArrayList<>();
        String simple = table.contains(".")
                ? table.substring(table.lastIndexOf('.') + 1) : table;
        for (String s : familySeeds.getOrDefault(currentFamilyKey, List.of())) {
            Matcher c = Pattern.compile(
                    "(?i)^\\s*CREATE OR REPLACE TABLE\\s+([\\w.\"]+)")
                    .matcher(s);
            if (!c.find()) {
                continue;
            }
            String created = c.group(1).replace("\"", "");
            String createdSimple = created.contains(".")
                    ? created.substring(created.lastIndexOf('.') + 1) : created;
            if (createdSimple.equalsIgnoreCase(simple)) {
                out.add(s);
            }
        }
        return out;
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
    private static com.legend.model.ImportScope importScopeOf(Corpus.TestFn fn) {
        List<String> wildcards = new ArrayList<>(fn.wildcardImports());
        int cut = fn.fqn().lastIndexOf("::");
        if (cut > 0) {
            wildcards.add(fn.fqn().substring(0, cut));
        }
        return new com.legend.model.ImportScope(wildcards, fn.imports());
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
