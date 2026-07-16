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

    private final List<String> walls = new ArrayList<>();
    /** Shared-file table DDL — replayed FIRST, before ANY data. */
    private final List<String> ddlSeeds;
    /** Shared-file executeInDb data literals — replayed after ALL DDL. */
    /**
     * FQNs of every zero-arg function defined in the shared seed files:
     * their body literals already ride the shared replay, so BeforePackage
     * expansion must not run them a SECOND time. Run-once emulation at
     * FUNCTION granularity — the old statement-level dedup also destroyed
     * deliberately repeated inserts feeding distinct() tests (audit A1) and
     * silently swallowed post-REPLACE refills (audit A2).
     */
    /** {@code <<test.BeforePackage>>} setups collected corpus-wide. */
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
        // shared files join the setup UNIVERSE too (relationalSetUp.pure
        // defines createTablesAndFillDb — cross-family setups call it)
        setupUniverse.addAll(sharedSources);
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
        // PHASE E: shared-file DATA seeds by EXECUTING the shared files'
        // functions through the platform, in definition order — the same
        // statement sequence the literal extraction produced. Functions
        // WITH parameters cannot be called, but their constant literals
        // replayed under the legacy extraction — the platform path's
        // gates give the identical silent-skip for the parameter-dependent
        // ones.
        for (String src : seedSources) {
            collectSetups(src);
            com.legend.model.ParsedModel unit;
            try {
                unit = com.legend.parser.ElementParser.parse(src);
            } catch (RuntimeException e) {
                continue;
            }
            for (com.legend.model.PackageableElement el : unit.elements()) {
                if (el instanceof com.legend.model.FunctionDefinition f) {
                    sharedSetupUnits.add(new Object[]{f.qualifiedName(),
                            f.body(), f.parameters().isEmpty()});
                }
            }
        }
    }

    /** Shared-file functions, definition order — {fqn, body AST, zeroArg};
     * executed for EVERY test (each test gets a fresh in-memory db). */
    private final List<Object[]> sharedSetupUnits = new ArrayList<>();


    // Phase D: setup functions as PARSED definitions — their bodies
    // EXECUTE through the platform (Compiler statement orchestration), no literal
    // extraction. beforePackagesParsed: {pkg, fqn} by STEREOTYPE.
    private final Map<String, java.util.List<com.legend.model.spec.ValueSpecification>>
            setupFnAsts = new LinkedHashMap<>();
    private final List<String[]> beforePackagesParsed = new ArrayList<>();
    private final java.util.Set<String> bpSeen = new java.util.HashSet<>();

    public void addBeforePackages(String source) {
        collectSetups(source);
        setupUniverse.add(source);
    }

    /** Every scanned source, for the cross-family setup FALLBACK module. */
    private final java.util.LinkedHashSet<String> setupUniverse =
            new java.util.LinkedHashSet<>();
    private com.legend.compiler.element.ModelContext setupUniverseCtx;
    private int setupUniverseSize = -1;

    /** The whole-corpus TOLERANT module: setup functions reach across
     * family files (projection::setUp calls join's createTablesAndFillDb)
     * — the per-test module deliberately excludes foreign families, so a
     * setup that fails there retries here. Rebuilt only when new sources
     * arrived; duplicate FQNs are first-wins (module semantics). */
    private com.legend.compiler.element.ModelContext setupUniverseContext() {
        if (setupUniverseCtx == null || setupUniverseSize != setupUniverse.size()) {
            List<com.legend.Compiler.ModelSource> sources = new ArrayList<>();
            int i = 0;
            for (String src : setupUniverse) {
                // known parse walls (#50) stay out — one unparseable file
                // must not dark the whole setup universe
                try {
                    com.legend.parser.ElementParser.parse(src);
                } catch (RuntimeException unparseable) {
                    continue;
                }
                sources.add(new com.legend.Compiler.ModelSource(
                        "setup-" + (i++) + ".pure", src));
            }
            setupUniverseCtx = com.legend.Compiler.buildModule(
                    com.legend.Compiler.parseSources(sources).model()).context();
            setupUniverseSize = setupUniverse.size();
        }
        return setupUniverseCtx;
    }

    /** Parse a source and collect zero-arg function ASTs + BeforePackage
     * stereotyped functions (Phase D discovery — no regex). */
    private void collectSetups(String source) {
        com.legend.model.ParsedModel unit;
        try {
            unit = com.legend.parser.ElementParser.parse(source);
        } catch (RuntimeException e) {
            return;   // unparseable file: its tests are walled anyway
        }
        for (com.legend.model.PackageableElement el : unit.elements()) {
            if (!(el instanceof com.legend.model.FunctionDefinition f)
                    || !f.parameters().isEmpty()) {
                continue;
            }
            setupFnAsts.putIfAbsent(f.qualifiedName(), f.body());
            boolean isBp = f.stereotypes().stream().anyMatch(st ->
                    st.profileName().substring(st.profileName().lastIndexOf(':') + 1)
                            .equals("test")
                            && st.stereotypeName().equals("BeforePackage"));
            if (isBp && bpSeen.add(f.qualifiedName())) {
                String fqn = f.qualifiedName();
                int cut = fqn.lastIndexOf("::");
                beforePackagesParsed.add(new String[]{
                        cut > 0 ? fqn.substring(0, cut) : "", fqn});
            }
        }
    }

    private final Map<String, List<String>> fileSeeds = new LinkedHashMap<>();
    private String currentFileKey = "";
    private final Map<String, List<String>> familySeeds = new LinkedHashMap<>();
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
        if (familyRaw.containsKey(familyKey)) {
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
            collectSetups(src);
        }
        for (String src : modelOnlySources) {
            collectSetups(src);
        }
        List<String> sql = new ArrayList<>();
        if (parentFamilyKey != null) {
            sql.addAll(familySeeds.getOrDefault(parentFamilyKey, List.of()));
        }
        List<String> preSql = new ArrayList<>();
        for (String src : setupSources) {
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
        // sibling TEST files contribute their table DDL (empty CREATE OR
        // REPLACE — a sibling's classes are queryable only if their tables
        // exist; the current file's own DDL and the BeforePackage seeds run
        // AFTER and win), but NOT data seeds
        for (String src : modelOnlySources) {
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
        familySeeds.put(familyKey, sql);
    }

    /**
     * Register a test file's own model elements: mandatory elements append;
     * its mappings probe-compile against base+file (walls recorded). The
     * file's own table DDL and executeInDb literals seed too.
     */
    public void useFile(String key, String source) {
        currentFileKey = key;
        if (fileRaw.containsKey(key)) {
            return;
        }
        fileRaw.put(key, new com.legend.Compiler.ModelSource(key, source));
        collectSetups(source);
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

    // ===== per-test execution =====



    // ===== Phase C: test discovery + execution from the PARSED model =====

    /** One discovered {@code <<test.Test>>} function: the parsed
     * definition (body is AST), with its section's import scope. */
    public record ParsedTest(String fqn,
            com.legend.model.FunctionDefinition fn,
            com.legend.model.ImportScope imports) {}

    /**
     * Discover the runnable tests of one corpus source through the REAL
     * parser: {@code <<test.Test>>} stereotyped functions, minus
     * ToFix/Ignore (engine harness parity) and ExcludeAlloy (legend-lite
     * executes the in-process Alloy-shaped path).
     */
    /** Does this source declare ANY test-stereotyped function — including
     * ToFix/Ignore/ExcludeAlloy ones (an all-excluded file is still a TEST
     * file, never a family SETUP file)? The family/test-file split runs on
     * this, through the real parser. */
    public static boolean hasTestFunctions(String source) {
        com.legend.model.ParsedModel unit;
        try {
            unit = com.legend.parser.ElementParser.parse(source);
        } catch (RuntimeException e) {
            return false;   // unparseable file: walled at model-build time
        }
        for (com.legend.model.PackageableElement el : unit.elements()) {
            if (!(el instanceof com.legend.model.FunctionDefinition f)) {
                continue;
            }
            for (com.legend.model.StereotypeApplication st : f.stereotypes()) {
                String profile = st.profileName();
                if (!profile.substring(profile.lastIndexOf(':') + 1).equals("test")) {
                    continue;
                }
                switch (st.stereotypeName()) {
                    case "Test", "ToFix", "Ignore", "ExcludeAlloy" -> {
                        return true;
                    }
                    default -> { }
                }
            }
        }
        return false;
    }

    public static List<ParsedTest> discoverTests(String source) {
        List<ParsedTest> out = new ArrayList<>();
        com.legend.model.ParsedModel unit;
        try {
            unit = com.legend.parser.ElementParser.parse(source);
        } catch (RuntimeException e) {
            return out;   // unparseable file: walled at model-build time
        }
        for (com.legend.model.PackageableElement el : unit.elements()) {
            if (!(el instanceof com.legend.model.FunctionDefinition f)) {
                continue;
            }
            boolean isTest = false;
            boolean excluded = false;
            for (com.legend.model.StereotypeApplication st : f.stereotypes()) {
                String profile = st.profileName();
                String simpleProfile = profile.substring(profile.lastIndexOf(':') + 1);
                if (!simpleProfile.equals("test")) {
                    continue;
                }
                switch (st.stereotypeName()) {
                    case "Test" -> isTest = true;
                    case "ToFix", "Ignore", "ExcludeAlloy" -> excluded = true;
                    default -> { }
                }
            }
            if (isTest && !excluded) {
                out.add(new ParsedTest(f.qualifiedName(), f,
                        unit.elementImports().get(f.qualifiedName())));
            }
        }
        return out;
    }

    /** The test's import scope: its section's imports + its own package
     * (implicit in real pure). */
    private static com.legend.model.ImportScope importScopeOf(ParsedTest t) {
        List<String> wildcards = new ArrayList<>();
        Map<String, String> typeImports = new LinkedHashMap<>();
        if (t.imports() != null) {
            wildcards.addAll(t.imports().wildcards());
            typeImports.putAll(t.imports().typeImports());
        }
        int cut = t.fqn().lastIndexOf("::");
        if (cut > 0) {
            wildcards.add(t.fqn().substring(0, cut));
        }
        return new com.legend.model.ImportScope(wildcards, typeImports);
    }

    /** The MAPPING refs of execute()/-&gt;from() calls, AST-walked and
     * qualified via the test's imports — they feed the synthesized
     * Runtime's mappings and the no-execute SHAPE gate. */
    private List<String> executeMappingRefs(ParsedTest t) {
        List<String> out = new ArrayList<>();
        java.util.ArrayDeque<com.legend.model.spec.ValueSpecification> work =
                new java.util.ArrayDeque<>(t.fn().body());
        while (!work.isEmpty()) {
            com.legend.model.spec.ValueSpecification v = work.poll();
            if (v instanceof com.legend.model.spec.AppliedFunction af) {
                String simple = af.function()
                        .substring(af.function().lastIndexOf(':') + 1);
                boolean executeShape = simple.equals("execute")
                        && af.parameters().size() >= 2;
                boolean fromShape = simple.equals("from")
                        && af.parameters().size() >= 2;
                if (executeShape || fromShape) {
                    if (af.parameters().get(1)
                            instanceof com.legend.model.spec.PackageableElementPtr ptr) {
                        String ref = qualify(ptr.fullPath(), t);
                        if (ref.matches("[\\w:]+") && !out.contains(ref)) {
                            out.add(ref);
                        }
                    }
                }
                work.addAll(af.parameters());
            } else if (v instanceof com.legend.model.spec.AppliedProperty ap) {
                work.add(ap.receiver());
            } else if (v instanceof com.legend.model.spec.LambdaFunction lf) {
                work.addAll(lf.body());
            } else if (v instanceof com.legend.model.spec.PureCollection pc) {
                work.addAll(pc.values());
            } else if (v instanceof com.legend.model.spec.NewInstance ni) {
                ni.properties().values().forEach(ke -> work.add(ke.value()));
            }
        }
        return out;
    }

    /** Qualify a bare mapping reference via the test's imports + presence
     * in the raw model sources. */
    private String qualify(String name, ParsedTest t) {
        if (name.contains("::")) {
            return name;
        }
        if (t.imports() != null && t.imports().typeImports().containsKey(name)) {
            return t.imports().typeImports().get(name);
        }
        StringBuilder sb = new StringBuilder();
        sharedRaw.forEach(m -> sb.append(m.text()));
        familyRaw.getOrDefault(currentFamilyKey, List.of())
                .forEach(m -> sb.append(m.text()));
        com.legend.Compiler.ModelSource f = fileRaw.get(currentFileKey);
        if (f != null) {
            sb.append(f.text());
        }
        String scope = sb.toString();
        List<String> pkgs = new ArrayList<>();
        if (t.imports() != null) {
            pkgs.addAll(t.imports().wildcards());
        }
        int cut = t.fqn().lastIndexOf("::");
        if (cut > 0) {
            pkgs.add(t.fqn().substring(0, cut));
        }
        for (String pkg : pkgs) {
            String candidate = pkg + "::" + name;
            if (scope.contains(candidate)) {
                return candidate;
            }
        }
        return name;
    }

    /** Run one PARSED test through the pipeline. */
    public Outcome run(ParsedTest t) {
        List<String> mappingRefs = executeMappingRefs(t);
        if (mappingRefs.isEmpty()) {
            return new Outcome(t.fqn(), Status.SHAPE, "no execute(|...) call");
        }
        try {
            com.legend.compiler.element.ModelContext ctx =
                    moduleContextFor(mappingRefs);
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
                try (var st = conn.createStatement()) {
                    st.execute("SET TimeZone='UTC'");
                }
                List<String> failedSeeds = replaySeeds(t.fqn(), ctx, conn);
                seedFailures.addAll(failedSeeds);
                com.legend.TestBody.Outcome o = com.legend.TestBody.run(
                        ctx, t.fn().body(), importScopeOf(t), "rcorpus::Rt",
                        conn, !failedSeeds.isEmpty(), failedSeeds);
                // body-time setup failures (added via the sink DURING the
                // run) join the run-wide report too (audit 17)
                seedFailures.addAll(failedSeeds);
                return score(t.fqn(), o);
            }
        } catch (Exception e) {
            return new Outcome(t.fqn(), Status.ERROR,
                    String.valueOf(e.getMessage()).replace("\n", " | "));
        }
    }

    private static Outcome score(String fqn, com.legend.TestBody.Outcome o) {
        return switch (o) {
            case com.legend.TestBody.Outcome.Unsupported u ->
                    new Outcome(fqn, Status.SHAPE, u.reason());
            case com.legend.TestBody.Outcome.Ran r -> {
                if (!r.failures().isEmpty()) {
                    yield new Outcome(fqn, Status.FAIL, r.failures().get(0));
                }
                if (r.verified() == 0 && r.advisory() > 0) {
                    yield new Outcome(fqn, Status.SHAPE,
                            "sql-only: " + r.advisory()
                                    + " advisory golden-SQL assert(s),"
                                    + " no row verification");
                }
                if (r.verified() == 0) {
                    yield new Outcome(fqn, Status.SHAPE, "no verifying assertions");
                }
                yield new Outcome(fqn, Status.PASS, r.verified() + " assert(s)");
            }
        };
    }


    /**
     * MODULE-assembled context (Phase B): the shared + parent + family +
     * test-file RAW sources compile TOGETHER through the real parser —
     * per-file import sections, structured per-element walls, no text
     * extraction. The synthesized Runtime is one more source unit; its
     * connections come from the module's own parsed Database elements.
     */
    private com.legend.compiler.element.ModelContext moduleContextFor(
            List<String> mappingRefs) {
        // the runtime's mappings list matters: NESTED getAll resolution
        // routes through runtime->mappings when no explicit from() context
        // reaches it — the test's own execute() mapping refs go in
        String cacheKey = currentFamilyKey + "|" + currentFileKey + "|"
                + String.join(",", mappingRefs);
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
                        + "Runtime rcorpus::Rt { mappings: [ "
                        + String.join(", ", mappingRefs)
                        + " ]; connections: [ " + conns + " ] }\n"));
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

    /** Seed replay: ALL DDL first, then the harness-owned SETUP FUNCTIONS
     * — shared-file units (legacy dataSeeds position) and BeforePackage
     * fns — execute as ordinary pure CALLS through the platform (K-natives
     * arc S4: Compiler's statement orchestration + executeInDb dispatch).
     * Setups the TEST BODY calls run at their own statement position in
     * TestBody — no pre-replay, engine-exact ordering. */
    private List<String> replaySeeds(String fqn,
            com.legend.compiler.element.ModelContext ctx, Connection conn) {
        List<String> allSeeds = new ArrayList<>(ddlSeeds);
        allSeeds.addAll(familySeeds.getOrDefault(currentFamilyKey, List.of()));
        allSeeds.addAll(fileSeeds.getOrDefault(currentFileKey, List.of()));
        List<String> failedSeeds = new ArrayList<>();
        for (String sql : allSeeds) {
            for (String raw : splitStatements(sql)) {
                String stmt = Corpus.DIALECT.adaptRawSql(raw);
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
        // shared-file zero-arg units first (the legacy dataSeeds position),
        // then this test's BeforePackage fns — each a real call through the
        // platform; parameterized shared fns run when a body CALLS them
        java.util.Set<String> executed = new java.util.HashSet<>();
        for (Object[] unit : sharedSetupUnits) {
            String sharedFqn = (String) unit[0];
            if ((Boolean) unit[2] && isEffectfulSetup(sharedFqn)
                    && executed.add(sharedFqn)) {
                callSetup(sharedFqn, ctx, conn, failedSeeds);
            }
        }
        for (String[] bp : beforePackagesParsed) {
            if (fqn.startsWith(bp[0] + "::") && setupFnAsts.containsKey(bp[1])
                    && isEffectfulSetup(bp[1]) && executed.add(bp[1])) {
                callSetup(bp[1], ctx, conn, failedSeeds);
            }
        }
        return failedSeeds;
    }

    /** Can the per-test module resolve this setup and every QUALIFIED
     * function name its transitive body calls? (Bare names resolve via
     * imports and are assumed local; the observed cross-family reach is
     * FQN-shaped.) A miss means the setup must run in the universe module
     * — decided BEFORE execution, so nothing ever partially runs twice. */
    private boolean preflightResolvable(String setupFqn,
            com.legend.compiler.element.ModelContext ctx) {
        if (safeFindFunction(ctx, setupFqn).isEmpty()) {
            return false;
        }
        java.util.Set<String> seen = new java.util.HashSet<>();
        java.util.ArrayDeque<String> work = new java.util.ArrayDeque<>();
        work.add(setupFqn);
        while (!work.isEmpty()) {
            String fqn = work.poll();
            if (!seen.add(fqn)) {
                continue;
            }
            List<com.legend.model.spec.ValueSpecification> body =
                    setupFnAsts.get(fqn);
            if (body == null) {
                continue;
            }
            java.util.Set<String> called = new java.util.HashSet<>();
            for (com.legend.model.spec.ValueSpecification stmt : body) {
                collectCalledFqns(stmt, called);
            }
            for (String c : called) {
                if (!c.contains("::")) {
                    continue;
                }
                if (safeFindFunction(ctx, c).isEmpty()) {
                    return false;
                }
                work.add(c);
            }
        }
        return true;
    }

    private static List<com.legend.compiler.element.TypedFunction> safeFindFunction(
            com.legend.compiler.element.ModelContext ctx, String fqn) {
        try {
            return ctx.findFunction(fqn);
        } catch (RuntimeException brokenOverloads) {
            return List.of();
        }
    }

    /** QUALIFIED call names in a statement tree (bare names skipped). */
    private static void collectCalledFqns(
            com.legend.model.spec.ValueSpecification v, java.util.Set<String> out) {
        if (v instanceof com.legend.model.spec.AppliedFunction af) {
            if (af.function().contains("::")) {
                out.add(af.function());
            }
            af.parameters().forEach(x -> collectCalledFqns(x, out));
        } else if (v instanceof com.legend.model.spec.AppliedProperty ap) {
            collectCalledFqns(ap.receiver(), out);
        } else if (v instanceof com.legend.model.spec.LambdaFunction lf) {
            lf.body().forEach(x -> collectCalledFqns(x, out));
        } else if (v instanceof com.legend.model.spec.PureCollection pc) {
            pc.values().forEach(x -> collectCalledFqns(x, out));
        }
    }

    /** Does this setup function (transitively, over the parsed setup-fn
     * universe) reach a K-native seeding call? Shared files also define
     * plain HELPERS (testRuntime(), result-to-json utilities) — eagerly
     * calling those is meaningless and pollutes the failed-seed ledger. */
    private boolean isEffectfulSetup(String setupFqn) {
        return isEffectfulSetup(setupFqn, new java.util.HashSet<>());
    }

    private boolean isEffectfulSetup(String setupFqn, java.util.Set<String> seen) {
        List<com.legend.model.spec.ValueSpecification> body =
                setupFnAsts.get(setupFqn);
        if (body == null || !seen.add(setupFqn)) {
            return false;
        }
        java.util.Set<String> called = new java.util.HashSet<>();
        for (com.legend.model.spec.ValueSpecification stmt : body) {
            collectCalledNames(stmt, called);
        }
        if (called.contains("executeInDb") || called.contains("dropAndCreateTableInDb")
                || called.contains("dropAndCreateSchemaInDb")
                || called.contains("setupTestData")
                || called.contains("loadCsvToDbTable")) {
            return true;
        }
        for (String name : called) {
            for (String candidate : setupFnAsts.keySet()) {
                if ((candidate.endsWith("::" + name) || candidate.equals(name))
                        && isEffectfulSetup(candidate, seen)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void collectCalledNames(
            com.legend.model.spec.ValueSpecification v, java.util.Set<String> out) {
        if (v instanceof com.legend.model.spec.AppliedFunction af) {
            String fn = af.function();
            out.add(fn.contains("::") ? fn.substring(fn.lastIndexOf(':') + 1) : fn);
            af.parameters().forEach(x -> collectCalledNames(x, out));
        } else if (v instanceof com.legend.model.spec.AppliedProperty ap) {
            collectCalledNames(ap.receiver(), out);
        } else if (v instanceof com.legend.model.spec.LambdaFunction lf) {
            lf.body().forEach(x -> collectCalledNames(x, out));
        } else if (v instanceof com.legend.model.spec.PureCollection pc) {
            pc.values().forEach(x -> collectCalledNames(x, out));
        }
    }

    /** One zero-arg setup call through the full pipeline; failures feed the
     * failed-seed ledger (and the emptiness guard). */
    private void callSetup(String setupFqn,
            com.legend.compiler.element.ModelContext ctx, Connection conn,
            List<String> failedSeeds) {
        com.legend.model.spec.ValueSpecification call =
                com.legend.compiler.NameResolver.resolveQuery(
                        new com.legend.model.spec.AppliedFunction(
                                setupFqn, List.of()));
        // PREFLIGHT, not retry (audit 17): the universe path re-running a
        // PARTIALLY-executed setup doubles non-drop-guarded inserts, so the
        // module choice is made BEFORE anything executes — if the per-test
        // module cannot resolve the setup or any QUALIFIED call in its
        // transitive body, the whole run happens in the setup universe.
        com.legend.compiler.element.ModelContext target =
                preflightResolvable(setupFqn, ctx) ? ctx : setupUniverseContext();
        try {
            com.legend.Compiler.executeResolved(call, target, "rcorpus::Rt", conn,
                    failedSeeds::add);
        } catch (Exception e) {
            failedSeeds.add("setup " + setupFqn + "() => "
                    + String.valueOf(e.getMessage()).split("\n")[0]);
        }
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
    /** The model CREATE statements for {@code table} (simple-name match)
     * across the FAMILY, the current test FILE, and the shared DDL — a
     * dropAndCreateTableInDb may target a table declared in any of them. */
    private List<String> familyCreatesOf(String table) {
        List<String> out = new ArrayList<>();
        String simple = table.contains(".")
                ? table.substring(table.lastIndexOf('.') + 1) : table;
        List<String> searchSpace = new ArrayList<>(
                familySeeds.getOrDefault(currentFamilyKey, List.of()));
        searchSpace.addAll(fileSeeds.getOrDefault(currentFileKey, List.of()));
        searchSpace.addAll(ddlSeeds);
        for (String s : searchSpace) {
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




    // ===== assertion evaluation =====















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
