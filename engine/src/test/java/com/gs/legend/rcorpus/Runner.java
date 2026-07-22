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
                    sharedSetupUnits.add(new SetupUnit(
                            f.qualifiedName(), f.parameters().isEmpty()));
                }
            }
        }
    }

    /** A shared-file function, definition order — executed for EVERY test
     * when zero-arg and effectful (each test gets a fresh in-memory db). */
    private record SetupUnit(String fqn, boolean zeroArg) {
    }

    private final List<SetupUnit> sharedSetupUnits = new ArrayList<>();


    // Phase D: setup functions as PARSED definitions — their bodies
    // EXECUTE through the platform (Compiler statement orchestration), no literal
    // extraction. beforePackagesParsed: {pkg, fqn} by STEREOTYPE.
    /** Every parsed corpus function: parameter names + body + imports —
     * the statement-position β-expansion index (audit 19d B1). */
    record FnDef(List<String> params,
            List<com.legend.model.spec.ValueSpecification> body,
            com.legend.model.ImportScope imports) {
    }

    private final Map<String, FnDef> fnIndex = new LinkedHashMap<>();

    private final Map<String, java.util.List<com.legend.model.spec.ValueSpecification>>
            setupFnAsts = new LinkedHashMap<>();
    private final Map<String, com.legend.model.ImportScope> setupFnImports =
            new LinkedHashMap<>();
    private final List<String[]> beforePackagesParsed = new ArrayList<>();
    private final java.util.Set<String> bpSeen = new java.util.HashSet<>();

    public void addBeforePackages(String source) {
        addBeforePackages(source, null);
    }

    /** {@code familyKey}: the corpus family this source belongs to — the
     * cross-family module pull unit (a family's files close over each
     * other's models; one file alone does not). */
    public void addBeforePackages(String source, String familyKey) {
        collectSetups(source);
        setupUniverse.add(source);
        if (familyKey != null) {
            sourceFamily.putIfAbsent(source, familyKey);
            familySources.computeIfAbsent(familyKey,
                    k -> new ArrayList<>()).add(source);
        }
    }

    private final Map<String, String> sourceFamily = new LinkedHashMap<>();
    private final Map<String, List<String>> familySources = new LinkedHashMap<>();

    /** Every scanned source, for the cross-family setup FALLBACK module. */
    private final java.util.LinkedHashSet<String> setupUniverse =
            new java.util.LinkedHashSet<>();

    /** FQN -> defining SOURCE for every parsed corpus element: module
     * assembly pulls the defining file when a test references a mapping
     * outside its family (graphFetch trees over the embedded family's
     * model — audit-17 bucket cluster). */
    private final Map<String, String> elementSource = new LinkedHashMap<>();
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
            elementSource.putIfAbsent(el.qualifiedName(), source);
            if (!(el instanceof com.legend.model.FunctionDefinition f)) {
                continue;
            }
            // EVERY parsed function joins the expansion index: test bodies
            // that reach execute()/toSQLString() through a HELPER call were
            // invisible to discovery AND to TestBody (audit 19d B1 — the
            // 498-SHAPE cliff). Statement-position calls β-expand with the
            // callee's parameters bound as lets.
            fnIndex.putIfAbsent(f.qualifiedName(), new FnDef(
                    f.parameters().stream()
                            .map(com.legend.model.FunctionDefinition.ParameterDefinition::name)
                            .toList(),
                    f.body(),
                    unit.elementImports().get(f.qualifiedName())));
            if (!f.parameters().isEmpty()) {
                continue;
            }
            setupFnAsts.putIfAbsent(f.qualifiedName(), f.body());
            var scope = unit.elementImports().get(f.qualifiedName());
            if (scope != null) {
                setupFnImports.putIfAbsent(f.qualifiedName(), scope);
            }
            boolean isBp = f.stereotypes().stream().anyMatch(st ->
                    (st.profileName().equals("test")
                            || st.profileName().equals("meta::pure::profiles::test"))
                            && st.stereotypeName().equals("BeforePackage"));
            if (isBp && bpSeen.add(f.qualifiedName())) {
                String fqn = f.qualifiedName();
                int cut = fqn.lastIndexOf("::");
                beforePackagesParsed.add(new String[]{
                        cut > 0 ? fqn.substring(0, cut) : "", fqn});
            }
        }
    }

    private String currentFileKey = "";
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
    /** ONE stereotype classifier for the whole harness (audit 17: two
     * hand-kept switches drifted): how does the {@code test} profile mark
     * this function? */
    enum TestKind { TEST, EXCLUDED, NONE }

    static TestKind testKindOf(com.legend.model.FunctionDefinition f) {
        boolean isTest = false;
        boolean excluded = false;
        for (com.legend.model.StereotypeApplication st : f.stereotypes()) {
            String profile = st.profileName();
            // the real profile is meta::pure::profiles::test — bare or FQN
            if (!(profile.equals("test")
                    || profile.equals("meta::pure::profiles::test"))) {
                continue;
            }
            switch (st.stereotypeName()) {
                case "Test" -> isTest = true;
                case "ToFix", "Ignore", "ExcludeAlloy" -> excluded = true;
                default -> { }
            }
        }
        return excluded ? TestKind.EXCLUDED : isTest ? TestKind.TEST : TestKind.NONE;
    }

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
            if (el instanceof com.legend.model.FunctionDefinition f
                    && testKindOf(f) != TestKind.NONE) {
                return true;
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
            if (testKindOf(f) == TestKind.TEST) {
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
    /**
     * β-expand statement-position calls to INDEXED corpus helpers whose
     * bodies (transitively, at this level) carry an execute/toSQLString
     * shape: parameters bind as lets, the callee's statements splice.
     * Query-level user functions (executeInDb wrappers etc.) are NOT
     * expanded here — they inline in the PLATFORM (UserCallInliner); the
     * execute-shape guard keeps this to test orchestration.
     */
    private List<com.legend.model.spec.ValueSpecification> expandHelperCalls(
            List<com.legend.model.spec.ValueSpecification> stmts,
            ParsedTest t, int depth) {
        if (depth >= 3) {
            return stmts;
        }
        List<com.legend.model.spec.ValueSpecification> out = new ArrayList<>();
        for (com.legend.model.spec.ValueSpecification stmt : stmts) {
            FnDef callee = null;
            com.legend.model.spec.AppliedFunction call = null;
            if (stmt instanceof com.legend.model.spec.AppliedFunction af
                    && !af.function().equals("letFunction")) {
                String fqn = af.function().contains("::")
                        ? af.function() : qualify(af.function(), t);
                FnDef fd = fnIndex.get(fqn);
                if (fd != null && fd.params().size() == af.parameters().size()
                        && !fd.body().isEmpty()
                        && containsExecuteShape(fd.body())) {
                    callee = fd;
                    call = af;
                }
            }
            if (callee == null) {
                out.add(stmt);
                continue;
            }
            for (int i = 0; i < callee.params().size(); i++) {
                out.add(new com.legend.model.spec.AppliedFunction("letFunction",
                        List.of(new com.legend.model.spec.CString(
                                        callee.params().get(i)),
                                call.parameters().get(i))));
            }
            out.addAll(expandHelperCalls(callee.body(), t, depth + 1));
        }
        return out;
    }

    /** Any execute/toSQLString/from call anywhere in these statements. */
    private static boolean containsExecuteShape(
            List<com.legend.model.spec.ValueSpecification> stmts) {
        java.util.ArrayDeque<com.legend.model.spec.ValueSpecification> work =
                new java.util.ArrayDeque<>(stmts);
        while (!work.isEmpty()) {
            var v = work.poll();
            if (v instanceof com.legend.model.spec.AppliedFunction af) {
                String simple = af.function()
                        .substring(af.function().lastIndexOf(':') + 1);
                if (simple.equals("execute") || simple.equals("toSQLString")
                        || simple.equals("from")) {
                    return true;
                }
                work.addAll(af.parameters());
            } else if (v instanceof com.legend.model.spec.AppliedProperty ap) {
                work.add(ap.receiver());
            } else if (v instanceof com.legend.model.spec.LambdaFunction lf) {
                work.addAll(lf.body());
            } else if (v instanceof com.legend.model.spec.PureCollection pc) {
                work.addAll(pc.values());
            }
        }
        return false;
    }

    private List<String> executeMappingRefs(
            List<com.legend.model.spec.ValueSpecification> body, ParsedTest t) {
        List<String> out = new ArrayList<>();
        // LET-BOUND mapping refs (the corpus's dominant graphFetch idiom:
        // `let mapping = X; ... execute($q, $mapping, ...)`) resolve through
        // this binding table — TestBody substitutes them at run time, so
        // the DISCOVERY gate must see through them too (bucket analysis:
        // 133/150 graphFetch execute calls were walled by this alone).
        Map<String, com.legend.model.spec.ValueSpecification> lets =
                new LinkedHashMap<>();
        java.util.ArrayDeque<com.legend.model.spec.ValueSpecification> work =
                new java.util.ArrayDeque<>(body);
        while (!work.isEmpty()) {
            com.legend.model.spec.ValueSpecification v = work.poll();
            if (v instanceof com.legend.model.spec.AppliedFunction af) {
                if (af.function().equals("letFunction")
                        && af.parameters().size() == 2
                        && af.parameters().get(0)
                                instanceof com.legend.model.spec.CString ln) {
                    lets.put(ln.value(), af.parameters().get(1));
                }
                String simple = af.function()
                        .substring(af.function().lastIndexOf(':') + 1);
                boolean executeShape = (simple.equals("execute")
                                || simple.equals("toSQLString"))
                        && af.parameters().size() >= 2;
                boolean fromShape = simple.equals("from")
                        && af.parameters().size() >= 2;
                if (executeShape || fromShape) {
                    com.legend.model.spec.ValueSpecification arg =
                            af.parameters().get(1);
                    if (arg instanceof com.legend.model.spec.Variable var
                            && lets.containsKey(var.name())) {
                        arg = lets.get(var.name());
                    }
                    if (arg instanceof com.legend.model.spec.PackageableElementPtr ptr) {
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

    /** Qualify a bare mapping reference via the test's imports + the
     * PARSED element index — never substring search over source text
     * (audit 19d R2: qualify was the last text-scan name resolver; the
     * elementSource keys are the real parser's FQNs). */
    private String qualify(String name, ParsedTest t) {
        if (name.contains("::")) {
            return name;
        }
        if (t.imports() != null && t.imports().typeImports().containsKey(name)) {
            return t.imports().typeImports().get(name);
        }
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
            if (elementSource.containsKey(candidate)) {
                return candidate;
            }
        }
        return name;
    }

    /** Run one PARSED test through the pipeline. */
    public Outcome run(ParsedTest t) {
        // Statement-position HELPER calls β-expand (params bound as lets)
        // for TWO reasons, verified separable by experiment (audit 20
        // follow-up): (a) DISCOVERY — executeMappingRefs must see execute
        // calls inside helpers to assemble the right module; (b) ASSERT
        // VISIBILITY — a helper body carrying assertEquals is HARNESS
        // vocabulary the platform can never type (G: unknown function),
        // so TestBody must see the expanded statements to intercept them.
        // The original execute-visibility rationale (audit 19d B1) is
        // OBSOLETE since B2b made execute a platform native — the sweep
        // with raw bodies regressed ONLY the assert-in-helper shape.
        List<com.legend.model.spec.ValueSpecification> body =
                expandHelperCalls(t.fn().body(), t, 0);
        List<String> mappingRefs = executeMappingRefs(body, t);
        if (mappingRefs.isEmpty()) {
            return new Outcome(t.fqn(), Status.SHAPE, "no execute(|...) call");
        }
        // QUALIFIED function/element references in the body pull their
        // defining families too (execute(..., other::family::runtime(),
        // ...) — the engine compiles ONE module; per-family modules must
        // close over what the test names). Same rule as the mapping pull.
        java.util.Set<String> called = new java.util.LinkedHashSet<>();
        java.util.Set<String> elements = new java.util.LinkedHashSet<>();
        for (com.legend.model.spec.ValueSpecification stmt : body) {
            collectCalledFqns(stmt, called, elements);
        }
        List<String> moduleRefs = new ArrayList<>(mappingRefs);
        for (String ref : called) {
            if (ref.contains("::") && elementSource.containsKey(ref)) {
                moduleRefs.add(ref);
            }
        }
        for (String ref : elements) {
            if (ref.contains("::") && elementSource.containsKey(ref)) {
                moduleRefs.add(ref);
            }
        }
        try {
            com.legend.compiler.element.ModelContext ctx =
                    moduleContextFor(moduleRefs);
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
                try (var st = conn.createStatement()) {
                    st.execute("SET TimeZone='UTC'");
                }
                List<String> failedSeeds = replaySeeds(t.fqn(), ctx, conn);
                seedFailures.addAll(failedSeeds);
                com.legend.harness.TestBody.Outcome o = com.legend.harness.TestBody.run(
                        ctx, body, importScopeOf(t), "rcorpus::Rt",
                        conn, !failedSeeds.isEmpty(), failedSeeds);
                // body-time setup failures (added via the sink DURING the
                // run) join the run-wide report too (audit 17)
                seedFailures.addAll(failedSeeds);
                return score(t.fqn(), o);
            }
        } catch (Exception e) {
            if (System.getenv("LEGEND_LITE_STACKS") != null) {
                e.printStackTrace();
            }
            return new Outcome(t.fqn(), Status.ERROR,
                    String.valueOf(e.getMessage()).replace("\n", " | "));
        }
    }

    private static Outcome score(String fqn, com.legend.harness.TestBody.Outcome o) {
        return switch (o) {
            case com.legend.harness.TestBody.Outcome.Unsupported u ->
                    new Outcome(fqn, Status.SHAPE, u.reason());
            case com.legend.harness.TestBody.Outcome.Ran r -> {
                if (!r.failures().isEmpty()) {
                    yield new Outcome(fqn, Status.FAIL, r.failures().get(0));
                }
                if (r.verified() == 0 && r.advisory() > 0) {
                    yield new Outcome(fqn, Status.SHAPE,
                            "sql-only: " + r.advisory()
                                    + " advisory golden-SQL assert(s),"
                                    + " no row verification");
                }
                if (r.verified() == 0 && r.executed() > 0) {
                    // the engine's own test is assert-free: running its
                    // body through the pipeline to completion IS the whole
                    // contract (engine parity), not a hollow pass
                    yield new Outcome(fqn, Status.PASS, "0 asserts — "
                            + r.executed() + " statement(s) executed");
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
        // CROSS-FAMILY mapping refs: a test executing against another
        // family's mapping (graphFetch over the embedded family's model)
        // pulls that mapping's DEFINING file into the module — otherwise
        // bare class names in trees resolve to the SHARED model's
        // same-named classes and fail with misleading no-such-property
        // errors. The engine compiles the whole project; per-family
        // modules must at least close over what the test names.
        java.util.Set<String> present = new java.util.HashSet<>();
        for (com.legend.Compiler.ModelSource src : sources) {
            present.add(src.text());
        }
        for (String ref : mappingRefs) {
            String defining = elementSource.get(ref);
            if (defining == null) {
                continue;
            }
            // pull the defining file's whole FAMILY: a family's files
            // close over each other's models (the single file did not —
            // milestoned mappings reference sibling-file classes).
            // first-wins dedup keeps the test's OWN definitions
            // authoritative over same-FQN foreign ones.
            String fam = sourceFamily.get(defining);
            List<String> pull = fam != null
                    ? familySources.getOrDefault(fam, List.of(defining))
                    : List.of(defining);
            int i = 0;
            for (String src : pull) {
                if (present.add(src)) {
                    sources.add(new com.legend.Compiler.ModelSource(
                            "xfam-" + Integer.toHexString(src.hashCode())
                                    + "-" + (i++) + ".pure", src));
                }
            }
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
    private static List<String> moduleDdl(
            com.legend.compiler.element.ModelContext ctx) {
        List<String> out = new ArrayList<>();
        java.util.Set<String> seenTables = new java.util.HashSet<>();
        java.util.Set<String> seenSchemas = new java.util.HashSet<>();
        for (String fqn : ctx.elementFqns()) {
            var dbOpt = ctx.findDatabase(fqn);
            if (dbOpt.isEmpty()) {
                continue;
            }
            var db = dbOpt.get();
            for (var td : db.tables()) {
                if (seenTables.add(td.name().toLowerCase())) {
                    out.add(com.legend.exec.Ddl.createTable(td, null));
                }
            }
            for (var schema : db.schemas()) {
                boolean defaultSchema = schema.name().isEmpty()
                        || "default".equals(schema.name());
                if (!defaultSchema && seenSchemas.add(schema.name())) {
                    out.add("CREATE SCHEMA IF NOT EXISTS " + schema.name());
                }
                for (var td : schema.tables()) {
                    // default-schema tables share the FLAT key — the same
                    // physical table declared both ways must create ONCE
                    String key = defaultSchema ? td.name().toLowerCase()
                            : (schema.name() + "." + td.name()).toLowerCase();
                    if (seenTables.add(key)) {
                        out.add(com.legend.exec.Ddl.createTable(td,
                                defaultSchema ? null : schema.name()));
                    }
                }
            }
        }
        return out;
    }

    private List<String> replaySeeds(String fqn,
            com.legend.compiler.element.ModelContext ctx, Connection conn) {
        // MODULE-DERIVED DDL (audit 19d B6 / task #55): every table of
        // every database the test's module compiled, spelled by Ddl.java
        // from the PARSED store — the regex extraction (tableDefsAll/
        // seedColumnTypes/pickBySeed, the last surviving shadow parser)
        // is retired. Same-named tables dedup first-wins (module order),
        // the same arbitration the module's element dedup already applies.
        List<String> allSeeds = moduleDdl(ctx);
        List<String> failedSeeds = new ArrayList<>();
        for (String sql : allSeeds) {
            for (String raw : com.legend.sql.RawSql.splitStatements(sql)) {
                String stmt = com.legend.exec.RawSqlBoundary.h2ToDuckDb(raw);
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
        for (SetupUnit unit : sharedSetupUnits) {
            if (unit.zeroArg() && isEffectfulSetup(unit.fqn())
                    && executed.add(unit.fqn())) {
                callSetup(unit.fqn(), ctx, conn, failedSeeds);
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
            java.util.Set<String> elements = new java.util.HashSet<>();
            for (com.legend.model.spec.ValueSpecification stmt : body) {
                collectCalledFqns(stmt, called, elements);
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
            // qualified ELEMENT references (^ConnectionStore(element=
            // some::family::myDB)): the engine compiles the whole project,
            // so a setup naming a foreign family's store resolves there —
            // when the per-test module can't see it, the run belongs in
            // the setup universe (same rule as unresolvable calls)
            for (String p : elements) {
                if (!p.contains("::")) {
                    continue;
                }
                if (!ctx.isExecutionContextElement(p)
                        && ctx.findClass(p).isEmpty()
                        && ctx.findEnum(p).isEmpty()
                        && safeFindFunction(ctx, p).isEmpty()) {
                    return false;
                }
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

    /** QUALIFIED call names + element-pointer paths in a statement tree
     * (bare names skipped by the consumers). Walks constructor arguments
     * too — the propertyLevel setups name foreign stores inside
     * {@code ^ConnectionStore(element=...)}. */
    private static void collectCalledFqns(
            com.legend.model.spec.ValueSpecification v, java.util.Set<String> out,
            java.util.Set<String> elements) {
        if (v instanceof com.legend.model.spec.AppliedFunction af) {
            if (af.function().contains("::")) {
                out.add(af.function());
            }
            af.parameters().forEach(x -> collectCalledFqns(x, out, elements));
        } else if (v instanceof com.legend.model.spec.AppliedProperty ap) {
            collectCalledFqns(ap.receiver(), out, elements);
        } else if (v instanceof com.legend.model.spec.LambdaFunction lf) {
            lf.body().forEach(x -> collectCalledFqns(x, out, elements));
        } else if (v instanceof com.legend.model.spec.PureCollection pc) {
            pc.values().forEach(x -> collectCalledFqns(x, out, elements));
        } else if (v instanceof com.legend.model.spec.NewInstance ni) {
            ni.properties().values().forEach(ke ->
                    collectCalledFqns(ke.value(), out, elements));
        } else if (v instanceof com.legend.model.spec.NewInstanceCast nic) {
            collectCalledFqns(nic.src(), out, elements);
        } else if (v instanceof com.legend.model.spec.PackageableElementPtr ptr) {
            elements.add(ptr.fullPath());
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
            for (String candidate : resolveSetupName(setupFqn, name)) {
                if (isEffectfulSetup(candidate, seen)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** EXACT resolution of a called name against the setup registry: an
     * FQN matches itself; a bare name resolves through the caller's OWN
     * package and its section's import wildcards — never by suffix
     * (audit 17: endsWith matched the wrong family's setup both ways). */
    private List<String> resolveSetupName(String callerFqn, String name) {
        if (name.contains("::")) {
            return setupFnAsts.containsKey(name) ? List.of(name) : List.of();
        }
        List<String> out = new ArrayList<>();
        int cut = callerFqn.lastIndexOf("::");
        if (cut > 0) {
            String samePkg = callerFqn.substring(0, cut) + "::" + name;
            if (setupFnAsts.containsKey(samePkg)) {
                out.add(samePkg);
            }
        }
        com.legend.model.ImportScope scope = setupFnImports.get(callerFqn);
        if (scope != null) {
            for (String w : scope.wildcards()) {
                String candidate = w + "::" + name;
                if (setupFnAsts.containsKey(candidate) && !out.contains(candidate)) {
                    out.add(candidate);
                }
            }
        }
        return out;
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





    // ===== assertion evaluation =====















    // ===== pure literal parsing (expected values) =====













    // ===== text machinery =====



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
