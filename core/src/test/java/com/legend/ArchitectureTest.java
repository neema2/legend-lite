package com.legend;


import com.legend.protocol.TypeExpression;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * The wall, and other structural invariants for {@code core/}.
 *
 * <p>If any test in this class fails, you have pierced an invariant
 * documented in {@code core/README.md §Strong invariants}. Do not add
 * an exception. Fix the offending code. The wall is non-negotiable.
 *
 * <p>The {@link #coreModuleHasNoDependenciesOnEngine wall test} is the
 * single most important assertion in this codebase: it guarantees that
 * the Strangler Fig migration stays clean — {@code core/} cannot
 * accidentally inherit a bug, a quirk, or a coupling from
 * {@code engine/}.
 *
 * <p>Permitted external dependencies (anything NOT under
 * {@code com.gs.legend.*}):
 * <ul>
 *   <li>JDK ({@code java.*}, {@code javax.*})</li>
 *   <li>JDBC drivers ({@code org.duckdb.*}, {@code org.sqlite.*})</li>
 *   <li>JUnit 5 + ArchUnit (test scope only)</li>
 * </ul>
 */
@Tag("guardrail")
final class ArchitectureTest {

    /**
     * Imports exactly the production classes: the product's jars, as core/BUILD.bazel's {@code :product_jars} declares
     * them (every core library, //base, //json and {@code :duckdb_cells}; Bazel workplan P3-27). Nothing is found from
     * a class's code source or excluded by where it sits: this test tree and the test support on the classpath are
     * simply not in the list. All structural rules apply to production code; tests may use whatever helpers they need
     * without piercing the production import boundary. {@link #theImportIsEveryProductLibrary} pins that every
     * product library is in it.
     */
    private static final JavaClasses CORE_PROD_CLASSES = importJars("legend.product.jars");

    /** The classes of the jars a declared list names (java_jars): a jar is imported as a jar, not as a file path. */
    private static JavaClasses importJars(String property) {
        java.util.List<java.util.jar.JarFile> jars = new java.util.ArrayList<>();
        try {
            for (java.nio.file.Path jar : com.legend.testing.Runfile.listed(property)) {
                jars.add(new java.util.jar.JarFile(jar.toFile()));
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return new ClassFileImporter().importJars(jars);
    }

    /** One class of every product library (`tools/deps/core-layers.txt`, plus `:duckdb_load`), by library. */
    private static final java.util.Map<String, String> PRODUCT_LIBRARIES = java.util.Map.ofEntries(
            java.util.Map.entry("spi", "com.legend.spi.ElementSink"),
            java.util.Map.entry("cache", "com.legend.cache.ContentStore"),
            java.util.Map.entry("values", "com.legend.values.PureDateLiteral"),
            java.util.Map.entry("error", "com.legend.error.LegendCompileException"),
            java.util.Map.entry("lexer", "com.legend.lexer.Lexer"),
            java.util.Map.entry("sql", "com.legend.sql.SqlQuery"),
            java.util.Map.entry("protocol", "com.legend.protocol.Protocol"),
            java.util.Map.entry("model", "com.legend.model.ConnectionDefinition"),
            java.util.Map.entry("parser", "com.legend.parser.ElementParser"),
            java.util.Map.entry("door", "com.legend.door.ModelFromText"),
            java.util.Map.entry("builtin", "com.legend.builtin.Pure"),
            java.util.Map.entry("platform", "com.legend.platform.WalledBodies"),
            // D71, 2026-10-30: the judge layer, out of exec; the test's run since slice 5 (2026-11-09)
            java.util.Map.entry("testexec", "com.legend.testexec.TestRunner"),
            java.util.Map.entry("compiler_element_type", "com.legend.compiler.element.type.ExprType"),
            java.util.Map.entry("sql_dialect", "com.legend.sql.dialect.SqlDialect"),
            java.util.Map.entry("database", "com.legend.database.Databases"),
            java.util.Map.entry("compiler", "com.legend.compiler.NameResolver"),
            java.util.Map.entry("normalizer", "com.legend.normalizer.ModelNormalizer"),
            java.util.Map.entry("lineage", "com.legend.lineage.ColumnLineageRows"),
            java.util.Map.entry("validation", "com.legend.validation.ValidateDesugar"),
            java.util.Map.entry("lowering", "com.legend.lowering.Lowerer"),
            java.util.Map.entry("plan", "com.legend.plan.QueryPlan"),
            java.util.Map.entry("resolver", "com.legend.resolver.StoreResolver"),
            java.util.Map.entry("setup", "com.legend.setup.CsvSeed"),
            java.util.Map.entry("exec", "com.legend.exec.PlanRunner"),   // Executor until its deletion, 2026-10-27
            java.util.Map.entry("probe", "com.legend.probe.Shadow"),
            java.util.Map.entry("testdatagen", "com.legend.testdatagen.TestDataGenerationNatives"),
            java.util.Map.entry("execution_plan", "com.legend.executionplan.ExecutionPlan"),
            // 2026-11-08 (docs/PLAN_AS_DATA_2026_11_07.md, steps 2 and 3): a test body's plan as data, and the exec
            // package's program half, its own target
            java.util.Map.entry("test_plan", "com.legend.testexecutionplan.TestPlan"),
            java.util.Map.entry("exec_program", "com.legend.exec.ProgramRunner"),
            java.util.Map.entry("result", "com.legend.result.Batch"),
            java.util.Map.entry("planner", "com.legend.Compiler"),
            java.util.Map.entry("pure_v1", "com.legend.server.PureV1Api"),
            // the test-planning library (the test-is-the-product plan's M3c, R17 C7, 2026-10-31)
            java.util.Map.entry("testplan", "com.legend.testplan.TestDoor"),
            // the door (the driver until slice 5, 2026-11-09)
            java.util.Map.entry("execution", "com.legend.Execution"),
            java.util.Map.entry("ide", "com.legend.ide.ModelIndex"),
            java.util.Map.entry("test", "com.legend.test.PureTestRunner"),
            java.util.Map.entry("server_lib", "com.legend.server.ConnectionResolver"),
            // DuckDB's cell types (:duckdb_load, with its Appender, until A2, 2026-11-10)
            java.util.Map.entry("duckdb_cells", "com.legend.exec.DuckDbCells"),
            // MongoDB's connector, beside the drivers (2026-10-17, docs/MONGODB_PLAN_2026_10_16.md D12)
            java.util.Map.entry("mongodb_connector", "com.legend.exec.MongoDbConnector"),
            // Postgres's JSON cells, beside its driver (the plan path program's step 9, 2026-10-19): on the product
            // jars since 2026-10-30 (D69, the audit's finding: a registered JDBC class no rule here had judged)
            java.util.Map.entry("postgres_cells", "com.legend.exec.PostgresCells"));

    /**
     * Every rule here judges {@link #CORE_PROD_CLASSES}; a product library missing from it is a library
     * no rule sees (2026-10-03: {@code :duckdb_load} was). So the import must hold a class of every
     * product library, and nothing of the test support.
     */
    @Test
    void theImportIsEveryProductLibrary() {
        java.util.List<String> missing = PRODUCT_LIBRARIES.entrySet().stream()
                .filter(e -> !CORE_PROD_CLASSES.contain(e.getValue()))
                .map(e -> e.getKey() + " (" + e.getValue() + ")").sorted().toList();
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of(), missing,
                "product libraries no ArchitectureTest rule sees — put them on //core:guardrails's classpath");
        java.util.List<String> support = CORE_PROD_CLASSES.stream().map(com.tngtech.archunit.core.domain.JavaClass::getName)
                .filter(n -> n.startsWith("com.legend.testing.") || n.startsWith("com.legend.tools.")).sorted().toList();
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of(), support, "test support judged as product");
    }


    /**
     * <strong>Invariant 1 — The wall.</strong> Nothing under
     * {@code com.legend.*} may depend on anything under
     * {@code com.gs.legend.*}.
     */
    @Test
    void coreModuleHasNoDependenciesOnEngine() {
        noClasses()
            .that().resideInAPackage("com.legend..")
            .should().dependOnClassesThat().resideInAPackage("com.gs.legend..")
            .as("THE WALL: no class in com.legend.* may import com.gs.legend.* — "
              + "reimplement what you need inside core/. See core/README.md.")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>Invariant 2 — No {@code util/} package.</strong> Helpers
     * live with the code that needs them; a {@code util/} package is a
     * landfill in disguise. Matches any package whose path contains a
     * {@code util} or {@code utils} segment.
     */
    @Test
    void coreModuleHasNoUtilPackage() {
        noClasses()
            .should().resideInAnyPackage("..util..", "..utils..")
            .as("Invariant 2: no util/ package — helpers live with the code that needs them.")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>The legend-sql wall (LEGEND_SQL_VISION.md).</strong> The SQL
     * layer is built to stand alone: it must not import the Pure compiler.
     * Frontend types meet SQL types only at the lowering boundary
     * ({@code com.legend.lowering.PureSql}).
     */
    @org.junit.jupiter.api.Test
    void sqlLayerIsStandalone() {
        noClasses()
            .that().resideInAPackage("com.legend.sql..")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "com.legend.compiler..", "com.legend.parser..",
                    "com.legend.normalizer..", "com.legend.builtin..")
            .as("legend-sql stands alone: no Pure-compiler imports in com.legend.sql..")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>Invariant 3 — Caches must be content-addressed.</strong> The only
     * sanctioned cache is {@code com.legend.cache.ContentStore}, keyed by a
     * {@link com.legend.cache.Hash} of content, which cannot desync. To keep a
     * name-/version-keyed cache (engine's {@code planCache} scar) from sneaking
     * in unreviewed, every {@code *Cache} / {@code *Store} type is funneled into
     * {@code com.legend.cache}. A reviewer there ensures it is content-addressed.
     *
     * <p>ArchUnit cannot prove the semantic &ldquo;no desync&rdquo; property;
     * the behavioral guarantee lives in {@code ContentStoreTest}. This rule is
     * the structural funnel that backs it.
     */
    /**
     * <strong>Invariant 4 — package layering is acyclic.</strong> Two cycles
     * (compiler&lt;-&gt;normalizer via a convenience overload; spec&lt;-&gt;spec.typed via
     * ExprType's location) shipped invisibly because one direction used
     * fully-qualified names no import-based review sees. This rule makes any
     * package cycle a test failure. The 2026-07 audit's fix; do not exclude
     * packages from it.
     */
    @Test
    void packageDependenciesAreAcyclic() {
        // Slices group by TOP segment: parser.* is one deliberate layer (its
        // parent<->child mutuality is sanctioned, audit §1e); everything else
        // must be acyclic ACROSS top-level packages.
        com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices()
            .matching("com.legend.(*)..")
            .should().beFreeOfCycles()
            .as("Invariant 4: no cycles across com.legend top-level packages — AUDIT_2026_07.md")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>Invariant 4c — the root package is the TOP layer.</strong>
     * The acyclic-slices matcher {@code com.legend.(*)..} skips root
     * classes, so nothing structural prevented a phase from importing the
     * driver (Compiler/BodyDriver) — audit 19's blind spot. Phases
     * never call up into orchestration. (The harness bridge left
     * production in F1.2 — no exemption remains.)
     */
    @Test
    void phasesNeverDependOnTheDriverLayer() {
        noClasses()
            .that().resideOutsideOfPackage("com.legend")
            // the top layer is three packages since step C (D73, 2026-10-30): the root's doors, the plan machinery
            // (com.legend.planner) and the execution side's machinery (com.legend.driver)
            .and().resideOutsideOfPackages("com.legend.testplan", "com.legend.planner")
            // the server shell is a driver CONSUMER (HTTP/LSP/diagram on top
            // of Compiler) — a top-layer sibling, not a phase
            .and().resideOutsideOfPackage("com.legend.server..")
            // the product's test runner (batch 7a) is a driver CONSUMER too:
            // it discovers tests in a model and runs them through Compiler
            .and().resideOutsideOfPackage("com.legend.test..")
            .and().resideInAPackage("com.legend..")
            .should().dependOnClassesThat().belongToAnyOf(
                    com.legend.Compiler.class,
                    com.legend.testplan.BodyDriver.class)
            .as("Invariant 4c: the root's doors, the planner and the test's planner are the top"
                    + " layer — audit 19; step C, D73; slice 5 (the driver deleted, 2026-11-09)")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>Invariant 4d — the golden-text renderer is quarantined.</strong>
     * EngineStyleH2 exists ONLY for the toSQLString golden surface; an
     * execution path reaching it (dialectOf returning it, a lowering
     * import) would run engine-H2 TEXT semantics against DuckDB. Only the
     * root layer (the harness bridge) may construct it.
     */
    @Test
    void engineStyleRendererIsQuarantinedToTheRootLayer() {
        noClasses()
            // the root layer's execution side was com.legend.driver from step C (D73, 2026-10-30) to slice 5
            // (2026-11-09: deleted); its planning is the test's planner's now, which holds no engine-style renderer
            .that().resideOutsideOfPackage("com.legend.testplan")
            .and().resideInAPackage("com.legend..")
            // 2026-10-31 (the seam program's S2g): the engine-text registry is planning work and left the driver for
            // the planner; it alone constructs the renderers, so it alone of the planner is exempt (a plan the runner
            // executes is never written by it: PlanMaker and the body planner's lowering hold no engine-style class)
            .and().doNotBelongToAnyOf(com.legend.planner.EngineText.class)
            // the engine-style FAMILY (H2 + DB2 golden-text renderers)
            // may compose internally; the quarantine is against the
            // execution path, not against sibling dialects
            .and().doNotBelongToAnyOf(
                    com.legend.sql.dialect.EngineStyleDB2.class,
                    com.legend.sql.dialect.EngineStyleComposite.class)
            .should().dependOnClassesThat().belongToAnyOf(
                    com.legend.sql.dialect.EngineStyleH2.class,
                    com.legend.sql.dialect.EngineStyleDB2.class,
                    com.legend.sql.dialect.EngineStyleComposite.class)
            .as("Invariant 4d: engine-style golden-text renderers are root"
                    + " layer only — audit 19")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * Invariant 4d for the engine-text renderers' interface (the dialect program's Phase 3, the PR audit of
     * 2026-10-16): {@code EngineTextDialect} is a {@code SqlDialect}, so the concrete-class rule above would let an
     * execution path hold one through it. Root layer only, and the test-data generator, which prints its plan in the
     * engine's text (TestDataGenerator.planText) while its session's own dialect runs every statement.
     */
    @Test
    void engineTextDialectIsQuarantinedToTheRootLayerAndThePlanPrinter() {
        noClasses()
            // com.legend.driver since step C (D73, 2026-10-30): the root layer's execution side; the root's own
            // exemption left the same day (no door holds the interface; the D73 audit)
            .that().resideOutsideOfPackages("com.legend.testplan", "com.legend.sql.dialect", "com.legend.testdatagen")
            .and().resideInAPackage("com.legend..")
            // 2026-10-31 (the seam program's S2g): the engine's plan text is planning work (D30: plan-time values) and
            // left the driver for the planner -- the registry, the plan printer, the envelope and the plan-node
            // factory, by name; no other planner class (PlanMaker, the body planner's lowering) holds the interface
            .and().doNotBelongToAnyOf(com.legend.planner.EngineText.class, com.legend.planner.EnginePlans.class,
                    com.legend.planner.PlanEnvelope.class, com.legend.planner.PlanAllocations.class)
            .should().dependOnClassesThat().belongToAnyOf(com.legend.sql.dialect.EngineTextDialect.class)
            .as("Invariant 4d: the engine-text dialect is the root layer's, and the generator's plan printer's")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * Sub-rule of Invariant 4 the top-level slices can't see: the typed HIR
     * package must not reach back into the checker package (the old
     * spec&lt;-&gt;spec.typed cycle existed solely because ExprType lived in spec).
     */
    @Test
    void typedHirDoesNotDependOnCheckers() {
        noClasses()
            .that().resideInAPackage("com.legend.compiler.spec.typed")
            .should().dependOnClassesThat().resideInAPackage("com.legend.compiler.spec")
            .as("Invariant 4b: compiler.spec.typed is below compiler.spec — AUDIT_2026_07.md")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>Invariant 5 — the lowering consumes the TYPED HIR, not the
     * parser AST.</strong> Dispatch keys on {@code signatureKey()} strings and
     * date/time values live in {@code com.legend.values}; a parser import in
     * the lowering means types are being stapled onto syntax again
     * (AUDIT_2026_07 §1c).
     */
    @Test
    void loweringDoesNotTouchTheParserAst() {
        noClasses()
            .that().resideInAPackage("com.legend.lowering")
            .should().dependOnClassesThat().resideInAnyPackage("com.legend.parser..")
            .as("Invariant 5: lowering is parser-free — AUDIT_2026_07 §1c")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>Invariant 6 — the pipeline's actual layer walls</strong>
     * (audit 15: all measured true, now pinned).
     */
    /** The nullness vocabulary (com.legend.base.Nullable/NonNull) is values-tier:
     * every layer may carry the annotations without breaching its wall. */
    private static final com.tngtech.archunit.base.DescribedPredicate<
            com.tngtech.archunit.core.domain.JavaClass> NULLNESS_ANNOTATIONS =
            com.tngtech.archunit.core.domain.JavaClass.Predicates
                    .belongToAnyOf(com.legend.base.Nullable.class,
                            com.legend.base.NonNull.class);

    @Test
    void sqlLayerIsFullyStandalone() {
        // stronger than Invariant 3's blacklist: sql depends on NOTHING
        // in com.legend outside itself (measured true — keep it so)
        com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes()
            .that().resideInAPackage("com.legend.sql..")
            .should().onlyDependOnClassesThat(
                    com.tngtech.archunit.core.domain.JavaClass.Predicates
                            .resideInAnyPackage("com.legend.sql..", "java..")
                            .or(NULLNESS_ANNOTATIONS)
                            // the runtime wall (2026-11-05, the deep audit): the renderer's one writer refuses to
                            // render while a plan runs -- RunScope, JDK-only in base, the scope the runner enters
                            // (its Exemption type went with the one exemption, 2026-11-05: zero exemptions)
                            .or(com.tngtech.archunit.core.domain.JavaClass.Predicates.type(
                                    com.legend.base.RunScope.class)))
            .as("Invariant 6a: com.legend.sql depends only on itself and the JDK")
            .check(CORE_PROD_CLASSES);
    }

    @Test
    void typedHirIsParserFree() {
        noClasses()
            .that().resideInAPackage("com.legend.compiler.spec.typed")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "com.legend.parser..", "com.legend.lexer..",
                    "com.legend.normalizer..")
            .as("Invariant 6b: the typed HIR never references the frontend")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * THE PRIZE RULE (audit 15 slice E): model-element records moved from
     * parser.element to {@code com.legend.model}, so every
     * post-normalization phase is FULLY parser-free — the model arrives
     * via the ModelContext facade as {@code com.legend.model} records and
     * nothing after the normalizer can see grammar machinery. The
     * resolver additionally never sees the untyped AST (model.spec).
     */
    @Test
    void postNormalizationPhasesAreParserFree() {
        noClasses()
            .that().resideInAnyPackage(
                    "com.legend.resolver", "com.legend.lowering",
                    "com.legend.exec", "com.legend.compiler.spec.typed")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "com.legend.parser..", "com.legend.lexer..",
                    "com.legend.normalizer..", "com.legend.ide..")
            .as("Invariant 6c: post-normalization phases never see the parser"
              + " — the model lives in com.legend.model")
            .check(CORE_PROD_CLASSES);
    }

    @Test
    void resolverNeverSeesTheUntypedAst() {
        // Strengthened 2026-08-04 with the protocol move: the resolver sees no
        // parse product at all — not the value-spec AST, not TypeExpression,
        // not Multiplicity. Measured true before pinning.
        noClasses()
            .that().resideInAPackage("com.legend.resolver")
            .should().dependOnClassesThat().resideInAPackage("com.legend.protocol..")
            .as("Invariant 6c': the resolver consumes model RECORDS and the"
              + " typed HIR — never any parse product (com.legend.protocol)")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * The model is DATA: element records for the compiler. It sits below every
     * phase and may reach only the parse vocabulary ({@code protocol} — the
     * value-spec AST and type/multiplicity records are parse products and live
     * there) and the value vocabulary. Amended 2026-08-04: protocol became the
     * bottom layer (PARSER_DROP_IN_STATUS.md §2.3), so {@code model → protocol}
     * is the sanctioned direction; {@code protocol → model} is banned by 7b.
     */
    @Test
    void modelIsPureData() {
        com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes()
            .that().resideInAnyPackage("com.legend.model..")
            .should().onlyDependOnClassesThat(
                    com.tngtech.archunit.core.domain.JavaClass.Predicates
                            .resideInAnyPackage("com.legend.model..",
                                    "com.legend.protocol..",
                                    "com.legend.values",
                                    "com.legend.error", "java..")
                            .or(NULLNESS_ANNOTATIONS))
            .as("Invariant 6j: com.legend.model depends only on protocol/values/error"
              + " and the JDK — producers and consumers both sit above it")
            .check(CORE_PROD_CLASSES);
    }

    // ================================================================
    // Invariant 7 — the standalone drop-in (PARSER_DROP_IN_PLAN.md).
    // The extractable parser artifact is {lexer, parser, protocol,
    // values}: a future legend-parser jar must carry NOTHING else.
    // These three rules are the wall that keeps it extractable. They
    // are deliberately allowlists, not blacklists — a new dependency
    // is a failure until it is argued into the rule.
    // ================================================================

    /**
     * <strong>Invariant 7a — the lexer depends on the JDK. Full stop.</strong>
     * No model, no protocol, no values — a token stream is characters in,
     * offsets out. (The nullness annotations are compile-time vocabulary,
     * carried by every layer.)
     */
    @Test
    void lexerDependsOnNothingButTheJdk() {
        com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes()
            .that().resideInAPackage("com.legend.lexer..")
            .should().onlyDependOnClassesThat(
                    com.tngtech.archunit.core.domain.JavaClass.Predicates
                            .resideInAnyPackage("com.legend.lexer..", "java..")
                            .or(NULLNESS_ANNOTATIONS))
            .as("Invariant 7a: com.legend.lexer depends on the JDK and nothing else")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>Invariant 7b — protocol is the bottom parse-product layer.</strong>
     * It holds everything the parser produces (elements-for-the-wire, the
     * value-spec AST, TypeExpression, Multiplicity, SourceInfo, Realization)
     * and may reach only {@code com.legend.values} (date/time literal
     * vocabulary — itself JDK-only by 6g), {@code com.legend.json} (the JSON
     * escape table the emitter writes with; amended 2026-09-26 when JSON left
     * core for its own module //json, which depends on nothing but the null
     * annotations) and the JDK. In particular:
     * <b>no model, no parser, no lexer</b>. The wire-shape knowledge stays in
     * {@code ProtocolEmitter}; the model adapter lives on the model side
     * ({@code com.legend.model.FromProtocol}).
     */
    @Test
    void protocolIsTheBottomLayer() {
        com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes()
            .that().resideInAPackage("com.legend.protocol..")
            .should().onlyDependOnClassesThat(
                    com.tngtech.archunit.core.domain.JavaClass.Predicates
                            .resideInAnyPackage("com.legend.protocol..",
                                    "com.legend.values", "com.legend.json", "java..")
                            .or(NULLNESS_ANNOTATIONS))
            .as("Invariant 7b: com.legend.protocol depends only on values, the JSON"
              + " module and the JDK — it is the bottom layer of the standalone drop-in")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>Invariant 7c — the parser's dependency surface is pinned.</strong>
     * Endgame ({@code PARSER_DROP_IN_PLAN.md} §2.1): the parser reads tokens
     * and produces protocol records — {@code lexer + protocol + values} and
     * nothing else. {@code model..} left this list 2026-10-21 (the protocol
     * program's leg 8, last step, docs/PROTOCOL_PROGRAM_2026_10_05.md): every
     * element kind is parsed into its record, and a text meets the compiler's
     * model only through {@code door.ModelFromText}, outside the parser.
     * <b>Shrink this list; never grow it.</b>
     */
    @Test
    void parserDependencySurfaceIsPinned() {
        com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes()
            .that().resideInAPackage("com.legend.parser..")
            .should().onlyDependOnClassesThat(
                    com.tngtech.archunit.core.domain.JavaClass.Predicates
                            .resideInAnyPackage("com.legend.parser..",
                                    "com.legend.lexer..",
                                    "com.legend.protocol..",
                                    "com.legend.values",
                                    "com.legend.error",     // ParseException extends the shared error vocabulary (a JDK-only leaf, 6g)
                                    "com.legend.spi",       // the OVERLAY seam (Phase M): SectionGrammarRegistry routes ### sections through it — a stable JDK-only contract, below the parser by design
                                    "java..")
                            .or(NULLNESS_ANNOTATIONS))
            .as("Invariant 7c: the parser consumes tokens and produces protocol"
              + " records — nothing above it, ever")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * THE OPAQUE CARRIER IS LOCKED. {@code OpaqueElementDefinition} exists for
     * ONE situation: a section owned by an overlay grammar in someone else's
     * jar, which core genuinely cannot open. It is not a hiding place for
     * built-in sections we have not modelled yet — used that way it turns a
     * loud parse failure into a silent hole, because an opaque element is
     * invisible to {@code findConnection}, to the resolver and to every
     * compiler phase. That happened once (###Data, and Measure and the
     * non-relational connections nearly followed), which is why the
     * construction sites are now pinned rather than merely discouraged.
     *
     * <p>{@link com.legend.parser.OverlayElementSink} is the only legitimate
     * producer. {@code ElementParser} WAS a named, temporary exemption for the
     * ###Data crutch; PARSER_COMPLETENESS_PLAN.md §3.1 landed 2026-09-16 —
     * data elements are {@code DataDefinition} now — and the whitelist is
     * empty, as the zero-regex gate's discipline demands.
     *
     * <p>KNOWN BLIND SPOT, verified by deliberately violating the rule: ArchUnit
     * sees constructor calls, field and return types, but NOT pattern-matching
     * switch cases ({@code case OpaqueElementDefinition oe ->} in NameResolver
     * does not trip it). That is acceptable because abuse requires CONSTRUCTING
     * a carrier, and construction is caught — but do not read this gate as
     * proving nothing merely READS the type.
     */
    @Test
    void opaqueCarrierIsLockedToTheOverlaySeam() {
        com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses()
            .that().resideOutsideOfPackages("com.legend.model..")
            .and().haveSimpleNameNotContaining("OverlayElementSink")
            .should().dependOnClassesThat()
                    .haveSimpleName("OpaqueElementDefinition")
            .as("Invariant 12: only the OVERLAY seam mints opaque elements —"
              + " a built-in section we cannot model is a LOUD failure, never"
              + " a blob (PARSER_COMPLETENESS_PLAN.md §2)")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * Invariant 12's record side (2026-10-10, the protocol program's leg 8: the parser's output is records only, so
     * the overlay seam mints a {@code Protocol.POpaqueElement} and the compiler's door carries it as the model's
     * opaque element): only {@code OverlayElementSink} constructs one; the protocol package names it in its exhaustive
     * switches (the emitter refuses it), the model's door converts it.
     */
    @Test
    void opaqueRecordIsLockedToTheOverlaySeam() {
        com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses()
            .that().resideOutsideOfPackages("com.legend.protocol..", "com.legend.model..")
            .and().haveSimpleNameNotContaining("OverlayElementSink")
            .should().dependOnClassesThat()
                    .haveSimpleName("POpaqueElement")
            .as("Invariant 12: only the OVERLAY seam mints opaque records")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>The driver has one caller (step C, D73, 2026-10-30; the router between them since the
     * test-is-the-product plan's M5, 2026-11-01, the M5 audit's S5).</strong> {@code BodyDriver} was package-private
     * by design while it shared the root package with the execution door; the driver's machinery is its own package
     * now ({@code com.legend.driver}). The execution door ({@code TestExecution.runProgram}) calls the body router
     * ({@code TestPlanner}), which runs a body of a moved class on the program road and walks every other with the
     * body driver: the walk is reached from the driver package alone, and the router from the door alone.
     */
    @Test
    void theDriverHasOneCaller() {
        // slice 5 (2026-11-09): the driver package is deleted; the body driver's planning environment is the test
        // planner's, its run the test's run's, each entry the door's alone
        noClasses()
            .that().resideOutsideOfPackage("com.legend.testplan")
            .should().dependOnClassesThat().haveNameMatching("com\\.legend\\.testplan\\.BodyDriver(\\$.*)?")
            .as("step C, M5: the body driver's planning environment is the test planner's alone")
            .check(CORE_PROD_CLASSES);
        noClasses()
            .that().resideOutsideOfPackage("com.legend.testplan")
            .and().doNotHaveFullyQualifiedName("com.legend.TestExecution")
            .should().dependOnClassesThat().haveNameMatching("com\\.legend\\.testplan\\.TestPlanner(\\$.*)?")
            .as("M5: the test planner is the test door's alone (TestExecution.planBody, planProgram)")
            .check(CORE_PROD_CLASSES);
        noClasses()
            .that().resideOutsideOfPackage("com.legend.testexec")
            .and().doNotHaveFullyQualifiedName("com.legend.TestExecution")
            .should().dependOnClassesThat().haveNameMatching("com\\.legend\\.testexec\\.TestRunner(\\$.*)?")
            .as("slice 5: the test's run is the test door's alone (TestExecution.runBody)")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>The planner is execution-free at the class level (step C, D73, 2026-10-30).</strong> The build says it
     * ({@code //core:planner} depends on neither {@code :driver} nor {@code :exec}); this rule says it where a reach by
     * name would show: the plan machinery ({@code com.legend.planner}) and the root's three plan-side doors
     * ({@code Compiler}, {@code TypedQuery}, {@code HeldModels}) depend on no class of the driver, the runtime or the
     * judge.
     */
    @Test
    void plannerIsExecutionFree() {
        noClasses()
            .that().resideInAPackage("com.legend.planner")
            .or().haveFullyQualifiedName("com.legend.Compiler")
            .or().haveFullyQualifiedName("com.legend.TypedQuery")
            .or().haveFullyQualifiedName("com.legend.HeldModels")
            .should().dependOnClassesThat().resideInAnyPackage("com.legend.testplan", "com.legend.exec..",
                    "com.legend.testexec")
            .as("step C: planning never touches the test's planner, the runtime or the test's run")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>The test-planning library is the driver's alone (the test-is-the-product plan's B4, brought forward to
     * M3 by R17 C7, 2026-10-31).</strong> The kinds only a test's program holds -- effects, seeds, the eager run,
     * asserts -- are planned by {@code com.legend.testplan} through the planner's extension point
     * ({@code planner.ProgramKinds}); the build keeps it off the plan side ({@code //core:plan_side} carries no
     * {@code :testplan}), and this rule says it at the class level: no class outside the library and the driver
     * depends on it -- not the planner, so the product's query door plans none of a test's kinds, and not the server.
     * The server carries the driver (it runs a test body for its IDE), so its binary holds the library; no server
     * class reaches it. The driver calls it from M5 (R16): until then its callers are its tests.
     */
    @Test
    void theTestPlanningLibraryIsTheDriversAlone() {
        noClasses()
            .that().resideOutsideOfPackages("com.legend.testplan", "com.legend")
            .should().dependOnClassesThat().resideInAPackage("com.legend.testplan")
            .as("B4, R17 C7: the test-planning library is the door's alone (the driver's until slice 5)")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>The program road's test entry has one caller (the test-is-the-product plan's M3c, 2026-10-31).</strong>
     * {@code PlanMaker.testProgram} plans a body whatever it holds, a test's kinds by the hook it is handed; the
     * product's query door ({@code PlanMaker.plan}) refuses those kinds first (QueryDoor). Only the test-planning
     * library's door calls it.
     */
    @Test
    void theProgramRoadsTestEntryIsTheTestDoors() {
        noClasses()
            .that().resideOutsideOfPackage("com.legend.testplan")
            .should().callMethodWhere(com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                    com.tngtech.archunit.core.domain.AccessTarget.Predicates.declaredIn(
                            com.legend.planner.PlanMaker.class))
                    .and(com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                            com.tngtech.archunit.core.domain.properties.HasName.Predicates.name("testProgram"))))
            .as("B12 item 1: the program road's test entry is the test door's")
            .check(CORE_PROD_CLASSES);
    }

    @Test
    void execIsABackend() {
        noClasses()
            .that().resideInAPackage("com.legend.exec")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "com.legend.parser..", "com.legend.lexer..",
                    "com.legend.normalizer..", "com.legend.resolver..",
                    "com.legend.lowering..", "com.legend.builtin..")
            .as("Invariant 6d: exec consumes SQL + result shapes, never the"
              + " frontend or middle-end")
            .check(CORE_PROD_CLASSES);
    }

    /** Only the root driver may drive the back half of the pipeline. */
    @Test
    void stageFunnelOnlyTheDriverDrivesResolverLoweringExec() {
        noClasses()
            .that().resideInAnyPackage(
                    "com.legend.parser..", "com.legend.normalizer..",
                    "com.legend.compiler..", "com.legend.builtin..",
                    "com.legend.ide..", "com.legend.sql..")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "com.legend.resolver..", "com.legend.lowering..",
                    "com.legend.exec..")
            .as("Invariant 6e: resolver/lowering/exec are driven only by the"
              + " root driver — no phase reaches forward into them")
            .check(CORE_PROD_CLASSES);
    }

    @Test
    void lexerIsPrivateToTheParser() {
        noClasses()
            .that().resideOutsideOfPackages(
                    "com.legend.lexer..", "com.legend.parser..",
                    "com.legend.ide..")
            .should().dependOnClassesThat().resideInAPackage("com.legend.lexer..")
            .as("Invariant 6f: only parser + ide read the token stream")
            .check(CORE_PROD_CLASSES);
    }

    @Test
    void leavesStayLeaves() {
        com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes()
            .that().resideInAnyPackage(
                    "com.legend.values", "com.legend.error", "com.legend.cache")
            .should().onlyDependOnClassesThat(
                    com.tngtech.archunit.core.domain.JavaClass.Predicates
                            .resideInAnyPackage("com.legend.values",
                                    "com.legend.error", "com.legend.cache",
                                    "java..")
                            .or(NULLNESS_ANNOTATIONS))
            .as("Invariant 6g: values/error/cache import nothing from the pipeline")
            .check(CORE_PROD_CLASSES);
    }

    /** THE UPSTREAM BOUNDARY (batch 7c, 2026-09-11): no class in core — main
     *  OR test — depends on the engine's or pure's Java. The Maven enforcer
     *  bans the artifacts; this bans the imports, so a class that arrives by
     *  any other road (a shaded jar, a transitive test dependency) is caught
     *  at the type level. Measured zero before the rule; the rule keeps it. */
    @Test
    void upstreamJavaNeverEntersCore() {
        // main AND tests: the production import already read, plus core's test jar alone, declared
        // (:core_test_jar; re-importing everything re-parsed every class)
        JavaClasses thisTestTree = importJars("legend.test.jars");
        var rule = noClasses()
            .that().resideInAPackage("com.legend..")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "org.finos.legend..")
            .as("the upstream boundary: core imports no org.finos.legend class"
                    + " (docs/UPSTREAM_BOUNDARY_PROGRAM.md workstream B)");
        rule.check(CORE_PROD_CLASSES);
        rule.check(thisTestTree);
    }

    /** Invariant 5 as an ALLOWLIST: lowering's whole dependency surface. */
    @Test
    void loweringDependencySurfaceIsPinned() {
        com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes()
            .that().resideInAPackage("com.legend.lowering")
            .should().onlyDependOnClassesThat(
                    com.tngtech.archunit.core.domain.JavaClass.Predicates
                            .resideInAnyPackage("com.legend.lowering",
                                    "com.legend.compiler.spec.typed",
                                    "com.legend.compiler.element",
                                    "com.legend.compiler.element.type",
                                    "com.legend.builtin", "com.legend.sql..",
                                    "com.legend.values",
                                    // the declaration/implementation tables and
                                    // the registration vocabularies (untangle
                                    // step 4a): lowering registers INTO them
                                    "com.legend.platform",
                                    "com.legend.error", "java..")
                            // SourceInfo rides typed nodes (the span
                            // component, Phase 4) — the ONE protocol type
                            // the HIR's surface carries; the parse-product
                            // AST itself stays out
                            .or(com.tngtech.archunit.core.domain.JavaClass
                                    .Predicates.type(
                                            com.legend.protocol.SourceInfo.class))
                            // the declaration IDENTITY the typed call carries
                            // (execution plan step 2, 2026-09-26): every rule
                            // table is keyed by it; it lives beside the
                            // declaration type in model and is a value, not
                            // the parse-product AST
                            .or(com.tngtech.archunit.core.domain.JavaClass
                                    .Predicates.type(
                                            com.legend.model.FunctionId.class))
                            // the runtime wall (2026-11-05, the deep audit): lowering refuses to lower while a plan
                            // runs -- RunScope, JDK-only in base, the scope the runner enters
                            .or(com.tngtech.archunit.core.domain.JavaClass
                                    .Predicates.type(
                                            com.legend.base.RunScope.class))
                            // a rule table is a constant of the process once its class is initialised (2026-11-09,
                            // the user's "fix all of it", mutable static state): Sealed, JDK-only in base, the
                            // registry's seal -- staticStateIsConstantOrRegistered reads a sealed map as constant
                            .or(com.tngtech.archunit.core.domain.JavaClass
                                    .Predicates.belongToAnyOf(
                                            com.legend.base.Sealed.class))
                            // the one measurement label a census line carries (2026-11-09: the stamp census's own
                            // label folded into base's, Attribution, JDK-only), never read by a decision
                            .or(com.tngtech.archunit.core.domain.JavaClass
                                    .Predicates.type(
                                            com.legend.base.Attribution.class))
                            .or(NULLNESS_ANNOTATIONS))
            .as("Invariant 6h: lowering consumes typed HIR + kernel + sql — "
              + "nothing else, ever (plus the SourceInfo span component and the declaration identity)")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>F1.3 — the {@code java.sql} funnel (Charter C1/C2
     * boundary).</strong> "Java orchestrates, the DATABASE executes"
     * becomes MECHANICAL: only the chartered JDBC classes may touch JDBC
     * or a driver. Every other allowlist in this file admits {@code java..}
     * (which includes {@code java.sql}) — this rule is the narrow pin
     * that made the tenet enforceable (docs/TENET_CHARTER.md, enforcement
     * map). The audit's proof it matters: round 1's worked example
     * (hashString over rs.getString) survived 691 commits because no
     * rule forbade it.
     *
     * <p>The chartered classes are the JDBC census register's rows ({@code JdbcSurfaceCensusTest.REGISTER}: each
     * file with the acts it may perform -- opens, checks, sends, loads, decodes, transacts, closes), ONE list for
     * the two guards (2026-10-30, the java.sql revamp's step 3, D69). Until then the funnel exempted four packages
     * whole ({@code exec}, {@code server..}, {@code test}, the root), so a class in them could reach a driver and
     * no rule turned red; now a class joins the exemption only by a dated row of the census, and the census
     * measures what the row lets it do. The limit, as F1.3b's javadoc stated before its deletion (D69): an
     * ArchUnit dependency from a call is on the callee's owner, so an unregistered class that only PASSES a
     * connection along was invisible here and to the signature rule below -- a hole D70 closed from the other side:
     * a session's connection is the seam's alone ({@code Sessions.Session.connection()} is package-private), so no
     * class outside {@code exec} has a connection to pass; "who handles a connection" is also
     * {@code theBodyDriverAndTheJudgesHoldNoJdbc}'s pin by name.
     */
    /** java.sql and every JDBC driver's own packages: a driver-native API rides with java.sql (F1.11). Every driver
     *  the product reaches since 2026-10-11 (the dialect program's Phase 0, 2026-10-13: the rules named DuckDB's and
     *  H2's only, blind to the nine drivers added since; docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md). */
    private static final String[] JDBC_AND_DRIVERS = {"java.sql..", "javax.sql..", "org.duckdb..", "org.h2..",
            "org.postgresql..", "org.sqlite..", "org.mariadb..", "com.mysql..", "com.clickhouse..", "io.trino..",
            "net.snowflake..", "com.databricks..", "com.simba..", "com.google.cloud.bigquery..",
            // MongoDB's driver (2026-10-17, docs/MONGODB_PLAN_2026_10_16.md D12): a client library, funnelled as a driver
            "com.mongodb..", "org.bson.."};

    /** A class the census registers (a nested class counted as its top-level class). */
    private static final com.tngtech.archunit.base.DescribedPredicate<com.tngtech.archunit.core.domain.JavaClass>
            CHARTERED_JDBC_CLASSES = new com.tngtech.archunit.base.DescribedPredicate<>(
                    "the chartered JDBC classes (the census register's rows, JdbcSurfaceCensusTest.REGISTER)") {
                private final java.util.Set<String> names = JdbcSurfaceCensusTest.registeredClasses();

                @Override
                public boolean test(com.tngtech.archunit.core.domain.JavaClass c) {
                    String n = c.getName();
                    int nested = n.indexOf('$');
                    return names.contains(nested < 0 ? n : n.substring(0, nested));
                }
            };

    @Test
    void javaSqlIsFunnelledToTheCharteredSeam() {
        noClasses()
            .that(com.tngtech.archunit.base.DescribedPredicate.not(CHARTERED_JDBC_CLASSES))
            .and().resideInAPackage("com.legend..")
            .should().dependOnClassesThat()
            // F1.11: driver-NATIVE APIs ride with java.sql — importing
            // org.duckdb/org.h2 types was a funnel bypass (the audit
            // framed the boundary as java.sql only)
            .resideInAnyPackage(JDBC_AND_DRIVERS)
            .as("F1.3: java.sql AND the driver-native APIs are funnelled to the chartered JDBC classes, the census"
                    + " register's rows (JdbcSurfaceCensusTest.REGISTER) — Charter clauses C1/C2")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>No JDBC type in a public signature outside the seam</strong> (2026-10-30, the java.sql revamp's step 3,
     * D69): a public class outside {@code com.legend.exec} declares no public or protected method, constructor or
     * field whose signature involves a {@code java.sql} or driver type -- the return, every parameter, the field,
     * every thrown type, and every type a generic argument or an array component names
     * ({@code getAllInvolvedRawTypes}: an {@code Optional<Connection>} or a {@code Connection[]} offers one too;
     * the D69 audit's finding). The seam's own public API may ({@code Sessions.owned} and {@code borrowed} take a
     * connection in; the openers return sessions since D70): it is where a connection changes hands. The server's
     * resolver and the test runners hold sessions (D68, D70). The funnel above covers the bytecode a class USES;
     * this covers what it OFFERS. Since D70 no registered JDBC class remains outside {@code exec}, so an
     * unregistered class offering a JDBC type depends on it and fails the funnel first; this rule stands for the
     * day one joins. The probe that proved the mechanism (the rule widened over {@code exec} names the seam's
     * public members) is recorded in the decisions log.
     */
    @Test
    void noJdbcTypeInAPublicSignatureOutsideTheSeam() {
        com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes()
            .that().arePublic()
            .and().resideInAPackage("com.legend..")
            .and().resideOutsideOfPackage("com.legend.exec")
            .should(new com.tngtech.archunit.lang.ArchCondition<com.tngtech.archunit.core.domain.JavaClass>(
                    "offer no JDBC or driver type in a public or protected signature") {
                @Override
                public void check(com.tngtech.archunit.core.domain.JavaClass c,
                        com.tngtech.archunit.lang.ConditionEvents events) {
                    for (com.tngtech.archunit.core.domain.JavaMethod m : c.getMethods()) {
                        if (exposed(m)) {
                            offered(events, m, m.getReturnType());
                            for (com.tngtech.archunit.core.domain.JavaType t : m.getParameterTypes()) {
                                offered(events, m, t);
                            }
                            for (com.tngtech.archunit.core.domain.ThrowsDeclaration<?> t : m.getThrowsClause()) {
                                offered(events, m, t.getRawType());
                            }
                        }
                    }
                    for (com.tngtech.archunit.core.domain.JavaConstructor k : c.getConstructors()) {
                        if (exposed(k)) {
                            for (com.tngtech.archunit.core.domain.JavaType t : k.getParameterTypes()) {
                                offered(events, k, t);
                            }
                            for (com.tngtech.archunit.core.domain.ThrowsDeclaration<?> t : k.getThrowsClause()) {
                                offered(events, k, t.getRawType());
                            }
                        }
                    }
                    for (com.tngtech.archunit.core.domain.JavaField f : c.getFields()) {
                        if (exposed(f)) {
                            offered(events, f, f.getType());
                        }
                    }
                }
            })
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>Only the seam makes sessions</strong> (2026-10-30, D70, the seam's step A): no product class outside
     * {@code com.legend.exec} is a {@code Sessions.Session}. The Java access rules already say so -- the class's
     * constructor and its one door to the driver's connection are package-private, so a subclass elsewhere cannot
     * compile -- and this rule says it where the other walls are read. Until D70 the server's resolver and the
     * test backends implemented sessions of their own over raw connections, and a session's state lived in a
     * process-wide map keyed by the connection because of it.
     */
    @Test
    void onlyTheSeamMakesSessions() {
        noClasses()
            .that().resideOutsideOfPackage("com.legend.exec")
            .and().resideInAPackage("com.legend..")
            .should().beAssignableTo(com.legend.exec.Sessions.Session.class)
            .as("D70: a session is made by com.legend.exec (Sessions.owned, borrowed, view, the openers) and nowhere"
                    + " else")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>The product borrows no connection</strong> (2026-10-30, D70, the audit's finding): a product session OWNS
     * its connection -- {@code Sessions.owned} is the openers' -- and no product class calls {@code Sessions.borrowed},
     * the test side's door ({@code TestSessions}). A connection wrapped twice would be two sessions with two memos,
     * and a declared seed (DROP IF EXISTS, CREATE) re-run on the second would silently drop what the first wrote;
     * this rule is why no refusal registry replaced the connection-keyed map.
     */
    @Test
    void theProductBorrowsNoConnection() {
        noClasses()
            .that().resideInAPackage("com.legend..")
            .should().callMethod(com.legend.exec.Sessions.class, "borrowed", java.sql.Connection.class)
            .as("D70: Sessions.borrowed is the test side's door; a product session owns its connection")
            .check(CORE_PROD_CLASSES);
    }

    /** Public or protected: what a class offers beyond its package. */
    private static boolean exposed(com.tngtech.archunit.core.domain.JavaMember m) {
        return m.getModifiers().contains(com.tngtech.archunit.core.domain.JavaModifier.PUBLIC)
                || m.getModifiers().contains(com.tngtech.archunit.core.domain.JavaModifier.PROTECTED);
    }

    /** Whether {@code type} is java.sql's or a driver's ({@link #JDBC_AND_DRIVERS}). */
    private static boolean jdbc(com.tngtech.archunit.core.domain.JavaClass type) {
        String pkg = type.getPackageName();
        for (String p : JDBC_AND_DRIVERS) {
            String prefix = p.substring(0, p.length() - "..".length());
            if (pkg.equals(prefix) || pkg.startsWith(prefix + ".")) {
                return true;
            }
        }
        return false;
    }

    /** Every raw type {@code type} involves (itself, its generic arguments and bounds, an array's component). */
    private static void offered(com.tngtech.archunit.lang.ConditionEvents events,
            com.tngtech.archunit.core.domain.JavaMember m, com.tngtech.archunit.core.domain.JavaType type) {
        for (com.tngtech.archunit.core.domain.JavaClass involved : type.getAllInvolvedRawTypes()) {
            if (jdbc(involved)) {
                events.add(com.tngtech.archunit.lang.SimpleConditionEvent.violated(m, m.getFullName() + " offers "
                        + involved.getName() + " (in " + type.getName()
                        + ") in a public or protected signature outside the seam (com.legend.exec)"));
            }
        }
    }

    /**
     * <strong>E4 final burn — the INTERPRETER performs no JDBC.</strong>
     * The metamodel channel (PlanText, AggAwareActivities — MetamodelWalk
     * and MetamodelSteps were DELETED in batch 55a) evaluates MODEL
     * CONSTANTS only:
     * grid chains COMPILE to SQL at the exec seam
     * ({@code GridReads.tryLower} — its JDBC sites are the chartered
     * grid egress, pinned by the eval ledger and scheduled for deletion
     * by the relation-typed {@code fetchDb} leg). This rule is the
     * ratified adjudication's mechanical guard: a database VALUE can
     * never be produced inside the channel, because the channel cannot
     * reach a connection.
     */
    @Test
    void theInterpreterPerformsNoJdbc() {
        noClasses()
            .that().haveNameMatching(".*\\.(PlanText|AggAwareActivities)")
            .should().dependOnClassesThat()
            .resideInAnyPackage(JDBC_AND_DRIVERS)
            .as("E4: the metamodel channel evaluates model constants"
                    + " only — grid chains compile to SQL at the"
                    + " GridReads.tryLower exec seam")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>The writers are carriage only (2026-10-28, docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md R3; the start
     * page's S1.4 item 3, approved by the user 2026-10-28).</strong> A writer spells the typed value it was handed, by
     * the plan's column facts alone, so swapping one writer for another changes no value: the result package (the
     * batch, the batcher, the collector, the row decode, the cell texts, the three writers, a host's rows) reads the
     * plan's columns ({@code executionplan}), the typed carriers ({@code values}), JSON and the base, and no other
     * layer -- never the compiler, the lowering, the SQL tree or a dialect, the runner or a session ({@code exec}),
     * the resolver, the plan side's setup or the legacy plan, the server. True when written (its Bazel target's deps
     * say the same); the rule keeps it true. What a writer may DO with a value is the source scan's
     * ({@code WriterCarriageGuardTest}).
     */
    @Test
    void writersAreCarriageOnly() {
        noClasses()
            .that().resideInAPackage("com.legend.result..")
            .should().dependOnClassesThat(com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage(
                    "com.legend..")
                    .and(com.tngtech.archunit.base.DescribedPredicate.not(com.tngtech.archunit.core.domain.JavaClass
                            .Predicates.resideInAnyPackage("com.legend.result..", "com.legend.executionplan..",
                                    "com.legend.values..", "com.legend.json..", "com.legend.base..",
                                    "com.legend.error..",
                                    // the Arrow IPC framer (track A, 2026-10-29): bytes in a layout, no value decided
                                    "com.legend.arrowipc.."))))
            .as("the writers are carriage only (R3): com.legend.result reads the plan's column facts and the typed"
                    + " batch -- executionplan, values, json, base, error, the Arrow framer -- and no compiler,"
                    + " lowering, sql, exec, resolver, setup, plan or server class")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>The runner is model-free (2026-10-09, docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 3).</strong>
     * What runs is the plan's own statement, reviewed when the plan was made: the runner, its parameter checks, its
     * sessions and the bulk load read the plan records, a connection's definition and {@code java.sql}, never the
     * compiler, the resolver, the lowering, the SQL tree or a dialect, the plan side's setup, or the legacy plan. (Since
     * D72, 2026-10-30, no class of {@code exec} does: {@code execReachesNoPlanning}, the wall.) One call is
     * the SQL typing owner's, and only one (2026-10-09, step 3's audit, S3): a Float value is bound as its literal is
     * typed (the numeric charter's Rule 1, {@code SqlTyping.floatDecimal}, the one owner the dialects spell a Float
     * literal by), which only its value decides, at run time; the SQL layer may depend on nothing outside itself, so
     * the rule's one home is there.
     */
    @Test
    void theRunnerIsModelFree() {
        // RowDecode (2026-10-18, the plan path program's step 6): the runner's typed-row decode, by the plan's columns;
        // since 2026-10-27 (the result boundary program's phase 3) in the plan side's result package, where a host's
        // rows are decoded by the same rule, and held model-free there too
        // BulkLoads went with DuckDB's Appender (A2, 2026-11-10)
        String runner = "com\\.legend\\.(exec\\.(PlanRunner|PlanParameters|PlanSessions)|result\\.RowDecode)"
                + "(\\$.*)?";
        noClasses()
            .that().haveNameMatching(runner)
            .should().dependOnClassesThat(com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAnyPackage(
                    "com.legend.compiler..", "com.legend.resolver..", "com.legend.lowering..", "com.legend.sql..",
                    "com.legend.setup..", "com.legend.plan..", "com.legend.builtin..", "com.legend.parser..",
                    "com.legend.normalizer..")
                    .and(com.tngtech.archunit.base.DescribedPredicate.not(com.tngtech.archunit.core.domain.JavaClass
                            .Predicates.equivalentTo(com.legend.sql.SqlTyping.class))))
            .as("the plan runner reaches no compiler, lowering, SQL tree, dialect or plan side: it runs the plan's"
                    + " statement as the plan says")
            .check(CORE_PROD_CLASSES);
        noClasses()
            .that().haveNameMatching(runner)
            .should().callMethodWhere(com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                    com.tngtech.archunit.core.domain.AccessTarget.Predicates.declaredIn(com.legend.sql.SqlTyping.class))
                    .and(com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                            com.tngtech.archunit.base.DescribedPredicate.not(
                                    com.tngtech.archunit.core.domain.properties.HasName.Predicates.name("floatDecimal")))))
            .orShould().accessFieldWhere(com.tngtech.archunit.core.domain.JavaFieldAccess.Predicates.target(
                    com.tngtech.archunit.core.domain.properties.HasOwner.Predicates.With.owner(
                            com.tngtech.archunit.core.domain.JavaClass.Predicates.equivalentTo(
                                    com.legend.sql.SqlTyping.class))))
            .as("the plan runner's one use of the SQL typing owner is a Float's binding (Rule 1, floatDecimal)")
            .check(CORE_PROD_CLASSES);
    }

    /** The packages an exec class may reach inside {@code com.legend}: exactly {@code //core:exec}'s dependency list
     *  (core/BUILD.bazel; tools/deps/core-layers.txt pins it) and exec itself -- the records (the plan, the model, the
     *  SQL tree, the values, the result boundary) and the switchboards (cache, diagnostics, error) beside base and
     *  json. Everything else of {@code com.legend} is planning or the driver side: the compiler, the resolver,
     *  lowering, the dialects ({@code com.legend.sql.dialect} is NOT {@code com.legend.sql}), setup, the plan side's
     *  database facts, the platform, the parser, the normalizer, the root, the judge, the generator, the server. An
     *  allowlist, not a blacklist of planning packages (the D72 audit): the two jars built against the whole of core
     *  ({@code duckdb_load}, {@code postgres_cells}) hold exec classes this rule judges too, and a blacklist left the
     *  root and the judge open to them. The model left the list with exec's model dependency (2026-11-08, the
     *  plan-as-data design's step 7a: exec opens a session from the plan's own connection description; the slice 2
     *  audit closed the class-level half). */
    private static final java.util.Set<String> EXEC_MAY_REACH = java.util.Set.of("com.legend.exec", "com.legend.base",
            "com.legend.json", "com.legend.cache", "com.legend.diagnostics", "com.legend.error",
            "com.legend.executionplan", "com.legend.result", "com.legend.sql", "com.legend.values");

    /* EXEC_PLANNING_REFERENCES, the ratchet of exec's reaches into planning (2026-10-09 to 2026-10-30): sixteen rows at
     * its widest, five after D71 (the judges and the probes out), none after D72 (the seam-and-wall program's B2: the
     * judges' column and row to judge, the driver's decode facts to the connector, the session source and the system
     * database to the driver, the wire table's compiler type a name in the plan's vocabulary). Deleted with the map
     * empty: the rule below is the wall. */

    /** {@code c}'s top-level class's name: a nested class's is its top-level class's, an array's its element's. */
    private static String topLevel(com.tngtech.archunit.core.domain.JavaClass c) {
        String name = c.getBaseComponentType().getName();
        int nested = name.indexOf('$');
        return nested < 0 ? name : name.substring(0, nested);
    }

    /**
     * <strong>THE WALL: the runtime reaches no planning library (D72, 2026-10-30, the seam-and-wall program's B2;
     * a shrink-only ratchet from 2026-10-09, docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §8, until its map emptied).</strong>
     * {@code exec} is a plan and a session to typed rows: every type and spelling fact it reads is the plan's
     * ({@code executionplan}), every driver fact its connector's. No class of it depends on the compiler, the
     * resolver, lowering, a dialect, the plan side's setup or database facts, the platform, the parser or the
     * normalizer; the build says the same (`//core:exec`'s deps, pinned by tools/deps/core-layers.txt), this rule
     * says it at the class level, where a reach through a type name the build would not list still shows.
     */
    /**
     * EXEC'S PLUG-INS ARE EXEC'S OWN (the deep audit, 2026-11-05): {@code ServiceLoader} joins a provider at run time with
     * no build dependency, so a provider outside {@code com.legend.exec} would reach past {@link #execReachesNoPlanning}
     * unseen. Every provider the classpath registers for an exec plug-in interface (a database's connector, its driver's
     * cells, its bulk load) is a class of {@code com.legend.exec}, and the rule above therefore covers it.
     */
    /** The test plan's extension codecs are the edge's (the plan-as-data design's step 8a, 2.7; the slice 2 audit,
     *  2026-11-08): a provider of {@code ExtensionCodec} outside {@code com.legend.edge} would put another library's
     *  records in the test plan. */
    @Test
    void extensionCodecsAreTheEdges() throws Exception {
        java.util.List<String> outside = new java.util.ArrayList<>();
        int providers = 0;
        java.util.Enumeration<java.net.URL> files = ArchitectureTest.class.getClassLoader()
                .getResources("META-INF/services/" + com.legend.testexecutionplan.ExtensionCodec.class.getName());
        while (files.hasMoreElements()) {
            java.net.URL file = files.nextElement();
            try (var in = file.openStream()) {
                for (String line : new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
                    String provider = line.replaceAll("#.*", "").trim();
                    if (provider.isEmpty()) {
                        continue;
                    }
                    providers++;
                    if (!provider.startsWith("com.legend.edge.") || provider.substring(16).contains(".")) {
                        outside.add(provider + " (" + file + ")");
                    }
                }
            }
        }
        org.junit.jupiter.api.Assertions.assertTrue(providers > 0, "no extension codec on the classpath to check");
        org.junit.jupiter.api.Assertions.assertTrue(outside.isEmpty(),
                "an extension codec outside com.legend.edge (the test plan's extensions are the edge's): " + outside);
    }

    @Test
    void execPluginsAreExecClasses() throws Exception {
        java.util.List<String> outside = new java.util.ArrayList<>();
        int providers = 0;
        // the BulkLoad seam went with DuckDB's Appender (A2, 2026-11-10: the model's data is statements in the plan)
        for (Class<?> spi : java.util.List.of(com.legend.exec.DatabaseConnector.class,
                com.legend.exec.DriverCells.class)) {
            java.util.Enumeration<java.net.URL> files =
                    ArchitectureTest.class.getClassLoader().getResources("META-INF/services/" + spi.getName());
            while (files.hasMoreElements()) {
                java.net.URL file = files.nextElement();
                try (var in = file.openStream()) {
                    for (String line : new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
                        String provider = line.replaceAll("#.*", "").trim();
                        if (provider.isEmpty()) {
                            continue;
                        }
                        providers++;
                        if (!provider.startsWith("com.legend.exec.") || provider.substring(16).contains(".")) {
                            outside.add(spi.getSimpleName() + " <- " + provider + " (" + file + ")");
                        }
                    }
                }
            }
        }
        org.junit.jupiter.api.Assertions.assertTrue(providers > 0, "no exec plug-in provider on the classpath to check");
        org.junit.jupiter.api.Assertions.assertTrue(outside.isEmpty(),
                "an exec plug-in provider outside com.legend.exec (move it into exec): " + outside);
    }

    /**
     * <strong>THE PLAN/RUN SPLIT, HARD (the plan-as-data design's slice 5, docs/PLAN_AS_DATA_2026_11_07.md section 11,
     * 2026-11-09).</strong> Until slice 5 a report-mode test measured the crossings between the halves and pinned them,
     * shrink-only (278 at its last measure); the package split took them to zero, and these rules keep them there. R1:
     * the test's run ({@code com.legend.testexec}) reaches only the run's libraries and the plan's records -- an
     * allow-list, so a planning library added later is refused too.
     */
    @Test
    void theRunReachesNoPlanning() {
        com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes()
            .that().resideInAPackage("com.legend.testexec")
            .should().onlyDependOnClassesThat().resideInAnyPackage("java..", "com.legend.base..",
                    "com.legend.diagnostics", "com.legend.error", "com.legend.exec", "com.legend.executionplan",
                    "com.legend.json..", "com.legend.result..", "com.legend.sql", "com.legend.testexecutionplan",
                    "com.legend.testexec", "com.legend.values..")
            .as("R1, slice 5: the test's run reaches no planning library")
            .check(CORE_PROD_CLASSES);
    }

    /** R2 (slice 5, 2026-11-09): planning -- the test's planner, the product's planner, the generator's planning, the
     *  compiler, the middle end -- reaches no runtime class and not the door. */
    @Test
    void planningReachesNoRun() {
        noClasses()
            .that().resideInAnyPackage("com.legend.testplan..", "com.legend.planner..", "com.legend.testdatagen..",
                    "com.legend.compiler..", "com.legend.lowering..", "com.legend.resolver..")
            .should().dependOnClassesThat().resideInAnyPackage("com.legend.exec..", "com.legend.testexec..")
            .orShould().dependOnClassesThat().belongToAnyOf(com.legend.Execution.class, com.legend.TestExecution.class,
                    com.legend.ExecuteOptions.class)
            .as("R2, slice 5: planning reaches no runtime class and not the door")
            .check(CORE_PROD_CLASSES);
    }

    /** R3 (slice 5, 2026-11-09): only the test door ({@code com.legend.TestExecution}, its own library since the doors'
     *  split the same day) sees both halves -- no other class depends on both the test's planner and the test's run. */
    @Test
    void onlyTheDoorSeesBothHalves() {
        java.util.Set<String> both = new java.util.TreeSet<>();
        for (com.tngtech.archunit.core.domain.JavaClass c : CORE_PROD_CLASSES) {
            boolean plans = false;
            boolean runs = false;
            for (com.tngtech.archunit.core.domain.Dependency d : c.getDirectDependenciesFromSelf()) {
                String pkg = d.getTargetClass().getBaseComponentType().getPackageName();
                plans |= pkg.equals("com.legend.testplan");
                runs |= pkg.equals("com.legend.testexec");
            }
            if (plans && runs && !topLevel(c).equals("com.legend.TestExecution")) {
                both.add(c.getName());
            }
        }
        org.junit.jupiter.api.Assertions.assertTrue(both.isEmpty(), "R3: a class other than the door depends on both"
                + " the test's planner and the test's run: " + both);
    }

    /** The planning libraries (R4, below): the compiler, the planners, the middle end, the generator's planning. */
    private static final java.util.List<String> PLANNING_PACKAGES = java.util.List.of("com.legend.compiler",
            "com.legend.planner", "com.legend.testplan", "com.legend.lowering", "com.legend.resolver",
            "com.legend.normalizer", "com.legend.testdatagen",
            // the rest of the plan side's libraries (the full audit, 2026-11-09: a class that renders or fills SQL and
            // runs it was unseen): the dialects, the setup, the databases, the plan's text, the platform, the parser,
            // the lexer, the built-ins, the protocol, the lineage, the validation
            "com.legend.sql.dialect", "com.legend.setup", "com.legend.database", "com.legend.plan",
            "com.legend.platform", "com.legend.parser", "com.legend.lexer", "com.legend.builtin",
            "com.legend.protocol", "com.legend.lineage", "com.legend.validation");

    /** The running libraries (R4): the runtime and the test's run. */
    private static final java.util.List<String> RUNNING_PACKAGES = java.util.List.of("com.legend.exec",
            "com.legend.testexec");

    /** The root's plan-side doors, planning's public entries. */
    private static final java.util.Set<String> PLAN_SIDE_DOORS = java.util.Set.of("com.legend.Compiler",
            "com.legend.TypedQuery", "com.legend.HeldModels");

    /** The two doors (the user's word of 2026-11-09, "split the door"): the product's and the test body's. */
    private static final java.util.Set<String> DOORS = java.util.Set.of("com.legend.Execution",
            "com.legend.TestExecution");

    /** What a caller above the doors may name of planning (R4): its entries -- the plan-side doors -- and the types
     *  they hand out (the compiled model, a program's facts) and take (the test planner's options). */
    private static final java.util.Set<String> PLANNING_API = java.util.Set.of("com.legend.Compiler",
            "com.legend.TypedQuery", "com.legend.HeldModels", "com.legend.compiler.element.ModelContext",
            "com.legend.compiler.element.PureModelContext", "com.legend.planner.ProgramFacts",
            "com.legend.testplan.PlanningOptions", "com.legend.platform.Feature");

    /** Planning's INPUT a caller builds and hands the compiler (R4): the parsed program, Pure's protocol values (a test
     *  runner composes a test body's call; the full audit's widening of R4 to every planning library, 2026-11-09). */
    private static final String PLANNING_INPUT_PACKAGE = "com.legend.protocol.";

    /** What a caller above the doors may name of running (R4): a session and where one comes from (the resource the
     *  caller owns and hands a door), and the types a door's run takes from it (the referee; the answer's output is the
     *  result library's, a record type: 2026-11-09). */
    private static final java.util.Set<String> RUN_API = java.util.Set.of("com.legend.exec.Sessions",
            "com.legend.exec.SessionSource", "com.legend.testexec.SqlReplayReferee");

    /**
     * WHAT A RUN TAKES FROM ITS CALLER IS DATA (2026-11-09, the user's question "why do we have any callbacks, esp
     * dangerous ones that can break our tenet?"): every parameter of a run's entry ({@link #RUN_ENTRY_CLASSES}: the
     * product's run and the test's, which every door's run half calls) and every record component inside one, is a
     * value (a record, an enum, a final
     * class, a collection of them) or a SEALED interface whose variants exec interprets (where the run's sessions come
     * from, where its answer goes), or one of the named receivers and resources ({@link #CALLER_INTERFACES}): an
     * output-only receiver of the answer or the verdicts, the edge's referee, the host's connection. No function type
     * crosses: until this day the run called a caller's factory with the plan, and a caller's function to choose each
     * statement's session -- the caller's code deciding inside the run. (Planning's inputs are not a run's: the test
     * door's planning options carry a resource resolver, a function the planner calls for a CSV load's path -- D3,
     * which the user moved into the data-in-the-plan design session, 2026-11-09.)
     */
    @Test
    void aRunTakesDataSealedSourcesAndNamedReceiversAlone() {
        java.util.Set<String> violations = new java.util.TreeSet<>();
        for (Class<?> entry : RUN_ENTRY_CLASSES) {
            for (java.lang.reflect.Method m : entry.getDeclaredMethods()) {
                if (!java.lang.reflect.Modifier.isPublic(m.getModifiers()) || m.isSynthetic()) {
                    continue;
                }
                for (java.lang.reflect.Type t : m.getGenericParameterTypes()) {
                    takesData(t, entry.getSimpleName() + "#" + m.getName(), new java.util.HashSet<>(), violations);
                }
            }
        }
        org.junit.jupiter.api.Assertions.assertTrue(violations.isEmpty(),
                "a run takes code from its caller (make it data: a record, or a sealed set exec interprets):\n  "
                        + String.join("\n  ", violations));
    }

    /** The classes whose public methods are a run's entries: the product's run and the test's (each door's run call is
     *  one of them: {@code theDoorsOnlyCompose}). */
    private static final java.util.List<Class<?>> RUN_ENTRY_CLASSES = java.util.List.of(
            com.legend.exec.ProductRun.class, com.legend.testexec.TestRunner.class);

    /** The open types a run may take, each named for what it is: receivers that decide nothing the run does (the
     *  answer's rows, the verdicts: {@code theListenerIsOutputOnly}; the stream an answer is written to), the edge's
     *  extension point, referee and data marker (the compatibility mode's, G5: every {@code Extension} is a record,
     *  {@code PlanTypesAreDataTest}), the host's connection (a resource, as a data source is), and a session, which exec
     *  alone makes ({@code onlyTheSeamMakesSessions}). */
    private static final java.util.Set<Class<?>> CALLER_INTERFACES = java.util.Set.of(
            com.legend.result.ResultSink.class, com.legend.testexecutionplan.AssertListener.class,
            java.io.Writer.class, java.io.OutputStream.class,
            com.legend.testexecutionplan.Edge.class, com.legend.testexec.SqlReplayReferee.class,
            com.legend.testexecutionplan.Extension.class, com.legend.exec.Sessions.Opener.class,
            com.legend.exec.Sessions.Session.class);

    private static void takesData(java.lang.reflect.Type t, String where, java.util.Set<Class<?>> seen,
            java.util.Set<String> violations) {
        if (t instanceof java.lang.reflect.ParameterizedType p) {
            takesData(p.getRawType(), where, seen, violations);
            for (java.lang.reflect.Type a : p.getActualTypeArguments()) {
                takesData(a, where, seen, violations);
            }
            return;
        }
        if (t instanceof java.lang.reflect.WildcardType w) {
            for (java.lang.reflect.Type b : w.getUpperBounds()) {
                takesData(b, where, seen, violations);
            }
            return;
        }
        if (t instanceof java.lang.reflect.TypeVariable<?> v) {
            violations.add(where + " takes a type variable, " + v.getName() + " (name the type)");
            return;
        }
        if (!(t instanceof Class<?> c) || !seen.add(c)) {
            return;
        }
        if (c.isArray()) {
            takesData(c.getComponentType(), where, seen, violations);
            return;
        }
        // (a file's path is a value, though the JDK declares it an interface; Object is a bound parameter's value; the
        // JDK's numbers and times are values, though BigDecimal and BigInteger are not declared final)
        if (c.isPrimitive() || c.isEnum() || c == String.class || c == Object.class || c == java.nio.file.Path.class
                || c.getName().startsWith("java.math.") || c.getName().startsWith("java.time.")
                || CALLER_INTERFACES.contains(c)) {
            return;
        }
        if (c.getName().startsWith("java.util.function.")) {
            violations.add(where + " takes a function, " + c.getName());
            return;
        }
        if (c == java.util.List.class || c == java.util.Map.class || c == java.util.Set.class
                || c == java.util.Collection.class) {
            return;
        }
        if (c.isInterface() && !c.isSealed()) {
            violations.add(where + " takes an open interface, " + c.getName());
            return;
        }
        if (c.isSealed()) {
            for (Class<?> v : c.getPermittedSubclasses()) {
                takesData(v, where, seen, violations);
            }
            return;
        }
        if (c.isRecord()) {
            for (java.lang.reflect.RecordComponent rc : c.getRecordComponents()) {
                takesData(rc.getGenericType(), where, seen, violations);
            }
            return;
        }
        // a final class (a typed query, a plan) is the product's own value, its fields its own; an open class is the
        // caller's code by subclass (the follow-up audit, 2026-11-09: the guard passed every class until then)
        if (!java.lang.reflect.Modifier.isFinal(c.getModifiers())) {
            violations.add(where + " takes an open class, " + c.getName());
        }
    }

    private static boolean inAnyPackage(String pkg, java.util.List<String> packages) {
        for (String p : packages) {
            if (pkg.equals(p) || pkg.startsWith(p + ".")) {
                return true;
            }
        }
        return false;
    }

    /**
     * R4, ONLY THE DOORS COMPOSE (slice 5, the user's word of 2026-11-09: "callers compile and call a door, nothing
     * else"): a class outside the planning and running libraries that reaches both -- the server, the test runners,
     * the doors' options -- names only planning's public entries and the types they hand out ({@link #PLANNING_API})
     * and only the session it owns and the run's parameter types ({@link #RUN_API}). The work of putting a plan and a
     * run together is a door's ({@link #DOORS}); no third place plans with the planner's machinery and runs with the
     * runtime's. Hard, no register: at its first measure (2026-11-09) three classes reached both, each through these
     * types alone.
     */
    @Test
    void onlyTheDoorsCompose() {
        java.util.Set<String> composers = new java.util.TreeSet<>();
        for (com.tngtech.archunit.core.domain.JavaClass c : CORE_PROD_CLASSES) {
            String top = topLevel(c);
            if (inAnyPackage(c.getPackageName(), PLANNING_PACKAGES) || inAnyPackage(c.getPackageName(), RUNNING_PACKAGES)
                    || DOORS.contains(top) || PLAN_SIDE_DOORS.contains(top)) {
                continue;
            }
            java.util.Set<String> plans = new java.util.TreeSet<>();
            java.util.Set<String> runs = new java.util.TreeSet<>();
            for (com.tngtech.archunit.core.domain.Dependency d : c.getDirectDependenciesFromSelf()) {
                com.tngtech.archunit.core.domain.JavaClass t = d.getTargetClass().getBaseComponentType();
                String target = topLevel(t);
                if (inAnyPackage(t.getPackageName(), PLANNING_PACKAGES) || PLAN_SIDE_DOORS.contains(target)) {
                    plans.add(target);
                }
                if (inAnyPackage(t.getPackageName(), RUNNING_PACKAGES)) {
                    runs.add(target);
                }
            }
            if (plans.isEmpty() || runs.isEmpty()) {
                continue;
            }
            plans.removeAll(PLANNING_API);
            plans.removeIf(t -> t.startsWith(PLANNING_INPUT_PACKAGE));
            runs.removeAll(RUN_API);
            if (!plans.isEmpty() || !runs.isEmpty()) {
                composers.add(top + " reaches planning " + plans + " and running " + runs);
            }
        }
        org.junit.jupiter.api.Assertions.assertTrue(composers.isEmpty(), "R4: a class other than the doors plans with"
                + " the planner's machinery and runs with the runtime's (call a door): " + composers);
    }

    /** The calls a door may make (the doors' shape, below): planning's entries, the runs' entries, and the test door's
     *  options' accessors -- {@code stores} since 2026-11-09 (docs/DATA_IN_THE_PLAN_2026_11_09.md L1: the data stores a
     *  run's loads resolve through, a run input as the system connections are; the resolver function planning called
     *  until then is deleted). */
    private static final java.util.Set<String> DOOR_PLANNING_CALLS = java.util.Set.of(
            "com.legend.Compiler#compileModel", "com.legend.Compiler#query", "com.legend.TypedQuery#executionPlan",
            "com.legend.TypedQuery#projectPlan",
            "com.legend.testplan.TestPlanner#plan",
            "com.legend.ExecuteOptions#planning", "com.legend.ExecuteOptions#edge",
            "com.legend.ExecuteOptions#systemConnections", "com.legend.ExecuteOptions#stores");

    private static final java.util.Set<String> DOOR_RUN_CALLS = java.util.Set.of("com.legend.exec.ProductRun#run",
            "com.legend.exec.ProductRun#onProjectDatabases", "com.legend.testexec.TestRunner#run",
            "com.legend.testexec.TestRunner#runOpening");

    /**
     * THE DOORS ONLY COMPOSE (the user's question of 2026-11-09: "something that enforces that this should only call
     * one line to call planner and one line to call executor and that's all it can do"): a door holds no state (no
     * field, no nested class) and each of its methods calls only planning's entries ({@link #DOOR_PLANNING_CALLS}),
     * the runs' entries ({@link #DOOR_RUN_CALLS}) and its own overloads; it calls at most one run -- a run entry or an
     * overload. It reads no field but an enum's constant. Its source is one statement per method, with no branch, loop,
     * lambda or {@code new} ({@code JavaEvalLedgerTest.theDoorsHoldNoLogic}: the bytecode shows neither a branch nor
     * which call comes first in one expression) -- so the run is the statement's outermost call, its plan an argument. A line cap would only measure it; this says what it may do.
     */
    @Test
    void theDoorsOnlyCompose() {
        java.util.List<String> found = new java.util.ArrayList<>();
        for (com.tngtech.archunit.core.domain.JavaClass c : CORE_PROD_CLASSES) {
            String top = topLevel(c);
            if (!DOORS.contains(top)) {
                continue;
            }
            if (!c.getName().equals(top)) {
                found.add("a nested class " + c.getName());
                continue;
            }
            for (com.tngtech.archunit.core.domain.JavaField f : c.getFields()) {
                found.add("a field " + f.getFullName());
            }
            for (com.tngtech.archunit.core.domain.JavaCodeUnit u : c.getCodeUnits()) {
                if (u.isConstructor() || u.getName().equals("<clinit>")) {
                    continue;
                }
                int runs = 0;
                for (com.tngtech.archunit.core.domain.JavaCall<?> call : u.getCallsFromSelf()) {
                    String target = call.getTargetOwner().getName() + "#" + call.getName();
                    if (DOOR_RUN_CALLS.contains(target) || call.getTargetOwner().getName().equals(top)) {
                        runs++;
                    } else if (!DOOR_PLANNING_CALLS.contains(target)) {
                        found.add(u.getFullName() + " calls " + target);
                    }
                }
                if (runs > 1) {
                    found.add(u.getFullName() + " runs " + runs + " times");
                }
                if (!u.getMethodReferencesFromSelf().isEmpty() || !u.getConstructorReferencesFromSelf().isEmpty()) {
                    found.add(u.getFullName() + " makes a method reference");
                }
                for (com.tngtech.archunit.core.domain.JavaFieldAccess a : u.getFieldAccesses()) {
                    com.tngtech.archunit.core.domain.JavaClass owner = a.getTarget().getOwner();
                    if (!owner.isEnum() || !a.getTarget().getRawType().equals(owner)) {
                        found.add(u.getFullName() + " reads " + a.getTarget().getFullName());
                    }
                }
            }
        }
        org.junit.jupiter.api.Assertions.assertTrue(found.isEmpty(), "a door does more than compose a plan and a run: "
                + found);
    }

    /** No product class reaches the compatibility edge (slice 5's spec, 2026-11-09): the edge depends on the product,
     *  never the reverse (G5); its codecs reach the test plan through the extension point's service file alone. The
     *  build's twin is {@code tools/deps/core-layers.txt} (no product library lists {@code edge}). */
    @Test
    void noProductClassReachesTheEdge() {
        noClasses()
            .that().resideOutsideOfPackage("com.legend.edge..")
            .should().dependOnClassesThat().resideInAPackage("com.legend.edge..")
            .as("G5: the edge depends on the product, never the reverse")
            .check(CORE_PROD_CLASSES);
    }

    /** No thread-scoped state (the user's ruling of 2026-11-09, "remove all thread local"): every value a step needs is
     *  handed to it. The one exception is base's {@code RunScope}, the run-time proof of the split the goals name
     *  (G4: "proven at run time (RunScope) and at compile time (ArchUnit)") -- a guard, never a channel. */
    @Test
    void noThreadLocalState() {
        // any thread-scoped holder, a subclass of ThreadLocal or a ScopedValue included (the full audit, 2026-11-09:
        // the rule matched ThreadLocal's raw type alone)
        com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noFields()
            .that().areDeclaredInClassesThat().doNotBelongToAnyOf(com.legend.base.RunScope.class)
            .should().haveRawType(com.tngtech.archunit.core.domain.JavaClass.Predicates.assignableTo(ThreadLocal.class))
            .orShould().haveRawType("java.lang.ScopedValue")
            .as("no thread-local state but the run-time wall's (RunScope)")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>NO COMPILED MODEL REACHES THE RUN (the plan-as-data design's slice 3, docs/PLAN_AS_DATA_2026_11_07.md
     * §11, 2026-11-08; a ratchet, ZERO AT SLICE 4).</strong> The run reads its plan and the database's rows, never the
     * model it was planned over: no code unit the run REACHES ({@link #reachable} from {@link #RUN_ENTRIES}: by calls,
     * a virtual call reaching every override, a lambda's calls its enclosing unit's) takes a {@code ModelContext} or a
     * {@code TypedQuery}, calls a method that takes, returns or belongs to one, or reads a field of either type.
     * Reachability, not a list of run classes (the slice 3 invariant audit, 2026-11-08: the first version scanned named
     * classes and methods and missed the run methods of the classes not yet split). Holes 1, 2 and 7 closed it (slice 4,
     * 2026-11-09): {@link #MODEL_SITES} is empty and the guard hard.
     */
    @Test
    void noCompiledModelReachesTheRun() {
        java.util.Set<String> modelTypes = java.util.Set.of("com.legend.compiler.element.ModelContext",
                "com.legend.TypedQuery");
        java.util.Set<String> sites = new java.util.TreeSet<>();
        for (com.tngtech.archunit.core.domain.JavaCodeUnit u : reachable(RUN_ENTRIES)) {
            String at = u.getOwner().getName() + "#" + u.getName();
            for (com.tngtech.archunit.core.domain.JavaClass p : u.getRawParameterTypes()) {
                if (modelTypes.contains(p.getName())) {
                    sites.add("takes " + at + "(" + p.getSimpleName() + ")");
                }
            }
            for (var t : targetsOf(u)) {
                boolean returns = t instanceof com.tngtech.archunit.core.domain.AccessTarget.MethodCallTarget m
                        && modelTypes.contains(m.getRawReturnType().getName())
                        || t instanceof com.tngtech.archunit.core.domain.AccessTarget.MethodReferenceTarget r
                        && modelTypes.contains(r.getRawReturnType().getName());
                if (returns || modelTypes.contains(t.getOwner().getName()) || t.getRawParameterTypes().stream()
                        .anyMatch(p -> modelTypes.contains(p.getName()))) {
                    sites.add("calls " + at + " -> " + t.getOwner().getName() + "#" + t.getName());
                }
            }
            for (com.tngtech.archunit.core.domain.JavaFieldAccess f : u.getFieldAccesses()) {
                if (modelTypes.contains(f.getTarget().getRawType().getName())) {
                    sites.add("reads " + at + " -> " + f.getTarget().getFullName());
                }
            }
        }
        java.util.Set<String> grew = new java.util.TreeSet<>(sites);
        grew.removeAll(MODEL_SITES);
        java.util.Set<String> gone = new java.util.TreeSet<>(MODEL_SITES);
        gone.removeAll(sites);
        org.junit.jupiter.api.Assertions.assertTrue(grew.isEmpty(), "a compiled model reaches the run at a NEW site"
                + " (carry the fact in the plan's records): " + grew);
        org.junit.jupiter.api.Assertions.assertTrue(gone.isEmpty(), "a site is gone: shrink MODEL_SITES, dated: " + gone);
    }

    /** Where the run begins: the router's run half, the door's, and the session sources a door opens the run's
     *  session from (hole 7: each is handed the model) -- the contract's method and its implementations, the caller's
     *  own session's ({@code Source.given}) and the server's ({@code ConnectionResolver.open}): the contract is
     *  implemented by lambdas and method references, which no call site expands (the slice 3 follow-up audit,
     *  2026-11-08). */
    private static final java.util.Set<String> RUN_ENTRIES = java.util.Set.of("com.legend.testexec.TestRunner#run",
            // slice 5, 2026-11-09: the doors are thin -- the runs' own entries, and the session sources, are the run's
            "com.legend.testexec.TestRunner#runOpening", "com.legend.exec.ProductRun#run",
            "com.legend.exec.ProductRun#onProjectDatabases", "com.legend.exec.SessionSource#open",
            "com.legend.exec.SessionSource#given",
            "com.legend.exec.PlanSessions#projectDatabase");

    /** Where planning begins: the router's planning halves and the doors'. */
    // TestPlanner#planned, #planAhead and TestExecution#planProgram left 2026-11-10 with the on-demand fixtures they
    // planned (census M9)
    private static final java.util.Set<String> PLANNING_ENTRIES = java.util.Set.of("com.legend.testplan.TestPlanner#plan",
            "com.legend.TestExecution#planBody");

    /** Every code unit of the product reached from {@code entries} ({@code Class#method}, every overload) by calls and
     *  by method references (a reference is a call made later: the slice 3 follow-up audit, 2026-11-08): a target's
     *  resolved unit, and -- the call being virtual -- every override among the product's classes; a lambda's calls
     *  count as its enclosing unit's (ArchUnit's reading). Only the product's own code is followed. */
    private static java.util.Set<com.tngtech.archunit.core.domain.JavaCodeUnit> reachable(java.util.Set<String> entries) {
        java.util.Map<String, java.util.List<com.tngtech.archunit.core.domain.JavaMethod>> bySignature =
                new java.util.HashMap<>();
        java.util.Set<com.tngtech.archunit.core.domain.JavaCodeUnit> seen = new java.util.LinkedHashSet<>();
        java.util.ArrayDeque<com.tngtech.archunit.core.domain.JavaCodeUnit> todo = new java.util.ArrayDeque<>();
        java.util.Set<String> found = new java.util.TreeSet<>();
        for (com.tngtech.archunit.core.domain.JavaClass c : CORE_PROD_CLASSES) {
            for (com.tngtech.archunit.core.domain.JavaMethod m : c.getMethods()) {
                bySignature.computeIfAbsent(signature(m.getName(), m.getRawParameterTypes()),
                        k -> new java.util.ArrayList<>()).add(m);
            }
            for (com.tngtech.archunit.core.domain.JavaCodeUnit u : c.getCodeUnits()) {
                if (entries.contains(c.getName() + "#" + u.getName())) {
                    found.add(c.getName() + "#" + u.getName());
                    if (seen.add(u)) {
                        todo.add(u);
                    }
                }
            }
        }
        org.junit.jupiter.api.Assertions.assertEquals(new java.util.TreeSet<>(entries), found,
                "an entry the reachability walk was given is no code unit of the product");
        while (!todo.isEmpty()) {
            com.tngtech.archunit.core.domain.JavaCodeUnit u = todo.poll();
            for (var t : targetsOf(u)) {
                if (!t.getOwner().getName().startsWith("com.legend.")
                        && !t.getOwner().getPackageName().equals("com.legend")) {
                    continue;
                }
                java.util.List<com.tngtech.archunit.core.domain.JavaCodeUnit> next = new java.util.ArrayList<>();
                t.resolveMember().ifPresent(next::add);
                if (t instanceof com.tngtech.archunit.core.domain.AccessTarget.MethodCallTarget
                        || t instanceof com.tngtech.archunit.core.domain.AccessTarget.MethodReferenceTarget) {
                    for (com.tngtech.archunit.core.domain.JavaMethod m : bySignature.getOrDefault(
                            signature(t.getName(), t.getRawParameterTypes()), java.util.List.of())) {
                        if (m.getOwner().isAssignableTo(t.getOwner().getName())) {
                            next.add(m);
                        }
                    }
                }
                for (com.tngtech.archunit.core.domain.JavaCodeUnit n : next) {
                    if (seen.add(n)) {
                        todo.add(n);
                    }
                }
            }
        }
        return seen;
    }

    /** What {@code u} calls or references (a method reference is a call made later). */
    private static java.util.List<com.tngtech.archunit.core.domain.AccessTarget.CodeUnitAccessTarget> targetsOf(
            com.tngtech.archunit.core.domain.JavaCodeUnit u) {
        java.util.List<com.tngtech.archunit.core.domain.AccessTarget.CodeUnitAccessTarget> targets =
                new java.util.ArrayList<>();
        u.getCallsFromSelf().forEach(c -> targets.add(c.getTarget()));
        u.getCodeUnitReferencesFromSelf().forEach(r -> targets.add(r.getTarget()));
        return targets;
    }

    private static String signature(String name, java.util.List<com.tngtech.archunit.core.domain.JavaClass> params) {
        StringBuilder b = new StringBuilder(name).append('(');
        for (com.tngtech.archunit.core.domain.JavaClass p : params) {
            b.append(p.getName()).append(',');
        }
        return b.append(')').toString();
    }

    /** {@link #noCompiledModelReachesTheRun}'s sites (the reachability measure, 2026-11-08, the slice 3 invariant
     *  audit's fix): AT ZERO since slice 4, the guard hard -- a new site is a defect, carried as the plan's data. */
    private static final java.util.Set<String> MODEL_SITES = java.util.Set.of(
            // (gone 2026-11-08, hole 7 v1, the user's go: the server's session opened from the plan's target alone --
            // the project database's identity and its Data fill planned into the target (ExecutionPlan.Project), the
            // runner's PlanSessions.projectDatabase finding or making it; ConnectionResolver's keys and fill deleted,
            // and the session contract takes no model: 25 sites)
            // (gone 2026-11-08, slice 4's hole 1: the appeal is decided from the plan's data -- the enumerations the
            // referee decodes planned into Appeal.Rows.enums, the referee's SPI handed them, never the model: decideAppeal
            // and rowsLegNow take no model, and verify and verifyPlan take Appeal.Enums -- nine sites)
            // (gone 2026-11-09, slice 4's hole 2: the system database found by the plan's own target -- its engine and
            // the graph's store key, Database.System.graphKey -- one current session per engine in SystemDatabase,
            // replaced when another graph's key comes; SystemDatabase.of(ctx) deleted, and runBody and com.legend.testexec.TestRunner.run
            // take no model: the last six sites)
            );

    /**
     * <strong>THE RUN RE-READS NO RULE (the plan-as-data design's slice 3, hole 8, 2026-11-08; hard).</strong> The plan
     * carries the rules it was planned under -- each verdict step's judging rules, its judge, its features; nothing the
     * run reaches asks the caller's options for them again ({@code ExecuteOptions}' judging rules, judge mode, features
     * or resources). The slice 3 invariant audit: hole 8's code held, and nothing kept it.
     */
    @Test
    void theRunRereadsNoRule() {
        // engineCompat too: the judging rules are derived from it (the slice 3 follow-up audit, 2026-11-08)
        java.util.Set<String> planningFacts = java.util.Set.of("judgingRules", "judgeMode", "features", "resources",
                "engineCompat");
        java.util.Set<String> reads = new java.util.TreeSet<>();
        for (com.tngtech.archunit.core.domain.JavaCodeUnit u : reachable(RUN_ENTRIES)) {
            for (var t : targetsOf(u)) {
                if (t.getOwner().getName().equals("com.legend.ExecuteOptions") && planningFacts.contains(t.getName())) {
                    reads.add(u.getOwner().getName() + "#" + u.getName() + " -> ExecuteOptions#" + t.getName());
                }
            }
        }
        org.junit.jupiter.api.Assertions.assertTrue(reads.isEmpty(), "the run asks the caller's options for a fact the"
                + " plan carries (read it from the plan): " + reads);
    }

    /**
     * <strong>THE LISTENER IS OUTPUT ONLY, AND PLANNING TELLS NO ONE (the plan-as-data design's slice 3, holes 4 and 5,
     * 2026-11-08; hard).</strong> The run tells the caller's {@code AssertListener} what happened and asks it for
     * nothing: its methods are exactly the four reports, each void, each taking texts and flags alone (a pinned set:
     * the slice 3 invariant audit found the first version would have let the deleted {@code beforeRun} back in). And
     * planning tells no one anything: no code unit planning REACHES ({@link #reachable} from
     * {@link #PLANNING_ENTRIES}) calls the listener, the held appeals' thread-local listener or hold, the engine-text
     * arms' decline reporters, the edge's run half (a ledger written), the referee, or the verdict census's outcome
     * counters (a verdict's outcome is counted at its turn in the run); no planning entry takes a listener or a
     * referee, and no planning environment holds one.
     */
    @Test
    void theListenerIsOutputOnly() {
        java.util.Set<String> found = new java.util.TreeSet<>();
        java.util.Set<String> reports = java.util.Set.of(
                "verdict(java.lang.String,boolean,java.lang.String)", "declined(java.lang.String,java.lang.String)",
                "unjudged(java.lang.String,java.lang.String)", "refereed(java.lang.String,java.lang.String)");
        java.util.Set<String> handed = java.util.Set.of("com.legend.testexecutionplan.AssertListener",
                "com.legend.testexec.SqlReplayReferee");
        java.util.Set<String> planningEnvironments = java.util.Set.of("com.legend.testplan.BodyDriver$ExecEnv",
                "com.legend.planner.PlanEnv");
        for (com.tngtech.archunit.core.domain.JavaClass c : CORE_PROD_CLASSES) {
            if (c.getName().equals("com.legend.testexecutionplan.AssertListener")) {
                java.util.Set<String> methods = new java.util.TreeSet<>();
                for (com.tngtech.archunit.core.domain.JavaMethod m : c.getMethods()) {
                    methods.add(m.getName() + "(" + String.join(",", m.getRawParameterTypes().stream()
                            .map(com.tngtech.archunit.core.domain.JavaClass::getName).toList()) + ")");
                    if (!m.getRawReturnType().getName().equals("void")) {
                        found.add("answers " + m.getFullName());
                    }
                }
                if (!methods.equals(new java.util.TreeSet<>(reports))) {
                    found.add("the listener's methods are " + methods + ", not the four reports");
                }
            }
            if (planningEnvironments.contains(c.getName())) {
                for (com.tngtech.archunit.core.domain.JavaField f : c.getFields()) {
                    if (handed.contains(f.getRawType().getName())) {
                        found.add("planning holds " + f.getFullName());
                    }
                }
            }
        }
        java.util.Set<String> told = java.util.Set.of("com.legend.testexecutionplan.AssertListener",
                "com.legend.testexec.SqlReplayReferee", "com.legend.testexec.Appeals#listenerNow", "com.legend.testexec.Appeals#hold",
                "com.legend.testplan.SqlTextVerdicts#declinedNow", "com.legend.testplan.SqlTextVerdicts#declinedTo",
                "com.legend.testexecutionplan.Edge#seeded", "com.legend.testexecutionplan.Edge#effectRan",
                "com.legend.testexec.CanonicalDivergence#sqlUnjudged", "com.legend.testexec.CanonicalDivergence#sqlRaised",
                "com.legend.testexec.CanonicalDivergence#sqlRaisedAt", "com.legend.testexec.CanonicalDivergence#sqlEnter");
        for (com.tngtech.archunit.core.domain.JavaCodeUnit u : reachable(PLANNING_ENTRIES)) {
            String at = u.getOwner().getName() + "#" + u.getName();
            if (PLANNING_ENTRIES.contains(at)) {
                for (com.tngtech.archunit.core.domain.JavaClass p : u.getRawParameterTypes()) {
                    if (handed.contains(p.getName())) {
                        found.add("planning takes " + u.getFullName());
                    }
                }
            }
            for (var t : targetsOf(u)) {
                String owner = t.getOwner().getName();
                if (told.contains(owner) || told.contains(owner + "#" + t.getName())) {
                    found.add("planning tells: " + at + " -> " + owner + "#" + t.getName());
                }
            }
        }
        org.junit.jupiter.api.Assertions.assertTrue(found.isEmpty(), "the listener answers, or planning tells someone"
                + " (the run tells, at each verdict's turn; planning tells no one): " + found);
    }

    @Test
    void execReachesNoPlanning() {
        java.util.Map<String, java.util.Set<String>> reached = new java.util.TreeMap<>();
        for (com.tngtech.archunit.core.domain.JavaClass c : CORE_PROD_CLASSES) {
            if (!c.getPackageName().equals("com.legend.exec")) {
                continue;
            }
            for (com.tngtech.archunit.core.domain.Dependency d : c.getDirectDependenciesFromSelf()) {
                String pkg = d.getTargetClass().getBaseComponentType().getPackageName();
                if ((pkg.equals("com.legend") || pkg.startsWith("com.legend.")) && !EXEC_MAY_REACH.contains(pkg)) {
                    reached.computeIfAbsent(topLevel(c).substring("com.legend.exec.".length()),
                            k -> new java.util.TreeSet<>()).add(topLevel(d.getTargetClass()));
                }
            }
        }
        org.junit.jupiter.api.Assertions.assertTrue(reached.isEmpty(), "the runtime reaches planning (the wall, D72):"
                + " move the work to the plan side or the driver, or carry the fact in the plan records -- " + reached);
    }

    /**
     * <strong>No exec class parses data</strong> (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §5; the A checkpoint's tenets
     * audit's 4b, 2026-11-10: the guard the design named was not built). A load's data reaches the database as the
     * plan's statements or as a file its database reads; exec delivers bytes and statements and reads none of the data's
     * cells: no exec class depends on the CSV parser ({@code base.CsvCells}, named here, never referenced: no load reads
     * it since B2b, 2026-11-11, and its file is deleted by its creator's commit, so the rule holds whether or not it is
     * there). Its one reach until then, the size check ({@code DataLoads.stage}), moved to {@code base.DataSize}, which
     * parses nothing.
     */
    @Test
    void execParsesNoData() {
        noClasses()
            .that().resideInAPackage("com.legend.exec..")
            .should().dependOnClassesThat().haveFullyQualifiedName("com.legend.base.CsvCells")
            .as("exec parses no data: a load's cells are the database's to read (design §5)")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>The pure/v1 API is plan side (2026-10-08, //core:pure_v1).</strong>
     * {@code PureV1Api} lives in {@code com.legend.server}, which the
     * JDBC funnel above exempts, but it is the plan side's: the compiler's
     * boundary answers with it (Python's engine for DataCube,
     * docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md), and its one run is
     * handed in by its host. Bazel keeps the driver and exec out of its
     * library; this rule keeps out what Bazel cannot, the JDK's
     * {@code java.sql}.
     */
    @Test
    void thePureV1ApiReachesNoDatabase() {
        noClasses()
            .that().haveNameMatching("com\\.legend\\.server\\.PureV1Api(\\$.*)?")
            .should().dependOnClassesThat()
            .resideInAnyPackage(JDBC_AND_DRIVERS)
            .as("the pure/v1 API is plan side: its host runs its queries")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>F1.11 — reflection is BANNED in production (USER
     * 2026-09-15: no pardons).</strong> Reflection is the one mechanism
     * that bypasses every dependency rule in this file (a
     * {@code Class.forName("java.sql...")} carries no bytecode dependency
     * ArchUnit can see), and the one way the product could discover or
     * invoke its own types by name. The two pardoned residues are gone
     * (ScanColumns walks the SQL tree's typed {@code children()};
     * server/Json names the array kinds it serializes); the rule now also
     * closes the reflective doors on {@code Class} itself. A walker uses
     * the typed children contract, a decision names its node kind (the
     * code-shape guardrail keeps {@code getSimpleName()} out of logic),
     * a plugin joins through the documented {@code ServiceLoader} seam.
     * Tests keep reflection (the guardrails themselves need it).
     */
    /**
     * THE COMPILER IS DIALECT-BLIND (single-compiler tenet, guard added
     * 2026-08-18 after a user challenge found it missing): the
     * compile-side layers produce ONE semantic MIR; every backend
     * difference is a dialect rewrite/render rule applied AFTER them.
     * Only the execution layer (exec, root) and the connection-setup writer
     * (setup, since 2026-10-08: it spells DDL through the target's dialect) may see
     * {@code com.legend.sql.dialect} — they must render and normalize.
     * ZERO exceptions (Phase 1 audit: the last breach — Scalars'
     * SUBSTRING TextGoldens branch — moved to DuckDb's SubstringClamp
     * rewrite pass; the frozen carve-out is retired).
     */
    @Test
    void compileSideLayersAreDialectBlind() {
        noClasses()
            .that().resideInAnyPackage("com.legend.parser..",
                    "com.legend.compiler..", "com.legend.lowering..",
                    "com.legend.resolver..", "com.legend.normalizer..",
                    "com.legend.lineage..", "com.legend.plan..",
                    "com.legend.protocol..", "com.legend.model..",
                    "com.legend.door..")
            .and().haveNameNotMatching(
                    "com\\.legend\\.lowering\\.Scalars(\\$.*)?")
            .should().dependOnClassesThat()
            .resideInAPackage("com.legend.sql.dialect..")
            .as("single-compiler tenet: compile-side layers are"
                    + " DIALECT-BLIND — backend differences are dialect"
                    + " rewrite/render rules, never branches in the"
                    + " compiler (zero exceptions since Phase 1)")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * THE RawSql QUARANTINE, at bytecode level (One-Platform Plan
     * Phase 1; companion to RawSqlLedgerTest's source register): only
     * the chartered seam may CONSTRUCT {@code SqlSource.RawSql}.
     * Renderers and rewriters legitimately pattern-match it (that is a
     * dependency), so the rule targets the CONSTRUCTOR CALL — the act
     * of wrapping text as a relation source. Anything else wrapping SQL
     * text in RawSql is smuggling past the compiler.
     */
    @Test
    void rawSqlSourceIsConstructedOnlyAtTheCharteredSeam() {
        noClasses()
            .that().haveNameNotMatching(
                    // sql.AliasPrefix (leg 3.4 step 2): re-aliases an EXISTING RawSql
                    // node — the authored text rides through verbatim (RawSqlAdapt is gone, 2026-10-13: the
                    // renderer translates the text in place, SqlDialect.raw); executionplan.MirJson
                    // (2026-10-18, the plan path program's step 1b (docs/PLAN_PATH_PROGRAM_2026_10_17.md §3)): the plan's typed tree read back from its JSON, exactly as the planner
                    // wrote it -- no text composed
                    // (GridProbe's LIMIT-0 probe deleted 2026-10-30, the planner-driver seam program's S3c: a raw grid's
                    // columns are the ones its result reports, read when the plan runs)
                    "com\\.legend\\.(lowering\\.Lowerer|sql\\.AliasPrefix|executionplan\\.MirJson)(\\$.*)?")
            .should().callConstructorWhere(
                    com.tngtech.archunit.core.domain.JavaCall.Predicates
                            .target(com.tngtech.archunit.core.domain
                                    .properties.HasOwner.Predicates.With
                                    .owner(com.tngtech.archunit.core
                                            .domain.properties.HasName
                                            .Predicates.name(
                                                    "com.legend.sql.SqlSource$RawSql"))))
            .as("Phase 1 quarantine: SqlSource.RawSql is constructed"
                    + " ONLY by the chartered seam (the Lowerer's"
                    + " TypedRawSqlRelation case) — it carries authored text, never"
                    + " platform-composed SQL")
            .check(CORE_PROD_CLASSES);
    }

    @Test
    void reflectionIsBannedInProduction() {
        noClasses()
            .that().resideInAPackage("com.legend..")
            .should().dependOnClassesThat()
            .resideInAnyPackage("java.lang.reflect..", "java.lang.invoke..")
            // and the reflective doors on Class itself (no java.lang.reflect
            // import needed to open them)
            .orShould().callMethodWhere(new com.tngtech.archunit.base.DescribedPredicate<
                    com.tngtech.archunit.core.domain.JavaMethodCall>(
                    "a reflective Class method (forName, getRecordComponents, getDeclared*,"
                    + " getMethod*, getField*, getConstructor*, newInstance)") {
                @Override
                public boolean test(com.tngtech.archunit.core.domain.JavaMethodCall call) {
                    var target = call.getTarget();
                    return target.getOwner().isEquivalentTo(Class.class)
                            && target.getName().matches(
                                    "forName|getRecordComponents|getDeclared\\w*|getMethods?"
                                    + "|getFields?|getConstructors?|newInstance");
                }
            })
            .as("F1.11: NO reflection in production — the product never discovers or"
                    + " invokes its own types by name: walk typed children, switch on"
                    + " node kinds, register plugins through the SPI seam")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * TeaVM's class library (0.15, the tab's WebAssembly build) answers {@code String.isBlank()} differently: it counts
     * only {@code ' '} as blank (found by //wasm:round_trip_test, the protocol program's leg 5, 2026-10-09; still so,
     * //wasm:conformance-known.tsv's {@code string} row). So lite does not ask it: blank is {@code s.strip().isEmpty()}
     * everywhere -- the JDK's definition of {@code isBlank}, and TeaVM's {@code strip} reads whitespace as the JDK does.
     * (A double's text, which TeaVM also wrote differently until 2026-10-10, needs no rule here any more:
     * //third_party/teavm_classlib makes TeaVM's conversions the JDK's, and //wasm:conformance_test holds them there.)
     */
    @Test
    void textTheTabWritesDifferentlyIsWrittenPortably() {
        noClasses()
            .that().resideInAPackage("com.legend..")
            .should().callMethod(String.class, "isBlank")
            .as("String.isBlank() is TeaVM's ' ' only in the tab: write s.strip().isEmpty()")
            .check(CORE_PROD_CLASSES);
    }

    // F1.3b, rootJavaSqlSurfaceIsPinned (2026-10-03 to 2026-10-30), DELETED with the java.sql revamp's step 3 (D69,
    // the audit's finding): it pinned the root package's java.sql surface to an enumerated set while the funnel
    // licensed the root as a package; the funnel now exempts the census register's classes only, none of them in the
    // root, so the pin was subsumed -- and its last entry, Execution, had held no java.sql since D67 (a shrink-only
    // pin that had not shrunk). Its javadoc's one standing lesson (a class that only passes a connection along is
    // not a java.sql dependency the way ArchUnit counts it) moved to F1.3's.

    /**
     * <strong>The body driver and the judges hold no JDBC (2026-10-29, the result boundary program's track B, B1b;
     * docs/RESULT_BOUNDARY_START_HERE.md, the track B block).</strong> A test body is a sequence of plans: the driver
     * ({@code BodyDriver}; {@code BodyCompiler} until the walk was deleted, 2026-11-06), the judges and their arms, the seed and error natives and the
     * test-data generator's natives hold a session ({@code Sessions.Session}) and reach the database only through its
     * statements ({@code Statements}), the plan runner and the session's state ({@code SessionState}); none depends
     * on {@code java.sql} or a driver's own API. The funnel above licensed root as a package until 2026-10-30 (D69:
     * it exempts the census register's classes only), when this rule named the classes the funnel would have let
     * through; it stays as the pin by name, readable without the register. The generator itself
     * ({@code TestDataGenerator}) joined 2026-10-29 (the doors): it runs every statement through the plan runner and
     * holds no JDBC (D30's residue closed).
     */
    @Test
    void theBodyDriverAndTheJudgesHoldNoJdbc() {
        noClasses()
            // the whole judge package since 2026-10-30 (D71: the judges out of exec into com.legend.judge), and
            // the probes with them at the root (ColumnProbe, WireProbe and GridProbe deleted 2026-10-30 and
            // DynamicPivot 2026-10-31, the seam program's S3: no probe is left)
            // the whole driver package since 2026-10-30 (D73, step C: the execution side's machinery -- the body driver
            // and compiler, the judges' arms, the seeds, the probes, the system database -- is com.legend.driver)
            .that().haveNameMatching("com\\.legend\\.(driver\\.[A-Za-z0-9_]+"
                    + "|judge\\.[A-Za-z0-9_]+"
                    + "|testdatagen\\.TestDataGenerationNatives|testdatagen\\.TestDataGenerator)"
                    + "(\\$.*)?")
            .should().dependOnClassesThat()
            .resideInAnyPackage(JDBC_AND_DRIVERS)
            .as("B1b: the body driver, the judges, the probes, the generator and its natives hold a session and no JDBC")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>The plan runner is the one caller of a session's statements (the doors, 2026-10-29; docs/RESULT_BOUNDARY_START_HERE.md,
     * "the doors").</strong> {@code Statements} is the session seam (text in; rows, a count, reported columns or text
     * out), and {@code PlanRunner} is the one place a statement meets it: every statement sent is a plan's, checked
     * against its target (the database, the server versions it is written for), given the target's session
     * statements and counted. No schema probe is left (the seam program's S3, 2026-10-31: a dynamic pivot's keys and a
     * raw grid's columns are read by a plan's own first step); the seeds, a program's session
     * statements and the system store's rows are setup steps it runs ({@code setUp}); the transaction bracket is the
     * session's own ({@code Sessions.Session.inTransaction}). The session owners ({@code PlanSessions},
     * {@code Sessions}) and the implementations ({@code JdbcStatements}, MongoDB's) are the seam itself. No other
     * class calls a method of {@code Statements}, and none takes a session's statements
     * ({@code Sessions.Session.statements}).
     */
    @Test
    void theRunnerIsTheOneCallerOfTheSessionsStatements() {
        // ProgramRunner since 2026-11-08 (the plan-as-data design's step 3, the user's ruling): the runner's program half, its
        // own target -- the program's transaction bracket and snapshot, as the runner took them before the split
        String seam = "com\\.legend\\.exec\\.(PlanRunner|ProgramRunner|PlanSessions|Sessions|JdbcStatements|MongoDbStatements|Statements)(\\$.*)?";
        noClasses()
            .that().haveNameNotMatching(seam)
            .should().callMethodWhere(com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                    com.tngtech.archunit.core.domain.properties.HasOwner.Predicates.With.owner(
                            com.tngtech.archunit.core.domain.JavaClass.Predicates.assignableTo(
                                    com.legend.exec.Statements.class))))
            .orShould().callMethodWhere(new com.tngtech.archunit.base.DescribedPredicate<
                    com.tngtech.archunit.core.domain.JavaMethodCall>("a session's statements (Sessions.Session"
                    + ".statements)") {
                @Override
                public boolean test(com.tngtech.archunit.core.domain.JavaMethodCall call) {
                    // statementsOf: the session's own unguarded form, package-private (THE WALL, 2026-11-06: the
                    // final audit -- a class of the seam's package calling it would pass the name check alone)
                    return call.getTarget().getOwner().isAssignableTo(com.legend.exec.Sessions.Session.class)
                            && (call.getTarget().getName().equals("statements")
                                    || call.getTarget().getName().equals("statementsOf"));
                }
            })
            .as("the doors: the plan runner is the one caller of a session's statements (Statements, "
                    + "Sessions.Session.statements)")
            .check(CORE_PROD_CLASSES);
    }

    /** Grammar cursors and section parsers are parse-time machinery. */
    @Test
    void parseMachineryIsUsedOnlyWhereSanctioned() {
        noClasses()
            .that().resideOutsideOfPackages(
                    "com.legend.parser..", "com.legend.ide..",
                    "com.legend.builtin", "com.legend",
                    // the server shell receives RAW PURE TEXT over HTTP —
                    // a parse ENTRY like the driver (LSP diagnostics,
                    // diagram extraction, runtime->connection resolution)
                    "com.legend.server..")
            .should().dependOnClassesThat().haveNameMatching(
                    "com\\.legend\\.parser\\.(ElementParser|SpecParser"
                    + "|MappingGrammarParser|RelationalGrammarParser"
                    + "|TokenStreamCursor)(\\$.*)?")
            .as("Invariant 6i: grammar parsers/cursors live and die at parse"
              + " time (builtin's bootstrap parse + the driver are the two"
              + " sanctioned exceptions)")
            .check(CORE_PROD_CLASSES);
    }

    @Test
    void cachesAreFunneledToContentAddressedStore() {
        noClasses()
            .that().resideOutsideOfPackage("com.legend.cache")
            .should().haveSimpleNameEndingWith("Cache")
            .orShould().haveSimpleNameEndingWith("Store")
            .as("Invariant 3: caches must be content-addressed — put them in "
              + "com.legend.cache on ContentStore (Hash-keyed). See core/README.md.")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>Invariant 3, field level (D5).</strong> The class-name rule
     * above is a funnel a FIELD walks straight past: ConnectionResolver's
     * {@code static Map CACHE} was a name-/version-keyed cache with a
     * check-then-act race and no reviewer, invisible to a {@code *Cache}
     * class-name check. A declared-type ArchUnit rule cannot close this
     * either — every field spells {@code Map<...>} whether the value is
     * {@code Map.of(...)} or {@code new ConcurrentHashMap<>()} — so this
     * guard reflects on the RUNTIME VALUES: every static collection field
     * outside {@code com.legend.cache} must hold an immutable collection,
     * or appear in the register below with a written justification.
     *
     * <p>Register discipline (see the corpus ratchets): rows only move
     * with same-commit justification; write-once init tables are burn-down
     * candidates (wrap in {@code Map.copyOf} and delete the row).
     */
    /**
     * <strong>V3 (OPEN_REGISTER) — the host verdict is referee-only.</strong>
     * The ratified dual-verdict design: the DB byte compare is the
     * verdict of record; the host lattice ({@code PureAsserts}) and the
     * canonical instruments ({@code CanonicalForm}/{@code
     * CanonicalDivergence}) exist ONLY at the verdict/referee seam. No
     * other production class may reach them — a new dependent means a
     * product path started making pure-equality decisions in Java,
     * which is the disease the whole verdict program deletes. Tests
     * (the harness IS the referee) are exempt by scope.
     */
    // (THE SNIFF-STOP PIN, spellingDecodeIsLabelDriven -- LiteralText.parse reachable only from the LITERAL slot's
    // decode arms -- left with its subject on 2026-10-29, the result boundary program's track L, L2: the LITERAL
    // spelling carrier, its parser and the decoders' arms are deleted; a mixed value is a tagged value, read in its
    // slot, and no cell is parsed by its text's shape.)

    /** Register S144's reader of upstream's JSON grammar is the two judges' alone (the user, 2026-11-07: "keep it
     *  isolated"): no product class, and no other judging class, reads JSON through it. It moves to the edge (P2). */
    @Test
    void theCompatibilityJsonReaderIsTheJudgesAlone() {
        noClasses()
            .that().doNotHaveFullyQualifiedName("com.legend.testplan.HostJudge")
            .and().doNotHaveFullyQualifiedName("com.legend.testplan.DatabaseJudge")
            .and().doNotHaveFullyQualifiedName("com.legend.testexecutionplan.JudgeJson")
            .and().doNotHaveFullyQualifiedName("com.legend.testexecutionplan.JudgeJson$JsonSimple")
            .and().doNotHaveFullyQualifiedName("com.legend.testexecutionplan.JudgeJson$JsonSimple$Kind")
            .should().dependOnClassesThat().haveNameMatching("com\\.legend\\.driver\\.JudgeJson(\\$.*)?")
            .as("S144: json-simple's grammar is read by the two judges alone")
            .check(CORE_PROD_CLASSES);
    }

    @Test
    void hostVerdictIsReachableOnlyFromTheVerdictSeam() {
        noClasses()
            .that().doNotHaveFullyQualifiedName("com.legend.testplan.AssertVerdicts")
            // task #27 (2026-09-21): the verdict seam's two arms — the host judge
            // (Java compare over database rows) and the database judge (the verdict
            // statement) — split out of the router
            .and().doNotHaveFullyQualifiedName("com.legend.testplan.HostJudge")
            .and().doNotHaveFullyQualifiedName("com.legend.testplan.DatabaseJudge")
            .and().doNotHaveFullyQualifiedName("com.legend.testplan.AssertErrorNative")
            .and().doNotHaveFullyQualifiedName("com.legend.testexec.PureAsserts")
            .and().doNotHaveFullyQualifiedName("com.legend.testexec.TdsCompare")
            .and().doNotHaveFullyQualifiedName("com.legend.testexec.CanonicalForm")
            .and().doNotHaveFullyQualifiedName("com.legend.testexec.CanonicalDivergence")
            // testdatagen composes seed SOURCE TEXT via the ONE repr
            // owner — spelling, never an equality decision
            .and().doNotHaveFullyQualifiedName("com.legend.testdatagen.TestDataGenerator")
            // (exec.JsonCompare, the one wire-tree walker of P2-4/P2-6, was exempted here until 2026-10-30: the class
            // was deleted long before -- the ledger's Equality row records it gone -- and the D71 audit found the
            // exemption naming nothing; deleted, a dated note)
            // JUDGING_TWO_MODES step 2: the host judge and the service-test
            // judge are the seam's own (Equality decides; TestAssertions
            // routes EqualToJson to it)
            .and().doNotHaveFullyQualifiedName("com.legend.testexec.Equality")
            .and().doNotHaveFullyQualifiedName("com.legend.test.TestAssertions")
            // leg 3.4 (2026-09-20): the verdict seam's SENDING arm — the
            // body's deferred verdict statements as one statement; it
            // counts the census rows the arm counted at the assert
            // (raised, unjudged) and decides nothing (the row's judgment
            // is injected from AssertVerdicts)
            .and().doNotHaveFullyQualifiedName("com.legend.testplan.VerdictBatch")
            // the batch's planned half (the test-is-the-product plan's M7, 2026-11-01) is data since the plan-as-data
            // design's step 5 (2026-11-08): its root loop is the plan's and runs in RootLoops, which makes the census
            // calls the planned half made (raised, unjudged); it decides nothing -- the row's judgment is handed in
            // (com.legend.testexec.DatabaseVerdicts.verdictOf), its appeal heard by number
            .and().doNotHaveFullyQualifiedName("com.legend.testexec.RootLoops")
            .and().doNotHaveFullyQualifiedName("com.legend.testexec.RootLoops$Run")
            // 2026-11-07, P1.2 (the goals of record: a compatibility appeal is the edge's, decided after the body's
            // run): a body's held appeals, the verdict seam's after the run -- a failed appeal counted as raised, as a
            // failure at its step is (the census call); the appeal's own judgment is SqlTextVerdicts', nothing here
            .and().doNotHaveFullyQualifiedName("com.legend.testexec.Appeals")
            // the four live in com.legend.judge since 2026-10-30 (D71, the seam-and-wall program's B1: the judge
            // layer out of exec); the rule's exemptions name them there too
            .should().dependOnClassesThat().haveNameMatching(
                "com\\.legend\\.judge\\.(PureAsserts|CanonicalForm|CanonicalDivergence|Equality)")
            .as("V3: host-verdict classes are reachable only from the"
                    + " verdict/referee seam — register a new dependent"
                    + " consciously or route through the DB byte verdict")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * Invariant 3 (field level, bytecode): every STATIC field of the product is {@code final}. It replaces the
     * line-based regex {@code CodeShapeGuardrailTest.noStaticMutableState}, which a declaration split over two lines
     * escaped ({@code CanonicalDivergence.CONTEXT_SOURCE}, execution plan W0.2(d)). Its known limit: a final field may
     * still hold a mutable object (a queue, a map, an AtomicReference); collections are held by
     * {@link #staticStateIsConstantOrRegistered} holds the rest (2026-11-09: until then collections alone).
     */
    @Test
    void staticFieldsAreFinal() {
        com.tngtech.archunit.lang.syntax.ArchRuleDefinition.fields()
                .that().areStatic()
                // //testing is the test-support library (testonly), not the product
                .and().areDeclaredInClassesThat().resideOutsideOfPackage("com.legend.testing..")
                .should().beFinal()
                .as("Invariant 3: no assignable static state in the product")
                .check(CORE_PROD_CLASSES);
    }

    /** The kinds of static state the product may hold (the user's ruling of 2026-11-09: "do we want mutable state? fix
     *  all of it"): anything else is a constant -- immutable, or a registry sealed once its class initialised
     *  ({@code base.Sealed}). */
    private enum StaticKind {
        /** The run-time wall's thread-scoped depth (G4: proven at run time), the one thread-local of the product. */
        RUN_GUARD,
        /** A holder of the process's resources: the databases and sessions it keeps, a counter that names them. */
        RESOURCE,
        /** A memo of a pure function of constant inputs (the platform's tables, the boot model): an answer kept is the
         *  answer computed again. */
        CACHE,
        /** Measurement: a census's counters, samples and attribution label -- written by the product, read only by its
         *  own class's reports, never by a decision. */
        MEASUREMENT,
        /** A constant table held as an array for its speed (a digest's constants, a calendar's names): private, never
         *  written after its class initialised. */
        CONSTANT_ARRAY
    }

    /** THE STATIC STATE REGISTER (2026-11-09; until then {@code staticCollectionStateIsImmutableOrRegistered}, which saw
     *  collections alone -- an atomic, an array, a shared dialect, a session store passed it): every static field of the
     *  product holding a mutable object after its class initialised, by kind. Exact in both directions. */
    private static final java.util.Map<String, StaticKind> STATIC_STATE = java.util.Map.ofEntries(
            java.util.Map.entry("com.legend.base.RunScope.DEPTH", StaticKind.RUN_GUARD),
            java.util.Map.entry("com.legend.base.RunScope.PLANNING", StaticKind.RUN_GUARD),
            java.util.Map.entry("com.legend.base.RunScope.JUDGING", StaticKind.RUN_GUARD),
            // the databases the process keeps: the server's project databases, the plans' own and named ones
            java.util.Map.entry("com.legend.exec.PlanSessions.SHARED", StaticKind.RESOURCE),
            java.util.Map.entry("com.legend.exec.PlanSessions.NAMED", StaticKind.RESOURCE),
            java.util.Map.entry("com.legend.exec.PlanSessions.PROJECTS", StaticKind.RESOURCE),
            java.util.Map.entry("com.legend.exec.PlanSessions.ATTEMPTS", StaticKind.RESOURCE),
            // the current system database of each engine, leased per run (hole 2)
            java.util.Map.entry("com.legend.exec.SystemDatabase.CURRENT", StaticKind.RESOURCE),
            // a private in-memory database's unique name
            java.util.Map.entry("com.legend.exec.H2Connector.PRIVATE_IDS", StaticKind.RESOURCE),
            // the boot model compiled once; a bare name's referents and a built call's names over the platform's tables
            java.util.Map.entry("com.legend.Compiler.BOOT", StaticKind.CACHE),
            java.util.Map.entry("com.legend.compiler.BareNames.REFERENTS", StaticKind.CACHE),
            java.util.Map.entry("com.legend.compiler.NameResolver.BUILT", StaticKind.CACHE),
            java.util.Map.entry("com.legend.diagnostics.Census.COUNTS", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.diagnostics.Census.KEYED", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.diagnostics.Census.FALLBACK_REASONS", StaticKind.MEASUREMENT),
            // the Arrow framer's constants (2026-11-09, the follow-up audit: //arrow_ipc joins the product's jars)
            java.util.Map.entry("com.legend.arrowipc.ArrowIpc.END", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.arrowipc.ZstdFrame.MAGIC", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.diagnostics.Census.UNPLANNED_ASSERTS", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.base.Attribution.LABEL", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.diagnostics.VerdictCensus.CURRENT_FAMILY", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.diagnostics.VerdictCensus.SQL_CENSUS", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.diagnostics.VerdictCensus.CHANNEL_SEEN", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.testexec.CanonicalDivergence.SAMPLES", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.testexec.CanonicalDivergence.DISAGREE_SAMPLES", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.testexec.CanonicalDivergence.V7_FORMS", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.testexec.CanonicalDivergence.V7_DECLINES", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.testexec.CanonicalDivergence.V7_SAMPLES", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.testexec.CanonicalDivergence.V7_SIDE_ROWS", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.testexec.CanonicalDivergence.V7_DECLINE_WITNESSES", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.testexec.CanonicalDivergence.QUARANTINED_WALL_TESTS",
                    StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.probe.Shadow.SEEN", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.probe.Shadow.TABLES", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.probe.Shadow.ROWS", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.sql.dialect.StableScanOrder.FIRINGS", StaticKind.MEASUREMENT),
            java.util.Map.entry("com.legend.sql.dialect.SQLite.MONTHS", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.sql.dialect.SQLite.DAYS", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.sql.dialect.SqliteDigests.MD5_K", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.sql.dialect.SqliteDigests.MD5_S", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.sql.dialect.SqliteDigests.SHA256_K", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.cache.Sha256.K", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.cache.Sha256.H0", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.protocol.sql.iq.IqFunctions.MONTHS", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.protocol.sql.iq.IqFunctions.WEEKDAYS", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.protocol.sql.iq.IqFunctions.WEEK_STARTS", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.lexer.TokenStream.TOKEN_TYPES", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.builtin.SystemMetamodel.DATA_TYPE_KINDS", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.builtin.SystemMetamodel.OP_KINDS", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.builtin.SystemMetamodel.PLAN_NODE_KINDS", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.builtin.SystemMetamodel.CONNECTION_KINDS", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.builtin.SystemMetamodel.DATASOURCE_KINDS", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.builtin.SystemMetamodel.VS_KINDS", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.builtin.SystemMetamodel.ACTIVITY_KINDS", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.builtin.SystemMetamodel.ELEMENT_KINDS", StaticKind.CONSTANT_ARRAY),
            java.util.Map.entry("com.legend.lowering.DateFormats.FORMAT_TOKENS", StaticKind.CONSTANT_ARRAY));

    /**
     * NO STATIC STATE BUT THE REGISTER'S (the user's ruling of 2026-11-09, "do we want mutable state? fix all of it"):
     * every static field of the product is read after its class initialised, and the object it holds is a constant --
     * immutable, a lambda, a record or an enum, or a registry sealed once built ({@code base.Sealed}) -- or a field of
     * {@link #STATIC_STATE}, by kind, and private (no other class reaches it: a census is read through its own reports,
     * never by a decision; a cache or a resource through its class's methods). The register is exact: an entry whose
     * field now holds a constant is stale.
     */
    @Test
    void staticStateIsConstantOrRegistered() throws Exception {
        java.util.List<String> violations = new java.util.ArrayList<>();
        java.util.Set<String> seen = new java.util.TreeSet<>();
        for (com.tngtech.archunit.core.domain.JavaClass jc : CORE_PROD_CLASSES) {
            Class<?> cls;
            try {
                cls = Class.forName(jc.getName());
            } catch (ExceptionInInitializerError e) {
                // a class whose initialiser fails is a finding, never a class skipped (the follow-up audit, 2026-11-09)
                violations.add(jc.getName() + "'s class initialiser fails: " + e.getCause());
                continue;
            } catch (NoClassDefFoundError | ClassNotFoundException e) {
                continue; // a class whose driver is not on the test JVM's classpath: nothing to inspect
            }
            for (java.lang.reflect.Field f : cls.getDeclaredFields()) {
                if (!java.lang.reflect.Modifier.isStatic(f.getModifiers()) || f.isSynthetic()
                        || f.getType().isPrimitive()) {
                    continue;
                }
                String id = cls.getName() + "." + f.getName();
                f.setAccessible(true);
                String mutable = mutableObject(f.get(null), 0);
                StaticKind kind = STATIC_STATE.get(id);
                if (mutable == null) {
                    if (kind != null) {
                        violations.add(id + " is registered as " + kind + " but holds a constant (shrink the register)");
                    }
                    continue;
                }
                seen.add(id);
                if (kind == null) {
                    violations.add(id + " holds mutable state, " + mutable + " (make it constant, or register its kind)");
                } else if (!java.lang.reflect.Modifier.isPrivate(f.getModifiers())) {
                    violations.add(id + " (" + kind + ") is not private: another class could read or write it");
                }
            }
        }
        java.util.Set<String> gone = new java.util.TreeSet<>(STATIC_STATE.keySet());
        gone.removeAll(seen);
        for (String id : gone) {
            if (violations.stream().noneMatch(v -> v.startsWith(id + " "))) {
                violations.add(id + " is registered but no longer exists (shrink the register)");
            }
        }
        org.junit.jupiter.api.Assertions.assertTrue(violations.isEmpty(),
                "no static state but the register's (constant, sealed, or registered by kind):\n  "
                        + String.join("\n  ", violations));
    }

    /** Why {@code v} is mutable, or null for a constant. */
    private static @com.legend.base.Nullable String mutableObject(@com.legend.base.Nullable Object v, int depth) {
        // (deep enough for a registry of registries: depth 4, the follow-up audit of 2026-11-09 -- 2 until then)
        if (v == null || depth > 4) {
            return null;
        }
        Class<?> k = v.getClass();
        String n = k.getName();
        if (v instanceof com.legend.base.Sealed.Seal seal) {
            return seal.sealed() ? null : "an unsealed registry " + n;
        }
        if (k.isArray()) {
            return java.lang.reflect.Array.getLength(v) == 0 ? null : "an array " + k.getComponentType().getName();
        }
        // a holder first: an atomic counter is a Number (the guard's own gap, found on its first Bazel run, 2026-11-09)
        if (n.startsWith("java.util.concurrent.atomic.") || v instanceof ThreadLocal<?>
                || v instanceof StringBuilder) {
            return "a holder " + n;
        }
        // the JDK's own mutable values (the follow-up audit, 2026-11-09)
        if (v instanceof java.util.Random || v instanceof java.util.BitSet || v instanceof java.util.Date
                || v instanceof java.util.Calendar) {
            return "a mutable value " + n;
        }
        // a record and a lambda are as constant as what they hold: their fields are looked at below (the follow-up
        // audit, 2026-11-09: a record holding a list, a lambda capturing one, passed until then)
        if (k.isEnum() || v instanceof String || v instanceof Number || v instanceof Boolean
                || v instanceof Character || v instanceof Class) {
            return null;
        }
        if (v instanceof java.util.Collection<?> || v instanceof java.util.Map<?, ?>) {
            return n.startsWith("java.util.ImmutableCollections") || n.startsWith("java.util.Collections$Unmodifiable")
                    || n.startsWith("java.util.Collections$Empty") || n.startsWith("java.util.Collections$Singleton")
                    ? null : "a collection " + n;
        }
        if (n.startsWith("java.") || n.startsWith("javax.") || n.startsWith("jdk.") || n.startsWith("sun.")) {
            return null;
        }
        // the product's own object: mutable if it has a field that is not final, or holds something mutable
        for (Class<?> x = k; x != null && x != Object.class; x = x.getSuperclass()) {
            for (java.lang.reflect.Field f : x.getDeclaredFields()) {
                if (java.lang.reflect.Modifier.isStatic(f.getModifiers())) {
                    continue;
                }
                if (!java.lang.reflect.Modifier.isFinal(f.getModifiers())) {
                    return "an object " + n + " (its field " + f.getName() + " is not final)";
                }
                try {
                    f.setAccessible(true);
                    String inner = mutableObject(f.get(v), depth + 1);
                    if (inner != null) {
                        return "an object " + n + " (its field " + f.getName() + ": " + inner + ")";
                    }
                } catch (ReflectiveOperationException | RuntimeException e) {
                    return "an object " + n + " (its field " + f.getName() + " unreadable: " + e + ")";
                }
            }
        }
        return null;
    }

    /**
     * <strong>Invariant 7 — compiler work lives in the compiler.</strong>
     * Constructing (minting/rewriting) typed-HIR nodes IS compiler work:
     * it decides stamps and tree shape, the facts the whole stamp
     * discipline pins. Only the compiler layers — {@code compiler},
     * {@code resolver}, {@code normalizer}, {@code lowering} — may
     * construct {@code com.legend.compiler.spec.typed.*} nodes. Reading
     * (pattern-matching, dispatch) is fine anywhere: the executor
     * consumes the tree; it must not grow it.
     *
     * <p>ZERO exceptions BY DESIGN (user directive 2026-08-21: no pin
     * list, no mechanism a future change can quietly grow). The
     * original census found four violators, all evicted the same day —
     * the eviction patterns to reuse when execution-bound facts seem to
     * force a mint outside the compiler:
     * <ul>
     *   <li>REWRITE RULES move behind an SPI the executor implements —
     *       splice rules → {@code ResultEnvelopeSplice.Frames}
     *       (frame lookup / JDBC frame builds stay executor-side).</li>
     *   <li>β-machinery joins the one engine — {@code UserCallInliner}
     *       (bindStringParam, callArgumentFrame).</li>
     *   <li>Staged assembly splits pure-prepare / effects / pure-chain —
     *       {@code ExecuteChainAssembly} (the executor interleaves its
     *       execution-bound steps BETWEEN compiler calls).</li>
     *   <li>Misfiled passes just MOVE — {@code resolver.DriverPkAppend}.</li>
     *   <li>LATE BINDING is a plan step, never a compiler input: a raw
     *       grid's columns are a keyed plan's keys, read when it runs
     *       (the planner-driver seam program's S3c, 2026-10-30; until
     *       then {@code resolver.RawGridSchema} took GridProbe's probed
     *       roster through its {@code SchemaReferee}).</li>
     *   <li>Verdict-query synthesis is compiler emission —
     *       {@code VerdictQueries} builds; {@code AssertVerdicts}
     *       fetches and judges, minting nothing.</li>
     * </ul>
     */
    /**
     * T4.1 step 2 — the normalizer READS the one model index and writes
     * nothing into it: Phase E's facts (poisons, mixed unions, key
     * threads, the nullable census, the mapped-class set) ride its
     * products. The index's only mutators are the driver's gate calls
     * ({@code add}, {@code retainLegacySurface}); the five write channels
     * ({@code mappingPoisons}, {@code mixedUnions}, {@code unionKeyThreads},
     * {@code requiredNullableRows()}, {@code registerMappedClass}) no
     * longer exist, and this rule keeps the index free of public state so
     * they cannot come back as fields either.
     */
    @Test
    void normalizerNeverWritesIntoTheModelIndex() {
        noClasses()
            .that().resideInAPackage("com.legend.normalizer..")
            .should().callCodeUnitWhere(
                    com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                            com.tngtech.archunit.core.domain.properties.HasOwner
                                    .Predicates.With.<com.tngtech.archunit.core
                                            .domain.JavaClass>owner(
                                    com.tngtech.archunit.core.domain.JavaClass
                                            .Predicates.simpleName("ModelBuilder")))
                    .and(com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                            com.tngtech.archunit.core.domain.properties.HasName
                                    .Predicates.nameMatching(
                                            "add|retainLegacySurface|registerMappedClass"))))
            .as("T4.1 invariant 5: the normalizer performs no write into the model index")
            .check(CORE_PROD_CLASSES);
        com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noFields()
            .that().areDeclaredIn(com.legend.compiler.ModelBuilder.class)
            .should().bePublic()
            .as("T4.1 invariant 5: the model index exposes no field a phase could write")
            .check(CORE_PROD_CLASSES);
    }

    @Test
    void typedNodesAreMintedOnlyByCompilerLayers() {
        noClasses()
            .that().resideOutsideOfPackages(
                    "com.legend.compiler..",
                    "com.legend.resolver..",
                    "com.legend.normalizer..",
                    "com.legend.lowering..")
            .should().callCodeUnitWhere(
                    com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                            com.tngtech.archunit.core.domain.properties.HasName
                                    .Predicates.name("<init>"))
                    .and(com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                            com.tngtech.archunit.core.domain.properties.HasOwner
                                    .Predicates.With.<com.tngtech.archunit.core
                                            .domain.JavaClass>owner(
                                    com.tngtech.archunit.core.domain.JavaClass
                                            .Predicates.resideInAPackage(
                                                    "com.legend.compiler.spec.typed")))))
            .as("Invariant 7: typed-HIR nodes are minted only by the compiler"
                    + " layers (compiler/resolver/normalizer/lowering) — the"
                    + " pinned exceptions only shrink; see the rule javadoc")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>THE PROGRAM IS PARSED ONCE.</strong> Protocol nodes
     * ({@code com.legend.protocol..}: value specifications, type expressions,
     * multiplicities) are constructed by the PARSER (text &rarr; protocol), by
     * the NORMALIZER (protocol &rarr; protocol, the pre-typing phase) and by the
     * protocol package's own helpers &mdash; nowhere else. A driver, a judge, a
     * generator or a resolver that spells Pure by hand in AST form injects a
     * program below the parser (user, 2026-09-22: "nothing can create protocol
     * except the one place that is allowed").
     *
     * <p>The compiler-layer desugaring sites that construct protocol today are
     * the MEASURED DEBT in {@link #PROTOCOL_DESUGAR_DEBT} (a typer that rewrites
     * syntax while typing; the driver wrapping parsed statements into a query
     * lambda; the test runners spelling a test call / a parameter let). Each row
     * only shrinks (a shrink is recorded in the same commit); a class absent from
     * the map constructs NONE. Counted from bytecode (every constructor call whose
     * target is a protocol class, lambdas included), never from text.
     */
    private static final java.util.Map<String, Integer> PROTOCOL_DESUGAR_DEBT =
            new java.util.TreeMap<>(java.util.Map.ofEntries(
                    // MEASURED 2026-09-22 (the day the rule was pinned), 34 classes /
                    // 389 constructor calls. The typer and its checkers rewrite syntax
                    // while typing (the bulk); the driver wraps parsed statements into
                    // the query lambda (Compiler 2); the test runners spell the test
                    // call and a parameter let (2 + 2); the lineage and the model
                    // builder carry 2 + 4. Owed: each family moves into the parser or
                    // the normalizer, and its row goes to 0 in that commit. The
                    // driver's own hand-built view accessor (StatementExecutor, 4)
                    // was deleted the day this was pinned — it is not a row.
                    // RE-PINNED 2026-10-10 (L7 "resolve once" step 3): every AppliedFunction construction in
                    // compiler/, validation/ moved into Calls (179 by script, 25 by hand; the design note's
                    // §11); the rows below are the other protocol nodes those classes still build (lambdas,
                    // variables, column specs, collections) at the measured counts, shrink-only as before
                    java.util.Map.entry("com.legend.Compiler", 2),
                    java.util.Map.entry("com.legend.compiler.DerivedProps", 3),
                    java.util.Map.entry("com.legend.compiler.LiteralMapUnroll", 2),
                    java.util.Map.entry("com.legend.compiler.NameResolver", 30),
                    java.util.Map.entry("com.legend.compiler.StatementInline", 5),
                    java.util.Map.entry("com.legend.compiler.element.TypeClassifier", 1),
                    java.util.Map.entry("com.legend.compiler.spec.AlphaRename", 7),
                    java.util.Map.entry("com.legend.compiler.spec.CallShapes", 4),
                    // ADDED 2026-10-28 (PR #77's checks lane: two rows its S112 and S114 commits had grown, never run
                    // locally): a function generic over relations is expanded at each call, its parameters substituted,
                    // so its body's own column names are known only then -- ColumnHygiene renames them (a ColSpec, its
                    // array, a property read: 3; AlphaRename's binder rename at the same moment, the same shape) and
                    // ReadChecker typed `$r->read(~col)` by building the read `$r.col` (1). Neither could move before
                    // the typer while the expansion is the typer's (docs/SEMANTICS_REGISTER.md S112, S114).
                    // SHRUNK 2026-10-28 (the user, the same day: "fix this"): ReadChecker's row went -- it types the
                    // row and reads its column through Typer.rowColumn, the method `$r.col` itself types by, and
                    // builds nothing
                    java.util.Map.entry("com.legend.compiler.spec.ColumnHygiene", 3),
                    java.util.Map.entry("com.legend.compiler.spec.DistinctChecker", 1),
                    java.util.Map.entry("com.legend.compiler.spec.EvalChecker", 1),
                    java.util.Map.entry("com.legend.compiler.spec.ExtendChecker", 4),
                    java.util.Map.entry("com.legend.compiler.spec.FoldChecker", 3),
                    java.util.Map.entry("com.legend.compiler.spec.FromChecker", 2),
                    java.util.Map.entry("com.legend.compiler.spec.GroupByChecker", 18),
                    java.util.Map.entry("com.legend.compiler.spec.GroupLambdaAggs", 14),
                    java.util.Map.entry("com.legend.compiler.spec.IfChecker", 1),
                    java.util.Map.entry("com.legend.compiler.spec.IsDistinctChecker", 3),
                    java.util.Map.entry("com.legend.compiler.spec.JoinChecker", 16),
                    java.util.Map.entry("com.legend.compiler.spec.JsonChecker", 11),
                    java.util.Map.entry("com.legend.compiler.spec.LambdaBodies", 3),
                    java.util.Map.entry("com.legend.compiler.spec.NewChecker", 1),
                    java.util.Map.entry("com.legend.compiler.spec.Overloads", 2),
                    java.util.Map.entry("com.legend.compiler.spec.ProjectChecker", 15),
                    java.util.Map.entry("com.legend.compiler.spec.SortChecker", 8),
                    java.util.Map.entry("com.legend.compiler.spec.SourceSubst", 10),
                    java.util.Map.entry("com.legend.compiler.spec.StaticFold", 12),
                    java.util.Map.entry("com.legend.compiler.spec.TdsDesugars", 58),
                    java.util.Map.entry("com.legend.compiler.spec.TdsNullForms", 1),
                    java.util.Map.entry("com.legend.compiler.spec.TypeAnnotations", 1),
                    java.util.Map.entry("com.legend.compiler.spec.Typer", 17),
                    java.util.Map.entry("com.legend.lineage.ScanRelations", 2),
                    java.util.Map.entry("com.legend.model.MappingFromProtocol", 4),
                    java.util.Map.entry("com.legend.test.PureTestRunner", 2),
                    java.util.Map.entry("com.legend.test.ServiceTestRunner", 2),
                    java.util.Map.entry("com.legend.validation.ValidateDesugar", 22)
            ));

    /**
     * <strong>P2, one constructor (docs/DATA_IN_THE_PLAN_2026_11_09.md 6; turned on 2026-11-09 after the M1 tenets
     * audit's finding 3).</strong> A plan's {@code ExecutionPlan.Load} is made by {@code planner.LoadPlanner} alone, with
     * its one identity scheme; the plan's JSON reader rebuilds what a writer wrote. (Until L4a, 2026-11-09,
     * {@code planner.SystemRoutes}' system store load was a dated exception, named by a constant; it is the load
     * planner's now, named by its content.) Its steps are guarded by {@link #onlyThePlannersMakersMakeALoadStep}, exec
     * by {@link #execMakesNoLoad}. {@code LoadPlanner.load} is public: the test planner's load makers
     * ({@code testplan.EffectPlanner}, {@code testplan.TestKinds}) call it across the package line; what this guards is
     * that every load takes the one constructor call and the one identity scheme.
     */
    @Test
    void onlyTheLoadPlannerMakesALoad() {
        noClasses()
            .that().resideInAPackage("com.legend..")
            .and().doNotHaveFullyQualifiedName("com.legend.planner.LoadPlanner")
            .and().doNotHaveFullyQualifiedName("com.legend.executionplan.PlanJson")
            .should().callConstructorWhere(com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                    com.tngtech.archunit.core.domain.properties.HasOwner.Predicates.With.owner(
                            com.tngtech.archunit.core.domain.properties.HasName.Predicates
                                    .name("com.legend.executionplan.ExecutionPlan$Load"))))
            .as("P2: a plan's load is made by the load planner alone (one identity scheme); the JSON reader rebuilds")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>P2, a load's steps (the tenets re-audit's A-5, 2026-11-09: until then named in a javadoc, guarded only
     * inside exec).</strong> A plan's {@code LoadStep} -- a statement, rows, a file and its delivery -- is made by the
     * planner's makers of statements and rows alone: {@code setup.CsvSeed}, {@code setup.Ddl}, {@code setup.RowLoad},
     * {@code setup.LoadSteps} (2026-11-10, A2), {@code planner.PlanMaker}, {@code planner.SystemCatalog},
     * {@code planner.DataSources} (2026-11-10, L6b),
     * {@code testplan.EffectPlanner}; the plan's JSON reader rebuilds what a writer wrote.
     */
    @Test
    void onlyThePlannersMakersMakeALoadStep() {
        noClasses()
            .that().resideInAPackage("com.legend..")
            // planner.DataSources since 2026-11-10 (L6b: a query's data: sources as one load into session tables);
            // planner.ContentRows the same day (L6c: the model's rows inline or from the load's bundle), replaced the
            // same day by setup.LoadSteps (A2, docs/DATA_DELIVERY_DESIGN_2026_11_10.md: every table's rows a content
            // blob of statements, made in one place)
            .and().haveNameNotMatching("com\\.legend\\.(setup\\.(CsvSeed|Ddl|RowLoad|LoadSteps)|planner\\.(PlanMaker|SystemCatalog|DataSources)"
                    + "|testplan\\.EffectPlanner|executionplan\\.PlanJson)(\\$.*)?")
            .should().callConstructorWhere(com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                    com.tngtech.archunit.core.domain.properties.HasOwner.Predicates.With.owner(
                            com.tngtech.archunit.core.domain.properties.HasName.Predicates
                                    .nameMatching("com\\.legend\\.executionplan\\.ExecutionPlan\\$LoadStep\\$.*"))))
            .as("P2: a load's steps are made by the planner's makers of statements and rows; the JSON reader rebuilds")
            .check(CORE_PROD_CLASSES);
    }

    /** Exec makes no load and no load step: it runs the plan's (the wall's data half; P2, 2026-11-09). */
    @Test
    void execMakesNoLoad() {
        noClasses()
            .that().resideInAnyPackage("com.legend.exec..", "com.legend.testexec..")
            .should().callConstructorWhere(com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                    com.tngtech.archunit.core.domain.properties.HasOwner.Predicates.With.owner(
                            com.tngtech.archunit.core.domain.properties.HasName.Predicates
                                    .nameMatching("com\\.legend\\.executionplan\\.ExecutionPlan\\$(Load|LoadStep\\$.*)"))))
            .as("P2: exec runs a plan's loads and makes none")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>L7 "resolve once", step 3 (2026-10-10).</strong> A call built after the name resolver is born with
     * its record, so no reader works a name out from a spelling: the typer, its checkers and desugars, the
     * normalizer, validation, lineage and test-data generation construct an {@code AppliedFunction} only through
     * {@code Calls} (the parser and the protocol readers build calls before resolution; the resolver records).
     */
    @Test
    void callsAreBuiltOnlyByTheBuilder() {
        noClasses()
            .that().resideInAPackage("com.legend..")
            // the parser and the protocol readers build calls before resolution; the test runners spell a test
            // call before resolveQuery; the resolver records; the builder is the one place after the resolver
            .and().resideOutsideOfPackages("com.legend.parser..", "com.legend.protocol..", "com.legend.test..")
            .and().haveNameNotMatching("com\\.legend\\.compiler\\.(spec\\.Calls|NameResolver)(\\$.*)?")
            .should().callConstructorWhere(com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                    com.tngtech.archunit.core.domain.properties.HasOwner.Predicates.With.owner(
                            com.tngtech.archunit.core.domain.properties.HasName.Predicates
                                    .name("com.legend.protocol.spec.AppliedFunction"))))
            .orShould().callMethodWhere(com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                    com.tngtech.archunit.core.domain.properties.HasOwner.Predicates.With.owner(
                            com.tngtech.archunit.core.domain.properties.HasName.Predicates
                                    .name("com.legend.protocol.spec.AppliedFunction")))
                    .and(com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                            com.tngtech.archunit.core.domain.properties.HasName.Predicates
                                    .nameMatching("infixRun|list|tableReference"))))
            .as("L7 step 3: a call built after the resolver is built by com.legend.compiler.spec.Calls, with its"
                    + " record; no other class constructs an AppliedFunction or calls its parser-side factories"
                    + " (infixRun, list, tableReference), the parser, the protocol readers and the test runners"
                    + " excepted (they build before resolution) and the resolver excepted (it records)")
            .check(CORE_PROD_CLASSES);
    }

    @Test
    void protocolNodesAreConstructedOnlyByTheParserAndTheNormalizer() {
        java.util.Map<String, Integer> actual = new java.util.TreeMap<>();
        for (com.tngtech.archunit.core.domain.JavaClass c : CORE_PROD_CLASSES) {
            String pkg = c.getPackageName();
            if (pkg.equals("com.legend.parser") || pkg.startsWith("com.legend.parser.")
                    || pkg.equals("com.legend.normalizer") || pkg.startsWith("com.legend.normalizer.")
                    || pkg.equals("com.legend.protocol") || pkg.startsWith("com.legend.protocol.")
                    // the one builder of calls after the resolver (L7 "resolve once" step 3, 2026-10-10;
                    // callsAreBuiltOnlyByTheBuilder keeps every other constructor call of a call out)
                    || c.getName().equals("com.legend.compiler.spec.Calls")) {
                continue;
            }
            int n = 0;
            for (com.tngtech.archunit.core.domain.JavaConstructorCall call
                    : c.getConstructorCallsFromSelf()) {
                String target = call.getTargetOwner().getPackageName();
                if (target.equals("com.legend.protocol") || target.startsWith("com.legend.protocol.")) {
                    n++;
                }
            }
            if (n > 0) {
                // nested and anonymous classes count against their top-level class
                actual.merge(c.getName().split("\\$")[0], n, Integer::sum);
            }
        }
        org.junit.jupiter.api.Assertions.assertEquals(PROTOCOL_DESUGAR_DEBT, actual,
                "protocol-node construction outside the parser / normalizer / protocol"
                + " packages drifted: GROWTH is a new program spelled by hand below the"
                + " parser — build it in the parser or the normalizer instead; SHRINKAGE"
                + " means a desugaring site went home — ratchet the row down in the same"
                + " commit");
    }
}
