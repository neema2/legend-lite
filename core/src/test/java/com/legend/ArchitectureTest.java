package com.legend;

import com.legend.driver.GridProbe;

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
     * them (every core library, //base, //json and {@code :duckdb_load}; Bazel workplan P3-27). Nothing is found from
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
            // D71, 2026-10-30: the judge layer, out of exec
            java.util.Map.entry("judge", "com.legend.judge.Equality"),
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
            java.util.Map.entry("result", "com.legend.result.Batch"),
            java.util.Map.entry("planner", "com.legend.Compiler"),
            java.util.Map.entry("pure_v1", "com.legend.server.PureV1Api"),
            java.util.Map.entry("driver", "com.legend.Execution"),
            java.util.Map.entry("ide", "com.legend.ide.ModelIndex"),
            java.util.Map.entry("test", "com.legend.test.PureTestRunner"),
            java.util.Map.entry("server_lib", "com.legend.server.ConnectionResolver"),
            java.util.Map.entry("duckdb_load", "com.legend.exec.DuckDbAppenderLoad"),
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
            .and().resideOutsideOfPackages("com.legend.driver", "com.legend.planner")
            // the server shell is a driver CONSUMER (HTTP/LSP/diagram on top
            // of Compiler) — a top-layer sibling, not a phase
            .and().resideOutsideOfPackage("com.legend.server..")
            // the product's test runner (batch 7a) is a driver CONSUMER too:
            // it discovers tests in a model and runs them through Compiler
            .and().resideOutsideOfPackage("com.legend.test..")
            .and().resideInAPackage("com.legend..")
            .should().dependOnClassesThat().belongToAnyOf(
                    com.legend.Compiler.class,
                    com.legend.driver.BodyDriver.class)
            .as("Invariant 4c: the root's doors, the planner and the driver are the top"
                    + " layer — audit 19; step C, D73")
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
            // the root layer's execution side is com.legend.driver since step C (D73, 2026-10-30: EngineText, the
            // engine-text registry, lives there); the root's doors reach no engine-style renderer, so the root's own
            // exemption, which the rule carried since audit 19, named nothing and left (the D73 audit)
            .that().resideOutsideOfPackage("com.legend.driver")
            .and().resideInAPackage("com.legend..")
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
            .that().resideOutsideOfPackages("com.legend.driver", "com.legend.sql.dialect", "com.legend.testdatagen")
            .and().resideInAPackage("com.legend..")
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
                            .or(NULLNESS_ANNOTATIONS))
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
     * <strong>The driver has one caller (step C, D73, 2026-10-30).</strong> {@code BodyDriver} was package-private by
     * design while it shared the root package with the execution door; the driver's machinery is its own package now
     * ({@code com.legend.driver}), so the class is public for exactly the caller the design named,
     * {@code Execution.runProgram}: no other class outside the driver depends on it.
     */
    @Test
    void theDriverHasOneCaller() {
        noClasses()
            .that().resideOutsideOfPackage("com.legend.driver")
            .and().doNotHaveFullyQualifiedName("com.legend.Execution")
            .should().dependOnClassesThat().haveNameMatching("com\\.legend\\.driver\\.BodyDriver(\\$.*)?")
            .as("step C: the body driver is the execution door's alone (Execution.runProgram)")
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
            .should().dependOnClassesThat().resideInAnyPackage("com.legend.driver", "com.legend.exec..", "com.legend.judge")
            .as("step C: planning never touches the driver, the runtime or the judge")
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
        String runner = "com\\.legend\\.(exec\\.(PlanRunner|PlanParameters|PlanSessions|BulkLoads?)|result\\.RowDecode)"
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
     *  root and the judge open to them. */
    private static final java.util.Set<String> EXEC_MAY_REACH = java.util.Set.of("com.legend.exec", "com.legend.base",
            "com.legend.json", "com.legend.cache", "com.legend.diagnostics", "com.legend.error",
            "com.legend.executionplan", "com.legend.model", "com.legend.result", "com.legend.sql", "com.legend.values");

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
                    // GridProbe at the root since 2026-10-30 (D71: the probes out of exec)
                    "com\\.legend\\.(driver\\.GridProbe|lowering\\.Lowerer|sql\\.AliasPrefix|executionplan\\.MirJson)(\\$.*)?")
            .should().callConstructorWhere(
                    com.tngtech.archunit.core.domain.JavaCall.Predicates
                            .target(com.tngtech.archunit.core.domain
                                    .properties.HasOwner.Predicates.With
                                    .owner(com.tngtech.archunit.core
                                            .domain.properties.HasName
                                            .Predicates.name(
                                                    "com.legend.sql.SqlSource$RawSql"))))
            .as("Phase 1 quarantine: SqlSource.RawSql is constructed"
                    + " ONLY by the chartered seams (GridProbe's"
                    + " LIMIT-0 probe + the Lowerer TypedRawSqlRelation"
                    + " case) — it carries authored text, never"
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
     * ({@code BodyDriver}, {@code BodyCompiler}), the judges and their arms, the seed and error natives and the
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
            // the probes with them at the root (ColumnProbe, GridProbe, WireProbe, DynamicPivot: a probe asks the
            // plan runner, never a connection)
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
     * statements and counted. The schema probes ({@code ColumnProbe}, {@code GridProbe}, {@code WireProbe},
     * {@code DynamicPivot}) ask the runner ({@code columns}, {@code describe}); the seeds, a program's session
     * statements and the system store's rows are setup steps it runs ({@code setUp}); the transaction bracket is the
     * session's own ({@code Sessions.Session.inTransaction}). The session owners ({@code PlanSessions},
     * {@code Sessions}) and the implementations ({@code JdbcStatements}, MongoDB's) are the seam itself. No other
     * class calls a method of {@code Statements}, and none takes a session's statements
     * ({@code Sessions.Session.statements}).
     */
    @Test
    void theRunnerIsTheOneCallerOfTheSessionsStatements() {
        String seam = "com\\.legend\\.exec\\.(PlanRunner|PlanSessions|Sessions|JdbcStatements|MongoDbStatements)(\\$.*)?";
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
                    return call.getTarget().getOwner().isAssignableTo(com.legend.exec.Sessions.Session.class)
                            && call.getTarget().getName().equals("statements");
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

    @Test
    void hostVerdictIsReachableOnlyFromTheVerdictSeam() {
        noClasses()
            .that().doNotHaveFullyQualifiedName("com.legend.driver.AssertVerdicts")
            // task #27 (2026-09-21): the verdict seam's two arms — the host judge
            // (Java compare over database rows) and the database judge (the verdict
            // statement) — split out of the router
            .and().doNotHaveFullyQualifiedName("com.legend.driver.HostJudge")
            .and().doNotHaveFullyQualifiedName("com.legend.driver.DatabaseJudge")
            .and().doNotHaveFullyQualifiedName("com.legend.driver.AssertErrorNative")
            .and().doNotHaveFullyQualifiedName("com.legend.judge.PureAsserts")
            .and().doNotHaveFullyQualifiedName("com.legend.judge.TdsCompare")
            .and().doNotHaveFullyQualifiedName("com.legend.judge.CanonicalForm")
            .and().doNotHaveFullyQualifiedName("com.legend.judge.CanonicalDivergence")
            // testdatagen composes seed SOURCE TEXT via the ONE repr
            // owner — spelling, never an equality decision
            .and().doNotHaveFullyQualifiedName("com.legend.testdatagen.TestDataGenerator")
            // (exec.JsonCompare, the one wire-tree walker of P2-4/P2-6, was exempted here until 2026-10-30: the class
            // was deleted long before -- the ledger's Equality row records it gone -- and the D71 audit found the
            // exemption naming nothing; deleted, a dated note)
            // JUDGING_TWO_MODES step 2: the host judge and the service-test
            // judge are the seam's own (Equality decides; TestAssertions
            // routes EqualToJson to it)
            .and().doNotHaveFullyQualifiedName("com.legend.judge.Equality")
            .and().doNotHaveFullyQualifiedName("com.legend.test.TestAssertions")
            // leg 3.4 (2026-09-20): the verdict seam's SENDING arm — the
            // body's deferred verdict statements as one statement; it
            // counts the census rows the arm counted at the assert
            // (raised, unjudged) and decides nothing (the row's judgment
            // is injected from AssertVerdicts)
            .and().doNotHaveFullyQualifiedName("com.legend.judge.VerdictBatch")
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
     * {@link #staticCollectionStateIsImmutableOrRegistered}, and the rest is the runner's to remove (W6.2).
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

    @Test
    void staticCollectionStateIsImmutableOrRegistered() throws Exception {
        java.util.Set<String> register = java.util.Set.of(
                // CENSUS (block-compiler homework 2026-09-21): the fused
                // statement's fallback reasons, appended by the batch's split
                // rung, read by the corpus lanes only — no verdict reads it
                // THE ONE CENSUS OWNER (cleanup move 3, 2026-09-22): every count the lanes
                // print or pin, one storage, one snapshot — read by the lanes and the
                // divergence report only, never by a verdict
                "com.legend.exec.Census.COUNTS",
                "com.legend.exec.Census.KEYED",
                "com.legend.exec.Census.FALLBACK_REASONS",
                // write-once static-init tables (populated once in <clinit>,
                // read-only thereafter) — burn-down: wrap immutable
                "com.legend.lexer.Lexer.KEYWORDS",
                "com.legend.lowering.Windows.FNS",
                "com.legend.lowering.Windows.AGGREGATES",
                "com.legend.lowering.Scalars.RULES",
                "com.legend.lowering.FeatureRules.UNDER",
                "com.legend.lowering.Aggregates.REDUCERS",
                "com.legend.platform.CoreFn.BY_NAME",
                "com.legend.builtin.Pure.ALL_CLASSES",
                "com.legend.builtin.Pure.ALL_ENUMS",
                "com.legend.builtin.Pure.ALL",
                "com.legend.builtin.Pure$Index.CLASS_BY_FQN",
                "com.legend.builtin.Pure$Index.ENUM_BY_FQN",
                "com.legend.builtin.Pure$Index.FN_BY_FQN",
                "com.legend.builtin.Pure$Index.FN_BY_ID",
                "com.legend.builtin.Pure$Index.USER_RESOLVABLE_FQNS",
                // the step-3 shadow diff's sinks (a probe, deleted at step 4)
                "com.legend.probe.Shadow.SEEN",
                "com.legend.probe.Shadow.TABLES",
                "com.legend.probe.Shadow.ROWS",
                "com.legend.compiler.NameResolver.PRELUDE_TYPES",
                "com.legend.compiler.NameResolver.PRELUDE_COLLISIONS",
                "com.legend.parser.SectionGrammarRegistry.REGISTRY",
                // warn-once diagnostic suppression set: genuinely mutable
                // runtime state, bounded by the model's FQN universe
                "com.legend.compiler.element.FunctionCompiler.SUPPRESSED_ONCE",
                // live diagnostic ledger (Phase 0 honesty instrument):
                // accumulates timings at runtime; content-addressing is
                // meaningless for a metrics sink
                // per-test wall of the same ledger (batch 8: the corpus
                // dump names its 30 slowest tests)
                // R1 divergence instrument (CANONICAL_FORM_SPEC §0):
                // bounded witness sample for the harness-published
                // table; measurement only, never verdict-affecting
                "com.legend.judge.CanonicalDivergence.SAMPLES",
                // V7 dual-channel census (V7_ASSERT_VERDICT_CHARTER
                // §4.1): per-form harness-vs-production verdict counts,
                // bounded classified declines, bounded disagreement
                // witnesses; measurement only — the CanonicalDivergence
                // pattern
                // reserved dual-verdict ALARM witness buffer (bounded
                // 50) — the alarm row must never lose its witness to
                // shared-sample crowding; measurement only
                "com.legend.judge.CanonicalDivergence.SQL_DISAGREE_SAMPLES",
                // §8.3b wobble burn: the BYTE channel's pinned-census
                // witnesses get the same reserved buffer — a ±1 count
                // wobble was unattributable because its rows sat past
                // the shared 200-cap (decline-row crowding, the
                // SQL_DISAGREE_SAMPLES lesson re-learned); bounded by
                // the exact-pinned disagree count (23); measurement only
                "com.legend.judge.CanonicalDivergence.DISAGREE_SAMPLES",
                "com.legend.judge.CanonicalDivergence.V7_FORMS",
                // leg 3.0 (DATABASE_MODE_HOMEWORK §4a): the SQL canon's
                // CLAIM / DECLINE census per assert family and reason —
                // bounded by families × decline-reason heads (~40 keys);
                // measurement only, printed by the corpus lanes, never
                // read by a verdict
                "com.legend.judge.CanonicalDivergence.SQL_CENSUS",
                "com.legend.judge.CanonicalDivergence.V7_DECLINES",
                "com.legend.judge.CanonicalDivergence.V7_SAMPLES",
                // step-0 residue census (FULL_RESIDUE_CENSUS_2026_08_30):
                // per-ROW decline attribution, UNCAPPED by census
                // doctrine but bounded by the sweep's own decline count
                // (~420); measurement only, never verdict-affecting
                "com.legend.judge.CanonicalDivergence.V7_DECLINE_WITNESSES",
                // slice-1 quarantine channel move (census §10h): the
                // wall-surfaced quarantine tests, vocabulary-matched —
                // bounded by the partition's own size (20 today);
                // measurement only, never verdict-affecting
                "com.legend.judge.CanonicalDivergence.QUARANTINED_WALL_TESTS",
                // SQLTEXT slice 3a: the text-emission census's
                // per-reason text-verdict tally (foreign dialects,
                // replay declines) — bounded by decline-reason
                // cardinality; measurement only, never
                // verdict-affecting (the CanonicalDivergence pattern)
                // TYPED-IR Slice 1: the label-lie census's classified
                // counters (declared-vs-computed pair -> count);
                // measurement only, never verdict-affecting — the
                // CanonicalDivergence pattern
                "com.legend.judge.SqlTypeCensus.CLASSES",
                // T3: bounded per-class WITNESSES (3 max each) — the
                // emission-seam locator; measurement only
                "com.legend.judge.SqlTypeCensus.SAMPLES",
                // §E3 M-N1: the nullability differential's classified
                // counters + bounded witnesses (fact-vs-label nullable
                // — the M-N3 flip payload); measurement only, the
                // §4AD navigation-arm census: runtime accumulation,
                // dumped by the corpus runner (NAV_ARM_CENSUS_4AD.md)
                // SqlTypeCensus pattern (runtime accumulation is the
                // legitimate static-census shape)
                "com.legend.judge.SqlTypeCensus.NUL_CLASSES",
                "com.legend.judge.SqlTypeCensus.NUL_SAMPLES",
                // (the §E3 slack census's SLACK_CLASSES and SLACK_SAMPLES LEFT this allowlist 2026-10-29: deleted
                // with the wire census's arm by the doors' audit, D64 -- callerless since the plan runner)
                // the primitive census (2026-10-12, the coverage map, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §20): a
                // tally per MIR primitive by outcome, bounded by the MIR's vocabulary; written only under
                // -Dlegend.diagnostics=primitives; measurement only, never verdict-affecting (the SqlTypeCensus
                // pattern)
                "com.legend.judge.PrimitiveCensus.TALLY",
                // and its per-test sets under primitives=per-test (the same day): bounded by tests x vocabulary
                "com.legend.judge.PrimitiveCensus.TESTS",
                // com.legend.exec.SessionState.OF LEFT this register 2026-10-30 (D70, the seam's step A): the
                // process-wide weak map keyed by the driver's connection is gone -- the state is the owning session's
                // own, shared by its views (Sessions.Session.state()); a product session owns its connection
                // (theProductBorrowsNoConnection), so nothing is wrapped as a session anew at every call any more
                // the bare-name rule's table (L7 "resolve once", step 3, 2026-10-10): the names a bare
                // call may mean at an arity, computed from static tables (the engine surface, the core
                // import group, the forms, the catalog) and kept per (name, arity) once computed; never
                // cleared, never keyed by anything but those inputs
                "com.legend.compiler.BareNames.REFERENTS",
                // the resolver's answers for built calls (L7 step 3, 2026-10-10): per (name, arity) under the
                // sectionless-query scope, from the same static inputs; never cleared
                "com.legend.compiler.NameResolver.BUILT");
        java.util.List<String> violations = new java.util.ArrayList<>();
        for (com.tngtech.archunit.core.domain.JavaClass jc : CORE_PROD_CLASSES) {
            if (jc.getPackageName().startsWith("com.legend.cache")) {
                continue;
            }
            Class<?> cls;
            try {
                cls = Class.forName(jc.getName());
            } catch (Throwable t) {
                continue; // unloadable in the test JVM — nothing to inspect
            }
            for (java.lang.reflect.Field f : cls.getDeclaredFields()) {
                if (!java.lang.reflect.Modifier.isStatic(f.getModifiers())
                        || !(java.util.Map.class.isAssignableFrom(f.getType())
                                || java.util.Collection.class.isAssignableFrom(f.getType()))) {
                    continue;
                }
                String id = cls.getName() + "." + f.getName();
                if (register.contains(id)) {
                    continue;
                }
                f.setAccessible(true);
                Object v = f.get(null);
                if (v == null || !isKnownImmutable(v)) {
                    violations.add(id + " holds "
                            + (v == null ? "null" : v.getClass().getName()));
                }
            }
        }
        org.junit.jupiter.api.Assertions.assertTrue(violations.isEmpty(),
                "Invariant 3 (field level): static collection state outside "
                        + "com.legend.cache must be immutable or registered "
                        + "with justification:\n  "
                        + String.join("\n  ", violations));
    }

    private static boolean isKnownImmutable(Object v) {
        String n = v.getClass().getName();
        return n.startsWith("java.util.ImmutableCollections")
                || n.startsWith("java.util.Collections$Unmodifiable")
                || n.startsWith("java.util.Collections$Empty")
                || n.startsWith("java.util.Collections$Singleton");
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
     *   <li>LATE BINDING is staged compilation — the runtime fact
     *       becomes an INPUT: {@code resolver.RawGridSchema} takes the
     *       probed roster through its {@code SchemaReferee};
     *       {@code GridProbe} (at the root since D71, 2026-10-30) is the executor's referee.</li>
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
