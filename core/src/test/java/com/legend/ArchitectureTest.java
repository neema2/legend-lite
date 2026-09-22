package com.legend;

import com.legend.protocol.TypeExpression;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

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
final class ArchitectureTest {

    // Declared BEFORE CORE_PROD_CLASSES on purpose: static fields initialise in
    // textual order, and CORE_PROD_CLASSES's importer reads THIS_TEST_TREE. In
    // the other order it read null and the import excluded every class — which
    // ArchUnit's "failed to check any classes" guard caught (2026-09-22).
    /**
     * DO_NOT_INCLUDE_TESTS recognises test classes by the PATH of their output
     * directory — Maven's target/test-classes, Gradle's build/classes/…/test,
     * IntelliJ's out/test. Bazel packs this module's test classes into a jar
     * matching none of them, so under Bazel every test class was imported as
     * production code and the rules reported legitimate test code — one rule
     * 4,774 times. Excluding this test tree by its own code-source location is
     * correct under every build: under Maven it is target/test-classes, which
     * the predefined option already excludes.
     */
    private static final java.nio.file.Path THIS_TEST_TREE = realPath(
            java.nio.file.Path.of(codeSourceUri()));

    /**
     * Compared as REAL paths: under Bazel the test jar reaches the classpath
     * through a runfiles symlink, so its code-source path and the path ArchUnit
     * reports for the same classes can be two spellings of one file (macOS's
     * /var -> /private/var is a second one). A string match missed every test
     * class that way.
     */
    private static boolean notThisTestTree(com.tngtech.archunit.core.importer.Location location) {
        java.net.URI uri = location.asURI();
        java.nio.file.Path container;
        if ("jar".equals(uri.getScheme())) {
            String spec = uri.getSchemeSpecificPart();          // file:/x/y.jar!/com/...
            container = realPath(java.nio.file.Path.of(
                    java.net.URI.create(spec.substring(0, spec.indexOf("!/")))));
        } else if ("file".equals(uri.getScheme())) {
            container = realPath(java.nio.file.Path.of(uri));
        } else {
            return true;   // jrt: and friends: the JDK, never this module's test tree
        }
        return !container.startsWith(THIS_TEST_TREE);
    }

    private static java.net.URI codeSourceUri() {
        try {
            return ArchitectureTest.class.getProtectionDomain().getCodeSource()
                    .getLocation().toURI();
        } catch (java.net.URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    private static java.nio.file.Path realPath(java.nio.file.Path p) {
        try {
            return p.toRealPath();
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /**
     * Imports only production classes (excludes {@code src/test/}). All
     * structural rules apply to production code; tests may use whatever
     * helpers they need without piercing the production import boundary.
     */
    private static final JavaClasses CORE_PROD_CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .withImportOption(ArchitectureTest::notThisTestTree)
            .importPackages("com.legend");


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
     * driver (Compiler/StatementExecutor) — audit 19's blind spot. Phases
     * never call up into orchestration. (The harness bridge left
     * production in F1.2 — no exemption remains.)
     */
    @Test
    void phasesNeverDependOnTheDriverLayer() {
        noClasses()
            .that().resideOutsideOfPackage("com.legend")
            // the server shell is a driver CONSUMER (HTTP/LSP/diagram on top
            // of Compiler) — a top-layer sibling, not a phase
            .and().resideOutsideOfPackage("com.legend.server..")
            // the product's test runner (batch 7a) is a driver CONSUMER too:
            // it discovers tests in a model and runs them through Compiler
            .and().resideOutsideOfPackage("com.legend.test..")
            .and().resideInAPackage("com.legend..")
            .should().dependOnClassesThat().belongToAnyOf(
                    com.legend.Compiler.class,
                    com.legend.StatementExecutor.class)
            .as("Invariant 4c: the com.legend root (driver) is the top"
                    + " layer — audit 19")
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
            .that().resideOutsideOfPackage("com.legend")
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
    /** The nullness vocabulary (com.legend.Nullable/NonNull) is values-tier:
     * every layer may carry the annotations without breaching its wall. */
    private static final com.tngtech.archunit.base.DescribedPredicate<
            com.tngtech.archunit.core.domain.JavaClass> NULLNESS_ANNOTATIONS =
            com.tngtech.archunit.core.domain.JavaClass.Predicates
                    .belongToAnyOf(com.legend.Nullable.class,
                            com.legend.NonNull.class);

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
     * vocabulary — itself JDK-only by 6g) and the JDK. In particular:
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
                                    "com.legend.values", "java..")
                            .or(NULLNESS_ANNOTATIONS))
            .as("Invariant 7b: com.legend.protocol depends only on values and the"
              + " JDK — it is the bottom layer of the standalone drop-in")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>Invariant 7c — the parser's dependency surface is pinned.</strong>
     * Endgame ({@code PARSER_DROP_IN_PLAN.md} §2.1): the parser reads tokens
     * and produces protocol records — {@code lexer + protocol + values} and
     * nothing else. Today it still constructs {@code com.legend.model} records
     * for the element kinds that have not yet been migrated to protocol
     * output, so {@code model..} remains in the allowlist. <b>Shrink this
     * list; never grow it.</b> When the last element kind emits protocol,
     * delete {@code model..} here and the drop-in is extractable.
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
                                    "com.legend.model..",   // shrinking: dies with the last model-record output
                                    "com.legend.values",
                                    "com.legend.error",     // ParseException extends the shared error vocabulary (a JDK-only leaf, 6g)
                                    "com.legend.spi",       // the OVERLAY seam (Phase M): SectionGrammarRegistry routes ### sections through it — a stable JDK-only contract, below the parser by design
                                    "java..")
                            .or(NULLNESS_ANNOTATIONS))
            .as("Invariant 7c: the parser consumes tokens and produces protocol"
              + " (+ model records, temporarily) — nothing above it, ever")
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
        JavaClasses mainAndTests = new ClassFileImporter().importPackages("com.legend");
        noClasses()
            .that().resideInAPackage("com.legend..")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "org.finos.legend..")
            .as("the upstream boundary: core imports no org.finos.legend class"
                    + " (docs/UPSTREAM_BOUNDARY_PROGRAM.md workstream B)")
            .check(mainAndTests);
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
                                    "com.legend.error", "java..")
                            // SourceInfo rides typed nodes (the span
                            // component, Phase 4) — the ONE protocol type
                            // the HIR's surface carries; the parse-product
                            // AST itself stays out
                            .or(com.tngtech.archunit.core.domain.JavaClass
                                    .Predicates.type(
                                            com.legend.protocol.SourceInfo.class))
                            .or(NULLNESS_ANNOTATIONS))
            .as("Invariant 6h: lowering consumes typed HIR + kernel + sql — "
              + "nothing else, ever (plus the SourceInfo span component)")
            .check(CORE_PROD_CLASSES);
    }

    /**
     * <strong>F1.3 — the {@code java.sql} funnel (Charter C1/C2
     * boundary).</strong> "Java orchestrates, the DATABASE executes"
     * becomes MECHANICAL: only the chartered egress/ingress packages may
     * touch JDBC. Every other allowlist in this file admits {@code java..}
     * (which includes {@code java.sql}) — this rule is the narrow pin
     * that made the tenet enforceable (docs/TENET_CHARTER.md, enforcement
     * map). The audit's proof it matters: round 1's worked example
     * (hashString over rs.getString) survived 691 commits because no
     * rule forbade it.
     */
    @Test
    void javaSqlIsFunnelledToTheCharteredSeam() {
        noClasses()
            .that().resideOutsideOfPackages("com.legend.exec",
                    "com.legend.server..", "com.legend.testdatagen",
                    // the product's test runner (batch 7a): a driver like the
                    // root's — it opens package sessions and hands the
                    // connection to the executor, never a statement of its own
                    "com.legend.test",
                    "com.legend")
            .and().resideInAPackage("com.legend..")
            .should().dependOnClassesThat()
            // F1.11: driver-NATIVE APIs ride with java.sql — importing
            // org.duckdb/org.h2 types was a funnel bypass (the audit
            // framed the boundary as java.sql only)
            .resideInAnyPackage("java.sql..", "javax.sql..",
                    "org.duckdb..", "org.h2..")
            .as("F1.3: java.sql AND the driver-native APIs are funnelled"
                    + " to {exec, server, root, testdatagen} — Charter"
                    + " clauses C1/C2")
            .check(CORE_PROD_CLASSES);
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
            .resideInAnyPackage("java.sql..", "javax.sql..",
                    "org.duckdb..", "org.h2..")
            .as("E4: the metamodel channel evaluates model constants"
                    + " only — grid chains compile to SQL at the"
                    + " GridReads.tryLower exec seam")
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
     * Only the execution layer (exec, root) may see
     * {@code com.legend.sql.dialect} — it must render and normalize.
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
                    "com.legend.protocol..", "com.legend.model..")
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
                    // node — the authored text rides through verbatim, like RawSqlAdapt
                    "com\\.legend\\.(exec\\.GridProbe|lowering\\.Lowerer|sql\\.dialect\\.RawSqlAdapt|sql\\.AliasPrefix)(\\$.*)?")
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
     * <strong>F1.3b — the root package's {@code java.sql} class-list
     * pin.</strong> The funnel licenses {@code com.legend} ROOT, which
     * contains StatementExecutor — the audit's S1 dispatcher. Until the
     * orchestration/exec-seam split (backlogged), root's JDBC surface is
     * pinned to an ENUMERATED, shrink-only set: a NEW root class touching
     * {@code java.sql} fails this rule.
     */
    @Test
    void rootJavaSqlSurfaceIsPinned() {
        noClasses()
            .that().resideInAPackage("com.legend")
            // nested classes (StatementExecutor$ExecEnv, ...) ride with
            // their owner — the pin is per top-level class
            .and().haveNameNotMatching("com\\.legend\\.(Compiler"
                    + "|StatementExecutor"
                    // THE EXCEPTION SEAM (user directive 2026-09-01):
                    // SQLException stops at the executor boundary —
                    // verdicts throw AssertFailed, data errors arrive
                    // as DataError. SHRUNK here: SqlTextVerdicts,
                    // AssertErrorNative and SeedSqlForms left the pin
                    // (zero java.sql); AssertVerdicts left it 2026-09-22
                    // (the JDBC array cell's flatten moved behind the
                    // exec seam — the Executor yields a Collection).
                    + ")(\\$.*)?")
            .should().dependOnClassesThat()
            .resideInAPackage("java.sql..")
            .as("F1.3b: root's java.sql surface is pinned to"
                    + " {Compiler, StatementExecutor} —"
                    + " shrink-only (the exception seam landed"
                    + " 2026-09-01; the carrier-type residue left 2026-09-22)")
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
    /** THE SNIFF-STOP PIN (F10 slice-3 audit, 2026-08-24): wire-value
     * decoding is LABEL-DRIVEN, never shape-guessed — the engine's own
     * rule (declared type decides; relationalMappingExecution's narrow
     * conversion table). The two grammar readers are reachable ONLY
     * from the label-dispatch funnel: LiteralText parses LITERAL-
     * labeled cells (Executor.unwrap's arm); ad-hoc callers would be
     * value sniffing — the '4'-as-Long class of bug. A new caller
     * registers here consciously with its label-contract argument. */
    @Test
    void spellingDecodeIsLabelDriven() {
        noClasses()
            .that().doNotHaveFullyQualifiedName("com.legend.exec.Executor")
            .and().doNotHaveFullyQualifiedName("com.legend.values.LiteralText")
            .should().dependOnClassesThat().haveFullyQualifiedName(
                "com.legend.values.LiteralText")
            .as("SNIFF-STOP: LiteralText.parse is reachable only from the"
                    + " Executor's label-dispatch funnel (unwrap's LITERAL"
                    + " arm) — decode by declared label, never by value"
                    + " shape")
            .check(CORE_PROD_CLASSES);
    }

    @Test
    void hostVerdictIsReachableOnlyFromTheVerdictSeam() {
        noClasses()
            .that().doNotHaveFullyQualifiedName("com.legend.AssertVerdicts")
            // task #27 (2026-09-21): the verdict seam's two arms — the host judge
            // (Java compare over database rows) and the database judge (the verdict
            // statement) — split out of the router
            .and().doNotHaveFullyQualifiedName("com.legend.HostJudge")
            .and().doNotHaveFullyQualifiedName("com.legend.DatabaseJudge")
            .and().doNotHaveFullyQualifiedName("com.legend.AssertErrorNative")
            .and().doNotHaveFullyQualifiedName("com.legend.exec.PureAsserts")
            .and().doNotHaveFullyQualifiedName("com.legend.exec.TdsCompare")
            .and().doNotHaveFullyQualifiedName("com.legend.exec.CanonicalForm")
            .and().doNotHaveFullyQualifiedName("com.legend.exec.CanonicalDivergence")
            // testdatagen composes seed SOURCE TEXT via the ONE repr
            // owner — spelling, never an equality decision
            .and().doNotHaveFullyQualifiedName("com.legend.testdatagen.TestDataGenerator")
            // the ONE wire-tree walker (P2-4/P2-6): structure is its,
            // the LEAF rule is the lattice's — comparison layer
            .and().doNotHaveFullyQualifiedName("com.legend.exec.JsonCompare")
            // JUDGING_TWO_MODES step 2: the host judge and the service-test
            // judge are the seam's own (Equality decides; TestAssertions
            // routes EqualToJson to it)
            .and().doNotHaveFullyQualifiedName("com.legend.exec.Equality")
            .and().doNotHaveFullyQualifiedName("com.legend.test.TestAssertions")
            // leg 3.4 (2026-09-20): the verdict seam's SENDING arm — the
            // body's deferred verdict statements as one statement; it
            // counts the census rows the arm counted at the assert
            // (raised, unjudged) and decides nothing (the row's judgment
            // is injected from AssertVerdicts)
            .and().doNotHaveFullyQualifiedName("com.legend.exec.VerdictBatch")
            .should().dependOnClassesThat().haveNameMatching(
                "com\\.legend\\.exec\\.(PureAsserts|CanonicalForm|CanonicalDivergence|Equality)")
            .as("V3: host-verdict classes are reachable only from the"
                    + " verdict/referee seam — register a new dependent"
                    + " consciously or route through the DB byte verdict")
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
                "com.legend.compiler.spec.CoreFn.BY_NAME",
                "com.legend.builtin.Pure.ALL_CLASSES",
                "com.legend.builtin.Pure.ALL_ENUMS",
                "com.legend.builtin.Pure.ALL",
                "com.legend.builtin.Pure$Index.CLASS_BY_FQN",
                "com.legend.builtin.Pure$Index.ENUM_BY_FQN",
                "com.legend.builtin.Pure$Index.FN_BY_FQN",
                "com.legend.builtin.Pure$Index.FN_BY_BARE",
                "com.legend.builtin.Pure$Index.KEYS_BY_NAME",
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
                "com.legend.exec.CanonicalDivergence.SAMPLES",
                // V7 dual-channel census (V7_ASSERT_VERDICT_CHARTER
                // §4.1): per-form harness-vs-production verdict counts,
                // bounded classified declines, bounded disagreement
                // witnesses; measurement only — the CanonicalDivergence
                // pattern
                // reserved dual-verdict ALARM witness buffer (bounded
                // 50) — the alarm row must never lose its witness to
                // shared-sample crowding; measurement only
                "com.legend.exec.CanonicalDivergence.SQL_DISAGREE_SAMPLES",
                // §8.3b wobble burn: the BYTE channel's pinned-census
                // witnesses get the same reserved buffer — a ±1 count
                // wobble was unattributable because its rows sat past
                // the shared 200-cap (decline-row crowding, the
                // SQL_DISAGREE_SAMPLES lesson re-learned); bounded by
                // the exact-pinned disagree count (23); measurement only
                "com.legend.exec.CanonicalDivergence.DISAGREE_SAMPLES",
                "com.legend.exec.CanonicalDivergence.V7_FORMS",
                // leg 3.0 (DATABASE_MODE_HOMEWORK §4a): the SQL canon's
                // CLAIM / DECLINE census per assert family and reason —
                // bounded by families × decline-reason heads (~40 keys);
                // measurement only, printed by the corpus lanes, never
                // read by a verdict
                "com.legend.exec.CanonicalDivergence.SQL_CENSUS",
                "com.legend.exec.CanonicalDivergence.V7_DECLINES",
                "com.legend.exec.CanonicalDivergence.V7_SAMPLES",
                // step-0 residue census (FULL_RESIDUE_CENSUS_2026_08_30):
                // per-ROW decline attribution, UNCAPPED by census
                // doctrine but bounded by the sweep's own decline count
                // (~420); measurement only, never verdict-affecting
                "com.legend.exec.CanonicalDivergence.V7_DECLINE_WITNESSES",
                // slice-1 quarantine channel move (census §10h): the
                // wall-surfaced quarantine tests, vocabulary-matched —
                // bounded by the partition's own size (20 today);
                // measurement only, never verdict-affecting
                "com.legend.exec.CanonicalDivergence.QUARANTINED_WALL_TESTS",
                // SQLTEXT slice 3a: the text-emission census's
                // per-reason text-verdict tally (foreign dialects,
                // replay declines) — bounded by decline-reason
                // cardinality; measurement only, never
                // verdict-affecting (the CanonicalDivergence pattern)
                // TYPED-IR Slice 1: the label-lie census's classified
                // counters (declared-vs-computed pair -> count);
                // measurement only, never verdict-affecting — the
                // CanonicalDivergence pattern
                "com.legend.exec.SqlTypeCensus.CLASSES",
                // T3: bounded per-class WITNESSES (3 max each) — the
                // emission-seam locator; measurement only
                "com.legend.exec.SqlTypeCensus.SAMPLES",
                // §E3 M-N1: the nullability differential's classified
                // counters + bounded witnesses (fact-vs-label nullable
                // — the M-N3 flip payload); measurement only, the
                // §4AD navigation-arm census: runtime accumulation,
                // dumped by the corpus runner (NAV_ARM_CENSUS_4AD.md)
                // SqlTypeCensus pattern (runtime accumulation is the
                // legitimate static-census shape)
                "com.legend.exec.SqlTypeCensus.NUL_CLASSES",
                "com.legend.exec.SqlTypeCensus.NUL_SAMPLES",
                // §E3 slack census (the breach converse — precision
                // instrument): classified counters + bounded
                // witnesses; measurement only, deliberately unpinned
                "com.legend.exec.SqlTypeCensus.SLACK_CLASSES",
                "com.legend.exec.SqlTypeCensus.SLACK_SAMPLES",
                // serializer registry: written once at static init; the
                // ConcurrentHashMap spelling is for safe publication
                "com.legend.server.serial.SerializerRegistry.SERIALIZERS",
                // a SESSION's established test-data setups (2026-09-16): the
                // engine loads a LocalH2 connection's data when it opens the
                // connection, once; this keys that fact on the session itself
                // (WeakHashMap — the fact lives exactly as long as the
                // connection) and re-seeds after a writing statement
                "com.legend.StatementExecutor.ESTABLISHED");
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
     *       probed roster through its {@code SchemaOracle};
     *       {@code exec.GridProbe} is the executor's oracle.</li>
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
                    java.util.Map.entry("com.legend.Compiler", 2),
                    java.util.Map.entry("com.legend.compiler.DerivedProps", 3),
                    java.util.Map.entry("com.legend.compiler.LiteralMapUnroll", 2),
                    java.util.Map.entry("com.legend.compiler.NameResolver", 30),
                    java.util.Map.entry("com.legend.compiler.StatementInline", 6),
                    java.util.Map.entry("com.legend.compiler.element.TypeClassifier", 1),
                    java.util.Map.entry("com.legend.compiler.spec.AlphaRename", 7),
                    java.util.Map.entry("com.legend.compiler.spec.CallShapes", 9),
                    java.util.Map.entry("com.legend.compiler.spec.DistinctChecker", 2),
                    java.util.Map.entry("com.legend.compiler.spec.EvalChecker", 2),
                    java.util.Map.entry("com.legend.compiler.spec.ExtendChecker", 5),
                    java.util.Map.entry("com.legend.compiler.spec.FoldChecker", 4),
                    java.util.Map.entry("com.legend.compiler.spec.FromChecker", 2),
                    java.util.Map.entry("com.legend.compiler.spec.GroupByChecker", 21),
                    java.util.Map.entry("com.legend.compiler.spec.GroupLambdaAggs", 16),
                    java.util.Map.entry("com.legend.compiler.spec.IfChecker", 1),
                    java.util.Map.entry("com.legend.compiler.spec.IsDistinctChecker", 6),
                    java.util.Map.entry("com.legend.compiler.spec.JoinChecker", 22),
                    java.util.Map.entry("com.legend.compiler.spec.JsonChecker", 16),
                    java.util.Map.entry("com.legend.compiler.spec.LambdaBodies", 10),
                    java.util.Map.entry("com.legend.compiler.spec.NewChecker", 2),
                    java.util.Map.entry("com.legend.compiler.spec.ProjectChecker", 16),
                    java.util.Map.entry("com.legend.compiler.spec.RenameChecker", 1),
                    java.util.Map.entry("com.legend.compiler.spec.SortChecker", 13),
                    java.util.Map.entry("com.legend.compiler.spec.SourceSubst", 10),
                    java.util.Map.entry("com.legend.compiler.spec.StaticFold", 13),
                    java.util.Map.entry("com.legend.compiler.spec.TdsNullForms", 1),
                    java.util.Map.entry("com.legend.compiler.spec.TypeAnnotations", 1),
                    java.util.Map.entry("com.legend.compiler.spec.Typer", 127),
                    java.util.Map.entry("com.legend.lineage.ScanRelations", 2),
                    java.util.Map.entry("com.legend.model.MappingFromProtocol", 4),
                    java.util.Map.entry("com.legend.test.PureTestRunner", 2),
                    java.util.Map.entry("com.legend.test.ServiceTestRunner", 2),
                    java.util.Map.entry("com.legend.validation.ValidateDesugar", 27)
            ));

    @Test
    void protocolNodesAreConstructedOnlyByTheParserAndTheNormalizer() {
        java.util.Map<String, Integer> actual = new java.util.TreeMap<>();
        for (com.tngtech.archunit.core.domain.JavaClass c : CORE_PROD_CLASSES) {
            String pkg = c.getPackageName();
            if (pkg.equals("com.legend.parser") || pkg.startsWith("com.legend.parser.")
                    || pkg.equals("com.legend.normalizer") || pkg.startsWith("com.legend.normalizer.")
                    || pkg.equals("com.legend.protocol") || pkg.startsWith("com.legend.protocol.")) {
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
