package com.legend;

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

    /**
     * Imports only production classes (excludes {@code src/test/}). All
     * structural rules apply to production code; tests may use whatever
     * helpers they need without piercing the production import boundary.
     */
    private static final JavaClasses CORE_PROD_CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
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
}
