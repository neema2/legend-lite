package com.gs.legend.architecture;

import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.TypeChecker;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.PureModelBuilder;
import com.gs.legend.model.def.EnumDefinition;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.store.Table;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * AGENTS.md §5 behavioral guard — Shape 2.
 *
 * <p>The structural guard ({@link NoEagerTypeReferencesTest}) catches resolved-reference
 * fields. This guard catches the other leak: code paths that <em>dynamically</em> resolve
 * unrelated user classes or enums via {@link ModelContext#findClass} / {@link ModelContext#findEnum}
 * even when the caller only needs a small subset.
 *
 * <p><b>Design:</b> a <em>fail-fast</em> {@link ModelContext} wrapper is threaded into
 * {@link TypeChecker}. After parsing completes (parse-time name resolution bypasses the
 * wrapper intentionally — parse is eager by contract), the wrapper is handed to the
 * compiler. Any lookup for a designated "cold" FQN throws immediately, producing a stack
 * trace that names the exact compile call site that violated §5.
 *
 * <p><b>Scope:</b> this test exercises the <em>compile phase only</em> ({@link TypeChecker}).
 * Extending coverage to the full {@link com.gs.legend.plan.PlanGenerator} pipeline
 * (mapping-normalize, mapping-resolve, plan-generate, dialect) would require
 * {@code PlanGenerator} and {@code MappingNormalizer} to accept a {@link ModelContext}
 * rather than a concrete {@link PureModelBuilder}. That's a worthwhile refactor but
 * outside this test's immediate scope.
 *
 * <p><b>Cold elements</b> are arranged to be structurally reachable from the model graph
 * (as a superclass of an unused sibling, as a declared enum, as an isolated class) so
 * naive "walk everything" leaks trip at least one of them — not just orphans the
 * simplest bug wouldn't find.
 */
class NoEagerUserClassLoadsTest {

    /**
     * Cold user elements. Any {@code findClass}/{@code findEnum} lookup for these FQNs
     * via the wrapped {@link ModelContext} during compile is a §5 violation.
     *
     * <ul>
     *   <li>{@code cold::ColdIsolated} — completely disconnected class.</li>
     *   <li>{@code cold::ColdSuper} — superclass of an unused sibling. Catches leaks
     *       that walk every class's superclass chain.</li>
     *   <li>{@code cold::ColdSibling} — extends ColdSuper; catches leaks that resolve
     *       all declared classes during compile.</li>
     *   <li>{@code cold::ColdEnum} — declared but never referenced. Catches leaks in
     *       {@code findEnum} paths (separate surface from {@code findClass}).</li>
     * </ul>
     */
    private static final Set<String> COLD_FQNS = Set.of(
            "cold::ColdIsolated",
            "cold::ColdSuper",
            "cold::ColdSibling",
            "cold::ColdEnum"
    );

    /**
     * Source with structural links: an unused sibling extends a cold class, so the cold
     * class is reachable via the superclass graph. Also declares a cold enum to cover
     * the {@code findEnum} surface independently.
     */
    private static final String MODEL_SOURCE = """
            Class cold::ColdIsolated { placeholder: String[1]; }
            Class cold::ColdSuper { s: String[1]; }
            Class cold::ColdSibling extends cold::ColdSuper { extra: String[1]; }
            Enum cold::ColdEnum { A, B, C }

            Class warm::UnusedSibling extends cold::ColdSuper { u: String[1]; }
            Class warm::User { name: String[1]; age: Integer[1]; }
            """;

    /**
     * Query shapes run through {@link TypeChecker} against the same wrapper. Each
     * exercises a different checker ({@code GetAllChecker}, {@code FilterChecker},
     * {@code ProjectChecker}) so eager traversals added in any of them are caught.
     */
    private static final List<String> QUERIES = List.of(
            "|warm::User.all()",
            "|warm::User.all()->filter(x|$x.age > 30)",
            "|warm::User.all()->project([x|$x.name, x|$x.age], ['n', 'a'])"
    );

    @Test
    void typeCheckingDoesNotLoadColdUserElements() {
        // 1. Parse phase. Parsing legitimately resolves every declared name including the
        //    cold ones (that's how they get into the registry). This goes through the raw
        //    builder, bypassing the fail-fast wrapper.
        PureModelBuilder builder = new PureModelBuilder();
        builder.addSource(MODEL_SOURCE);

        // 2. Wrap the builder in a fail-fast ModelContext. From here on, any findClass /
        //    findEnum for a cold FQN throws with a stack trace pointing at the exact
        //    offending compile call site.
        FailFastModelContext strict = new FailFastModelContext(builder, COLD_FQNS);

        // 3. Run every query shape through TypeChecker with the strict wrapper. If any
        //    checker walks into a cold element, the wrapper throws and the test fails
        //    with a stack trace pinning the culprit.
        for (String query : QUERIES) {
            // Parse (against raw builder) is legitimately eager; compile (against strict
            // wrapper) must be lazy.
            ValueSpecification vs = builder.resolveQuery(query);
            new TypeChecker(strict).check(vs);
        }

        // 4. Sanity: warm class was looked up, so we know the pipeline actually ran.
        //    Without this check the test could pass vacuously if every query short-
        //    circuited before any findClass call.
        if (!strict.warmLookupsSeen()) {
            throw new AssertionError(
                    "Test setup sanity check failed: type-check completed but never looked up "
                            + "warm::User. Either the queries are trivial or the wrapper wiring is wrong — "
                            + "this test is vacuously passing and must be fixed.");
        }
    }

    // ========== Wrapper ==========

    /**
     * Fail-fast {@link ModelContext} decorator. Any lookup for a cold FQN throws
     * immediately; the exception propagates up through the compile stack so the stack
     * trace names the exact violating call site. All other calls pass through to the
     * wrapped builder unchanged.
     *
     * <p>Also tracks whether the warm class was looked up at all, so the test can
     * distinguish "type-check ran and didn't touch cold" from "type-check silently
     * no-op'd without hitting any lookup".
     */
    private static final class FailFastModelContext implements ModelContext {
        private final ModelContext delegate;
        private final Set<String> forbiddenFqns;
        private volatile boolean warmSeen = false;

        FailFastModelContext(ModelContext delegate, Set<String> forbiddenFqns) {
            this.delegate = delegate;
            this.forbiddenFqns = forbiddenFqns;
        }

        boolean warmLookupsSeen() { return warmSeen; }

        @Override
        public Optional<PureClass> findClass(String className) {
            guard(className, "findClass");
            return delegate.findClass(className);
        }

        @Override
        public Optional<EnumDefinition> findEnum(String enumName) {
            guard(enumName, "findEnum");
            return delegate.findEnum(enumName);
        }

        private void guard(String fqn, String method) {
            if (fqn != null && fqn.startsWith("warm::")) {
                warmSeen = true;
            }
            if (forbiddenFqns.contains(fqn)) {
                throw new AssertionError(
                        "AGENTS.md §5 violation: " + method + "(\"" + fqn + "\") was called "
                                + "during the compile phase. This is a cold user element that no "
                                + "query in the test references — some compiler path is eagerly "
                                + "resolving unrelated classes. See the stack trace below for the "
                                + "exact call site.");
            }
        }

        // Pass-through for everything else. Lookup guarding lives on the two
        // "resolve a packageable user element by FQN" methods, which are the lazy-load
        // boundary defined in AGENTS.md §5.

        @Override
        public Optional<ModelContext.AssociationNavigation> findAssociationByProperty(
                String fromClassName, String propertyName) {
            return delegate.findAssociationByProperty(fromClassName, propertyName);
        }

        @Override
        public Optional<Table> findTable(String db, String name) {
            return delegate.findTable(db, name);
        }

        @Override
        public Map<String, ModelContext.AssociationNavigation> findAllAssociationNavigations(String className) {
            return delegate.findAllAssociationNavigations(className);
        }

        @Override
        public List<com.gs.legend.model.m3.PureFunction> findFunction(String name) {
            return delegate.findFunction(name);
        }

        @Override
        public Optional<ValueSpecification> findSourceSpec(String className) {
            return delegate.findSourceSpec(className);
        }
    }
}
