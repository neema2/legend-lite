package com.gs.legend.model;

import com.gs.legend.compiler.PureCompileException;
import com.gs.legend.compiler.TypeChecker;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the contract of the opt-in {@link PureModelBuilder#compile()} verb:
 *
 * <ul>
 *   <li>Surfaces body-level errors (declared-vs-actual return mismatches, etc.)
 *       that interpreted++ would have caught only lazily at query time.</li>
 *   <li>Is idempotent — repeat calls are no-ops for already-verified functions.</li>
 *   <li>Leaves interpreted++ semantics intact for callers that do not opt in.</li>
 *   <li>Per-function isolation — a body error in one function does not prevent
 *       another from compiling when the caller separates them.</li>
 * </ul>
 */
class CompileVerbTest {

    @Test
    void badBodyIsRejectedByWhicheverVerbRunsEagerBodyCheck() {
        // Single invariant across both modes: a declared-vs-actual return mismatch is
        // always caught by whatever verb runs body verification eagerly.
        //  - Interpreted++ (default): addSource accepts (bodies not verified); the
        //    explicit compile() verb is what catches the mismatch.
        //  - Force-compile (-Dlegend.lite.forceCompile=true): addSource itself runs
        //    compile() internally, so the error surfaces at ingest.
        // Both branches assert the error still surfaces, just from the verb that happens
        // to be eager in the current mode.
        String source = """
                function test::wrong(x: Integer[1]): Boolean[1]
                {
                  $x + 1
                }
                """;

        if (Boolean.getBoolean("legend.lite.forceCompile")) {
            // Under force-compile, addSource IS the verification pass — it must reject.
            var ex = assertThrows(PureCompileException.class,
                    () -> new PureModelBuilder().addSource(source),
                    "force-compile: addSource must reject body mismatch");
            assertTrue(ex.getMessage().contains("test::wrong"),
                    "Error must name the failing function: " + ex.getMessage());
        } else {
            // Default: addSource accepts; compile() is the verification verb.
            PureModelBuilder built = new PureModelBuilder().addSource(source);
            assertDoesNotThrow(() -> new PureModelBuilder().addSource(source),
                    "interpreted++: addSource must not fail on bad body");
            var ex = assertThrows(PureCompileException.class, built::compile,
                    "interpreted++: compile() must reject body mismatch");
            assertTrue(ex.getMessage().contains("test::wrong"),
                    "Error must name the failing function: " + ex.getMessage());
        }
    }

    @Test
    void compileIsIdempotentWithinOneCheckerInstance() {
        // Repeat compile() calls on the same builder must be safe. TypeChecker
        // memoizes by PureFunction identity, so the second pass is a no-op.
        String source = """
                function test::ok(x: Integer[1]): Integer[1]
                {
                  $x + 1
                }
                """;

        PureModelBuilder built = new PureModelBuilder().addSource(source);
        TypeChecker tc = new TypeChecker(built);

        assertDoesNotThrow(() -> built.compile(tc));
        assertDoesNotThrow(() -> built.compile(tc),
                "Repeat compile() on the same TypeChecker must be a no-op");
    }

    @Test
    void compileDoesNotAffectInterpretedQueryBehaviour() {
        // A model that compiles cleanly must still work the same way in interpreted++
        // afterwards — compile() is a verification pass, not a transformation.
        String source = """
                function test::double(x: Integer[1]): Integer[1]
                {
                  $x * 2
                }
                """;

        PureModelBuilder built = new PureModelBuilder().addSource(source);
        built.compile();

        // Post-compile, findFunction / typed signature lookup still behaves identically.
        var pf = built.findFunction("test::double").getFirst();
        assertTrue(pf.parameters().size() == 1,
                "Signature must be unchanged by compile(): " + pf);
    }

    @Test
    void compileSurfacesConcreteOperationOnAnyParam() {
        // An Any-typed param with a body operation that requires a concrete type is a
        // latent bug under interpreted++ — the body is never verified standalone, only
        // specialized at call sites. compile() binds the declared Any param into the
        // body-check context, so `$x + 1` must fail because `+` cannot dispatch on
        // (Any, Integer). Pins the contract that compile() rejects this; if it ever
        // silently passes, the TypeChecker has grown an unintended Any-leniency.
        String source = """
                function test::badAny(x: meta::pure::metamodel::type::Any[1]): Integer[1]
                {
                  $x + 1
                }
                """;

        // Invariant: whichever verb runs eager body check in the current mode rejects
        // this. Force-compile catches at addSource; interpreted++ catches at compile().
        if (Boolean.getBoolean("legend.lite.forceCompile")) {
            assertThrows(PureCompileException.class,
                    () -> new PureModelBuilder().addSource(source),
                    "force-compile: addSource must reject concrete op on Any-typed param");
        } else {
            PureModelBuilder built = new PureModelBuilder().addSource(source);
            assertDoesNotThrow(() -> new PureModelBuilder().addSource(source),
                    "interpreted++ must accept Any-param-with-concrete-op (no body verification)");
            assertThrows(PureCompileException.class, built::compile,
                    "interpreted++: compile() must reject concrete op on Any-typed param");
        }
    }

    @Test
    void compileSurfacesColumnReferenceAgainstUnknownRelationSchema() {
        // Relation-world analog of the Any-param leniency test. Parameter is declared
        // Any[1] — no relation schema at all — but the body treats it as a relation and
        // dereferences a concrete column. Interpreted++ leaves this latent; compile()
        // binds the declared Any into the body-check context and must reject when the
        // body tries to filter by $x.AGE against an Any-typed $x.
        String source = """
                function test::filterAdults(r: meta::pure::metamodel::type::Any[1]): meta::pure::metamodel::type::Any[1]
                {
                  $r->filter(x | $x.AGE > 21)
                }
                """;

        // Invariant: eager body check rejects "Unresolved type for property: AGE".
        // Surfaces from addSource under force-compile; from compile() under interpreted++.
        if (Boolean.getBoolean("legend.lite.forceCompile")) {
            assertThrows(PureCompileException.class,
                    () -> new PureModelBuilder().addSource(source),
                    "force-compile: addSource must reject column deref on Any-typed param");
        } else {
            PureModelBuilder built = new PureModelBuilder().addSource(source);
            assertDoesNotThrow(() -> new PureModelBuilder().addSource(source),
                    "interpreted++ must accept Any-relation-with-column-access");
            assertThrows(PureCompileException.class, built::compile,
                    "interpreted++: compile() must reject column deref on Any-typed param");
        }
    }

    @Test
    void compileOnEmptyModelIsNoOp() {
        // Nothing to verify, nothing to fail.
        assertDoesNotThrow(() -> new PureModelBuilder().compile());
    }

    @Test
    void compileSharesTypeCheckerSoQueriesReuseCompiledBodies() {
        // The (TypeChecker) overload lets callers pre-warm a shared checker so that
        // a subsequent query using the same checker does not recompile.
        String source = """
                function test::square(x: Integer[1]): Integer[1]
                {
                  $x * $x
                }
                """;

        PureModelBuilder built = new PureModelBuilder().addSource(source);
        TypeChecker shared = new TypeChecker(built);
        built.compile(shared);

        // The shared checker already carries the CompiledFunction for test::square.
        // Asking for it again returns the same instance (identity-memoized).
        var pf = built.findFunction("test::square").getFirst();
        var first = shared.check(pf);
        var second = shared.check(pf);
        assertTrue(first == second,
                "TypeChecker must return the cached CompiledFunction for a pre-compiled PureFunction");
    }
}
