package com.gs.legend.model;

import com.gs.legend.compiler.PureCompileException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Call-graph cycle detection runs inside
 * {@link PureModelBuilder#buildPureFunctions()} and rejects recursive user functions
 * at ingest. Graph nodes are (FQN, arity) so overloads distinguished by arity are
 * separate nodes and overload chaining does NOT register as a cycle.
 *
 * <p>These tests pin the contract downstream consumers rely on: any
 * {@link com.gs.legend.model.m3.PureFunction} returned from the builder is part of
 * an acyclic call graph, so TypeChecker / PlanGenerator can walk bodies without
 * their own recursion guard.
 */
class CallGraphCycleDetectionTest {

    @Test
    void directSelfRecursionIsRejected() {
        String source = """
                function test::r(x: Integer[1]): Integer[1]
                {
                  test::r($x)
                }
                """;
        var ex = assertThrows(PureCompileException.class,
                () -> new PureModelBuilder().addSource(source));
        assertTrue(ex.getMessage().contains("test::r/1"),
                "Error must name the cycle node (FQN/arity): " + ex.getMessage());
        assertTrue(ex.getMessage().toLowerCase().contains("cycl"),
                "Error must identify it as a cycle: " + ex.getMessage());
    }

    @Test
    void mutualRecursionIsRejectedWithBothNamesInPath() {
        // a -> b -> a. Both nodes must appear in the rendered cycle path.
        String source = """
                function test::a(x: Integer[1]): Integer[1]
                {
                  test::b($x)
                }
                function test::b(x: Integer[1]): Integer[1]
                {
                  test::a($x)
                }
                """;
        var ex = assertThrows(PureCompileException.class,
                () -> new PureModelBuilder().addSource(source));
        assertTrue(ex.getMessage().contains("test::a/1")
                        && ex.getMessage().contains("test::b/1"),
                "Cycle path must name both participants: " + ex.getMessage());
    }

    @Test
    void overloadChainingByArityIsNotACycle() {
        // Same FQN, different arities — (FQN, arity) keys must keep them distinct, so
        // test::t/2 calling test::t/1 is a legitimate call chain, not recursion.
        String source = """
                function test::t(x: Integer[1]): Integer[1]
                {
                  $x * 2
                }
                function test::t(x: Integer[1], y: Integer[1]): Integer[1]
                {
                  test::t($x) + $y
                }
                """;
        assertDoesNotThrow(() -> new PureModelBuilder().addSource(source),
                "Arity-distinguished overload chaining must not be flagged as a cycle");
    }

    @Test
    void cycleSpanningBatchSourcesIsRejected() {
        // addSources batch: foo in source A calls bar in source B; bar calls foo.
        // Detection runs on every buildPureFunctions pass, so whichever addSource
        // completes the cycle surfaces the error.
        String srcA = """
                function test::foo(x: Integer[1]): Integer[1]
                {
                  test::bar($x)
                }
                """;
        String srcB = """
                function test::bar(x: Integer[1]): Integer[1]
                {
                  test::foo($x)
                }
                """;
        var ex = assertThrows(PureCompileException.class,
                () -> new PureModelBuilder().addSources(srcA, srcB));
        assertTrue(ex.getMessage().contains("test::foo/1")
                        && ex.getMessage().contains("test::bar/1"),
                "Multi-source cycle must be reported with both participants: " + ex.getMessage());
    }

    @Test
    void cycleReachableOnlyViaLambdaBodyIsDetected() {
        // Exercises the LambdaFunction branch of the AST walker. The recursive call
        // appears only inside a lambda passed to a higher-order function (map); the
        // outer call is a builtin (map) which is not itself a user function. If the
        // walker failed to descend into lambda bodies, this cycle would be missed.
        String source = """
                function test::loop(x: Integer[1]): Integer[1]
                {
                  [1, 2]->map(i | test::loop($x))->size()
                }
                """;
        var ex = assertThrows(PureCompileException.class,
                () -> new PureModelBuilder().addSource(source));
        assertTrue(ex.getMessage().contains("test::loop/1"),
                "Cycle reachable only through a lambda body must be detected: " + ex.getMessage());
    }

    @Test
    void cycleReachableOnlyViaPureCollectionIsDetected() {
        // Exercises the PureCollection branch of the AST walker. The recursive call
        // appears only as a member of a list literal that is then consumed by a
        // builtin. If the walker failed to descend into PureCollection values, this
        // cycle would be missed.
        String source = """
                function test::loop(x: Integer[1]): Integer[1]
                {
                  [test::loop($x)]->size()
                }
                """;
        var ex = assertThrows(PureCompileException.class,
                () -> new PureModelBuilder().addSource(source));
        assertTrue(ex.getMessage().contains("test::loop/1"),
                "Cycle reachable only through a list literal must be detected: " + ex.getMessage());
    }

    @Test
    void emptyModelHasNoWorkAndSucceeds() {
        // No user functions -> detector is a no-op. Guards against a NullPointerException
        // or similar regression when the function table is empty.
        assertDoesNotThrow(() -> new PureModelBuilder().addSource(
                "Class model::Only { name: String[1]; }"));
    }
}
