// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.database.Databases;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;
import com.legend.testing.Program;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A LAMBDA'S LEADING STATEMENT IS NEVER DROPPED (C5-J's batch, 3a, the orchestrator's ruling of 2026-11-16): a lambda
 * body of several statements -- an {@code if}'s branch, a {@code map}'s or {@code forAll}'s function -- ran its last
 * statement and typed and dropped the others, so a leading {@code assertSize} or {@code assertContains} that fails, or a
 * {@code toOne()} over two values, made no difference: a false green. Pure evaluates each statement. Now an assert of
 * pure's family is carried as pure composes it ({@code assert(condition, message)}, its default message pure's own, the
 * database raising it), and any other leading statement whose value is discarded is refused by name (register
 * S-batch-6, pending). E-Java's five probes, each red on revert.
 */
class LambdaStatementsTest {

    private static final String MODEL = "###Pure\nClass t::Marker {}\n";

    private static Throwable failure(String body) throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            return assertThrows(Throwable.class, () -> Program.run(StorelessRuntime.with(MODEL, Databases.PLATFORM),
                    body, StorelessRuntime.RUNTIME, c));
        }
    }

    private static void failsWith(String body, String words) throws Exception {
        Throwable failed = failure(body);
        assertTrue(String.valueOf(failed.getMessage()).contains(words), body + " -> " + failed);
    }

    @Test
    void anAssertLeadingAnIfsBranchIsRaised() throws Exception {
        failsWith("|if(true, |assertSize([1,2], 3); assertEquals(1, 1);, |true)", "expected size: 3, actual size: 2");
    }

    @Test
    void anAssertLeadingAMapsFunctionIsRaised() throws Exception {
        failsWith("|assertEquals(2, [1,2]->map(x | assertContains([9], $x); $x;)->size())", "[9] does not contain 1");
    }

    @Test
    void anAssertLeadingAForAllsFunctionIsRaised() throws Exception {
        failsWith("|assert([1,2]->forAll(x | assertContains([9], $x); true;))", "[9] does not contain 1");
    }

    @Test
    void anyOtherDiscardedStatementIsRefusedByName() throws Exception {
        failsWith("|assertEquals([1,2], [1,2]->map(x | [$x, $x]->toOne(); $x;))",
                "a statement whose value is discarded (meta::pure::functions::multiplicity::toOne) inside a lambda");
        Throwable refused = assertThrows(Throwable.class, () -> Lean.service(
                StorelessRuntime.with(MODEL, Databases.PLATFORM), "|[1,2]->map(x | [$x, $x]->toOne(); $x;)",
                StorelessRuntime.RUNTIME));
        assertTrue(String.valueOf(refused.getMessage()).contains("a statement whose value is discarded"),
                refused.toString());
    }

    /** The refusal is at PLANNING, not typing (the user's ruling of 2026-11-16, S-batch-6): a model whose function
     *  holds such a lambda compiles -- every body types, as the platform's prelude must at boot -- and a query that
     *  runs it is refused by name when it is planned. */
    @Test
    void aModelHoldingOneCompiles_andPlanningRefusesItByName() {
        String model = MODEL + "function t::twice(): Integer[*] { [1, 2]->map(x | [$x, $x]->toOne(); $x;) }\n";
        assertTrue(Compiler.compileErrors(model).isEmpty(), "the model compiles: " + Compiler.compileErrors(model));
        Throwable refused = assertThrows(Throwable.class, () -> Lean.service(
                StorelessRuntime.with(model, Databases.PLATFORM), "|t::twice()", StorelessRuntime.RUNTIME));
        assertTrue(String.valueOf(refused.getMessage()).contains("a statement whose value is discarded"
                + " (meta::pure::functions::multiplicity::toOne) inside a lambda: lite plans no effect it cannot carry;"
                + " bind it with let or make it the lambda's value"), refused.toString());
    }

    /** A leading statement that always raises -- an if whose every side fails -- is that raise, as pure runs it (the
     *  value after it unreachable): carried, never dropped and never refused. */
    @Test
    void aLeadingStatementThatAlwaysRaisesIsTheRaise() throws Exception {
        Throwable raised = assertThrows(Throwable.class, () -> Lean.service(
                StorelessRuntime.with(MODEL, Databases.PLATFORM),
                "|[1]->map(x | if($x == 1, |fail('one'), |fail('other')); $x;)", StorelessRuntime.RUNTIME));
        assertTrue(String.valueOf(raised.getMessage()).contains("one"), raised.toString());
    }

    /** An assert that holds is carried too: the lambda's value is its last statement's. */
    @Test
    void anAssertThatHoldsLeavesTheValue() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            assertTrue(Program.run(StorelessRuntime.with(MODEL, Databases.PLATFORM),
                    "|assertEquals([1,2], [1,2]->map(x | assertContains([1, 2], $x); $x;))",
                    StorelessRuntime.RUNTIME, c).passed());
        }
    }

    /** Each of the family's default messages is pure's, as pure's own tests pin them (essential/tests/*.pure). */
    @Test
    void eachDefaultMessageIsPures() throws Exception {
        // each case: the assert, pure's message, and whether its values are scalars (a database with no list encoding
        // holds a scalar case alone)
        String[][] cases = {
            {"assertSize([1, 2], 3)", "expected size: 3, actual size: 2", "lists"},
            {"assertContains([1, 2], 3)", "[1, 2] does not contain 3", "lists"},
            {"assertEmpty([1, 2])", "[1, 2] is not empty", "lists"},
            {"assertEmpty(['a', 'b'])", "[a, b] is not empty", "lists"},
            {"assertEq(1, 2)", "\nexpected: 1\nactual:   2", "scalars"},
            {"assertEquals(1, 2)", "\nexpected: 1\nactual:   2", "scalars"},
            {"assertEquals('a', 'b')", "\nexpected: 'a'\nactual:   'b'", "scalars"},
            {"assertEquals([1, 3, 2], [2, 4, 1, 5])", "\nexpected: [1, 3, 2]\nactual:   [2, 4, 1, 5]", "lists"},
            {"assertNotEquals(1, 1)", "1 should not equal 1", "scalars"},
            {"assertNotEquals([1, 2], [1, 2])", "[1, 2] should not equal [1, 2]", "lists"},
            {"assertNotEmpty([])", "Expected non-empty collection", "scalars"},
            {"assertFalse(true)", "Assert failed", "scalars"},
            {"assert(false)", "Assert failed", "scalars"},
            {"assert(false, 'Test message: %s', [5])", "Test message: 5", "scalars"},
            {"assertInstanceOf(3, Boolean)", "expected 3 to be an instance of Boolean, actual: Integer", "scalars"},
            {"assertInstanceOf('x', Integer)", "expected 'x' to be an instance of Integer, actual: String", "scalars"},
            {"assertSameElements([1, 3, 2], [2, 4, 1, 5])", "\nexpected: [1, 2, 3]\nactual:   [1, 2, 4, 5]", "lists"},
            {"assertEqWithinTolerance(1, 3, 1)", "\nexpected: 1\nactual:   3", "scalars"},
        };
        // each assert leading an if's side, planned by the judges: over scalars on each in-process database (a message
        // the assert does not raise is planned beside its condition, and a database that folds a constant raise in an
        // arm it never takes would raise it there), every case on the platform database; and leading a map's function
        // on the platform database, where the database raises it inside the query
        List<String> refused = new ArrayList<>();
        for (com.legend.testbackend.TestBackend backend : TestBackends.inProcess()) {
            String model = backend.withStorelessRuntime(MODEL);
            for (String[] c : cases) {
                boolean platform = backend.name().equals(TestBackends.platform().name());
                List<String> bodies = new ArrayList<>();
                if (platform || c[2].equals("scalars")) {
                    bodies.add("|if(true, |" + c[0] + "; true;, |true)");
                }
                if (platform) {
                    bodies.add("|[1]->map(x | " + c[0] + "; $x;)");
                }
                for (String body : bodies) {
                    try (Connection conn = backend.open()) {
                        Throwable failed = assertThrows(Throwable.class,
                                () -> Program.run(model, body, StorelessRuntime.RUNTIME, conn));
                        if (!String.valueOf(failed.getMessage()).contains(c[1])) {
                            refused.add(backend.name() + ": " + body + " -> " + failed);
                        }
                    }
                }
            }
        }
        assertTrue(refused.isEmpty(), String.join("\n", refused));
    }
}
