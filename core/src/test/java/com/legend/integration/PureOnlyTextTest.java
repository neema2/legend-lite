// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TEXTS ONLY LEGEND-PURE WRITES, ON THE PRODUCT ROAD (phase D's D1.3, 2026-11-15; register S314, the user's rulings of
 * 2026-11-10: "format: %s %d %f %t{} in the product; %r and padding at the edge", "objects and relations are refused,
 * pointing to graph fetch"): the product refuses {@code format}'s {@code %0<w>d} and an instance's {@code toString} by
 * name; the compatibility edge writes legend-pure's text for each, pinned beside it. {@code %r} is the judges' too (the
 * assert bodies' messages write it): it moves with them, D1.9.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class PureOnlyTextTest {

    @Test
    void theProductRefusesPuresDirectivesByNameAndTheEdgeWritesThem() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("Class s::P { name: String[1]; }\n");
        try (Connection c = backend.open()) {
            assertEquals("x 7 y", scalar(Lean.run(model, "|format('x %s y', [7])", StorelessRuntime.RUNTIME, c)),
                    backend.name());
            Exception refused = assertThrows(Exception.class,
                    () -> Lean.run(model, "|format('%05d', [-3])", StorelessRuntime.RUNTIME, c));
            assertTrue(String.valueOf(refused.getMessage()).contains("format's %05d")
                    && refused.getMessage().contains("compatibility edge's"), refused.toString());
            assertEquals("'a'", scalar(Lean.runInStyle(model, "|format('%r', ['a'])", StorelessRuntime.RUNTIME, backend,
                    com.legend.edge.PureText.UPSTREAM)), backend.name());
            Exception instance = assertThrows(Exception.class,
                    () -> Lean.run(model, "|^s::P(name='a')->toString()", StorelessRuntime.RUNTIME, c));
            assertTrue(String.valueOf(instance.getMessage()).contains("toString of an instance")
                    && instance.getMessage().contains("graph fetch"), instance.toString());
            assertTrue(scalar(Lean.runInStyle(model, "|^s::P(name='a')->toString()", StorelessRuntime.RUNTIME, backend,
                    com.legend.edge.PureText.UPSTREAM)).startsWith("@_"), backend.name());
            assertEquals("-00003", scalar(Lean.runInStyle(model, "|format('%05d', [-3])", StorelessRuntime.RUNTIME, backend,
                    com.legend.edge.PureText.UPSTREAM)), backend.name());
        }
    }

    /**
     * legend-pure's {@code %r} and {@code toRepresentation} of a Float at the edge (the edition program's step 8, slice
     * 1, 2026-11-26): the edge composes the text at lowering from the product's own nodes
     * ({@code edge.EdgePureOnlyTexts}); until then it wrote the node FLOAT_SHORTEST_TEXT, which the product's dialects
     * spelled. Each answer was measured on the commit before the move, on every lane this test runs on, and is the same
     * after it. Inside the text's windows it is legend-pure's print on every lane. Outside them (below 1e-17, from 1e38)
     * the database's own exponent form stays, which differs by lane in its case, its sign and its {@code .0}: the test
     * holds each to the forms measured. The product refuses both calls by name. (A whole value past 64 bits is left
     * out: one lane's integer does not hold it, and answered a wrong text before the move and after it; the slice's
     * record names it.)
     */
    @Test
    void theEdgeWritesPuresRepresentationOfAFloat() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("Class s::P { name: String[1]; }\n");
        try (Connection c = backend.open()) {
            Exception refused = assertThrows(Exception.class,
                    () -> Lean.run(model, "|toRepresentation(1.5 * 2.0)", StorelessRuntime.RUNTIME, c));
            assertTrue(String.valueOf(refused.getMessage()).contains("compatibility edge's"), refused.toString());
        }
        java.util.Map<String, List<String>> expected = new java.util.LinkedHashMap<>();
        expected.put("|format('%r', [1.5])", List.of("1.5"));
        expected.put("|format('%r', [(1.5 * 2.0)])", List.of("3.0"));
        expected.put("|format('a %r b %r', [(1.0/3.0), 2])", List.of("a 0.3333333333333333 b 2"));
        expected.put("|toRepresentation(3.0 * 1.0)", List.of("3.0"));
        expected.put("|toRepresentation(0.1 + 0.2)", List.of("0.30000000000000004"));
        expected.put("|toRepresentation(1.5e-8)", List.of("0.000000015"));
        expected.put("|toRepresentation(1.5e-8 * 1.0)", List.of("0.000000015"));
        expected.put("|toRepresentation(-2.5e15 * 10.0)", List.of("-25000000000000000.0"));
        expected.put("|toRepresentation(123456789012345680.0 * 1.0)", List.of("123456789012345680.0"));
        // by lane: the last digit of a root as each database prints its double, and outside the windows each database's
        // own exponent form
        expected.put("|format('%r', [sqrt(2.0)])", List.of("1.4142135623730951", "1.4142135623730952"));
        expected.put("|toRepresentation(1.0e-20 * 1.0)", List.of("1e-20", "1.0E-20", "1.0e-20"));
        expected.put("|toRepresentation(1.0e40 * 1.0)", List.of("1e+40", "1.0E40", "1.0e+40"));
        for (var e : expected.entrySet()) {
            String got = scalar(Lean.runInStyle(model, e.getKey(), StorelessRuntime.RUNTIME, backend,
                    com.legend.edge.PureText.UPSTREAM));
            assertTrue(e.getValue().contains(got), backend.name() + ": " + e.getKey() + " answered " + got
                    + ", expected one of " + e.getValue());
        }
    }

    /** An assert in a lambda body with no message of its caller's (item 3's S6, 2026-11-23; the user's Q-C = C1, Q-S6
     *  and Q15 = A): it raises its assert kind, worded by the one catalog on every route -- the product's and the
     *  compatibility edge's the same sentence (until then the edge wrote legend-pure's "Assert failed", the user's R1 of
     *  2026-11-18). The kinds per guard and the plan's site: {@code exec.InBodyAssertRaiseTest}. */
    @Test
    void anInBodyAssertRaisesItsKindOnBothRoads() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("Class s::P { name: String[1]; }\n");
        try (Connection c = backend.open()) {
            for (List<String> guard : List.of(List.of("assert($r.x == 0)", "Assert failed"),
                    List.of("assertFalse($r.x < 0)", "Assert failed"),
                    List.of("assertEquals(0, $r.x)", "Assert failed"))) {
                String query = "|#TDS\n  x:Integer\n  -1\n#->extend(~y: r|if($r.x > 0, |$r.x, {|" + guard.get(0)
                        + "; 0;}))";
                assertEquals(guard.get(1), assertThrows(Exception.class,
                        () -> Lean.run(model, query, StorelessRuntime.RUNTIME, c)).getMessage(), guard.get(0));
                assertEquals(guard.get(1), assertThrows(Exception.class, () -> Lean.runInStyle(model, query,
                        StorelessRuntime.RUNTIME, backend, com.legend.edge.PureText.UPSTREAM)).getMessage(),
                        guard.get(0));
            }
        }
    }

    private static String scalar(Lean.Result r) {
        return String.valueOf(r.rows().get(0).get(0));
    }
}
