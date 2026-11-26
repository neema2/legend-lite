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

    /** legend-pure's {@code %r} and {@code toRepresentation} of a Float used further at the edge are lite's own text of
     *  a Float (the edition program's step 8, 2026-11-26; tenet 1 as the user amended it on 2026-11-25, and the user's
     *  word on the register row, 2026-11-26: "yes that's right but remember we have a list of chained functions that
     *  are okay after that"): six decimals, trailing zeros trimmed, a whole value keeping one, the same on every lane.
     *  Until this step the edge composed legend-pure's print in SQL (the node FLOAT_SHORTEST_TEXT, deleted), which
     *  answered {@code 0.3333333333333333}, {@code 0.30000000000000004} and, for a whole value past 64 bits on the lane
     *  whose integer does not hold one, a wrong {@code 9223372036854775807.0}. The product refuses both calls by name. */
    @Test
    void aFloatsRepresentationUsedFurtherAtTheEdgeIsLitesOwnText() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("Class s::P { name: String[1]; }\n");
        try (Connection c = backend.open()) {
            Exception refused = assertThrows(Exception.class,
                    () -> Lean.run(model, "|toRepresentation(1.5 * 2.0)", StorelessRuntime.RUNTIME, c));
            assertTrue(String.valueOf(refused.getMessage()).contains("compatibility edge's"), refused.toString());
        }
        java.util.Map<String, String> expected = new java.util.LinkedHashMap<>();
        expected.put("|format('%r', [1.5])", "1.5");
        expected.put("|format('%r', [(1.5 * 2.0)])", "3.0");
        expected.put("|format('a %r b %r', [(1.0/3.0), 2])", "a 0.333333 b 2");
        expected.put("|toRepresentation(0.1 + 0.2)", "0.3");
        expected.put("|toRepresentation(1.5e-8 * 1.0)", "0.0");
        expected.put("|toRepresentation(-2.5e15 * 10.0)", "-25000000000000000.0");
        expected.put("|toRepresentation(1.0e20)", "100000000000000000000.0");
        expected.put("|toRepresentation(1.0e20 * 1.0)", "100000000000000000000.0");
        for (var e : expected.entrySet()) {
            assertEquals(e.getValue(), scalar(Lean.runInStyle(model, e.getKey(), StorelessRuntime.RUNTIME, backend,
                    com.legend.edge.PureText.UPSTREAM)), backend.name() + ": " + e.getKey());
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
