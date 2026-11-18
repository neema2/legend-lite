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

    /** An assert in a lambda body with no message of its caller's (the user's R1, 2026-11-18, PR #118's fix round, N2):
     *  the product raises lite's sentence; the compatibility edge, legend-pure's "Assert failed" (assert.pure) -- one
     *  native the plan's style words at lowering ({@code Pure.Lite.ASSERT_FAILED_TEXT}). */
    @Test
    void anInBodyAssertsDefaultIsLitesSentenceAndLegendPuresOnTheEdge() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("Class s::P { name: String[1]; }\n");
        String query = "|#TDS\n  x:Integer\n  -1\n#->extend(~y: r|if($r.x > 0, |$r.x, {|assert($r.x == 0); 0;}))";
        try (Connection c = backend.open()) {
            Exception product = assertThrows(Exception.class, () -> Lean.run(model, query, StorelessRuntime.RUNTIME, c));
            assertEquals("expected true, got false", product.getMessage(), backend.name());
            Exception edge = assertThrows(Exception.class, () -> Lean.runInStyle(model, query, StorelessRuntime.RUNTIME,
                    backend, com.legend.edge.PureText.UPSTREAM));
            assertEquals("Assert failed", edge.getMessage(), backend.name());
            // the six other guards keep their text on both roads: R1 ruled assert's default alone (the re-review of
            // 373e42960, 2026-11-18)
            for (String guard : List.of("assertFalse($r.x < 0)", "assertEquals(0, $r.x)")) {
                String other = "|#TDS\n  x:Integer\n  -1\n#->extend(~y: r|if($r.x > 0, |$r.x, {|" + guard + "; 0;}))";
                assertEquals("Assert failed", assertThrows(Exception.class,
                        () -> Lean.run(model, other, StorelessRuntime.RUNTIME, c)).getMessage(), guard);
                assertEquals("Assert failed", assertThrows(Exception.class, () -> Lean.runInStyle(model, other,
                        StorelessRuntime.RUNTIME, backend, com.legend.edge.PureText.UPSTREAM)).getMessage(), guard);
            }
        }
    }

    private static String scalar(Lean.Result r) {
        return String.valueOf(r.rows().get(0).get(0));
    }
}
