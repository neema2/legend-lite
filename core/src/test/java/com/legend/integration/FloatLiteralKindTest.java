// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A FLOAT LITERAL IS THE TARGET'S KIND (register S20, built 2026-11-17, Phase 5b; the edge program's §6 row 21): each
 * dialect says how its database computes with a Float literal ({@code SqlDialect.floatLiteralKind}), the lowering builds
 * the literal's fact by it and the dialect writes the literal by that fact. The product's is the double ("IEEE
 * everywhere"); the compatibility edge's is legend-engine's bare literal, a DECIMAL where it writes one.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class FloatLiteralKindTest {

    /** The kind and the written literal agree, on the lane's database, product and edge: {@code (0.1 + 0.2) == 0.3} is
     *  true exactly where the database computes the literals as DECIMALs. */
    @Test
    void theKindAndTheWrittenLiteralAgree() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        String query = "|(0.1 + 0.2) == 0.3";
        try (Connection c = backend.open()) {
            boolean productDecimal = com.legend.database.PlanStyle.PRODUCT.dialect(backend.type()).floatLiteralKind()
                    == com.legend.sql.SqlExpr.FloatLit.Kind.DECIMAL;
            assertEquals(String.valueOf(productDecimal),
                    String.valueOf(Lean.run(model, query, StorelessRuntime.RUNTIME, c).rows().get(0).get(0)),
                    backend.name() + " product");
            boolean edgeDecimal = com.legend.edge.PureText.UPSTREAM.dialect(backend.type()).floatLiteralKind()
                    == com.legend.sql.SqlExpr.FloatLit.Kind.DECIMAL;
            assertEquals(String.valueOf(edgeDecimal), String.valueOf(Lean.runInStyle(model, query,
                    StorelessRuntime.RUNTIME, backend, com.legend.edge.PureText.UPSTREAM).rows().get(0).get(0)),
                    backend.name() + " edge");
        }
    }

    /** The answers a Float literal's fact once decided wrongly where the database computed a double under a DECIMAL
     *  fact (Phase 5b's audit, 2026-11-17), each legend-pure's compiled answer (the double's arithmetic: S20, S127). */
    @Test
    void aDoubleLiteralsAnswersAreTheDoublesAnswers() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        try (Connection c = backend.open()) {
            // IEEE arithmetic, and eighteen written digits are the double's (DuckDB's DECIMAL(18) raised "Overflow in
            // multiplication" until S20)
            assertEquals(0.1 + 0.2, number(model, c, "|0.1 + 0.2"), backend.name());
            assertEquals(123456789.123456789 * 3.0, number(model, c, "|123456789.123456789 * 3.0"), backend.name());
            // a list's difference, reduced in the double (a DECIMAL accumulator rounded it to 0.7)
            assertEquals(1.0 - 0.1 - 0.2, number(model, c, "|[1.0, 0.1, 0.2]->minus()"), backend.name());
            // a Number root delivers the Float it holds
            assertEquals(1.5, number(model, c, "|1.5->cast(@Number)"), backend.name());
            // the three a DECIMAL fact over a double refused: a computed Float beside an Integer in a list, a fold whose
            // accumulator's fact the body widened
            // (its values: the Integer's kind in a list a computed element joins is the list's, product and edge alike
            // -- a mixed list's own question, not the literal's); a database with no list encoding refuses by name
            try {
                assertEquals(java.util.List.of(1.0, 3.0), Lean.run(model, "|let x = 1.5 * 2; [1, $x]->sort();",
                        StorelessRuntime.RUNTIME, c).rows().stream().map(r -> ((Number) r.get(0)).doubleValue())
                        .toList(), backend.name());
            } catch (com.legend.sql.DialectCapability refused) {
                assertEquals(true, String.valueOf(refused.getMessage()).contains("without an unnest placement"),
                        backend.name() + ": " + refused.getMessage());
            }
            try {
                assertEquals(4.5, number(model, c, "|[1.5, 2.5]->fold({x, a|$a + $x}, 0.5)"), backend.name());
            } catch (com.legend.sql.DialectCapability refused) {
                assertEquals(true, String.valueOf(refused.getMessage()).contains("without a fold encoding"),
                        backend.name() + ": " + refused.getMessage());
            }
        }
    }

    private static double number(String model, Connection c, String query) {
        return ((Number) Lean.run(model, query, StorelessRuntime.RUNTIME, c).rows().get(0).get(0)).doubleValue();
    }
}
