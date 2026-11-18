// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE NET UNDER THE HARNESS'S H2 SESSIONS (the audit's J22, 2026-11-17, the orchestrator's ruling): a test's
 * {@code TestBackend.open()} is the product's database (H2's own settings), and an edge run opens
 * {@code open(PlanStyle)}; exec's read-back refuses by name an edge plan run on a product session, so a caller that
 * forgot the style cannot pass silently. On the edge's own session the same plan runs.
 */
class EdgeH2SessionTest {

    @Test
    void anEdgePlanOnAProductSessionIsRefusedByName_onItsOwnSessionItRuns() throws Exception {
        TestBackend h2 = TestBackends.of(DatabaseType.H2);
        String model = h2.withStorelessRuntime("");
        try (Connection product = h2.open()) {
            Exception refused = assertThrows(Exception.class, () -> com.legend.testing.Lean.runInStyle(model, "|1",
                    com.legend.testbackend.StorelessRuntime.RUNTIME, product, PureText.UPSTREAM));
            assertTrue(chain(refused).contains("the session's database does not have the settings the plan's target"
                    + " declares"), chain(refused));
        }
        assertEquals(1L, ((Number) com.legend.testing.Lean.runInStyle(model, "|1",
                com.legend.testbackend.StorelessRuntime.RUNTIME, h2, PureText.UPSTREAM).rows().get(0).get(0)).longValue());
    }

    private static String chain(Throwable t) {
        StringBuilder out = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            out.append(c.getMessage()).append(" | ");
        }
        return out.toString();
    }
}
