// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The text plan (generatePlan, arrowPlan: {@code TypedQuery.plan}) is one statement's text, which carries neither a
 * load nor a bound path, so a query reading a {@code data:} or {@code file:} source is refused there by name before
 * render (register S188, TEMPORARY until B3 renders the text from the execution plan; A5, 2026-11-10). Its execution
 * plan carries both.
 */
class TextPlanSourceTest {

    @Test
    void aLiveSourceIsRefusedByTheTextPlanAndCarriedByTheExecutionPlan() {
        var ctx = Compiler.compileModel(DataDeliveryMatrixTest.model(DataDeliveryMatrixTest.bareConnection(
                DatabaseType.DuckDB), ""));
        for (String url : java.util.List.of(DataDeliveryMatrixTest.PAYLOAD, "file:///no/such/rows.json")) {
            var query = Compiler.query(ctx, "|sourceUrl('" + url + "')" + DataDeliveryMatrixTest.FROM_JSON);
            var refused = assertThrows(com.legend.error.NotImplementedException.class, () -> query.plan("x::RT"));
            assertTrue(refused.getMessage().contains("a text plan (generatePlan, arrowPlan) is one statement"),
                    refused.getMessage());
            query.executionPlan("x::RT");
        }
    }
}
