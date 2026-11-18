// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.executionplan.ExecutionPlan;
import com.legend.result.Batch;
import com.legend.result.CellText;
import com.legend.values.PureDateLiteral;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE PURE/V1 ARROW ROUTE WRITES LEGEND-PURE'S DATE TEXT (W4's audit, 2026-11-16, DoD-2: an edge answer does not move
 * with the product's): a TEMPORAL_TEXT cell and a nested date, in the exact text the route answered before W4
 * ({@link Presenter#LEGEND_PURE}: {@code +0000}, a value at its written precision, a timestamp cell at nine digits), where
 * the product's writer writes register S312's six digits and Z ({@link CellText#PRODUCT}). Arrow's utf8 data is the
 * text's bytes, uncompressed, so each is found in the stream as it is.
 */
class PureDatesArrowTest {

    private static final ExecutionPlan.Column WRITTEN = new ExecutionPlan.Column("written", "Date",
            new ExecutionPlan.Multiplicity(0, 1), "TEMPORAL_TEXT", null, ExecutionPlan.Precision.WRITTEN);
    private static final ExecutionPlan.Column INSTANT = new ExecutionPlan.Column("instant", "Date",
            new ExecutionPlan.Multiplicity(0, 1), "TEMPORAL_TEXT", null, ExecutionPlan.Precision.INSTANT);
    private static final ExecutionPlan.Column NESTED = new ExecutionPlan.Column("nested", "p::C",
            new ExecutionPlan.Multiplicity(0, 1), null, null, null);

    private static String arrow(CellText.Forms dates) throws Exception {
        List<ExecutionPlan.Column> columns = List.of(WRITTEN, INSTANT, NESTED);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowWriter w = new ArrowWriter(out, cs -> Map.of(), dates);
        w.columns(columns);
        Batch.Builder b = Batch.builder(columns, 1);
        b.add(List.of(PureDateLiteral.parse("2024-01-02T10:30"), PureDateLiteral.parse("2024-01-02T10:30:00"),
                Map.of("at", PureDateLiteral.parse("2024-01-02T10:30"))));
        w.batch(b.build());
        w.end();
        return new String(out.toByteArray(), StandardCharsets.ISO_8859_1);
    }

    @Test
    void theEdgesRouteWritesLegendPuresText() throws Exception {
        String edge = arrow(Presenter.LEGEND_PURE);
        assertTrue(edge.contains("2024-01-02T10:30+0000"), "a written minute, legend-pure's zone");
        assertTrue(edge.contains("2024-01-02T10:30:00.000000000+0000"), "a timestamp cell, nine digits and +0000");
        assertTrue(edge.contains("{\"at\":\"2024-01-02T10:30+0000\"}"), "a nested date, legend-pure's text");
        assertFalse(edge.contains("Z\""), "no product zone in a nested date");
    }

    @Test
    void theProductWritesSixDigitsAndZ() throws Exception {
        String product = arrow(CellText.PRODUCT);
        assertTrue(product.contains("2024-01-02T10:30:00.000000Z"), "S312's text");
        assertTrue(product.contains("{\"at\":\"2024-01-02T10:30:00.000000Z\"}"), "a nested date, S312's text");
        assertFalse(product.contains("+0000"), "no legend-pure zone in the product's answer");
    }
}
