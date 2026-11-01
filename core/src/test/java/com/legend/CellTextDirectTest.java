// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;
import com.legend.result.CellText;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A cell's JSON token written straight into the writer ({@link CellText#writePureJson}, {@link CellText#writeEngine}:
 * the tier 1 of the product path's payoff, 2026-11-01) is the token its node writes ({@link CellText#pureJson},
 * {@link CellText#engine}), character for character: every carrier a batch holds, under every column kind and
 * written-precision fact that changes a spelling, written alone and back to back as the writers write them.
 */
class CellTextDirectTest {

    private static final List<String> TYPES = List.of("Integer", "Float", "Decimal", "Number", "String", "Boolean",
            "Date", "DateTime", "StrictDate", "StrictTime", "Any", "meta::pure::metamodel::variant::Variant");

    private static List<ExecutionPlan.Column> columns() {
        List<ExecutionPlan.Column> out = new ArrayList<>();
        for (String type : TYPES) {
            out.add(new ExecutionPlan.Column("c", type, new ExecutionPlan.Multiplicity(0, 1), null, null, null));
            for (ExecutionPlan.Precision p : ExecutionPlan.Precision.values()) {
                out.add(new ExecutionPlan.Column("c", type, new ExecutionPlan.Multiplicity(0, 1), null, null, p));
            }
        }
        return out;
    }

    private static List<Object> cells() {
        Map<String, Object> struct = new LinkedHashMap<>();
        struct.put("a", 1L);
        struct.put("b", "x\"y");
        struct.put("c", new BigDecimal("2.50"));
        struct.put("d", PureDateLiteral.parse("2024-01-02T03:04:05.123"));
        struct.put("e", null);
        return Arrays.asList(null, 0L, -1L, Long.MAX_VALUE, Long.MIN_VALUE,
                new BigInteger("18446744073709551614"), new BigInteger("-5"),
                0.0, -0.0, 1.5, 1e-4, 1e21, 123456789.123, Double.NaN, Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY, new BigDecimal("0"), new BigDecimal("2.50"), new BigDecimal("-0.001"),
                new BigDecimal("1E+3"), new BigDecimal("123456789012345678901234567890.5"),
                new BigDecimal("18446744073709551614"), true, false, "", "plain", "q\"uo\\te\n\u0001é",
                PureDateLiteral.parse("2024-01-02"), PureDateLiteral.parse("2024-01-02T03:04:05"),
                PureDateLiteral.parse("2024-01-02T03:04:05.123456789"), PureDateLiteral.parse("2024-01-02T03"),
                PureDateLiteral.parse("2024-01"), PureDateLiteral.parse("2024"),
                PureDateLiteral.fromDatabaseTimestamp(java.time.LocalDateTime.of(2024, 1, 2, 0, 0)),
                PureTimeLiteral.parse("10:30:00"), PureTimeLiteral.parse("10:30:00.5"),
                List.of(1L, "a", 2.5), List.of(), struct, Map.of());
    }

    @Test
    void theDirectTokensAreTheNodes() {
        int compared = 0;
        for (ExecutionPlan.Column c : columns()) {
            for (Object cell : cells()) {
                compared += same(cell, c, true) + same(cell, c, false);
            }
        }
        assertEquals(true, compared > 1000, "compared " + compared);
    }

    /** Back to back at the root, as a batch's writer writes a row's cells: no separator added or lost. */
    @Test
    void cellsBackToBackAreTheNodesBackToBack() {
        for (ExecutionPlan.Column c : columns()) {
            StringBuilder direct = new StringBuilder();
            StringBuilder nodes = new StringBuilder();
            Json.Writer w = Json.compactWriter(direct);
            Json.Writer n = Json.compactWriter(nodes);
            w.beginArray();
            n.beginArray();
            for (Object cell : cells()) {
                if (ok(() -> CellText.pureJson(cell, c))) {
                    CellText.writePureJson(w, cell, c);
                    Json.write(n, CellText.pureJson(cell, c));
                }
            }
            w.endArray();
            n.endArray();
            assertEquals(nodes.toString(), direct.toString(), c.toString());
        }
    }

    /** One cell's two forms, equal; a cell the node refuses is refused the same way directly. 1 compared, 0 not. */
    private static int same(Object cell, ExecutionPlan.Column c, boolean pure) {
        StringBuilder direct = new StringBuilder();
        Json.Writer w = Json.compactWriter(direct);
        Json.Node node;
        try {
            node = pure ? CellText.pureJson(cell, c) : CellText.engine(cell, c);
        } catch (RuntimeException refused) {
            RuntimeException again = assertThrows(RuntimeException.class, () -> {
                if (pure) {
                    CellText.writePureJson(w, cell, c);
                } else {
                    CellText.writeEngine(w, cell, c);
                }
            }, "refused as a node, written directly: " + cell + " under " + c);
            assertEquals(refused.getClass(), again.getClass());
            assertEquals(refused.getMessage(), again.getMessage());
            return 0;
        }
        if (pure) {
            CellText.writePureJson(w, cell, c);
        } else {
            CellText.writeEngine(w, cell, c);
        }
        assertEquals(Json.toCompact(node), direct.toString(), (pure ? "pure " : "engine ") + cell + " under " + c);
        return 1;
    }

    private static boolean ok(java.util.function.Supplier<Object> f) {
        try {
            return f.get() != null;
        } catch (RuntimeException refused) {
            return false;
        }
    }
}
