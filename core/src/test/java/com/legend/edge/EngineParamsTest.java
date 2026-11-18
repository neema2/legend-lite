// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * legend-engine's plan-template parameters are the edge's own table (the audit's J31, 2026-11-18): each lowers to a
 * plain {@link SqlExpr.PlanParam} that binds nothing (its type unknown), and what only the FreeMarker template knows --
 * an enumeration's mapping function, the temp-table protocol's bare splice -- is held by name here, never in the MIR.
 */
class EngineParamsTest {

    @Test
    void aTemplateParameterIsAPlainSlotItsTemplateDataTheEdges() {
        EngineParams params = EngineParams.none();
        params.declare("s", Type.Primitive.STRING, false, "enumMap_m_x");
        params.declare("n", Type.Primitive.INTEGER, true, null);
        assertEquals(List.of(
                new SqlExpr.PlanParam("s", SqlExpr.PlanParam.ValueType.STRING, false, com.legend.sql.SqlTyping.UNKNOWN, null),
                new SqlExpr.PlanParam("n", SqlExpr.PlanParam.ValueType.OTHER, true, com.legend.sql.SqlTyping.UNKNOWN, null)),
                List.copyOf(params.slots()));
        assertEquals("enumMap_m_x", params.enumMapFn("s"));
        assertNull(params.enumMapFn("n"));
        assertFalse(params.isSplice("inFilterClause_n"));
        EngineParams spliced = params.withProtocol(List.of("inFilterClause_n"), List.of("tempTableForIn_m"));
        assertTrue(spliced.isSplice("inFilterClause_n"));
        // the protocol's temp table, read back by legend-engine's temp-select template (J40: no MIR node)
        assertTrue(spliced.isTempTable("tempTableForIn_m"));
        assertFalse(params.isTempTable("tempTableForIn_m"));
        assertFalse(params.isSplice("inFilterClause_n"), "the table it came from is unchanged");
    }

    @Test
    void aParametersSignatureIsLegendEnginesSpelling() {
        assertEquals("String[0..1]", EngineParams.signature(Type.Primitive.STRING, Multiplicity.Bounded.ZERO_ONE));
    }
}
