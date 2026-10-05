// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Execution;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pure null ordering is NULL-IS-LARGEST (DESC puts nulls FIRST) — and a
 * sort hoisted INTO an ordered aggregate (relation toString's
 * string_agg ORDER BY) must keep the declared placement: the renderer
 * dropped it and nulls sank to the backend default (witness PCT
 * testRange_ExplicitOffsets_WithNullValues_..._WithOrderByDESC, the
 * relation-scope wall burn's two TRUE wire bugs).
 */
class AggOrderNullPlacementTest {

    @Test
    @DisplayName("DESC NULLS FIRST survives the toString aggregate hoist")
    void descNullsFirstInsideAggregate() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            ExecutionResult r = Execution.execute(StorelessRuntime.with("", DatabaseType.DuckDB),
                    "{|#TDS\n"
                    + "  p, o, i\n"
                    + "  200, 1, 10\n"
                    + "  200, null, 20\n"
                    + "  200, 3, 30\n"
                    + "#->sort([~p->descending(), ~o->descending(),"
                    + " ~i->descending()])->toString()}", StorelessRuntime.RUNTIME,
                    conn);
            assertEquals("#TDS\n   p,o,i\n   200,null,20\n   200,3,30\n"
                    + "   200,1,10\n#",
                    ((ExecutionResult.Scalar) r).value());
        }
    }
}
