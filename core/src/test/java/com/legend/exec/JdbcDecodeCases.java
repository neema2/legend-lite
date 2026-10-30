// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The typed seam's decode (docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §3 and §4; the plan path program's step 7,
 * 2026-10-18): a JDBC session's typed rows, each cell read from the driver by the plan's column -- the slot first,
 * the Pure type where the slot is none -- as the one carrier the judges read. The cases are each database's own
 * ({@code JdbcDecodeDuckDbTest}, {@code JdbcDecodeH2Test}: the drivers hand the same kinds back in different Java
 * classes; split by database at main's merge, 2026-10-22); these are their columns and their read.
 */
final class JdbcDecodeCases {

    private JdbcDecodeCases() {
    }

    // (no written-precision fact: the decoders read none; it is the writers' -- CellText -- and ResultWritersTest's)
    static ExecutionPlan.Column col(String name, String type, String slot) {
        return new ExecutionPlan.Column(name, type, new ExecutionPlan.Multiplicity(1, 1), slot, null, null);
    }

    static ExecutionPlan.Column opt(String name, String type, String slot) {
        return new ExecutionPlan.Column(name, type, new ExecutionPlan.Multiplicity(0, 1), slot, null, null);
    }

    static ExecutionPlan.Column many(String name, String type, String slot) {
        return new ExecutionPlan.Column(name, type, new ExecutionPlan.Multiplicity(0, null), slot, null, null);
    }

    static List<List<Object>> rows(Connection c, com.legend.model.ConnectionDefinition.DatabaseType type, String sql,
            List<ExecutionPlan.Column> columns) throws Exception {
        List<List<Object>> out = new ArrayList<>();
        // checked against its database first, as the runner checks every session (the timestamp re-read is its fact)
        JdbcStatements st = new JdbcStatements(c);
        st.check(new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(type),
                new ExecutionPlan.Servers.Every(), List.of(), List.of()));
        st.rows(sql, List.of(), List.of(), columns, out::add);
        return out;
    }

    static List<Object> row(Connection c, com.legend.model.ConnectionDefinition.DatabaseType type, String sql,
            List<ExecutionPlan.Column> columns) throws Exception {
        List<List<Object>> rows = rows(c, type, sql, columns);
        assertEquals(1, rows.size(), sql);
        return rows.get(0);
    }
}
