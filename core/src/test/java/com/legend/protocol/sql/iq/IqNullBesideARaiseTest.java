// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.Execution;
import com.legend.exec.ExecutionResult;
import com.legend.exec.Row;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * An operand that raises beside one that is NULL, as IQ computes it (the IQ scale program's Q30, 2026-10-24; the
 * answers of lite/null_beside_a_raise.yaml, measured on IQ 17.1 and SQL Anywhere 17): IQ's engine computes every
 * operand, so {@code NULLIF(sm, 0) + it / sm} over a zero {@code sm} raises its divide-by-zero whichever side the NULL
 * is on; SQL Anywhere's front end computes them from the left and stops at the first NULL, so it answers NULL there and
 * raises for {@code it / sm + NULLIF(sm, 0)}. Every in-process database answers each alike.
 */
class IqNullBesideARaiseTest {

    /** Row 3 holds zeros, row 5 NULLs, row 1 values. */
    private static final String ROWS = "INSERT INTO t VALUES (1, 2, 7, 9, 1.50), (3, 0, 0, 0, 0.00),"
            + " (5, NULL, NULL, NULL, NULL)";

    private static final String RAISES = "raises";

    static List<Arguments> cases() {
        String[][] table = {
                // IQ's engine (a query over IQ's tables): every operand computed (an untyped NULL is no operand there,
                // IQ's error -1009145, refused by name)
                {"iq17-default", "NULLIF(sm, 0) + it / sm", "3", RAISES},
                {"iq17-default", "it / sm + NULLIF(sm, 0)", "3", RAISES},
                {"iq17-default", "CAST(NULL AS INTEGER) + it / sm", "3", RAISES},
                {"iq17-default", "NULLIF(it, 0) * (bg / sm)", "3", RAISES},
                {"iq17-default", "nm / NULLIF(sm, 0) + (it + bg + id) / sm", "3", RAISES},
                {"iq17-default", "NULLIF(sm, 0) + it / NULLIF(sm, 0)", "3", null},
                {"iq17-default", "NULLIF(sm, 0) + it / sm", "1", "5"},
                {"iq17-default", "it / sm + NULLIF(sm, 0) + bg / sm", "5", null},
                // SQL Anywhere's front end: from the left, to the first NULL (a NULL constant read first: Q30's second
                // batch, it / sm + NULL is NULL there)
                {"sqla17-default", "NULLIF(sm, 0) + it / sm", "3", null},
                {"sqla17-default", "it / sm + NULLIF(sm, 0)", "3", RAISES},
                {"sqla17-default", "NULL + it / sm", "3", null},
                {"sqla17-default", "it / sm + NULL", "3", null},
                {"sqla17-default", "NULLIF(it, 0) * (bg / sm)", "3", null},
                {"sqla17-default", "(bg / sm) * NULLIF(it, 0)", "3", RAISES},
                {"sqla17-default", "NULLIF(sm, 0) + it / sm", "1", "5"},
                // IQ's engine: a comparison's operands and a function's arguments (Q30's second batch)
                {"iq17-default", "CASE WHEN NULLIF(sm, 0) < it / sm THEN 1 ELSE 0 END", "3", RAISES},
                {"iq17-default", "CASE WHEN it / sm < NULLIF(sm, 0) THEN 1 ELSE 0 END", "3", RAISES},
                {"iq17-default", "CASE WHEN NULLIF(sm, 0) < it / sm THEN 1 ELSE 0 END", "1", "1"},
                {"iq17-default", "MOD(NULLIF(it, 0), bg / sm)", "3", RAISES},
                {"iq17-default", "MOD(NULLIF(it, 0), bg / sm)", "1", "3"},
        };
        List<Arguments> out = new ArrayList<>();
        for (TestBackend backend : TestBackends.inProcess()) {
            for (String[] c : table) {
                out.add(Arguments.of(backend, c[0], c[1], c[2], c[3]));
            }
        }
        return out;
    }

    private static String model(TestBackend backend) {
        return """
                ###Relational
                Database e2e::DB
                (
                  Table t (id INTEGER, sm SMALLINT, it INTEGER, bg BIGINT, nm DECIMAL(10, 2))
                )
                ###Connection
                RelationalDatabaseConnection e2e::Conn { store: e2e::DB; %s }
                ###Runtime
                Runtime e2e::RT { mappings: []; connections: [ e2e::DB: [ c: e2e::Conn ] ]; }
                """.formatted(backend.connectionBody());
    }

    @ParameterizedTest(name = "{0} {1}: {2} at row {3} is {4}")
    @MethodSource("cases")
    void anOperandThatRaisesBesideANull(TestBackend backend, String profile, String expr, String id,
            String expected) throws Exception {
        String island = "|#IQ:" + profile + "{SELECT " + expr + " AS v FROM tb('e2e::DB.t') AS t WHERE id = " + id
                + "}#";
        try (Connection conn = backend.open()) {
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE t (id INTEGER, sm SMALLINT, it INTEGER, bg BIGINT, nm DECIMAL(10, 2))");
                st.execute(ROWS);
            }
            ExecutionResult r;
            try {
                r = Execution.execute(model(backend), island, "e2e::RT", conn);
            } catch (Exception e) {
                if (!RAISES.equals(expected)) {
                    throw new AssertionError(island + " raised where it answers " + expected, e);
                }
                String message = String.valueOf(e.getMessage()).toLowerCase(Locale.ROOT);
                assertTrue(message.contains("zero"), island + ": " + e.getMessage());
                return;
            }
            if (RAISES.equals(expected)) {
                fail(island + " answered where IQ raises its divide-by-zero");
            }
            List<String> cells = new ArrayList<>();
            for (Row row : ((ExecutionResult.Tabular) r).rows()) {
                Object v = row.values().get(0);
                cells.add(v == null ? null : v.toString());
            }
            assertEquals(java.util.Collections.singletonList(expected), cells, island);
        }
    }
}
