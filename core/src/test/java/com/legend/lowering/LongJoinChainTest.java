// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.Execution;
import com.legend.exec.ExecutionResult;
import com.legend.exec.Row;
import com.legend.testbackend.TestBackend;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A long chain of joins keeps its rows, a LEFT join in it its NULL-padded ones, and each column read is the declared
 * one. The IQ scale program's Q16 (2026-10-23) read a chain past 32 relations as a derived table ({@code SELECT *}, so
 * DuckDB ordered each group's joins on its own), and an independent review found it read the tables' undeclared
 * columns too: a declared column of one table named like an undeclared one of another read the wrong one. It was
 * reverted; these hold any grouping to it.
 */
class LongJoinChainTest {

    private static String model(TestBackend backend) {
        return """
                ###Relational
                Database c::db
                (
                  Table T (ID INTEGER PRIMARY KEY, V INTEGER)
                  Table A (ID INTEGER PRIMARY KEY, X INTEGER)
                  Table B (BID INTEGER PRIMARY KEY, Z INTEGER)
                )
                ###Connection
                RelationalDatabaseConnection c::Conn { store: c::db; %s }
                ###Runtime
                Runtime c::RT { mappings: []; connections: [ c::db: [ c: c::Conn ] ]; }
                """.formatted(backend.connectionBody());
    }

    private static List<String> rows(Connection conn, TestBackend backend, String query) throws Exception {
        ExecutionResult r = Execution.execute(model(backend), query, "c::RT", conn);
        List<String> rows = new ArrayList<>();
        for (Row row : ((ExecutionResult.Tabular) r).rows()) {
            rows.add(row.values().toString());
        }
        return rows;
    }

    /** {@code n} joins of T to itself on its id, the {@code left}-th a LEFT join to the rows of V over 20 alone. */
    private static String chain(int n, int left) {
        StringBuilder q = new StringBuilder("|#>{c::db.T}#");
        for (int i = 1; i <= n; i++) {
            String side = "#>{c::db.T}#" + (i == left ? "->filter(x|$x.V > 20)" : "") + "->rename(~ID, ~ID" + i
                    + ")->rename(~V, ~V" + i + ")";
            String kind = i == left ? "LEFT" : "INNER";
            String prev = i == 1 ? "ID" : i == left + 1 ? "ID" : "ID" + (i - 1);
            q.append("->join(").append(side).append(", meta::pure::functions::relation::JoinKind.").append(kind)
                    .append(", {a, b|$a.").append(prev).append(" == $b.ID").append(i).append("})");
        }
        return q.append("->select(~[ID, V").append(left).append(", V").append(n).append("])->sort(~ID->ascending())")
                .toString();
    }

    @ParameterizedTest(name = "{0}: a chain of 40 joins keeps its rows, and reads its declared columns")
    @MethodSource("com.legend.testbackend.TestBackends#inProcess")
    void aLongChainKeepsItsRows(TestBackend backend) throws Exception {
        try (Connection conn = backend.open()) {
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE T (ID INTEGER PRIMARY KEY, V INTEGER)");
                st.execute("INSERT INTO T VALUES (1, 10), (2, 20), (3, 30)");
                // A holds a Z the model does not declare, beside B's declared Z
                st.execute("CREATE TABLE A (ID INTEGER PRIMARY KEY, X INTEGER, Z INTEGER)");
                st.execute("INSERT INTO A VALUES (1, 10, 999)");
                st.execute("CREATE TABLE B (BID INTEGER PRIMARY KEY, Z INTEGER)");
                st.execute("INSERT INTO B VALUES (1, 20)");
            }
            // the LEFT join keeps ids 1 and 2 padded; the joins past it read the chain's own ID
            assertEquals(List.of("[1, null, 10]", "[2, null, 20]", "[3, 30, 30]"), rows(conn, backend, chain(40, 35)),
                    backend.toString());
            // B's Z, never A's undeclared one, past 31 more joins
            StringBuilder q = new StringBuilder("|#>{c::db.A}#->join(#>{c::db.B}#, ")
                    .append("meta::pure::functions::relation::JoinKind.INNER, {a, b|$a.ID == $b.BID})");
            for (int i = 1; i <= 31; i++) {
                q.append("->join(#>{c::db.T}#->rename(~ID, ~ID").append(i).append(")->rename(~V, ~V").append(i)
                        .append("), meta::pure::functions::relation::JoinKind.INNER, {a, b|$a.ID == $b.ID")
                        .append(i).append("})");
            }
            q.append("->select(~[ID, X, Z, V31])");
            assertEquals(List.of("[1, 10, 20, 10]"), rows(conn, backend, q.toString()), backend.toString());
        }
    }
}
