// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.Compiler;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A chain of joins past {@link Lowerer#JOIN_GROUP} relations is read in groups, each a derived table, and keeps its
 * rows (the IQ scale program's Q16, 2026-10-23: DuckDB ordered 350 joins as one set in 210 s). A LEFT join past a
 * group's edge keeps its NULL-padded rows.
 */
class JoinChainGroupsTest {

    private static String model(TestBackend backend) {
        return """
                ###Relational
                Database c::db
                (
                  Table T (ID INTEGER PRIMARY KEY, V INTEGER)
                )
                ###Connection
                RelationalDatabaseConnection c::Conn { store: c::db; %s }
                ###Runtime
                Runtime c::RT { mappings: []; connections: [ c::db: [ c: c::Conn ] ]; }
                """.formatted(backend.connectionBody());
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

    @ParameterizedTest(name = "{0}: a chain of 40 joins in groups keeps its rows")
    @MethodSource("com.legend.testbackend.TestBackends#inProcess")
    void aLongChainKeepsItsRows(TestBackend backend) throws Exception {
        String query = chain(40, 35);
        String sql = Compiler.query(Compiler.compileModel(model(backend)), query).plan("c::RT").sql();
        String shorter = Compiler.query(Compiler.compileModel(model(backend)), chain(20, 15)).plan("c::RT").sql();
        // a group reads its chain whole (SELECT *); a chain of 20 is not grouped
        assertTrue(sql.contains("SELECT *") && !shorter.contains("SELECT *"), "the chain is read in groups:\n" + sql);
        try (Connection conn = backend.open()) {
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE T (ID INTEGER PRIMARY KEY, V INTEGER)");
                st.execute("INSERT INTO T VALUES (1, 10), (2, 20), (3, 30)");
            }
            ExecutionResult r = Execution.execute(model(backend), query, "c::RT", conn);
            List<String> rows = new ArrayList<>();
            for (Row row : ((ExecutionResult.Tabular) r).rows()) {
                rows.add(row.values().toString());
            }
            // the LEFT join keeps ids 1 and 2 padded; the joins past it read the chain's own ID
            assertEquals(List.of("[1, null, 10]", "[2, null, 20]", "[3, 30, 30]"), rows, backend + "\n" + sql);
        }
    }
}
