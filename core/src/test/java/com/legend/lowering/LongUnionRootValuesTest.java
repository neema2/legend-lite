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

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A root union of more than 64 branches keeps its values as the database delivers them. An independent review
 * (2026-10-23) found the IQ scale program's grouping of long unions (BalancedUnions, then a pass of every dialect)
 * moving a root union's branches off the root, where a dialect converting the root's values as it writes them no
 * longer did: SQLite's DECIMAL(10,2) came back 1.5, a Double. The grouping is DuckDB's alone now.
 */
class LongUnionRootValuesTest {

    private static String model(TestBackend backend) {
        return """
                ###Relational
                Database c::db
                (
                  Table T (ID INTEGER PRIMARY KEY, D DECIMAL(10, 2))
                )
                ###Connection
                RelationalDatabaseConnection c::Conn { store: c::db; %s }
                ###Runtime
                Runtime c::RT { mappings: []; connections: [ c::db: [ c: c::Conn ] ]; }
                """.formatted(backend.connectionBody());
    }

    @ParameterizedTest(name = "{0}: a root union of 65 branches keeps its decimals")
    @MethodSource("com.legend.testbackend.TestBackends#inProcess")
    void aLongRootUnionKeepsItsDecimals(TestBackend backend) throws Exception {
        String branch = "#>{c::db.T}#->filter(x|$x.ID == 1)->select(~[ID, D])";
        StringBuilder q = new StringBuilder("|").append(branch);
        for (int i = 1; i < 65; i++) {
            q.append("->concatenate(").append(branch).append(")");
        }
        try (Connection conn = backend.open()) {
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE T (ID INTEGER PRIMARY KEY, D DECIMAL(10, 2))");
                st.execute("INSERT INTO T VALUES (1, 1.50), (2, 3.00)");
            }
            ExecutionResult r = Execution.execute(model(backend), q.toString(), "c::RT", conn);
            int n = 0;
            for (Row row : ((ExecutionResult.Tabular) r).rows()) {
                Object d = row.values().get(1);
                assertEquals("BigDecimal 1.50", d.getClass().getSimpleName() + " " + d, backend + " row " + n);
                n++;
            }
            assertEquals(65, n, backend.toString());
        }
    }
}
