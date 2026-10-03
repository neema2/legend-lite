// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.Compiler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A mapping {@code ~groupBy} using the engine dynafunction {@code average(COL)}
 * (extensionDefaults.pure renders it {@code avg(1.0 * %s)}) lowers and executes.
 * Until the 2026-09-11 audit this test wrote {@code avg(COL)} against an invented
 * lite shim: the engine registers no {@code avg} dynafunction.
 */
class GroupByAverageMappingTest {

    private static final String MODEL = """
            Class g::Acct { k: String[1]; avgQty: Float[1]; }
            ###Relational
            Database g::DB ( Table T (K VARCHAR(10), QTY INTEGER) )
            ###Mapping
            Mapping g::M (
              *g::Acct : Relational {
                ~groupBy([g::DB] T.K)
                ~mainTable [g::DB] T
                k: T.K,
                avgQty: average(T.QTY)
              }
            )
            ###Connection
            RelationalDatabaseConnection g::DBDuckDB { store: g::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime g::RT { mappings: [g::M]; connections: [ g::DB: [ c0: g::DBDuckDB ] ]; }
            """;

    @Test
    @DisplayName("~groupBy average(COL) lowers and executes")
    void averageLowersAndExecutes() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE T (K VARCHAR, QTY INTEGER)");
                st.execute("INSERT INTO T VALUES ('a', 10), ('a', 20), ('b', 5)");
            }
            var r = Compiler.execute(MODEL,
                    "|g::Acct.all()->project(~[k: x|$x.k, q: x|$x.avgQty])"
                            + "->sort(~k->ascending())", "g::RT", c);
            assertEquals(List.of("a|15.0", "b|5.0"),
                    r.rows().stream().map(row -> row.get(0) + "|" + row.get(1))
                            .toList());
        }
    }
}
