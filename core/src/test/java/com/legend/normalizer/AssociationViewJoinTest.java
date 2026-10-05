// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.Execution;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Remediation T1.10 — an association whose join condition is spelled
 * THROUGH A VIEW over the source's physical table (VA.vid = TB.AID
 * while the class maps TA): the synthesis picked the target from the
 * view-RESOLVED condition (TA.ID = TB.AID) but translated the RAW
 * operation, whose VA reference binds nothing in the condition scope.
 * Both now ride the same resolved tree; pinned end to end.
 */
class AssociationViewJoinTest {

    private static final String MODEL = """
            Class model::A { id: Integer[1]; }
            Class model::B { bid: Integer[1]; }
            Association model::AB { a: model::A[1]; b: model::B[*]; }
            ###Relational
            Database db::DB (
              Table TA (ID INTEGER)
              Table TB (ID INTEGER, AID INTEGER)
              View VA ( vid: TA.ID )
              Join AB_J (VA.vid = TB.AID)
            )
            ###Mapping
            Mapping my::M (
              *model::A: Relational { ~mainTable [db::DB] TA id: TA.ID }
              *model::B: Relational { ~mainTable [db::DB] TB bid: TB.ID }
              model::AB: Relational {
                AssociationMapping ( b: [db::DB]@AB_J )
              }
            )
            ###Connection
            RelationalDatabaseConnection db::DBDuckDB { store: db::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime my::RT { mappings: [my::M]; connections: [ db::DB: [ c0: db::DBDuckDB ] ]; }
            """;

    @Test
    @DisplayName("association joining through a view column navigates")
    void viewJoinNavigates() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE TA (ID INTEGER)");
                st.execute("CREATE TABLE TB (ID INTEGER, AID INTEGER)");
                st.execute("INSERT INTO TA VALUES (1), (2)");
                st.execute("INSERT INTO TB VALUES (10, 1), (11, 1), (12, 2)");
            }
            var r = Execution.execute(MODEL,
                    "|model::A.all()->project(~[aid: a|$a.id, bid: a|$a.b.bid])"
                            + "->sort([~aid->ascending(), ~bid->ascending()])",
                    "my::RT", c);
            assertEquals(List.of("1|10", "1|11", "2|12"),
                    r.rows().stream().map(row -> row.get(0) + "|" + row.get(1))
                            .toList());
        }
    }

    private static final String VIEW_SOURCE_MODEL = MODEL.replace(
            "*model::A: Relational { ~mainTable [db::DB] TA id: TA.ID }",
            "*model::A: Relational { ~mainTable [db::DB] VA id: VA.vid }");

    @Test
    @DisplayName("view-mapped SOURCE keeps its frame columns (backing-view exemption)")
    void viewMappedSourceNavigates() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE TA (ID INTEGER)");
                st.execute("CREATE TABLE TB (ID INTEGER, AID INTEGER)");
                st.execute("INSERT INTO TA VALUES (1), (2)");
                st.execute("INSERT INTO TB VALUES (10, 1), (11, 1), (12, 2)");
            }
            var r = Execution.execute(VIEW_SOURCE_MODEL,
                    "|model::A.all()->project(~[aid: a|$a.id, bid: a|$a.b.bid])"
                            + "->sort([~aid->ascending(), ~bid->ascending()])",
                    "my::RT", c);
            assertEquals(List.of("1|10", "1|11", "2|12"),
                    r.rows().stream().map(row -> row.get(0) + "|" + row.get(1))
                            .toList());
        }
    }
}
