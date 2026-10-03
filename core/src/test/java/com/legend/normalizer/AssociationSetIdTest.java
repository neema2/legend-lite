// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.Compiler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * An association end written with set ids ({@code b[a, b2]}) navigates to
 * THAT set, not the class's root set (engine RelationalValidator
 * .validateAssociationId resolves the ids against the mapping's sets).
 * Here B's root set reads TB1 and set b2 reads TB2; the association's join
 * lands on TB2, so b's rows are TB2's.
 */
class AssociationSetIdTest {

    private static final String MODEL = """
            Class model::A { id: Integer[1]; }
            Class model::B { bid: Integer[1]; }
            Association model::AB { a: model::A[1]; b: model::B[*]; }
            ###Relational
            Database db::DB (
              Table TA (ID INTEGER)
              Table TB1 (ID INTEGER, AID INTEGER)
              Table TB2 (ID INTEGER, AID INTEGER)
              Join AB2 (TA.ID = TB2.AID)
            )
            ###Mapping
            Mapping my::M (
              *model::A[a]: Relational { ~mainTable [db::DB] TA id: TA.ID }
              *model::B[b1]: Relational { ~mainTable [db::DB] TB1 bid: TB1.ID }
              model::B[b2]: Relational { ~mainTable [db::DB] TB2 bid: TB2.ID }
              model::AB: Relational {
                AssociationMapping ( b[a, b2]: [db::DB]@AB2 )
              }
            )
            ###Connection
            RelationalDatabaseConnection db::DBDuckDB { store: db::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime my::RT { mappings: [my::M]; connections: [ db::DB: [ c0: db::DBDuckDB ] ]; }
            """;

    @Test
    @DisplayName("an end written with a set id navigates to that set")
    void explicitTargetSetIdNavigates() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE TA (ID INTEGER)");
                st.execute("CREATE TABLE TB1 (ID INTEGER, AID INTEGER)");
                st.execute("CREATE TABLE TB2 (ID INTEGER, AID INTEGER)");
                st.execute("INSERT INTO TA VALUES (1), (2)");
                st.execute("INSERT INTO TB1 VALUES (100, 1), (200, 2)");
                st.execute("INSERT INTO TB2 VALUES (10, 1), (11, 1), (12, 2)");
            }
            var r = Compiler.execute(MODEL,
                    "|model::A.all()->project(~[aid: a|$a.id, bid: a|$a.b.bid])"
                            + "->sort([~aid->ascending(), ~bid->ascending()])",
                    "my::RT", c);
            assertEquals(List.of("1|10", "1|11", "2|12"),
                    r.rows().stream().map(row -> row.get(0) + "|" + row.get(1))
                            .toList());
        }
    }
}
