// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * DERIVED GRAPH LEAF whose body navigates through a DATED hop reading
 * the GENERATED context date (corpus testMilestonedClassAtRootWithQualifierBD:
 * {@code classificationTypeStr(){$this.classification($this.businessDate).type}})
 * — map §7 contract 4: qualified properties are CHILD COMPUTATIONS
 * (correlated scalar subqueries), the context read normalizes to the
 * fetch's root date.
 */
class ResolveDerivedLeafProbeTest {

    private static final String MODEL = """
            Class <<temporal.businesstemporal>> q::Product {
              name: String[1];
              classification: q::Classification[0..1];
              classificationTypeStr() {$this.classification($this.businessDate).type->toOne()} : String[1];
            }
            Class <<temporal.businesstemporal>> q::Classification { type: String[1]; }
            ###Relational
            Database q::DB (
              Table ProductTable (
                milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                id INTEGER PRIMARY KEY, name VARCHAR(200), type VARCHAR(200),
                from_z DATE, thru_z DATE)
              Table ClassificationTable (
                milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                type VARCHAR(200) PRIMARY KEY, typeName VARCHAR(200),
                from_z DATE, thru_z DATE)
              Join ProdClass (ProductTable.type = ClassificationTable.type)
            )
            ###Mapping
            Mapping q::M (
              *q::Product : Relational { ~mainTable [q::DB] ProductTable
                name: ProductTable.name,
                classification: [q::DB] @ProdClass }
              *q::Classification : Relational { ~mainTable [q::DB] ClassificationTable
                type: ClassificationTable.type }
            )
            ###Connection
            RelationalDatabaseConnection q::DBDuckDB { store: q::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime q::RT { mappings: [q::M]; connections: [ q::DB: [ c0: q::DBDuckDB ] ]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ProductTable (id INTEGER, name VARCHAR(200),"
                    + " type VARCHAR(200), from_z DATE, thru_z DATE)");
            st.execute("CREATE TABLE ClassificationTable (type VARCHAR(200),"
                    + " typeName VARCHAR(200), from_z DATE, thru_z DATE)");
            st.execute("INSERT INTO ProductTable VALUES"
                    + " (1, 'P1', 'STOCK', DATE '2015-01-01', DATE '9999-12-31')");
            // two classification versions; only 'STOCK' current at 2015-08-20
            st.execute("INSERT INTO ClassificationTable VALUES"
                    + " ('STOCK', 'Old', DATE '2014-01-01', DATE '2015-01-01'),"
                    + " ('STOCK', 'Cur', DATE '2015-01-01', DATE '9999-12-31')");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    @Test
    @DisplayName("derived leaf with dated nav + generated-date read = child computation")
    void derivedLeafDatedNav() throws SQLException {
        String query = "q::Product.all(%2015-08-20)"
                + "->graphFetch(#{q::Product{name, classificationTypeStr()}}#)"
                + "->serialize(#{q::Product{name, classificationTypeStr()}}#)"
                + "->from(q::M, q::RT)";
        ExecutionResult r = Compiler.execute(MODEL, query, "q::RT", conn);
        String json = r instanceof ExecutionResult.Graph g ? g.json()
                : String.valueOf(r);
        System.out.println("[derived-leaf] " + json);
        assertEquals("[{\"name\":\"P1\","
                + "\"classificationTypeStr()\":\"STOCK\"}]", json);
    }
}
