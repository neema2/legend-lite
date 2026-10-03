// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.exec.Column;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A pivot over a class query's projection — the shape DataCube emits
 * (project, then pivot). Every other pivot test starts from a {@code #TDS}
 * literal, which never reaches store resolution, so the resolver's missing
 * {@code TypedPivot} arm hid behind 23 green pivot tests.
 */
class PivotOverClassQueryTest {

    private static final String MODEL = """
            Class x::Sale { region: String[1]; year: Integer[1]; amount: Integer[1]; }
            ###Relational
            Database x::DB ( Table SALES (ID INTEGER PRIMARY KEY, REGION VARCHAR(16), YR INTEGER, AMOUNT INTEGER) )
            ###Mapping
            Mapping x::M ( *x::Sale: Relational { ~mainTable [x::DB] SALES
                region: [x::DB] SALES.REGION, year: [x::DB] SALES.YR, amount: [x::DB] SALES.AMOUNT } )
            ###Connection
            RelationalDatabaseConnection x::DBDuckDB { store: x::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime x::RT { mappings: [x::M]; connections: [ x::DB: [ c0: x::DBDuckDB ] ]; }
            """;

    @Test
    @DisplayName("project -> pivot over a mapped class: one column per year, totals per region")
    void pivotOverProjection() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE SALES (ID INTEGER, REGION VARCHAR, YR INTEGER, AMOUNT INTEGER)");
                st.execute("INSERT INTO SALES VALUES (1,'EU',2023,10), (2,'EU',2023,5), (3,'EU',2024,7),"
                        + " (4,'US',2023,1), (5,'US',2024,2), (6,'US',2024,3)");
            }
            var r = Compiler.execute(MODEL, "|x::Sale.all()"
                    + "->project(~[region: s|$s.region, year: s|$s.year, amount: s|$s.amount])"
                    + "->pivot(~[year], ~[total: x|$x.amount: y|$y->plus()])"
                    + "->sort(~region->ascending())", "x::RT", c);
            // a pivot column's NAME carries literal single quotes, as the engine
            // presents it (pureToSQLQuery.pure mayQuotePivotColNames); the SQL
            // column underneath stays bare
            assertEquals(List.of("region", "'2023__|__total'", "'2024__|__total'"),
                    r.columns().stream().map(Column::name).toList());
            assertEquals(List.of("EU|15|7", "US|1|5"), r.rows().stream()
                    .map(row -> row.get(0) + "|" + row.get(1) + "|" + row.get(2)).toList());
        }
    }
}
