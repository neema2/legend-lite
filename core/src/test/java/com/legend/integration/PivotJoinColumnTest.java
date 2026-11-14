// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testing.LaneFloor;

import com.legend.testing.Lean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A raw pivot LEFT-joined to a statically typed relation, then read by a
 * column only the right side has (2026-09-28; found while DataCube tried
 * joining its pivot totals in, a design main does not use). A pivot claims
 * every name (its value columns are late-bound), so resolution asked the
 * left side first and qualified the right side's column with the pivot's
 * alias: DuckDB refused it ("Values list t2 does not have a column named ...").
 */
@DisplayName("A column only the right side of a join names resolves there")
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class PivotJoinColumnTest {

    private static final String MODEL = """
            ###Relational
            Database local::DB ( Table T ( region VARCHAR(20), desk VARCHAR(20), amt DOUBLE ) )
            ###Connection
            RelationalDatabaseConnection local::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime local::RT { mappings: [ ]; connections: [ local::DB: [ environment: local::Conn ] ]; }
            """;

    /** A pivot, per-region totals joined by region (null-safely), ordered by the total. */
    private static final String QUERY = """
            #>{local::DB.T}#->select(~[region, desk, amt])
              ->pivot(~[desk], ~[amt:x|$x.amt:y|$y->sum()])
              ->join(#>{local::DB.T}#->select(~[region, amt])
                       ->groupBy(~[region], ~[total:x|$x.amt:y|$y->sum()])
                       ->rename(~region, ~key),
                     meta::pure::functions::relation::JoinKind.LEFT,
                     {a,b|($a.region == $b.key) || ($a.region->isEmpty() && $b.key->isEmpty())})
              ->sort(~total->descending())
            """;

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = LaneFloor.backend().open();
        try (var st = connection.createStatement()) {
            st.execute("CREATE TABLE T (region VARCHAR, desk VARCHAR, amt DOUBLE)");
            st.execute("INSERT INTO T VALUES ('EMEA','FX',10), ('EMEA','Rates',5),"
                    + " ('APAC','FX',100), (NULL,'Rates',7)");
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        connection.close();
    }

    @Test
    void ordersByTheRightSidesColumn() throws SQLException {
        Lean.Result r = Lean.run(MODEL, QUERY, "local::RT", connection);
        int region = r.columns().stream().map(Lean.Column::name).toList().indexOf("region");
        int total = r.columns().stream().map(Lean.Column::name).toList().indexOf("total");
        List<Object> regions = r.rows().stream().map(row -> row.get(region)).toList();
        List<Object> totals = r.rows().stream().map(row -> row.get(total)).toList();
        assertEquals(java.util.Arrays.asList("APAC", "EMEA", null), regions);
        // the null group finds its total too
        assertEquals(List.of(100.0, 15.0, 7.0), totals);
    }
}
