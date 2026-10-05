// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.Execution;
import com.legend.testing.KnownDefect;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An explicitly dated navigation to a milestoned class filters the target's versions no
 * matter what other columns the source table carries. When an unmapped source column
 * spells {@code <property>_<target column>}, Pipelines.slotPrefix mints
 * {@code product_2_}; TemporalFrame looks the join up by {@code product_} (the alias key
 * StoreResolver writes), misses, and treats the target as a physical slot dated by the
 * (empty) root context: the window is dropped and every version joins.
 */
class NavPrefixCollisionTemporalTest {

    private static String model(String extraOrderColumn) {
        return """
                Class c::Order { id: Integer[1]; product: c::Product[0..1]; }
                Class <<temporal.businesstemporal>> c::Product { name: String[1]; }
                ###Relational
                Database c::DB (
                  Table OrderT ( ID INTEGER PRIMARY KEY, PID INTEGER%s )
                  Table ProdT (
                    milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                    ID INTEGER PRIMARY KEY, name VARCHAR(64), from_z DATE, thru_z DATE )
                  Join OP (OrderT.PID = ProdT.ID)
                )
                ###Mapping
                Mapping c::M (
                  *c::Order : Relational { ~mainTable [c::DB] OrderT
                    id: OrderT.ID, product: [c::DB]@OP }
                  *c::Product : Relational { ~mainTable [c::DB] ProdT name: ProdT.name }
                )
                ###Connection
                RelationalDatabaseConnection c::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
                ###Runtime
                Runtime c::RT { mappings: [c::M]; connections: [ c::DB: [ c1: c::Conn ] ]; }
                """.formatted(extraOrderColumn);
    }

    private static final String QUERY =
            "c::Order.all()->project(~[id: o|$o.id, name: o|$o.product(%2015-06-01).name])";

    private static List<String> rows(String model) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE OrderT (ID INTEGER, PID INTEGER, product_name VARCHAR)");
                st.execute("INSERT INTO OrderT VALUES (1, 10, 'unmapped')");
                st.execute("CREATE TABLE ProdT (ID INTEGER, name VARCHAR, from_z DATE, thru_z DATE)");
                st.execute("INSERT INTO ProdT VALUES"
                        + " (10, 'old', DATE '2010-01-01', DATE '2015-01-01'),"
                        + " (10, 'new', DATE '2015-01-01', DATE '9999-12-31')");
            }
            var r = Execution.execute(model, QUERY, "c::RT", c);
            List<String> out = new ArrayList<>();
            for (var row : r.rows()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < r.columns().size(); i++) {
                    sb.append(i == 0 ? "" : "|").append(row.get(i));
                }
                out.add(sb.toString());
            }
            return out;
        }
    }

    @Test
    void datedNavigationFiltersTargetVersions() throws Exception {
        String m = model("");
        String sql = Compiler.compile(m, QUERY, "c::RT");
        assertTrue(sql.contains("from_z <= DATE '2015-06-01'"), sql);
        assertEquals(List.of("1|new"), rows(m));
    }

    @Test
    @KnownDefect(owner = "W4.3", reason = "a slot prefix minted clear of a colliding source"
            + " column (product_2_) misses TemporalFrame's alias-keyed navPrefixToClass, so the"
            + " dated navigation is stamped as a physical slot with the empty root context")
    void datedNavigationFiltersTargetVersionsWhenPrefixCollides() throws Exception {
        String m = model(", product_name VARCHAR(64)");
        String sql = Compiler.compile(m, QUERY, "c::RT");
        assertTrue(sql.contains("from_z <= DATE '2015-06-01'"), sql);
        assertEquals(List.of("1|new"), rows(m));
    }
}
