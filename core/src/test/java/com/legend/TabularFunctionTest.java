// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code TabularFunction} — a named relation that is a function CALL. The
 * grammar accepted one and the protocol round-tripped it, then a reference to
 * it failed to compile ("unknown table"): the parser admitted what the compiler
 * denied existed (datacube/dual-plane b829ac2a3). Upstream models it beside
 * {@code Table} (both {@code NamedRelation}s with declared columns) and renders
 * it as a call, {@code schema.fn()} — with no arguments: the grammar declares
 * columns only and the protocol carries no parameters, so none is authorable.
 * Checked on rows: DuckDB answers a table macro.
 */
class TabularFunctionTest {

    private static final String MODEL = """
            ###Relational
            Database tf::DB
            (
              Table PLAIN ( a VARCHAR(64), n DOUBLE )
              TabularFunction FN ( a VARCHAR(64), n DOUBLE )
            )
            ###Connection
            RelationalDatabaseConnection tf::Conn
            { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime tf::RT
            { mappings: []; connections: [ tf::DB: [ c1: tf::Conn ] ]; }
            """;

    private static List<String> run(String query) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE MACRO FN() AS TABLE SELECT * FROM (VALUES ('EMEA', 1.5), ('EMEA', 2.0),"
                        + " ('APAC', 4.0)) v(a, n)");
                st.execute("CREATE TABLE PLAIN (a VARCHAR, n DOUBLE)");
                st.execute("INSERT INTO PLAIN VALUES ('X', 9.0)");
            }
            var r = Execution.execute(MODEL, query, "tf::RT", c);
            return r.rows().stream().map(row -> row.get(0) + "|" + row.get(1)).toList();
        }
    }

    @Test
    @DisplayName("a tabular function is read like a table: select")
    void selectFromTabularFunction() throws Exception {
        assertEquals(List.of("APAC|4.0", "EMEA|1.5", "EMEA|2.0"),
                run("|#>{tf::DB.FN}#->select(~[a, n])->sort([~a->ascending(), ~n->ascending()])"));
    }

    @Test
    @DisplayName("filter and groupBy over a tabular function keep calling it")
    void filterAndGroupBy() throws Exception {
        assertEquals(List.of("EMEA|3.5"),
                run("|#>{tf::DB.FN}#->filter(x|$x.a == 'EMEA')->groupBy(~[a], ~[m: x|$x.n: y|$y->sum()])"));
    }

    @Test
    @DisplayName("it renders as a call; a plain table as a reference")
    void rendersAsCall() {
        String fn = Compiler.plan(MODEL, "#>{tf::DB.FN}#->select(~[a, n])", "tf::RT").sql();
        assertTrue(fn.contains("FN()"), "a tabular function is CALLED: " + fn);
        String plain = Compiler.plan(MODEL, "#>{tf::DB.PLAIN}#->select(~[a, n])", "tf::RT").sql();
        assertTrue(plain.contains("PLAIN") && !plain.contains("PLAIN()"), plain);
    }

    @Test
    @DisplayName("a plain table still reads its rows")
    void plainTable() throws Exception {
        assertEquals(List.of("X|9.0"), run("|#>{tf::DB.PLAIN}#->select(~[a, n])"));
        assertFalse(run("|#>{tf::DB.PLAIN}#->select(~[a, n])").isEmpty());
    }
}
