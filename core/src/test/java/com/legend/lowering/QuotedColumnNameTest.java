// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.Execution;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A column whose name needs quoting is usable EVERYWHERE — the DataCube shapes
 * that broke over non-ASCII / spaced / punctuated headers (datacube/dual-plane
 * 0f5176f80, df2cce6dd): sort and groupBy keys, a subtotal grid (group then sort
 * on the same column), a pivot, a derived column, not only select and filter.
 * The store's quotes are a spelling, not the name: relation space is bare (the
 * engine's) and quoting reaches the SQL as a rendering fact. Checked on ROWS, on
 * DuckDB and on H2 — H2 folds unquoted identifiers to upper case, so a column
 * declared quoted in mixed case ({@code "firstName"}) must still be read by its
 * exact spelling.
 */
class QuotedColumnNameTest {

    private static final String MODEL = """
            ###Relational
            Database local::DB
            (
                Table t
                (
                    "total pnl" DOUBLE,
                    "x,y" VARCHAR(32),
                    "a\\"b" VARCHAR(32),
                    "sales region" VARCHAR(32),
                    "製品" VARCHAR(32),
                    "مبلغ" DOUBLE,
                    "firstName" VARCHAR(32),
                    plain VARCHAR(32)
                )
            )
            ###Connection
            RelationalDatabaseConnection local::Conn
            { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime local::RT
            { mappings: []; connections: [ local::DB: [ c1: local::Conn ] ]; }
            """;

    private static List<String> run(String engine, String query) throws Exception {
        String url = "duckdb".equals(engine) ? "jdbc:duckdb:" : "jdbc:h2:mem:quoted" + System.nanoTime();
        try (Connection c = DriverManager.getConnection(url)) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE t (\"total pnl\" DOUBLE, \"x,y\" VARCHAR(32), \"a\"\"b\" VARCHAR(32),"
                        + " \"sales region\" VARCHAR(32), \"製品\" VARCHAR(32), \"مبلغ\" DOUBLE,"
                        + " \"firstName\" VARCHAR(32), plain VARCHAR(32))");
                st.execute("INSERT INTO t VALUES"
                        + " (3.5, 'k1', 'q2', 'EU', '茶', 10.0, 'Cy', 'p1'),"
                        + " (1.5, 'k2', 'q1', 'US', '米', 4.0, 'Al', 'p2'),"
                        + " (2.0, 'k1', 'q3', 'EU', '茶', 6.0, 'Bo', 'p3')");
            }
            // the runtime declares the engine the session runs on
            String model = "duckdb".equals(engine) ? MODEL : MODEL.replace(
                    "{ type: DuckDB; specification: DuckDB { }; auth: Test; }",
                    "{ type: H2; specification: LocalH2 { }; auth: DefaultH2; }");
            var r = Execution.execute(model, query, "local::RT", c);
            return r.rows().stream().map(row -> {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < r.columns().size(); i++) {
                    sb.append(i == 0 ? "" : "|").append(row.get(i));
                }
                return sb.toString();
            }).toList();
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"duckdb", "h2"})
    @DisplayName("sort by a column whose name has a space")
    void sortBySpacedName(String engine) throws Exception {
        assertEquals(List.of("1.5|p2", "2.0|p3", "3.5|p1"), run(engine,
                "|#>{local::DB.t}#->select(~['total pnl', plain])->sort([~'total pnl'->ascending()])"));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"duckdb", "h2"})
    @DisplayName("group by a spaced name, then sort on the same column (a subtotal grid)")
    void groupThenSortSpacedName(String engine) throws Exception {
        assertEquals(List.of("EU|5.5", "US|1.5"), run(engine,
                "|#>{local::DB.t}#->groupBy(~['sales region'], ~[s: x|$x.'total pnl': y|$y->sum()])"
                        + "->sort([~'sales region'->ascending()])"));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"duckdb", "h2"})
    @DisplayName("group by a column whose name has a comma")
    void groupByCommaName(String engine) throws Exception {
        assertEquals(List.of("k1|5.5", "k2|1.5"), run(engine,
                "|#>{local::DB.t}#->groupBy(~['x,y'], ~[s: x|$x.'total pnl': y|$y->sum()])"
                        + "->sort([~'x,y'->ascending()])"));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"duckdb", "h2"})
    @DisplayName("non-ASCII headers: group, aggregate and sort")
    void nonAsciiNames(String engine) throws Exception {
        assertEquals(List.of("米|4.0", "茶|16.0"), run(engine,
                "|#>{local::DB.t}#->groupBy(~['製品'], ~[m: x|$x.'مبلغ': y|$y->sum()])"
                        + "->sort([~'製品'->ascending()])"));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"duckdb", "h2"})
    @DisplayName("a derived column over a quoted column")
    void derivedOverQuoted(String engine) throws Exception {
        assertEquals(List.of("p1|20.0", "p2|8.0", "p3|12.0"), run(engine,
                "|#>{local::DB.t}#->extend(~[d: x|$x.'مبلغ'->toOne() * 2])->select(~[plain, d])"
                        + "->sort([~plain->ascending()])"));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"duckdb", "h2"})
    @DisplayName("sort by a column whose name has a quote")
    void sortByQuotedQuoteName(String engine) throws Exception {
        assertEquals(List.of("q1", "q2", "q3"), run(engine,
                "|#>{local::DB.t}#->select(~['a\"b'])->sort([~'a\"b'->ascending()])"));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"duckdb", "h2"})
    @DisplayName("a mixed-case name declared quoted is read by its exact spelling")
    void mixedCaseQuoted(String engine) throws Exception {
        assertEquals(List.of("Al", "Bo", "Cy"), run(engine,
                "|#>{local::DB.t}#->select(~[firstName])->sort([~firstName->ascending()])"));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"duckdb", "h2"})
    @DisplayName("plain names still resolve")
    void plainName(String engine) throws Exception {
        assertEquals(List.of("p1", "p2", "p3"), run(engine,
                "|#>{local::DB.t}#->select(~[plain])->sort([~plain->ascending()])"));
    }

    @org.junit.jupiter.api.Test
    @DisplayName("pivot on a quoted column (DataCube's engine: DuckDB)")
    void pivotOnQuotedColumn() throws Exception {
        assertEquals(List.of("EU|5.5|null", "US|null|1.5"), run("duckdb",
                "|#>{local::DB.t}#->select(~['sales region', 'x,y', 'total pnl'])"
                        + "->pivot(~['x,y'], ~[s: x|$x.'total pnl': y|$y->sum()])"
                        + "->sort([~'sales region'->ascending()])"));
    }

    @org.junit.jupiter.api.Test
    @DisplayName("a column that does not exist is still refused")
    void missingColumnRefused() {
        assertThrows(RuntimeException.class,
                () -> run("duckdb", "|#>{local::DB.t}#->sort([~'no such column'->ascending()])"));
    }
}
