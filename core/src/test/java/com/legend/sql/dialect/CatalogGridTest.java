// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SqlSource.Catalog}, the {@code fetchDb*MetaData} catalog grids (the dialect program's Phase 3, 2026-10-16):
 * the dialects whose databases keep the SQL standard's {@code information_schema} write the schema, table and key
 * grids as the compiler wrote them (its {@code CatalogGrids}, run at 5ef3a00), the column grid's types as each
 * database's own JDBC driver reports them (legend-pure's native is {@code DatabaseMetaData.getColumns}), their
 * databases answer every grid, and every other dialect refuses a grid rather than guess its catalog.
 */
class CatalogGridTest {

    /** The text {@code compiler/spec/CatalogGrids.fetchSql} wrote at 5ef3a00, each grid without patterns and with
     *  them (schema {@code S%}, table {@code T'_x}), run from that code. Its column grid read a DuckDB type name as
     *  H2's, so H2's VARCHAR was OTHER: that grid is checked against each driver below. */
    private static final String WRITTEN_BEFORE = """
            SCHEMAS\tSELECT upper(schema_name) AS "TABLE_SCHEM", upper(catalog_name) AS "TABLE_CATALOG" FROM \
            information_schema.schemata WHERE schema_name NOT IN ('information_schema','pg_catalog') ORDER BY 1
            SCHEMAS\tSELECT upper(schema_name) AS "TABLE_SCHEM", upper(catalog_name) AS "TABLE_CATALOG" FROM \
            information_schema.schemata WHERE schema_name NOT IN ('information_schema','pg_catalog') AND \
            upper(schema_name) LIKE 'S%' ORDER BY 1
            TABLES\tSELECT upper(table_catalog) AS "TABLE_CAT", upper(table_schema) AS "TABLE_SCHEM", \
            upper(table_name) AS "TABLE_NAME", table_type AS "TABLE_TYPE", NULL AS "REMARKS" FROM \
            information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog') ORDER BY 2, 3
            TABLES\tSELECT upper(table_catalog) AS "TABLE_CAT", upper(table_schema) AS "TABLE_SCHEM", \
            upper(table_name) AS "TABLE_NAME", table_type AS "TABLE_TYPE", NULL AS "REMARKS" FROM \
            information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog') AND \
            upper(table_schema) LIKE 'S%' AND upper(table_name) LIKE 'T''_x' ORDER BY 2, 3
            PRIMARY_KEYS\tSELECT NULL AS "TABLE_CAT", upper(k.table_schema) AS "TABLE_SCHEM", upper(k.table_name) \
            AS "TABLE_NAME", upper(k.column_name) AS "COLUMN_NAME", k.ordinal_position AS "KEY_SEQ", NULL AS \
            "PK_NAME" FROM information_schema.key_column_usage k JOIN information_schema.table_constraints t ON \
            k.constraint_name = t.constraint_name AND k.table_schema = t.table_schema AND k.table_name = \
            t.table_name WHERE t.constraint_type = 'PRIMARY KEY' AND k.table_schema NOT IN \
            ('information_schema','pg_catalog') ORDER BY 2, 3, 5
            PRIMARY_KEYS\tSELECT NULL AS "TABLE_CAT", upper(k.table_schema) AS "TABLE_SCHEM", upper(k.table_name) \
            AS "TABLE_NAME", upper(k.column_name) AS "COLUMN_NAME", k.ordinal_position AS "KEY_SEQ", NULL AS \
            "PK_NAME" FROM information_schema.key_column_usage k JOIN information_schema.table_constraints t ON \
            k.constraint_name = t.constraint_name AND k.table_schema = t.table_schema AND k.table_name = \
            t.table_name WHERE t.constraint_type = 'PRIMARY KEY' AND k.table_schema NOT IN \
            ('information_schema','pg_catalog') AND upper(k.table_schema) LIKE 'S%' AND upper(k.table_name) LIKE \
            'T''_x' ORDER BY 2, 3, 5
            """;

    private static final Map<SqlSource.Catalog.Grid, List<String>> COLUMNS = Map.of(
            SqlSource.Catalog.Grid.SCHEMAS, List.of("TABLE_SCHEM", "TABLE_CATALOG"),
            SqlSource.Catalog.Grid.TABLES,
            List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS"),
            SqlSource.Catalog.Grid.COLUMNS,
            List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "SQL_TYPE_NAME"),
            SqlSource.Catalog.Grid.PRIMARY_KEYS, List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                    "KEY_SEQ", "PK_NAME"));

    private static SqlSelect grid(SqlSource.Catalog.Grid g, String schema, String table, String column) {
        List<OutputCol> outputs = new ArrayList<>();
        for (String name : COLUMNS.get(g)) {
            outputs.add(new OutputCol(name, SqlType.Scalar.VARCHAR, true));
        }
        return SqlSelect.starOf(new SqlSource.Catalog(g, schema, table, column, "g", outputs));
    }

    private static SqlSelect patterned(SqlSource.Catalog.Grid g) {
        return grid(g, "S%", g == SqlSource.Catalog.Grid.SCHEMAS ? null : "T'_x",
                g == SqlSource.Catalog.Grid.COLUMNS ? "C%" : null);
    }

    @Test
    void theInformationSchemaDialectsWriteTheTextTheCompilerWrote() {
        List<String[]> before = WRITTEN_BEFORE.lines().map(l -> l.split("\t", 2)).toList();
        assertEquals(6, before.size());
        for (SqlDialect d : List.<SqlDialect>of(new DuckDb(), new H2())) {
            for (int i = 0; i < before.size(); i++) {
                SqlSource.Catalog.Grid g = SqlSource.Catalog.Grid.valueOf(before.get(i)[0]);
                String sql = d.render(i % 2 == 0 ? grid(g, null, null, null) : patterned(g));
                assertTrue(sql.contains("(" + before.get(i)[1] + ") AS "),
                        d.getClass().getSimpleName() + " " + g + ":\n" + sql + "\nwas:\n" + before.get(i)[1]);
            }
        }
    }

    /** The column grid's types are what each database's own driver reports for the same columns: its code, its
     *  name, and that code's {@code java.sql.Types} name (legend-pure's {@code SQL_TYPE_NAME}). */
    @Test
    void theColumnGridTypesAColumnAsItsDriverDoes() throws Exception {
        String ddl = "CREATE TABLE CT.T(A INTEGER, B VARCHAR(20), C BIGINT, D SMALLINT, E TINYINT, F DOUBLE, G FLOAT,"
                + " H REAL, I DECIMAL(10,2), J NUMERIC(8,1), K DATE, L TIMESTAMP, M BOOLEAN, N CHAR(3), O TIME,"
                + " R TIMESTAMP WITH TIME ZONE";
        for (var db : List.<Map.Entry<SqlDialect, List<String>>>of(
                Map.entry(new DuckDb(), List.of("jdbc:duckdb:", ddl + ", P BLOB)")),
                Map.entry(new H2(),
                        List.of("jdbc:h2:mem:catalog_types", ddl + ", P VARBINARY(8), Q BINARY(4), S BIT)")))) {
            SqlDialect d = db.getKey();
            String name = d.getClass().getSimpleName();
            try (Connection c = DriverManager.getConnection(db.getValue().get(0)); Statement st = c.createStatement()) {
                st.execute("CREATE SCHEMA CT");
                st.execute(db.getValue().get(1));
                List<List<String>> driver = new ArrayList<>();
                try (ResultSet rs = c.getMetaData().getColumns(null, "CT", "T", null)) {
                    while (rs.next()) {
                        int code = rs.getInt("DATA_TYPE");
                        driver.add(List.of(rs.getString("COLUMN_NAME"), String.valueOf(code), rs.getString("TYPE_NAME"),
                                java.sql.JDBCType.valueOf(code).getName()));
                    }
                }
                assertTrue(driver.size() >= 17, name + ": " + driver);
                assertEquals(driver,
                        rows(st, d.render(grid(SqlSource.Catalog.Grid.COLUMNS, "CT", "T", null)), 4, 5, 6, 7), name);
            }
        }
    }

    /** Asked of DuckDB and H2 (in process): a table with a key, in a schema of its own. */
    @Test
    void theirDatabasesAnswerEveryGrid() throws Exception {
        for (var db : List.<Map.Entry<SqlDialect, String>>of(Map.entry(new DuckDb(), "jdbc:duckdb:"),
                Map.entry(new H2(), "jdbc:h2:mem:catalog_grid"))) {
            String name = db.getKey().getClass().getSimpleName();
            try (Connection c = DriverManager.getConnection(db.getValue()); Statement st = c.createStatement()) {
                st.execute("CREATE SCHEMA CG");
                st.execute("CREATE TABLE CG.T1(ID INTEGER NOT NULL, NAME VARCHAR(10), PRIMARY KEY(ID))");
                SqlDialect d = db.getKey();
                assertEquals(List.of(List.of("CG")),
                        rows(st, d.render(grid(SqlSource.Catalog.Grid.SCHEMAS, "CG", null, null)), 1), name);
                assertEquals(List.of(List.of("CG", "T1")),
                        rows(st, d.render(grid(SqlSource.Catalog.Grid.TABLES, "CG", "T%", null)), 2, 3), name);
                assertEquals(List.of(List.of("ID", "4"), List.of("NAME", "12")),
                        rows(st, d.render(grid(SqlSource.Catalog.Grid.COLUMNS, "CG", "T1", null)), 4, 5), name);
                assertEquals(List.of(List.of("NAME", "VARCHAR")),
                        rows(st, d.render(grid(SqlSource.Catalog.Grid.COLUMNS, "CG", "T1", "N%")), 4, 7), name);
                assertEquals(List.of(List.of("ID", "1")),
                        rows(st, d.render(grid(SqlSource.Catalog.Grid.PRIMARY_KEYS, "CG", "T1", null)), 4, 5), name);
            }
        }
    }

    /** Every other dialect refuses a grid: its database's catalog is not proven to be the standard's. */
    @Test
    void everyOtherDialectRefusesIt() {
        for (SqlDialect d : List.<SqlDialect>of(new Postgres(), new SQLite(), new MariaDb(), new MySql(), new Trino(),
                new ClickHouse(), new Snowflake(), new Databricks(), new BigQuery(), new EngineStyleH2(),
                new EngineStyleDB2(), new EngineStyleComposite())) {
            for (SqlSource.Catalog.Grid g : SqlSource.Catalog.Grid.values()) {
                DialectCapability refusal = assertThrows(DialectCapability.class,
                        () -> d.render(grid(g, null, null, null)), d.getClass().getSimpleName() + " " + g);
                assertTrue(refusal.getMessage().contains(g + " catalog grid"), refusal.getMessage());
            }
        }
    }

    /** A schema grid takes no table pattern, and only the column grid a column pattern. */
    @Test
    void aGridTakesOnlyItsOwnPatterns() {
        assertThrows(IllegalArgumentException.class, () -> grid(SqlSource.Catalog.Grid.SCHEMAS, null, "T", null));
        assertThrows(IllegalArgumentException.class, () -> grid(SqlSource.Catalog.Grid.TABLES, null, null, "C"));
        assertThrows(IllegalArgumentException.class,
                () -> grid(SqlSource.Catalog.Grid.PRIMARY_KEYS, null, null, "C"));
    }

    private static List<List<String>> rows(Statement st, String sql, int... columns) throws Exception {
        List<List<String>> out = new ArrayList<>();
        try (ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int c : columns) {
                    row.add(String.valueOf(rs.getObject(c)));
                }
                out.add(row);
            }
        }
        return out;
    }
}
