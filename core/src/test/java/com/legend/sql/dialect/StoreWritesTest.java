// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.NameQuoting;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlDml;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlTyping;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The store writes' statements (docs/WRITES_DESIGN_2026_10_18.md §4, L1): a query's rows into a table
 * ({@link SqlDml.InsertSelect}), an UPDATE and a DELETE through an alias ({@link SqlDml.Update},
 * {@link SqlDml.DeleteWhere}) and an upsert by key ({@link SqlDml.Merge}), each spelled where its database was measured
 * to run it (DuckDB, H2, Postgres 16; the standard forms, 2026-10-18) and refused by name by every other dialect.
 * DuckDB's and H2's run here, in process; Postgres's text is the same standard text, run by the writes' Postgres test.
 */
class StoreWritesTest {

    private static final NameQuoting BARE = NameQuoting.BARE;

    /** A read of a table's column, as the lowering stamps one (a PHYSICAL origin: Postgres folds its name). */
    private static SqlExpr.Column stored(String alias, String column) {
        return new SqlExpr.Column(alias, column, SqlTyping.UNKNOWN, OutputCol.Origin.PHYSICAL);
    }

    /** {@code SELECT s.ID AS ID, s.NAME AS NAME, s.AGE AS AGE FROM SRC AS s}, the source's columns as the table's (each
     *  output a derived name, as the lowering stamps a projection's). */
    private static SqlSelect source() {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (String c : List.of("ID", "NAME", "AGE")) {
            ps.add(new SqlSelect.Projection(stored("s", c), c, new OutputCol(c,
                    c.equals("NAME") ? SqlType.Scalar.VARCHAR : SqlType.Scalar.INTEGER, true, OutputCol.Origin.DERIVED)));
        }
        return SqlSelect.starOf(new SqlSource.Table("SRC", "s", List.of(), false, Map.of(), BARE)).withProjections(ps);
    }

    private static SqlDml.InsertSelect insert() {
        return new SqlDml.InsertSelect(null, "T", BARE, List.of("ID", "NAME", "AGE"), Set.of(), source());
    }

    /** {@code UPDATE T AS t0 SET AGE = t0.AGE + 1 WHERE t0.ID > 1}. */
    private static SqlDml.Update update() {
        return new SqlDml.Update(null, "T", BARE, "t0", Map.of(), List.of(new SqlDml.Update.Assignment("AGE", false,
                new SqlExpr.Call(SqlFn.PLUS, List.of(stored("t0", "AGE"), new SqlExpr.IntLit(1))))),
                new SqlExpr.Call(SqlFn.GREATER, List.of(stored("t0", "ID"), new SqlExpr.IntLit(1))));
    }

    /** {@code DELETE FROM T AS t0 WHERE t0.ID = 3}. */
    private static SqlDml.DeleteWhere delete() {
        return new SqlDml.DeleteWhere(null, "T", BARE, "t0", Map.of(),
                new SqlExpr.Call(SqlFn.EQUAL, List.of(stored("t0", "ID"), new SqlExpr.IntLit(3))));
    }

    private static SqlDml.Merge merge() {
        return new SqlDml.Merge(null, "T", BARE, List.of("ID", "NAME", "AGE"), Set.of(), List.of("ID"), source(), "t0",
                "m");
    }

    private static List<String> rows(Connection c) throws Exception {
        List<String> out = new ArrayList<>();
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT ID, NAME, AGE FROM T ORDER BY ID")) {
            while (rs.next()) {
                out.add(rs.getInt(1) + "|" + rs.getString(2) + "|" + rs.getInt(3));
            }
        }
        return out;
    }

    /** One statement on its own JDBC statement (DuckDB closes a statement its error ended). */
    private static int run(Connection c, String sql) throws Exception {
        try (Statement st = c.createStatement()) {
            return st.executeUpdate(sql);
        }
    }

    @Test
    void theMeasuredDatabasesRunEachWrite() throws Exception {
        for (var db : List.<Map.Entry<SqlDialect, String>>of(Map.entry(new DuckDb(), "jdbc:duckdb:"),
                Map.entry(new H2(), "jdbc:h2:mem:store_writes"), Map.entry(new H2Modern(), "jdbc:h2:mem:store_writes_m"))) {
            SqlDialect d = db.getKey();
            String name = d.getClass().getSimpleName();
            try (Connection c = DriverManager.getConnection(db.getValue())) {
                run(c, "CREATE TABLE T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20), AGE INTEGER)");
                run(c, "CREATE TABLE SRC (ID INTEGER, NAME VARCHAR(20), AGE INTEGER)");
                run(c, "INSERT INTO SRC VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");
                assertEquals(3, run(c, d.render(insert())), name);
                assertEquals(List.of("1|a|10", "2|b|20", "3|c|30"), rows(c), name);
                assertEquals(2, run(c, d.render(update())), name);
                assertEquals(List.of("1|a|10", "2|b|21", "3|c|31"), rows(c), name);
                assertEquals(1, run(c, d.render(delete())), name);
                assertEquals(List.of("1|a|10", "2|b|21"), rows(c), name);
                // the upsert: 1 and 2 updated from the source, 3 inserted
                assertEquals(3, run(c, d.render(merge())), name);
                assertEquals(List.of("1|a|10", "2|b|20", "3|c|30"), rows(c), name);
                // a failing write is the database's error, and leaves the table as it was (a statement is atomic)
                assertThrows(java.sql.SQLException.class, () -> run(c, d.render(insert())), name);
                assertEquals(List.of("1|a|10", "2|b|20", "3|c|30"), rows(c), name);
            }
        }
    }

    /** A plan's parameter, typed as the planner types an {@code Integer[1]} (QueryParameters.Declared.slot). */
    private static SqlExpr.PlanParam integer(String name) {
        return new SqlExpr.PlanParam(name, SqlExpr.PlanParam.Kind.OTHER, false, null,
                SqlTyping.typed(SqlType.Scalar.BIGINT), null);
    }

    /** A write's values bind a plan's parameters where they are written (docs/WRITES_DESIGN_2026_10_18.md §8, leg 7):
     *  the statement's text has a {@code ?} at each, its parameters in placeholder order; its text alone is refused. */
    @Test
    void aWriteBindsAPlansParametersWhereTheyAreWritten() throws Exception {
        // UPDATE T AS t0 SET AGE = ? WHERE t0.ID > ?
        SqlDml.Update bound = new SqlDml.Update(null, "T", BARE, "t0", Map.of(), List.of(new SqlDml.Update.Assignment(
                "AGE", false, integer("age"))), new SqlExpr.Call(SqlFn.GREATER, List.of(stored("t0", "ID"),
                integer("above"))));
        // CREATE TEMPORARY TABLE S AS SELECT ... FROM SRC AS s WHERE s.AGE > ?
        SqlDml.CreateTempTableAs staged = new SqlDml.CreateTempTableAs("S", source().withWhere(new SqlExpr.Call(
                SqlFn.GREATER, List.of(stored("s", "AGE"), integer("older")))));
        for (var db : List.<Map.Entry<SqlDialect, String>>of(Map.entry(new DuckDb(), "jdbc:duckdb:"),
                Map.entry(new H2(), "jdbc:h2:mem:store_writes_bound"),
                Map.entry(new H2Modern(), "jdbc:h2:mem:store_writes_bound_m"))) {
            SqlDialect d = db.getKey();
            String name = d.getClass().getSimpleName();
            RenderedStatement update = d.renderStatement(bound);
            assertEquals(List.of("age", "above"), update.binds().stream().map(RenderedStatement.Bind::parameter)
                    .toList(), name + ": " + update.sql());
            RenderedStatement create = d.renderStatement(staged);
            assertEquals(List.of("older"), create.binds().stream().map(RenderedStatement.Bind::parameter).toList(),
                    name + ": " + create.sql());
            var refused = assertThrows(IllegalStateException.class, () -> d.render(bound), name);
            assertTrue(refused.getMessage().contains("render it as a statement"), refused.getMessage());
            try (Connection c = DriverManager.getConnection(db.getValue())) {
                run(c, "CREATE TABLE T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20), AGE INTEGER)");
                run(c, "CREATE TABLE SRC (ID INTEGER, NAME VARCHAR(20), AGE INTEGER)");
                run(c, "INSERT INTO SRC VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");
                run(c, d.render(insert()));
                try (var st = c.prepareStatement(update.sql())) {
                    st.setLong(1, 99L);
                    st.setLong(2, 1L);
                    assertEquals(2, st.executeUpdate(), name);
                }
                assertEquals(List.of("1|a|10", "2|b|99", "3|c|99"), rows(c), name);
                try (var st = c.prepareStatement(create.sql())) {
                    st.setLong(1, 15L);
                    st.executeUpdate();
                }
                try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT count(*) FROM S")) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1), name);
                }
            }
        }
        // Postgres's: the same placeholders, in the same order
        RenderedStatement pg = new Postgres().renderStatement(bound);
        assertEquals(List.of("age", "above"), pg.binds().stream().map(RenderedStatement.Bind::parameter).toList(),
                pg.sql());
    }

    /** Postgres's text, each statement run on Postgres 16 by a probe (2026-10-18: 3, 2, 1 and 3 rows, the rows as
     *  DuckDB's and H2's above); a stored column's name folded, a name the statement writes (an alias, a label)
     *  delimited, as its queries spell them. */
    @Test
    void postgresWritesTheStandardText() {
        Postgres pg = new Postgres();
        assertEquals("INSERT INTO t (id, name, age) SELECT \"s\".id AS \"ID\", \"s\".name AS \"NAME\", \"s\".age AS"
                + " \"AGE\" FROM src AS \"s\";", oneLine(pg.render(insert())));
        assertEquals("UPDATE t AS \"t0\" SET age = \"t0\".age + 1 WHERE \"t0\".id > 1;", oneLine(pg.render(update())));
        assertEquals("DELETE FROM t AS \"t0\" WHERE \"t0\".id = 3;", oneLine(pg.render(delete())));
        assertEquals("MERGE INTO t AS \"t0\" USING (SELECT \"s\".id AS \"ID\", \"s\".name AS \"NAME\", \"s\".age AS"
                + " \"AGE\" FROM src AS \"s\") AS \"m\" ON \"t0\".id = \"m\".\"ID\" WHEN MATCHED THEN UPDATE SET name ="
                + " \"m\".\"NAME\", age = \"m\".\"AGE\" WHEN NOT MATCHED THEN INSERT (id, name, age) VALUES (\"m\".\"ID\","
                + " \"m\".\"NAME\", \"m\".\"AGE\");", oneLine(pg.render(merge())));
    }

    @Test
    void everyOtherDialectRefusesEachWrite() {
        for (SqlDialect d : List.<SqlDialect>of(new SQLite(), new MariaDb(), new MySql(), new Trino(), new ClickHouse(),
                new Snowflake(), new Databricks(), new BigQuery(), new EngineStyleH2(), new EngineStyleDB2(),
                new EngineStyleComposite())) {
            for (SqlDml dml : List.<SqlDml>of(insert(), update(), delete(), merge())) {
                DialectCapability refusal = assertThrows(DialectCapability.class, () -> d.render(dml),
                        d.getClass().getSimpleName() + " " + dml.getClass().getSimpleName());
                assertTrue(refusal.getMessage().contains(d.getClass().getSimpleName())
                        && refusal.getMessage().contains("no spelling measured"), refusal.getMessage());
            }
        }
    }

    /** A rendered statement on one line (the query's layout newlines and indents folded). */
    private static String oneLine(String sql) {
        return sql.replaceAll("\\s*\\n\\s*", " ");
    }
}
