package com.legend.sql;

import com.legend.sql.dialect.DuckDb;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every SQL shape the renderer can produce must be VALID DuckDB — executed
 * against a real in-memory instance, not believed. Where the semantics
 * contract pins a behavior (positive mod, float division), the RESULT is
 * asserted too, so the golden strings in {@link DuckDbRenderTest} and real
 * DuckDB semantics can never drift apart silently.
 */
class DuckDbValidityTest {

    private static Connection conn;
    private static final DuckDb duck = new DuckDb();

    private static final SqlSource.Table T_PERSON =
            new SqlSource.Table("T_PERSON", "t0", List.of());

    private static SqlExpr col(String name) {
        return new SqlExpr.Column("t0", name);
    }

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE T_PERSON (ID INTEGER, NAME VARCHAR, AGE INTEGER,"
                    + " FIRM_ID INTEGER, TS TIMESTAMP)");
            st.execute("CREATE TABLE T_FIRM (ID INTEGER, LEGAL_NAME VARCHAR, TS TIMESTAMP)");
            st.execute("INSERT INTO T_PERSON VALUES"
                    + " (1, 'Ann', 25, 10, TIMESTAMP '2024-01-01 10:00:00'),"
                    + " (2, 'Bob', 35, 10, TIMESTAMP '2024-01-02 10:00:00'),"
                    + " (3, 'Eve', 45, NULL, TIMESTAMP '2024-01-03 10:00:00')");
            st.execute("INSERT INTO T_FIRM VALUES"
                    + " (10, 'ACME', TIMESTAMP '2024-01-01 09:00:00')");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    /** Execute rendered SQL; return first column of first row (null if no rows). */
    private Object exec(SqlQuery q) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(duck.render(q))) {
            return rs.next() ? rs.getObject(1) : null;
        }
    }

    private Object execExpr(SqlExpr e) throws SQLException {
        return exec(SqlSelect.starOf(T_PERSON)
                .withProjections(List.of(new SqlSelect.Projection(e, "v")), List.of())
                .withLimit(1L));
    }

    @Test
    @DisplayName("full clause set is valid DuckDB")
    void fullClauseSet() throws SQLException {
        SqlSelect s = SqlSelect.starOf(T_PERSON)
                .withProjections(List.of(
                        new SqlSelect.Projection(col("FIRM_ID"), null),
                        new SqlSelect.Projection(SqlAgg.Reducer.of("SUM", col("AGE")), "total")),
                        List.of())
                .withWhere(SqlExpr.Call.of("greater", col("AGE"), new SqlExpr.IntLit(20)))
                .withGroupBy(List.of(col("FIRM_ID")))
                .withHaving(SqlExpr.Call.of("greaterEqual",
                        SqlAgg.Reducer.of("COUNT"), new SqlExpr.IntLit(1)))
                .withOrderBy(List.of(SqlSelect.SortKey.desc(col("FIRM_ID"))))
                .withLimit(10L).withOffset(0L);
        exec(s);
    }

    @Test
    @DisplayName("semantics: positive mod, float division, banker's-independent rem")
    void pinnedSemantics() throws SQLException {
        assertEquals(2L, ((Number) execExpr(SqlExpr.Call.of("mod",
                new SqlExpr.IntLit(-7), new SqlExpr.IntLit(3)))).longValue(),
                "mod(-7,3) must be POSITIVE 2 (Pure semantics)");
        assertEquals(-1L, ((Number) execExpr(SqlExpr.Call.of("rem",
                new SqlExpr.IntLit(-7), new SqlExpr.IntLit(3)))).longValue(),
                "rem(-7,3) keeps sign: -1");
        assertEquals(3.5, ((Number) execExpr(SqlExpr.Call.of("divide",
                new SqlExpr.IntLit(7), new SqlExpr.IntLit(2)))).doubleValue(),
                "integer/integer must NOT truncate");
    }

    @Test
    @DisplayName("QUALIFY on a window alias is valid; window frames execute")
    void qualifyAndFrames() throws SQLException {
        SqlExpr.WindowCall rank = new SqlExpr.WindowCall(
                new SqlAgg.RankingFn("ROW_NUMBER", List.of()),
                List.of(col("FIRM_ID")),
                List.of(new SqlSelect.SortKey(col("AGE"), false,
                        SqlSelect.SortKey.NullOrder.NULLS_FIRST)),
                null);
        SqlExpr.WindowCall running = new SqlExpr.WindowCall(
                SqlAgg.Reducer.of("SUM", col("AGE")),
                List.of(), List.of(SqlSelect.SortKey.asc(col("AGE"))),
                new SqlExpr.WindowCall.Frame(SqlExpr.WindowCall.Frame.Kind.ROWS,
                        new SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding(),
                        new SqlExpr.WindowCall.Frame.Bound.CurrentRow()));
        SqlSelect s = SqlSelect.starOf(T_PERSON)
                .withProjections(List.of(
                        new SqlSelect.Projection(col("NAME"), null),
                        new SqlSelect.Projection(rank, "rn"),
                        new SqlSelect.Projection(running, "running")), List.of())
                .withQualify(SqlExpr.Call.of("equal",
                        new SqlExpr.Column(null, "rn"), new SqlExpr.IntLit(1)));
        exec(s);
    }

    @Test
    @DisplayName("ASOF LEFT JOIN spelling is valid DuckDB")
    void asofJoin() throws SQLException {
        SqlSource j = new SqlSource.Join(T_PERSON,
                new SqlSource.Table("T_FIRM", "t1", List.of()),
                SqlSource.Join.Kind.ASOF_LEFT,
                SqlExpr.Call.of("greaterEqual", col("TS"), new SqlExpr.Column("t1", "TS")));
        exec(SqlSelect.starOf(j));
    }

    @Test
    @DisplayName("flat multi-join, EXISTS, IN, CASE, arrays, lambdas all execute")
    void remainingShapes() throws SQLException {
        SqlSource joined = new SqlSource.Join(T_PERSON,
                new SqlSource.Table("T_FIRM", "t1", List.of()),
                SqlSource.Join.Kind.LEFT,
                SqlExpr.Call.of("equal", col("FIRM_ID"), new SqlExpr.Column("t1", "ID")));
        exec(SqlSelect.starOf(joined));

        exec(SqlSelect.starOf(T_PERSON).withWhere(new SqlExpr.Exists(
                SqlSelect.starOf(new SqlSource.Table("T_FIRM", "t1", List.of()))
                        .withWhere(SqlExpr.Call.of("equal",
                                new SqlExpr.Column("t1", "ID"), col("FIRM_ID"))))));

        assertEquals(true, execExpr(SqlExpr.Call.of("in",
                new SqlExpr.IntLit(1), new SqlExpr.IntLit(1), new SqlExpr.IntLit(2))));

        assertEquals("adult", execExpr(new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(SqlExpr.Call.of("greater",
                        new SqlExpr.IntLit(20), new SqlExpr.IntLit(18)),
                        new SqlExpr.StringLit("adult"))),
                new SqlExpr.StringLit("minor"))));

        // [1,2,3] filtered to elements > 1 → list of size 2
        Object filtered = execExpr(new SqlExpr.Call("list_filter", List.of(
                new SqlExpr.ArrayLit(List.of(new SqlExpr.IntLit(1),
                        new SqlExpr.IntLit(2), new SqlExpr.IntLit(3))),
                new SqlExpr.Lambda(List.of("x"),
                        SqlExpr.Call.of("greater", new SqlExpr.Column(null, "x"),
                                new SqlExpr.IntLit(1))))));
        assertEquals("[2, 3]", String.valueOf(filtered));
    }

    @Test
    @DisplayName("values, union, subselect, distinct, scalar subquery execute")
    void sourcesAndSetOps() throws SQLException {
        SqlSource.Values v = new SqlSource.Values(List.of(
                List.of(new SqlExpr.IntLit(1), new SqlExpr.StringLit("a")),
                List.of(new SqlExpr.IntLit(2), new SqlExpr.StringLit("b"))),
                List.of("ID", "NAME"), "tds", List.of());
        exec(SqlSelect.starOf(v));

        assertEquals(3L, ((Number) exec(new SqlSelect(
                List.of(new SqlSelect.Projection(SqlAgg.Reducer.of("COUNT"), null)),
                false,
                new SqlSource.Subselect(new SqlUnion(List.of(
                        SqlSelect.starOf(T_PERSON), SqlSelect.starOf(T_PERSON)), false, List.of()),
                        "u"),
                null, List.of(), null, null, List.of(), null, null, List.of()))).longValue(),
                "UNION dedups 3 rows; UNION ALL would give 6");

        exec(SqlSelect.starOf(T_PERSON).withDistinct());

        assertEquals("ACME", execExpr(new SqlExpr.ScalarSubquery(
                new SqlSelect(List.of(new SqlSelect.Projection(
                        new SqlExpr.Column("t1", "LEGAL_NAME"), null)), false,
                        new SqlSource.Table("T_FIRM", "t1", List.of()),
                        null, List.of(), null, null, List.of(), null, null, List.of()))));
    }

    @Test
    @DisplayName("quoted identifiers round-trip: reserved word + spaces")
    void quotedIdentifiers() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE \"my table\" (\"order\" INTEGER, \"first name\" VARCHAR)");
            st.execute("INSERT INTO \"my table\" VALUES (1, 'x')");
        }
        SqlSelect s = SqlSelect.starOf(new SqlSource.Table("my table", "t0", List.of()))
                .withProjections(List.of(
                        new SqlSelect.Projection(new SqlExpr.Column("t0", "order"), null),
                        new SqlSelect.Projection(new SqlExpr.Column("t0", "first name"), null)),
                        List.of());
        assertEquals(1, ((Number) exec(s)).intValue());
    }

    @Test
    @DisplayName("date/timestamp literals compare correctly")
    void dateLiterals() throws SQLException {
        assertEquals(true, execExpr(SqlExpr.Call.of("less",
                new SqlExpr.DateLit("2024-01-01"), new SqlExpr.DateLit("2024-06-01"))));
        assertTrue((Boolean) execExpr(SqlExpr.Call.of("equal",
                new SqlExpr.TimestampLit("2024-01-01 10:00:00"),
                new SqlExpr.TimestampLit("2024-01-01 10:00:00"))));
    }
}
