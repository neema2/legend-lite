// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** E (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §10): the one writer, the statement entry point beside render, and — from
 *  E-2 — a plan parameter bound where its placeholder is written, through expressions, calls and subqueries. */
class SqlWriterTest {

    private static final SqlSelect QUERY = SqlSelect.starOf(
            new SqlSource.Table("T_PERSON", "t0", List.of(), false, java.util.Map.of()));

    /** A writer for pieces with no sub-expression: its expressions refuse. */
    private static SqlWriter textOnly() {
        return new SqlWriter((writer, e, prec) -> {
            throw new IllegalStateException("this test writes no expression: " + e);
        });
    }

    @Test
    void parametersAreListedInTheOrderTheirPlaceholdersAreWritten() {
        SqlWriter w = textOnly();
        RenderedStatement.Bind x = new RenderedStatement.Bind("x", new RenderedStatement.Binding.One("BIGINT", null));
        RenderedStatement.Bind ids = new RenderedStatement.Bind("ids", new RenderedStatement.Binding.Array("INTEGER"));
        w.append("a = ").bind(x).append(" AND b = ANY(").bind(ids).append(") AND c = ").bind(x);
        assertEquals(new RenderedStatement("a = ? AND b = ANY(?) AND c = ?", List.of(x, ids, x)), w.statement());
    }

    /** A placeholder typed by its value: the type hole is recorded at its place in the text, on that placeholder. */
    @Test
    void aTypeHoleIsRecordedWhereItIsWritten_onThePlaceholderBoundLast() {
        var types = java.util.Map.of(com.legend.sql.ValueKind.DECIMAL,
                new RenderedStatement.TypeSpelling("NUMERIC", RenderedStatement.Digits.PRECISION_AND_SCALE));
        SqlWriter w = textOnly().append("a = CAST(")
                .bind(new RenderedStatement.Bind("f", new RenderedStatement.Binding.One("DECIMAL", null)))
                .append(" AS ").typeHole(types, com.legend.sql.ValueKind.DECIMAL).append(")");
        assertEquals(new RenderedStatement("a = CAST(? AS )", List.of(new RenderedStatement.Bind("f",
                new RenderedStatement.Binding.One("DECIMAL", new RenderedStatement.TypeHole(14, types,
                        com.legend.sql.ValueKind.DECIMAL))))), w.statement());
        assertThrows(IllegalStateException.class, () -> textOnly().append("a = ").typeHole(types,
                com.legend.sql.ValueKind.DECIMAL), "a type hole follows a placeholder of one value");
    }

    @Test
    void aStatementWithBoundParametersIsRefusedAsText() {
        SqlWriter w = textOnly().append("a = ").bind(new RenderedStatement.Bind("x",
                new RenderedStatement.Binding.One("BIGINT", null)));
        var refused = assertThrows(IllegalStateException.class, w::text);
        assertTrue(refused.getMessage().contains("render it as a statement"), refused.getMessage());
    }

    @Test
    void aQueryWithNoParametersRendersTheSameTextEitherWay() {
        for (SqlDialect d : List.of(new DuckDb(), new H2(), new Postgres())) {
            RenderedStatement s = d.renderStatement(QUERY);
            assertEquals(d.render(QUERY), s.sql(), d.getClass().getSimpleName());
            assertEquals(List.of(), s.binds(), d.getClass().getSimpleName());
        }
    }

    @Test
    void theLegacyEngineTextPlanBindsNothing() {
        var refused = assertThrows(DialectCapability.class, () -> new EngineStyleH2().renderStatement(QUERY));
        assertTrue(refused.getMessage().contains("template variables"), refused.getMessage());
    }

    // ---- E-2: parameters bound where they are written ------------------------------------------------------------

    private static final SqlExpr NAME = SqlExpr.Column.physical("t0", "NAME");
    private static final SqlExpr AGE = SqlExpr.Column.physical("t0", "AGE");
    // a plan's parameters, typed as the planner types them (QueryParameters.Declared.slot): one bound must have a type
    private static final SqlExpr.PlanParam P_NAME = new SqlExpr.PlanParam("name", SqlExpr.PlanParam.Kind.STRING, false,
            null, com.legend.sql.SqlTyping.typed(com.legend.sql.SqlType.Scalar.VARCHAR), null);
    private static final SqlExpr.PlanParam P_AGE = new SqlExpr.PlanParam("maxAge", SqlExpr.PlanParam.Kind.OTHER, false,
            null, com.legend.sql.SqlTyping.typed(com.legend.sql.SqlType.Scalar.BIGINT), null);

    private static SqlSelect where(SqlExpr predicate) {
        return QUERY.withWhere(predicate);
    }

    /** Runs the statement on a DuckDB holding three people, binding each placeholder from {@code values} by its
     *  parameter, in the statement's order: the NAMEs it answers, sorted (the statements have no ORDER BY). */
    private static List<String> run(RenderedStatement s, Map<String, Object> values) throws SQLException {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            c.createStatement().execute("CREATE TABLE T_PERSON(NAME VARCHAR, AGE INTEGER)");
            c.createStatement().execute("INSERT INTO T_PERSON VALUES ('a', 20), ('b', 40), ('c', 60)");
            try (PreparedStatement ps = c.prepareStatement(s.sql())) {
                for (int i = 0; i < s.binds().size(); i++) {
                    ps.setObject(i + 1, values.get(s.binds().get(i).parameter()));
                }
                List<String> names = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        names.add(rs.getString("NAME"));
                    }
                }
                names.sort(null);
                return names;
            }
        }
    }

    @Test
    void parametersUnderAndAndNotBindInPlaceholderOrder() throws SQLException {
        SqlSelect q = where(SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.NOT, SqlExpr.Call.of(SqlFn.EQUAL, NAME, P_NAME)),
                SqlExpr.Call.of(SqlFn.LESS_EQUAL, AGE, P_AGE)));
        RenderedStatement s = new DuckDb().renderStatement(q);
        assertEquals(List.of("name", "maxAge"), s.binds().stream().map(RenderedStatement.Bind::parameter).toList(), s.sql());
        assertEquals(List.of("b"), run(s, Map.of("name", "a", "maxAge", 40)), s.sql());
    }

    @Test
    void aParameterWrittenTwiceIsBoundTwice() throws SQLException {
        // XOR writes each operand twice: (x AND NOT y) OR (NOT x AND y)
        SqlSelect q = where(SqlExpr.Call.of(SqlFn.XOR,
                SqlExpr.Call.of(SqlFn.EQUAL, NAME, P_NAME), SqlExpr.Call.of(SqlFn.LESS_EQUAL, AGE, P_AGE)));
        RenderedStatement s = new DuckDb().renderStatement(q);
        assertEquals(List.of("name", "maxAge", "name", "maxAge"),
                s.binds().stream().map(RenderedStatement.Bind::parameter).toList(), s.sql());
        // name = 'c' XOR age <= 40: a (20) and b (40) by age, c by name
        assertEquals(List.of("a", "b", "c"), run(s, Map.of("name", "c", "maxAge", 40)), s.sql());
    }

    @Test
    void aParameterInsideASubqueryIsBoundInPlace() throws SQLException {
        SqlSelect inner = SqlSelect.starOf(new SqlSource.Table("T_PERSON", "t1", List.of(), false, Map.of()))
                .withProjections(List.of(new SqlSelect.Projection(SqlExpr.Column.physical("t1", "NAME"), null, null)))
                .withWhere(SqlExpr.Call.of(SqlFn.LESS_EQUAL, SqlExpr.Column.physical("t1", "AGE"), P_AGE));
        SqlSelect q = where(SqlExpr.Call.of(SqlFn.AND,
                new SqlExpr.InSubquery(NAME, inner, com.legend.sql.SqlTyping.UNKNOWN),
                SqlExpr.Call.of(SqlFn.NOT, SqlExpr.Call.of(SqlFn.EQUAL, NAME, P_NAME))));
        RenderedStatement s = new DuckDb().renderStatement(q);
        assertEquals(List.of("maxAge", "name"), s.binds().stream().map(RenderedStatement.Bind::parameter).toList(),
                s.sql());
        assertEquals(List.of("a"), run(s, Map.of("name", "b", "maxAge", 40)), s.sql());
    }

    @Test
    void whatOneBoundValueCannotCarryIsRefusedByName() {
        SqlExpr.PlanParam raw = new SqlExpr.PlanParam("wrapper", SqlExpr.PlanParam.Kind.RAW);
        SqlExpr.PlanParam level = new SqlExpr.PlanParam("level", SqlExpr.PlanParam.Kind.ENUM, false, "test::Level");
        for (Map.Entry<SqlExpr.Call, String> c : List.of(
                Map.entry(SqlExpr.Call.of(SqlFn.EQUAL, NAME, raw), "is RAW"),
                Map.entry(SqlExpr.Call.of(SqlFn.EQUAL, NAME, level), "legacy printer's mapping function"),
                Map.entry(SqlExpr.Call.of(SqlFn.IN, NAME, P_NAME), "is not a list parameter"))) {
            var refused = assertThrows(DialectCapability.class, () -> new DuckDb().renderStatement(where(c.getKey())));
            assertTrue(refused.getMessage().contains(c.getValue()), refused.getMessage());
        }
    }

    @Test
    void aParameterUnderAHelperIsBoundEveryPlaceItIsWritten() throws SQLException {
        // DuckDB's acos writes its argument twice: the domain guard, then the call (E-3)
        SqlSelect q = where(SqlExpr.Call.of(SqlFn.LESS_EQUAL, SqlExpr.Call.of(SqlFn.ACOS, P_AGE), AGE));
        RenderedStatement s = new DuckDb().renderStatement(q);
        assertEquals(List.of("maxAge", "maxAge"), s.binds().stream().map(RenderedStatement.Bind::parameter).toList(),
                s.sql());
        // acos(1) is 0, at most every age; acos(2) is out of the domain: NaN, at most none
        assertEquals(List.of("a", "b", "c"), run(s, Map.of("maxAge", 1)), s.sql());
        assertEquals(List.of(), run(s, Map.of("maxAge", 2)), s.sql());
    }

    @Test
    void aParameterInADmlRowIsRefused() {
        // a row holds values; a DML statement is text, with no parameters to bind
        var rows = new com.legend.sql.SqlDml.InsertValues(null, "T_PERSON", List.of("NAME"), List.of(List.of(P_NAME)));
        var refused = assertThrows(DialectCapability.class, () -> new DuckDb().render(rows));
        assertTrue(refused.getMessage().contains("in a DML row"), refused.getMessage());
    }

    @Test
    void aQueryWithAParameterIsRefusedAsText() {
        SqlSelect q = where(SqlExpr.Call.of(SqlFn.EQUAL, NAME, P_NAME));
        var refused = assertThrows(IllegalStateException.class, () -> new DuckDb().render(q));
        assertTrue(refused.getMessage().contains("render it as a statement"), refused.getMessage());
    }
}
