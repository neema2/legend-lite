package com.legend.sql;

import com.legend.sql.dialect.DuckDb;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Golden renderer pins for the DuckDB dialect: exact SQL text (the leanness is
 * IN the goldens — clause layout, quote-only-when-needed, minimal parens) plus
 * engine-style flatness counts ({@code count(sql, "SELECT")}).
 */
class DuckDbRenderTest {

    private final DuckDb duck = new DuckDb();

    private static final SqlSource.Table T_PERSON =
            new SqlSource.Table("T_PERSON", "t0", List.of());

    private static SqlExpr col(String name) {
        return new SqlExpr.Column("t0", name);
    }

    private int count(String sql, String keyword) {
        int c = 0;
        for (int i = sql.indexOf(keyword); i >= 0; i = sql.indexOf(keyword, i + keyword.length())) c++;
        return c;
    }

    @Test
    @DisplayName("minimal: SELECT * FROM table")
    void minimalStar() {
        assertEquals("""
                SELECT *
                FROM T_PERSON AS t0""",
                duck.render(SqlSelect.starOf(T_PERSON)));
    }

    @Test
    @DisplayName("every clause folds into ONE SELECT, canonical order")
    void fullSingleSelect() {
        SqlSelect s = SqlSelect.starOf(T_PERSON)
                .withProjections(List.of(
                        new SqlSelect.Projection(col("FIRM"), null),
                        new SqlSelect.Projection(SqlAgg.Reducer.of("SUM", col("AGE")), "totalAge")),
                        List.of())
                .withWhere(SqlExpr.Call.of("greater", col("AGE"), new SqlExpr.IntLit(30)))
                .withGroupBy(List.of(col("FIRM")))
                .withHaving(SqlExpr.Call.of("greater",
                        SqlAgg.Reducer.of("COUNT"), new SqlExpr.IntLit(2)))
                .withOrderBy(List.of(SqlSelect.SortKey.desc(col("FIRM"))))
                .withLimit(10L)
                .withOffset(5L);
        String sql = duck.render(s);
        assertEquals("""
                SELECT t0.FIRM, SUM(t0.AGE) AS totalAge
                FROM T_PERSON AS t0
                WHERE t0.AGE > 30
                GROUP BY t0.FIRM
                HAVING COUNT(*) > 2
                ORDER BY t0.FIRM DESC
                LIMIT 10
                OFFSET 5""",
                sql);
        assertEquals(1, count(sql, "SELECT"), "flat: exactly 1 SELECT");
    }

    @Test
    @DisplayName("identifiers quote ONLY when necessary")
    void leanQuoting() {
        SqlSelect s = SqlSelect.starOf(new SqlSource.Table("my table", "t0", List.of()))
                .withProjections(List.of(
                        new SqlSelect.Projection(new SqlExpr.Column("t0", "NAME"), null),
                        new SqlSelect.Projection(new SqlExpr.Column("t0", "order"), null),
                        new SqlSelect.Projection(new SqlExpr.Column("t0", "first name"), null)),
                        List.of());
        assertEquals("""
                SELECT t0.NAME, t0."order", t0."first name"
                FROM "my table" AS t0""",
                duck.render(s));
    }

    @Test
    @DisplayName("minimal parens: precedence drives grouping")
    void minimalParens() {
        // (a OR b) AND NOT c — OR needs parens under AND; comparisons never do.
        SqlExpr pred = SqlExpr.Call.of("and",
                SqlExpr.Call.of("or",
                        SqlExpr.Call.of("equal", col("A"), new SqlExpr.IntLit(1)),
                        SqlExpr.Call.of("equal", col("B"), new SqlExpr.IntLit(2))),
                SqlExpr.Call.of("not",
                        SqlExpr.Call.of("less", col("C"), new SqlExpr.IntLit(3))));
        assertEquals("(t0.A = 1 OR t0.B = 2) AND NOT t0.C < 3",
                duck.render(SqlSelect.starOf(T_PERSON).withWhere(pred)).lines()
                        .filter(l -> l.startsWith("WHERE")).findFirst().orElseThrow()
                        .substring("WHERE ".length()));
    }

    @Test
    @DisplayName("literals: escaping, dates, booleans, arrays")
    void literals() {
        SqlSelect s = SqlSelect.starOf(T_PERSON).withProjections(List.of(
                new SqlSelect.Projection(new SqlExpr.StringLit("O'Brien"), "s"),
                new SqlSelect.Projection(new SqlExpr.DateLit("2024-01-15"), "d"),
                new SqlSelect.Projection(new SqlExpr.TimestampLit("2024-01-15 10:30:00"), "ts"),
                new SqlSelect.Projection(new SqlExpr.BoolLit(true), "b"),
                new SqlSelect.Projection(new SqlExpr.NullLit(), "n"),
                new SqlSelect.Projection(new SqlExpr.ArrayLit(List.of(
                        new SqlExpr.IntLit(1), new SqlExpr.IntLit(2))), "arr")),
                List.of());
        assertEquals("SELECT 'O''Brien' AS s, DATE '2024-01-15' AS d,"
                        + " TIMESTAMP '2024-01-15 10:30:00' AS ts, TRUE AS b, NULL AS n,"
                        + " [1, 2] AS arr",
                duck.render(s).lines().findFirst().orElseThrow());
    }

    @Test
    @DisplayName("MUST-honor semantics: float division, positive mod, rem")
    void semanticContract() {
        assertEquals("((1.0 * t0.A) / t0.B)",
                renderExpr(SqlExpr.Call.of("divide", col("A"), col("B"))));
        assertEquals("MOD(MOD(t0.A, t0.B) + t0.B, t0.B)",
                renderExpr(SqlExpr.Call.of("mod", col("A"), col("B"))));
        assertEquals("MOD(t0.A, t0.B)",
                renderExpr(SqlExpr.Call.of("rem", col("A"), col("B"))));
        assertEquals("t0.A IS NULL", renderExpr(SqlExpr.Call.of("isNull", col("A"))));
    }

    @Test
    @DisplayName("unknown semantic name throws — no silent pass-through")
    void unknownSemanticNameThrows() {
        assertThrows(IllegalStateException.class,
                () -> renderExpr(SqlExpr.Call.of("frobnicate", col("A"))));
    }

    @Test
    @DisplayName("join tree renders FLAT: one SELECT, joins inlined")
    void flatJoinTree() {
        SqlSource joined = new SqlSource.Join(
                new SqlSource.Join(T_PERSON,
                        new SqlSource.Table("T_FIRM", "t1", List.of()),
                        SqlSource.Join.Kind.LEFT,
                        SqlExpr.Call.of("equal", col("FIRM_ID"), new SqlExpr.Column("t1", "ID"))),
                new SqlSource.Table("T_CITY", "t2", List.of()),
                SqlSource.Join.Kind.LEFT,
                SqlExpr.Call.of("equal", new SqlExpr.Column("t1", "CITY_ID"),
                        new SqlExpr.Column("t2", "ID")));
        String sql = duck.render(SqlSelect.starOf(joined));
        assertEquals("""
                SELECT *
                FROM T_PERSON AS t0
                LEFT OUTER JOIN T_FIRM AS t1 ON t0.FIRM_ID = t1.ID
                LEFT OUTER JOIN T_CITY AS t2 ON t1.CITY_ID = t2.ID""",
                sql);
        assertEquals(1, count(sql, "SELECT"), "flat: exactly 1 SELECT");
        assertEquals(2, count(sql, "LEFT OUTER JOIN"));
    }

    @Test
    @DisplayName("subselect: the ONLY nesting construct, indented")
    void subselectIndents() {
        SqlSelect inner = SqlSelect.starOf(T_PERSON)
                .withWhere(SqlExpr.Call.of("greater", col("AGE"), new SqlExpr.IntLit(30)));
        SqlSelect outer = SqlSelect.starOf(new SqlSource.Subselect(inner, "t1"))
                .withLimit(5L);
        assertEquals("""
                SELECT *
                FROM (
                  SELECT *
                  FROM T_PERSON AS t0
                  WHERE t0.AGE > 30
                ) AS t1
                LIMIT 5""",
                duck.render(outer));
    }

    @Test
    @DisplayName("EXISTS renders inline — Boolean-composable")
    void existsInline() {
        SqlExpr exists = new SqlExpr.Exists(SqlSelect.starOf(
                new SqlSource.Table("T_FIRM", "t1", List.of()))
                .withWhere(SqlExpr.Call.of("equal",
                        new SqlExpr.Column("t1", "ID"), col("FIRM_ID"))));
        SqlExpr pred = SqlExpr.Call.of("or", exists,
                SqlExpr.Call.of("isNull", col("FIRM_ID")));
        String where = duck.render(SqlSelect.starOf(T_PERSON).withWhere(pred)).lines()
                .filter(l -> l.startsWith("WHERE")).findFirst().orElseThrow();
        assertEquals("WHERE EXISTS (SELECT * FROM T_FIRM AS t1 WHERE t1.ID = t0.FIRM_ID)"
                + " OR t0.FIRM_ID IS NULL", where);
    }

    @Test
    @DisplayName("window: OVER with partition/order/frame; QUALIFY clause")
    void windowAndQualify() {
        SqlExpr.WindowCall rank = new SqlExpr.WindowCall(
                new SqlAgg.RankingFn("ROW_NUMBER", List.of()),
                List.of(col("FIRM")),
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
                        new SqlSelect.Projection(rank, "rn"),
                        new SqlSelect.Projection(running, "running")),
                        List.of())
                .withQualify(SqlExpr.Call.of("equal", new SqlExpr.Column(null, "rn"),
                        new SqlExpr.IntLit(1)));
        assertEquals("""
                SELECT ROW_NUMBER() OVER (PARTITION BY t0.FIRM ORDER BY t0.AGE DESC NULLS FIRST) AS rn,\
                 SUM(t0.AGE) OVER (ORDER BY t0.AGE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
                FROM T_PERSON AS t0
                QUALIFY rn = 1""",
                duck.render(s));
    }

    @Test
    @DisplayName("union: bare set operation, no wrapping SELECT *")
    void bareUnion() {
        SqlUnion u = new SqlUnion(List.of(
                SqlSelect.starOf(T_PERSON),
                SqlSelect.starOf(new SqlSource.Table("T_PERSON2", "t1", List.of()))),
                true, List.of());
        String sql = duck.render(u);
        assertEquals("""
                SELECT *
                FROM T_PERSON AS t0
                UNION ALL
                SELECT *
                FROM T_PERSON2 AS t1""",
                sql);
        assertEquals(2, count(sql, "SELECT"), "two branches, zero wrappers");
    }

    @Test
    @DisplayName("VALUES source with named columns")
    void valuesSource() {
        SqlSource.Values v = new SqlSource.Values(List.of(
                List.of(new SqlExpr.IntLit(1), new SqlExpr.StringLit("a")),
                List.of(new SqlExpr.IntLit(2), new SqlExpr.StringLit("b"))),
                List.of("ID", "NAME"), "tds", List.of());
        assertEquals("""
                SELECT *
                FROM (VALUES (1, 'a'), (2, 'b')) AS tds(ID, NAME)""",
                duck.render(SqlSelect.starOf(v)));
    }

    @Test
    @DisplayName("CASE WHEN and DISTINCT aggregate")
    void caseAndDistinctAgg() {
        SqlExpr c = new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(
                        SqlExpr.Call.of("greater", col("AGE"), new SqlExpr.IntLit(18)),
                        new SqlExpr.StringLit("adult"))),
                new SqlExpr.StringLit("minor"));
        assertEquals("CASE WHEN t0.AGE > 18 THEN 'adult' ELSE 'minor' END", renderExpr(c));
        assertEquals("COUNT(DISTINCT t0.FIRM)",
                renderExpr(new SqlAgg.Reducer("COUNT", List.of(col("FIRM")), true)));
    }

    @Test
    @DisplayName("all join kinds render their SQL spellings")
    void allJoinKinds() {
        for (var k : SqlSource.Join.Kind.values()) {
            SqlSource j = new SqlSource.Join(T_PERSON,
                    new SqlSource.Table("T_FIRM", "t1", List.of()), k,
                    k == SqlSource.Join.Kind.CROSS ? null
                            : SqlExpr.Call.of("greaterEqual", col("ID"),
                                    new SqlExpr.Column("t1", "ID")));
            String sql = duck.render(SqlSelect.starOf(j));
            org.junit.jupiter.api.Assertions.assertTrue(sql.contains(k.sql),
                    () -> k + " must render as its spelling; got: " + sql);
        }
    }

    @Test
    @DisplayName("scalar subquery, lambda, qualified star, numeric literals, negate, IN")
    void remainingExprVariants() {
        assertEquals("(SELECT * FROM T_FIRM AS t1)",
                renderExpr(new SqlExpr.ScalarSubquery(SqlSelect.starOf(
                        new SqlSource.Table("T_FIRM", "t1", List.of())))));
        assertEquals("list_filter(t0.XS, x -> x > 1)",
                renderExpr(new SqlExpr.Call("list_filter", List.of(col("XS"),
                        new SqlExpr.Lambda(List.of("x"),
                                SqlExpr.Call.of("greater",
                                        new SqlExpr.Column(null, "x"),
                                        new SqlExpr.IntLit(1)))))));
        assertEquals("t0.*", renderExpr(new SqlExpr.Star("t0")));
        assertEquals("2.5", renderExpr(new SqlExpr.FloatLit(2.5)));
        assertEquals("1.50", renderExpr(new SqlExpr.DecimalLit(new java.math.BigDecimal("1.50"))));
        assertEquals("-t0.A", renderExpr(SqlExpr.Call.of("negate", col("A"))));
        assertEquals("t0.A IN (1, 2)", renderExpr(SqlExpr.Call.of("in",
                col("A"), new SqlExpr.IntLit(1), new SqlExpr.IntLit(2))));
        assertEquals("CASE WHEN t0.A = 1 THEN 'x' END",
                renderExpr(new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                        SqlExpr.Call.of("equal", col("A"), new SqlExpr.IntLit(1)),
                        new SqlExpr.StringLit("x"))), null)));
    }

    @Test
    @DisplayName("depth-2 subselect indentation stays consistent")
    void depthTwoIndent() {
        SqlSelect inner = SqlSelect.starOf(T_PERSON)
                .withWhere(SqlExpr.Call.of("greater", col("AGE"), new SqlExpr.IntLit(30)));
        SqlSelect mid = SqlSelect.starOf(new SqlSource.Subselect(inner, "t1")).withLimit(5L);
        SqlSelect outer = SqlSelect.starOf(new SqlSource.Subselect(mid, "t2")).withLimit(3L);
        assertEquals("""
                SELECT *
                FROM (
                  SELECT *
                  FROM (
                    SELECT *
                    FROM T_PERSON AS t0
                    WHERE t0.AGE > 30
                  ) AS t1
                  LIMIT 5
                ) AS t2
                LIMIT 3""",
                duck.render(outer));
    }

    @Test
    @DisplayName("union of three branches; fewer than two throws")
    void unionEdges() {
        SqlUnion u3 = new SqlUnion(List.of(SqlSelect.starOf(T_PERSON),
                SqlSelect.starOf(T_PERSON), SqlSelect.starOf(T_PERSON)), false, List.of());
        assertEquals(2, count(duck.render(u3), "\nUNION\n".replace("\n", "\n")),
                "two separators for three branches");
        assertThrows(IllegalArgumentException.class,
                () -> new SqlUnion(List.of(SqlSelect.starOf(T_PERSON)), true, List.of()));
    }

    private String renderExpr(SqlExpr e) {
        return duck.render(SqlSelect.starOf(T_PERSON)
                        .withProjections(List.of(new SqlSelect.Projection(e, null)), List.of()))
                .lines().findFirst().orElseThrow().substring("SELECT ".length());
    }
}
