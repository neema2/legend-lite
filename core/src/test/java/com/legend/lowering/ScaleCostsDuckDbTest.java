// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.lowering;

import com.legend.testing.Lean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSource;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.SqlIslandCases;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The two costs the tenth line's scale suite measured in the compiler's files (2026-10-14), pinned as work, not
 * seconds. A join's delivered columns are computed once, when the join is made: every reader had the list built
 * again down to the leaves, so a chain of N joins built N levels at each column lookup. A fold writes a computed
 * projection in a read's place within a budget ({@link Fold.Budget}, Calcite's bloat of 100 nodes a pass): a column
 * read twice doubled at every level of nesting: the tenth line's suite measured 20 nested derived tables at 53 MB
 * of SQL; the chain here, reading the level below twice, gave 7 MB at 10 levels and ran out of memory at 15.
 */
class ScaleCostsDuckDbTest {

    /** DuckDB, whose rendering the read counts pin and whose session gives the answer (named for it by the dialect
     *  program's Phase 6, 2026-10-17: it was the platform's engine). */
    private static final TestBackend DUCKDB = TestBackends.of(DatabaseType.DuckDB);

    private static final String MODEL =
            SqlIslandCases.model(DUCKDB.connectionBody());
    private static final String EMP = SqlIslandCases.SOURCE;
    private static ModelContext ctx;
    private static Connection session;

    @BeforeAll
    static void seed() throws SQLException {
        ctx = Compiler.compileModel(MODEL);
        session = DUCKDB.open();
        try (Statement s = session.createStatement()) {
            s.execute(SqlIslandCases.table(DUCKDB));
            s.execute("INSERT INTO EMP VALUES " + SqlIslandCases.ROWS);
        }
    }

    @AfterAll
    static void close() throws SQLException {
        session.close();
    }

    @Test
    @DisplayName("a join's delivered columns are one list, computed when the join is made")
    void joinOutputsAreComputedOnce() {
        OutputCol id = new OutputCol("ID", com.legend.sql.SqlType.Scalar.INTEGER, false);
        OutputCol name = new OutputCol("NAME", com.legend.sql.SqlType.Scalar.VARCHAR, false);
        SqlSource.Table left = table("t1", id);
        SqlSource.Table right = table("t2", name);
        SqlSource.Join join = new SqlSource.Join(left, right, SqlSource.Join.Kind.LEFT,
                new SqlExpr.BoolLit(true));
        assertSame(join.outputs(), join.outputs(), "the same list on every read");
        // the pad truth is stamped at the source, as before: a LEFT join's right side is nullable
        assertEquals(List.of("ID", "NAME"), join.outputs().stream().map(OutputCol::name).toList());
        assertFalse(join.outputs().get(0).nullable());
        assertTrue(join.outputs().get(1).nullable());
        // a chain reuses the lower join's list rather than walking it again
        SqlSource.Join chain = new SqlSource.Join(join, table("t3", id),
                SqlSource.Join.Kind.INNER, new SqlExpr.BoolLit(true));
        assertEquals(3, chain.outputs().size());
        assertSame(chain.outputs(), chain.outputs());
    }

    @Test
    @DisplayName("a chain of a hundred joins lowers")
    void aLongJoinChainLowers() {
        StringBuilder sql = new StringBuilder("SELECT t1.ID AS k FROM " + EMP + " t1");
        for (int i = 2; i <= 100; i++) {
            sql.append(" JOIN ").append(EMP).append(" t").append(i).append(" ON t").append(i).append(".ID = t")
                    .append(i - 1).append(".ID");
        }
        String rendered = render(sql.toString());
        assertEquals(99, Pattern.compile("\\bJOIN\\b").matcher(rendered).results().count(),
                rendered.substring(0, Math.min(200, rendered.length())));
    }

    @Test
    @DisplayName("a computed column read twice at every level of nesting stays linear in the SQL")
    void aDoublingChainStaysLinear() throws SQLException {
        // level k reads level k-1's x twice: without the budget each level doubled the text (2^k reads of age)
        String twelve = render(nest(12));
        int reads = count(twelve, "age");
        // x + x lowers null-safe (CASE WHEN x IS NULL OR x IS NULL THEN NULL ELSE x + x END): four reads of x, eleven
        // nodes. A level over a plain column costs nothing; the next folds into it (three further reads of eleven
        // nodes, 30); the one after would cost three of fifty-one, past the budget, so it isolates. Two levels a
        // select, and the base column is read by the first level alone, four times (4^11 before)
        assertEquals(4, reads, "reads of the base column in 12 levels");
        assertTrue(twelve.length() < 6_000, "chars: " + twelve.length());
        // the answer is DuckDB's own, across the isolation the budget forces
        assertEquals(direct(nest(9)), island(nest(9)));
    }

    @Test
    @DisplayName("a small computed column read twice still folds into one select")
    void aSmallComputedColumnReadTwiceStillFolds() {
        String rendered = render("SELECT q.x AS a, q.x AS b FROM (SELECT salary * 2 AS x FROM " + EMP + ") q");
        assertEquals(2, count(rendered, "* 2"), rendered);
        assertEquals(0, count(rendered, "FROM ("), rendered);
    }

    @Test
    @DisplayName("a computed column read once under a rule that writes its argument many times is written once")
    void aRuleCopiedArgumentIsReadByName() throws SQLException {
        // sqrt's domain guard writes its argument some 27 times (its message spells the value), so a CASE read once
        // under a sqrt was written 27 times within the budget's one charge (2026-10-30: a 63-day volatility wrote its
        // periods-a-year CASE 59 times, 74% of its SQL); the copies are counted as written (Fold.overCopied), and past
        // the budget the CASE stays in the select below, read by name
        String sql = "SELECT sqrt(q.x) AS r FROM (SELECT CASE WHEN age < 26 THEN 1 WHEN age < 31 THEN 4"
                + " WHEN age < 36 THEN 9 ELSE 16 END AS x FROM " + EMP + ") q ORDER BY r";
        String rendered = render(sql);
        assertEquals(1, count(rendered, "age < 26"), rendered);
        assertEquals(direct(sql), island(sql));
    }

    @Test
    @DisplayName("an integer division by a written literal other than 0 carries no zero guard")
    void aLiteralDivisorNeedsNoGuard() throws SQLException {
        // the literal's token is compared, nothing computed (2026-10-30: Wilder's (w - 1) / w was guarded at each
        // of RSI's 64 terms); a divisor of 0, or one read from a row, keeps the guard that raises Pure's error
        String byTwo = "SELECT age / 2 AS h FROM " + EMP + " ORDER BY h";
        assertEquals(0, count(render(byTwo), "Division by zero"), render(byTwo));
        assertEquals(direct(byTwo), island(byTwo));
        assertEquals(1, count(render("SELECT age / 0 AS h FROM " + EMP), "Division by zero"));
        assertEquals(1, count(render("SELECT age / id AS h FROM " + EMP), "Division by zero"));
    }

    private static SqlSource.Table table(String alias, OutputCol column) {
        return new SqlSource.Table("EMP", alias, List.of(column), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE);
    }

    @Test
    @DisplayName("an extend chain whose each column adds the two before it stays linear in the SQL")
    void anExtendChainStaysLinear() throws SQLException {
        // every level keeps its columns (an extend) and adds one: the kept computed columns are the budget's first
        // writes, so a new column's read of one costs, and the pass isolates once the level's growth passes 100
        // nodes; without that the sizes grew as Fibonacci's numbers (the audit's find)
        String twenty = render(fib(20));
        int reads = count(twenty, "age");
        // the budget folds c2, c3 and c4 into the base's select (the null-safe sum reads each operand twice, so c4
        // alone reads the base column 28 times: 1 + 1 + 4 + 10 + 28), and c5's reads of c3 and c4 would add more than
        // 100 nodes, so it isolates; every later block reads the aliases below it (Fibonacci's growth before)
        assertEquals(44, reads, "reads of the base column in 20 levels");
        assertTrue(twenty.length() < 12_000, "chars: " + twenty.length());
        assertEquals(direct(fib(9)), island(fib(9)));
    }

    /** {@code n} levels of {@code SELECT *, c(k-2) + c(k-1) AS ck FROM (...) qk} over two copies of the age. */
    private static String fib(int n) {
        String sql = "SELECT age AS c0, age AS c1 FROM " + EMP;
        for (int k = 2; k <= n + 1; k++) {
            sql = "SELECT *, c" + (k - 2) + " + c" + (k - 1) + " AS c" + k + " FROM (" + sql + ") q" + k;
        }
        return sql + " ORDER BY c0";
    }

    /** {@code n} levels of {@code SELECT x + x AS x FROM (...) qk} over the employees' ages. */
    private static String nest(int n) {
        String sql = "SELECT age AS x FROM " + EMP;
        for (int i = 1; i <= n; i++) {
            sql = "SELECT x + x AS x FROM (" + sql + ") q" + i;
        }
        return sql + " ORDER BY x";
    }

    private static String render(String sql) {
        // parsed through the suite's door (Own), as every test parses query text
        var parsed = com.legend.testing.Own.spec("|#SQL{" + sql + "}#");
        List<com.legend.protocol.spec.ValueSpecification> statements =
                parsed instanceof com.legend.protocol.spec.LambdaFunction lf && lf.parameters().isEmpty()
                        ? lf.body() : List.of(parsed);
        var resolved = Compiler.resolveQuery(statements, new com.legend.model.ImportScope(List.of()), ctx);
        SqlQuery q = Compiler.lowerResolved(resolved, ctx, SqlIslandCases.RUNTIME, false);
        return DUCKDB.dialect().render(q);
    }

    private static int count(String text, String needle) {
        Matcher m = Pattern.compile(Pattern.quote(needle)).matcher(text);
        int n = 0;
        while (m.find()) {
            n++;
        }
        return n;
    }

    private static List<List<String>> island(String sql) throws SQLException {
        Lean.Result result = Lean.run(MODEL, "|#SQL{" + sql + "}#", SqlIslandCases.RUNTIME,
                session);
        List<List<String>> rows = new ArrayList<>();
        for (var row : result.rows()) {
            List<String> cells = new ArrayList<>();
            for (Object value : row) {
                cells.add(SqlIslandCases.cell(value));
            }
            rows.add(cells);
        }
        return rows;
    }

    private static List<List<String>> direct(String sql) throws SQLException {
        List<List<String>> rows = new ArrayList<>();
        try (Statement s = session.createStatement(); ResultSet rs = s.executeQuery(sql.replace(EMP, "EMP"))) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> cells = new ArrayList<>();
                for (int i = 1; i <= n; i++) {
                    cells.add(SqlIslandCases.cell(rs.getObject(i)));
                }
                rows.add(cells);
            }
        }
        return rows;
    }
}
