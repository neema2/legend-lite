package com.legend.lowering;

import com.legend.Compiler;
import com.legend.sql.SqlQuery;
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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * M2 end-to-end: Pure query text → Phase G typed HIR → {@link Lowerer} →
 * {@link DuckDb} → EXECUTED against real DuckDB. Every test asserts BOTH
 * dimensions: the SQL's shape (exact golden or flatness count — the lean
 * tenet) and its results (the semantics).
 */
class LowerRelationTest {

    private static final String MODEL = """
            Database test::DB
            (
              Table T_PERSON (NAME VARCHAR(100) NOT NULL, AGE INTEGER NOT NULL,
                              FIRM VARCHAR(50))
            )
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE T_PERSON (NAME VARCHAR NOT NULL, AGE INTEGER NOT NULL,"
                    + " FIRM VARCHAR)");
            st.execute("INSERT INTO T_PERSON VALUES ('Ann', 25, 'ACME'),"
                    + " ('Bob', 35, 'ACME'), ('Cat', 45, 'Widget'), ('Dan', 55, NULL)");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private String sqlOf(String query) {
        SqlQuery q = new Lowerer().lower(Compiler.compileQuery(MODEL, query));
        return new DuckDb().render(q);
    }

    /** Execute; return rows as "cell|cell" strings. */
    private List<String> exec(String sql) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) {
                        b.append("|");
                    }
                    b.append(rs.getObject(i));
                }
                rows.add(b.toString());
            }
        }
        return rows;
    }

    private int count(String sql, String kw) {
        int c = 0;
        for (int i = sql.indexOf(kw); i >= 0; i = sql.indexOf(kw, i + kw.length())) c++;
        return c;
    }

    @Test
    @DisplayName("THE tenet: filter→select→sort→limit folds into ONE flat SELECT")
    void wholeChainOneSelect() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->filter(x|$x.AGE > 30)"
                + "->select(~[NAME, AGE])"
                + "->sort(~AGE->descending())"
                + "->limit(2)");
        assertEquals("""
                SELECT t0.NAME, t0.AGE
                FROM T_PERSON AS t0
                WHERE t0.AGE > 30
                ORDER BY t0.AGE DESC
                LIMIT 2""", sql);
        assertEquals(List.of("Dan|55", "Cat|45"), exec(sql));
    }

    @Test
    @DisplayName("filter AND-merges with filter; select-then-filter still folds (plain columns)")
    void filterMergesAndSubstitutes() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->filter(x|$x.AGE > 30)"
                + "->select(~[NAME, AGE])"
                + "->filter(x|$x.AGE < 50)");
        assertEquals(1, count(sql, "SELECT"), "still one SELECT: " + sql);
        String where = sql.lines().filter(l -> l.startsWith("WHERE")).findFirst().orElseThrow();
        assertEquals("WHERE t0.AGE > 30 AND t0.AGE < 50", where, "both predicates AND-merged");
        assertEquals(List.of("Bob|35", "Cat|45"), exec(sql), "full result equality");
    }

    @Test
    @DisplayName("filter after limit ISOLATES — filtering does not commute with truncation")
    void filterAfterLimitIsolates() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->sort(~AGE->ascending())->limit(2)"
                + "->filter(x|$x.AGE > 30)");
        assertEquals(2, count(sql, "SELECT"), "boundary: exactly 2 SELECTs: " + sql);
        assertEquals(List.of("Bob|35|ACME"), exec(sql));
    }

    @Test
    @DisplayName("rename lowers to a flat explicit projection (schema-driven)")
    void renameFlat() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#->rename(~NAME, ~FULL_NAME)");
        assertEquals("""
                SELECT t0.NAME AS FULL_NAME, t0.AGE, t0.FIRM
                FROM T_PERSON AS t0""", sql);
        assertEquals(List.of("Ann|25|ACME", "Bob|35|ACME", "Cat|45|Widget", "Dan|55|null"),
                exec(sql));
    }

    @Test
    @DisplayName("rename→filter-on-new-name folds via substitution: one SELECT")
    void renameThenFilterSubstitutes() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#->rename(~AGE, ~YEARS)"
                + "->filter(x|$x.YEARS > 50)");
        assertEquals(1, count(sql, "SELECT"), "substitution keeps it flat: " + sql);
        assertEquals(List.of("Dan|55|null"), exec(sql));
    }

    @Test
    @DisplayName("distinct folds; distinct-then-sort folds; sort-then-distinct isolates")
    void distinctBoundaries() throws SQLException {
        String flat = sqlOf("#>{test::DB.T_PERSON}#->select(~FIRM)->distinct()"
                + "->sort(~FIRM->ascending())");
        assertEquals(1, count(flat, "SELECT"), "distinct before sort is flat: " + flat);
        assertEquals(List.of("ACME", "Widget", "null"), exec(flat), "deduped AND sorted values");

        // Full-row dedup COMMUTES with sort — G desugars distinct() to all
        // columns, so this stays FLAT (leaner than master, still correct).
        String stillFlat = sqlOf("#>{test::DB.T_PERSON}#->sort(~AGE->ascending())->distinct()");
        assertEquals(1, count(stillFlat, "SELECT"), "whole-row distinct commutes with sort: " + stillFlat);
        assertEquals(4, exec(stillFlat).size());

        // NARROWING dedup after sort would drop the ORDER BY column under
        // DISTINCT (invalid SQL) — must isolate.
        String nested = sqlOf("#>{test::DB.T_PERSON}#->sort(~AGE->ascending())->distinct(~[FIRM])");
        assertEquals(2, count(nested, "SELECT"), "narrowing distinct after sort isolates: " + nested);
        assertEquals(List.of("ACME", "Widget", "null"),
                exec(nested).stream().sorted(java.util.Comparator.nullsLast(
                        java.util.Comparator.naturalOrder())).toList(),
                "exactly the three distinct firms");
    }

    @Test
    @DisplayName("slice/drop map to LIMIT/OFFSET; second slicing isolates")
    void slicing() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#->sort(~AGE->ascending())->slice(1, 3)");
        assertEquals(1, count(sql, "SELECT"));
        assertEquals(List.of("Bob|35|ACME", "Cat|45|Widget"), exec(sql));

        String nested = sqlOf("#>{test::DB.T_PERSON}#->sort(~AGE->ascending())"
                + "->drop(1)->drop(1)");
        assertEquals(2, count(nested, "SELECT"), "drop-of-drop isolates: " + nested);
        assertEquals(List.of("Cat|45|Widget", "Dan|55|null"), exec(nested));
    }

    @Test
    @DisplayName("scalar semantics ride through: isEmpty→IS NULL, string plus→concat")
    void scalarSemantics() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#->filter(x|$x.FIRM->isEmpty())");
        assertEquals(List.of("Dan|55|null"), exec(sql));

        String concat = sqlOf("#>{test::DB.T_PERSON}#"
                + "->filter(x|($x.NAME + '!') == 'Ann!')");
        assertEquals(List.of("Ann|25|ACME"), exec(concat));
    }

    @Test
    @DisplayName("empty TDS keeps its schema with ZERO rows (WHERE 1=0 gate)")
    void emptyTds() throws SQLException {
        String sql = sqlOf("#TDS\n  id, name\n#->select(~[id, name])");
        assertEquals(List.of(), exec(sql), "no rows survive the 1=0 gate");
        try (var st = conn.createStatement(); var rs = st.executeQuery(sql)) {
            assertEquals("id", rs.getMetaData().getColumnName(1), "schema survives");
            assertEquals("name", rs.getMetaData().getColumnName(2));
        }
    }

    @Test
    @DisplayName("unregistered scalar overload fails LOUDLY naming the signature")
    void unregisteredOverloadThrows() {
        IllegalStateException ex = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class,
                () -> sqlOf("#>{test::DB.T_PERSON}#->filter(x|$x.NAME->startsWith('A'))"));
        org.junit.jupiter.api.Assertions.assertTrue(
                ex.getMessage().contains("startsWith"),
                "error names the overload; got: " + ex.getMessage());
    }

    @Test
    @DisplayName("groupBy folds: keys+aggs replace projections, ONE SELECT")
    void groupByFlat() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->filter(x|$x.AGE > 20)"
                + "->groupBy(~FIRM, ~total : x|$x.AGE : y|$y->sum())");
        assertEquals("""
                SELECT t0.FIRM, SUM(t0.AGE) AS total
                FROM T_PERSON AS t0
                WHERE t0.AGE > 20
                GROUP BY t0.FIRM
                """.strip(), sql);
        assertEquals(List.of("ACME|60", "Widget|45", "null|55"),
                exec(sql + "\nORDER BY t0.FIRM"), "grouped sums");
    }

    @Test
    @DisplayName("HAVING goes live: filter after groupBy folds as HAVING with the agg expr")
    void filterAfterGroupByIsHaving() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->groupBy(~FIRM, ~total : x|$x.AGE : y|$y->sum())"
                + "->filter(x|$x.total > 50)");
        assertEquals(1, count(sql, "SELECT"), "HAVING keeps it flat: " + sql);
        String having = sql.lines().filter(l -> l.startsWith("HAVING")).findFirst().orElseThrow();
        assertEquals("HAVING SUM(t0.AGE) > 50", having,
                "the aggregate EXPRESSION substitutes into HAVING");
        assertEquals(List.of("ACME|60", "null|55"),
                exec(sql + "\nORDER BY 1"), "only groups above 50");
    }

    @Test
    @DisplayName("filter on a group KEY also folds to HAVING (standard SQL)")
    void filterOnGroupKeyIsHaving() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->groupBy(~FIRM, ~n : x|$x : y|$y->count())"
                + "->filter(x|$x.FIRM == 'ACME')");
        assertEquals(1, count(sql, "SELECT"));
        assertEquals(List.of("ACME|2"), exec(sql));
    }

    @Test
    @DisplayName("aggregate collapses to one row, no GROUP BY clause")
    void aggregateOneRow() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->aggregate(~avgAge : x|$x.AGE : y|$y->average())");
        assertEquals("""
                SELECT AVG(t0.AGE) AS avgAge
                FROM T_PERSON AS t0""", sql);
        assertEquals(List.of("40.0"), exec(sql));
    }

    @Test
    @DisplayName("extend commutes with LIMIT — appends stay flat where master nested")
    void extendAfterLimitStaysFlat() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#->sort(~AGE->ascending())->limit(2)"
                + "->extend(~doubled : x|$x.AGE * 2)");
        assertEquals(1, count(sql, "SELECT"), "extend adds a column, row count untouched: " + sql);
        assertEquals(List.of("Ann|25|ACME|50", "Bob|35|ACME|70"), exec(sql));
    }

    @Test
    @DisplayName("extend-on-extend chains flat; computed ref forces isolation")
    void extendChains() throws SQLException {
        String flat = sqlOf("#>{test::DB.T_PERSON}#"
                + "->extend(~a : x|$x.AGE + 1)->extend(~b : x|$x.AGE + 2)");
        assertEquals(1, count(flat, "SELECT"), "independent extends fold: " + flat);

        String nested = sqlOf("#>{test::DB.T_PERSON}#"
                + "->extend(~a : x|$x.AGE + 1)->extend(~b : x|$x.a * 2)");
        assertEquals(2, count(nested, "SELECT"),
                "ref to a COMPUTED column isolates (no silent recompute): " + nested);
        assertEquals(List.of("Ann|25|ACME|26|52"),
                exec(nested + "\nLIMIT 1"));
    }

    @Test
    @DisplayName("project replaces columns with computed ones")
    void projectComputed() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->project(~[who : x|$x.NAME, older : x|$x.AGE + 10])");
        assertEquals("""
                SELECT t0.NAME AS who, t0.AGE + 10 AS older
                FROM T_PERSON AS t0""", sql);
        assertEquals(List.of("Ann|35", "Bob|45", "Cat|55", "Dan|65"), exec(sql));
    }

    @Test
    @DisplayName("concatenate: bare UNION ALL, zero wrapper SELECTs; folds under later ops")
    void concatenateShapes() throws SQLException {
        String bare = sqlOf("#>{test::DB.T_PERSON}#->concatenate(#>{test::DB.T_PERSON}#)");
        assertEquals(2, count(bare, "SELECT"), "two branches, NO wrapper: " + bare);
        assertEquals(8, exec(bare).size());

        String filtered = sqlOf("#>{test::DB.T_PERSON}#"
                + "->concatenate(#>{test::DB.T_PERSON}#)"
                + "->filter(x|$x.AGE > 50)");
        assertEquals(3, count(filtered, "SELECT"), "union wraps once, filter folds on top: " + filtered);
        assertEquals(List.of("Dan|55|null", "Dan|55|null"), exec(filtered));
    }

    @Test
    @DisplayName("TDS literal → VALUES; filter folds onto it")
    void tdsLiterals() throws SQLException {
        String sql = sqlOf("#TDS\n  id, name\n  1, a\n  2, b\n#->filter(x|$x.id > 1)");
        assertEquals(1, count(sql, "SELECT"), "VALUES + WHERE folds: " + sql);
        assertEquals(List.of("2|b"), exec(sql));
    }
}
