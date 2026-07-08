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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
              Table T_FIRM (F_NAME VARCHAR(50) NOT NULL, CITY VARCHAR(50) NOT NULL)
              Table T_EVENTS (E_NAME VARCHAR(50) NOT NULL, E_TS TIMESTAMP NOT NULL)
              Table T_QUOTES (Q_NAME VARCHAR(50) NOT NULL, Q_TS TIMESTAMP NOT NULL,
                              PRICE INTEGER NOT NULL)
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
            st.execute("CREATE TABLE T_FIRM (F_NAME VARCHAR NOT NULL, CITY VARCHAR NOT NULL)");
            st.execute("INSERT INTO T_FIRM VALUES ('ACME', 'NYC'), ('Widget', 'SF')");
            st.execute("CREATE TABLE T_EVENTS (E_NAME VARCHAR NOT NULL, E_TS TIMESTAMP NOT NULL)");
            st.execute("INSERT INTO T_EVENTS VALUES"
                    + " ('A', TIMESTAMP '2024-01-01 10:30:00'),"
                    + " ('A', TIMESTAMP '2024-01-01 11:30:00')");
            st.execute("CREATE TABLE T_QUOTES (Q_NAME VARCHAR NOT NULL, Q_TS TIMESTAMP NOT NULL,"
                    + " PRICE INTEGER NOT NULL)");
            st.execute("INSERT INTO T_QUOTES VALUES"
                    + " ('A', TIMESTAMP '2024-01-01 10:00:00', 100),"
                    + " ('A', TIMESTAMP '2024-01-01 11:00:00', 110)");
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

    /** The statement-sequence path (query-level lets). */
    private String sqlOfBody(String query) {
        var ctx = Compiler.compileModel(MODEL);
        var body = new com.legend.compiler.spec.SpecCompiler(ctx).typeQueryBody(
                com.legend.compiler.NameResolver.resolveQuery(
                        com.legend.parser.SpecParser.parse(query)));
        return new DuckDb().render(new Lowerer().lower(body));
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
                () -> sqlOf("#>{test::DB.T_PERSON}#->filter(x|$x.NAME->decodeBase64() == 'x')"));
        org.junit.jupiter.api.Assertions.assertTrue(
                ex.getMessage().contains("decodeBase64"),
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

    // ---- windows: extend(over(...)) — the QUALIFY slot's end-to-end debut ----

    @Test
    @DisplayName("window extend folds: ROW_NUMBER with partition+order, ONE SELECT")
    void windowExtendFlat() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->extend(over(~FIRM, ~AGE->descending()), ~rn : {p, w, r | $p->rowNumber($r)})");
        assertEquals("""
                SELECT t0.*, ROW_NUMBER() OVER (PARTITION BY t0.FIRM ORDER BY t0.AGE DESC NULLS FIRST) AS rn
                FROM T_PERSON AS t0""", sql);
        assertEquals(List.of("Bob|35|ACME|1", "Ann|25|ACME|2", "Cat|45|Widget|1", "Dan|55|null|1"),
                exec(sql + "\nORDER BY t0.FIRM NULLS LAST, rn"));
    }

    @Test
    @DisplayName("QUALIFY goes live: filter on a window column folds, no subquery")
    void filterOnWindowColumnIsQualify() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->extend(over(~FIRM, ~AGE->descending()), ~rn : {p, w, r | $p->rowNumber($r)})"
                + "->filter(x|$x.rn == 1)");
        assertEquals(1, count(sql, "SELECT"), "QUALIFY keeps it flat: " + sql);
        String qualify = sql.lines().filter(l -> l.startsWith("QUALIFY")).findFirst().orElseThrow();
        assertEquals("QUALIFY ROW_NUMBER() OVER (PARTITION BY t0.FIRM"
                        + " ORDER BY t0.AGE DESC NULLS FIRST) = 1", qualify,
                "the window EXPRESSION substitutes into QUALIFY");
        assertEquals(List.of("Bob|35|ACME|1", "Cat|45|Widget|1", "Dan|55|null|1"),
                exec(sql + "\nORDER BY t0.AGE"), "oldest per firm");
    }

    @Test
    @DisplayName("windowed aggregate with a running frame executes correctly")
    void windowedAggWithFrame() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->extend(over(~FIRM, [asc(~AGE)], rows(unbounded(), 0)),"
                + " ~running : {p, w, r | $r.AGE} : y|$y->sum())");
        assertEquals(1, count(sql, "SELECT"));
        assertTrue(sql.contains("SUM(t0.AGE) OVER (PARTITION BY t0.FIRM ORDER BY t0.AGE NULLS LAST"
                        + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"),
                "frame renders: " + sql);
        assertEquals(List.of("Ann|25|ACME|25", "Bob|35|ACME|60", "Cat|45|Widget|45", "Dan|55|null|55"),
                exec(sql + "\nORDER BY t0.AGE"), "running total per firm");
    }

    @Test
    @DisplayName("lag with property access + toOne wrapper lowers through scalar composition")
    void lagComposesInScalars() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->extend(over([asc(~AGE)]),"
                + " ~delta : {p, w, r | $r.AGE - $p->lag($r).AGE->toOne()})");
        assertEquals(1, count(sql, "SELECT"));
        assertTrue(sql.contains("t0.AGE - LAG(t0.AGE) OVER (ORDER BY t0.AGE NULLS LAST)"),
                "lag column from the property access; toOne erased: " + sql);
        assertEquals(List.of("Ann|25|ACME|null", "Bob|35|ACME|10", "Cat|45|Widget|10", "Dan|55|null|10"),
                exec(sql + "\nORDER BY t0.AGE"), "consecutive age deltas");
    }

    @Test
    @DisplayName("whole-relation agg extend: SUM(x) OVER (), flat")
    void extendAggOverAll() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->extend(~total : x|$x.AGE : y|$y->sum())");
        assertEquals(1, count(sql, "SELECT"));
        assertTrue(sql.contains("SUM(t0.AGE) OVER ()"), sql);
        assertEquals(List.of("Ann|25|ACME|160"), exec(sql + "\nORDER BY t0.AGE\nLIMIT 1"));
    }

    @Test
    @DisplayName("MATRIX: every over() form renders its OVER clause and executes")
    void overFormMatrix() throws SQLException {
        String[][] cases = {
            // over form fragment                      expected OVER text
            {"over(~FIRM)",
             "OVER (PARTITION BY t0.FIRM)"},
            {"over(~FIRM, [asc(~AGE)])",
             "OVER (PARTITION BY t0.FIRM ORDER BY t0.AGE NULLS LAST)"},
            {"over(~[FIRM, NAME])",
             "OVER (PARTITION BY t0.FIRM, t0.NAME)"},
            {"over(~[FIRM, NAME], [desc(~AGE)])",
             "OVER (PARTITION BY t0.FIRM, t0.NAME ORDER BY t0.AGE DESC NULLS FIRST)"},
            {"over([desc(~AGE)])",
             "OVER (ORDER BY t0.AGE DESC NULLS FIRST)"},
            {"over([asc(~NAME), desc(~AGE)])",
             "OVER (ORDER BY t0.NAME NULLS LAST, t0.AGE DESC NULLS FIRST)"},
        };
        for (String[] c : cases) {
            String sql = sqlOf("#>{test::DB.T_PERSON}#->extend(" + c[0]
                    + ", ~n : {p, w, r | $p->rowNumber($r)})");
            assertTrue(sql.contains(c[1]), () -> c[0] + " must render " + c[1] + "; got: " + sql);
            assertEquals(4, exec(sql).size(), () -> c[0] + " must execute");
        }
    }

    @Test
    @DisplayName("MATRIX: every frame bound combination renders and executes")
    void frameBoundMatrix() throws SQLException {
        String[][] cases = {
            {"rows(-1, 0)", "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"},
            {"rows(-2, -1)", "ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING"},
            {"rows(0, 1)", "ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING"},
            {"rows(1, 2)", "ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING"},
            {"rows(-1, 1)", "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"},
            {"rows(unbounded(), 0)", "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"},
            {"rows(0, unbounded())", "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING"},
            {"rows(unbounded(), unbounded())",
             "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"},
            {"_range(-2, 0)", "RANGE BETWEEN 2 PRECEDING AND CURRENT ROW"},
        };
        for (String[] c : cases) {
            String sql = sqlOf("#>{test::DB.T_PERSON}#->extend(over(~FIRM, [asc(~AGE)], " + c[0]
                    + "), ~s : {p, w, r | $r.AGE} : y|$y->sum())");
            assertTrue(sql.contains(c[1]), () -> c[0] + " must render " + c[1] + "; got: " + sql);
            assertEquals(4, exec(sql).size(), () -> c[0] + " must execute on DuckDB");
        }
    }

    @Test
    @DisplayName("frame SEMANTICS: forward-looking frame sums the remaining rows")
    void forwardFrameSemantics() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->extend(over(~FIRM, [asc(~AGE)], rows(0, unbounded())),"
                + " ~remaining : {p, w, r | $r.AGE} : y|$y->sum())");
        assertEquals(List.of("Ann|25|ACME|60", "Bob|35|ACME|35", "Cat|45|Widget|45", "Dan|55|null|55"),
                exec(sql + "\nORDER BY t0.AGE"),
                "each row sums itself + everything AFTER it in its partition");
    }

    @Test
    @DisplayName("window after limit ISOLATES — the window must see the truncated set")
    void windowAfterLimitIsolates() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#->sort(~AGE->ascending())->limit(2)"
                + "->extend(over(~FIRM), ~n : {p, w, r | $p->rowNumber($r)})");
        assertEquals(2, count(sql, "SELECT"), "boundary: " + sql);
        assertEquals(2, exec(sql).size(), "window ran over the 2 surviving rows");
    }

    // ---- joins: structural JoinTree source, per-side variable binding ----

    @Test
    @DisplayName("join: tables join DIRECTLY — one SELECT, inline ON, no side wrapping")
    void joinFlat() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#->join(#>{test::DB.T_FIRM}#,"
                + " JoinKind.INNER, {p, f | $p.FIRM == $f.F_NAME})");
        assertEquals("""
                SELECT *
                FROM T_PERSON AS t0
                JOIN T_FIRM AS t1 ON t0.FIRM = t1.F_NAME""", sql);
        assertEquals(List.of("Ann|25|ACME|ACME|NYC", "Bob|35|ACME|ACME|NYC",
                "Cat|45|Widget|Widget|SF"), exec(sql + "\nORDER BY t0.AGE"));
    }

    @Test
    @DisplayName("LEFT join keeps unmatched rows (Dan, null firm)")
    void leftJoinKeepsUnmatched() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#->join(#>{test::DB.T_FIRM}#,"
                + " JoinKind.LEFT, {p, f | $p.FIRM == $f.F_NAME})");
        assertTrue(sql.contains("LEFT OUTER JOIN"), sql);
        assertEquals(List.of("Dan|55|null|null|null"),
                exec(sql + "\nORDER BY t0.AGE").subList(3, 4));
    }

    @Test
    @DisplayName("prefix join renames EVERY right column explicitly")
    void prefixJoinRenames() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#->join(#>{test::DB.T_FIRM}#,"
                + " JoinKind.INNER, {p, f | $p.FIRM == $f.F_NAME}, 'f_')");
        assertEquals("""
                SELECT t0.*, t1.F_NAME AS f_F_NAME, t1.CITY AS f_CITY
                FROM T_PERSON AS t0
                JOIN T_FIRM AS t1 ON t0.FIRM = t1.F_NAME""", sql);
        assertEquals(3, exec(sql).size());
    }

    @Test
    @DisplayName("filter after join FOLDS: WHERE resolves to the correct SIDE alias")
    void joinThenFilterFolds() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#->join(#>{test::DB.T_FIRM}#,"
                + " JoinKind.INNER, {p, f | $p.FIRM == $f.F_NAME})"
                + "->filter(x|$x.CITY == 'NYC')");
        assertEquals(1, count(sql, "SELECT"), "folds onto the join select: " + sql);
        assertTrue(sql.contains("WHERE t1.CITY = 'NYC'"),
                "the ref resolves to the RIGHT side's alias: " + sql);
        assertEquals(List.of("Ann|25|ACME|ACME|NYC", "Bob|35|ACME|ACME|NYC"),
                exec(sql + "\nORDER BY t0.AGE"));
    }

    @Test
    @DisplayName("a filtered side wraps ONCE; the plain side joins directly")
    void filteredSideWrapsOnce() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#->filter(x|$x.AGE > 30)"
                + "->join(#>{test::DB.T_FIRM}#, JoinKind.INNER, {p, f | $p.FIRM == $f.F_NAME})");
        assertEquals(2, count(sql, "SELECT"), "left wraps (it has a WHERE), right does not: " + sql);
        assertTrue(sql.contains("JOIN T_FIRM AS"), "right side joins directly: " + sql);
        assertEquals(List.of("Bob|35|ACME|ACME|NYC", "Cat|45|Widget|Widget|SF"),
                exec(sql + "\nORDER BY AGE"));
    }

    @Test
    @DisplayName("asOfJoin: ASOF LEFT JOIN picks the latest quote at-or-before each event")
    void asOfJoinExecutes() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_EVENTS}#->asOfJoin(#>{test::DB.T_QUOTES}#,"
                + " {e, q | $e.E_TS >= $q.Q_TS})");
        assertTrue(sql.contains("ASOF LEFT JOIN T_QUOTES AS t1 ON t0.E_TS >= t1.Q_TS"), sql);
        assertEquals(List.of(
                "A|2024-01-01 10:30:00.0|A|2024-01-01 10:00:00.0|100",
                "A|2024-01-01 11:30:00.0|A|2024-01-01 11:00:00.0|110"),
                exec(sql + "\nORDER BY t0.E_TS"), "each event gets its latest prior quote");
    }

    // ---- scalar roots, from(), flatten ----

    @Test
    @DisplayName("REAL pure 4-arg window aggregate: average(p, w, r, ~col) -> AVG(col) OVER (...)")
    void fourArgWindowAverage() throws SQLException {
        // Verbatim real-pure signature (core_functions_standard/math/
        // aggregator/average.pure) — the engine-lite 3-arg row-returning
        // forms were made up and are gone.
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->extend(over(~FIRM), ~avgAge:{p,w,r|$p->average($w, $r, ~AGE)})");
        assertEquals("SELECT t0.*, AVG(t0.AGE) OVER (PARTITION BY t0.FIRM) AS avgAge\n"
                + "FROM T_PERSON AS t0", sql);
        assertEquals(4, exec(sql).size());
    }

    @Test
    @DisplayName("multi-statement lambda query: let-chains bind forward, last statement is the value")
    void letChainQuery() throws SQLException {
        String sql = sqlOfBody("|let first = 'John'; let last = 'Smith';"
                + " let full = $first + ' ' + $last; $full;");
        assertEquals("SELECT 'John' || ' ' || 'Smith' AS value", sql,
                "lets substitute through; ONE flat scalar select");
        assertEquals(List.of("John Smith"), exec(sql));
    }

    @Test
    @DisplayName("SCALAR result shape: bare scalar query is a FROM-less SELECT")
    void scalarRoot() throws SQLException {
        String sql = sqlOf("1 + 1");
        assertEquals("SELECT 1 + 1 AS value", sql);
        assertEquals(List.of("2"), exec(sql));
        assertEquals(List.of("7"), exec(sqlOf("1 + 2 * 3")),
                "REAL Pure precedence: * binds tighter (engine-lite's flat grammar gave 9)");
        assertEquals(List.of("true"), exec(sqlOf("true || true && false")),
                "&& binds tighter than || (flat grammar gives false)");
    }

    @Test
    @DisplayName("from(runtime) is a pass-through — zero SQL footprint")
    void fromPassThrough() throws SQLException {
        String withFrom = sqlOf("#>{test::DB.T_PERSON}#->filter(x|$x.AGE > 50)"
                + "->from(test::DB)");
        String without = sqlOf("#>{test::DB.T_PERSON}#->filter(x|$x.AGE > 50)");
        assertEquals(without, withFrom, "from() adds nothing to the SQL");
        assertEquals(List.of("Dan|55|null"), exec(withFrom));
    }

    @Test
    @DisplayName("flatten explodes a variant column via UNNEST; other columns ride along")
    void flattenVariant() throws SQLException {
        try (var st = conn.createStatement()) {
            st.execute("CREATE TABLE T_ORDERS (ID INTEGER NOT NULL, ITEMS VARCHAR)");
            st.execute("INSERT INTO T_ORDERS VALUES (1, '[10, 20]'), (2, '[30]'), (3, NULL)");
        }
        String model = """
                Database test::DB
                (
                  Table T_ORDERS (ID INTEGER NOT NULL, ITEMS SEMISTRUCTURED)
                )
                """;
        SqlQuery q = new Lowerer().lower(Compiler.compileQuery(model,
                "#>{test::DB.T_ORDERS}#->flatten(~ITEMS)"));
        String sql = new DuckDb().render(q);
        assertEquals(1, count(sql, "SELECT"), "flatten folds: " + sql);
        assertTrue(sql.contains("unnest(CAST(t0.ITEMS AS JSON[])) AS ITEMS"), sql);
        assertEquals(List.of("1|10", "1|20", "2|30"), exec(sql + "\nORDER BY ID, ITEMS"),
                "rows explode per element; NULL list yields no rows");
    }

    @Test
    @DisplayName("fold strategies: concat, same-type reduce, map-reduce — executed")
    void foldStrategies() throws SQLException {
        // SameType: running sum with init.
        assertEquals(List.of("7"), exec(sqlOf("[1, 2, 4]->fold({e, a | $e + $a}, 0)")));
        // CollectionBuild (G can't decompose the mixed-type body): sum of lengths.
        assertEquals(List.of("6"), exec(sqlOf(
                "['ab', 'cdef']->fold({e, a | $e->length() + $a}, 0)")));
        // Concatenation: accumulate the source onto init.
        assertEquals(List.of("[9, 1, 2]"), exec(sqlOf(
                "[1, 2]->fold({e, a | $a->add($e)}, [9])")).stream()
                .map(String::valueOf).toList());
    }

    @Test
    @DisplayName("pivot: DuckDB native PIVOT ... ON ... USING, executed")
    void pivotNative() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->pivot(~FIRM, ~total : x|$x.AGE : y|$y->sum())");
        assertTrue(sql.contains("PIVOT") && sql.contains("ON FIRM")
                && sql.contains("USING SUM(AGE) AS total"), sql);
        // Columns: NAME + one per firm value (ACME, Widget, NULL bucket).
        List<String> rows = exec(sql + "\nORDER BY NAME");
        assertEquals(4, rows.size(), "one row per person name: " + rows);
    }

    // ---- variant navigation: get / to(@T) / toMany(@T) ----

    @Test
    @DisplayName("variant get chain + to(@T): -> hops, ->> under the scalar cast")
    void variantGetChain() throws SQLException {
        try (var st = conn.createStatement()) {
            st.execute("CREATE TABLE T_DOCS (ID INTEGER NOT NULL, PAYLOAD VARCHAR)");
            st.execute("INSERT INTO T_DOCS VALUES"
                    + " (1, '{\"items\": [{\"sku\": \"A-7\"}], \"qty\": 3}'),"
                    + " (2, '{\"items\": [{\"sku\": \"B-2\"}], \"qty\": 5}')");
        }
        String model = """
                Database test::DB
                (
                  Table T_DOCS (ID INTEGER NOT NULL, PAYLOAD SEMISTRUCTURED)
                )
                """;
        SqlQuery q = new Lowerer().lower(Compiler.compileQuery(model,
                "#>{test::DB.T_DOCS}#->extend(~sku : x |"
                        + " $x.PAYLOAD->get('items')->get(0)->get('sku')->to(@String))"));
        String sql = new DuckDb().render(q);
        assertTrue(sql.contains(
                "CAST(t0.PAYLOAD -> 'items' -> 0 ->> 'sku' AS VARCHAR) AS sku"),
                "-> hops, ->> at the conversion: " + sql);
        assertEquals(List.of("1|A-7", "2|B-2"),
                exec("SELECT ID, sku FROM (" + sql + ")\nORDER BY ID"));
    }

    @Test
    @DisplayName("toMany(@Variant)->map->fold: the engine aggregation idiom, executed")
    void variantToManyMapFold() throws SQLException {
        try (var st = conn.createStatement()) {
            st.execute("CREATE TABLE T_CARTS (ID INTEGER NOT NULL, NUMS VARCHAR)");
            st.execute("INSERT INTO T_CARTS VALUES (1, '[1, 2, 3]'), (2, '[10]')");
        }
        String model = """
                Database test::DB
                (
                  Table T_CARTS (ID INTEGER NOT NULL, NUMS SEMISTRUCTURED)
                )
                """;
        SqlQuery q = new Lowerer().lower(Compiler.compileQuery(model,
                "#>{test::DB.T_CARTS}#->extend(~total : x | $x.NUMS->toMany(@Variant)"
                        + "->map(i | $i->to(@Integer)->toOne())"
                        + "->fold({e, a | $e + $a}, 0))"));
        String sql = new DuckDb().render(q);
        // The composition's SHAPE is pinned, not just its results: elements
        // via JSON[] cast, per-element CAST inside list_transform, reduced
        // with the swapped (acc, elem) lambda — all in ONE flat SELECT.
        assertEquals(1, count(sql, "SELECT"), sql);
        assertTrue(sql.contains("list_reduce(list_transform("
                        + "CAST(t0.NUMS AS JSON[]), i -> CAST(i AS BIGINT)), "),
                "composition shape: " + sql);
        assertEquals(List.of("1|6", "2|10"),
                exec("SELECT ID, total FROM (" + sql + ")\nORDER BY ID"),
                "sum of each row's JSON array");

        // toMany(@Integer): the TYPED-array cast branch.
        String typed = new DuckDb().render(new Lowerer().lower(Compiler.compileQuery(model,
                "#>{test::DB.T_CARTS}#->extend(~first : x |"
                        + " $x.NUMS->toMany(@Integer)->fold({e, a | $e + $a}, 0))")));
        assertTrue(typed.contains("CAST(t0.NUMS AS BIGINT[])"),
                "typed array cast: " + typed);
        assertEquals(List.of("1|6", "2|10"),
                exec("SELECT ID, first FROM (" + typed + ")\nORDER BY ID"));

        // to(@String) on a BARE variant column (no get to swap): plain CAST.
        String bare = new DuckDb().render(new Lowerer().lower(Compiler.compileQuery(model,
                "#>{test::DB.T_CARTS}#->extend(~txt : x | $x.NUMS->to(@String))")));
        assertTrue(bare.contains("CAST(t0.NUMS AS VARCHAR) AS txt"), bare);
        assertEquals(List.of("1|[1, 2, 3]", "2|[10]"),
                exec("SELECT ID, txt FROM (" + bare + ")\nORDER BY ID"),
                "whole-value text rendering");
    }

    @Test
    @DisplayName("cast erases on non-variant scalars; relation cast is a pass-through")
    void castErasure() throws SQLException {
        assertEquals(List.of("42"), exec(sqlOf("42->cast(@Number)")), "identity");
        String relCast = sqlOf("#>{test::DB.T_PERSON}#"
                + "->cast(@Relation<(NAME:String, AGE:Integer, FIRM:String)>)");
        assertEquals(sqlOf("#>{test::DB.T_PERSON}#"), relCast,
                "schema re-typing has zero SQL footprint");
    }

    @Test
    @DisplayName("TDS literal → VALUES; filter folds onto it")
    void tdsLiterals() throws SQLException {
        String sql = sqlOf("#TDS\n  id, name\n  1, a\n  2, b\n#->filter(x|$x.id > 1)");
        assertEquals(1, count(sql, "SELECT"), "VALUES + WHERE folds: " + sql);
        assertEquals(List.of("2|b"), exec(sql));
    }
}
