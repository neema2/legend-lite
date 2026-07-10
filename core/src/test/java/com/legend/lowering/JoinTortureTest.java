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
 * The join torture battery: multi-hop chains, mixed kinds, self-joins,
 * row explosions, diamonds, joins over aggregated/windowed sides, prefixed
 * re-joins — every shape rendered lean AND executed with exact row
 * assertions. Leanness dimension: a chain of N clause-free joins is ONE
 * SELECT with N join clauses (left-associative SQL); a side wraps only when
 * it carries clauses of its own.
 *
 * <p>(EXISTS / correlated-subquery predicates enter the pipeline with
 * Phase H association navigation; their IR + rendering are already
 * execution-tested at the IR level in {@code DuckDbValidityTest}.)
 */
class JoinTortureTest {

    private static final String MODEL = """
            Database test::DB
            (
              Table T_PERSON (NAME VARCHAR(100) NOT NULL, AGE INTEGER NOT NULL,
                              FIRM VARCHAR(50))
              Table T_FIRM (F_NAME VARCHAR(50) NOT NULL, CITY VARCHAR(50) NOT NULL)
              Table T_CITY (C_NAME VARCHAR(50) NOT NULL, POP INTEGER NOT NULL)
              Table T_TAGS (T_NAME VARCHAR(50) NOT NULL, TAG VARCHAR(50) NOT NULL)
            )
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE T_PERSON (NAME VARCHAR NOT NULL, AGE INTEGER NOT NULL,"
                    + " FIRM VARCHAR)");
            st.execute("INSERT INTO T_PERSON VALUES ('Ann', 25, 'ACME'), ('Bob', 35, 'ACME'),"
                    + " ('Cat', 45, 'Widget'), ('Dan', 55, NULL)");
            st.execute("CREATE TABLE T_FIRM (F_NAME VARCHAR NOT NULL, CITY VARCHAR NOT NULL)");
            st.execute("INSERT INTO T_FIRM VALUES ('ACME', 'NYC'), ('Widget', 'SF'),"
                    + " ('Ghost', 'LA')");
            st.execute("CREATE TABLE T_CITY (C_NAME VARCHAR NOT NULL, POP INTEGER NOT NULL)");
            st.execute("INSERT INTO T_CITY VALUES ('NYC', 8), ('SF', 1)");
            st.execute("CREATE TABLE T_TAGS (T_NAME VARCHAR NOT NULL, TAG VARCHAR NOT NULL)");
            st.execute("INSERT INTO T_TAGS VALUES ('Ann', 'java'), ('Ann', 'sql'),"
                    + " ('Bob', 'java'), ('Cat', 'ops')");
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

    private static final String P = "#>{test::DB.T_PERSON}#";
    private static final String F = "#>{test::DB.T_FIRM}#";
    private static final String C = "#>{test::DB.T_CITY}#";
    private static final String TAGS = "#>{test::DB.T_TAGS}#";

    @Test
    @DisplayName("exists over a collection: Pure semantics incl. exists([]) = false")
    void existsOverCollection() throws SQLException {
        String sql = sqlOf(P + "->filter(x|[30, 50]->exists(n|$n < $x.AGE))");
        assertTrue(sql.contains("coalesce(list_bool_or(list_transform([30, 50], n -> n < t0.AGE)), FALSE)"),
                sql);
        assertEquals(List.of("Bob|35|ACME", "Cat|45|Widget", "Dan|55|null"),
                exec(sql + "\nORDER BY t0.AGE"), "anyone older than 30 OR 50");
        // exists over an EMPTY collection is FALSE (not NULL) — nobody matches.
        assertEquals(0, exec(sqlOf(P + "->filter(x|[]->exists(n|$n == $x.AGE))")).size());
    }

    @Test
    @DisplayName("forAll over a collection: Pure semantics incl. forAll([]) = TRUE")
    void forAllOverCollection() throws SQLException {
        String sql = sqlOf(P + "->filter(x|[30, 50]->forAll(n|$n < $x.AGE))");
        assertTrue(sql.contains("list_bool_and"), sql);
        assertEquals(List.of("Dan|55|null"), exec(sql + "\nORDER BY t0.AGE"),
                "only Dan is older than BOTH");
        // forAll over an EMPTY collection is TRUE — everyone matches.
        assertEquals(4, exec(sqlOf(P + "->filter(x|[]->forAll(n|$n == $x.AGE))")).size());
    }

    @Test
    @DisplayName("CORRELATED subquery: nested relation query referencing the outer row")
    void correlatedNestedRelation() throws SQLException {
        String sql = sqlOf(P + "->filter(x|"
                + F + "->filter(f|$f.F_NAME == $x.FIRM)->size() > 0)");
        assertTrue(sql.contains("(SELECT COUNT(*) FROM T_FIRM AS t1 WHERE t1.F_NAME = t0.FIRM) > 0"),
                "correlated scalar subquery, outer ref resolved: " + sql);
        assertEquals(List.of("Ann|25|ACME", "Bob|35|ACME", "Cat|45|Widget"),
                exec(sql + "\nORDER BY t0.AGE"), "semi-join semantics; Dan's null firm drops");
    }

    @Test
    @DisplayName("NOT-correlated (anti-join): negated nested count keeps only the unmatched")
    void antiJoinViaNegatedCount() throws SQLException {
        String sql = sqlOf(P + "->filter(x|!("
                + F + "->filter(f|$f.F_NAME == $x.FIRM)->size() > 0))");
        assertEquals(List.of("Dan|55|null"), exec(sql),
                "NULL-firm rows: the correlated count is 0, negation keeps them");
    }

    @Test
    @DisplayName("RELATION exists: true SQL EXISTS, correlated, semi-join semantics")
    void relationExists() throws SQLException {
        String sql = sqlOf(P + "->filter(x|"
                + F + "->exists(f|$f.F_NAME == $x.FIRM))");
        assertTrue(sql.contains(
                "WHERE EXISTS (SELECT * FROM T_FIRM AS t1 WHERE t1.F_NAME = t0.FIRM)"), sql);
        assertEquals(List.of("Ann|25|ACME", "Bob|35|ACME", "Cat|45|Widget"),
                exec(sql + "\nORDER BY t0.AGE"), "Dan's null firm matches nothing");
    }

    @Test
    @DisplayName("RELATION forAll: NOT EXISTS with negated predicate; vacuous truth")
    void relationForAll() throws SQLException {
        // 'every firm's city is not LA' — Ghost is in LA, so this is FALSE for
        // every person when quantified over ALL firms... filter on a per-row
        // correlated subset instead: every firm MATCHING my firm is in NYC.
        String sql = sqlOf(P + "->filter(x|"
                + F + "->filter(f|$f.F_NAME == $x.FIRM)->forAll(f|$f.CITY == 'NYC'))");
        assertTrue(sql.contains("NOT EXISTS"), sql);
        assertTrue(sql.contains("NOT t1.CITY = 'NYC'"), "negated predicate inside: " + sql);
        // Ann/Bob: ACME->NYC true. Cat: Widget->SF false. Dan: NO matching
        // firms -> VACUOUSLY TRUE (forAll on empty).
        assertEquals(List.of("Ann|25|ACME", "Bob|35|ACME", "Dan|55|null"),
                exec(sql + "\nORDER BY t0.AGE"));
    }

    @Test
    @DisplayName("RELATION isEmpty/isNotEmpty: NOT EXISTS / EXISTS")
    void relationEmptiness() throws SQLException {
        String anti = sqlOf(P + "->filter(x|"
                + F + "->filter(f|$f.F_NAME == $x.FIRM)->isEmpty())");
        assertTrue(anti.contains("NOT EXISTS"), anti);
        assertEquals(List.of("Dan|55|null"), exec(anti), "anti-join via isEmpty");

        String semi = sqlOf(P + "->filter(x|"
                + F + "->filter(f|$f.F_NAME == $x.FIRM)->isNotEmpty())");
        assertEquals(3, exec(semi).size(), "semi-join via isNotEmpty");
    }

    @Test
    @DisplayName("3-hop chain is ONE flat SELECT with two inline joins")
    void threeHopChainFlat() throws SQLException {
        String sql = sqlOf(P
                + "->join(" + F + ", JoinKind.INNER, {p, f | $p.FIRM == $f.F_NAME})"
                + "->join(" + C + ", JoinKind.INNER, {pf, c | $pf.CITY == $c.C_NAME})");
        assertEquals("""
                SELECT *
                FROM T_PERSON AS t0
                JOIN T_FIRM AS t1 ON t0.FIRM = t1.F_NAME
                JOIN T_CITY AS t2 ON t1.CITY = t2.C_NAME""", sql);
        assertEquals(List.of("Ann|25|ACME|ACME|NYC|NYC|8", "Bob|35|ACME|ACME|NYC|NYC|8",
                "Cat|45|Widget|Widget|SF|SF|1"), exec(sql + "\nORDER BY t0.AGE"));
    }

    @Test
    @DisplayName("4-hop chain with MIXED kinds stays flat; LEFT nulls propagate")
    void fourHopMixedKinds() throws SQLException {
        String sql = sqlOf(P
                + "->join(" + F + ", JoinKind.LEFT, {p, f | $p.FIRM == $f.F_NAME})"
                + "->join(" + C + ", JoinKind.LEFT, {pf, c | $pf.CITY == $c.C_NAME})"
                + "->join(" + TAGS + ", JoinKind.LEFT, {pfc, t | $pfc.NAME == $t.T_NAME})");
        assertEquals(1, count(sql, "SELECT"), "4 tables, one SELECT: " + sql);
        assertEquals(3, count(sql, "LEFT OUTER JOIN"));
        // Dan: no firm -> no city -> no tag; every hop null but the row SURVIVES.
        List<String> dan = exec(sql + "\nORDER BY t0.AGE").stream()
                .filter(r -> r.startsWith("Dan")).toList();
        assertEquals(List.of("Dan|55|null|null|null|null|null|null|null"), dan);
        // Ann has 2 tags -> 2 rows (explosion propagates through the chain).
        assertEquals(2, exec(sql).stream().filter(r -> r.startsWith("Ann")).count());
    }

    @Test
    @DisplayName("self-join with prefix: same table twice, distinct aliases, pair explosion")
    void selfJoinPairs() throws SQLException {
        String sql = sqlOf(P
                + "->join(" + P + ", JoinKind.INNER, {a, b | $a.FIRM == $b.FIRM}, 'o_')");
        assertTrue(sql.contains("T_PERSON AS t0") && sql.contains("T_PERSON AS t1"),
                "two scans, two aliases: " + sql);
        // ACME has 2 people -> 2x2 pairs; Widget 1 -> 1; Dan's NULL firm never matches.
        assertEquals(5, exec(sql).size(), "2*2 + 1*1 pairs, NULL joins nothing");
        assertEquals(List.of("Ann|25|ACME|o_Bob", "Bob|35|ACME|o_Bob"),
                exec("SELECT NAME, AGE, FIRM, 'o_' || o_NAME AS pair FROM (" + sql
                        + ") WHERE o_NAME = 'Bob' ORDER BY AGE"));
    }

    @Test
    @DisplayName("row explosion: ON TRUE cross product, exact cardinality")
    void crossProductOnTrue() throws SQLException {
        String sql = sqlOf(P + "->join(" + F + ", JoinKind.INNER, {p, f | true})");
        assertEquals(1, count(sql, "SELECT"));
        assertTrue(sql.contains("ON TRUE"), sql);
        assertEquals(12, exec(sql).size(), "4 persons x 3 firms");
    }

    @Test
    @DisplayName("DIAMOND: two independently-joined arms re-join on the shared key")
    void diamondJoin() throws SQLException {
        // Arm 1: person->firm (prefix f_). Arm 2: person->tags (prefix g_, and
        // person cols prefixed too via a second prefix join back onto tags).
        String sql = sqlOf(P
                + "->join(" + F + ", JoinKind.INNER, {p, f | $p.FIRM == $f.F_NAME}, 'f_')"
                + "->join(" + TAGS + "->join(" + P + ", JoinKind.INNER,"
                + "         {t, p2 | $t.T_NAME == $p2.NAME}, 'p_'),"
                + "     JoinKind.INNER, {l, r | $l.NAME == $r.T_NAME})");
        // Left arm: the f_ prefixed select WRAPS under an UNPREFIXED outer
        // join — hosting fires only for prefixed joins (an unprefixed
        // joined() is SELECT * and would DROP the f_ renames; audit
        // blocker). Right arm: a join tree on the RIGHT wraps as always.
        assertEquals(3, count(sql, "SELECT"), "prefixed left arm wraps under unprefixed join: " + sql);
        // Ann(2 tags), Bob(1), Cat(1) survive both arms; Dan has no firm and no tags.
        assertEquals(4, exec(sql).size());
        assertTrue(exec(sql).stream().noneMatch(r -> r.startsWith("Dan")));
    }

    @Test
    @DisplayName("join AFTER groupBy: the aggregated side wraps, the plain side does not")
    void joinAfterGroupBy() throws SQLException {
        String sql = sqlOf(P
                + "->groupBy(~FIRM, ~headcount : x|$x : y|$y->count())"
                + "->join(" + F + ", JoinKind.INNER, {g, f | $g.FIRM == $f.F_NAME})");
        assertEquals(2, count(sql, "SELECT"), "grouped side wraps once: " + sql);
        assertTrue(sql.contains("JOIN T_FIRM AS"), "plain side joins directly: " + sql);
        assertEquals(List.of("ACME|2|ACME|NYC", "Widget|1|Widget|SF"),
                exec(sql + "\nORDER BY FIRM"));
    }

    @Test
    @DisplayName("join after WINDOW extend: windowed side wraps; window results ride through")
    void joinAfterWindow() throws SQLException {
        String sql = sqlOf(P
                + "->extend(over(~FIRM, [desc(~AGE)]), ~rnk : {p, w, r | $p->rowNumber($r)})"
                + "->filter(x|$x.rnk == 1)"
                + "->join(" + F + ", JoinKind.INNER, {p, f | $p.FIRM == $f.F_NAME})");
        assertEquals(2, count(sql, "SELECT"), "QUALIFY select wraps as a side: " + sql);
        assertEquals(List.of("Bob|35|ACME|1|ACME|NYC", "Cat|45|Widget|1|Widget|SF"),
                exec(sql + "\nORDER BY AGE"), "oldest per firm, joined to firm details");
    }

    @Test
    @DisplayName("filter BETWEEN joins breaks the chain exactly once")
    void filterBetweenJoins() throws SQLException {
        String sql = sqlOf(P
                + "->join(" + F + ", JoinKind.INNER, {p, f | $p.FIRM == $f.F_NAME})"
                + "->filter(x|$x.CITY == 'NYC')"
                + "->join(" + TAGS + ", JoinKind.INNER, {l, t | $l.NAME == $t.T_NAME})");
        assertEquals(2, count(sql, "SELECT"), "the filtered join wraps, then chains: " + sql);
        assertEquals(List.of("Ann|25|ACME|ACME|NYC|Ann|java", "Ann|25|ACME|ACME|NYC|Ann|sql",
                "Bob|35|ACME|ACME|NYC|Bob|java"), exec(sql + "\nORDER BY AGE, TAG"));
    }

    @Test
    @DisplayName("FULL join keeps unmatched rows from BOTH sides")
    void fullJoinBothSides() throws SQLException {
        String sql = sqlOf(P + "->join(" + F + ", JoinKind.FULL, {p, f | $p.FIRM == $f.F_NAME})");
        assertTrue(sql.contains("FULL OUTER JOIN"), sql);
        List<String> rows = exec(sql);
        assertEquals(5, rows.size(), "3 matches + Dan (left-only) + Ghost (right-only)");
        assertTrue(rows.stream().anyMatch(r -> r.startsWith("Dan|55")), "left-only survives");
        assertTrue(rows.stream().anyMatch(r -> r.endsWith("Ghost|LA")), "right-only survives");
    }

    @Test
    @DisplayName("RIGHT join keeps unmatched right rows")
    void rightJoin() throws SQLException {
        String sql = sqlOf(P + "->join(" + F + ", JoinKind.RIGHT, {p, f | $p.FIRM == $f.F_NAME})");
        assertTrue(sql.contains("RIGHT OUTER JOIN"), sql);
        assertEquals(List.of("null|null|null|Ghost|LA"),
                exec(sql).stream().filter(r -> r.contains("Ghost")).toList());
    }

    @Test
    @DisplayName("filter on a PREFIXED column resolves through the rename projections")
    void filterOnPrefixedColumn() throws SQLException {
        String sql = sqlOf(P
                + "->join(" + F + ", JoinKind.INNER, {p, f | $p.FIRM == $f.F_NAME}, 'f_')"
                + "->filter(x|$x.f_CITY == 'NYC')");
        assertEquals(1, count(sql, "SELECT"), "prefixed ref substitutes, still flat: " + sql);
        assertTrue(sql.contains("WHERE t1.CITY = 'NYC'"),
                "f_CITY resolves to the underlying right column: " + sql);
        assertEquals(2, exec(sql).size());
    }

    @Test
    @DisplayName("groupBy OVER a join chain: aggregate the joined result, one boundary")
    void groupByOverJoin() throws SQLException {
        String sql = sqlOf(P
                + "->join(" + F + ", JoinKind.INNER, {p, f | $p.FIRM == $f.F_NAME})"
                + "->groupBy(~CITY, ~avgAge : x|$x.AGE : y|$y->average())");
        assertEquals(1, count(sql, "SELECT"), "groupBy folds ONTO the join select: " + sql);
        assertTrue(sql.contains("GROUP BY t1.CITY"), "key resolves to the right side: " + sql);
        assertEquals(List.of("NYC|30.0", "SF|45.0"), exec(sql + "\nORDER BY 1"));
    }

    @Test
    @DisplayName("asOfJoin chained after a regular join executes")
    void asOfAfterJoin() throws SQLException {
        String sql = sqlOf("#>{test::DB.T_PERSON}#"
                + "->join(" + F + ", JoinKind.INNER, {p, f | $p.FIRM == $f.F_NAME})"
                + "->asOfJoin(" + TAGS + "->join(" + C + ", JoinKind.INNER, {t, c | true}, 'c_'),"
                + " {l, r | $l.AGE >= $r.c_POP})");
        assertEquals(1, count(sql, "ASOF LEFT JOIN"), sql);
        assertTrue(exec(sql).size() >= 3, "executes with a wrapped composite right side");
    }

    /** Column labels from ResultSet metadata — value-only asserts miss label bugs. */
    private List<String> labels(String sql) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            List<String> out = new ArrayList<>();
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                out.add(rs.getMetaData().getColumnLabel(i));
            }
            return out;
        }
    }

    // ---- audit pins: flat-hosting must NOT fire for unprefixed joins ----

    @Test
    @DisplayName("audit: rename before an UNPREFIXED join keeps the rename (no hosting)")
    void renameSurvivesUnprefixedJoin() throws SQLException {
        String sql = sqlOf(P + "->rename(~NAME, ~PNAME)"
                + "->join(" + F + ", JoinKind.INNER, {l, r | $l.FIRM == $r.F_NAME})");
        assertTrue(labels(sql).contains("PNAME"),
                "the rename must survive the join: " + labels(sql) + "\n" + sql);
    }

    @Test
    @DisplayName("audit: select-narrowing before an UNPREFIXED join keeps the narrowing")
    void narrowingSurvivesUnprefixedJoin() throws SQLException {
        String sql = sqlOf(P + "->select(~[NAME, FIRM])"
                + "->join(" + F + ", JoinKind.INNER, {l, r | $l.FIRM == $r.F_NAME})");
        assertEquals(List.of("NAME", "FIRM", "F_NAME", "CITY"), labels(sql),
                "dropped AGE must NOT leak back: " + sql);
    }

    @Test
    @DisplayName("audit: prefixed join then UNPREFIXED join keeps the prefixed labels")
    void prefixedLabelsSurviveUnprefixedJoin() throws SQLException {
        String sql = sqlOf(P
                + "->join(" + F + ", JoinKind.INNER, {p, f | $p.FIRM == $f.F_NAME}, 'f_')"
                + "->join(" + C + ", JoinKind.INNER, {l, c | $l.f_CITY == $c.C_NAME})");
        assertTrue(labels(sql).containsAll(List.of("f_F_NAME", "f_CITY")),
                "prefixed columns must keep their prefixes: " + labels(sql) + "\n" + sql);
    }

    @Test
    @DisplayName("audit: unprefixed chain then PREFIXED join — column count equals typed arity")
    void unprefixedChainThenPrefixedJoin() throws SQLException {
        String sql = sqlOf(P
                + "->join(" + F + ", JoinKind.INNER, {p, f | $p.FIRM == $f.F_NAME})"
                + "->join(" + C + ", JoinKind.INNER, {l, c | $l.CITY == $c.C_NAME}, 'c_')");
        // T_PERSON(3) + T_FIRM(2) + prefixed T_CITY(2) = 7 — no bare-star
        // leak of the right side (audit blocker 2).
        assertEquals(7, labels(sql).size(),
                "no unprefixed right-side leak: " + labels(sql) + "\n" + sql);
        assertTrue(labels(sql).containsAll(List.of("c_C_NAME", "c_POP")), labels(sql).toString());
    }

    @Test
    @DisplayName("audit: in([]) is FALSE, not NULL — negated filter keeps all rows")
    void inEmptyListIsFalse() throws SQLException {
        String sql = sqlOf(P + "->filter(x | !($x.AGE->in([])))");
        assertEquals(4, exec(sql).size(), "in(x,[]) is false; !false keeps every row: " + sql);
    }
}
