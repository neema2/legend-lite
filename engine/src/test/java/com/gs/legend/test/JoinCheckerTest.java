package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.sqlgen.DuckDBDialect;
import com.gs.legend.sqlgen.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for JoinChecker — Relation API {@code join()}.
 * <ul>
 * <li>Join types: INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER</li>
 * <li>Value assertions: every test verifies actual matched data, not just row
 * counts</li>
 * <li>NULL verification: outer join tests assert NULL in unmatched columns</li>
 * <li>Prefix / column deduplication</li>
 * <li>Complex conditions (AND, OR, inequality)</li>
 * <li>Chained operations: join→filter, join→select, join→extend, join→sort</li>
 * <li>Edge cases: empty tables, single rows, many-to-many, string
 * conditions</li>
 * </ul>
 */
public class JoinCheckerTest extends AbstractDatabaseTest {

    @Override
    protected String getDatabaseType() {
        return "DuckDB";
    }

    @Override
    protected SQLDialect getDialect() {
        return DuckDBDialect.INSTANCE;
    }

    @Override
    protected String getJdbcUrl() {
        return "jdbc:duckdb:";
    }

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(getJdbcUrl());
        setupPureModel();
        setupDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null)
            connection.close();
    }

    // ==================== INNER JOIN ====================

    @Nested
    @DisplayName("join() INNER")
    class InnerJoin {

        @Test
        @DisplayName("inner join TDS — verifies matched name-score pairs")
        void testInnerJoinMatchingRows() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                        3, Charlie
                    #->join(
                        #TDS
                            person_id, score
                            1, 90
                            2, 85
                            4, 70
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(2, result.rowCount(), "Only ids 1 and 2 match");
            var byName = collectResults(result, "name", "score");
            assertEquals(90L, ((Number) byName.get("Alice")).longValue());
            assertEquals(85L, ((Number) byName.get("Bob")).longValue());
            assertFalse(byName.containsKey("Charlie"), "Charlie has no match");
        }

        @Test
        @DisplayName("inner join TDS — no matches returns empty")
        void testInnerJoinNoMatches() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                    #->join(
                        #TDS
                            person_id, score
                            99, 100
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertTrue(result.rows().isEmpty(), "No matching ids");
        }

        @Test
        @DisplayName("inner join non-overlapping schemas — verifies column schema and values")
        void testInnerJoinNonOverlapping() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                    #->join(
                        #TDS
                            person_id, score
                            1, 90
                            2, 85
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(2, result.rowCount());
            assertEquals(4, result.columns().size(), "id, name, person_id, score");
            var byName = collectResults(result, "name", "score");
            assertEquals(90L, ((Number) byName.get("Alice")).longValue());
            assertEquals(85L, ((Number) byName.get("Bob")).longValue());
        }

        @Test
        @DisplayName("inner join — one-to-many produces multiple rows with same name")
        void testInnerJoinOneToMany() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                    #->join(
                        #TDS
                            person_id, score
                            1, 90
                            1, 85
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(2, result.rowCount(), "Alice matches both score rows");
            int nameIdx = columnIndex(result, "name");
            int scoreIdx = columnIndex(result, "score");
            // Both rows should have name=Alice
            for (var row : result.rows()) {
                assertEquals("Alice", row.get(nameIdx));
            }
            // Scores should be 85 and 90 (in some order)
            var scores = result.rows().stream()
                    .map(r -> ((Number) r.get(scoreIdx)).longValue())
                    .sorted().toList();
            assertEquals(85L, scores.get(0));
            assertEquals(90L, scores.get(1));
        }
    }

    // ==================== LEFT OUTER JOIN ====================

    @Nested
    @DisplayName("join() LEFT_OUTER")
    class LeftOuterJoin {

        @Test
        @DisplayName("left outer join — unmatched rows have NULL right columns")
        void testLeftOuterJoinPreservesLeft() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                        3, Charlie
                    #->join(
                        #TDS
                            person_id, score
                            1, 90
                            2, 85
                        #,
                        meta::pure::functions::relation::JoinKind.LEFT_OUTER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(3, result.rowCount(), "All 3 left rows preserved");
            int nameIdx = columnIndex(result, "name");
            int personIdIdx = columnIndex(result, "person_id");
            int scoreIdx = columnIndex(result, "score");
            for (var row : result.rows()) {
                String name = row.get(nameIdx).toString();
                if ("Charlie".equals(name)) {
                    assertNull(row.get(personIdIdx), "Charlie's person_id should be NULL");
                    assertNull(row.get(scoreIdx), "Charlie's score should be NULL");
                } else if ("Alice".equals(name)) {
                    assertEquals(90L, ((Number) row.get(scoreIdx)).longValue());
                } else if ("Bob".equals(name)) {
                    assertEquals(85L, ((Number) row.get(scoreIdx)).longValue());
                }
            }
        }

        @Test
        @DisplayName("left outer join — all match, no NULLs")
        void testLeftOuterJoinAllMatch() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                    #->join(
                        #TDS
                            person_id, score
                            1, 90
                            2, 85
                        #,
                        meta::pure::functions::relation::JoinKind.LEFT_OUTER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(2, result.rowCount());
            int scoreIdx = columnIndex(result, "score");
            for (var row : result.rows()) {
                assertNotNull(row.get(scoreIdx), "All rows should have a score");
            }
        }

        @Test
        @DisplayName("left outer join — no matches → all right cols NULL")
        void testLeftOuterJoinNoMatches() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                    #->join(
                        #TDS
                            person_id, score
                            99, 100
                        #,
                        meta::pure::functions::relation::JoinKind.LEFT_OUTER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(2, result.rowCount(), "Both left rows preserved");
            int scoreIdx = columnIndex(result, "score");
            for (var row : result.rows()) {
                assertNull(row.get(scoreIdx), "No match → score is NULL");
            }
        }
    }

    // ==================== RIGHT OUTER JOIN ====================

    @Nested
    @DisplayName("join() RIGHT_OUTER")
    class RightOuterJoin {

        @Test
        @DisplayName("right outer join — unmatched right rows have NULL left columns")
        void testRightOuterJoinPreservesRight() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                    #->join(
                        #TDS
                            person_id, score
                            1, 90
                            2, 85
                            3, 75
                        #,
                        meta::pure::functions::relation::JoinKind.RIGHT_OUTER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(3, result.rowCount(), "All 3 right rows preserved");
            int nameIdx = columnIndex(result, "name");
            int scoreIdx = columnIndex(result, "score");
            for (var row : result.rows()) {
                long score = ((Number) row.get(scoreIdx)).longValue();
                if (score == 90L) {
                    assertEquals("Alice", row.get(nameIdx));
                } else {
                    assertNull(row.get(nameIdx), "Unmatched right row → name is NULL, score=" + score);
                }
            }
        }

        @Test
        @DisplayName("right outer join — empty left → all left cols NULL")
        void testRightOuterJoinEmptyLeft() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                    #->join(
                        #TDS
                            person_id, score
                            1, 90
                            2, 85
                        #,
                        meta::pure::functions::relation::JoinKind.RIGHT_OUTER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(2, result.rowCount(), "Both right rows preserved");
            int nameIdx = columnIndex(result, "name");
            for (var row : result.rows()) {
                assertNull(row.get(nameIdx), "Empty left → all names are NULL");
            }
        }

        @Test
        @DisplayName("right outer join — all match")
        void testRightOuterJoinAllMatch() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                    #->join(
                        #TDS
                            person_id, score
                            1, 90
                            2, 85
                        #,
                        meta::pure::functions::relation::JoinKind.RIGHT_OUTER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(2, result.rowCount());
            var byName = collectResults(result, "name", "score");
            assertEquals(90L, ((Number) byName.get("Alice")).longValue());
            assertEquals(85L, ((Number) byName.get("Bob")).longValue());
        }

        @Test
        @DisplayName("right outer join — no matches → all left cols NULL")
        void testRightOuterJoinNoMatches() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                    #->join(
                        #TDS
                            person_id, score
                            99, 100
                            98, 200
                        #,
                        meta::pure::functions::relation::JoinKind.RIGHT_OUTER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(2, result.rowCount(), "Both right rows preserved");
            int nameIdx = columnIndex(result, "name");
            for (var row : result.rows()) {
                assertNull(row.get(nameIdx), "No match → name is NULL");
            }
        }
    }

    // ==================== FULL OUTER JOIN ====================

    @Nested
    @DisplayName("join() FULL_OUTER")
    class FullOuterJoin {

        @Test
        @DisplayName("full outer join — both sides preserved with NULLs verified")
        void testFullOuterJoinPreservesBoth() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                    #->join(
                        #TDS
                            person_id, score
                            2, 85
                            3, 75
                        #,
                        meta::pure::functions::relation::JoinKind.FULL_OUTER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(3, result.rowCount(),
                    "Alice(no match) + Bob-85(match) + 3-75(no match)");
            int nameIdx = columnIndex(result, "name");
            int scoreIdx = columnIndex(result, "score");
            int personIdIdx = columnIndex(result, "person_id");
            for (var row : result.rows()) {
                Object name = row.get(nameIdx);
                Object score = row.get(scoreIdx);
                if (name != null && "Alice".equals(name.toString())) {
                    assertNull(score, "Alice has no right match → score NULL");
                    assertNull(row.get(personIdIdx), "Alice has no right match → person_id NULL");
                } else if (name != null && "Bob".equals(name.toString())) {
                    assertEquals(85L, ((Number) score).longValue(), "Bob matched score=85");
                } else {
                    // Unmatched right row (person_id=3)
                    assertNull(name, "Right-only row → name is NULL");
                    assertEquals(75L, ((Number) score).longValue(), "Unmatched right row score=75");
                }
            }
        }

        @Test
        @DisplayName("full outer join — no overlap at all → all rows have NULLs on other side")
        void testFullOuterJoinNoOverlap() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                    #->join(
                        #TDS
                            person_id, score
                            99, 100
                        #,
                        meta::pure::functions::relation::JoinKind.FULL_OUTER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(2, result.rowCount(), "One from each side, no match");
            int nameIdx = columnIndex(result, "name");
            int scoreIdx = columnIndex(result, "score");
            boolean foundAlice = false, foundRight = false;
            for (var row : result.rows()) {
                Object name = row.get(nameIdx);
                Object score = row.get(scoreIdx);
                if (name != null && "Alice".equals(name.toString())) {
                    assertNull(score, "Alice → no right match → score NULL");
                    foundAlice = true;
                } else {
                    assertNull(name, "Right-only row → name NULL");
                    assertEquals(100L, ((Number) score).longValue());
                    foundRight = true;
                }
            }
            assertTrue(foundAlice, "Alice should be in the result");
            assertTrue(foundRight, "Right-only row should be in the result");
        }

        @Test
        @DisplayName("full outer join — empty left preserves right with NULL left cols")
        void testFullOuterJoinEmptyLeft() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                    #->join(
                        #TDS
                            person_id, score
                            1, 90
                            2, 85
                        #,
                        meta::pure::functions::relation::JoinKind.FULL_OUTER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(2, result.rowCount(), "Both right rows preserved");
            int nameIdx = columnIndex(result, "name");
            for (var row : result.rows()) {
                assertNull(row.get(nameIdx), "Empty left → all names NULL");
            }
        }

        @Test
        @DisplayName("full outer join — empty right preserves left with NULL right cols")
        void testFullOuterJoinEmptyRight() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                    #->join(
                        #TDS
                            person_id, score
                        #,
                        meta::pure::functions::relation::JoinKind.FULL_OUTER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(2, result.rowCount(), "Both left rows preserved");
            int scoreIdx = columnIndex(result, "score");
            for (var row : result.rows()) {
                assertNull(row.get(scoreIdx), "Empty right → all scores NULL");
            }
        }
    }

    // ==================== Prefix / Column Deduplication ====================

    @Nested
    @DisplayName("join() prefix / column deduplication")
    class PrefixDeduplication {

        @Test
        @DisplayName("overlapping column with prefix — verifies values through projected aliases")
        void testOverlappingColumnWithPrefix() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                    #->project(~[id1:x|$x.id, name1:x|$x.name])
                    ->join(
                        #TDS
                            id, col
                            1, MoreAlice
                            2, MoreBob
                        #->project(~[id2:x|$x.id, col:x|$x.col]),
                        meta::pure::functions::relation::JoinKind.INNER,
                        {x, y | $x.id1 == $y.id2}
                    )""");
            assertEquals(2, result.rowCount());
            assertEquals(4, result.columns().size());
            var byName = collectResults(result, "name1", "col");
            assertEquals("MoreAlice", byName.get("Alice"));
            assertEquals("MoreBob", byName.get("Bob"));
        }

        @Test
        @DisplayName("self-join with prefix — verifies self-joined values")
        void testSelfJoinWithPrefix() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name, score
                        1, Alice, 90
                        2, Bob, 85
                    #->join(
                        #TDS
                            id, name, score
                            1, Alice, 90
                            2, Bob, 85
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.id == $r.id},
                        'r'
                    )""");
            assertEquals(2, result.rowCount());
            var colNames = result.columns().stream().map(c -> c.name()).toList();
            assertTrue(colNames.contains("id"), "Left id kept");
            assertTrue(colNames.contains("r_id"), "Right id prefixed");
            // In a self-join, left and right values should match
            int scoreIdx = columnIndex(result, "score");
            int rScoreIdx = columnIndex(result, "r_score");
            for (var row : result.rows()) {
                assertEquals(
                        ((Number) row.get(scoreIdx)).longValue(),
                        ((Number) row.get(rScoreIdx)).longValue(),
                        "Self-join: left score == right score");
            }
        }

        @Test
        @DisplayName("partial overlap — only overlapping columns get prefix")
        void testPartialOverlapPrefix() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                    #->join(
                        #TDS
                            id, score
                            1, 90
                            2, 85
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.id == $r.id},
                        'right'
                    )""");
            var colNames = result.columns().stream().map(c -> c.name()).toList();
            assertTrue(colNames.contains("id"), "Left id kept");
            assertTrue(colNames.contains("right_id"), "Overlapping right id prefixed");
            assertTrue(colNames.contains("score"), "Non-overlapping score not prefixed");
            assertTrue(colNames.contains("name"), "Left name kept");
            var byName = collectResults(result, "name", "score");
            assertEquals(90L, ((Number) byName.get("Alice")).longValue());
            assertEquals(85L, ((Number) byName.get("Bob")).longValue());
        }

        @Test
        @DisplayName("duplicate columns without prefix throws compile error")
        void testDuplicateColumnsWithoutPrefixThrows() {
            assertThrows(Exception.class, () -> executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                    #->join(
                        #TDS
                            id, score
                            1, 90
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.id == $r.id}
                    )"""),
                    "Duplicate 'id' column without prefix should throw");
        }
    }

    // ==================== Complex Conditions ====================

    @Nested
    @DisplayName("join() complex conditions")
    class ComplexConditions {

        @Test
        @DisplayName("join with AND condition — verifies matched values")
        void testJoinWithAndCondition() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name, dept
                        1, Alice, eng
                        2, Bob, sales
                        3, Charlie, eng
                    #->join(
                        #TDS
                            person_id, level, dept
                            1, senior, eng
                            2, junior, sales
                            3, senior, marketing
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | ($l.id == $r.person_id) && ($l.dept == $r.dept)},
                        'right'
                    )""");
            assertEquals(2, result.rowCount(),
                    "Alice(eng-eng) and Bob(sales-sales); Charlie eng≠marketing");
            var byName = collectResults(result, "name", "level");
            assertEquals("senior", byName.get("Alice"));
            assertEquals("junior", byName.get("Bob"));
            assertFalse(byName.containsKey("Charlie"), "Charlie's dept doesn't match");
        }

        @Test
        @DisplayName("join with inequality condition (non-equi join) — verifies matched values")
        void testJoinWithInequality() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, val
                        1, 10
                        2, 20
                        3, 30
                    #->join(
                        #TDS
                            threshold_id, threshold
                            1, 15
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.val > $r.threshold}
                    )""");
            assertEquals(2, result.rowCount(), "val 20 and 30 > threshold 15");
            int idIdx = columnIndex(result, "id");
            var ids = result.rows().stream()
                    .map(r -> ((Number) r.get(idIdx)).longValue())
                    .sorted().toList();
            assertEquals(2L, ids.get(0));
            assertEquals(3L, ids.get(1));
        }

        @Test
        @DisplayName("join with OR condition — matches either predicate")
        void testJoinWithOrCondition() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name, dept
                        1, Alice, eng
                        2, Bob, sales
                        3, Charlie, hr
                    #->join(
                        #TDS
                            dept_name, budget
                            eng, 100000
                            marketing, 75000
                            hr, 30000
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | ($l.dept == $r.dept_name) || ($l.dept == 'sales' && $r.dept_name == 'marketing')}
                    )""");
            // Alice: eng==eng → match (budget=100000)
            // Bob: sales==marketing via OR → match (budget=75000)
            // Charlie: hr==hr → match (budget=30000)
            assertEquals(3, result.rowCount());
            var byName = collectResults(result, "name", "budget");
            assertEquals(100000L, ((Number) byName.get("Alice")).longValue());
            assertEquals(75000L, ((Number) byName.get("Bob")).longValue());
            assertEquals(30000L, ((Number) byName.get("Charlie")).longValue());
        }
    }

    // ==================== Chained Operations ====================

    @Nested
    @DisplayName("join() chained with other operations")
    class ChainedOperations {

        @Test
        @DisplayName("join then filter — verifies filtered matched values")
        void testJoinThenFilter() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                        3, Charlie
                    #->join(
                        #TDS
                            person_id, score
                            1, 90
                            2, 60
                            3, 85
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.id == $r.person_id}
                    )->filter(x | $x.score > 80)""");
            assertEquals(2, result.rowCount(), "Alice(90) and Charlie(85) pass filter");
            var byName = collectResults(result, "name", "score");
            assertEquals(90L, ((Number) byName.get("Alice")).longValue());
            assertEquals(85L, ((Number) byName.get("Charlie")).longValue());
            assertFalse(byName.containsKey("Bob"), "Bob(60) filtered out");
        }

        @Test
        @DisplayName("join then select — verifies selected columns and values")
        void testJoinThenSelect() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                    #->join(
                        #TDS
                            person_id, score
                            1, 90
                            2, 85
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.id == $r.person_id}
                    )->select(~[name, score])""");
            assertEquals(2, result.columns().size());
            var byName = collectResults(result, "name", "score");
            assertEquals(90L, ((Number) byName.get("Alice")).longValue());
            assertEquals(85L, ((Number) byName.get("Bob")).longValue());
        }

        @Test
        @DisplayName("filter then join — only active rows participate")
        void testFilterThenJoin() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name, active
                        1, Alice, 1
                        2, Bob, 0
                        3, Charlie, 1
                    #->filter(x | $x.active == 1)
                    ->join(
                        #TDS
                            person_id, score
                            1, 90
                            2, 85
                            3, 75
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(2, result.rowCount(), "Only active Alice and Charlie join");
            var byName = collectResults(result, "name", "score");
            assertEquals(90L, ((Number) byName.get("Alice")).longValue());
            assertEquals(75L, ((Number) byName.get("Charlie")).longValue());
            assertFalse(byName.containsKey("Bob"), "Inactive Bob filtered before join");
        }

        @Test
        @DisplayName("project-join-project (PCT pattern) — verifies values through projections")
        void testProjectJoinProject() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, George
                        4, David
                    #->project(~[id1:x|$x.id, name1:x|$x.name])
                    ->join(
                        #TDS
                            id, col
                            1, MoreGeorge
                            4, MoreDavid
                        #->project(~[id2:x|$x.id, col:x|$x.col]),
                        meta::pure::functions::relation::JoinKind.INNER,
                        {x, y|$x.id1 == $y.id2}
                    )
                    ->project(~[resultId:x|$x.id1, resultCol:x|$x.col])""");
            assertEquals(2, result.rowCount());
            assertEquals(2, result.columns().size());
            var byId = collectResults(result, "resultId", "resultCol");
            assertEquals("MoreGeorge", byId.get(1));
            assertEquals("MoreDavid", byId.get(4));
        }

        @Test
        @DisplayName("join then sort — verifies sort order and values")
        void testJoinThenSort() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                        3, Charlie
                    #->join(
                        #TDS
                            person_id, score
                            1, 90
                            2, 60
                            3, 85
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.id == $r.person_id}
                    )->sort(~score->ascending())""");
            assertEquals(3, result.rowCount());
            int scoreIdx = columnIndex(result, "score");
            int nameIdx = columnIndex(result, "name");
            assertEquals(60, ((Number) result.rows().get(0).get(scoreIdx)).intValue());
            assertEquals("Bob", result.rows().get(0).get(nameIdx));
            assertEquals(85, ((Number) result.rows().get(1).get(scoreIdx)).intValue());
            assertEquals("Charlie", result.rows().get(1).get(nameIdx));
            assertEquals(90, ((Number) result.rows().get(2).get(scoreIdx)).intValue());
            assertEquals("Alice", result.rows().get(2).get(nameIdx));
        }

        @Test
        @DisplayName("join then extend — computes derived column from join result")
        void testJoinThenExtend() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name, qty
                        1, Alice, 5
                        2, Bob, 3
                    #->join(
                        #TDS
                            person_id, price
                            1, 100
                            2, 200
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.id == $r.person_id}
                    )->extend(~total: x | $x.qty * $x.price)""");
            assertEquals(2, result.rowCount());
            var byName = collectResults(result, "name", "total");
            assertEquals(500L, ((Number) byName.get("Alice")).longValue(), "Alice: 5*100=500");
            assertEquals(600L, ((Number) byName.get("Bob")).longValue(), "Bob: 3*200=600");
        }
    }

    // ==================== Edge Cases ====================

    @Nested
    @DisplayName("join() edge cases")
    class EdgeCases {

        @Test
        @DisplayName("join with single-row tables — verifies values")
        void testJoinSingleRows() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                    #->join(
                        #TDS
                            person_id, score
                            1, 100
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(1, result.rowCount());
            int nameIdx = columnIndex(result, "name");
            int scoreIdx = columnIndex(result, "score");
            assertEquals("Alice", result.rows().get(0).get(nameIdx));
            assertEquals(100L, ((Number) result.rows().get(0).get(scoreIdx)).longValue());
        }

        @Test
        @DisplayName("join where left is empty")
        void testJoinEmptyLeft() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                    #->join(
                        #TDS
                            person_id, score
                            1, 100
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertTrue(result.rows().isEmpty(), "Empty left → empty inner join");
        }

        @Test
        @DisplayName("join where right is empty")
        void testJoinEmptyRight() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                    #->join(
                        #TDS
                            person_id, score
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertTrue(result.rows().isEmpty(), "Empty right → empty inner join");
        }

        @Test
        @DisplayName("left outer join with empty right — preserves left with NULLs")
        void testLeftJoinEmptyRight() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                    #->join(
                        #TDS
                            person_id, score
                        #,
                        meta::pure::functions::relation::JoinKind.LEFT_OUTER,
                        {l, r | $l.id == $r.person_id}
                    )""");
            assertEquals(2, result.rowCount(), "Both left rows preserved with NULLs");
            int scoreIdx = columnIndex(result, "score");
            for (var row : result.rows()) {
                assertNull(row.get(scoreIdx), "Empty right → score is NULL");
            }
        }

        @Test
        @DisplayName("join with many-to-many produces cartesian — verifies all combos")
        void testJoinManyToMany() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        grp, name
                        1, Alice
                        1, Bob
                    #->join(
                        #TDS
                            grp, score
                            1, 90
                            1, 85
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.grp == $r.grp},
                        'right'
                    )""");
            assertEquals(4, result.rowCount(), "2×2 = 4 rows (cartesian on grp=1)");
            int nameIdx = columnIndex(result, "name");
            // Each name should appear with each score
            long aliceCount = result.rows().stream()
                    .filter(r -> "Alice".equals(r.get(nameIdx))).count();
            long bobCount = result.rows().stream()
                    .filter(r -> "Bob".equals(r.get(nameIdx))).count();
            assertEquals(2, aliceCount, "Alice appears with both scores");
            assertEquals(2, bobCount, "Bob appears with both scores");
        }

        @Test
        @DisplayName("join with custom lambda param names")
        void testJoinCustomParamNames() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, name
                        1, Alice
                        2, Bob
                    #->join(
                        #TDS
                            person_id, score
                            1, 90
                            2, 85
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {left, right | $left.id == $right.person_id}
                    )""");
            assertEquals(2, result.rowCount());
            var byName = collectResults(result, "name", "score");
            assertEquals(90L, ((Number) byName.get("Alice")).longValue());
            assertEquals(85L, ((Number) byName.get("Bob")).longValue());
        }

        @Test
        @DisplayName("join with string equality condition — verifies values")
        void testJoinStringCondition() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        name, dept
                        Alice, eng
                        Bob, sales
                    #->join(
                        #TDS
                            dept_name, budget
                            eng, 100000
                            sales, 50000
                            marketing, 75000
                        #,
                        meta::pure::functions::relation::JoinKind.INNER,
                        {l, r | $l.dept == $r.dept_name}
                    )""");
            assertEquals(2, result.rowCount(), "String equality join works");
            var byName = collectResults(result, "name", "budget");
            assertEquals(100000L, ((Number) byName.get("Alice")).longValue());
            assertEquals(50000L, ((Number) byName.get("Bob")).longValue());
        }
    }

    // ==================== Utilities ====================

    /** Collects results into a map keyed by the specified column value. */
    private <K> Map<K, Object> collectResults(
            ExecutionResult result, String keyCol, String valCol) {
        int keyIdx = columnIndex(result, keyCol);
        int valIdx = columnIndex(result, valCol);
        Map<K, Object> map = new HashMap<>();
        for (var row : result.rows()) {
            @SuppressWarnings("unchecked")
            K key = (K) row.get(keyIdx);
            map.put(key, row.get(valIdx));
        }
        return map;
    }

    /** Finds column index by name. */
    private int columnIndex(ExecutionResult result, String name) {
        for (int i = 0; i < result.columns().size(); i++) {
            if (name.equals(result.columns().get(i).name()))
                return i;
        }
        throw new AssertionError("Column '" + name + "' not found in " +
                result.columns().stream().map(c -> c.name()).toList());
    }
}
