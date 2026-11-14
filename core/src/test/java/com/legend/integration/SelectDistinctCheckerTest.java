package com.legend.integration;

import com.legend.testing.Lean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SelectChecker and DistinctChecker.
 *
 * <p>Covers select (column subset) and distinct (dedup) operations on TDS and class-based sources.
 */
public class SelectDistinctCheckerTest extends AbstractDatabaseTest {

    @BeforeEach
    void setUp() throws SQLException {
        connection = backend().open();
        setupDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) connection.close();
    }

    // ==================== Select ====================

    @Nested
    @DisplayName("select — column subset")
    class SelectTests {

        @Test
        @DisplayName("select single column from TDS")
        void testSelectSingle() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name, score
                    1, Alice, 95
                    2, Bob, 87
                    #->select(~name)->sort(ascending(~name))""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            assertEquals("Alice", result.rows().get(0).get(0));
            assertEquals("Bob", result.rows().get(1).get(0));
        }

        @Test
        @DisplayName("select multiple columns from TDS")
        void testSelectMultiple() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name, score
                    1, Alice, 95
                    2, Bob, 87
                    #->select(~[id, name])->sort(ascending(~id))""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            assertEquals(1L, result.rows().get(0).get(0));
            assertEquals("Alice", result.rows().get(0).get(1));
        }

        @Test
        @DisplayName("select after project on class-based source")
        void testSelectAfterProject() throws SQLException {
            var result = executeRelation("""
                    model::Person.all()
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->select(~name)
                      ->sort(ascending(~name))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            var names = result.rows().stream().map(r -> r.get(0)).toList();
            assertEquals(List.of("Bob", "Jane", "John"), names);
        }

        @Test
        @DisplayName("select non-existent column throws")
        void testSelectNonExistent() {
            assertThrows(Exception.class, () ->
                    executeRelation("""
                            |#TDS
                            id, name
                            1, Alice
                            #->select(~nonexistent)"""));
        }
    }

    // ==================== Distinct ====================

    @Nested
    @DisplayName("distinct — deduplication")
    class DistinctTests {

        @Test
        @DisplayName("distinct removes duplicates")
        void testDistinctSimple() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    city
                    New York
                    Chicago
                    New York
                    Chicago
                    Boston
                    #->distinct()->sort(ascending(~city))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            assertEquals("Boston", result.rows().get(0).get(0));
            assertEquals("Chicago", result.rows().get(1).get(0));
            assertEquals("New York", result.rows().get(2).get(0));
        }

        @Test
        @DisplayName("distinct on specific columns")
        void testDistinctColumns() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    dept, name
                    Eng, Alice
                    Eng, Bob
                    Sales, Alice
                    Sales, Charlie
                    #->distinct(~dept)->sort(ascending(~dept))""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            assertEquals("Eng", result.rows().get(0).get(0));
            assertEquals("Sales", result.rows().get(1).get(0));
        }

        @Test
        @DisplayName("distinct on class-based source after project")
        void testDistinctClassBased() throws SQLException {
            var result = executeRelation("""
                    model::Person.all()
                      ->project(~[age:p|$p.age])
                      ->distinct()
                      ->sort(ascending(~age))""");
            assertNotNull(result);
            // 3 unique ages: 28, 30, 45
            assertEquals(3, result.rows().size());
            assertEquals(28L, result.rows().get(0).get(0));
            assertEquals(30L, result.rows().get(1).get(0));
            assertEquals(45L, result.rows().get(2).get(0));
        }
    }

    // ==================== Without (lite surface, docs/SEMANTICS_REGISTER.md S113) ====================

    @Nested
    @DisplayName("without — the relation less the named columns")
    class WithoutTests {

        private static final String SCORES = """
                |#TDS
                id, name, score, team
                1, Alice, 95, red
                2, Bob, 87, blue
                #""";

        private List<String> names(Lean.Result result) {
            return result.columns().stream().map(c -> c.name()).toList();
        }

        @Test
        @DisplayName("one column dropped: the others, in the relation's order")
        void oneColumn() throws SQLException {
            var result = executeRelation(SCORES + "->without(~score)->sort(ascending(~id))");
            assertEquals(List.of("id", "name", "team"), names(result));
            assertEquals(List.of(1L, "Alice", "red"), result.rows().get(0));
        }

        @Test
        @DisplayName("several columns dropped")
        void severalColumns() throws SQLException {
            var result = executeRelation(SCORES + "->without(~[name, score])->sort(ascending(~id))");
            assertEquals(List.of("id", "team"), names(result));
            assertEquals(List.of(2L, "blue"), result.rows().get(1));
        }

        @Test
        @DisplayName("a column made, used and dropped: the relation as it came")
        void madeUsedAndDropped() throws SQLException {
            var result = executeRelation(SCORES
                    + "->extend(~twice:x|$x.score * 2)->filter(x|$x.twice > 180)->without(~twice)");
            assertEquals(List.of("id", "name", "score", "team"), names(result));
            assertEquals(1, result.rows().size());
            assertEquals("Alice", result.rows().get(0).get(1));
        }

        @Test
        @DisplayName("its SQL is the select of the columns kept")
        void theSelectOfTheRest() {
            String people = "model::Person.all()->project(~[name:p|$p.firstName, age:p|$p.age, last:p|$p.lastName])";
            assertEquals(generateSql(people + "->select(~[name, last])"), generateSql(people + "->without(~age)"));
        }

        @Test
        @DisplayName("a dropped column is gone from the type: a later step that reads it is refused by name")
        void droppedIsGone() {
            var ex = assertThrows(Exception.class, () -> executeRelation(SCORES + "->without(~score)->select(~score)"));
            assertTrue(ex.getMessage().contains("score"), ex.getMessage());
        }

        @Test
        @DisplayName("a column the relation has not: refused by name")
        void columnItHasNot() {
            var ex = assertThrows(Exception.class, () -> executeRelation(SCORES + "->without(~nope)"));
            assertTrue(ex.getMessage().contains("nope"), ex.getMessage());
        }

        @Test
        @DisplayName("every column dropped: refused, a relation keeps one")
        void everyColumn() {
            var ex = assertThrows(Exception.class,
                    () -> executeRelation(SCORES + "->without(~[id, name, score, team])"));
            assertTrue(ex.getMessage().contains("drops every column"), ex.getMessage());
        }
    }

    // ==================== Select + Distinct chains ====================

    @Nested
    @DisplayName("select/distinct chains")
    class Chains {

        @Test
        @DisplayName("project→select→distinct→sort")
        void testProjectSelectDistinctSort() throws SQLException {
            var result = executeRelation("""
                    model::Person.all()
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->select(~age)
                      ->distinct()
                      ->sort(ascending(~age))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size()); // 28, 30, 45
        }
    }
}
