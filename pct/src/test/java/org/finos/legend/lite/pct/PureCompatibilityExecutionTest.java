package org.finos.legend.lite.pct;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Disabled;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for PureCompatibilityExecution.
 * 
 * These tests validate that we can execute Pure code through legend-lite
 * and get correct results back - the foundation for PCT testing.
 * 
 * NOTE: Currently using legend-lite's grammar (which differs from
 * legend-engine).
 * PCT will help identify and track these grammar gaps.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PureCompatibilityExecutionTest {

    private PureCompatibilityExecution execution;

    @BeforeAll
    void setUp() {
        execution = new PureCompatibilityExecution();
    }

    /**
     * Using legend-lite's current grammar (differs from legend-engine).
     * This is a KNOWN gap that PCT will help track.
     */
    private static final String PURE_SOURCE = """
            Class model::Person {
                firstName: String[1];
                lastName: String[1];
                age: Integer[1];
            }

            Database TestDatabase (
                Table T_PERSON (
                    FIRST_NAME VARCHAR(100),
                    LAST_NAME VARCHAR(100),
                    AGE_VAL INTEGER
                )
            )

            Mapping model::PersonMapping (
                Person: Relational {
                    ~mainTable [TestDatabase] T_PERSON
                    firstName: [TestDatabase] T_PERSON.FIRST_NAME,
                    lastName: [TestDatabase] T_PERSON.LAST_NAME,
                    age: [TestDatabase] T_PERSON.AGE_VAL
                }
            )

            RelationalDatabaseConnection store::TestConnection {
                type: DuckDB;
                specification: InMemory { };
            }

            Runtime test::TestRuntime {
                mappings:
                [
                    model::PersonMapping
                ];
                connections:
                [
                    TestDatabase:
                    [
                        environment: store::TestConnection
                    ]
                ];
            }
            """;

    @Test
    void testCompileSimpleQuery() {
        String query = "model::Person.all()->project([x|$x.firstName, x|$x.lastName], ['First Name', 'Last Name'])";

        boolean compiled = execution.compile(PURE_SOURCE, query, "test::TestRuntime");
        assertTrue(compiled, "Should compile successfully");
    }

    @Test
    void testCompileFilterQuery() {
        String query = "model::Person.all()->filter(p|$p.age > 25)->project([x|$x.firstName], ['name'])";

        boolean compiled = execution.compile(PURE_SOURCE, query, "test::TestRuntime");
        assertTrue(compiled, "Filter query should compile");
    }

    @Test
    @Disabled("groupBy syntax differs from legend-engine - grammar compatibility gap")
    void testCompileAggregateQuery() {
        String query = "model::Person.all()->groupBy([x|$x.lastName], agg(x|$x.age, y|$y->sum()), ['lastName', 'totalAge'])";

        boolean compiled = execution.compile(PURE_SOURCE, query, "test::TestRuntime");
        assertTrue(compiled, "Aggregate query should compile");
    }

    @Test
    void testCompileFailsForInvalidSyntax() {
        String invalidSource = "Class Invalid { missing syntax";
        String query = "Invalid.all()";

        boolean compiled = execution.compile(invalidSource, query, "test::TestRuntime");
        assertFalse(compiled, "Should fail to compile invalid Pure source");
    }

    @Test
    void testExecutionReturnsErrorForInvalidSource() {
        String invalidSource = "Class Invalid { missing syntax";
        String query = "Invalid.all()";

        PureCompatibilityExecution.ExecutionResult result = execution.execute(invalidSource, query,
                "test::TestRuntime");

        assertFalse(result.isSuccess(), "Should fail for invalid Pure source");
        assertNotNull(result.getError(), "Should have error message");
    }
}
