package org.finos.legend.engine.test;

import org.finos.legend.engine.server.QueryService;
import org.finos.legend.pure.dsl.definition.*;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for TestSuiteRunner.
 * 
 * Tests the actual execution of testSuites against a database.
 * Parser-only tests are in MappingTestSuiteParserTest.
 */
@DisplayName("Test Suite Runner Integration Tests")
class MappingTestIntegrationTest {

    private Connection connection;
    private QueryService queryService;

    private static final String RELATIONAL_MODEL = """
            Class model::Person
            {
                firstName: String[1];
                lastName: String[1];
                age: Integer[1];
            }

            Database store::PersonDatabase
            (
                Table T_PERSON
                (
                    ID INTEGER PRIMARY KEY,
                    FIRST_NAME VARCHAR(100) NOT NULL,
                    LAST_NAME VARCHAR(100) NOT NULL,
                    AGE INTEGER NOT NULL
                )
            )

            Mapping model::PersonMapping
            (
                Person: Relational
                {
                    ~mainTable [PersonDatabase] T_PERSON
                    firstName: [PersonDatabase] T_PERSON.FIRST_NAME,
                    lastName: [PersonDatabase] T_PERSON.LAST_NAME,
                    age: [PersonDatabase] T_PERSON.AGE
                }

                testSuites:
                [
                    PersonSuite:
                    {
                        function: |Person.all()->project({p | $p.firstName}, {p | $p.lastName});
                        tests:
                        [
                            TestAllPersons:
                            {
                                doc: 'Verify all persons are returned';
                                data:
                                [
                                    ModelStore:
                                        ExternalFormat
                                        #{
                                            contentType: 'application/json';
                                            data: '[{"firstName":"John","lastName":"Doe"},{"firstName":"Jane","lastName":"Smith"}]';
                                        }#
                                ];
                                asserts:
                                [
                                    expectedPersons:
                                        EqualToJson
                                        #{
                                            expected:
                                                ExternalFormat
                                                #{
                                                    contentType: 'application/json';
                                                    data: '[{"firstName":"John","lastName":"Doe"},{"firstName":"Jane","lastName":"Smith"}]';
                                                }#;
                                        }#
                                ];
                            }
                        ];
                    }
                ]
            )

            RelationalDatabaseConnection store::TestConnection
            {
                type: DuckDB;
                specification: InMemory { };
                auth: NoAuth { };
            }

            Runtime test::TestRuntime
            {
                mappings: [ model::PersonMapping ];
                connections: [ store::PersonDatabase: store::TestConnection ];
            }
            """;

    @BeforeEach
    void setUp() throws Exception {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        queryService = new QueryService();

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                        CREATE TABLE T_PERSON (
                            ID INTEGER PRIMARY KEY,
                            FIRST_NAME VARCHAR(100),
                            LAST_NAME VARCHAR(100),
                            AGE INTEGER
                        )
                    """);
            stmt.execute("INSERT INTO T_PERSON VALUES (1, 'John', 'Doe', 30)");
            stmt.execute("INSERT INTO T_PERSON VALUES (2, 'Jane', 'Smith', 25)");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    @DisplayName("Execute mapping testSuite using TestSuiteRunner")
    void testExecuteRelationalMappingWithRunner() throws Exception {
        String mappingOnly = RELATIONAL_MODEL.substring(
                RELATIONAL_MODEL.indexOf("Mapping model::PersonMapping"),
                RELATIONAL_MODEL.indexOf("RelationalDatabaseConnection")).trim();

        MappingDefinition mapping = PureDefinitionParser.parseMappingDefinition(mappingOnly);

        // Create and run the test runner
        TestSuiteRunner runner = new TestSuiteRunner(
                queryService, connection, RELATIONAL_MODEL, "test::TestRuntime");

        var results = runner.runAllTestSuites(mapping.testSuites());

        // Verify results
        assertEquals(1, results.size());

        var suiteResult = results.get(0);
        assertEquals("PersonSuite", suiteResult.suiteName());
        assertEquals(1, suiteResult.testResults().size());

        // Verify test executed and got actual data
        var testResult = suiteResult.testResults().get(0);
        assertEquals("TestAllPersons", testResult.testName());
        assertNotNull(testResult.actualJson(), "Expected actual JSON result from execution");
        assertTrue(testResult.actualJson().contains("John"), "Result should contain John");
        assertTrue(testResult.actualJson().contains("Doe"), "Result should contain Doe");
        assertTrue(testResult.actualJson().contains("Jane"), "Result should contain Jane");
        assertTrue(testResult.actualJson().contains("Smith"), "Result should contain Smith");
    }

    @Test
    @DisplayName("Verify TestSuiteResult API")
    void testSuiteResultApi() throws Exception {
        String mappingOnly = RELATIONAL_MODEL.substring(
                RELATIONAL_MODEL.indexOf("Mapping model::PersonMapping"),
                RELATIONAL_MODEL.indexOf("RelationalDatabaseConnection")).trim();

        MappingDefinition mapping = PureDefinitionParser.parseMappingDefinition(mappingOnly);

        TestSuiteRunner runner = new TestSuiteRunner(
                queryService, connection, RELATIONAL_MODEL, "test::TestRuntime");

        var results = runner.runAllTestSuites(mapping.testSuites());
        var suiteResult = results.get(0);

        // Test API methods
        assertEquals(1, suiteResult.testResults().size());
        assertEquals(1, suiteResult.passedCount() + suiteResult.failedCount());
    }
}
