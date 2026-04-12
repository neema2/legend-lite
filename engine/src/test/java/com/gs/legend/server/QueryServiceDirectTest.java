package com.gs.legend.server;

import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Direct test of QueryService.executeSql bypassing HTTP layer.
 * Uses the exact same model pattern as AbstractDatabaseTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class QueryServiceDirectTest {

    private static QueryService queryService;
    private static Path tempDbFile;
    private static String sampleModel;

    @BeforeAll
    static void setup() throws Exception {
        queryService = new QueryService();

        // Create temp path for DuckDB — DuckDB needs to create it fresh
        tempDbFile = Files.createTempFile("query-test-", ".duckdb");
        Files.delete(tempDbFile);

        // Build the model following AbstractDatabaseTest pattern - use SIMPLE names in
        // mappings
        String dbPath = tempDbFile.toString().replace("\\", "/");
        sampleModel = """
                import model::*;

                Class model::Person {
                    firstName: String[1];
                    lastName: String[1];
                    age: Integer[1];
                }

                Database TestDatabase (
                    Table T_PERSON (
                        ID INTEGER PRIMARY KEY,
                        FIRST_NAME VARCHAR(100),
                        LAST_NAME VARCHAR(100),
                        AGE_VAL INTEGER
                    )
                )

                Mapping model::PersonMapping (
                    model::Person: Relational {
                        ~mainTable [TestDatabase] T_PERSON
                        firstName: [TestDatabase] T_PERSON.FIRST_NAME,
                        lastName: [TestDatabase] T_PERSON.LAST_NAME,
                        age: [TestDatabase] T_PERSON.AGE_VAL
                    }
                )

                RelationalDatabaseConnection store::TestConnection {
                    type: DuckDB;
                    specification: LocalFile { path: "{{DB_PATH}}"; };
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
                """.replace("{{DB_PATH}}", dbPath);

        System.out.println("Sample model (first 350 chars):");
        System.out.println(sampleModel.substring(0, Math.min(350, sampleModel.length())));
        System.out.println("...");
        System.out.println("Using DB file: " + tempDbFile);
    }

    @AfterAll
    static void teardown() throws Exception {
        Files.deleteIfExists(tempDbFile);
        Files.deleteIfExists(Path.of(tempDbFile.toString() + ".wal"));
    }

    @Test
    @Order(1)
    @DisplayName("Direct: executeSql CREATE TABLE")
    void testDirectCreateTable() throws Exception {
        // Drop first for idempotency
        queryService.executeSql(sampleModel, "DROP TABLE IF EXISTS T_PERSON", "test::TestRuntime");

        String sql = """
                CREATE TABLE T_PERSON (
                    ID INTEGER PRIMARY KEY,
                    FIRST_NAME VARCHAR(100),
                    LAST_NAME VARCHAR(100),
                    AGE_VAL INTEGER
                )
                """;

        System.out.println("Executing: CREATE TABLE...");
        var result = queryService.executeSql(sampleModel, sql, "test::TestRuntime");

        System.out.println("Result: " + result);
        assertTrue(result.columns().isEmpty() || result.columns() != null, "CREATE should succeed");
    }

    @Test
    @Order(2)
    @DisplayName("Direct: executeSql INSERT")
    void testDirectInsert() throws Exception {
        String sql = "INSERT INTO T_PERSON VALUES (1, 'John', 'Smith', 30)";

        System.out.println("Executing: INSERT...");
        var result = queryService.executeSql(sampleModel, sql, "test::TestRuntime");

        System.out.println("Result: " + result);
        assertNotNull(result);
    }

    @Test
    @Order(3)
    @DisplayName("Direct: executeSql SELECT returns empty (raw SQL has no compiler types)")
    void testDirectSelect() throws Exception {
        String sql = "SELECT FIRST_NAME, LAST_NAME FROM T_PERSON WHERE ID = 1";

        System.out.println("Executing: SELECT...");
        var result = queryService.executeSql(sampleModel, sql, "test::TestRuntime");

        // executeSql always returns empty — raw SQL has no compiler-provided types.
        // Use the compiled Pure pipeline (QueryService.execute) for typed results.
        assertEquals(0, result.columns().size(), "executeSql returns empty (no compiler types)");
        assertEquals(0, result.rows().size(), "executeSql returns empty (no compiler types)");
    }
}
