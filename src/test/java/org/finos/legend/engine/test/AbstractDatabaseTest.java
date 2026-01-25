package org.finos.legend.engine.test;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.plan.ExecutionPlan;
import org.finos.legend.engine.server.QueryService;
import org.finos.legend.engine.store.MappingRegistry;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.pure.dsl.PureCompiler;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for database integration tests.
 * Provides common setup, teardown, and utility methods.
 * 
 * Uses Pure syntax for defining Class, Database, and Mapping.
 */
public abstract class AbstractDatabaseTest {

    protected Connection connection;
    protected SQLGenerator sqlGenerator;
    protected MappingRegistry mappingRegistry;
    protected PureCompiler pureCompiler;
    protected PureModelBuilder modelBuilder;
    protected QueryService queryService = new QueryService();

    // ==================== Pure Definitions ====================

    /**
     * Pure Class definition for Person.
     */
    protected static final String PERSON_CLASS = """
            Class model::Person
            {
                firstName: String[1];
                lastName: String[1];
                age: Integer[1];
            }
            """;

    /**
     * Pure Class definition for Address.
     */
    protected static final String ADDRESS_CLASS = """
            Class model::Address
            {
                street: String[1];
                city: String[1];
            }
            """;

    /**
     * Pure Association linking Person to Address (1:*).
     */
    protected static final String PERSON_ADDRESS_ASSOCIATION = """
            Association model::Person_Address
            {
                person: Person[1];
                addresses: Address[*];
            }
            """;

    /**
     * Pure Database definition with T_PERSON and T_ADDRESS tables.
     * Note: Uses ###Relational section header for section-aware parsing.
     */
    protected static final String PERSON_DATABASE = """
            ###Relational
            Database store::PersonDatabase
            (
                Table T_PERSON
                (
                    ID INTEGER PRIMARY KEY,
                    FIRST_NAME VARCHAR(100) NOT NULL,
                    LAST_NAME VARCHAR(100) NOT NULL,
                    AGE_VAL INTEGER NOT NULL
                )
                Table T_ADDRESS
                (
                    ID INTEGER PRIMARY KEY,
                    PERSON_ID INTEGER NOT NULL,
                    STREET VARCHAR(200) NOT NULL,
                    CITY VARCHAR(100) NOT NULL
                )
                Join Person_Address(T_PERSON.ID = T_ADDRESS.PERSON_ID)
            )
            """;

    /**
     * Pure Mapping definition from Person to T_PERSON.
     * Note: Uses ###Mapping section header for section-aware parsing.
     */
    protected static final String PERSON_MAPPING = """
            ###Mapping
            Mapping model::PersonMapping
            (
                Person: Relational
                {
                    ~mainTable [PersonDatabase] T_PERSON
                    firstName: [PersonDatabase] T_PERSON.FIRST_NAME,
                    lastName: [PersonDatabase] T_PERSON.LAST_NAME,
                    age: [PersonDatabase] T_PERSON.AGE_VAL
                }

                Address: Relational
                {
                    ~mainTable [PersonDatabase] T_ADDRESS
                    street: [PersonDatabase] T_ADDRESS.STREET,
                    city: [PersonDatabase] T_ADDRESS.CITY
                }
            )
            """;

    /**
     * Complete Pure model (Classes + Association + Database + Mapping).
     * Each element type uses its own section header for section-aware parsing.
     */
    protected static final String COMPLETE_PURE_MODEL = "###Pure\n" + PERSON_CLASS + "\n" +
            ADDRESS_CLASS + "\n" +
            PERSON_ADDRESS_ASSOCIATION + "\n" +
            PERSON_DATABASE + "\n" + // Already has ###Relational header
            PERSON_MAPPING; // Already has ###Mapping header

    // ==================== Connection/Runtime Definitions (Abstract)
    // ====================

    /**
     * @return The database type string for this test ("DuckDB" or "SQLite")
     */
    protected abstract String getDatabaseType();

    /**
     * Builds the Connection Pure definition for this database.
     * Uses ###Connection section header.
     */
    protected String getConnectionDefinition() {
        return """
                ###Connection
                RelationalDatabaseConnection store::TestConnection
                {
                    type: %s;
                    specification: InMemory { };
                    auth: NoAuth { };
                }
                """.formatted(getDatabaseType());
    }

    /**
     * Builds the Runtime Pure definition.
     * Uses ###Runtime section header.
     * 
     * Note: Legend Engine requires nested bracket syntax for connections:
     * connections: [ storeName: [ connectionId: connectionRef ] ];
     */
    protected String getRuntimeDefinition() {
        return """
                ###Runtime
                Runtime test::TestRuntime
                {
                    mappings: [ model::PersonMapping ];
                    connections: [
                        store::PersonDatabase: [
                            conn1: store::TestConnection
                        ]
                    ];
                }
                """;
    }

    /**
     * Complete Pure model including Connection and Runtime.
     */
    protected String getCompletePureModelWithRuntime() {
        return COMPLETE_PURE_MODEL + "\n" +
                getConnectionDefinition() + "\n" +
                getRuntimeDefinition();
    }

    // ==================== Pure Model Setup ====================

    /**
     * Sets up the model from Pure syntax definitions.
     * This parses the Pure source and creates all metamodel objects.
     * 
     * The compiler is configured with the ModelContext to support
     * association navigation (e.g., $p.addresses.street) and automatic
     * EXISTS generation for to-many filters.
     */
    protected void setupPureModel() {
        modelBuilder = new PureModelBuilder()
                .addSource(COMPLETE_PURE_MODEL);

        mappingRegistry = modelBuilder.getMappingRegistry();
        // Pass modelBuilder as ModelContext for association navigation support
        pureCompiler = new PureCompiler(mappingRegistry, modelBuilder);
    }

    /**
     * Sets up the mapping registry using Pure syntax.
     * 
     * @see #setupPureModel()
     */
    protected void setupMappingRegistry() {
        setupPureModel();
    }

    /**
     * Compiles a Pure query and generates SQL using QueryService.
     * Uses the full model with Runtime to get dialect-specific SQL.
     * 
     * @param pureQuery The Pure query string
     * @return The generated SQL
     */
    protected String generateSql(String pureQuery) {
        ExecutionPlan plan = queryService.compile(
                getCompletePureModelWithRuntime(),
                pureQuery,
                "test::TestRuntime");

        // Get SQL for the first store in the plan
        return plan.sqlByStore().values().iterator().next().sql();
    }

    /**
     * Executes a Pure query and returns results.
     * Uses QueryService for full compile → plan → execute flow.
     * 
     * @param pureQuery The Pure query string
     * @return List of PersonResult
     */
    protected List<PersonResult> executePureQuery(String pureQuery) throws SQLException {
        BufferedResult result = executeRelation(pureQuery);
        return convertToPersonResults(result);
    }

    /**
     * Converts BufferedResult to PersonResult list.
     */
    private List<PersonResult> convertToPersonResults(BufferedResult result) {
        List<PersonResult> personResults = new ArrayList<>();

        for (var row : result.rows()) {
            String firstName = null;
            String lastName = null;
            Integer age = null;

            for (int i = 0; i < result.columns().size(); i++) {
                String colName = result.columns().get(i).name();
                Object value = row.get(i);

                switch (colName) {
                    case "firstName" -> firstName = value != null ? value.toString() : null;
                    case "lastName" -> lastName = value != null ? value.toString() : null;
                    case "age" -> age = value != null ? ((Number) value).intValue() : null;
                }
            }

            personResults.add(new PersonResult(firstName, lastName, age));
        }

        return personResults;
    }

    /**
     * Executes a Pure query and returns relation results (tabular data).
     * Uses QueryService for complete compile → plan → execute flow.
     * 
     * @param pureQuery The Pure query to execute
     * @return BufferedResult containing column metadata and rows
     */
    protected BufferedResult executeRelation(String pureQuery) throws SQLException {
        return queryService.execute(
                getCompletePureModelWithRuntime(),
                pureQuery,
                "test::TestRuntime",
                connection);
    }

    // ==================== SQL Dialect ====================

    /**
     * @return The SQL dialect for this test database
     */
    protected abstract SQLDialect getDialect();

    /**
     * @return The JDBC URL for the in-memory database
     */
    protected abstract String getJdbcUrl();

    // ==================== Database Setup ====================

    /**
     * Creates the test tables and inserts sample data.
     */
    protected void setupDatabase() throws SQLException {
        // Create the T_PERSON table
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                    CREATE TABLE T_PERSON (
                        ID INTEGER PRIMARY KEY,
                        FIRST_NAME VARCHAR(100) NOT NULL,
                        LAST_NAME VARCHAR(100) NOT NULL,
                        AGE_VAL INTEGER NOT NULL
                    )
                    """);
        }

        // Create the T_ADDRESS table
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                    CREATE TABLE T_ADDRESS (
                        ID INTEGER PRIMARY KEY,
                        PERSON_ID INTEGER NOT NULL,
                        STREET VARCHAR(200) NOT NULL,
                        CITY VARCHAR(100) NOT NULL
                    )
                    """);
        }

        // Insert person test data
        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO T_PERSON (ID, FIRST_NAME, LAST_NAME, AGE_VAL) VALUES (?, ?, ?, ?)")) {

            insertPerson(ps, 1, "John", "Smith", 30);
            insertPerson(ps, 2, "Jane", "Smith", 28);
            insertPerson(ps, 3, "Bob", "Jones", 45);
        }

        // Insert address test data
        // John has 2 addresses (123 Main St, 456 Oak Ave)
        // Jane has 1 address (789 Main Rd)
        // Bob has 1 address (999 Pine Lane - no 'Main' in it)
        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO T_ADDRESS (ID, PERSON_ID, STREET, CITY) VALUES (?, ?, ?, ?)")) {

            insertAddress(ps, 1, 1, "123 Main St", "New York");
            insertAddress(ps, 2, 1, "456 Oak Ave", "Boston");
            insertAddress(ps, 3, 2, "789 Main Rd", "Chicago");
            insertAddress(ps, 4, 3, "999 Pine Lane", "Detroit");
        }
    }

    private void insertPerson(PreparedStatement ps, int id, String firstName, String lastName, int age)
            throws SQLException {
        ps.setInt(1, id);
        ps.setString(2, firstName);
        ps.setString(3, lastName);
        ps.setInt(4, age);
        ps.executeUpdate();
    }

    private void insertAddress(PreparedStatement ps, int id, int personId, String street, String city)
            throws SQLException {
        ps.setInt(1, id);
        ps.setInt(2, personId);
        ps.setString(3, street);
        ps.setString(4, city);
        ps.executeUpdate();
    }

    /**
     * Simple record to hold person query results.
     */
    public record PersonResult(String firstName, String lastName, Integer age) {
    }
}
