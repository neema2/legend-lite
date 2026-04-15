package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.model.PureModelBuilder;
import com.gs.legend.model.mapping.MappingRegistry;
import com.gs.legend.server.QueryService;
import com.gs.legend.sqlgen.SQLDialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
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
    protected MappingRegistry mappingRegistry;
    protected PureModelBuilder modelBuilder;
    protected QueryService queryService = new QueryService();

    // ==================== Pure Definitions ====================

    /**
     * Pure Class definition for Person.
     */
    protected static final String PERSON_CLASS = """
            import model::*;

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
            import model::*;

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
            import model::*;

            Association model::Person_Address
            {
                person: Person[1];
                addresses: Address[*];
            }
            """;

    /**
     * Pure Association linking Person to primary Address (to-one).
     */
    protected static final String PERSON_PRIMARY_ADDRESS_ASSOCIATION = """
            import model::*;

            Association model::Person_PrimaryAddress
            {
                personPrimary: Person[1];
                primaryAddress: Address[0..1];
            }
            """;

    /**
     * Pure Database definition with T_PERSON and T_ADDRESS tables.
     */
    protected static final String PERSON_DATABASE = """
            import store::*;

            Database store::PersonDatabase
            (
                Table T_PERSON
                (
                    ID INTEGER PRIMARY KEY,
                    FIRST_NAME VARCHAR(100) NOT NULL,
                    LAST_NAME VARCHAR(100) NOT NULL,
                    AGE_VAL INTEGER NOT NULL,
                    PRIMARY_ADDR_ID INTEGER
                )
                Table T_ADDRESS
                (
                    ID INTEGER PRIMARY KEY,
                    PERSON_ID INTEGER NOT NULL,
                    STREET VARCHAR(200) NOT NULL,
                    CITY VARCHAR(100) NOT NULL
                )
                Join Person_Address(T_PERSON.ID = T_ADDRESS.PERSON_ID)
                Join Person_PrimaryAddress(T_PERSON.PRIMARY_ADDR_ID = T_ADDRESS.ID)
            )
            """;

    /**
     * Pure Mapping definition from Person to T_PERSON.
     */
    protected static final String PERSON_MAPPING = """
            import model::*;

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

                model::Person_Address: Relational { AssociationMapping ( person: [store::PersonDatabase]@Person_Address, addresses: [store::PersonDatabase]@Person_Address ) }
                model::Person_PrimaryAddress: Relational { AssociationMapping ( personPrimary: [store::PersonDatabase]@Person_PrimaryAddress, primaryAddress: [store::PersonDatabase]@Person_PrimaryAddress ) }
            )
            """;

    /**
     * Pure Class definitions for struct array tests.
     */
    protected static final String STRUCT_TEST_CLASSES = """
            import test::*;

            Class test::StructAddress
            {
                val: String[1];
            }

            Class test::StructValue
            {
                val: Integer[1];
            }

            Class test::TypeForProjectTest
            {
                name: String[1];
                addresses: test::StructAddress[*];
                values: test::StructValue[*];
            }
            """;

    /**
     * PCT test class definitions used by TypeInference struct literal tests.
     * These were previously injected via TypeEnvironment side-channel.
     */
    protected static final String PCT_CLASS_DEFS = """
            import meta::pure::functions::boolean::tests::equalitymodel::*;
            import meta::pure::functions::collection::tests::contains::*;
            import meta::pure::functions::collection::tests::map::model::*;
            import meta::pure::functions::collection::tests::model::*;
            import meta::pure::functions::lang::tests::model::*;
            import meta::pure::functions::string::tests::plus::model::*;

            Class meta::pure::functions::collection::tests::model::CO_Person
            {
                firstName: String[1];
                lastName: String[1];
            }

            Class meta::pure::functions::collection::tests::model::CO_Firm
            {
                legalName: String[1];
                employees: meta::pure::functions::collection::tests::model::CO_Person[*];
            }

            Class meta::pure::functions::collection::tests::map::model::M_Person
            {
                firstName: String[1];
                lastName: String[1];
            }

            Class meta::pure::functions::collection::tests::contains::ClassWithoutEquality
            {
                name: String[1];
            }

            Class meta::pure::functions::lang::tests::model::LA_Person
            {
                firstName: String[1];
                lastName: String[1];
            }

            Class meta::pure::functions::string::tests::plus::model::P_Person
            {
                firstName: String[1];
                lastName: String[1];
            }

            Class meta::pure::functions::collection::tests::model::CO_GeographicEntity
            {
            }

            Class meta::pure::functions::collection::tests::model::CO_Address extends meta::pure::functions::collection::tests::model::CO_GeographicEntity
            {
                name: String[1];
            }

            Class meta::pure::functions::collection::tests::model::CO_Location extends meta::pure::functions::collection::tests::model::CO_GeographicEntity
            {
                place: String[1];
            }

            Class meta::pure::functions::boolean::tests::equalitymodel::SideClass
            {
                stringId: String[1];
                intId: Integer[1];
            }
            """;

    /**
     * User-defined function definitions for PCT and integration tests.
     * Previously hardcoded in PureFunctionRegistry; now part of the Pure model
     * so PureModelBuilder.addSource() picks them up via normal parsing.
     */
    protected static final String FUNCTION_DEFS = """
            function meta::pure::functions::relation::tests::composition::testVariantColumn_functionComposition_filterValues(val: Integer[*]):Boolean[1]
            {
                $val->filter(y | $y->mod(2) == 0)->size() == 2
            }

            function meta::pure::functions::lang::tests::letFn::letAsLastStatement():String[1]
            {
                let last = 'last statement string'
            }

            function meta::pure::functions::lang::tests::letFn::letWithParam(val: String[1]):Any[*]
            {
                let a = $val
            }

            function meta::pure::functions::lang::tests::letFn::letChainedWithAnotherFunction(elements: ModelElement[*]):ModelElement[*]
            {
                let classes = $elements->removeDuplicates()
            }
            """;

    /**
     * Complete Pure model (Classes + Association + Database + Mapping + PCT classes + Functions).
     */
    protected static final String COMPLETE_PURE_MODEL = PERSON_CLASS + "\n" +
            ADDRESS_CLASS + "\n" +
            PERSON_ADDRESS_ASSOCIATION + "\n" +
            PERSON_PRIMARY_ADDRESS_ASSOCIATION + "\n" +
            STRUCT_TEST_CLASSES + "\n" +
            PCT_CLASS_DEFS + "\n" +
            FUNCTION_DEFS + "\n" +
            PERSON_DATABASE + "\n" +
            PERSON_MAPPING;

    // ==================== Connection/Runtime Definitions (Abstract)
    // ====================

    /**
     * @return The database type string for this test ("DuckDB" or "SQLite")
     */
    protected abstract String getDatabaseType();

    /**
     * Builds the Connection Pure definition for this database.
     */
    protected String getConnectionDefinition() {
        return """
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
     */
    protected String getRuntimeDefinition() {
        return """
                import test::*;

                Runtime test::TestRuntime
                {
                    mappings:
                    [
                        model::PersonMapping
                    ];
                    connections:
                    [
                        store::PersonDatabase:
                        [
                            environment: store::TestConnection
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
        return com.gs.legend.plan.PlanGenerator
                .generate(getCompletePureModelWithRuntime(), pureQuery, "test::TestRuntime")
                .sql();
    }

    /**
     * Executes a Pure query and returns results.
     * Uses QueryService for full compile → plan → execute flow.
     * 
     * @param pureQuery The Pure query string
     * @return List of PersonResult
     */
    protected List<PersonResult> executePureQuery(String pureQuery) throws SQLException {
        ExecutionResult result = executeRelation(pureQuery);
        return convertToPersonResults(result);
    }

    /**
     * Converts ExecutionResult to PersonResult list.
     */
    private List<PersonResult> convertToPersonResults(ExecutionResult result) {
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
     * Executes a Pure query and returns an ExecutionResult.
     * Uses QueryService for complete compile → plan → execute flow.
     *
     * @param pureQuery The Pure query to execute
     * @return ExecutionResult containing column metadata and rows
     */
    protected ExecutionResult executeRelation(String pureQuery) throws SQLException {
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
                        AGE_VAL INTEGER NOT NULL,
                        PRIMARY_ADDR_ID INTEGER
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

        // Insert person test data (PRIMARY_ADDR_ID links to T_ADDRESS)
        // John→addr 1 (New York), Jane→addr 3 (Chicago), Bob→addr 4 (Detroit)
        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO T_PERSON (ID, FIRST_NAME, LAST_NAME, AGE_VAL, PRIMARY_ADDR_ID) VALUES (?, ?, ?, ?, ?)")) {

            insertPerson(ps, 1, "John", "Smith", 30, 1);   // New York
            insertPerson(ps, 2, "Jane", "Smith", 28, 3);   // Chicago
            insertPerson(ps, 3, "Bob", "Jones", 45, 4);    // Detroit
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

    private void insertPerson(PreparedStatement ps, int id, String firstName, String lastName, int age,
                              int primaryAddrId)
            throws SQLException {
        ps.setInt(1, id);
        ps.setString(2, firstName);
        ps.setString(3, lastName);
        ps.setInt(4, age);
        ps.setInt(5, primaryAddrId);
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
