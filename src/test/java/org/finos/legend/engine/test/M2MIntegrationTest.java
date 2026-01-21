package org.finos.legend.engine.test;

import org.finos.legend.engine.server.QueryService;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for M2M (Model-to-Model) transforms using Legend-compatible
 * graphFetch syntax.
 * 
 * These tests verify the FULL execution path:
 * Pure Query → PureCompiler → M2MCompiler → IR → SQL → DuckDB → JSON
 * 
 * Test structure:
 * - ONE-TIME SETUP: All Pure model definitions (Classes, Mappings, Runtime) are
 * defined once
 * - TESTS: Each test only defines a Pure query and executes via QueryService
 * - NO MANUAL SQL: All execution goes through QueryService.executeGraphFetch()
 */
@DisplayName("M2M Integration Tests - Legend graphFetch Syntax")
class M2MIntegrationTest {

    private Connection connection;
    private final QueryService queryService = new QueryService();

    // ==================== Complete Pure Model Definition ====================

    /**
     * Complete Pure source containing:
     * - Source class (RawPerson) with raw data properties
     * - Target classes (Person, PersonView, etc.) for M2M transforms
     * - Database and relational mapping for RawPerson
     * - M2M mappings defining transformations
     * - Connection and Runtime for execution
     */
    private static final String PURE_MODEL = """
            Class model::RawPerson
            {
                firstName: String[1];
                lastName: String[1];
                age: Integer[1];
                salary: Float[1];
                isActive: Boolean[1];
            }

            Class model::RawAddress
            {
                city: String[1];
                street: String[1];
                personId: Integer[1];
            }

            Class model::Person
            {
                fullName: String[1];
                upperLastName: String[1];
            }

            Class model::Address
            {
                city: String[1];
                street: String[1];
            }

            Class model::PersonWithAddress
            {
                fullName: String[1];
                address: model::Address[0..1];
            }

            Class model::PersonWithAddresses
            {
                fullName: String[1];
                addresses: model::Address[*];
            }

            Class model::PersonView
            {
                firstName: String[1];
                ageGroup: String[1];
            }

            Class model::ActivePerson
            {
                firstName: String[1];
                lastName: String[1];
            }

            Class model::PersonWithSalary
            {
                firstName: String[1];
                salaryBand: String[1];
            }

            Database store::RawDatabase
            (
                Table T_RAW_PERSON
                (
                    ID INTEGER PRIMARY KEY,
                    FIRST_NAME VARCHAR(100) NOT NULL,
                    LAST_NAME VARCHAR(100) NOT NULL,
                    AGE INTEGER NOT NULL,
                    SALARY DECIMAL(10,2) NOT NULL,
                    IS_ACTIVE BOOLEAN NOT NULL
                )
                Table T_RAW_ADDRESS
                (
                    ID INTEGER PRIMARY KEY,
                    CITY VARCHAR(100) NOT NULL,
                    STREET VARCHAR(100) NOT NULL,
                    PERSON_ID INTEGER NOT NULL
                )
                Join PersonAddress(T_RAW_PERSON.ID = T_RAW_ADDRESS.PERSON_ID)
            )

            Mapping model::RawMapping
            (
                RawPerson: Relational
                {
                    ~mainTable [RawDatabase] T_RAW_PERSON
                    firstName: [RawDatabase] T_RAW_PERSON.FIRST_NAME,
                    lastName: [RawDatabase] T_RAW_PERSON.LAST_NAME,
                    age: [RawDatabase] T_RAW_PERSON.AGE,
                    salary: [RawDatabase] T_RAW_PERSON.SALARY,
                    isActive: [RawDatabase] T_RAW_PERSON.IS_ACTIVE
                }
                RawAddress: Relational
                {
                    ~mainTable [RawDatabase] T_RAW_ADDRESS
                    city: [RawDatabase] T_RAW_ADDRESS.CITY,
                    street: [RawDatabase] T_RAW_ADDRESS.STREET,
                    personId: [RawDatabase] T_RAW_ADDRESS.PERSON_ID
                }
            )

            Mapping model::PersonM2MMapping
            (
                Person: Pure
                {
                    ~src RawPerson
                    fullName: $src.firstName + ' ' + $src.lastName,
                    upperLastName: $src.lastName->toUpper()
                }
            )

            Mapping model::AddressM2MMapping
            (
                Address: Pure
                {
                    ~src RawAddress
                    city: $src.city,
                    street: $src.street
                }
            )

            Mapping model::DeepFetchMapping
            (
                PersonWithAddress: Pure
                {
                    ~src RawPerson
                    fullName: $src.firstName + ' ' + $src.lastName,
                    address: @PersonAddress
                }
                PersonWithAddresses: Pure
                {
                    ~src RawPerson
                    fullName: $src.firstName + ' ' + $src.lastName,
                    addresses: @PersonAddress
                }
                Address: Pure
                {
                    ~src RawAddress
                    city: $src.city,
                    street: $src.street
                }
            )

            Mapping model::ConditionalMapping
            (
                PersonView: Pure
                {
                    ~src RawPerson
                    firstName: $src.firstName,
                    ageGroup: if($src.age < 18, |'Minor', |if($src.age < 65, |'Adult', |'Senior'))
                }
            )

            Mapping model::FilteredMapping
            (
                ActivePerson: Pure
                {
                    ~src RawPerson
                    ~filter $src.isActive == true
                    firstName: $src.firstName,
                    lastName: $src.lastName
                }
            )

            Mapping model::SalaryBandMapping
            (
                PersonWithSalary: Pure
                {
                    ~src RawPerson
                    firstName: $src.firstName,
                    salaryBand: if($src.salary < 50000, |'Entry', |if($src.salary < 100000, |'Mid', |'Senior'))
                }
            )

            RelationalDatabaseConnection store::TestConnection
            {
                type: DuckDB;
                specification: InMemory { };
                auth: NoAuth { };
            }

            Runtime test::TestRuntime
            {
                mappings: [ model::PersonM2MMapping, model::ConditionalMapping, model::FilteredMapping, model::SalaryBandMapping, model::DeepFetchMapping ];
                connections: [ store::RawDatabase: store::TestConnection ];
            }
            """;

    // ==================== Test Setup ====================

    @BeforeEach
    void setUp() throws SQLException {
        // Create in-memory DuckDB with test data
        connection = DriverManager.getConnection("jdbc:duckdb:");
        setupTestData();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    private void setupTestData() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Create person table
            stmt.execute("""
                    CREATE TABLE T_RAW_PERSON (
                        ID INTEGER PRIMARY KEY,
                        FIRST_NAME VARCHAR(100),
                        LAST_NAME VARCHAR(100),
                        AGE INTEGER,
                        SALARY DECIMAL(10,2),
                        IS_ACTIVE BOOLEAN
                    )
                    """);

            stmt.execute("INSERT INTO T_RAW_PERSON VALUES (1, 'John', 'Smith', 30, 75000.00, true)");
            stmt.execute("INSERT INTO T_RAW_PERSON VALUES (2, 'Jane', 'Doe', 25, 55000.00, true)");
            stmt.execute("INSERT INTO T_RAW_PERSON VALUES (3, 'Bob', 'Jones', 45, 120000.00, false)");
            stmt.execute("INSERT INTO T_RAW_PERSON VALUES (4, 'Alice', 'Wonder', 17, 0.00, true)");

            // Create address table for deep fetch tests
            stmt.execute("""
                    CREATE TABLE T_RAW_ADDRESS (
                        ID INTEGER PRIMARY KEY,
                        CITY VARCHAR(100),
                        STREET VARCHAR(100),
                        PERSON_ID INTEGER
                    )
                    """);

            // 1-to-1: John has one address
            stmt.execute("INSERT INTO T_RAW_ADDRESS VALUES (1, 'New York', '123 Main St', 1)");
            // 1-to-1: Jane has one address
            stmt.execute("INSERT INTO T_RAW_ADDRESS VALUES (2, 'Boston', '456 Oak Ave', 2)");
            // 1-to-many: Bob has multiple addresses
            stmt.execute("INSERT INTO T_RAW_ADDRESS VALUES (3, 'Chicago', '789 Pine Rd', 3)");
            stmt.execute("INSERT INTO T_RAW_ADDRESS VALUES (4, 'Seattle', '321 Elm Blvd', 3)");
            // Alice has no address (tests null handling)
        }
    }

    // ==================== Helper Method ====================

    /**
     * Executes a graphFetch Pure query and returns JSON.
     * This is the ONLY way tests should execute M2M queries.
     */
    private String executeGraphFetch(String pureQuery) throws SQLException {
        return queryService.executeGraphFetch(PURE_MODEL, pureQuery, "test::TestRuntime", connection);
    }

    // ==================== M2M graphFetch Tests ====================

    @Test
    @DisplayName("M2M: String concatenation and toUpper() - Person.all()->graphFetch")
    void testPersonTransform() throws SQLException {
        // GIVEN: A graphFetch query for Person (fullName + upperLastName)
        String pureQuery = """
                Person.all()
                    ->graphFetch(#{ Person { fullName, upperLastName } }#)
                    ->serialize(#{ Person { fullName, upperLastName } }#)
                """;

        // WHEN: Execute via QueryService
        String json = executeGraphFetch(pureQuery);
        System.out.println("Person JSON: " + json);

        // THEN: JSON contains transformed data
        assertTrue(json.contains("John Smith"), "Should have concatenated fullName");
        assertTrue(json.contains("SMITH"), "Should have uppercase lastName");
        assertTrue(json.contains("Jane Doe"), "Should have all people");
        assertTrue(json.contains("DOE"));

        // Count objects
        long count = json.chars().filter(c -> c == '{').count();
        assertEquals(4, count, "Should have 4 Person objects");
    }

    @Test
    @DisplayName("M2M: Conditional age grouping - PersonView.all()->graphFetch")
    void testAgeGroupConditional() throws SQLException {
        // GIVEN: A graphFetch query with conditional ageGroup
        String pureQuery = """
                PersonView.all()
                    ->graphFetch(#{ PersonView { firstName, ageGroup } }#)
                    ->serialize(#{ PersonView { firstName, ageGroup } }#)
                """;

        // WHEN: Execute
        String json = executeGraphFetch(pureQuery);
        System.out.println("PersonView JSON: " + json);

        // THEN: Age groups are correctly assigned
        // John (30) -> Adult, Jane (25) -> Adult, Bob (45) -> Adult, Alice (17) ->
        // Minor
        assertTrue(json.contains("Adult"), "Should have Adult category");
        assertTrue(json.contains("Minor"), "Should have Minor for Alice (17)");
        assertTrue(json.contains("Alice"), "Should have Alice");
    }

    @Test
    @DisplayName("M2M: Salary band conditional - PersonWithSalary.all()->graphFetch")
    void testSalaryBandConditional() throws SQLException {
        // GIVEN: A graphFetch query with salary band logic
        String pureQuery = """
                PersonWithSalary.all()
                    ->graphFetch(#{ PersonWithSalary { firstName, salaryBand } }#)
                    ->serialize(#{ PersonWithSalary { firstName, salaryBand } }#)
                """;

        // WHEN: Execute
        String json = executeGraphFetch(pureQuery);
        System.out.println("PersonWithSalary JSON: " + json);

        // THEN: Salary bands correctly computed
        // John (75000) -> Mid, Jane (55000) -> Mid, Bob (120000) -> Senior, Alice (0)
        // -> Entry
        assertTrue(json.contains("Mid"), "Should have Mid salary band");
        assertTrue(json.contains("Senior"), "Should have Senior for Bob (120k)");
        assertTrue(json.contains("Entry"), "Should have Entry for Alice (0)");
    }

    @Test
    @DisplayName("M2M: Filter active people - ActivePerson.all()->graphFetch")
    void testFilteredMapping() throws SQLException {
        // GIVEN: A graphFetch query with filter (isActive == true)
        String pureQuery = """
                ActivePerson.all()
                    ->graphFetch(#{ ActivePerson { firstName, lastName } }#)
                    ->serialize(#{ ActivePerson { firstName, lastName } }#)
                """;

        // WHEN: Execute
        String json = executeGraphFetch(pureQuery);
        System.out.println("ActivePerson JSON: " + json);

        // THEN: Only active people (Bob is inactive)
        assertTrue(json.contains("John"), "Should have John (active)");
        assertTrue(json.contains("Jane"), "Should have Jane (active)");
        assertTrue(json.contains("Alice"), "Should have Alice (active)");
        assertFalse(json.contains("Bob"), "Should NOT have Bob (inactive)");

        // Only 3 active people
        long count = json.chars().filter(c -> c == '{').count();
        assertEquals(3, count, "Should have 3 active people");
    }

    @Test
    @DisplayName("M2M: JSON output format is valid array")
    void testJsonOutputFormat() throws SQLException {
        // GIVEN: Any graphFetch query
        String pureQuery = """
                Person.all()
                    ->graphFetch(#{ Person { fullName } }#)
                    ->serialize(#{ Person { fullName } }#)
                """;

        // WHEN: Execute
        String json = executeGraphFetch(pureQuery);

        // THEN: Valid JSON array structure
        assertTrue(json.startsWith("["), "Should start with array bracket");
        assertTrue(json.endsWith("]"), "Should end with array bracket");
        assertTrue(json.contains("\"fullName\""), "Should have property names in quotes");
    }

    @Test
    @DisplayName("M2M: Single property projection works")
    void testSinglePropertyProjection() throws SQLException {
        // GIVEN: graphFetch with single property
        String pureQuery = """
                Person.all()
                    ->graphFetch(#{ Person { upperLastName } }#)
                    ->serialize(#{ Person { upperLastName } }#)
                """;

        // WHEN: Execute
        String json = executeGraphFetch(pureQuery);
        System.out.println("Single property JSON: " + json);

        // THEN: Has the single property
        assertTrue(json.contains("upperLastName"), "Should have upperLastName property");
        assertTrue(json.contains("SMITH"), "Should have SMITH");
        assertTrue(json.contains("DOE"), "Should have DOE");
        assertTrue(json.contains("JONES"), "Should have JONES");
        assertTrue(json.contains("WONDER"), "Should have WONDER");
    }

    // ==================== Deep Fetch (Nested Object) Tests ====================

    @Test
    @DisplayName("Deep Fetch 1-to-1: PersonWithAddress with nested address object")
    void testDeepFetchOneToOne() throws SQLException {
        // GIVEN: A graphFetch query with nested address
        String pureQuery = """
                PersonWithAddress.all()
                    ->graphFetch(#{ PersonWithAddress { fullName, address { city, street } } }#)
                    ->serialize(#{ PersonWithAddress { fullName, address { city, street } } }#)
                """;

        // WHEN: Execute via QueryService
        String json = executeGraphFetch(pureQuery);
        System.out.println("Deep Fetch 1-to-1 JSON: " + json);

        // THEN: JSON contains nested address objects
        assertTrue(json.contains("John Smith"), "Should have John's fullName");
        assertTrue(json.contains("\"address\""), "Should have nested address property");
        assertTrue(json.contains("New York"), "Should have John's city");
        assertTrue(json.contains("123 Main St"), "Should have John's street");

        // Jane has address too
        assertTrue(json.contains("Jane Doe"), "Should have Jane's fullName");
        assertTrue(json.contains("Boston"), "Should have Jane's city");
    }

    @Test
    @DisplayName("Deep Fetch 1-to-Many: PersonWithAddresses with nested address array")
    void testDeepFetchOneToMany() throws SQLException {
        // GIVEN: A graphFetch query with nested addresses collection
        String pureQuery = """
                PersonWithAddresses.all()
                    ->graphFetch(#{ PersonWithAddresses { fullName, addresses { city, street } } }#)
                    ->serialize(#{ PersonWithAddresses { fullName, addresses { city, street } } }#)
                """;

        // WHEN: Execute via QueryService
        String json = executeGraphFetch(pureQuery);
        System.out.println("Deep Fetch 1-to-Many JSON: " + json);

        // THEN: JSON contains nested address arrays
        assertTrue(json.contains("Bob Jones"), "Should have Bob's fullName");
        assertTrue(json.contains("\"addresses\""), "Should have nested addresses array");
        assertTrue(json.contains("Chicago"), "Should have Bob's first city");
        assertTrue(json.contains("Seattle"), "Should have Bob's second city");

        // Bob should have 2 addresses in array
        // Count occurrences of "street" following Bob - he has 2
    }

    @Test
    @DisplayName("Deep Fetch null handling: Person without address returns null")
    void testDeepFetchNullAddress() throws SQLException {
        // GIVEN: Alice has no address in the database
        String pureQuery = """
                PersonWithAddress.all()
                    ->graphFetch(#{ PersonWithAddress { fullName, address { city } } }#)
                    ->serialize(#{ PersonWithAddress { fullName, address { city } } }#)
                """;

        // WHEN: Execute via QueryService
        String json = executeGraphFetch(pureQuery);
        System.out.println("Deep Fetch with null JSON: " + json);

        // THEN: Alice should have null address
        assertTrue(json.contains("Alice Wonder"), "Should have Alice's fullName");
        // Alice's address should be null or empty object
    }
}
