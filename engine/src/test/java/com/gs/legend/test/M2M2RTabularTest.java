package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.server.QueryService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end M2M2R tabular tests: M2M mapping compiled via clean pipeline,
 * output as tabular (project) rather than JSON (graphFetch/serialize).
 *
 * Pipeline: PureParser → TypeChecker → PlanGenerator → SQL → DuckDB → ExecutionResult
 *
 * This validates that Person.all()->project(~[fullName]) works when Person
 * has a Pure (M2M) mapping like:  fullName: $src.firstName + ' ' + $src.lastName
 */
@DisplayName("M2M2R Tabular Tests - Clean Pipeline")
class M2M2RTabularTest {

    private Connection connection;
    private final QueryService qs = new QueryService();

    // ==================== Pure Model ====================

    private static final String PURE_MODEL = """
            Class model::RawPerson
            {
                firstName: String[1];
                lastName: String[1];
                age: Integer[1];
                salary: Float[1];
                isActive: Boolean[1];
            }

            Class model::Person
            {
                fullName: String[1];
                upperLastName: String[1];
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

            Class model::PersonSummary
            {
                name: String[1];
                nameUpper: String[1];
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

            Mapping model::PersonViewMapping
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

            Mapping model::ChainedM2MMapping
            (
                PersonSummary: Pure
                {
                    ~src Person
                    name: $src.fullName,
                    nameUpper: $src.upperLastName
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
                mappings:
                [
                    model::PersonM2MMapping,
                    model::PersonViewMapping,
                    model::FilteredMapping,
                    model::SalaryBandMapping,
                    model::ChainedM2MMapping
                ];
                connections:
                [
                    store::RawDatabase:
                    [
                        environment: store::TestConnection
                    ]
                ];
            }
            """;

    // ==================== Setup ====================

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement stmt = connection.createStatement()) {
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
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) connection.close();
    }

    // ==================== Basic M2M2R Tests ====================

    @Test
    @DisplayName("String concat: project(~[fullName])")
    void testProjectFullName() throws SQLException {
        String query = "Person.all()->project(~[fullName:x|$x.fullName])";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        assertEquals(1, result.columns().size());
        assertEquals("fullName", result.columns().get(0).name());
        assertEquals(4, result.rows().size());

        var names = result.rows().stream()
                .map(r -> r.get(0).toString())
                .sorted()
                .toList();
        assertEquals("Alice Wonder", names.get(0));
        assertEquals("Bob Jones", names.get(1));
        assertEquals("Jane Doe", names.get(2));
        assertEquals("John Smith", names.get(3));
    }

    @Test
    @DisplayName("Multi-column: project(~[fullName, upperLastName])")
    void testProjectMultipleColumns() throws SQLException {
        String query = "Person.all()->project(~[fullName:x|$x.fullName, upper:x|$x.upperLastName])";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        assertEquals(2, result.columns().size());
        assertEquals(4, result.rows().size());

        // Find John's row
        for (var row : result.rows()) {
            if ("John Smith".equals(row.get(0).toString())) {
                assertEquals("SMITH", row.get(1).toString());
                return;
            }
        }
        fail("Should find John Smith in results");
    }

    @Test
    @DisplayName("toUpper only: project(~[upper])")
    void testProjectUpperOnly() throws SQLException {
        String query = "Person.all()->project(~[upper:x|$x.upperLastName])";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        assertEquals(1, result.columns().size());
        assertEquals(4, result.rows().size());

        var values = result.rows().stream()
                .map(r -> r.get(0).toString())
                .sorted()
                .toList();
        assertEquals("DOE", values.get(0));
        assertEquals("JONES", values.get(1));
        assertEquals("SMITH", values.get(2));
        assertEquals("WONDER", values.get(3));
    }

    // ==================== Filter on M2M Computed Columns ====================

    @Test
    @DisplayName("Filter on concat: project(~[fullName])->filter(startsWith('J'))")
    void testProjectWithFilter() throws SQLException {
        String query = "Person.all()->project(~[fullName:x|$x.fullName])->filter(x|$x.fullName->startsWith('J'))";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        assertEquals(2, result.rows().size());

        var names = result.rows().stream()
                .map(r -> r.get(0).toString())
                .sorted()
                .toList();
        assertTrue(names.contains("Jane Doe"));
        assertTrue(names.contains("John Smith"));
    }

    @Test
    @DisplayName("Filter on toUpper: project(~[upper])->filter(contains('SM'))")
    void testFilterOnUpperColumn() throws SQLException {
        String query = "Person.all()->project(~[upper:x|$x.upperLastName])->filter(x|$x.upper->contains('SM'))";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        // Only SMITH matches
        assertEquals(1, result.rows().size());
        assertEquals("SMITH", result.rows().get(0).get(0).toString());
    }

    // ==================== Conditional Expressions ====================

    @Test
    @DisplayName("Conditional: ageGroup via if/else")
    void testConditionalAgeGroup() throws SQLException {
        String query = "PersonView.all()->project(~[name:x|$x.firstName, group:x|$x.ageGroup])";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        assertEquals(4, result.rows().size());

        for (var row : result.rows()) {
            String name = row.get(0).toString();
            String group = row.get(1).toString();
            switch (name) {
                case "Alice" -> assertEquals("Minor", group, "Alice (17) should be Minor");
                case "John" -> assertEquals("Adult", group, "John (30) should be Adult");
                case "Jane" -> assertEquals("Adult", group, "Jane (25) should be Adult");
                case "Bob" -> assertEquals("Adult", group, "Bob (45) should be Adult");
            }
        }
    }

    @Test
    @DisplayName("Conditional: salaryBand via if/else")
    void testConditionalSalaryBand() throws SQLException {
        String query = "PersonWithSalary.all()->project(~[name:x|$x.firstName, band:x|$x.salaryBand])";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        assertEquals(4, result.rows().size());

        for (var row : result.rows()) {
            String name = row.get(0).toString();
            String band = row.get(1).toString();
            switch (name) {
                case "Alice" -> assertEquals("Entry", band, "Alice ($0) should be Entry");
                case "John" -> assertEquals("Mid", band, "John ($75k) should be Mid");
                case "Jane" -> assertEquals("Mid", band, "Jane ($55k) should be Mid");
                case "Bob" -> assertEquals("Senior", band, "Bob ($120k) should be Senior");
            }
        }
    }

    // ==================== Passthrough (identity) M2M ====================

    @Test
    @DisplayName("Passthrough: $src.firstName (no transform, just pass-through)")
    void testPassthroughProperty() throws SQLException {
        String query = "PersonView.all()->project(~[name:x|$x.firstName])";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        assertEquals(1, result.columns().size());
        assertEquals(4, result.rows().size());

        var names = result.rows().stream()
                .map(r -> r.get(0).toString())
                .sorted()
                .toList();
        assertEquals("Alice", names.get(0));
        assertEquals("Bob", names.get(1));
        assertEquals("Jane", names.get(2));
        assertEquals("John", names.get(3));
    }

    // ==================== Sort on M2M columns ====================

    @Test
    @DisplayName("Sort: project(~[fullName])->sort(asc(fullName))")
    void testSortOnM2MColumn() throws SQLException {
        String query = "Person.all()->project(~[fullName:x|$x.fullName])->sort(asc(~fullName))";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        assertEquals(4, result.rows().size());
        assertEquals("Alice Wonder", result.rows().get(0).get(0).toString());
        assertEquals("Bob Jones", result.rows().get(1).get(0).toString());
        assertEquals("Jane Doe", result.rows().get(2).get(0).toString());
        assertEquals("John Smith", result.rows().get(3).get(0).toString());
    }

    // ==================== GroupBy + Aggregation ====================

    @Test
    @DisplayName("GroupBy: count per ageGroup")
    void testGroupByOnConditional() throws SQLException {
        String query = "PersonView.all()->project(~[group:x|$x.ageGroup, name:x|$x.firstName])->groupBy(~[group], ~cnt:x|$x.name:y|$y->count())";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        // Adult=3, Minor=1
        assertEquals(2, result.rows().size());
        for (var row : result.rows()) {
            String group = row.get(0).toString();
            long cnt = ((Number) row.get(1)).longValue();
            switch (group) {
                case "Adult" -> assertEquals(3, cnt);
                case "Minor" -> assertEquals(1, cnt);
                default -> fail("Unexpected group: " + group);
            }
        }
    }

    @Test
    @DisplayName("GroupBy: count per salaryBand")
    void testGroupByOnSalaryBand() throws SQLException {
        String query = "PersonWithSalary.all()->project(~[band:x|$x.salaryBand, name:x|$x.firstName])->groupBy(~[band], ~cnt:x|$x.name:y|$y->count())";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        // Entry=1, Mid=2, Senior=1
        assertEquals(3, result.rows().size());
        for (var row : result.rows()) {
            String band = row.get(0).toString();
            long cnt = ((Number) row.get(1)).longValue();
            switch (band) {
                case "Entry" -> assertEquals(1, cnt);
                case "Mid" -> assertEquals(2, cnt);
                case "Senior" -> assertEquals(1, cnt);
                default -> fail("Unexpected band: " + band);
            }
        }
    }

    // ==================== Distinct ====================

    @Test
    @DisplayName("Distinct: unique ageGroups")
    void testDistinctOnM2M() throws SQLException {
        String query = "PersonView.all()->project(~[group:x|$x.ageGroup])->distinct()";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        // Adult and Minor only
        assertEquals(2, result.rows().size());
        var groups = result.rows().stream()
                .map(r -> r.get(0).toString())
                .sorted()
                .toList();
        assertEquals("Adult", groups.get(0));
        assertEquals("Minor", groups.get(1));
    }

    // ==================== Sort + Limit/Take ====================

    @Test
    @DisplayName("Sort + take: top 2 fullNames alphabetically")
    void testSortAndTake() throws SQLException {
        String query = "Person.all()->project(~[fullName:x|$x.fullName])->sort(asc(~fullName))->take(2)";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        assertEquals(2, result.rows().size());
        assertEquals("Alice Wonder", result.rows().get(0).get(0).toString());
        assertEquals("Bob Jones", result.rows().get(1).get(0).toString());
    }

    @Test
    @DisplayName("Sort desc + take: top 2 fullNames reverse")
    void testSortDescAndTake() throws SQLException {
        String query = "Person.all()->project(~[fullName:x|$x.fullName])->sort(desc(~fullName))->take(2)";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        assertEquals(2, result.rows().size());
        assertEquals("John Smith", result.rows().get(0).get(0).toString());
        assertEquals("Jane Doe", result.rows().get(1).get(0).toString());
    }

    // ==================== Extend (Window Functions) ====================

    @Test
    @DisplayName("Extend: add computed column on M2M output")
    void testExtendOnM2MOutput() throws SQLException {
        // Extend M2M projected output with a new computed column
        String query = "PersonView.all()->project(~[name:x|$x.firstName, group:x|$x.ageGroup])->extend(~label:x|$x.name->toOne() + '_' + $x.group->toOne())";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        assertEquals(4, result.rows().size());
        assertEquals(3, result.columns().size()); // name, group, label

        // Verify the computed label column
        for (var row : result.rows()) {
            String name = row.get(0).toString();
            String group = row.get(1).toString();
            String label = row.get(2).toString();
            assertEquals(name + "_" + group, label, "label should be name_group");
        }
    }

    // ==================== Full Pipeline Chain ====================

    @Test
    @DisplayName("Full chain: project → filter → sort → take")
    void testFullPipelineChain() throws SQLException {
        // PersonView: project ageGroup → filter only Adults → sort by name → take top 2
        String query = "PersonView.all()" +
                "->project(~[name:x|$x.firstName, group:x|$x.ageGroup])" +
                "->filter(x|$x.group == 'Adult')" +
                "->sort(asc(~name))" +
                "->take(2)";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        assertEquals(2, result.rows().size());
        // Adults sorted by name: Bob, Jane, John → take 2 → Bob, Jane
        assertEquals("Bob", result.rows().get(0).get(0).toString());
        assertEquals("Jane", result.rows().get(1).get(0).toString());
        // Both should be Adult
        assertEquals("Adult", result.rows().get(0).get(1).toString());
        assertEquals("Adult", result.rows().get(1).get(1).toString());
    }

    // ==================== Chained M2M (M2M→M2M→R) ====================

    @Test
    @DisplayName("Chained M2M: PersonSummary (src=Person, src=RawPerson)")
    void testChainedM2M() throws SQLException {
        // PersonSummary.name resolves through Person.fullName → $src.firstName + ' ' + $src.lastName
        // PersonSummary.nameUpper resolves through Person.upperLastName → $src.lastName->toUpper()
        String query = "PersonSummary.all()->project(~[n:x|$x.name, u:x|$x.nameUpper])";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        assertEquals(2, result.columns().size());
        assertEquals("n", result.columns().get(0).name());
        assertEquals("u", result.columns().get(1).name());
        assertEquals(4, result.rows().size());

        var names = result.rows().stream()
                .map(r -> r.get(0).toString())
                .sorted()
                .toList();
        assertEquals("Alice Wonder", names.get(0));
        assertEquals("Bob Jones", names.get(1));
        assertEquals("Jane Doe", names.get(2));
        assertEquals("John Smith", names.get(3));

        var uppers = result.rows().stream()
                .map(r -> r.get(1).toString())
                .sorted()
                .toList();
        assertEquals("DOE", uppers.get(0));
        assertEquals("JONES", uppers.get(1));
        assertEquals("SMITH", uppers.get(2));
        assertEquals("WONDER", uppers.get(3));
    }

    @Test
    @DisplayName("Chained M2M + filter: PersonSummary filtered by name")
    void testChainedM2MWithFilter() throws SQLException {
        String query = "PersonSummary.all()" +
                "->project(~[n:x|$x.name])" +
                "->filter(x|$x.n->startsWith('J'))" +
                "->sort(asc(~n))";

        ExecutionResult result = qs.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        assertEquals(2, result.rows().size());
        assertEquals("Jane Doe", result.rows().get(0).get(0).toString());
        assertEquals("John Smith", result.rows().get(1).get(0).toString());
    }
}
