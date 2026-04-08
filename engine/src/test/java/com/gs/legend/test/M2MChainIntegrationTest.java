package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.server.QueryService;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive M2M chain tests covering:
 * <ul>
 *   <li><b>project()</b> through single and multi-hop M2M chains</li>
 *   <li><b>graphFetch()</b> through single and multi-hop M2M chains</li>
 *   <li><b>Mapping filters</b> at various levels in the chain</li>
 *   <li><b>User query filters</b> composed with M2M mapping filters</li>
 *   <li><b>Deep fetch</b> through chained M2M with nested objects</li>
 *   <li><b>3-hop chains</b>: ViewClass → MiddleClass → RawClass → Table</li>
 * </ul>
 *
 * <pre>
 * Model layers:
 *   Table:  T_EMPLOYEE (ID, FIRST_NAME, LAST_NAME, AGE, DEPT, SALARY, IS_ACTIVE)
 *           T_DEPARTMENT (ID, NAME, LOCATION)
 *
 *   Raw:    Employee     ← Relational (T_EMPLOYEE)
 *           Department   ← Relational (T_DEPARTMENT)
 *
 *   L1:     StaffMember  ← Pure (~src Employee)    — simple rename + computed fullName
 *           ActiveStaff  ← Pure (~src Employee, ~filter isActive)
 *
 *   L2:     StaffCard    ← Pure (~src StaffMember)  — 2-hop chain, formatted display
 *           StaffBadge   ← Pure (~src ActiveStaff)  — 2-hop chain with filter at L1
 *
 *   L3:     DirectoryEntry ← Pure (~src StaffCard)  — 3-hop chain
 *
 *   Deep:   StaffWithDept ← Pure (~src Employee)    — M2M with association navigation
 *           StaffProfile  ← Pure (~src StaffMember)  — 2-hop deep fetch
 * </pre>
 */
@DisplayName("M2M Chain Integration Tests — project() + graphFetch() through multi-hop chains")
class M2MChainIntegrationTest {

    private Connection conn;
    private final QueryService qs = new QueryService();

    private static final String PURE_MODEL = """
            import model::*;
            import store::*;

            // ===== Raw classes (mapped to tables) =====
            Class model::Employee
            {
                firstName: String[1];
                lastName: String[1];
                age: Integer[1];
                dept: String[1];
                salary: Float[1];
                isActive: Boolean[1];
            }

            Class model::Department
            {
                name: String[1];
                location: String[1];
            }

            Association model::EmpDept
            {
                employees: Employee[*];
                department: Department[1];
            }

            // ===== L1: Single-hop M2M =====
            Class model::StaffMember
            {
                fullName: String[1];
                firstName: String[1];
                lastName: String[1];
                age: Integer[1];
                dept: String[1];
            }

            Class model::ActiveStaff
            {
                fullName: String[1];
                dept: String[1];
            }

            // ===== L2: 2-hop M2M =====
            Class model::StaffCard
            {
                displayName: String[1];
                department: String[1];
            }

            Class model::StaffBadge
            {
                label: String[1];
            }

            // ===== L3: 3-hop M2M =====
            Class model::DirectoryEntry
            {
                entry: String[1];
            }

            // ===== Deep fetch classes =====
            Class model::DeptInfo
            {
                name: String[1];
                location: String[1];
            }

            Class model::StaffWithDept
            {
                fullName: String[1];
                department: model::DeptInfo[*];
            }

            // ===== Database =====
            Database store::CompanyDB
            (
                Table T_EMPLOYEE
                (
                    ID INTEGER PRIMARY KEY,
                    FIRST_NAME VARCHAR(100) NOT NULL,
                    LAST_NAME VARCHAR(100) NOT NULL,
                    AGE INTEGER NOT NULL,
                    DEPT VARCHAR(50) NOT NULL,
                    SALARY DECIMAL(10,2) NOT NULL,
                    IS_ACTIVE BOOLEAN NOT NULL,
                    DEPT_ID INTEGER
                )
                Table T_DEPARTMENT
                (
                    ID INTEGER PRIMARY KEY,
                    NAME VARCHAR(100) NOT NULL,
                    LOCATION VARCHAR(100) NOT NULL
                )
                Join EmpDept(T_EMPLOYEE.DEPT_ID = T_DEPARTMENT.ID)
            )

            // ===== Relational mappings =====
            Mapping model::RelationalMapping
            (
                Employee: Relational
                {
                    ~mainTable [CompanyDB] T_EMPLOYEE
                    firstName: [CompanyDB] T_EMPLOYEE.FIRST_NAME,
                    lastName: [CompanyDB] T_EMPLOYEE.LAST_NAME,
                    age: [CompanyDB] T_EMPLOYEE.AGE,
                    dept: [CompanyDB] T_EMPLOYEE.DEPT,
                    salary: [CompanyDB] T_EMPLOYEE.SALARY,
                    isActive: [CompanyDB] T_EMPLOYEE.IS_ACTIVE,
                    department: [CompanyDB] @EmpDept
                }
                Department: Relational
                {
                    ~mainTable [CompanyDB] T_DEPARTMENT
                    name: [CompanyDB] T_DEPARTMENT.NAME,
                    location: [CompanyDB] T_DEPARTMENT.LOCATION,
                    employees: [CompanyDB] @EmpDept
                }
            )

            // ===== L1 M2M mappings =====
            Mapping model::StaffMemberMapping
            (
                StaffMember: Pure
                {
                    ~src Employee
                    fullName: $src.firstName + ' ' + $src.lastName,
                    firstName: $src.firstName,
                    lastName: $src.lastName,
                    age: $src.age,
                    dept: $src.dept
                }
            )

            Mapping model::ActiveStaffMapping
            (
                ActiveStaff: Pure
                {
                    ~src Employee
                    ~filter $src.isActive == true
                    fullName: $src.firstName + ' ' + $src.lastName,
                    dept: $src.dept
                }
            )

            // ===== L2 M2M mappings (2-hop) =====
            Mapping model::StaffCardMapping
            (
                StaffCard: Pure
                {
                    ~src StaffMember
                    displayName: $src.fullName->toUpper(),
                    department: $src.dept
                }
            )

            Mapping model::StaffBadgeMapping
            (
                StaffBadge: Pure
                {
                    ~src ActiveStaff
                    label: $src.fullName + ' [' + $src.dept + ']'
                }
            )

            // ===== L3 M2M mapping (3-hop) =====
            Mapping model::DirectoryMapping
            (
                DirectoryEntry: Pure
                {
                    ~src StaffCard
                    entry: $src.displayName + ' - ' + $src.department
                }
            )

            // ===== Deep fetch M2M mappings =====
            Mapping model::DeepFetchMapping
            (
                StaffWithDept: Pure
                {
                    ~src Employee
                    fullName: $src.firstName + ' ' + $src.lastName,
                    department: $src.department
                }
                DeptInfo: Pure
                {
                    ~src Department
                    name: $src.name,
                    location: $src.location
                }
            )

            // ===== Runtime =====
            RelationalDatabaseConnection store::Conn
            {
                type: DuckDB;
                specification: InMemory { };
                auth: NoAuth { };
            }

            Runtime test::RT
            {
                mappings:
                [
                    model::RelationalMapping,
                    model::StaffMemberMapping,
                    model::ActiveStaffMapping,
                    model::StaffCardMapping,
                    model::StaffBadgeMapping,
                    model::DirectoryMapping,
                    model::DeepFetchMapping
                ];
                connections:
                [
                    store::CompanyDB:
                    [
                        environment: store::Conn
                    ]
                ];
            }
            """;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement s = conn.createStatement()) {
            s.execute("""
                    CREATE TABLE T_EMPLOYEE (
                        ID INTEGER PRIMARY KEY,
                        FIRST_NAME VARCHAR(100),
                        LAST_NAME VARCHAR(100),
                        AGE INTEGER,
                        DEPT VARCHAR(50),
                        SALARY DECIMAL(10,2),
                        IS_ACTIVE BOOLEAN,
                        DEPT_ID INTEGER
                    )""");
            s.execute("INSERT INTO T_EMPLOYEE VALUES (1, 'Alice', 'Smith', 30, 'Engineering', 95000, true, 1)");
            s.execute("INSERT INTO T_EMPLOYEE VALUES (2, 'Bob',   'Jones', 45, 'Sales',       120000, true, 2)");
            s.execute("INSERT INTO T_EMPLOYEE VALUES (3, 'Carol', 'White', 28, 'Engineering', 85000, false, 1)");
            s.execute("INSERT INTO T_EMPLOYEE VALUES (4, 'Dave',  'Brown', 55, 'Marketing',   110000, true, 3)");

            s.execute("""
                    CREATE TABLE T_DEPARTMENT (
                        ID INTEGER PRIMARY KEY,
                        NAME VARCHAR(100),
                        LOCATION VARCHAR(100)
                    )""");
            s.execute("INSERT INTO T_DEPARTMENT VALUES (1, 'Engineering', 'San Francisco')");
            s.execute("INSERT INTO T_DEPARTMENT VALUES (2, 'Sales', 'New York')");
            s.execute("INSERT INTO T_DEPARTMENT VALUES (3, 'Marketing', 'Chicago')");
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null) conn.close();
    }

    // ==================== Helpers ====================

    private ExecutionResult exec(String query) throws SQLException {
        return qs.execute(PURE_MODEL, query, "test::RT", conn);
    }

    private String execGraph(String query) throws SQLException {
        var result = exec(query);
        assertInstanceOf(ExecutionResult.GraphResult.class, result);
        return result.asGraph().json();
    }

    private List<String> col(ExecutionResult r, int idx) {
        return r.rows().stream()
                .map(row -> row.get(idx) != null ? row.get(idx).toString() : null)
                .collect(Collectors.toList());
    }

    // ==================== L1: Single M2M → project() ====================

    @Nested
    @DisplayName("L1: Single-hop M2M")
    class L1SingleHop {

        @Test
        @DisplayName("project(): StaffMember computed fullName + passthrough props")
        void testProjectSimple() throws SQLException {
            var r = exec("StaffMember.all()->project(~[fullName:x|$x.fullName, dept:x|$x.dept])");
            assertEquals(4, r.rowCount());
            var names = col(r, 0);
            assertTrue(names.contains("Alice Smith"));
            assertTrue(names.contains("Bob Jones"));
            assertTrue(names.contains("Carol White"));
            assertTrue(names.contains("Dave Brown"));
        }

        @Test
        @DisplayName("project(): StaffMember all properties")
        void testProjectAllProps() throws SQLException {
            var r = exec("StaffMember.all()->project(~[fullName:x|$x.fullName, firstName:x|$x.firstName, lastName:x|$x.lastName, age:x|$x.age, dept:x|$x.dept])");
            assertEquals(4, r.rowCount());
            var firstNames = col(r, 1);
            assertTrue(firstNames.contains("Alice"));
            assertTrue(firstNames.contains("Bob"));
        }

        @Test
        @DisplayName("graphFetch(): StaffMember")
        void testGraphFetch() throws SQLException {
            var json = execGraph("""
                    StaffMember.all()
                        ->graphFetch(#{ StaffMember { fullName, dept } }#)
                        ->serialize(#{ StaffMember { fullName, dept } }#)
                    """);
            assertTrue(json.contains("Alice Smith"));
            assertTrue(json.contains("Engineering"));
            assertTrue(json.contains("Sales"));
        }

        @Test
        @DisplayName("project() with mapping filter: ActiveStaff excludes inactive")
        void testProjectWithFilter() throws SQLException {
            var r = exec("ActiveStaff.all()->project(~[fullName:x|$x.fullName, dept:x|$x.dept])");
            assertEquals(3, r.rowCount(), "Carol is inactive, should be excluded");
            var names = col(r, 0);
            assertTrue(names.contains("Alice Smith"));
            assertTrue(names.contains("Bob Jones"));
            assertTrue(names.contains("Dave Brown"));
            assertFalse(names.contains("Carol White"), "Carol is inactive");
        }

        @Test
        @DisplayName("graphFetch() with mapping filter: ActiveStaff")
        void testGraphFetchWithFilter() throws SQLException {
            var json = execGraph("""
                    ActiveStaff.all()
                        ->graphFetch(#{ ActiveStaff { fullName, dept } }#)
                        ->serialize(#{ ActiveStaff { fullName, dept } }#)
                    """);
            assertTrue(json.contains("Alice Smith"));
            assertFalse(json.contains("Carol White"), "Carol is inactive");
        }

        @Test
        @DisplayName("project() with user query filter on top of M2M")
        void testProjectWithUserFilter() throws SQLException {
            var r = exec("StaffMember.all()->filter({x|$x.dept == 'Engineering'})->project(~[fullName:x|$x.fullName])");
            assertEquals(2, r.rowCount(), "Only Engineering staff");
            var names = col(r, 0);
            assertTrue(names.contains("Alice Smith"));
            assertTrue(names.contains("Carol White"));
        }

        @Test
        @DisplayName("project() with user filter + mapping filter (both compose)")
        void testProjectUserFilterPlusMappingFilter() throws SQLException {
            var r = exec("ActiveStaff.all()->filter({x|$x.dept == 'Engineering'})->project(~[fullName:x|$x.fullName])");
            assertEquals(1, r.rowCount(), "Only active Engineering staff");
            assertEquals("Alice Smith", col(r, 0).get(0));
        }
    }

    // ==================== L2: 2-hop M2M → project() ====================

    @Nested
    @DisplayName("L2: Two-hop M2M chains")
    class L2TwoHop {

        @Test
        @DisplayName("project(): StaffCard through StaffMember → Employee → Table")
        void testProjectTwoHop() throws SQLException {
            var r = exec("StaffCard.all()->project(~[displayName:x|$x.displayName, department:x|$x.department])");
            assertEquals(4, r.rowCount());
            var names = col(r, 0);
            // displayName is fullName->toUpper()
            assertTrue(names.contains("ALICE SMITH"));
            assertTrue(names.contains("BOB JONES"));
            assertTrue(names.contains("CAROL WHITE"));
            assertTrue(names.contains("DAVE BROWN"));
        }

        @Test
        @DisplayName("graphFetch(): StaffCard 2-hop")
        void testGraphFetchTwoHop() throws SQLException {
            var json = execGraph("""
                    StaffCard.all()
                        ->graphFetch(#{ StaffCard { displayName, department } }#)
                        ->serialize(#{ StaffCard { displayName, department } }#)
                    """);
            assertTrue(json.contains("ALICE SMITH"));
            assertTrue(json.contains("Engineering"));
        }

        @Test
        @DisplayName("project(): StaffBadge — 2-hop with mapping filter at L1")
        void testProjectTwoHopWithFilterAtL1() throws SQLException {
            // StaffBadge ~src ActiveStaff (which has ~filter isActive)
            var r = exec("StaffBadge.all()->project(~[label:x|$x.label])");
            assertEquals(3, r.rowCount(), "Carol is excluded by ActiveStaff filter");
            var labels = col(r, 0);
            assertTrue(labels.contains("Alice Smith [Engineering]"));
            assertTrue(labels.contains("Bob Jones [Sales]"));
            assertTrue(labels.contains("Dave Brown [Marketing]"));
            assertFalse(labels.stream().anyMatch(l -> l != null && l.contains("Carol")));
        }

        @Test
        @DisplayName("graphFetch(): StaffBadge — 2-hop with filter")
        void testGraphFetchTwoHopWithFilter() throws SQLException {
            var json = execGraph("""
                    StaffBadge.all()
                        ->graphFetch(#{ StaffBadge { label } }#)
                        ->serialize(#{ StaffBadge { label } }#)
                    """);
            assertTrue(json.contains("Alice Smith [Engineering]"));
            assertFalse(json.contains("Carol"), "Carol excluded by ActiveStaff filter");
        }

        @Test
        @DisplayName("project(): 2-hop + user query filter")
        void testProjectTwoHopWithUserFilter() throws SQLException {
            var r = exec("StaffCard.all()->filter({x|$x.department == 'Engineering'})->project(~[displayName:x|$x.displayName])");
            assertEquals(2, r.rowCount());
            var names = col(r, 0);
            assertTrue(names.contains("ALICE SMITH"));
            assertTrue(names.contains("CAROL WHITE"));
        }
    }

    // ==================== L3: 3-hop M2M chain ====================

    @Nested
    @DisplayName("L3: Three-hop M2M chains")
    class L3ThreeHop {

        @Test
        @DisplayName("project(): DirectoryEntry through StaffCard → StaffMember → Employee → Table")
        void testProjectThreeHop() throws SQLException {
            var r = exec("DirectoryEntry.all()->project(~[entry:x|$x.entry])");
            assertEquals(4, r.rowCount());
            var entries = col(r, 0);
            // entry = displayName + ' - ' + department
            //       = toUpper(fullName) + ' - ' + dept
            assertTrue(entries.contains("ALICE SMITH - Engineering"));
            assertTrue(entries.contains("BOB JONES - Sales"));
            assertTrue(entries.contains("CAROL WHITE - Engineering"));
            assertTrue(entries.contains("DAVE BROWN - Marketing"));
        }

        @Test
        @DisplayName("graphFetch(): DirectoryEntry 3-hop")
        void testGraphFetchThreeHop() throws SQLException {
            var json = execGraph("""
                    DirectoryEntry.all()
                        ->graphFetch(#{ DirectoryEntry { entry } }#)
                        ->serialize(#{ DirectoryEntry { entry } }#)
                    """);
            assertTrue(json.contains("ALICE SMITH - Engineering"));
            assertTrue(json.contains("BOB JONES - Sales"));
        }

        @Test
        @DisplayName("project(): 3-hop + user query filter")
        void testProjectThreeHopWithUserFilter() throws SQLException {
            var r = exec("DirectoryEntry.all()->filter({x|$x.entry->contains('Sales')})->project(~[entry:x|$x.entry])");
            assertEquals(1, r.rowCount());
            assertEquals("BOB JONES - Sales", col(r, 0).get(0));
        }
    }

    // ==================== Deep fetch: M2M with association navigation (JOINs) ====================

    @Nested
    @DisplayName("Deep Fetch / Joins: M2M with nested objects via association navigation")
    class DeepFetch {

        @Test
        @DisplayName("graphFetch(): StaffWithDept — correlated subquery JOIN")
        void testDeepFetchViaAssociation() throws SQLException {
            var json = execGraph("""
                    StaffWithDept.all()
                        ->graphFetch(#{ StaffWithDept { fullName, department { name, location } } }#)
                        ->serialize(#{ StaffWithDept { fullName, department { name, location } } }#)
                    """);
            assertTrue(json.contains("Alice Smith"));
            assertTrue(json.contains("Engineering"));
            assertTrue(json.contains("San Francisco"));
            assertTrue(json.contains("Bob Jones"));
            assertTrue(json.contains("Sales"));
            assertTrue(json.contains("New York"));
        }

        @Test
        @DisplayName("graphFetch(): deep fetch with only root props (no join triggered)")
        void testDeepFetchRootOnlyNoJoin() throws SQLException {
            var json = execGraph("""
                    StaffWithDept.all()
                        ->graphFetch(#{ StaffWithDept { fullName } }#)
                        ->serialize(#{ StaffWithDept { fullName } }#)
                    """);
            assertTrue(json.contains("Alice Smith"));
            assertTrue(json.contains("Dave Brown"));
            // No department subquery should be generated
            assertFalse(json.contains("Engineering"), "department not requested");
        }

        @Test
        @DisplayName("graphFetch(): deep fetch all 4 employees have correct departments")
        void testDeepFetchAllEmployeesDepts() throws SQLException {
            var json = execGraph("""
                    StaffWithDept.all()
                        ->graphFetch(#{ StaffWithDept { fullName, department { name } } }#)
                        ->serialize(#{ StaffWithDept { fullName, department { name } } }#)
                    """);
            // Alice & Carol → Engineering, Bob → Sales, Dave → Marketing
            assertTrue(json.contains("Alice Smith"));
            assertTrue(json.contains("Carol White"));
            assertTrue(json.contains("Engineering"));
            assertTrue(json.contains("Bob Jones"));
            assertTrue(json.contains("Sales"));
            assertTrue(json.contains("Dave Brown"));
            assertTrue(json.contains("Marketing"));
        }

        @Test
        @DisplayName("graphFetch(): deep fetch only location (partial nested selection)")
        void testDeepFetchPartialNestedSelection() throws SQLException {
            var json = execGraph("""
                    StaffWithDept.all()
                        ->graphFetch(#{ StaffWithDept { fullName, department { location } } }#)
                        ->serialize(#{ StaffWithDept { fullName, department { location } } }#)
                    """);
            assertTrue(json.contains("San Francisco"));
            assertTrue(json.contains("New York"));
            assertTrue(json.contains("Chicago"));
        }
    }
}
