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
                deptName: String[1];
                deptLocation: String[1];
                orgName: String[1];
            }

            Class model::Organization
            {
                name: String[1];
            }

            Class model::Department
            {
                name: String[1];
                location: String[1];
            }

            Class model::Project
            {
                name: String[1];
                budget: Integer[1];
            }

            Association model::EmpDept
            {
                employees: Employee[*];
                department: Department[1];
            }

            Association model::EmpProject
            {
                employee: Employee[1];
                projects: Project[*];
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

            // ===== L4: 4-hop M2M =====
            Class model::Badge
            {
                text: String[1];
            }

            // ===== Shared classes =====
            Class model::DeptInfo
            {
                name: String[1];
                location: String[1];
            }

            // ===== Traverse-column M2M classes =====
            Class model::StaffDetail
            {
                fullName: String[1];
                deptName: String[1];
                deptLocation: String[1];
            }

            Class model::StaffProfile
            {
                fullName: String[1];
                orgName: String[1];
            }

            Class model::StaffFull
            {
                fullName: String[1];
                deptName: String[1];
                department: model::DeptInfo[*];
            }

            // ===== Deep fetch classes =====

            Class model::StaffWithDept
            {
                fullName: String[1];
                department: model::DeptInfo[*];
            }

            Class model::ProjectInfo
            {
                name: String[1];
                budget: Integer[1];
            }

            Class model::StaffComplete
            {
                fullName: String[1];
                department: model::DeptInfo[*];
                projects: model::ProjectInfo[*];
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
                    LOCATION VARCHAR(100) NOT NULL,
                    ORG_ID INTEGER
                )
                Table T_ORGANIZATION
                (
                    ID INTEGER PRIMARY KEY,
                    NAME VARCHAR(100) NOT NULL
                )
                Table T_PROJECT
                (
                    ID INTEGER PRIMARY KEY,
                    NAME VARCHAR(100) NOT NULL,
                    BUDGET INTEGER NOT NULL,
                    EMP_ID INTEGER NOT NULL
                )
                Join EmpDept(T_EMPLOYEE.DEPT_ID = T_DEPARTMENT.ID)
                Join DeptOrg(T_DEPARTMENT.ORG_ID = T_ORGANIZATION.ID)
                Join EmpProject(T_EMPLOYEE.ID = T_PROJECT.EMP_ID)
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
                    department: [CompanyDB] @EmpDept,
                    deptName: [CompanyDB] @EmpDept | T_DEPARTMENT.NAME,
                    deptLocation: [CompanyDB] @EmpDept | T_DEPARTMENT.LOCATION,
                    orgName: [CompanyDB] @EmpDept > @DeptOrg | T_ORGANIZATION.NAME
                }
                Department: Relational
                {
                    ~mainTable [CompanyDB] T_DEPARTMENT
                    name: [CompanyDB] T_DEPARTMENT.NAME,
                    location: [CompanyDB] T_DEPARTMENT.LOCATION,
                    employees: [CompanyDB] @EmpDept
                }
                Project: Relational
                {
                    ~mainTable [CompanyDB] T_PROJECT
                    name: [CompanyDB] T_PROJECT.NAME,
                    budget: [CompanyDB] T_PROJECT.BUDGET,
                    employee: [CompanyDB] @EmpProject
                }
            
                model::EmpDept: Relational { AssociationMapping ( employees: [store::CompanyDB]@EmpDept, department: [store::CompanyDB]@EmpDept ) }
                model::EmpProject: Relational { AssociationMapping ( employee: [store::CompanyDB]@EmpProject, projects: [store::CompanyDB]@EmpProject ) }
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

            // ===== L4 M2M mapping (4-hop) =====
            Mapping model::BadgeMapping
            (
                Badge: Pure
                {
                    ~src DirectoryEntry
                    text: '[' + $src.entry + ']'
                }
            )

            // ===== Traverse-column M2M mappings =====
            Mapping model::StaffDetailMapping
            (
                StaffDetail: Pure
                {
                    ~src Employee
                    fullName: $src.firstName + ' ' + $src.lastName,
                    deptName: $src.deptName,
                    deptLocation: $src.deptLocation
                }
            )

            Mapping model::StaffProfileMapping
            (
                StaffProfile: Pure
                {
                    ~src Employee
                    fullName: $src.firstName + ' ' + $src.lastName,
                    orgName: $src.orgName
                }
            )

            Mapping model::StaffFullMapping
            (
                StaffFull: Pure
                {
                    ~src Employee
                    fullName: $src.firstName + ' ' + $src.lastName,
                    deptName: $src.deptName,
                    department: $src.department
                }
                DeptInfo: Pure
                {
                    ~src Department
                    name: $src.name,
                    location: $src.location
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

            // ===== Disjoint 1-to-many M2M mapping =====
            Mapping model::DisjointDeepFetchMapping
            (
                StaffComplete: Pure
                {
                    ~src Employee
                    fullName: $src.firstName + ' ' + $src.lastName,
                    department: $src.department,
                    projects: $src.projects
                }
                DeptInfo: Pure
                {
                    ~src Department
                    name: $src.name,
                    location: $src.location
                }
                ProjectInfo: Pure
                {
                    ~src Project
                    name: $src.name,
                    budget: $src.budget
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
                    model::BadgeMapping,
                    model::DeepFetchMapping,
                    model::DisjointDeepFetchMapping,
                    model::StaffDetailMapping,
                    model::StaffProfileMapping,
                    model::StaffFullMapping
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
                        LOCATION VARCHAR(100),
                        ORG_ID INTEGER
                    )""");
            s.execute("INSERT INTO T_DEPARTMENT VALUES (1, 'Engineering', 'San Francisco', 1)");
            s.execute("INSERT INTO T_DEPARTMENT VALUES (2, 'Sales', 'New York', 1)");
            s.execute("INSERT INTO T_DEPARTMENT VALUES (3, 'Marketing', 'Chicago', 2)");

            s.execute("""
                    CREATE TABLE T_ORGANIZATION (
                        ID INTEGER PRIMARY KEY,
                        NAME VARCHAR(100)
                    )""");
            s.execute("INSERT INTO T_ORGANIZATION VALUES (1, 'Acme Corp')");
            s.execute("INSERT INTO T_ORGANIZATION VALUES (2, 'Globex Inc')");

            s.execute("""
                    CREATE TABLE T_PROJECT (
                        ID INTEGER PRIMARY KEY,
                        NAME VARCHAR(100),
                        BUDGET INTEGER,
                        EMP_ID INTEGER
                    )""");
            // Alice: 2 projects, Bob: 1 project, Carol: 0 projects, Dave: 1 project
            s.execute("INSERT INTO T_PROJECT VALUES (1, 'Alpha',  500000, 1)");
            s.execute("INSERT INTO T_PROJECT VALUES (2, 'Beta',   300000, 1)");
            s.execute("INSERT INTO T_PROJECT VALUES (3, 'Gamma',  750000, 2)");
            s.execute("INSERT INTO T_PROJECT VALUES (4, 'Delta', 1200000, 4)");
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

    // ==================== L4: 4-hop M2M chain ====================

    @Nested
    @DisplayName("L4: Four-hop M2M chains")
    class L4FourHop {

        @Test
        @DisplayName("project(): Badge through DirectoryEntry → StaffCard → StaffMember → Employee → Table")
        void testProjectFourHop() throws SQLException {
            var r = exec("Badge.all()->project(~[text:x|$x.text])");
            assertEquals(4, r.rowCount());
            var texts = col(r, 0);
            assertTrue(texts.contains("[ALICE SMITH - Engineering]"));
            assertTrue(texts.contains("[BOB JONES - Sales]"));
            assertTrue(texts.contains("[CAROL WHITE - Engineering]"));
            assertTrue(texts.contains("[DAVE BROWN - Marketing]"));
        }

        @Test
        @DisplayName("graphFetch(): Badge 4-hop")
        void testGraphFetchFourHop() throws SQLException {
            var json = execGraph("""
                    Badge.all()
                        ->graphFetch(#{ Badge { text } }#)
                        ->serialize(#{ Badge { text } }#)
                    """);
            assertTrue(json.contains("[ALICE SMITH - Engineering]"));
            assertTrue(json.contains("[BOB JONES - Sales]"));
        }

        @Test
        @DisplayName("project(): 4-hop + user query filter")
        void testProjectFourHopWithFilter() throws SQLException {
            var r = exec("Badge.all()->filter({x|$x.text->contains('Marketing')})->project(~[text:x|$x.text])");
            assertEquals(1, r.rowCount());
            assertEquals("[DAVE BROWN - Marketing]", col(r, 0).get(0));
        }
    }

    // ==================== Edge Cases ====================

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        @DisplayName("filter eliminates all rows")
        void testFilterEliminatesAll() throws SQLException {
            var r = exec("StaffMember.all()->filter({x|$x.age > 100})->project(~[fullName:x|$x.fullName])");
            assertEquals(0, r.rowCount());
        }

        @Test
        @DisplayName("filter keeps exactly one row")
        void testFilterKeepsOne() throws SQLException {
            var r = exec("StaffMember.all()->filter({x|$x.firstName == 'Alice'})->project(~[fullName:x|$x.fullName])");
            assertEquals(1, r.rowCount());
            assertEquals("Alice Smith", col(r, 0).get(0));
        }

        @Test
        @DisplayName("compound filter with AND")
        void testCompoundFilter() throws SQLException {
            var r = exec("StaffMember.all()->filter({x|($x.age > 25) && ($x.dept == 'Engineering')})->project(~[fullName:x|$x.fullName])");
            assertEquals(2, r.rowCount());
            var names = col(r, 0);
            assertTrue(names.contains("Alice Smith"));
            assertTrue(names.contains("Carol White"));
        }

        @Test
        @DisplayName("sort through M2M chain")
        void testSortThroughChain() throws SQLException {
            var r = exec("StaffMember.all()->sort({x|$x.age})->project(~[fullName:x|$x.fullName, age:x|$x.age])");
            assertEquals(4, r.rowCount());
            assertEquals("Carol White", col(r, 0).get(0));
            assertEquals("Dave Brown", col(r, 0).get(3));
        }

        @Test
        @DisplayName("2-hop + sort")
        void testTwoHopSort() throws SQLException {
            var r = exec("StaffCard.all()->sort({x|$x.displayName})->project(~[displayName:x|$x.displayName])");
            assertEquals(4, r.rowCount());
            assertEquals("ALICE SMITH", col(r, 0).get(0));
            assertEquals("BOB JONES", col(r, 0).get(1));
        }
    }

    // ==================== Join Chains: M2M with traverse-column properties ====================

    @Nested
    @DisplayName("Join Chains: M2M referencing traverse-column properties")
    class JoinChainTests {

        @Test
        @DisplayName("project(): single-hop traverse columns through M2M")
        void testProjectSingleHopTraverse() throws SQLException {
            var r = exec("StaffDetail.all()->project(~[fullName:x|$x.fullName, deptName:x|$x.deptName, deptLocation:x|$x.deptLocation])");
            assertEquals(4, r.rowCount());
            var names = col(r, 0);
            var depts = col(r, 1);
            var locs  = col(r, 2);
            assertTrue(names.contains("Alice Smith"));
            assertTrue(depts.contains("Engineering"));
            assertTrue(locs.contains("San Francisco"));
            assertTrue(depts.contains("Sales"));
            assertTrue(locs.contains("New York"));
        }

        @Test
        @DisplayName("graphFetch(): single-hop traverse columns through M2M")
        void testGraphFetchSingleHopTraverse() throws SQLException {
            var json = execGraph("""
                    StaffDetail.all()
                        ->graphFetch(#{ StaffDetail { fullName, deptName, deptLocation } }#)
                        ->serialize(#{ StaffDetail { fullName, deptName, deptLocation } }#)
                    """);
            assertTrue(json.contains("Alice Smith"));
            assertTrue(json.contains("Engineering"));
            assertTrue(json.contains("San Francisco"));
            assertTrue(json.contains("Bob Jones"));
            assertTrue(json.contains("New York"));
        }

        @Test
        @DisplayName("project(): filter on traverse column through M2M")
        void testProjectTraverseWithFilter() throws SQLException {
            var r = exec("StaffDetail.all()->filter({x|$x.deptLocation == 'San Francisco'})->project(~[fullName:x|$x.fullName, deptName:x|$x.deptName])");
            assertEquals(2, r.rowCount(), "Alice and Carol are in Engineering (San Francisco)");
            var names = col(r, 0);
            assertTrue(names.contains("Alice Smith"));
            assertTrue(names.contains("Carol White"));
        }

        @Test
        @DisplayName("project(): multi-hop traverse column through M2M")
        void testProjectMultiHopTraverse() throws SQLException {
            var r = exec("StaffProfile.all()->project(~[fullName:x|$x.fullName, orgName:x|$x.orgName])");
            assertEquals(4, r.rowCount());
            var orgs = col(r, 1);
            // Eng & Sales → Acme Corp (org 1), Marketing → Globex Inc (org 2)
            assertTrue(orgs.contains("Acme Corp"));
            assertTrue(orgs.contains("Globex Inc"));
        }

        @Test
        @DisplayName("graphFetch(): mixed scalar traverse + association in same M2M")
        void testGraphFetchMixedTraverseAndAssociation() throws SQLException {
            var json = execGraph("""
                    StaffFull.all()
                        ->graphFetch(#{ StaffFull { fullName, deptName, department { name, location } } }#)
                        ->serialize(#{ StaffFull { fullName, deptName, department { name, location } } }#)
                    """);
            // Scalar traverse column
            assertTrue(json.contains("Engineering"));
            assertTrue(json.contains("Sales"));
            // Association navigation (nested objects)
            assertTrue(json.contains("San Francisco"));
            assertTrue(json.contains("New York"));
            assertTrue(json.contains("Alice Smith"));
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

        // ---------- Disjoint 1-to-many: department (via @EmpDept) + projects (via @EmpProject) ----------

        @Test
        @DisplayName("graphFetch(): 2 disjoint 1-to-many — department + projects")
        void testDisjointOneToMany() throws SQLException {
            var json = execGraph("""
                    StaffComplete.all()
                        ->graphFetch(#{ StaffComplete { fullName, department { name }, projects { name, budget } } }#)
                        ->serialize(#{ StaffComplete { fullName, department { name }, projects { name, budget } } }#)
                    """);
            // Alice has 2 projects + Engineering dept
            assertTrue(json.contains("Alice Smith"));
            assertTrue(json.contains("Alpha"));
            assertTrue(json.contains("Beta"));
            assertTrue(json.contains("Engineering"));
            // Bob has 1 project + Sales dept
            assertTrue(json.contains("Bob Jones"));
            assertTrue(json.contains("Gamma"));
            assertTrue(json.contains("Sales"));
            // Dave has 1 project + Marketing dept
            assertTrue(json.contains("Dave Brown"));
            assertTrue(json.contains("Delta"));
            assertTrue(json.contains("Marketing"));
        }

        @Test
        @DisplayName("graphFetch(): disjoint 1-to-many — only dept (no project subquery)")
        void testDisjointOnlyDept() throws SQLException {
            var json = execGraph("""
                    StaffComplete.all()
                        ->graphFetch(#{ StaffComplete { fullName, department { name, location } } }#)
                        ->serialize(#{ StaffComplete { fullName, department { name, location } } }#)
                    """);
            assertTrue(json.contains("Engineering"));
            assertTrue(json.contains("San Francisco"));
            assertFalse(json.contains("Alpha"), "projects not requested");
        }

        @Test
        @DisplayName("graphFetch(): disjoint 1-to-many — only projects (no dept subquery)")
        void testDisjointOnlyProjects() throws SQLException {
            var json = execGraph("""
                    StaffComplete.all()
                        ->graphFetch(#{ StaffComplete { fullName, projects { name, budget } } }#)
                        ->serialize(#{ StaffComplete { fullName, projects { name, budget } } }#)
                    """);
            assertTrue(json.contains("Alpha"));
            assertTrue(json.contains("500000"));
            assertFalse(json.contains("Engineering"), "department not requested");
        }

        @Test
        @DisplayName("graphFetch(): disjoint 1-to-many — root only (0 subqueries)")
        void testDisjointRootOnly() throws SQLException {
            var json = execGraph("""
                    StaffComplete.all()
                        ->graphFetch(#{ StaffComplete { fullName } }#)
                        ->serialize(#{ StaffComplete { fullName } }#)
                    """);
            assertTrue(json.contains("Alice Smith"));
            assertTrue(json.contains("Bob Jones"));
            assertFalse(json.contains("Engineering"), "department not requested");
            assertFalse(json.contains("Alpha"), "projects not requested");
        }

        @Test
        @DisplayName("graphFetch(): disjoint 1-to-many — employee with 0 projects")
        void testDisjointZeroProjects() throws SQLException {
            var json = execGraph("""
                    StaffComplete.all()
                        ->graphFetch(#{ StaffComplete { fullName, projects { name } } }#)
                        ->serialize(#{ StaffComplete { fullName, projects { name } } }#)
                    """);
            // Carol has 0 projects — should still appear with empty projects array
            assertTrue(json.contains("Carol White"));
        }

        @Test
        @DisplayName("graphFetch(): disjoint 1-to-many — Alice has 2 projects")
        void testDisjointMultipleProjects() throws SQLException {
            var json = execGraph("""
                    StaffComplete.all()
                        ->filter({x|$x.fullName == 'Alice Smith'})
                        ->graphFetch(#{ StaffComplete { fullName, projects { name, budget } } }#)
                        ->serialize(#{ StaffComplete { fullName, projects { name, budget } } }#)
                    """);
            assertTrue(json.contains("Alice Smith"));
            assertTrue(json.contains("Alpha"));
            assertTrue(json.contains("Beta"));
            assertTrue(json.contains("500000"));
            assertTrue(json.contains("300000"));
        }
    }
}
