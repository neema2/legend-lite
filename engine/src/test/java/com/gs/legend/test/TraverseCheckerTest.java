package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.server.QueryService;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for association navigation via traverse().
 *
 * <p>Tests the full pipeline: Parser -> MappingNormalizer -> TypeChecker (TraverseChecker)
 * -> MappingResolver -> PlanGenerator -> DuckDB execution.
 *
 * <p>Covers:
 * <ul>
 *   <li>1a. Basic to-one association navigation</li>
 *   <li>1b. To-many association navigation (LEFT JOIN, EXISTS)</li>
 *   <li>1c. Multi-hop navigation (Person->Dept->Org)</li>
 *   <li>1d. Multiple associations in one query</li>
 *   <li>1e. Composition with Relation API (filter, sort, groupBy, distinct, limit, extend)</li>
 *   <li>1f. Composition with mapping features (~filter, ~distinct, join chains, AssociationMapping)</li>
 *   <li>1g. Compound join conditions</li>
 *   <li>1h. Edge cases (empty tables, NULL FKs, circular models)</li>
 *   <li>1i. Demand-driven verification (no unnecessary JOINs)</li>
 *   <li>1j. Self-join (GAP)</li>
 * </ul>
 *
 * <p><b>Assertion Standards</b>: exact row counts, specific values, NULL where expected,
 * no row explosion for to-many filters.
 */
@DisplayName("TraverseChecker — Association Navigation Tests")
class TraverseCheckerTest {

    private Connection conn;
    private final QueryService qs = new QueryService();

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) conn.close();
    }

    // ==================== Helpers ====================

    private String withRuntime(String model, String dbName, String mappingName) {
        return model + """

                RelationalDatabaseConnection store::Conn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::RT { mappings: [ %s ]; connections: [ %s: [ environment: store::Conn ] ]; }
                """.formatted(mappingName, dbName);
    }

    private ExecutionResult exec(String model, String query) throws SQLException {
        return qs.execute(model, query, "test::RT", conn);
    }

    private void sql(String... statements) throws SQLException {
        try (Statement s = conn.createStatement()) {
            for (String stmt : statements) s.execute(stmt);
        }
    }

    private List<String> colStr(ExecutionResult r, int idx) {
        return r.rows().stream().map(row -> row.get(idx) != null ? row.get(idx).toString() : null)
                .collect(Collectors.toList());
    }

    private List<Integer> colInt(ExecutionResult r, int idx) {
        return r.rows().stream().map(row -> row.get(idx) != null ? ((Number) row.get(idx)).intValue() : null)
                .collect(Collectors.toList());
    }

    // ==================== Shared Test Data ====================

    /**
     * Shared 5-table setup:
     * <pre>
     * T_PERSON:  (1,Alice,firm1,dept1), (2,Bob,firm1,dept2), (3,Charlie,firm2,dept1), (4,Diana,null,null)
     * T_FIRM:    (1,Acme), (2,Globex)
     * T_DEPT:    (1,Engineering,org1), (2,Sales,org1)
     * T_ORG:     (1,Acme Corp)
     * T_ADDRESS: (1,person1,NYC), (2,person1,LA), (3,person2,Chicago)
     * </pre>
     * Diana has NULL firm_id and dept_id -> tests LEFT JOIN / NULL FK.
     * Alice has 2 addresses -> tests to-many expansion and EXISTS non-explosion.
     * Charlie has no addresses -> tests LEFT JOIN NULL.
     */
    private void setupTables() throws SQLException {
        sql("CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INT, DEPT_ID INT)",
            "CREATE TABLE T_FIRM (ID INT PRIMARY KEY, LEGAL_NAME VARCHAR(100))",
            "CREATE TABLE T_DEPT (ID INT PRIMARY KEY, NAME VARCHAR(50), ORG_ID INT)",
            "CREATE TABLE T_ORG (ID INT PRIMARY KEY, NAME VARCHAR(100))",
            "CREATE TABLE T_ADDRESS (ID INT PRIMARY KEY, PERSON_ID INT, CITY VARCHAR(100))",
            "INSERT INTO T_ORG VALUES (1, 'Acme Corp')",
            "INSERT INTO T_FIRM VALUES (1, 'Acme'), (2, 'Globex')",
            "INSERT INTO T_DEPT VALUES (1, 'Engineering', 1), (2, 'Sales', 1)",
            "INSERT INTO T_PERSON VALUES (1, 'Alice', 1, 1), (2, 'Bob', 1, 2), (3, 'Charlie', 2, 1), (4, 'Diana', NULL, NULL)",
            "INSERT INTO T_ADDRESS VALUES (1, 1, 'NYC'), (2, 1, 'LA'), (3, 2, 'Chicago')");
    }

    /**
     * Full model with 5 classes, 4 associations, database with joins, and mappings.
     */
    private String fullModel() {
        return withRuntime("""
                Class test::Person { id: Integer[1]; name: String[1]; }
                Class test::Firm { id: Integer[1]; legalName: String[1]; }
                Class test::Dept { id: Integer[1]; name: String[1]; }
                Class test::Org { id: Integer[1]; name: String[1]; }
                Class test::Address { id: Integer[1]; city: String[1]; }
                Association test::PersonFirm { firm: Firm[0..1]; employees: Person[*]; }
                Association test::PersonDept { dept: Dept[0..1]; members: Person[*]; }
                Association test::PersonAddress { addresses: Address[*]; person: Person[1]; }
                Association test::DeptOrg { org: Org[1]; departments: Dept[*]; }
                Database store::DB
                (
                    Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INTEGER, DEPT_ID INTEGER)
                    Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))
                    Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), ORG_ID INTEGER)
                    Table T_ORG (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                    Table T_ADDRESS (ID INTEGER PRIMARY KEY, PERSON_ID INTEGER, CITY VARCHAR(100))
                    Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)
                    Join PersonDept(T_PERSON.DEPT_ID = T_DEPT.ID)
                    Join PersonAddress(T_PERSON.ID = T_ADDRESS.PERSON_ID)
                    Join DeptOrg(T_DEPT.ORG_ID = T_ORG.ID)
                )
                Mapping test::M
                (
                    Person: Relational
                    {
                        ~mainTable [store::DB] T_PERSON
                        id: [store::DB] T_PERSON.ID,
                        name: [store::DB] T_PERSON.NAME
                    }
                    Firm: Relational
                    {
                        ~mainTable [store::DB] T_FIRM
                        id: [store::DB] T_FIRM.ID,
                        legalName: [store::DB] T_FIRM.LEGAL_NAME
                    }
                    Dept: Relational
                    {
                        ~mainTable [store::DB] T_DEPT
                        id: [store::DB] T_DEPT.ID,
                        name: [store::DB] T_DEPT.NAME
                    }
                    Org: Relational
                    {
                        ~mainTable [store::DB] T_ORG
                        id: [store::DB] T_ORG.ID,
                        name: [store::DB] T_ORG.NAME
                    }
                    Address: Relational
                    {
                        ~mainTable [store::DB] T_ADDRESS
                        id: [store::DB] T_ADDRESS.ID,
                        city: [store::DB] T_ADDRESS.CITY
                    }
                    test::PersonFirm: AssociationMapping
                    (
                        employees: [store::DB]@PersonFirm,
                        firm: [store::DB]@PersonFirm
                    )
                    test::PersonDept: AssociationMapping
                    (
                        members: [store::DB]@PersonDept,
                        dept: [store::DB]@PersonDept
                    )
                    test::PersonAddress: AssociationMapping
                    (
                        person: [store::DB]@PersonAddress,
                        addresses: [store::DB]@PersonAddress
                    )
                    test::DeptOrg: AssociationMapping
                    (
                        departments: [store::DB]@DeptOrg,
                        org: [store::DB]@DeptOrg
                    )
                )
                """, "store::DB", "test::M");
    }

    // ==================== 1a. Basic To-One Association ====================

    @Nested
    @DisplayName("1a. Basic To-One Association")
    class ToOneBasic {

        @BeforeEach
        void setup() throws SQLException { setupTables(); }

        @Test
        @DisplayName("Project through to-one: Person -> Firm")
        void testProjectToOne() throws SQLException {
            var r = exec(fullModel(), "Person.all()->project([x|$x.name, x|$x.firm.legalName], ['name', 'firm'])");
            assertEquals(4, r.rows().size());
            Map<String, String> map = new HashMap<>();
            for (var row : r.rows()) map.put(row.get(0).toString(), row.get(1) != null ? row.get(1).toString() : null);
            assertEquals("Acme", map.get("Alice"));
            assertEquals("Acme", map.get("Bob"));
            assertEquals("Globex", map.get("Charlie"));
            assertNull(map.get("Diana"));
        }

        @Test
        @DisplayName("Filter on to-one: firm.legalName == 'Acme'")
        void testFilterToOne() throws SQLException {
            var r = exec(fullModel(), "Person.all()->filter({p|$p.firm.legalName == 'Acme'})->project(~[name:p|$p.name])");
            assertEquals(2, r.rowCount());
            var names = colStr(r, 0);
            assertTrue(names.contains("Alice"));
            assertTrue(names.contains("Bob"));
            assertFalse(names.contains("Charlie"));
            assertFalse(names.contains("Diana"));
        }

        @Test
        @DisplayName("Project local + association in same projection")
        void testProjectLocalAndAssoc() throws SQLException {
            var r = exec(fullModel(), "Person.all()->project([x|$x.name, x|$x.id, x|$x.firm.legalName], ['name', 'id', 'firm'])");
            assertEquals(4, r.rows().size());
            Map<String, String> nameFirm = new HashMap<>();
            Map<String, Integer> nameId = new HashMap<>();
            for (var row : r.rows()) {
                String name = row.get(0).toString();
                nameFirm.put(name, row.get(2) != null ? row.get(2).toString() : null);
                nameId.put(name, ((Number) row.get(1)).intValue());
            }
            assertEquals("Acme", nameFirm.get("Alice"));
            assertEquals(Integer.valueOf(1), nameId.get("Alice"));
            assertNull(nameFirm.get("Diana"));
        }

        @Test
        @DisplayName("Multiple to-one from same class: Person -> Firm AND Person -> Dept")
        void testMultipleToOne() throws SQLException {
            var r = exec(fullModel(), "Person.all()->project([x|$x.name, x|$x.firm.legalName, x|$x.dept.name], ['name', 'firm', 'dept'])");
            assertEquals(4, r.rows().size());
            Map<String, String> firms = new HashMap<>();
            Map<String, String> depts = new HashMap<>();
            for (var row : r.rows()) {
                String name = row.get(0).toString();
                firms.put(name, row.get(1) != null ? row.get(1).toString() : null);
                depts.put(name, row.get(2) != null ? row.get(2).toString() : null);
            }
            assertEquals("Acme", firms.get("Alice"));
            assertEquals("Engineering", depts.get("Alice"));
            assertEquals("Acme", firms.get("Bob"));
            assertEquals("Sales", depts.get("Bob"));
            assertEquals("Globex", firms.get("Charlie"));
            assertEquals("Engineering", depts.get("Charlie"));
            assertNull(firms.get("Diana"));
            assertNull(depts.get("Diana"));
        }
    }

    // ==================== 1b. To-Many Association ====================

    @Nested
    @DisplayName("1b. To-Many Association")
    class ToMany {

        @BeforeEach
        void setup() throws SQLException { setupTables(); }

        @Test
        @DisplayName("Project through to-many: Person -> Address (LEFT JOIN row expansion)")
        void testProjectToMany() throws SQLException {
            var r = exec(fullModel(), "Person.all()->project([x|$x.name, x|$x.addresses.city], ['name', 'city'])");
            // Alice: 2 addresses (NYC, LA), Bob: 1 (Chicago), Charlie: 0 (NULL), Diana: 0 (NULL)
            assertEquals(5, r.rows().size());
            Map<String, List<String>> nameCity = new HashMap<>();
            for (var row : r.rows()) {
                String name = row.get(0).toString();
                nameCity.computeIfAbsent(name, k -> new ArrayList<>())
                        .add(row.get(1) != null ? row.get(1).toString() : null);
            }
            assertEquals(2, nameCity.get("Alice").size());
            assertTrue(nameCity.get("Alice").contains("NYC"));
            assertTrue(nameCity.get("Alice").contains("LA"));
            assertEquals(List.of("Chicago"), nameCity.get("Bob"));
            assertEquals(1, nameCity.get("Charlie").size());
            assertNull(nameCity.get("Charlie").get(0));
        }

        @Test
        @DisplayName("Filter to-many EXISTS: no row explosion")
        void testFilterToManyExists() throws SQLException {
            var r = exec(fullModel(), "Person.all()->filter({p|$p.addresses.city == 'NYC'})->project(~[name:p|$p.name])");
            // Alice has NYC address — should appear exactly ONCE (EXISTS, not JOIN)
            assertEquals(1, r.rowCount());
            assertEquals("Alice", colStr(r, 0).get(0));
        }

        @Test
        @DisplayName("No-match LEFT JOIN produces NULL")
        void testToManyNoMatch() throws SQLException {
            var r = exec(fullModel(), "Person.all()->filter({p|$p.name == 'Charlie'})->project([x|$x.name, x|$x.addresses.city], ['name', 'city'])");
            assertEquals(1, r.rows().size());
            assertEquals("Charlie", r.rows().get(0).get(0).toString());
            assertNull(r.rows().get(0).get(1));
        }

        @Test
        @DisplayName("Filter to-many: multiple matches still returns person once")
        void testFilterToManyMultipleMatchesOnePerson() throws SQLException {
            // Alice has addresses in NYC and LA — filter city in ('NYC', 'LA') should return Alice once
            var r = exec(fullModel(), "Person.all()->filter({p|$p.addresses.city == 'NYC' || $p.addresses.city == 'LA'})->project(~[name:p|$p.name])");
            var names = colStr(r, 0);
            assertEquals(1, Collections.frequency(names, "Alice"), "Alice should appear exactly once");
        }
    }

    // ==================== 1c. Multi-Hop Navigation ====================

    @Nested
    @DisplayName("1c. Multi-Hop Navigation")
    class MultiHop {

        @BeforeEach
        void setup() throws SQLException { setupTables(); }

        @Test
        @DisplayName("Two-hop to-one: Person -> Dept -> Org")
        void testTwoHopToOne() throws SQLException {
            var r = exec(fullModel(), "Person.all()->project([x|$x.name, x|$x.dept.org.name], ['name', 'org'])");
            assertEquals(4, r.rows().size());
            Map<String, String> map = new HashMap<>();
            for (var row : r.rows()) map.put(row.get(0).toString(), row.get(1) != null ? row.get(1).toString() : null);
            assertEquals("Acme Corp", map.get("Alice"));
            assertEquals("Acme Corp", map.get("Bob"));
            assertEquals("Acme Corp", map.get("Charlie"));
            assertNull(map.get("Diana"));
        }

        @Test
        @DisplayName("Two-hop with filter: dept.org.name == 'Acme Corp'")
        void testTwoHopFilter() throws SQLException {
            var r = exec(fullModel(), "Person.all()->filter({p|$p.dept.org.name == 'Acme Corp'})->project(~[name:p|$p.name])");
            assertEquals(3, r.rowCount());
            var names = colStr(r, 0);
            assertTrue(names.contains("Alice"));
            assertTrue(names.contains("Bob"));
            assertTrue(names.contains("Charlie"));
            assertFalse(names.contains("Diana"));
        }

        @Test
        @DisplayName("To-one then to-many: Person -> Dept, then Dept -> members (reverse)")
        void testToOneThenToMany() throws SQLException {
            // Navigate to dept, then navigate dept's properties
            var r = exec(fullModel(), "Person.all()->project([x|$x.name, x|$x.dept.name], ['name', 'deptName'])");
            assertEquals(4, r.rows().size());
            Map<String, String> map = new HashMap<>();
            for (var row : r.rows()) map.put(row.get(0).toString(), row.get(1) != null ? row.get(1).toString() : null);
            assertEquals("Engineering", map.get("Alice"));
            assertEquals("Sales", map.get("Bob"));
            assertEquals("Engineering", map.get("Charlie"));
            assertNull(map.get("Diana"));
        }

        @Test
        @DisplayName("Filter on two-hop, project local")
        void testFilterTwoHopProjectLocal() throws SQLException {
            var r = exec(fullModel(), "Person.all()->filter({p|$p.dept.org.name == 'Acme Corp'})->project(~[name:p|$p.name, id:p|$p.id])");
            assertEquals(3, r.rowCount());
            Map<String, Integer> nameId = new HashMap<>();
            for (var row : r.rows()) nameId.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(Integer.valueOf(1), nameId.get("Alice"));
            assertEquals(Integer.valueOf(2), nameId.get("Bob"));
            assertEquals(Integer.valueOf(3), nameId.get("Charlie"));
        }
    }

    // ==================== 1d. Multiple Associations in One Query ====================

    @Nested
    @DisplayName("1d. Multiple Associations in One Query")
    class MultipleAssoc {

        @BeforeEach
        void setup() throws SQLException { setupTables(); }

        @Test
        @DisplayName("To-one + to-many: project firm AND addresses")
        void testToOneAndToMany() throws SQLException {
            var r = exec(fullModel(), "Person.all()->project([x|$x.name, x|$x.firm.legalName, x|$x.addresses.city], ['name', 'firm', 'city'])");
            // Alice: 2 address rows, Bob: 1, Charlie: 1 (NULL city), Diana: 1 (NULL both)
            assertEquals(5, r.rows().size());
            Map<String, Set<String>> firmsByName = new HashMap<>();
            for (var row : r.rows()) {
                String name = row.get(0).toString();
                firmsByName.computeIfAbsent(name, k -> new HashSet<>())
                        .add(row.get(1) != null ? row.get(1).toString() : null);
            }
            // Alice's firm should be consistently Acme across both address rows
            assertEquals(Set.of("Acme"), firmsByName.get("Alice"));
        }

        @Test
        @DisplayName("Filter on one assoc, project through another")
        void testFilterOneProjectAnother() throws SQLException {
            var r = exec(fullModel(), "Person.all()->filter({p|$p.firm.legalName == 'Acme'})->project([x|$x.name, x|$x.addresses.city], ['name', 'city'])");
            // Acme people: Alice (2 addresses) and Bob (1 address) = 3 rows
            assertEquals(3, r.rows().size());
            var names = colStr(r, 0);
            assertTrue(names.contains("Alice"));
            assertTrue(names.contains("Bob"));
            assertFalse(names.contains("Charlie"));
        }

        @Test
        @DisplayName("Two to-one filters ANDed: firm AND dept")
        void testTwoToOneFilters() throws SQLException {
            var r = exec(fullModel(), "Person.all()->filter({p|$p.firm.legalName == 'Acme' && $p.dept.name == 'Engineering'})->project(~[name:p|$p.name])");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", colStr(r, 0).get(0));
        }
    }

    // ==================== 1e. Composition with Relation API ====================

    @Nested
    @DisplayName("1e. Composition with Relation API")
    class CompositionRelationApi {

        @BeforeEach
        void setup() throws SQLException { setupTables(); }

        @Test
        @DisplayName("Filter local + project association")
        void testFilterLocalProjectAssoc() throws SQLException {
            var r = exec(fullModel(), "Person.all()->filter({p|$p.name == 'Alice'})->project([x|$x.name, x|$x.firm.legalName], ['name', 'firm'])");
            assertEquals(1, r.rows().size());
            assertEquals("Alice", r.rows().get(0).get(0).toString());
            assertEquals("Acme", r.rows().get(0).get(1).toString());
        }

        @Test
        @DisplayName("Sort by association property")
        void testSortByAssociation() throws SQLException {
            var r = exec(fullModel(), "Person.all()->project(~[firm:x|$x.firm.legalName, name:x|$x.name])->sort(~firm->ascending())");
            assertEquals(4, r.rowCount());
            var firms = colStr(r, 0);
            // Acme (Alice, Bob), Globex (Charlie), NULL (Diana) — sorted ASC NULLS LAST
            assertEquals("Acme", firms.get(0));
            assertEquals("Acme", firms.get(1));
            assertEquals("Globex", firms.get(2));
            assertNull(firms.get(3));
        }

        @Test
        @DisplayName("GroupBy association property with count")
        void testGroupByAssociation() throws SQLException {
            var r = exec(fullModel(), "Person.all()->project(~[firm:x|$x.firm.legalName, name:x|$x.name])->groupBy(~firm, ~cnt:x|$x.name:y|$y->count())->sort(~firm->ascending())");
            assertEquals(3, r.rowCount());
            Map<String, Integer> firmCount = new HashMap<>();
            for (var row : r.rows()) firmCount.put(
                    row.get(0) != null ? row.get(0).toString() : null,
                    ((Number) row.get(1)).intValue());
            assertEquals(Integer.valueOf(2), firmCount.get("Acme"));
            assertEquals(Integer.valueOf(1), firmCount.get("Globex"));
            assertEquals(Integer.valueOf(1), firmCount.get(null));
        }

        @Test
        @DisplayName("Distinct on association projection")
        void testDistinctAssociation() throws SQLException {
            var r = exec(fullModel(), "Person.all()->project(~[firm:x|$x.firm.legalName])->distinct()");
            // 3 distinct values: Acme, Globex, NULL
            assertEquals(3, r.rowCount());
            var firms = colStr(r, 0);
            assertTrue(firms.contains("Acme"));
            assertTrue(firms.contains("Globex"));
            assertTrue(firms.contains(null));
        }

        @Test
        @DisplayName("Limit after association project")
        void testLimitAssociation() throws SQLException {
            var r = exec(fullModel(), "Person.all()->project(~[name:x|$x.name, firm:x|$x.firm.legalName])->sort(~name->ascending())->limit(2)");
            assertEquals(2, r.rowCount());
            assertEquals("Alice", colStr(r, 0).get(0));
            assertEquals("Bob", colStr(r, 0).get(1));
        }

        @Test
        @DisplayName("Extend with computed from association")
        void testExtendFromAssociation() throws SQLException {
            var r = exec(fullModel(), "Person.all()->project(~[name:x|$x.name, firm:x|$x.firm.legalName])->extend(~upper:x|$x.firm->toUpper())->sort(~name->ascending())->limit(2)");
            assertEquals(2, r.rowCount());
            assertEquals("ACME", colStr(r, 2).get(0));
            assertEquals("ACME", colStr(r, 2).get(1));
        }

        @Test
        @DisplayName("Full pipeline: filter -> sort -> limit through association")
        void testFullPipeline() throws SQLException {
            var r = exec(fullModel(), "Person.all()->filter({p|$p.firm.legalName == 'Acme'})->project(~[name:x|$x.name, dept:x|$x.dept.name])->sort(~name->ascending())->limit(1)");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", colStr(r, 0).get(0));
            assertEquals("Engineering", colStr(r, 1).get(0));
        }
    }

    // ==================== 1f. Composition with Mapping Features ====================

    @Nested
    @DisplayName("1f. Composition with Mapping Features")
    class CompositionMappingFeatures {

        @BeforeEach
        void setup() throws SQLException { setupTables(); }

        @Test
        @DisplayName("Association + ~filter (mapping-level filter)")
        void testAssociationWithMappingFilter() throws SQLException {
            String model = withRuntime("""
                    Class test::Person { id: Integer[1]; name: String[1]; }
                    Class test::Firm { id: Integer[1]; legalName: String[1]; }
                    Association test::PersonFirm { firm: Firm[0..1]; employees: Person[*]; }
                    Database store::DB
                    (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INTEGER, DEPT_ID INTEGER)
                        Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))
                        Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)
                        Filter ActivePerson(T_PERSON.ID <= 3)
                    )
                    Mapping test::M
                    (
                        Person: Relational
                        {
                            ~filter [store::DB] ActivePerson
                            ~mainTable [store::DB] T_PERSON
                            id: [store::DB] T_PERSON.ID,
                            name: [store::DB] T_PERSON.NAME
                        }
                        Firm: Relational
                        {
                            ~mainTable [store::DB] T_FIRM
                            id: [store::DB] T_FIRM.ID,
                            legalName: [store::DB] T_FIRM.LEGAL_NAME
                        }
                        test::PersonFirm: AssociationMapping
                        (
                            employees: [store::DB]@PersonFirm,
                            firm: [store::DB]@PersonFirm
                        )
                    )
                    """, "store::DB", "test::M");

            var r = exec(model, "Person.all()->project([x|$x.name, x|$x.firm.legalName], ['name', 'firm'])");
            // ~filter excludes Diana (ID=4), so 3 rows
            assertEquals(3, r.rows().size());
            var names = colStr(r, 0);
            assertFalse(names.contains("Diana"));
        }

        @Test
        @DisplayName("Association + ~distinct")
        void testAssociationWithMappingDistinct() throws SQLException {
            String model = withRuntime("""
                    Class test::Person { id: Integer[1]; name: String[1]; }
                    Class test::Firm { id: Integer[1]; legalName: String[1]; }
                    Association test::PersonFirm { firm: Firm[0..1]; employees: Person[*]; }
                    Database store::DB
                    (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INTEGER, DEPT_ID INTEGER)
                        Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))
                        Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)
                    )
                    Mapping test::M
                    (
                        Person: Relational
                        {
                            ~distinct
                            ~mainTable [store::DB] T_PERSON
                            id: [store::DB] T_PERSON.ID,
                            name: [store::DB] T_PERSON.NAME
                        }
                        Firm: Relational
                        {
                            ~mainTable [store::DB] T_FIRM
                            id: [store::DB] T_FIRM.ID,
                            legalName: [store::DB] T_FIRM.LEGAL_NAME
                        }
                        test::PersonFirm: AssociationMapping
                        (
                            employees: [store::DB]@PersonFirm,
                            firm: [store::DB]@PersonFirm
                        )
                    )
                    """, "store::DB", "test::M");

            var r = exec(model, "Person.all()->project([x|$x.name, x|$x.firm.legalName], ['name', 'firm'])");
            assertEquals(4, r.rows().size());
        }

        @Test
        @DisplayName("Join chain property AND association nav in same query")
        void testJoinChainAndAssociation() throws SQLException {
            String model = withRuntime("""
                    Class test::Person { id: Integer[1]; name: String[1]; orgName: String[1]; }
                    Class test::Firm { id: Integer[1]; legalName: String[1]; }
                    Association test::PersonFirm { firm: Firm[0..1]; employees: Person[*]; }
                    Database store::DB
                    (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INTEGER, DEPT_ID INTEGER)
                        Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))
                        Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), ORG_ID INTEGER)
                        Table T_ORG (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                        Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)
                        Join Person_Dept(T_PERSON.DEPT_ID = T_DEPT.ID)
                        Join Dept_Org(T_DEPT.ORG_ID = T_ORG.ID)
                    )
                    Mapping test::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            id: [store::DB] T_PERSON.ID,
                            name: [store::DB] T_PERSON.NAME,
                            orgName: [store::DB] @Person_Dept > @Dept_Org | T_ORG.NAME
                        }
                        Firm: Relational
                        {
                            ~mainTable [store::DB] T_FIRM
                            id: [store::DB] T_FIRM.ID,
                            legalName: [store::DB] T_FIRM.LEGAL_NAME
                        }
                        test::PersonFirm: AssociationMapping
                        (
                            employees: [store::DB]@PersonFirm,
                            firm: [store::DB]@PersonFirm
                        )
                    )
                    """, "store::DB", "test::M");

            var r = exec(model, "Person.all()->project([x|$x.name, x|$x.orgName, x|$x.firm.legalName], ['name', 'org', 'firm'])");
            assertEquals(4, r.rows().size());
            Map<String, String> orgs = new HashMap<>();
            Map<String, String> firms = new HashMap<>();
            for (var row : r.rows()) {
                String name = row.get(0).toString();
                orgs.put(name, row.get(1) != null ? row.get(1).toString() : null);
                firms.put(name, row.get(2) != null ? row.get(2).toString() : null);
            }
            assertEquals("Acme Corp", orgs.get("Alice"));
            assertEquals("Acme", firms.get("Alice"));
            assertNull(orgs.get("Diana"));
            assertNull(firms.get("Diana"));
        }

        @Test
        @DisplayName("Explicit AssociationMapping: navigate both directions")
        void testExplicitAssocMappingBothDirections() throws SQLException {
            // Person -> Firm (to-one)
            var r1 = exec(fullModel(), "Person.all()->filter({p|$p.name == 'Alice'})->project([x|$x.name, x|$x.firm.legalName], ['name', 'firm'])");
            assertEquals(1, r1.rows().size());
            assertEquals("Acme", r1.rows().get(0).get(1).toString());

            // Firm -> employees (to-many, EXISTS filter)
            var r2 = exec(fullModel(), "Firm.all()->filter({f|$f.employees.name == 'Alice'})->project(~[legalName:f|$f.legalName])");
            assertEquals(1, r2.rowCount());
            assertEquals("Acme", colStr(r2, 0).get(0));
        }
    }

    // ==================== 1g. Compound Join Conditions ====================

    @Nested
    @DisplayName("1g. Compound Join Conditions")
    class CompoundJoins {

        @Test
        @DisplayName("Multi-column equi-join")
        void testMultiColumnEquiJoin() throws SQLException {
            sql("CREATE TABLE T_EMP (DEPT_ID INT, TEAM_ID INT, NAME VARCHAR(100))",
                "CREATE TABLE T_TEAM (DEPT_ID INT, TEAM_ID INT, TEAM_NAME VARCHAR(100))",
                "INSERT INTO T_TEAM VALUES (1, 1, 'Alpha'), (1, 2, 'Beta'), (2, 1, 'Gamma')",
                "INSERT INTO T_EMP VALUES (1, 1, 'Alice'), (1, 2, 'Bob'), (2, 1, 'Charlie'), (1, 3, 'Diana')");

            String model = withRuntime("""
                    Class test::Emp { name: String[1]; }
                    Class test::Team { teamName: String[1]; }
                    Association test::EmpTeam { team: Team[0..1]; members: Emp[*]; }
                    Database store::DB
                    (
                        Table T_EMP (DEPT_ID INTEGER, TEAM_ID INTEGER, NAME VARCHAR(100))
                        Table T_TEAM (DEPT_ID INTEGER, TEAM_ID INTEGER, TEAM_NAME VARCHAR(100))
                        Join EmpTeam(T_EMP.DEPT_ID = T_TEAM.DEPT_ID and T_EMP.TEAM_ID = T_TEAM.TEAM_ID)
                    )
                    Mapping test::M
                    (
                        Emp: Relational
                        {
                            ~mainTable [store::DB] T_EMP
                            name: [store::DB] T_EMP.NAME
                        }
                        Team: Relational
                        {
                            ~mainTable [store::DB] T_TEAM
                            teamName: [store::DB] T_TEAM.TEAM_NAME
                        }
                        test::EmpTeam: AssociationMapping
                        (
                            members: [store::DB]@EmpTeam,
                            team: [store::DB]@EmpTeam
                        )
                    )
                    """, "store::DB", "test::M");

            var r = exec(model, "Emp.all()->project([x|$x.name, x|$x.team.teamName], ['name', 'team'])");
            assertEquals(4, r.rows().size());
            Map<String, String> map = new HashMap<>();
            for (var row : r.rows()) map.put(row.get(0).toString(), row.get(1) != null ? row.get(1).toString() : null);
            assertEquals("Alpha", map.get("Alice"));
            assertEquals("Beta", map.get("Bob"));
            assertEquals("Gamma", map.get("Charlie"));
            assertNull(map.get("Diana")); // team_id=3 has no match
        }

        @Test
        @DisplayName("Join with AND + inequality filter in condition")
        void testJoinWithInequality() throws SQLException {
            sql("CREATE TABLE T_PARENT (ID INT, NAME VARCHAR(100))",
                "CREATE TABLE T_CHILD (ID INT, PARENT_ID INT, ACTIVE INT, NAME VARCHAR(100))",
                "INSERT INTO T_PARENT VALUES (1, 'Parent1'), (2, 'Parent2')",
                "INSERT INTO T_CHILD VALUES (1, 1, 1, 'ActiveChild'), (2, 1, 0, 'InactiveChild'), (3, 2, 1, 'OtherChild')");

            String model = withRuntime("""
                    Class test::Parent { name: String[1]; }
                    Class test::Child { name: String[1]; }
                    Association test::ParentChild { activeChildren: Child[*]; parent: Parent[1]; }
                    Database store::DB
                    (
                        Table T_PARENT (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                        Table T_CHILD (ID INTEGER, PARENT_ID INTEGER, ACTIVE INTEGER, NAME VARCHAR(100))
                        Join ParentChild(T_PARENT.ID = T_CHILD.PARENT_ID and T_CHILD.ACTIVE = 1)
                    )
                    Mapping test::M
                    (
                        Parent: Relational
                        {
                            ~mainTable [store::DB] T_PARENT
                            name: [store::DB] T_PARENT.NAME
                        }
                        Child: Relational
                        {
                            ~mainTable [store::DB] T_CHILD
                            name: [store::DB] T_CHILD.NAME
                        }
                        test::ParentChild: AssociationMapping
                        (
                            parent: [store::DB]@ParentChild,
                            activeChildren: [store::DB]@ParentChild
                        )
                    )
                    """, "store::DB", "test::M");

            // Filter: only parents with active children
            var r = exec(model, "Parent.all()->filter({p|$p.activeChildren.name == 'ActiveChild'})->project(~[name:p|$p.name])");
            assertEquals(1, r.rowCount());
            assertEquals("Parent1", colStr(r, 0).get(0));
        }
    }

    // ==================== 1h. Edge Cases ====================

    @Nested
    @DisplayName("1h. Edge Cases")
    class EdgeCases {

        @Test
        @DisplayName("Empty target table: all associations NULL")
        void testEmptyTargetTable() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT, NAME VARCHAR(100), FIRM_ID INT)",
                "CREATE TABLE T_FIRM (ID INT, LEGAL_NAME VARCHAR(100))",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 1), (2, 'Bob', 2)");
            // T_FIRM is empty — no rows

            String model = withRuntime("""
                    Class test::Person { name: String[1]; }
                    Class test::Firm { legalName: String[1]; }
                    Association test::PersonFirm { firm: Firm[0..1]; employees: Person[*]; }
                    Database store::DB
                    (
                        Table T_PERSON (ID INTEGER, NAME VARCHAR(100), FIRM_ID INTEGER)
                        Table T_FIRM (ID INTEGER, LEGAL_NAME VARCHAR(100))
                        Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)
                    )
                    Mapping test::M
                    (
                        Person: Relational { ~mainTable [store::DB] T_PERSON name: [store::DB] T_PERSON.NAME }
                        Firm: Relational { ~mainTable [store::DB] T_FIRM legalName: [store::DB] T_FIRM.LEGAL_NAME }
                        test::PersonFirm: AssociationMapping ( employees: [store::DB]@PersonFirm, firm: [store::DB]@PersonFirm )
                    )
                    """, "store::DB", "test::M");

            var r = exec(model, "Person.all()->project([x|$x.name, x|$x.firm.legalName], ['name', 'firm'])");
            assertEquals(2, r.rows().size());
            assertNull(r.rows().get(0).get(1));
            assertNull(r.rows().get(1).get(1));
        }

        @Test
        @DisplayName("NULL foreign keys: person with NULL firm_id")
        void testNullForeignKey() throws SQLException {
            setupTables();
            // Diana has NULL firm_id
            var r = exec(fullModel(), "Person.all()->filter({p|$p.name == 'Diana'})->project([x|$x.name, x|$x.firm.legalName], ['name', 'firm'])");
            assertEquals(1, r.rows().size());
            assertEquals("Diana", r.rows().get(0).get(0).toString());
            assertNull(r.rows().get(0).get(1));
        }

        @Test
        @DisplayName("All NULLs in FK: every person has NULL firm_id")
        void testAllNullForeignKeys() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT, NAME VARCHAR(100), FIRM_ID INT)",
                "CREATE TABLE T_FIRM (ID INT, LEGAL_NAME VARCHAR(100))",
                "INSERT INTO T_FIRM VALUES (1, 'Acme')",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', NULL), (2, 'Bob', NULL)");

            String model = withRuntime("""
                    Class test::Person { name: String[1]; }
                    Class test::Firm { legalName: String[1]; }
                    Association test::PersonFirm { firm: Firm[0..1]; employees: Person[*]; }
                    Database store::DB
                    (
                        Table T_PERSON (ID INTEGER, NAME VARCHAR(100), FIRM_ID INTEGER)
                        Table T_FIRM (ID INTEGER, LEGAL_NAME VARCHAR(100))
                        Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)
                    )
                    Mapping test::M
                    (
                        Person: Relational { ~mainTable [store::DB] T_PERSON name: [store::DB] T_PERSON.NAME }
                        Firm: Relational { ~mainTable [store::DB] T_FIRM legalName: [store::DB] T_FIRM.LEGAL_NAME }
                        test::PersonFirm: AssociationMapping ( employees: [store::DB]@PersonFirm, firm: [store::DB]@PersonFirm )
                    )
                    """, "store::DB", "test::M");

            var r = exec(model, "Person.all()->project([x|$x.name, x|$x.firm.legalName], ['name', 'firm'])");
            assertEquals(2, r.rows().size());
            assertNull(r.rows().get(0).get(1));
            assertNull(r.rows().get(1).get(1));
        }

        @Test
        @DisplayName("Circular model (A->B, B->A): navigate one direction without loop")
        void testCircularModel() throws SQLException {
            setupTables();
            // Person->Firm (to-one) and Firm->employees (to-many) form a cycle
            // Navigate Person->Firm only — no infinite loop
            var r = exec(fullModel(), "Person.all()->project([x|$x.name, x|$x.firm.legalName], ['name', 'firm'])");
            assertEquals(4, r.rows().size());
        }

        @Test
        @DisplayName("Same join used by two association ends: both directions work")
        void testSameJoinBothDirections() throws SQLException {
            setupTables();
            // Person->Firm uses @PersonFirm, Firm->employees uses same @PersonFirm
            var r1 = exec(fullModel(), "Person.all()->filter({p|$p.name == 'Alice'})->project([x|$x.firm.legalName], ['firm'])");
            assertEquals(1, r1.rows().size());
            assertEquals("Acme", r1.rows().get(0).get(0).toString());

            var r2 = exec(fullModel(), "Firm.all()->filter({f|$f.legalName == 'Acme'})->project([x|$x.legalName], ['firm'])");
            assertEquals(1, r2.rows().size());
            assertEquals("Acme", r2.rows().get(0).get(0).toString());
        }
    }

    // ==================== 1i. Demand-Driven Verification ====================

    @Nested
    @DisplayName("1i. Demand-Driven Verification")
    class DemandDriven {

        @BeforeEach
        void setup() throws SQLException { setupTables(); }

        @Test
        @DisplayName("No association access -> correct row count (no unnecessary JOIN)")
        void testNoAssociationAccess() throws SQLException {
            var r = exec(fullModel(), "Person.all()->project(~[name:p|$p.name])");
            // 4 persons — no JOIN should mean exactly 4 rows
            assertEquals(4, r.rowCount());
        }

        @Test
        @DisplayName("One association access -> correct data from one JOIN")
        void testOneAssociationAccess() throws SQLException {
            var r = exec(fullModel(), "Person.all()->project([x|$x.name, x|$x.firm.legalName], ['name', 'firm'])");
            assertEquals(4, r.rows().size());
            Map<String, String> map = new HashMap<>();
            for (var row : r.rows()) map.put(row.get(0).toString(), row.get(1) != null ? row.get(1).toString() : null);
            assertEquals("Acme", map.get("Alice"));
        }

        @Test
        @DisplayName("Two association accesses -> both resolved correctly")
        void testTwoAssociationAccesses() throws SQLException {
            var r = exec(fullModel(), "Person.all()->project([x|$x.name, x|$x.firm.legalName, x|$x.dept.name], ['name', 'firm', 'dept'])");
            assertEquals(4, r.rows().size());
            Map<String, String> firms = new HashMap<>();
            Map<String, String> depts = new HashMap<>();
            for (var row : r.rows()) {
                String name = row.get(0).toString();
                firms.put(name, row.get(1) != null ? row.get(1).toString() : null);
                depts.put(name, row.get(2) != null ? row.get(2).toString() : null);
            }
            assertEquals("Acme", firms.get("Alice"));
            assertEquals("Engineering", depts.get("Alice"));
            assertEquals("Globex", firms.get("Charlie"));
            assertEquals("Engineering", depts.get("Charlie"));
        }
    }

    // ==================== 1j. Self-Join (GAP) ====================

    @Nested
    @DisplayName("1j. Self-Join")
    class SelfJoin {

        @Test
        @DisplayName("Self-join: Person -> manager (same table)")
        void testSelfJoin() throws SQLException {
            sql("CREATE TABLE T_EMP (ID INT PRIMARY KEY, NAME VARCHAR(100), MANAGER_ID INT)",
                "INSERT INTO T_EMP VALUES (1, 'CEO', NULL), (2, 'VP', 1), (3, 'Dev', 2)");

            String model = withRuntime("""
                    Class test::Emp { name: String[1]; }
                    Association test::EmpManager { manager: Emp[0..1]; reports: Emp[*]; }
                    Database store::DB
                    (
                        Table T_EMP (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), MANAGER_ID INTEGER)
                        Join EmpManager(T_EMP.MANAGER_ID = {target}.ID)
                    )
                    Mapping test::M
                    (
                        Emp: Relational
                        {
                            ~mainTable [store::DB] T_EMP
                            name: [store::DB] T_EMP.NAME
                        }
                        test::EmpManager: AssociationMapping
                        (
                            manager: [store::DB]@EmpManager,
                            reports: [store::DB]@EmpManager
                        )
                    )
                    """, "store::DB", "test::M");

            // Navigate to manager (to-one self-join)
            var r = exec(model, "Emp.all()->project([x|$x.name, x|$x.manager.name], ['name', 'manager'])");
            assertEquals(3, r.rows().size());
            Map<String, String> map = new HashMap<>();
            for (var row : r.rows()) map.put(row.get(0).toString(), row.get(1) != null ? row.get(1).toString() : null);
            assertNull(map.get("CEO"));       // CEO has no manager
            assertEquals("CEO", map.get("VP"));    // VP's manager is CEO
            assertEquals("VP", map.get("Dev"));    // Dev's manager is VP
        }
    }
}
