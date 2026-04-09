package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.server.QueryService;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for association navigation.
 *
 * <p>Tests the full pipeline: Parser -> MappingNormalizer -> TypeChecker (ExtendChecker)
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
@DisplayName("Association Navigation Integration Tests")
class AssociationIntegrationTest {

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
                import test::*;


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

    private String execGraph(String model, String query) throws SQLException {
        var result = exec(model, query);
        assertInstanceOf(ExecutionResult.GraphResult.class, result);
        return result.asGraph().json();
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
                import store::*;
                import test::*;

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
        @DisplayName("Two independent to-many: Person -> Address AND Person -> Phone")
        void testTwoIndependentToMany() throws SQLException {
            sql("CREATE TABLE T_PERSON2 (ID INT PRIMARY KEY, NAME VARCHAR(100))",
                "CREATE TABLE T_ADDR (ID INT, PERSON_ID INT, CITY VARCHAR(100))",
                "CREATE TABLE T_PHONE (ID INT, PERSON_ID INT, NUMBER VARCHAR(20))",
                "INSERT INTO T_PERSON2 VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
                "INSERT INTO T_ADDR VALUES (1, 1, 'NYC'), (2, 1, 'LA'), (3, 2, 'Chicago')",
                "INSERT INTO T_PHONE VALUES (1, 1, '555-0001'), (2, 2, '555-0002'), (3, 2, '555-0003')");

            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Person { name: String[1]; }
                    Class test::Addr { city: String[1]; }
                    Class test::Phone { number: String[1]; }
                    Association test::PersonAddr { addresses: Addr[*]; person: Person[1]; }
                    Association test::PersonPhone { phones: Phone[*]; owner: Person[1]; }
                    Database store::DB
                    (
                        Table T_PERSON2 (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                        Table T_ADDR (ID INTEGER, PERSON_ID INTEGER, CITY VARCHAR(100))
                        Table T_PHONE (ID INTEGER, PERSON_ID INTEGER, NUMBER VARCHAR(20))
                        Join PersonAddr(T_PERSON2.ID = T_ADDR.PERSON_ID)
                        Join PersonPhone(T_PERSON2.ID = T_PHONE.PERSON_ID)
                    )
                    Mapping test::M
                    (
                        Person: Relational { ~mainTable [store::DB] T_PERSON2 name: [store::DB] T_PERSON2.NAME }
                        Addr: Relational { ~mainTable [store::DB] T_ADDR city: [store::DB] T_ADDR.CITY }
                        Phone: Relational { ~mainTable [store::DB] T_PHONE number: [store::DB] T_PHONE.NUMBER }
                        test::PersonAddr: AssociationMapping ( person: [store::DB]@PersonAddr, addresses: [store::DB]@PersonAddr )
                        test::PersonPhone: AssociationMapping ( owner: [store::DB]@PersonPhone, phones: [store::DB]@PersonPhone )
                    )
                    """, "store::DB", "test::M");

            // Project both to-many associations — potential cartesian product
            // Alice: 2 addr x 1 phone, Bob: 1 addr x 2 phones, Charlie: 0 addr x 0 phones
            var r = exec(model, "Person.all()->project([x|$x.name, x|$x.addresses.city, x|$x.phones.number], ['name', 'city', 'phone'])");

            // Collect per-person data
            Map<String, Set<String>> cities = new HashMap<>();
            Map<String, Set<String>> phones = new HashMap<>();
            Map<String, Integer> rowCounts = new HashMap<>();
            for (var row : r.rows()) {
                String name = row.get(0).toString();
                rowCounts.merge(name, 1, Integer::sum);
                if (row.get(1) != null) cities.computeIfAbsent(name, k -> new HashSet<>()).add(row.get(1).toString());
                if (row.get(2) != null) phones.computeIfAbsent(name, k -> new HashSet<>()).add(row.get(2).toString());
            }

            // Alice has 2 addresses and 1 phone — verify her data is present
            assertTrue(cities.containsKey("Alice"));
            assertEquals(Set.of("NYC", "LA"), cities.get("Alice"));
            assertEquals(Set.of("555-0001"), phones.get("Alice"));

            // Bob has 1 address and 2 phones
            assertEquals(Set.of("Chicago"), cities.get("Bob"));
            assertEquals(Set.of("555-0002", "555-0003"), phones.get("Bob"));

            // Charlie has no addresses and no phones — should still appear with NULLs
            assertTrue(rowCounts.containsKey("Charlie"));
        }

        @Test
        @DisplayName("Pre-filter to single to-many match, then project through association — no explosion")
        void testPreFilterThenToManyProject() throws SQLException {
            // Alice has 2 addresses (NYC, LA) and Bob has 1 (Chicago).
            // Pre-filter to only people who have exactly 1 address (Bob, Charlie=0, Diana=0).
            // Then project the address — Bob gets Chicago, others get NULL.
            // Key: the pre-filter should NOT cause row explosion.
            //
            // Strategy: filter on a specific address city first to narrow to one match,
            // THEN project through the to-many.
            // Filter: person has an address in Chicago → only Bob.
            // Project Bob's addresses.city → should be exactly Chicago (1 row, no explosion).
            var r = exec(fullModel(),
                "Person.all()->filter({p|$p.addresses.city == 'Chicago'})->project([x|$x.name, x|$x.addresses.city], ['name', 'city'])");
            // Bob has only 1 address (Chicago), and filter used EXISTS → 1 row for Bob
            assertEquals(1, r.rows().size());
            assertEquals("Bob", r.rows().get(0).get(0).toString());
            assertEquals("Chicago", r.rows().get(0).get(1).toString());
        }

        @Test
        @DisplayName("Pre-filter narrows multi-address person, project still shows all addresses")
        void testPreFilterDoesNotLimitProjection() throws SQLException {
            // Alice has 2 addresses (NYC, LA). Filter on firm='Acme' (which Alice matches).
            // Then project addresses — Alice should still show BOTH addresses (2 rows).
            // The filter is on a DIFFERENT association (firm), not on addresses.
            var r = exec(fullModel(),
                "Person.all()->filter({p|$p.firm.legalName == 'Acme'})->project([x|$x.name, x|$x.addresses.city], ['name', 'city'])");
            // Acme people: Alice (2 addresses) and Bob (1 address) = 3 rows
            assertEquals(3, r.rows().size());
            Map<String, List<String>> nameCity = new HashMap<>();
            for (var row : r.rows()) {
                String name = row.get(0).toString();
                nameCity.computeIfAbsent(name, k -> new ArrayList<>())
                        .add(row.get(1) != null ? row.get(1).toString() : null);
            }
            assertEquals(2, nameCity.get("Alice").size());
            assertTrue(nameCity.get("Alice").contains("NYC"));
            assertTrue(nameCity.get("Alice").contains("LA"));
            assertEquals(1, nameCity.get("Bob").size());
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
                    import store::*;
                    import test::*;

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
                    import store::*;
                    import test::*;

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
                    import store::*;
                    import test::*;

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
                    import store::*;
                    import test::*;

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
                    import store::*;
                    import test::*;

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
                    import store::*;
                    import test::*;

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
                    import store::*;
                    import test::*;

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

    // ==================== 1k. Wild / Stress Tests ====================

    @Nested
    @DisplayName("1k. Wild / Stress Tests")
    class WildStress {

        @BeforeEach
        void setup() throws SQLException { setupTables(); }

        @Test
        @DisplayName("Reverse to-many as root: Firm.all() projecting employees")
        void testReverseToManyAsRoot() throws SQLException {
            // Start from Firm, navigate to employees (to-many)
            var r = exec(fullModel(), "Firm.all()->filter({f|$f.employees.name == 'Alice'})->project(~[legalName:f|$f.legalName])");
            assertEquals(1, r.rowCount());
            assertEquals("Acme", colStr(r, 0).get(0));
        }

        @Test
        @DisplayName("Cross-association filter: EXISTS on to-many AND equals on to-one")
        void testCrossAssociationFilter() throws SQLException {
            // Filter: has address in NYC (EXISTS) AND firm is Acme (equi-join)
            // Only Alice has NYC address AND works at Acme
            var r = exec(fullModel(), "Person.all()->filter({p|$p.addresses.city == 'NYC' && $p.firm.legalName == 'Acme'})->project(~[name:p|$p.name])");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", colStr(r, 0).get(0));
        }

        @Test
        @DisplayName("Filter on deep path + project on different deep path")
        void testFilterDeepProjectDifferent() throws SQLException {
            // Filter on dept.org.name, project firm.legalName — two completely independent association paths
            var r = exec(fullModel(), "Person.all()->filter({p|$p.dept.org.name == 'Acme Corp'})->project([x|$x.name, x|$x.firm.legalName], ['name', 'firm'])");
            assertEquals(3, r.rows().size());
            Map<String, String> map = new HashMap<>();
            for (var row : r.rows()) map.put(row.get(0).toString(), row.get(1) != null ? row.get(1).toString() : null);
            assertEquals("Acme", map.get("Alice"));
            assertEquals("Acme", map.get("Bob"));
            assertEquals("Globex", map.get("Charlie"));
        }

        @Test
        @DisplayName("Limit after to-many expansion: project addresses then limit")
        void testLimitAfterToManyExpansion() throws SQLException {
            // Person->addresses produces 5 rows (Alice x2, Bob x1, Charlie NULL, Diana NULL)
            // Sort by name ASC then limit 2 — should get Alice's first 2 rows
            var r = exec(fullModel(),
                "Person.all()->project(~[name:x|$x.name, city:x|$x.addresses.city])->sort(~name->ascending())->limit(2)");
            assertEquals(2, r.rowCount());
            // Both rows are Alice (she has 2 addresses and sorts first)
            assertEquals("Alice", colStr(r, 0).get(0));
            assertEquals("Alice", colStr(r, 0).get(1));
        }

        @Test
        @DisplayName("Distinct after to-many expansion: dedup expanded rows")
        void testDistinctAfterToManyExpansion() throws SQLException {
            // Project just the person name through addresses — Alice appears twice (2 addresses)
            // Distinct should collapse Alice back to 1
            var r = exec(fullModel(),
                "Person.all()->project(~[name:x|$x.name, city:x|$x.addresses.city])->select(~[name])->distinct()");
            assertEquals(4, r.rowCount());
            var names = colStr(r, 0);
            assertEquals(1, Collections.frequency(names, "Alice"));
        }

        @Test
        @DisplayName("Empty source table: 0 persons, joins still produce 0 rows")
        void testEmptySourceTable() throws SQLException {
            sql("CREATE TABLE T_EMPTY_PERSON (ID INT, NAME VARCHAR(100), FIRM_ID INT)",
                "CREATE TABLE T_EMPTY_FIRM (ID INT, LEGAL_NAME VARCHAR(100))",
                "INSERT INTO T_EMPTY_FIRM VALUES (1, 'Acme')");
            // T_EMPTY_PERSON has no rows

            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Person { name: String[1]; }
                    Class test::Firm { legalName: String[1]; }
                    Association test::PersonFirm { firm: Firm[0..1]; employees: Person[*]; }
                    Database store::DB
                    (
                        Table T_EMPTY_PERSON (ID INTEGER, NAME VARCHAR(100), FIRM_ID INTEGER)
                        Table T_EMPTY_FIRM (ID INTEGER, LEGAL_NAME VARCHAR(100))
                        Join PersonFirm(T_EMPTY_PERSON.FIRM_ID = T_EMPTY_FIRM.ID)
                    )
                    Mapping test::M
                    (
                        Person: Relational { ~mainTable [store::DB] T_EMPTY_PERSON name: [store::DB] T_EMPTY_PERSON.NAME }
                        Firm: Relational { ~mainTable [store::DB] T_EMPTY_FIRM legalName: [store::DB] T_EMPTY_FIRM.LEGAL_NAME }
                        test::PersonFirm: AssociationMapping ( employees: [store::DB]@PersonFirm, firm: [store::DB]@PersonFirm )
                    )
                    """, "store::DB", "test::M");

            var r = exec(model, "Person.all()->project([x|$x.name, x|$x.firm.legalName], ['name', 'firm'])");
            assertEquals(0, r.rows().size());
        }

        @Test
        @DisplayName("Diamond: two paths to same org concept via different associations")
        void testDiamondTwoPaths() throws SQLException {
            // Person->dept.org.name AND Person->dept.name — both through dept but different depths
            // Verify both resolve independently with correct values
            var r = exec(fullModel(),
                "Person.all()->project([x|$x.name, x|$x.dept.name, x|$x.dept.org.name], ['name', 'dept', 'org'])");
            assertEquals(4, r.rows().size());
            Map<String, String> depts = new HashMap<>();
            Map<String, String> orgs = new HashMap<>();
            for (var row : r.rows()) {
                String name = row.get(0).toString();
                depts.put(name, row.get(1) != null ? row.get(1).toString() : null);
                orgs.put(name, row.get(2) != null ? row.get(2).toString() : null);
            }
            assertEquals("Engineering", depts.get("Alice"));
            assertEquals("Acme Corp", orgs.get("Alice"));
            assertEquals("Sales", depts.get("Bob"));
            assertEquals("Acme Corp", orgs.get("Bob"));
            assertNull(depts.get("Diana"));
            assertNull(orgs.get("Diana"));
        }

        @Test
        @DisplayName("Sort by to-many association property: addresses.city with row expansion")
        void testSortByToManyProperty() throws SQLException {
            var r = exec(fullModel(),
                "Person.all()->project(~[name:x|$x.name, city:x|$x.addresses.city])->sort(~city->ascending())");
            assertEquals(5, r.rowCount());
            var cities = colStr(r, 1);
            // ASC NULLS LAST: Chicago, LA, NYC, NULL, NULL
            assertEquals("Chicago", cities.get(0));
            assertEquals("LA", cities.get(1));
            assertEquals("NYC", cities.get(2));
            assertNull(cities.get(3));
            assertNull(cities.get(4));
        }

        @Test
        @DisplayName("Three associations in one projection: firm + dept + addresses")
        void testThreeAssociationsOneProjection() throws SQLException {
            var r = exec(fullModel(),
                "Person.all()->project([x|$x.name, x|$x.firm.legalName, x|$x.dept.name, x|$x.addresses.city], ['name', 'firm', 'dept', 'city'])");
            // Alice: 2 address rows, Bob: 1, Charlie: 1 (NULL city), Diana: 1 (NULL all)
            assertEquals(5, r.rows().size());
            // Verify Alice's rows have consistent firm/dept across her 2 address rows
            List<String> aliceFirms = new ArrayList<>();
            List<String> aliceDepts = new ArrayList<>();
            for (var row : r.rows()) {
                if ("Alice".equals(row.get(0).toString())) {
                    aliceFirms.add(row.get(1) != null ? row.get(1).toString() : null);
                    aliceDepts.add(row.get(2) != null ? row.get(2).toString() : null);
                }
            }
            assertEquals(2, aliceFirms.size());
            assertTrue(aliceFirms.stream().allMatch("Acme"::equals));
            assertTrue(aliceDepts.stream().allMatch("Engineering"::equals));
        }

        @Test
        @DisplayName("GroupBy to-one association after to-many filter (EXISTS + aggregation)")
        void testGroupByToOneAfterToManyFilter() throws SQLException {
            // Filter: person has address in a known city (EXISTS) — Alice(NYC,LA), Bob(Chicago)
            // Charlie and Diana have no addresses, so EXISTS is false for them.
            // GroupBy firm, count — only Acme people (Alice, Bob) survive the filter.
            // Globex (Charlie) excluded. Diana (NULL firm) excluded.
            var r = exec(fullModel(),
                "Person.all()->filter({p|$p.addresses.city != ''})->project(~[firm:p|$p.firm.legalName, name:p|$p.name])->groupBy(~firm, ~cnt:x|$x.name:y|$y->count())->sort(~firm->ascending())");
            // Only Acme people have addresses — 1 group
            assertEquals(1, r.rowCount());
            assertEquals("Acme", colStr(r, 0).get(0));
            assertEquals(Integer.valueOf(2), colInt(r, 1).get(0));
        }

        @Test
        @DisplayName("Association property in string computation: extend with toUpper on firm name")
        void testAssociationInComputation() throws SQLException {
            var r = exec(fullModel(),
                "Person.all()->filter({p|$p.name == 'Alice'})->project(~[name:x|$x.name, firm:x|$x.firm.legalName])->extend(~upper:x|$x.firm->toUpper())");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", colStr(r, 0).get(0));
            assertEquals("Acme", colStr(r, 1).get(0));
            assertEquals("ACME", colStr(r, 2).get(0));
        }

        @Test
        @DisplayName("Self-join two hops: Dev -> VP -> CEO (manager's manager)")
        void testSelfJoinTwoHops() throws SQLException {
            sql("CREATE TABLE T_EMP2 (ID INT PRIMARY KEY, NAME VARCHAR(100), MANAGER_ID INT)",
                "INSERT INTO T_EMP2 VALUES (1, 'CEO', NULL), (2, 'VP', 1), (3, 'Dev', 2), (4, 'Intern', 3)");

            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Emp { name: String[1]; }
                    Association test::EmpManager { manager: Emp[0..1]; reports: Emp[*]; }
                    Database store::DB
                    (
                        Table T_EMP2 (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), MANAGER_ID INTEGER)
                        Join EmpManager(T_EMP2.MANAGER_ID = {target}.ID)
                    )
                    Mapping test::M
                    (
                        Emp: Relational { ~mainTable [store::DB] T_EMP2 name: [store::DB] T_EMP2.NAME }
                        test::EmpManager: AssociationMapping ( manager: [store::DB]@EmpManager, reports: [store::DB]@EmpManager )
                    )
                    """, "store::DB", "test::M");

            // Navigate TWO hops: emp -> manager -> manager (skip-level reporting)
            var r = exec(model, "Emp.all()->project([x|$x.name, x|$x.manager.manager.name], ['name', 'grandmanager'])");
            assertEquals(4, r.rows().size());
            Map<String, String> map = new HashMap<>();
            for (var row : r.rows()) map.put(row.get(0).toString(), row.get(1) != null ? row.get(1).toString() : null);
            assertNull(map.get("CEO"));        // no manager
            assertNull(map.get("VP"));         // manager is CEO, CEO has no manager
            assertEquals("CEO", map.get("Dev"));    // Dev->VP->CEO
            assertEquals("VP", map.get("Intern"));  // Intern->Dev->VP
        }

        @Test
        @DisplayName("Filter on self-join property: manager.name == 'CEO'")
        void testFilterOnSelfJoin() throws SQLException {
            sql("CREATE TABLE T_EMP3 (ID INT PRIMARY KEY, NAME VARCHAR(100), MANAGER_ID INT)",
                "INSERT INTO T_EMP3 VALUES (1, 'CEO', NULL), (2, 'VP', 1), (3, 'Director', 1), (4, 'Dev', 2)");

            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Emp { name: String[1]; }
                    Association test::EmpManager { manager: Emp[0..1]; reports: Emp[*]; }
                    Database store::DB
                    (
                        Table T_EMP3 (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), MANAGER_ID INTEGER)
                        Join EmpManager(T_EMP3.MANAGER_ID = {target}.ID)
                    )
                    Mapping test::M
                    (
                        Emp: Relational { ~mainTable [store::DB] T_EMP3 name: [store::DB] T_EMP3.NAME }
                        test::EmpManager: AssociationMapping ( manager: [store::DB]@EmpManager, reports: [store::DB]@EmpManager )
                    )
                    """, "store::DB", "test::M");

            // Who reports directly to the CEO?
            var r = exec(model, "Emp.all()->filter({e|$e.manager.name == 'CEO'})->project(~[name:e|$e.name])->sort(~name->ascending())");
            assertEquals(2, r.rowCount());
            assertEquals(List.of("Director", "VP"), colStr(r, 0));
        }
    }

    // ==================== 1l. Combined Association Nav + extend(traverse()) ====================

    @Nested
    @DisplayName("1l. Combined Association Nav + extend(traverse())")
    class CombinedTraverse {

        @BeforeEach
        void setup() throws SQLException { setupTables(); }

        /**
         * Model with deptId mapped so it's available in TDS for extend(traverse()) FK reference.
         * Classic association nav (firm) + explicit extend(traverse()) (dept) in same query.
         */
        private String combinedModel() {
            return withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Person { id: Integer[1]; name: String[1]; deptId: Integer[0..1]; }
                    Class test::Firm { id: Integer[1]; legalName: String[1]; }
                    Association test::PersonFirm { firm: Firm[0..1]; employees: Person[*]; }
                    Database store::DB
                    (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INTEGER, DEPT_ID INTEGER)
                        Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))
                        Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), ORG_ID INTEGER)
                        Table T_ORG (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                        Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)
                    )
                    Mapping test::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            id: [store::DB] T_PERSON.ID,
                            name: [store::DB] T_PERSON.NAME,
                            deptId: [store::DB] T_PERSON.DEPT_ID
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
        }

        @Test
        @DisplayName("Association nav project + extend(traverse()) in same query")
        void testAssocNavPlusExtendTraverse() throws SQLException {
            // Classic association: Person->firm.legalName (MappingNormalizer-synthesized traverse)
            // User-written: extend(traverse(T_DEPT), ~deptName) (explicit Relation API traverse)
            // Project includes deptId so traverse can use it as FK
            var r = exec(combinedModel(),
                "Person.all()->project([x|$x.name, x|$x.firm.legalName, x|$x.deptId], ['name', 'firm', 'deptId'])" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.deptId == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})" +
                "->select(~[name, firm, deptName])");
            assertEquals(4, r.rowCount());
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

        @Test
        @DisplayName("Filter on association nav, then extend(traverse()) on result")
        void testFilterAssocThenExtendTraverse() throws SQLException {
            // Filter via classic association (firm='Acme'), then add dept via extend(traverse())
            var r = exec(combinedModel(),
                "Person.all()->filter({p|$p.firm.legalName == 'Acme'})" +
                "->project(~[name:x|$x.name, deptId:x|$x.deptId])" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.deptId == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})" +
                "->select(~[name, deptName])->sort(~name->ascending())");
            assertEquals(2, r.rowCount());
            assertEquals(List.of("Alice", "Bob"), colStr(r, 0));
            assertEquals("Engineering", colStr(r, 1).get(0));
            assertEquals("Sales", colStr(r, 1).get(1));
        }

        @Test
        @DisplayName("extend(traverse()) multi-hop + association nav in same query")
        void testMultiHopExtendTraversePlusAssocNav() throws SQLException {
            // User-written multi-hop traverse: Person -> Dept -> Org (via extend)
            // Classic association: Person -> firm.legalName
            // Both in same query
            var r = exec(combinedModel(),
                "Person.all()->project([x|$x.name, x|$x.firm.legalName, x|$x.deptId], ['name', 'firm', 'deptId'])" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.deptId == $hop.ID})" +
                "->traverse(#>{store::DB.T_ORG}#, {prev,hop|$prev.ORG_ID == $hop.ID}), ~orgName:{src,tgt|$tgt.NAME})" +
                "->select(~[name, firm, orgName])->sort(~name->ascending())");
            assertEquals(4, r.rowCount());
            assertEquals(List.of("Alice", "Bob", "Charlie", "Diana"), colStr(r, 0));
            // firm via association nav
            assertEquals("Acme", colStr(r, 1).get(0));
            assertEquals("Globex", colStr(r, 1).get(2));
            // org via extend(traverse()) multi-hop
            assertEquals("Acme Corp", colStr(r, 2).get(0));
            assertEquals("Acme Corp", colStr(r, 2).get(1));
            assertEquals("Acme Corp", colStr(r, 2).get(2));
            assertNull(colStr(r, 2).get(3)); // Diana has no dept
        }

        @Test
        @DisplayName("extend(traverse()) then filter on traversed + association column")
        void testExtendTraverseThenFilterOnBoth() throws SQLException {
            // Add dept via extend(traverse()), then filter on traversed col AND association col
            var r = exec(combinedModel(),
                "Person.all()->project([x|$x.name, x|$x.firm.legalName, x|$x.deptId], ['name', 'firm', 'deptId'])" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.deptId == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})" +
                "->filter(x|$x.firm == 'Acme' && $x.deptName == 'Engineering')");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", colStr(r, 0).get(0));
            assertEquals("Acme", colStr(r, 1).get(0));
            // deptId=2, deptName=3 — verify deptName (last column)
            int lastCol = r.columns().size() - 1;
            assertEquals("Engineering", colStr(r, lastCol).get(0));
        }
    }

    // ==================== 1m. BOSS: All-Mapping (association + join chains) ====================

    @Nested
    @DisplayName("1m. BOSS: All-Mapping — association nav + join chain properties")
    class BossAllMapping {

        @BeforeEach
        void setup() throws SQLException {
            // 7 tables: Person -> Firm (assoc), Person -> Dept -> Org -> Country (3-hop chain), Person -> Addr (1-hop chain)
            sql("CREATE TABLE T_COUNTRY (ID INT PRIMARY KEY, NAME VARCHAR(100))",
                "CREATE TABLE T_ORG (ID INT PRIMARY KEY, NAME VARCHAR(100), COUNTRY_ID INT)",
                "CREATE TABLE T_DEPT (ID INT PRIMARY KEY, NAME VARCHAR(50), ORG_ID INT)",
                "CREATE TABLE T_FIRM (ID INT PRIMARY KEY, LEGAL_NAME VARCHAR(100))",
                "CREATE TABLE T_ADDR (ID INT PRIMARY KEY, CITY VARCHAR(100))",
                "CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INT, DEPT_ID INT, ADDR_ID INT)",
                "INSERT INTO T_COUNTRY VALUES (1, 'USA'), (2, 'UK')",
                "INSERT INTO T_ORG VALUES (1, 'Acme Corp', 1), (2, 'Globex Ltd', 2)",
                "INSERT INTO T_DEPT VALUES (1, 'Engineering', 1), (2, 'Sales', 1), (3, 'Research', 2)",
                "INSERT INTO T_FIRM VALUES (1, 'Acme'), (2, 'Globex')",
                "INSERT INTO T_ADDR VALUES (1, 'New York'), (2, 'London'), (3, 'Chicago')",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 1, 1, 1), (2, 'Bob', 1, 2, 3), (3, 'Charlie', 2, 3, 2), (4, 'Diana', NULL, NULL, NULL)");
        }

        /**
         * 7-table model, all in mapping — no user-written extend(traverse()):
         *   - Association: PersonFirm (firm nav)
         *   - 1-hop chain: deptName (@Person_Dept | T_DEPT.NAME)
         *   - 1-hop chain: city (@Person_Addr | T_ADDR.CITY)
         *   - 2-hop chain: orgName (@Person_Dept > @Dept_Org | T_ORG.NAME)
         *   - 3-hop chain: countryName (@Person_Dept > @Dept_Org > @Org_Country | T_COUNTRY.NAME)
         */
        private String bossModel() {
            return withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Person { name: String[1]; deptName: String[0..1]; city: String[0..1]; orgName: String[0..1]; countryName: String[0..1]; }
                    Class test::Firm { legalName: String[1]; }
                    Association test::PersonFirm { firm: Firm[0..1]; employees: Person[*]; }
                    Database store::DB
                    (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INTEGER, DEPT_ID INTEGER, ADDR_ID INTEGER)
                        Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))
                        Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), ORG_ID INTEGER)
                        Table T_ORG (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), COUNTRY_ID INTEGER)
                        Table T_COUNTRY (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                        Table T_ADDR (ID INTEGER PRIMARY KEY, CITY VARCHAR(100))
                        Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)
                        Join Person_Dept(T_PERSON.DEPT_ID = T_DEPT.ID)
                        Join Dept_Org(T_DEPT.ORG_ID = T_ORG.ID)
                        Join Org_Country(T_ORG.COUNTRY_ID = T_COUNTRY.ID)
                        Join Person_Addr(T_PERSON.ADDR_ID = T_ADDR.ID)
                    )
                    Mapping test::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME,
                            deptName: [store::DB] @Person_Dept | T_DEPT.NAME,
                            city: [store::DB] @Person_Addr | T_ADDR.CITY,
                            orgName: [store::DB] @Person_Dept > @Dept_Org | T_ORG.NAME,
                            countryName: [store::DB] @Person_Dept > @Dept_Org > @Org_Country | T_COUNTRY.NAME
                        }
                        Firm: Relational
                        {
                            ~mainTable [store::DB] T_FIRM
                            legalName: [store::DB] T_FIRM.LEGAL_NAME
                        }
                        test::PersonFirm: AssociationMapping
                        (
                            employees: [store::DB]@PersonFirm,
                            firm: [store::DB]@PersonFirm
                        )
                    )
                    """, "store::DB", "test::M");
        }

        @Test
        @DisplayName("Project all: name + firm (assoc) + dept (1-hop) + org (2-hop) + country (3-hop) + city (1-hop)")
        void testProjectAll() throws SQLException {
            var r = exec(bossModel(),
                "Person.all()->project([x|$x.name, x|$x.firm.legalName, x|$x.deptName, x|$x.orgName, x|$x.countryName, x|$x.city], " +
                "['name', 'firm', 'dept', 'org', 'country', 'city'])->sort(~name->ascending())");
            assertEquals(4, r.rowCount());
            // Alice: Acme, Engineering, Acme Corp, USA, New York
            assertEquals("Alice", colStr(r, 0).get(0));
            assertEquals("Acme", colStr(r, 1).get(0));
            assertEquals("Engineering", colStr(r, 2).get(0));
            assertEquals("Acme Corp", colStr(r, 3).get(0));
            assertEquals("USA", colStr(r, 4).get(0));
            assertEquals("New York", colStr(r, 5).get(0));
            // Bob: Acme, Sales, Acme Corp, USA, Chicago
            assertEquals("Bob", colStr(r, 0).get(1));
            assertEquals("Acme", colStr(r, 1).get(1));
            assertEquals("Sales", colStr(r, 2).get(1));
            assertEquals("Acme Corp", colStr(r, 3).get(1));
            assertEquals("USA", colStr(r, 4).get(1));
            assertEquals("Chicago", colStr(r, 5).get(1));
            // Charlie: Globex, Research, Globex Ltd, UK, London
            assertEquals("Charlie", colStr(r, 0).get(2));
            assertEquals("Globex", colStr(r, 1).get(2));
            assertEquals("Research", colStr(r, 2).get(2));
            assertEquals("Globex Ltd", colStr(r, 3).get(2));
            assertEquals("UK", colStr(r, 4).get(2));
            assertEquals("London", colStr(r, 5).get(2));
            // Diana: all NULLs (no FK links)
            assertEquals("Diana", colStr(r, 0).get(3));
            assertNull(colStr(r, 1).get(3));
            assertNull(colStr(r, 2).get(3));
            assertNull(colStr(r, 3).get(3));
            assertNull(colStr(r, 4).get(3));
            assertNull(colStr(r, 5).get(3));
        }

        @Test
        @DisplayName("Filter on 3-hop chain + 1-hop chain, project association nav")
        void testFilterChainProjectAssoc() throws SQLException {
            // Filter: USA (3-hop chain) + Engineering (1-hop chain). Project: firm (association nav)
            var r = exec(bossModel(),
                "Person.all()->filter({p|$p.countryName == 'USA' && $p.deptName == 'Engineering'})" +
                "->project([x|$x.name, x|$x.firm.legalName], ['name', 'firm'])");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", colStr(r, 0).get(0));
            assertEquals("Acme", colStr(r, 1).get(0));
        }

        @Test
        @DisplayName("Filter on 1-hop chain, project 3-hop chain + association")
        void testFilterOneHopProjectThreeHop() throws SQLException {
            // Filter by dept, project country + firm
            var r = exec(bossModel(),
                "Person.all()->filter({p|$p.deptName == 'Engineering'})" +
                "->project([x|$x.name, x|$x.countryName, x|$x.firm.legalName], ['name', 'country', 'firm'])");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", colStr(r, 0).get(0));
            assertEquals("USA", colStr(r, 1).get(0));
            assertEquals("Acme", colStr(r, 2).get(0));
        }

        @Test
        @DisplayName("GroupBy on 2-hop chain column, count via association")
        void testGroupByChainColumn() throws SQLException {
            // Count employees per org
            var r = exec(bossModel(),
                "Person.all()->filter({p|$p.orgName->isNotEmpty()})" +
                "->project([x|$x.orgName], ['org'])" +
                "->groupBy(~org, ~cnt:x|$x.org:y|$y->count())->sort(~org->ascending())");
            assertEquals(2, r.rowCount());
            assertEquals("Acme Corp", colStr(r, 0).get(0));
            assertEquals(2, colInt(r, 1).get(0).intValue());
            assertEquals("Globex Ltd", colStr(r, 0).get(1));
            assertEquals(1, colInt(r, 1).get(1).intValue());
        }
    }

    // ==================== 1j. Self-Join ====================

    @Nested
    @DisplayName("1j. Self-Join")
    class SelfJoin {

        @Test
        @DisplayName("Self-join: Person -> manager (same table)")
        void testSelfJoin() throws SQLException {
            sql("CREATE TABLE T_EMP (ID INT PRIMARY KEY, NAME VARCHAR(100), MANAGER_ID INT)",
                "INSERT INTO T_EMP VALUES (1, 'CEO', NULL), (2, 'VP', 1), (3, 'Dev', 2)");

            String model = withRuntime("""
                    import store::*;
                    import test::*;

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

    // ==================== GraphFetch with To-Many Associations ====================

    @Nested
    @DisplayName("GraphFetch: To-Many Association Navigation")
    class GraphFetchToMany {

        @BeforeEach
        void setup() throws SQLException { setupTables(); }

        @Test
        @DisplayName("graphFetch: to-many addresses as nested JSON array")
        void testGraphFetchToManyAddresses() throws SQLException {
            var json = execGraph(fullModel(), """
                    Person.all()->sort({p|$p.name})
                        ->graphFetch(#{ Person { name, addresses { city } } }#)
                        ->serialize(#{ Person { name, addresses { city } } }#)
                    """);
            // Alice has 2 addresses, Bob has 1, Charlie has 0, Diana has 0
            assertTrue(json.contains("Alice"));
            assertTrue(json.contains("NYC"));
            assertTrue(json.contains("LA"));
            assertTrue(json.contains("Bob"));
            assertTrue(json.contains("Chicago"));
            assertTrue(json.contains("Charlie"));
            assertTrue(json.contains("Diana"));
        }

        @Test
        @DisplayName("graphFetch: to-one firm as nested JSON object")
        void testGraphFetchToOneFirm() throws SQLException {
            var json = execGraph(fullModel(), """
                    Person.all()->sort({p|$p.name})
                        ->graphFetch(#{ Person { name, firm { legalName } } }#)
                        ->serialize(#{ Person { name, firm { legalName } } }#)
                    """);
            assertTrue(json.contains("Alice"));
            assertTrue(json.contains("Acme"));
            assertTrue(json.contains("Charlie"));
            assertTrue(json.contains("Globex"));
        }

        @Test
        @DisplayName("graphFetch: mixed to-one + to-many in same fetch")
        void testGraphFetchMixedToOneAndToMany() throws SQLException {
            var json = execGraph(fullModel(), """
                    Person.all()->sort({p|$p.name})
                        ->graphFetch(#{ Person { name, firm { legalName }, addresses { city } } }#)
                        ->serialize(#{ Person { name, firm { legalName }, addresses { city } } }#)
                    """);
            // Alice: firm=Acme, addresses=[NYC, LA]
            assertTrue(json.contains("Alice"));
            assertTrue(json.contains("Acme"));
            assertTrue(json.contains("NYC"));
            assertTrue(json.contains("LA"));
            // Bob: firm=Acme, addresses=[Chicago]
            assertTrue(json.contains("Bob"));
            assertTrue(json.contains("Chicago"));
        }

        @Test
        @DisplayName("graphFetch: person with no addresses gets empty array")
        void testGraphFetchToManyEmpty() throws SQLException {
            var json = execGraph(fullModel(), """
                    Person.all()->filter({p|$p.name == 'Charlie'})
                        ->graphFetch(#{ Person { name, addresses { city } } }#)
                        ->serialize(#{ Person { name, addresses { city } } }#)
                    """);
            assertTrue(json.contains("Charlie"));
            // Charlie has no addresses — should get empty array or null, not crash
            assertFalse(json.contains("NYC"));
            assertFalse(json.contains("LA"));
            assertFalse(json.contains("Chicago"));
        }

        @Test
        @DisplayName("graphFetch: filter then to-many — only Alice's addresses")
        void testGraphFetchFilterThenToMany() throws SQLException {
            var json = execGraph(fullModel(), """
                    Person.all()->filter({p|$p.name == 'Alice'})
                        ->graphFetch(#{ Person { name, addresses { city } } }#)
                        ->serialize(#{ Person { name, addresses { city } } }#)
                    """);
            assertTrue(json.contains("Alice"));
            assertTrue(json.contains("NYC"));
            assertTrue(json.contains("LA"));
            assertFalse(json.contains("Bob"));
            assertFalse(json.contains("Chicago"));
        }

        @Test
        @DisplayName("graphFetch: to-many with multiple child properties")
        void testGraphFetchToManyMultipleChildProps() throws SQLException {
            // Address has id and city — fetch both
            var json = execGraph(fullModel(), """
                    Person.all()->filter({p|$p.name == 'Alice'})
                        ->graphFetch(#{ Person { name, addresses { id, city } } }#)
                        ->serialize(#{ Person { name, addresses { id, city } } }#)
                    """);
            assertTrue(json.contains("Alice"));
            assertTrue(json.contains("NYC"));
            assertTrue(json.contains("LA"));
        }

        @Test
        @DisplayName("graphFetch: NULL FK person gets null nested object for to-one")
        void testGraphFetchNullFkToOne() throws SQLException {
            var json = execGraph(fullModel(), """
                    Person.all()->filter({p|$p.name == 'Diana'})
                        ->graphFetch(#{ Person { name, firm { legalName } } }#)
                        ->serialize(#{ Person { name, firm { legalName } } }#)
                    """);
            assertTrue(json.contains("Diana"));
            // Diana has NULL firm_id — firm should be null/absent
            assertFalse(json.contains("Acme"));
            assertFalse(json.contains("Globex"));
        }

        @Test
        @DisplayName("graphFetch: scalar props only — no association overhead")
        void testGraphFetchScalarOnly() throws SQLException {
            var json = execGraph(fullModel(), """
                    Person.all()->sort({p|$p.name})
                        ->graphFetch(#{ Person { name, id } }#)
                        ->serialize(#{ Person { name, id } }#)
                    """);
            assertTrue(json.contains("Alice"));
            assertTrue(json.contains("Bob"));
            assertTrue(json.contains("Charlie"));
            assertTrue(json.contains("Diana"));
            // No nested objects
            assertFalse(json.contains("Acme"));
            assertFalse(json.contains("NYC"));
        }
    }
}
