package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.server.QueryService;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive JSON-source M2M chain tests.
 *
 * <p>ALL data comes from inline {@code data:application/json,...} URIs — NO physical
 * database tables. The data column is JSON, accessed via {@code ->>} operators.
 *
 * <pre>
 * Coverage:
 *   - Single-hop M2M from JSON (project, graphFetch, filter)
 *   - 2-hop M2M chains: L2 ← L1 ← JSON
 *   - 3-hop M2M chains: L3 ← L2 ← L1 ← JSON
 *   - 4-hop M2M chains: L4 ← L3 ← L2 ← L1 ← JSON
 *   - M2M with multiple JSON sources (multiple JsonModelConnections)
 *   - M2M with mapping filters at various levels
 *   - Computed properties, string concatenation, type casts
 *   - Edge cases: empty arrays, single element, null-ish values
 *   - Numeric operations through M2M chains
 * </pre>
 */
@DisplayName("JSON Source M2M Chain Integration Tests")
class JsonM2MChainIntegrationTest {

    private Connection conn;
    private final QueryService qs = new QueryService();

    // ==================== JSON Data ====================

    private static final String PERSON_JSON =
            "[{\"firstName\":\"Alice\",\"lastName\":\"Smith\",\"age\":30,\"dept\":\"Engineering\",\"salary\":95000,\"active\":true},"
          + "{\"firstName\":\"Bob\",\"lastName\":\"Jones\",\"age\":45,\"dept\":\"Sales\",\"salary\":120000,\"active\":true},"
          + "{\"firstName\":\"Carol\",\"lastName\":\"White\",\"age\":28,\"dept\":\"Engineering\",\"salary\":85000,\"active\":false},"
          + "{\"firstName\":\"Dave\",\"lastName\":\"Brown\",\"age\":55,\"dept\":\"Marketing\",\"salary\":110000,\"active\":true}]";

    private static final String SINGLE_PERSON_JSON =
            "[{\"firstName\":\"Solo\",\"lastName\":\"Person\",\"age\":99,\"dept\":\"Alone\",\"salary\":1,\"active\":true}]";

    private static final String DEPT_JSON =
            "[{\"name\":\"Engineering\",\"location\":\"San Francisco\",\"headcount\":150},"
          + "{\"name\":\"Sales\",\"location\":\"New York\",\"headcount\":80},"
          + "{\"name\":\"Marketing\",\"location\":\"Chicago\",\"headcount\":45}]";

    // ==================== Pure Models ====================

    /**
     * Base model with 4-level M2M chain:
     *   RawPerson (JSON) → StaffMember (L1) → StaffCard (L2) → DirectoryEntry (L3) → Badge (L4)
     *
     * Also includes:
     *   - ActiveStaff (L1 with filter)
     *   - StaffBadge (L2 from filtered L1)
     *   - NumericView (L1 with arithmetic)
     */
    private static final String CHAIN_MODEL = """
            import model::*;
            import store::*;

            // ===== JSON source class =====
            Class model::RawPerson
            {
                firstName: String[1];
                lastName:  String[1];
                age:       Integer[1];
                dept:      String[1];
                salary:    Integer[1];
                active:    Boolean[1];
            }

            // ===== L1: Single-hop M2M targets =====
            Class model::StaffMember
            {
                fullName:  String[1];
                firstName: String[1];
                lastName:  String[1];
                age:       Integer[1];
                dept:      String[1];
            }

            Class model::ActiveStaff
            {
                fullName: String[1];
                dept:     String[1];
            }

            Class model::NumericView
            {
                name:       String[1];
                ageInMonths: Integer[1];
                salaryCents: Integer[1];
            }

            // ===== L2: Two-hop M2M targets =====
            Class model::StaffCard
            {
                displayName: String[1];
                department:  String[1];
            }

            Class model::StaffBadge
            {
                label: String[1];
            }

            // ===== L3: Three-hop M2M target =====
            Class model::DirectoryEntry
            {
                entry: String[1];
            }

            // ===== L4: Four-hop M2M target =====
            Class model::Badge
            {
                text: String[1];
            }

            // ===== L1 M2M Mappings =====
            Mapping model::StaffMemberMapping
            (
                StaffMember: Pure
                {
                    ~src RawPerson
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
                    ~src RawPerson
                    ~filter $src.active == true
                    fullName: $src.firstName + ' ' + $src.lastName,
                    dept: $src.dept
                }
            )

            Mapping model::NumericViewMapping
            (
                NumericView: Pure
                {
                    ~src RawPerson
                    name: $src.firstName,
                    ageInMonths: $src.age * 12,
                    salaryCents: $src.salary * 100
                }
            )

            // ===== L2 M2M Mappings (2-hop) =====
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

            // ===== L3 M2M Mapping (3-hop) =====
            Mapping model::DirectoryMapping
            (
                DirectoryEntry: Pure
                {
                    ~src StaffCard
                    entry: $src.displayName + ' - ' + $src.department
                }
            )

            // ===== L4 M2M Mapping (4-hop) =====
            Mapping model::BadgeMapping
            (
                Badge: Pure
                {
                    ~src DirectoryEntry
                    text: '[' + $src.entry + ']'
                }
            )

            // ===== Connection & Runtime =====
            RelationalDatabaseConnection store::Conn
            {
                type: DuckDB;
                specification: InMemory { };
                auth: NoAuth { };
            }

            Runtime test::ChainRT
            {
                mappings:
                [
                    model::StaffMemberMapping,
                    model::ActiveStaffMapping,
                    model::NumericViewMapping,
                    model::StaffCardMapping,
                    model::StaffBadgeMapping,
                    model::DirectoryMapping,
                    model::BadgeMapping
                ];
                connections:
                [
                    store::TestDb:
                    [
                        env: store::Conn
                    ],
                    ModelStore:
                    [
                        json: #{
                            JsonModelConnection
                            {
                                class: model::RawPerson;
                                url: 'data:application/json,""" + PERSON_JSON + """
            ';
                            }
                        }#
                    ]
                ];
            }
            """;

    /**
     * Single-element JSON source — edge case with 1 row.
     */
    private static final String SINGLE_MODEL = """
            import model::*;
            import store::*;

            Class model::RawPerson
            {
                firstName: String[1];
                lastName:  String[1];
                age:       Integer[1];
                dept:      String[1];
                salary:    Integer[1];
                active:    Boolean[1];
            }

            Class model::StaffMember
            {
                fullName:  String[1];
                firstName: String[1];
                lastName:  String[1];
                age:       Integer[1];
                dept:      String[1];
            }

            Mapping model::StaffMemberMapping
            (
                StaffMember: Pure
                {
                    ~src RawPerson
                    fullName: $src.firstName + ' ' + $src.lastName,
                    firstName: $src.firstName,
                    lastName: $src.lastName,
                    age: $src.age,
                    dept: $src.dept
                }
            )

            RelationalDatabaseConnection store::Conn
            {
                type: DuckDB;
                specification: InMemory { };
                auth: NoAuth { };
            }

            Runtime test::SingleRT
            {
                mappings: [ model::StaffMemberMapping ];
                connections:
                [
                    store::TestDb: [ env: store::Conn ],
                    ModelStore:
                    [
                        json: #{
                            JsonModelConnection
                            {
                                class: model::RawPerson;
                                url: 'data:application/json,""" + SINGLE_PERSON_JSON + """
            ';
                            }
                        }#
                    ]
                ];
            }
            """;

    // ==================== Setup / Teardown ====================

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null) conn.close();
    }

    // ==================== Helpers ====================

    private ExecutionResult exec(String model, String runtime, String query) throws SQLException {
        return qs.execute(model, query, runtime, conn);
    }

    private String execGraph(String model, String runtime, String query) throws SQLException {
        var result = exec(model, runtime, query);
        assertInstanceOf(ExecutionResult.GraphResult.class, result);
        return result.asGraph().json();
    }

    private List<String> col(ExecutionResult r, int idx) {
        return r.rows().stream()
                .map(row -> row.get(idx) != null ? row.get(idx).toString() : null)
                .collect(Collectors.toList());
    }

    // shorthand for chain model
    private ExecutionResult exec(String query) throws SQLException {
        return exec(CHAIN_MODEL, "test::ChainRT", query);
    }

    private String execGraph(String query) throws SQLException {
        return execGraph(CHAIN_MODEL, "test::ChainRT", query);
    }

    // ==================== L1: Single-hop JSON → M2M ====================

    @Nested
    @DisplayName("L1: Single-hop M2M from JSON")
    class L1SingleHop {

        @Test
        @DisplayName("project: all properties")
        void testProjectAllProps() throws SQLException {
            var r = exec("StaffMember.all()->project(~[fullName:x|$x.fullName, firstName:x|$x.firstName, lastName:x|$x.lastName, age:x|$x.age, dept:x|$x.dept])");
            assertEquals(4, r.rowCount());
            var names = col(r, 0);
            assertTrue(names.contains("Alice Smith"));
            assertTrue(names.contains("Bob Jones"));
            assertTrue(names.contains("Carol White"));
            assertTrue(names.contains("Dave Brown"));
        }

        @Test
        @DisplayName("project: single computed column")
        void testProjectSingleComputed() throws SQLException {
            var r = exec("StaffMember.all()->project(~[fullName:x|$x.fullName])");
            assertEquals(4, r.rowCount());
            assertTrue(col(r, 0).contains("Alice Smith"));
        }

        @Test
        @DisplayName("project: passthrough properties unchanged")
        void testProjectPassthrough() throws SQLException {
            var r = exec("StaffMember.all()->project(~[firstName:x|$x.firstName, dept:x|$x.dept])");
            assertEquals(4, r.rowCount());
            assertTrue(col(r, 0).contains("Alice"));
            assertTrue(col(r, 1).contains("Engineering"));
        }

        @Test
        @DisplayName("graphFetch: StaffMember with all props")
        void testGraphFetch() throws SQLException {
            var json = execGraph("""
                    StaffMember.all()
                        ->graphFetch(#{ StaffMember { fullName, firstName, lastName, age, dept } }#)
                        ->serialize(#{ StaffMember { fullName, firstName, lastName, age, dept } }#)
                    """);
            assertTrue(json.contains("Alice Smith"));
            assertTrue(json.contains("Alice"));
            assertTrue(json.contains("Smith"));
            assertTrue(json.contains("30"));
            assertTrue(json.contains("Engineering"));
        }

        @Test
        @DisplayName("graphFetch: partial selection (only fullName)")
        void testGraphFetchPartial() throws SQLException {
            var json = execGraph("""
                    StaffMember.all()
                        ->graphFetch(#{ StaffMember { fullName } }#)
                        ->serialize(#{ StaffMember { fullName } }#)
                    """);
            assertTrue(json.contains("Alice Smith"));
            assertTrue(json.contains("Bob Jones"));
            assertFalse(json.contains("Engineering"), "dept not requested");
        }

        @Test
        @DisplayName("project: user filter on string equality")
        void testProjectFilterStringEq() throws SQLException {
            var r = exec("StaffMember.all()->filter({x|$x.dept == 'Engineering'})->project(~[fullName:x|$x.fullName])");
            assertEquals(2, r.rowCount());
            var names = col(r, 0);
            assertTrue(names.contains("Alice Smith"));
            assertTrue(names.contains("Carol White"));
        }

        @Test
        @DisplayName("project: user filter on numeric comparison")
        void testProjectFilterNumeric() throws SQLException {
            var r = exec("StaffMember.all()->filter({x|$x.age > 40})->project(~[fullName:x|$x.fullName, age:x|$x.age])");
            assertEquals(2, r.rowCount());
            var names = col(r, 0);
            assertTrue(names.contains("Bob Jones"));
            assertTrue(names.contains("Dave Brown"));
        }

        @Test
        @DisplayName("project: mapping filter excludes inactive")
        void testProjectMappingFilter() throws SQLException {
            var r = exec("ActiveStaff.all()->project(~[fullName:x|$x.fullName, dept:x|$x.dept])");
            assertEquals(3, r.rowCount(), "Carol is inactive");
            var names = col(r, 0);
            assertTrue(names.contains("Alice Smith"));
            assertTrue(names.contains("Bob Jones"));
            assertTrue(names.contains("Dave Brown"));
            assertFalse(names.contains("Carol White"));
        }

        @Test
        @DisplayName("project: mapping filter + user filter compose")
        void testProjectMappingAndUserFilter() throws SQLException {
            var r = exec("ActiveStaff.all()->filter({x|$x.dept == 'Engineering'})->project(~[fullName:x|$x.fullName])");
            assertEquals(1, r.rowCount(), "Only Alice is active + Engineering");
            assertEquals("Alice Smith", col(r, 0).get(0));
        }

        @Test
        @DisplayName("graphFetch: ActiveStaff mapping filter + partial selection")
        void testGraphFetchMappingFilterPartial() throws SQLException {
            var json = execGraph("""
                    ActiveStaff.all()
                        ->graphFetch(#{ ActiveStaff { fullName } }#)
                        ->serialize(#{ ActiveStaff { fullName } }#)
                    """);
            assertTrue(json.contains("Alice Smith"));
            assertTrue(json.contains("Bob Jones"));
            assertTrue(json.contains("Dave Brown"));
            assertFalse(json.contains("Carol White"), "Carol inactive");
        }

        @Test
        @DisplayName("graphFetch: with mapping filter")
        void testGraphFetchMappingFilter() throws SQLException {
            var json = execGraph("""
                    ActiveStaff.all()
                        ->graphFetch(#{ ActiveStaff { fullName, dept } }#)
                        ->serialize(#{ ActiveStaff { fullName, dept } }#)
                    """);
            assertTrue(json.contains("Alice Smith"));
            assertFalse(json.contains("Carol White"), "Carol inactive");
        }

        @Test
        @DisplayName("project: numeric computation through M2M")
        void testProjectNumericComputation() throws SQLException {
            var r = exec("NumericView.all()->project(~[name:x|$x.name, ageInMonths:x|$x.ageInMonths, salaryCents:x|$x.salaryCents])");
            assertEquals(4, r.rowCount());
            // Alice: age=30 → 360 months, salary=95000 → 9500000 cents
            var names = col(r, 0);
            var months = col(r, 1);
            int aliceIdx = names.indexOf("Alice");
            assertEquals("360", months.get(aliceIdx));
        }
    }

    // ==================== L2: Two-hop JSON → M2M → M2M ====================

    @Nested
    @DisplayName("L2: Two-hop M2M chains from JSON")
    class L2TwoHop {

        @Test
        @DisplayName("project: StaffCard (toUpper fullName)")
        void testProjectTwoHop() throws SQLException {
            var r = exec("StaffCard.all()->project(~[displayName:x|$x.displayName, department:x|$x.department])");
            assertEquals(4, r.rowCount());
            var names = col(r, 0);
            assertTrue(names.contains("ALICE SMITH"));
            assertTrue(names.contains("BOB JONES"));
            assertTrue(names.contains("CAROL WHITE"));
            assertTrue(names.contains("DAVE BROWN"));
        }

        @Test
        @DisplayName("graphFetch: StaffCard 2-hop")
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
        @DisplayName("project: StaffBadge — 2-hop with mapping filter at L1")
        void testProjectTwoHopWithFilterAtL1() throws SQLException {
            var r = exec("StaffBadge.all()->project(~[label:x|$x.label])");
            assertEquals(3, r.rowCount(), "Carol excluded by ActiveStaff filter");
            var labels = col(r, 0);
            assertTrue(labels.contains("Alice Smith [Engineering]"));
            assertTrue(labels.contains("Bob Jones [Sales]"));
            assertTrue(labels.contains("Dave Brown [Marketing]"));
            assertFalse(labels.stream().anyMatch(l -> l != null && l.contains("Carol")));
        }

        @Test
        @DisplayName("graphFetch: StaffBadge with filter")
        void testGraphFetchTwoHopWithFilter() throws SQLException {
            var json = execGraph("""
                    StaffBadge.all()
                        ->graphFetch(#{ StaffBadge { label } }#)
                        ->serialize(#{ StaffBadge { label } }#)
                    """);
            assertTrue(json.contains("Alice Smith [Engineering]"));
            assertFalse(json.contains("Carol"), "excluded by ActiveStaff filter");
        }

        @Test
        @DisplayName("project: 2-hop + user query filter")
        void testProjectTwoHopWithUserFilter() throws SQLException {
            var r = exec("StaffCard.all()->filter({x|$x.department == 'Engineering'})->project(~[displayName:x|$x.displayName])");
            assertEquals(2, r.rowCount());
            var names = col(r, 0);
            assertTrue(names.contains("ALICE SMITH"));
            assertTrue(names.contains("CAROL WHITE"));
        }

        @Test
        @DisplayName("project: 2-hop + user filter on computed column")
        void testProjectTwoHopFilterOnComputed() throws SQLException {
            var r = exec("StaffCard.all()->filter({x|$x.displayName->contains('BOB')})->project(~[displayName:x|$x.displayName, department:x|$x.department])");
            assertEquals(1, r.rowCount());
            assertEquals("BOB JONES", col(r, 0).get(0));
            assertEquals("Sales", col(r, 1).get(0));
        }
    }

    // ==================== L3: Three-hop JSON → M2M → M2M → M2M ====================

    @Nested
    @DisplayName("L3: Three-hop M2M chains from JSON")
    class L3ThreeHop {

        @Test
        @DisplayName("project: DirectoryEntry 3-hop chain")
        void testProjectThreeHop() throws SQLException {
            var r = exec("DirectoryEntry.all()->project(~[entry:x|$x.entry])");
            assertEquals(4, r.rowCount());
            var entries = col(r, 0);
            assertTrue(entries.contains("ALICE SMITH - Engineering"));
            assertTrue(entries.contains("BOB JONES - Sales"));
            assertTrue(entries.contains("CAROL WHITE - Engineering"));
            assertTrue(entries.contains("DAVE BROWN - Marketing"));
        }

        @Test
        @DisplayName("graphFetch: DirectoryEntry 3-hop")
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
        @DisplayName("project: 3-hop + filter on string contains")
        void testProjectThreeHopWithFilter() throws SQLException {
            var r = exec("DirectoryEntry.all()->filter({x|$x.entry->contains('Sales')})->project(~[entry:x|$x.entry])");
            assertEquals(1, r.rowCount());
            assertEquals("BOB JONES - Sales", col(r, 0).get(0));
        }

        @Test
        @DisplayName("project: 3-hop + filter yields no results")
        void testProjectThreeHopFilterEmpty() throws SQLException {
            var r = exec("DirectoryEntry.all()->filter({x|$x.entry->contains('NonExistent')})->project(~[entry:x|$x.entry])");
            assertEquals(0, r.rowCount());
        }
    }

    // ==================== L4: Four-hop JSON → M2M → M2M → M2M → M2M ====================

    @Nested
    @DisplayName("L4: Four-hop M2M chains from JSON")
    class L4FourHop {

        @Test
        @DisplayName("project: Badge 4-hop chain")
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
        @DisplayName("graphFetch: Badge 4-hop chain")
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
        @DisplayName("project: 4-hop + filter")
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
        @DisplayName("single-element JSON array")
        void testSingleElement() throws SQLException {
            var r = exec(SINGLE_MODEL, "test::SingleRT",
                    "StaffMember.all()->project(~[fullName:x|$x.fullName, age:x|$x.age])");
            assertEquals(1, r.rowCount());
            assertEquals("Solo Person", col(r, 0).get(0));
        }

        @Test
        @DisplayName("single-element JSON with graphFetch")
        void testSingleElementGraphFetch() throws SQLException {
            var json = execGraph(SINGLE_MODEL, "test::SingleRT", """
                    StaffMember.all()
                        ->graphFetch(#{ StaffMember { fullName, age } }#)
                        ->serialize(#{ StaffMember { fullName, age } }#)
                    """);
            assertTrue(json.contains("Solo Person"));
            assertTrue(json.contains("99"));
        }

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
            // Ages: 28, 30, 45, 55
            assertEquals("Carol White", col(r, 0).get(0));
            assertEquals("Dave Brown", col(r, 0).get(3));
        }

        @Test
        @DisplayName("2-hop + sort")
        void testTwoHopSort() throws SQLException {
            var r = exec("StaffCard.all()->sort({x|$x.displayName})->project(~[displayName:x|$x.displayName])");
            assertEquals(4, r.rowCount());
            // Alphabetical: ALICE SMITH, BOB JONES, CAROL WHITE, DAVE BROWN
            assertEquals("ALICE SMITH", col(r, 0).get(0));
            assertEquals("BOB JONES", col(r, 0).get(1));
        }
    }

    // ==================== Multiple JSON Sources ====================

    @Nested
    @DisplayName("Multiple JSON Sources")
    class MultiSource {

        /**
         * Model with two separate JSON classes connected via M2M.
         * RawPerson JSON + separate RawDept JSON, joined conceptually via dept name.
         */
        private static final String MULTI_JSON_MODEL = """
                import model::*;
                import store::*;

                Class model::RawPerson
                {
                    firstName: String[1];
                    lastName:  String[1];
                    age:       Integer[1];
                    dept:      String[1];
                    salary:    Integer[1];
                    active:    Boolean[1];
                }

                Class model::RawDept
                {
                    name:      String[1];
                    location:  String[1];
                    headcount: Integer[1];
                }

                Class model::PersonSummary
                {
                    fullName: String[1];
                    dept:     String[1];
                    age:      Integer[1];
                }

                Class model::DeptSummary
                {
                    name:      String[1];
                    location:  String[1];
                    headcount: Integer[1];
                }

                Mapping model::PersonSummaryMapping
                (
                    PersonSummary: Pure
                    {
                        ~src RawPerson
                        fullName: $src.firstName + ' ' + $src.lastName,
                        dept: $src.dept,
                        age: $src.age
                    }
                )

                Mapping model::DeptSummaryMapping
                (
                    DeptSummary: Pure
                    {
                        ~src RawDept
                        name: $src.name,
                        location: $src.location,
                        headcount: $src.headcount
                    }
                )

                RelationalDatabaseConnection store::Conn
                {
                    type: DuckDB;
                    specification: InMemory { };
                    auth: NoAuth { };
                }

                Runtime test::MultiRT
                {
                    mappings:
                    [
                        model::PersonSummaryMapping,
                        model::DeptSummaryMapping
                    ];
                    connections:
                    [
                        store::TestDb: [ env: store::Conn ],
                        ModelStore:
                        [
                            json: #{
                                JsonModelConnection
                                {
                                    class: model::RawPerson;
                                    url: 'data:application/json,""" + PERSON_JSON + """
                ';
                                }
                            }#,
                            json2: #{
                                JsonModelConnection
                                {
                                    class: model::RawDept;
                                    url: 'data:application/json,""" + DEPT_JSON + """
                ';
                                }
                            }#
                        ]
                    ];
                }
                """;

        @Test
        @DisplayName("project: PersonSummary from first JSON source")
        void testPersonFromFirstSource() throws SQLException {
            var r = exec(MULTI_JSON_MODEL, "test::MultiRT",
                    "PersonSummary.all()->project(~[fullName:x|$x.fullName, dept:x|$x.dept])");
            assertEquals(4, r.rowCount());
            assertTrue(col(r, 0).contains("Alice Smith"));
        }

        @Test
        @DisplayName("project: DeptSummary from second JSON source")
        void testDeptFromSecondSource() throws SQLException {
            var r = exec(MULTI_JSON_MODEL, "test::MultiRT",
                    "DeptSummary.all()->project(~[name:x|$x.name, location:x|$x.location, headcount:x|$x.headcount])");
            assertEquals(3, r.rowCount());
            var names = col(r, 0);
            assertTrue(names.contains("Engineering"));
            assertTrue(names.contains("Sales"));
            assertTrue(names.contains("Marketing"));
            var locations = col(r, 1);
            assertTrue(locations.contains("San Francisco"));
        }

        @Test
        @DisplayName("graphFetch: DeptSummary from second JSON source")
        void testDeptGraphFetch() throws SQLException {
            var json = execGraph(MULTI_JSON_MODEL, "test::MultiRT", """
                    DeptSummary.all()
                        ->graphFetch(#{ DeptSummary { name, location, headcount } }#)
                        ->serialize(#{ DeptSummary { name, location, headcount } }#)
                    """);
            assertTrue(json.contains("Engineering"));
            assertTrue(json.contains("San Francisco"));
            assertTrue(json.contains("150"));
        }

        @Test
        @DisplayName("filter: DeptSummary headcount > 100")
        void testDeptFilterNumeric() throws SQLException {
            var r = exec(MULTI_JSON_MODEL, "test::MultiRT",
                    "DeptSummary.all()->filter({x|$x.headcount > 100})->project(~[name:x|$x.name])");
            assertEquals(1, r.rowCount());
            assertEquals("Engineering", col(r, 0).get(0));
        }

        @Test
        @DisplayName("both sources in same runtime, each queried independently")
        void testBothSourcesIndependent() throws SQLException {
            var persons = exec(MULTI_JSON_MODEL, "test::MultiRT",
                    "PersonSummary.all()->project(~[fullName:x|$x.fullName])");
            var depts = exec(MULTI_JSON_MODEL, "test::MultiRT",
                    "DeptSummary.all()->project(~[name:x|$x.name])");
            assertEquals(4, persons.rowCount());
            assertEquals(3, depts.rowCount());
        }
    }

    // ==================== File-based JSON source ====================

    /**
     * Tests M2M from external JSON files in all 3 DuckDB-supported formats:
     *
     * <pre>
     * Format              File                        DuckDB reads via
     * ─────────────────── ─────────────────────────── ──────────────────────────────
     * NDJSON              persons.json                read_json_objects (newline_delimited)
     * JSON Array          persons-array.json          read_json_objects (array)
     * Unstructured        persons-unstructured.json   read_json_objects (unstructured)
     * </pre>
     *
     * All 3 formats contain the same 4 persons. DuckDB auto-detects the format.
     */
    @Nested
    @DisplayName("File Source: M2M from external JSON files (NDJSON / Array / Unstructured)")
    class FileSource {

        /**
         * Resolves a classpath resource to a file: URL for use in Pure JsonModelConnection.
         */
        private String fileUrl(String resourcePath) {
            var url = getClass().getClassLoader().getResource(resourcePath);
            assertNotNull(url, resourcePath + " must be on classpath");
            return url.toString();
        }

        private String fileModel(String resourcePath) {
            return """
                    import model::*;
                    import store::*;

                    Class model::RawPerson
                    {
                        firstName: String[1];
                        lastName:  String[1];
                        age:       Integer[1];
                        dept:      String[1];
                        salary:    Integer[1];
                        active:    Boolean[1];
                    }

                    Class model::StaffMember
                    {
                        fullName:  String[1];
                        age:       Integer[1];
                        dept:      String[1];
                    }

                    Class model::StaffCard
                    {
                        displayName: String[1];
                        department:  String[1];
                    }

                    Mapping model::StaffMemberMapping
                    (
                        StaffMember: Pure
                        {
                            ~src RawPerson
                            fullName: $src.firstName + ' ' + $src.lastName,
                            age: $src.age,
                            dept: $src.dept
                        }
                    )

                    Mapping model::StaffCardMapping
                    (
                        StaffCard: Pure
                        {
                            ~src StaffMember
                            displayName: $src.fullName->toUpper(),
                            department: $src.dept
                        }
                    )

                    RelationalDatabaseConnection store::Conn
                    {
                        type: DuckDB;
                        specification: InMemory { };
                        auth: NoAuth { };
                    }

                    Runtime test::FileRT
                    {
                        mappings: [ model::StaffMemberMapping, model::StaffCardMapping ];
                        connections:
                        [
                            store::TestDb: [ env: store::Conn ],
                            ModelStore:
                            [
                                json: #{
                                    JsonModelConnection
                                    {
                                        class: model::RawPerson;
                                        url: '""" + fileUrl(resourcePath) + """
                    ';
                                    }
                                }#
                            ]
                        ];
                    }
                    """;
        }

        // ---------- shared assertions ----------

        private void assertL1Project(String resourcePath) throws SQLException {
            var r = exec(fileModel(resourcePath), "test::FileRT",
                    "StaffMember.all()->project(~[fullName:x|$x.fullName, age:x|$x.age, dept:x|$x.dept])");
            assertEquals(4, r.rowCount());
            var names = col(r, 0);
            assertTrue(names.contains("Alice Smith"));
            assertTrue(names.contains("Bob Jones"));
            assertTrue(names.contains("Carol White"));
            assertTrue(names.contains("Dave Brown"));
        }

        private void assertL1GraphFetch(String resourcePath) throws SQLException {
            var json = execGraph(fileModel(resourcePath), "test::FileRT", """
                    StaffMember.all()
                        ->graphFetch(#{ StaffMember { fullName, age, dept } }#)
                        ->serialize(#{ StaffMember { fullName, age, dept } }#)
                    """);
            assertTrue(json.contains("Alice Smith"));
            assertTrue(json.contains("30"));
            assertTrue(json.contains("Engineering"));
        }

        private void assertL1Filter(String resourcePath) throws SQLException {
            var r = exec(fileModel(resourcePath), "test::FileRT",
                    "StaffMember.all()->filter({x|$x.age > 40})->project(~[fullName:x|$x.fullName])");
            assertEquals(2, r.rowCount());
            var names = col(r, 0);
            assertTrue(names.contains("Bob Jones"));
            assertTrue(names.contains("Dave Brown"));
        }

        private void assertL2Chain(String resourcePath) throws SQLException {
            var r = exec(fileModel(resourcePath), "test::FileRT",
                    "StaffCard.all()->project(~[displayName:x|$x.displayName, department:x|$x.department])");
            assertEquals(4, r.rowCount());
            var names = col(r, 0);
            assertTrue(names.contains("ALICE SMITH"));
            assertTrue(names.contains("BOB JONES"));
        }

        private void assertL2Filter(String resourcePath) throws SQLException {
            var r = exec(fileModel(resourcePath), "test::FileRT",
                    "StaffCard.all()->filter({x|$x.department == 'Engineering'})->project(~[displayName:x|$x.displayName])");
            assertEquals(2, r.rowCount());
            var names = col(r, 0);
            assertTrue(names.contains("ALICE SMITH"));
            assertTrue(names.contains("CAROL WHITE"));
        }

        // ---------- NDJSON (newline-delimited) ----------

        @Test
        @DisplayName("NDJSON: L1 project")
        void testNdjsonProject() throws SQLException {
            assertL1Project("test-data/persons-ndjson.txt");
        }

        @Test
        @DisplayName("NDJSON: L1 graphFetch")
        void testNdjsonGraphFetch() throws SQLException {
            assertL1GraphFetch("test-data/persons-ndjson.txt");
        }

        @Test
        @DisplayName("NDJSON: L1 filter")
        void testNdjsonFilter() throws SQLException {
            assertL1Filter("test-data/persons-ndjson.txt");
        }

        @Test
        @DisplayName("NDJSON: L2 chain")
        void testNdjsonTwoHop() throws SQLException {
            assertL2Chain("test-data/persons-ndjson.txt");
        }

        @Test
        @DisplayName("NDJSON: L2 filter")
        void testNdjsonTwoHopFilter() throws SQLException {
            assertL2Filter("test-data/persons-ndjson.txt");
        }

        // ---------- JSON Array ----------

        @Test
        @DisplayName("JSON Array: L1 project")
        void testArrayProject() throws SQLException {
            assertL1Project("test-data/persons-array.json");
        }

        @Test
        @DisplayName("JSON Array: L1 graphFetch")
        void testArrayGraphFetch() throws SQLException {
            assertL1GraphFetch("test-data/persons-array.json");
        }

        @Test
        @DisplayName("JSON Array: L1 filter")
        void testArrayFilter() throws SQLException {
            assertL1Filter("test-data/persons-array.json");
        }

        @Test
        @DisplayName("JSON Array: L2 chain")
        void testArrayTwoHop() throws SQLException {
            assertL2Chain("test-data/persons-array.json");
        }

        @Test
        @DisplayName("JSON Array: L2 filter")
        void testArrayTwoHopFilter() throws SQLException {
            assertL2Filter("test-data/persons-array.json");
        }

        // ---------- Unstructured (whitespace-separated objects) ----------

        @Test
        @DisplayName("Unstructured: L1 project")
        void testUnstructuredProject() throws SQLException {
            assertL1Project("test-data/persons-unstructured.txt");
        }

        @Test
        @DisplayName("Unstructured: L1 graphFetch")
        void testUnstructuredGraphFetch() throws SQLException {
            assertL1GraphFetch("test-data/persons-unstructured.txt");
        }

        @Test
        @DisplayName("Unstructured: L1 filter")
        void testUnstructuredFilter() throws SQLException {
            assertL1Filter("test-data/persons-unstructured.txt");
        }

        @Test
        @DisplayName("Unstructured: L2 chain")
        void testUnstructuredTwoHop() throws SQLException {
            assertL2Chain("test-data/persons-unstructured.txt");
        }

        @Test
        @DisplayName("Unstructured: L2 filter")
        void testUnstructuredTwoHopFilter() throws SQLException {
            assertL2Filter("test-data/persons-unstructured.txt");
        }
    }
}
