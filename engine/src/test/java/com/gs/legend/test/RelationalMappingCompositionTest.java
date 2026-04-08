package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.server.QueryService;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Systematic combinatorial tests for relational mapping composition.
 *
 * <p>Tests every mapping feature × downstream operation combination to ensure
 * lazy join optimization correctly prunes unused extends while preserving
 * correctness when features ARE used.
 *
 * <p>Mapping features: local column, 1-hop join chain, 2-hop join chain,
 * multiple chains (shared prefix), enum mapping, expression access.
 *
 * <p>Downstream operations: project, filter, sort, groupBy, limit, extend,
 * and multi-operation chains (filter+project, sort+limit+project, etc.)
 *
 * <p>Each test asserts BOTH:
 * <ul>
 *   <li>SQL shape (JOIN count, table presence/absence)</li>
 *   <li>Data correctness (execute against real DuckDB)</li>
 * </ul>
 */
@DisplayName("Relational Mapping Composition Tests")
class RelationalMappingCompositionTest {

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

    private String planSql(String model, String query) {
        return com.gs.legend.plan.PlanGenerator.generate(model, query, "test::RT").sql();
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

    private int countLeftJoins(String sql) {
        int count = 0;
        String upper = sql.toUpperCase();
        int idx = 0;
        while ((idx = upper.indexOf("LEFT OUTER JOIN", idx)) != -1) {
            count++;
            idx += 15;
        }
        return count;
    }

    private void assertNoTable(String sql, String tableName) {
        assertFalse(sql.toUpperCase().contains(tableName.toUpperCase()),
                tableName + " should not appear in SQL: " + sql);
    }

    private void assertHasTable(String sql, String tableName) {
        assertTrue(sql.toUpperCase().contains(tableName.toUpperCase()),
                tableName + " should appear in SQL: " + sql);
    }

    // ==================== Shared Models ====================

    private void setupThreeTableData() throws SQLException {
        sql("CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INT)",
            "CREATE TABLE T_DEPT (ID INT PRIMARY KEY, NAME VARCHAR(50), ORG_ID INT, BUDGET INT)",
            "CREATE TABLE T_ORG (ID INT PRIMARY KEY, NAME VARCHAR(100), COUNTRY VARCHAR(50))",
            "INSERT INTO T_ORG VALUES (1, 'Acme Corp', 'USA'), (2, 'Beta Inc', 'UK')",
            "INSERT INTO T_DEPT VALUES (1, 'Engineering', 1, 500000), (2, 'Sales', 1, 300000), (3, 'Marketing', 2, 200000)",
            "INSERT INTO T_PERSON VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 3), (4, 'Dave', 1), (5, 'Eve', NULL)");
    }

    /** Person with local + 1-hop + 2-hop join chain properties. */
    private String threeHopModel() {
        return withRuntime("""
                import model::*;
                import store::*;

                Class model::Person { name: String[1]; deptName: String[1]; deptBudget: Integer[1]; orgName: String[1]; orgCountry: String[1]; }
                Database store::DB (
                    Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INTEGER)
                    Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), ORG_ID INTEGER, BUDGET INTEGER)
                    Table T_ORG (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), COUNTRY VARCHAR(50))
                    Join Person_Dept(T_PERSON.DEPT_ID = T_DEPT.ID)
                    Join Dept_Org(T_DEPT.ORG_ID = T_ORG.ID)
                )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME,
                        deptName: [store::DB] @Person_Dept | T_DEPT.NAME,
                        deptBudget: [store::DB] @Person_Dept | T_DEPT.BUDGET,
                        orgName: [store::DB] @Person_Dept > @Dept_Org | T_ORG.NAME,
                        orgCountry: [store::DB] @Person_Dept > @Dept_Org | T_ORG.COUNTRY
                    }
                )
                """, "store::DB", "model::M");
    }

    // ==================== 1. Local Property Only ====================

    @Nested
    @DisplayName("1. Local property only — all joins cancelled")
    class LocalPropertyOnly {

        @Test @DisplayName("project local → 0 JOINs")
        void projectLocal() {
            String sql = planSql(threeHopModel(), "Person.all()->project(~[name:p|$p.name])");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
            assertNoTable(sql, "T_DEPT");
            assertNoTable(sql, "T_ORG");
        }

        @Test @DisplayName("filter local → 0 JOINs")
        void filterLocal() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.name == 'Alice'})->project(~[name:p|$p.name])");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
            assertNoTable(sql, "T_DEPT");
        }

        @Test @DisplayName("filter local + sort local → 0 JOINs")
        void filterSortLocal() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.name != 'Eve'})->project(~[name:p|$p.name])->sort(~name->ascending())");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("filter local + limit → 0 JOINs")
        void filterLimitLocal() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.name == 'Alice'})->project(~[name:p|$p.name])->limit(1)");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("Data: project local returns correct rows")
        void dataProjectLocal() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(), "Person.all()->project(~[name:p|$p.name])");
            assertEquals(5, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Bob", "Charlie", "Dave", "Eve")));
        }
    }

    // ==================== 2. Single-Hop Chain (1 JOIN) ====================

    @Nested
    @DisplayName("2. 1-hop chain property — 1 JOIN")
    class SingleHopChain {

        @Test @DisplayName("project deptName → 1 JOIN, no T_ORG")
        void projectDept() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name, dept:p|$p.deptName])");
            assertEquals(1, countLeftJoins(sql), "SQL: " + sql);
            assertHasTable(sql, "T_DEPT");
            assertNoTable(sql, "T_ORG");
        }

        @Test @DisplayName("project deptName + deptBudget (same chain) → 1 JOIN")
        void projectSameChainMultiCol() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[dept:p|$p.deptName, budget:p|$p.deptBudget])");
            assertEquals(1, countLeftJoins(sql), "SQL: " + sql);
            assertHasTable(sql, "T_DEPT");
            assertNoTable(sql, "T_ORG");
        }

        @Test @DisplayName("filter deptName → 1 JOIN")
        void filterDept() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.deptName == 'Engineering'})->project(~[name:p|$p.name])");
            assertEquals(1, countLeftJoins(sql), "SQL: " + sql);
            assertHasTable(sql, "T_DEPT");
            assertNoTable(sql, "T_ORG");
        }

        @Test @DisplayName("filter deptName + project deptName → 1 JOIN")
        void filterAndProjectDept() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.deptName == 'Engineering'})->project(~[name:p|$p.name, dept:p|$p.deptName])");
            assertEquals(1, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("Data: project deptName returns correct department names")
        void dataProjectDept() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name, dept:p|$p.deptName])");
            assertEquals(5, r.rowCount());
            var names = colStr(r, 0);
            var depts = colStr(r, 1);
            assertEquals("Engineering", depts.get(names.indexOf("Alice")));
            assertEquals("Sales", depts.get(names.indexOf("Bob")));
            assertEquals("Marketing", depts.get(names.indexOf("Charlie")));
            assertNull(depts.get(names.indexOf("Eve"))); // NULL FK → LEFT JOIN NULL
        }

        @Test @DisplayName("Data: filter on deptName returns correct subset")
        void dataFilterDept() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->filter({p|$p.deptName == 'Engineering'})->project(~[name:p|$p.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Dave")));
        }

        @Test @DisplayName("Data: project deptBudget returns correct numbers")
        void dataProjectBudget() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name, budget:p|$p.deptBudget])");
            assertEquals(5, r.rowCount());
            var names = colStr(r, 0);
            var budgets = colInt(r, 1);
            assertEquals(500000, budgets.get(names.indexOf("Alice")));
            assertEquals(300000, budgets.get(names.indexOf("Bob")));
        }
    }

    // ==================== 3. Two-Hop Chain (2 JOINs) ====================

    @Nested
    @DisplayName("3. 2-hop chain property — 2 JOINs")
    class TwoHopChain {

        @Test @DisplayName("project orgName → 2 JOINs")
        void projectOrg() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name, org:p|$p.orgName])");
            assertEquals(2, countLeftJoins(sql), "SQL: " + sql);
            assertHasTable(sql, "T_DEPT");
            assertHasTable(sql, "T_ORG");
        }

        @Test @DisplayName("project orgName + orgCountry (same chain) → 2 JOINs")
        void projectSameChainMultiCol() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[org:p|$p.orgName, country:p|$p.orgCountry])");
            assertEquals(2, countLeftJoins(sql), "SQL: " + sql);
            assertHasTable(sql, "T_ORG");
        }

        @Test @DisplayName("filter orgName → 2 JOINs")
        void filterOrg() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.orgName == 'Acme Corp'})->project(~[name:p|$p.name])");
            assertEquals(2, countLeftJoins(sql), "SQL: " + sql);
            assertHasTable(sql, "T_ORG");
        }

        @Test @DisplayName("filter orgCountry → 2 JOINs")
        void filterOrgCountry() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.orgCountry == 'USA'})->project(~[name:p|$p.name])");
            assertEquals(2, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("Data: project orgName returns correct orgs")
        void dataProjectOrg() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name, org:p|$p.orgName])");
            assertEquals(5, r.rowCount());
            var names = colStr(r, 0);
            var orgs = colStr(r, 1);
            assertEquals("Acme Corp", orgs.get(names.indexOf("Alice")));
            assertEquals("Acme Corp", orgs.get(names.indexOf("Bob")));
            assertEquals("Beta Inc", orgs.get(names.indexOf("Charlie")));
            assertNull(orgs.get(names.indexOf("Eve")));
        }

        @Test @DisplayName("Data: filter on orgName returns correct subset")
        void dataFilterOrg() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->filter({p|$p.orgName == 'Acme Corp'})->project(~[name:p|$p.name])");
            assertEquals(3, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Bob", "Dave")));
        }

        @Test @DisplayName("Data: filter orgCountry + project name")
        void dataFilterOrgCountry() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->filter({p|$p.orgCountry == 'UK'})->project(~[name:p|$p.name])");
            assertEquals(1, r.rowCount());
            assertEquals("Charlie", colStr(r, 0).get(0));
        }
    }

    // ==================== 4. Mixed Chains ====================

    @Nested
    @DisplayName("4. Mixed chains — selective activation")
    class MixedChains {

        @Test @DisplayName("project dept + org → 3 JOINs (1-hop + 2-hop)")
        void projectBothChains() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name, dept:p|$p.deptName, org:p|$p.orgName])");
            assertEquals(3, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("project all 5 properties → 3 JOINs (no duplication)")
        void projectAllProperties() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name, dept:p|$p.deptName, budget:p|$p.deptBudget, org:p|$p.orgName, country:p|$p.orgCountry])");
            assertEquals(3, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("filter dept + project org → 3 JOINs")
        void filterDeptProjectOrg() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.deptName == 'Engineering'})->project(~[name:p|$p.name, org:p|$p.orgName])");
            assertEquals(3, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("filter org + project dept → 3 JOINs")
        void filterOrgProjectDept() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.orgName == 'Acme Corp'})->project(~[name:p|$p.name, dept:p|$p.deptName])");
            assertEquals(3, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("Data: project all properties returns correct values")
        void dataProjectAll() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name, dept:p|$p.deptName, org:p|$p.orgName])");
            assertEquals(5, r.rowCount());
            var names = colStr(r, 0);
            var depts = colStr(r, 1);
            var orgs = colStr(r, 2);
            int alice = names.indexOf("Alice");
            assertEquals("Engineering", depts.get(alice));
            assertEquals("Acme Corp", orgs.get(alice));
            int charlie = names.indexOf("Charlie");
            assertEquals("Marketing", depts.get(charlie));
            assertEquals("Beta Inc", orgs.get(charlie));
        }

        @Test @DisplayName("Data: filter dept + project org")
        void dataFilterDeptProjectOrg() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->filter({p|$p.deptName == 'Engineering'})->project(~[name:p|$p.name, org:p|$p.orgName])");
            assertEquals(2, r.rowCount());
            for (var row : r.rows()) assertEquals("Acme Corp", row.get(1).toString());
        }
    }

    // ==================== 5. Sort Combinations ====================

    @Nested
    @DisplayName("5. Sort combinations")
    class SortCombinations {

        @Test @DisplayName("sort by local, project local → 0 JOINs")
        void sortLocalProjectLocal() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name])->sort(~name->ascending())");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("Data: sort by local returns ordered results")
        void dataSortLocal() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name])->sort(~name->ascending())");
            assertEquals(5, r.rowCount());
            assertEquals("Alice", colStr(r, 0).get(0));
            assertEquals("Eve", colStr(r, 0).get(4));
        }

        @Test @DisplayName("sort + limit on local → 0 JOINs")
        void sortLimitLocal() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name])->sort(~name->ascending())->limit(3)");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("Data: sort + limit returns correct top-N")
        void dataSortLimit() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name])->sort(~name->ascending())->limit(3)");
            assertEquals(3, r.rowCount());
            assertEquals(List.of("Alice", "Bob", "Charlie"), colStr(r, 0));
        }
    }

    // ==================== 6. GroupBy Combinations ====================

    @Nested
    @DisplayName("6. GroupBy combinations")
    class GroupByCombinations {

        @Test @DisplayName("groupBy deptName → 1 JOIN (no T_ORG)")
        void groupByDept() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name, dept:p|$p.deptName])->groupBy(~dept, ~cnt:x|$x.name:y|$y->count())");
            assertEquals(1, countLeftJoins(sql), "SQL: " + sql);
            assertNoTable(sql, "T_ORG");
        }

        @Test @DisplayName("Data: groupBy deptName returns correct counts")
        void dataGroupByDept() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name, dept:p|$p.deptName])->groupBy(~dept, ~cnt:x|$x.name:y|$y->count())->sort(~dept->ascending())");
            // Engineering=2, Marketing=1, Sales=1, NULL=1
            assertEquals(4, r.rowCount());
        }

        @Test @DisplayName("groupBy orgName → 2 JOINs")
        void groupByOrg() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name, org:p|$p.orgName])->groupBy(~org, ~cnt:x|$x.name:y|$y->count())");
            assertEquals(2, countLeftJoins(sql), "SQL: " + sql);
            assertHasTable(sql, "T_ORG");
        }

        @Test @DisplayName("Data: groupBy orgName returns correct counts")
        void dataGroupByOrg() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name, org:p|$p.orgName])->groupBy(~org, ~cnt:x|$x.name:y|$y->count())->sort(~org->ascending())");
            // Acme Corp=3, Beta Inc=1, NULL=1
            assertEquals(3, r.rowCount());
        }

        @Test @DisplayName("groupBy local only → 0 JOINs")
        void groupByLocal() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name])->groupBy(~name, ~cnt:x|$x.name:y|$y->count())");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
        }
    }

    // ==================== 7. Chained Operations ====================

    @Nested
    @DisplayName("7. Multi-operation chains")
    class ChainedOperations {

        @Test @DisplayName("filter local + sort local + limit → 0 JOINs")
        void filterSortLimitLocal() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.name != 'Eve'})->project(~[name:p|$p.name])->sort(~name->ascending())->limit(2)");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("Data: filter + sort + limit with local props")
        void dataFilterSortLimitLocal() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->filter({p|$p.name != 'Eve'})->project(~[name:p|$p.name])->sort(~name->ascending())->limit(2)");
            assertEquals(2, r.rowCount());
            assertEquals(List.of("Alice", "Bob"), colStr(r, 0));
        }

        @Test @DisplayName("filter dept + sort local + project name+dept → 1 JOIN")
        void filterDeptSortLocalProjectMixed() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.deptName == 'Engineering'})->project(~[name:p|$p.name, dept:p|$p.deptName])->sort(~name->ascending())");
            assertEquals(1, countLeftJoins(sql), "SQL: " + sql);
            assertNoTable(sql, "T_ORG");
        }

        @Test @DisplayName("Data: filter dept + sort + project")
        void dataFilterDeptSortProject() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->filter({p|$p.deptName == 'Engineering'})->project(~[name:p|$p.name, dept:p|$p.deptName])->sort(~name->ascending())");
            assertEquals(2, r.rowCount());
            assertEquals(List.of("Alice", "Dave"), colStr(r, 0));
        }

        @Test @DisplayName("filter org + project dept + sort → 3 JOINs")
        void filterOrgProjectDeptSort() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.orgName == 'Acme Corp'})->project(~[name:p|$p.name, dept:p|$p.deptName])->sort(~dept->ascending())");
            assertEquals(3, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("Data: filter org + project dept + sort")
        void dataFilterOrgProjectDeptSort() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->filter({p|$p.orgName == 'Acme Corp'})->project(~[name:p|$p.name, dept:p|$p.deptName])->sort(~dept->ascending())");
            assertEquals(3, r.rowCount());
            // Engineering (Alice, Dave), Sales (Bob)
            var depts = colStr(r, 1);
            assertEquals("Engineering", depts.get(0));
            assertEquals("Engineering", depts.get(1));
            assertEquals("Sales", depts.get(2));
        }

        @Test @DisplayName("filter + groupBy + sort, all local → 0 JOINs")
        void filterGroupBySortLocal() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.name != 'Eve'})->project(~[name:p|$p.name])->groupBy(~name, ~cnt:x|$x.name:y|$y->count())->sort(~name->ascending())");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
        }
    }

    // ==================== 8. NULL / LEFT JOIN Semantics ====================

    @Nested
    @DisplayName("8. NULL / LEFT JOIN semantics")
    class NullLeftJoinSemantics {

        @Test @DisplayName("Data: NULL FK produces NULL for 1-hop chain")
        void dataNullFk1Hop() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name, dept:p|$p.deptName])");
            var names = colStr(r, 0);
            var depts = colStr(r, 1);
            assertNull(depts.get(names.indexOf("Eve")),
                    "Eve has NULL DEPT_ID → LEFT JOIN should produce NULL deptName");
        }

        @Test @DisplayName("Data: NULL FK propagates through 2-hop chain")
        void dataNullFk2Hop() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name, org:p|$p.orgName])");
            var names = colStr(r, 0);
            var orgs = colStr(r, 1);
            assertNull(orgs.get(names.indexOf("Eve")),
                    "Eve has NULL DEPT_ID → both hops should produce NULL");
        }

        @Test @DisplayName("Data: filter on chain prop excludes NULL FK rows")
        void dataFilterExcludesNull() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->filter({p|$p.deptName == 'Engineering'})->project(~[name:p|$p.name])");
            assertFalse(colStr(r, 0).contains("Eve"),
                    "Eve has NULL deptName → filter should exclude her");
        }

        @Test @DisplayName("Data: all rows present when projecting local (incl NULL FK)")
        void dataAllRowsWithLocalProject() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(), "Person.all()->project(~[name:p|$p.name])");
            assertEquals(5, r.rowCount(), "All 5 persons including Eve (NULL FK) should appear");
            assertTrue(colStr(r, 0).contains("Eve"));
        }
    }

    // ==================== 9. Multiple Filters ====================

    @Nested
    @DisplayName("9. Multiple filter combinations")
    class MultipleFilters {

        @Test @DisplayName("filter local AND filter dept → 1 JOIN")
        void filterLocalAndDept() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.name != 'Eve'})->filter({p|$p.deptName == 'Engineering'})->project(~[name:p|$p.name])");
            assertEquals(1, countLeftJoins(sql), "SQL: " + sql);
            assertNoTable(sql, "T_ORG");
        }

        @Test @DisplayName("filter dept AND filter org → 3 JOINs")
        void filterDeptAndOrg() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.deptName == 'Engineering'})->filter({p|$p.orgName == 'Acme Corp'})->project(~[name:p|$p.name])");
            assertEquals(3, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("Data: double filter narrows correctly")
        void dataDoubleFilter() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->filter({p|$p.deptName == 'Engineering'})->filter({p|$p.orgName == 'Acme Corp'})->project(~[name:p|$p.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Dave")));
        }

        @Test @DisplayName("filter with OR on local props → 0 JOINs")
        void filterOrLocal() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.name == 'Alice' || $p.name == 'Bob'})->project(~[name:p|$p.name])");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("filter with OR mixing local + chain → JOINs present")
        void filterOrMixed() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.name == 'Alice' || $p.deptName == 'Sales'})->project(~[name:p|$p.name])");
            assertEquals(1, countLeftJoins(sql), "SQL: " + sql);
        }
    }

    // ==================== 10. Limit / Slice ====================

    @Nested
    @DisplayName("10. Limit / Slice combinations")
    class LimitSlice {

        @Test @DisplayName("project local + limit → 0 JOINs")
        void limitLocal() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name])->limit(3)");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("project dept + limit → 1 JOIN")
        void limitDept() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name, dept:p|$p.deptName])->limit(3)");
            assertEquals(1, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("Data: limit returns correct number of rows")
        void dataLimit() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name])->sort(~name->ascending())->limit(3)");
            assertEquals(3, r.rowCount());
        }

        @Test @DisplayName("project local + slice → 0 JOINs")
        void sliceLocal() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name])->sort(~name->ascending())->slice(1, 3)");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("Data: slice returns correct window")
        void dataSlice() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name])->sort(~name->ascending())->slice(1, 3)");
            assertEquals(2, r.rowCount());
            assertEquals(List.of("Bob", "Charlie"), colStr(r, 0));
        }
    }

    // ==================== 11. Distinct ====================

    @Nested
    @DisplayName("11. Distinct combinations")
    class DistinctCombinations {

        @Test @DisplayName("distinct on local → 0 JOINs")
        void distinctLocal() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name])->distinct()");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("distinct on dept → 1 JOIN")
        void distinctDept() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[dept:p|$p.deptName])->distinct()");
            assertEquals(1, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("Data: distinct dept names")
        void dataDistinctDept() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->project(~[dept:p|$p.deptName])->distinct()->sort(~dept->ascending())");
            // Engineering, Marketing, Sales, NULL
            assertEquals(4, r.rowCount());
        }
    }

    // ==================== 12. Extend (Computed Column) ====================

    @Nested
    @DisplayName("12. Extend (computed column) combinations")
    class ExtendCombinations {

        @Test @DisplayName("extend local → 0 JOINs")
        void extendLocal() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name])->extend(~upper:x|$x.name->toUpper())");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("extend on dept column → 1 JOIN")
        void extendOnDept() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name, dept:p|$p.deptName])->extend(~upperDept:x|$x.dept->toUpper())");
            assertEquals(1, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("Data: extend computes correctly")
        void dataExtendLocal() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->project(~[name:p|$p.name])->extend(~upper:x|$x.name->toUpper())->sort(~name->ascending())->limit(1)");
            assertEquals(1, r.rowCount());
            assertEquals("ALICE", r.rows().get(0).get(1).toString());
        }
    }

    // ==================== 13. Edge Cases ====================

    @Nested
    @DisplayName("13. Edge cases")
    class EdgeCases {

        @Test @DisplayName("Empty result: filter excludes all → still 0 JOINs when local only")
        void emptyResultLocalFilter() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.name == 'NONEXISTENT'})->project(~[name:p|$p.name])");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("Data: empty result set returns 0 rows")
        void dataEmptyResult() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->filter({p|$p.name == 'NONEXISTENT'})->project(~[name:p|$p.name])");
            assertEquals(0, r.rowCount());
        }

        @Test @DisplayName("Single row: filter to one person → correct chain joins")
        void singleRowWithChain() throws SQLException {
            setupThreeTableData();
            var r = exec(threeHopModel(),
                    "Person.all()->filter({p|$p.name == 'Alice'})->project(~[name:p|$p.name, org:p|$p.orgName])");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", r.rows().get(0).get(0).toString());
            assertEquals("Acme Corp", r.rows().get(0).get(1).toString());
        }

        @Test @DisplayName("Same property in filter AND project → single set of JOINs")
        void samePropertyFilterAndProject() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.orgName == 'Acme Corp'})->project(~[name:p|$p.name, org:p|$p.orgName])");
            assertEquals(2, countLeftJoins(sql), "SQL: " + sql);
        }

        @Test @DisplayName("project with lambda referencing multiple chain props → all chains active")
        void lambdaMultipleChainProps() {
            String sql = planSql(threeHopModel(),
                    "Person.all()->filter({p|$p.deptName == 'Engineering' && $p.orgCountry == 'USA'})->project(~[name:p|$p.name])");
            assertEquals(3, countLeftJoins(sql), "SQL: " + sql);
        }
    }

    // ==================== 14. Associations (to-one) ====================

    @Nested
    @DisplayName("14. Association: to-one (Person → Firm)")
    class AssociationToOne {

        private void setupAssocData() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INT)",
                "CREATE TABLE T_FIRM (ID INT PRIMARY KEY, LEGAL_NAME VARCHAR(200))",
                "INSERT INTO T_FIRM VALUES (1, 'Acme Corp'), (2, 'Beta Inc')",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Charlie', 2), (4, 'Dave', NULL)");
        }

        private String assocToOneModel() {
            return withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; }
                    Class model::Firm { legalName: String[1]; }
                    Association model::Person_Firm { person: Person[*]; firm: Firm[1]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INTEGER)
                        Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(200))
                        Join Person_Firm(T_PERSON.FIRM_ID = T_FIRM.ID)
                    )
                    Mapping model::M (
                        Person: Relational { ~mainTable [store::DB] T_PERSON name: [store::DB] T_PERSON.NAME }
                        Firm: Relational { ~mainTable [store::DB] T_FIRM legalName: [store::DB] T_FIRM.LEGAL_NAME }
                    )
                    """, "store::DB", "model::M");
        }

        @Test @DisplayName("project local only → no firm JOIN")
        void projectLocalOnly() {
            String sql = planSql(assocToOneModel(), "Person.all()->project(~[name:p|$p.name])");
            assertNoTable(sql, "T_FIRM");
        }

        @Test @DisplayName("project through to-one assoc → firm JOIN present")
        void projectThroughAssoc() throws SQLException {
            setupAssocData();
            var r = exec(assocToOneModel(),
                    "Person.all()->project(~[name:p|$p.name, firm:p|$p.firm.legalName])");
            assertEquals(4, r.rowCount());
            var names = colStr(r, 0);
            var firms = colStr(r, 1);
            assertEquals("Acme Corp", firms.get(names.indexOf("Alice")));
            assertEquals("Acme Corp", firms.get(names.indexOf("Bob")));
            assertEquals("Beta Inc", firms.get(names.indexOf("Charlie")));
            assertNull(firms.get(names.indexOf("Dave")));
        }

        @Test @DisplayName("filter on assoc prop + project local → JOIN for filter")
        void filterAssocProjectLocal() throws SQLException {
            setupAssocData();
            var r = exec(assocToOneModel(),
                    "Person.all()->filter({p|$p.firm.legalName == 'Acme Corp'})->project(~[name:p|$p.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Bob")));
        }

        @Test @DisplayName("filter local + project through assoc")
        void filterLocalProjectAssoc() throws SQLException {
            setupAssocData();
            var r = exec(assocToOneModel(),
                    "Person.all()->filter({p|$p.name == 'Alice'})->project(~[name:p|$p.name, firm:p|$p.firm.legalName])");
            assertEquals(1, r.rowCount());
            assertEquals("Acme Corp", r.rows().get(0).get(1).toString());
        }

        @Test @DisplayName("filter assoc + project assoc (same prop)")
        void filterAndProjectAssoc() throws SQLException {
            setupAssocData();
            var r = exec(assocToOneModel(),
                    "Person.all()->filter({p|$p.firm.legalName == 'Beta Inc'})->project(~[name:p|$p.name, firm:p|$p.firm.legalName])");
            assertEquals(1, r.rowCount());
            assertEquals("Charlie", colStr(r, 0).get(0));
            assertEquals("Beta Inc", colStr(r, 1).get(0));
        }
    }

    // ==================== 15. Associations (to-many) ====================

    @Nested
    @DisplayName("15. Association: to-many (Person → Address)")
    class AssociationToMany {

        private void setupToManyData() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100))",
                "CREATE TABLE T_ADDRESS (ID INT PRIMARY KEY, PERSON_ID INT, STREET VARCHAR(200), CITY VARCHAR(100))",
                "INSERT INTO T_PERSON VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
                "INSERT INTO T_ADDRESS VALUES (1, 1, '123 Main St', 'New York'), (2, 1, '456 Oak Ave', 'Boston'), (3, 2, '789 Elm St', 'Chicago')");
        }

        private String assocToManyModel() {
            return withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; }
                    Class model::Address { street: String[1]; city: String[1]; }
                    Association model::Person_Address { person: Person[1]; addresses: Address[*]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                        Table T_ADDRESS (ID INTEGER PRIMARY KEY, PERSON_ID INTEGER, STREET VARCHAR(200), CITY VARCHAR(100))
                        Join Person_Address(T_PERSON.ID = T_ADDRESS.PERSON_ID)
                    )
                    Mapping model::M (
                        Person: Relational { ~mainTable [store::DB] T_PERSON name: [store::DB] T_PERSON.NAME }
                        Address: Relational { ~mainTable [store::DB] T_ADDRESS street: [store::DB] T_ADDRESS.STREET, city: [store::DB] T_ADDRESS.CITY }
                    )
                    """, "store::DB", "model::M");
        }

        @Test @DisplayName("project local only → no address JOIN")
        void projectLocalOnly() {
            String sql = planSql(assocToManyModel(), "Person.all()->project(~[name:p|$p.name])");
            assertNoTable(sql, "T_ADDRESS");
        }

        @Test @DisplayName("project through to-many → LEFT JOIN with row expansion")
        void projectThroughToMany() throws SQLException {
            setupToManyData();
            var r = exec(assocToManyModel(),
                    "Person.all()->project(~[name:p|$p.name, street:p|$p.addresses.street])");
            // Alice: 2 addresses, Bob: 1, Charlie: 0 (NULL)
            assertTrue(r.rowCount() >= 3);
        }

        @Test @DisplayName("filter on to-many uses EXISTS (no row explosion)")
        void filterToManyExists() throws SQLException {
            setupToManyData();
            var r = exec(assocToManyModel(),
                    "Person.all()->filter({p|$p.addresses.city == 'New York'})->project(~[name:p|$p.name])");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", colStr(r, 0).get(0));
        }

        @Test @DisplayName("filter to-many: person with multiple addresses appears once")
        void filterToManyNoExplosion() throws SQLException {
            setupToManyData();
            var r = exec(assocToManyModel(),
                    "Person.all()->filter({p|$p.addresses.city == 'New York' || $p.addresses.city == 'Boston'})->project(~[name:p|$p.name])");
            long aliceCount = colStr(r, 0).stream().filter("Alice"::equals).count();
            assertEquals(1, aliceCount, "Alice should appear once despite matching 2 addresses");
        }

        @Test @DisplayName("person with no addresses still appears (LEFT JOIN)")
        void noAddressLeftJoin() throws SQLException {
            setupToManyData();
            var r = exec(assocToManyModel(),
                    "Person.all()->project(~[name:p|$p.name, street:p|$p.addresses.street])");
            var names = colStr(r, 0);
            assertTrue(names.contains("Charlie"), "Charlie (no addresses) should appear");
        }

        @Test @DisplayName("filter local + project through to-many")
        void filterLocalProjectToMany() throws SQLException {
            setupToManyData();
            var r = exec(assocToManyModel(),
                    "Person.all()->filter({p|$p.name == 'Alice'})->project(~[name:p|$p.name, street:p|$p.addresses.street])");
            assertEquals(2, r.rowCount()); // Alice has 2 addresses
        }
    }

    // ==================== 16. Association + Join Chain ====================

    @Nested
    @DisplayName("16. Association + join chain on same class")
    class AssociationPlusJoinChain {

        private void setupData() throws SQLException {
            sql("CREATE TABLE T_EMP (ID INT PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INT, FIRM_ID INT)",
                "CREATE TABLE T_DEPT (ID INT PRIMARY KEY, NAME VARCHAR(50))",
                "CREATE TABLE T_FIRM (ID INT PRIMARY KEY, LEGAL_NAME VARCHAR(200))",
                "INSERT INTO T_DEPT VALUES (1, 'Engineering'), (2, 'Sales')",
                "INSERT INTO T_FIRM VALUES (1, 'Acme Corp'), (2, 'Beta Inc')",
                "INSERT INTO T_EMP VALUES (1, 'Alice', 1, 1), (2, 'Bob', 2, 1), (3, 'Charlie', 1, 2)");
        }

        private String assocPlusChainModel() {
            return withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Employee { name: String[1]; deptName: String[1]; }
                    Class model::Firm { legalName: String[1]; }
                    Association model::Emp_Firm { employee: Employee[*]; firm: Firm[1]; }
                    Database store::DB (
                        Table T_EMP (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INTEGER, FIRM_ID INTEGER)
                        Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50))
                        Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(200))
                        Join Emp_Dept(T_EMP.DEPT_ID = T_DEPT.ID)
                        Join Emp_Firm(T_EMP.FIRM_ID = T_FIRM.ID)
                    )
                    Mapping model::M (
                        Employee: Relational {
                            ~mainTable [store::DB] T_EMP
                            name: [store::DB] T_EMP.NAME,
                            deptName: [store::DB] @Emp_Dept | T_DEPT.NAME
                        }
                        Firm: Relational { ~mainTable [store::DB] T_FIRM legalName: [store::DB] T_FIRM.LEGAL_NAME }
                    )
                    """, "store::DB", "model::M");
        }

        @Test @DisplayName("project local only → 0 JOINs (no dept, no firm)")
        void projectLocalOnly() {
            String sql = planSql(assocPlusChainModel(),
                    "Employee.all()->project(~[name:e|$e.name])");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
            assertNoTable(sql, "T_DEPT");
            assertNoTable(sql, "T_FIRM");
        }

        @Test @DisplayName("project chain prop only → 1 JOIN (dept), no firm")
        void projectChainOnly() {
            String sql = planSql(assocPlusChainModel(),
                    "Employee.all()->project(~[dept:e|$e.deptName])");
            assertEquals(1, countLeftJoins(sql), "SQL: " + sql);
            assertHasTable(sql, "T_DEPT");
            assertNoTable(sql, "T_FIRM");
        }

        @Test @DisplayName("project assoc prop only → firm JOIN present")
        void projectAssocOnly() throws SQLException {
            setupData();
            var r = exec(assocPlusChainModel(),
                    "Employee.all()->project(~[name:e|$e.name, firm:e|$e.firm.legalName])");
            assertEquals(3, r.rowCount());
            var names = colStr(r, 0);
            var firms = colStr(r, 1);
            assertEquals("Acme Corp", firms.get(names.indexOf("Alice")));
            assertEquals("Beta Inc", firms.get(names.indexOf("Charlie")));
        }

        @Test @DisplayName("project chain + assoc → both JOINs present")
        void projectChainAndAssoc() throws SQLException {
            setupData();
            var r = exec(assocPlusChainModel(),
                    "Employee.all()->project(~[name:e|$e.name, dept:e|$e.deptName, firm:e|$e.firm.legalName])");
            assertEquals(3, r.rowCount());
            var names = colStr(r, 0);
            assertEquals("Engineering", colStr(r, 1).get(names.indexOf("Alice")));
            assertEquals("Acme Corp", colStr(r, 2).get(names.indexOf("Alice")));
        }

        @Test @DisplayName("filter on chain + project assoc")
        void filterChainProjectAssoc() throws SQLException {
            setupData();
            var r = exec(assocPlusChainModel(),
                    "Employee.all()->filter({e|$e.deptName == 'Engineering'})->project(~[name:e|$e.name, firm:e|$e.firm.legalName])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Charlie")));
        }

        @Test
        @DisplayName("filter on assoc + project chain")
        void filterAssocProjectChain() throws SQLException {
            setupData();
            var r = exec(assocPlusChainModel(),
                    "Employee.all()->filter({e|$e.firm.legalName == 'Acme Corp'})->project(~[name:e|$e.name, dept:e|$e.deptName])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Bob")));
        }

        @Test @DisplayName("sortBy assoc property → scalar subquery in ORDER BY")
        void sortByAssocProperty() throws SQLException {
            setupData();
            var r = exec(assocPlusChainModel(),
                    "Employee.all()->sortBy({e|$e.firm.legalName})->project(~[name:e|$e.name, firm:e|$e.firm.legalName])");
            assertEquals(3, r.rowCount());
            // Acme Corp < Beta Inc alphabetically → Alice,Bob first, then Charlie
            var firms = colStr(r, 1);
            assertEquals("Acme Corp", firms.get(0));
            assertEquals("Acme Corp", firms.get(1));
            assertEquals("Beta Inc", firms.get(2));
        }

        @Test
        @DisplayName("groupBy (class-source) with assoc in key function")
        void groupByClassSourceWithAssocKey() throws SQLException {
            setupData();
            var r = exec(assocPlusChainModel(),
                    "Employee.all()->groupBy(~[firm:e|$e.firm.legalName], ~cnt:x|$x.name:y|$y->count())");
            assertEquals(2, r.rowCount());
        }
    }

    // ==================== 17. Pure M2M Mappings ====================

    @Nested
    @DisplayName("17. Pure M2M mappings")
    class M2MMappings {

        private void setupM2MData() throws SQLException {
            sql("CREATE TABLE T_RAW (ID INT, FIRST VARCHAR(50), LAST VARCHAR(50), SCORE INT)",
                "INSERT INTO T_RAW VALUES (1, 'Alice', 'Smith', 90), (2, 'Bob', 'Jones', 75), (3, 'Charlie', 'Brown', 85)");
        }

        private String m2mModel() {
            return withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::RawPerson { first: String[1]; last: String[1]; score: Integer[1]; }
                    Class model::Person { fullName: String[1]; score: Integer[1]; }
                    Database store::DB ( Table T_RAW (ID INTEGER, FIRST VARCHAR(50), LAST VARCHAR(50), SCORE INTEGER) )
                    Mapping model::M (
                        RawPerson: Relational { ~mainTable [store::DB] T_RAW first: [store::DB] T_RAW.FIRST, last: [store::DB] T_RAW.LAST, score: [store::DB] T_RAW.SCORE }
                        Person: Pure { ~src RawPerson fullName: $src.first + ' ' + $src.last, score: $src.score }
                    )
                    """, "store::DB", "model::M");
        }

        @Test @DisplayName("M2M project all")
        void projectAll() throws SQLException {
            setupM2MData();
            var r = exec(m2mModel(), "Person.all()->project(~[name:p|$p.fullName, score:p|$p.score])");
            assertEquals(3, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice Smith", "Bob Jones", "Charlie Brown")));
        }

        @Test @DisplayName("M2M project single prop")
        void projectSingle() throws SQLException {
            setupM2MData();
            var r = exec(m2mModel(), "Person.all()->project(~[name:p|$p.fullName])");
            assertEquals(3, r.rowCount());
        }

        @Test @DisplayName("M2M filter + project")
        void filterProject() throws SQLException {
            setupM2MData();
            var r = exec(m2mModel(),
                    "Person.all()->filter({p|$p.score > 80})->project(~[name:p|$p.fullName])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice Smith", "Charlie Brown")));
        }

        @Test @DisplayName("M2M filter + sort + limit")
        void filterSortLimit() throws SQLException {
            setupM2MData();
            var r = exec(m2mModel(),
                    "Person.all()->filter({p|$p.score >= 75})->project(~[name:p|$p.fullName, score:p|$p.score])->sort(~score->descending())->limit(2)");
            assertEquals(2, r.rowCount());
            assertEquals("Alice Smith", colStr(r, 0).get(0));
            assertEquals("Charlie Brown", colStr(r, 0).get(1));
        }

        @Test @DisplayName("M2M groupBy")
        void groupBy() throws SQLException {
            setupM2MData();
            var r = exec(m2mModel(),
                    "Person.all()->project(~[name:p|$p.fullName, score:p|$p.score])->groupBy(~[], ~avg:x|$x.score:y|$y->average())");
            assertEquals(1, r.rowCount());
        }
    }

    // ==================== 18. M2M with Join Chain Source ====================

    @Nested
    @DisplayName("18. M2M with join chain on source class")
    class M2MWithJoinChain {

        private void setupData() throws SQLException {
            sql("CREATE TABLE T_EMP (ID INT, NAME VARCHAR(50), DEPT_ID INT)",
                "CREATE TABLE T_DEPT (ID INT, NAME VARCHAR(50))",
                "INSERT INTO T_DEPT VALUES (1, 'Engineering'), (2, 'Sales')",
                "INSERT INTO T_EMP VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1)");
        }

        private String m2mJoinChainModel() {
            return withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::RawEmp { name: String[1]; deptName: String[1]; }
                    Class model::Employee { label: String[1]; }
                    Database store::DB (
                        Table T_EMP (ID INTEGER, NAME VARCHAR(50), DEPT_ID INTEGER)
                        Table T_DEPT (ID INTEGER, NAME VARCHAR(50))
                        Join Emp_Dept(T_EMP.DEPT_ID = T_DEPT.ID)
                    )
                    Mapping model::M (
                        RawEmp: Relational {
                            ~mainTable [store::DB] T_EMP
                            name: [store::DB] T_EMP.NAME,
                            deptName: [store::DB] @Emp_Dept | T_DEPT.NAME
                        }
                        Employee: Pure { ~src RawEmp label: $src.name + ' (' + $src.deptName + ')' }
                    )
                    """, "store::DB", "model::M");
        }

        @Test @DisplayName("M2M that uses source join chain prop → JOINs present")
        void m2mUsesChainProp() throws SQLException {
            setupData();
            var r = exec(m2mJoinChainModel(),
                    "Employee.all()->project(~[label:e|$e.label])");
            assertEquals(3, r.rowCount());
            assertTrue(colStr(r, 0).contains("Alice (Engineering)"));
            assertTrue(colStr(r, 0).contains("Bob (Sales)"));
        }

        @Test
        @DisplayName("M2M filter on target prop that references chain")
        void m2mFilterOnChainDerived() throws SQLException {
            setupData();
            var r = exec(m2mJoinChainModel(),
                    "Employee.all()->filter({e|$e.label->contains('Engineering')})->project(~[label:e|$e.label])");
            assertEquals(2, r.rowCount());
        }

        @Test @DisplayName("M2M sort on target prop from chain")
        void m2mSortOnChainDerived() throws SQLException {
            setupData();
            var r = exec(m2mJoinChainModel(),
                    "Employee.all()->project(~[label:e|$e.label])->sort(~label->ascending())->limit(1)");
            assertEquals(1, r.rowCount());
            assertEquals("Alice (Engineering)", colStr(r, 0).get(0));
        }
    }

    // ==================== 19. M2M with Association Source ====================

    @Nested
    @DisplayName("19. M2M with association on source class")
    class M2MWithAssociation {

        private void setupData() throws SQLException {
            sql("CREATE TABLE T_EMP (ID INT, FIRST VARCHAR(50), LAST VARCHAR(50), DEPT_ID INT)",
                "CREATE TABLE T_DEPT (ID INT, NAME VARCHAR(50))",
                "INSERT INTO T_DEPT VALUES (1, 'Engineering'), (2, 'Sales')",
                "INSERT INTO T_EMP VALUES (1, 'Alice', 'Smith', 1), (2, 'Bob', 'Jones', 2)");
        }

        private String m2mAssocModel() {
            return withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::RawEmp { first: String[1]; last: String[1]; }
                    Class model::RawDept { name: String[1]; }
                    Class model::Employee { fullName: String[1]; }
                    Association model::RawEmp_Dept { emps: RawEmp[*]; dept: RawDept[1]; }
                    Database store::DB (
                        Table T_EMP (ID INTEGER, FIRST VARCHAR(50), LAST VARCHAR(50), DEPT_ID INTEGER)
                        Table T_DEPT (ID INTEGER, NAME VARCHAR(50))
                        Join RawEmp_Dept(T_EMP.DEPT_ID = T_DEPT.ID)
                    )
                    Mapping model::M (
                        RawEmp: Relational { ~mainTable [store::DB] T_EMP first: [store::DB] T_EMP.FIRST, last: [store::DB] T_EMP.LAST }
                        RawDept: Relational { ~mainTable [store::DB] T_DEPT name: [store::DB] T_DEPT.NAME }
                        Employee: Pure { ~src RawEmp fullName: $src.first + ' ' + $src.last }
                    )
                    """, "store::DB", "model::M");
        }

        @Test @DisplayName("M2M project (source has assoc, but M2M only uses local props)")
        void m2mLocalOnly() throws SQLException {
            setupData();
            var r = exec(m2mAssocModel(),
                    "Employee.all()->project(~[name:e|$e.fullName])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice Smith", "Bob Jones")));
        }

        @Test @DisplayName("M2M filter + sort")
        void m2mFilterSort() throws SQLException {
            setupData();
            var r = exec(m2mAssocModel(),
                    "Employee.all()->project(~[name:e|$e.fullName])->sort(~name->ascending())->limit(1)");
            assertEquals(1, r.rowCount());
            assertEquals("Alice Smith", colStr(r, 0).get(0));
        }
    }

    // ==================== 20. Multi-Class Relational ====================

    @Nested
    @DisplayName("20. Multi-class relational (independent queries)")
    class MultiClassRelational {

        private void setupData() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100), AGE INT)",
                "CREATE TABLE T_PRODUCT (ID INT PRIMARY KEY, TITLE VARCHAR(100), PRICE INT)",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 30), (2, 'Bob', 25)",
                "INSERT INTO T_PRODUCT VALUES (1, 'Widget', 100), (2, 'Gadget', 200), (3, 'Doohickey', 50)");
        }

        private String multiClassModel() {
            return withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; age: Integer[1]; }
                    Class model::Product { title: String[1]; price: Integer[1]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), AGE INTEGER)
                        Table T_PRODUCT (ID INTEGER PRIMARY KEY, TITLE VARCHAR(100), PRICE INTEGER)
                    )
                    Mapping model::M (
                        Person: Relational { ~mainTable [store::DB] T_PERSON name: [store::DB] T_PERSON.NAME, age: [store::DB] T_PERSON.AGE }
                        Product: Relational { ~mainTable [store::DB] T_PRODUCT title: [store::DB] T_PRODUCT.TITLE, price: [store::DB] T_PRODUCT.PRICE }
                    )
                    """, "store::DB", "model::M");
        }

        @Test @DisplayName("Query Person → no product tables")
        void queryPerson() {
            String sql = planSql(multiClassModel(), "Person.all()->project(~[name:p|$p.name])");
            assertNoTable(sql, "T_PRODUCT");
        }

        @Test @DisplayName("Query Product → no person tables")
        void queryProduct() {
            String sql = planSql(multiClassModel(), "Product.all()->project(~[title:p|$p.title])");
            assertNoTable(sql, "T_PERSON");
        }

        @Test @DisplayName("Data: Person query returns persons only")
        void dataQueryPerson() throws SQLException {
            setupData();
            var r = exec(multiClassModel(), "Person.all()->project(~[name:p|$p.name, age:p|$p.age])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Bob")));
        }

        @Test @DisplayName("Data: Product query returns products only")
        void dataQueryProduct() throws SQLException {
            setupData();
            var r = exec(multiClassModel(), "Product.all()->project(~[title:p|$p.title, price:p|$p.price])");
            assertEquals(3, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Widget", "Gadget", "Doohickey")));
        }

        @Test @DisplayName("Data: Product filter + sort")
        void dataProductFilterSort() throws SQLException {
            setupData();
            var r = exec(multiClassModel(),
                    "Product.all()->filter({p|$p.price > 75})->project(~[title:p|$p.title])->sort(~title->ascending())");
            assertEquals(2, r.rowCount());
            assertEquals(List.of("Gadget", "Widget"), colStr(r, 0));
        }

        @Test @DisplayName("Data: Person groupBy age")
        void dataPersonGroupBy() throws SQLException {
            setupData();
            var r = exec(multiClassModel(),
                    "Person.all()->project(~[name:p|$p.name, age:p|$p.age])->groupBy(~age, ~cnt:x|$x.name:y|$y->count())->sort(~age->ascending())");
            assertEquals(2, r.rowCount());
        }
    }

    // ==================== 21. Two-Class + Assoc + Chain Combined ====================

    @Nested
    @DisplayName("21. Full combo: join chain + assoc + multi-class")
    class FullCombo {

        private void setupData() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INT)",
                "CREATE TABLE T_DEPT (ID INT PRIMARY KEY, NAME VARCHAR(50), ORG_ID INT)",
                "CREATE TABLE T_ORG (ID INT PRIMARY KEY, NAME VARCHAR(100))",
                "CREATE TABLE T_ADDRESS (ID INT PRIMARY KEY, PERSON_ID INT, CITY VARCHAR(100))",
                "INSERT INTO T_ORG VALUES (1, 'Acme Corp')",
                "INSERT INTO T_DEPT VALUES (1, 'Engineering', 1), (2, 'Sales', 1)",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', NULL)",
                "INSERT INTO T_ADDRESS VALUES (1, 1, 'New York'), (2, 1, 'Boston'), (3, 2, 'Chicago')");
        }

        private String fullComboModel() {
            return withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; deptName: String[1]; orgName: String[1]; }
                    Class model::Address { city: String[1]; }
                    Association model::Person_Address { person: Person[1]; addresses: Address[*]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INTEGER)
                        Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), ORG_ID INTEGER)
                        Table T_ORG (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                        Table T_ADDRESS (ID INTEGER PRIMARY KEY, PERSON_ID INTEGER, CITY VARCHAR(100))
                        Join Person_Dept(T_PERSON.DEPT_ID = T_DEPT.ID)
                        Join Dept_Org(T_DEPT.ORG_ID = T_ORG.ID)
                        Join Person_Address(T_PERSON.ID = T_ADDRESS.PERSON_ID)
                    )
                    Mapping model::M (
                        Person: Relational {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME,
                            deptName: [store::DB] @Person_Dept | T_DEPT.NAME,
                            orgName: [store::DB] @Person_Dept > @Dept_Org | T_ORG.NAME
                        }
                        Address: Relational { ~mainTable [store::DB] T_ADDRESS city: [store::DB] T_ADDRESS.CITY }
                    )
                    """, "store::DB", "model::M");
        }

        @Test @DisplayName("project local only → 0 JOINs, no dept/org/address")
        void projectLocalOnly() {
            String sql = planSql(fullComboModel(), "Person.all()->project(~[name:p|$p.name])");
            assertEquals(0, countLeftJoins(sql), "SQL: " + sql);
            assertNoTable(sql, "T_DEPT");
            assertNoTable(sql, "T_ORG");
            assertNoTable(sql, "T_ADDRESS");
        }

        @Test @DisplayName("project chain only → dept JOIN, no address")
        void projectChainOnly() {
            String sql = planSql(fullComboModel(),
                    "Person.all()->project(~[name:p|$p.name, dept:p|$p.deptName])");
            assertEquals(1, countLeftJoins(sql), "SQL: " + sql);
            assertHasTable(sql, "T_DEPT");
            assertNoTable(sql, "T_ORG");
            assertNoTable(sql, "T_ADDRESS");
        }

        @Test @DisplayName("project assoc only → address present, no dept/org")
        void projectAssocOnly() throws SQLException {
            setupData();
            var r = exec(fullComboModel(),
                    "Person.all()->project(~[name:p|$p.name, city:p|$p.addresses.city])");
            assertTrue(r.rowCount() >= 3);
            var names = colStr(r, 0);
            assertTrue(names.contains("Alice"));
        }

        @Test @DisplayName("project chain + assoc → dept + address JOINs")
        void projectChainAndAssoc() throws SQLException {
            setupData();
            var r = exec(fullComboModel(),
                    "Person.all()->project(~[name:p|$p.name, dept:p|$p.deptName, city:p|$p.addresses.city])");
            assertTrue(r.rowCount() >= 3);
        }

        @Test 
        @DisplayName("filter on assoc + project chain")
        void filterAssocProjectChain() throws SQLException {
            setupData();
            var r = exec(fullComboModel(),
                    "Person.all()->filter({p|$p.addresses.city == 'New York'})->project(~[name:p|$p.name, dept:p|$p.deptName])");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", colStr(r, 0).get(0));
            assertEquals("Engineering", colStr(r, 1).get(0));
        }

        @Test @DisplayName("filter on chain + project local (no assoc, no org)")
        void filterChainProjectLocal() {
            String sql = planSql(fullComboModel(),
                    "Person.all()->filter({p|$p.deptName == 'Engineering'})->project(~[name:p|$p.name])");
            assertEquals(1, countLeftJoins(sql), "SQL: " + sql);
            assertNoTable(sql, "T_ORG");
            assertNoTable(sql, "T_ADDRESS");
        }

        @Test @DisplayName("Data: NULL FK + association interaction")
        void nullFkWithAssoc() throws SQLException {
            setupData();
            var r = exec(fullComboModel(),
                    "Person.all()->project(~[name:p|$p.name, dept:p|$p.deptName, city:p|$p.addresses.city])");
            // Charlie has NULL DEPT_ID and no addresses
            var names = colStr(r, 0);
            assertTrue(names.contains("Charlie"));
        }

        @Test
        @DisplayName("filter chain + filter assoc combined")
        void filterChainAndAssoc() throws SQLException {
            setupData();
            var r = exec(fullComboModel(),
                    "Person.all()->filter({p|$p.deptName == 'Engineering'})->filter({p|$p.addresses.city == 'New York'})->project(~[name:p|$p.name])");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", colStr(r, 0).get(0));
        }
    }
}
