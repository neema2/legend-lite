package com.legend.integration;

import com.legend.exec.ExecutionResult;
import com.legend.server.QueryService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE MAPPING-SEAM WINDOW RULE (ClassSources.sealExtentWindows /
 * Lowerer.extentBoundary): a window inside a Relation {@code ~func} class
 * mapping computes over the mapped relation's OWN rows; a query filter on
 * the mapped class only drops rows afterwards. The engine treats the
 * mapped relation as a non-mergeable view (corpus
 * meta::relational::tests::mapping::relation::testMappingWithWindowColumn
 * expects John ranked 2nd in Group A although the 1st-ranked Peter is
 * filtered out by {@code age > 25}).
 *
 * <p>The OPPOSITE holds in plain relation composition — the engine folds an
 * ordinary predicate to WHERE under a window (PCT testExtendFilterOutNull:
 * the window sees the FILTERED rows) — and the second test pins that the
 * stamp is seam-local: the same pipeline written as a relation query keeps
 * the fold.
 */
@DisplayName("Relation ~func mapping: windows are extent boundaries")
class RelationMappingWindowSeamTest {

    private Connection conn;
    private final QueryService qs = new QueryService();

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE PERSON (ID INT, NAME VARCHAR(100), AGE INT, GRP VARCHAR(20), SAL INT)");
            // Group A: Peter (23, sal 10) ranks 1st, John (30, sal 20) 2nd;
            // Group C: Oliver (26, 30) 1st, Fabrice (45, 40) 2nd.
            s.execute("INSERT INTO PERSON VALUES (1, 'Peter', 23, 'A', 10), (2, 'John', 30, 'A', 20),"
                    + " (3, 'Oliver', 26, 'C', 30), (4, 'Fabrice', 45, 'C', 40)");
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    private static final String MODEL = """
            ###Pure
            Class test::Person
            {
              name: String[1];
              age: Integer[1];
              grp: String[1];
              rank: Integer[1];
            }

            function test::personWithRank(): meta::pure::metamodel::relation::Relation<Any>[1]
            {
              #>{store::DB.PERSON}#->extend(over(~GRP, ~SAL->ascending()), ~[RANK:{p,w,r|$p->rank($w, $r)}])
            }

            ###Relational
            Database store::DB
            (
              Table PERSON (ID INTEGER, NAME VARCHAR(100), AGE INTEGER, GRP VARCHAR(20), SAL INTEGER)
            )

            ###Mapping
            Mapping test::M
            (
              *test::Person[person]: Relation
              {
                ~func test::personWithRank():meta::pure::metamodel::relation::Relation<Any>[1]
                name: NAME,
                age: AGE,
                grp: GRP,
                rank: RANK
              }
            )

            ###Connection
            RelationalDatabaseConnection store::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }

            ###Runtime
            Runtime test::RT { mappings: [ test::M ]; connections: [ store::DB: [ environment: store::Conn ] ]; }
            """;

    private ExecutionResult exec(String query) throws SQLException {
        return qs.execute(MODEL, query, "test::RT", conn);
    }

    private static Map<String, Integer> rankByName(ExecutionResult r) {
        Map<String, Integer> out = new TreeMap<>();
        for (var row : r.rows()) {
            out.put(String.valueOf(row.get(0)), ((Number) row.get(1)).intValue());
        }
        return out;
    }

    @Test
    @DisplayName("class filter drops rows AFTER the ~func window ranked all of them")
    void classFilterDoesNotEnterTheExtentWindow() throws SQLException {
        var r = exec("test::Person.all()->filter(x|$x.age > 25)"
                + "->project(~[name:x|$x.name, rank:x|$x.rank])");
        // Peter (rank 1, age 23) is filtered out; John keeps rank 2 — the
        // window computed over the whole group before the class filter.
        assertEquals(Map.of("John", 2, "Oliver", 1, "Fabrice", 2), rankByName(r));
    }

    @Test
    @DisplayName("plain relation composition keeps the engine's WHERE-under-window fold")
    void relationCompositionFoldsTheFilterUnderTheWindow() throws SQLException {
        var r = exec("#>{store::DB.PERSON}#"
                + "->extend(over(~GRP, ~SAL->ascending()), ~[RANK:{p,w,r|$p->rank($w, $r)}])"
                + "->filter(x|$x.AGE > 25)->select(~[NAME, RANK])");
        // PCT testExtendFilterOutNull semantics: the window sees the
        // FILTERED rows, so John is alone in Group A and ranks 1st.
        assertEquals(Map.of("John", 1, "Oliver", 1, "Fabrice", 2), rankByName(r));
    }

    @Test
    @DisplayName("the boundary is a subquery around the window, filter outside")
    void lowersToAnIsolatedWindowSelect() {
        String sql = com.legend.Compiler.query(com.legend.Compiler.compileModel(MODEL), "test::Person.all()->filter(x|$x.age > 25)->project(~[name:x|$x.name, rank:x|$x.rank])").plan("test::RT").sql();
        int over = sql.indexOf("OVER (");
        int where = sql.indexOf("WHERE");
        assertTrue(over >= 0, sql);
        assertTrue(where > over, "the class filter must sit OUTSIDE the window select:\n" + sql);
    }
}
