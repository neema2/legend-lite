// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.Lowerer;
import com.legend.parser.SpecParser;
import com.legend.sql.SqlQuery;
import com.legend.sql.dialect.DuckDb;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * TWO-HOP to-many emptiness in FILTER position (corpus testIsNotEmpty:
 * {@code Firm.all()->filter(f|$f.employees.locations->isNotEmpty())}) —
 * engine: LEFT JOIN to a DISTINCT subselect on the join keys + IS NOT
 * NULL (buildExistsAsJoinWithNullCheck), never a subquery inside a SQL
 * list lambda.
 */
class ResolveDeepEmptinessProbeTest {

    private static final String MODEL = """
            Class e::Firm { legalName: String[1]; }
            Class e::Person { name: String[1]; age: Integer[1]; }
            Class e::Location { place: String[1]; }
            Association e::FP { firm: e::Firm[0..1]; employees: e::Person[*]; }
            Association e::PL { person: e::Person[0..1]; locations: e::Location[*]; }
            ###Relational
            Database e::DB (
              Table F (ID INTEGER PRIMARY KEY, LEGALNAME VARCHAR(200))
              Table P (ID INTEGER PRIMARY KEY, NAME VARCHAR(200), AGE INTEGER, FIRMID INTEGER)
              Table L (ID INTEGER PRIMARY KEY, PLACE VARCHAR(200), PERSONID INTEGER)
              Join FP (F.ID = P.FIRMID)
              Join PL (P.ID = L.PERSONID)
            )
            ###Mapping
            Mapping e::M (
              *e::Firm : Relational { ~mainTable [e::DB] F
                legalName: F.LEGALNAME,
                employees: [e::DB] @FP }
              *e::Person : Relational { ~mainTable [e::DB] P
                name: P.NAME,
                age: P.AGE,
                firm: [e::DB] @FP,
                locations: [e::DB] @PL }
              *e::Location : Relational { ~mainTable [e::DB] L
                place: L.PLACE,
                person: [e::DB] @PL }
            )
            ###Runtime
            Runtime e::RT { mappings: [e::M]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE F (ID INTEGER, LEGALNAME VARCHAR(200))");
            st.execute("CREATE TABLE P (ID INTEGER, NAME VARCHAR(200), AGE INTEGER, FIRMID INTEGER)");
            st.execute("CREATE TABLE L (ID INTEGER, PLACE VARCHAR(200), PERSONID INTEGER)");
            // Firm 1: employee with a location; Firm 2: employee without;
            // Firm 3: no employees
            st.execute("INSERT INTO F VALUES (1,'A'),(2,'B'),(3,'C')");
            st.execute("INSERT INTO P VALUES (10,'p1',30,1),(20,'p2',40,2)");
            st.execute("INSERT INTO L VALUES (100,'NY',10)");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static String sqlOf(String query) {
        var ctx = Compiler.compileModel(MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(
                NameResolver.resolveQuery(com.legend.testing.Own.spec(query)));
        List<TypedSpec> resolved = new StoreResolver(ctx, specs).resolve(body, null);
        SqlQuery plan = new Lowerer(com.legend.lowering.PlatformRegistrations.catalogTable()).lower(resolved);
        return new DuckDb().render(plan);
    }

    private List<String> exec(String sql) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                rows.add(rs.getString(1));
            }
        }
        return rows;
    }

    @Test
    @DisplayName("CLASS-rooted frame read over the deep-emptiness filter (corpus shape)")
    void classRootedFrameRead() throws SQLException {
        String query = "{|let result = execute("
                + "|e::Firm.all()->filter(f|$f.employees.locations->isNotEmpty()),"
                + " e::M, e::RT.runtimeValue, []);"
                + " $result.values.legalName->removeDuplicates()->sort();}";
        Object r;
        try {
            r = Compiler.execute(MODEL, query, "e::RT", conn);
        } catch (com.legend.error.DataError e) {
            // the seam: database failures arrive as DataError now
            System.out.println("[class-frame-read-FULL-ERR] " + e.getMessage());
            throw e;
        }
        System.out.println("[class-frame-read] " + r);
        org.junit.jupiter.api.Assertions.assertTrue(
                String.valueOf(r).contains("A"), String.valueOf(r));
    }

    @Test
    @DisplayName("groupBy after deep-emptiness filter (corpus testWithFilterGroupBy shape)")
    void groupByAfterEmptinessFilter() throws SQLException {
        String sql = sqlOf("e::Person.all()"
                + "->filter(p|$p.firm.employees->isNotEmpty())"
                + "->groupBy([k|$k.firm.legalName],"
                + " [agg(s|$s.age, t|$t->sum())], ['firm','ageSum'])"
                + "->from(e::M, e::RT)");
        System.out.println("[groupby-emptiness]\n" + sql);
        assertEquals(List.of("A|30", "B|40"), execTwo(sql + " ORDER BY 1"), sql);
    }

    private List<String> execTwo(String sql) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                rows.add(rs.getString(1) + "|" + rs.getString(2));
            }
        }
        return rows;
    }

    @Test
    @DisplayName("2-hop to-many isNotEmpty in filter = chained EXISTS, no list lambda")
    void deepIsNotEmptyInFilter() throws SQLException {
        String sql = sqlOf("e::Firm.all()"
                + "->filter(f|$f.employees.locations->isNotEmpty())"
                + "->project([f|$f.legalName], ['n'])->from(e::M, e::RT)");
        System.out.println("[deep-emptiness]\n" + sql);
        assertEquals(List.of("A"), exec(sql + " ORDER BY 1"), sql);
    }
}
