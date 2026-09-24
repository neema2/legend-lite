// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.Lowerer;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Legacy routes as composition (docs/LEGACY_ROUTES_AS_COMPOSITION_2026_09_13.md
 * §5), the primitive on its own: a hand-written mapping whose Firm navigates
 * to a two-set Person through ONE several-route legacyNavigate — each route
 * names the target set's own function, its rows and its join as written.
 * No union publishes anything; the navigator composed it. Person's root is
 * the PLAIN set p1, so the router's rule (R-target, docs/LEG2_STACK_AUDIT_
 * 2026_09_14.md) resolves the two distinct pins to that root: the p2 route
 * is dead — its keys ride the row as typed NULLs and no row of T2 ever
 * joins (the union of both sets through routes is {@code
 * StackDesignWitnessTest} W2/W5, where the target's root is an operation).
 * Rows are the verdict; the SQL is the keyed join.
 */
class RoutedNavigateTest {

    private static final String MODEL = """
            Class u::Person { name: String[1]; }
            Class u::Firm { id: Integer[1]; employees: u::Person[*]; }
            function u::p1(): u::Person[*]
            {
              #>{u::DB.T1}# -> map(r | ^u::Person(name = $r.NAME->meta::legend::lite::trustOne()))
            }
            function u::p2(): u::Person[*]
            {
              #>{u::DB.T2}# -> map(r | ^u::Person(name = $r.NAME->meta::legend::lite::trustOne()))
            }
            function u::firmSameShape(): u::Firm[*]
            {
              #>{u::DB.FIRM}#
                -> meta::legend::lite::legacyNavigate(~employees: u::Person.all(), [
                     meta::legend::lite::route(u::p1(), #>{u::DB.T1}#, {f, r | $f.ID == $r.FIRM_ID}),
                     meta::legend::lite::route(u::p2(), #>{u::DB.T2}#, {f, r | $f.ID == $r.FID})
                   ])
                -> map(r | ^u::Firm(id = $r.ID->meta::legend::lite::trustOne(), employees = $r.employees))
            }
            function u::firmTwoShapes(): u::Firm[*]
            {
              #>{u::DB.FIRM}#
                -> meta::legend::lite::legacyNavigate(~employees: u::Person.all(), [
                     meta::legend::lite::route(u::p1(), #>{u::DB.T1}#, {f, r | $f.ID == $r.FIRM_ID}),
                     meta::legend::lite::route(u::p2(), #>{u::DB.T2}#, {f, r | $f.ID == $r.FID && $r.KIND == 'x'})
                   ])
                -> map(r | ^u::Firm(id = $r.ID->meta::legend::lite::trustOne(), employees = $r.employees))
            }
            ###Relational
            Database u::DB (
              Table T1 (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INTEGER)
              Table T2 (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FID INTEGER, KIND VARCHAR(10))
              Table FIRM (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
            )
            ###Mapping
            Mapping u::M (
              *u::Person[p1]: Relational { u::p1 }
               u::Person[p2]: Relational { u::p2 }
              *u::Firm: Relational { u::firmSameShape }
            )
            Mapping u::M2 (
              *u::Person[p1]: Relational { u::p1 }
               u::Person[p2]: Relational { u::p2 }
              *u::Firm: Relational { u::firmTwoShapes }
            )
            ###Runtime
            Runtime u::RT { mappings: [u::M]; }
            Runtime u::RT2 { mappings: [u::M2]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE T1 (ID INTEGER, NAME VARCHAR, FIRM_ID INTEGER)");
            st.execute("CREATE TABLE T2 (ID INTEGER, NAME VARCHAR, FID INTEGER, KIND VARCHAR)");
            st.execute("CREATE TABLE FIRM (ID INTEGER, NAME VARCHAR)");
            st.execute("INSERT INTO FIRM VALUES (1, 'F1'), (2, 'F2')");
            st.execute("INSERT INTO T1 VALUES (10, 'A', 1), (11, 'B', 2)");
            st.execute("INSERT INTO T2 VALUES (20, 'C', 2, 'x'), (21, 'D', 1, 'y')");
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
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) {
                        b.append("|");
                    }
                    b.append(rs.getObject(i));
                }
                rows.add(b.toString());
            }
        }
        return rows;
    }

    @Test
    @DisplayName("two routes of one shape into a plain root: one keyed join to the root arm, the other pin dead")
    void sameShapeRoutesShareOneKey() throws SQLException {
        String sql = sqlOf("u::Firm.all()->project([f|$f.id, f|$f.employees.name], ['firm', 'person'])"
                + "->from(u::M, u::RT)");
        // D (T2, firm 1) and C (T2, firm 2) are behind the dead p2 pin
        assertEquals(List.of("1|A", "2|B"), exec(sql + " ORDER BY 1, 2"), sql);
        assertTrue(sql.contains("__route0_0"), sql);
        assertFalse(sql.contains(" OR "), sql);
        assertEquals(1, sql.split(" ON ").length - 1, "one join: " + sql);
        assertFalse(sql.contains("UNION ALL"), "one live arm, no union: " + sql);
    }

    @Test
    @DisplayName("the exists path resolves the same routed target (no class-level fallback)")
    void existsThroughRoutes() throws SQLException {
        String sql = sqlOf("u::Firm.all()->filter(f | $f.employees->exists(e | $e.name == 'A'))"
                + "->project([f|$f.id], ['firm'])->from(u::M, u::RT)");
        assertEquals(List.of("1"), exec(sql + " ORDER BY 1"), sql);
        // the dead pin's rows never satisfy the exists either
        String dead = sqlOf("u::Firm.all()->filter(f | $f.employees->exists(e | $e.name == 'D'))"
                + "->project([f|$f.id], ['firm'])->from(u::M, u::RT)");
        assertEquals(List.of(), exec(dead + " ORDER BY 1"), dead);
    }

    @Test
    @DisplayName("two routes of different shapes: their own keys, the dead pin's key a typed NULL")
    void differentShapesKeepTheirOwnKeys() throws SQLException {
        String sql = sqlOf("u::Firm.all()->project([f|$f.id, f|$f.employees.name], ['firm', 'person'])"
                + "->from(u::M2, u::RT2)");
        assertEquals(List.of("1|A", "2|B"), exec(sql + " ORDER BY 1, 2"), sql);
        assertTrue(sql.contains("__route0_0") && sql.contains("__route1_0"), sql);
        assertTrue(sql.contains(" OR "), sql);
    }
}
