// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.server.QueryService;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * THE LEAN UNION JOIN (USER ruling 2026-09-13, docs/UNION_OR_JOIN_REMOVAL_DESIGN §7):
 * a plain class navigating INTO a union-mapped target through several routes
 * that share ONE raw condition (the same source expression against a
 * same-named column on each member's own table) joins on ONE equality over a
 * coalesce of the members' suffixed keys — an equi-join the database hashes —
 * instead of the engine's k-way OR. The rows are the contract: outer
 * semantics (a firm with no employees appears ONCE with a null), members
 * matched in both sets, a member matched in one set only.
 */
class UnionTargetLeanJoinTest {
    private static final String MODEL = """
            ###Pure
            Class ul::Firm { name: String[1]; employees: ul::Person[*]; contractors: ul::Contractor[*]; }
            Class ul::Person { lastName: String[1]; }
            Class ul::Contractor { lastName: String[1]; }
            ###Relational
            Database ul::db
            (
                Table FIRM (ID INT PRIMARY KEY, NAME VARCHAR(20))
                Table P1 (ID INT PRIMARY KEY, LAST VARCHAR(20), FIRM_ID INT)
                Table P2 (ID INT PRIMARY KEY, LAST VARCHAR(20), FIRM_ID INT)
                Table C1 (ID INT PRIMARY KEY, LAST VARCHAR(20), FIRM_ID INT)
                Table C2 (ID INT PRIMARY KEY, LAST VARCHAR(20), OWNER_ID INT)
                Join f_p1(FIRM.ID = P1.FIRM_ID)
                Join f_p2(FIRM.ID = P2.FIRM_ID)
                Join f_c1(FIRM.ID = C1.FIRM_ID)
                Join f_c2(FIRM.ID = C2.OWNER_ID)
            )
            ###Mapping
            Mapping ul::m
            (
                ul::Firm: Relational
                {
                    ~mainTable [ul::db]FIRM
                    name: [ul::db]FIRM.NAME,
                    employees[p1]: [ul::db]@f_p1,
                    employees[p2]: [ul::db]@f_p2,
                    contractors[c1]: [ul::db]@f_c1,
                    contractors[c2]: [ul::db]@f_c2
                }
                *ul::Contractor: Operation
                {
                    meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(c1, c2)
                }
                ul::Contractor[c1]: Relational
                {
                    ~mainTable [ul::db]C1
                    lastName: [ul::db]C1.LAST
                }
                ul::Contractor[c2]: Relational
                {
                    ~mainTable [ul::db]C2
                    lastName: [ul::db]C2.LAST
                }
                *ul::Person: Operation
                {
                    meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(p1, p2)
                }
                ul::Person[p1]: Relational
                {
                    ~mainTable [ul::db]P1
                    lastName: [ul::db]P1.LAST
                }
                ul::Person[p2]: Relational
                {
                    ~mainTable [ul::db]P2
                    lastName: [ul::db]P2.LAST
                }
            )
            ###Connection
            RelationalDatabaseConnection ul::conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime ul::rt { mappings: [ ul::m ]; connections: [ ul::db: [ environment: ul::conn ] ]; }
            """;

    private static List<List<Object>> rows(Connection connection, String query) throws Exception {
        var result = new QueryService().execute(MODEL, query, "ul::rt", connection);
        List<List<Object>> out = new ArrayList<>();
        for (var row : result.rows()) {
            out.add(new ArrayList<>(row.values()));
        }
        out.sort((x, y) -> String.valueOf(x).compareTo(String.valueOf(y)));
        return out;
    }

    private static String sqlOf(String query) {
        var ctx = com.legend.Compiler.compileModel(MODEL);
        var specs = new com.legend.compiler.spec.SpecCompiler(ctx);
        var body = specs.typeQueryBody(com.legend.compiler.NameResolver.resolveQuery(
                com.legend.testing.Own.spec(query + "->from(ul::m, ul::rt)")));
        var resolved = new com.legend.resolver.StoreResolver(ctx, specs).resolve(body, null);
        return new com.legend.sql.dialect.DuckDb().render(new com.legend.lowering.Lowerer(com.legend.lowering.PlatformRegistrations.catalogTable()).lower(resolved));
    }

    /**
     * THE SHAPE (legacy routes as composition, 2026-09-14): the routed
     * union's arms each project their OWN join column under the route
     * shape's key name (Firm's employees: FIRM_ID on both members;
     * contractors: FIRM_ID on one, OWNER_ID on the other — one shape, one
     * key), so the join is a single equality on one name the database
     * hashes. No OR, no coalesce, no member ordinal, no set id anywhere in
     * the navigating class; the union publishes nothing.
     */
    @Test
    @DisplayName("one minted key per arm: a single equality, uniform or not")
    void oneKeyPerArm() {
        for (String prop : List.of("employees", "contractors")) {
            String sql = sqlOf("|ul::Firm.all()->project(~[firm: f|$f.name, x: f|$f." + prop + ".lastName])");
            assertEquals(1, sql.split(" ON ").length - 1, "one join: " + sql);
            String on = sql.substring(sql.indexOf(" ON ") + 4).split("\\n")[0];
            assertEquals(false, on.contains(" OR "), "no OR in: " + on);
            assertEquals(false, on.toLowerCase().contains("coalesce"), "no coalesce in: " + on);
            assertEquals(true, on.matches("t0\\.ID = t\\d+\\.__route0_0"), "one equality on the route key the arms project: " + on);
        }
    }

    @Test
    @DisplayName("routes sharing one condition into a union target: one equi-join, outer semantics kept")
    void leanUnionJoinKeepsRows() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            try (var stmt = connection.createStatement()) {
                stmt.execute("create table FIRM (ID int primary key, NAME varchar(20))");
                stmt.execute("create table P1 (ID int primary key, LAST varchar(20), FIRM_ID int)");
                stmt.execute("create table P2 (ID int primary key, LAST varchar(20), FIRM_ID int)");
                stmt.execute("insert into FIRM values (1, 'Both'), (2, 'OnlyP2'), (3, 'None')");
                stmt.execute("insert into P1 values (10, 'a1', 1)");
                stmt.execute("insert into P2 values (20, 'b1', 1), (21, 'b2', 2)");
                stmt.execute("create table C1 (ID int primary key, LAST varchar(20), FIRM_ID int)");
                stmt.execute("create table C2 (ID int primary key, LAST varchar(20), OWNER_ID int)");
                stmt.execute("insert into C1 values (30, 'k1', 1)");
                stmt.execute("insert into C2 values (40, 'm1', 1), (41, 'm2', 3)");
            }
            assertEquals(List.of(
                            List.of("Both", "a1"), List.of("Both", "b1"),
                            java.util.Arrays.asList("None", null),
                            List.of("OnlyP2", "b2")),
                    rows(connection, "|ul::Firm.all()->project(~[firm: f|$f.name, emp: f|$f.employees.lastName])"),
                    "both sets match through one equality; a firm with no employees appears once with a null");
            assertEquals(List.of(List.of("Both", 2L), List.of("OnlyP2", 1L)),
                    rows(connection, "|ul::Firm.all()->filter(f|$f.employees->isNotEmpty())"
                            + "->project(~[firm: f|$f.name, n: f|$f.employees->size()])"),
                    "membership counts per firm across both sets");
            // NON-UNIFORM routes (member 1 keyed on FIRM_ID, member 2 on OWNER_ID):
            // no shared raw condition, so the per-member OR stays — and stays right
            assertEquals(List.of(
                            List.of("Both", "k1"), List.of("Both", "m1"),
                            List.of("None", "m2"),
                            java.util.Arrays.asList("OnlyP2", null)),
                    rows(connection, "|ul::Firm.all()->project(~[firm: f|$f.name, c: f|$f.contractors.lastName])"),
                    "different key columns per member: each member matches on its own condition");
        }
    }
}
