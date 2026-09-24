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
 * B3.2 witness: a routed navigation whose routes differ in length — one
 * single-hop route and one CHAINED route through a mid table. The engine
 * roots the chained arm at the chain's first mid and projects that mid's
 * column as the arm's key (testUnionWithChainedJoinsAcross3SetsV4,
 * unionOfViews2 goldens); the navigator reads ONE link-key name for both
 * routes (same shape: {@code $s.fk = $t.?}), spelled by the navigating set
 * and the property — never an ordinal-named chain key, never an OR.
 */
class RoutedChainKeyTest {

    private static final String UNION_FQN =
            "meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_";

    private static final String MODEL = ("""
            Class u::A { pk: Integer[1]; b: u::B[1]; }
            Class u::B { pk: Integer[1]; }
            ###Relational
            Database u::DB (
              Table srcT (pk INTEGER PRIMARY KEY, fk INTEGER)
              Table bT1 (pk INTEGER PRIMARY KEY, fk INTEGER)
              Table bT2 (pk INTEGER PRIMARY KEY, fk INTEGER)
              Table mT (afk INTEGER, bfk INTEGER)
              Join A_B1 (srcT.fk = bT1.fk)
              Join A_M (srcT.fk = mT.afk)
              Join M_B2 (mT.bfk = bT2.fk)
            )
            ###Mapping
            Mapping u::M (
              *u::B : Operation { %s(b1, b2) }
              u::A[a0] : Relational { ~mainTable [u::DB] srcT
                pk: srcT.pk, b[b1]: [u::DB]@A_B1, b[b2]: [u::DB]@A_M > @M_B2 }
              u::B[b1] : Relational { ~mainTable [u::DB] bT1 pk: bT1.pk }
              u::B[b2] : Relational { ~mainTable [u::DB] bT2 pk: bT2.pk }
            )
            ###Runtime
            Runtime u::RT { mappings: [u::M]; }
            """).formatted(UNION_FQN);

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE srcT (pk INTEGER, fk INTEGER)");
            st.execute("CREATE TABLE bT1 (pk INTEGER, fk INTEGER)");
            st.execute("CREATE TABLE bT2 (pk INTEGER, fk INTEGER)");
            st.execute("CREATE TABLE mT (afk INTEGER, bfk INTEGER)");
            // a(1) -> b1(11) directly; a(2) -> mid -> b2(22)
            st.execute("INSERT INTO srcT VALUES (1, 100), (2, 200)");
            st.execute("INSERT INTO bT1 VALUES (11, 100)");
            st.execute("INSERT INTO mT VALUES (200, 900)");
            // TRAP: a b2 row sharing a(1)'s key 100 — b2 is reached
            // through the mid only, so a(1) must NOT match it
            st.execute("INSERT INTO bT2 VALUES (22, 900), (33, 100)");
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
    @DisplayName("chained route: the arm carries its mid, the navigator reads one link key")
    void chainedRouteReadsOneLinkKey() throws SQLException {
        String sql = sqlOf("u::A.all()->project([a|$a.pk, a|$a.b.pk], ['a_pk', 'b_pk'])"
                + "->from(u::M, u::RT)");
        assertEquals(List.of("1|11", "2|22"), exec(sql + " ORDER BY 1"), sql);
        // the key is the route shape's, projected by the routed union's arm
        assertTrue(sql.contains("__route0_0"), sql);
        // never an ordinal-named chain key, never a disjunction
        assertFalse(sql.matches("(?s).*__b_\\d.*"), sql);
        assertFalse(sql.contains(" OR "), sql);
        // one equality on the key; the mid rides the chained arm (the
        // thread after UNION ALL), not the navigator
        assertTrue(sql.matches("(?s).*ON t\\d+\\.fk = t\\d+\\.__route0_0.*"), sql);
        assertTrue(sql.indexOf("mT") > sql.indexOf("UNION ALL"), sql);
    }
}
