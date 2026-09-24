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
 * Leg 3 / U4 (map §4.1): union-INTO-union chained navigation — engine
 * testUnionForSQLQueryMerging golden: every arm NULL-crosses its member
 * key ({@code fk_0/fk_1}) and joins pair MEMBER-SUFFIXED both sides
 * ({@code uB.fk_0 = ua.fk_0 OR uB.fk_1 = ua.fk_1}). The trap row pins the
 * pairing: a member-1 source must NEVER match a member-2 target through a
 * shared key value.
 */
class ResolveUnionChainTest {

    private static final String UNION_FQN =
            "meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_";

    private static final String MODEL = ("""
            Class u::A { pk: Integer[1]; b: u::B[1]; }
            Class u::B { pk: Integer[1]; c: u::C[1]; }
            Class u::C { pk: Integer[1]; }
            ###Relational
            Database u::DB (
              Table aT1 (pk INTEGER PRIMARY KEY, fk INTEGER)
              Table aT2 (pk INTEGER PRIMARY KEY, fk INTEGER)
              Table bT1 (pk INTEGER PRIMARY KEY, fk INTEGER)
              Table bT2 (pk INTEGER PRIMARY KEY, fk INTEGER)
              Table cT1 (pk INTEGER PRIMARY KEY, fk INTEGER)
              Table cT2 (pk INTEGER PRIMARY KEY, fk INTEGER)
              Join A1B1 (aT1.fk = bT1.fk)
              Join A2B2 (aT2.fk = bT2.fk)
              Join B1C1 (bT1.fk = cT1.fk)
              Join B2C2 (bT2.fk = cT2.fk)
            )
            ###Mapping
            Mapping u::M (
              *u::A : Operation { %s(a1, a2) }
              *u::B : Operation { %s(b1, b2) }
              *u::C : Operation { %s(c1, c2) }
              u::A[a1] : Relational { ~mainTable [u::DB] aT1
                pk: aT1.pk, b[b1]: [u::DB]@A1B1 }
              u::A[a2] : Relational { ~mainTable [u::DB] aT2
                pk: aT2.pk, b[b2]: [u::DB]@A2B2 }
              u::B[b1] : Relational { ~mainTable [u::DB] bT1
                pk: bT1.pk, c[c1]: [u::DB]@B1C1 }
              u::B[b2] : Relational { ~mainTable [u::DB] bT2
                pk: bT2.pk, c[c2]: [u::DB]@B2C2 }
              u::C[c1] : Relational { ~mainTable [u::DB] cT1 pk: cT1.pk }
              u::C[c2] : Relational { ~mainTable [u::DB] cT2 pk: cT2.pk }
            )
            ###Runtime
            Runtime u::RT { mappings: [u::M]; }
            """).formatted(UNION_FQN, UNION_FQN, UNION_FQN);

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            for (String t : List.of("aT1", "aT2", "bT1", "bT2", "cT1", "cT2")) {
                st.execute("CREATE TABLE " + t + " (pk INTEGER, fk INTEGER)");
            }
            // member-1 chain: a(1) -> b(11) -> c(111): MATCHES both filters
            st.execute("INSERT INTO aT1 VALUES (1, 100), (3, 300)");
            st.execute("INSERT INTO bT1 VALUES (11, 100)");
            st.execute("INSERT INTO cT1 VALUES (111, 100)");
            // member-2 chain: a(2) -> b(22): fails b.pk == 11
            st.execute("INSERT INTO aT2 VALUES (2, 200)");
            st.execute("INSERT INTO bT2 VALUES (22, 200),"
                    // TRAP: b.pk 11 in MEMBER 2 sharing a1-row-3's key 300 —
                    // route pairing (a1->b1 only) must NOT match it
                    + " (11, 300)");
            st.execute("INSERT INTO cT2 VALUES (222, 200), (111, 300)");
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
    @DisplayName("chained union filters: member-paired routes, trap row excluded")
    void chainedUnionFilters() throws SQLException {
        String sql = sqlOf("u::A.all()->filter(a|$a.b.pk == 11)"
                + "->filter(a|$a.b.c.pk == 111)"
                + "->project([a|$a.pk], ['a_pk'])->from(u::M, u::RT)");
        System.out.println("[u4-sql]\n" + sql);
        // ONLY member-1's a(1): a1(3) shares key 300 with the member-2
        // b(11)/c(111) rows — cross-member matching would wrongly add 3.
        assertEquals(List.of("1"), exec(sql + " ORDER BY 1"), sql);
    }
}
