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
 * MULTI-HOP navigation through a UNION-LIFTED head (corpus
 * testUnionQueryWithPropagationOnNonTemporalRootWithTemporalProperty
 * shape, temporal stripped): {@code Order.all()->filter(o|
 * $o.product.classification.description == ...)} where Order is
 * union-mapped and product is a lifted navigate — the head's AssocSub
 * must carry the classification SUB-NAV (deep tails), not wall with
 * subNavs=[].
 */
class ResolveUnionMultiHopProbeTest {

    private static final String UNION_FQN =
            "meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_";

    private static final String MODEL = ("""
            Class m::Order { oid: Integer[1]; }
            Class m::Product { pname: String[1]; classification: m::Classification[0..1]; }
            Class m::Classification { description: String[1]; ctype: String[1]; }
            Association m::OP { product: m::Product[0..1]; orders: m::Order[*]; }
            ###Relational
            Database m::DB (
              Table O1 (ID INTEGER PRIMARY KEY, PRODFK INTEGER)
              Table O2 (ID INTEGER PRIMARY KEY, PRODFK INTEGER)
              Table PT (ID INTEGER PRIMARY KEY, PNAME VARCHAR(200), CTYPE VARCHAR(200))
              Table CT (CTYPE VARCHAR(200) PRIMARY KEY, DESCRIPTION VARCHAR(200))
              Join O1P (O1.PRODFK = PT.ID)
              Join O2P (O2.PRODFK = PT.ID)
              Join PC (PT.CTYPE = CT.CTYPE)
            )
            ###Mapping
            Mapping m::M (
              *m::Order : Operation { %s(s1, s2) }
              m::Order[s1] : Relational { ~mainTable [m::DB] O1
                oid: O1.ID,
                product[s1, prod]: [m::DB] @O1P }
              m::Order[s2] : Relational { ~mainTable [m::DB] O2
                oid: O2.ID,
                product[s2, prod]: [m::DB] @O2P }
              *m::Product[prod] : Relational { ~mainTable [m::DB] PT
                pname: PT.PNAME,
                classification: [m::DB] @PC }
              *m::Classification : Relational { ~mainTable [m::DB] CT
                description: CT.DESCRIPTION,
                ctype: CT.CTYPE }
            )
            ###Runtime
            Runtime m::RT { mappings: [m::M]; }
            """).formatted(UNION_FQN);

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE O1 (ID INTEGER, PRODFK INTEGER)");
            st.execute("CREATE TABLE O2 (ID INTEGER, PRODFK INTEGER)");
            st.execute("CREATE TABLE PT (ID INTEGER, PNAME VARCHAR, CTYPE VARCHAR)");
            st.execute("CREATE TABLE CT (CTYPE VARCHAR, DESCRIPTION VARCHAR)");
            st.execute("INSERT INTO O1 VALUES (1, 10)");
            st.execute("INSERT INTO O2 VALUES (2, 20)");
            st.execute("INSERT INTO PT VALUES (10, 'A', 'S'), (20, 'B', 'O')");
            st.execute("INSERT INTO CT VALUES ('S', 'STOCK'), ('O', 'OPTION')");
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
    @DisplayName("filter through union-lifted head's 2nd hop + project same chain")
    void multiHopThroughLiftedHead() throws SQLException {
        String sql = sqlOf("m::Order.all()"
                + "->filter(o|$o.product.classification.description == 'STOCK')"
                + "->project([o|$o.product.classification.ctype], ['t'])"
                + "->from(m::M, m::RT)");
        System.out.println("[union-multihop]\n" + sql);
        assertEquals(List.of("S"), exec(sql + " ORDER BY 1"), sql);
    }
}
