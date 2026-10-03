// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Graph-over-union probe (Leg 5 / graphFetch union::propertyLevel): a
 * graph tree whose CHILD property is union-mapped — the child computation
 * must stitch by the parent key against the union's member-suffixed pk
 * projections, not a raw physical column (the corpus wall: "filter
 * predicate references column 'ID', unresolvable even after isolation").
 */
class ResolveGraphUnionProbeTest {

    private static final String UNION_FQN =
            "meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_";

    private static final String SPECIAL_UNION_FQN =
            "meta::pure::router::operations::special_union_OperationSetImplementation_1__SetImplementation_MANY_";

    private static final String MODEL = ("""
            Class g::Firm { legalName: String[1]; }
            Class g::Person { lastName: String[1]; }
            Class g::Address { name: String[1]; }
            Association g::FP { firm: g::Firm[0..1]; employees: g::Person[*]; }
            Association g::PA { resident: g::Person[0..1]; address: g::Address[0..1]; }
            ###Relational
            Database g::DB (
              Table F1 (ID INTEGER PRIMARY KEY, LEGAL VARCHAR(200))
              Table F2 (ID INTEGER PRIMARY KEY, LEGAL VARCHAR(200))
              Table P1 (ID INTEGER PRIMARY KEY, NAME VARCHAR(200), FID INTEGER, AID INTEGER)
              Table P2 (ID INTEGER PRIMARY KEY, NAME VARCHAR(200), FID INTEGER, AID INTEGER)
              Table A1 (ID INTEGER PRIMARY KEY, NAME VARCHAR(200))
              Table A2 (ID INTEGER PRIMARY KEY, NAME VARCHAR(200))
              Join F1_P1 (F1.ID = P1.FID)
              Join F1_P2 (F1.ID = P2.FID)
              Join F2_P1 (F2.ID = P1.FID)
              Join F2_P2 (F2.ID = P2.FID)
              Join P1_A1 (P1.AID = A1.ID)
              Join P1_A2 (P1.AID = A2.ID)
              Join P2_A1 (P2.AID = A1.ID)
              Join P2_A2 (P2.AID = A2.ID)
            )
            ###Mapping
            Mapping g::M (
              *g::Firm : Operation { %s(f1, f2) }
              *g::Person : Operation { %s(p1, p2) }
              *g::Address : Operation { %s(a1, a2) }
              g::Firm[f1] : Relational { ~mainTable [g::DB] F1
                legalName: F1.LEGAL,
                employees[p1]: [g::DB] @F1_P1,
                employees[p2]: [g::DB] @F1_P2 }
              g::Firm[f2] : Relational { ~mainTable [g::DB] F2
                legalName: F2.LEGAL,
                employees[p1]: [g::DB] @F2_P1,
                employees[p2]: [g::DB] @F2_P2 }
              g::Person[p1] : Relational { ~mainTable [g::DB] P1
                lastName: P1.NAME,
                address[a1]: [g::DB] @P1_A1,
                address[a2]: [g::DB] @P1_A2 }
              g::Person[p2] : Relational { ~mainTable [g::DB] P2
                lastName: P2.NAME,
                address[a1]: [g::DB] @P2_A1,
                address[a2]: [g::DB] @P2_A2 }
              g::Address[a1] : Relational { ~mainTable [g::DB] A1 name: A1.NAME }
              g::Address[a2] : Relational { ~mainTable [g::DB] A2 name: A2.NAME }
            )
            ###Connection
            RelationalDatabaseConnection g::DBDuckDB { store: g::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime g::RT { mappings: [g::M]; connections: [ g::DB: [ c0: g::DBDuckDB ] ]; }
            """).formatted(UNION_FQN, UNION_FQN, UNION_FQN);

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE F1 (ID INTEGER, LEGAL VARCHAR(200))");
            st.execute("CREATE TABLE F2 (ID INTEGER, LEGAL VARCHAR(200))");
            st.execute("CREATE TABLE P1 (ID INTEGER, NAME VARCHAR(200), FID INTEGER, AID INTEGER)");
            st.execute("CREATE TABLE P2 (ID INTEGER, NAME VARCHAR(200), FID INTEGER, AID INTEGER)");
            st.execute("CREATE TABLE A1 (ID INTEGER, NAME VARCHAR(200))");
            st.execute("CREATE TABLE A2 (ID INTEGER, NAME VARCHAR(200))");
            st.execute("INSERT INTO F1 VALUES (1, 'ACME')");
            st.execute("INSERT INTO F2 VALUES (2, 'Globex')");
            st.execute("INSERT INTO P1 VALUES (10, 'Ann', 1, 100)");
            st.execute("INSERT INTO P2 VALUES (20, 'Bob', 1, 200), (21, 'Cid', 2, 100)");
            st.execute("INSERT INTO A1 VALUES (100, 'New York')");
            st.execute("INSERT INTO A2 VALUES (200, 'Hoboken')");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static final String MODEL_ROOT_ONLY = ("""
            Class g::Firm { legalName: String[1]; }
            Class g::Person { lastName: String[1]; }
            Association g::FP { firm: g::Firm[0..1]; employees: g::Person[*]; }
            ###Relational
            Database g::DB (
              Table F1 (ID INTEGER PRIMARY KEY, LEGAL VARCHAR(200))
              Table F2 (ID INTEGER PRIMARY KEY, LEGAL VARCHAR(200))
              Table P1 (ID INTEGER PRIMARY KEY, NAME VARCHAR(200), FID INTEGER)
              Join F1_P1 (F1.ID = P1.FID)
              Join F2_P1 (F2.ID = P1.FID)
            )
            ###Mapping
            Mapping g::M2 (
              *g::Firm : Operation { %s(f1, f2) }
              g::Firm[f1] : Relational { ~mainTable [g::DB] F1
                legalName: F1.LEGAL,
                employees[p1]: [g::DB] @F1_P1 }
              g::Firm[f2] : Relational { ~mainTable [g::DB] F2
                legalName: F2.LEGAL,
                employees[p1]: [g::DB] @F2_P1 }
              g::Person[p1] : Relational { ~mainTable [g::DB] P1
                lastName: P1.NAME }
            )
            ###Connection
            RelationalDatabaseConnection g::DBDuckDB { store: g::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime g::RT2 { mappings: [g::M2]; connections: [ g::DB: [ c0: g::DBDuckDB ] ]; }
            """).formatted(UNION_FQN);

    private static final String MODEL_DIAGONAL = ("""
            Class g::Trade { tradeId: Integer[1]; }
            Class g::Product { productId: String[1]; productName: String[1]; }
            Association g::TP { trade: g::Trade[0..1]; product: g::Product[0..1]; }
            ###Relational
            Database g::DB3 (
              Table T1 (tradeId INTEGER PRIMARY KEY, productId VARCHAR(200))
              Table T2 (tradeId INTEGER PRIMARY KEY, productId VARCHAR(200))
              Table PR1 (productId VARCHAR(200) PRIMARY KEY, NAME VARCHAR(200))
              Table PR2 (productId VARCHAR(200) PRIMARY KEY, NAME VARCHAR(200))
              Join trade_product (PR1.productId = T1.productId)
              Join trade2_product2 (PR2.productId = T2.productId)
            )
            ###Mapping
            Mapping g::M3 (
              *g::Trade : Operation { %s(t1, t2) }
              *g::Product : Operation { %s(p1, p2) }
              // corpus SameStoreMapping uses special_union at BOTH roots
              g::Trade[t1] : Relational { ~mainTable [g::DB3] T1
                tradeId: T1.tradeId,
                product[p1]: [g::DB3] @trade_product }
              g::Trade[t2] : Relational { ~mainTable [g::DB3] T2
                tradeId: T2.tradeId,
                product[p2]: [g::DB3] @trade2_product2 }
              g::Product[p1] : Relational { ~mainTable [g::DB3] PR1
                productId: PR1.productId,
                productName: PR1.NAME }
              g::Product[p2] : Relational { ~mainTable [g::DB3] PR2
                productId: PR2.productId,
                productName: PR2.NAME }
            )
            ###Connection
            RelationalDatabaseConnection g::DB3DuckDB { store: g::DB3; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime g::RT3 { mappings: [g::M3]; connections: [ g::DB3: [ c0: g::DB3DuckDB ] ]; }
            """).formatted(UNION_FQN, UNION_FQN);

    private static final String MODEL_EMBEDDED = """
            Class g::EPerson { firstName: String[1]; firm: g::EFirm[1]; }
            Class g::EFirm { legalName: String[1]; employees: g::EPerson[*]; }
            ###Relational
            Database g::DB4 (
              Table PT (ID INTEGER PRIMARY KEY, FN VARCHAR(200), FL VARCHAR(200))
              Join firmEmployees (PT.FL = {target}.FL)
            )
            ###Mapping
            Mapping g::M4 (
              g::EPerson[p] : Relational { ~mainTable [g::DB4] PT
                firstName: PT.FN,
                firm ( legalName: PT.FL,
                       employees: [g::DB4] @firmEmployees ) }
            )
            ###Connection
            RelationalDatabaseConnection g::DB4DuckDB { store: g::DB4; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime g::RT4 { mappings: [g::M4]; connections: [ g::DB4: [ c0: g::DB4DuckDB ] ]; }
            """;

    private static final String MODEL_MATRIX = ("""
            Class g::MOrder { oid: Integer[1]; }
            Class g::MProduct { pname: String[1]; }
            Association g::OP { order: g::MOrder[0..1]; product: g::MProduct[*]; }
            ###Relational
            Database g::DB5 (
              Table OT1 (oid INTEGER PRIMARY KEY, pfk INTEGER)
              Table OT2 (oid INTEGER PRIMARY KEY, pfk INTEGER)
              Table PT1 (pid INTEGER PRIMARY KEY, pname VARCHAR(200))
              Table PT2 (pid INTEGER PRIMARY KEY, pname VARCHAR(200))
              Join O1_P1 (OT1.pfk = PT1.pid)
              Join O1_P2 (OT1.pfk = PT2.pid)
              Join O2_P1 (OT2.pfk = PT1.pid)
              Join O2_P2 (OT2.pfk = PT2.pid)
            )
            ###Mapping
            Mapping g::M5 (
              *g::MOrder : Operation { %s(o1, o2) }
              *g::MProduct : Operation { %s(p1, p2) }
              g::MOrder[o1] : Relational { ~mainTable [g::DB5] OT1
                oid: OT1.oid,
                product[p1]: [g::DB5] @O1_P1,
                product[p2]: [g::DB5] @O1_P2 }
              g::MOrder[o2] : Relational { ~mainTable [g::DB5] OT2
                oid: OT2.oid,
                product[p1]: [g::DB5] @O2_P1,
                product[p2]: [g::DB5] @O2_P2 }
              g::MProduct[p1] : Relational { ~mainTable [g::DB5] PT1
                pname: PT1.pname }
              g::MProduct[p2] : Relational { ~mainTable [g::DB5] PT2
                pname: PT2.pname }
            )
            ###Connection
            RelationalDatabaseConnection g::DB5DuckDB { store: g::DB5; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime g::RT5 { mappings: [g::M5]; connections: [ g::DB5: [ c0: g::DB5DuckDB ] ]; }
            """).formatted(UNION_FQN, UNION_FQN);

    private static final String MODEL_NAMED_SET = ("""
            Class g::NOrder { oid: Integer[1]; }
            Class g::NProduct { pname: String[1]; }
            Association g::NOP { order: g::NOrder[0..1]; product: g::NProduct[0..1]; }
            ###Relational
            Database g::DB7 (
              Table NOT1 (oid INTEGER PRIMARY KEY)
              Table NOT2 (oid INTEGER PRIMARY KEY, pfk INTEGER)
              Table NPT1 (pid INTEGER PRIMARY KEY, pname VARCHAR(200))
              Table NPT2 (pid INTEGER PRIMARY KEY, pname VARCHAR(200))
              Join N_OP (NOT2.pfk = NPT2.pid)
            )
            ###Mapping
            Mapping g::M7 (
              *g::NOrder : Operation { %s(no1, no2) }
              g::NOrder[no1] : Relational { ~mainTable [g::DB7] NOT1
                oid: NOT1.oid }
              g::NOrder[no2] : Relational { ~mainTable [g::DB7] NOT2
                oid: NOT2.oid,
                product[np2]: [g::DB7] @N_OP }
              g::NProduct[np1] : Relational { ~mainTable [g::DB7] NPT1
                pname: NPT1.pname }
              g::NProduct[np2] : Relational { ~mainTable [g::DB7] NPT2
                pname: NPT2.pname }
            )
            ###Connection
            RelationalDatabaseConnection g::DB7DuckDB { store: g::DB7; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime g::RT7 { mappings: [g::M7]; connections: [ g::DB7: [ c0: g::DB7DuckDB ] ]; }
            """).formatted(UNION_FQN);

    @Test
    @DisplayName("union member routes to a NAMED SET of a multi-set non-union class")
    void namedSetRoute() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE NOT1 (oid INTEGER)");
            st.execute("CREATE TABLE NOT2 (oid INTEGER, pfk INTEGER)");
            st.execute("CREATE TABLE NPT1 (pid INTEGER, pname VARCHAR(200))");
            st.execute("CREATE TABLE NPT2 (pid INTEGER, pname VARCHAR(200))");
            st.execute("INSERT INTO NOT1 VALUES (1)");
            st.execute("INSERT INTO NOT2 VALUES (2, 20)");
            // the same key exists in NPT1 — the navigate must read ONLY
            // the ROUTE-NAMED set's table (np2 -> NPT2)
            st.execute("INSERT INTO NPT1 VALUES (20, 'WRONG')");
            st.execute("INSERT INTO NPT2 VALUES (20, 'def2')");
        }
        ExecutionResult r = Compiler.execute(MODEL_NAMED_SET,
                "g::NOrder.all()->filter(o|$o.product.pname == 'def2')"
                        + "->project([o|$o.oid], ['id'])->from(g::M7, g::RT7)",
                "g::RT7", conn);
        System.out.println("[named-set] " + r);
        assertEquals(true,
                String.valueOf(r).contains("rows=[Row[values=[2]]]"),
                String.valueOf(r));
    }

    private static final String MODEL_TEMPORAL = """
            Class g::TOrder { oid: Integer[1]; }
            Class <<temporal.businesstemporal>> g::TProduct { pname: String[1]; }
            Association g::TOP { order: g::TOrder[0..1]; product: g::TProduct[*]; }
            ###Relational
            Database g::DB6 (
              Table TOT (oid INTEGER PRIMARY KEY, pfk INTEGER)
              Table TPT (
                milestoning(business(BUS_FROM=from_z, BUS_THRU=thru_z))
                pid INTEGER PRIMARY KEY, pname VARCHAR(200),
                from_z DATE, thru_z DATE)
              Join O_P (TOT.pfk = TPT.pid)
            )
            ###Mapping
            Mapping g::M6 (
              g::TOrder[o] : Relational { ~mainTable [g::DB6] TOT
                oid: TOT.oid,
                product: [g::DB6] @O_P }
              g::TProduct[p] : Relational { ~mainTable [g::DB6] TPT
                pname: TPT.pname }
            )
            ###Connection
            RelationalDatabaseConnection g::DB6DuckDB { store: g::DB6; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime g::RT6 { mappings: [g::M6]; connections: [ g::DB6: [ c0: g::DB6DuckDB ] ]; }
            """;

    @Test
    @DisplayName("MILESTONED graph child: tree-arg date filters version rows")
    void temporalGraphChild() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE TOT (oid INTEGER, pfk INTEGER)");
            st.execute("CREATE TABLE TPT (pid INTEGER, pname VARCHAR(200),"
                    + " from_z DATE, thru_z DATE)");
            st.execute("INSERT INTO TOT VALUES (1, 10)");
            // TWO VERSIONS of product 10: only 'Current' is in-window at
            // 2015-08-20 — an unfiltered child serializes BOTH (the
            // corpus multi-level test's 2->4 duplication mechanism)
            st.execute("INSERT INTO TPT VALUES"
                    + " (10, 'Old', DATE '2014-01-01', DATE '2015-01-01'),"
                    + " (10, 'Current', DATE '2015-01-01', DATE '9999-12-31')");
        }
        String query = "g::TOrder.all()"
                + "->graphFetch(#{g::TOrder{oid, product(%2015-08-20){pname}}}#)"
                + "->serialize(#{g::TOrder{oid, product(%2015-08-20){pname}}}#)"
                + "->from(g::M6, g::RT6)";
        ExecutionResult r = Compiler.execute(MODEL_TEMPORAL, query, "g::RT6", conn);
        String json = r instanceof ExecutionResult.Graph g ? g.json()
                : String.valueOf(r);
        System.out.println("[graph-temporal] " + json);
        assertEquals("[{\"oid\":1,\"product(2015-08-20)\":"
                + "[{\"pname\":\"Current\"}]}]", json);
    }

    @Test
    @DisplayName("FULL route matrix: a child reached via multiple routes serializes ONCE")
    void fullMatrixNoDuplicates() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE OT1 (oid INTEGER, pfk INTEGER)");
            st.execute("CREATE TABLE OT2 (oid INTEGER, pfk INTEGER)");
            st.execute("CREATE TABLE PT1 (pid INTEGER, pname VARCHAR(200))");
            st.execute("CREATE TABLE PT2 (pid INTEGER, pname VARCHAR(200))");
            // order 1 (member o1) pfk 10: product 10 exists in BOTH PT1 and
            // PT2 — reached via BOTH of o1's routes; the engine's per-node
            // identity dedup keeps... BOTH (different members = different
            // identities); a SINGLE-member double-reach cannot happen with
            // one route per (source, target) pair. Order 2 (o2) pfk 20:
            // only PT2 carries it — exactly one match.
            st.execute("INSERT INTO OT1 VALUES (1, 10)");
            st.execute("INSERT INTO OT2 VALUES (2, 20)");
            st.execute("INSERT INTO PT1 VALUES (10, 'A1')");
            st.execute("INSERT INTO PT2 VALUES (10, 'A2'), (20, 'B2')");
        }
        String query = "g::MOrder.all()"
                + "->graphFetch(#{g::MOrder{oid, product{pname}}}#)"
                + "->serialize(#{g::MOrder{oid, product{pname}}}#)"
                + "->from(g::M5, g::RT5)";
        ExecutionResult r = Compiler.execute(MODEL_MATRIX, query, "g::RT5", conn);
        String json = r instanceof ExecutionResult.Graph g ? g.json()
                : String.valueOf(r);
        System.out.println("[graph-matrix] " + json);
        assertEquals("[{\"oid\":1,\"product\":[{\"pname\":\"A1\"},{\"pname\":\"A2\"}]},"
                + "{\"oid\":2,\"product\":[{\"pname\":\"B2\"}]}]", json);
    }

    @Test
    @DisplayName("graph tree with an EMBEDDED child (corpus embedded family shape)")
    void graphEmbeddedChild() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE PT (ID INTEGER, FN VARCHAR(200), FL VARCHAR(200))");
            st.execute("INSERT INTO PT VALUES (1, 'Peter', 'Firm X')");
        }
        String query = "g::EPerson.all()"
                + "->graphFetch(#{g::EPerson{firstName, firm{legalName}}}#)"
                + "->serialize(#{g::EPerson{firstName, firm{legalName}}}#)"
                + "->from(g::M4, g::RT4)";
        ExecutionResult r = Compiler.execute(MODEL_EMBEDDED, query, "g::RT4", conn);
        String json = r instanceof ExecutionResult.Graph g ? g.json()
                : String.valueOf(r);
        System.out.println("[graph-embedded] " + json);
        assertEquals("[{\"firstName\":\"Peter\",\"firm\":{\"legalName\":\"Firm X\"}}]",
                json);
    }

    @Test
    @DisplayName("DIAGONAL union routes: member pairing yields NULL, never a cross-member match")
    void diagonalUnionPairing() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE T1 (tradeId INTEGER, productId VARCHAR(200))");
            st.execute("CREATE TABLE T2 (tradeId INTEGER, productId VARCHAR(200))");
            st.execute("CREATE TABLE PR1 (productId VARCHAR(200), NAME VARCHAR(200))");
            st.execute("CREATE TABLE PR2 (productId VARCHAR(200), NAME VARCHAR(200))");
            // TRAPS both directions (corpus SameStore data shape: same
            // column NAME on both sides, VARCHAR(200) keys, target-first join
            // spelling): trade 5 (member 1, product 40 ONLY in PR2) and
            // trade 3 (member 2, product 30 ONLY in PR1) must both be NULL
            st.execute("INSERT INTO T1 VALUES (1, '30'), (5, '40')");
            st.execute("INSERT INTO T2 VALUES (2, '31'), (3, '30')");
            st.execute("INSERT INTO PR1 VALUES ('30', 'Prod_1')");
            st.execute("INSERT INTO PR2 VALUES ('31', 'Prod_2'), ('40', 'Prod_3')");
        }
        String query = "g::Trade.all()"
                + "->graphFetch(#{g::Trade{tradeId, product{productId, productName}}}#)"
                + "->serialize(#{g::Trade{tradeId, product{productId, productName}}}#)"
                + "->from(g::M3, g::RT3)";
        ExecutionResult r = Compiler.execute(MODEL_DIAGONAL, query, "g::RT3", conn);
        String json = r instanceof ExecutionResult.Graph g ? g.json()
                : String.valueOf(r);
        System.out.println("[graph-diagonal] " + json);
        assertEquals("[{\"tradeId\":1,\"product\":{\"productId\":\"30\",\"productName\":\"Prod_1\"}},"
                + "{\"tradeId\":5,\"product\":null},"
                + "{\"tradeId\":2,\"product\":{\"productId\":\"31\",\"productName\":\"Prod_2\"}},"
                + "{\"tradeId\":3,\"product\":null}]", json);
    }

    @Test
    @DisplayName("graph tree from a UNION ROOT into a single-set child")
    void graphUnionRootOnly() throws SQLException {
        String query = "g::Firm.all()"
                + "->graphFetch(#{g::Firm{legalName, employees{lastName}}}#)"
                + "->serialize(#{g::Firm{legalName, employees{lastName}}}#)"
                + "->from(g::M2, g::RT2)";
        ExecutionResult r = Compiler.execute(MODEL_ROOT_ONLY, query, "g::RT2", conn);
        String json = r instanceof ExecutionResult.Graph g ? g.json()
                : String.valueOf(r);
        System.out.println("[graph-union-root] " + json);
        assertEquals(true, json.contains("ACME") && json.contains("Ann"), json);
    }

    @Test
    @DisplayName("graph tree with a union-mapped to-many child")
    void graphOverUnionChild() throws SQLException {
        String query = "g::Firm.all()"
                + "->graphFetch(#{g::Firm{legalName, employees{lastName,"
                + " address{name}}}}#)"
                + "->serialize(#{g::Firm{legalName, employees{lastName,"
                + " address{name}}}}#)"
                + "->from(g::M, g::RT)";
        ExecutionResult r = Compiler.execute(MODEL, query, "g::RT", conn);
        String json = r instanceof ExecutionResult.Graph g ? g.json()
                : String.valueOf(r);
        System.out.println("[graph-union] " + json);
        assertEquals(true,
                json.contains("\"legalName\":\"ACME\"")
                        && json.contains("Ann") && json.contains("Bob")
                        && json.contains("Cid")
                        && json.contains("New York") && json.contains("Hoboken"),
                json);
    }
}
