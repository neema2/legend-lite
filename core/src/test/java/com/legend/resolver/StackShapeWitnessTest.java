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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The stack's lift shapes, judged by ROWS against the engine's own rules
 * (docs/LEG2_STACK_AUDIT_2026_09_14.md, "Receipts"):
 * <ul>
 *   <li>R-key — a target key column the target set maps as a scalar
 *       property is MODELED: one name across the union's arms, matched by
 *       value (a pin is not honoured); any other column is per-set
 *       (a pin is honoured); with an arm that has no route, every column
 *       is per-set;</li>
 *   <li>R-target — several distinct pinned sets resolve to the target
 *       class's ROOT under the queried mapping (its members for an
 *       operation); ONE distinct pinned set resolves to that set, root
 *       or not.</li>
 * </ul>
 * Each witness names the engine rule that fixes its rows. Rows are the
 * verdict; nothing here pins SQL text.
 */
class StackShapeWitnessTest {

    private static final String UNION_OP =
            "meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_";

    private static final String MODEL = """
            Class w::Order { id: Integer[1]; product: w::Product[0..1]; thing: w::Thing[0..1]; }
            Class w::Product { id: Integer[1]; name: String[1]; }
            Class w::Thing { id: Integer[1]; name: String[1]; }
            ###Relational
            Database w::DB (
              Table O1 (ID INTEGER PRIMARY KEY, prodFk INTEGER)
              Table O2 (ID INTEGER PRIMARY KEY, prodFk INTEGER)
              Table O3 (ID INTEGER PRIMARY KEY, prodFk INTEGER)
              Table P1 (ID INTEGER PRIMARY KEY, name VARCHAR(64), key2 INTEGER)
              Table P2 (ID INTEGER PRIMARY KEY, name VARCHAR(64), key2 INTEGER)
              Table Q1 (ID INTEGER PRIMARY KEY, name VARCHAR(64))
              Table Q2 (ID INTEGER PRIMARY KEY, name VARCHAR(64))
              Join O1P1 (O1.prodFk = P1.ID)
              Join O2P2 (O2.prodFk = P2.ID)
              Join O3P2 (O3.prodFk = P2.ID)
              Join O1P1k (O1.prodFk = P1.key2)
              Join O2P2k (O2.prodFk = P2.key2)
              Join O1Q1 (O1.prodFk = Q1.ID)
              Join O2Q2 (O2.prodFk = Q2.ID)
              Join O1Q2 (O1.prodFk = Q2.ID)
            )
            ###Mapping
            Mapping w::Products (
              *w::Product : Operation { %s(p1, p2) }
              w::Product[p1] : Relational { ~mainTable [w::DB] P1 id: P1.ID, name: P1.name }
              w::Product[p2] : Relational { ~mainTable [w::DB] P2 id: P2.ID, name: P2.name }
              *w::Thing[q1] : Relational { ~mainTable [w::DB] Q1 id: Q1.ID, name: Q1.name }
              w::Thing[q2] : Relational { ~mainTable [w::DB] Q2 id: Q2.ID, name: Q2.name }
            )
            Mapping w::NonModeledKey (
              include w::Products
              *w::Order : Operation { %s(o1, o2) }
              w::Order[o1] : Relational { ~mainTable [w::DB] O1 id: O1.ID, product[p1]: [w::DB]@O1P1k }
              w::Order[o2] : Relational { ~mainTable [w::DB] O2 id: O2.ID, product[p2]: [w::DB]@O2P2k }
            )
            Mapping w::ModeledKeyThreeArms (
              include w::Products
              *w::Order : Operation { %s(o1, o2, o3) }
              w::Order[o1] : Relational { ~mainTable [w::DB] O1 id: O1.ID, product[p1]: [w::DB]@O1P1 }
              w::Order[o2] : Relational { ~mainTable [w::DB] O2 id: O2.ID, product[p2]: [w::DB]@O2P2 }
              w::Order[o3] : Relational { ~mainTable [w::DB] O3 id: O3.ID, product[p2]: [w::DB]@O3P2 }
            )
            Mapping w::ArmWithoutRoute (
              include w::Products
              *w::Order : Operation { %s(o1, o3) }
              w::Order[o1] : Relational { ~mainTable [w::DB] O1 id: O1.ID, product[p1]: [w::DB]@O1P1 }
              w::Order[o3] : Relational { ~mainTable [w::DB] O3 id: O3.ID }
            )
            Mapping w::PlainTwoPins (
              include w::Products
              *w::Order : Operation { %s(o1, o2) }
              w::Order[o1] : Relational { ~mainTable [w::DB] O1 id: O1.ID, thing[q1]: [w::DB]@O1Q1 }
              w::Order[o2] : Relational { ~mainTable [w::DB] O2 id: O2.ID, thing[q2]: [w::DB]@O2Q2 }
            )
            Mapping w::PlainOneNonRootPin (
              include w::Products
              *w::Order : Operation { %s(o1, o2) }
              w::Order[o1] : Relational { ~mainTable [w::DB] O1 id: O1.ID, thing[q2]: [w::DB]@O1Q2 }
              w::Order[o2] : Relational { ~mainTable [w::DB] O2 id: O2.ID, thing[q2]: [w::DB]@O2Q2 }
            )
            ###Runtime
            Runtime w::RT { mappings: [w::Products]; }
            """.formatted(UNION_OP, UNION_OP, UNION_OP, UNION_OP, UNION_OP, UNION_OP);

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE O1 (ID INTEGER, prodFk INTEGER)");
            st.execute("INSERT INTO O1 VALUES (10, 1), (11, 200)");
            st.execute("CREATE TABLE O2 (ID INTEGER, prodFk INTEGER)");
            st.execute("INSERT INTO O2 VALUES (20, 1), (21, 200)");
            st.execute("CREATE TABLE O3 (ID INTEGER, prodFk INTEGER)");
            st.execute("INSERT INTO O3 VALUES (30, 3)");
            st.execute("CREATE TABLE P1 (ID INTEGER, name VARCHAR, key2 INTEGER)");
            st.execute("INSERT INTO P1 VALUES (1, 'P1-a', 100), (2, 'P1-b', 200)");
            st.execute("CREATE TABLE P2 (ID INTEGER, name VARCHAR, key2 INTEGER)");
            st.execute("INSERT INTO P2 VALUES (1, 'P2-a', 200), (3, 'P2-c', 300)");
            st.execute("CREATE TABLE Q1 (ID INTEGER, name VARCHAR)");
            st.execute("INSERT INTO Q1 VALUES (1, 'Q1-a')");
            st.execute("CREATE TABLE Q2 (ID INTEGER, name VARCHAR)");
            st.execute("INSERT INTO Q2 VALUES (1, 'Q2-a')");
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

    private List<String> rows(String mapping, String prop) throws SQLException {
        String sql = sqlOf("|w::Order.all()->project([o|$o.id, o|$o." + prop + ".name], ['oid', 'name'])"
                + "->from(w::" + mapping + ", w::RT)");
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql + " ORDER BY 1, 2")) {
            while (rs.next()) {
                out.add(rs.getObject(1) + "|" + rs.getObject(2));
            }
        }
        return out;
    }

    @Test
    @DisplayName("W-a R-key: a NON-modeled target key is per set — every pin honoured")
    void nonModeledTargetKeyHonoursPins() throws SQLException {
        // key2 is not a property of Product: o1 matches P1 by key2 only,
        // o2 matches P2 by key2 only (P1.key2 = 200 and P2.key2 = 200 both
        // exist — a cross-match would double the rows)
        assertEquals(List.of("10|null", "11|P1-b", "20|null", "21|P2-a"),
                rows("NonModeledKey", "product"));
    }

    @Test
    @DisplayName("W-b R-key + R-target: a MODELED key over three arms, two pinned to one set")
    void modeledKeyCrossMatchesEveryTargetArm() throws SQLException {
        // id is a property of Product: the target sets {p1, p2} union and
        // `id` is one column — every order matches every product arm by value
        assertEquals(List.of("10|P1-a", "10|P2-a", "11|null", "20|P1-a", "20|P2-a", "21|null",
                "30|P2-c"), rows("ModeledKeyThreeArms", "product"));
    }

    @Test
    @DisplayName("W-c R-key: an arm without a route makes every key per set")
    void armWithoutRouteMakesEveryKeyPerSet() throws SQLException {
        // o3 has no product route: avoidModeledProperties — o1's modeled `id`
        // becomes per-set, so o1 matches P1 only (not P2's id 1)
        assertEquals(List.of("10|P1-a", "11|null", "30|null"),
                rows("ArmWithoutRoute", "product"));
    }

    @Test
    @DisplayName("W-d R-target: two pins into a PLAIN class resolve to its root only")
    void twoPinsIntoPlainClassKeepTheRootOnly() throws SQLException {
        // Thing's root is q1; q2's pin is not among the resolved sets → dead
        assertEquals(List.of("10|Q1-a", "11|null", "20|null", "21|null"),
                rows("PlainTwoPins", "thing"));
    }

    @Test
    @DisplayName("W-e R-target: ONE distinct pin to a non-root set is honoured")
    void oneNonRootPinIsHonoured() throws SQLException {
        // every arm pins q2: one distinct id resolves to q2, root or not
        assertEquals(List.of("10|Q2-a", "11|null", "20|Q2-a", "21|null"),
                rows("PlainOneNonRootPin", "thing"));
    }

    @Test
    @DisplayName("F11: the three shapes resolve and lower within budget")
    void shapesLowerWithinBudget() {
        for (String m : List.of("NonModeledKey", "ModeledKeyThreeArms", "PlainTwoPins")) {
            long t0 = System.nanoTime();
            sqlOf("|w::Order.all()->project([o|$o.id], ['oid'])->from(w::" + m + ", w::RT)");
            long ms = (System.nanoTime() - t0) / 1_000_000;
            System.out.println("[stack-timing] " + m + " " + ms + " ms");
            assertTrue(ms < 5_000, m + " took " + ms + " ms");
        }
    }
}
