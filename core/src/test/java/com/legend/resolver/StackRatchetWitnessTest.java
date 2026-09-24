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

/**
 * The shapes the leg 3 audit left without a judge (docs/LEG2_STACK_AUDIT_
 * 2026_09_14.md; the leg 3b landing's own accounting), each judged by ROWS
 * against the engine's rules:
 * <ul>
 *   <li>R-a — a ROOT route beside a member route into a plain class: several
 *       distinct pins resolve to the class's root; the non-root pin is
 *       dead;</li>
 *   <li>R-b — a union arm pinned to a SUBCLASS set of the target class: the
 *       pin names a set of another class; several distinct pins resolve to
 *       the target's root, so that pin is dead (never the extent);</li>
 *   <li>R-c — the user's own {@code ==} on two optional columns inside a
 *       derived property under a graph fetch keeps Pure's null-safe
 *       equality (the correlation stamp covers the join condition alone);</li>
 *   <li>R-d — a correlated aggregation over a stack parent whose route key
 *       is a MODELED (shared) column: one aggregate per parent row.</li>
 * </ul>
 */
class StackRatchetWitnessTest {

    private static final String UNION_OP =
            "meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_";

    private static final String MODEL = """
            Class w::Owner { id: Integer[1]; things: w::Thing[*]; }
            Class w::Thing { name: String[1]; }
            Class w::Address { id: Integer[1]; coordinate: w::Coordinate[0..1]; }
            Class w::Coordinate { x: Integer[1]; }
            Class w::SubCoordinate extends w::Coordinate { y: Integer[1]; }
            Class w::Firm { id: Integer[1]; employees: w::Emp[*];
              sameCityCount() { $this.employees->map(e | if($e.city == $e.altCity, |1, |0))->sum() } : Integer[1];
              hasSameCityEmployee() { $this.employees->filter(e | $e.city == $e.altCity)->isNotEmpty() } : Boolean[1];
              sameCityHeads() { $this.employees->filter(e | $e.city == $e.altCity)->count() } : Integer[1];
            }
            Class w::Emp { last: String[1]; city: String[0..1]; altCity: String[0..1]; }
            ###Relational
            Database w::DB (
              Table O (ID INTEGER PRIMARY KEY)
              Table T0 (ID INTEGER PRIMARY KEY, NAME VARCHAR(64), FK INTEGER)
              Table T1 (ID INTEGER PRIMARY KEY, NAME VARCHAR(64), FK INTEGER)
              Table ADDR (ID INTEGER PRIMARY KEY, KIND VARCHAR(8), COORD INTEGER)
              Table C1 (ID INTEGER PRIMARY KEY, X INTEGER)
              Table C2 (ID INTEGER PRIMARY KEY, X INTEGER, Y INTEGER)
              Table F1 (ID INTEGER PRIMARY KEY)
              Table F2 (ID INTEGER PRIMARY KEY)
              Table E (ID INTEGER PRIMARY KEY, LAST VARCHAR(64), CITY VARCHAR(64), ALT VARCHAR(64), FIRM_ID INTEGER)
              Join O_T0 (O.ID = T0.FK)
              Join O_T1 (O.ID = T1.FK)
              Join A_C1 (ADDR.COORD = C1.ID)
              Join A_C2 (ADDR.COORD = C2.ID)
              Join F1_E (F1.ID = E.FIRM_ID)
              Join F2_E (F2.ID = E.FIRM_ID)
              Filter street (ADDR.KIND = 'S')
              Filter city (ADDR.KIND = 'C')
            )
            ###Mapping
            Mapping w::RootBesideMember (
              *w::Thing[t0] : Relational { ~mainTable [w::DB] T0 name: T0.NAME }
              w::Thing[t1] : Relational { ~mainTable [w::DB] T1 name: T1.NAME }
              *w::Owner : Relational { ~mainTable [w::DB] O id: O.ID,
                things[t0]: [w::DB]@O_T0,
                things[t1]: [w::DB]@O_T1 }
            )
            Mapping w::SubclassPin (
              *w::Address : Operation { %s(s, c) }
              w::Address[s] : Relational { ~filter [w::DB] street ~mainTable [w::DB] ADDR id: ADDR.ID, coordinate[c1]: [w::DB]@A_C1 }
              w::Address[c] : Relational { ~filter [w::DB] city ~mainTable [w::DB] ADDR id: ADDR.ID, coordinate[c2]: [w::DB]@A_C2 }
              *w::Coordinate[c1] : Relational { ~mainTable [w::DB] C1 x: C1.X }
              *w::SubCoordinate[c2] : Relational { ~mainTable [w::DB] C2 x: C2.X, y: C2.Y }
            )
            Mapping w::Firms (
              *w::Firm : Operation { %s(f1, f2) }
              w::Firm[f1] : Relational { ~mainTable [w::DB] F1 id: F1.ID, employees: [w::DB]@F1_E }
              w::Firm[f2] : Relational { ~mainTable [w::DB] F2 id: F2.ID, employees: [w::DB]@F2_E }
              *w::Emp : Relational { ~mainTable [w::DB] E last: E.LAST, city: E.CITY, altCity: E.ALT }
            )
            ###Runtime
            Runtime w::RT { mappings: [w::Firms]; }
            """.formatted(UNION_OP, UNION_OP);

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE O (ID INTEGER)");
            st.execute("INSERT INTO O VALUES (1)");
            st.execute("CREATE TABLE T0 (ID INTEGER, NAME VARCHAR, FK INTEGER)");
            st.execute("INSERT INTO T0 VALUES (10, 'T0-a', 1)");
            st.execute("CREATE TABLE T1 (ID INTEGER, NAME VARCHAR, FK INTEGER)");
            st.execute("INSERT INTO T1 VALUES (20, 'T1-a', 1)");
            st.execute("CREATE TABLE ADDR (ID INTEGER, KIND VARCHAR, COORD INTEGER)");
            st.execute("INSERT INTO ADDR VALUES (1, 'S', 7), (2, 'C', 7), (3, 'S', 9)");
            st.execute("CREATE TABLE C1 (ID INTEGER, X INTEGER)");
            st.execute("INSERT INTO C1 VALUES (7, 100)");
            st.execute("CREATE TABLE C2 (ID INTEGER, X INTEGER, Y INTEGER)");
            st.execute("INSERT INTO C2 VALUES (7, 200, 1), (9, 900, 2)");
            st.execute("CREATE TABLE F1 (ID INTEGER)");
            st.execute("INSERT INTO F1 VALUES (1)");
            st.execute("CREATE TABLE F2 (ID INTEGER)");
            st.execute("INSERT INTO F2 VALUES (2)");
            st.execute("CREATE TABLE E (ID INTEGER, LAST VARCHAR, CITY VARCHAR, ALT VARCHAR, FIRM_ID INTEGER)");
            // firm 1: Ash (both cities NULL), Bay (NY/LA); firm 2: Cox (NY/LA)
            st.execute("INSERT INTO E VALUES (1, 'Ash', NULL, NULL, 1), (2, 'Bay', 'NY', 'LA', 1), (3, 'Cox', 'NY', 'LA', 2)");
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

    private List<String> rows(String query) throws SQLException {
        String sql = sqlOf(query);
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql + " ORDER BY ALL")) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) {
                        b.append('|');
                    }
                    b.append(rs.getObject(i));
                }
                out.add(b.toString());
            }
        }
        return out;
    }

    @Test
    @DisplayName("R-a: a root route beside a member route into a plain class — the root only")
    void rootRouteBesideMemberRoute() throws SQLException {
        // pins {t0, t1} (two distinct ids) resolve to Thing's root t0; the
        // t1 pin is dead — owner 1 sees T0-a, never T1-a
        assertEquals(List.of("1|T0-a"),
                rows("|w::Owner.all()->project([o|$o.id, o|$o.things.name], ['oid', 'thing'])"
                        + "->from(w::RootBesideMember, w::RT)"));
    }

    @Test
    @DisplayName("R-b: a union arm pinned to a SUBCLASS set of the target — that pin is dead")
    void pinToSubclassSetIsDead() throws SQLException {
        // pins {c1, c2} resolve to Coordinate's root c1; the city arm's c2
        // pin (a set of SubCoordinate) is dead — city rows get null, never
        // C1's x through the extent
        assertEquals(List.of("1|100", "2|null", "3|null"),
                rows("|w::Address.all()->project([a|$a.id, a|$a.coordinate.x], ['aid', 'x'])"
                        + "->from(w::SubclassPin, w::RT)"));
    }

    /** A graph fetch's serialized JSON (one row, one column) — the rows in
     * their engine shape. */
    private String json(String query) throws SQLException {
        String sql = sqlOf(query);
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return String.valueOf(rs.getObject(1));
        } catch (SQLException e) {
            throw new SQLException(e.getMessage() + "\n[sql] " + sql, e);
        }
    }

    @Test
    @DisplayName("R-c: the user's == on two optional columns stays null-safe inside a derived property under a graph fetch")
    void userEqualityStaysNullSafeInsideDerivedProperty() throws SQLException {
        // Pure: empty == empty is true — firm 1 counts Ash (both NULL) and
        // no one else; Bay and Cox (NY/LA) never match, so firm 2 counts 0.
        // The correlation (firm id = FIRM_ID) is the one equality lowered `=`.
        String tree = "#{w::Firm{id, sameCityCount()}}#";
        assertEquals("[{\"id\":1,\"sameCityCount()\":1},{\"id\":2,\"sameCityCount()\":0}]",
                json("|w::Firm.all()->graphFetch(" + tree + ")->serialize(" + tree + ")"
                        + "->from(w::Firms, w::RT)"));
    }

    @Test
    @DisplayName("R-e: a FILTERED navigation head under isNotEmpty in a derived leaf")
    void filteredHeadUnderEmptiness() throws SQLException {
        // firm 1 has Ash (both NULL: Pure's empty == empty holds); firm 2's
        // Cox (NY/LA) never matches — the filter rides the correlated relation
        String tree = "#{w::Firm{id, hasSameCityEmployee()}}#";
        assertEquals("[{\"id\":1,\"hasSameCityEmployee()\":true},{\"id\":2,\"hasSameCityEmployee()\":false}]",
                json("|w::Firm.all()->graphFetch(" + tree + ")->serialize(" + tree + ")"
                        + "->from(w::Firms, w::RT)"));
    }

    @Test
    @DisplayName("R-f: a FILTERED navigation head under count in a derived leaf")
    void filteredHeadUnderCount() throws SQLException {
        String tree = "#{w::Firm{id, sameCityHeads()}}#";
        assertEquals("[{\"id\":1,\"sameCityHeads()\":1},{\"id\":2,\"sameCityHeads()\":0}]",
                json("|w::Firm.all()->graphFetch(" + tree + ")->serialize(" + tree + ")"
                        + "->from(w::Firms, w::RT)"));
    }

    @Test
    @DisplayName("R-d: a correlated aggregation over a stack parent keyed on a modeled column")
    void aggregationOverStackParentWithModeledKey() throws SQLException {
        // one aggregate per firm row: the join-back is the OR of the key
        // pairs (a thread's key is NULL in the other arm), never their AND
        assertEquals(List.of("1|Ash,Bay", "2|Cox"),
                rows("|w::Firm.all()->project([f|$f.id, f|$f.employees.last->joinStrings(',')], ['fid', 'names'])"
                        + "->from(w::Firms, w::RT)"));
    }
}
