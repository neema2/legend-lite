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
 * The design's witnesses the corpus cannot supply (docs/LEGACY_ROUTES_AS_
 * COMPOSITION_2026_09_13.md §10.5), judged by ROWS on DuckDB:
 * <ul>
 *   <li>W1 — a MIXED union (one Relational member, one Pure member): the
 *       stack builds it from the members' sources;</li>
 *   <li>W2 — a route INTO a {@code Relation ~func} member: the route's
 *       rows are the function's rows, its key a column of them;</li>
 *   <li>W4 — pinned routes from two SUBCLASS sets of a union into a union
 *       of a class and its subclass: each pin resolves under the queried
 *       mapping;</li>
 *   <li>W5 — route keys over a two-filter ONE-table union: a routed
 *       navigator reads each filtered arm's own key.</li>
 * </ul>
 * W3 ({@code importDataFlow} over a union) is judged by the corpus row
 * {@code mapping::union::testPksWithImportDataFlow}.
 */
class StackDesignWitnessTest {

    private static final String UNION_OP =
            "meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_";

    private static final String MODEL = """
            Class w::Person { name: String[1]; }
            Class w::RawPerson { n: String[1]; }
            Class w::Owner { id: Integer[1]; things: w::Thing[*]; }
            Class w::Thing { id: Integer[1]; name: String[1]; }
            Class w::Address { id: Integer[1]; coordinate: w::Coordinate[0..1]; }
            Class w::Coordinate { x: Integer[1]; }
            Class w::SubCoordinate extends w::Coordinate { y: Integer[1]; }
            Class w::Firm { id: Integer[1]; employees: w::Emp[*]; }
            Class w::Emp { last: String[1]; }
            Class w::Vehicle { name: String[1]; mechanic: w::Mechanic[0..1]; }
            Class w::Mechanic { name: String[1]; }
            Class w::Car extends w::Vehicle { }
            Class w::Bicycle extends w::Vehicle { }
            Class w::Garage { id: Integer[1]; vehicles: w::Vehicle[*]; }
            function w::thingsFn(): meta::pure::metamodel::relation::Relation<Any>[1]
            {
              #>{w::DB.T2}#
            }
            ###Relational
            Database w::DB (
              Table R (NAME VARCHAR(64))
              Table R2 (N VARCHAR(64))
              Table O (ID INTEGER PRIMARY KEY)
              Table T1 (ID INTEGER PRIMARY KEY, NAME VARCHAR(64), FK INTEGER)
              Table T2 (ID INTEGER PRIMARY KEY, NAME VARCHAR(64), FK INTEGER)
              Table ADDR (ID INTEGER PRIMARY KEY, KIND VARCHAR(8), COORD INTEGER)
              Table C1 (ID INTEGER PRIMARY KEY, X INTEGER)
              Table C2 (ID INTEGER PRIMARY KEY, X INTEGER, Y INTEGER)
              Table F (ID INTEGER PRIMARY KEY)
              Table PT (ID INTEGER PRIMARY KEY, LAST VARCHAR(64), KIND VARCHAR(8), FIRM_ID INTEGER)
              Table G (ID INTEGER PRIMARY KEY)
              Table V (ID INTEGER PRIMARY KEY, NAME VARCHAR(64), CAR_MECH VARCHAR(64), BIKE_MECH VARCHAR(64), G_ID INTEGER)
              Join G_V (G.ID = V.G_ID)
              Join O_T1 (O.ID = T1.FK)
              Join O_T2 (O.ID = T2.FK)
              Join A_C1 (ADDR.COORD = C1.ID)
              Join A_C2 (ADDR.COORD = C2.ID)
              Join F_PT (F.ID = PT.FIRM_ID)
              Filter street (ADDR.KIND = 'S')
              Filter city (ADDR.KIND = 'C')
              Filter kindA (PT.KIND = 'A')
              Filter kindB (PT.KIND = 'B')
              Filter kindX (PT.KIND = 'X')
            )
            ###Mapping
            Mapping w::Mixed (
              *w::Person : Operation { %s(rel, m2m) }
              w::Person[rel] : Relational { ~mainTable [w::DB] R name: R.NAME }
              *w::RawPerson : Relational { ~mainTable [w::DB] R2 n: R2.N }
              w::Person[m2m] : Pure { ~src w::RawPerson name: $src.n }
            )
            Mapping w::IntoFunc (
              *w::Thing : Operation { %s(t1, t2) }
              w::Thing[t1] : Relational { ~mainTable [w::DB] T1 id: T1.ID, name: T1.NAME }
              w::Thing[t2] : Relation { ~func w::thingsFn():meta::pure::metamodel::relation::Relation<Any>[1] id: ID, name: NAME }
              *w::Owner : Relational { ~mainTable [w::DB] O id: O.ID,
                things[t1]: [w::DB]@O_T1,
                things[t2]: [w::DB]@O_T2 }
            )
            Mapping w::Subclass (
              *w::Address : Operation { %s(s, c) }
              w::Address[s] : Relational { ~filter [w::DB] street ~mainTable [w::DB] ADDR id: ADDR.ID, coordinate[c1]: [w::DB]@A_C1 }
              w::Address[c] : Relational { ~filter [w::DB] city ~mainTable [w::DB] ADDR id: ADDR.ID, coordinate[c2]: [w::DB]@A_C2 }
              *w::Coordinate : Operation { %s(c1, c2) }
              w::Coordinate[c1] : Relational { ~mainTable [w::DB] C1 x: C1.X }
              w::SubCoordinate[c2] : Relational { ~mainTable [w::DB] C2 x: C2.X, y: C2.Y }
            )
            Mapping w::OneTable (
              *w::Emp : Operation { %s(pa, pb) }
              w::Emp[pa] : Relational { ~filter [w::DB] kindA ~mainTable [w::DB] PT last: PT.LAST }
              w::Emp[pb] : Relational { ~filter [w::DB] kindB ~mainTable [w::DB] PT last: PT.LAST }
              *w::Firm : Relational { ~mainTable [w::DB] F id: F.ID,
                employees[pa]: [w::DB]@F_PT,
                employees[pb]: [w::DB]@F_PT }
            )
            Mapping w::OutsidePin (
              *w::Emp : Operation { %s(pa, pb) }
              w::Emp[pa] : Relational { ~filter [w::DB] kindA ~mainTable [w::DB] PT last: PT.LAST }
              w::Emp[pb] : Relational { ~filter [w::DB] kindB ~mainTable [w::DB] PT last: PT.LAST }
              w::Emp[px] : Relational { ~filter [w::DB] kindX ~mainTable [w::DB] PT last: PT.LAST }
              *w::Firm : Relational { ~mainTable [w::DB] F id: F.ID,
                employees[px]: [w::DB]@F_PT }
            )
            Mapping w::SameTable (
              *w::Vehicle : Operation { meta::pure::router::operations::inheritance_OperationSetImplementation_1__SetImplementation_MANY_() }
              w::Car[car] : Relational { ~mainTable [w::DB] V name: V.NAME, mechanic( name: V.CAR_MECH ) }
              w::Bicycle[bike] : Relational { ~mainTable [w::DB] V name: V.NAME, mechanic( name: V.BIKE_MECH ) }
              *w::Garage : Relational { ~mainTable [w::DB] G id: G.ID,
                vehicles[car]: [w::DB]@G_V,
                vehicles[bike]: [w::DB]@G_V }
            )
            ###Runtime
            Runtime w::RT { mappings: [w::Mixed]; }
            """.formatted(UNION_OP, UNION_OP, UNION_OP, UNION_OP, UNION_OP, UNION_OP);

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE R (NAME VARCHAR)");
            st.execute("INSERT INTO R VALUES ('Ann-rel')");
            st.execute("CREATE TABLE R2 (N VARCHAR)");
            st.execute("INSERT INTO R2 VALUES ('Bob-m2m')");
            st.execute("CREATE TABLE O (ID INTEGER)");
            st.execute("INSERT INTO O VALUES (1), (2)");
            st.execute("CREATE TABLE T1 (ID INTEGER, NAME VARCHAR, FK INTEGER)");
            st.execute("INSERT INTO T1 VALUES (10, 'T1-a', 1), (11, 'T1-b', 2)");
            st.execute("CREATE TABLE T2 (ID INTEGER, NAME VARCHAR, FK INTEGER)");
            st.execute("INSERT INTO T2 VALUES (20, 'T2-a', 1)");
            st.execute("CREATE TABLE ADDR (ID INTEGER, KIND VARCHAR, COORD INTEGER)");
            st.execute("INSERT INTO ADDR VALUES (1, 'S', 7), (2, 'C', 7), (3, 'S', 9)");
            st.execute("CREATE TABLE C1 (ID INTEGER, X INTEGER)");
            st.execute("INSERT INTO C1 VALUES (7, 100)");
            st.execute("CREATE TABLE C2 (ID INTEGER, X INTEGER, Y INTEGER)");
            st.execute("INSERT INTO C2 VALUES (7, 200, 1), (9, 900, 2)");
            st.execute("CREATE TABLE F (ID INTEGER)");
            st.execute("INSERT INTO F VALUES (1), (2)");
            st.execute("CREATE TABLE PT (ID INTEGER, LAST VARCHAR, KIND VARCHAR, FIRM_ID INTEGER)");
            st.execute("INSERT INTO PT VALUES (1, 'Ash', 'A', 1), (2, 'Bay', 'B', 1), (3, 'Cox', 'A', 2), (4, 'Dee', 'X', 2)");
            st.execute("CREATE TABLE G (ID INTEGER)");
            st.execute("INSERT INTO G VALUES (1), (2)");
            st.execute("CREATE TABLE V (ID INTEGER, NAME VARCHAR, CAR_MECH VARCHAR, BIKE_MECH VARCHAR, G_ID INTEGER)");
            st.execute("INSERT INTO V VALUES (1, 'v1', 'cm1', 'bm1', 1), (2, 'v2', 'cm2', 'bm2', 1), (3, 'v3', 'cm3', 'bm3', 2)");
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
    @DisplayName("W1: a mixed union (Relational + Pure member) is a stack of its members' sources")
    void mixedUnionRows() throws SQLException {
        assertEquals(List.of("Ann-rel", "Bob-m2m"),
                rows("|w::Person.all()->project([p|$p.name], ['name'])->from(w::Mixed, w::RT)"));
    }

    @Test
    @DisplayName("W2: a route into a Relation ~func member reads the function's rows")
    void routeIntoFunctionMember() throws SQLException {
        // owner 1 owns T1-a (t1) and T2-a (t2); owner 2 owns T1-b only
        assertEquals(List.of("1|T1-a", "1|T2-a", "2|T1-b"),
                rows("|w::Owner.all()->project([o|$o.id, o|$o.things.name], ['oid', 'thing'])"
                        + "->from(w::IntoFunc, w::RT)"));
    }

    @Test
    @DisplayName("W4: pinned routes from two subclass-level sets into a class/subclass union")
    void pinnedRoutesFromSubclassSets() throws SQLException {
        // street rows pin c1 (COORD 7 -> x 100; COORD 9 absent from C1),
        // city rows pin c2 (COORD 7 -> x 200)
        assertEquals(List.of("1|100", "2|200", "3|null"),
                rows("|w::Address.all()->project([a|$a.id, a|$a.coordinate.x], ['aid', 'x'])"
                        + "->from(w::Subclass, w::RT)"));
    }

    @Test
    @DisplayName("W6: a single-table hierarchy scans its table once — through the extent and through two routes")
    void singleTableHierarchyScansOnce() throws SQLException {
        // the inheritance operation's two arms sit on the bare table V: the
        // extent is V's rows once (never once per arm), the identically
        // mapped base property reads plainly
        assertEquals(List.of("v1", "v2", "v3"),
                rows("|w::Vehicle.all()->project([v|$v.name], ['name'])->from(w::SameTable, w::RT)"));
        // a navigation routed to both arms joins the one scan once per row
        assertEquals(List.of("1|v1", "1|v2", "2|v3"),
                rows("|w::Garage.all()->project([g|$g.id, g|$g.vehicles.name], ['gid', 'name'])"
                        + "->from(w::SameTable, w::RT)"));
    }

    @Test
    @DisplayName("W6b: a property the arms map differently binds nowhere on the base — a bare read is loud")
    void differentlyMappedPropertyIsLoudOnTheBase() {
        var e = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                () -> sqlOf("|w::Vehicle.all()->project([v|$v.mechanic.name], ['m'])->from(w::SameTable, w::RT)"));
        assertTrue(e.getMessage().contains("mechanic"), e.getMessage());
    }

    @Test
    @DisplayName("W7: a property's only pin to a set outside the target's union is that set, never the root")
    void singlePinOutsideTheUnionIsThatSet() throws SQLException {
        // R-target: ONE distinct pin resolves to the pinned set (px: kind X)
        // — never the root union (pa, pb) the old dead-route rule fell to.
        // firm 1 has no kind-X employee; firm 2 has Dee
        assertEquals(List.of("1|null", "2|Dee"),
                rows("|w::Firm.all()->project([f|$f.id, f|$f.employees.last], ['fid', 'last'])"
                        + "->from(w::OutsidePin, w::RT)"));
    }

    @Test
    @DisplayName("W5: route keys over a two-filter one-table union")
    void routedNavigatorOverFilteredOneTableUnion() throws SQLException {
        // firm 1: Ash (A), Bay (B); firm 2: Cox (A); Dee (kind X) is in no arm
        assertEquals(List.of("1|Ash", "1|Bay", "2|Cox"),
                rows("|w::Firm.all()->project([f|$f.id, f|$f.employees.last], ['fid', 'last'])"
                        + "->from(w::OneTable, w::RT)"));
    }
}
