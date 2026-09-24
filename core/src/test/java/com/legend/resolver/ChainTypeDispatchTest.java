package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.Lowerer;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Run-time type dispatch in CHAIN position (harness burn-down leg 1,
 * 2026-09-02): {@code chain->cast(@Sub)} over a partial-membership row
 * keeps the union row GATED &mdash; a non-conforming row RAISES exactly
 * where pure does (never a silent filter), and reads of the target's own
 * properties are the witness-gated subtype reads; {@code chain->match([...])}
 * and {@code ->map(o|$o.nav->match([...]))} are the per-row match on a
 * map parameter. Rows are the verdict; the database raises.
 */
@DisplayName("Run-time type dispatch in chain position: cast gate and chain match")
class ChainTypeDispatchTest {

    static final String MODEL = """
            Class rt::Vehicle { id: Integer[1]; name: String[1]; }
            Class rt::Car extends rt::Vehicle { engineType: String[1]; }
            Class rt::Bicycle extends rt::Vehicle { gears: Integer[1]; }
            Class rt::Owner { name: String[1]; vehicle: rt::Vehicle[1]; }
            ###Relational
            Database rt::DB (
              Table CAR (ID INTEGER PRIMARY KEY, NAME VARCHAR(64), ENGINE VARCHAR(20))
              Table BIKE (ID INTEGER PRIMARY KEY, NAME VARCHAR(64), GEARS INTEGER)
              Table OWNER (NAME VARCHAR(64) PRIMARY KEY, VID INTEGER)
              Join OC (OWNER.VID = CAR.ID)
              Join OB (OWNER.VID = BIKE.ID)
            )
            ###Mapping
            Mapping rt::M (
              *rt::Vehicle : Operation { meta::pure::router::operations::inheritance_OperationSetImplementation_1__SetImplementation_MANY_() }
              rt::Car[car] : Relational { ~mainTable [rt::DB] CAR id: CAR.ID, name: CAR.NAME, engineType: CAR.ENGINE }
              rt::Bicycle[bike] : Relational { ~mainTable [rt::DB] BIKE id: BIKE.ID, name: BIKE.NAME, gears: BIKE.GEARS }
              *rt::Owner : Relational { ~mainTable [rt::DB] OWNER name: OWNER.NAME, vehicle[car]: [rt::DB]@OC, vehicle[bike]: [rt::DB]@OB }
            )
            ###Runtime
            Runtime rt::RT { mappings: [rt::M]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE CAR(ID INTEGER, NAME VARCHAR, ENGINE VARCHAR)");
            st.execute("INSERT INTO CAR VALUES (1,'Sedan','V8'),(3,'Hatch','electric')");
            st.execute("CREATE TABLE BIKE(ID INTEGER, NAME VARCHAR, GEARS INTEGER)");
            st.execute("INSERT INTO BIKE VALUES (2,'Roadster',21)");
            st.execute("CREATE TABLE OWNER(NAME VARCHAR, VID INTEGER)");
            st.execute("INSERT INTO OWNER VALUES ('ann',1),('bob',2)");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static String sqlOf(String chain) {
        var ctx = Compiler.compileModel(MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(NameResolver.resolveQuery(
                com.legend.testing.Own.spec("|" + chain + "->from(rt::M, rt::RT)")));
        return new DuckDb().render(new Lowerer(com.legend.lowering.PlatformRegistrations.catalogTable()).lower(
                new StoreResolver(ctx, specs).resolve(body, null)));
    }

    private static List<String> rows(String chain) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sqlOf(chain))) {
            while (rs.next()) {
                out.add(rs.getString(1));
            }
        }
        return out;
    }

    @Test
    @DisplayName("cast(@Sub) in chain position: the narrowed rows read the subtype's own property")
    void chainCastReadsThroughTheGate() throws SQLException {
        assertEquals(List.of("V8"),
                rows("rt::Vehicle.all()->filter(v|$v.id == 1)->cast(@rt::Car).engineType"));
        assertEquals(List.of("1"),
                rows("rt::Vehicle.all()->filter(v|$v.id == 1)->cast(@rt::Car)->size()"));
    }

    @Test
    @DisplayName("cast(@Sub) over a row of another type RAISES like pure — never a silent filter")
    void chainCastRaisesOnANonConformingRow() {
        for (String chain : List.of(
                "rt::Vehicle.all()->cast(@rt::Car).engineType",
                "rt::Vehicle.all()->cast(@rt::Car)->size()")) {
            SQLException ex = assertThrows(SQLException.class, () -> rows(chain), chain);
            assertTrue(ex.getMessage().contains("Cast exception"), ex.getMessage());
        }
    }

    @Test
    @DisplayName("match([...]) in chain position dispatches per row")
    void chainMatch() throws SQLException {
        assertEquals(List.of("V8", "electric", "bike"),
                rows("rt::Vehicle.all()->match([c:rt::Car[1]|$c.engineType, b:rt::Bicycle[1]|'bike'])"));
    }

    @Test
    @DisplayName("match with ROW arms is the union of one filtered, cast branch per arm")
    void chainMatchWithRowArms() throws SQLException {
        assertEquals(List.of("Hatch", "Roadster", "Sedan"),
                rows("rt::Vehicle.all()->match([c:rt::Car[1]|$c, b:rt::Bicycle[1]|$b]).name")
                        .stream().sorted().toList());
    }

    @Test
    @DisplayName("dispatch over a navigation routed to TWO sets: chain match, map-body match, chain cast")
    void dispatchOverATwoRouteNavigation() throws SQLException {
        assertEquals(List.of("V8", "bike"),
                rows("rt::Owner.all().vehicle->match([c:rt::Car[1]|$c.engineType, b:rt::Bicycle[1]|'bike'])"));
        assertEquals(List.of("V8", "bike"),
                rows("rt::Owner.all()->map(o|$o.vehicle->match([c:rt::Car[1]|$c.engineType, b:rt::Bicycle[1]|'bike']))"));
        assertEquals(List.of("V8"),
                rows("rt::Owner.all()->filter(o|$o.name == 'ann').vehicle->cast(@rt::Car).engineType"));
        SQLException ex = assertThrows(SQLException.class,
                () -> rows("rt::Owner.all().vehicle->cast(@rt::Car).engineType"),
                "bob's vehicle is a bicycle");
        assertTrue(ex.getMessage().contains("Cast exception"), ex.getMessage());
    }
}
