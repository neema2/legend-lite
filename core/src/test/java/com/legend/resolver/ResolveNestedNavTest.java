package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.NotImplementedException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Nested-navigation pins (audit 12): the N2 recursive slot-target
 * machinery's wrong-rows invariants get local detectors.
 * <ul>
 *   <li>a 3-hop leaf resolves through the SUB-TARGET'S BINDING — a decoy
 *       physical column spelled like the property must not be read;</li>
 *   <li>a nested navigation to a TEMPORAL grandchild stays LOUD (an
 *       unfiltered join would leak every version);</li>
 *   <li>auto-map inlining applies only to property-path mapper bodies —
 *       {@code ->map(b|1)} stays at the loud wall.</li>
 * </ul>
 */
class ResolveNestedNavTest {

    private static final String MODEL = """
            Class n::A { aname: String[1]; }
            Class n::B { bname: String[1]; }
            Class n::C { cname: String[1]; }
            Class <<temporal.businesstemporal>> n::TC { tname: String[1]; }
            Class n::BT { btname: String[1]; }
            Database n::DB (
              Table TA (ID INTEGER PRIMARY KEY, aname VARCHAR(64), BID INTEGER)
              Table TB (ID INTEGER PRIMARY KEY, bname VARCHAR(64), CID INTEGER)
              Table TC (ID INTEGER PRIMARY KEY, C_NAME VARCHAR(64), cname VARCHAR(64))
              Table TTC (
                milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                ID INTEGER PRIMARY KEY, tname VARCHAR(64), from_z DATE, thru_z DATE)
              Table TBT (ID INTEGER PRIMARY KEY, btname VARCHAR(64), TCID INTEGER)
              Join AB (TA.BID = TB.ID)
              Join BC (TB.CID = TC.ID)
              Join BTTC (TBT.TCID = TTC.ID)
              Join ABT (TA.BID = TBT.ID)
            )
            Mapping n::M (
              *n::A : Relational { ~mainTable [n::DB] TA
                aname: TA.aname, b: [n::DB]@AB, bt: [n::DB]@ABT }
              *n::B : Relational { ~mainTable [n::DB] TB
                bname: TB.bname, c: [n::DB]@BC }
              *n::C : Relational { ~mainTable [n::DB] TC cname: TC.C_NAME }
              *n::BT : Relational { ~mainTable [n::DB] TBT
                btname: TBT.btname, tc: [n::DB]@BTTC }
            )
            Association n::AB_A { b: n::B[0..1]; backA: n::A[*]; }
            Association n::BC_A { c: n::C[0..1]; backB: n::B[*]; }
            Association n::ABT_A { bt: n::BT[0..1]; backA2: n::A[*]; }
            Association n::BTTC_A { tc: n::TC[0..1]; backBT: n::BT[*]; }
            Runtime n::RT { mappings: [n::M]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE TA (ID INTEGER, aname VARCHAR, BID INTEGER)");
            st.execute("INSERT INTO TA VALUES (1, 'A1', 10)");
            st.execute("CREATE TABLE TB (ID INTEGER, bname VARCHAR, CID INTEGER)");
            st.execute("INSERT INTO TB VALUES (10, 'B1', 20)");
            // cname column holds DECOY garbage; the mapped column is C_NAME
            st.execute("CREATE TABLE TC (ID INTEGER, C_NAME VARCHAR, cname VARCHAR)");
            st.execute("INSERT INTO TC VALUES (20, 'REAL-VALUE', 'DECOY-GARBAGE')");
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
                NameResolver.resolveQuery(SpecParser.parse(query)));
        List<TypedSpec> resolved = new StoreResolver(ctx, specs).resolve(body, null);
        SqlQuery plan = new Lowerer().lower(resolved);
        return new DuckDb().render(plan);
    }

    private List<String> exec(String sql) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) {
                        sb.append('|');
                    }
                    sb.append(rs.getString(i));
                }
                rows.add(sb.toString());
            }
        }
        rows.sort(String::compareTo);
        return rows;
    }

    @Test
    @DisplayName("3-hop leaf resolves through the sub-target's BINDING, not the property name")
    void threeHopLeafUsesBinding() throws SQLException {
        String sql = sqlOf("|n::A.all()"
                + "->project([x|$x.b.c.cname], ['v'])->from(n::M, n::RT)");
        assertEquals(List.of("REAL-VALUE"), exec(sql),
                "the decoy physical column 'cname' must not be read:\n" + sql);
        assertTrue(sql.contains("C_NAME"), sql);
    }

    @Test
    @DisplayName("nested navigation to a temporal grandchild stays LOUD")
    void nestedTemporalGrandchildLoud() {
        assertThrows(RuntimeException.class, () -> sqlOf("|n::A.all()"
                + "->project([x|$x.bt.tc.tname], ['v'])->from(n::M, n::RT)"),
                "an unfiltered join to TTC would leak every version");
    }

    @Test
    @DisplayName("auto-map inlining only for property-path mapper bodies")
    void autoMapOnlyForPaths() {
        // ->map(b|1)->sum() must NOT collapse to the constant (audit 12
        // F4) — it stays loud somewhere in the pipeline (any layer)
        assertThrows(RuntimeException.class, () -> sqlOf("|n::A.all()"
                + "->project([x|$x.backA2->map(b|1)->sum()], ['v'])"
                + "->from(n::M, n::RT)"));
    }
}
