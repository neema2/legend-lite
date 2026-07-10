package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.Lowerer;
import com.legend.parser.SpecParser;
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
 * M-H3b — association navigation to-one + embedded per-leaf dispatch
 * (docs/PHASE_H2_H3_RESOLVER_PLAN.md fixtures 11/12/14/16/18): navigation
 * joins are demand-gated LEFT joins against the target class's own
 * pipeline, deduped by head across the WHOLE op chain; embedded reads are
 * parent-alias columns, never joins; un-navigated associations cost
 * nothing.
 */
class ResolveNavigationTest {

    private static final String MODEL = """
            Class m::Person { name: String[1]; addr: m::Addr[1]; }
            Class m::Addr { city: String[1]; }
            Class m::Firm { legal: String[1]; }
            Association m::Emp { employer: m::Firm[1]; staff: m::Person[*]; }
            Association m::Mgr { boss: m::Person[1]; reports: m::Person[*]; }
            Database s::DB (
              Table P (NAME VARCHAR(50), CITY VARCHAR(50), FID INTEGER, BOSS INTEGER, ID INTEGER)
              Table F (ID INTEGER, LEGAL VARCHAR(50))
              Join PF (P.FID = F.ID)
              Join PB (P.BOSS = {target}.ID)
            )
            Mapping m::M (
              *m::Person: Relational { ~mainTable [s::DB] P
                name: P.NAME,
                addr ( city: P.CITY ) }
              *m::Firm: Relational { ~mainTable [s::DB] F legal: F.LEGAL }
              m::Emp: Relational { AssociationMapping ( employer: [s::DB] @PF ) }
              m::Mgr: Relational { AssociationMapping ( boss: [s::DB] @PB ) }
            )
            Runtime m::RT { mappings: [m::M]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE P (NAME VARCHAR, CITY VARCHAR, FID INTEGER,"
                    + " BOSS INTEGER, ID INTEGER)");
            st.execute("INSERT INTO P VALUES ('Ann', 'NYC', 1, 2, 1),"
                    + " ('Bob', 'SF', NULL, NULL, 2)");
            st.execute("CREATE TABLE F (ID INTEGER, LEGAL VARCHAR)");
            st.execute("INSERT INTO F VALUES (1, 'ACME')");
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
        return new DuckDb().render(new Lowerer().lower(resolved));
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

    private static int count(String sql, String kw) {
        int c = 0;
        for (int i = sql.indexOf(kw); i >= 0; i = sql.indexOf(kw, i + kw.length())) c++;
        return c;
    }

    @Test
    @DisplayName("11: two properties through one association — ONE join")
    void twoPropsOneAssociation() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[name: p|$p.name,"
                + " legal: p|$p.employer.legal])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(1, count(sql, "SELECT"), sql);
        assertEquals(List.of("Ann|ACME", "Bob|null"), exec(sql),
                "LEFT semantics: Bob's NULL FID keeps his row");
    }

    @Test
    @DisplayName("12: same navigation in filter AND project — ONE join (whole-chain dedup)")
    void filterAndProjectShareTheJoin() throws SQLException {
        String sql = sqlOf("m::Person.all()->filter(p|$p.employer.legal == 'ACME')"
                + "->project(~[legal: p|$p.employer.legal])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"),
                "the beats-V1 pin — one registry across the whole chain:\n" + sql);
        assertEquals(1, count(sql, "SELECT"), sql);
        assertEquals(List.of("ACME"), exec(sql));
    }

    @Test
    @DisplayName("14: self-association — distinct aliases, right orientation")
    void selfAssociation() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[name: p|$p.name,"
                + " bossName: p|$p.boss.name])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertTrue(sql.contains("P AS t0") && sql.contains("P AS t1"),
                "same table twice, two aliases: " + sql);
        assertEquals(List.of("Ann|Bob", "Bob|null"), exec(sql),
                "Ann's boss is Bob (orientation pin — a swapped condition"
                        + " would join BOSS to the wrong side)");
    }

    @Test
    @DisplayName("16: embedded navigation — parent-alias column, ZERO joins")
    void embeddedIsJoinless() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[city: p|$p.addr.city])"
                + "->from(m::RT)");
        assertEquals(0, count(sql, "JOIN"), sql);
        assertEquals(1, count(sql, "SELECT"), sql);
        assertTrue(sql.contains("t0.CITY AS city"), sql);
        assertEquals(List.of("NYC", "SF"), exec(sql));
    }

    @Test
    @DisplayName("18: association declared but NOT navigated — ZERO joins")
    void unNavigatedAssociationCostsNothing() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[name: p|$p.name])"
                + "->from(m::RT)");
        assertEquals(0, count(sql, "JOIN"), sql);
        assertEquals(List.of("Ann", "Bob"), exec(sql));
    }

    @Test
    @DisplayName("14b: self-association property2 to-one variant navigates correctly")
    void selfAssociationOtherEnd() throws SQLException {
        // reports is [*] (loud, H3c) — pin the loud path for the other end.
        var ctx = Compiler.compileModel(MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        var body = specs.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(
                "m::Person.all()->project(~[r: p|$p.reports.name])->from(m::RT)")));
        var e = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.NotImplementedException.class,
                () -> new StoreResolver(ctx, specs).resolve(body, null));
        assertTrue(e.getMessage().contains("EXISTS in"), e.getMessage());
    }

    @Test
    @DisplayName("two DIFFERENT associations in one chain — two joins, one SELECT")
    void twoAssociationsOneChain() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[legal: p|$p.employer.legal,"
                + " bossName: p|$p.boss.name])->from(m::RT)");
        assertEquals(2, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(1, count(sql, "SELECT"), sql);
        assertEquals(List.of("ACME|Bob", "null|null"), exec(sql));
    }

    @Test
    @DisplayName("toOne()-wrapped navigation is transparent to the path (audit R3)")
    void toOneWrappedNavigation() throws SQLException {
        String sql = sqlOf("m::Person.all()"
                + "->project(~[legal: p|$p.employer->toOne().legal])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(List.of("ACME", "null"), exec(sql));
    }

    @Test
    @DisplayName("bare association head gets the honest unsupported story, not 'not mapped'")
    void bareAssocHeadHonestError() {
        var ctx = Compiler.compileModel(MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        var body = specs.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(
                "m::Person.all()->filter(p|$p.employer->isEmpty())"
                        + "->project(~[name: p|$p.name])->from(m::RT)")));
        var e = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.NotImplementedException.class,
                () -> new StoreResolver(ctx, specs).resolve(body, null));
        assertTrue(e.getMessage().contains("navigation head"), e.getMessage());
    }

    @Test
    @DisplayName("audit: assoc leaf mapped through the TARGET's own join slot is loud")
    void targetSlotLeafIsLoud() {
        String model = """
            Class m::P { name: String[1]; }
            Class m::E { orgName: String[1]; }
            Association m::PE { emp: m::E[1]; owner: m::P[1]; }
            Database s::DB (
              Table P (NAME VARCHAR(50), EID INTEGER)
              Table E (ID INTEGER, OID INTEGER)
              Table O (ID INTEGER, ONAME VARCHAR(50))
              Join PE (P.EID = E.ID)
              Join EO (E.OID = O.ID) )
            Mapping m::M (
              *m::P: Relational { ~mainTable [s::DB] P name: P.NAME }
              *m::E: Relational { ~mainTable [s::DB] E orgName: @EO | O.ONAME }
              m::PE: Relational { AssociationMapping ( emp: [s::DB] @PE ) } )
            Runtime m::RT { mappings: [m::M]; }
            """;
        var ctx = Compiler.compileModel(model);
        SpecCompiler specs = new SpecCompiler(ctx);
        var body = specs.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(
                "m::P.all()->project(~[o: p|$p.emp.orgName])->from(m::RT)")));
        var e = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.NotImplementedException.class,
                () -> new StoreResolver(ctx, specs).resolve(body, null));
        assertTrue(e.getMessage().contains("join slots"), e.getMessage());
    }

    // ---- part 3: class-typed Join PM (navigate-step) navigation ----

    private static final String A7_MODEL = """
            Class m::P { name: String[1]; firm: m::F[1]; }
            Class m::F { legal: String[1]; }
            Database s::DB (
              Table P (NAME VARCHAR(50), FID INTEGER)
              Table F (ID INTEGER, LEGAL VARCHAR(50))
              Join PF (P.FID = F.ID) )
            Mapping m::M (
              *m::P: Relational { ~mainTable [s::DB] P name: P.NAME, firm: [s::DB] @PF }
              *m::F: Relational { ~mainTable [s::DB] F legal: F.LEGAL } )
            Runtime m::RT { mappings: [m::M]; }
            """;

    private static String sqlOfA7(String query) {
        var ctx = Compiler.compileModel(A7_MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        var body = specs.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(query)));
        return new DuckDb().render(new Lowerer().lower(
                new StoreResolver(ctx, specs).resolve(body, null)));
    }

    @Test
    @DisplayName("25a: class-typed Join PM navigated — ONE LEFT JOIN against the target pipeline")
    void classTypedJoinPmNavigates() throws SQLException {
        String sql = sqlOfA7("m::P.all()->project(~[name: p|$p.name,"
                + " legal: p|$p.firm.legal])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(1, count(sql, "SELECT"), sql);
        assertEquals(List.of("Ann|ACME", "Bob|null"), exec(sql));
    }

    @Test
    @DisplayName("25b: class-typed Join PM NOT navigated — the step strips, ZERO joins")
    void classTypedJoinPmElides() throws SQLException {
        String sql = sqlOfA7("m::P.all()->project(~[name: p|$p.name])->from(m::RT)");
        assertEquals(0, count(sql, "JOIN"),
                "un-navigated class slot must cancel (was the A7 lowerer wall):\n" + sql);
        assertEquals(List.of("Ann", "Bob"), exec(sql));
    }

    @Test
    @DisplayName("23: multiple nav paths in ONE computed expression")
    void multiplePathsOneExpression() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[combo:"
                + " p|$p.employer.legal + '/' + $p.boss.name])->from(m::RT)");
        assertEquals(2, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(1, count(sql, "SELECT"), sql);
        assertEquals(List.of("ACME/Bob", "null"), exec(sql),
                "NULL string concat propagates per SQL semantics");
    }

    @Test
    @DisplayName("audit: bare class-typed heads (nav slot / embedded) get the H4 story")
    void bareClassTypedHeadsHonest() {
        var ctx = Compiler.compileModel(A7_MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        var body = specs.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(
                "m::P.all()->project(~[f: p|$p.firm])->from(m::RT)")));
        var e = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.NotImplementedException.class,
                () -> new StoreResolver(ctx, specs).resolve(body, null));
        assertTrue(e.getMessage().contains("H4"), e.getMessage());

        var ctx2 = Compiler.compileModel(MODEL);
        SpecCompiler specs2 = new SpecCompiler(ctx2);
        var body2 = specs2.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(
                "m::Person.all()->project(~[a: p|$p.addr])->from(m::RT)")));
        var e2 = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.NotImplementedException.class,
                () -> new StoreResolver(ctx2, specs2).resolve(body2, null));
        assertTrue(e2.getMessage().contains("H4"), e2.getMessage());
    }
}
