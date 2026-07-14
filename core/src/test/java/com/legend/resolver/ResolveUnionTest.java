package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.MappingResolutionException;
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
 * Union operation-mapping pins (audit 11: the machinery had corpus-only
 * verification). Each pin guards a behavior whose silent regression
 * returns WRONG ROWS:
 * <ul>
 *   <li>member-suffixed keys are NULL-crossed — un-routed members can
 *       never match a routed navigation (engine partial-union goldens);</li>
 *   <li>route coverage is per-entry — members without a route stay
 *       unmatched even when every routed entry is structurally equal;</li>
 *   <li>key provenance beats name patterns — a REAL column spelled like
 *       a member suffix cannot hijack the NULL thread;</li>
 *   <li>route outcomes are independent of textual PM order (per-PM
 *       {@code targetSetId}, not the name-keyed map);</li>
 *   <li>union-to-union emits the OR over the declared pairs;</li>
 *   <li>temporal unions stay LOUD until the lift threads the
 *       milestoning context.</li>
 * </ul>
 */
class ResolveUnionTest {

    private static final String UNION_OP =
            "meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_";

    private static final String MODEL = """
            Class u::PersonA { lastName: String[1]; }
            Class u::FirmA { legalName: String[1]; }
            Association u::EmploymentA { firmA: u::FirmA[0..1]; employeesA: u::PersonA[*]; }
            Class u::PersonB { lastName: String[1]; }
            Class u::FirmB { legalName: String[1]; }
            Association u::EmploymentB { firmB: u::FirmB[0..1]; employeesB: u::PersonB[*]; }
            Class u::FirmB2 { legalName: String[1]; }
            Association u::EmploymentB2 { firmB2: u::FirmB2[0..1]; employeesB2: u::PersonB[*]; }
            Class u::PersonD { lastName: String[1]; }
            Class <<temporal.businesstemporal>> u::FirmD { legalName: String[1]; }
            Association u::EmploymentD { firmD: u::FirmD[0..1]; employeesD: u::PersonD[*]; }
            Database u::DB (
              Table PA1 (ID INTEGER PRIMARY KEY, lastName_s1 VARCHAR(64), FirmID INTEGER)
              Table PA2 (ID INTEGER PRIMARY KEY, lastName_s2 VARCHAR(64), FirmID INTEGER)
              Table PA3 (ID INTEGER PRIMARY KEY, lastName_s3 VARCHAR(64), FirmID INTEGER)
              Table FA (ID INTEGER PRIMARY KEY, name VARCHAR(64))
              Table PB (ID INTEGER PRIMARY KEY, lastName VARCHAR(64), FirmID INTEGER)
              Table FB1 (ID INTEGER PRIMARY KEY, name VARCHAR(64), ID_1 INTEGER)
              Table FB2 (ID INTEGER PRIMARY KEY, name VARCHAR(64))
              Table PB2 (ID INTEGER PRIMARY KEY, lastName VARCHAR(64), FirmID INTEGER, LegacyID INTEGER)
              Table FG1 (ID INTEGER PRIMARY KEY, name VARCHAR(64))
              Table FG2 (ID INTEGER PRIMARY KEY, name VARCHAR(64))
              Table PD (ID INTEGER PRIMARY KEY, lastName VARCHAR(64), FirmID INTEGER)
              Table FD (
                milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                ID INTEGER PRIMARY KEY, name VARCHAR(64), from_z DATE, thru_z DATE)
              Join PA1FA (PA1.FirmID = FA.ID)
              Join PA2FA (PA2.FirmID = FA.ID)
              Join PA3FA (PA3.FirmID = FA.ID)
              Join PBFB1 (PB.FirmID = FB1.ID)
              Join PBFB2 (PB.FirmID = FB2.ID)
              Join PB2FG1 (PB2.FirmID = FG1.ID)
              Join PB2FG2 (PB2.LegacyID = FG2.ID)
              Join PDFD (PD.FirmID = FD.ID)
            )
            Mapping u::MA (
              *u::PersonA : Operation { %s(a1, a2, a3) }
              u::PersonA[a1] : Relational { ~mainTable [u::DB] PA1 lastName: PA1.lastName_s1 }
              u::PersonA[a2] : Relational { ~mainTable [u::DB] PA2 lastName: PA2.lastName_s2 }
              u::PersonA[a3] : Relational { ~mainTable [u::DB] PA3 lastName: PA3.lastName_s3 }
              *u::FirmA : Relational { ~mainTable [u::DB] FA
                legalName: FA.name,
                employeesA[a1]: [u::DB]@PA1FA,
                employeesA[a2]: [u::DB]@PA2FA }
            )
            Mapping u::MA2 (
              *u::PersonA : Operation { %s(a1, a2, a3) }
              u::PersonA[a1] : Relational { ~mainTable [u::DB] PA1 lastName: PA1.lastName_s1 }
              u::PersonA[a2] : Relational { ~mainTable [u::DB] PA2 lastName: PA2.lastName_s2 }
              u::PersonA[a3] : Relational { ~mainTable [u::DB] PA3 lastName: PA3.lastName_s3 }
              *u::FirmA : Relational { ~mainTable [u::DB] FA
                legalName: FA.name,
                employeesA[a2]: [u::DB]@PA2FA,
                employeesA[a1]: [u::DB]@PA1FA }
            )
            Mapping u::MB (
              *u::FirmB : Operation { %s(fb1, fb2) }
              u::FirmB[fb1] : Relational { ~mainTable [u::DB] FB1 legalName: FB1.name }
              u::FirmB[fb2] : Relational { ~mainTable [u::DB] FB2 legalName: FB2.name }
              *u::PersonB : Relational { ~mainTable [u::DB] PB
                lastName: PB.lastName,
                firmB[fb2]: [u::DB]@PBFB2 }
            )
            Mapping u::MB2 (
              *u::FirmB2 : Operation { %s(g1, g2) }
              u::FirmB2[g1] : Relational { ~mainTable [u::DB] FG1 legalName: FG1.name }
              u::FirmB2[g2] : Relational { ~mainTable [u::DB] FG2 legalName: FG2.name }
              *u::PersonB : Relational { ~mainTable [u::DB] PB2
                lastName: PB2.lastName,
                firmB2[g1]: [u::DB]@PB2FG1,
                firmB2[g2]: [u::DB]@PB2FG2 }
            )
            Mapping u::MD (
              *u::PersonD : Operation { %s(d1, d2) }
              u::PersonD[d1] : Relational { ~mainTable [u::DB] PD
                lastName: PD.lastName, firmD: [u::DB]@PDFD }
              u::PersonD[d2] : Relational { ~mainTable [u::DB] PD
                lastName: PD.lastName, firmD: [u::DB]@PDFD }
              *u::FirmD : Relational { ~mainTable [u::DB] FD legalName: FD.name }
            )
            Runtime u::RT { mappings: [u::MA]; }
            """.formatted(UNION_OP, UNION_OP, UNION_OP, UNION_OP, UNION_OP);

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE PA1 (ID INTEGER, lastName_s1 VARCHAR, FirmID INTEGER)");
            st.execute("INSERT INTO PA1 VALUES (1, 'A1-Ann', 10)");
            st.execute("CREATE TABLE PA2 (ID INTEGER, lastName_s2 VARCHAR, FirmID INTEGER)");
            st.execute("INSERT INTO PA2 VALUES (2, 'A2-Bob', 10)");
            st.execute("CREATE TABLE PA3 (ID INTEGER, lastName_s3 VARCHAR, FirmID INTEGER)");
            st.execute("INSERT INTO PA3 VALUES (3, 'A3-UNROUTED', 10)");
            st.execute("CREATE TABLE FA (ID INTEGER, name VARCHAR)");
            st.execute("INSERT INTO FA VALUES (10, 'FirmA-X')");
            st.execute("CREATE TABLE PB (ID INTEGER, lastName VARCHAR, FirmID INTEGER)");
            st.execute("INSERT INTO PB VALUES (1, 'B-Scott', 5)");
            // FB1.ID_1 is a REAL column holding GARBAGE that would match
            // PB.FirmID if provenance ever regressed to name-pattern guessing
            st.execute("CREATE TABLE FB1 (ID INTEGER, name VARCHAR, ID_1 INTEGER)");
            st.execute("INSERT INTO FB1 VALUES (99, 'WRONG-FIRM', 5)");
            st.execute("CREATE TABLE FB2 (ID INTEGER, name VARCHAR)");
            st.execute("INSERT INTO FB2 VALUES (5, 'RIGHT-FIRM')");
            st.execute("CREATE TABLE PB2 (ID INTEGER, lastName VARCHAR,"
                    + " FirmID INTEGER, LegacyID INTEGER)");
            // FirmID dangles (no FG1 row 99); LegacyID matches FG2
            st.execute("INSERT INTO PB2 VALUES (1, 'B-Scott', 99, 7)");
            st.execute("CREATE TABLE FG1 (ID INTEGER, name VARCHAR)");
            st.execute("INSERT INTO FG1 VALUES (7, 'WRONG-VIA-FIRMID')");
            st.execute("CREATE TABLE FG2 (ID INTEGER, name VARCHAR)");
            st.execute("INSERT INTO FG2 VALUES (7, 'LEGACY-FIRM')");
            st.execute("CREATE TABLE PD (ID INTEGER, lastName VARCHAR, FirmID INTEGER)");
            st.execute("INSERT INTO PD VALUES (1, 'D-Ann', 7)");
            st.execute("CREATE TABLE FD (ID INTEGER, name VARCHAR,"
                    + " from_z DATE, thru_z DATE)");
            st.execute("INSERT INTO FD VALUES (7, 'FirmD-X',"
                    + " DATE '2019-01-01', DATE '9999-12-31')");
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
    @DisplayName("routed navigation: un-routed members never match (coverage)")
    void coverageNeverMatchesUnroutedMember() throws SQLException {
        String sql = sqlOf("|u::FirmA.all()"
                + "->project([f|$f.legalName, f|$f.employeesA.lastName], ['legal', 'name'])"
                + "->from(u::MA, u::RT)");
        List<String> rows = exec(sql);
        assertEquals(List.of("FirmA-X|A1-Ann", "FirmA-X|A2-Bob"), rows,
                "the never-routed member a3 must not match");
        assertTrue(sql.contains("NULL AS"),
                "un-routed threads carry typed-NULL keys:\n" + sql);
    }

    @Test
    @DisplayName("route outcomes are independent of textual PM order")
    void routeOrderIrrelevant() throws SQLException {
        List<String> forward = exec(sqlOf("|u::FirmA.all()"
                + "->project([f|$f.legalName, f|$f.employeesA.lastName], ['legal', 'name'])"
                + "->from(u::MA, u::RT)"));
        List<String> reversed = exec(sqlOf("|u::FirmA.all()"
                + "->project([f|$f.legalName, f|$f.employeesA.lastName], ['legal', 'name'])"
                + "->from(u::MA2, u::RT)"));
        assertEquals(forward, reversed);
    }

    @Test
    @DisplayName("a REAL column spelled like a member suffix cannot hijack the NULL thread")
    void realColumnCannotHijackSuffix() throws SQLException {
        String sql = sqlOf("|u::PersonB.all()"
                + "->project([p|$p.lastName, p|$p.firmB.legalName], ['name', 'firm'])"
                + "->from(u::MB, u::RT)");
        List<String> rows = exec(sql);
        assertEquals(List.of("B-Scott|RIGHT-FIRM"), rows,
                "FB1's physical ID_1 garbage must not match:\n" + sql);
    }

    @Test
    @DisplayName("partial route: single-member navigation, suffixed key join")
    void partialRouteSuffixedKey() throws SQLException {
        String sql = sqlOf("|u::PersonB.all()"
                + "->filter(p|$p.firmB.legalName == 'RIGHT-FIRM')"
                + "->project([p|$p.lastName], ['name'])"
                + "->from(u::MB, u::RT)");
        assertTrue(sql.contains("ID_1"), "the routed member's suffixed key:\n" + sql);
        assertEquals(List.of("B-Scott"), exec(sql));
    }

    @Test
    @DisplayName("route merge requires ONE JOIN: two joins on one owner keep both conditions")
    void mergeRequiresSameJoin() throws SQLException {
        // audit 12 F3: coverage-only merging dropped the second route's
        // join entirely (J1(FirmID)=kept, J2(LegacyID)=vanished)
        String sql = sqlOf("|u::PersonB.all()"
                + "->project([p|$p.lastName, p|$p.firmB2.legalName], ['name', 'firm'])"
                + "->from(u::MB2, u::RT)");
        assertTrue(sql.contains("FirmID") && sql.contains("LegacyID"),
                "both routes' key columns must survive:\n" + sql);
        assertEquals(List.of("B-Scott|LEGACY-FIRM"), exec(sql),
                "the LegacyID route must match (and dangling FirmID must not):\n" + sql);
    }

    @Test
    @DisplayName("temporal union navigation: bare access LOUD, dated access point-filters")
    void temporalUnionNavigation() {
        // bare access to a temporal target still requires a date (engine)
        MappingResolutionException bare = assertThrows(MappingResolutionException.class,
                () -> sqlOf("|u::PersonD.all()"
                        + "->filter(p|$p.firmD.legalName == 'FirmD-X')"
                        + "->project([p|$p.lastName], ['name'])"
                        + "->from(u::MD, u::RT)"));
        assertTrue(bare.getMessage().contains("milestoning date"), bare.getMessage());
        // DATED access lifts and point-filters the navigate target
        // (single-dimension temporal unions are ungated; only BITEMPORAL
        // unions stay loud — the hybrid capability-mix rung)
        String sql = sqlOf("|u::PersonD.all()"
                + "->filter(p|$p.firmD(%2020-01-01).legalName == 'FirmD-X')"
                + "->project([p|$p.lastName], ['name'])"
                + "->from(u::MD, u::RT)");
        assertTrue(sql.contains("from_z") && sql.contains("thru_z"),
                "point filter must reach the navigate target:\n" + sql);
    }
}
