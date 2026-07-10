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

/**
 * M-H3c — OTHERWISE per-leaf dispatch (docs/PHASE_H2_H3_RESOLVER_PLAN.md
 * fixture 17; V1 &sect;D.5): a leaf mapped by the embedded partial reads
 * the PARENT row (no join); any other leaf goes through the fallback's
 * demanded navigate slot (LEFT join). The SAME head can go both ways in
 * one query. Dispatch happens at each access site — there is no
 * whole-property granularity.
 */
class ResolveOtherwiseTest {

    private static final String MODEL = """
            Class m::Person { name: String[1]; }
            Class m::Firm { legalName: String[1]; revenue: Integer[1]; }
            Association m::PF { person: m::Person[*]; firm: m::Firm[1]; }
            Database s::DB (
              Table T_PERSON (ID INTEGER, NAME VARCHAR(100), FIRM_NAME VARCHAR(200), FIRM_ID INTEGER)
              Table T_FIRM (ID INTEGER, LEGAL_NAME VARCHAR(200), REVENUE INTEGER)
              Join PF (T_PERSON.FIRM_ID = T_FIRM.ID)
            )
            Mapping m::M (
              *m::Person: Relational { ~mainTable [s::DB] T_PERSON
                name: T_PERSON.NAME,
                firm ( legalName: T_PERSON.FIRM_NAME ) Otherwise ([firm_set1]: [s::DB] @PF)
              }
              m::Firm[firm_set1]: Relational { ~mainTable [s::DB] T_FIRM
                legalName: T_FIRM.LEGAL_NAME,
                revenue: T_FIRM.REVENUE }
              m::PF: Relational { AssociationMapping ( person: [s::DB] @PF, firm: [s::DB] @PF ) }
            )
            Runtime m::RT { mappings: [m::M]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE T_PERSON (ID INTEGER, NAME VARCHAR,"
                    + " FIRM_NAME VARCHAR, FIRM_ID INTEGER)");
            st.execute("INSERT INTO T_PERSON VALUES (1, 'Alice', 'Acme Corp', 1),"
                    + " (2, 'Bob', 'Beta Inc', 2), (3, 'Charlie', 'Acme Corp', 1)");
            st.execute("CREATE TABLE T_FIRM (ID INTEGER, LEGAL_NAME VARCHAR, REVENUE INTEGER)");
            st.execute("INSERT INTO T_FIRM VALUES (1, 'Acme Corp', 1000000),"
                    + " (2, 'Beta Inc', 500000)");
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
    @DisplayName("17a: embedded leaf — parent-alias column, ZERO joins")
    void embeddedLeafIsJoinless() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[name: p|$p.name,"
                + " firmName: p|$p.firm.legalName])->from(m::RT)");
        assertEquals(0, count(sql, "JOIN"), sql);
        assertEquals(List.of("Alice|Acme Corp", "Bob|Beta Inc", "Charlie|Acme Corp"),
                exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("17b: fallback leaf — ONE LEFT join through the navigate slot")
    void fallbackLeafJoins() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[name: p|$p.name,"
                + " rev: p|$p.firm.revenue])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(1, count(sql, "SELECT"), sql);
        assertEquals(List.of("Alice|1000000", "Bob|500000", "Charlie|1000000"),
                exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("17c: SAME head both ways in one query — embedded from parent + fallback joined")
    void sameHeadBothWays() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[name: p|$p.name,"
                + " firmName: p|$p.firm.legalName, rev: p|$p.firm.revenue])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"),
                "per-leaf dispatch: legalName stays on the parent, only"
                        + " revenue fires the join:\n" + sql);
        assertEquals(1, count(sql, "T_FIRM"), sql);
        assertEquals(List.of("Alice|Acme Corp|1000000", "Bob|Beta Inc|500000",
                        "Charlie|Acme Corp|1000000"),
                exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("17d: filter on an embedded leaf — ZERO joins (the corpus pin)")
    void filterOnEmbeddedLeaf() throws SQLException {
        String sql = sqlOf("m::Person.all()->filter(p|$p.firm.legalName == 'Acme Corp')"
                + "->project(~[name: p|$p.name])->from(m::RT)");
        assertEquals(0, count(sql, "JOIN"), sql);
        assertEquals(List.of("Alice", "Charlie"), exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("17e: filter on a FALLBACK leaf + project an embedded one — one join, right rows")
    void filterOnFallbackProjectEmbedded() throws SQLException {
        String sql = sqlOf("m::Person.all()->filter(p|$p.firm.revenue > 600000)"
                + "->project(~[firmName: p|$p.firm.legalName])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(List.of("Acme Corp", "Acme Corp"),
                exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("17f: un-navigated otherwise head costs NOTHING")
    void unNavigatedOtherwiseCostsNothing() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[name: p|$p.name])->from(m::RT)");
        assertEquals(0, count(sql, "JOIN"), sql);
        assertEquals(0, count(sql, "T_FIRM"), sql);
        assertEquals(List.of("Alice", "Bob", "Charlie"),
                exec(sql).stream().sorted().toList());
    }
}
