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
 * M-H2b golden SQL + execution — the plan's fixtures 1-9 + 22
 * (docs/PHASE_H2_H3_RESOLVER_PLAN.md): simple class queries resolved
 * against a relational mapping land as the SAME flat SELECT shape the
 * relation surface produces (the real engine's flat-by-default rule),
 * DuckDB-executed, with 0-JOIN absence pins for slot elision and
 * {@code count(sql,"json")==0} for the map-terminal invariant.
 *
 * <p>Goldens are pinned via DIRECT resolver+Lowerer composition — the
 * driver seam wires in M-H2c.
 */
class ResolveSimpleClassTest {

    private static final String MODEL = """
            Class m::Person { name: String[1]; age: Integer[1]; nick: String[1]; }
            Class m::Coded { code: m::Status[1]; label: String[1]; }
            Enum m::Status { ACTIVE, INACTIVE }
            Class m::Emp { name: String[1]; firmName: String[1]; }
            Database s::DB (
              Table T (NAME VARCHAR(50), AGE INTEGER, ACTIVE INTEGER)
              Table C (CODE VARCHAR(10), LABEL VARCHAR(50))
              Table P (NAME VARCHAR(50), FID INTEGER)
              Table F (ID INTEGER, LEGAL VARCHAR(50))
              Join PF (P.FID = F.ID)
              Filter ActiveF ( T.ACTIVE = 1 )
            )
            Mapping m::M (
              m::Status: EnumerationMapping StatusM { ACTIVE: 'A', INACTIVE: 'I' }
              *m::Person: Relational { ~filter [s::DB] ActiveF ~mainTable [s::DB] T
                name: T.NAME, age: T.AGE, nick: toUpper(T.NAME) }
              *m::Coded: Relational { ~mainTable [s::DB] C
                code: EnumerationMapping StatusM: C.CODE, label: C.LABEL }
              *m::Emp: Relational { ~mainTable [s::DB] P
                name: P.NAME, firmName: @PF | F.LEGAL }
            )
            Runtime m::RT { mappings: [m::M]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE T (NAME VARCHAR, AGE INTEGER, ACTIVE INTEGER)");
            st.execute("INSERT INTO T VALUES ('Ann', 25, 1), ('Bob', 35, 1),"
                    + " ('Cat', 45, 0), ('Dan', 55, 1)");
            st.execute("CREATE TABLE C (CODE VARCHAR, LABEL VARCHAR)");
            st.execute("INSERT INTO C VALUES ('A', 'first'), ('I', 'second')");
            st.execute("CREATE TABLE P (NAME VARCHAR, FID INTEGER)");
            st.execute("INSERT INTO P VALUES ('Ann', 1), ('Bob', NULL)");
            st.execute("CREATE TABLE F (ID INTEGER, LEGAL VARCHAR)");
            st.execute("INSERT INTO F VALUES (1, 'ACME')");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    /** Direct resolver + Lowerer composition (the driver seam is M-H2c). */
    private static String sqlOf(String query) {
        var ctx = Compiler.compileModel(MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(
                NameResolver.resolveQuery(SpecParser.parse(query)));
        List<TypedSpec> resolved = new StoreResolver(ctx, specs)
                .resolve(body, "m::RT_UNUSED".equals(query) ? null : null);
        // H2b: mapping supplied via ->from(...) in the query text.
        SqlQuery plan = new Lowerer().lower(resolved);
        return new DuckDb().render(plan);
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

    private static void noJson(String sql) {
        assertEquals(0, count(sql.toLowerCase(), "json"),
                "map-terminal invariant: no JSON in relation-shaped SQL:\n" + sql);
    }

    // ---- fixture 1: project only ----
    @Test
    @DisplayName("1: getAll->project — flat SELECT, mapping ~filter present")
    void projectOnly() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[name: p|$p.name])->from(m::RT)");
        assertEquals("""
                SELECT t0.NAME AS name
                FROM T AS t0
                WHERE t0.ACTIVE = 1""", sql);
        noJson(sql);
        assertEquals(List.of("Ann", "Bob", "Dan"), exec(sql));
    }

    // ---- fixture 2: filter + project ----
    @Test
    @DisplayName("2: getAll->filter->project — ONE flat SELECT, AND-merged WHERE")
    void filterProject() throws SQLException {
        // The 3-arg from(): explicit mapping wins (context precedence pin).
        String sql = sqlOf("m::Person.all()->filter(p|$p.age > 30)"
                + "->project(~[name: p|$p.name, age: p|$p.age])->from(m::M, m::RT)");
        assertEquals("""
                SELECT t0.NAME AS name, t0.AGE AS age
                FROM T AS t0
                WHERE t0.ACTIVE = 1 AND t0.AGE > 30""", sql);
        assertEquals(1, count(sql, "SELECT"));
        assertEquals(List.of("Bob|35", "Dan|55"), exec(sql));
    }

    // ---- fixture 3: mapping ~filter ordering ----
    @Test
    @DisplayName("3: mapping ~filter fires even with no user filter (ordering pin)")
    void mappingFilterAlone() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[age: p|$p.age])->from(m::RT)");
        assertTrue(sql.contains("WHERE t0.ACTIVE = 1"), sql);
        assertEquals(List.of("25", "35", "55"), exec(sql));
    }

    // ---- fixture 4: dynafunction PM in both positions ----
    @Test
    @DisplayName("4: expression PM (toUpper) inlines in BOTH project and filter")
    void dynafunctionBothPositions() throws SQLException {
        String sql = sqlOf("m::Person.all()->filter(p|$p.nick == 'ANN')"
                + "->project(~[nick: p|$p.nick])->from(m::RT)");
        assertEquals(1, count(sql, "SELECT"), sql);
        assertTrue(sql.contains("upper(t0.NAME) AS nick"), sql);
        assertTrue(sql.contains("WHERE t0.ACTIVE = 1 AND upper(t0.NAME) = 'ANN'"), sql);
        assertEquals(List.of("ANN"), exec(sql));
    }

    // ---- fixture 5: enum PM projected ----
    @Test
    @DisplayName("5: enum PM projects as the decode expression (CASE form)")
    void enumProjected() throws SQLException {
        String sql = sqlOf("m::Coded.all()->project(~[label: c|$c.label, code: c|$c.code])"
                + "->from(m::RT)");
        assertEquals(1, count(sql, "SELECT"), sql);
        noJson(sql);
        assertEquals(List.of("first|ACTIVE", "second|INACTIVE"), exec(sql));
    }

    // ---- fixture 6: whole chain stays one select ----
    @Test
    @DisplayName("6: filter->project->sort->limit — still ONE flat SELECT")
    void wholeChainOneSelect() throws SQLException {
        String sql = sqlOf("m::Person.all()->filter(p|$p.age > 30)"
                + "->project(~[name: p|$p.name, age: p|$p.age])"
                + "->sort(desc(~age))->limit(1)->from(m::RT)");
        assertEquals(1, count(sql, "SELECT"), sql);
        assertEquals(List.of("Dan|55"), exec(sql));
    }

    // ---- fixture 7: unmapped property message golden ----
    @Test
    @DisplayName("7: unmapped property is loud, naming property, class, mapping")
    void unmappedPropertyLoud() {
        String model = MODEL.replace("age: T.AGE, ", "");
        var ctx = Compiler.compileModel(model);
        SpecCompiler specs = new SpecCompiler(ctx);
        var body = specs.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(
                "m::Person.all()->project(~[age: p|$p.age])->from(m::RT)")));
        MappingResolutionException e = assertThrows(MappingResolutionException.class,
                () -> new StoreResolver(ctx, specs).resolve(body, null));
        assertEquals("property 'age' of class 'm::Person' is not mapped in"
                + " mapping 'm::M'", e.getMessage());
    }

    // ---- fixture 8: slot elision (the H2 preview of join cancellation) ----
    @Test
    @DisplayName("8: join-carrying mapping, main-table-only query — ZERO JOIN")
    void slotElision() throws SQLException {
        String sql = sqlOf("m::Emp.all()->project(~[name: e|$e.name])->from(m::RT)");
        assertEquals(0, count(sql, "JOIN"),
                "un-demanded @PF slot must be elided:\n" + sql);
        assertEquals(1, count(sql, "SELECT"));
        assertEquals(List.of("Ann", "Bob"), exec(sql));
    }

    // ---- fixture 8b: the slot-demanding binding is loud (H3-pending) ----
    @Test
    @DisplayName("8b: consuming a slot-mapped property is loud H3-pending, not wrong SQL")
    void slotDemandIsLoud() {
        var ctx = Compiler.compileModel(MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        var body = specs.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(
                "m::Emp.all()->project(~[firmName: e|$e.firmName])->from(m::RT)")));
        var e = assertThrows(com.legend.error.NotImplementedException.class,
                () -> new StoreResolver(ctx, specs).resolve(body, null));
        assertTrue(e.getMessage().contains("join slot"), e.getMessage());
    }

    // ---- M-H2c: the driver seam — no-from query + driver runtime ----
    @Test
    @DisplayName("H2c: from-less query resolves via the 4-arg execute (the corpus shape)")
    void driverSeamNoFrom() throws SQLException {
        var result = Compiler.execute(MODEL,
                "m::Person.all()->filter(p|$p.age > 30)->project(~[name: p|$p.name])",
                "m::RT", conn);
        assertEquals(List.of(List.of("Bob"), List.of("Dan")),
                result.rows().stream()
                        .map(r -> r.values().stream().map(String::valueOf).toList())
                        .toList());
    }

    // ---- fixture 9: limit family passes through object space ----
    @Test
    @DisplayName("9: object-space limit stacks on the pipeline (limit-not-last isolates: 2 SELECTs)")
    void objectSpaceLimit() throws SQLException {
        String sql = sqlOf("m::Person.all()->limit(2)"
                + "->project(~[name: p|$p.name])->from(m::RT)");
        // INTENTIONAL nesting: projection over a limited source isolates
        // (both references wrap limit-not-last; Fold.projectionFolds guard).
        assertEquals(2, count(sql, "SELECT"), sql);
        assertTrue(sql.contains("LIMIT 2"), sql);
        assertEquals(2, exec(sql).size());
    }
}
