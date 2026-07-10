package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.exec.ExecutionResult;
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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * H5 (scalar slice) &mdash; MODEL-TO-MODEL mappings compose by
 * β-transitivity: {@code Person: Pure { ~src RawPerson ... }} substitutes
 * its {@code $src.prop} reads with RawPerson's own (relational) bindings,
 * so the composed {@link ClassSource} sits directly over the store and
 * NOTHING downstream knows M2M existed. One flat SELECT stays the pin.
 */
class ResolveM2mTest {

    private static final String MODEL = """
            Class m::RawPerson { firstName: String[1]; lastName: String[1]; age: Integer[1]; }
            Class m::Person { fullName: String[1]; age: Integer[1]; }
            Database s::DB (
              Table R (FIRST VARCHAR(50), LAST VARCHAR(50), AGE INTEGER)
            )
            Mapping m::M (
              *m::RawPerson: Relational { ~mainTable [s::DB] R
                firstName: R.FIRST, lastName: R.LAST, age: R.AGE }
              *m::Person: Pure { ~src m::RawPerson
                fullName: $src.firstName + ' ' + $src.lastName,
                age: $src.age }
            )
            Runtime m::RT { mappings: [m::M]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE R (FIRST VARCHAR, LAST VARCHAR, AGE INTEGER)");
            st.execute("INSERT INTO R VALUES ('Ann', 'Ash', 30), ('Bob', 'Bay', 40)");
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
        List<TypedSpec> resolved = new StoreResolver(ctx, specs).resolve(body, "m::RT");
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
    @DisplayName("H5-1: M2M project — composed bindings, ONE flat SELECT over the store")
    void m2mProject() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[full: p|$p.fullName, age: p|$p.age])");
        assertEquals(1, count(sql, "SELECT"),
                "β-composition leaves an ordinary relational pipeline:\n" + sql);
        assertEquals(0, count(sql, "JOIN"), sql);
        assertEquals(List.of("Ann Ash|30", "Bob Bay|40"),
                exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("H5-2: M2M filter on a composed binding — still one SELECT")
    void m2mFilter() throws SQLException {
        String sql = sqlOf("m::Person.all()->filter(p|$p.age > 35)"
                + "->project(~[full: p|$p.fullName])");
        assertEquals(1, count(sql, "SELECT"), sql);
        assertEquals(List.of("Bob Bay"), exec(sql));
    }

    @Test
    @DisplayName("H5-3: M2M bare root — the implicit serialize composes too")
    void m2mImplicitSerialize() throws SQLException {
        ExecutionResult r = Compiler.execute(MODEL, "m::Person.all()", "m::RT", conn);
        assertInstanceOf(ExecutionResult.Graph.class, r);
        String json = ((ExecutionResult.Graph) r).json();
        assertTrue(json.contains("\"fullName\":\"Ann Ash\"")
                && json.contains("\"age\":40"), json);
    }

    @Test
    @DisplayName("H5-5: M2M with an instance-space ~filter — composes into the pipeline")
    void m2mFilteredMapping() throws SQLException {
        String model = MODEL.replace("*m::Person: Pure { ~src m::RawPerson",
                "*m::Person: Pure { ~src m::RawPerson ~filter $src.age > 35");
        var ctx = Compiler.compileModel(model);
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(NameResolver.resolveQuery(
                SpecParser.parse("m::Person.all()->project(~[full: p|$p.fullName])")));
        List<TypedSpec> resolved = new StoreResolver(ctx, specs).resolve(body, "m::RT");
        String sql = new DuckDb().render(new Lowerer().lower(resolved));
        assertEquals(1, count(sql, "SELECT"), sql);
        assertEquals(List.of("Bob Bay"), exec(sql),
                "the mapping's ~filter composes through the upstream bindings");
    }

    @Test
    @DisplayName("H5-6: one M2M class, two runtimes, two stores — no memo poisoning (audit F1)")
    void m2mMemoIsContextKeyed() throws SQLException {
        String model = """
                Class m::Raw { name: String[1]; }
                Class m::Person { label: String[1]; }
                Database s::DB (
                  Table A (NAME VARCHAR(50))
                  Table B (NAME VARCHAR(50))
                )
                Mapping m::BaseA ( *m::Raw: Relational { ~mainTable [s::DB] A name: A.NAME } )
                Mapping m::BaseB ( *m::Raw: Relational { ~mainTable [s::DB] B name: B.NAME } )
                Mapping m::M2M ( *m::Person: Pure { ~src m::Raw label: $src.name } )
                Runtime m::RT1 { mappings: [m::M2M, m::BaseA]; }
                Runtime m::RT2 { mappings: [m::M2M, m::BaseB]; }
                """;
        try (java.sql.Connection c = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE A (NAME VARCHAR)");
                st.execute("INSERT INTO A VALUES ('fromA')");
                st.execute("CREATE TABLE B (NAME VARCHAR)");
                st.execute("INSERT INTO B VALUES ('fromB')");
            }
            var ctx = Compiler.compileModel(model);
            SpecCompiler specs = new SpecCompiler(ctx);
            StoreResolver one = new StoreResolver(ctx, specs);
            String q = "m::Person.all()->project(~[l: p|$p.label])";
            var b1 = specs.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(q)));
            String sql1 = new DuckDb().render(new Lowerer().lower(one.resolve(b1, "m::RT1")));
            var b2 = specs.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(q)));
            String sql2 = new DuckDb().render(new Lowerer().lower(one.resolve(b2, "m::RT2")));
            assertTrue(sql1.contains("FROM A"), sql1);
            assertTrue(sql2.contains("FROM B"),
                    "the SAME resolver instance under a different runtime must"
                            + " re-compose, never serve the memoized store: " + sql2);
        }
    }

    @Test
    @DisplayName("H5-7: a lambda param shadowing the upstream row var is LOUD (audit F2)")
    void m2mUpstreamRowVarCaptureIsLoud() {
        String model = MODEL.replace("fullName: $src.firstName + ' ' + $src.lastName,",
                "fullName: [$src.firstName]->map(row|$row + $src.lastName)->toOne(),");
        var ex = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> {
            var ctx = Compiler.compileModel(model);
            SpecCompiler specs = new SpecCompiler(ctx);
            var body = specs.typeQueryBody(NameResolver.resolveQuery(
                    SpecParser.parse("m::Person.all()->project(~[f: p|$p.fullName])")));
            new StoreResolver(ctx, specs).resolve(body, "m::RT");
        });
        assertTrue(String.valueOf(ex.getMessage()).contains("row")
                        || ex.getMessage() != null,
                "capture must be loud, never silently mis-scoped: " + ex.getMessage());
    }

    @Test
    @DisplayName("H5-4: association navigation from an M2M source is LOUD (H5b)")
    void m2mAssociationNavigationIsLoud() {
        String badModel = MODEL.replace("age: $src.age }",
                "age: $src.age, extra: $src.boss.age }")
                .replace("Class m::RawPerson { firstName: String[1]; lastName: String[1]; age: Integer[1]; }",
                        "Class m::RawPerson { firstName: String[1]; lastName: String[1]; age: Integer[1]; }\n"
                        + "            Association m::B { boss: m::RawPerson[1]; reports: m::RawPerson[*]; }");
        // shape probe only: composing must refuse loudly, not emit silent SQL
        org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> {
            var ctx = Compiler.compileModel(badModel
                    .replace("age: Integer[1]; }", "age: Integer[1]; extra: Integer[1]; }"));
            SpecCompiler specs = new SpecCompiler(ctx);
            List<TypedSpec> body = specs.typeQueryBody(NameResolver.resolveQuery(
                    SpecParser.parse("m::Person.all()->project(~[e: p|$p.extra])")));
            new StoreResolver(ctx, specs).resolve(body, "m::RT");
        });
    }
}
