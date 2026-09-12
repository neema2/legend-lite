package com.legend.compiler.spec;

import com.legend.Compiler;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase G&frac12; &mdash; user-call &beta;-inlining, executed end to end:
 * scalar calls in every position, nested calls, callee lets, capture
 * hygiene, relation- and class-returning callees, higher-order eval
 * reduction, and the loud recursion guard.
 */
class UserCallInlinerTest {

    private static final String MODEL = """
            Class m::Person { name: String[1]; age: Integer[1]; }
            ###Relational
            Database s::DB ( Table P (NAME VARCHAR(50), AGE INTEGER) )
            ###Mapping
            Mapping m::M (
              *m::Person: Relational { ~mainTable [s::DB] P name: P.NAME, age: P.AGE }
            )
            ###Runtime
            Runtime m::RT { mappings: [m::M]; }

            ###Pure
            function m::isAdult(p: m::Person[1]): Boolean[1] { $p.age >= 18 }
            function m::shout(s: String[1]): String[1] { $s->toUpper() + '!' }
            function m::doubleShout(s: String[1]): String[1] { m::shout(m::shout($s)) }
            function m::span(lo: Integer[1], hi: Integer[1], v: Integer[1]): Boolean[1]
            { let low = $lo <= $v; let high = $v <= $hi; $low && $high; }
            function m::adults(): m::Person[*] { m::Person.all()->filter(p|m::isAdult($p)) }
            function m::applyTo(f: Function<{Integer[1]->Integer[1]}>[1], v: Integer[1]): Integer[1]
            { $f->eval($v) }
            function m::recurse(n: Integer[1]): Integer[1] { m::recurse($n - 1) }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE P (NAME VARCHAR, AGE INTEGER)");
            st.execute("INSERT INTO P VALUES ('Ann', 30), ('Bob', 15), ('Cat', 45)");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private ExecutionResult run(String query) throws SQLException {
        return Compiler.execute(MODEL, query, "m::RT", conn);
    }

    private java.util.List<?> col(ExecutionResult r) {
        return ((ExecutionResult.Tabular) r).rows().stream()
                .map(row -> row.values().get(0)).toList();
    }

    @Test
    @DisplayName("a user call in OBJECT-space filter position — join demand sees through")
    void objectSpaceCall() throws SQLException {
        var r = run("m::Person.all()->filter(p|m::isAdult($p))"
                + "->project(~[name: p|$p.name])->sort(~name->ascending())");
        assertEquals(java.util.List.of("Ann", "Cat"), col(r));
    }

    @Test
    @DisplayName("nested calls compose (shout(shout(x)))")
    void nestedCalls() throws SQLException {
        var r = run("m::Person.all()->project(~[loud: p|m::doubleShout($p.name)])"
                + "->sort(~loud->ascending())");
        assertEquals(java.util.List.of("ANN!!", "BOB!!", "CAT!!"), col(r));
    }

    @Test
    @DisplayName("callee lets reduce forward — one expression comes out")
    void calleeLets() throws SQLException {
        var r = run("m::Person.all()->filter(p|m::span(20, 40, $p.age))"
                + "->project(~[name: p|$p.name])");
        assertEquals(java.util.List.of("Ann"), col(r));
    }

    @Test
    @DisplayName("a CLASS-returning callee splices its getAll chain — H resolves it normally")
    void classReturningCallee() throws SQLException {
        var r = run("m::adults()->project(~[name: p|$p.name])->sort(~name->ascending())");
        assertEquals(java.util.List.of("Ann", "Cat"), col(r));
    }

    @Test
    @DisplayName("higher-order: eval of a lambda literal β-reduces after substitution")
    void higherOrderEval() throws SQLException {
        var r = run("m::Person.all()->filter(p|m::applyTo(v|$v * 2, $p.age) > 80)"
                + "->project(~[name: p|$p.name])");
        assertEquals(java.util.List.of("Cat"), col(r));
    }

    @Test
    @DisplayName("capture hygiene: an argument's variable survives a same-named callee binder")
    void captureHygiene() throws SQLException {
        // shout's body has no lambda, so exercise via applyTo whose eval
        // lambda parameter is named 'v' — the ARGUMENT also reads a var
        // named 'v' from the enclosing filter lambda.
        var r = run("m::Person.all()->filter(v|m::applyTo(x|$x + $v.age, $v.age) > 80)"
                + "->project(~[name: v|$v.name])");
        assertEquals(java.util.List.of("Cat"), col(r),
                "the eval lambda's own binder must not capture the outer $v");
    }

    @Test
    @DisplayName("recursion DEFERS: the call stands and SQL-bound use is loud")
    void recursionIsLoud() {
        // CONTRACT CHANGE (relationalMapper leg): a recursive call no
        // longer throws at inline time — it STANDS (host call frames and
        // the plan-config extraction consume standing calls). SQL-bound
        // use still fails LOUDLY naming the function, at the resolver's
        // TypedUserCall wall.
        var ex = assertThrows(Exception.class,
                () -> run("m::Person.all()->filter(p|m::recurse($p.age) > 0)"
                        + "->project(~[name: p|$p.name])"));
        assertTrue(String.valueOf(ex.getMessage()).contains("m::recurse")
                        && ex.getMessage().contains("TypedUserCall"),
                "SQL-bound recursion walls naming the function: "
                        + ex.getMessage());
    }

    @Test
    @DisplayName("agg orderKey survives the inliner rebuild (remediation T2.1)")
    void aggOrderKeySurvivesInlining() {
        // the hand-written TypedGroupBy arm rebuilt each TypedAggCol through
        // the 3-arg convenience constructor, silently nulling orderKey —
        // graft one on and prove the withChildren rebuild keeps it
        var ctx = Compiler.buildModel(com.legend.testing.Own.model(MODEL));
        var specs = new SpecCompiler(ctx);
        var body = specs.typeQueryBody(
                com.legend.compiler.NameResolver.resolveQuery(
                        com.legend.testing.Own.spec(
                                "|m::Person.all()"
                                        + "->project(~[name: p|$p.name, age: p|$p.age])"
                                        + "->groupBy(~[name], ~[total: x|$x.age : y|$y->sum()])")));
        var gb = org.junit.jupiter.api.Assertions.assertInstanceOf(
                com.legend.compiler.spec.typed.TypedGroupBy.class,
                body.get(body.size() - 1));
        var a = gb.aggs().get(0);
        var grafted = new com.legend.compiler.spec.typed.TypedGroupBy(gb.source(), gb.keys(),
                java.util.List.of(new com.legend.compiler.spec.typed.TypedAggCol(
                        a.name(), a.map(), a.reduce(), java.util.List.of(
                                new com.legend.compiler.spec.typed.TypedAggCol.AggOrder(a.map(), false, null)))),
                gb.info());
        var out = new UserCallInliner(specs).inlineBody(java.util.List.of(grafted));
        var g2 = org.junit.jupiter.api.Assertions.assertInstanceOf(
                com.legend.compiler.spec.typed.TypedGroupBy.class,
                out.get(out.size() - 1));
        org.junit.jupiter.api.Assertions.assertEquals(1, g2.aggs().get(0).order().size(),
                "the inliner rebuild must not drop the agg's order");
        org.junit.jupiter.api.Assertions.assertFalse(g2.aggs().get(0).order().get(0).ascending(),
                "the direction must ride along too");
    }
}
