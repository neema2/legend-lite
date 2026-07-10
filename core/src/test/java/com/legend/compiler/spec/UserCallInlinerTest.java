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
            Database s::DB ( Table P (NAME VARCHAR(50), AGE INTEGER) )
            Mapping m::M (
              *m::Person: Relational { ~mainTable [s::DB] P name: P.NAME, age: P.AGE }
            )
            Runtime m::RT { mappings: [m::M]; }

            function m::isAdult(p: m::Person[1]): Boolean[1] { $p.age >= 18 }
            function m::shout(s: String[1]): String[1] { $s->toUpper() + '!' }
            function m::doubleShout(s: String[1]): String[1] { m::shout(m::shout($s)) }
            function m::span(lo: Integer[1], hi: Integer[1], v: Integer[1]): Boolean[1]
            { let low = $lo <= $v; let high = $v <= $hi; $low && $high }
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
                + "->project(~[name: p|$p.name])->sortBy(x|$x.name)");
        assertEquals(java.util.List.of("Ann", "Cat"), col(r));
    }

    @Test
    @DisplayName("nested calls compose (shout(shout(x)))")
    void nestedCalls() throws SQLException {
        var r = run("m::Person.all()->project(~[loud: p|m::doubleShout($p.name)])"
                + "->sortBy(x|$x.loud)");
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
        var r = run("m::adults()->project(~[name: p|$p.name])->sortBy(x|$x.name)");
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
    @DisplayName("recursion is LOUD with the cycle path")
    void recursionIsLoud() {
        var ex = assertThrows(Exception.class,
                () -> run("m::Person.all()->filter(p|m::recurse($p.age) > 0)"
                        + "->project(~[name: p|$p.name])"));
        assertTrue(ex.getMessage().contains("m::recurse/1")
                        && ex.getMessage().toLowerCase().contains("recursion"),
                "cycle named with fn/arity: " + ex.getMessage());
    }
}
