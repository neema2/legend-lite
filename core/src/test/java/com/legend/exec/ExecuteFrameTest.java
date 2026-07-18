// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.Compiler;
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
 * The RESULT FRAME (audit 19d B2): a let-bound {@code execute()} runs
 * EAGERLY and its {@code Result} becomes a typing surface plus an
 * orchestration handle — downstream {@code .values} reads splice into the
 * typed query chain and evaluate through the ordinary pipeline (Java
 * orchestrates, the database executes; no host object graph). The splice
 * rules are the harness's audited envelope semantics, moved platform-side:
 * a RELATION-rooted query's {@code values} holds ONE TDS ({@code ->at(0)}
 * / {@code ->toOne()} collapse, {@code at(k>0)} is loud, an alias'
 * {@code ->size()} is 1); for a class/scalar root, {@code values} IS the
 * collection.
 */
class ExecuteFrameTest {

    private static final String MODEL = """
            Class m::Person { name: String[1]; age: Integer[1]; }
            Database s::DB ( Table T (NAME VARCHAR(50), AGE INTEGER) )
            Mapping m::M (
              *m::Person: Relational { ~mainTable [s::DB] T
                name: T.NAME, age: T.AGE }
            )
            Runtime m::RT { mappings: [m::M]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE T (NAME VARCHAR, AGE INTEGER)");
            st.execute("INSERT INTO T VALUES ('Ann', 25), ('Bob', 35), ('Cat', 45)");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static ExecutionResult run(String query) throws SQLException {
        return Compiler.execute(MODEL, query, "m::RT", conn);
    }

    @Test
    @DisplayName("relation root: $r.values reads splice into the typed query and execute")
    void relationRootValues() throws Exception {
        ExecutionResult r = run("{| let r = meta::pure::mapping::execute("
                + "|m::Person.all()->project([p|$p.name], ['name']),"
                + " m::M, m::RT, []);\n"
                + "$r.values;}");
        ExecutionResult.Tabular t = (ExecutionResult.Tabular) r;
        assertEquals(3, t.rows().size());
    }

    @Test
    @DisplayName("relation root: downstream ops COMPOSE over the spliced chain")
    void relationRootComposes() throws Exception {
        ExecutionResult r = run("{| let r = meta::pure::mapping::execute("
                + "|m::Person.all()->project([p|$p.name, p|$p.age],"
                + " ['name', 'age']), m::M, m::RT, []);\n"
                + "$r.values->filter(x|$x.age > 30);}");
        ExecutionResult.Tabular t = (ExecutionResult.Tabular) r;
        assertEquals(2, t.rows().size());
    }

    @Test
    @DisplayName("relation root: ->at(0)/->toOne() collapse (the envelope holds one TDS)")
    void relationRootAtZeroCollapses() throws Exception {
        ExecutionResult r = run("{| let r = meta::pure::mapping::execute("
                + "|m::Person.all()->project([p|$p.name], ['name']),"
                + " m::M, m::RT, []);\n"
                + "$r.values->at(0);}");
        assertEquals(3, ((ExecutionResult.Tabular) r).rows().size());
    }

    @Test
    @DisplayName("relation root: ops compose OVER the collapsed ->at(0) wrapper")
    void relationRootOpsOverAtZero() throws Exception {
        ExecutionResult r = run("{| let r = meta::pure::mapping::execute("
                + "|m::Person.all()->project([p|$p.name], ['name']),"
                + " m::M, m::RT, []);\n"
                + "$r.values->at(0)->map(x|$x.name);}");
        assertEquals(3, ((ExecutionResult.Collection) r).values().size());
    }

    @Test
    @DisplayName("relation root: at(k>0) is loud — the envelope has ONE element")
    void relationRootAtOneIsLoud() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> run("{| let r = meta::pure::mapping::execute("
                        + "|m::Person.all()->project([p|$p.name], ['name']),"
                        + " m::M, m::RT, []);\n"
                        + "$r.values->at(1);}"));
        assertTrue(ex.getMessage().contains("at(k>0)"), ex.getMessage());
    }

    @Test
    @DisplayName("envelope alias: let tds = $r.values->at(0) keeps ONE-TDS semantics ($tds->size() == 1)")
    void envelopeAliasSize() throws Exception {
        ExecutionResult r = run("{| let r = meta::pure::mapping::execute("
                + "|m::Person.all()->project([p|$p.name], ['name']),"
                + " m::M, m::RT, []);\n"
                + "let tds = $r.values->at(0);\n"
                + "$tds->size();}");
        assertEquals(1, ((Number) ((ExecutionResult.Scalar) r).value()).intValue());
    }

    @Test
    @DisplayName("the eager run: a broken query surfaces AT the let, no read required")
    void eagerRunSurfacesAtLet() {
        assertThrows(RuntimeException.class,
                () -> run("{| let r = meta::pure::mapping::execute("
                        + "|m::Person.all()->project([p|$p.nope], ['nope']),"
                        + " m::M, m::RT, []);\n"
                        + "true;}"));
    }

    @Test
    @DisplayName("execute() in result position: the frame's run is the value")
    void executeInResultPosition() throws Exception {
        ExecutionResult r = run("{| meta::pure::mapping::execute("
                + "|m::Person.all()->project([p|$p.name], ['name']),"
                + " m::M, m::RT, []);}");
        assertEquals(3, ((ExecutionResult.Tabular) r).rows().size());
    }

    @Test
    @DisplayName("a let-bound query lambda resolves through the caller's lets")
    void letBoundQueryLambda() throws Exception {
        ExecutionResult r = run("{| let q = {|m::Person.all()"
                + "->project([p|$p.name], ['name'])};\n"
                + "let r = meta::pure::mapping::execute($q, m::M, m::RT, []);\n"
                + "$r.values;}");
        assertEquals(3, ((ExecutionResult.Tabular) r).rows().size());
    }
}
