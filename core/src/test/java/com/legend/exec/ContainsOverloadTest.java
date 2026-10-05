// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.Compiler;
import com.legend.Execution;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@code contains} over a table column, which may be empty (2026-09-28, DataCube T5):
 * a text column resolves to legend-engine's {@code string::contains(String[0..1], String[1])}
 * (core/pure/corefunctions/stringExtension.pure:21), never the collection function; and a
 * number column's {@code contains} IS the collection function -- membership in a list of at
 * most one -- whose SQL once passed the bare column to {@code list_contains}, which DuckDB
 * refuses. Rows are the verdict, the empty row included.
 */
class ContainsOverloadTest {

    private static final String MODEL = """
            ###Relational
            Database m::DB ( Table T ( S VARCHAR(8), I INTEGER ) )
            ###Connection
            RelationalDatabaseConnection m::Conn { store: m::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime m::RT { mappings: []; connections: [ m::DB: [ c: m::Conn ] ]; }
            """;

    /** The table is this session's: its runtime declares the DuckDB it executes on. */
    private static final String RUNTIME = "m::RT";

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE T (S VARCHAR(8), I INTEGER)");
            s.execute("INSERT INTO T VALUES ('abc', 1), (NULL, NULL), ('xyz', 2)");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private List<Object> column(String query) throws SQLException {
        ExecutionResult.Tabular t = (ExecutionResult.Tabular) Execution.execute(MODEL, query, RUNTIME, conn);
        List<Object> out = new ArrayList<>();
        for (Row row : t.rows()) {
            out.add(row.get(0));
        }
        return out;
    }

    @Test
    @DisplayName("a text column's contains is engine's [0..1] string overload")
    void textColumnResolvesToTheStringOverload() {
        TypedSpec typed = Compiler.query(Compiler.compileModel(MODEL), "|#>{m::DB.T}#->filter(x|$x.S->contains('b'))").expression();
        List<String> calls = new ArrayList<>();
        ArrayDeque<TypedSpec> work = new ArrayDeque<>(List.of(typed));
        while (!work.isEmpty()) {
            TypedSpec n = work.poll();
            if (n instanceof TypedNativeCall c && c.callee().qualifiedName().endsWith("::contains")) {
                calls.add(c.callee().qualifiedName() + " "
                        + c.callee().parameters().get(0).multiplicity().text());
            }
            work.addAll(n.children());
        }
        assertEquals(List.of("meta::pure::functions::string::contains [0..1]"), calls);
    }

    @Test
    @DisplayName("text contains: the empty row contains nothing")
    void textContains() throws SQLException {
        assertEquals(List.of("abc"),
                column("|#>{m::DB.T}#->filter(x|$x.S->contains('b'))->select(~[S])"));
        List<Object> not = column("|#>{m::DB.T}#->filter(x|!$x.S->contains('b'))->select(~[S])->sort(~S->ascending())");
        assertEquals(2, not.size(), "the empty row and 'xyz': " + not);
        assertEquals("xyz", not.get(not.get(0) == null ? 1 : 0));
    }

    @Test
    @DisplayName("number contains is membership in a list of at most one: it runs")
    void numberContainsIsMembership() throws SQLException {
        assertEquals(List.of(1),
                column("|#>{m::DB.T}#->filter(x|$x.I->contains(1))->select(~[I])").stream()
                        .map(v -> ((Number) v).intValue()).toList());
        List<Object> not = column("|#>{m::DB.T}#->filter(x|!$x.I->contains(1))->select(~[I])");
        assertEquals(2, not.size(), "the empty row and 2: " + not);
        assertEquals(true, not.contains(null));
    }

    @Test
    @DisplayName("the collection callee's first parameter is many; a text column never reaches it")
    void collectionOverloadIsForLists() {
        TypedSpec typed = Compiler.query(Compiler.compileModel(MODEL), "|#>{m::DB.T}#->filter(x|$x.I->contains(1))").expression();
        ArrayDeque<TypedSpec> work = new ArrayDeque<>(List.of(typed));
        String found = null;
        while (!work.isEmpty()) {
            TypedSpec n = work.poll();
            if (n instanceof TypedNativeCall c && c.callee().qualifiedName().endsWith("::contains")) {
                Multiplicity m = c.callee().parameters().get(0).multiplicity();
                found = c.callee().qualifiedName() + " " + m.text();
            }
            work.addAll(n.children());
        }
        assertEquals("meta::pure::functions::collection::contains [*]", found);
    }
}
