// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.ladder;

import com.legend.testing.Repo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.test.PureTestRunner;
import com.legend.test.PureTests;
import com.legend.test.TestObserver;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * THE LEAN SQL LADDER (user north star, 2026-09-20 —
 * docs/LEAN_VERDICT_LADDER_2026_09_20.md): one statement per test body, every
 * {@code let}'s product SQL in it exactly once, unchanged from what the
 * platform emits for a user, and the thinnest assert wrapper around it.
 *
 * <p>We own every rung here: a tiny model over one three-row table, and one
 * {@code <<test.Test>>} function per rung, each adding ONE construct to the
 * previous. Each rung runs through the real runner in DATABASE judge mode
 * on an in-memory DuckDB; every statement the runner sends is captured by a
 * recording JDBC proxy (no product hook) and compared, byte for byte, to the
 * rung's pinned CURRENT emission ({@code ladder/<rung>.current.sql}). Beside
 * it sits the hand-written LEAN target ({@code <rung>.lean.sql}). A rung is
 * CLOSED when current equals lean; until then the test prints the distance
 * (chars, subqueries, copies of the product query) and the register
 * ({@code ladder/register.txt}) says OPEN. Drift in either direction is loud.
 *
 * <p>Record mode ({@code -Dladder.record=1}) rewrites the current pins from
 * the run — used once when a rung is added or a shape is deliberately
 * changed; the diff is then reviewed like any other pin move.
 */
class LeanSqlLadderTest {

    private static final String MODEL = """
            Class l::Thing
            {
              id: Integer[1];
              name: String[1];
              amount: Float[1];
            }
            function <<test.Test>> l::r01_constant(): Boolean[1]
            {
              assertEquals(1, 1);
            }
            function <<test.Test>> l::r02_literalListSize(): Boolean[1]
            {
              assertEquals(3, [1, 2, 3]->size());
            }
            function <<test.Test>> l::r03_stringConstant(): Boolean[1]
            {
              assertEquals('a', 'a');
            }
            function <<test.Test>> l::r04_countNoLet(): Boolean[1]
            {
              assertSize(execute(|l::Thing.all(), l::M, l::RT.runtimeValue, []).values, 3);
            }
            function <<test.Test>> l::r05_countOneLet(): Boolean[1]
            {
              let r = execute(|l::Thing.all(), l::M, l::RT.runtimeValue, []);
              assertSize($r.values, 3);
            }
            function <<test.Test>> l::r06_columnVsList(): Boolean[1]
            {
              let r = execute(|l::Thing.all(), l::M, l::RT.runtimeValue, []);
              assertSameElements(['a', 'b', 'c'], $r.values.name);
            }
            function <<test.Test>> l::r07_projectRows(): Boolean[1]
            {
              let r = execute(|l::Thing.all()->project([t | $t.id, t | $t.name], ['id', 'name'])->sort('id'), l::M, l::RT.runtimeValue, []);
              assertSize($r.values.rows, 3);
            }
            function <<test.Test>> l::r08_positionalCell(): Boolean[1]
            {
              let r = execute(|l::Thing.all()->project([t | $t.id, t | $t.name], ['id', 'name'])->sort('id'), l::M, l::RT.runtimeValue, []);
              assertEquals('a', $r.values.rows->at(0).getString('name'));
            }
            function <<test.Test>> l::r09_twoAssertsOneLet(): Boolean[1]
            {
              let r = execute(|l::Thing.all()->project([t | $t.id, t | $t.name], ['id', 'name'])->sort('id'), l::M, l::RT.runtimeValue, []);
              assertSize($r.values.rows, 3);
              assertEquals('a', $r.values.rows->at(0).getString('name'));
            }
            function <<test.Test>> l::r10_twoLets(): Boolean[1]
            {
              let r = execute(|l::Thing.all(), l::M, l::RT.runtimeValue, []);
              let s = execute(|l::Thing.all()->filter(t | $t.amount > 1.0), l::M, l::RT.runtimeValue, []);
              assertSize($r.values, 3);
              assertSize($s.values, 2);
            }
            function <<test.Test>> l::r11_floatLeniency(): Boolean[1]
            {
              let r = execute(|l::Thing.all(), l::M, l::RT.runtimeValue, []);
              assertSameElements([0.5, 1.5, 2.5], $r.values.amount);
            }
            function <<test.Test>> l::r12_classLetManyReaders(): Boolean[1]
            {
              let r = execute(|l::Thing.all(), l::M, l::RT.runtimeValue, []);
              assertSize($r.values, 3);
              assertSameElements(['a', 'b', 'c'], $r.values.name);
              assertEquals(3, $r.values->filter(t | $t.amount > 0.0)->size());
            }
            ###Relational
            Database l::DB (
              Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), AMOUNT DOUBLE)
            )
            ###Mapping
            Mapping l::M (
              *l::Thing : Relational { ~mainTable [l::DB] T
                id: T.ID,
                name: T.NAME,
                amount: T.AMOUNT }
            )
            ###Runtime
            Runtime l::RT { mappings: [l::M]; }
            """;

    private static final Path PINS = Repo.module("src/test/resources/ladder");
    private static final boolean RECORD = System.getProperty("ladder.record") != null;

    private static ModelContext ctx;
    private static PureTests.Discovery found;
    @BeforeAll
    static void compile() throws Exception {
        Compiler.ParsedModule parsed = Compiler.parseSources(
                List.of(new Compiler.ModelSource("ladder.pure", MODEL)));
        ctx = Compiler.buildModule(parsed.model()).context();
        found = PureTests.discover(parsed.model(), Set.of());
        Files.createDirectories(PINS);
    }

    /** Every statement the runner sent for one rung, trace comments stripped. */
    private static List<String> statementsOf(String fqn) throws Exception {
        PureTests.TestCase test = found.tests().stream()
                .filter(t -> t.fqn().equals(fqn)).findFirst().orElseThrow();
        List<String> sent = new ArrayList<>();
        // the run's judge mode is the runner's (one mode per run, on its options)
        try (PureTestRunner runner = new PureTestRunner(ctx, "l::RT",
                () -> recording(freshDuck(), sent),
                List.of(), found.setupsByPackage(), TestObserver.NONE,
                com.legend.ExecuteOptions.JudgeMode.DATABASE)) {
            PureTestRunner.Result r = runner.run(test);
            assertEquals(PureTestRunner.Status.PASS, r.status(), fqn + ": " + r.reason());
        }
        List<String> out = new ArrayList<>();
        for (String s : sent) {
            String bare = s.lines().filter(l -> !l.startsWith("-- \"executionTraceID\""))
                    .reduce((a, b) -> a + "\n" + b).orElse("");
            if (!bare.startsWith("SET ") && !bare.startsWith("CREATE ") && !bare.startsWith("INSERT ")) {
                out.add(bare);
            }
        }
        return out;
    }

    private static Connection freshDuck() {
        try {
            Connection c = DriverManager.getConnection("jdbc:duckdb:");
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE T (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), AMOUNT DOUBLE)");
                st.execute("INSERT INTO T VALUES (1, 'a', 0.5), (2, 'b', 1.5), (3, 'c', 2.5)");
            }
            return c;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    /** A JDBC proxy that records the text of every statement sent through it. */
    private static Connection recording(Connection inner, List<String> sent) {
        InvocationHandler h = (proxy, method, args) -> {
            Object r;
            try {
                r = method.invoke(inner, args);
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
            if ("prepareStatement".equals(method.getName()) && args != null && args[0] instanceof String sql
                    && r instanceof PreparedStatement ps) {
                // a prepare is a metadata probe until the statement EXECUTES —
                // only an execution is a statement sent (the wire-type probe
                // prepares a plan to read its reported columns and never runs it)
                return Proxy.newProxyInstance(LeanSqlLadderTest.class.getClassLoader(),
                        new Class<?>[] {PreparedStatement.class}, (p2, m2, a2) -> {
                            if (m2.getName().startsWith("execute")) {
                                sent.add(sql);
                            }
                            try {
                                return m2.invoke(ps, a2);
                            } catch (java.lang.reflect.InvocationTargetException e) {
                                throw e.getCause();
                            }
                        });
            } else if ("createStatement".equals(method.getName()) && r instanceof Statement st) {
                return Proxy.newProxyInstance(LeanSqlLadderTest.class.getClassLoader(),
                        new Class<?>[] {Statement.class}, (p2, m2, a2) -> {
                            if (m2.getName().startsWith("execute") && a2 != null && a2.length > 0
                                    && a2[0] instanceof String sql) {
                                sent.add(sql);
                            }
                            try {
                                return m2.invoke(st, a2);
                            } catch (java.lang.reflect.InvocationTargetException e) {
                                throw e.getCause();
                            }
                        });
            }
            return r;
        };
        return (Connection) Proxy.newProxyInstance(LeanSqlLadderTest.class.getClassLoader(),
                new Class<?>[] {Connection.class}, h);
    }

    private static final List<String> RUNGS = List.of(
            "l::r01_constant", "l::r02_literalListSize", "l::r03_stringConstant",
            "l::r04_countNoLet", "l::r05_countOneLet", "l::r06_columnVsList",
            "l::r07_projectRows", "l::r08_positionalCell", "l::r09_twoAssertsOneLet",
            "l::r10_twoLets", "l::r11_floatLeniency", "l::r12_classLetManyReaders");

    @Test
    @DisplayName("every rung: passes in database mode, sends exactly its pinned statements; distance to the lean target reported")
    void ladder() throws Exception {
        List<String> report = new ArrayList<>();
        List<String> drift = new ArrayList<>();
        for (String fqn : RUNGS) {
            String rung = fqn.substring(fqn.indexOf("::") + 2);
            List<String> sent = statementsOf(fqn);
            String current = String.join("\n;;\n", sent) + "\n";
            Path currentPin = PINS.resolve(rung + ".current.sql");
            Path leanPin = PINS.resolve(rung + ".lean.sql");
            if (RECORD) {
                Files.writeString(currentPin, current, StandardCharsets.UTF_8);
            }
            String pinned = Files.exists(currentPin) ? Files.readString(currentPin) : null;
            if (pinned == null) {
                drift.add(rung + ": no current pin (run once with -Dladder.record=1)");
            } else if (!pinned.equals(current)) {
                Path currentOut = Repo.out(rung + ".current.sql");
                Files.writeString(currentOut, current);
                drift.add(rung + ": emission drifted from the pin (" + currentOut
                        + " holds the new text; re-record only for a deliberate shape change)");
            }
            String lean = Files.exists(leanPin) ? Files.readString(leanPin) : null;
            String status = lean == null ? "OPEN (no lean target written yet)"
                    : normalize(lean).equals(normalize(current)) ? "CLOSED" : "OPEN";
            report.add(String.format("%-24s %-8s statements=%d chars=%5d subqueries=%3d  %s",
                    rung, status.split(" ")[0], sent.size(), current.length(),
                    count(current, "(SELECT "), lean == null ? "" : "lean chars=" + lean.length()));
        }
        report.forEach(l -> System.out.println("[ladder] " + l));
        assertTrue(drift.isEmpty(), String.join("\n", drift));
    }

    private static String normalize(String sql) {
        return sql.strip().replaceAll("\\s+", " ");
    }

    private static int count(String s, String needle) {
        int n = 0;
        for (int i = s.indexOf(needle); i >= 0; i = s.indexOf(needle, i + 1)) {
            n++;
        }
        return n;
    }
}
