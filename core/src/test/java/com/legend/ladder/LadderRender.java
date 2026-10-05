// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.ladder;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.test.PureTestRunner;
import com.legend.test.PureTests;
import com.legend.test.TestObserver;
import java.lang.reflect.InvocationHandler;
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
import java.util.TreeSet;

/**
 * THE LEAN SQL LADDER's CURRENT EMISSION (docs/LEAN_VERDICT_LADDER_2026_09_20.md): each rung of the model below run
 * through the real runner in DATABASE judge mode on an in-memory DuckDB, every statement it sends captured by a
 * recording JDBC proxy (no product hook), written as {@code <rung>.current.sql}. {@code //core:ladder_report} runs it;
 * {@code bazel run //core:update_ladder} writes the pins, and {@code //core:update_ladder_test} (in //:generated) fails
 * when the emission moves -- a deliberate shape change is re-pinned through that run and reviewed like any other pin
 * move. A rung that does not pass fails the action. {@link LeanSqlLadderTest} reports each rung's distance to its
 * hand-written LEAN target ({@code <rung>.lean.sql}).
 *
 * <pre>
 *   LadderRender &lt;output directory&gt; &lt;rung&gt;...   (the rungs core/BUILD.bazel declares, checked against the model's)
 * </pre>
 */
public final class LadderRender {

    static final String MODEL = """
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
            ###Connection
            RelationalDatabaseConnection l::DBDuckDB { store: l::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime l::RT { mappings: [l::M]; connections: [ l::DB: [ c0: l::DBDuckDB ] ]; }
            """;

    private static final Compiler.ParsedModule PARSED = Compiler.parseSources(
            List.of(new Compiler.ModelSource("ladder.pure", MODEL)));
    private static final ModelContext CTX = Compiler.buildModule(PARSED.model()).context();
    private static final PureTests.Discovery FOUND = PureTests.discover(PARSED.model(), Set.of());

    private LadderRender() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("usage: LadderRender <output directory> <rung>...");
        }
        Set<String> declared = new TreeSet<>(List.of(args).subList(1, args.length));
        Set<String> model = new TreeSet<>(rungs());
        if (!declared.equals(model)) {
            Set<String> missing = new TreeSet<>(model);
            missing.removeAll(declared);
            Set<String> extra = new TreeSet<>(declared);
            extra.removeAll(model);
            throw new IllegalStateException("core/BUILD.bazel's _LADDER_RUNGS must name exactly the ladder's rungs:"
                    + (missing.isEmpty() ? "" : " add " + missing) + (extra.isEmpty() ? "" : " remove " + extra));
        }
        Path out = Path.of(args[0]);
        for (String rung : declared) {
            Files.writeString(out.resolve(rung + ".current.sql"), current("l::" + rung), StandardCharsets.UTF_8);
        }
    }

    /** The rungs: every {@code <<test.Test>>} function of the model, by name, in order. */
    public static List<String> rungs() {
        List<String> out = new ArrayList<>();
        for (PureTests.TestCase t : FOUND.tests()) {
            out.add(t.fqn().substring(t.fqn().indexOf("::") + 2));
        }
        out.sort(null);
        return out;
    }

    /** One rung's current emission: its statements, {@code ;;}-separated. */
    static String current(String fqn) throws Exception {
        return String.join("\n;;\n", statementsOf(fqn)) + "\n";
    }

    /** Every statement the runner sent for one rung, trace comments stripped. */
    private static List<String> statementsOf(String fqn) throws Exception {
        PureTests.TestCase test = FOUND.tests().stream()
                .filter(t -> t.fqn().equals(fqn)).findFirst().orElseThrow();
        List<String> sent = new ArrayList<>();
        // the run's judge mode is the runner's (one mode per run, on its options)
        try (PureTestRunner runner = new PureTestRunner(CTX, "l::RT",
                () -> recording(freshDuck(), sent),
                List.of(), FOUND.setupsByPackage(), TestObserver.NONE,
                com.legend.ExecuteOptions.JudgeMode.DATABASE)) {
            PureTestRunner.Result r = runner.run(test);
            if (r.status() != PureTestRunner.Status.PASS) {
                throw new IllegalStateException(fqn + " does not pass in database mode: " + r.reason());
            }
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
                return Proxy.newProxyInstance(LadderRender.class.getClassLoader(),
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
                return Proxy.newProxyInstance(LadderRender.class.getClassLoader(),
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
        return (Connection) Proxy.newProxyInstance(LadderRender.class.getClassLoader(),
                new Class<?>[] {Connection.class}, h);
    }
}
