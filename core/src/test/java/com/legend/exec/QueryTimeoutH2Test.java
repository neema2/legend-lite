// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlannedSql;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ConnectionSpecification;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A CONNECTION'S DECLARED QUERY TIME-OUT IS THE STATEMENT'S (C5-J's batch, 4(b), P2, the user's ruling of 2026-11-16): a
 * connection that declares {@code queryTimeOutInSeconds} carries it in the plan's target, and exec sets it on every
 * statement it sends for that target ({@code Statement.setQueryTimeout}), as legend-engine's {@code SQLResult} does; a
 * connection that declares none sets none -- no default, as upstream's. Until then the declaration was parsed and
 * dropped: a query ran unbounded.
 */
class QueryTimeoutH2Test {

    private static ConnectionDefinition connection(Integer timeout) {
        return new ConnectionDefinition("s::Timed", "s::DB", DatabaseType.H2, new ConnectionSpecification.InMemory(),
                new AuthenticationSpec.TestAuth(), List.of(), timeout);
    }

    private static ExecutionPlan.Target target(Integer timeout) {
        return new ExecutionPlan.Target(new ExecutionPlan.Database.Declared(
                com.legend.planner.ConnectionPlans.of(connection(timeout)), null), List.of(), List.of(), null,
                TestBackends.settings(DatabaseType.H2), null);
    }

    /** {@code real}, every statement it makes recording the time-outs set on it into {@code set}. */
    private static Connection recording(Connection real, List<Integer> set) {
        return (Connection) Proxy.newProxyInstance(QueryTimeoutH2Test.class.getClassLoader(),
                new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                    Object made = invoke(real, method, args);
                    if (made instanceof Statement st) {
                        Class<?>[] kinds = made instanceof java.sql.PreparedStatement
                                ? new Class<?>[] {java.sql.PreparedStatement.class} : new Class<?>[] {Statement.class};
                        return Proxy.newProxyInstance(QueryTimeoutH2Test.class.getClassLoader(), kinds,
                                (p, m, a) -> {
                                    if (m.getName().equals("setQueryTimeout")) {
                                        set.add((Integer) a[0]);
                                    }
                                    return invoke(st, m, a);
                                });
                    }
                    return made;
                });
    }

    private static Object invoke(Object on, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(on, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    /** The time-outs set on the statements the plan's statement was sent with, on {@code target}. */
    private static List<Integer> sent(ExecutionPlan.Target target) throws Exception {
        List<Integer> set = new ArrayList<>();
        try (Connection real = TestBackends.of(DatabaseType.H2).open();
                com.legend.base.RunScope run = com.legend.base.RunScope.enter()) {
            Sessions.Session session = TestSessions.over(recording(real, set));
            Statements st = PlanRunner.opened(session, target);
            set.clear();   // the session's own checks, before the plan's statement
            st.execute(PlannedSql.fromPlan("SELECT 1"));
        }
        return set;
    }

    @Test
    void theDeclaredTimeOutIsTheStatements() throws Exception {
        assertEquals(List.of(7), sent(target(7)));
    }

    @Test
    void noDeclarationSetsNone() throws Exception {
        assertEquals(List.of(), sent(target(null)));
    }

    /** The plan carries it from the connection's text, and its JSON keeps it -- written only where it is declared. */
    @Test
    void thePlanCarriesItFromTheConnectionsText() {
        String model = """
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY ) )
                ###Pure
                Class s::Row { id: Integer[1]; }
                ###Mapping
                Mapping s::M ( s::Row: Relational { ~mainTable [s::DB] T id: [s::DB] T.ID } )
                ###Connection
                RelationalDatabaseConnection s::C
                {
                  store: s::DB;
                  type: H2;
                  specification: LocalH2 { };
                  auth: DefaultH2;
                  queryTimeOutInSeconds: 7;
                }
                ###Runtime
                Runtime s::R { mappings: [s::M]; connections: [s::DB: [env: s::C]]; }
                """;
        ExecutionPlan plan = com.legend.Compiler.query(com.legend.Compiler.compileModel(model),
                "|s::Row.all()->project(~[id: x | $x.id])").executionPlan("s::R");
        ExecutionPlan.Target target = plan.answerSql().target();
        assertTrue(target.database() instanceof ExecutionPlan.Database.Declared, target.toString());
        assertEquals(7, ((ExecutionPlan.Database.Declared) target.database()).connection().queryTimeOutInSeconds());
        String json = com.legend.executionplan.PlanJson.write(plan);
        assertTrue(json.contains("\"queryTimeOutInSeconds\":7"), json);
        ExecutionPlan read = com.legend.executionplan.PlanJson.read(json);
        assertEquals(7, ((ExecutionPlan.Database.Declared) read.answerSql().target().database()).connection()
                .queryTimeOutInSeconds());
        // undeclared: none in the plan, no key in its JSON
        ExecutionPlan bare = com.legend.Compiler.query(com.legend.Compiler.compileModel(
                model.replace("  queryTimeOutInSeconds: 7;\n", "")), "|s::Row.all()->project(~[id: x | $x.id])")
                .executionPlan("s::R");
        assertNull(((ExecutionPlan.Database.Declared) bare.answerSql().target().database()).connection()
                .queryTimeOutInSeconds());
        assertTrue(!com.legend.executionplan.PlanJson.write(bare).contains("queryTimeOutInSeconds"));
    }
}
