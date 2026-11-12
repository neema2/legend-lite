// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

/**
 * A test's JDBC connection that records every statement sent on it, in order (2026-11-09, no mutable static state:
 * the product's process-wide statement observer, {@code StatementOrigin.observing}, is gone -- a test that compares the
 * statements two roads send records them on the connection it hands in). Every call is the connection's own; a
 * statement's text is recorded when it is executed (a prepared one's at each execution), and {@code unwrap} reaches
 * the driver beneath as before (the DuckDB bulk loader's appender).
 */
public final class RecordingConnection {

    private static final Set<String> EXECUTES = Set.of("execute", "executeQuery", "executeUpdate", "executeLargeUpdate",
            "addBatch");

    private RecordingConnection() {
    }

    /** {@code connection}, each statement sent on it appended to {@code into}. */
    public static Connection wrap(Connection connection, List<String> into) {
        return (Connection) Proxy.newProxyInstance(RecordingConnection.class.getClassLoader(),
                new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                    Object out = invoke(connection, method, args);
                    if (out instanceof PreparedStatement p && args != null && args.length > 0
                            && args[0] instanceof String sql) {
                        return recording(p, PreparedStatement.class, sql, into);
                    }
                    // a plain statement, whatever class the driver makes it of (DuckDB's createStatement answers a
                    // PreparedStatement: until 2026-11-12, C4, its executes went unrecorded, and every proof that read
                    // this list was blind to DuckDB's session statements, effects and load statements)
                    if (out instanceof Statement s && method.getName().equals("createStatement")) {
                        return recording(s, Statement.class, null, into);
                    }
                    return out;
                });
    }

    private static Object recording(Statement statement, Class<?> type, String prepared, List<String> into) {
        InvocationHandler handler = (proxy, method, args) -> {
            if (EXECUTES.contains(method.getName())) {
                String sql = args != null && args.length > 0 && args[0] instanceof String s ? s : prepared;
                if (sql != null) {
                    into.add(sql);
                }
            }
            return invoke(statement, method, args);
        };
        return Proxy.newProxyInstance(RecordingConnection.class.getClassLoader(), new Class<?>[] {type}, handler);
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
