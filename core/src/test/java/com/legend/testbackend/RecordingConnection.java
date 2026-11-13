// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

/**
 * A test's JDBC connection that records every statement sent on it, in order (2026-11-09, no mutable static state:
 * the product's process-wide statement observer, {@code StatementOrigin.observing}, is gone -- a test that compares the
 * statements two roads send records them on the connection it hands in). Every call is the connection's own; a
 * statement's text is recorded when it is executed (a prepared one's at each execution). No way leads past it to the
 * driver beneath, where a statement would go unrecorded (the third re-audit's SHOULD-FIX 10, 2026-11-12: until then
 * {@code unwrap} reached it, for the DuckDB bulk loader's appender, which went with A2): {@code unwrap} is refused by
 * name and {@code isWrapperFor} answers no, on the connection, its statements and its metadata; and a statement's or
 * the metadata's {@code getConnection} answers this recording connection.
 */
public final class RecordingConnection {

    private static final Set<String> EXECUTES = Set.of("execute", "executeQuery", "executeUpdate", "executeLargeUpdate",
            "addBatch");

    private RecordingConnection() {
    }

    /** {@code connection}, each statement sent on it appended to {@code into}. */
    public static Connection wrap(Connection connection, List<String> into) {
        Connection[] recording = {null};
        recording[0] = (Connection) Proxy.newProxyInstance(RecordingConnection.class.getClassLoader(),
                new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                    Object refused = refusedPastIt(method);
                    if (refused != null) {
                        return refused;
                    }
                    Object out = invoke(connection, method, args);
                    if (out instanceof PreparedStatement p && args != null && args.length > 0
                            && args[0] instanceof String sql) {
                        return recording(p, PreparedStatement.class, sql, into, recording[0]);
                    }
                    // a plain statement, whatever class the driver makes it of (DuckDB's createStatement answers a
                    // PreparedStatement: until 2026-11-12, C4, its executes went unrecorded, and every proof that read
                    // this list was blind to DuckDB's session statements, effects and load statements)
                    if (out instanceof Statement s && method.getName().equals("createStatement")) {
                        return recording(s, Statement.class, null, into, recording[0]);
                    }
                    if (out instanceof DatabaseMetaData m) {
                        return passing(m, DatabaseMetaData.class, recording[0]);
                    }
                    return out;
                });
        return recording[0];
    }

    /** What a call that would lead past the recording answers: {@code unwrap} refused by name, {@code isWrapperFor}
     *  no; null for any other call. */
    private static Object refusedPastIt(Method method) throws SQLException {
        if (method.getName().equals("unwrap")) {
            throw new SQLException("a recording connection refuses unwrap: a statement sent on the driver beneath it"
                    + " would go unrecorded");
        }
        if (method.getName().equals("isWrapperFor")) {
            return false;
        }
        return null;
    }

    /** {@code target}, its {@code getConnection} answering {@code recording}, never the driver's connection. */
    private static Object passing(Object target, Class<?> type, Connection recording) {
        InvocationHandler handler = (proxy, method, args) -> {
            Object refused = refusedPastIt(method);
            if (refused != null) {
                return refused;
            }
            if (method.getName().equals("getConnection") && (args == null || args.length == 0)) {
                return recording;
            }
            return invoke(target, method, args);
        };
        return Proxy.newProxyInstance(RecordingConnection.class.getClassLoader(), new Class<?>[] {type}, handler);
    }

    private static Object recording(Statement statement, Class<?> type, String prepared, List<String> into,
            Connection recordingConnection) {
        InvocationHandler handler = (proxy, method, args) -> {
            Object refused = refusedPastIt(method);
            if (refused != null) {
                return refused;
            }
            if (method.getName().equals("getConnection") && (args == null || args.length == 0)) {
                return recordingConnection;
            }
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
