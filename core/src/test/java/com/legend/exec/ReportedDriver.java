// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;

/**
 * A real in-process connection whose driver reports another name ({@code DatabaseMetaData.getDriverName}): a session as
 * another driver of the same product reports it -- the warehouse's, which reports DuckDB and reaches it over HTTP -- for
 * the one fact read off a session's driver, {@link DatabaseConnector#readsRunnerFiles} (track Q2, 2026-11-19).
 * Everything else is the real connection's.
 */
final class ReportedDriver {

    private ReportedDriver() {
    }

    static Connection as(Connection real, String driver) {
        InvocationHandler metadata = (proxy, m, args) -> m.getName().equals("getDriverName") ? driver
                : forward(real.getMetaData(), m, args);
        InvocationHandler connection = (proxy, m, args) -> m.getName().equals("getMetaData")
                ? Proxy.newProxyInstance(ReportedDriver.class.getClassLoader(), new Class<?>[] {DatabaseMetaData.class},
                        metadata)
                : forward(real, m, args);
        return (Connection) Proxy.newProxyInstance(ReportedDriver.class.getClassLoader(),
                new Class<?>[] {Connection.class}, connection);
    }

    private static Object forward(Object target, java.lang.reflect.Method m, Object[] args) throws Throwable {
        try {
            return m.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
