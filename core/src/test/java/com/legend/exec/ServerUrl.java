// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;

/**
 * A real in-process connection whose driver reports another URL ({@code DatabaseMetaData.getURL}): a session as a driver
 * that reaches its database on a server reports it -- the warehouse's ({@code jdbc:warehouse:http://...}), an H2
 * server's ({@code jdbc:h2:tcp://...}) -- for the one fact read off a session's URL,
 * {@link DatabaseConnector#readsRunnerFiles} (track Q2, 2026-11-19). Everything else is the real connection's.
 */
final class ServerUrl {

    private ServerUrl() {
    }

    static Connection reportedAt(Connection real, String url) {
        InvocationHandler metadata = (proxy, m, args) -> m.getName().equals("getURL") ? url : forward(real.getMetaData(), m, args);
        InvocationHandler connection = (proxy, m, args) -> m.getName().equals("getMetaData")
                ? Proxy.newProxyInstance(ServerUrl.class.getClassLoader(), new Class<?>[] {DatabaseMetaData.class}, metadata)
                : forward(real, m, args);
        return (Connection) Proxy.newProxyInstance(ServerUrl.class.getClassLoader(), new Class<?>[] {Connection.class},
                connection);
    }

    private static Object forward(Object target, java.lang.reflect.Method m, Object[] args) throws Throwable {
        try {
            return m.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
