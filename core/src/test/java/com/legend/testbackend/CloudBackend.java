// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * A cloud warehouse on a real account (docs/CLOUD_BACKENDS_PCT_2026_10_11.md; moved from the PCT harness's
 * {@code CloudLane}, the dialect program's Phase 6, 2026-10-16). Its coordinates and credentials come from the
 * properties file the run names in {@code LEGENDLITE_PCT_CLOUD_PROPERTIES}, written from the account's secret store
 * before the run and never from the repository. For each cloud {@code c} (its {@link #name}):
 *
 * <ul>
 *   <li>{@code c.url}: the JDBC URL, credentials left out;</li>
 *   <li>{@code c.property.<name>}: each driver property the session opens with (the credentials among them);</li>
 *   <li>the coordinates the runtime declares, which are not secret (each cloud's {@link #specificationAndAuth});</li>
 *   <li>{@code c.system.property.<name>}: each driver property the SYSTEM connection adds to the session's (the
 *       platform's system database for the engine, {@link #openSystem}).</li>
 * </ul>
 *
 * A missing file or key is refused by name, never defaulted. One session per cloud per test JVM, opened at the first
 * test and held for the suite (a login costs a second or more): the expressions are storeless, so no test leaves state
 * behind.
 */
abstract class CloudBackend implements TestBackend {

    /** The variable naming the properties file. */
    static final String PROPERTIES = "LEGENDLITE_PCT_CLOUD_PROPERTIES";

    private static Properties properties;
    /** Each cloud's one session and its one failed login, by its name (the audit of the branch, 2026-10-16: one field
     *  for every cloud was safe while a JVM ran one lane, not behind a registry of every backend). */
    private static final java.util.Map<String, Connection> SESSIONS = new java.util.HashMap<>();
    private static final java.util.Map<String, IllegalStateException> FAILED = new java.util.HashMap<>();

    /** The runtime's connection declaration: the coordinates as they are, the credential by reference. */
    abstract String specificationAndAuth(Properties p);

    /** {@code name()}'s property {@code key}, as a Pure string literal. */
    final String quoted(Properties p, String key) {
        return "'" + required(p, name() + "." + key).replace("\\", "\\\\").replace("'", "\\'") + "'";
    }

    /** Its name: what a parameterised test shows. */
    @Override
    public final String toString() {
        return name();
    }

    @Override
    public final boolean inProcess() {
        return false;
    }

    @Override
    public final String specificationAndAuth() {
        synchronized (CloudBackend.class) {
            return specificationAndAuth(properties());
        }
    }

    /** The suite's one session: closing it is a no-op; the JVM's exit closes it. */
    @Override
    public final Connection open() throws SQLException {
        synchronized (CloudBackend.class) {
            IllegalStateException failed = FAILED.get(name());
            if (failed != null) {
                throw failed;
            }
            Connection session = SESSIONS.get(name());
            if (session == null || session.isClosed()) {
                Properties p = properties();
                Properties driver = driverProperties(p, name() + ".property.", new Properties());
                Connection opened;
                try {
                    opened = DriverManager.getConnection(required(p, name() + ".url"), driver);
                } catch (SQLException | RuntimeException e) {
                    // one login attempt per suite: every later test fails with the first one's reason, at once
                    failed = new IllegalStateException("opening the " + name() + " session failed: " + e.getMessage(),
                            e);
                    FAILED.put(name(), failed);
                    throw failed;
                }
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        opened.close();
                    } catch (SQLException e) {
                        System.err.println("[LegendLite PCT] closing the " + name() + " session: " + e.getMessage());
                    }
                }));
                session = opened;
                SESSIONS.put(name(), opened);
            }
            return unclosable(session);
        }
    }

    /** {@inheritDoc} {@link #open}'s: the suite's one session. */
    @Override
    public final Connection held() throws SQLException {
        return open();
    }

    /** {@code held}, its {@code close()} a no-op. */
    private static Connection unclosable(Connection held) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                    if (method.getName().equals("close") && method.getParameterCount() == 0) {
                        return null;
                    }
                    try {
                        return method.invoke(held, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    /**
     * A NEW session for the platform's system database (closing it closes it, and the server drops its temporary
     * tables). This harness gives the system connection the suite's own account and credentials -- its choice: a host
     * configures its system connection apart -- with the system's own driver properties,
     * {@code c.system.property.<name>}, added.
     */
    @Override
    public final Connection openSystem() throws SQLException {
        synchronized (CloudBackend.class) {
            Properties p = properties();
            Properties driver = driverProperties(p, name() + ".system.property.",
                    driverProperties(p, name() + ".property.", new Properties()));
            return DriverManager.getConnection(required(p, name() + ".url"), driver);
        }
    }

    /** {@code into} with each {@code prefix<name>} of {@code p} set as the driver property {@code <name>}. */
    private static Properties driverProperties(Properties p, String prefix, Properties into) {
        for (String name : p.stringPropertyNames()) {
            if (name.startsWith(prefix)) {
                into.setProperty(name.substring(prefix.length()), p.getProperty(name));
            }
        }
        return into;
    }

    private static Properties properties() {
        if (properties == null) {
            String file = System.getenv(PROPERTIES);
            if (file == null || file.isEmpty()) {
                throw new IllegalStateException("a cloud PCT lane needs " + PROPERTIES + ": the properties file with"
                        + " the account's coordinates and credentials (docs/CLOUD_BACKENDS_PCT_2026_10_11.md)");
            }
            Properties p = new Properties();
            try (InputStream in = Files.newInputStream(Path.of(file))) {
                p.load(in);
            } catch (IOException e) {
                throw new UncheckedIOException(PROPERTIES + "=" + file + " cannot be read", e);
            }
            properties = p;
        }
        return properties;
    }

    private static String required(Properties p, String name) {
        String value = p.getProperty(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException(PROPERTIES + " has no " + name
                    + " (docs/CLOUD_BACKENDS_PCT_2026_10_11.md)");
        }
        return value;
    }
}
