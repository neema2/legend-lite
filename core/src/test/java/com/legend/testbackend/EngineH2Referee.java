// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import com.legend.exec.H2Connector;
import com.legend.model.ConnectionDefinition.DatabaseType;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * THE REPLAY REFEREE'S DATABASE: legend-engine's own H2, 2.1.214 (its root POM's {@code h2.version}), standing in for
 * the engine's database when the referee replays the engine's golden SQL (the user, 2026-11-05: "Engine's 2.1.214,
 * isolated"). Lite's own H2 is 2.5.252 ({@link H2Backend}, register S141); this one is a file every test JVM and
 * corpus pass is handed by path ({@code legend.referee.h2}, never on a classpath) and loads through a class loader of
 * its own, which reads H2's classes from that file and every other class from the test's (the engine's extension
 * functions, {@link H2ExtensionFunctions}, are the test's own). DriverManager never hands this driver to lite: it gives
 * a caller only a driver whose class the caller's own class loader resolves to the same class. Its sessions carry the
 * engine's session settings and extension functions, and every lane's golden replays on one beside the lane's session
 * (the mirror), the H2 lane's too.
 */
final class EngineH2Referee implements RefereeBackend {

    static final EngineH2Referee INSTANCE = new EngineH2Referee();

    /** The system property naming the engine's H2 jar (a corpus pass's exec path; a test's {@code .rlocation}). */
    static final String JAR = "legend.referee.h2";

    private volatile Driver driver;

    private EngineH2Referee() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.H2;
    }

    /** The H2 lane's name: its rosters are the referee's database's ({@code rcorpus/h2-*}). */
    @Override
    public String name() {
        return "h2";
    }

    @Override
    public String toString() {
        return "h2 (the engine's 2.1.214, the referee's)";
    }

    @Override
    public boolean available() {
        return System.getProperty(JAR) != null || System.getProperty(JAR + ".rlocation") != null;
    }

    @Override
    public Connection openReferee(String name) throws SQLException {
        Properties credentials = new Properties();
        credentials.setProperty("user", "sa");
        credentials.setProperty("password", "");
        Connection referee = driver().connect("jdbc:h2:mem:" + name + H2Connector.SETTINGS, credentials);
        if (referee == null) {
            throw new SQLException("the engine's H2 driver refused its own in-memory URL");
        }
        // the engine's H2 test database carries its extension functions, and its goldens call them
        // (legend_h2_extension_*): every referee session does too
        try (Statement st = referee.createStatement()) {
            for (String alias : H2ExtensionFunctions.aliases()) {
                com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
                st.execute(alias);
            }
        } catch (SQLException e) {
            referee.close();
            throw e;
        }
        return referee;
    }

    /** A fresh referee session: what a test reads the referee's database through. */
    @Override
    public Connection open() throws SQLException {
        return openReferee("referee" + System.nanoTime());
    }

    @Override
    public boolean inProcess() {
        return true;
    }

    @Override
    public Connection openSystem() {
        throw new IllegalStateException("the replay referee's database holds no system store");
    }

    @Override
    public String specificationAndAuth() {
        throw new IllegalStateException("the replay referee's database is no runtime's: it replays the engine's text"
                + " alone");
    }

    /** The engine's H2 driver, loaded once through its own class loader. */
    private Driver driver() throws SQLException {
        Driver d = driver;
        if (d == null) {
            synchronized (this) {
                d = driver;
                if (d == null) {
                    d = load();
                    driver = d;
                }
            }
        }
        return d;
    }

    private static Driver load() throws SQLException {
        if (System.getProperty(JAR) == null && System.getProperty(JAR + ".rlocation") == null) {
            throw new SQLException("the replay referee's H2 (legend-engine's 2.1.214) is not given: -D" + JAR
                    + " (a corpus pass's) or -D" + JAR + ".rlocation (a test's) names its jar");
        }
        Path jar = com.legend.testing.ProgramPaths.file(JAR);
        try {
            URLClassLoader loader = new H2First(jar.toUri().toURL(), EngineH2Referee.class.getClassLoader());
            Driver d = (Driver) loader.loadClass("org.h2.Driver").getDeclaredConstructor().newInstance();
            String version = d.getMajorVersion() + "." + d.getMinorVersion();
            if (!"2.1".equals(version)) {
                throw new SQLException("the replay referee's H2 is " + version + ", not legend-engine's 2.1 (" + jar
                        + ")");
            }
            return d;
        } catch (MalformedURLException | ReflectiveOperationException e) {
            throw new SQLException("the replay referee's H2 could not be loaded from " + jar + ": " + e, e);
        }
    }

    /** A class loader that reads H2's classes ({@code org.h2.}) from the engine's jar, before its parent, and every
     *  other class from its parent: the referee's H2 and lite's never meet. */
    private static final class H2First extends URLClassLoader {

        static {
            ClassLoader.registerAsParallelCapable();
        }

        H2First(URL jar, ClassLoader parent) {
            super(new URL[] {jar}, parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!name.startsWith("org.h2.")) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> c = findLoadedClass(name);
                if (c == null) {
                    c = findClass(name);
                }
                if (resolve) {
                    resolveClass(c);
                }
                return c;
            }
        }
    }
}
