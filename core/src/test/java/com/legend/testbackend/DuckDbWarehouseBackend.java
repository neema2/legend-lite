// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import com.legend.model.ConnectionDefinition.DatabaseType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * DuckDB as the warehouse serves it (W1c, docs/WAREHOUSE_W1_DESIGN_2026_09_26.md): every connection a session on a
 * warehouse this backend starts, spoken to through its JDBC driver, its corpus workspaces catalogs of the warehouse's
 * one instance ({@link DuckDbWorkspaces}). The warehouse is its native binary (//warehouse:server_native), never a child
 * JVM of this one (Bazel workplan P3-19), named by {@code -Dlegend.warehouse.server}. Not one of
 * {@link TestBackends#all()} (it is no database type of its own): a lane names it ({@link TestBackends#named}).
 */
final class DuckDbWarehouseBackend implements TestBackend {

    static final DuckDbWarehouseBackend INSTANCE = new DuckDbWarehouseBackend();

    /** The warehouse's DuckDB session in UTC, as the platform's own (DuckDbBackend; the T-d2 audit, 2026-10-29). */
    @Override
    public java.util.List<String> sessionSetUp() {
        return java.util.List.of("SET timezone='UTC'");
    }

    private @com.legend.base.Nullable String url;

    private final DuckDbWorkspaces workspaces = new DuckDbWorkspaces(new DuckDbWorkspaces.Instance() {
        @Override
        public Connection root() throws SQLException {
            return DriverManager.getConnection(url());
        }

        @Override
        public Connection another(Connection root) throws SQLException {
            return DriverManager.getConnection(url());
        }
    });

    private DuckDbWarehouseBackend() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.DuckDB;
    }

    @Override
    public String name() {
        return "warehouse";
    }

    @Override
    public Connection open() throws SQLException {
        return DriverManager.getConnection(url());
    }

    @Override
    public boolean inProcess() {
        return false;
    }

    @Override
    public Connection openSystem() {
        throw new IllegalStateException("the warehouse lane keeps no system connection");
    }

    @Override
    public String specificationAndAuth() {
        throw new IllegalStateException("the warehouse lane declares no connection of its own: a corpus model declares"
                + " its stores' connections");
    }

    @Override
    public Connection openWorkspace() throws SQLException {
        return workspaces.open();
    }

    @Override
    public FixtureAside setFixtureAside(Connection workspace) throws SQLException {
        return workspaces.setAside(workspace, name());
    }

    @Override
    public String toString() {
        return name();
    }

    /** The warehouse's URL, starting it on first use. */
    private synchronized String url() throws SQLException {
        if (url == null) {
            String binary = System.getProperty("legend.warehouse.server");
            if (binary == null) {
                throw new IllegalStateException("the warehouse lane needs -Dlegend.warehouse.server: the warehouse's"
                        + " native binary (//warehouse:server_native)");
            }
            url = start(binary);
        }
        return url;
    }

    /** Starts the warehouse as a child process on a free port, with an empty data directory and one user; stopped when
     *  this JVM exits. Its DuckDB library is named by {@code -Dlegend.warehouse.library} where it has no runfiles to
     *  find it in (the host pass, a build action: spec/corpus.bzl; Bazel workplan P1-16); its data is kept where
     *  {@code -Dlegend.warehouse.data} names (its query history, to read after). */
    private static String start(String binary) throws SQLException {
        try {
            String keep = System.getProperty("legend.warehouse.data");
            java.nio.file.Path data = keep != null ? java.nio.file.Files.createDirectories(java.nio.file.Path.of(keep))
                    : java.nio.file.Files.createTempDirectory("rcorpus-warehouse");
            java.util.List<String> command = new java.util.ArrayList<>(java.util.List.of(binary,
                    "--port", "0", "--data", data.toString(), "--user", "rcorpus:rcorpus", "--owner", "rcorpus",
                    "--concurrency", "4"));
            String library = System.getProperty("legend.warehouse.library");
            if (library != null) {
                command.addAll(java.util.List.of("--duckdb-library", library));
            }
            Process p = new ProcessBuilder(command)
                    .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                    .start();
            Runtime.getRuntime().addShutdownHook(new Thread(p::destroy));
            java.io.BufferedReader err = new java.io.BufferedReader(
                    new java.io.InputStreamReader(p.getErrorStream(), java.nio.charset.StandardCharsets.UTF_8));
            java.util.regex.Pattern ready =
                    java.util.regex.Pattern.compile("warehouse listening on 127\\.0\\.0\\.1:(\\d+)");
            String line;
            while ((line = err.readLine()) != null) {
                System.err.println("[warehouse] " + line);
                java.util.regex.Matcher m = ready.matcher(line);
                if (m.find()) {
                    Thread drain = new Thread(() -> {
                        try {
                            String l;
                            while ((l = err.readLine()) != null) {
                                System.err.println("[warehouse] " + l);
                            }
                        } catch (java.io.IOException ignored) {
                            // the process ended
                        }
                    }, "warehouse-stderr");
                    drain.setDaemon(true);
                    drain.start();
                    return "jdbc:warehouse:http://127.0.0.1:" + m.group(1) + "/main?user=rcorpus&password=rcorpus";
                }
            }
            throw new SQLException("the warehouse exited before listening (exit " + p.waitFor() + ")");
        } catch (java.io.IOException e) {
            throw new SQLException("cannot start the warehouse from " + binary, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted starting the warehouse", e);
        }
    }
}
