package com.legend.testing;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * A REAL Postgres for a test JVM (docs/POSTGRES_DIALECT_HOMEWORK_2026_10_01.md, Q5; leg P2): the pinned
 * binaries Bazel unpacked ({@code @embedded_postgres}, tools/postgres/postgres.bzl), started once per JVM
 * on a free loopback port, its cluster in the test's own temporary directory, and stopped when the JVM
 * exits. No Docker and no jar: a test names the binaries' root with
 * {@code -Dembedded.postgres.root=$(rlocationpath @embedded_postgres//:pg/PG_ROOT)} and asks for
 * {@link #shared()}.
 *
 * <p>The cluster is made for determinism, not durability: the C locale (strings compare by bytes, as
 * DuckDB's do), UTF-8, the session zone UTC, trust authentication on 127.0.0.1 only, no Unix socket (a
 * macOS socket path is capped at ~104 bytes), no fsync.
 *
 * <p>How it runs (Bazel workplan P3-20, A16): on macOS and Linux {@code postgres -D <cluster>} is this JVM's own
 * child, in the test's process group, so a test that times out takes it down with it (Bazel kills the group), and
 * the JVM's exit stops it ({@code destroy}: SIGTERM, a smart shutdown). {@code pg_ctl} would have called
 * {@code setsid()} and left the group, so a killed test left a server behind. On Windows it runs through
 * {@code pg_ctl}, which is how Postgres runs under an administrator account (the runners), where {@code postgres}
 * itself refuses. A port taken between choosing it and binding it is retried, three times, each with a new port.
 * The cluster lives in the test's TEST_TMPDIR only; {@code initdb} refuses root, said before it runs.
 */
public final class EmbeddedPostgres {

    /** The superuser {@code initdb} makes; trust authentication, so no password. */
    public static final String USER = "postgres";

    private static EmbeddedPostgres shared;
    /** Why the one start failed: every later {@link #shared()} says so instead of starting again. */
    private static IllegalStateException failed;

    private static final boolean WINDOWS = System.getProperty("os.name").toLowerCase(Locale.ROOT).startsWith("windows");

    private final Path bin;
    private final Path data;
    private int port;
    /** The server, this JVM's child (macOS, Linux); null where pg_ctl runs it (Windows). */
    private Process server;

    private EmbeddedPostgres(Path bin, Path data) {
        this.bin = bin;
        this.data = data;
    }

    /** This JVM's server, started on first use and stopped at exit. */
    public static synchronized EmbeddedPostgres shared() {
        if (failed != null) {
            throw failed;
        }
        if (shared == null) {
            try {
                shared = start();
            } catch (IllegalStateException e) {
                failed = e;
                throw e;
            }
            EmbeddedPostgres started = shared;
            Runtime.getRuntime().addShutdownHook(new Thread(started::stop, "embedded-postgres-stop"));
        }
        return shared;
    }

    /** {@code jdbc:postgresql://127.0.0.1:<port>/<database>?user=postgres}. */
    public String jdbcUrl(String database) {
        return "jdbc:postgresql://127.0.0.1:" + port + "/" + database + "?user=" + USER;
    }

    /** The libpq connection string, for a client that is not JDBC (DuckDB's postgres extension). */
    public String dsn(String database) {
        return "host=127.0.0.1 port=" + port + " dbname=" + database + " user=" + USER;
    }

    public int port() {
        return port;
    }

    private static EmbeddedPostgres start() {
        String marker = System.getProperty("embedded.postgres.root");
        if (marker == null) {
            throw new IllegalStateException("no -Dembedded.postgres.root: give the test"
                    + " data = [\"@embedded_postgres//:postgres\", \"@embedded_postgres//:pg/PG_ROOT\"] and"
                    + " jvm_flags = [\"-Dembedded.postgres.root=$(rlocationpath @embedded_postgres//:pg/PG_ROOT)\"]");
        }
        if ("root".equals(System.getProperty("user.name"))) {
            throw new IllegalStateException("initdb refuses root: run the CI job as a non-root user");
        }
        String tmp = System.getenv("TEST_TMPDIR");
        if (tmp == null) {
            throw new IllegalStateException("no TEST_TMPDIR: the embedded Postgres runs under bazel test, its cluster in"
                    + " the test's own temporary directory");
        }
        try {
            // the install's real directory: postgres finds its lib/ and share/ beside its own binary
            Path root = Runfile.of(marker).toRealPath().getParent();
            Path data = Files.createTempDirectory(Path.of(tmp), "pg");
            Path cluster = data.resolve("cluster");
            EmbeddedPostgres pg = new EmbeddedPostgres(root.resolve("bin"), data);
            pg.run(List.of(pg.tool("initdb"), "-D", cluster.toString(), "-U", USER, "-A", "trust", "-E", "UTF8",
                    "--no-locale", "--no-sync"));
            // the settings go in the cluster's own configuration, never through pg_ctl's -o: that string
            // reaches postgres through a shell, and cmd.exe keeps quotes (unix_socket_directories=''
            // became the directory '' on Windows, CI 2026-10-02)
            Files.writeString(cluster.resolve("postgresql.conf"), String.join("\n", "",
                    "listen_addresses = '127.0.0.1'",
                    "unix_socket_directories = ''",
                    "fsync = off",
                    "TimeZone = 'UTC'",
                    "max_connections = 200", ""), StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.APPEND);
            IllegalStateException last = null;
            for (int attempt = 1; attempt <= 3; attempt++) {
                try (ServerSocket s = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
                    pg.port = s.getLocalPort();
                }
                // the last `port` line wins: each attempt appends its own
                Files.writeString(cluster.resolve("postgresql.conf"), "port = " + pg.port + "\n", StandardCharsets.UTF_8,
                        java.nio.file.StandardOpenOption.APPEND);
                try {
                    pg.startServer(cluster);
                    return pg;
                } catch (IllegalStateException e) {
                    last = e;   // the port was taken before the server bound it: another, up to three
                }
            }
            throw last;
        } catch (IOException e) {
            throw new IllegalStateException("the embedded Postgres did not start: " + e.getMessage(), e);
        }
    }

    /** Starts the server on {@link #port}, returning once it accepts connections; a server that exits first fails. */
    private void startServer(Path cluster) throws IOException {
        Path log = data.resolve("postgres.log");
        if (WINDOWS) {
            run(List.of(tool("pg_ctl"), "-D", cluster.toString(), "-l", log.toString(), "-w", "-t", "120", "start"));
            return;
        }
        server = new ProcessBuilder(tool("postgres"), "-D", cluster.toString()).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile())).start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (System.nanoTime() < deadline) {
            if (!server.isAlive()) {
                throw new IllegalStateException("postgres exited " + server.exitValue() + " before accepting connections:\n"
                        + Files.readString(log, StandardCharsets.UTF_8));
            }
            try (java.net.Socket socket = new java.net.Socket(java.net.InetAddress.getLoopbackAddress(), port)) {
                return;
            } catch (IOException notYet) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted waiting for postgres", e);
                }
            }
        }
        server.destroyForcibly();
        throw new IllegalStateException("postgres did not accept connections in 120 s:\n"
                + Files.readString(log, StandardCharsets.UTF_8));
    }

    private void stop() {
        if (server != null) {
            server.destroy();   // SIGTERM: a smart shutdown
            try {
                if (!server.waitFor(30, TimeUnit.SECONDS)) {
                    server.destroyForcibly();
                }
            } catch (InterruptedException e) {
                server.destroyForcibly();
                Thread.currentThread().interrupt();
            }
            return;
        }
        try {
            run(List.of(tool("pg_ctl"), "-D", data.resolve("cluster").toString(), "-m", "immediate", "-w", "stop"));
        } catch (IOException | IllegalStateException e) {
            System.err.println("the embedded Postgres did not stop cleanly: " + e.getMessage());
        }
    }

    private String tool(String name) {
        return bin.resolve(WINDOWS ? name + ".exe" : name).toString();
    }

    /** Runs one of the server's tools to completion; a failure says what it printed (and the server's log). */
    private void run(List<String> command) throws IOException {
        Path out = data.resolve("tool.out");
        Process p = new ProcessBuilder(new ArrayList<>(command)).redirectErrorStream(true)
                .redirectOutput(out.toFile()).start();
        try {
            if (!p.waitFor(180, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                throw new IllegalStateException(String.join(" ", command) + " did not finish in 180 s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted running " + command.get(0), e);
        }
        if (p.exitValue() != 0) {
            Path log = data.resolve("postgres.log");
            throw new IllegalStateException(String.join(" ", command) + " exited " + p.exitValue() + ":\n"
                    + Files.readString(out, StandardCharsets.UTF_8)
                    + (Files.exists(log) ? "\nthe server's log:\n" + Files.readString(log, StandardCharsets.UTF_8) : ""));
        }
    }
}
