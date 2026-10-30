package com.legend.testing;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A REAL ClickHouse for a test JVM (docs/CLOUD_BACKENDS_PCT_2026_10_11.md §19), as {@link EmbeddedTrino} is a Trino: the
 * pinned static build Bazel unpacked ({@code @embedded_clickhouse}, tools/clickhouse/clickhouse.bzl), started once per
 * JVM on free loopback ports, its data in the test's own temporary directory, and stopped when the JVM exits. No Docker:
 * a test names the install's root with
 * {@code -Dembedded.clickhouse.root=$(rlocationpath @embedded_clickhouse//:clickhouse/CLICKHOUSE_ROOT)} and asks for
 * {@link #shared()}.
 *
 * <p>The server is configured by a file of its own (measured 2026-10-12): its HTTP port (the JDBC driver's) and a native
 * port, on 127.0.0.1 only, the user {@code default} with no password, warnings logged to the data directory. It is this
 * JVM's own child: a test that times out takes it down with it, and the JVM's exit stops it.
 */
public final class EmbeddedClickHouse {

    private static EmbeddedClickHouse shared;
    /** Why the one start failed: every later {@link #shared()} says so instead of starting again. */
    private static IllegalStateException failed;

    private final Path root;
    private final Path data;
    private int port;
    private int nativePort;
    private Process server;

    private EmbeddedClickHouse(Path root, Path data) {
        this.root = root;
        this.data = data;
    }

    /** This JVM's server, started on first use and stopped at exit. */
    public static synchronized EmbeddedClickHouse shared() {
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
            EmbeddedClickHouse started = shared;
            Runtime.getRuntime().addShutdownHook(new Thread(started::stop, "embedded-clickhouse-stop"));
        } else if (!shared.server.isAlive()) {
            // a statement that killed the server (as the MariaDB lane measured) fails alone: it starts again
            System.err.println("[embedded-clickhouse] the server died (exit " + shared.server.exitValue()
                    + "); restarting it on port " + shared.port + ". Its log's tail:\n" + shared.logTail());
            try {
                shared.startServer();
            } catch (IOException e) {
                throw new IllegalStateException("the embedded ClickHouse did not restart: " + e.getMessage(), e);
            }
        }
        return shared;
    }

    /**
     * {@code jdbc:clickhouse://127.0.0.1:<http port>/<database>?user=default}: the driver speaks HTTP. The server's
     * {@code max_query_size} (256 KiB by default) and its AST limits are raised on the connection, since a statement's
     * own SETTINGS clause is read only after the limit has refused it (measured 25.8: a query spelling many Floats'
     * text). The driver passes a {@code clickhouse_setting_} property as an HTTP parameter, a server setting. A
     * product ClickHouse connector (the dialect-leak plan's P5) carries the same.
     */
    public String jdbcUrl(String database) {
        return "jdbc:clickhouse://127.0.0.1:" + port + "/" + database
                + "?user=default&clickhouse_setting_max_query_size=16777216&clickhouse_setting_max_ast_elements=1000000"
                + "&clickhouse_setting_max_expanded_ast_elements=10000000";
    }

    /**
     * {@link #jdbcUrl} in a session of its own: a new {@code session_id} each call, which the driver passes as an HTTP
     * parameter like a setting, so the connection's temporary tables live as long as the session (an hour idle at
     * most, the server's default {@code max_session_timeout}) and no other connection sees them. The system
     * database's connection is one (the root's SystemDatabase, D72). A session runs one statement at a time.
     */
    public String sessionJdbcUrl(String database) {
        return jdbcUrl(database) + "&clickhouse_setting_session_id=legendlite_" + java.util.UUID.randomUUID()
                + "&clickhouse_setting_session_timeout=3600";
    }

    /** The HTTP port (the driver's). */
    public int port() {
        return port;
    }

    private String logTail() {
        try {
            String log = read(data.resolve("err.log")) + read(data.resolve("stdout.log"));
            return log.substring(Math.max(0, log.length() - 3000));
        } catch (IOException e) {
            return "(its log could not be read: " + e.getMessage() + ")";
        }
    }

    private static EmbeddedClickHouse start() {
        String marker = System.getProperty("embedded.clickhouse.root");
        if (marker == null) {
            throw new IllegalStateException("no -Dembedded.clickhouse.root: give the test"
                    + " data = [\"@embedded_clickhouse//:clickhouse\", \"@embedded_clickhouse//:clickhouse/CLICKHOUSE_ROOT\"]"
                    + " and jvm_flags = [\"-Dembedded.clickhouse.root=$(rlocationpath"
                    + " @embedded_clickhouse//:clickhouse/CLICKHOUSE_ROOT)\"]");
        }
        String tmp = System.getenv("TEST_TMPDIR");
        if (tmp == null) {
            throw new IllegalStateException("no TEST_TMPDIR: the embedded ClickHouse runs under bazel test, its data in"
                    + " the test's own temporary directory");
        }
        try {
            Path root = Runfile.of(marker).toRealPath().getParent();
            Path data = Files.createTempDirectory(Path.of(tmp), "clickhouse");
            EmbeddedClickHouse db = new EmbeddedClickHouse(root, data);
            IllegalStateException last = null;
            for (int attempt = 1; attempt <= 3; attempt++) {
                db.port = freePort();
                db.nativePort = freePort();
                try {
                    db.startServer();
                    return db;
                } catch (IllegalStateException e) {
                    if (!(read(data.resolve("err.log")) + read(data.resolve("stdout.log"))).contains("Address already in use")) {
                        throw e;   // not a taken port: no retry hides it
                    }
                    last = e;
                }
            }
            throw last;
        } catch (IOException e) {
            throw new IllegalStateException("the embedded ClickHouse did not start: " + e.getMessage(), e);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
            return s.getLocalPort();
        }
    }

    /** Starts the server, returning once its HTTP port answers a query. A server that exits first fails. */
    private void startServer() throws IOException {
        for (String d : List.of("db", "tmp", "user_files")) {
            Files.createDirectories(data.resolve(d));
        }
        Path config = data.resolve("config.xml");
        Files.writeString(config, String.format(java.util.Locale.ROOT, """
                <clickhouse>
                  <logger><level>warning</level><log>%s</log><errorlog>%s</errorlog><console>0</console></logger>
                  <http_port>%d</http_port>
                  <tcp_port>%d</tcp_port>
                  <listen_host>127.0.0.1</listen_host>
                  <path>%s/</path>
                  <tmp_path>%s/</tmp_path>
                  <user_files_path>%s/</user_files_path>
                  <mark_cache_size>268435456</mark_cache_size>
                  <users>
                    <default>
                      <password></password>
                      <networks><ip>127.0.0.1</ip></networks>
                      <profile>default</profile>
                      <quota>default</quota>
                    </default>
                  </users>
                  <profiles><default/></profiles>
                  <quotas><default/></quotas>
                </clickhouse>
                """, data.resolve("server.log"), data.resolve("err.log"), port, nativePort, data.resolve("db"),
                data.resolve("tmp"), data.resolve("user_files")), StandardCharsets.UTF_8);
        ProcessBuilder pb = new ProcessBuilder(root.resolve("bin/clickhouse").toString(), "server",
                "--config-file=" + config).directory(data.toFile()).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(data.resolve("stdout.log").toFile()));
        server = pb.start();
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        HttpRequest ping = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/?query=SELECT%201"))
                .timeout(Duration.ofSeconds(2)).build();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (System.nanoTime() < deadline) {
            if (!server.isAlive()) {
                throw new IllegalStateException("the ClickHouse server exited " + server.exitValue() + " before it"
                        + " started:\n" + logTail());
            }
            try {
                HttpResponse<String> r = http.send(ping, HttpResponse.BodyHandlers.ofString());
                if (r.statusCode() == 200) {
                    return;
                }
            } catch (IOException notYet) {
                // not listening yet
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted waiting for the ClickHouse server", e);
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted waiting for the ClickHouse server", e);
            }
        }
        stop();
        throw new IllegalStateException("the ClickHouse server did not start in 120 s:\n" + logTail());
    }

    private static String read(Path log) throws IOException {
        return Files.exists(log) ? Files.readString(log, StandardCharsets.UTF_8) : "";
    }

    private void stop() {
        if (server != null && server.isAlive()) {
            server.destroy();
            try {
                if (!server.waitFor(30, TimeUnit.SECONDS)) {
                    server.destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                server.destroyForcibly();
            }
        }
    }
}
