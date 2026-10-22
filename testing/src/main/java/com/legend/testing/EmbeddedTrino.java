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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A REAL Trino for a test JVM (docs/CLOUD_BACKENDS_PCT_2026_10_11.md §18), as {@link EmbeddedMySql} is a MySQL: the
 * pinned server Bazel unpacked ({@code @embedded_trino}, tools/trino/trino.bzl), its coordinator started once per JVM on
 * a free loopback port, scheduling work on itself, with the memory connector as its one catalog ({@code memory}), and
 * stopped when the JVM exits. No Docker: a test names the install's root with
 * {@code -Dembedded.trino.root=$(rlocationpath @embedded_trino//:trino/TRINO_ROOT)} and asks for {@link #shared()}.
 *
 * <p>The coordinator runs on THIS JVM's Java (Trino 483 is built for Java 25, the toolchain's), as the JVM's own child:
 * a test that times out takes it down with it, and the JVM's exit stops it. Its flags are the ones Trino's launcher
 * would pass (measured 2026-10-12): the node's environment, id, data directory and address (bound to 127.0.0.1: Trino 483
 * names no HTTP host, {@code node.bind-ip} binds it) as system properties, the Vector API module (the server refuses
 * to start without it), a 2 GB heap.
 */
public final class EmbeddedTrino {

    /** The user every session names: Trino's coordinator authenticates no one on a plain HTTP port. */
    public static final String USER = "legend";

    private static EmbeddedTrino shared;
    /** Why the one start failed: every later {@link #shared()} says so instead of starting again. */
    private static IllegalStateException failed;

    private final Path root;
    private final Path data;
    private int port;
    private Process server;

    private EmbeddedTrino(Path root, Path data) {
        this.root = root;
        this.data = data;
    }

    /** This JVM's coordinator, started on first use and stopped at exit. */
    public static synchronized EmbeddedTrino shared() {
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
            EmbeddedTrino started = shared;
            Runtime.getRuntime().addShutdownHook(new Thread(started::stop, "embedded-trino-stop"));
        } else if (!shared.server.isAlive()) {
            // a statement that killed the server (as the MariaDB lane measured) fails alone: it starts again
            System.err.println("[embedded-trino] the coordinator died (exit " + shared.server.exitValue()
                    + "); restarting it on port " + shared.port + ". Its log's tail:\n" + shared.logTail());
            try {
                shared.startServer();
            } catch (IOException e) {
                throw new IllegalStateException("the embedded Trino did not restart: " + e.getMessage(), e);
            }
        }
        return shared;
    }

    /** {@code jdbc:trino://127.0.0.1:<port>/memory/<schema>?user=legend&timezone=UTC&explicitPrepare=false}: the
     *  session zone UTC, and a prepared statement sent as one EXECUTE IMMEDIATE (step 9, 2026-10-19, as the platform's
     *  own Trino opening does: through PREPARE the 689 KB statement of format('%s %f', ['fox', 1.5]) filled this
     *  coordinator's 2 GB heap until it died, measured; as EXECUTE IMMEDIATE it answers in 3 to 14 s). */
    public String jdbcUrl(String schema) {
        return "jdbc:trino://127.0.0.1:" + port + "/memory/" + schema + "?user=" + USER
                + "&timezone=UTC&explicitPrepare=false";
    }

    public int port() {
        return port;
    }

    private String logTail() {
        try {
            String log = read(data.resolve("server.log"));
            return log.substring(Math.max(0, log.length() - 3000));
        } catch (IOException e) {
            return "(its log could not be read: " + e.getMessage() + ")";
        }
    }

    private static EmbeddedTrino start() {
        String marker = System.getProperty("embedded.trino.root");
        if (marker == null) {
            throw new IllegalStateException("no -Dembedded.trino.root: give the test"
                    + " data = [\"@embedded_trino//:trino\", \"@embedded_trino//:trino/TRINO_ROOT\"] and"
                    + " jvm_flags = [\"-Dembedded.trino.root=$(rlocationpath @embedded_trino//:trino/TRINO_ROOT)\"]");
        }
        String tmp = System.getenv("TEST_TMPDIR");
        if (tmp == null) {
            throw new IllegalStateException("no TEST_TMPDIR: the embedded Trino runs under bazel test, its data in the"
                    + " test's own temporary directory");
        }
        try {
            Path root = Runfile.of(marker).toRealPath().getParent();
            Path data = Files.createTempDirectory(Path.of(tmp), "trino");
            Files.createDirectories(data.resolve("etc/catalog"));
            Files.writeString(data.resolve("etc/catalog/memory.properties"),
                    "connector.name=memory\nmemory.max-data-per-node=512MB\n", StandardCharsets.UTF_8);
            EmbeddedTrino db = new EmbeddedTrino(root, data);
            IllegalStateException last = null;
            for (int attempt = 1; attempt <= 3; attempt++) {
                try (ServerSocket s = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
                    db.port = s.getLocalPort();
                }
                try {
                    db.startServer();
                    return db;
                } catch (IllegalStateException e) {
                    if (!read(data.resolve("server.log")).contains("Address already in use")) {
                        throw e;   // not a taken port: no retry hides it
                    }
                    last = e;
                }
            }
            throw last;
        } catch (IOException e) {
            throw new IllegalStateException("the embedded Trino did not start: " + e.getMessage(), e);
        }
    }

    /** Starts the coordinator on {@link #port}, returning once {@code /v1/info} says it has started. A server that
     *  exits first fails. */
    private void startServer() throws IOException {
        Path log = data.resolve("server.log");
        Files.deleteIfExists(log);
        Path config = data.resolve("etc/config.properties");
        Files.writeString(config, "coordinator=true\n"
                + "node-scheduler.include-coordinator=true\n"
                + "http-server.http.port=" + port + "\n"
                + "discovery.uri=http://127.0.0.1:" + port + "\n"
                + "query.max-memory=1GB\n"
                + "query.max-memory-per-node=1GB\n"
                // a finished query's info (its plan included) is kept 15 minutes by default: a suite's thousands of
                // statements, some of whose plans are large, filled the 2 GB heap until it stopped (measured), so it
                // is forgotten at once
                + "query.min-expire-age=1s\n"
                + "query.max-history=10\n"
                + "plugin.dir=" + root.resolve("plugin") + "\n", StandardCharsets.UTF_8);
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<String> command = new ArrayList<>(List.of(java, "-Xmx2G", "-XX:+UseG1GC", "-XX:+ExitOnOutOfMemoryError",
                "-Djdk.attach.allowAttachSelf=true", "--enable-native-access=ALL-UNNAMED",
                "--add-modules=jdk.incubator.vector", "-Dnode.environment=pct", "-Dnode.id=pct", "-Dnode.bind-ip=127.0.0.1",
                "-Dnode.internal-address=127.0.0.1",
                "-Dnode.data-dir=" + data.resolve("node"), "-Dconfig=" + config,
                "-Dcatalog.config-dir=" + data.resolve("etc/catalog"), "-Duser.timezone=UTC",
                "-cp", root.resolve("lib") + "/*", "io.trino.server.TrinoServer"));
        ProcessBuilder pb = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile()));
        pb.environment().remove("JAVA_TOOL_OPTIONS");
        server = pb.start();
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        HttpRequest info = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/v1/info"))
                .timeout(Duration.ofSeconds(2)).build();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(180);
        while (System.nanoTime() < deadline) {
            if (!server.isAlive()) {
                throw new IllegalStateException("the Trino coordinator exited " + server.exitValue()
                        + " before it started:\n" + read(log));
            }
            try {
                HttpResponse<String> r = http.send(info, HttpResponse.BodyHandlers.ofString());
                if (r.statusCode() == 200 && r.body().contains("\"starting\":false")) {
                    return;
                }
            } catch (IOException notYet) {
                // not listening yet
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted waiting for the Trino coordinator", e);
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted waiting for the Trino coordinator", e);
            }
        }
        stop();
        throw new IllegalStateException("the Trino coordinator did not start in 180 s:\n" + read(log));
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
