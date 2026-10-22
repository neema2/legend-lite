package com.legend.testing;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A REAL MongoDB for a test JVM (docs/MONGODB_PLAN_2026_10_16.md §5.3, D8), as {@link EmbeddedClickHouse} is a
 * ClickHouse: the pinned Community Server Bazel unpacked ({@code @embedded_mongodb}, tools/mongodb/mongodb.bzl),
 * started once per JVM on a free loopback port with access control on, its data in the test's own temporary
 * directory, and stopped when the JVM exits. No Docker: a test names the install's root with
 * {@code -Dembedded.mongodb.root=$(rlocationpath @embedded_mongodb//:mongodb/MONGODB_ROOT)} and asks for
 * {@link #shared()}.
 *
 * <p>The server runs with authentication, as a MongoDBConnection always declares one: the launcher makes the user
 * {@link #USER} in the database {@link #DATABASE} (through the server's localhost exception, which admits the first
 * user), with the password {@link #PASSWORD} -- a throwaway of this JVM's own server, bound to loopback only. A test
 * that declares the connection puts the password in the system property the connection's secret names.
 */
public final class EmbeddedMongoDb {

    /** The database the lanes use, and the user that owns it. */
    public static final String DATABASE = "pct";
    public static final String USER = "pct";
    /** The user's password: a throwaway for a server of this JVM alone, on loopback. */
    public static final String PASSWORD = "pct-embedded";

    private static EmbeddedMongoDb shared;
    /** Why the one start failed: every later {@link #shared()} says so instead of starting again. */
    private static IllegalStateException failed;

    private final Path root;
    private final Path data;
    private int port;
    private Process server;

    private EmbeddedMongoDb(Path root, Path data) {
        this.root = root;
        this.data = data;
    }

    /** This JVM's server, started on first use and stopped at exit. */
    public static synchronized EmbeddedMongoDb shared() {
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
            EmbeddedMongoDb started = shared;
            Runtime.getRuntime().addShutdownHook(new Thread(started::stop, "embedded-mongodb-stop"));
        } else if (!shared.server.isAlive()) {
            System.err.println("[embedded-mongodb] the server died (exit " + shared.server.exitValue()
                    + "); restarting it on port " + shared.port + ". Its log's tail:\n" + shared.logTail());
            try {
                shared.startServer();
            } catch (IOException e) {
                throw new IllegalStateException("the embedded MongoDB did not restart: " + e.getMessage(), e);
            }
        }
        return shared;
    }

    /** The server's port, on 127.0.0.1. */
    public int port() {
        return port;
    }

    /** A client of the lanes' database, as {@link #USER}: the caller closes it. */
    public MongoClient client() {
        return MongoClients.create(settings(MongoCredential.createCredential(USER, DATABASE, PASSWORD.toCharArray())));
    }

    private MongoClientSettings settings(MongoCredential credential) {
        MongoClientSettings.Builder b = MongoClientSettings.builder()
                .applyToClusterSettings(c -> c.hosts(List.of(new ServerAddress("127.0.0.1", port)))
                        .serverSelectionTimeout(10, TimeUnit.SECONDS));
        if (credential != null) {
            b.credential(credential);
        }
        return b.build();
    }

    private String logTail() {
        try {
            String log = read(data.resolve("mongod.log"));
            return log.substring(Math.max(0, log.length() - 3000));
        } catch (IOException e) {
            return "(its log could not be read: " + e.getMessage() + ")";
        }
    }

    private static EmbeddedMongoDb start() {
        String marker = System.getProperty("embedded.mongodb.root");
        if (marker == null) {
            throw new IllegalStateException("no -Dembedded.mongodb.root: give the test"
                    + " data = [\"@embedded_mongodb//:mongodb\", \"@embedded_mongodb//:mongodb/MONGODB_ROOT\"]"
                    + " and jvm_flags = [\"-Dembedded.mongodb.root=$(rlocationpath"
                    + " @embedded_mongodb//:mongodb/MONGODB_ROOT)\"]");
        }
        String tmp = System.getenv("TEST_TMPDIR");
        if (tmp == null) {
            throw new IllegalStateException("no TEST_TMPDIR: the embedded MongoDB runs under bazel test, its data in"
                    + " the test's own temporary directory");
        }
        try {
            Path root = Runfile.of(marker).toRealPath().getParent();
            Path data = Files.createTempDirectory(Path.of(tmp), "mongodb");
            EmbeddedMongoDb db = new EmbeddedMongoDb(root, data);
            IllegalStateException last = null;
            for (int attempt = 1; attempt <= 3; attempt++) {
                db.port = freePort();
                try {
                    db.startServer();
                    db.createUser();
                    return db;
                } catch (IllegalStateException e) {
                    if (!read(data.resolve("mongod.log")).contains("Address already in use")) {
                        throw e;   // not a taken port: no retry hides it
                    }
                    last = e;
                }
            }
            throw last;
        } catch (IOException e) {
            throw new IllegalStateException("the embedded MongoDB did not start: " + e.getMessage(), e);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return s.getLocalPort();
        }
    }

    /** Starts the server, returning once its port accepts a connection. A server that exits first fails. */
    private void startServer() throws IOException {
        Path db = data.resolve("db");
        Files.createDirectories(db);
        Path log = data.resolve("mongod.log");
        ProcessBuilder pb = new ProcessBuilder(root.resolve("bin/mongod").toString(),
                "--dbpath", db.toString(),
                "--port", String.valueOf(port),
                "--bind_ip", "127.0.0.1",
                "--auth",
                "--logpath", log.toString(), "--logappend",
                "--wiredTigerCacheSizeGB", "0.25",
                // no diagnostic data collection: the test's directory holds the data alone
                "--setParameter", "diagnosticDataCollectionEnabled=false")
                .directory(data.toFile()).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(data.resolve("stdout.log").toFile()));
        server = pb.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (System.nanoTime() < deadline) {
            if (!server.isAlive()) {
                throw new IllegalStateException("the MongoDB server exited " + server.exitValue() + " before it"
                        + " started:\n" + logTail());
            }
            try (Socket s = new Socket()) {
                s.connect(new java.net.InetSocketAddress(InetAddress.getLoopbackAddress(), port), 1000);
                return;
            } catch (IOException notYet) {
                // not listening yet
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted waiting for the MongoDB server", e);
            }
        }
        stop();
        throw new IllegalStateException("the MongoDB server did not start in 120 s:\n" + logTail());
    }

    /** The lanes' user, through the localhost exception (the first user, before any other exists): a root
     *  administrator first, then {@link #USER} on its database with that administrator's leave. */
    private void createUser() {
        String admin = "admin-" + port;
        try (MongoClient open = MongoClients.create(settings(null))) {
            open.getDatabase("admin").runCommand(createUser(admin, PASSWORD, "root", "admin"));
        }
        try (MongoClient asAdmin = MongoClients.create(settings(
                MongoCredential.createCredential(admin, "admin", PASSWORD.toCharArray())))) {
            asAdmin.getDatabase(DATABASE).runCommand(createUser(USER, PASSWORD, "dbOwner", DATABASE));
        }
    }

    private static BsonDocument createUser(String user, String password, String role, String database) {
        return new BsonDocument("createUser", new BsonString(user))
                .append("pwd", new BsonString(password))
                .append("roles", new BsonArray(List.of(new BsonDocument("role", new BsonString(role))
                        .append("db", new BsonString(database)))));
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
