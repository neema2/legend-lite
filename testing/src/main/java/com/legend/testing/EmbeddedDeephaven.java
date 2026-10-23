package com.legend.testing;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A REAL Deephaven for a test (docs/DEEPHAVEN_PLAN_2026_10_22.md DH8, L3b), as {@link EmbeddedTrino} is a Trino: the
 * pinned Community server Bazel unpacked ({@code @embedded_deephaven}, tools/deephaven/deephaven.bzl), started on a
 * free loopback port with HTTPS (Deephaven's browser protocol needs HTTP/2, which a browser speaks only over TLS), a
 * pre-shared key made for this run, and a 100 ms update cycle (the server's default is a second), and stopped when the
 * JVM exits. No Docker: a test names the install's root with
 * {@code -Dembedded.deephaven.root=$(rlocationpath @embedded_deephaven//:deephaven/DEEPHAVEN_ROOT)}.
 *
 * <p>The server runs on THIS JVM's Java as its child, from its main class (its start script passes a flag Java 25
 * removed), with the JVM options its build ships for Java 25. Its certificate is made here by the JDK's own
 * {@code keytool} (a self-signed key pair for {@code localhost} and {@code 127.0.0.1}) and written as the PEM files the
 * server reads; a browser told to accept it is the test's. Its Groovy console is on, so a test seeds its tables the
 * way a person would, and only here: a deployment turns it off (DH21).
 *
 * <p>With {@code -Dembedded.deephaven.application=$(rlocationpath <an .app file>)}, the server runs the Deephaven
 * application that file defines (its start-up script, beside it) every time it starts: a demo's data, seeded again by
 * every restart.
 *
 * <p>As a program ({@link #main}), for a browser test's driver: it prints one JSON line, the server's URL, key and
 * certificate, then runs until its input closes; a {@code restart} line restarts the server (on the same port, with the
 * same key and certificate) and prints {@code {"restarted": true}} once it is back.
 */
public final class EmbeddedDeephaven implements AutoCloseable {

    /** The authentication handler a session signs in with, its token {@link #psk()}. */
    public static final String PSK_HANDLER = "io.deephaven.authentication.psk.PskAuthenticationHandler";

    private final Path root;
    private final Path data;
    private final String psk;
    private final int port;
    private final Path application;
    private Process server;

    private EmbeddedDeephaven(Path root, Path data, String psk, int port, Path application)
            throws IOException {
        this.root = root;
        this.data = data;
        this.psk = psk;
        this.port = port;
        this.application = application;
        this.server = launch();
    }

    /** The server's HTTPS origin. */
    public String url() {
        return "https://127.0.0.1:" + port;
    }

    /** The pre-shared key this run's sessions sign in with. */
    public String psk() {
        return psk;
    }

    /** The server's certificate, PEM. */
    public Path certificate() {
        return data.resolve("cert.pem");
    }

    /** Starts the server in a fresh directory under {@code TEST_TMPDIR}, returning once it says it has started. */
    public static EmbeddedDeephaven start() {
        String marker = System.getProperty("embedded.deephaven.root");
        if (marker == null) {
            throw new IllegalStateException("no -Dembedded.deephaven.root: give the test"
                    + " data = [\"@embedded_deephaven//:deephaven\", \"@embedded_deephaven//:deephaven/DEEPHAVEN_ROOT\"]"
                    + " and jvm_flags = [\"-Dembedded.deephaven.root="
                    + "$(rlocationpath @embedded_deephaven//:deephaven/DEEPHAVEN_ROOT)\"]");
        }
        String tmp = System.getenv("TEST_TMPDIR");
        if (tmp == null) {
            throw new IllegalStateException("no TEST_TMPDIR: the embedded Deephaven runs under bazel test, its data in"
                    + " the test's own temporary directory");
        }
        try {
            Path root = Runfile.of(marker).toRealPath().getParent();
            Path data = Files.createTempDirectory(Path.of(tmp), "deephaven");
            for (String d : List.of("cache", "data", "config")) {
                Files.createDirectories(data.resolve(d));
            }
            certificate(data);
            byte[] key = new byte[24];
            new SecureRandom().nextBytes(key);
            String psk = HexFormat.of().formatHex(key);
            int port;
            try (ServerSocket s = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
                port = s.getLocalPort();
            }
            String app = System.getProperty("embedded.deephaven.application");
            Path application = app == null ? null : Runfile.of(app).toRealPath().getParent();
            return new EmbeddedDeephaven(root, data, psk, port, application);
        } catch (IOException e) {
            throw new IllegalStateException("the embedded Deephaven did not start: " + e.getMessage(), e);
        }
    }

    /** Stops the server and starts it again, on the same port with the same key and certificate; Deephaven keeps
     *  nothing across it, and its application seeds again. */
    public void restart() throws IOException {
        stop();
        server = launch();
    }

    private Process launch() throws IOException {
        Path log = data.resolve("server.log");
        Files.deleteIfExists(log);
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Path etc = root.resolve("etc");
        List<String> command = new ArrayList<>(List.of(java, "-server", "-Xmx1g",
                "--add-exports", "java.management/sun.management=ALL-UNNAMED",
                "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens", "java.base/java.nio=ALL-UNNAMED",
                "-XX:+UnlockDiagnosticVMOptions", "-XX:CompilerDirectivesFile=" + etc.resolve("dh-compiler-directives.txt"),
                "-XX:VMOptionsFile=" + etc.resolve("dh-default-25.vmoptions"),
                "-Dauthentication.client.configuration.list=AuthHandlers",
                "-Dauthentication.psk=" + psk,
                "-Ddeephaven.console.type=groovy",
                "-Ddeephaven.cacheDir=" + data.resolve("cache"),
                "-Ddeephaven.dataDir=" + data.resolve("data"),
                "-Ddeephaven.configDir=" + data.resolve("config"),
                "-Dhttp.host=127.0.0.1",
                "-Dhttp.port=" + port,
                "-Dssl.identity.type=privatekey",
                "-Dssl.identity.certChainPath=" + data.resolve("cert.pem"),
                "-Dssl.identity.privateKeyPath=" + data.resolve("key.pem"),
                "-DPeriodicUpdateGraph.targetCycleDurationMillis=100"));
        if (application != null) {
            command.add("-Ddeephaven.application.dir=" + application);
        }
        command.addAll(List.of("-cp", root.resolve("lib") + "/*", "io.deephaven.server.jetty.JettyMain"));
        ProcessBuilder pb = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile()));
        pb.environment().remove("JAVA_TOOL_OPTIONS");
        Process server = pb.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(180);
        while (System.nanoTime() < deadline) {
            String text = read(log);
            if (text.contains("Server started on port " + port)) {
                return server;
            }
            if (!server.isAlive()) {
                throw new IllegalStateException("the Deephaven server exited " + server.exitValue()
                        + " before it started:\n" + tail(text));
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                server.destroyForcibly();
                throw new IllegalStateException("interrupted waiting for the Deephaven server", e);
            }
        }
        server.destroyForcibly();
        throw new IllegalStateException("the Deephaven server did not start in 180 s:\n" + tail(read(log)));
    }

    /** A self-signed key pair for localhost and 127.0.0.1, made by the JDK's keytool, written as cert.pem and key.pem. */
    private static void certificate(Path data) throws IOException {
        Path store = data.resolve("identity.p12");
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        String password = "deephaven-test";
        ProcessBuilder pb = new ProcessBuilder(keytool, "-genkeypair", "-alias", "deephaven", "-keyalg", "RSA",
                "-keysize", "2048", "-validity", "2", "-dname", "CN=localhost",
                "-ext", "san=dns:localhost,ip:127.0.0.1", "-keystore", store.toString(), "-storetype", "PKCS12",
                "-storepass", password, "-keypass", password).redirectErrorStream(true);
        pb.environment().remove("JAVA_TOOL_OPTIONS");
        Process p = pb.start();
        String out;
        try (InputStream in = p.getInputStream()) {
            out = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        try {
            if (p.waitFor() != 0) {
                throw new IOException("keytool failed: " + out);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted making the certificate", e);
        }
        try (InputStream in = Files.newInputStream(store)) {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(in, password.toCharArray());
            Key key = ks.getKey("deephaven", password.toCharArray());
            Certificate cert = ks.getCertificate("deephaven");
            if (!(key instanceof PrivateKey)) {
                throw new IOException("keytool's store holds no private key");
            }
            Files.writeString(data.resolve("key.pem"), pem("PRIVATE KEY", key.getEncoded()), StandardCharsets.UTF_8);
            Files.writeString(data.resolve("cert.pem"), pem("CERTIFICATE", cert.getEncoded()), StandardCharsets.UTF_8);
        } catch (java.security.GeneralSecurityException e) {
            throw new IOException("the certificate keytool made could not be read: " + e.getMessage(), e);
        }
    }

    private static String pem(String what, byte[] der) {
        return "-----BEGIN " + what + "-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der)
                + "\n-----END " + what + "-----\n";
    }

    private static String read(Path log) throws IOException {
        return Files.exists(log) ? Files.readString(log, StandardCharsets.UTF_8) : "";
    }

    private static String tail(String log) {
        return log.substring(Math.max(0, log.length() - 3000));
    }

    @Override
    public void close() {
        stop();
    }

    private void stop() {
        if (server.isAlive()) {
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

    /** For a browser test's driver: starts the server, prints {@code {"url", "psk", "certificate"}} on one line, and
     *  runs until standard input closes. */
    public static void main(String[] args) throws IOException {
        try (EmbeddedDeephaven dh = start()) {
            System.out.println("{\"url\": \"" + dh.url() + "\", \"psk\": \"" + dh.psk() + "\", \"certificate\": \""
                    + dh.certificate().toString().replace("\\", "\\\\") + "\"}");
            System.out.flush();
            java.io.BufferedReader in = new java.io.BufferedReader(
                    new java.io.InputStreamReader(System.in, StandardCharsets.UTF_8));
            for (String line; (line = in.readLine()) != null; ) {
                if (line.trim().equals("restart")) {
                    dh.restart();
                    System.out.println("{\"restarted\": true}");
                    System.out.flush();
                }
            }
        }
    }
}
