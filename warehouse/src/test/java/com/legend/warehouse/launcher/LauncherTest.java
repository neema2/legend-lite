package com.legend.warehouse.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.legend.base.Nullable;
import com.legend.testing.EmbeddedPostgres;
import com.legend.testing.Runfile;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * //warehouse:serve as `bazel run` starts it (docs/WINDOWS_APP_DESIGN_2026_10_02.md, §3): the bash launcher
 * on Linux and macOS, hermetic-launcher on Windows. Nothing else runs a warehouse_run launcher on every
 * platform; {@code //datacube:verify_app} is manual. {@code :launcher_test_serve_site} is the launcher in
 * {@code //datacube:app}'s shape: a site, and fixed arguments before the caller's.
 */
class LauncherTest {

    /** {@code $(rootpath //warehouse:serve)}: this platform's launcher, in this test's runfiles. */
    private static final Path LAUNCHER = Runfile.of(required("WAREHOUSE_SERVE"));

    /** {@code $(rootpath //warehouse:launcher_test_serve_site)}: a site, and {@code --single-user} fixed. */
    private static final Path LAUNCHER_WITH_SITE = Runfile.of(required("WAREHOUSE_SERVE_SITE"));

    @Test
    void theCallersArgumentsReachTheServerAndItsExitCodeComesBack() throws Exception {
        // one argument holding both an '&' and a space: a .bat, or Bazel's bash launcher on Windows, cuts it
        // at the '&', and a launcher that leaves the space unquoted splits it in two
        Process p = start(LAUNCHER, List.of("--port", "x&y z"));
        List<String> said = linesUntilExit(p);
        assertEquals(2, p.exitValue(), String.join("\n", said));
        assertTrue(said.contains("warehouse: For input string: \"x&y z\""), String.join("\n", said));
    }

    @Test
    void theServerAndThePostgresExtensionDirectoryResolveThroughTheLauncher() throws Exception {
        EmbeddedPostgres pg = EmbeddedPostgres.shared();
        Path data = Files.createTempDirectory(Path.of(required("TEST_TMPDIR")), "launcher-data");
        // a catalog by URL, '&' and all: attaching it loads the postgres extension from the directory the
        // launcher named (without --duckdb-extensions a native server looks beside its executable, where
        // no extension sits). DuckDB's library is not judged here: the launcher passes --duckdb-library,
        // but the server would find the library beside itself too, where Bazel puts it.
        String url = "postgresql://" + EmbeddedPostgres.USER + "@127.0.0.1:" + pg.port()
                + "/postgres?sslmode=disable&connect_timeout=10";
        Process p = start(LAUNCHER, List.of("--data", data.toString(), "--port", "0", "--user", "alice:alice-pw", url));
        try {
            String listening = awaitLine(p, "warehouse listening on ");
            assertTrue(listening.matches("warehouse listening on 127\\.0\\.0\\.1:\\d+, catalogs \\[main, postgres\\]"),
                    listening);
        } finally {
            stop(p);
        }
    }

    @Test
    void theSiteResolvesThroughTheLauncherAndTheFixedArgumentsReachTheServer() throws Exception {
        Path data = Files.createTempDirectory(Path.of(required("TEST_TMPDIR")), "launcher-site-data");
        // the server prints the page's address only with --site and --single-user, the launcher's own two
        Process p = start(LAUNCHER_WITH_SITE, List.of("--data", data.toString(), "--port", "0"));
        try {
            String address = awaitLine(p, "DataCube: ");
            Matcher m = Pattern.compile("DataCube: (http://127\\.0\\.0\\.1:\\d+)/#key=\\S+").matcher(address);
            assertTrue(m.matches(), address);
            // and the page is the site's directory, named through runfiles
            HttpResponse<String> page;
            try (HttpClient http = HttpClient.newHttpClient()) {
                page = http.send(HttpRequest.newBuilder(URI.create(m.group(1) + "/")).build(),
                        HttpResponse.BodyHandlers.ofString());
            }
            assertEquals(200, page.statusCode(), page.body());
            assertTrue(page.body().contains("served through the launcher"), page.body());
        } finally {
            stop(p);
        }
    }

    @Test
    void theDefaultDataDirectoryIsWhereBazelRunWasStarted() throws Exception {
        // `bazel run` from here: on Windows the launcher starts the server in its runfiles folder, where
        // `bazel clean` would take the default --data warehouse-data with it
        Path startedIn = Files.createTempDirectory(Path.of(required("TEST_TMPDIR")), "started-in");
        Process p = start(LAUNCHER, List.of("--port", "0", "--user", "alice:alice-pw"), startedIn);
        try {
            awaitLine(p, "warehouse listening on ");
            assertTrue(Files.isDirectory(startedIn.resolve("warehouse-data")),
                    "no warehouse-data in " + startedIn);
        } finally {
            stop(p);
        }
    }

    private static Process start(Path launcher, List<String> args) throws IOException {
        return start(launcher, args, null);
    }

    /** {@code startedIn}: where `bazel run` was started ({@code BUILD_WORKING_DIRECTORY}), or null. */
    private static Process start(Path launcher, List<String> args, @Nullable Path startedIn) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(launcher.toString());
        command.addAll(args);
        ProcessBuilder b = new ProcessBuilder(command).redirectErrorStream(true);
        // the launcher finds the server, DuckDB's library and the extension in this test's runfiles
        b.environment().putAll(Runfile.env());
        b.environment().remove("BUILD_WORKING_DIRECTORY");
        if (startedIn != null) b.environment().put("BUILD_WORKING_DIRECTORY", startedIn.toString());
        return b.start();
    }

    /** Everything the launcher and the server printed, once both have exited. */
    private static List<String> linesUntilExit(Process p) throws IOException, InterruptedException {
        List<String> lines;
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            lines = r.lines().toList();
        }
        assertTrue(p.waitFor(60, TimeUnit.SECONDS), "the launcher did not exit");
        return lines;
    }

    /** The first line starting with {@code prefix}, within two minutes; failing that, what was printed. */
    private static String awaitLine(Process p, String prefix) throws Exception {
        // the reader appends while a failure message is built: join a copy (toArray holds the list's lock;
        // iterating a synchronized list does not)
        List<String> seen = Collections.synchronizedList(new ArrayList<>());
        CompletableFuture<String> found = new CompletableFuture<>();
        Thread reader = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                // read to the end, so a server that keeps printing never blocks on a full pipe
                for (String line = r.readLine(); line != null; line = r.readLine()) {
                    if (line.startsWith(prefix)) found.complete(line);
                    seen.add(line);
                }
            } catch (IOException e) {
                found.completeExceptionally(e);
            }
            found.completeExceptionally(new IllegalStateException(
                    "the launcher exited before printing '" + prefix + "':\n"
                            + String.join("\n", seen.toArray(String[]::new))));
        }, "launcher-output");
        reader.setDaemon(true);
        reader.start();
        try {
            return found.get(120, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            return fail("no line '" + prefix + "…' in 120 s:\n" + String.join("\n", seen.toArray(String[]::new)));
        }
    }

    /** The launcher and the server it started: on Windows two processes; elsewhere one, the launcher exec'd it. */
    private static void stop(Process p) throws InterruptedException {
        p.descendants().forEach(ProcessHandle::destroy);
        p.destroy();
        if (!p.waitFor(30, TimeUnit.SECONDS)) {
            p.descendants().forEach(ProcessHandle::destroyForcibly);
            p.destroyForcibly().waitFor();
        }
    }

    private static String required(String variable) {
        String value = System.getenv(variable);
        if (value == null) throw new IllegalStateException(variable + " is not set: run //warehouse:launcher_test");
        return value;
    }
}
